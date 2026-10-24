package docker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"regexp"
	"slices"
	"sync"
	"time"

	"github.com/docker/docker/client"
)

// Reconnect policy for one container's stats stream. A stream that ends is
// reopened after an exponential delay from 500ms up to a 30s cap, with equal
// jitter so containers do not reconnect in lockstep after a daemon restart. The
// cap keeps a long outage from spinning the socket, and retries continue
// indefinitely because giving up would silently stop collection.
const (
	streamRetryBase = 500 * time.Millisecond
	streamRetryMax  = 30 * time.Second
)

// Options configures a Supervisor.
type Options struct {
	// Socket is the Docker Engine Unix socket path.
	Socket string
	// Exclude is an unanchored regular expression matched against container
	// names. Empty matches every container.
	Exclude string
	// RescanInterval is how often the running container set is refreshed.
	RescanInterval time.Duration
	// StreamStats opens one reconnecting stats stream per container instead of
	// leaving stats acquisition to one-shot polling.
	StreamStats bool
}

// Supervisor owns the Docker client, the running container set, and in stream
// mode the per-container stats streams.
//
// The client is created and probed by NewSupervisor and released by Close. Run
// does not close the client, so a caller that constructs a supervisor and never
// runs it still has one obvious cleanup path. Run is single-use; Containers may
// be called concurrently with it, and Close must be called after Run returns.
type Supervisor struct {
	client       *client.Client
	acquirer     *statsAcquirer
	logger       *slog.Logger
	exclude      *regexp.Regexp
	rescan       time.Duration
	streamStats  bool
	stats        chan rawStats
	containersMu sync.RWMutex
	containers   []Container

	workersMu sync.Mutex
	workers   map[string]*streamWorker
	workersWG sync.WaitGroup

	// discoveryErr is only touched by the single Run goroutine. It suppresses
	// repeated warnings for the same persistent discovery failure.
	discoveryErr string

	closeOnce sync.Once
	closeErr  error
}

// streamWorker is one container's stream loop and the signal that its goroutine
// has exited. Removal waits on done before the new container snapshot becomes
// visible, so a removed container's goroutine is gone before callers observe
// the change.
type streamWorker struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// NewSupervisor connects to the daemon, probes it, and performs the initial
// container discovery. A missing or unreachable socket, an incompatible API
// version, or a failed first listing is returned as an error so an enabled
// Docker source can fail startup instead of collecting nothing.
func NewSupervisor(ctx context.Context, opts Options, logger *slog.Logger) (*Supervisor, error) {
	switch {
	case opts.Socket == "":
		return nil, fmt.Errorf("docker: socket path is empty")
	case opts.RescanInterval <= 0:
		return nil, fmt.Errorf("docker: rescan interval must be positive; got %s", opts.RescanInterval)
	}

	var exclude *regexp.Regexp
	if opts.Exclude != "" {
		compiled, err := regexp.Compile(opts.Exclude)
		if err != nil {
			return nil, fmt.Errorf("docker: exclude: %w", err)
		}
		exclude = compiled
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	c, err := Connect(ctx, opts.Socket)
	if err != nil {
		return nil, err
	}
	containers, err := discover(ctx, c, exclude)
	if err != nil {
		_ = c.Close()
		return nil, err
	}

	s := &Supervisor{
		client:      c,
		acquirer:    newStatsAcquirer(c, logger),
		logger:      logger,
		exclude:     exclude,
		rescan:      opts.RescanInterval,
		streamStats: opts.StreamStats,
		containers:  containers,
		workers:     make(map[string]*streamWorker),
	}
	if opts.StreamStats {
		// One pending payload per container known at startup gives a tick
		// something to drain without letting a stalled consumer grow memory.
		// When this buffer fills, workers block and the per-stream one-slot
		// buffer applies its documented drop policy.
		s.stats = make(chan rawStats, max(1, len(containers)))
	}
	return s, nil
}

// Containers returns a copy of the current container set, sorted by name then
// ID. Stopped containers and containers rejected by the exclude filter are
// absent.
func (s *Supervisor) Containers() []Container {
	s.containersMu.RLock()
	defer s.containersMu.RUnlock()
	return slices.Clone(s.containers)
}

// Run rescans the container set until ctx is canceled. Discovery failures are
// logged and retried on the next rescan rather than stopping the process. In
// stream mode it also starts one reconnecting stats stream per container,
// cancels streams for removed containers, and closes the stats channel after
// every stream goroutine has exited. Run does not close the client.
func (s *Supervisor) Run(ctx context.Context) {
	if s.streamStats {
		s.syncStreams(ctx, s.Containers())
		defer s.stopStreams()
	}

	ticker := time.NewTicker(s.rescan)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			containers, err := discover(ctx, s.client, s.exclude)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				s.warnDiscovery(err)
				continue
			}
			s.clearDiscoveryWarning(len(containers))
			if s.streamStats {
				s.syncStreams(ctx, containers)
				if ctx.Err() != nil {
					return
				}
			}
			s.setContainers(containers)
		}
	}
}

// Close releases the Docker client. It is idempotent.
func (s *Supervisor) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.client.Close()
	})
	return s.closeErr
}

// setContainers replaces the published snapshot. Callers pass a freshly
// discovered slice that they do not retain.
func (s *Supervisor) setContainers(containers []Container) {
	s.containersMu.Lock()
	s.containers = containers
	s.containersMu.Unlock()
}

// warnDiscovery logs the first occurrence of a persistent discovery failure at
// warn and later repeats at debug. A different error is a different failure and
// is reported again.
func (s *Supervisor) warnDiscovery(err error) {
	message := err.Error()
	if message != s.discoveryErr {
		s.discoveryErr = message
		s.logger.Warn("discover containers; retrying on next rescan", "error", err)
		return
	}
	s.logger.Debug("container discovery still failing", "error", err)
}

// clearDiscoveryWarning re-arms warning suppression after a successful scan.
func (s *Supervisor) clearDiscoveryWarning(containers int) {
	if s.discoveryErr == "" {
		return
	}
	s.discoveryErr = ""
	s.logger.Info("container discovery recovered", "containers", containers)
}

// syncStreams reconciles the running workers with containers. Unchanged
// containers keep their existing stream; added containers get a new worker;
// removed containers are cancelled and joined before syncStreams returns.
func (s *Supervisor) syncStreams(ctx context.Context, containers []Container) {
	desired := make(map[string]struct{}, len(containers))
	for _, c := range containers {
		desired[c.ID] = struct{}{}
	}

	var removed []*streamWorker
	s.workersMu.Lock()
	for id, worker := range s.workers {
		if _, keep := desired[id]; keep {
			continue
		}
		worker.cancel()
		delete(s.workers, id)
		removed = append(removed, worker)
	}
	for _, c := range containers {
		if _, running := s.workers[c.ID]; running {
			continue
		}
		workerCtx, cancel := context.WithCancel(ctx)
		worker := &streamWorker{cancel: cancel, done: make(chan struct{})}
		s.workers[c.ID] = worker
		s.workersWG.Add(1)
		go func(id string, worker *streamWorker) {
			defer s.workersWG.Done()
			defer close(worker.done)
			s.runWorker(workerCtx, id)
		}(c.ID, worker)
	}
	s.workersMu.Unlock()

	for _, worker := range removed {
		select {
		case <-worker.done:
		case <-ctx.Done():
			return
		}
	}
}

// stopStreams cancels every worker, waits for all of them, and only then
// closes the stats channel. Closing after the join is what makes channel close
// a leak-free shutdown signal for the consumer.
func (s *Supervisor) stopStreams() {
	s.workersMu.Lock()
	for id, worker := range s.workers {
		worker.cancel()
		delete(s.workers, id)
	}
	s.workersMu.Unlock()

	s.workersWG.Wait()
	close(s.stats)
}

// runWorker keeps one container's stats stream open. It reuses the acquirer's
// single decoder and drop policy: each iteration owns the channel returned by
// stream and reopens it after a jittered backoff when it closes. Backoff is
// reset only after a connection delivered a payload, so a daemon that accepts
// connections and immediately closes them still backs off.
func (s *Supervisor) runWorker(ctx context.Context, id string) {
	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}

		stream := s.acquirer.stream(ctx, []string{id})
		delivered := false
		for payload := range stream {
			if !delivered {
				delivered = true
				if attempt > 0 {
					s.logger.Info("container stats stream reconnected", "container", id)
				}
				attempt = 0
			}
			select {
			case s.stats <- payload:
			case <-ctx.Done():
				// The acquirer's reader and channel closer must finish before
				// this worker is considered stopped. Draining waits for the
				// close, which happens after the reader observes cancellation.
				for range stream {
				}
				return
			}
		}
		if ctx.Err() != nil {
			return
		}

		if delivered {
			attempt = 0
		}
		attempt++
		delay := streamJitter(streamBackoff(attempt), rand.Float64())
		if attempt == 1 {
			s.logger.Warn("container stats stream ended; reconnecting",
				"container", id, "retry_in", delay)
		} else {
			s.logger.Debug("container stats stream still unavailable; retrying",
				"container", id, "attempt", attempt, "retry_in", delay)
		}
		if !sleepContext(ctx, delay) {
			return
		}
	}
}

// streamBackoff returns the capped exponential delay before the next attempt.
// attempt is one-based, so the first retry waits streamRetryBase.
func streamBackoff(attempt int) time.Duration {
	delay := streamRetryBase
	for i := 1; i < attempt && delay < streamRetryMax; i++ {
		delay *= 2
	}
	if delay > streamRetryMax {
		return streamRetryMax
	}
	return delay
}

// streamJitter spreads a delay over the second half of its range, from d/2 up
// to but not including d. The fixed half keeps reconnects progressing while the
// random half desynchronizes containers after a daemon restart. u is in [0, 1).
func streamJitter(d time.Duration, u float64) time.Duration {
	half := d / 2
	return half + time.Duration(u*float64(half))
}

// sleepContext waits for d or until ctx is canceled, reporting whether the wait
// completed.
func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
