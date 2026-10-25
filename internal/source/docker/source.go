package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/confuzeus/smob/internal/model"
)

const (
	// scope is the collector scope this source reports. The collector adds the
	// host and scope labels to every sample.
	scope = "container"

	// maxPendingRateSamples bounds the stream bridge when the collector falls
	// behind. It holds about three rate samples per container per observation,
	// so this is roughly a minute of backlog for fifty containers with one
	// interface and a one-second stream cadence. When the limit is reached the
	// oldest samples are dropped so the next tick sees the freshest data.
	maxPendingRateSamples = 16384
)

// Source collects per-container CPU, memory, and network metrics from the
// Docker Engine API. The collector drives Collect from one goroutine; Start and
// Close own the supervisor's goroutines.
type Source struct {
	supervisor *Supervisor
	acquirer   *statsAcquirer
	logger     *slog.Logger
	interval   time.Duration
	stream     bool

	// mu guards the session fields below and, in stream mode, the accumulator,
	// pending buffer, and metadata cache shared by Collect and consume. In
	// one-shot mode Collect is the only accumulator user and Start runs no
	// bridge, so the accumulator is not shared.
	mu           sync.Mutex
	acc          *accumulator
	containers   map[string]Container
	rates        sampleBuffer
	memory       map[string]model.Sample
	bufferWarned bool

	started    bool
	cancel     context.CancelFunc
	runDone    chan struct{}
	bridgeDone chan struct{}

	closeOnce sync.Once
	closeErr  error
}

// New returns a Docker container source. It connects to the daemon and
// performs the initial container discovery, so an enabled source fails at
// startup rather than on the first tick. The caller owns the source: call
// Start for the collection lifetime and Close after the collector returns.
func New(ctx context.Context, opts Options, logger *slog.Logger) (*Source, error) {
	if opts.Interval <= 0 {
		return nil, fmt.Errorf("docker: interval must be positive; got %s", opts.Interval)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	supervisor, err := NewSupervisor(ctx, opts, logger)
	if err != nil {
		return nil, err
	}

	return &Source{
		supervisor: supervisor,
		acquirer:   supervisor.acquirer,
		logger:     logger,
		interval:   opts.Interval,
		stream:     opts.StreamStats,
		acc:        newAccumulator(logger),
		containers: containersByID(supervisor.Containers()),
		rates:      newSampleBuffer(maxPendingRateSamples),
		memory:     make(map[string]model.Sample),
	}, nil
}

// Scope implements the collector's Source interface.
func (s *Source) Scope() string { return scope }

// Interval implements the collector's Source interface.
func (s *Source) Interval() time.Duration { return s.interval }

// Collect implements the collector's Source interface.
//
// In one-shot mode it fetches one payload per discovered container, observes
// each at now, and prunes state using the discovered ID set. In stream mode it
// drains observations buffered between ticks and ignores now, because each
// streamed payload is timestamped when it arrives; using the tick time would
// misreport interval_ms.
func (s *Source) Collect(ctx context.Context, now time.Time) ([]model.Sample, error) {
	if s.stream {
		return s.collectStream(ctx)
	}
	return s.collectOneShot(ctx, now)
}

// Start runs container discovery and, in stream mode, the bridge that turns
// streamed payloads into pending samples. It must be called before the
// collector runs; one-shot Collect still works without Start against the
// snapshot taken by New, but only Start rescans for new and removed
// containers. Repeated calls are no-ops. Close does not return until the
// goroutines started here have stopped.
func (s *Source) Start(ctx context.Context) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.started = true
	s.cancel = cancel
	s.runDone = make(chan struct{})
	if s.stream {
		s.bridgeDone = make(chan struct{})
	}
	runDone, bridgeDone := s.runDone, s.bridgeDone
	s.mu.Unlock()

	go func() {
		defer close(runDone)
		s.supervisor.Run(runCtx)
	}()
	if bridgeDone != nil {
		go func() {
			defer close(bridgeDone)
			s.consume()
		}()
	}
}

// Close stops the supervisor and the stream bridge, waits for both, and
// releases the Docker client. It is idempotent.
func (s *Source) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		cancel := s.cancel
		runDone := s.runDone
		bridgeDone := s.bridgeDone
		s.mu.Unlock()

		if cancel != nil {
			cancel()
			<-runDone
			if bridgeDone != nil {
				<-bridgeDone
			}
		}
		s.closeErr = s.supervisor.Close()
	})
	return s.closeErr
}

// collectOneShot fetches one stats payload per discovered container. A
// container that fails or disappears is omitted by the acquirer, never
// returned as an error.
func (s *Source) collectOneShot(ctx context.Context, now time.Time) ([]model.Sample, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	containers := s.supervisor.Containers()
	byID := containersByID(containers)
	ids := make([]string, len(containers))
	for i, container := range containers {
		ids[i] = container.ID
	}

	payloads, err := s.acquirer.oneShot(ctx, ids)
	if err != nil {
		return nil, err
	}
	samples := make([]model.Sample, 0, 4*len(payloads))
	for _, payload := range payloads {
		container, ok := byID[payload.containerID]
		if !ok {
			continue
		}
		memory, rates := containerSamples(container, s.acc.observe(payload, now), now)
		samples = append(samples, memory)
		samples = append(samples, rates...)
	}
	// Prune with the discovered set, not the payloads that succeeded: a failed
	// fetch must not reset a live container's rate baseline.
	s.acc.prune(ids)
	return samples, nil
}

// collectStream drains observations buffered since the previous tick. It never
// waits for the stream; an empty buffer is a valid tick.
func (s *Source) collectStream(ctx context.Context) ([]model.Sample, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return nil, errors.New("docker: stream stats requires Start before Collect")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.containers = containersByID(s.supervisor.Containers())
	ids := make([]string, 0, len(s.containers))
	for id := range s.containers {
		ids = append(ids, id)
	}
	s.acc.prune(ids)

	samples := s.drainRates()
	for id, sample := range s.memory {
		if _, ok := s.containers[id]; !ok {
			continue
		}
		samples = append(samples, sample)
	}
	clear(s.memory)
	s.bufferWarned = false
	model.SortSamples(samples)
	return samples, nil
}

// drainRates drains the buffered rate samples and drops any whose container is
// absent from the current snapshot. Rates carry no container ID, so a container
// removed between ticks can still have up to one tick of buffered rates; writing
// them would extend a vanished container's series with stale labels. Memory
// gauges are coalesced per ID and filtered by the caller. The caller must hold
// s.mu.
func (s *Source) drainRates() []model.Sample {
	drained := s.rates.drain()
	if len(drained) == 0 {
		return nil
	}

	live := make(map[[2]string]struct{}, len(s.containers))
	for _, container := range s.containers {
		live[[2]string{container.Name, container.Image}] = struct{}{}
	}

	kept := drained[:0]
	for _, sample := range drained {
		key := [2]string{sample.Labels["container"], sample.Labels["image"]}
		if _, ok := live[key]; ok {
			kept = append(kept, sample)
		}
	}
	return kept
}

// consume is the stream bridge. It runs for the life of the source, timestamps
// each payload on receipt, and buffers the result for the next Collect. It
// exits when Supervisor.Run closes the stats channel after joining every
// stream worker.
func (s *Source) consume() {
	for payload := range s.supervisor.stats {
		at := time.Now()

		s.mu.Lock()
		container, ok := s.containers[payload.containerID]
		if !ok {
			// The payload can arrive before the supervisor publishes a newly
			// discovered container. Refresh once; a still-unknown ID is dropped
			// because there is no trustworthy name or image to label it with.
			s.containers = containersByID(s.supervisor.Containers())
			container, ok = s.containers[payload.containerID]
		}
		if !ok {
			s.mu.Unlock()
			continue
		}

		memory, rates := containerSamples(container, s.acc.observe(payload, at), at)
		s.memory[payload.containerID] = memory
		for _, rate := range rates {
			if s.rates.add(rate) && !s.bufferWarned {
				s.bufferWarned = true
				s.logger.Warn("container metrics buffer full; dropping oldest samples",
					"capacity", maxPendingRateSamples)
			}
		}
		s.mu.Unlock()
	}
}

// containerSamples converts one observation into its memory gauge and rate
// samples. Labels are series labels only; the collector adds host and scope.
func containerSamples(container Container, obs containerMetrics, at time.Time) (model.Sample, []model.Sample) {
	timestamp := at.UTC()
	labels := map[string]string{
		"container": container.Name,
		"image":     container.Image,
	}
	memory := model.Sample{
		Timestamp: timestamp,
		Metric:    metricContainerMemoryUsedBytes,
		Value:     float64(obs.memoryUsedBytes),
		Labels:    labels,
	}

	interval := model.Millis(obs.window)
	rates := make([]model.Sample, 0, 1+2*len(obs.networks))
	if obs.cpuKnown {
		rates = append(rates, model.Sample{
			Timestamp:  timestamp,
			Metric:     metricContainerCPUPercent,
			Value:      obs.cpuPercent,
			IntervalMS: interval,
			Labels:     labels,
		})
	}
	for _, network := range obs.networks {
		netLabels := map[string]string{
			"container": container.Name,
			"image":     container.Image,
			"interface": network.name,
		}
		if network.rxKnown {
			rates = append(rates, model.Sample{
				Timestamp:  timestamp,
				Metric:     metricContainerNetworkRXBytesPerSecond,
				Value:      network.rx,
				IntervalMS: interval,
				Labels:     netLabels,
			})
		}
		if network.txKnown {
			rates = append(rates, model.Sample{
				Timestamp:  timestamp,
				Metric:     metricContainerNetworkTXBytesPerSecond,
				Value:      network.tx,
				IntervalMS: interval,
				Labels:     netLabels,
			})
		}
	}
	return memory, rates
}

// containersByID indexes a discovered container set. A container re-created
// with the same name and image has a new ID, so its rate baseline is new, but
// the series labels are unchanged and the samples continue the same series
// after a gap.
func containersByID(containers []Container) map[string]Container {
	byID := make(map[string]Container, len(containers))
	for _, container := range containers {
		byID[container.ID] = container
	}
	return byID
}

// sampleBuffer is a bounded FIFO of rate samples. When it is full, adding a
// sample drops the oldest one, so a lagging collector resumes with fresh
// observations rather than a stale backlog.
type sampleBuffer struct {
	samples []model.Sample
	limit   int
}

// newSampleBuffer returns an empty buffer with the given maximum size.
func newSampleBuffer(limit int) sampleBuffer {
	if limit < 1 {
		limit = 1
	}
	return sampleBuffer{
		samples: make([]model.Sample, 0, limit),
		limit:   limit,
	}
}

// add appends sample, dropping the oldest sample first when the buffer is full.
// It reports whether a sample was dropped.
func (b *sampleBuffer) add(sample model.Sample) bool {
	dropped := false
	for len(b.samples) >= b.limit {
		copy(b.samples, b.samples[1:])
		b.samples = b.samples[:len(b.samples)-1]
		dropped = true
	}
	b.samples = append(b.samples, sample)
	return dropped
}

// drain returns the buffered samples in insertion order and empties the
// buffer.
func (b *sampleBuffer) drain() []model.Sample {
	if len(b.samples) == 0 {
		return nil
	}
	out := b.samples
	b.samples = make([]model.Sample, 0, b.limit)
	return out
}
