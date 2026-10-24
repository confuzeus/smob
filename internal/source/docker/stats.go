package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// oneShotConcurrency bounds how many stats requests run at once during one
// one-shot poll. Eight parallel requests keep a tick short on a host with many
// containers without flooding the daemon or saturating the Unix socket; the
// collector's per-tick timeout is still the hard wall-clock bound.
const oneShotConcurrency = 8

// rawStats is one container's stats payload exactly as the daemon returned it.
// Acquisition does not interpret CPU, memory, or network values; the source
// computes metrics from these payloads later.
type rawStats struct {
	containerID string
	stats       container.StatsResponse
}

// statsAcquirer fetches raw container stats from one Docker client. The caller
// owns the client; the acquirer never closes it.
//
// One-shot is the default mode: the collector calls oneShot on each tick. Its
// failure memo is mutable, so one-shot calls must be driven from one goroutine.
// stream is the opt-in mode for sub-interval resolution. Calls to stream may run
// concurrently: they only read the client and logger and keep no shared state.
type statsAcquirer struct {
	client *client.Client
	logger *slog.Logger
	// failed remembers the last one-shot error per container so a container
	// that is gone is reported once, not on every tick. It is rebuilt from the
	// current call's IDs, so churn cannot grow it without bound.
	failed map[string]string
}

// newStatsAcquirer returns an acquirer backed by c.
func newStatsAcquirer(c *client.Client, logger *slog.Logger) *statsAcquirer {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &statsAcquirer{
		client: c,
		logger: logger,
		failed: make(map[string]string),
	}
}

// oneShot requests one ContainerStatsOneShot sample per container and returns
// the payloads that succeeded. It starts at most oneShotConcurrency workers,
// each handling a strided share of ids.
//
// A container that disappeared or failed is logged at warn once per distinct
// error and omitted; it never fails the whole tick. A canceled context stops
// outstanding requests and returns the samples that completed.
func (a *statsAcquirer) oneShot(ctx context.Context, ids []string) []rawStats {
	if len(ids) == 0 {
		return nil
	}

	type result struct {
		stats rawStats
		err   error
	}
	results := make([]result, len(ids))
	workers := min(oneShotConcurrency, len(ids))

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for i := start; i < len(ids); i += workers {
				if err := ctx.Err(); err != nil {
					results[i].err = err
					continue
				}
				results[i].stats, results[i].err = a.oneShotContainer(ctx, ids[i])
			}
		}(w)
	}
	wg.Wait()

	previous := a.failed
	failed := make(map[string]string)
	out := make([]rawStats, 0, len(ids))
	for i, r := range results {
		id := ids[i]
		if r.err != nil {
			if ctx.Err() == nil {
				message := r.err.Error()
				if previous[id] != message {
					a.logger.Warn("container stats unavailable", "container", id, "error", r.err)
				}
				failed[id] = message
			}
			continue
		}
		out = append(out, r.stats)
	}
	a.failed = failed
	return out
}

// oneShotContainer fetches and decodes one stats payload. The response body is
// always closed, including on a decode error.
func (a *statsAcquirer) oneShotContainer(ctx context.Context, id string) (rawStats, error) {
	reader, err := a.client.ContainerStatsOneShot(ctx, id)
	if err != nil {
		return rawStats{}, fmt.Errorf("stats for container %s: %w", id, err)
	}
	defer reader.Body.Close()

	var stats container.StatsResponse
	if err := json.NewDecoder(reader.Body).Decode(&stats); err != nil {
		return rawStats{}, fmt.Errorf("decode stats for container %s: %w", id, err)
	}
	return rawStats{containerID: id, stats: stats}, nil
}

// stream opens one streaming stats connection per container and fans the
// decoded payloads into a single channel. Calling stream starts it; canceling
// ctx stops every reader. The returned channel is owned by the acquirer and is
// closed exactly once, after every reader has returned, so callers must not
// close it.
//
// Streaming is deliberately expensive: it uses one goroutine and one HTTP
// connection per container, which is why it is opt-in. A stream that ends or
// fails closes only that reader; other streams continue. The caller owns the
// reconnect policy: it watches the channel close, backs off, and opens a new
// stream. Open and read failures are logged at debug here because the caller
// decides when to warn; a lagging consumer is still logged at warn once per
// connection.
//
// The channel holds one pending payload per container. A consumer that falls
// behind drops new payloads instead of growing a queue; the first drop per
// container is logged at warn. Dropped payloads are never replayed.
func (a *statsAcquirer) stream(ctx context.Context, ids []string) <-chan rawStats {
	out := make(chan rawStats, len(ids))

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			a.streamContainer(ctx, id, out)
		}(id)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// streamContainer reads one container's stats stream until the stream ends or
// ctx is canceled. It is the only place that decodes a streaming stats payload
// and the only place that applies the one-pending-payload drop policy.
func (a *statsAcquirer) streamContainer(ctx context.Context, id string, out chan<- rawStats) {
	reader, err := a.client.ContainerStats(ctx, id, true)
	if err != nil {
		if ctx.Err() == nil {
			a.logger.Debug("open container stats stream failed", "container", id, "error", err)
		}
		return
	}
	defer reader.Body.Close()

	dec := json.NewDecoder(reader.Body)
	dropped := false
	for {
		var stats container.StatsResponse
		if err := dec.Decode(&stats); err != nil {
			switch {
			case ctx.Err() != nil:
				// Cancellation tears down the HTTP body; this is a clean stop.
			case errors.Is(err, io.EOF):
				a.logger.Debug("container stats stream ended", "container", id)
			default:
				a.logger.Debug("read container stats stream failed", "container", id, "error", err)
			}
			return
		}

		select {
		case out <- rawStats{containerID: id, stats: stats}:
			continue
		case <-ctx.Done():
			return
		default:
		}
		if ctx.Err() != nil {
			return
		}
		if !dropped {
			dropped = true
			a.logger.Warn("container stats consumer is behind; dropping samples", "container", id)
		}
	}
}
