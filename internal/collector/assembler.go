package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/store"
)

// pendingChunk is the chunk currently being assembled for one scope. Its
// samples are not sorted until the chunk is written.
type pendingChunk struct {
	start   time.Time
	end     time.Time
	samples []model.Sample
}

// assembler groups samples by scope and UTC-aligned window, then hands closed
// chunks to the store. It is not safe for concurrent use: the collector drives
// it from one goroutine, which keeps exactly one writer per store.
type assembler struct {
	host     string
	interval time.Duration
	store    store.Store
	logger   *slog.Logger
	active   map[string]*pendingChunk
}

func newAssembler(host string, interval time.Duration, st store.Store, logger *slog.Logger) *assembler {
	return &assembler{
		host:     host,
		interval: interval,
		store:    st,
		logger:   logger,
		active:   make(map[string]*pendingChunk),
	}
}

// add appends a batch of samples for scope. A batch normally shares one
// timestamp, but it is sorted first so a delayed or mixed batch still lands in
// the right windows and produces deterministic chunk contents.
func (a *assembler) add(ctx context.Context, scope string, samples []model.Sample) error {
	if len(samples) == 0 {
		return nil
	}

	batch := make([]model.Sample, len(samples))
	for i, sample := range samples {
		batch[i] = a.stamp(scope, sample)
	}
	sortSamples(batch)

	for _, sample := range batch {
		start, end := windowFor(sample.Timestamp, a.interval)

		cur := a.active[scope]
		switch {
		case cur == nil:
			a.active[scope] = &pendingChunk{start: start, end: end}
		case start.After(cur.start):
			if err := a.write(ctx, scope, cur, cur.end); err != nil {
				return err
			}
			a.active[scope] = &pendingChunk{start: start, end: end}
		case start.Before(cur.start):
			a.logger.Warn("wall clock moved backwards; closing the active chunk",
				"scope", scope, "chunk_start", cur.start, "sample_time", sample.Timestamp)
			if err := a.write(ctx, scope, cur, cur.end); err != nil {
				return err
			}
			a.active[scope] = &pendingChunk{start: start, end: end}
		}

		a.active[scope].samples = append(a.active[scope].samples, sample)
	}
	return nil
}

// flushAll writes every active chunk as a partial chunk ending just after the
// latest sample. It is used on shutdown and by collect --once.
func (a *assembler) flushAll(ctx context.Context, at time.Time) error {
	var errs []error
	for _, scope := range slices.Sorted(maps.Keys(a.active)) {
		cur := a.active[scope]
		if err := a.write(ctx, scope, cur, partialEnd(cur, at)); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// write closes cur at end and hands it to the store. The chunk is removed from
// the active set before the write so a failed write is reported, not retried
// forever.
func (a *assembler) write(ctx context.Context, scope string, cur *pendingChunk, end time.Time) error {
	delete(a.active, scope)
	if len(cur.samples) == 0 {
		return nil
	}
	if !end.After(cur.start) {
		end = cur.start.Add(time.Second)
	}

	sortSamples(cur.samples)
	chunk := store.Chunk{
		Host:    a.host,
		Scope:   scope,
		Start:   cur.start,
		End:     end,
		Samples: cur.samples,
	}
	if err := a.store.Write(ctx, chunk); err != nil {
		return fmt.Errorf("write %s chunk starting %s: %w", scope, cur.start.Format(time.RFC3339), err)
	}
	return nil
}

// partialEnd returns a second-aligned end that is after every sample in cur and
// after the flush time. A partial chunk therefore stays a valid half-open
// window without spilling into the next chunk's filename.
func partialEnd(cur *pendingChunk, at time.Time) time.Time {
	end := at.UTC().Truncate(time.Second).Add(time.Second)
	for _, sample := range cur.samples {
		next := sample.Timestamp.UTC().Truncate(time.Second).Add(time.Second)
		if next.After(end) {
			end = next
		}
	}
	if !end.After(cur.start) {
		end = cur.start.Add(time.Second)
	}
	return end
}

// stamp copies the sample labels and sets host and scope, so sources cannot
// forget or contradict the collector's identity.
func (a *assembler) stamp(scope string, sample model.Sample) model.Sample {
	labels := make(map[string]string, len(sample.Labels)+2)
	maps.Copy(labels, sample.Labels)
	labels["host"] = a.host
	labels["scope"] = scope
	sample.Labels = labels
	return sample
}

// sortSamples orders a chunk by timestamp, then metric, then canonical label
// order. Identical inputs therefore produce identical chunk contents.
func sortSamples(samples []model.Sample) {
	sort.SliceStable(samples, func(i, j int) bool {
		a, b := samples[i], samples[j]
		if !a.Timestamp.Equal(b.Timestamp) {
			return a.Timestamp.Before(b.Timestamp)
		}
		if a.Metric != b.Metric {
			return a.Metric < b.Metric
		}
		return labelsLess(a.Labels, b.Labels)
	})
}

// labelsLess compares two label sets as sorted key/value pairs.
func labelsLess(a, b map[string]string) bool {
	aKeys := slices.Sorted(maps.Keys(a))
	bKeys := slices.Sorted(maps.Keys(b))

	for i := 0; i < len(aKeys) && i < len(bKeys); i++ {
		if aKeys[i] != bKeys[i] {
			return aKeys[i] < bKeys[i]
		}
		if a[aKeys[i]] != b[bKeys[i]] {
			return a[aKeys[i]] < b[bKeys[i]]
		}
	}
	return len(aKeys) < len(bKeys)
}
