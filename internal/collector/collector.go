// Package collector schedules metric sources and assembles their samples into
// immutable chunks for the store.
package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/confuzeus/smob/internal/config"
	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/store"
)

// Source is one polled metric source, such as the system or process source.
//
// Collect returns the samples for one tick. The first call may return gauges
// only: rate samples need a previous snapshot. Samples carry series labels
// only; the collector adds host and scope.
type Source interface {
	// Scope is the scope label value and chunk path component.
	Scope() string
	// Interval is the collection cadence for this source.
	Interval() time.Duration
	// Collect returns the samples for the tick ending at now.
	Collect(ctx context.Context, now time.Time) ([]model.Sample, error)
}

// tickResult carries one source tick to the assembler goroutine.
type tickResult struct {
	scope   string
	samples []model.Sample
	err     error
}

// Collector polls sources and writes closed chunks. It owns its store: Run and
// RunOnce close it before returning, so callers must not close it separately.
type Collector struct {
	sources   []Source
	store     store.Store
	logger    *slog.Logger
	assembler *assembler
}

// New returns a collector for sources. cfg supplies the host label and chunk
// interval.
func New(cfg config.Config, sources []Source, st store.Store, logger *slog.Logger) *Collector {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Collector{
		sources:   sources,
		store:     st,
		logger:    logger,
		assembler: newAssembler(cfg.Host, cfg.Storage.ChunkInterval.Std(), st, logger),
	}
}

// Run polls every source until ctx is canceled or a source or store fails. On
// shutdown it flushes partial chunks and closes the store. A canceled ctx is a
// clean stop; a source or store error is returned to the caller.
func (c *Collector) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	// One in-flight tick per source can wait here while a chunk write is in
	// progress; further ticks block on the send, which is the desired
	// backpressure when the store is slow.
	results := make(chan tickResult, len(c.sources))

	var wg sync.WaitGroup
	for _, src := range c.sources {
		wg.Add(1)
		go func(src Source) {
			defer wg.Done()
			c.poll(runCtx, src, results)
		}(src)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	// Writes are detached from ctx so a SIGTERM still persists the buffered
	// ticks and the active chunks. A second signal exits without waiting.
	writeCtx := context.WithoutCancel(ctx)

	var runErr error
	for res := range results {
		if runErr != nil {
			continue
		}
		if res.err != nil {
			runErr = res.err
			cancel(runErr)
			continue
		}
		if err := c.assembler.add(writeCtx, res.scope, res.samples); err != nil {
			runErr = err
			cancel(runErr)
		}
	}

	flushErr := c.assembler.flushAll(writeCtx, time.Now())
	closeErr := c.store.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("close store: %w", closeErr)
	}
	return errors.Join(runErr, flushErr, closeErr)
}

// RunOnce collects one tick from every source, writes one partial chunk per
// scope, and closes the store. It backs collect --once; because every source
// sees only one tick, it writes gauges and no rate samples.
func (c *Collector) RunOnce(ctx context.Context) error {
	writeCtx := context.WithoutCancel(ctx)
	var errs []error

	for _, src := range c.sources {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		at := time.Now()
		samples, err := c.collectOnce(ctx, src, at)
		if err != nil {
			errs = append(errs, fmt.Errorf("collect %s: %w", src.Scope(), err))
			continue
		}
		if err := c.assembler.add(writeCtx, src.Scope(), samples); err != nil {
			errs = append(errs, err)
		}
	}

	if err := c.assembler.flushAll(writeCtx, time.Now()); err != nil {
		errs = append(errs, err)
	}
	if err := c.store.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close store: %w", err))
	}
	return errors.Join(errs...)
}

// poll collects one source until ctx is canceled. Ticks never overlap: the
// next wait starts after the previous Collect returns.
func (c *Collector) poll(ctx context.Context, src Source, out chan<- tickResult) {
	interval := src.Interval()
	for {
		if !sleep(ctx, jitter(interval)) {
			return
		}

		at := time.Now()
		samples, err := c.collectOnce(ctx, src, at)
		if err != nil && ctx.Err() != nil {
			// Shutdown interrupted the tick; it is not a source failure.
			return
		}

		res := tickResult{scope: src.Scope(), samples: samples}
		if err != nil {
			res.err = fmt.Errorf("collect %s: %w", src.Scope(), err)
		}

		select {
		case out <- res:
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// collectOnce runs one source collection bounded by the per-tick timeout.
func (c *Collector) collectOnce(ctx context.Context, src Source, at time.Time) ([]model.Sample, error) {
	tickCtx, cancel := context.WithTimeout(ctx, tickTimeout(src.Interval()))
	defer cancel()
	return src.Collect(tickCtx, at)
}

// sleep waits for d or until ctx is canceled, reporting whether the wait
// completed.
func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// jitter returns d adjusted by up to five percent, so independent sources and
// hosts do not sample in lockstep. Rate windows are measured, so the spread does
// not affect reported values.
func jitter(d time.Duration) time.Duration {
	delta := d / 20
	if delta <= 0 {
		return d
	}
	adjusted := d + time.Duration(rand.Int64N(int64(2*delta))) - delta
	if adjusted <= 0 {
		return d
	}
	return adjusted
}

// tickTimeout bounds one collection. A slow source cannot overlap its next
// tick, and a hung read fails the collector instead of stalling forever.
func tickTimeout(interval time.Duration) time.Duration {
	timeout := interval / 2
	if timeout < 100*time.Millisecond {
		timeout = 100 * time.Millisecond
	}
	if timeout > 30*time.Second {
		timeout = 30 * time.Second
	}
	return timeout
}
