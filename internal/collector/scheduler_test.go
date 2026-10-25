package collector

// The scheduler contract lives in poll and tickTimeout: a jittered wait after
// each Collect returns, one tick in flight per source, a per-tick deadline, and
// a clean stop. These tests drive the real Run loop over a real disk store; the
// clock, the filesystem, and the store are not faked. The sources are scripted
// because the host's /proc cannot be made slow, hung, or failing on demand.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/confuzeus/smob/internal/config"
	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/store"
	"github.com/confuzeus/smob/internal/store/disk"
)

// sourceStats is a snapshot of what one source observed while Run drove it.
type sourceStats struct {
	calls     int
	maxActive int
	starts    []time.Time
	deadlines []time.Duration
}

// funcSource is a real Source whose tick behavior is scripted. The mutex covers
// the counters because the test reads them while poll writes them.
type funcSource struct {
	scope    string
	interval time.Duration
	collect  func(ctx context.Context, now time.Time, call int) ([]model.Sample, error)

	mu     sync.Mutex
	stats  sourceStats
	active int
}

func (s *funcSource) Scope() string { return s.scope }

func (s *funcSource) Interval() time.Duration { return s.interval }

func (s *funcSource) Collect(ctx context.Context, now time.Time) ([]model.Sample, error) {
	s.mu.Lock()
	s.stats.calls++
	call := s.stats.calls
	s.stats.starts = append(s.stats.starts, time.Now())
	if deadline, ok := ctx.Deadline(); ok {
		s.stats.deadlines = append(s.stats.deadlines, time.Until(deadline))
	} else {
		s.stats.deadlines = append(s.stats.deadlines, 0)
	}
	s.active++
	if s.active > s.stats.maxActive {
		s.stats.maxActive = s.active
	}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.active--
		s.mu.Unlock()
	}()

	if s.collect == nil {
		return tickSamples(now, call), nil
	}
	return s.collect(ctx, now, call)
}

func (s *funcSource) snapshot() sourceStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := s.stats
	stats.starts = slices.Clone(s.stats.starts)
	stats.deadlines = slices.Clone(s.stats.deadlines)
	return stats
}

func tickSamples(now time.Time, call int) []model.Sample {
	return []model.Sample{{
		Timestamp: now.UTC(),
		Metric:    "test.value",
		Value:     float64(call),
	}}
}

// waitGoroutines polls the live goroutine count until cond holds. Run's closer
// goroutine can outlive Run returning by a scheduling step, so cleanup is
// observed rather than slept through.
func waitGoroutines(t *testing.T, what string, cond func(int) bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if cond(runtime.NumGoroutine()) {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			t.Fatalf("%s: goroutines = %d; stacks:\n%s", what, runtime.NumGoroutine(), buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(time.Millisecond)
	}
}

// TestJitterBounds pins the documented five percent band. jitter is the wait
// before each tick, so its band is the whole claim the code makes about cadence:
// sleep(jitter(interval)) after every Collect, never a fixed period.
func TestJitterBounds(t *testing.T) {
	cases := []struct {
		name     string
		interval time.Duration
		fixed    bool // delta is zero, so jitter returns the interval unchanged
	}{
		{name: "below the resolution of a five percent delta", interval: 10 * time.Nanosecond, fixed: true},
		{name: "millisecond interval", interval: time.Millisecond},
		{name: "one second interval", interval: time.Second},
		{name: "five minute interval", interval: 5 * time.Minute},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			delta := tc.interval / 20
			lo, hi := tc.interval-delta, tc.interval+delta
			seen := make(map[time.Duration]struct{})
			for range 2000 {
				got := jitter(tc.interval)
				if tc.fixed {
					if got != tc.interval {
						t.Fatalf("jitter(%s) = %s, want the interval unchanged", tc.interval, got)
					}
					continue
				}
				// delta is zero for tiny intervals; they are covered above.
				if got < lo || got >= hi {
					t.Fatalf("jitter(%s) = %s, want within [%s, %s)", tc.interval, got, lo, hi)
				}
				seen[got] = struct{}{}
			}
			if !tc.fixed && len(seen) < 2 {
				t.Fatalf("jitter(%s) returned one value over 2000 draws, want a spread", tc.interval)
			}
		})
	}
}

// TestTickCadenceUsesJitteredSleepAfterCollect documents what poll actually
// does: it sleeps jitter(interval) *after* the previous Collect returns, then
// collects. Wake-ups are therefore never a fixed period and never early; the
// gap between tick starts is the collect duration plus a jittered wait.
func TestTickCadenceUsesJitteredSleepAfterCollect(t *testing.T) {
	const interval = 30 * time.Millisecond
	const ticks = 7

	enough := make(chan struct{})
	var once sync.Once
	src := &funcSource{
		scope:    "system",
		interval: interval,
		collect: func(_ context.Context, now time.Time, call int) ([]model.Sample, error) {
			if call >= ticks {
				once.Do(func() { close(enough) })
			}
			return tickSamples(now, call), nil
		},
	}
	c, _ := newTestCollector(t, []Source{src}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-enough:
	case <-time.After(5 * time.Second):
		t.Fatalf("source produced fewer than %d ticks", ticks)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned %v, want a clean stop", err)
	}

	stats := src.snapshot()
	if len(stats.starts) < ticks {
		t.Fatalf("got %d tick starts, want %d", len(stats.starts), ticks)
	}

	gaps := make([]time.Duration, 0, ticks-1)
	for i := 1; i < ticks; i++ {
		gaps = append(gaps, stats.starts[i].Sub(stats.starts[i-1]))
	}
	// Timers do not fire before their deadline; the two millisecond allowance
	// covers timestamping the tick while the timer has already fired.
	earliest := interval - interval/20 - 2*time.Millisecond
	for i, gap := range gaps {
		if gap < earliest {
			t.Errorf("gap %d between tick starts = %s, want at least %s (0.95 x interval)", i, gap, earliest)
		}
	}
	// The median is used so one delayed wake-up under load does not fail the
	// test; the allowance covers scheduler delay only, and stays well below a
	// cadence that skips a tick.
	slices.Sort(gaps)
	median := gaps[len(gaps)/2]
	latest := interval + interval/20 + 15*time.Millisecond
	if median > latest {
		t.Errorf("median gap between tick starts = %s, want at most %s (1.05 x interval plus scheduling delay)", median, latest)
	}
}

// TestTicksDoNotOverlap makes a Collect run six intervals long and asserts the
// source is never entered twice at once. The next wait begins after Collect
// returns, so starts are at least the collect duration apart even though the
// wait is shorter than a tick.
func TestTicksDoNotOverlap(t *testing.T) {
	const interval = 5 * time.Millisecond
	const collectFor = 30 * time.Millisecond

	src := &funcSource{
		scope:    "system",
		interval: interval,
		collect: func(ctx context.Context, now time.Time, call int) ([]model.Sample, error) {
			timer := time.NewTimer(collectFor)
			defer timer.Stop()
			select {
			case <-timer.C:
				return tickSamples(now, call), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	c, _ := newTestCollector(t, []Source{src}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatalf("Run returned %v, want a clean stop", err)
	}

	stats := src.snapshot()
	if stats.calls < 2 {
		t.Fatalf("source called %d times, want at least two ticks", stats.calls)
	}
	if stats.maxActive != 1 {
		t.Fatalf("source entered %d times concurrently, want 1", stats.maxActive)
	}
	for i := 1; i < len(stats.starts); i++ {
		if gap := stats.starts[i].Sub(stats.starts[i-1]); gap < collectFor {
			t.Errorf("tick %d started %s after the previous one, shorter than the %s Collect", i, gap, collectFor)
		}
	}
}

// TestTickTimeout pins the per-tick deadline: half the interval, clamped to the
// documented 100ms floor and 30s ceiling.
func TestTickTimeout(t *testing.T) {
	cases := []struct {
		name     string
		interval time.Duration
		want     time.Duration
	}{
		{name: "zero interval takes the floor", interval: 0, want: 100 * time.Millisecond},
		{name: "negative interval takes the floor", interval: -time.Second, want: 100 * time.Millisecond},
		{name: "millisecond interval takes the floor", interval: 5 * time.Millisecond, want: 100 * time.Millisecond},
		{name: "interval below twice the floor takes the floor", interval: 199 * time.Millisecond, want: 100 * time.Millisecond},
		{name: "exactly twice the floor", interval: 200 * time.Millisecond, want: 100 * time.Millisecond},
		{name: "one second interval halves", interval: time.Second, want: 500 * time.Millisecond},
		{name: "just below the ceiling", interval: 59 * time.Second, want: 29*time.Second + 500*time.Millisecond},
		{name: "exactly the ceiling", interval: 60 * time.Second, want: 30 * time.Second},
		{name: "above the ceiling is clamped", interval: 61 * time.Second, want: 30 * time.Second},
		{name: "hourly interval is clamped", interval: time.Hour, want: 30 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tickTimeout(tc.interval); got != tc.want {
				t.Errorf("tickTimeout(%s) = %s, want %s", tc.interval, got, tc.want)
			}
		})
	}
}

// TestSlowSourceTimesOutAndRunContinues locks in C1's behavior: a tick that
// blocks past its deadline is reported as a source error and the next tick
// still runs. tickTimeout derives the deadline from the interval, and the source
// sees that deadline on its context.
func TestSlowSourceTimesOutAndRunContinues(t *testing.T) {
	const interval = 10 * time.Millisecond

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	slow := &funcSource{
		scope:    "slow",
		interval: interval,
		collect: func(ctx context.Context, now time.Time, call int) ([]model.Sample, error) {
			if call > 1 {
				return tickSamples(now, call), nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	fast := &funcSource{scope: "system", interval: interval}
	c, st := newTestCollector(t, []Source{slow, fast}, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatalf("Run returned %v after a tick timeout, want a clean stop", err)
	}

	stats := slow.snapshot()
	if stats.calls < 2 {
		t.Fatalf("slow source called %d times, want a retry after its timed-out tick", stats.calls)
	}
	// The tick context carries tickTimeout(interval). context.WithTimeout also
	// clamps it to the parent's deadline, so ticks late in a bounded run see
	// less than the full budget; the first tick, the one that times out, must
	// carry the derived value.
	want := tickTimeout(interval)
	for i, remaining := range stats.deadlines {
		if remaining <= 0 {
			t.Fatalf("tick %d ran without a deadline, want %s from the interval", i, want)
		}
		if remaining > want+time.Millisecond {
			t.Fatalf("tick %d deadline was %s away, more than the %s derived from %s", i, remaining, want, interval)
		}
		if i == 0 && remaining < want-10*time.Millisecond {
			t.Fatalf("first tick deadline was %s away, want about %s (half of %s, floored at 100ms)", remaining, want, interval)
		}
	}

	// The successful tick after the timeout reaches the store: collection
	// continued instead of dropping the source.
	var continued bool
	if err := st.Query(context.Background(), model.Query{}, func(s model.Sample) error {
		if s.Labels["scope"] == "slow" && s.Value > 1 {
			continued = true
		}
		return nil
	}); err != nil {
		t.Fatalf("query: %v", err)
	}
	if !continued {
		t.Fatal("no sample from the slow source after its timed-out tick")
	}

	var timedOut bool
	scanner := bufio.NewScanner(&logs)
	for scanner.Scan() {
		var record struct {
			Msg   string `json:"msg"`
			Scope string `json:"scope"`
			Error string `json:"error"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("decode log line: %v", err)
		}
		if record.Scope == "slow" && record.Msg == "source collection failed; continuing" &&
			strings.Contains(record.Error, context.DeadlineExceeded.Error()) {
			timedOut = true
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read log: %v", err)
	}
	if !timedOut {
		t.Error("tick timeout was not logged as a source failure for scope slow")
	}
}

// TestRunStopsWhileSleeping cancels between ticks. With an hourly interval the
// source must never be called, and Run must return without leaving goroutines.
func TestRunStopsWhileSleeping(t *testing.T) {
	baseline := runtime.NumGoroutine()
	src := &funcSource{scope: "system", interval: time.Hour}
	c, _ := newTestCollector(t, []Source{src}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// One poll goroutine plus the goroutine that closes results.
	waitGoroutines(t, "poll loop did not start", func(n int) bool { return n >= baseline+2 })
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after cancellation, want a clean stop", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation while sleeping")
	}
	if got := src.snapshot().calls; got != 0 {
		t.Errorf("source called %d times after cancellation while sleeping, want 0", got)
	}
	waitGoroutines(t, "goroutines leaked after cancellation", func(n int) bool { return n <= baseline })
}

// TestRunStopsWhileCollecting cancels inside a tick. The interrupted tick is not
// reported as a source failure, and Run returns without leaving goroutines.
func TestRunStopsWhileCollecting(t *testing.T) {
	baseline := runtime.NumGoroutine()
	entered := make(chan struct{}, 1)
	src := &funcSource{
		scope:    "system",
		interval: 5 * time.Millisecond,
		collect: func(ctx context.Context, _ time.Time, _ int) ([]model.Sample, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	c, _ := newTestCollector(t, []Source{src}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("source never entered Collect")
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after cancelling a tick, want a clean stop", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation while collecting")
	}
	waitGoroutines(t, "goroutines leaked after cancellation", func(n int) bool { return n <= baseline })
}

// TestStoreErrorStopsRun keeps a store failure fatal, unlike a source failure.
// The data directory is a regular file, so the real disk store fails its write
// with a filesystem error instead of a fake store.
func TestStoreErrorStopsRun(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(root, nil, 0o600); err != nil {
		t.Fatalf("arrange data directory: %v", err)
	}
	st, err := disk.New(root, "", nil)
	if err != nil {
		t.Fatalf("new disk store: %v", err)
	}

	// Each tick lands in a later chunk window, so the second sample forces a
	// chunk write on the second tick.
	src := &funcSource{
		scope:    "system",
		interval: 5 * time.Millisecond,
		collect: func(_ context.Context, now time.Time, call int) ([]model.Sample, error) {
			samples := tickSamples(now.Add(time.Duration(call-1)*time.Hour), call)
			return samples, nil
		},
	}
	cfg := config.Config{
		Host:    "test-host",
		Storage: config.Storage{ChunkInterval: config.Duration(time.Second)},
	}
	c := New(cfg, []Source{src}, st, nil)

	done := make(chan error, 1)
	go func() { done <- c.Run(context.Background()) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run returned nil after the store rejected a chunk")
		}
		if !errors.Is(err, store.ErrPersistent) {
			t.Fatalf("Run error = %v, want it to wrap store.ErrPersistent", err)
		}
		if !strings.Contains(err.Error(), "write system chunk") {
			t.Errorf("Run error = %v, want it to name the failed scope", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after the store rejected a chunk")
	}
	// Run waits for every poll goroutine before returning, so a small call count
	// here means collection stopped instead of retrying through the failure.
	if got := src.snapshot().calls; got > 3 {
		t.Errorf("source called %d times after a fatal store error, want collection stopped", got)
	}
}
