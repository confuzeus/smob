package process

import (
	"context"
	"math"
	"testing"
	"time"
)

// TestAdvanceRates covers the (pid, starttime) rate-state transition: which
// ticks emit a CPU rate, what the next baseline is, and which processes are
// reported as reset or pruned. PID reuse is the dangerous case: the sequence
// number in starttime must make the lookup miss so one process's counters are
// never subtracted from another's.
func TestAdvanceRates(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	prevAt := now.Add(-time.Second)

	tests := []struct {
		name   string
		prev   map[pidKey]procState
		cands  []candidate
		rates  []wantRate
		state  map[pidKey]statSnapshot
		resets []rateReset
	}{
		{
			name: "same pid and starttime emits a rate",
			prev: map[pidKey]procState{
				{pid: 100, starttime: 5555}: {stat: snapshot(1000, 0, 5555), at: prevAt},
			},
			cands: []candidate{
				{pid: 100, comm: "busy", stat: snapshot(1100, 0, 5555)},
			},
			rates: []wantRate{{cpu: 100, known: true, window: time.Second}},
			state: map[pidKey]statSnapshot{
				{pid: 100, starttime: 5555}: snapshot(1100, 0, 5555),
			},
		},
		{
			// A reused PID has a new starttime, so it must miss the lookup and
			// skip its first delta instead of inheriting the old counters.
			name: "reused pid skips the first delta and replaces the baseline",
			prev: map[pidKey]procState{
				{pid: 100, starttime: 5000}: {stat: snapshot(1000, 500, 5000), at: prevAt},
			},
			cands: []candidate{
				{pid: 100, comm: "new", stat: snapshot(2000, 1000, 6000)},
			},
			rates: []wantRate{{known: false}},
			state: map[pidKey]statSnapshot{
				{pid: 100, starttime: 6000}: snapshot(2000, 1000, 6000),
			},
		},
		{
			name: "new pid skips the first delta",
			prev: nil,
			cands: []candidate{
				{pid: 200, comm: "fresh", stat: snapshot(5, 5, 7000)},
			},
			rates: []wantRate{{known: false}},
			state: map[pidKey]statSnapshot{
				{pid: 200, starttime: 7000}: snapshot(5, 5, 7000),
			},
		},
		{
			// The next map is rebuilt from the candidates, so an exited process
			// cannot leave a stale baseline behind.
			name: "disappeared process is pruned",
			prev: map[pidKey]procState{
				{pid: 100, starttime: 5000}: {stat: snapshot(10, 0, 5000), at: prevAt},
				{pid: 200, starttime: 6000}: {stat: snapshot(0, 0, 6000), at: prevAt},
			},
			cands: []candidate{
				{pid: 100, comm: "busy", stat: snapshot(110, 0, 5000)},
			},
			rates: []wantRate{{cpu: 100, known: true, window: time.Second}},
			state: map[pidKey]statSnapshot{
				{pid: 100, starttime: 5000}: snapshot(110, 0, 5000),
			},
		},
		{
			name: "utime regression skips the rate and advances the baseline",
			prev: map[pidKey]procState{
				{pid: 100, starttime: 5555}: {stat: snapshot(1000, 100, 5555), at: prevAt},
			},
			cands: []candidate{
				{pid: 100, comm: "reset", stat: snapshot(900, 100, 5555)},
			},
			rates: []wantRate{{known: false}},
			state: map[pidKey]statSnapshot{
				{pid: 100, starttime: 5555}: snapshot(900, 100, 5555),
			},
			resets: []rateReset{
				{key: pidKey{pid: 100, starttime: 5555}, comm: "reset"},
			},
		},
		{
			name: "stime regression skips the rate and advances the baseline",
			prev: map[pidKey]procState{
				{pid: 100, starttime: 5555}: {stat: snapshot(100, 1000, 5555), at: prevAt},
			},
			cands: []candidate{
				{pid: 100, comm: "reset", stat: snapshot(100, 900, 5555)},
			},
			rates: []wantRate{{known: false}},
			state: map[pidKey]statSnapshot{
				{pid: 100, starttime: 5555}: snapshot(100, 900, 5555),
			},
			resets: []rateReset{
				{key: pidKey{pid: 100, starttime: 5555}, comm: "reset"},
			},
		},
		{
			// A too-short window is not a reset: the baseline still advances,
			// but no warning is reported.
			name: "window under one millisecond keeps the baseline without a reset",
			prev: map[pidKey]procState{
				{pid: 100, starttime: 5555}: {stat: snapshot(1000, 0, 5555), at: now},
			},
			cands: []candidate{
				{pid: 100, comm: "busy", stat: snapshot(1100, 0, 5555)},
			},
			rates: []wantRate{{known: false}},
			state: map[pidKey]statSnapshot{
				{pid: 100, starttime: 5555}: snapshot(1100, 0, 5555),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.cands) != len(tt.rates) {
				t.Fatalf("test table: %d candidates, %d rate expectations", len(tt.cands), len(tt.rates))
			}

			state, resets := advanceRates(tt.prev, tt.cands, now)

			for i, cand := range tt.cands {
				want := tt.rates[i]
				if cand.cpuKnown != want.known {
					t.Fatalf("candidate %d cpuKnown = %v, want %v (cpu %v)", i, cand.cpuKnown, want.known, cand.cpu)
				}
				if !want.known {
					continue
				}
				if math.Abs(cand.cpu-want.cpu) > 1e-9 {
					t.Errorf("candidate %d cpu = %v, want %v", i, cand.cpu, want.cpu)
				}
				if cand.window != want.window {
					t.Errorf("candidate %d window = %s, want %s", i, cand.window, want.window)
				}
			}

			if len(state) != len(tt.state) {
				t.Fatalf("state has %d entries %v, want %d %v", len(state), state, len(tt.state), tt.state)
			}
			for key, want := range tt.state {
				got, ok := state[key]
				if !ok {
					t.Fatalf("state is missing %+v; got %v", key, state)
				}
				if got.stat != want {
					t.Errorf("state[%+v].stat = %+v, want %+v", key, got.stat, want)
				}
				if !got.at.Equal(now) {
					t.Errorf("state[%+v].at = %s, want %s", key, got.at, now)
				}
			}

			if len(resets) != len(tt.resets) {
				t.Fatalf("resets = %+v, want %+v", resets, tt.resets)
			}
			for i := range tt.resets {
				if resets[i] != tt.resets[i] {
					t.Errorf("reset %d = %+v, want %+v", i, resets[i], tt.resets[i])
				}
			}
		})
	}
}

// wantRate is the rate expectation for one candidate after advanceRates.
type wantRate struct {
	cpu    float64
	known  bool
	window time.Duration
}

// TestCollectFirstTickEmitsRSS covers the gauge side of the transition: a
// process with no previous snapshot has no CPU rate, but its RSS gauge is still
// emitted. A regression that gated the whole sample on the rate would silently
// drop memory for every process on its first tick.
func TestCollectFirstTickEmitsRSS(t *testing.T) {
	root := t.TempDir()
	writeProcSelf(t, root)
	writeProcPID(t, root, 100, statLine("100", "gauge", statTail), testStatus)

	src, err := New(Options{
		ProcPath:       root,
		Interval:       time.Second,
		RescanInterval: time.Hour,
	}, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	now := time.Now()
	samples, err := src.Collect(context.Background(), now)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("samples = %+v, want only the RSS gauge on the first tick", samples)
	}

	got := samples[0]
	if got.Metric != metricMemoryRSSBytes {
		t.Fatalf("metric = %q, want %q", got.Metric, metricMemoryRSSBytes)
	}
	if got.Value != 1024*1024 {
		t.Errorf("value = %v, want %d bytes", got.Value, 1024*1024)
	}
	if got.Labels["pid"] != "100" || got.Labels["comm"] != "gauge" {
		t.Errorf("labels = %v, want pid=100 comm=gauge", got.Labels)
	}
	if !got.Timestamp.Equal(now.UTC()) {
		t.Errorf("timestamp = %s, want %s", got.Timestamp, now.UTC())
	}
}
