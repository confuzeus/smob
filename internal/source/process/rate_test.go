package process

import (
	"math"
	"testing"
	"time"
)

// snapshot returns a stat snapshot with the given counters and starttime. Tests
// that are not about PID reuse pass the same starttime to both snapshots.
func snapshot(utime, stime, starttime uint64) statSnapshot {
	return statSnapshot{utime: utime, stime: stime, starttime: starttime}
}

func TestCPUPercent(t *testing.T) {
	tests := []struct {
		name    string
		prev    statSnapshot
		cur     statSnapshot
		elapsed time.Duration
		want    float64
		ok      bool
	}{
		{
			name:    "one core fully busy",
			prev:    snapshot(1000, 0, 100),
			cur:     snapshot(1100, 0, 100),
			elapsed: time.Second,
			want:    100,
			ok:      true,
		},
		{
			name:    "two cores fully busy",
			prev:    snapshot(0, 0, 100),
			cur:     snapshot(200, 0, 100),
			elapsed: time.Second,
			want:    200,
			ok:      true,
		},
		{
			name:    "half a core over half a second",
			prev:    snapshot(500, 0, 100),
			cur:     snapshot(525, 0, 100),
			elapsed: 500 * time.Millisecond,
			want:    50,
			ok:      true,
		},
		{
			name:    "user and system time both count",
			prev:    snapshot(100, 50, 100),
			cur:     snapshot(150, 100, 100),
			elapsed: time.Second,
			want:    100,
			ok:      true,
		},
		{
			name:    "idle process is a valid zero",
			prev:    snapshot(900, 400, 100),
			cur:     snapshot(900, 400, 100),
			elapsed: time.Second,
			want:    0,
			ok:      true,
		},
		{
			name:    "utime decreased",
			prev:    snapshot(1000, 0, 100),
			cur:     snapshot(900, 0, 100),
			elapsed: time.Second,
			ok:      false,
		},
		{
			name:    "stime decreased",
			prev:    snapshot(0, 1000, 100),
			cur:     snapshot(0, 900, 100),
			elapsed: time.Second,
			ok:      false,
		},
		{
			name:    "pid reused",
			prev:    snapshot(1000, 500, 100),
			cur:     snapshot(2000, 1000, 200),
			elapsed: time.Second,
			ok:      false,
		},
		{
			name:    "window under one millisecond",
			prev:    snapshot(1000, 0, 100),
			cur:     snapshot(1100, 0, 100),
			elapsed: 999 * time.Microsecond,
			ok:      false,
		},
		{
			name:    "zero window",
			prev:    snapshot(1000, 0, 100),
			cur:     snapshot(1100, 0, 100),
			elapsed: 0,
			ok:      false,
		},
		{
			name:    "negative window",
			prev:    snapshot(1000, 0, 100),
			cur:     snapshot(1100, 0, 100),
			elapsed: -time.Second,
			ok:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := cpuPercent(tt.prev, tt.cur, tt.elapsed)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v (value %v)", ok, tt.ok, got)
			}
			if !ok {
				return
			}
			if math.Abs(got-tt.want) > 1e-9 {
				t.Fatalf("percent = %v, want %v", got, tt.want)
			}
		})
	}
}
