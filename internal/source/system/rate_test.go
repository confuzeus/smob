package system

import (
	"math"
	"testing"
	"time"
)

func TestCPUPercent(t *testing.T) {
	cases := []struct {
		name   string
		prev   cpuTimes
		cur    cpuTimes
		want   float64
		wantOK bool
	}{
		{
			name:   "half busy",
			prev:   cpuTimes{user: 100, idle: 300},
			cur:    cpuTimes{user: 150, idle: 350},
			want:   50,
			wantOK: true,
		},
		{
			name:   "fully busy",
			prev:   cpuTimes{user: 100},
			cur:    cpuTimes{user: 200},
			want:   100,
			wantOK: true,
		},
		{
			name:   "idle only",
			prev:   cpuTimes{idle: 100},
			cur:    cpuTimes{idle: 200},
			want:   0,
			wantOK: true,
		},
		{
			name:   "iowait counts as idle",
			prev:   cpuTimes{idle: 100},
			cur:    cpuTimes{idle: 150, iowait: 50},
			want:   0,
			wantOK: true,
		},
		{
			name:   "guest time is not double counted",
			prev:   cpuTimes{user: 100, guest: 40},
			cur:    cpuTimes{user: 200, guest: 60},
			want:   100,
			wantOK: true,
		},
		{
			name:   "counter reset drops the tick",
			prev:   cpuTimes{user: 100, idle: 100},
			cur:    cpuTimes{user: 50, idle: 150},
			wantOK: false,
		},
		{
			name:   "no advance drops the tick",
			prev:   cpuTimes{user: 100, idle: 100},
			cur:    cpuTimes{user: 100, idle: 100},
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := cpuPercent(tc.prev, tc.cur)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (value %v)", ok, tc.wantOK, got)
			}
			if !ok {
				return
			}
			if math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("percent = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBytesPerSecond(t *testing.T) {
	cases := []struct {
		name    string
		prev    uint64
		cur     uint64
		elapsed time.Duration
		want    float64
		wantOK  bool
	}{
		{
			name:    "two seconds",
			prev:    1000,
			cur:     3000,
			elapsed: 2 * time.Second,
			want:    1000,
			wantOK:  true,
		},
		{
			name:    "no traffic",
			prev:    1000,
			cur:     1000,
			elapsed: time.Second,
			want:    0,
			wantOK:  true,
		},
		{
			name:    "counter reset drops the tick",
			prev:    500,
			cur:     100,
			elapsed: time.Second,
			wantOK:  false,
		},
		{
			name:    "zero window drops the tick",
			prev:    100,
			cur:     200,
			elapsed: 0,
			wantOK:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := bytesPerSecond(tc.prev, tc.cur, tc.elapsed)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (value %v)", ok, tc.wantOK, got)
			}
			if !ok {
				return
			}
			if math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("rate = %v, want %v", got, tc.want)
			}
		})
	}
}
