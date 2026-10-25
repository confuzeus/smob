package docker

import (
	"testing"
	"time"
)

func TestStreamBackoff(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 500 * time.Millisecond},
		{2, time.Second},
		{3, 2 * time.Second},
		{4, 4 * time.Second},
		{5, 8 * time.Second},
		{6, 16 * time.Second},
		{7, 30 * time.Second},
		{8, 30 * time.Second},
		{100, 30 * time.Second},
	}
	for _, tc := range tests {
		if got := streamBackoff(tc.attempt); got != tc.want {
			t.Errorf("streamBackoff(%d) = %s, want %s", tc.attempt, got, tc.want)
		}
	}
}

func TestStreamIdleTimeout(t *testing.T) {
	tests := []struct {
		interval time.Duration
		want     time.Duration
	}{
		{0, 30 * time.Second},
		{time.Second, 30 * time.Second},
		{5 * time.Second, 30 * time.Second},
		{10 * time.Second, 30 * time.Second},
		{20 * time.Second, 60 * time.Second},
		{time.Minute, 3 * time.Minute},
	}
	for _, tc := range tests {
		if got := streamIdleTimeout(tc.interval); got != tc.want {
			t.Errorf("streamIdleTimeout(%s) = %s, want %s", tc.interval, got, tc.want)
		}
	}
}

func TestStreamJitter(t *testing.T) {
	tests := []struct {
		d    time.Duration
		u    float64
		want time.Duration
	}{
		{time.Second, 0, 500 * time.Millisecond},
		{time.Second, 0.5, 750 * time.Millisecond},
		{time.Second, 0.25, 625 * time.Millisecond},
		{30 * time.Second, 0, 15 * time.Second},
		{500 * time.Millisecond, 0.5, 375 * time.Millisecond},
	}
	for _, tc := range tests {
		if got := streamJitter(tc.d, tc.u); got != tc.want {
			t.Errorf("streamJitter(%s, %v) = %s, want %s", tc.d, tc.u, got, tc.want)
		}
	}
}
