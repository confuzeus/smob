package collector

import (
	"testing"
	"time"
)

func TestWindowFor(t *testing.T) {
	cases := []struct {
		name     string
		ts       time.Time
		interval time.Duration
		start    time.Time
		end      time.Time
	}{
		{
			name:     "inside a five minute window",
			ts:       time.Date(2026, 10, 23, 16, 3, 27, 500_000_000, time.UTC),
			interval: 5 * time.Minute,
			start:    time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			end:      time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
		},
		{
			name:     "exact window start",
			ts:       time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
			interval: 5 * time.Minute,
			start:    time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
			end:      time.Date(2026, 10, 23, 16, 10, 0, 0, time.UTC),
		},
		{
			name:     "one second window across midnight",
			ts:       time.Date(2026, 10, 24, 0, 0, 0, 400_000_000, time.UTC),
			interval: time.Second,
			start:    time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2026, 10, 24, 0, 0, 1, 0, time.UTC),
		},
		{
			name:     "non UTC input is normalized",
			ts:       time.Date(2026, 10, 23, 18, 3, 0, 0, time.FixedZone("CEST", 2*60*60)),
			interval: 5 * time.Minute,
			start:    time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC),
			end:      time.Date(2026, 10, 23, 16, 5, 0, 0, time.UTC),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start, end := windowFor(tc.ts, tc.interval)
			if start.Location() != time.UTC {
				t.Errorf("start location = %v, want UTC", start.Location())
			}
			if !start.Equal(tc.start) {
				t.Errorf("start = %s, want %s", start, tc.start)
			}
			if !end.Equal(tc.end) {
				t.Errorf("end = %s, want %s", end, tc.end)
			}
		})
	}
}
