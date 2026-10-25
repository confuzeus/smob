package model

import (
	"testing"
	"time"
)

func TestLessSample(t *testing.T) {
	base := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)
	labels := func(kv ...string) map[string]string {
		m := make(map[string]string, len(kv)/2)
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return m
	}

	cases := []struct {
		name string
		a    Sample
		b    Sample
		want bool
	}{
		{
			name: "earlier timestamp sorts first",
			a:    Sample{Timestamp: base, Metric: "m"},
			b:    Sample{Timestamp: base.Add(time.Nanosecond), Metric: "m"},
			want: true,
		},
		{
			name: "later timestamp sorts last",
			a:    Sample{Timestamp: base.Add(time.Nanosecond), Metric: "m"},
			b:    Sample{Timestamp: base, Metric: "m"},
			want: false,
		},
		{
			name: "same instant in another zone is not less",
			a:    Sample{Timestamp: base.In(time.FixedZone("UTC+2", 2*60*60)), Metric: "m"},
			b:    Sample{Timestamp: base, Metric: "m"},
			want: false,
		},
		{
			name: "metric breaks a timestamp tie",
			a:    Sample{Timestamp: base, Metric: "system.cpu.percent"},
			b:    Sample{Timestamp: base, Metric: "system.memory.used_bytes"},
			want: true,
		},
		{
			name: "metric order reverses",
			a:    Sample{Timestamp: base, Metric: "system.memory.used_bytes"},
			b:    Sample{Timestamp: base, Metric: "system.cpu.percent"},
			want: false,
		},
		{
			name: "label key decides when metrics tie",
			a:    Sample{Timestamp: base, Metric: "m", Labels: labels("cpu", "0")},
			b:    Sample{Timestamp: base, Metric: "m", Labels: labels("interface", "eth0")},
			want: true,
		},
		{
			name: "label key prefix sorts before its extension",
			a:    Sample{Timestamp: base, Metric: "m", Labels: labels("cpu", "0")},
			b:    Sample{Timestamp: base, Metric: "m", Labels: labels("cpux", "0")},
			want: true,
		},
		{
			name: "label key extension does not sort first",
			a:    Sample{Timestamp: base, Metric: "m", Labels: labels("cpux", "0")},
			b:    Sample{Timestamp: base, Metric: "m", Labels: labels("cpu", "0")},
			want: false,
		},
		{
			name: "shared key prefix falls through to the next key",
			a:    Sample{Timestamp: base, Metric: "m", Labels: labels("cpu", "0", "pid", "1")},
			b:    Sample{Timestamp: base, Metric: "m", Labels: labels("cpux", "0")},
			want: true,
		},
		{
			name: "label value breaks a key tie lexicographically",
			a:    Sample{Timestamp: base, Metric: "m", Labels: labels("cpu", "10")},
			b:    Sample{Timestamp: base, Metric: "m", Labels: labels("cpu", "2")},
			want: true,
		},
		{
			name: "fewer shared labels sort first",
			a:    Sample{Timestamp: base, Metric: "m", Labels: labels("cpu", "0")},
			b:    Sample{Timestamp: base, Metric: "m", Labels: labels("cpu", "0", "pid", "1")},
			want: true,
		},
		{
			name: "identical samples are not less",
			a:    Sample{Timestamp: base, Metric: "m", Labels: labels("cpu", "0")},
			b:    Sample{Timestamp: base, Metric: "m", Labels: labels("cpu", "0")},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := LessSample(tc.a, tc.b); got != tc.want {
				t.Fatalf("LessSample(%+v, %+v) = %t, want %t", tc.a, tc.b, got, tc.want)
			}
			// The ordering must be strict: two samples cannot both sort first.
			if got := LessSample(tc.b, tc.a); got && tc.want {
				t.Fatalf("LessSample is not antisymmetric for %q", tc.name)
			}
		})
	}
}

func TestMillis(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want int64
	}{
		{"zero", 0, 0},
		{"negative", -500 * time.Microsecond, 0},
		{"one nanosecond", time.Nanosecond, 1},
		{"rounds up below half a millisecond", 400 * time.Microsecond, 1},
		{"rounds half a millisecond up", 500 * time.Microsecond, 1},
		{"rounds 1.4ms down", 1400 * time.Microsecond, 1},
		{"rounds 1.5ms up", 1500 * time.Microsecond, 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Millis(tc.in); got != tc.want {
				t.Fatalf("Millis(%s) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}
