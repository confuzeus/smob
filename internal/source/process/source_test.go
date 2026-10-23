package process

import (
	"slices"
	"testing"
)

func TestTop(t *testing.T) {
	// cpuKnown is only informational here; top orders by the cpu value, and a
	// process whose first delta is not measured carries the zero value.
	candidates := []candidate{
		{pid: 10, cpu: 25, rss: 100, cpuKnown: true},
		{pid: 11, cpu: 50, rss: 10, cpuKnown: true},
		{pid: 12, cpu: 0, rss: 900},
		{pid: 13, cpu: 0, rss: 900, cpuKnown: true},
		{pid: 14, cpu: 50, rss: 50, cpuKnown: true},
		{pid: 15, cpu: 10, rss: 100, cpuKnown: true},
	}

	tests := []struct {
		name  string
		limit int
		want  []int
	}{
		{"cpu then rss then pid", 0, []int{14, 11, 10, 15, 12, 13}},
		{"limit truncates", 2, []int{14, 11}},
		{"limit larger than the input", 10, []int{14, 11, 10, 15, 12, 13}},
		{"negative limit means all", -1, []int{14, 11, 10, 15, 12, 13}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := slices.Clone(candidates)
			got := top(in, tt.limit)
			pids := make([]int, len(got))
			for i, cand := range got {
				pids[i] = cand.pid
			}
			if !slices.Equal(pids, tt.want) {
				t.Fatalf("pids = %v, want %v", pids, tt.want)
			}
		})
	}
}
