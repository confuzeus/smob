package process

import (
	"context"
	"testing"
	"time"
)

// BenchmarkCollect measures one collection tick against the real proc
// filesystem, including enumeration, stat and status parsing, rate math, and
// sample construction. Run it on a host with a representative process count:
//
//	go test -run '^$' -bench BenchmarkCollect -benchtime 100x ./internal/source/process
func BenchmarkCollect(b *testing.B) {
	src, err := New(Options{
		ProcPath:       "/proc",
		TopN:           0,
		Interval:       30 * time.Second,
		RescanInterval: time.Hour,
	}, nil)
	if err != nil {
		b.Fatalf("new process source: %v", err)
	}

	// The first tick scans the process list and primes the rate state.
	if _, err := src.Collect(context.Background(), time.Now()); err != nil {
		b.Fatalf("collect: %v", err)
	}
	pids, err := listPIDs("/proc")
	if err != nil {
		b.Fatalf("list pids: %v", err)
	}
	b.ReportMetric(float64(len(pids)), "processes")

	b.ResetTimer()
	for range b.N {
		if _, err := src.Collect(context.Background(), time.Now()); err != nil {
			b.Fatalf("collect: %v", err)
		}
	}
}
