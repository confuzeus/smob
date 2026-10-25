package disk

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/store"
)

// The CLI resolves both range bounds, so a zero (unbounded) start or end is
// only reachable through the store API. These tests use the real store and a
// real t.TempDir; the corrupt chunks prove a one-sided range never lists or
// opens a directory it can rule out.

func queryWindow() time.Time {
	return time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)
}

func writeQueryChunk(t *testing.T, s *Store, host, scope string, start time.Time, value float64) {
	t.Helper()
	err := s.Write(context.Background(), store.Chunk{
		Host:  host,
		Scope: scope,
		Start: start,
		End:   start.Add(5 * time.Minute),
		Samples: []model.Sample{{
			Timestamp: start.Add(time.Second),
			Metric:    "test.value",
			Value:     value,
			Labels:    map[string]string{"host": host, "scope": scope},
		}},
	})
	if err != nil {
		t.Fatalf("write chunk: %v", err)
	}
}

func plantCorruptChunk(t *testing.T, root, host, scope string, start time.Time) {
	t.Helper()
	dir := filepath.Join(root, host, scope,
		start.Format("2006"), start.Format("01"), start.Format("02"), start.Format("15"))
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		t.Fatalf("create corrupt chunk dir: %v", err)
	}
	name := start.Format("20060102T150405Z") + "-" + start.Add(5*time.Minute).Format("20060102T150405Z") + ".jsonl.gz"
	if err := os.WriteFile(filepath.Join(dir, name), []byte("not gzip"), filePerm); err != nil {
		t.Fatalf("write corrupt chunk: %v", err)
	}
}

func collectValues(t *testing.T, s *Store, q model.Query) []float64 {
	t.Helper()
	var values []float64
	err := s.Query(context.Background(), q, func(sample model.Sample) error {
		values = append(values, sample.Value)
		return nil
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	return values
}

func assertValues(t *testing.T, got, want []float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("values = %v, want %v", got, want)
		}
	}
}

// TestQueryZeroEndPrunesEarlierHours covers an unbounded end. A chunk before the
// start is corrupt, so the query fails if the year/month/day levels before the
// start are not pruned.
func TestQueryZeroEndPrunesEarlierHours(t *testing.T) {
	s, root := newTestStore(t)
	base := queryWindow()
	nextDay := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	earlier := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)

	writeQueryChunk(t, s, "alpha", "system", base, 1)
	writeQueryChunk(t, s, "alpha", "system", base.Add(2*time.Hour), 3)
	writeQueryChunk(t, s, "alpha", "system", nextDay, 4)
	plantCorruptChunk(t, root, "alpha", "system", earlier)
	plantCorruptChunk(t, root, "beta", "system", base)

	start := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
	got := collectValues(t, s, model.Query{
		Range: model.TimeRange{Start: start},
		Host:  "alpha",
		Scope: "system",
	})
	assertValues(t, got, []float64{4})

	got = collectValues(t, s, model.Query{
		Range: model.TimeRange{Start: base.Add(90 * time.Minute)},
		Host:  "alpha",
	})
	assertValues(t, got, []float64{3, 4})

	got = collectValues(t, s, model.Query{
		Range: model.TimeRange{Start: start},
		Host:  "gamma",
	})
	assertValues(t, got, nil)
}

// TestQueryZeroStartPrunesLaterHours covers an unbounded start. A chunk after
// the end and another host's chunk are corrupt, so the query fails if either is
// enumerated.
func TestQueryZeroStartPrunesLaterHours(t *testing.T) {
	s, root := newTestStore(t)
	base := queryWindow()
	later := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)

	writeQueryChunk(t, s, "alpha", "system", base, 1)
	writeQueryChunk(t, s, "alpha", "process", base.Add(time.Hour), 2)
	writeQueryChunk(t, s, "alpha", "system", base.Add(2*time.Hour), 3)
	plantCorruptChunk(t, root, "alpha", "system", later)
	plantCorruptChunk(t, root, "beta", "system", base)

	end := base.Add(90 * time.Minute)

	got := collectValues(t, s, model.Query{
		Range: model.TimeRange{End: end},
		Host:  "alpha",
		Scope: "system",
	})
	assertValues(t, got, []float64{1})

	got = collectValues(t, s, model.Query{
		Range: model.TimeRange{End: end},
		Host:  "alpha",
	})
	assertValues(t, got, []float64{1, 2})

	got = collectValues(t, s, model.Query{
		Range: model.TimeRange{End: end},
		Scope: "process",
	})
	assertValues(t, got, []float64{2})

	got = collectValues(t, s, model.Query{
		Range: model.TimeRange{End: end},
		Host:  "gamma",
	})
	assertValues(t, got, nil)
}

// TestQueryUnboundedRange covers both bounds zero with a host filter. The scan
// still descends only the selected host and scope.
func TestQueryUnboundedRange(t *testing.T) {
	s, _ := newTestStore(t)
	base := queryWindow()
	nextDay := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)

	writeQueryChunk(t, s, "alpha", "system", nextDay, 2)
	writeQueryChunk(t, s, "alpha", "system", base, 1)
	writeQueryChunk(t, s, "beta", "system", base, 3)

	got := collectValues(t, s, model.Query{Host: "alpha", Scope: "system"})
	assertValues(t, got, []float64{1, 2})

	got = collectValues(t, s, model.Query{Host: "alpha"})
	assertValues(t, got, []float64{1, 2})
}
