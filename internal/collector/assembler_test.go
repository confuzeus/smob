package collector

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/store/chunk"
	"github.com/confuzeus/smob/internal/store/disk"
)

// The assembler tests drive the real assembler over a real disk store in a
// temporary directory. Only the clock is supplied as an argument: add and
// flushAll already take sample timestamps and a flush time, so a scripted
// sequence of calls covers window changes and backward steps without a fake
// clock or store.

func TestPartialEnd(t *testing.T) {
	ts := func(hour, minute, second int) time.Time {
		return time.Date(2026, 10, 23, hour, minute, second, 0, time.UTC)
	}
	sample := func(at time.Time) model.Sample {
		return model.Sample{Timestamp: at, Metric: "test.value", Value: 1}
	}

	cases := []struct {
		name    string
		cur     *pendingChunk
		at      time.Time
		wantEnd time.Time
	}{
		{
			name: "flush inside the window keeps the flush time",
			cur: &pendingChunk{
				start:   ts(12, 55, 0),
				end:     ts(13, 0, 0),
				samples: []model.Sample{sample(ts(12, 56, 30))},
			},
			at:      ts(12, 57, 10),
			wantEnd: ts(12, 57, 11),
		},
		{
			name: "sample after the flush extends the end",
			cur: &pendingChunk{
				start:   ts(12, 55, 0),
				end:     ts(13, 0, 0),
				samples: []model.Sample{sample(ts(12, 59, 59))},
			},
			at:      ts(12, 56, 0),
			wantEnd: ts(13, 0, 0),
		},
		{
			name: "flush exactly at the window end",
			cur: &pendingChunk{
				start:   ts(12, 55, 0),
				end:     ts(13, 0, 0),
				samples: []model.Sample{sample(ts(12, 59, 30))},
			},
			at:      ts(13, 0, 0),
			wantEnd: ts(13, 0, 0),
		},
		{
			name: "flush long after the window end is clamped",
			cur: &pendingChunk{
				start:   ts(12, 55, 0),
				end:     ts(13, 0, 0),
				samples: []model.Sample{sample(ts(12, 57, 0))},
			},
			at:      ts(13, 7, 42),
			wantEnd: ts(13, 0, 0),
		},
		{
			name:    "no samples still ends after the flush time",
			cur:     &pendingChunk{start: ts(12, 55, 0), end: ts(13, 0, 0)},
			at:      ts(12, 55, 30),
			wantEnd: ts(12, 55, 31),
		},
		{
			name: "flush just past an hour boundary is clamped",
			cur: &pendingChunk{
				start:   ts(12, 59, 0),
				end:     ts(13, 0, 0),
				samples: []model.Sample{sample(ts(12, 59, 59))},
			},
			at:      time.Date(2026, 10, 23, 13, 0, 0, 700_000_000, time.UTC),
			wantEnd: ts(13, 0, 0),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := partialEnd(tc.cur, tc.at)
			if !got.Equal(tc.wantEnd) {
				t.Errorf("partialEnd() = %s, want %s", got, tc.wantEnd)
			}
			if got.After(tc.cur.end) {
				t.Errorf("partialEnd() = %s is past the window end %s", got, tc.cur.end)
			}
		})
	}
}

// assemblerAt returns a fixed UTC time on the shared test date.
func assemblerAt(hour, minute, second int) time.Time {
	return time.Date(2026, 10, 23, hour, minute, second, 0, time.UTC)
}

// newTestAssembler returns an assembler writing one-minute windows to a real
// disk store rooted in a fresh temporary directory.
func newTestAssembler(t *testing.T, logger *slog.Logger) (*assembler, string) {
	t.Helper()
	root := t.TempDir()
	st, err := disk.New(root, "", nil)
	if err != nil {
		t.Fatalf("new disk store: %v", err)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return newAssembler("test-host", time.Minute, st, logger), root
}

// writtenChunk is one chunk file read back from the disk store.
type writtenChunk struct {
	name    string
	start   time.Time
	end     time.Time
	samples []model.Sample
}

// readChunks walks root and decodes every chunk file it finds, sorted by name.
func readChunks(t *testing.T, root string) []writtenChunk {
	t.Helper()
	var chunks []writtenChunk
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), chunk.FileSuffix) {
			return nil
		}
		start, end, err := chunk.ParseName(d.Name())
		if err != nil {
			return fmt.Errorf("parse chunk name %s: %w", path, err)
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		gz, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return fmt.Errorf("open gzip %s: %w", path, err)
		}

		var samples []model.Sample
		dec := json.NewDecoder(gz)
		for {
			var sample model.Sample
			if err := dec.Decode(&sample); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				gz.Close()
				f.Close()
				return fmt.Errorf("decode %s: %w", path, err)
			}
			samples = append(samples, sample)
		}
		if err := errors.Join(gz.Close(), f.Close()); err != nil {
			return fmt.Errorf("close %s: %w", path, err)
		}
		chunks = append(chunks, writtenChunk{name: d.Name(), start: start, end: end, samples: samples})
		return nil
	})
	if err != nil {
		t.Fatalf("read chunks: %v", err)
	}
	slices.SortFunc(chunks, func(a, b writtenChunk) int { return strings.Compare(a.name, b.name) })
	return chunks
}

// chunkByName indexes readChunks by filename for exact-name assertions.
func chunkByName(t *testing.T, root string) map[string]writtenChunk {
	t.Helper()
	byName := make(map[string]writtenChunk)
	for _, c := range readChunks(t, root) {
		byName[c.name] = c
	}
	return byName
}

// valueAt is a compact sample constructor for assembler sequences.
func valueAt(at time.Time, metric string, value float64) model.Sample {
	return model.Sample{Timestamp: at, Metric: metric, Value: value}
}

// addSamples feeds one batch to the assembler and fails on a write error.
func addSamples(t *testing.T, a *assembler, scope string, samples ...model.Sample) {
	t.Helper()
	if err := a.add(context.Background(), scope, samples); err != nil {
		t.Fatalf("add %s samples: %v", scope, err)
	}
}

// flushAt flushes every active chunk and fails on a write error.
func flushAt(t *testing.T, a *assembler, at time.Time) {
	t.Helper()
	if err := a.flushAll(context.Background(), at); err != nil {
		t.Fatalf("flushAll: %v", err)
	}
}

// warnRecords returns the decoded JSON log records at warn level.
func warnRecords(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var records []map[string]any
	scanner := bufio.NewScanner(logs)
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("decode log line: %v", err)
		}
		if record["level"] == "WARN" {
			records = append(records, record)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read log: %v", err)
	}
	return records
}

// TestAssemblerLaterWindowClosesAtAlignedEnd locks in the no-overshoot rule: a
// sample in a later window closes the current chunk at that window's aligned
// end, not at the new sample's timestamp, and opens a new pending chunk.
func TestAssemblerLaterWindowClosesAtAlignedEnd(t *testing.T) {
	a, root := newTestAssembler(t, nil)

	addSamples(t, a, "system", valueAt(assemblerAt(12, 0, 10), "test.value", 1))
	addSamples(t, a, "system", valueAt(assemblerAt(12, 1, 5), "test.value", 2))

	chunks := readChunks(t, root)
	if len(chunks) != 1 {
		t.Fatalf("chunks after the later-window sample = %d, want the first window closed and the second pending", len(chunks))
	}
	closed := chunks[0]
	if want := assemblerAt(12, 0, 0); !closed.start.Equal(want) {
		t.Errorf("closed chunk start = %s, want the aligned window start %s", closed.start, want)
	}
	if want := assemblerAt(12, 1, 0); !closed.end.Equal(want) {
		t.Errorf("closed chunk end = %s, want the aligned window end %s (no partial overshoot)", closed.end, want)
	}
	if got := len(closed.samples); got != 1 || closed.samples[0].Value != 1 {
		t.Fatalf("closed chunk samples = %+v, want only the first-window sample", closed.samples)
	}

	flushAt(t, a, assemblerAt(12, 1, 20))
	chunks = readChunks(t, root)
	if len(chunks) != 2 {
		t.Fatalf("chunks after flush = %d, want 2", len(chunks))
	}
	second := chunks[1]
	if want := assemblerAt(12, 1, 0); !second.start.Equal(want) {
		t.Errorf("second chunk start = %s, want %s", second.start, want)
	}
	if want := assemblerAt(12, 1, 21); !second.end.Equal(want) {
		t.Errorf("second chunk end = %s, want the flush time plus one second %s", second.end, want)
	}
}

// TestAssemblerBackwardClockStep covers the documented clock rule. A step that
// lands in an earlier window closes the active chunk at its aligned end and logs
// the warning; a smaller step inside the same window is absorbed.
func TestAssemblerBackwardClockStep(t *testing.T) {
	t.Run("larger than one interval", func(t *testing.T) {
		var logs bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logs, nil))
		a, root := newTestAssembler(t, logger)

		addSamples(t, a, "system", valueAt(assemblerAt(12, 5, 0), "test.value", 1))
		addSamples(t, a, "system", valueAt(assemblerAt(12, 3, 0), "test.value", 2))

		chunks := readChunks(t, root)
		if len(chunks) != 1 {
			t.Fatalf("chunks after the backward step = %d, want the active chunk closed", len(chunks))
		}
		if want := assemblerAt(12, 5, 0); !chunks[0].start.Equal(want) {
			t.Errorf("closed chunk start = %s, want %s", chunks[0].start, want)
		}
		if want := assemblerAt(12, 6, 0); !chunks[0].end.Equal(want) {
			t.Errorf("closed chunk end = %s, want the aligned window end %s", chunks[0].end, want)
		}

		warns := warnRecords(t, &logs)
		if len(warns) != 1 {
			t.Fatalf("warnings = %d, want the one documented backward-clock warning", len(warns))
		}
		if got, want := warns[0]["msg"], "wall clock moved backwards; closing the active chunk"; got != want {
			t.Errorf("warning msg = %q, want %q", got, want)
		}
		if got := warns[0]["scope"]; got != "system" {
			t.Errorf("warning scope = %v, want system", got)
		}

		flushAt(t, a, assemblerAt(12, 3, 30))
		chunks = readChunks(t, root)
		if len(chunks) != 2 {
			t.Fatalf("chunks after flush = %d, want the new pending window written", len(chunks))
		}
	})

	t.Run("smaller than one interval stays in the window", func(t *testing.T) {
		var logs bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logs, nil))
		a, root := newTestAssembler(t, logger)

		addSamples(t, a, "system", valueAt(assemblerAt(12, 5, 30), "test.value", 1))
		addSamples(t, a, "system", valueAt(assemblerAt(12, 5, 10), "test.value", 2))

		if chunks := readChunks(t, root); len(chunks) != 0 {
			t.Fatalf("chunks after a small backward step = %d, want the same pending chunk", len(chunks))
		}
		if warns := warnRecords(t, &logs); len(warns) != 0 {
			t.Fatalf("warnings = %d, want none for a step inside the window", len(warns))
		}

		flushAt(t, a, assemblerAt(12, 5, 40))
		chunks := readChunks(t, root)
		if len(chunks) != 1 {
			t.Fatalf("chunks after flush = %d, want 1", len(chunks))
		}
		if got := len(chunks[0].samples); got != 2 {
			t.Errorf("samples = %d, want both samples in the one window", got)
		}
	})

	// The implementation closes on any sample that lands in an earlier window,
	// while docs/IMPLEMENTATION.md describes only steps larger than the chunk
	// interval. A one-second step across a window boundary therefore also closes
	// the chunk and warns. Asserted as actual behavior; see the report.
	t.Run("boundary crossing under one interval also closes", func(t *testing.T) {
		var logs bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logs, nil))
		a, root := newTestAssembler(t, logger)

		addSamples(t, a, "system", valueAt(assemblerAt(13, 0, 0), "test.value", 1))
		addSamples(t, a, "system", valueAt(assemblerAt(12, 59, 59), "test.value", 2))

		if chunks := readChunks(t, root); len(chunks) != 1 {
			t.Fatalf("chunks = %d, want the active chunk closed on a one-second backward step", len(chunks))
		}
		if warns := warnRecords(t, &logs); len(warns) != 1 {
			t.Errorf("warnings = %d, want the backward-clock warning", len(warns))
		}
	})
}

// TestAssemblerLateSampleForClosedWindow documents what happens when a sample
// arrives for a window that was already written. It is not dropped: the active
// chunk is emitted first, the late sample reopens the earlier window as the
// pending chunk, and the flush writes that window again under a collision
// suffix. Asserted as-is; the duplication is the documented collision behavior.
func TestAssemblerLateSampleForClosedWindow(t *testing.T) {
	a, root := newTestAssembler(t, nil)

	addSamples(t, a, "system", valueAt(assemblerAt(12, 0, 10), "test.value", 1))
	addSamples(t, a, "system", valueAt(assemblerAt(12, 1, 10), "test.value", 2))
	// Late sample for the 12:00 window, which was closed by the previous add.
	addSamples(t, a, "system", valueAt(assemblerAt(12, 0, 30), "test.value", 3))

	byName := chunkByName(t, root)
	base, err := chunk.Key("test-host", "system", assemblerAt(12, 0, 0), assemblerAt(12, 1, 0))
	if err != nil {
		t.Fatalf("chunk key: %v", err)
	}
	baseName := filepath.Base(base)
	later, err := chunk.Key("test-host", "system", assemblerAt(12, 1, 0), assemblerAt(12, 2, 0))
	if err != nil {
		t.Fatalf("chunk key: %v", err)
	}
	laterName := filepath.Base(later)

	if len(byName) != 2 {
		t.Fatalf("chunks after the late sample = %d, want the closed window and the emitted active chunk", len(byName))
	}
	if _, ok := byName[baseName]; !ok {
		t.Fatalf("chunks = %v, want the original %s", chunkNames(byName), baseName)
	}
	emitted, ok := byName[laterName]
	if !ok {
		t.Fatalf("chunks = %v, want the emitted active chunk %s", chunkNames(byName), laterName)
	}
	if want := assemblerAt(12, 2, 0); !emitted.end.Equal(want) {
		t.Errorf("emitted active chunk end = %s, want its aligned end %s", emitted.end, want)
	}

	flushAt(t, a, assemblerAt(12, 1, 30))

	byName = chunkByName(t, root)
	if len(byName) != 3 {
		t.Fatalf("chunks after flush = %v, want the closed window written twice", chunkNames(byName))
	}
	suffixedName := filepath.Base(chunk.CollisionKey(base, 1))
	suffixed, ok := byName[suffixedName]
	if !ok {
		t.Fatalf("chunks = %v, want the collision-suffixed %s", chunkNames(byName), suffixedName)
	}
	if got := len(suffixed.samples); got != 1 || suffixed.samples[0].Value != 3 {
		t.Fatalf("suffixed chunk samples = %+v, want only the late sample", suffixed.samples)
	}
	if want := assemblerAt(12, 1, 0); !suffixed.end.Equal(want) {
		t.Errorf("suffixed chunk end = %s, want the window end %s", suffixed.end, want)
	}
	if got := countValue(readChunks(t, root), 3); got != 1 {
		t.Errorf("late sample appears %d times, want exactly once", got)
	}
}

// chunkNames lists filenames for failure messages.
func chunkNames(byName map[string]writtenChunk) []string {
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// countValue counts samples carrying value across chunks.
func countValue(chunks []writtenChunk, value float64) int {
	var n int
	for _, c := range chunks {
		for _, sample := range c.samples {
			if sample.Value == value {
				n++
			}
		}
	}
	return n
}

// TestAssemblerWritesCanonicalOrder pins the written order to timestamp, then
// metric, then canonical label order, matching the store contract that query
// relies on. Identical series keep their input order because the sort is stable.
func TestAssemblerWritesCanonicalOrder(t *testing.T) {
	t.Run("timestamps, metrics, and labels", func(t *testing.T) {
		a, root := newTestAssembler(t, nil)

		batch := []model.Sample{
			{Timestamp: assemblerAt(12, 0, 30), Metric: "b", Value: 21, Labels: map[string]string{"cpu": "0"}},
			{Timestamp: assemblerAt(12, 0, 10), Metric: "a", Value: 22, Labels: map[string]string{"cpu": "0"}},
			{Timestamp: assemblerAt(12, 0, 30), Metric: "a", Value: 10, Labels: map[string]string{"cpu": "0"}},
			{Timestamp: assemblerAt(12, 0, 30), Metric: "a", Value: 9},
			{Timestamp: assemblerAt(12, 0, 10), Metric: "b", Value: 20, Labels: map[string]string{"cpu": "0"}},
			{Timestamp: assemblerAt(12, 0, 30), Metric: "a", Value: 11, Labels: map[string]string{"cpu": "1"}},
		}
		addSamples(t, a, "system", batch...)
		flushAt(t, a, assemblerAt(12, 0, 45))

		chunks := readChunks(t, root)
		if len(chunks) != 1 {
			t.Fatalf("chunks = %d, want 1", len(chunks))
		}
		want := []string{
			sampleKey(assemblerAt(12, 0, 10), "a", "cpu=0,host=test-host,scope=system"),
			sampleKey(assemblerAt(12, 0, 10), "b", "cpu=0,host=test-host,scope=system"),
			sampleKey(assemblerAt(12, 0, 30), "a", "cpu=0,host=test-host,scope=system"),
			sampleKey(assemblerAt(12, 0, 30), "a", "cpu=1,host=test-host,scope=system"),
			sampleKey(assemblerAt(12, 0, 30), "a", "host=test-host,scope=system"),
			sampleKey(assemblerAt(12, 0, 30), "b", "cpu=0,host=test-host,scope=system"),
		}
		if got := sampleKeys(chunks[0].samples); !slices.Equal(got, want) {
			t.Errorf("chunk order:\n got %v\nwant %v", got, want)
		}
	})

	t.Run("identical series keep input order", func(t *testing.T) {
		a, root := newTestAssembler(t, nil)

		var batch []model.Sample
		for _, value := range []float64{60, 50, 40, 30, 20, 10} {
			batch = append(batch, model.Sample{
				Timestamp: assemblerAt(12, 0, 30),
				Metric:    "test.value",
				Value:     value,
				Labels:    map[string]string{"cpu": "0"},
			})
		}
		addSamples(t, a, "system", batch...)
		flushAt(t, a, assemblerAt(12, 0, 45))

		chunks := readChunks(t, root)
		if len(chunks) != 1 {
			t.Fatalf("chunks = %d, want 1", len(chunks))
		}
		var values []float64
		for _, sample := range chunks[0].samples {
			values = append(values, sample.Value)
		}
		if want := []float64{60, 50, 40, 30, 20, 10}; !slices.Equal(values, want) {
			t.Errorf("identical-series order = %v, want the input order %v", values, want)
		}
	})
}

// sampleKey renders one sample as an orderable string; labels are sorted by key
// on the test side so the expectation does not depend on the production helper.
func sampleKey(at time.Time, metric, labels string) string {
	return at.Format(time.RFC3339Nano) + " " + metric + " " + labels
}

func sampleKeys(samples []model.Sample) []string {
	keys := make([]string, len(samples))
	for i, sample := range samples {
		pairs := make([]string, 0, len(sample.Labels))
		for key, value := range sample.Labels {
			pairs = append(pairs, key+"="+value)
		}
		slices.Sort(pairs)
		keys[i] = sampleKey(sample.Timestamp, sample.Metric, strings.Join(pairs, ","))
	}
	return keys
}

// TestAssemblerWritesActiveChunkOnce covers the flush contract: an empty
// assembler writes nothing, and an active chunk is written exactly once however
// many flushes follow.
func TestAssemblerWritesActiveChunkOnce(t *testing.T) {
	t.Run("empty assembler", func(t *testing.T) {
		a, root := newTestAssembler(t, nil)
		flushAt(t, a, assemblerAt(12, 0, 0))
		if len(a.active) != 0 {
			t.Fatalf("active chunks = %d, want 0", len(a.active))
		}
		if chunks := readChunks(t, root); len(chunks) != 0 {
			t.Fatalf("chunks = %d, want none from an empty flush", len(chunks))
		}
	})

	t.Run("repeated flush", func(t *testing.T) {
		a, root := newTestAssembler(t, nil)
		addSamples(t, a, "system", valueAt(assemblerAt(12, 0, 10), "test.value", 1))
		addSamples(t, a, "system", valueAt(assemblerAt(12, 1, 10), "test.value", 2))

		flushAt(t, a, assemblerAt(12, 1, 30))
		flushAt(t, a, assemblerAt(12, 1, 40))

		chunks := readChunks(t, root)
		if len(chunks) != 2 {
			t.Fatalf("chunks = %d, want one per window and no rewrite", len(chunks))
		}
		seen := make(map[time.Time]int)
		for _, c := range chunks {
			seen[c.start]++
		}
		for start, n := range seen {
			if n != 1 {
				t.Errorf("window %s written %d times, want 1", start, n)
			}
		}
	})
}

// TestAssemblerOnceFlushesOneChunkPerScope pins the collect --once contract: one
// partial chunk per scope, starting at the aligned window start and ending one
// second after the flush time.
func TestAssemblerOnceFlushesOneChunkPerScope(t *testing.T) {
	a, root := newTestAssembler(t, nil)

	addSamples(t, a, "process", valueAt(assemblerAt(12, 0, 30), "process.cpu.percent", 1))
	addSamples(t, a, "system", valueAt(assemblerAt(12, 0, 30), "system.cpu.percent", 2))
	addSamples(t, a, "process", valueAt(assemblerAt(12, 0, 45), "process.memory.bytes", 3))
	flushAt(t, a, assemblerAt(12, 0, 50))

	chunks := readChunks(t, root)
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d, want one per scope", len(chunks))
	}
	wantEnd := assemblerAt(12, 0, 51)
	perScope := make(map[string]int)
	for _, c := range chunks {
		if want := assemblerAt(12, 0, 0); !c.start.Equal(want) {
			t.Errorf("chunk %s start = %s, want the aligned window start %s", c.name, c.start, want)
		}
		if !c.end.Equal(wantEnd) {
			t.Errorf("chunk %s end = %s, want the flush time plus one second %s", c.name, c.end, wantEnd)
		}
		if len(c.samples) == 0 {
			t.Errorf("chunk %s has no samples", c.name)
		}
		scope := c.samples[0].Label("scope")
		if scope == "" {
			t.Fatalf("chunk %s samples are not stamped with a scope", c.name)
		}
		perScope[scope]++
	}
	for _, scope := range []string{"process", "system"} {
		if got := perScope[scope]; got != 1 {
			t.Errorf("scope %s has %d chunks, want 1", scope, got)
		}
	}
}
