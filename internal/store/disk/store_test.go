package disk

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/store"
	"github.com/confuzeus/smob/internal/store/chunk"
)

// These tests cover filesystem edges the CLI cannot reach: there is no flag to
// seed a symlink at a temp path, write the same window twice, or put a
// directory where a chunk belongs, and adding test-only flags is prohibited.
// They use a real t.TempDir and the real store, never mocks.

func newTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	root := t.TempDir()
	s, err := New(root, "", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return s, root
}

func testWindow() time.Time {
	return time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)
}

func testChunk(start time.Time, value float64) store.Chunk {
	return store.Chunk{
		Host:  "web-1",
		Scope: "system",
		Start: start,
		End:   start.Add(5 * time.Minute),
		Samples: []model.Sample{{
			Timestamp: start,
			Metric:    "test.value",
			Value:     value,
			Labels:    map[string]string{"scope": "system"},
		}},
	}
}

func chunkPath(t *testing.T, root string, c store.Chunk) string {
	t.Helper()
	key, err := chunk.Key(c.Host, c.Scope, c.Start, c.End)
	if err != nil {
		t.Fatalf("chunk key: %v", err)
	}
	return filepath.Join(root, filepath.FromSlash(key))
}

func chunkFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), chunk.FileSuffix) {
			names = append(names, e.Name())
		}
	}
	slices.Sort(names)
	return names
}

func decodeSamples(t *testing.T, path string) []model.Sample {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip %s: %v", path, err)
	}
	defer gz.Close()

	var samples []model.Sample
	dec := json.NewDecoder(gz)
	for {
		var sample model.Sample
		err := dec.Decode(&sample)
		if errors.Is(err, io.EOF) {
			return samples
		}
		if err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		samples = append(samples, sample)
	}
}

func decodeValues(t *testing.T, path string) []float64 {
	t.Helper()
	samples := decodeSamples(t, path)
	values := make([]float64, len(samples))
	for i, sample := range samples {
		values[i] = sample.Value
	}
	return values
}

// filesBelow lists every regular file below root, which lets a test assert that
// a failed write or a probe left nothing behind anywhere in the store.
func filesBelow(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return files
}

// assertNoChunkOrTemp fails when any chunk or temp pattern exists below root.
func assertNoChunkOrTemp(t *testing.T, root string) {
	t.Helper()
	for _, path := range filesBelow(t, root) {
		name := filepath.Base(path)
		if strings.HasSuffix(name, chunk.FileSuffix) || isTempName(name) {
			t.Fatalf("failed write left %s behind", path)
		}
	}
}

func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".smob-") {
			t.Fatalf("leftover temp file %s", e.Name())
		}
	}
}

func TestWriteIgnoresPlantedLegacyTempSymlink(t *testing.T) {
	s, root := newTestStore(t)
	c := testChunk(testWindow(), 1)

	final := chunkPath(t, root, c)
	if err := os.MkdirAll(filepath.Dir(final), dirPerm); err != nil {
		t.Fatalf("create hour dir: %v", err)
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("sentinel"), filePerm); err != nil {
		t.Fatalf("write target: %v", err)
	}
	if err := os.Symlink(target, final+".tmp"); err != nil {
		t.Fatalf("plant symlink: %v", err)
	}

	if err := s.Write(context.Background(), c); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got) != "sentinel" {
		t.Fatalf("symlink target truncated to %q", got)
	}
	if values := decodeValues(t, final); !slices.Equal(values, []float64{1}) {
		t.Fatalf("chunk values = %v, want [1]", values)
	}
	assertNoTempFiles(t, filepath.Dir(final))
}

func TestWriteSameWindowTwiceKeepsBoth(t *testing.T) {
	s, root := newTestStore(t)
	c := testChunk(testWindow(), 1)

	if err := s.Write(context.Background(), c); err != nil {
		t.Fatalf("first write: %v", err)
	}
	second := testChunk(testWindow(), 2)
	if err := s.Write(context.Background(), second); err != nil {
		t.Fatalf("second write: %v", err)
	}

	dir := filepath.Dir(chunkPath(t, root, c))
	base := filepath.Base(chunkPath(t, root, c))
	suffixed := chunk.CollisionKey(base, 1)
	if names, want := chunkFiles(t, dir), []string{base, suffixed}; !slices.Equal(names, slices.Sorted(slices.Values(want))) {
		t.Fatalf("chunk files = %v, want %v", names, want)
	}

	for name, wantValue := range map[string]float64{base: 1, suffixed: 2} {
		path := filepath.Join(dir, name)
		if values := decodeValues(t, path); !slices.Equal(values, []float64{wantValue}) {
			t.Fatalf("%s values = %v, want [%v]", name, values, wantValue)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if got, want := info.Mode().Perm(), fs.FileMode(0o640); got != want {
			t.Fatalf("%s mode = %o, want %o", name, got, want)
		}
	}
	dirInfo, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat %s: %v", dir, err)
	}
	if got, want := dirInfo.Mode().Perm(), fs.FileMode(0o750); got != want {
		t.Fatalf("chunk directory mode = %o, want %o", got, want)
	}
}

func TestWriteSuffixesWhenBaseKeyIsDirectory(t *testing.T) {
	s, root := newTestStore(t)
	c := testChunk(testWindow(), 1)

	base := chunkPath(t, root, c)
	if err := os.MkdirAll(base, dirPerm); err != nil {
		t.Fatalf("create directory at base key: %v", err)
	}

	if err := s.Write(context.Background(), c); err != nil {
		t.Fatalf("write: %v", err)
	}

	info, err := os.Stat(base)
	if err != nil {
		t.Fatalf("stat base: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("%s is no longer a directory", base)
	}

	dir := filepath.Dir(base)
	suffixed := chunk.CollisionKey(base, 1)
	if values := decodeValues(t, suffixed); !slices.Equal(values, []float64{1}) {
		t.Fatalf("suffixed chunk values = %v, want [1]", values)
	}
	if names := chunkFiles(t, dir); !slices.Equal(names, []string{filepath.Base(suffixed)}) {
		t.Fatalf("chunk files = %v, want [%s]", names, filepath.Base(suffixed))
	}
}

// TestCheckRemovesCrashLeftovers covers the startup cleanup: only the temp
// patterns are removed, chunks and unrelated files or directories stay.
func TestCheckRemovesCrashLeftovers(t *testing.T) {
	s, root := newTestStore(t)
	c := testChunk(testWindow(), 1)
	if err := s.Write(context.Background(), c); err != nil {
		t.Fatalf("write: %v", err)
	}

	hourDir := filepath.Dir(chunkPath(t, root, c))
	leftovers := []string{
		".smob-123.tmp",
		"20261023T160000Z-20261023T160500Z.jsonl.gz.tmp",
		".check-456",
	}
	for _, name := range leftovers {
		if err := os.WriteFile(filepath.Join(hourDir, name), []byte("stale"), filePerm); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	staleDir := filepath.Join(hourDir, ".smob-dir.tmp")
	if err := os.MkdirAll(staleDir, dirPerm); err != nil {
		t.Fatalf("seed temp directory: %v", err)
	}
	kept := []string{"notes.txt", ".smob-keep"}
	for _, name := range kept {
		if err := os.WriteFile(filepath.Join(hourDir, name), []byte("keep"), filePerm); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}

	if err := s.Check(context.Background()); err != nil {
		t.Fatalf("check: %v", err)
	}

	for _, name := range leftovers {
		if _, err := os.Lstat(filepath.Join(hourDir, name)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s still exists after cleanup: %v", name, err)
		}
	}
	for _, name := range kept {
		if _, err := os.Lstat(filepath.Join(hourDir, name)); err != nil {
			t.Fatalf("%s removed by cleanup: %v", name, err)
		}
	}
	if info, err := os.Stat(staleDir); err != nil || !info.IsDir() {
		t.Fatalf("temp directory removed by cleanup: %v", err)
	}
	if names := chunkFiles(t, hourDir); len(names) != 1 {
		t.Fatalf("chunk files = %v, want one", names)
	}
	if values := decodeValues(t, chunkPath(t, root, c)); !slices.Equal(values, []float64{1}) {
		t.Fatalf("chunk values = %v, want [1]", values)
	}
}

// TestWriteRejectsSymlinkedHost covers containment: a symlinked host component
// would put the chunk outside the data directory, so the write fails instead.
func TestWriteRejectsSymlinkedHost(t *testing.T) {
	s, root := newTestStore(t)
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "web-1")); err != nil {
		t.Fatalf("plant host symlink: %v", err)
	}

	err := s.Write(context.Background(), testChunk(testWindow(), 1))
	if err == nil {
		t.Fatal("write through a symlinked host succeeded")
	}
	if !errors.Is(err, store.ErrPersistent) {
		t.Fatalf("write error = %v, want a persistent store failure", err)
	}

	var files []string
	walkErr := filepath.WalkDir(outside, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk outside directory: %v", walkErr)
	}
	if len(files) != 0 {
		t.Fatalf("write escaped the data directory: %v", files)
	}
}

// TestWriteThroughSymlinkedRootPublishes keeps the normal case working when the
// data directory itself is reached through a symlink.
func TestWriteThroughSymlinkedRootPublishes(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "data")
	if err := os.MkdirAll(root, dirPerm); err != nil {
		t.Fatalf("create root: %v", err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatalf("plant root symlink: %v", err)
	}
	s, err := New(link, "", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	c := testChunk(testWindow(), 3)
	if err := s.Write(context.Background(), c); err != nil {
		t.Fatalf("write: %v", err)
	}
	if values := decodeValues(t, chunkPath(t, root, c)); !slices.Equal(values, []float64{3}) {
		t.Fatalf("chunk values = %v, want [3]", values)
	}
	assertNoTempFiles(t, filepath.Dir(chunkPath(t, root, c)))
}

// TestWriteRoundTripsPreparedSamples covers the wire format end to end: the
// published file is gzip-decodable, its samples equal what chunk.Prepare handed
// the encoder, and its bytes are exactly the deterministic encoding of those
// prepared samples. Input order, label maps, and sub-second timestamps all
// survive the round trip.
func TestWriteRoundTripsPreparedSamples(t *testing.T) {
	s, root := newTestStore(t)
	start := testWindow()
	end := start.Add(5 * time.Minute)
	input := []model.Sample{
		{
			Timestamp: start.Add(3*time.Second + 250*time.Millisecond),
			Metric:    "test.value",
			Value:     3.25,
			Labels:    map[string]string{"scope": "system", "cpu": "1"},
		},
		{
			Timestamp: start.Add(time.Second),
			Metric:    "test.value",
			Value:     1.5,
			Labels:    map[string]string{"scope": "system", "cpu": "0"},
		},
		{
			Timestamp: start.Add(2*time.Second + 123456789*time.Nanosecond),
			Metric:    "test.other",
			Value:     -2,
			Labels:    map[string]string{"scope": "system"},
		},
	}
	prepared, err := chunk.Prepare(start, end, input)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	c := store.Chunk{Host: "web-1", Scope: "system", Start: start, End: end, Samples: input}
	if err := s.Write(context.Background(), c); err != nil {
		t.Fatalf("write: %v", err)
	}

	path := chunkPath(t, root, c)
	got := decodeSamples(t, path)
	if !reflect.DeepEqual(got, prepared) {
		t.Fatalf("round-trip samples = %+v, want %+v", got, prepared)
	}

	var want bytes.Buffer
	if err := chunk.Encode(&want, prepared); err != nil {
		t.Fatalf("encode prepared samples: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read chunk: %v", err)
	}
	if !bytes.Equal(raw, want.Bytes()) {
		t.Fatalf("chunk bytes = %d bytes, want the %d-byte deterministic encoding", len(raw), want.Len())
	}
	assertNoTempFiles(t, filepath.Dir(path))
}

// TestWriteInvalidChunkLeavesNothing covers the failure path before the temp
// file exists: chunk.Prepare rejects a sample outside the half-open window, and
// chunk.Key rejects a reversed window. Neither is a durability failure, so the
// error must not be marked persistent, and no chunk or temp may appear.
func TestWriteInvalidChunkLeavesNothing(t *testing.T) {
	start := testWindow()
	cases := []struct {
		name  string
		chunk func() store.Chunk
	}{
		{
			name: "sample before window",
			chunk: func() store.Chunk {
				c := testChunk(start, 1)
				c.Samples[0].Timestamp = start.Add(-time.Second)
				return c
			},
		},
		{
			name: "sample at window end",
			chunk: func() store.Chunk {
				c := testChunk(start, 1)
				c.Samples[0].Timestamp = start.Add(5 * time.Minute)
				return c
			},
		},
		{
			name: "reversed window",
			chunk: func() store.Chunk {
				c := testChunk(start, 1)
				c.End = start.Add(-time.Minute)
				c.Samples[0].Timestamp = start
				return c
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, root := newTestStore(t)
			err := s.Write(context.Background(), tc.chunk())
			if err == nil {
				t.Fatal("write of an invalid chunk succeeded")
			}
			if errors.Is(err, store.ErrPersistent) {
				t.Fatalf("validation error marked persistent: %v", err)
			}
			assertNoChunkOrTemp(t, root)
		})
	}
}

// TestCheckCreatesMissingRoot covers the fresh-install startup probe. New must
// not create anything; Check creates the root, leaves no .check-* probe file,
// and accepts the directory on a second call.
func TestCheckCreatesMissingRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "nested", "data")
	s, err := New(root, "", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	if _, err := os.Stat(root); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("New created the root: %v", err)
	}
	if err := s.Check(context.Background()); err != nil {
		t.Fatalf("check: %v", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("stat root after check: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("root is not a directory")
	}
	if files := filesBelow(t, root); len(files) != 0 {
		t.Fatalf("check left files behind: %v", files)
	}
	if err := s.Check(context.Background()); err != nil {
		t.Fatalf("second check: %v", err)
	}
}

// TestCheckFailsWhenRootIsRegularFile covers an unusable data directory at
// startup: every Check call reports a persistent failure and the file is not
// replaced or truncated.
func TestCheckFailsWhenRootIsRegularFile(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	const contents = "not a directory"
	if err := os.WriteFile(root, []byte(contents), filePerm); err != nil {
		t.Fatalf("seed root file: %v", err)
	}
	s, err := New(root, "", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		err := s.Check(context.Background())
		if err == nil {
			t.Fatalf("check %d of a file root succeeded", attempt)
		}
		if !errors.Is(err, store.ErrPersistent) {
			t.Fatalf("check %d error = %v, want a persistent store failure", attempt, err)
		}
	}

	got, err := os.ReadFile(root)
	if err != nil {
		t.Fatalf("read root file: %v", err)
	}
	if string(got) != contents {
		t.Fatalf("root file = %q, want %q", got, contents)
	}
	if files := filesBelow(t, filepath.Dir(root)); len(files) != 1 {
		t.Fatalf("check left files behind: %v", files)
	}
}

// TestWriteFailsWhenParentDirectoryIsReadOnly covers the directory-chain part of
// the durability path that a caller can observe: Write must report a persistent
// failure when it cannot create or open the chunk's parent. The fsync calls in
// writeSamples and syncChain themselves are not observable without a test-only
// seam, which this repository prohibits, so their success is inferred from a
// successful write and their failure from this permission error.
func TestWriteFailsWhenParentDirectoryIsReadOnly(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions, so the parent stays writable")
	}

	root := t.TempDir()
	s, err := New(root, "", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	hostDir := filepath.Join(root, "web-1")
	if err := os.Mkdir(hostDir, dirPerm); err != nil {
		t.Fatalf("create host dir: %v", err)
	}
	if err := os.Chmod(hostDir, 0o550); err != nil {
		t.Fatalf("make host dir read-only: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(hostDir, dirPerm) })

	err = s.Write(context.Background(), testChunk(testWindow(), 1))
	if err == nil {
		t.Fatal("write below a read-only parent succeeded")
	}
	if !errors.Is(err, store.ErrPersistent) {
		t.Fatalf("write error = %v, want a persistent store failure", err)
	}
	assertNoChunkOrTemp(t, root)
}

// TestWriteThreeCollisionsKeepsEveryChunk extends the two-write case: the base
// name, -1, and -2 all exist, each decodes, and the earlier chunks are not
// truncated by the later writes.
func TestWriteThreeCollisionsKeepsEveryChunk(t *testing.T) {
	s, root := newTestStore(t)
	base := testChunk(testWindow(), 1)
	path := chunkPath(t, root, base)
	dir := filepath.Dir(path)

	var firstBytes, secondBytes []byte
	for i, value := range []float64{1, 2, 3} {
		if err := s.Write(context.Background(), testChunk(testWindow(), value)); err != nil {
			t.Fatalf("write %d: %v", i+1, err)
		}
		if i == 0 {
			var err error
			if firstBytes, err = os.ReadFile(path); err != nil {
				t.Fatalf("read first chunk: %v", err)
			}
		}
		if i == 1 {
			var err error
			if secondBytes, err = os.ReadFile(chunk.CollisionKey(path, 1)); err != nil {
				t.Fatalf("read second chunk: %v", err)
			}
		}
	}

	want := map[string]float64{
		filepath.Base(path):                        1,
		filepath.Base(chunk.CollisionKey(path, 1)): 2,
		filepath.Base(chunk.CollisionKey(path, 2)): 3,
	}
	nameList := slices.Sorted(maps.Keys(want))
	if names := chunkFiles(t, dir); !slices.Equal(names, nameList) {
		t.Fatalf("chunk files = %v, want %v", names, nameList)
	}
	for name, value := range want {
		values := decodeValues(t, filepath.Join(dir, name))
		if !slices.Equal(values, []float64{value}) {
			t.Fatalf("%s values = %v, want [%v]", name, values, value)
		}
	}

	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("re-read first chunk: %v", err)
	}
	if !bytes.Equal(first, firstBytes) {
		t.Fatal("third write changed the base chunk")
	}
	second, err := os.ReadFile(chunk.CollisionKey(path, 1))
	if err != nil {
		t.Fatalf("re-read second chunk: %v", err)
	}
	if !bytes.Equal(second, secondBytes) {
		t.Fatal("third write changed the -1 chunk")
	}
	assertNoTempFiles(t, dir)
}
