package s3

import (
	"bytes"
	"context"
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
)

func newTestSpool(t *testing.T, maxBytes int64) (*spool, string) {
	t.Helper()
	dir := t.TempDir()
	return newSpool(dir, maxBytes, slog.New(slog.DiscardHandler)), dir
}

func chunkKeyAt(t *testing.T, host, scope string, start time.Time) string {
	t.Helper()
	key, err := chunk.Key(host, scope, start, start.Add(5*time.Minute))
	if err != nil {
		t.Fatalf("chunk key: %v", err)
	}
	return key
}

func TestSpoolWriteLayout(t *testing.T) {
	sp, dir := newTestSpool(t, 1<<20)
	start := time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)
	key := chunkKeyAt(t, "web-1", "system", start)
	body := []byte("encoded chunk")

	used, err := sp.write(context.Background(), key, body)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if used != key {
		t.Fatalf("used key = %q, want %q", used, key)
	}

	path := filepath.Join(dir, filepath.FromSlash(key))
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read spooled chunk: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("spooled bytes = %q, want %q", got, body)
	}

	entries, err := sp.entries(context.Background())
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	want := []spoolEntry{{key: key, size: int64(len(body))}}
	if !slices.Equal(entries, want) {
		t.Fatalf("entries = %+v, want %+v", entries, want)
	}

	assertNoTempFiles(t, dir)
}

func TestSpoolWriteCollisionPreservesBoth(t *testing.T) {
	sp, dir := newTestSpool(t, 1<<20)
	start := time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)
	key := chunkKeyAt(t, "web-1", "system", start)

	first, err := sp.write(context.Background(), key, []byte("first"))
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	second, err := sp.write(context.Background(), key, []byte("second"))
	if err != nil {
		t.Fatalf("second write: %v", err)
	}
	if first != key {
		t.Fatalf("first key = %q, want %q", first, key)
	}
	if want := chunk.CollisionKey(key, 1); second != want {
		t.Fatalf("second key = %q, want %q", second, want)
	}

	for key, want := range map[string]string{first: "first", second: "second"} {
		got, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(key)))
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}

	assertNoTempFiles(t, dir)
}

func TestSpoolCheckCreatesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "spool")
	sp := newSpool(dir, 1<<20, slog.New(slog.DiscardHandler))

	if err := sp.check(context.Background()); err != nil {
		t.Fatalf("check: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat spool: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("%s is not a directory", dir)
	}
	assertNoTempFiles(t, dir)
}

func TestSpoolEntriesIgnoreJunk(t *testing.T) {
	sp, dir := newTestSpool(t, 1<<20)
	start := time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)
	first := chunkKeyAt(t, "web-1", "system", start)
	second := chunkKeyAt(t, "web-1", "system", start.Add(5*time.Minute))

	for _, key := range []string{first, second} {
		if _, err := sp.write(context.Background(), key, []byte(key)); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
	}

	hourDir := filepath.Join(dir, "web-1", "system", "2026", "10", "24", "10")
	junk := []string{
		filepath.Join(dir, ".spool-crash"),
		filepath.Join(hourDir, ".spool-crash"),
		filepath.Join(hourDir, "notes.txt"),
		filepath.Join(hourDir, "not-a-chunk.jsonl.gz"),
	}
	for _, path := range junk {
		if err := os.WriteFile(path, []byte("junk"), 0o644); err != nil {
			t.Fatalf("write junk %s: %v", path, err)
		}
	}

	entries, err := sp.entries(context.Background())
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	got := make([]string, len(entries))
	for i, entry := range entries {
		got[i] = entry.key
	}
	want := []string{first, second}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("entries = %v, want %v", got, want)
	}
}

func TestSpoolEnforceCapDropsOldest(t *testing.T) {
	sp, _ := newTestSpool(t, 250)
	start := time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)
	body := bytes.Repeat([]byte("x"), 100)

	var keys []string
	for i := 0; i < 4; i++ {
		key := chunkKeyAt(t, "web-1", "system", start.Add(time.Duration(i)*5*time.Minute))
		keys = append(keys, key)
		used, err := sp.write(context.Background(), key, body)
		if err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
		sp.enforceCap(context.Background(), used)
	}

	entries, err := sp.entries(context.Background())
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	got := make([]string, len(entries))
	for i, entry := range entries {
		got[i] = entry.key
	}
	want := keys[2:]
	if !slices.Equal(got, want) {
		t.Fatalf("remaining spool entries = %v, want %v", got, want)
	}
}

func TestSpoolEnforceCapProtectsInFlight(t *testing.T) {
	sp, _ := newTestSpool(t, 250)
	start := time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)
	body := bytes.Repeat([]byte("x"), 100)

	var keys []string
	for i := 0; i < 3; i++ {
		key := chunkKeyAt(t, "web-1", "system", start.Add(time.Duration(i)*5*time.Minute))
		keys = append(keys, key)
		if _, err := sp.write(context.Background(), key, body); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
	}

	if !sp.acquire(keys[0]) {
		t.Fatal("acquire oldest: already in flight")
	}
	defer sp.release(keys[0])
	sp.enforceCap(context.Background(), keys[2])

	entries, err := sp.entries(context.Background())
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	got := make([]string, len(entries))
	for i, entry := range entries {
		got[i] = entry.key
	}
	want := []string{keys[0], keys[2]}
	if !slices.Equal(got, want) {
		t.Fatalf("remaining spool entries = %v, want %v", got, want)
	}
}

func TestSpoolListHour(t *testing.T) {
	sp, _ := newTestSpool(t, 1<<20)
	hour := time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)

	cases := []struct {
		host  string
		scope string
		start time.Time
		want  bool
	}{
		{"web-1", "system", hour, true},
		{"web-1", "system", hour.Add(time.Hour), false},
		{"web-1", "process", hour, false},
		{"web-2", "system", hour, false},
		{"web-1", "system", hour.Add(55 * time.Minute), true},
	}
	for _, tc := range cases {
		if _, err := sp.write(context.Background(), chunkKeyAt(t, tc.host, tc.scope, tc.start), []byte("chunk")); err != nil {
			t.Fatalf("write %s/%s: %v", tc.host, tc.scope, err)
		}
	}

	keys, err := sp.listHour("web-1", "system", hour, model.TimeRange{
		Start: hour,
		End:   hour.Add(56 * time.Minute),
	})
	if err != nil {
		t.Fatalf("listHour: %v", err)
	}
	got := map[string]bool{}
	for _, key := range keys {
		got[key] = true
	}
	for _, tc := range cases {
		if tc.host != "web-1" || tc.scope != "system" {
			continue
		}
		key := chunkKeyAt(t, tc.host, tc.scope, tc.start)
		if got[key] != tc.want {
			t.Errorf("listHour contains %q = %t, want %t", key, got[key], tc.want)
		}
		if tc.want {
			delete(got, key)
		}
	}
	if len(got) != 0 {
		t.Errorf("listHour returned unexpected keys: %v", got)
	}

	missing, err := sp.listHour("web-1", "system", hour.Add(5*time.Hour), model.TimeRange{})
	if err != nil {
		t.Fatalf("listHour missing dir: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("listHour missing dir = %v, want empty", missing)
	}
}

func TestSpoolListUnbounded(t *testing.T) {
	sp, _ := newTestSpool(t, 1<<20)
	start := time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)

	keys := []string{
		chunkKeyAt(t, "web-1", "system", start),
		chunkKeyAt(t, "web-1", "system", start.Add(time.Hour)),
		chunkKeyAt(t, "web-2", "system", start),
	}
	for _, key := range keys {
		if _, err := sp.write(context.Background(), key, []byte("chunk")); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
	}

	byHour, err := sp.list(context.Background(), model.TimeRange{}, "", "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(byHour) != 2 {
		t.Fatalf("hours = %d, want 2", len(byHour))
	}

	filtered, err := sp.list(context.Background(), model.TimeRange{}, "web-1", "system")
	if err != nil {
		t.Fatalf("list filtered: %v", err)
	}
	var got []string
	for _, group := range filtered {
		got = append(got, group...)
	}
	slices.Sort(got)
	want := slices.Clone(keys[:2])
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("filtered keys = %v, want %v", got, want)
	}
}

func TestSpoolPathRejectsEscape(t *testing.T) {
	sp, _ := newTestSpool(t, 1<<20)
	for _, key := range []string{"../outside", "/absolute", "host/../../outside", ".", ".."} {
		if _, err := sp.path(key); err == nil {
			t.Errorf("path(%q) succeeded, want an error", key)
		}
	}
}

func TestUnionRefs(t *testing.T) {
	got := unionRefs(
		[]string{"b", "a", "a"},
		[]string{"a", "c", "c"},
	)
	want := []chunkRef{
		{key: "a"},
		{key: "b"},
		{key: "c", local: true},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("unionRefs = %+v, want %+v", got, want)
	}
}

func assertNoTempFiles(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".spool-") || strings.HasPrefix(d.Name(), ".check-") {
			t.Errorf("left temp file behind: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}
