//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const retentionHost = "retention-host"

// seededChunk is one real chunk file arranged in the documented layout.
type seededChunk struct {
	path string
	key  string
	size int64
}

// seedRetentionChunk writes a valid chunk at an arbitrary window and returns
// its path, relative key, and on-disk size. Seeded times are real UTC windows;
// nothing is mocked.
func seedRetentionChunk(t *testing.T, dataDir string, start, end time.Time) seededChunk {
	t.Helper()

	start = start.UTC().Truncate(time.Second)
	end = end.UTC().Truncate(time.Second)
	writeChunk(t, dataDir, retentionHost, "system", start, end, []wireSample{{
		Timestamp: start.Add(time.Second),
		Metric:    "system.memory.used_bytes",
		Value:     1,
		Labels:    map[string]string{"host": retentionHost, "scope": "system"},
	}})

	path := filepath.Join(dataDir, retentionHost, "system",
		start.Format("2006"), start.Format("01"), start.Format("02"), start.Format("15"),
		start.Format("20060102T150405Z")+"-"+end.Format("20060102T150405Z")+".jsonl.gz")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat seeded chunk %s: %v", path, err)
	}
	rel, err := filepath.Rel(dataDir, path)
	if err != nil {
		t.Fatalf("relative seeded chunk path: %v", err)
	}
	return seededChunk{path: path, key: filepath.ToSlash(rel), size: info.Size()}
}

// retentionConfig writes a fast system-only config with the supplied retention
// block appended.
func retentionConfig(t *testing.T, dataDir, retention string) string {
	t.Helper()
	return writeConfig(t, fmt.Sprintf(`data_dir: %s
host: %s
log_level: info
log_format: json
sources:
  system:
    enabled: true
    interval: 100ms
storage:
  primary: disk
  chunk_interval: 1s
%s`, dataDir, retentionHost, retention))
}

func chunkExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	switch {
	case err == nil:
		return true
	case os.IsNotExist(err):
		return false
	default:
		t.Fatalf("stat %s: %v", path, err)
		return false
	}
}

// deletionLog is the subset of the structured log needed to assert retention.
type deletionLog struct {
	Msg    string `json:"msg"`
	Key    string `json:"key"`
	Bytes  int64  `json:"bytes"`
	Reason string `json:"reason"`
}

func assertDeletionLogged(t *testing.T, stderr, key string, size int64, reason string) {
	t.Helper()

	for _, line := range strings.Split(stderr, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry deletionLog
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if entry.Msg == "deleted chunk" && entry.Key == key && entry.Bytes == size && entry.Reason == reason {
			return
		}
	}
	t.Fatalf("no info deletion log for key %q, bytes %d, reason %q\nstderr:\n%s", key, size, reason, stderr)
}

func hasLogMessage(stderr, msg string) bool {
	for _, line := range strings.Split(stderr, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry deletionLog
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if entry.Msg == msg {
			return true
		}
	}
	return false
}

func TestRetentionAge(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	now := time.Now().UTC().Truncate(time.Second)

	old := seedRetentionChunk(t, dataDir, now.Add(-2*time.Hour), now.Add(-119*time.Minute))
	fresh := seedRetentionChunk(t, dataDir, now.Add(-2*time.Minute), now.Add(-time.Minute))

	cfg := retentionConfig(t, dataDir, "retention:\n  disk:\n    max_age: 1h\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	if chunkExists(t, old.path) {
		t.Fatalf("old chunk %s survived max_age", old.key)
	}
	if !chunkExists(t, fresh.path) {
		t.Fatalf("fresh chunk %s was deleted by max_age", fresh.key)
	}
	if got := countChunks(t, dataDir); got < 2 {
		t.Fatalf("chunks = %d, want the fresh seed and the --once chunk", got)
	}
	assertDeletionLogged(t, res.Stderr, old.key, old.size, "age")
}

func TestRetentionSize(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	now := time.Now().UTC().Truncate(time.Second)

	oldest := seedRetentionChunk(t, dataDir, now.Add(-40*time.Minute), now.Add(-39*time.Minute))
	middle := seedRetentionChunk(t, dataDir, now.Add(-30*time.Minute), now.Add(-29*time.Minute))
	newest := seedRetentionChunk(t, dataDir, now.Add(-20*time.Minute), now.Add(-19*time.Minute))

	// The cap fits the two newest chunks, so only the oldest is removed.
	maxBytes := middle.size + newest.size
	cfg := retentionConfig(t, dataDir, fmt.Sprintf("retention:\n  disk:\n    max_bytes: %d\n", maxBytes))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	if chunkExists(t, oldest.path) {
		t.Fatalf("oldest chunk %s survived a max_bytes overrun", oldest.key)
	}
	if !chunkExists(t, middle.path) || !chunkExists(t, newest.path) {
		t.Fatal("size retention deleted a chunk that fits the cap")
	}
	assertDeletionLogged(t, res.Stderr, oldest.key, oldest.size, "size")
}

func TestRetentionAgeThenSize(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	now := time.Now().UTC().Truncate(time.Second)

	aged := seedRetentionChunk(t, dataDir, now.Add(-2*time.Hour), now.Add(-119*time.Minute))
	oldest := seedRetentionChunk(t, dataDir, now.Add(-40*time.Minute), now.Add(-39*time.Minute))
	middle := seedRetentionChunk(t, dataDir, now.Add(-30*time.Minute), now.Add(-29*time.Minute))
	newest := seedRetentionChunk(t, dataDir, now.Add(-20*time.Minute), now.Add(-19*time.Minute))

	maxBytes := middle.size + newest.size
	cfg := retentionConfig(t, dataDir, fmt.Sprintf("retention:\n  disk:\n    max_age: 1h\n    max_bytes: %d\n", maxBytes))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	if chunkExists(t, aged.path) {
		t.Fatalf("age limit left %s behind", aged.key)
	}
	if chunkExists(t, oldest.path) {
		t.Fatalf("size limit left %s behind after the age pass", oldest.key)
	}
	if !chunkExists(t, middle.path) || !chunkExists(t, newest.path) {
		t.Fatal("combined retention deleted a chunk that fits both limits")
	}
	assertDeletionLogged(t, res.Stderr, aged.key, aged.size, "age")
	assertDeletionLogged(t, res.Stderr, oldest.key, oldest.size, "size")
}

func TestRetentionDisabled(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	now := time.Now().UTC().Truncate(time.Second)

	old := seedRetentionChunk(t, dataDir, now.Add(-48*time.Hour), now.Add(-47*time.Hour))
	older := seedRetentionChunk(t, dataDir, now.Add(-72*time.Hour), now.Add(-71*time.Hour))

	cfg := retentionConfig(t, dataDir, "retention:\n  disk:\n    max_age: 0s\n    max_bytes: 0\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	if !chunkExists(t, old.path) || !chunkExists(t, older.path) {
		t.Fatal("disabled retention deleted a seeded chunk")
	}
	if hasLogMessage(res.Stderr, "deleted chunk") {
		t.Fatalf("disabled retention logged a deletion\nstderr:\n%s", res.Stderr)
	}
}

func TestRetentionDryRunDeletesNothing(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	now := time.Now().UTC().Truncate(time.Second)
	old := seedRetentionChunk(t, dataDir, now.Add(-2*time.Hour), now.Add(-119*time.Minute))

	cfg := retentionConfig(t, dataDir, "retention:\n  disk:\n    max_age: 1s\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--dry-run", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if !chunkExists(t, old.path) {
		t.Fatalf("dry run deleted %s", old.key)
	}
	if hasLogMessage(res.Stderr, "deleted chunk") {
		t.Fatalf("dry run logged a deletion\nstderr:\n%s", res.Stderr)
	}
}

func TestRetentionMissingDataDir(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "absent")
	cfg := retentionConfig(t, dataDir, "retention:\n  disk:\n    max_age: 1s\n")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if got := countChunks(t, dataDir); got < 1 {
		t.Fatalf("chunks = %d, want the --once chunk in a freshly created data dir", got)
	}
}

func TestRetentionIgnoresNonChunks(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	now := time.Now().UTC().Truncate(time.Second)

	valid := seedRetentionChunk(t, dataDir, now.Add(-2*time.Hour), now.Add(-119*time.Minute))

	tmp := valid.path + ".tmp"
	if err := os.WriteFile(tmp, []byte("partial"), 0o644); err != nil {
		t.Fatalf("write tmp file: %v", err)
	}
	notes := filepath.Join(dataDir, "notes.txt")
	if err := os.WriteFile(notes, []byte("keep"), 0o644); err != nil {
		t.Fatalf("write notes file: %v", err)
	}

	malformedDir := filepath.Join(dataDir, retentionHost, "system", "2026", "01", "01", "00")
	if err := os.MkdirAll(malformedDir, 0o755); err != nil {
		t.Fatalf("create malformed dir: %v", err)
	}
	malformed := filepath.Join(malformedDir, "bogus.jsonl.gz")
	if err := os.WriteFile(malformed, []byte("not a chunk"), 0o644); err != nil {
		t.Fatalf("write malformed chunk: %v", err)
	}

	spoolDir := filepath.Join(dataDir, "spool", retentionHost, "system", "2026", "01", "01", "00")
	if err := os.MkdirAll(spoolDir, 0o755); err != nil {
		t.Fatalf("create spool dir: %v", err)
	}
	spooled := filepath.Join(spoolDir, "20260101T000000Z-20260101T000500Z.jsonl.gz")
	if err := os.WriteFile(spooled, []byte("spooled"), 0o644); err != nil {
		t.Fatalf("write spooled chunk: %v", err)
	}

	cfg := retentionConfig(t, dataDir, "retention:\n  disk:\n    max_age: 1s\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	if chunkExists(t, valid.path) {
		t.Fatalf("valid old chunk %s survived", valid.key)
	}
	for _, path := range []string{tmp, notes, malformed, spooled} {
		if !chunkExists(t, path) {
			t.Fatalf("retention deleted a non-chunk file %s", path)
		}
	}
}

func TestRetentionProtectsFutureEndedChunk(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	now := time.Now().UTC().Truncate(time.Second)

	older := seedRetentionChunk(t, dataDir, now.Add(-2*time.Hour), now.Add(-119*time.Minute))
	// A chunk whose window ends after the sweep starts represents the partial
	// chunk flushed at the same instant. It must survive that pass.
	future := seedRetentionChunk(t, dataDir, now.Add(time.Minute), now.Add(2*time.Minute))

	cfg := retentionConfig(t, dataDir, "retention:\n  disk:\n    max_bytes: 1\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	if chunkExists(t, older.path) {
		t.Fatalf("size retention left the older chunk %s behind", older.key)
	}
	if !chunkExists(t, future.path) {
		t.Fatalf("size retention deleted a future-ended chunk %s", future.key)
	}
	if !hasLogMessage(res.Stderr, "retention size limit remains exceeded") {
		t.Fatalf("no warning that the size cap could not be met\nstderr:\n%s", res.Stderr)
	}
}

func TestRetentionDaemonShutdown(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	now := time.Now().UTC().Truncate(time.Second)
	old := seedRetentionChunk(t, dataDir, now.Add(-2*time.Hour), now.Add(-119*time.Minute))

	cfg := retentionConfig(t, dataDir, "retention:\n  disk:\n    max_age: 1h\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	d := startSmob(t, ctx, "collect", "--config", cfg)
	waitFor(t, 15*time.Second, "startup retention to delete the old chunk", func() bool {
		return !chunkExists(t, old.path)
	})
	d.stop(t, 10*time.Second)
}
