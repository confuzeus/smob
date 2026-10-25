//go:build integration

// The spool's on-disk layout, cap enforcement, and listing are observable
// through the CLI with a failing endpoint and a spool directory, so they run
// with the extended suite like the store parity and Docker tests instead of
// `just test-unit`.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/confuzeus/smob/internal/config"
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
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat spooled chunk: %v", err)
	}
	if got, want := info.Mode().Perm(), fs.FileMode(0o640); got != want {
		t.Fatalf("spooled chunk mode = %o, want %o", got, want)
	}
	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("stat spool directory: %v", err)
	}
	if got, want := dirInfo.Mode().Perm(), fs.FileMode(0o750); got != want {
		t.Fatalf("spool directory mode = %o, want %o", got, want)
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

// TestSpoolEnforceCapKeepsOversizedChunk proves the just-written entry is
// protected even when it alone exceeds the cap: its only copy stays on disk and
// the over-cap condition is logged at error instead of silently dropped.
func TestSpoolEnforceCapKeepsOversizedChunk(t *testing.T) {
	var logs bytes.Buffer
	sp := newSpool(t.TempDir(), 50, slog.New(slog.NewTextHandler(&logs, nil)))
	start := time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)
	key := chunkKeyAt(t, "web-1", "system", start)

	used, err := sp.write(context.Background(), key, bytes.Repeat([]byte("x"), 100))
	if err != nil {
		t.Fatalf("write oversized chunk: %v", err)
	}
	sp.enforceCap(context.Background(), used)

	path, err := sp.path(key)
	if err != nil {
		t.Fatalf("spool path: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("oversized chunk was dropped: %v", err)
	}
	if want := "spool remains over spool_max_bytes after dropping unprotected chunks"; !strings.Contains(logs.String(), want) {
		t.Fatalf("over-cap condition was not logged:\n%s", logs.String())
	}
	if dropped := "dropped oldest spooled chunk over spool_max_bytes"; strings.Contains(logs.String(), dropped) {
		t.Fatalf("oversized chunk was reported as dropped:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("over-cap condition was not logged at error:\n%s", logs.String())
	}
}

// TestSpoolEnforceCapSparesInFlightAndLogsDrop proves eviction skips a key the
// replayer owns while still dropping the unprotected middle entry at error.
func TestSpoolEnforceCapSparesInFlightAndLogsDrop(t *testing.T) {
	var logs bytes.Buffer
	sp := newSpool(t.TempDir(), 150, slog.New(slog.NewTextHandler(&logs, nil)))
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
	// Protect the newest as the write path would, so the middle entry is the
	// only one eligible for eviction and the spool stays over the cap.
	sp.enforceCap(context.Background(), keys[2])

	entries, err := sp.entries(context.Background())
	if err != nil {
		t.Fatalf("entries: %v", err)
	}
	got := make([]string, len(entries))
	for i, entry := range entries {
		got[i] = entry.key
	}
	if want := []string{keys[0], keys[2]}; !slices.Equal(got, want) {
		t.Fatalf("remaining spool entries = %v, want %v", got, want)
	}

	drops := strings.Count(logs.String(), "dropped oldest spooled chunk over spool_max_bytes")
	if drops != 1 {
		t.Fatalf("drop logged %d times, want once:\n%s", drops, logs.String())
	}
	if !strings.Contains(logs.String(), "key="+keys[1]) {
		t.Fatalf("drop log does not name %s:\n%s", keys[1], logs.String())
	}
	if !strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("drop was not logged at error:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), "spool remains over spool_max_bytes") {
		t.Fatalf("spool above the cap with an in-flight entry was not reported:\n%s", logs.String())
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

// assertDelayIn checks equal jitter: at least half the fixed delay and never
// more than the full fixed delay.
func assertDelayIn(t *testing.T, got, fixed time.Duration) {
	t.Helper()
	if got < fixed/2 || got > fixed {
		t.Errorf("replay delay = %s, want within [%s, %s]", got, fixed/2, fixed)
	}
}

func TestNextReplayDelayJitter(t *testing.T) {
	var backoff time.Duration
	first := nextReplayDelay(&backoff)
	if backoff != replayBackoffBase {
		t.Fatalf("fixed backoff = %s, want the base %s", backoff, replayBackoffBase)
	}
	assertDelayIn(t, first, replayBackoffBase)

	// Equal jitter varies the delay while the fixed backoff stays deterministic.
	seen := make(map[time.Duration]struct{})
	for range 50 {
		var b time.Duration
		delay := nextReplayDelay(&b)
		assertDelayIn(t, delay, replayBackoffBase)
		seen[delay] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatal("replay delay never varied; equal jitter is missing")
	}

	// The fixed backoff still doubles to the cap, and every delay stays bounded.
	for {
		delay := nextReplayDelay(&backoff)
		if backoff > replayBackoffMax {
			t.Fatalf("fixed backoff = %s, want at most %s", backoff, replayBackoffMax)
		}
		assertDelayIn(t, delay, backoff)
		if backoff == replayBackoffMax {
			break
		}
	}
}

func TestSpoolVerifyRejectsCorruptChunks(t *testing.T) {
	sp, _ := newTestSpool(t, 1<<20)
	valid := testChunkBody(t)
	rotted := slices.Clone(valid)
	rotted[len(rotted)/2] ^= 0xff

	cases := []struct {
		name string
		body []byte
	}{
		{"truncated gzip trailer", valid[:len(valid)-8]},
		{"truncated gzip length", valid[:len(valid)-4]},
		{"truncated payload", valid[:len(valid)/2]},
		{"bit rot", rotted},
		{"not gzip", []byte("not a chunk")},
		{"empty", nil},
	}
	start := time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)
	for i, tc := range cases {
		key := chunkKeyAt(t, "web-1", "system", start.Add(time.Duration(i)*5*time.Minute))
		if _, err := sp.write(context.Background(), key, tc.body); err != nil {
			t.Fatalf("write %s: %v", tc.name, err)
		}
		if err := sp.verify(key); err == nil {
			t.Errorf("verify(%s) succeeded, want a decoding error", tc.name)
		}
	}

	key := chunkKeyAt(t, "web-1", "system", start.Add(time.Hour))
	if _, err := sp.write(context.Background(), key, valid); err != nil {
		t.Fatalf("write valid chunk: %v", err)
	}
	if err := sp.verify(key); err != nil {
		t.Fatalf("verify valid chunk: %v", err)
	}
}

func TestReplayBlocksCorruptChunk(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	dir := t.TempDir()
	s := &Store{spool: newSpool(dir, 1<<20, logger), logger: logger}

	valid := testChunkBody(t)
	key, err := s.spool.write(context.Background(),
		chunkKeyAt(t, "web-1", "system", time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)),
		valid[:len(valid)-8])
	if err != nil {
		t.Fatalf("write truncated chunk: %v", err)
	}

	blocked := make(map[string]struct{})
	failed, progressed, err := s.replayPass(context.Background(), blocked)
	if err != nil {
		t.Fatalf("replay pass: %v", err)
	}
	if failed {
		t.Fatal("corrupt chunk was treated as a transient upload failure")
	}
	if !progressed {
		t.Fatal("corrupt chunk did not count as progress")
	}

	// The key is blocked, so a later pass neither retries nor logs it again.
	failed, _, err = s.replayPass(context.Background(), blocked)
	if err != nil {
		t.Fatalf("second replay pass: %v", err)
	}
	if failed {
		t.Fatal("blocked chunk was treated as a transient upload failure")
	}

	if _, ok := blocked[key]; !ok {
		t.Fatalf("corrupt key %q was not blocked", key)
	}
	if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(key))); err != nil {
		t.Fatalf("corrupt chunk left the spool: %v", err)
	}
	if n := strings.Count(logs.String(), "leaving it for an operator"); n != 1 {
		t.Fatalf("verification failure logged %d times, want once", n)
	}
}

func TestReplayEntryAcceptsValidChunk(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	s := &Store{spool: newSpool(t.TempDir(), 1<<20, logger), logger: logger}
	key, err := s.spool.write(context.Background(),
		chunkKeyAt(t, "web-1", "system", time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)),
		testChunkBody(t))
	if err != nil {
		t.Fatalf("write valid chunk: %v", err)
	}
	path, err := s.spool.path(key)
	if err != nil {
		t.Fatalf("spool path: %v", err)
	}

	// A canceled context stops at the upload step without touching the network,
	// so the error proves the valid file passed verification and stayed on disk.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	replayed, err := s.replayEntry(ctx, key)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("replayEntry error = %v, want context.Canceled", err)
	}
	if errors.Is(err, errSpoolCorrupt) {
		t.Fatalf("valid chunk was rejected as corrupt: %v", err)
	}
	if replayed {
		t.Fatal("replayEntry reported an upload that never happened")
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("valid chunk left the spool: %v", statErr)
	}
}

// TestReplayConflictLeavesFileAndLogsOnce proves a spooled chunk whose exact
// key already holds different bytes is a permanent conflict: replay never
// suffixes to create a -1 duplicate, keeps the local file for an operator, and
// reports the conflict once rather than on every pass.
func TestReplayConflictLeavesFileAndLogsOnce(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	s := newS3TestStore(t, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	start := time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC)
	key := chunkKeyAt(t, "web-1", "system", start)
	target := s.objectKey(key)
	existing := []byte("a different chunk already owns this window")
	if _, err := s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(target),
		Body:          bytes.NewReader(existing),
		ContentLength: aws.Int64(int64(len(existing))),
	}); err != nil {
		t.Fatalf("seed conflicting object: %v", err)
	}
	if _, err := s.spool.write(ctx, key, testChunkBody(t)); err != nil {
		t.Fatalf("write spooled chunk: %v", err)
	}

	blocked := make(map[string]struct{})
	failed, progressed, err := s.replayPass(ctx, blocked)
	if err != nil {
		t.Fatalf("first replay pass: %v", err)
	}
	if failed {
		t.Fatal("permanent conflict was treated as a transient upload failure")
	}
	if !progressed {
		t.Fatal("conflict did not count as progress")
	}

	// The key stays blocked, so the next pass neither retries nor logs it.
	failed, progressed, err = s.replayPass(ctx, blocked)
	if err != nil {
		t.Fatalf("second replay pass: %v", err)
	}
	if failed {
		t.Fatal("blocked conflict was treated as a transient upload failure")
	}
	if progressed {
		t.Fatal("blocked conflict counted as progress on the second pass")
	}
	if _, ok := blocked[key]; !ok {
		t.Fatalf("conflicting key %q was not blocked", key)
	}

	if got := strings.Count(logs.String(), "conflicts with an existing object; leaving it for an operator"); got != 1 {
		t.Fatalf("conflict logged %d times, want once:\n%s", got, logs.String())
	}
	if !strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("conflict was not logged at error:\n%s", logs.String())
	}

	spoolPath, err := s.spool.path(key)
	if err != nil {
		t.Fatalf("spool path: %v", err)
	}
	if _, err := os.Stat(spoolPath); err != nil {
		t.Fatalf("conflicting chunk left the spool: %v", err)
	}
	got, found, err := s.getObject(ctx, target)
	if err != nil {
		t.Fatalf("read conflicting object: %v", err)
	}
	if !found || !bytes.Equal(got, existing) {
		t.Fatalf("existing object = %q (found %t), want %q", got, found, existing)
	}
	if _, found, err := s.getObject(ctx, s.objectKey(chunk.CollisionKey(key, 1))); err != nil {
		t.Fatalf("read collision suffix: %v", err)
	} else if found {
		t.Fatalf("replay created a duplicate object at %s", chunk.CollisionKey(key, 1))
	}
}

// testChunkBody returns one valid chunk encoded as the store writes it.
func testChunkBody(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	err := chunk.Encode(&buf, []model.Sample{{
		Timestamp: time.Date(2026, 10, 24, 10, 0, 0, 0, time.UTC),
		Metric:    "test.spool.bytes",
		Value:     7,
		Labels:    map[string]string{"host": "web-1", "scope": "system"},
	}})
	if err != nil {
		t.Fatalf("encode test chunk: %v", err)
	}
	return buf.Bytes()
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

// newS3TestStore returns a Store over a unique prefix of the real S3 endpoint.
// It skips with the missing variables when the service is absent, and fails
// under strict mode so the extended suite cannot pass without it.
func newS3TestStore(t *testing.T, logger *slog.Logger) *Store {
	t.Helper()

	endpoint := os.Getenv("SMOB_TEST_S3_ENDPOINT")
	bucket := os.Getenv("SMOB_TEST_S3_BUCKET")
	var missing []string
	if endpoint == "" {
		missing = append(missing, "SMOB_TEST_S3_ENDPOINT")
	}
	if bucket == "" {
		missing = append(missing, "SMOB_TEST_S3_BUCKET")
	}
	for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		reason := fmt.Sprintf("S3 test requires %s; set them and run just test-all", strings.Join(missing, ", "))
		if os.Getenv("SMOB_TEST_STRICT") == "1" {
			t.Fatal(reason)
		}
		t.Skip(reason)
	}
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	pathStyle := true
	prefix := "smob-it/s3-pkg-" +
		strings.ToLower(strings.NewReplacer("/", "-", " ", "-").Replace(t.Name())) +
		fmt.Sprintf("-%d", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s, err := New(ctx, config.S3Storage{
		Region:        "us-east-1",
		Bucket:        bucket,
		Endpoint:      endpoint,
		Prefix:        prefix,
		PathStyle:     &pathStyle,
		SpoolDir:      t.TempDir(),
		SpoolMaxBytes: 1 << 20,
	}, logger)
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	ensureTestBucket(t, ctx, s.client, bucket)
	t.Cleanup(func() { deleteTestPrefix(t, s.client, bucket, prefix) })
	return s
}

func ensureTestBucket(t *testing.T, ctx context.Context, client *awss3.Client, bucket string) {
	t.Helper()

	if _, err := client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(bucket)}); err == nil {
		return
	}
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		if _, headErr := client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(bucket)}); headErr != nil {
			t.Fatalf("ensure S3 bucket %s: create: %v; head: %v", bucket, err, headErr)
		}
	}
}

func deleteTestPrefix(t *testing.T, client *awss3.Client, bucket, prefix string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	paginator := awss3.NewListObjectsV2Paginator(client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix + "/"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Errorf("cleanup list %s: %v", prefix, err)
			return
		}
		if len(page.Contents) == 0 {
			continue
		}
		objects := make([]types.ObjectIdentifier, 0, len(page.Contents))
		for _, object := range page.Contents {
			objects = append(objects, types.ObjectIdentifier{Key: object.Key})
		}
		if _, err := client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		}); err != nil {
			t.Errorf("cleanup delete under %s: %v", prefix, err)
		}
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
