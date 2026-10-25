package process

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// writePIDDirs creates numeric directory entries below root.
func writePIDDirs(t *testing.T, root string, pids ...int) {
	t.Helper()
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", root, err)
	}
	for _, pid := range pids {
		dir := filepath.Join(root, strconv.Itoa(pid))
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
}

// TestPIDCacheKeepsPreviousSetOnRescanFailure mirrors the Docker supervisor:
// a proc root that fails after startup keeps the known PIDs, warns once, and is
// retried on the next rescan instead of aborting the tick.
func TestPIDCacheKeepsPreviousSetOnRescanFailure(t *testing.T) {
	root := filepath.Join(t.TempDir(), "proc")
	writePIDDirs(t, root, 10, 20)

	handler := &recordingHandler{}
	cache := newPIDCache(root, time.Second, slog.New(handler))
	ctx := context.Background()
	now := time.Now()

	pids, refreshed, err := cache.list(ctx, now)
	if err != nil {
		t.Fatalf("first list: %v", err)
	}
	if !refreshed || len(pids) != 2 || pids[0] != 10 || pids[1] != 20 {
		t.Fatalf("first list = %v, refreshed %v, want [10 20] and a scan", pids, refreshed)
	}

	if err := os.RemoveAll(root); err != nil {
		t.Fatalf("remove proc root: %v", err)
	}

	pids, refreshed, err = cache.list(ctx, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("failing rescan returned an error: %v", err)
	}
	if refreshed {
		t.Fatal("failing rescan reported a refresh")
	}
	if len(pids) != 2 || pids[0] != 10 || pids[1] != 20 {
		t.Fatalf("failing rescan = %v, want the previous set [10 20]", pids)
	}
	if got := len(handler.warns()); got != 1 {
		t.Fatalf("warns = %d, want 1 after the first failing rescan", got)
	}

	// A second failure inside a new interval repeats at debug, not warn.
	pids, refreshed, err = cache.list(ctx, now.Add(4*time.Second))
	if err != nil || refreshed {
		t.Fatalf("second failing rescan = (%v, %v), want the cached set", refreshed, err)
	}
	if len(pids) != 2 {
		t.Fatalf("second failing rescan = %v, want the previous set", pids)
	}
	if got := len(handler.warns()); got != 1 {
		t.Fatalf("warns = %d, want the failure logged once", got)
	}

	// A later successful rescan replaces the set and re-arms the warning.
	writePIDDirs(t, root, 30, 40)
	pids, refreshed, err = cache.list(ctx, now.Add(6*time.Second))
	if err != nil {
		t.Fatalf("recovering rescan: %v", err)
	}
	if !refreshed || len(pids) != 2 || pids[0] != 30 || pids[1] != 40 {
		t.Fatalf("recovering rescan = %v, refreshed %v, want [30 40] and a scan", pids, refreshed)
	}
}

// TestPIDCacheInitialScanFailure returns the failure: there is no previous set
// to keep, and the caller must report the source as unusable for that tick.
func TestPIDCacheInitialScanFailure(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	handler := &recordingHandler{}
	cache := newPIDCache(root, time.Second, slog.New(handler))
	ctx := context.Background()
	now := time.Now()

	pids, refreshed, err := cache.list(ctx, now)
	if err == nil {
		t.Fatal("first list on a missing root returned no error")
	}
	if pids != nil || refreshed {
		t.Fatalf("first list = (%v, %v), want (nil, false)", pids, refreshed)
	}

	// The root becoming readable recovers without a cache from a failed scan.
	writePIDDirs(t, root, 50)
	pids, refreshed, err = cache.list(ctx, now.Add(time.Second))
	if err != nil {
		t.Fatalf("recovering list: %v", err)
	}
	if !refreshed || len(pids) != 1 || pids[0] != 50 {
		t.Fatalf("recovering list = %v, refreshed %v, want [50] and a scan", pids, refreshed)
	}
}
