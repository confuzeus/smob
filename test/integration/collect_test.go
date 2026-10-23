//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCollectOnce(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := collectConfig(t, dataDir, "/proc")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	paths := chunkPaths(t, dataDir)
	if len(paths) != 1 {
		t.Fatalf("chunks = %v, want one", paths)
	}
	rel, err := filepath.Rel(dataDir, paths[0])
	if err != nil {
		t.Fatalf("relative chunk path: %v", err)
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) != 7 {
		t.Fatalf("chunk path = %q, want 7 components", rel)
	}
	if parts[0] != "test-host" || parts[1] != "system" || !strings.HasSuffix(parts[6], ".jsonl.gz") {
		t.Fatalf("chunk path = %q, want <test-host>/system/YYYY/MM/DD/HH/*.jsonl.gz", rel)
	}

	samples := readSamples(t, dataDir)
	if len(samples) < 3 {
		t.Fatalf("got %d samples, want at least the three memory gauges", len(samples))
	}
	gauges := map[string]bool{
		"system.memory.total_bytes":     true,
		"system.memory.available_bytes": true,
		"system.memory.used_bytes":      true,
	}
	for _, sample := range samples {
		if sample.Labels["host"] != "test-host" || sample.Labels["scope"] != "system" {
			t.Fatalf("sample labels = %v, want host=test-host scope=system", sample.Labels)
		}
		if !gauges[sample.Metric] {
			t.Fatalf("--once emitted rate metric %s; the first snapshot has no previous values", sample.Metric)
		}
		if sample.IntervalMS != 0 {
			t.Fatalf("gauge %s has interval_ms = %d, want 0", sample.Metric, sample.IntervalMS)
		}
		if sample.Timestamp.Location() != time.UTC {
			t.Fatalf("timestamp %s is not UTC", sample.Timestamp)
		}
	}
	if got := countTmpFiles(t, dataDir); got != 0 {
		t.Fatalf("found %d temporary files after a completed write", got)
	}
}

func TestCollectThenQuery(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := collectConfig(t, dataDir, "/proc")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if res := runSmob(t, ctx, "collect", "--once", "--config", cfg); res.ExitCode != 0 {
		t.Fatalf("collect exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	res := runSmob(t, ctx, "query", "--config", cfg, "--since", "1h", "--format", "json")
	if res.ExitCode != 0 {
		t.Fatalf("query exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	samples := decodeJSONSamples(t, res.Stdout)
	if len(samples) < 3 {
		t.Fatalf("query returned %d samples, want at least 3", len(samples))
	}

	found := false
	for _, sample := range samples {
		if sample.Metric == "system.memory.total_bytes" {
			found = true
		}
	}
	if !found {
		t.Fatalf("query did not return system.memory.total_bytes:\n%s", res.Stdout)
	}
}

func TestCollectDaemon(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := collectConfig(t, dataDir, "/proc")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	d := startSmob(t, ctx, "collect", "--config", cfg)
	waitFor(t, 15*time.Second, "the first chunk", func() bool {
		return countChunks(t, dataDir) >= 1
	})
	d.stop(t, 10*time.Second)

	samples := readSamples(t, dataDir)
	if len(samples) < 2 {
		t.Fatalf("got %d samples after a daemon run", len(samples))
	}

	var haveRate, haveGauge bool
	var last time.Time
	for _, sample := range samples {
		if sample.Labels["host"] != "test-host" || sample.Labels["scope"] != "system" {
			t.Fatalf("sample labels = %v, want host=test-host scope=system", sample.Labels)
		}
		if sample.Timestamp.Before(last) {
			t.Fatalf("timestamp %s is before %s in chunk path order", sample.Timestamp, last)
		}
		last = sample.Timestamp
		if sample.IntervalMS > 0 {
			haveRate = true
		} else {
			haveGauge = true
		}
	}
	if !haveRate {
		t.Fatal("daemon run emitted no rate samples")
	}
	if !haveGauge {
		t.Fatal("daemon run emitted no gauge samples")
	}
	if got := countTmpFiles(t, dataDir); got != 0 {
		t.Fatalf("found %d temporary files after a clean shutdown", got)
	}

	res := runSmob(t, ctx, "query", "--config", cfg, "--since", "1h", "--format", "json")
	if res.ExitCode != 0 {
		t.Fatalf("query exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if got := len(decodeJSONSamples(t, res.Stdout)); got < 2 {
		t.Fatalf("query returned %d samples after a daemon run", got)
	}
}

func TestCollectDataDirOverride(t *testing.T) {
	ignored := filepath.Join(t.TempDir(), "ignored")
	cfg := collectConfig(t, ignored, "/proc")
	override := filepath.Join(t.TempDir(), "override")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg, "--data-dir", override)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if got := countChunks(t, override); got != 1 {
		t.Fatalf("override data dir has %d chunks, want 1", got)
	}
	if got := countChunks(t, ignored); got != 0 {
		t.Fatalf("configured data dir has %d chunks despite the override", got)
	}

	res = runSmob(t, ctx, "query", "--config", cfg, "--data-dir", override, "--since", "1h", "--format", "json")
	if res.ExitCode != 0 {
		t.Fatalf("query exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if got := len(decodeJSONSamples(t, res.Stdout)); got == 0 {
		t.Fatal("query with --data-dir returned no samples")
	}

	res = runSmob(t, ctx, "query", "--config", cfg, "--since", "1h", "--format", "json")
	if res.ExitCode != 0 {
		t.Fatalf("query exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if res.Stdout != "[]\n" {
		t.Fatalf("query without the override = %q, want an empty result", res.Stdout)
	}
}

func TestCollectDryRun(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := collectConfig(t, dataDir, "/proc")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--dry-run", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if got := countChunks(t, dataDir); got != 0 {
		t.Fatalf("dry run wrote %d chunks", got)
	}
}

func TestCollectUnusableStore(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(dataDir, nil, 0o644); err != nil {
		t.Fatalf("create blocking file: %v", err)
	}
	cfg := collectConfig(t, dataDir, "/proc")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for _, args := range [][]string{
		{"collect", "--dry-run", "--config", cfg},
		{"collect", "--once", "--config", cfg},
	} {
		res := runSmob(t, ctx, args...)
		if res.ExitCode != 3 {
			t.Fatalf("%v exit = %d, want 3\nstderr:\n%s", args, res.ExitCode, res.Stderr)
		}
		if !strings.Contains(res.Stderr, dataDir) {
			t.Fatalf("%v stderr does not name the data dir:\n%s", args, res.Stderr)
		}
	}
}

func TestCollectMissingProc(t *testing.T) {
	procPath := t.TempDir()
	cfg := collectConfig(t, filepath.Join(t.TempDir(), "data"), procPath)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 1 {
		t.Fatalf("exit = %d, want 1\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if !strings.Contains(res.Stderr, procPath) {
		t.Fatalf("stderr does not name the proc path:\n%s", res.Stderr)
	}
}

func TestCollectFlagsExclusive(t *testing.T) {
	cfg := collectConfig(t, filepath.Join(t.TempDir(), "data"), "/proc")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--dry-run", "--once", "--config", cfg)
	if res.ExitCode != 2 {
		t.Fatalf("exit = %d, want 2\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "mutually exclusive") {
		t.Fatalf("stderr does not explain the conflict:\n%s", res.Stderr)
	}
}
