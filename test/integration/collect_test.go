//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
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
	values := make(map[string]float64, len(gauges))
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
		if !isFinite(sample.Value) {
			t.Fatalf("gauge %s = %v, want a finite value", sample.Metric, sample.Value)
		}
		values[sample.Metric] = sample.Value
	}

	// Memory gauges come from one /proc/meminfo read, so their relationship is
	// exact: total is positive, available never exceeds it, and used is the
	// remainder. A zeroed, swapped, or misderived value breaks one of these.
	for metric := range gauges {
		if _, ok := values[metric]; !ok {
			t.Fatalf("--once did not emit %s; got %v", metric, values)
		}
	}
	total := values["system.memory.total_bytes"]
	available := values["system.memory.available_bytes"]
	used := values["system.memory.used_bytes"]
	if total <= 0 {
		t.Fatalf("system.memory.total_bytes = %v, want > 0", total)
	}
	if available > total {
		t.Fatalf("system.memory.available_bytes = %v exceeds total %v", available, total)
	}
	if used < 0 {
		t.Fatalf("system.memory.used_bytes = %v, want >= 0", used)
	}
	// One byte of slack absorbs float64 rounding without hiding a swapped or
	// zeroed value.
	if diff := used - (total - available); diff < -1 || diff > 1 {
		t.Fatalf("system.memory.used_bytes = %v, want total-available = %v (diff %v)",
			used, total-available, diff)
	}
	if got := countTmpFiles(t, dataDir); got != 0 {
		t.Fatalf("found %d temporary files after a completed write", got)
	}
}

// isFinite reports whether v is neither NaN nor an infinity.
func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

// assertSeriesLabels asserts that a sample carries exactly want in addition to
// the host and scope labels every system sample has. An unexpected label is a
// failure, so a renamed or duplicated dimension cannot slip through.
func assertSeriesLabels(t *testing.T, sample wireSample, want ...string) {
	t.Helper()
	labels := make(map[string]bool, len(sample.Labels))
	for name := range sample.Labels {
		labels[name] = true
	}
	for _, name := range append([]string{"host", "scope"}, want...) {
		if !labels[name] {
			t.Errorf("%s sample is missing the %s label: %v", sample.Metric, name, sample.Labels)
		}
		delete(labels, name)
	}
	for name := range labels {
		t.Errorf("%s sample has unexpected label %s=%q", sample.Metric, name, sample.Labels[name])
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
	waitFor(t, 15*time.Second, "the first closed chunk", func() bool {
		return countChunks(t, dataDir) >= 1
	})
	// Snapshot the closed chunks so the shutdown flush is observable: a chunk
	// that appears after this point was written by the signal handler.
	before := chunkPaths(t, dataDir)
	d.stop(t, 10*time.Second)
	added := newPaths(before, chunkPaths(t, dataDir))
	if len(added) == 0 {
		t.Fatalf("SIGINT flushed no chunk for the active window (before=%d)", len(before))
	}

	samples := readSamples(t, dataDir)
	if len(samples) < 2 {
		t.Fatalf("got %d samples after a daemon run", len(samples))
	}

	var haveRate, haveGauge bool
	var haveCPUAll bool
	interfaces := make(map[string]bool)
	var last time.Time
	for _, sample := range samples {
		if sample.Labels["host"] != "test-host" || sample.Labels["scope"] != "system" {
			t.Fatalf("sample labels = %v, want host=test-host scope=system", sample.Labels)
		}
		if sample.Timestamp.Before(last) {
			t.Fatalf("timestamp %s is before %s in chunk path order", sample.Timestamp, last)
		}
		last = sample.Timestamp
		if !isFinite(sample.Value) {
			t.Fatalf("%s = %v, want a finite value", sample.Metric, sample.Value)
		}
		if sample.IntervalMS > 0 {
			haveRate = true
		} else {
			haveGauge = true
		}

		switch sample.Metric {
		case "system.cpu.percent":
			if sample.IntervalMS <= 0 {
				t.Fatalf("cpu rate has interval_ms = %d, want > 0", sample.IntervalMS)
			}
			assertSeriesLabels(t, sample, "cpu")
			cpu := sample.Labels["cpu"]
			if cpu == "" {
				t.Fatalf("cpu sample has no cpu label: %v", sample.Labels)
			}
			if cpu != "all" {
				if n, err := strconv.Atoi(cpu); err != nil || n < 0 {
					t.Fatalf("cpu label = %q, want \"all\" or a non-negative index", cpu)
				}
			}
			if sample.Value < 0 || sample.Value > 100 {
				t.Fatalf("system.cpu.percent cpu=%s = %v, want 0 <= value <= 100", cpu, sample.Value)
			}
			if cpu == "all" {
				haveCPUAll = true
			}
		case "system.network.rx_bytes_per_second", "system.network.tx_bytes_per_second":
			if sample.IntervalMS <= 0 {
				t.Fatalf("network rate has interval_ms = %d, want > 0", sample.IntervalMS)
			}
			assertSeriesLabels(t, sample, "interface")
			iface := sample.Labels["interface"]
			if iface == "" {
				t.Fatalf("network sample has no interface label: %v", sample.Labels)
			}
			if sample.Value < 0 {
				t.Fatalf("%s interface=%s = %v, want >= 0", sample.Metric, iface, sample.Value)
			}
			interfaces[iface] = true
		case "system.memory.total_bytes", "system.memory.available_bytes", "system.memory.used_bytes":
			if sample.IntervalMS != 0 {
				t.Fatalf("gauge %s has interval_ms = %d, want 0", sample.Metric, sample.IntervalMS)
			}
			if sample.Value < 0 {
				t.Fatalf("gauge %s = %v, want >= 0", sample.Metric, sample.Value)
			}
		default:
			t.Fatalf("daemon emitted unexpected system metric %s", sample.Metric)
		}
	}
	if !haveRate {
		t.Fatal("daemon run emitted no rate samples")
	}
	if !haveGauge {
		t.Fatal("daemon run emitted no gauge samples")
	}
	if !haveCPUAll {
		t.Fatal("daemon run emitted no system.cpu.percent sample with cpu=all")
	}
	if len(interfaces) == 0 {
		t.Fatal("daemon run emitted no labelled network interface series")
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

// TestCollectSIGTERMFlushesActiveChunk proves SIGTERM follows the same shutdown
// contract as SIGINT: the collector writes the chunk it is still assembling and
// exits 0.
func TestCollectSIGTERMFlushesActiveChunk(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := collectConfig(t, dataDir, "/proc")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	d := startSmob(t, ctx, "collect", "--config", cfg)
	waitFor(t, 15*time.Second, "the first closed chunk", func() bool {
		return countChunks(t, dataDir) >= 1
	})
	before := chunkPaths(t, dataDir)
	d.stopTerm(t, 10*time.Second)

	added := newPaths(before, chunkPaths(t, dataDir))
	if len(added) == 0 {
		t.Fatalf("SIGTERM flushed no chunk for the active window (before=%d)", len(before))
	}
	for _, path := range added {
		if got := len(readChunkSamples(t, path)); got == 0 {
			t.Fatalf("chunk written after SIGTERM has no samples: %s", path)
		}
	}
	if got := countTmpFiles(t, dataDir); got != 0 {
		t.Fatalf("found %d temporary files after a SIGTERM shutdown", got)
	}
}

func TestCollectDataDirOverride(t *testing.T) {
	ignored := filepath.Join(t.TempDir(), "ignored")
	if err := os.MkdirAll(ignored, 0o755); err != nil {
		t.Fatalf("create empty data dir: %v", err)
	}
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

// decodeLogLines decodes every non-empty stderr line as one JSON log record. A
// line that does not parse is a test failure: --log-format json promises a
// stream that a log consumer can parse line by line.
func decodeLogLines(t *testing.T, stderr string) []map[string]any {
	t.Helper()
	var entries []map[string]any
	for _, line := range strings.Split(stderr, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("stderr line is not JSON: %v\nline: %s\nstderr:\n%s", err, line, stderr)
		}
		entries = append(entries, entry)
	}
	return entries
}

func TestCollectStartupLogDisk(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := collectConfig(t, dataDir, "/proc")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg, "--log-format", "json")
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	entry := findLogEntry(t, res.Stderr, "collector ready")
	if got := entry["key_root"]; got != dataDir {
		t.Errorf("key_root = %v, want %q", got, dataDir)
	}
	if got := entry["storage"]; got != "disk" {
		t.Errorf("storage = %v, want disk", got)
	}
	if got := entry["system_interval"]; got != "100ms" {
		t.Errorf("system_interval = %v, want 100ms", got)
	}
	retention, ok := entry["retention"].(map[string]any)
	if !ok {
		t.Fatalf("retention = %v, want an object", entry["retention"])
	}
	for _, key := range []string{"max_age", "max_bytes"} {
		if _, ok := retention[key]; !ok {
			t.Errorf("retention has no %s: %v", key, retention)
		}
	}
}

func TestCollectStartupLogS3(t *testing.T) {
	env := requireS3(t)

	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
proc_path: /proc
storage:
  primary: s3
  s3:
    endpoint: %s
    bucket: %s
    prefix: %s
`, dataDir, env.endpoint, env.bucket, env.prefix))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--dry-run", "--config", cfg, "--log-format", "json")
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	entry := findLogEntry(t, res.Stderr, "collector ready")
	wantRoot := "s3://" + env.bucket + "/" + env.prefix
	if got := entry["key_root"]; got != wantRoot {
		t.Errorf("key_root = %v, want %q", got, wantRoot)
	}
	if got := entry["storage"]; got != "s3" {
		t.Errorf("storage = %v, want s3", got)
	}
	spool, ok := entry["spool"].(map[string]any)
	if !ok {
		t.Fatalf("spool = %v, want an object", entry["spool"])
	}
	if got := spool["dir"]; got != filepath.Join(dataDir, "spool") {
		t.Errorf("spool.dir = %v, want %q", got, filepath.Join(dataDir, "spool"))
	}
}

func TestCollectDryRunProbeLog(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
proc_path: /proc
sources:
  system:
    enabled: true
    interval: 100ms
  process:
    enabled: true
    interval: 200ms
storage:
  primary: disk
  chunk_interval: 1s
retention:
  disk:
    max_age: 1h
`, dataDir))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--dry-run", "--config", cfg, "--log-format", "json")
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if res.Stdout != "" {
		t.Errorf("dry-run wrote to stdout: %q", res.Stdout)
	}

	entries := decodeLogLines(t, res.Stderr)
	probed := make(map[string]bool)
	for _, entry := range entries {
		if entry["msg"] == "dry-run: source probe passed" {
			scope, _ := entry["scope"].(string)
			probed[scope] = true
		}
	}
	for _, scope := range []string{"system", "process"} {
		if !probed[scope] {
			t.Errorf("no dry-run probe line for the %s source\nstderr:\n%s", scope, res.Stderr)
		}
	}
	if len(probed) != 2 {
		t.Errorf("dry-run probed %v, want exactly system and process", probed)
	}

	store := findLogEntry(t, res.Stderr, "dry-run: store check passed")
	if got := store["key_root"]; got != dataDir {
		t.Errorf("store check key_root = %v, want %q", got, dataDir)
	}
	retention := findLogEntry(t, res.Stderr, "dry-run: retention skipped")
	if got := retention["max_age"]; got != "1h0m0s" {
		t.Errorf("skipped retention max_age = %v, want 1h0m0s", got)
	}
	free := findLogEntry(t, res.Stderr, "dry-run: free space checked")
	if bytes, ok := free["free_bytes"].(float64); !ok || bytes <= 0 {
		t.Errorf("free_bytes = %v, want a positive number", free["free_bytes"])
	}
}

func TestCollectOnceChunkLog(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := collectConfig(t, dataDir, "/proc")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg, "--log-format", "json")
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

	entry := findLogEntry(t, res.Stderr, "chunk written")
	if got, want := entry["key"], filepath.ToSlash(rel); got != want {
		t.Errorf("chunk key = %v, want %q", got, want)
	}
	if samples, ok := entry["samples"].(float64); !ok || samples < 1 {
		t.Errorf("chunk samples = %v, want at least 1", entry["samples"])
	}
}

func TestLogJSONTimestampsUTC(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := collectConfig(t, dataDir, "/proc")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg, "--log-format", "json", "--log-level", "debug")
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	entries := decodeLogLines(t, res.Stderr)
	if len(entries) == 0 {
		t.Fatal("no log lines")
	}
	for _, entry := range entries {
		stamp, ok := entry["time"].(string)
		if !ok {
			t.Fatalf("log record has no time string: %v", entry)
		}
		if !strings.HasSuffix(stamp, "Z") {
			t.Errorf("timestamp %q does not end in Z", stamp)
		}
	}
}

func TestLogJSONWithS3(t *testing.T) {
	env := requireS3(t)

	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
proc_path: /proc
sources:
  system:
    enabled: true
    interval: 100ms
storage:
  primary: s3
  chunk_interval: 1s
  s3:
    endpoint: %s
    bucket: %s
    prefix: %s
`, dataDir, env.endpoint, env.bucket, env.prefix))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg, "--log-format", "json", "--log-level", "debug")
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	for _, entry := range decodeLogLines(t, res.Stderr) {
		stamp, ok := entry["time"].(string)
		if !ok || !strings.HasSuffix(stamp, "Z") {
			t.Errorf("log record time = %v, want a UTC string ending in Z", entry["time"])
		}
	}
	findLogEntry(t, res.Stderr, "chunk written")
}

// blockHostDir makes every subsequent chunk write below dataDir fail. The host
// directory is moved aside and a regular file is left at its path, so the
// MkdirAll for the next chunk fails with ENOTDIR. This works for root as well,
// so the test needs no privilege skip; a 0555 parent would be bypassed by root.
//
// The swap takes two steps. A window closes between the rename and the write
// only rarely, and then the collector re-creates the directory and the write
// fails with EEXIST; move the new directory aside and try again. Each caller
// invokes this right after a chunk appeared, while the collector is idle until
// the next window closes, so one attempt normally suffices.
func blockHostDir(t *testing.T, dataDir string) {
	t.Helper()
	hostDir := filepath.Join(dataDir, "test-host")
	for attempt := 0; attempt < 4; attempt++ {
		moved := fmt.Sprintf("%s.blocked-%d", hostDir, attempt)
		if err := os.Rename(hostDir, moved); err != nil && !os.IsNotExist(err) {
			t.Fatalf("move %s aside: %v", hostDir, err)
		}
		err := os.WriteFile(hostDir, nil, 0o644)
		if err == nil {
			return
		}
		if !os.IsExist(err) {
			t.Fatalf("block %s: %v", hostDir, err)
		}
	}
	t.Fatalf("could not block %s: the collector kept re-creating it", hostDir)
}

// TestCollectWriteFailureExits3 covers a store failure after collection has
// started: startup Check passed and at least one chunk landed, then the next
// write fails. A persistent store failure must stop the collector on its own,
// name the path, and exit 3 instead of dropping samples and continuing.
//
// ENOSPC cannot be simulated portably without a loopback mount or a quota, so
// no test arranges it. A full filesystem fails inside the same disk write() call
// and is wrapped in the same store.ErrPersistent, so this write-failure test
// covers the exit-3 mapping for it.
func TestCollectWriteFailureExits3(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := collectConfig(t, dataDir, "/proc")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	d := startSmob(t, ctx, "collect", "--config", cfg)
	waitFor(t, 15*time.Second, "the first chunk", func() bool {
		return countChunks(t, dataDir) >= 1
	})
	blockHostDir(t, dataDir)

	// No signal is sent: the write failure alone must end the process, which is
	// what "does not silently continue" means here.
	if code := d.waitExit(t, 20*time.Second); code != 3 {
		t.Fatalf("exit = %d, want 3 after a failed chunk write\n%s", code, d.stderr.String())
	}
	want := filepath.Join(dataDir, "test-host")
	if !strings.Contains(d.stderr.String(), want) {
		t.Fatalf("stderr does not name the failed path %s:\n%s", want, d.stderr.String())
	}
}

// TestCollectOnceWriteFailureExits3 covers the --once write failure startup
// Check cannot see: the data directory is usable, but the host path below it is
// a regular file, so the chunk mkdir fails during the run. The failure exits 3,
// names the path, and leaves no temp file behind.
func TestCollectOnceWriteFailureExits3(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}
	hostPath := filepath.Join(dataDir, "test-host")
	if err := os.WriteFile(hostPath, nil, 0o644); err != nil {
		t.Fatalf("block host path: %v", err)
	}
	cfg := collectConfig(t, dataDir, "/proc")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 3 {
		t.Fatalf("exit = %d, want 3\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if !strings.Contains(res.Stderr, hostPath) {
		t.Fatalf("stderr does not name %s:\n%s", hostPath, res.Stderr)
	}
	if got := countChunks(t, dataDir); got != 0 {
		t.Fatalf("failed --once left %d chunks", got)
	}
	if got := countTmpFiles(t, dataDir); got != 0 {
		t.Fatalf("failed --once left %d temporary files", got)
	}
}

// TestCollectFlushFailureDoesNotMaskWriteFailure induces a write failure and a
// shutdown flush failure in the same run. With a system and a process source,
// the first scope whose window closes reports the write error and cancels the
// run; the other scope still holds an active chunk, which the shutdown flush
// writes and fails as well. Run returns errors.Join(runErr, flushErr), so the
// persistent classification survives both branches and the exit code stays 3.
func TestCollectFlushFailureDoesNotMaskWriteFailure(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
proc_path: /proc
sources:
  system:
    enabled: true
    interval: 100ms
  process:
    enabled: true
    filter: ".*"
    top_n: 5
    interval: 200ms
    rescan_interval: 1s
storage:
  primary: disk
  chunk_interval: 1s
`, dataDir))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	d := startSmob(t, ctx, "collect", "--config", cfg)
	waitFor(t, 15*time.Second, "the first chunk", func() bool {
		return countChunks(t, dataDir) >= 1
	})
	blockHostDir(t, dataDir)

	if code := d.waitExit(t, 20*time.Second); code != 3 {
		t.Fatalf("exit = %d, want 3 with both a write and a flush failure\n%s", code, d.stderr.String())
	}
	stderr := d.stderr.String()
	for _, want := range []string{"write system chunk", "write process chunk"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr is missing the %q failure:\n%s", want, stderr)
		}
	}
	if path := filepath.Join(dataDir, "test-host"); !strings.Contains(stderr, path) {
		t.Fatalf("stderr does not name the failed path %s:\n%s", path, stderr)
	}
}
