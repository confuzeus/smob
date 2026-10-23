//go:build integration

package integration

import (
	"context"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// processConfig is a process-only configuration with a short cadence for live
// /proc tests.
func processConfig(t *testing.T, dataDir, procPath, filter string, topN int, interval, rescan string) string {
	t.Helper()
	return writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
proc_path: %s
sources:
  system:
    enabled: false
  process:
    enabled: true
    filter: %q
    top_n: %d
    interval: %s
    rescan_interval: %s
storage:
  primary: disk
  chunk_interval: 1s
`, dataDir, procPath, filter, topN, interval, rescan))
}

// startSleep starts a real long-lived process and returns it. The process is
// killed and reaped when the test ends.
func startSleep(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("sleep", "300")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return cmd
}

// startBusy starts a real CPU-bound process and returns it. yes writes to the
// null device, so it burns a core without producing output. The process is
// killed and reaped when the test ends.
func startBusy(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("yes")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start yes: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return cmd
}

// stopProcess kills cmd and reaps it. Cleanup ignores the repeated Wait.
func stopProcess(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill sleep: %v", err)
	}
	_ = cmd.Wait()
}

// processSamples returns every recorded sample for one PID.
func processSamples(t *testing.T, dataDir string, pid int) []wireSample {
	t.Helper()
	want := strconv.Itoa(pid)
	var matches []wireSample
	for _, sample := range readSamples(t, dataDir) {
		if sample.Labels["pid"] == want {
			matches = append(matches, sample)
		}
	}
	return matches
}

// firstTimestamp returns the earliest timestamp in samples, which must not be
// empty.
func firstTimestamp(samples []wireSample) time.Time {
	first := samples[0].Timestamp
	for _, sample := range samples[1:] {
		if sample.Timestamp.Before(first) {
			first = sample.Timestamp
		}
	}
	return first
}

func TestProcessCollectOnce(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	helper := startSleep(t)
	pid := helper.Process.Pid
	cfg := processConfig(t, dataDir, "/proc", "^sleep$", 0, "100ms", "30s")

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
	if len(parts) != 7 || parts[0] != "test-host" || parts[1] != "process" {
		t.Fatalf("chunk path = %q, want <test-host>/process/YYYY/MM/DD/HH/*.jsonl.gz", rel)
	}

	pidLabel := strconv.Itoa(pid)
	wantLabels := map[string]string{
		"host":  "test-host",
		"scope": "process",
		"pid":   pidLabel,
		"comm":  "sleep",
	}
	var found *wireSample
	samples := readSamples(t, dataDir)
	for i := range samples {
		sample := &samples[i]
		if sample.Metric != "process.memory.rss_bytes" {
			t.Fatalf("metric = %q, want process.memory.rss_bytes", sample.Metric)
		}
		if sample.IntervalMS != 0 {
			t.Fatalf("gauge %s has interval_ms = %d, want 0", sample.Metric, sample.IntervalMS)
		}
		if sample.Timestamp.Location() != time.UTC {
			t.Fatalf("timestamp %s is not UTC", sample.Timestamp)
		}
		if sample.Labels["pid"] == pidLabel {
			if !maps.Equal(sample.Labels, wantLabels) {
				t.Fatalf("labels = %v, want %v", sample.Labels, wantLabels)
			}
			found = sample
		}
	}
	if found == nil {
		t.Fatalf("no sample for pid %d; got %+v", pid, samples)
	}
	if found.Value <= 0 {
		t.Fatalf("rss for pid %d = %v, want greater than zero", pid, found.Value)
	}

	queried := runSmob(t, ctx, "query", "--config", cfg, "--since", "1h",
		"--format", "json", "--label", "pid="+pidLabel)
	if queried.ExitCode != 0 {
		t.Fatalf("query exit = %d\nstderr:\n%s", queried.ExitCode, queried.Stderr)
	}
	out := decodeJSONSamples(t, queried.Stdout)
	if len(out) == 0 {
		t.Fatalf("query by pid returned no samples")
	}
	for _, sample := range out {
		if sample.Labels["pid"] != pidLabel {
			t.Fatalf("query by pid returned pid %q", sample.Labels["pid"])
		}
	}
}

func TestProcessFilterExcludes(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	startSleep(t)
	cfg := processConfig(t, dataDir, "/proc", "^smob-process-test-no-match$", 0, "100ms", "30s")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if got := countChunks(t, dataDir); got != 0 {
		t.Fatalf("chunks = %d, want none when the filter matches nothing", got)
	}
}

func TestProcessTopN(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	for range 4 {
		startSleep(t)
	}
	cfg := processConfig(t, dataDir, "/proc", "^sleep$", 2, "100ms", "30s")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	samples := readSamples(t, dataDir)
	if len(samples) != 2 {
		t.Fatalf("samples = %d, want top_n = 2", len(samples))
	}
	pids := make(map[string]bool, len(samples))
	for _, sample := range samples {
		if sample.Metric != "process.memory.rss_bytes" {
			t.Fatalf("metric = %q, want process.memory.rss_bytes", sample.Metric)
		}
		if sample.Value <= 0 {
			t.Fatalf("rss for pid %s = %v, want greater than zero", sample.Labels["pid"], sample.Value)
		}
		pids[sample.Labels["pid"]] = true
	}
	if len(pids) != 2 {
		t.Fatalf("distinct pids = %d, want 2", len(pids))
	}
}

func TestProcessCPUPercent(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	busy := startBusy(t)
	pid := busy.Process.Pid
	cfg := processConfig(t, dataDir, "/proc", "^yes$", 0, "100ms", "30s")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	d := startSmob(t, ctx, "collect", "--config", cfg)
	waitFor(t, 20*time.Second, "a CPU sample for the busy process", func() bool {
		for _, sample := range processSamples(t, dataDir, pid) {
			if sample.Metric == "process.cpu.percent" {
				return true
			}
		}
		return false
	})
	d.stop(t, 10*time.Second)

	var cpuSamples, rssSamples []wireSample
	for _, sample := range processSamples(t, dataDir, pid) {
		switch sample.Metric {
		case "process.cpu.percent":
			cpuSamples = append(cpuSamples, sample)
		case "process.memory.rss_bytes":
			rssSamples = append(rssSamples, sample)
		default:
			t.Fatalf("unexpected metric %q for pid %d", sample.Metric, pid)
		}
	}
	if len(cpuSamples) == 0 {
		t.Fatalf("no CPU samples for the busy process")
	}
	if len(rssSamples) == 0 {
		t.Fatalf("no RSS samples for the busy process")
	}

	wantLabels := map[string]string{
		"host":  "test-host",
		"scope": "process",
		"pid":   strconv.Itoa(pid),
		"comm":  "yes",
	}
	maxPercent := 100 * float64(runtime.NumCPU())
	for _, sample := range cpuSamples {
		if sample.Value <= 0 || sample.Value > maxPercent {
			t.Fatalf("cpu = %v, want greater than zero and at most %v", sample.Value, maxPercent)
		}
		if sample.IntervalMS <= 0 || sample.IntervalMS > 10_000 {
			t.Fatalf("interval_ms = %d, want a measured window", sample.IntervalMS)
		}
		if sample.Timestamp.Location() != time.UTC {
			t.Fatalf("timestamp %s is not UTC", sample.Timestamp)
		}
		if !maps.Equal(sample.Labels, wantLabels) {
			t.Fatalf("labels = %v, want %v", sample.Labels, wantLabels)
		}
	}

	// The first tick records RSS only: a CPU rate needs two snapshots.
	if !firstTimestamp(cpuSamples).After(firstTimestamp(rssSamples)) {
		t.Fatalf("first CPU sample at %s is not after the first RSS sample at %s",
			firstTimestamp(cpuSamples), firstTimestamp(rssSamples))
	}

	queried := runSmob(t, ctx, "query", "--config", cfg, "--since", "1h",
		"--format", "json", "--metric", "process.cpu.percent",
		"--label", "pid="+strconv.Itoa(pid))
	if queried.ExitCode != 0 {
		t.Fatalf("query exit = %d\nstderr:\n%s", queried.ExitCode, queried.Stderr)
	}
	out := decodeJSONSamples(t, queried.Stdout)
	if len(out) != len(cpuSamples) {
		t.Fatalf("query returned %d CPU samples, chunks hold %d", len(out), len(cpuSamples))
	}
	for _, sample := range out {
		if sample.Metric != "process.cpu.percent" {
			t.Fatalf("query returned metric %q", sample.Metric)
		}
		if sample.Labels["pid"] != strconv.Itoa(pid) {
			t.Fatalf("query by pid returned pid %q", sample.Labels["pid"])
		}
	}
}

func TestProcessRescanDiscoversNewPID(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	first := startSleep(t)
	cfg := processConfig(t, dataDir, "/proc", "^sleep$", 0, "100ms", "200ms")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	d := startSmob(t, ctx, "collect", "--config", cfg)

	waitFor(t, 15*time.Second, "the first process sample", func() bool {
		return len(processSamples(t, dataDir, first.Process.Pid)) > 0
	})

	second := startSleep(t)
	waitFor(t, 15*time.Second, "a sample for the process started after collection began", func() bool {
		return len(processSamples(t, dataDir, second.Process.Pid)) > 0
	})

	d.stop(t, 10*time.Second)
}

func TestProcessExitingPIDIsNormal(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	exiting := startSleep(t)
	surviving := startSleep(t)
	// A long rescan interval keeps the exited PID in the cached list, so the
	// next tick must tolerate ENOENT instead of failing.
	cfg := processConfig(t, dataDir, "/proc", "^sleep$", 0, "100ms", "30s")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	d := startSmob(t, ctx, "collect", "--config", cfg)

	waitFor(t, 15*time.Second, "samples for both processes", func() bool {
		return len(processSamples(t, dataDir, exiting.Process.Pid)) > 0 &&
			len(processSamples(t, dataDir, surviving.Process.Pid)) > 0
	})

	killedAt := time.Now().UTC()
	stopProcess(t, exiting)

	waitFor(t, 15*time.Second, "a sample for the surviving process after the other exited", func() bool {
		for _, sample := range processSamples(t, dataDir, surviving.Process.Pid) {
			if sample.Timestamp.After(killedAt) {
				return true
			}
		}
		return false
	})

	select {
	case <-d.done:
		t.Fatalf("collector exited after a process disappeared:\n%s", d.stderr.String())
	default:
	}
	d.stop(t, 10*time.Second)
}

func TestProcessProcPathMissing(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	missing := filepath.Join(t.TempDir(), "missing-proc")
	cfg := processConfig(t, dataDir, missing, "^sleep$", 0, "100ms", "30s")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 1 {
		t.Fatalf("exit = %d, want 1\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if !strings.Contains(res.Stderr, missing) {
		t.Fatalf("stderr does not name the proc path %q:\n%s", missing, res.Stderr)
	}
}
