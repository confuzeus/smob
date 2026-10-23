//go:build integration

package integration

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

type result struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

func runSmob(t *testing.T, ctx context.Context, args ...string) result {
	t.Helper()
	cmd := exec.CommandContext(ctx, smobBin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	err := cmd.Run()
	res := result{Stdout: stdout.String(), Stderr: stderr.String()}
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("run smob %v: %v", args, err)
		}
		res.ExitCode = exitErr.ExitCode()
	}
	return res
}

// daemon is a running smob process.
type daemon struct {
	cmd     *exec.Cmd
	stderr  bytes.Buffer
	done    chan struct{}
	waitErr error
}

func startSmob(t *testing.T, ctx context.Context, args ...string) *daemon {
	t.Helper()
	cmd := exec.CommandContext(ctx, smobBin, args...)
	cmd.WaitDelay = 5 * time.Second
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }

	d := &daemon{cmd: cmd, done: make(chan struct{})}
	cmd.Stderr = &d.stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start smob: %v", err)
	}
	go func() {
		d.waitErr = cmd.Wait()
		close(d.done)
	}()

	t.Cleanup(func() {
		select {
		case <-d.done:
		default:
			_ = cmd.Process.Kill()
			<-d.done
		}
		if t.Failed() {
			t.Logf("smob stderr:\n%s", d.stderr.String())
		}
	})
	return d
}

func (d *daemon) stop(t *testing.T, timeout time.Duration) {
	t.Helper()
	if err := d.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signal smob: %v", err)
	}
	select {
	case <-d.done:
		if d.waitErr != nil {
			t.Fatalf("smob did not exit cleanly: %v\n%s", d.waitErr, d.stderr.String())
		}
	case <-time.After(timeout):
		t.Fatalf("smob did not exit within %s\n%s", timeout, d.stderr.String())
	}
}

// waitFor polls an observable condition. It is the only permitted sleep.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// wireSample mirrors the documented sample schema. The duplication is
// intentional: a schema change must break the contract test.
type wireSample struct {
	Timestamp  time.Time         `json:"timestamp"`
	Metric     string            `json:"metric"`
	Value      float64           `json:"value"`
	IntervalMS int64             `json:"interval_ms,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
}

// writeChunk seeds one chunk in the documented layout.
func writeChunk(t *testing.T, dataDir, host, scope string, start, end time.Time, samples []wireSample) {
	t.Helper()
	start, end = start.UTC().Truncate(time.Second), end.UTC().Truncate(time.Second)
	dir := filepath.Join(dataDir, host, scope,
		start.Format("2006"), start.Format("01"), start.Format("02"), start.Format("15"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create chunk dir: %v", err)
	}

	name := start.Format("20060102T150405Z") + "-" + end.Format("20060102T150405Z") + ".jsonl.gz"
	f, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("create chunk: %v", err)
	}
	gz := gzip.NewWriter(f)
	enc := json.NewEncoder(gz)
	for _, sample := range samples {
		if err := enc.Encode(sample); err != nil {
			t.Fatalf("encode chunk sample: %v", err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("close chunk gzip: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close chunk: %v", err)
	}
}

// chunkPaths returns every completed chunk below dataDir.
func chunkPaths(t *testing.T, dataDir string) []string {
	t.Helper()
	var paths []string
	err := filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() && strings.HasSuffix(path, ".jsonl.gz") {
			paths = append(paths, path)
		}
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("walk data dir: %v", err)
	}
	return paths
}

func countChunks(t *testing.T, dataDir string) int {
	t.Helper()
	return len(chunkPaths(t, dataDir))
}

func countTmpFiles(t *testing.T, dataDir string) int {
	t.Helper()
	var count int
	err := filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() && strings.HasSuffix(path, ".tmp") {
			count++
		}
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatalf("walk data dir: %v", err)
	}
	return count
}

// readSamples decodes every chunk below dataDir in path order.
func readSamples(t *testing.T, dataDir string) []wireSample {
	t.Helper()
	var samples []wireSample
	for _, path := range chunkPaths(t, dataDir) {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		gz, err := gzip.NewReader(f)
		if err != nil {
			t.Fatalf("open gzip %s: %v", path, err)
		}
		dec := json.NewDecoder(gz)
		for {
			var sample wireSample
			err := dec.Decode(&sample)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("decode %s: %v", path, err)
			}
			samples = append(samples, sample)
		}
		_ = gz.Close()
		_ = f.Close()
	}
	return samples
}

func decodeJSONSamples(t *testing.T, out string) []wireSample {
	t.Helper()
	var samples []wireSample
	if err := json.Unmarshal([]byte(out), &samples); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, out)
	}
	return samples
}

func decodeJSONLSamples(t *testing.T, out string) []wireSample {
	t.Helper()
	var samples []wireSample
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		var sample wireSample
		if err := json.Unmarshal([]byte(line), &sample); err != nil {
			t.Fatalf("decode jsonl line: %v\n%s", err, line)
		}
		samples = append(samples, sample)
	}
	return samples
}

func decodeCSVSamples(t *testing.T, out string) []wireSample {
	t.Helper()
	rows, err := csv.NewReader(strings.NewReader(out)).ReadAll()
	if err != nil {
		t.Fatalf("decode csv output: %v\n%s", err, out)
	}
	want := []string{"timestamp", "metric", "value", "interval_ms", "labels"}
	if len(rows) == 0 || !slices.Equal(rows[0], want) {
		t.Fatalf("csv header = %v, want %v", rows, want)
	}

	var samples []wireSample
	for _, row := range rows[1:] {
		if len(row) != len(want) {
			t.Fatalf("csv row = %v, want %d columns", row, len(want))
		}
		timestamp, err := time.Parse(time.RFC3339Nano, row[0])
		if err != nil {
			t.Fatalf("csv timestamp %q: %v", row[0], err)
		}
		value, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			t.Fatalf("csv value %q: %v", row[2], err)
		}
		interval, err := strconv.ParseInt(row[3], 10, 64)
		if err != nil {
			t.Fatalf("csv interval %q: %v", row[3], err)
		}
		samples = append(samples, wireSample{
			Timestamp:  timestamp,
			Metric:     row[1],
			Value:      value,
			IntervalMS: interval,
			Labels:     parseLabels(row[4]),
		})
	}
	return samples
}

func parseLabels(text string) map[string]string {
	if text == "" {
		return nil
	}
	labels := make(map[string]string)
	for _, part := range strings.Split(text, ",") {
		name, value, ok := strings.Cut(part, "=")
		if ok {
			labels[name] = value
		}
	}
	return labels
}

func tableRows(out string) int {
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return 0
	}
	return len(strings.Split(trimmed, "\n")) - 1
}

func assertSameSamples(t *testing.T, got, want []wireSample) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d samples, want %d", len(got), len(want))
	}
	for i := range got {
		if !got[i].Timestamp.Equal(want[i].Timestamp) ||
			got[i].Metric != want[i].Metric ||
			got[i].Value != want[i].Value ||
			got[i].IntervalMS != want[i].IntervalMS ||
			!maps.Equal(got[i].Labels, want[i].Labels) {
			t.Fatalf("sample %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// writeConfig writes body to a fresh smob.yaml and returns its path.
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "smob.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// validConfig writes the smallest configuration that passes validation and
// returns its path.
func validConfig(t *testing.T) string {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	return writeConfig(t, "data_dir: "+dataDir+"\n")
}

// collectConfig is a fast system-only configuration for live /proc tests.
func collectConfig(t *testing.T, dataDir, procPath string) string {
	t.Helper()
	return writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
proc_path: %s
sources:
  system:
    enabled: true
    interval: 100ms
storage:
  primary: disk
  chunk_interval: 1s
`, dataDir, procPath))
}

func hasLine(out, line string) bool {
	for _, got := range bytes.Split([]byte(out), []byte("\n")) {
		if string(bytes.TrimSpace(got)) == line {
			return true
		}
	}
	return false
}

func updateGolden() bool {
	return os.Getenv("SMOB_TEST_UPDATE") == "1"
}

func compareGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join(moduleRoot(), "test", "integration", "testdata", "golden", name)

	if updateGolden() {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create golden dir: %v", err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden %s: %v", name, err)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run just test-update)", name, err)
	}
	if got != string(want) {
		t.Errorf("%s does not match its golden file; run just test-update and review the diff\ngot:\n%s", name, got)
	}
}
