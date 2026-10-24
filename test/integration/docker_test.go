//go:build integration

package integration

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// dockerTestImage is pinned so a cached image does not change between runs.
const dockerTestImage = "busybox:1.36"

// TestDockerDisabledDoesNotTouchSocket proves that the default disabled source
// is never constructed: a bogus socket path must not affect a system-only run.
func TestDockerDisabledDoesNotTouchSocket(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	socket := filepath.Join(t.TempDir(), "missing.sock")
	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
sources:
  system:
    enabled: true
    interval: 100ms
  docker:
    enabled: false
    socket: %s
storage:
  primary: disk
  chunk_interval: 1s
`, dataDir, socket))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d, want 0; a disabled Docker source must not be probed\nstderr:\n%s",
			res.ExitCode, res.Stderr)
	}

	samples := readSamples(t, dataDir)
	if len(samples) == 0 {
		t.Fatal("collect --once wrote no system samples")
	}
	for _, sample := range samples {
		if sample.Labels["scope"] == "container" {
			t.Fatalf("disabled Docker source emitted a container sample: %+v", sample)
		}
	}
}

// TestDockerMissingSocketFails checks every collect mode against an enabled
// source whose socket does not exist. The daemon probe is a runtime failure
// (exit 1), names the socket, and happens before any chunk is written.
func TestDockerMissingSocketFails(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "missing.sock")

	cases := []struct {
		name string
		args []string
	}{
		{"daemon", []string{"collect"}},
		{"once", []string{"collect", "--once"}},
		{"dry run", []string{"collect", "--dry-run"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := filepath.Join(t.TempDir(), "data")
			cfg := dockerConfig(t, dataDir, socket, false)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			args := append(append([]string{}, tc.args...), "--config", cfg)
			res := runSmob(t, ctx, args...)
			if res.ExitCode != 1 {
				t.Fatalf("exit = %d, want 1 (runtime failure)\nstderr:\n%s", res.ExitCode, res.Stderr)
			}
			if !strings.Contains(res.Stderr, socket) {
				t.Fatalf("stderr does not name the socket %q:\n%s", socket, res.Stderr)
			}
			if got := countChunks(t, dataDir); got != 0 {
				t.Fatalf("wrote %d chunks despite an unreachable Docker socket", got)
			}
		})
	}
}

// TestDockerCollectOnceThenQuery runs one snapshot against a real container.
// A single observation emits only the memory gauge: there is no previous CPU or
// network counter to diff, so the query must contain no rate samples.
func TestDockerCollectOnceThenQuery(t *testing.T) {
	socket := requireDockerCLI(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	name := startDockerContainer(t, ctx, socket)

	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := dockerConfig(t, dataDir, socket, false)

	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("collect --once exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	paths := chunkPaths(t, dataDir)
	if len(paths) != 1 {
		t.Fatalf("chunks = %v, want one container chunk", paths)
	}
	rel, err := filepath.Rel(dataDir, paths[0])
	if err != nil {
		t.Fatalf("relative chunk path: %v", err)
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) != 7 || parts[0] != "test-host" || parts[1] != "container" {
		t.Fatalf("chunk path = %q, want <test-host>/container/YYYY/MM/DD/HH/*.jsonl.gz", rel)
	}

	samples := samplesForContainer(readSamples(t, dataDir), name)
	if len(samples) != 1 {
		t.Fatalf("first snapshot samples for %s = %d, want one memory gauge: %+v",
			name, len(samples), samples)
	}
	if got := samples[0].Metric; got != "container.memory.used_bytes" {
		t.Fatalf("first snapshot metric = %q, want container.memory.used_bytes", got)
	}
	if samples[0].IntervalMS != 0 {
		t.Fatalf("memory gauge interval_ms = %d, want 0", samples[0].IntervalMS)
	}
	if samples[0].Value <= 0 {
		t.Fatalf("memory gauge = %v, want greater than zero", samples[0].Value)
	}
	assertContainerLabels(t, samples[0], name)

	query := runSmob(t, ctx, "query", "--config", cfg,
		"--since", "1h", "--scope", "container",
		"--label", "container="+name, "--format", "json")
	if query.ExitCode != 0 {
		t.Fatalf("query exit = %d\nstderr:\n%s", query.ExitCode, query.Stderr)
	}
	queried := decodeJSONSamples(t, query.Stdout)
	if len(queried) != 1 {
		t.Fatalf("query returned %d samples for %s, want the memory gauge only: %+v",
			len(queried), name, queried)
	}
	if got := queried[0].Metric; got != "container.memory.used_bytes" {
		t.Fatalf("queried metric = %q, want container.memory.used_bytes", got)
	}
	if queried[0].IntervalMS != 0 {
		t.Fatalf("queried memory gauge interval_ms = %d, want 0", queried[0].IntervalMS)
	}
	assertContainerLabels(t, queried[0], name)
}

// TestDockerCollectDaemon runs the collector across several ticks and stops it
// with SIGINT. It asserts the rates and interval_ms that a single snapshot
// cannot produce, and that shutdown joins the source goroutines.
func TestDockerCollectDaemon(t *testing.T) {
	socket := requireDockerCLI(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	name := startDockerContainer(t, ctx, socket)

	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := dockerConfig(t, dataDir, socket, false)

	d := startSmob(t, ctx, "collect", "--config", cfg)

	// A CPU rate needs two observations in one closed chunk. Waiting for the
	// sample itself (not just any chunk) avoids stopping after a window that
	// happened to contain only the first memory gauge.
	waitFor(t, 30*time.Second, "a container CPU rate in a closed chunk", func() bool {
		for _, sample := range samplesForContainer(readSamples(t, dataDir), name) {
			if sample.Metric == "container.cpu.percent" {
				return true
			}
		}
		return false
	})
	d.stop(t, 15*time.Second)

	samples := samplesForContainer(readSamples(t, dataDir), name)
	var memory, cpu, network int
	for _, sample := range samples {
		assertContainerLabels(t, sample, name)
		switch sample.Metric {
		case "container.memory.used_bytes":
			memory++
			if sample.IntervalMS != 0 {
				t.Fatalf("memory gauge interval_ms = %d, want 0", sample.IntervalMS)
			}
			if sample.Value <= 0 {
				t.Fatalf("memory gauge = %v, want greater than zero", sample.Value)
			}
		case "container.cpu.percent":
			cpu++
			assertRate(t, sample)
		case "container.network.rx_bytes_per_second", "container.network.tx_bytes_per_second":
			network++
			assertRate(t, sample)
			if sample.Labels["interface"] == "" {
				t.Fatalf("%s has no interface label", sample.Metric)
			}
		default:
			t.Fatalf("unexpected container metric %q", sample.Metric)
		}
	}
	if memory == 0 {
		t.Fatal("daemon run emitted no container memory gauge")
	}
	if cpu == 0 {
		t.Fatal("daemon run emitted no container CPU rate")
	}
	if network == 0 {
		t.Fatal("daemon run emitted no container network rate")
	}
}

// TestDockerOnceStreamStatsDowngrades makes the fallback explicit: a valid
// daemon configuration with stream_stats still supports collect --once, logs
// that it used one-shot stats, and writes the memory gauge.
func TestDockerOnceStreamStatsDowngrades(t *testing.T) {
	socket := requireDockerCLI(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	name := startDockerContainer(t, ctx, socket)

	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := dockerConfig(t, dataDir, socket, true)

	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("collect --once exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "stream_stats") || !strings.Contains(res.Stderr, "one-shot") {
		t.Fatalf("stderr does not explain the --once downgrade:\n%s", res.Stderr)
	}

	samples := samplesForContainer(readSamples(t, dataDir), name)
	if len(samples) != 1 || samples[0].Metric != "container.memory.used_bytes" {
		t.Fatalf("downgraded --once samples = %+v, want one memory gauge", samples)
	}
}

// dockerConfig writes a Docker-only collect configuration with a short
// interval and a one-second chunk, so a daemon run flushes quickly.
func dockerConfig(t *testing.T, dataDir, socket string, streamStats bool) string {
	t.Helper()
	return writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
sources:
  system:
    enabled: false
  docker:
    enabled: true
    socket: %s
    interval: 100ms
    rescan_interval: 100ms
    stream_stats: %t
storage:
  primary: disk
  chunk_interval: 1s
`, dataDir, socket, streamStats))
}

// requireDockerCLI gates CLI container tests on a real socket and the docker
// CLI used only to arrange and remove the container. Skip messages say how to
// provide the dependency; strict mode turns a skip into a failure.
func requireDockerCLI(t *testing.T) string {
	t.Helper()

	socket := os.Getenv("SMOB_TEST_DOCKER_SOCKET")
	if socket == "" {
		socket = "/var/run/docker.sock"
	}
	if _, err := os.Stat(socket); err != nil {
		reason := fmt.Sprintf("Docker socket %s is unavailable (%v); set SMOB_TEST_DOCKER_SOCKET or run `just test-all`", socket, err)
		if os.Getenv("SMOB_TEST_STRICT") == "1" {
			t.Fatal(reason)
		}
		t.Skip(reason)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		reason := fmt.Sprintf("docker CLI is unavailable (%v); it is required to arrange the container fixture", err)
		if os.Getenv("SMOB_TEST_STRICT") == "1" {
			t.Fatal(reason)
		}
		t.Skip(reason)
	}
	return socket
}

// startDockerContainer starts a uniquely named busybox container and removes
// it when the test ends. The daemon and image are real; there is no fake.
func startDockerContainer(t *testing.T, ctx context.Context, socket string) string {
	t.Helper()

	name := fmt.Sprintf("smob-cli-%d-%d", os.Getpid(), time.Now().UnixNano())
	t.Cleanup(func() {
		removeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		cmd := exec.CommandContext(removeCtx, "docker", "rm", "-f", name)
		cmd.Env = dockerEnv(socket)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Logf("remove container %s: %v\n%s", name, err, out)
		}
	})

	runDocker(t, ctx, socket, "run", "-d", "--name", name, dockerTestImage, "sleep", "120")
	return name
}

// runDocker runs one docker CLI command against socket and returns its trimmed
// combined output.
func runDocker(t *testing.T, ctx context.Context, socket string, args ...string) string {
	t.Helper()

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env = dockerEnv(socket)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// dockerEnv points the docker CLI at the same daemon smob is configured to use.
func dockerEnv(socket string) []string {
	return append(os.Environ(), "DOCKER_HOST=unix://"+socket)
}

// samplesForContainer returns the samples labelled with the named container.
func samplesForContainer(samples []wireSample, name string) []wireSample {
	var matches []wireSample
	for _, sample := range samples {
		if sample.Labels["container"] == name {
			matches = append(matches, sample)
		}
	}
	return matches
}

// assertContainerLabels checks the collector-supplied identity and the
// documented series labels, and rejects labels from another scope.
func assertContainerLabels(t *testing.T, sample wireSample, name string) {
	t.Helper()

	if got := sample.Labels["host"]; got != "test-host" {
		t.Fatalf("%s host label = %q, want test-host", sample.Metric, got)
	}
	if got := sample.Labels["scope"]; got != "container" {
		t.Fatalf("%s scope label = %q, want container", sample.Metric, got)
	}
	if got := sample.Labels["container"]; got != name {
		t.Fatalf("%s container label = %q, want %q", sample.Metric, got, name)
	}
	if image := sample.Labels["image"]; !strings.Contains(image, "busybox") {
		t.Fatalf("%s image label = %q, want the busybox image", sample.Metric, image)
	}
	for label := range sample.Labels {
		switch label {
		case "host", "scope", "container", "image", "interface":
		default:
			t.Fatalf("%s has unexpected label %q", sample.Metric, label)
		}
	}
}

// assertRate checks the rate contract: a positive measurement window and a
// finite, non-negative value.
func assertRate(t *testing.T, sample wireSample) {
	t.Helper()
	if sample.IntervalMS <= 0 {
		t.Fatalf("%s interval_ms = %d, want greater than zero", sample.Metric, sample.IntervalMS)
	}
	if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) || sample.Value < 0 {
		t.Fatalf("%s = %v, want a finite non-negative value", sample.Metric, sample.Value)
	}
}
