//go:build integration

package integration

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
			cfg := dockerConfig(t, dataDir, socket, dockerConfigOptions{})

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
	cfg := dockerConfig(t, dataDir, socket, dockerConfigOptions{})

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
	cfg := dockerConfig(t, dataDir, socket, dockerConfigOptions{})

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
	cfg := dockerConfig(t, dataDir, socket, dockerConfigOptions{StreamStats: true})

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

// TestDockerExcludeRemovesMatchingContainer proves the exclude filter is
// applied on the way a user reaches it. The pattern is an unanchored substring
// in the middle of the excluded name; the kept name does not contain it.
func TestDockerExcludeRemovesMatchingContainer(t *testing.T) {
	socket := requireDockerCLI(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	unique := fmt.Sprintf("smob-cli-exclude-%d-%d", os.Getpid(), time.Now().UnixNano())
	keptName := unique + "-keep"
	excludedName := "x-" + unique + "-skip-me"
	startNamedDockerContainer(t, ctx, socket, keptName)
	startNamedDockerContainer(t, ctx, socket, excludedName)

	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := dockerConfig(t, dataDir, socket, dockerConfigOptions{Exclude: unique + "-skip"})

	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("collect --once exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	samples := readSamples(t, dataDir)
	if got := samplesForContainer(samples, keptName); len(got) == 0 {
		t.Fatalf("kept container %s produced no samples", keptName)
	}
	if got := samplesForContainer(samples, excludedName); len(got) != 0 {
		t.Fatalf("excluded container %s produced %d samples: %+v", excludedName, len(got), got)
	}
}

// TestDockerIntervalControlsCadence checks the configured interval on the CLI
// path. Eight memory gauges inside closed chunks with gaps well below the 10s
// default cannot result from the default cadence.
func TestDockerIntervalControlsCadence(t *testing.T) {
	socket := requireDockerCLI(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	name := startDockerContainer(t, ctx, socket)

	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := dockerConfig(t, dataDir, socket, dockerConfigOptions{Interval: 100 * time.Millisecond})

	d := startSmob(t, ctx, "collect", "--config", cfg)

	waitFor(t, 20*time.Second, "a closed container chunk", func() bool {
		return countChunks(t, dataDir) >= 1
	})
	waitFor(t, 20*time.Second, "eight memory gauges in closed chunks", func() bool {
		gauges := samplesForMetric(readSamples(t, dataDir), name, "container.memory.used_bytes")
		return len(gauges) >= 8
	})
	d.stop(t, 15*time.Second)

	gauges := samplesForMetric(readSamples(t, dataDir), name, "container.memory.used_bytes")
	if len(gauges) < 8 {
		t.Fatalf("memory gauges = %d, want at least 8 after the closed-chunk wait", len(gauges))
	}
	var maxGap time.Duration
	for i := 1; i < len(gauges); i++ {
		gap := gauges[i].Timestamp.Sub(gauges[i-1].Timestamp)
		if gap <= 0 {
			t.Fatalf("memory gauge timestamps are not increasing: %s then %s",
				gauges[i-1].Timestamp, gauges[i].Timestamp)
		}
		maxGap = max(maxGap, gap)
	}
	if maxGap > 500*time.Millisecond {
		t.Fatalf("largest memory gauge gap = %s, want at most 500ms for a 100ms interval", maxGap)
	}
}

// TestDockerRescanDiscoversNewContainer proves a container created after smob
// started contributes without restarting the collector.
func TestDockerRescanDiscoversNewContainer(t *testing.T) {
	socket := requireDockerCLI(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	baselineName := startDockerContainer(t, ctx, socket)

	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := dockerConfig(t, dataDir, socket, dockerConfigOptions{RescanInterval: 100 * time.Millisecond})

	d := startSmob(t, ctx, "collect", "--config", cfg)
	waitFor(t, 20*time.Second, "samples for the container present at startup", func() bool {
		return len(samplesForContainer(readSamples(t, dataDir), baselineName)) > 0
	})

	lateName := fmt.Sprintf("smob-cli-late-%d-%d", os.Getpid(), time.Now().UnixNano())
	startNamedDockerContainer(t, ctx, socket, lateName)

	waitFor(t, 30*time.Second, "samples for a container created after startup", func() bool {
		return len(samplesForContainer(readSamples(t, dataDir), lateName)) > 0
	})
	d.stop(t, 15*time.Second)

	if got := samplesForMetric(readSamples(t, dataDir), lateName, "container.memory.used_bytes"); len(got) == 0 {
		t.Fatalf("container %s created after startup produced no memory gauge", lateName)
	}
}

// TestDockerRecreateResetsRateSeries removes and re-creates a container with
// the same name while one daemon keeps running. Labels carry the name and image
// only, so both incarnations occupy the same label series; what the CLI can
// observe is that rate state resets. The removed container stops producing
// samples, nothing is fabricated in the removal gap, and the re-created
// container's first observation is the memory gauge only because its new ID has
// no previous CPU or network counters to diff.
func TestDockerRecreateResetsRateSeries(t *testing.T) {
	socket := requireDockerCLI(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	unique := fmt.Sprintf("smob-cli-recreate-%d-%d", os.Getpid(), time.Now().UnixNano())
	name := unique + "-target"
	canary := unique + "-canary"

	// The canary keeps container-scope chunks closing while the target is
	// absent, so the quiet window is observable in closed chunks rather than
	// inferred from a lack of chunks.
	startNamedDockerContainer(t, ctx, socket, canary)
	startNamedDockerContainer(t, ctx, socket, name)

	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := dockerConfig(t, dataDir, socket, dockerConfigOptions{})

	d := startSmob(t, ctx, "collect", "--config", cfg)

	waitFor(t, 30*time.Second, "container rates before removal", func() bool {
		return len(samplesForMetric(readSamples(t, dataDir), name, "container.cpu.percent")) > 0
	})

	runDocker(t, ctx, socket, "rm", "-f", name)
	removedAt := time.Now()

	// A tick or rescan already in flight can emit trailing samples after the
	// removal: a 2s grace is twenty times both the polling and rescan
	// intervals. After that, the canary must have moved collection a further
	// quiet period forward, backed by closed chunks rather than a sleep.
	const (
		removalGrace = 2 * time.Second
		quietPeriod  = 2 * time.Second
	)
	gapStart := removedAt.Add(removalGrace)
	waitFor(t, 30*time.Second, "canary samples past the removal grace and quiet period", func() bool {
		return latestSampleTimestamp(readSamples(t, dataDir), canary).After(gapStart.Add(quietPeriod))
	})

	// The gap ends when the re-creation is requested: the new container cannot
	// exist before this instant, so any target sample in between is fabricated
	// or belongs to the removed incarnation after the grace period.
	recreateIssuedAt := time.Now()
	runDocker(t, ctx, socket, "run", "-d", "--name", name, dockerTestImage, "sleep", "120")

	waitFor(t, 30*time.Second, "post-recreation memory and CPU samples in closed chunks", func() bool {
		var memory, cpu bool
		for _, sample := range samplesForContainer(readSamples(t, dataDir), name) {
			if sample.Timestamp.Before(recreateIssuedAt) {
				continue
			}
			switch sample.Metric {
			case "container.memory.used_bytes":
				memory = true
			case "container.cpu.percent":
				cpu = true
			}
		}
		return memory && cpu
	})
	d.stop(t, 15*time.Second)

	samples := samplesForContainer(readSamples(t, dataDir), name)
	var pre, gap, post []wireSample
	for _, sample := range samples {
		switch {
		case sample.Timestamp.Before(gapStart):
			pre = append(pre, sample)
		case sample.Timestamp.Before(recreateIssuedAt):
			gap = append(gap, sample)
		default:
			post = append(post, sample)
		}
	}

	if len(pre) == 0 {
		t.Fatalf("no samples for %s before the removal gap", name)
	}
	if got := samplesForMetric(pre, name, "container.cpu.percent"); len(got) == 0 {
		t.Fatalf("no CPU rate for the original %s before removal", name)
	}
	if len(gap) != 0 {
		t.Fatalf("%d samples for %s in the removal gap [%s, %s): %+v",
			len(gap), name, gapStart, recreateIssuedAt, gap)
	}
	if len(post) == 0 {
		t.Fatalf("no samples for %s after recreation at %s", name, recreateIssuedAt)
	}

	firstAt := post[0].Timestamp
	for _, sample := range post[1:] {
		if sample.Timestamp.Before(firstAt) {
			firstAt = sample.Timestamp
		}
	}
	var first []wireSample
	for _, sample := range post {
		if sample.Timestamp.Equal(firstAt) {
			first = append(first, sample)
		}
	}
	if len(first) != 1 || first[0].Metric != "container.memory.used_bytes" {
		t.Fatalf("first post-recreation observation for %s at %s = %+v, want exactly the memory gauge; a leaked rate baseline would emit a rate here",
			name, firstAt, first)
	}
	if first[0].IntervalMS != 0 {
		t.Fatalf("first post-recreation memory interval_ms = %d, want 0", first[0].IntervalMS)
	}
	if first[0].Value <= 0 {
		t.Fatalf("first post-recreation memory gauge = %v, want greater than zero", first[0].Value)
	}
	assertContainerLabels(t, first[0], name)

	// A corrupted delta from the old container ID would either be rejected as a
	// counter decrease (leaving no rates at all) or, if it survived, be far
	// outside these sanity bounds. An idle busybox cannot plausibly move a
	// gibibyte per second, and CPU stays within the documented per-core bound.
	const maxPlausibleNetworkRate = 1 << 30
	var cpu, network int
	for _, sample := range post {
		if !sample.Timestamp.After(firstAt) {
			continue
		}
		assertContainerLabels(t, sample, name)
		switch sample.Metric {
		case "container.memory.used_bytes":
			if sample.IntervalMS != 0 {
				t.Fatalf("memory gauge interval_ms = %d, want 0", sample.IntervalMS)
			}
			if sample.Value <= 0 {
				t.Fatalf("memory gauge = %v, want greater than zero", sample.Value)
			}
		case "container.cpu.percent":
			cpu++
			assertRate(t, sample)
			if limit := 100 * float64(runtime.NumCPU()); sample.Value > limit {
				t.Fatalf("CPU percent = %v, want at most %v for this host", sample.Value, limit)
			}
		case "container.network.rx_bytes_per_second", "container.network.tx_bytes_per_second":
			network++
			assertRate(t, sample)
			if sample.Value > maxPlausibleNetworkRate {
				t.Fatalf("%s = %v, want at most %d for an idle container",
					sample.Metric, sample.Value, maxPlausibleNetworkRate)
			}
			if sample.Labels["interface"] == "" {
				t.Fatalf("%s has no interface label", sample.Metric)
			}
		default:
			t.Fatalf("unexpected container metric %q after recreation", sample.Metric)
		}
	}
	if cpu == 0 {
		t.Fatalf("no CPU rate for %s after the fresh baseline", name)
	}
	if network == 0 {
		t.Fatalf("no network rate for %s after the fresh baseline", name)
	}
}

// TestDockerStreamStatsSubInterval proves stream_stats produces observations
// finer than the collector tick. The collector ticks every 3s, so a streamed
// CPU sample with interval_ms below 3000 cannot come from one-shot polling. If
// no CPU delta can be observed in the bounded window (fewer than two payloads),
// the test skips unless strict mode is on.
func TestDockerStreamStatsSubInterval(t *testing.T) {
	socket := requireDockerCLI(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	name := startDockerContainer(t, ctx, socket)

	const collectorInterval = 3 * time.Second
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := dockerConfig(t, dataDir, socket, dockerConfigOptions{
		Interval:    collectorInterval,
		StreamStats: true,
	})

	d := startSmob(t, ctx, "collect", "--config", cfg)

	observedAt := time.Now()
	waitFor(t, 40*time.Second, "streamed CPU evidence or the observation deadline", func() bool {
		if len(samplesForMetric(readSamples(t, dataDir), name, "container.cpu.percent")) > 0 {
			return true
		}
		// Bound the observation: a daemon that delivers only one payload per
		// window has no CPU delta to report, so keep waiting would never help.
		return time.Since(observedAt) >= 15*time.Second
	})
	d.stop(t, 15*time.Second)

	cpu := samplesForMetric(readSamples(t, dataDir), name, "container.cpu.percent")
	subInterval := 0
	for _, sample := range cpu {
		assertContainerLabels(t, sample, name)
		if sample.IntervalMS <= 0 {
			t.Fatalf("CPU interval_ms = %d, want greater than zero", sample.IntervalMS)
		}
		if sample.IntervalMS < collectorInterval.Milliseconds() {
			subInterval++
		}
	}
	if subInterval > 0 {
		return
	}
	if len(cpu) == 0 {
		reason := fmt.Sprintf("Docker delivered fewer than two stream payloads in the %s observation window; cannot observe stream cadence", 15*time.Second)
		if os.Getenv("SMOB_TEST_STRICT") == "1" {
			t.Fatal(reason)
		}
		t.Skip(reason)
	}
	t.Fatalf("all %d CPU samples report interval_ms >= %d; stream_stats produced one-shot cadence: %+v",
		len(cpu), collectorInterval.Milliseconds(), cpu)
}

// dockerConfigOptions overrides the short defaults used by Docker CLI tests.
// Zero durations take the test default so each test states only what it varies.
type dockerConfigOptions struct {
	Exclude        string
	Interval       time.Duration
	RescanInterval time.Duration
	StreamStats    bool
}

// dockerConfig writes a Docker-only collect configuration with a one-second
// chunk, so a daemon run flushes quickly.
func dockerConfig(t *testing.T, dataDir, socket string, opts dockerConfigOptions) string {
	t.Helper()
	if opts.Interval == 0 {
		opts.Interval = 100 * time.Millisecond
	}
	if opts.RescanInterval == 0 {
		opts.RescanInterval = 100 * time.Millisecond
	}
	return writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
sources:
  system:
    enabled: false
  docker:
    enabled: true
    socket: %s
    exclude: %q
    interval: %s
    rescan_interval: %s
    stream_stats: %t
storage:
  primary: disk
  chunk_interval: 1s
`, dataDir, socket, opts.Exclude, opts.Interval, opts.RescanInterval, opts.StreamStats))
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
	startNamedDockerContainer(t, ctx, socket, name)
	return name
}

// startNamedDockerContainer starts a busybox container with a caller-chosen
// name and removes it when the test ends.
func startNamedDockerContainer(t *testing.T, ctx context.Context, socket, name string) {
	t.Helper()

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

// samplesForMetric returns one container's samples for a metric, in stored
// order.
func samplesForMetric(samples []wireSample, name, metric string) []wireSample {
	var matches []wireSample
	for _, sample := range samplesForContainer(samples, name) {
		if sample.Metric == metric {
			matches = append(matches, sample)
		}
	}
	return matches
}

// latestSampleTimestamp returns the newest timestamp for a container in
// samples, or the zero time when it has none.
func latestSampleTimestamp(samples []wireSample, name string) time.Time {
	var latest time.Time
	for _, sample := range samplesForContainer(samples, name) {
		if sample.Timestamp.After(latest) {
			latest = sample.Timestamp
		}
	}
	return latest
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
