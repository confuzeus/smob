//go:build integration

package docker

// These tests drive the stats acquisition layer beside the package, as the
// client probe test does. The daemon is real and no fake server stands in for
// it.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
)

const (
	// testImage is pinned so a cached image does not change between runs.
	testImage = "busybox:1.36"
	// testSleepSeconds keeps the container alive long enough for one-shot and
	// stream tests to observe several stats payloads.
	testSleepSeconds = 120
)

func TestStatsOneShotRealContainer(t *testing.T) {
	c, ctx := connectDocker(t)
	id := startSleepContainer(t, ctx, c)

	acq := newTestStatsAcquirer(c)
	got := waitForOneShotStats(t, ctx, acq, id)
	assertStatsPayload(t, got, id)
}

func TestStatsOneShotSkipsGoneContainer(t *testing.T) {
	c, ctx := connectDocker(t)
	id := startSleepContainer(t, ctx, c)

	acq := newTestStatsAcquirer(c)
	// Make sure the live container is already reporting before adding an ID
	// that can never succeed.
	_ = waitForOneShotStats(t, ctx, acq, id)

	got, err := acq.oneShot(ctx, []string{id, "smob-no-such-container"})
	if err != nil {
		t.Fatalf("one-shot: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("one-shot returned %d payloads, want 1; a failed container must not fail the batch", len(got))
	}
	assertStatsPayload(t, got[0], id)
}

// TestStatsOneShotTickDeadlineReturnsError proves a tick deadline is reported
// to the collector instead of being swallowed as an empty, successful batch.
func TestStatsOneShotTickDeadlineReturnsError(t *testing.T) {
	c, ctx := connectDocker(t)
	id := startSleepContainer(t, ctx, c)
	acq := newTestStatsAcquirer(c)
	_ = waitForOneShotStats(t, ctx, acq, id)

	// The collector bounds a tick with context.WithTimeout, so an expired
	// deadline is the timeout the acquirer must surface.
	tickCtx, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()

	got, err := acq.oneShot(tickCtx, []string{id})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("one-shot with an expired tick deadline: error = %v, want context.DeadlineExceeded", err)
	}
	if len(got) != 0 {
		t.Fatalf("one-shot returned %d payloads after the deadline, want none", len(got))
	}
}

// TestStatsOneShotParentCancelReturnsNoError proves shutdown is not a source
// failure: a canceled parent returns the completed payloads with no error.
func TestStatsOneShotParentCancelReturnsNoError(t *testing.T) {
	c, ctx := connectDocker(t)
	id := startSleepContainer(t, ctx, c)
	acq := newTestStatsAcquirer(c)
	_ = waitForOneShotStats(t, ctx, acq, id)

	parentCtx, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := acq.oneShot(parentCtx, []string{id}); err != nil {
		t.Fatalf("one-shot after parent cancellation: %v, want nil", err)
	}
}

func TestStatsStreamRealContainer(t *testing.T) {
	c, ctx := connectDocker(t)
	id := startSleepContainer(t, ctx, c)

	acq := newTestStatsAcquirer(c)
	streamCtx, stopStream := context.WithCancel(ctx)
	stream := acq.stream(streamCtx, []string{id})

	got := firstStreamPayload(t, stream)
	assertStatsPayload(t, got, id)

	stopStream()
	waitForStreamClose(t, stream, 15*time.Second)
}

func TestStatsStreamClosesOnOpenError(t *testing.T) {
	c, ctx := connectDocker(t)

	acq := newTestStatsAcquirer(c)
	// The streaming primitive stops its reader and closes the fan-in channel on
	// an open error. Reconnect is the supervisor's job; this pins the primitive
	// contract the supervisor relies on.
	stream := acq.stream(ctx, []string{"smob-no-such-container"})
	waitForStreamClose(t, stream, 15*time.Second)
}

// connectDocker opens a client for the test daemon and returns a per-test
// context that outlives container cleanup.
func connectDocker(t *testing.T) (*client.Client, context.Context) {
	t.Helper()
	socket := requireDocker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	c, err := Connect(ctx, socket)
	if err != nil {
		t.Fatalf("connect to %s: %v", socket, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, ctx
}

// startSleepContainer starts a uniquely named busybox container and removes it
// when the test ends.
func startSleepContainer(t *testing.T, ctx context.Context, c *client.Client) string {
	t.Helper()
	name := fmt.Sprintf("smob-stats-%d-%d", os.Getpid(), time.Now().UnixNano())
	return startNamedContainer(t, ctx, c, name)
}

// startNamedContainer starts a busybox sleep container under name and removes
// it when the test ends. The image is pulled only when it is absent; a pull
// failure is a test failure, never a skip.
func startNamedContainer(t *testing.T, ctx context.Context, c *client.Client, name string) string {
	t.Helper()
	ensureImage(t, ctx, c, testImage)

	created, err := c.ContainerCreate(ctx,
		&container.Config{
			Image: testImage,
			Cmd:   []string{"sleep", strconv.Itoa(testSleepSeconds)},
		},
		&container.HostConfig{},
		nil,
		nil,
		name,
	)
	if err != nil {
		t.Fatalf("create container %s: %v", name, err)
	}
	t.Cleanup(func() {
		removeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := c.ContainerRemove(removeCtx, created.ID, container.RemoveOptions{Force: true}); err != nil {
			t.Logf("remove container %s: %v", created.ID, err)
		}
	})

	if err := c.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		t.Fatalf("start container %s: %v", created.ID, err)
	}
	return created.ID
}

// ensureImage checks for a local image and pulls it when absent.
func ensureImage(t *testing.T, ctx context.Context, c *client.Client, ref string) {
	t.Helper()
	if _, err := c.ImageInspect(ctx, ref); err == nil {
		return
	}

	reader, err := c.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		t.Fatalf("pull image %s: %v", ref, err)
	}
	defer reader.Close()
	if _, err := io.Copy(io.Discard, reader); err != nil {
		t.Fatalf("pull image %s: %v", ref, err)
	}
	if _, err := c.ImageInspect(ctx, ref); err != nil {
		t.Fatalf("inspect image %s after pull: %v", ref, err)
	}
}

// newTestStatsAcquirer sends library warnings to stderr so a failed run shows
// why a container was omitted.
func newTestStatsAcquirer(c *client.Client) *statsAcquirer {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return newStatsAcquirer(c, logger)
}

// waitForOneShotStats polls until the container reports one sample. Docker can
// accept the request before the first stats sample is populated; polling keeps
// the test from sleeping for an arbitrary interval.
func waitForOneShotStats(t *testing.T, ctx context.Context, acq *statsAcquirer, id string) rawStats {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		stats, err := acq.oneShot(ctx, []string{id})
		if err != nil {
			t.Fatalf("one-shot stats for container %s: %v", id, err)
		}
		if len(stats) == 1 {
			return stats[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("no one-shot stats for container %s within 20s", id)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// firstStreamPayload waits for the stream to deliver one payload.
func firstStreamPayload(t *testing.T, stream <-chan rawStats) rawStats {
	t.Helper()
	select {
	case got, ok := <-stream:
		if !ok {
			t.Fatal("stats stream closed before delivering a payload")
		}
		return got
	case <-time.After(30 * time.Second):
		t.Fatal("no streaming stats payload within 30s")
		return rawStats{}
	}
}

// waitForStreamClose drains the fan-in channel until it closes. A closed
// channel is the deterministic proof that every reader goroutine has exited.
func waitForStreamClose(t *testing.T, stream <-chan rawStats, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case _, ok := <-stream:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatalf("stats stream did not close within %s", timeout)
		}
	}
}

// assertStatsPayload checks that the raw payload carries the fields the metric
// layer will need, without asserting live values.
func assertStatsPayload(t *testing.T, got rawStats, id string) {
	t.Helper()
	if got.containerID != id {
		t.Fatalf("payload container id = %q, want %q", got.containerID, id)
	}

	stats := got.stats
	if stats.ID == "" {
		t.Fatal("stats payload has an empty id")
	}
	if stats.Read.IsZero() {
		t.Fatal("stats payload has a zero read time")
	}
	if stats.CPUStats.SystemUsage == 0 {
		t.Fatal("cpu_stats.system_cpu_usage is zero")
	}
	if stats.CPUStats.OnlineCPUs == 0 {
		t.Fatal("cpu_stats.online_cpus is zero")
	}
	if stats.MemoryStats.Usage == 0 {
		t.Fatal("memory_stats.usage is zero")
	}
	if stats.MemoryStats.Limit == 0 {
		t.Fatal("memory_stats.limit is zero")
	}
	if len(stats.Networks) == 0 {
		t.Fatal("networks is empty")
	}
	for name, network := range stats.Networks {
		if name == "" {
			t.Fatal("network interface name is empty")
		}
		t.Logf("container %s interface %s rx_bytes=%d tx_bytes=%d", id, name, network.RxBytes, network.TxBytes)
		break
	}
}

// TestStatsOneShotConcurrencyCap observes the real stats requests at the HTTP
// transport: oneShot may keep at most oneShotConcurrency requests in flight
// against the daemon. A short delay on each stats response keeps the window
// open long enough for a local daemon to expose the cap; without it responses
// arrive serially and the bound is never exercised.
func TestStatsOneShotConcurrencyCap(t *testing.T) {
	socket := requireDocker(t)
	c, ctx := connectDocker(t)

	const containers = 9
	ids := make([]string, 0, containers)
	for i := range containers {
		name := fmt.Sprintf("smob-conc-%d-%d-%d", os.Getpid(), time.Now().UnixNano(), i)
		ids = append(ids, startNamedContainer(t, ctx, c, name))
	}

	httpClient := c.HTTPClient()
	tracker := &inflightTracker{
		base:  httpClient.Transport,
		delay: 50 * time.Millisecond,
	}
	httpClient.Transport = tracker

	instrumented, err := client.NewClientWithOpts(
		client.WithHost("unix://"+socket),
		client.WithVersion(apiVersion),
		client.WithHTTPClient(httpClient),
	)
	if err != nil {
		t.Fatalf("create instrumented client: %v", err)
	}
	t.Cleanup(func() { _ = instrumented.Close() })

	acq := newStatsAcquirer(instrumented, newSupervisorTestLogger())
	got, err := acq.oneShot(ctx, ids)
	if err != nil {
		t.Fatalf("one-shot for %d containers: %v", len(ids), err)
	}
	if len(got) != len(ids) {
		t.Fatalf("one-shot returned %d payloads, want %d", len(got), len(ids))
	}

	peak := tracker.peak.Load()
	if peak > oneShotConcurrency {
		t.Fatalf("peak in-flight stats requests = %d, want at most %d", peak, oneShotConcurrency)
	}
	if peak != oneShotConcurrency {
		t.Fatalf("peak in-flight stats requests = %d, want %d; the worker cap was not exercised",
			peak, oneShotConcurrency)
	}
}

// inflightTracker is an http.RoundTripper over the real Docker transport. It
// counts simultaneous requests and delays stats requests before they reach the
// daemon, the only way a local daemon's sub-millisecond responses can expose
// the one-shot worker cap deterministically.
type inflightTracker struct {
	base     http.RoundTripper
	delay    time.Duration
	inFlight atomic.Int64
	peak     atomic.Int64
}

func (t *inflightTracker) RoundTrip(req *http.Request) (*http.Response, error) {
	current := t.inFlight.Add(1)
	defer t.inFlight.Add(-1)

	for {
		peak := t.peak.Load()
		if current <= peak || t.peak.CompareAndSwap(peak, current) {
			break
		}
	}

	if t.delay > 0 && strings.Contains(req.URL.Path, "/stats") {
		timer := time.NewTimer(t.delay)
		select {
		case <-timer.C:
		case <-req.Context().Done():
			timer.Stop()
			return nil, req.Context().Err()
		}
	}
	return t.base.RoundTrip(req)
}

// TestStatsStreamDropsPayloadsWhenConsumerLags leaves the fan-in channel
// unread until the daemon produced more than its one-payload capacity. The drop
// warning proves the reader dropped a new payload instead of blocking, the
// buffered payload read afterwards predates the drop, and a newer payload
// arrives once the consumer resumes.
func TestStatsStreamDropsPayloadsWhenConsumerLags(t *testing.T) {
	c, ctx := connectDocker(t)
	id := startSleepContainer(t, ctx, c)

	logs := &logCapture{}
	acq := newStatsAcquirer(c, slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))

	streamCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	stream := acq.stream(streamCtx, []string{id})

	waitForCondition(t, 45*time.Second, "a dropped stats payload while the consumer is behind", func() bool {
		return countLogs(logs.records(t), "container stats consumer is behind; dropping samples", id) > 0
	})
	droppedAt := time.Now()

	var kept rawStats
	select {
	case got, ok := <-stream:
		if !ok {
			t.Fatal("stats stream closed before the buffered payload could be read")
		}
		kept = got
	case <-time.After(15 * time.Second):
		t.Fatal("buffered stats payload was not readable; the reader blocked instead of dropping")
	}
	if !kept.stats.Read.Before(droppedAt) {
		t.Fatalf("buffered payload read time %s is not older than the drop at %s; the newest payload was kept",
			kept.stats.Read, droppedAt)
	}

	fresh := firstStreamPayload(t, stream)
	if !fresh.stats.Read.After(kept.stats.Read) {
		t.Fatalf("payload after resuming = %s, want newer than the kept payload at %s",
			fresh.stats.Read, kept.stats.Read)
	}

	cancel()
	waitForStreamClose(t, stream, 15*time.Second)
}

// TestStatsStreamCancellationClosesBodies cancels a live stream repeatedly and
// counts this process's socket descriptors. Each cycle leaves the stream
// mid-body, so a missing Body.Close leaks one socket per cycle; the count must
// return to its baseline.
func TestStatsStreamCancellationClosesBodies(t *testing.T) {
	c, ctx := connectDocker(t)
	id := startSleepContainer(t, ctx, c)
	acq := newTestStatsAcquirer(c)

	baseline := countSocketFDs(t)
	const cycles = 12
	for i := 0; i < cycles; i++ {
		streamCtx, cancel := context.WithCancel(ctx)
		stream := acq.stream(streamCtx, []string{id})
		_ = firstStreamPayload(t, stream)
		cancel()
		waitForStreamClose(t, stream, 15*time.Second)
	}

	waitForCondition(t, 10*time.Second, "cancelled stream socket descriptors to return to baseline", func() bool {
		return countSocketFDs(t) <= baseline+2
	})
	if got := countSocketFDs(t); got > baseline+2 {
		t.Fatalf("socket descriptors after %d cancelled streams = %d, want at most %d (baseline %d); cancelled response bodies were not closed",
			cycles, got, baseline+2, baseline)
	}
}

// countSocketFDs returns how many descriptors in /proc/self/fd are sockets.
func countSocketFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("read /proc/self/fd: %v", err)
	}
	sockets := 0
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err != nil {
			continue
		}
		if strings.HasPrefix(target, "socket:") {
			sockets++
		}
	}
	return sockets
}
