//go:build integration

package docker

// The Docker source is not registered yet, so these tests drive the stats
// acquisition layer beside the package, as the client probe test does. The
// daemon is real and no fake server stands in for it.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
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

	got := acq.oneShot(ctx, []string{id, "smob-no-such-container"})
	if len(got) != 1 {
		t.Fatalf("one-shot returned %d payloads, want 1; a failed container must not fail the batch", len(got))
	}
	assertStatsPayload(t, got[0], id)
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
		stats := acq.oneShot(ctx, []string{id})
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
