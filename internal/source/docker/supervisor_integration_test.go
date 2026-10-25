//go:build integration

package docker

// These tests drive the supervisor beside the package. The daemon is real: the
// fault proxy only forwards the Unix socket and can pause forwarding, which is
// the same technique the S3 spool test uses for a network partition.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
)

func TestSupervisorDiscoveryAndRescan(t *testing.T) {
	socket := requireDocker(t)
	c, ctx := connectDocker(t)
	proxy := newUnixFaultProxy(t, socket)

	unique := fmt.Sprintf("smob-sup-%d-%d", os.Getpid(), time.Now().UnixNano())
	includeName := unique + "-include"
	excludeName := unique + "-exclude"
	embeddedName := "x-" + excludeName

	includeID := startNamedContainer(t, ctx, c, includeName)
	excludeID := startNamedContainer(t, ctx, c, excludeName)
	embeddedID := startNamedContainer(t, ctx, c, embeddedName)

	// The pattern is deliberately unanchored: both the plain and the embedded
	// name match, while the include container does not.
	sup, err := NewSupervisor(ctx, Options{
		Socket:         proxy.Socket(),
		Exclude:        unique + "-exclude",
		RescanInterval: 200 * time.Millisecond,
	}, newSupervisorTestLogger())
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}

	got, ok := containerByID(sup, includeID)
	if !ok {
		t.Fatalf("initial discovery did not find container %s", includeID)
	}
	if got.Name != includeName {
		t.Fatalf("container name = %q, want %q", got.Name, includeName)
	}
	if got.Image != testImage {
		t.Fatalf("container image = %q, want %q", got.Image, testImage)
	}
	if hasContainer(sup, excludeID) {
		t.Fatalf("excluded container %s was discovered", excludeID)
	}
	if hasContainer(sup, embeddedID) {
		t.Fatalf("unanchored exclude did not match embedded name %s", embeddedID)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		sup.Run(runCtx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("supervisor did not stop within 10s")
		}
		_ = sup.Close()
	})

	proxy.Pause()
	waitForCondition(t, 15*time.Second, "a rescan rejected by the paused proxy", func() bool {
		return proxy.Rejected() > 0
	})

	// Change the set while discovery is failing. The supervisor keeps the old
	// snapshot until the daemon is reachable again, then reconciles.
	if err := c.ContainerRemove(ctx, includeID, container.RemoveOptions{Force: true}); err != nil {
		t.Fatalf("remove container %s: %v", includeID, err)
	}
	createdID := startNamedContainer(t, ctx, c, unique+"-created")

	proxy.Resume()
	waitForCondition(t, 30*time.Second, "newly created container to be discovered", func() bool {
		return hasContainer(sup, createdID)
	})
	waitForCondition(t, 30*time.Second, "removed container to disappear", func() bool {
		return !hasContainer(sup, includeID)
	})
	if hasContainer(sup, excludeID) || hasContainer(sup, embeddedID) {
		t.Fatalf("excluded containers reappeared after rescan")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("supervisor did not stop within 10s")
	}
	_ = sup.Close()
}

func TestSupervisorStreamReconnect(t *testing.T) {
	socket := requireDocker(t)
	c, ctx := connectDocker(t)
	proxy := newUnixFaultProxy(t, socket)

	unique := fmt.Sprintf("smob-sup-%d-%d", os.Getpid(), time.Now().UnixNano())
	aID := startNamedContainer(t, ctx, c, unique+"-a")

	sup, err := NewSupervisor(ctx, Options{
		Socket:         proxy.Socket(),
		RescanInterval: 200 * time.Millisecond,
		StreamStats:    true,
	}, newSupervisorTestLogger())
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		sup.Run(runCtx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("supervisor did not stop within 10s")
		}
		_ = sup.Close()
	})

	// A payload proves the initial stream is live before the outage.
	first := waitForStats(t, runCtx, sup, aID, 45*time.Second)
	aWorker := supervisorWorker(sup, aID)
	if aWorker == nil {
		t.Fatalf("no stream worker for container %s", aID)
	}

	pausedAt := time.Now()
	proxy.Pause()
	waitForCondition(t, 15*time.Second, "a stream reconnect rejected by the paused proxy", func() bool {
		return proxy.Rejected() > 0
	})
	drainStats(sup)

	// A container created during the outage must be discovered and streamed
	// once the proxy forwards again.
	bID := startNamedContainer(t, ctx, c, unique+"-b")
	proxy.Resume()

	// Read time is set by the daemon when the payload is produced, so a
	// payload read after pausedAt cannot be one buffered before the outage.
	aFresh := waitForStatsAfter(t, runCtx, sup, aID, pausedAt, 60*time.Second)
	if !aFresh.stats.Read.After(first.stats.Read) {
		t.Fatalf("reconnected payload read time %s is not after the pre-outage payload %s",
			aFresh.stats.Read, first.stats.Read)
	}
	waitForCondition(t, 30*time.Second, "container created during the outage to be discovered", func() bool {
		return hasContainer(sup, bID)
	})
	waitForStats(t, runCtx, sup, bID, 60*time.Second)

	// Several rescans have run while the streams recovered. Unchanged
	// containers must keep their worker rather than being reconnected on every
	// scan.
	if got := supervisorWorker(sup, aID); got != aWorker {
		t.Fatalf("rescan restarted the unchanged stream for container %s", aID)
	}

	// Removing a container cancels its worker. syncStreams joins the worker
	// before the new snapshot is published, so absence from Containers is
	// evidence that the goroutine and its stream are gone.
	if err := c.ContainerRemove(ctx, bID, container.RemoveOptions{Force: true}); err != nil {
		t.Fatalf("remove container %s: %v", bID, err)
	}
	waitForCondition(t, 30*time.Second, "removed container to disappear", func() bool {
		return !hasContainer(sup, bID)
	})
	if workerPresent(sup, bID) {
		t.Fatalf("stream worker for removed container %s is still present", bID)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("supervisor did not stop within 10s")
	}

	// Run waits for every worker and closes the stats channel before it
	// returns. Ranging it must terminate promptly, which is the no-leak proof.
	closed := make(chan struct{})
	go func() {
		for range sup.stats {
		}
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("stats channel stayed open after Run returned")
	}
	_ = sup.Close()
}

// TestSupervisorPublishesSnapshotBeforeStreams proves the first payload for a
// newly discovered container is attributed against a snapshot that already
// contains it. A worker started before setContainers would race that publish.
func TestSupervisorPublishesSnapshotBeforeStreams(t *testing.T) {
	socket := requireDocker(t)
	c, ctx := connectDocker(t)

	unique := fmt.Sprintf("smob-sup-%d-%d", os.Getpid(), time.Now().UnixNano())
	firstID := startNamedContainer(t, ctx, c, unique+"-first")

	sup, err := NewSupervisor(ctx, Options{
		Socket:         socket,
		RescanInterval: 200 * time.Millisecond,
		StreamStats:    true,
	}, newSupervisorTestLogger())
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		sup.Run(runCtx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("supervisor did not stop within 10s")
		}
		_ = sup.Close()
	})

	// The initial stream must be live before the new container appears, so the
	// payload that matters is produced after Run established the snapshot.
	_ = waitForStats(t, runCtx, sup, firstID, 45*time.Second)

	name := unique + "-new"
	newID := startNamedContainer(t, ctx, c, name)
	first := waitForStats(t, runCtx, sup, newID, 60*time.Second)

	container, ok := containerByID(sup, first.containerID)
	if !ok {
		t.Fatalf("first payload for container %s arrived before its snapshot was published", newID)
	}
	if container.ID != newID || container.Name != name {
		t.Fatalf("payload attributed to %+v, want id %s name %s", container, newID, name)
	}
}

// TestSupervisorRunIsSingleUse proves a second Run is a no-op: it must not
// start a second lifecycle, disturb the first, or close the stats channel
// twice.
func TestSupervisorRunIsSingleUse(t *testing.T) {
	socket := requireDocker(t)
	c, ctx := connectDocker(t)

	sup, err := NewSupervisor(ctx, Options{
		Socket:         socket,
		RescanInterval: 200 * time.Millisecond,
		StreamStats:    true,
	}, newSupervisorTestLogger())
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		sup.Run(runCtx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("supervisor did not stop within 10s")
		}
		_ = sup.Close()
	})

	// Wait until the first call owns the lifecycle; otherwise the second call
	// could win the race and the test would prove nothing.
	waitForCondition(t, 10*time.Second, "the first Run to claim the supervisor", func() bool {
		sup.runMu.Lock()
		defer sup.runMu.Unlock()
		return sup.running
	})

	second := make(chan struct{})
	go func() {
		sup.Run(runCtx)
		close(second)
	}()
	select {
	case <-second:
	case <-time.After(10 * time.Second):
		t.Fatal("second Run did not return; Run must be single-use")
	}

	// The first Run still owns the lifecycle.
	unique := fmt.Sprintf("smob-sup-%d-%d", os.Getpid(), time.Now().UnixNano())
	id := startNamedContainer(t, ctx, c, unique+"-after-second-run")
	_ = waitForStats(t, runCtx, sup, id, 45*time.Second)

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("supervisor did not stop within 10s")
	}

	// A second close of the stats channel would have panicked; reaching a
	// closed channel here proves the single close.
	closed := make(chan struct{})
	go func() {
		for range sup.stats {
		}
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("stats channel stayed open after Run returned")
	}
}

// TestSupervisorFlapDoesNotResetBackoff drives repeated one-payload streams.
// A daemon that closes each connection after one sample must not reset the
// reconnect backoff to the minimum or warn on every flap.
func TestSupervisorFlapDoesNotResetBackoff(t *testing.T) {
	socket := requireDocker(t)
	c, ctx := connectDocker(t)
	proxy := newUnixFaultProxy(t, socket)

	unique := fmt.Sprintf("smob-sup-%d-%d", os.Getpid(), time.Now().UnixNano())
	id := startNamedContainer(t, ctx, c, unique+"-flap")

	logs := &logCapture{}
	sup, err := NewSupervisor(ctx, Options{
		Socket:         proxy.Socket(),
		RescanInterval: 200 * time.Millisecond,
		StreamStats:    true,
	}, slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		sup.Run(runCtx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("supervisor did not stop within 10s")
		}
		_ = sup.Close()
	})

	_ = waitForStats(t, runCtx, sup, id, 45*time.Second)

	// Each cycle closes the live stream after one payload, rejects the
	// immediate reconnect, then resumes so the next one-payload stream can be
	// closed again. That is a flap, not a stable connection.
	rejected := proxy.Rejected()
	for i := 0; i < 3; i++ {
		cutoff := time.Now()
		proxy.Pause()
		waitForCondition(t, 15*time.Second, "a reconnect rejected by the paused proxy", func() bool {
			return proxy.Rejected() > rejected
		})
		rejected = proxy.Rejected()
		proxy.Resume()
		_ = waitForStatsAfter(t, runCtx, sup, id, cutoff, 45*time.Second)
	}

	records := logs.records(t)
	warned := countLogs(records, "container stats stream ended; reconnecting", id)
	if warned > 1 {
		t.Fatalf("stream ended warning logged %d times; a flapping stream must back off after the first", warned)
	}
	if warned == 0 {
		t.Fatal("flapping stream was never reported")
	}
	if debugged := countLogs(records, "container stats stream still unavailable; retrying", id); debugged == 0 {
		t.Fatal("repeated flaps were not logged at debug")
	}

	last := time.Duration(0)
	for _, record := range records {
		if record["container"] != id {
			continue
		}
		if d, ok := retryIn(record); ok {
			last = d
		}
	}
	if last < time.Second {
		t.Fatalf("last retry_in = %s, want at least 1s; the flap reset the backoff", last)
	}
}

// TestSupervisorIdleStreamRestarts proves a stream that is established but
// delivers nothing is torn down by the idle watchdog and reconnected.
func TestSupervisorIdleStreamRestarts(t *testing.T) {
	socket := requireDocker(t)
	c, ctx := connectDocker(t)
	proxy := newUnixFaultProxy(t, socket)

	unique := fmt.Sprintf("smob-sup-%d-%d", os.Getpid(), time.Now().UnixNano())
	id := startNamedContainer(t, ctx, c, unique+"-idle")

	logs := &logCapture{}
	sup, err := NewSupervisor(ctx, Options{
		Socket:         proxy.Socket(),
		RescanInterval: time.Second,
		StreamStats:    true,
		// The watchdog is max(3*interval, 30s), so a short interval still
		// exercises the floor and the restart.
		Interval: 100 * time.Millisecond,
	}, slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		sup.Run(runCtx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("supervisor did not stop within 10s")
		}
		_ = sup.Close()
	})

	_ = waitForStats(t, runCtx, sup, id, 45*time.Second)

	stalledAt := time.Now()
	proxy.Stall()
	waitForCondition(t, 60*time.Second, "the idle watchdog to restart the stalled stream", func() bool {
		return countLogs(logs.records(t), "container stats stream idle; reconnecting", id) > 0
	})
	proxy.Unstall()

	// The replacement stream must deliver a payload newer than the stall.
	_ = waitForStatsAfter(t, runCtx, sup, id, stalledAt, 45*time.Second)
}

// logCapture is a slog destination that keeps JSON records for assertions. It
// is safe for the discovery and stream goroutines to write concurrently.
type logCapture struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (c *logCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data.Write(p)
}

// records decodes the captured log lines.
func (c *logCapture) records(t *testing.T) []map[string]any {
	t.Helper()
	c.mu.Lock()
	body := c.data.String()
	c.mu.Unlock()

	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

// countLogs counts records with the given message and container.
func countLogs(records []map[string]any, msg, container string) int {
	count := 0
	for _, record := range records {
		if record["msg"] == msg && record["container"] == container {
			count++
		}
	}
	return count
}

// retryIn reads the structured retry delay from a record. slog encodes a
// duration as nanoseconds.
func retryIn(record map[string]any) (time.Duration, bool) {
	value, ok := record["retry_in"].(float64)
	if !ok {
		return 0, false
	}
	return time.Duration(value), true
}

// unixFaultProxy forwards a Unix socket to the real daemon and can stop
// forwarding on demand. It is a fault injector, not a fake Docker service.
type unixFaultProxy struct {
	path     string
	listener net.Listener
	target   string

	mu       sync.Mutex
	paused   bool
	stall    chan struct{}
	closed   chan struct{}
	conns    map[net.Conn]struct{}
	rejected int
}

func newUnixFaultProxy(t *testing.T, target string) *unixFaultProxy {
	t.Helper()
	path := filepath.Join(t.TempDir(), "docker.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen on %s: %v", path, err)
	}

	p := &unixFaultProxy{
		path:     path,
		listener: listener,
		target:   target,
		closed:   make(chan struct{}),
		conns:    make(map[net.Conn]struct{}),
	}
	go p.accept()
	t.Cleanup(p.close)
	return p
}

func (p *unixFaultProxy) Socket() string { return p.path }

func (p *unixFaultProxy) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	for conn := range p.conns {
		_ = conn.Close()
	}
}

func (p *unixFaultProxy) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
}

// Stall closes every forwarded connection and then holds new connections open
// without forwarding a byte, so a stats stream is established but delivers
// nothing. Unstall releases the held connections.
func (p *unixFaultProxy) Stall() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	if p.stall == nil {
		p.stall = make(chan struct{})
	}
	for conn := range p.conns {
		_ = conn.Close()
	}
}

func (p *unixFaultProxy) Unstall() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stall != nil {
		close(p.stall)
		p.stall = nil
	}
}

func (p *unixFaultProxy) Rejected() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rejected
}

func (p *unixFaultProxy) close() {
	close(p.closed)
	p.mu.Lock()
	for conn := range p.conns {
		_ = conn.Close()
	}
	p.mu.Unlock()
	_ = p.listener.Close()
}

func (p *unixFaultProxy) accept() {
	for {
		clientConn, err := p.listener.Accept()
		if err != nil {
			return
		}

		p.mu.Lock()
		if p.paused {
			p.rejected++
			p.mu.Unlock()
			_ = clientConn.Close()
			continue
		}
		p.mu.Unlock()

		go p.handle(clientConn)
	}
}

func (p *unixFaultProxy) handle(clientConn net.Conn) {
	p.mu.Lock()
	stall := p.stall
	p.mu.Unlock()
	if stall != nil {
		// Hold the connection without forwarding: the client sees an accepted
		// socket whose request never gets a response.
		select {
		case <-stall:
		case <-p.closed:
			_ = clientConn.Close()
			return
		}
	}

	upstream, err := net.Dial("unix", p.target)
	if err != nil {
		_ = clientConn.Close()
		return
	}

	p.mu.Lock()
	if p.paused {
		p.mu.Unlock()
		_ = clientConn.Close()
		_ = upstream.Close()
		return
	}
	p.conns[clientConn] = struct{}{}
	p.conns[upstream] = struct{}{}
	p.mu.Unlock()

	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(upstream, clientConn)
		_ = upstream.Close()
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(clientConn, upstream)
		_ = clientConn.Close()
		done <- struct{}{}
	}()
	<-done
	<-done

	p.mu.Lock()
	delete(p.conns, clientConn)
	delete(p.conns, upstream)
	p.mu.Unlock()
}

// waitForCondition polls observable state. Its sleep is the only wait in these
// tests; assertions are made on the state, never on elapsed time.
func waitForCondition(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func newSupervisorTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

func containerByID(s *Supervisor, id string) (Container, bool) {
	for _, c := range s.Containers() {
		if c.ID == id {
			return c, true
		}
	}
	return Container{}, false
}

func hasContainer(s *Supervisor, id string) bool {
	_, ok := containerByID(s, id)
	return ok
}

func workerPresent(s *Supervisor, id string) bool {
	return supervisorWorker(s, id) != nil
}

func supervisorWorker(s *Supervisor, id string) *streamWorker {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	return s.workers[id]
}

// waitForStats consumes payloads until the requested container produces one.
func waitForStats(t *testing.T, ctx context.Context, s *Supervisor, id string, timeout time.Duration) rawStats {
	t.Helper()
	return waitForStatsAfter(t, ctx, s, id, time.Time{}, timeout)
}

// waitForStatsAfter ignores payloads read at or before after. It is used to
// prove that a reconnected stream produced data newer than the pre-outage
// payload rather than replaying something buffered before the proxy paused.
func waitForStatsAfter(t *testing.T, ctx context.Context, s *Supervisor, id string, after time.Time, timeout time.Duration) rawStats {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case payload, ok := <-s.stats:
			if !ok {
				t.Fatalf("stats channel closed before container %s produced a payload", id)
			}
			if payload.containerID != id {
				continue
			}
			if !after.IsZero() && !payload.stats.Read.After(after) {
				continue
			}
			return payload
		case <-timer.C:
			t.Fatalf("no stats payload for container %s within %s", id, timeout)
		case <-ctx.Done():
			t.Fatalf("context ended while waiting for container %s stats: %v", id, ctx.Err())
		}
	}
}

// drainStats discards payloads that were queued before an injected outage.
func drainStats(s *Supervisor) {
	for {
		select {
		case <-s.stats:
		default:
			return
		}
	}
}
