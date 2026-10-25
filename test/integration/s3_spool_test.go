//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// faultProxy forwards TCP to a real S3 endpoint and can drop every connection
// on demand. It injects a network partition; it is not a fake S3 service.
type faultProxy struct {
	listener net.Listener
	target   string
	scheme   string

	mu     sync.Mutex
	paused bool
	drop   string
	conns  map[net.Conn]struct{}
}

func newFaultProxy(t *testing.T, endpoint string) *faultProxy {
	t.Helper()

	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse S3 endpoint: %v", err)
	}
	if u.Host == "" {
		t.Fatalf("S3 endpoint %q has no host", endpoint)
	}
	if u.Scheme != "http" {
		reason := fmt.Sprintf("network-partition proxy requires an http:// S3 endpoint; got %q", endpoint)
		if os.Getenv("SMOB_TEST_STRICT") == "1" {
			t.Fatal(reason)
		}
		t.Skip(reason)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for proxy: %v", err)
	}

	p := &faultProxy{
		listener: listener,
		target:   u.Host,
		scheme:   u.Scheme,
		conns:    make(map[net.Conn]struct{}),
	}
	go p.accept()
	t.Cleanup(p.close)
	return p
}

func (p *faultProxy) URL() string {
	return p.scheme + "://" + p.listener.Addr().String()
}

func (p *faultProxy) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	for conn := range p.conns {
		_ = conn.Close()
	}
}

func (p *faultProxy) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
}

// DropRequestsContaining aborts the connection carrying any HTTP request whose
// request line contains marker. It makes one key's upload fail the way a
// partition would, while every other request reaches the real endpoint.
func (p *faultProxy) DropRequestsContaining(marker string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.drop = marker
}

func (p *faultProxy) dropMarker() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.drop
}

func (p *faultProxy) close() {
	p.mu.Lock()
	for conn := range p.conns {
		_ = conn.Close()
	}
	p.mu.Unlock()
	_ = p.listener.Close()
}

func (p *faultProxy) accept() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}

		p.mu.Lock()
		if p.paused {
			p.mu.Unlock()
			_ = client.Close()
			continue
		}
		p.mu.Unlock()

		go p.handle(client)
	}
}

func (p *faultProxy) handle(client net.Conn) {
	upstream, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}

	p.mu.Lock()
	if p.paused {
		p.mu.Unlock()
		_ = client.Close()
		_ = upstream.Close()
		return
	}
	p.conns[client] = struct{}{}
	p.conns[upstream] = struct{}{}
	p.mu.Unlock()

	done := make(chan struct{}, 2)
	go func() {
		if marker := p.dropMarker(); marker != "" {
			p.forwardRequests(upstream, client, marker)
		} else {
			_, _ = io.Copy(upstream, client)
		}
		_ = upstream.Close()
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(client, upstream)
		_ = client.Close()
		done <- struct{}{}
	}()
	<-done
	<-done

	p.mu.Lock()
	delete(p.conns, client)
	delete(p.conns, upstream)
	p.mu.Unlock()
}

// forwardRequests copies HTTP requests upstream and aborts the connection when
// a request line contains marker. It skips each request body so a later
// keep-alive request is still inspected. This is a network fault injector: it
// forwards every byte it does not drop to the real endpoint.
func (p *faultProxy) forwardRequests(upstream, client net.Conn, marker string) {
	reader := bufio.NewReader(client)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		if strings.Contains(line, marker) {
			return
		}
		if _, err := io.WriteString(upstream, line); err != nil {
			return
		}
		length, chunked, err := copyHeaders(upstream, reader)
		if err != nil {
			return
		}
		if chunked {
			// A streamed body cannot be skipped without decoding it, so stop
			// inspecting this connection and forward the rest unchanged.
			_, _ = io.Copy(upstream, reader)
			return
		}
		if length == 0 {
			continue
		}
		if _, err := io.CopyN(upstream, reader, length); err != nil {
			return
		}
	}
}

// copyHeaders forwards one HTTP header block and returns its declared body
// length and whether the body is chunked.
func copyHeaders(dst io.Writer, src *bufio.Reader) (length int64, chunked bool, err error) {
	for {
		line, err := src.ReadString('\n')
		if err != nil {
			return 0, false, err
		}
		if _, err := io.WriteString(dst, line); err != nil {
			return 0, false, err
		}
		if line == "\r\n" || line == "\n" {
			return length, chunked, nil
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch {
		case strings.EqualFold(strings.TrimSpace(name), "Content-Length"):
			length, err = strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if err != nil {
				return 0, false, err
			}
		case strings.EqualFold(strings.TrimSpace(name), "Transfer-Encoding"):
			chunked = strings.Contains(strings.ToLower(value), "chunked")
		}
	}
}

func TestS3SpoolDrain(t *testing.T) {
	env := requireS3(t)
	proxy := newFaultProxy(t, env.endpoint)

	dataDir := filepath.Join(t.TempDir(), "data")
	spoolDir := filepath.Join(dataDir, "spool")

	// Seed an over-cap spool before collect starts: one large old chunk and two
	// recent ones. The startup trim must drop the oldest and keep the rest, and
	// it must finish before collection can write its first chunk.
	seedStart := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Hour)
	oldestKey := seedSpoolChunk(t, spoolDir, seedStart, incompressibleLabel(t, 1<<20))
	recentKeys := []string{
		seedSpoolChunk(t, spoolDir, seedStart.Add(time.Hour), "recent-a"),
		seedSpoolChunk(t, spoolDir, seedStart.Add(2*time.Hour), "recent-b"),
	}
	oldestSize := fileSize(t, filepath.Join(spoolDir, filepath.FromSlash(oldestKey)))
	recentSize := fileSize(t, filepath.Join(spoolDir, filepath.FromSlash(recentKeys[0]))) +
		fileSize(t, filepath.Join(spoolDir, filepath.FromSlash(recentKeys[1])))
	spoolCap := recentSize + 256<<10
	if spoolCap >= oldestSize+recentSize {
		t.Fatalf("seeded spool (%d bytes) does not exceed spool_max_bytes %d", oldestSize+recentSize, spoolCap)
	}

	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
proc_path: /proc
log_format: json
log_level: debug
sources:
  system:
    enabled: true
    interval: 200ms
storage:
  primary: s3
  chunk_interval: 10s
  s3:
    endpoint: %s
    bucket: %s
    prefix: %s
    spool_dir: %s
    spool_max_bytes: %d
`, dataDir, proxy.URL(), env.bucket, env.prefix, spoolDir, spoolCap))

	// The bounded waits below total 5.5 minutes in the worst case; keep the
	// context above that so a specific wait times out with its own message.
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	d := startSmob(t, ctx, "collect", "--config", cfg)
	waitFor(t, 60*time.Second, "first object in S3", func() bool {
		return len(listS3Keys(t, ctx, env.client, env.bucket, env.prefix+"/")) > 0
	})
	before := len(listS3Keys(t, ctx, env.client, env.bucket, env.prefix+"/"))

	proxy.Pause()
	waitFor(t, 90*time.Second, "a chunk in the spool", func() bool {
		return len(spoolChunkPaths(t, spoolDir)) > 0
	})
	spooled := spoolRelativeKeys(t, spoolDir)
	if len(spooled) == 0 {
		t.Fatal("no spooled keys captured")
	}
	select {
	case <-d.done:
		t.Fatalf("collect exited while the network was down: %v\n%s", d.waitErr, d.stderr.String())
	default:
	}

	proxy.Resume()
	waitFor(t, 2*time.Minute, "spool to drain", func() bool {
		return len(spoolChunkPaths(t, spoolDir)) == 0
	})
	waitFor(t, 30*time.Second, "replayed objects in S3", func() bool {
		keys := listS3Keys(t, ctx, env.client, env.bucket, env.prefix+"/")
		if len(keys) <= before {
			return false
		}
		set := make(map[string]struct{}, len(keys))
		for _, key := range keys {
			set[key] = struct{}{}
		}
		for _, rel := range spooled {
			if _, ok := set[env.prefix+"/"+rel]; !ok {
				return false
			}
		}
		return true
	})

	d.stop(t, 30*time.Second)

	// Startup trimming must be logged before the collector is ready, and so
	// before the first new chunk can be written or spooled.
	logs := d.stderr.String()
	trimLine := logLineIndex(t, logs, "dropped oldest spooled chunk over spool_max_bytes", oldestKey)
	readyLine := logLineIndex(t, logs, "collector ready", "")
	spooledLine := logLineIndex(t, logs, "upload failed; chunk spooled", "")
	if trimLine > readyLine || trimLine > spooledLine {
		t.Fatalf("spool trim at log line %d, want before collector ready (%d) and the first spooled chunk (%d)",
			trimLine, readyLine, spooledLine)
	}

	objectKeys := listS3Keys(t, ctx, env.client, env.bucket, env.prefix+"/")
	if slices.Contains(objectKeys, env.prefix+"/"+oldestKey) {
		t.Fatalf("trimmed spool chunk %s was uploaded to the bucket", oldestKey)
	}
	for _, key := range recentKeys {
		if !slices.Contains(objectKeys, env.prefix+"/"+key) {
			t.Fatalf("retained spool chunk %s was never replayed", key)
		}
	}

	out := runSmob(t, ctx, "query", "--config", cfg, "--since", "1h", "--format", "json")
	if out.ExitCode != 0 {
		t.Fatalf("query after drain exit = %d\nstderr:\n%s", out.ExitCode, out.Stderr)
	}
	samples := decodeJSONSamples(t, out.Stdout)
	if len(samples) == 0 {
		t.Fatal("query after drain returned no samples")
	}
	assertOrderedSamples(t, samples)
}

func TestS3QueryMergesSpool(t *testing.T) {
	env := requireS3(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dataDir := filepath.Join(t.TempDir(), "data")
	spoolDir := filepath.Join(dataDir, "spool")
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
    spool_dir: %s
`, dataDir, env.endpoint, env.bucket, env.prefix, spoolDir))

	if res := runSmob(t, ctx, "collect", "--once", "--config", cfg); res.ExitCode != 0 {
		t.Fatalf("collect --once exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	before := queryJSONSamples(t, ctx, cfg, "--since", "1h")
	if len(before) == 0 {
		t.Fatal("query before spool merge returned no samples")
	}

	keys := listS3Keys(t, ctx, env.client, env.bucket, env.prefix+"/")
	if len(keys) == 0 {
		t.Fatal("collect --once wrote no S3 objects")
	}
	key := keys[0]
	rel := strings.TrimPrefix(key, env.prefix+"/")

	out, err := env.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(env.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("download %s: %v", key, err)
	}
	data, readErr := io.ReadAll(out.Body)
	closeErr := out.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		t.Fatalf("read %s: %v", key, err)
	}

	spoolPath := filepath.Join(spoolDir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(spoolPath), 0o755); err != nil {
		t.Fatalf("create spool dir: %v", err)
	}
	if err := os.WriteFile(spoolPath, data, 0o644); err != nil {
		t.Fatalf("seed spool duplicate: %v", err)
	}

	after := queryJSONSamples(t, ctx, cfg, "--since", "1h")
	assertSameSamples(t, after, before)

	// A spool-only chunk must be visible even though the bucket has no object
	// for this host.
	start := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
	sample := wireSample{
		Timestamp: start,
		Metric:    "test.spool.bytes",
		Value:     7,
		Labels:    map[string]string{"host": "spool-host", "scope": "system"},
	}
	writeChunk(t, spoolDir, "spool-host", "system", start, start.Add(time.Minute), []wireSample{sample})

	spoolOnly := queryJSONSamples(t, ctx, cfg, "--host", "spool-host", "--scope", "system", "--since", "1h")
	if len(spoolOnly) != 1 {
		t.Fatalf("spool-only query returned %d samples, want 1", len(spoolOnly))
	}
	if spoolOnly[0].Metric != sample.Metric || spoolOnly[0].Value != sample.Value {
		t.Fatalf("spool-only sample = %+v, want %+v", spoolOnly[0], sample)
	}
}

func TestS3CheckCreatesSpool(t *testing.T) {
	env := requireS3(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dataDir := filepath.Join(t.TempDir(), "data")
	spoolDir := filepath.Join(t.TempDir(), "nested", "spool")
	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
sources:
  system:
    enabled: true
storage:
  primary: s3
  s3:
    endpoint: %s
    bucket: %s
    prefix: %s
    spool_dir: %s
`, dataDir, env.endpoint, env.bucket, env.prefix, spoolDir))

	res := runSmob(t, ctx, "collect", "--dry-run", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("dry-run exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	info, err := os.Stat(spoolDir)
	if err != nil {
		t.Fatalf("spool was not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("%s is not a directory", spoolDir)
	}
	assertNoSpoolTemps(t, spoolDir)

	// A spool path that is a regular file is a persistent store failure.
	badSpool := filepath.Join(t.TempDir(), "spool")
	if err := os.WriteFile(badSpool, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("create bad spool: %v", err)
	}
	badCfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
sources:
  system:
    enabled: true
storage:
  primary: s3
  s3:
    endpoint: %s
    bucket: %s
    prefix: %s
    spool_dir: %s
`, dataDir, env.endpoint, env.bucket, env.prefix, badSpool))

	res = runSmob(t, ctx, "collect", "--dry-run", "--config", badCfg)
	if res.ExitCode != 3 {
		t.Fatalf("bad spool exit = %d, want 3\nstdout:\n%s\nstderr:\n%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "persistent store failure") {
		t.Fatalf("stderr does not name a persistent store failure:\n%s", res.Stderr)
	}
}

func queryJSONSamples(t *testing.T, ctx context.Context, cfg string, args ...string) []wireSample {
	t.Helper()
	args = append([]string{"query", "--config", cfg, "--format", "json"}, args...)
	out := runSmob(t, ctx, args...)
	if out.ExitCode != 0 {
		t.Fatalf("query exit = %d\nstderr:\n%s", out.ExitCode, out.Stderr)
	}
	return decodeJSONSamples(t, out.Stdout)
}

func spoolChunkPaths(t *testing.T, spoolDir string) []string {
	t.Helper()
	var paths []string
	err := filepath.WalkDir(spoolDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".jsonl.gz") {
			paths = append(paths, path)
		}
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("walk spool %s: %v", spoolDir, err)
	}
	return paths
}

func spoolRelativeKeys(t *testing.T, spoolDir string) []string {
	t.Helper()
	paths := spoolChunkPaths(t, spoolDir)
	keys := make([]string, 0, len(paths))
	for _, path := range paths {
		rel, err := filepath.Rel(spoolDir, path)
		if err != nil {
			t.Fatalf("relative spool path %s: %v", path, err)
		}
		keys = append(keys, filepath.ToSlash(rel))
	}
	slices.Sort(keys)
	return keys
}

func assertNoSpoolTemps(t *testing.T, spoolDir string) {
	t.Helper()
	err := filepath.WalkDir(spoolDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".spool-") || strings.HasPrefix(d.Name(), ".check-") {
			t.Errorf("left temp file behind: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk spool %s: %v", spoolDir, err)
	}
}

// TestS3SpoolUsesFailedCandidate proves a spooled chunk is named after the
// candidate the PUT path was attempting, not the base key. The base key of the
// next two minutes is pre-created in the bucket, and the proxy drops every
// request for the -1 suffix, so the retry budget expires while attempting -1.
// The chunk must land in the spool as -1; a base-key spool file would collide
// with the existing object on replay and never become queryable.
func TestS3SpoolUsesFailedCandidate(t *testing.T) {
	env := requireS3(t)
	proxy := newFaultProxy(t, env.endpoint)

	dataDir := filepath.Join(t.TempDir(), "data")
	spoolDir := filepath.Join(dataDir, "spool")
	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
proc_path: /proc
log_format: json
log_level: debug
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
    spool_dir: %s
`, dataDir, proxy.URL(), env.bucket, env.prefix, spoolDir))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// With a one-second chunk interval the collector writes the second it
	// sampled, so occupy the base key for every second this run can reach.
	start := time.Now().UTC().Truncate(time.Second)
	bases := make(map[string]struct{}, 120)
	for i := 0; i < 120; i++ {
		rel := chunkPath("test-host", "system", start.Add(time.Duration(i)*time.Second))
		putS3Object(t, ctx, env.client, env.bucket, env.prefix+"/"+rel, []byte("occupied"))
		bases[rel] = struct{}{}
	}

	proxy.DropRequestsContaining("-1.jsonl.gz")

	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 0 {
		t.Fatalf("collect --once exit = %d, want 0 because the chunk is spooled\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	spooled := spoolRelativeKeys(t, spoolDir)
	if len(spooled) != 1 {
		t.Fatalf("spool holds %v, want exactly one chunk", spooled)
	}
	spooledKey := spooled[0]
	if !strings.HasSuffix(spooledKey, "-1.jsonl.gz") {
		t.Fatalf("spool key = %q, want the -1 candidate the PUT path failed on", spooledKey)
	}
	baseKey := strings.TrimSuffix(spooledKey, "-1.jsonl.gz") + ".jsonl.gz"
	if _, ok := bases[baseKey]; !ok {
		t.Fatalf("spool key %q does not derive from a base key this test occupied", spooledKey)
	}

	entry := findLogEntry(t, res.Stderr, "upload failed; chunk spooled")
	if entry["key"] != spooledKey || entry["spool_key"] != spooledKey {
		t.Fatalf("warn fields key=%v spool_key=%v, want both %q", entry["key"], entry["spool_key"], spooledKey)
	}

	for _, key := range listS3Keys(t, ctx, env.client, env.bucket, env.prefix+"/") {
		if strings.HasSuffix(key, "-1.jsonl.gz") {
			t.Fatalf("bucket holds %q, want the suffix only in the spool", key)
		}
	}
}

// TestS3SpoolReplayIdenticalObject seeds the bucket with the exact bytes the
// spool holds under the same key. Replay must treat the object as already
// uploaded: leave its bytes unchanged, delete the spool entry, and query must
// emit the window once instead of duplicating it.
func TestS3SpoolReplayIdenticalObject(t *testing.T) {
	env := requireS3(t)

	dataDir := filepath.Join(t.TempDir(), "data")
	spoolDir := filepath.Join(dataDir, "spool")
	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
proc_path: /proc
log_format: json
log_level: debug
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
    spool_dir: %s
`, dataDir, env.endpoint, env.bucket, env.prefix, spoolDir))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	start := time.Now().UTC().Truncate(time.Second).Add(-5 * time.Minute)
	key := seedSpoolChunk(t, spoolDir, start, "identical")
	body, err := os.ReadFile(filepath.Join(spoolDir, filepath.FromSlash(key)))
	if err != nil {
		t.Fatalf("read seeded spool chunk: %v", err)
	}
	target := env.prefix + "/" + key
	putS3Object(t, ctx, env.client, env.bucket, target, body)

	d := startSmob(t, ctx, "collect", "--config", cfg)
	waitFor(t, 60*time.Second, "identical spool entry replayed", func() bool {
		return !slices.Contains(spoolRelativeKeys(t, spoolDir), key)
	})

	if got := getS3Object(t, ctx, env.client, env.bucket, target); !bytes.Equal(got, body) {
		t.Fatalf("identical object changed during replay: got %d bytes, want the original %d", len(got), len(body))
	}

	samples := queryJSONSamples(t, ctx, cfg, "--host", "test-host", "--scope", "system", "--since", "1h")
	seen := 0
	for _, sample := range samples {
		if sample.Metric == "test.spool.seed" && sample.Timestamp.Equal(start) {
			seen++
		}
	}
	if seen != 1 {
		t.Fatalf("query emitted the replayed window %d times, want once", seen)
	}

	d.stop(t, 30*time.Second)
}

// TestS3SpoolQueryPrefersBucket seeds the same relative key in the bucket and
// the spool with different bytes. Query must read the bucket copy once and never
// emit the pending local copy as a second window.
func TestS3SpoolQueryPrefersBucket(t *testing.T) {
	env := requireS3(t)

	dataDir := filepath.Join(t.TempDir(), "data")
	spoolDir := filepath.Join(dataDir, "spool")
	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
sources:
  system:
    enabled: true
storage:
  primary: s3
  s3:
    endpoint: %s
    bucket: %s
    prefix: %s
    spool_dir: %s
`, dataDir, env.endpoint, env.bucket, env.prefix, spoolDir))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	start := time.Now().UTC().Truncate(time.Second).Add(-2 * time.Minute)
	end := start.Add(time.Second)
	labels := map[string]string{"host": "merge-host", "scope": "system"}
	bucketSample := wireSample{Timestamp: start, Metric: "test.spool.merge", Value: 1, Labels: labels}
	spoolSample := wireSample{Timestamp: start, Metric: "test.spool.merge", Value: 2, Labels: labels}

	staging := t.TempDir()
	writeChunk(t, staging, "merge-host", "system", start, end, []wireSample{bucketSample})
	staged := chunkPaths(t, staging)
	if len(staged) != 1 {
		t.Fatalf("staged %d chunks, want 1", len(staged))
	}
	rel, err := filepath.Rel(staging, staged[0])
	if err != nil {
		t.Fatalf("relative staged chunk: %v", err)
	}
	rel = filepath.ToSlash(rel)
	body, err := os.ReadFile(staged[0])
	if err != nil {
		t.Fatalf("read staged chunk: %v", err)
	}
	putS3Object(t, ctx, env.client, env.bucket, env.prefix+"/"+rel, body)
	writeChunk(t, spoolDir, "merge-host", "system", start, end, []wireSample{spoolSample})

	samples := queryJSONSamples(t, ctx, cfg, "--host", "merge-host", "--scope", "system", "--since", "1h")
	if len(samples) != 1 {
		t.Fatalf("query returned %d samples, want only the bucket copy once", len(samples))
	}
	if samples[0].Value != bucketSample.Value {
		t.Fatalf("query emitted value %v, want the bucket copy %v", samples[0].Value, bucketSample.Value)
	}
}

// TestS3SpoolStopWithPendingReplay sends SIGTERM while a spooled upload is
// failing against a partitioned endpoint. The process must exit 0, leave the
// pending file for the next run, and a restarted collect must drain it.
func TestS3SpoolStopWithPendingReplay(t *testing.T) {
	env := requireS3(t)
	proxy := newFaultProxy(t, env.endpoint)

	dataDir := filepath.Join(t.TempDir(), "data")
	spoolDir := filepath.Join(dataDir, "spool")
	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
proc_path: /proc
log_format: json
log_level: debug
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
    spool_dir: %s
`, dataDir, proxy.URL(), env.bucket, env.prefix, spoolDir))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	d := startSmob(t, ctx, "collect", "--config", cfg)
	waitFor(t, 60*time.Second, "first object in S3", func() bool {
		return len(listS3Keys(t, ctx, env.client, env.bucket, env.prefix+"/")) > 0
	})

	proxy.Pause()
	waitFor(t, 90*time.Second, "a chunk in the spool", func() bool {
		return len(spoolChunkPaths(t, spoolDir)) > 0
	})
	waitFor(t, 90*time.Second, "replay to fail against the partitioned endpoint", func() bool {
		return strings.Contains(d.stderr.String(), "replay spooled chunk")
	})
	pending := spoolRelativeKeys(t, spoolDir)
	if len(pending) == 0 {
		t.Fatal("no pending spool keys captured before SIGTERM")
	}

	d.stopTerm(t, 45*time.Second)

	remaining := spoolRelativeKeys(t, spoolDir)
	for _, key := range pending {
		if !slices.Contains(remaining, key) {
			t.Fatalf("pending spool entry %s did not survive shutdown; spool holds %v", key, remaining)
		}
	}

	proxy.Resume()
	next := startSmob(t, ctx, "collect", "--config", cfg)
	waitFor(t, 2*time.Minute, "next run to drain the spool", func() bool {
		return len(spoolChunkPaths(t, spoolDir)) == 0
	})
	next.stopTerm(t, 45*time.Second)
}

// TestS3SpoolOversizedChunkLogged drives collection into a spool whose cap is
// smaller than one chunk. The freshly written entry is protected, so the spool
// stays above the cap and reports that at error instead of discarding its only
// copy.
func TestS3SpoolOversizedChunkLogged(t *testing.T) {
	env := requireS3(t)
	proxy := newFaultProxy(t, env.endpoint)

	dataDir := filepath.Join(t.TempDir(), "data")
	spoolDir := filepath.Join(dataDir, "spool")
	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
proc_path: /proc
log_format: json
log_level: debug
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
    spool_dir: %s
    spool_max_bytes: 64
`, dataDir, proxy.URL(), env.bucket, env.prefix, spoolDir))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	d := startSmob(t, ctx, "collect", "--config", cfg)
	waitFor(t, 60*time.Second, "first object in S3", func() bool {
		return len(listS3Keys(t, ctx, env.client, env.bucket, env.prefix+"/")) > 0
	})

	proxy.Pause()
	const overCap = "spool remains over spool_max_bytes after dropping unprotected chunks"
	waitFor(t, 90*time.Second, "over-cap spool to be logged", func() bool {
		return strings.Contains(d.stderr.String(), overCap) && len(spoolChunkPaths(t, spoolDir)) > 0
	})

	entry := findLogEntry(t, d.stderr.String(), overCap)
	if entry["level"] != "ERROR" {
		t.Fatalf("over-cap log level = %v, want ERROR", entry["level"])
	}
	spoolBytes, ok := entry["spool_bytes"].(float64)
	if !ok {
		t.Fatalf("over-cap log has no spool_bytes field: %v", entry)
	}
	maxBytes, ok := entry["spool_max_bytes"].(float64)
	if !ok {
		t.Fatalf("over-cap log has no spool_max_bytes field: %v", entry)
	}
	if spoolBytes <= maxBytes {
		t.Fatalf("logged spool_bytes %v does not exceed spool_max_bytes %v", spoolBytes, maxBytes)
	}
	if len(spoolChunkPaths(t, spoolDir)) == 0 {
		t.Fatal("oversized chunk was dropped from the spool")
	}

	d.stop(t, 45*time.Second)
}

// chunkPath returns the documented relative key for a one-second chunk window
// starting at start.
func chunkPath(host, scope string, start time.Time) string {
	const layout = "20060102T150405Z"
	start = start.UTC()
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s/%s-%s.jsonl.gz",
		host, scope,
		start.Format("2006"), start.Format("01"), start.Format("02"), start.Format("15"),
		start.Format(layout), start.Add(time.Second).Format(layout),
	)
}

// putS3Object seeds one object directly with the test's own client.
func putS3Object(t *testing.T, ctx context.Context, client *awss3.Client, bucket, key string, body []byte) {
	t.Helper()
	_, err := client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
	})
	if err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}
}

// getS3Object downloads one object's bytes with the test's own client.
func getS3Object(t *testing.T, ctx context.Context, client *awss3.Client, bucket, key string) []byte {
	t.Helper()

	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("download %s: %v", key, err)
	}
	data, readErr := io.ReadAll(out.Body)
	closeErr := out.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	return data
}

// findLogEntry returns the fields of the first JSON log line whose msg matches.
func findLogEntry(t *testing.T, stderr, msg string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(stderr, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if entry["msg"] == msg {
			return entry
		}
	}
	t.Fatalf("no %q log line in stderr:\n%s", msg, stderr)
	return nil
}

// logLineIndex returns the line number of the first JSON log line whose msg
// matches and, when key is non-empty, whose key field matches. It proves
// ordering between startup work and collection writes from one log stream.
func logLineIndex(t *testing.T, stderr, msg, key string) int {
	t.Helper()
	for i, line := range strings.Split(stderr, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if entry["msg"] != msg {
			continue
		}
		if key != "" && entry["key"] != key {
			continue
		}
		return i
	}
	t.Fatalf("no %q log line (key %q) in stderr:\n%s", msg, key, stderr)
	return -1
}

// seedSpoolChunk writes one valid chunk directly into the spool and returns its
// relative key.
func seedSpoolChunk(t *testing.T, spoolDir string, start time.Time, label string) string {
	t.Helper()
	sample := wireSample{
		Timestamp: start,
		Metric:    "test.spool.seed",
		Value:     1,
		Labels:    map[string]string{"host": "test-host", "scope": "system", "seed": label},
	}
	writeChunk(t, spoolDir, "test-host", "system", start, start.Add(time.Second), []wireSample{sample})
	return chunkPath("test-host", "system", start)
}

// incompressibleLabel returns n random hex characters, so a seeded chunk stays
// large after gzip and can exceed a spool cap.
func incompressibleLabel(t *testing.T, n int) string {
	t.Helper()
	buf := make([]byte, (n+1)/2)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("random label: %v", err)
	}
	return hex.EncodeToString(buf)[:n]
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}
