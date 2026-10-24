//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
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
		t.Skipf("network-partition proxy requires an http:// S3 endpoint; got %q", endpoint)
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
		_, _ = io.Copy(upstream, client)
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

func TestS3SpoolDrain(t *testing.T) {
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
    interval: 200ms
storage:
  primary: s3
  chunk_interval: 10s
  s3:
    endpoint: %s
    bucket: %s
    prefix: %s
    spool_dir: %s
`, dataDir, proxy.URL(), env.bucket, env.prefix, spoolDir))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
