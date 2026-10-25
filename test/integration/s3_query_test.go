//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestS3QueryConcurrentMerge seeds enough objects across hosts, scopes, and
// hours that the bounded GET pool runs several workers per hour, then checks
// that ordering, Limit, filters, and bucket/spool dedup still hold.
func TestS3QueryConcurrentMerge(t *testing.T) {
	env := requireS3(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const (
		hostA           = "ittest-host-a"
		hostB           = "ittest-host-b"
		chunksPerHour   = 12
		samplesPerChunk = 3
	)
	type seedTarget struct{ host, scope string }
	targets := []seedTarget{
		{host: hostA, scope: "system"},
		{host: hostA, scope: "process"},
		{host: hostB, scope: "system"},
	}

	// Two completed hours, so a Limit can cross an hour boundary without
	// depending on the current wall clock. The query range leaves several hours
	// of margin below base, so a run that straddles an hour boundary cannot
	// shift the range past the first seeded sample.
	const since = "5h"
	base := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	var seeded []wireSample
	for h := 0; h < 2; h++ {
		hour := base.Add(time.Duration(h) * time.Hour)
		for targetIndex, target := range targets {
			for chunkIndex := 0; chunkIndex < chunksPerHour; chunkIndex++ {
				start := hour.Add(time.Duration(chunkIndex) * 5 * time.Minute)
				end := start.Add(5 * time.Minute)

				samples := make([]wireSample, 0, samplesPerChunk)
				for i := 0; i < samplesPerChunk; i++ {
					ordinal := h*1000 + targetIndex*100 + chunkIndex*samplesPerChunk + i
					samples = append(samples, seededSample(start.Add(time.Duration(i)*10*time.Second), target.host, target.scope, ordinal))
				}
				seedS3Chunk(t, ctx, env, target.host, target.scope, start, end, samples)
				seeded = append(seeded, samples...)
			}
		}
	}
	sortWireSamples(seeded)

	// An hour outside the queried range must never appear.
	stale := base.Add(-6 * time.Hour)
	seedS3Chunk(t, ctx, env, hostA, "system", stale, stale.Add(5*time.Minute), []wireSample{{
		Timestamp: stale.Add(time.Second),
		Metric:    "test.stale.bytes",
		Value:     1,
		Labels:    map[string]string{"host": hostA, "scope": "system"},
	}})

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

	// The bucket already owns this key with different bytes. The spool copy
	// must lose to the bucket, so its metric may not appear in the result.
	duplicateStart := base
	writeChunk(t, spoolDir, hostA, "system", duplicateStart, duplicateStart.Add(5*time.Minute), []wireSample{{
		Timestamp: duplicateStart.Add(time.Second),
		Metric:    "test.spool.duplicate",
		Value:     1,
		Labels:    map[string]string{"host": hostA, "scope": "system"},
	}})

	full := queryJSONSamples(t, ctx, cfg, "--since", since)
	assertSameSamples(t, full, seeded)
	assertOrderedSamples(t, full)
	for _, sample := range full {
		if sample.Metric == "test.spool.duplicate" || sample.Metric == "test.stale.bytes" {
			t.Fatalf("query returned %q, which is either superseded by the bucket or outside the range", sample.Metric)
		}
	}

	// The first hour holds 3 targets x 12 chunks x 3 samples = 108 samples.
	// 17 stays inside it; 150 crosses into the second hour.
	for _, limit := range []int{17, 150} {
		got := queryJSONSamples(t, ctx, cfg, "--since", since, "--limit", fmt.Sprintf("%d", limit))
		assertSameSamples(t, got, seeded[:limit])
	}

	metric := queryJSONSamples(t, ctx, cfg, "--since", since, "--metric", "system.cpu.percent")
	var wantMetric []wireSample
	for _, sample := range seeded {
		if sample.Metric == "system.cpu.percent" {
			wantMetric = append(wantMetric, sample)
		}
	}
	assertSameSamples(t, metric, wantMetric)

	host := queryJSONSamples(t, ctx, cfg, "--since", since, "--host", hostA)
	var wantHost []wireSample
	for _, sample := range seeded {
		if sample.Labels["host"] == hostA {
			wantHost = append(wantHost, sample)
		}
	}
	assertSameSamples(t, host, wantHost)

	process := queryJSONSamples(t, ctx, cfg, "--since", since, "--host", hostA, "--scope", "process")
	var wantProcess []wireSample
	for _, sample := range seeded {
		if sample.Labels["host"] == hostA && sample.Labels["scope"] == "process" {
			wantProcess = append(wantProcess, sample)
		}
	}
	assertSameSamples(t, process, wantProcess)

	system := queryJSONSamples(t, ctx, cfg, "--since", since, "--scope", "system")
	var wantSystem []wireSample
	for _, sample := range seeded {
		if sample.Labels["scope"] == "system" {
			wantSystem = append(wantSystem, sample)
		}
	}
	assertSameSamples(t, system, wantSystem)
}

// TestS3QueryWindowOrder seeds the same chunk windows for two hosts, so each
// window group holds more than one chunk. It checks that the window-ordered
// fetch keeps samples ordered across hosts, that --limit can stop inside a
// later window, and that --until exactly on a window boundary excludes the
// window starting there.
func TestS3QueryWindowOrder(t *testing.T) {
	env := requireS3(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const (
		hostA = "ittest-window-a"
		hostB = "ittest-window-b"
	)
	hosts := []string{hostA, hostB}

	// One fixed hour in the past. The last window starts on the hour boundary,
	// so a limit crossing it also crosses an hour boundary.
	base := time.Now().UTC().Truncate(time.Hour).Add(-4 * time.Hour)
	const window = 5 * time.Minute
	starts := []time.Time{base, base.Add(window), base.Add(2 * window), base.Add(time.Hour)}

	var seeded []wireSample
	for windowIndex, start := range starts {
		for hostIndex, host := range hosts {
			samples := []wireSample{
				windowSample(start.Add(time.Second), host, windowIndex, hostIndex, 1),
				windowSample(start.Add(3*time.Second), host, windowIndex, hostIndex, 3),
			}
			seedS3Chunk(t, ctx, env, host, "system", start, start.Add(window), samples)
			seeded = append(seeded, samples...)
		}
	}
	sortWireSamples(seeded)

	// Every window holds four samples: two hosts at the same second, ordered by
	// the host label. Five samples therefore stop inside the second window.
	const perWindow = 4
	fullUntil := base.Add(3 * window)
	boundaryUntil := base.Add(2 * window)

	dataDir := filepath.Join(t.TempDir(), "data")
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
`, dataDir, env.endpoint, env.bucket, env.prefix, filepath.Join(dataDir, "spool")))

	sinceArg := base.Format(time.RFC3339)

	// The full bounded range includes the first three windows and must not reach
	// the window starting on the hour boundary.
	full := queryJSONSamples(t, ctx, cfg, "--since", sinceArg, "--until", fullUntil.Format(time.RFC3339))
	assertSameSamples(t, full, seeded[:3*perWindow])
	assertOrderedSamples(t, full)
	for i := 1; i < len(full); i++ {
		if full[i].Timestamp.Equal(full[i-1].Timestamp) &&
			full[i].Labels["host"] < full[i-1].Labels["host"] {
			t.Fatalf("sample %d host %s sorts before %s at the same timestamp",
				i, full[i].Labels["host"], full[i-1].Labels["host"])
		}
	}

	// --until is half-open: a sample at exactly the boundary belongs to the next
	// window and must not appear.
	boundary := queryJSONSamples(t, ctx, cfg,
		"--since", sinceArg,
		"--until", boundaryUntil.Format(time.RFC3339))
	assertSameSamples(t, boundary, seeded[:2*perWindow])
	for _, sample := range boundary {
		if !sample.Timestamp.Before(boundaryUntil) {
			t.Fatalf("query returned sample at %s, not before --until boundary %s",
				sample.Timestamp, boundaryUntil.Format(time.RFC3339))
		}
	}

	// A limit inside the second window proves the fetch stopped between window
	// groups without losing ordering across hosts.
	limited := queryJSONSamples(t, ctx, cfg,
		"--since", sinceArg,
		"--until", fullUntil.Format(time.RFC3339),
		"--limit", fmt.Sprintf("%d", perWindow+1))
	assertSameSamples(t, limited, seeded[:perWindow+1])
	assertOrderedSamples(t, limited)

	// The window on the hour boundary is still reachable and ordered.
	crossHour := queryJSONSamples(t, ctx, cfg,
		"--since", sinceArg,
		"--until", base.Add(2*time.Hour).Format(time.RFC3339))
	assertSameSamples(t, crossHour, seeded)
	assertOrderedSamples(t, crossHour)
}

// TestS3QueryEmptyPrefix checks that a bounded query over a prefix with no
// objects exits successfully with an empty result instead of failing a listing.
// Every output format must still produce its valid empty document.
func TestS3QueryEmptyPrefix(t *testing.T) {
	env := requireS3(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dataDir := filepath.Join(t.TempDir(), "data")
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
`, dataDir, env.endpoint, env.bucket, path.Join(env.prefix, "empty"), filepath.Join(dataDir, "spool")))

	now := time.Now().UTC()
	rangeArgs := []string{
		"--since", now.Add(-2 * time.Hour).Format(time.RFC3339),
		"--until", now.Add(-time.Hour).Format(time.RFC3339),
	}

	for _, format := range []string{"table", "json", "jsonl", "csv"} {
		t.Run(format, func(t *testing.T) {
			args := append([]string{"query", "--config", cfg, "--format", format}, rangeArgs...)
			res := runSmob(t, ctx, args...)
			if res.ExitCode != 0 {
				t.Fatalf("empty prefix %s query exit = %d\nstderr:\n%s", format, res.ExitCode, res.Stderr)
			}

			switch format {
			case "json":
				if res.Stdout != "[]\n" {
					t.Fatalf("empty prefix json = %q, want %q", res.Stdout, "[]\n")
				}
			case "jsonl":
				if res.Stdout != "" {
					t.Fatalf("empty prefix jsonl = %q, want empty", res.Stdout)
				}
			case "csv":
				if got := decodeCSVSamples(t, res.Stdout); len(got) != 0 {
					t.Fatalf("empty prefix csv returned %d samples: %+v", len(got), got)
				}
			case "table":
				if got := tableRows(res.Stdout); got != 0 {
					t.Fatalf("empty prefix table returned %d rows:\n%s", got, res.Stdout)
				}
				header := strings.Fields(strings.Split(strings.TrimSpace(res.Stdout), "\n")[0])
				if len(header) != 5 {
					t.Fatalf("empty prefix table header = %v, want five columns", header)
				}
			}
		})
	}
}

// TestS3QueryLimitOneSkipsLaterWindows proves Limit stops the window-ordered
// fetch as soon as the first matching sample is yielded. Only the first window
// holds readable objects; the two later windows are corrupt, so a query that
// fetched them would fail instead of returning the globally earliest sample.
func TestS3QueryLimitOneSkipsLaterWindows(t *testing.T) {
	env := requireS3(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const (
		hostA  = "ittest-limit-a"
		hostB  = "ittest-limit-b"
		window = 5 * time.Minute
	)
	base := time.Now().UTC().Truncate(time.Hour).Add(-4 * time.Hour)
	starts := []time.Time{base, base.Add(window), base.Add(2 * window)}

	// The first window holds both hosts; the earliest sample across the whole
	// range is hostA at base+1s.
	earliest := windowSample(base.Add(time.Second), hostA, 0, 0, 1)
	seedS3Chunk(t, ctx, env, hostA, "system", starts[0], starts[0].Add(window), []wireSample{
		earliest,
		windowSample(base.Add(4*time.Second), hostA, 0, 0, 4),
	})
	seedS3Chunk(t, ctx, env, hostB, "system", starts[0], starts[0].Add(window), []wireSample{
		windowSample(base.Add(2*time.Second), hostB, 0, 1, 2),
		windowSample(base.Add(5*time.Second), hostB, 0, 1, 5),
	})

	// Later windows are unreadable. Listing them still succeeds because their
	// names are valid; only opening the body fails.
	var corrupt []string
	for _, start := range starts[1:] {
		key := s3ChunkKey(env.prefix, hostA, "system", start, start.Add(window))
		putS3Object(t, ctx, env.client, env.bucket, key, []byte("not gzip"))
		corrupt = append(corrupt, key)
	}

	dataDir := filepath.Join(t.TempDir(), "data")
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
`, dataDir, env.endpoint, env.bucket, env.prefix, filepath.Join(dataDir, "spool")))

	since := base.Format(time.RFC3339)
	until := base.Add(3 * window).Format(time.RFC3339)

	got := queryJSONSamples(t, ctx, cfg, "--since", since, "--until", until, "--limit", "1")
	assertSameSamples(t, got, []wireSample{earliest})

	// The corruption is real: without a limit the query reaches the second
	// window and fails, naming the object it could not decode.
	all := runSmob(t, ctx, "query", "--config", cfg,
		"--since", since, "--until", until, "--limit", "0", "--format", "json")
	if all.ExitCode != 1 {
		t.Fatalf("unlimited query exit = %d, want 1\nstderr:\n%s", all.ExitCode, all.Stderr)
	}
	if !strings.Contains(all.Stderr, path.Base(corrupt[0])) {
		t.Fatalf("stderr does not name the corrupt later-window object %q:\n%s", path.Base(corrupt[0]), all.Stderr)
	}
}

// TestS3QueryCancelDuringFetch proves SIGINT while an S3 query is still reading
// windows returns promptly with a non-zero exit and a complete JSON document.
func TestS3QueryCancelDuringFetch(t *testing.T) {
	env := requireS3(t)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	const (
		host      = "ittest-cancel"
		windows   = 60
		perWindow = 5_000
	)
	base := time.Now().UTC().Truncate(time.Hour).Add(-4 * time.Hour)
	for w := 0; w < windows; w++ {
		start := base.Add(time.Duration(w) * 5 * time.Minute)
		samples := make([]wireSample, perWindow)
		for i := range samples {
			samples[i] = wireSample{
				Timestamp: start.Add(time.Duration(i) * time.Millisecond),
				Metric:    "system.cpu.percent",
				Value:     float64(i),
				Labels:    map[string]string{"host": host, "scope": "system", "cpu": "all"},
			}
		}
		seedS3Chunk(t, ctx, env, host, "system", start, start.Add(5*time.Minute), samples)
	}

	dataDir := filepath.Join(t.TempDir(), "data")
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
`, dataDir, env.endpoint, env.bucket, env.prefix, filepath.Join(dataDir, "spool")))

	args := []string{
		"query", "--config", cfg,
		"--since", base.Format(time.RFC3339),
		"--until", base.Add(6 * time.Hour).Format(time.RFC3339),
		"--format", "json", "--limit", "0",
	}

	cmd := exec.CommandContext(ctx, smobBin, args...)
	cmd.WaitDelay = 5 * time.Second
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start query: %v", err)
	}

	var output bytes.Buffer
	sawSample := make(chan struct{})
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		reader := bufio.NewReader(stdout)
		for {
			line, readErr := reader.ReadString('\n')
			output.WriteString(line)
			if strings.HasPrefix(line, "{") {
				select {
				case <-sawSample:
				default:
					close(sawSample)
				}
			}
			if readErr != nil {
				return
			}
		}
	}()

	select {
	case <-sawSample:
	case <-time.After(60 * time.Second):
		_ = cmd.Process.Kill()
		<-readDone
		_ = cmd.Wait()
		t.Fatalf("query wrote no sample before the timeout\nstderr:\n%s", stderr.String())
	}

	signalled := time.Now()
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signal query: %v", err)
	}
	select {
	case <-readDone:
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		<-readDone
		t.Fatalf("query did not stop after SIGINT\nstderr:\n%s", stderr.String())
	}
	if elapsed := time.Since(signalled); elapsed > 15*time.Second {
		t.Fatalf("query took %s to exit after SIGINT", elapsed)
	}

	exitErr := cmd.Wait()
	var execErr *exec.ExitError
	if !errors.As(exitErr, &execErr) {
		t.Fatalf("query exit = %v, want a non-zero exit\nstderr:\n%s", exitErr, stderr.String())
	}
	if execErr.ExitCode() != 1 {
		t.Fatalf("exit = %d, want 1\nstderr:\n%s", execErr.ExitCode(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "query interrupted") {
		t.Fatalf("stderr does not report the interruption:\n%s", stderr.String())
	}
	if !json.Valid(output.Bytes()) {
		t.Fatalf("stdout is not a complete JSON document after cancellation:\n%s", output.String())
	}
	decoded := decodeJSONSamples(t, output.String())
	if len(decoded) == 0 || len(decoded) >= windows*perWindow {
		t.Fatalf("cancelled query returned %d samples, want a partial result", len(decoded))
	}
}

// TestS3QueryMissingBucketFailsLoudly proves a query against a bucket that does
// not exist is an error, not an empty result.
func TestS3QueryMissingBucketFailsLoudly(t *testing.T) {
	env := requireS3(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	name := strings.Trim(strings.ToLower(env.bucket), "-.")
	if len(name) > 24 {
		name = name[:24]
	}
	missing := fmt.Sprintf("%s-missing-%d", name, time.Now().UnixNano())

	dataDir := filepath.Join(t.TempDir(), "data")
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
`, dataDir, env.endpoint, missing, env.prefix, filepath.Join(dataDir, "spool")))

	now := time.Now().UTC()
	res := runSmob(t, ctx, "query", "--config", cfg,
		"--since", now.Add(-time.Hour).Format(time.RFC3339),
		"--until", now.Format(time.RFC3339),
		"--format", "json")
	// A missing bucket is a persistent store failure (exit 3), not a runtime
	// error: no configuration change to core dump. The important part is that
	// it fails loudly instead of printing an empty result.
	if res.ExitCode != 3 {
		t.Fatalf("missing bucket query exit = %d, want 3\nstdout:\n%s\nstderr:\n%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stderr, missing) {
		t.Fatalf("stderr does not name the missing bucket %q:\n%s", missing, res.Stderr)
	}
	if res.Stdout != "" && !json.Valid([]byte(res.Stdout)) {
		t.Fatalf("stdout is not a complete JSON document after a missing-bucket failure:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "{") {
		t.Fatalf("missing bucket returned samples:\n%s", res.Stdout)
	}
}

// windowSample builds one sample for the cross-host ordering check. Both hosts
// use the same metric and timestamps, so the host label is the tie-break.
func windowSample(timestamp time.Time, host string, windowIndex, hostIndex, offset int) wireSample {
	return wireSample{
		Timestamp: timestamp,
		Metric:    "system.cpu.percent",
		Value:     float64(windowIndex*100 + hostIndex*10 + offset),
		Labels:    map[string]string{"host": host, "scope": "system", "cpu": "all"},
	}
}

// seededSample builds one deterministic sample for a host and scope. Values only
// need to be distinct enough to identify the sample; ordering does not depend on
// them.
func seededSample(timestamp time.Time, host, scope string, ordinal int) wireSample {
	labels := map[string]string{"host": host, "scope": scope}
	switch scope {
	case "process":
		labels["pid"] = "4242"
		labels["comm"] = "seed"
	}
	metric := "system.memory.used_bytes"
	if ordinal%2 == 0 {
		metric = "system.cpu.percent"
		if scope == "system" {
			labels["cpu"] = "all"
		}
	}
	return wireSample{Timestamp: timestamp, Metric: metric, Value: float64(ordinal), Labels: labels}
}

// seedS3Chunk writes one encoded chunk directly under the test prefix. Seeding
// through the SDK is arrangement; smob itself is still driven as a black box.
func seedS3Chunk(t *testing.T, ctx context.Context, env s3TestEnv, host, scope string, start, end time.Time, samples []wireSample) {
	t.Helper()
	start, end = start.UTC().Truncate(time.Second), end.UTC().Truncate(time.Second)

	var body bytes.Buffer
	gz := gzip.NewWriter(&body)
	enc := json.NewEncoder(gz)
	for _, sample := range samples {
		if err := enc.Encode(sample); err != nil {
			t.Fatalf("encode seeded sample: %v", err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("close seeded gzip: %v", err)
	}

	key := s3ChunkKey(env.prefix, host, scope, start, end)
	if _, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(env.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body.Bytes()),
	}); err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}
}

// s3ChunkKey returns the documented object key for one chunk under prefix.
func s3ChunkKey(prefix, host, scope string, start, end time.Time) string {
	start, end = start.UTC().Truncate(time.Second), end.UTC().Truncate(time.Second)
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s/%s/%s-%s.jsonl.gz",
		prefix, host, scope,
		start.Format("2006"), start.Format("01"), start.Format("02"), start.Format("15"),
		start.Format("20060102T150405Z"), end.Format("20060102T150405Z"))
}

// sortWireSamples applies the documented timestamp, metric, then canonical
// label order without importing the production model.
func sortWireSamples(samples []wireSample) {
	sort.SliceStable(samples, func(i, j int) bool {
		a, b := samples[i], samples[j]
		if !a.Timestamp.Equal(b.Timestamp) {
			return a.Timestamp.Before(b.Timestamp)
		}
		if a.Metric != b.Metric {
			return a.Metric < b.Metric
		}
		return sampleIdentity(a) < sampleIdentity(b)
	})
}
