//go:build integration

// Package store_test verifies the store contract.
//
// This file is the one narrow exception to the rule that integration tests
// drive the smob binary as a black box: exact disk/S3 write-path parity for
// identical input is only observable at the store boundary, because the CLI
// cannot replay the same /proc values and timestamps twice. The test feeds one
// fixed set of store.Chunks through the real disk and S3 stores, against a real
// filesystem and a real S3-compatible endpoint. It uses no mocks and no fake
// clock. The exception is limited to store parity; importing internals from
// test/integration remains prohibited.
package store_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/confuzeus/smob/internal/config"
	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/store"
	"github.com/confuzeus/smob/internal/store/disk"
	stores3 "github.com/confuzeus/smob/internal/store/s3"
)

const (
	equivalenceHost  = "equivalence-host"
	equivalenceScope = "system"
	// secondHost and secondScope are deliberately different from the primary
	// pair. Their chunks overlap the primary chunks in time, so the query merge
	// must interleave three host/scope targets rather than concatenate them.
	secondHost  = "equivalence-host-b"
	secondScope = "process"
	// rateMetric carries intervals and appears in every chunk, so a limited
	// query proves the limit is applied to the globally merged stream.
	rateMetric = "system.network.rx_bytes_per_second"
)

// smobBin is built once by TestMain so the CLI layer of the test exercises the
// same black-box command users run.
var smobBin string

func TestMain(m *testing.M) {
	root, err := moduleRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	dir, err := os.MkdirTemp("", "smob-store-it-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create test binary dir: %v\n", err)
		os.Exit(1)
	}

	smobBin = filepath.Join(dir, "smob")
	build := exec.Command("go", "build", "-o", smobBin, "./cmd/smob")
	build.Dir = root
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "build smob: %v\n", err)
		_ = os.RemoveAll(dir)
		os.Exit(1)
	}

	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found above test directory")
		}
		dir = parent
	}
}

// TestDiskS3ChunkEquivalence writes the same fixed chunks through the real disk
// and S3 stores, then compares decoded samples, the store Query results, and the
// CLI query output. Each backend receives a different permutation of every
// chunk's samples, so a backend that skips chunk.Prepare stores a different
// order. The fixture spans two hosts and two scopes with overlapping
// timestamps, so key layout and the global merge are both exercised.
// Compressed bytes are deliberately not compared.
func TestDiskS3ChunkEquivalence(t *testing.T) {
	env := requireS3(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	chunks := equivalenceChunks()
	keys := chunkKeys(chunks)
	assertFixtureShape(t, chunks, keys)
	window := equivalenceWindow(chunks)

	dataDir := filepath.Join(t.TempDir(), "data")
	diskStore, err := disk.New(dataDir, filepath.Join(dataDir, "spool"), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("new disk store: %v", err)
	}
	t.Cleanup(func() { _ = diskStore.Close() })

	s3DataDir := filepath.Join(t.TempDir(), "data")
	s3Store, err := stores3.New(ctx, config.S3Storage{
		Endpoint:      env.endpoint,
		Region:        "us-east-1",
		Bucket:        env.bucket,
		Prefix:        env.prefix,
		PathStyle:     aws.Bool(true),
		SpoolDir:      filepath.Join(s3DataDir, "spool"),
		SpoolMaxBytes: 1 << 20,
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	t.Cleanup(func() { _ = s3Store.Close() })

	wantChunks := make([][]model.Sample, len(chunks))
	for i, c := range chunks {
		wantChunks[i] = canonicalOrder(c.Samples)

		// encoding/json sorts map keys, so rebuilding label maps in another
		// order proves nothing. Permuting the sample slice is what exercises
		// the shared chunk.Prepare write path.
		diskChunk := c
		diskChunk.Samples = rotatedSamples(c.Samples, 1)
		s3Chunk := c
		s3Chunk.Samples = reversedSamples(c.Samples)
		assertScrambled(t, "disk", diskChunk.Samples)
		assertScrambled(t, "S3", s3Chunk.Samples)

		if err := diskStore.Write(ctx, diskChunk); err != nil {
			t.Fatalf("disk write %s: %v", keys[i], err)
		}
		if err := s3Store.Write(ctx, s3Chunk); err != nil {
			t.Fatalf("S3 write %s: %v", keys[i], err)
		}
	}

	wantKeys := slices.Sorted(slices.Values(keys))
	if got := diskChunkKeys(t, dataDir); !slices.Equal(got, wantKeys) {
		t.Fatalf("disk keys = %v, want %v", got, wantKeys)
	}
	wantS3Keys := make([]string, len(wantKeys))
	for i, key := range wantKeys {
		wantS3Keys[i] = env.prefix + "/" + key
	}
	if got := listS3Keys(t, ctx, env.client, env.bucket, env.prefix+"/"); !slices.Equal(got, wantS3Keys) {
		t.Fatalf("S3 keys = %v, want %v", got, wantS3Keys)
	}

	for i, key := range keys {
		diskSamples := diskChunkSamples(t, dataDir, key)
		assertSamplesEqual(t, diskSamples, wantChunks[i])

		body := getS3Object(t, ctx, env.client, env.bucket, env.prefix+"/"+key)
		s3Samples := decodeStoredSamples(t, bytes.NewReader(body))
		assertSamplesEqual(t, s3Samples, wantChunks[i])
		assertSamplesEqual(t, s3Samples, diskSamples)
	}

	var union []model.Sample
	for _, samples := range wantChunks {
		union = append(union, samples...)
	}
	wantAll := canonicalOrder(union)

	q := model.Query{Range: window}
	diskAll := queryStoreSamples(t, ctx, diskStore, q)
	assertSamplesEqual(t, diskAll, wantAll)
	assertGloballyOrdered(t, diskAll)

	s3All := queryStoreSamples(t, ctx, s3Store, q)
	assertSamplesEqual(t, s3All, wantAll)
	assertGloballyOrdered(t, s3All)

	scoped := q
	scoped.Host = equivalenceHost
	scoped.Scope = equivalenceScope
	wantScoped := canonicalOrder(matchingSamples(wantAll, func(sample model.Sample) bool {
		return sample.Labels["host"] == equivalenceHost && sample.Labels["scope"] == equivalenceScope
	}))
	if len(wantScoped) == 0 || len(wantScoped) == len(wantAll) {
		t.Fatalf("host/scope filter matched %d of %d samples; the fixture must exercise the filter", len(wantScoped), len(wantAll))
	}
	assertSamplesEqual(t, queryStoreSamples(t, ctx, diskStore, scoped), wantScoped)
	assertSamplesEqual(t, queryStoreSamples(t, ctx, s3Store, scoped), wantScoped)

	limited := q
	limited.Metric = rateMetric
	limited.Limit = 2
	wantLimited := matchingSamples(wantAll, func(sample model.Sample) bool {
		return sample.Metric == rateMetric
	})
	if len(wantLimited) <= limited.Limit {
		t.Fatalf("metric %s matched %d samples; want more than the limit %d", rateMetric, len(wantLimited), limited.Limit)
	}
	wantLimited = wantLimited[:limited.Limit]
	assertSamplesEqual(t, queryStoreSamples(t, ctx, diskStore, limited), wantLimited)
	assertSamplesEqual(t, queryStoreSamples(t, ctx, s3Store, limited), wantLimited)

	diskCfg := writeEquivalenceConfig(t, fmt.Sprintf(`data_dir: %s
host: %s
sources:
  system:
    enabled: true
storage:
  primary: disk
`, dataDir, equivalenceHost))
	s3Cfg := writeEquivalenceConfig(t, fmt.Sprintf(`data_dir: %s
host: %s
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
`, s3DataDir, equivalenceHost, env.endpoint, env.bucket, env.prefix, filepath.Join(s3DataDir, "spool")))

	rangeArgs := []string{
		"--since", window.Start.Format(time.RFC3339),
		"--until", window.End.Format(time.RFC3339),
	}
	assertSamplesEqual(t, queryCLI(t, ctx, diskCfg, rangeArgs...), wantAll)
	assertSamplesEqual(t, queryCLI(t, ctx, s3Cfg, rangeArgs...), wantAll)

	scopedArgs := slices.Concat(rangeArgs, []string{"--host", equivalenceHost, "--scope", equivalenceScope})
	assertSamplesEqual(t, queryCLI(t, ctx, diskCfg, scopedArgs...), wantScoped)
	assertSamplesEqual(t, queryCLI(t, ctx, s3Cfg, scopedArgs...), wantScoped)

	limitedArgs := slices.Concat(rangeArgs, []string{"--metric", rateMetric, "--limit", "2"})
	assertSamplesEqual(t, queryCLI(t, ctx, diskCfg, limitedArgs...), wantLimited)
	assertSamplesEqual(t, queryCLI(t, ctx, s3Cfg, limitedArgs...), wantLimited)

	t.Run("non-finite values are rejected", func(t *testing.T) {
		start := window.End.Add(10 * time.Minute)
		end := start.Add(5 * time.Minute)
		key := testChunkKey(equivalenceHost, equivalenceScope, start, end)

		// encoding/json cannot represent NaN or an infinity, and both stores
		// encode through chunk.Encode before writing anything. Rejecting them
		// is the documented behavior: a value with no JSON representation must
		// not be silently dropped or rewritten.
		cases := []struct {
			name  string
			value float64
		}{
			{name: "NaN", value: math.NaN()},
			{name: "+Inf", value: math.Inf(1)},
			{name: "-Inf", value: math.Inf(-1)},
		}
		for _, tc := range cases {
			c := store.Chunk{
				Host:  equivalenceHost,
				Scope: equivalenceScope,
				Start: start,
				End:   end,
				Samples: []model.Sample{{
					Timestamp: start.Add(time.Second),
					Metric:    "system.precision.non_finite",
					Value:     tc.value,
					Labels:    map[string]string{"host": equivalenceHost, "scope": equivalenceScope},
				}},
			}

			if err := diskStore.Write(ctx, c); !errors.Is(err, store.ErrPersistent) {
				t.Fatalf("disk write %s value = %v, want an error wrapping store.ErrPersistent", tc.name, err)
			}
			if err := s3Store.Write(ctx, c); !errors.Is(err, store.ErrPersistent) {
				t.Fatalf("S3 write %s value = %v, want an error wrapping store.ErrPersistent", tc.name, err)
			}
			if _, err := os.Stat(filepath.Join(dataDir, filepath.FromSlash(key))); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("disk wrote %s after rejecting a %s value: %v", key, tc.name, err)
			}
			if slices.Contains(listS3Keys(t, ctx, env.client, env.bucket, env.prefix+"/"), env.prefix+"/"+key) {
				t.Fatalf("S3 wrote %s after rejecting a %s value", key, tc.name)
			}
		}
	})
}

// equivalenceChunks builds the input shared by both backends. Each chunk's
// samples are deliberately unsorted; every backend receives a different
// permutation. The fixture covers the primary host and scope, a second host,
// and a second scope, all sharing timestamps inside the same UTC hour.
func equivalenceChunks() []store.Chunk {
	first := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	second := first.Add(5 * time.Minute)
	third := second.Add(5 * time.Minute)

	labels := func(host, scope string, extra map[string]string) map[string]string {
		labels := map[string]string{"host": host, "scope": scope}
		maps.Copy(labels, extra)
		return labels
	}

	return []store.Chunk{
		{
			Host:  equivalenceHost,
			Scope: equivalenceScope,
			Start: first,
			End:   second,
			Samples: []model.Sample{
				{
					Timestamp:  first.Add(40 * time.Second),
					Metric:     rateMetric,
					Value:      1024.25,
					IntervalMS: 10_000,
					Labels:     labels(equivalenceHost, equivalenceScope, map[string]string{"interface": "eth0"}),
				},
				{
					Timestamp:  first.Add(20 * time.Second),
					Metric:     "system.cpu.percent",
					Value:      7.25,
					IntervalMS: 10_000,
					Labels:     labels(equivalenceHost, equivalenceScope, map[string]string{"cpu": "all"}),
				},
				{
					Timestamp: first.Add(12 * time.Second),
					Metric:    "system.precision.subnormal",
					Value:     math.SmallestNonzeroFloat64,
					Labels:    labels(equivalenceHost, equivalenceScope, nil),
				},
				{
					Timestamp:  first.Add(20 * time.Second),
					Metric:     "system.cpu.percent",
					Value:      3.5,
					IntervalMS: 10_000,
					Labels:     labels(equivalenceHost, equivalenceScope, map[string]string{"cpu": "0"}),
				},
				{
					Timestamp: first.Add(14 * time.Second),
					Metric:    "system.precision.max",
					Value:     math.MaxFloat64,
					Labels:    labels(equivalenceHost, equivalenceScope, nil),
				},
				{
					Timestamp:  first.Add(40 * time.Second),
					Metric:     rateMetric,
					Value:      0,
					IntervalMS: 10_000,
					Labels:     labels(equivalenceHost, equivalenceScope, map[string]string{"interface": "eth1"}),
				},
				{
					Timestamp: first.Add(10 * time.Second),
					Metric:    "system.precision.float32",
					Value:     16777217,
					Labels:    labels(equivalenceHost, equivalenceScope, nil),
				},
				{
					Timestamp:  first.Add(20 * time.Second),
					Metric:     "system.cpu.percent",
					Value:      3.5,
					IntervalMS: 10_000,
					Labels:     labels(equivalenceHost, equivalenceScope, map[string]string{"cpu": "1"}),
				},
				{
					Timestamp: first.Add(11 * time.Second),
					Metric:    "system.precision.negative_zero",
					Value:     math.Copysign(0, -1),
					Labels:    labels(equivalenceHost, equivalenceScope, nil),
				},
				{
					Timestamp: first.Add(30 * time.Second),
					Metric:    "system.memory.used_bytes",
					Value:     4096,
					Labels:    labels(equivalenceHost, equivalenceScope, nil),
				},
				{
					Timestamp: first.Add(13 * time.Second),
					Metric:    "system.precision.large",
					Value:     1e300,
					Labels:    labels(equivalenceHost, equivalenceScope, nil),
				},
			},
		},
		{
			Host:  equivalenceHost,
			Scope: equivalenceScope,
			Start: second,
			End:   third,
			Samples: []model.Sample{
				{
					Timestamp:  second.Add(2 * time.Second),
					Metric:     rateMetric,
					Value:      512.125,
					IntervalMS: 10_000,
					Labels:     labels(equivalenceHost, equivalenceScope, map[string]string{"interface": "eth0"}),
				},
				{
					Timestamp:  second.Add(5 * time.Second),
					Metric:     "system.cpu.percent",
					Value:      50,
					IntervalMS: 10_000,
					Labels:     labels(equivalenceHost, equivalenceScope, map[string]string{"cpu": "all"}),
				},
				{
					Timestamp: second.Add(8 * time.Second),
					Metric:    "system.precision.tiny",
					Value:     1e-300,
					Labels:    labels(equivalenceHost, equivalenceScope, nil),
				},
				{
					Timestamp: second.Add(6 * time.Second),
					Metric:    "system.memory.available_bytes",
					Value:     1_073_741_824,
					Labels:    labels(equivalenceHost, equivalenceScope, nil),
				},
			},
		},
		{
			Host:  secondHost,
			Scope: equivalenceScope,
			Start: first,
			End:   second,
			Samples: []model.Sample{
				{
					Timestamp: first.Add(15 * time.Second),
					Metric:    "system.memory.used_bytes",
					Value:     8_589_934_592,
					Labels:    labels(secondHost, equivalenceScope, nil),
				},
				{
					Timestamp:  first.Add(10 * time.Second),
					Metric:     "system.cpu.percent",
					Value:      11.5,
					IntervalMS: 10_000,
					Labels:     labels(secondHost, equivalenceScope, map[string]string{"cpu": "all"}),
				},
				{
					Timestamp: first.Add(11 * time.Second),
					Metric:    "system.precision.negative_zero",
					Value:     math.Copysign(0, -1),
					Labels:    labels(secondHost, equivalenceScope, nil),
				},
				{
					Timestamp:  first.Add(40 * time.Second),
					Metric:     rateMetric,
					Value:      2048.5,
					IntervalMS: 10_000,
					Labels:     labels(secondHost, equivalenceScope, map[string]string{"interface": "eth0"}),
				},
			},
		},
		{
			Host:  equivalenceHost,
			Scope: secondScope,
			Start: first,
			End:   second,
			Samples: []model.Sample{
				{
					Timestamp: first.Add(11 * time.Second),
					Metric:    "system.precision.negative_zero",
					Value:     math.Copysign(0, -1),
					Labels:    labels(equivalenceHost, secondScope, map[string]string{"pid": "1234", "comm": "smob"}),
				},
				{
					Timestamp:  first.Add(10 * time.Second),
					Metric:     "system.cpu.percent",
					Value:      99.5,
					IntervalMS: 10_000,
					Labels:     labels(equivalenceHost, secondScope, map[string]string{"cpu": "all", "pid": "1234", "comm": "smob"}),
				},
				{
					Timestamp: first.Add(25 * time.Second),
					Metric:    "system.process.rss_bytes",
					Value:     1_048_576,
					Labels:    labels(equivalenceHost, secondScope, map[string]string{"pid": "1234", "comm": "smob"}),
				},
				{
					Timestamp:  first.Add(40 * time.Second),
					Metric:     rateMetric,
					Value:      4096.75,
					IntervalMS: 10_000,
					Labels:     labels(equivalenceHost, secondScope, map[string]string{"interface": "eth0", "pid": "1234", "comm": "smob"}),
				},
			},
		},
	}
}

// chunkKeys returns the expected key for each chunk in fixture order. The key
// layout is mirrored here rather than derived from chunk.Key, so a layout
// change breaks this contract test.
func chunkKeys(chunks []store.Chunk) []string {
	keys := make([]string, len(chunks))
	for i, c := range chunks {
		keys[i] = testChunkKey(c.Host, c.Scope, c.Start, c.End)
	}
	return keys
}

// testChunkKey mirrors the documented key layout:
// <host>/<scope>/YYYY/MM/DD/HH/<start>-<end>.jsonl.gz.
func testChunkKey(host, scope string, start, end time.Time) string {
	start, end = start.UTC(), end.UTC()
	return path.Join(
		host,
		scope,
		start.Format("2006"),
		start.Format("01"),
		start.Format("02"),
		start.Format("15"),
		start.Format("20060102T150405Z")+"-"+end.Format("20060102T150405Z")+".jsonl.gz",
	)
}

// equivalenceWindow returns the half-open range covering every fixture chunk.
func equivalenceWindow(chunks []store.Chunk) model.TimeRange {
	window := model.TimeRange{Start: chunks[0].Start, End: chunks[0].End}
	for _, c := range chunks[1:] {
		if c.Start.Before(window.Start) {
			window.Start = c.Start
		}
		if c.End.After(window.End) {
			window.End = c.End
		}
	}
	return window
}

// assertFixtureShape guards the arrangement so the test cannot quietly degrade
// to a single-host, pre-sorted, single-value case.
func assertFixtureShape(t *testing.T, chunks []store.Chunk, keys []string) {
	t.Helper()

	hosts := make(map[string]struct{}, len(chunks))
	scopes := make(map[string]struct{}, len(chunks))
	ownerByTimestamp := make(map[int64]string, len(chunks))
	overlapping := false
	for i, c := range chunks {
		if len(c.Samples) < 3 {
			t.Fatalf("chunk %d (%s/%s) has %d samples; want at least 3 so a permutation changes the order",
				i, c.Host, c.Scope, len(c.Samples))
		}
		hosts[c.Host] = struct{}{}
		scopes[c.Scope] = struct{}{}
		owner := c.Host + "/" + c.Scope
		for _, sample := range c.Samples {
			nanos := sample.Timestamp.UTC().UnixNano()
			if previous, ok := ownerByTimestamp[nanos]; ok && previous != owner {
				overlapping = true
			}
			ownerByTimestamp[nanos] = owner
		}
	}
	if len(hosts) < 2 {
		t.Fatalf("fixture covers %d hosts, want at least 2", len(hosts))
	}
	if len(scopes) < 2 {
		t.Fatalf("fixture covers %d scopes, want at least 2", len(scopes))
	}
	if !overlapping {
		t.Fatal("fixture shares no timestamp between two host/scope targets")
	}

	if len(keys) != len(chunks) {
		t.Fatalf("got %d keys for %d chunks", len(keys), len(chunks))
	}
	unique := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, ok := unique[key]; ok {
			t.Fatalf("duplicate chunk key %s", key)
		}
		unique[key] = struct{}{}
	}
	secondHostPrefix := secondHost + "/"
	secondScopePrefix := equivalenceHost + "/" + secondScope + "/"
	if !slices.ContainsFunc(keys, func(key string) bool { return strings.HasPrefix(key, secondHostPrefix) }) {
		t.Fatalf("no chunk key below %s: %v", secondHostPrefix, keys)
	}
	if !slices.ContainsFunc(keys, func(key string) bool { return strings.HasPrefix(key, secondScopePrefix) }) {
		t.Fatalf("no chunk key below %s: %v", secondScopePrefix, keys)
	}
}

// canonicalOrder returns samples in the documented chunk order: timestamp, then
// metric, then label key/value pairs in sorted order. It is implemented here
// rather than with model.SortSamples so a regression in the production ordering
// rule fails this contract test instead of moving the expectation with it.
func canonicalOrder(samples []model.Sample) []model.Sample {
	ordered := slices.Clone(samples)
	sort.SliceStable(ordered, func(i, j int) bool { return lessSample(ordered[i], ordered[j]) })
	return ordered
}

// lessSample reports whether a sorts before b by timestamp, metric, then
// canonical label order.
func lessSample(a, b model.Sample) bool {
	if !a.Timestamp.Equal(b.Timestamp) {
		return a.Timestamp.Before(b.Timestamp)
	}
	if a.Metric != b.Metric {
		return a.Metric < b.Metric
	}
	aKeys := slices.Sorted(maps.Keys(a.Labels))
	bKeys := slices.Sorted(maps.Keys(b.Labels))
	for i := 0; i < min(len(aKeys), len(bKeys)); i++ {
		if aKeys[i] != bKeys[i] {
			return aKeys[i] < bKeys[i]
		}
		if a.Labels[aKeys[i]] != b.Labels[bKeys[i]] {
			return a.Labels[aKeys[i]] < b.Labels[bKeys[i]]
		}
	}
	return len(aKeys) < len(bKeys)
}

// isCanonicallyOrdered reports whether samples are already in the order the
// stores must write.
func isCanonicallyOrdered(samples []model.Sample) bool {
	for i := 1; i < len(samples); i++ {
		if lessSample(samples[i], samples[i-1]) {
			return false
		}
	}
	return true
}

// rotatedSamples returns samples rotated by by positions.
func rotatedSamples(samples []model.Sample, by int) []model.Sample {
	if len(samples) == 0 {
		return nil
	}
	by %= len(samples)
	rotated := make([]model.Sample, len(samples))
	for i := range samples {
		rotated[i] = samples[(i+by)%len(samples)]
	}
	return rotated
}

// reversedSamples returns samples in reverse order.
func reversedSamples(samples []model.Sample) []model.Sample {
	reversed := slices.Clone(samples)
	slices.Reverse(reversed)
	return reversed
}

// assertScrambled fails when the input permutation is already canonical, which
// would make the case pass without exercising chunk.Prepare.
func assertScrambled(t *testing.T, backend string, samples []model.Sample) {
	t.Helper()

	if len(samples) > 1 && isCanonicallyOrdered(samples) {
		t.Fatalf("%s input is already in canonical order; the case would not prove chunk.Prepare normalises it", backend)
	}
}

// assertGloballyOrdered fails unless samples are in global canonical order and
// the merge actually interleaves more than one host and scope.
func assertGloballyOrdered(t *testing.T, samples []model.Sample) {
	t.Helper()

	for i := 1; i < len(samples); i++ {
		if lessSample(samples[i], samples[i-1]) {
			t.Fatalf("sample %d (%s at %s, labels %v) sorts before sample %d",
				i, samples[i].Metric, samples[i].Timestamp.Format(time.RFC3339Nano), samples[i].Labels, i-1)
		}
	}

	hosts := make(map[string]struct{})
	scopes := make(map[string]struct{})
	for _, sample := range samples {
		hosts[sample.Labels["host"]] = struct{}{}
		scopes[sample.Labels["scope"]] = struct{}{}
	}
	if len(hosts) < 2 {
		t.Fatalf("global query covered %d hosts, want at least 2", len(hosts))
	}
	if len(scopes) < 2 {
		t.Fatalf("global query covered %d scopes, want at least 2", len(scopes))
	}

	// A merge that concatenated host by host could still be ordered by
	// accident. Require a second-host sample before the last primary-host
	// sample, so the result is demonstrably interleaved.
	other, lastPrimary := -1, -1
	for i, sample := range samples {
		if sample.Labels["host"] == secondHost && other < 0 {
			other = i
		}
		if sample.Labels["host"] == equivalenceHost {
			lastPrimary = i
		}
	}
	if other < 0 || lastPrimary < 0 || other >= lastPrimary {
		t.Fatalf("second-host sample index %d is not before the last primary-host sample index %d; the merge is not global", other, lastPrimary)
	}
}

// matchingSamples returns the samples keep accepts, preserving order.
func matchingSamples(samples []model.Sample, keep func(model.Sample) bool) []model.Sample {
	var matched []model.Sample
	for _, sample := range samples {
		if keep(sample) {
			matched = append(matched, sample)
		}
	}
	return matched
}

// diskChunkSamples decodes one stored disk chunk.
func diskChunkSamples(t *testing.T, dataDir, key string) []model.Sample {
	t.Helper()

	file, err := os.Open(filepath.Join(dataDir, filepath.FromSlash(key)))
	if err != nil {
		t.Fatalf("open disk chunk %s: %v", key, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("close disk chunk %s: %v", key, err)
		}
	}()
	return decodeStoredSamples(t, file)
}

// assertSamplesEqual compares every field of each decoded sample. Values are
// compared by bit pattern so -0.0 stays distinct from 0.0; NaN never reaches
// storage because chunk.Encode rejects it.
func assertSamplesEqual(t *testing.T, got, want []model.Sample) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d samples, want %d\ngot: %+v\nwant: %+v", len(got), len(want), got, want)
	}
	for i := range got {
		g, w := got[i], want[i]
		at := g.Timestamp.Format(time.RFC3339Nano)
		switch {
		case !g.Timestamp.Equal(w.Timestamp):
			t.Fatalf("sample %d (%s) timestamp = %s, want %s",
				i, g.Metric, at, w.Timestamp.Format(time.RFC3339Nano))
		case g.Metric != w.Metric:
			t.Fatalf("sample %d timestamp %s metric = %q, want %q", i, at, g.Metric, w.Metric)
		case math.Float64bits(g.Value) != math.Float64bits(w.Value):
			t.Fatalf("sample %d (%s at %s) value bits = %#016x (%v), want %#016x (%v)",
				i, g.Metric, at, math.Float64bits(g.Value), g.Value, math.Float64bits(w.Value), w.Value)
		case g.IntervalMS != w.IntervalMS:
			t.Fatalf("sample %d (%s at %s) interval_ms = %d, want %d",
				i, g.Metric, at, g.IntervalMS, w.IntervalMS)
		case !maps.Equal(g.Labels, w.Labels):
			t.Fatalf("sample %d (%s at %s) labels = %v, want %v", i, g.Metric, at, g.Labels, w.Labels)
		}
	}
}

func queryStoreSamples(t *testing.T, ctx context.Context, s store.Store, q model.Query) []model.Sample {
	t.Helper()

	var samples []model.Sample
	err := s.Query(ctx, q, func(sample model.Sample) error {
		samples = append(samples, sample)
		return nil
	})
	if err != nil {
		t.Fatalf("query store: %v", err)
	}
	return samples
}

func queryCLI(t *testing.T, ctx context.Context, cfg string, args ...string) []model.Sample {
	t.Helper()

	args = append([]string{"query", "--config", cfg, "--format", "json"}, args...)
	cmd := exec.CommandContext(ctx, smobBin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("smob %s: %v\nstderr:\n%s", strings.Join(args, " "), err, stderr.String())
	}

	var samples []model.Sample
	if err := json.Unmarshal(stdout.Bytes(), &samples); err != nil {
		t.Fatalf("decode query JSON: %v\n%s", err, stdout.String())
	}
	return samples
}

func decodeStoredSamples(t *testing.T, r io.Reader) []model.Sample {
	t.Helper()

	gz, err := gzip.NewReader(r)
	if err != nil {
		t.Fatalf("open chunk gzip: %v", err)
	}
	defer gz.Close()

	dec := json.NewDecoder(gz)
	var samples []model.Sample
	for {
		var sample model.Sample
		err := dec.Decode(&sample)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode chunk sample: %v", err)
		}
		samples = append(samples, sample)
	}
	return samples
}

func diskChunkKeys(t *testing.T, dataDir string) []string {
	t.Helper()

	var keys []string
	err := filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".jsonl.gz") {
			return nil
		}
		rel, err := filepath.Rel(dataDir, path)
		if err != nil {
			return err
		}
		keys = append(keys, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dataDir, err)
	}
	slices.Sort(keys)
	return keys
}

// equivalenceS3 is one real S3-compatible endpoint and a unique prefix inside
// it. The SDK is used only to read and clean up that prefix; the store under
// test is the production S3 store.
type equivalenceS3 struct {
	endpoint string
	bucket   string
	prefix   string
	client   *awss3.Client
}

func requireS3(t *testing.T) equivalenceS3 {
	t.Helper()

	required := []string{
		"SMOB_TEST_S3_ENDPOINT",
		"SMOB_TEST_S3_BUCKET",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
	}
	var missing []string
	for _, name := range required {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		reason := fmt.Sprintf("S3 test requires %s; set them and run just test-all", strings.Join(missing, ", "))
		if os.Getenv("SMOB_TEST_STRICT") == "1" {
			t.Fatal(reason)
		}
		t.Skip(reason)
	}

	endpoint := os.Getenv("SMOB_TEST_S3_ENDPOINT")
	bucket := os.Getenv("SMOB_TEST_S3_BUCKET")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-east-1"))
	if err != nil {
		t.Fatalf("load AWS configuration: %v", err)
	}
	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	ensureBucket(t, ctx, client, bucket)

	prefix := path.Join(
		"smob-it",
		"store-equivalence",
		fmt.Sprintf("%d", time.Now().UnixNano()),
	)
	t.Cleanup(func() { deleteS3Prefix(t, client, bucket, prefix) })

	return equivalenceS3{endpoint: endpoint, bucket: bucket, prefix: prefix, client: client}
}

// ensureBucket creates the shared test bucket when it does not exist. Tests
// isolate themselves with unique prefixes rather than unique buckets. The CLI
// integration package may create the same bucket concurrently, so a failed
// create is accepted when a follow-up HeadBucket succeeds.
func ensureBucket(t *testing.T, ctx context.Context, client *awss3.Client, bucket string) {
	t.Helper()

	if _, err := client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(bucket)}); err == nil {
		return
	}
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		if _, headErr := client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(bucket)}); headErr != nil {
			t.Fatalf("ensure S3 bucket %s: create: %v; head: %v", bucket, err, headErr)
		}
	}
}

func listS3Keys(t *testing.T, ctx context.Context, client *awss3.Client, bucket, prefix string) []string {
	t.Helper()

	var keys []string
	paginator := awss3.NewListObjectsV2Paginator(client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Fatalf("list %s: %v", prefix, err)
		}
		for _, object := range page.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
	}
	slices.Sort(keys)
	return keys
}

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

func deleteS3Prefix(t *testing.T, client *awss3.Client, bucket, prefix string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	paginator := awss3.NewListObjectsV2Paginator(client, &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix + "/"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Errorf("cleanup list %s: %v", prefix, err)
			return
		}
		if len(page.Contents) == 0 {
			continue
		}

		objects := make([]types.ObjectIdentifier, 0, len(page.Contents))
		for _, object := range page.Contents {
			objects = append(objects, types.ObjectIdentifier{Key: object.Key})
		}
		if _, err := client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		}); err != nil {
			t.Errorf("cleanup delete under %s: %v", prefix, err)
		}
	}
}

func writeEquivalenceConfig(t *testing.T, body string) string {
	t.Helper()

	configPath := filepath.Join(t.TempDir(), "smob.yaml")
	if err := os.WriteFile(configPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return configPath
}
