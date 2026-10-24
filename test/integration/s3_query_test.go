//go:build integration

package integration

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
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

	key := fmt.Sprintf("%s/%s/%s/%s/%s/%s/%s/%s-%s.jsonl.gz",
		env.prefix, host, scope,
		start.Format("2006"), start.Format("01"), start.Format("02"), start.Format("15"),
		start.Format("20060102T150405Z"), end.Format("20060102T150405Z"))
	if _, err := env.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(env.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body.Bytes()),
	}); err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}
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
