//go:build integration

// Package store_test verifies the store contract.
//
// This file is the one narrow exception to the rule that integration tests
// drive the smob binary as a black box: exact disk/S3 write-path parity for
// identical input is only observable at the store boundary, because the CLI
// cannot replay the same /proc values and timestamps twice. The test feeds one
// fixed store.Chunk through the real disk and S3 stores, against a real
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
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
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
// and S3 stores, then compares the stored objects, the store Query results, and
// the CLI query output. Compressed bytes are deliberately not compared.
func TestDiskS3ChunkEquivalence(t *testing.T) {
	env := requireS3(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	chunks := fixedChunks()
	want := flattenChunks(chunks)

	dataDir := filepath.Join(t.TempDir(), "data")
	diskStore, err := disk.New(dataDir)
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

	for _, c := range chunks {
		if err := diskStore.Write(ctx, c); err != nil {
			t.Fatalf("disk write %s: %v", c.Start.Format(time.RFC3339), err)
		}
		// Rebuild the label maps in reverse key order. The samples are
		// semantically identical, so JSON map ordering must not change them.
		if err := s3Store.Write(ctx, withReversedLabelMaps(c)); err != nil {
			t.Fatalf("S3 write %s: %v", c.Start.Format(time.RFC3339), err)
		}
	}

	keys := fixedChunkKeys()
	if got := diskChunkKeys(t, dataDir); !slices.Equal(got, keys) {
		t.Fatalf("disk keys = %v, want %v", got, keys)
	}
	wantS3Keys := make([]string, len(keys))
	for i, key := range keys {
		wantS3Keys[i] = env.prefix + "/" + key
	}
	if got := listS3Keys(t, ctx, env.client, env.bucket, env.prefix+"/"); !slices.Equal(got, wantS3Keys) {
		t.Fatalf("S3 keys = %v, want %v", got, wantS3Keys)
	}

	for i, c := range chunks {
		key := keys[i]

		file, err := os.Open(filepath.Join(dataDir, filepath.FromSlash(key)))
		if err != nil {
			t.Fatalf("open disk chunk %s: %v", key, err)
		}
		diskSamples := decodeStoredSamples(t, file)
		closeErr := file.Close()
		if closeErr != nil {
			t.Fatalf("close disk chunk %s: %v", key, closeErr)
		}
		assertSamplesEqual(t, diskSamples, c.Samples)

		body := getS3Object(t, ctx, env.client, env.bucket, env.prefix+"/"+key)
		s3Samples := decodeStoredSamples(t, bytes.NewReader(body))
		assertSamplesEqual(t, s3Samples, c.Samples)
		assertSamplesEqual(t, s3Samples, diskSamples)
	}

	q := model.Query{
		Range: model.TimeRange{Start: chunks[0].Start, End: chunks[len(chunks)-1].End},
		Host:  equivalenceHost,
		Scope: equivalenceScope,
	}
	assertSamplesEqual(t, queryStoreSamples(t, ctx, diskStore, q), want)
	assertSamplesEqual(t, queryStoreSamples(t, ctx, s3Store, q), want)

	limited := q
	limited.Metric = "system.network.rx_bytes_per_second"
	limited.Limit = 2
	wantLimited := []model.Sample{want[3], want[4]}
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

	since := chunks[0].Start.Format(time.RFC3339)
	until := chunks[len(chunks)-1].End.Format(time.RFC3339)
	args := []string{"--since", since, "--until", until, "--host", equivalenceHost, "--scope", equivalenceScope}
	assertSamplesEqual(t, queryCLI(t, ctx, diskCfg, args...), want)
	assertSamplesEqual(t, queryCLI(t, ctx, s3Cfg, args...), want)
}

// fixedChunks builds the input shared by both backends. Samples are in the
// documented timestamp, metric, then canonical label order; two chunks in one
// hour exercise the query merge.
func fixedChunks() []store.Chunk {
	first := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	second := first.Add(5 * time.Minute)

	labels := func(extra map[string]string) map[string]string {
		labels := map[string]string{"host": equivalenceHost, "scope": equivalenceScope}
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
					Timestamp:  first.Add(10 * time.Second),
					Metric:     "system.cpu.percent",
					Value:      3.5,
					IntervalMS: 10_000,
					Labels:     labels(map[string]string{"cpu": "0"}),
				},
				{
					Timestamp:  first.Add(10 * time.Second),
					Metric:     "system.cpu.percent",
					Value:      7.25,
					IntervalMS: 10_000,
					Labels:     labels(map[string]string{"cpu": "all"}),
				},
				{
					Timestamp: first.Add(10 * time.Second),
					Metric:    "system.memory.used_bytes",
					Value:     4096,
					Labels:    labels(nil),
				},
				{
					Timestamp:  first.Add(20 * time.Second),
					Metric:     "system.network.rx_bytes_per_second",
					Value:      1024.25,
					IntervalMS: 10_000,
					Labels:     labels(map[string]string{"interface": "eth0"}),
				},
				{
					Timestamp:  first.Add(20 * time.Second),
					Metric:     "system.network.rx_bytes_per_second",
					Value:      0,
					IntervalMS: 10_000,
					Labels:     labels(map[string]string{"interface": "eth1"}),
				},
				{
					Timestamp:  first.Add(20 * time.Second),
					Metric:     "system.network.tx_bytes_per_second",
					Value:      512.125,
					IntervalMS: 10_000,
					Labels:     labels(map[string]string{"interface": "eth0"}),
				},
				{
					Timestamp: first.Add(30 * time.Second),
					Metric:    "system.memory.total_bytes",
					Value:     8_589_934_592,
					Labels:    labels(nil),
				},
			},
		},
		{
			Host:  equivalenceHost,
			Scope: equivalenceScope,
			Start: second,
			End:   second.Add(5 * time.Minute),
			Samples: []model.Sample{
				{
					Timestamp:  second.Add(5 * time.Second),
					Metric:     "system.cpu.percent",
					Value:      50,
					IntervalMS: 10_000,
					Labels:     labels(map[string]string{"cpu": "all"}),
				},
				{
					Timestamp: second.Add(6 * time.Second),
					Metric:    "system.memory.available_bytes",
					Value:     1_073_741_824,
					Labels:    labels(nil),
				},
			},
		},
	}
}

// fixedChunkKeys mirrors the documented key layout instead of deriving it from
// the production key helper, so a layout change breaks this contract test.
func fixedChunkKeys() []string {
	return []string{
		equivalenceHost + "/system/2026/01/02/03/20260102T030000Z-20260102T030500Z.jsonl.gz",
		equivalenceHost + "/system/2026/01/02/03/20260102T030500Z-20260102T031000Z.jsonl.gz",
	}
}

// withReversedLabelMaps returns a value-identical chunk whose label maps are
// rebuilt in reverse key order.
func withReversedLabelMaps(c store.Chunk) store.Chunk {
	samples := make([]model.Sample, len(c.Samples))
	for i, sample := range c.Samples {
		keys := slices.Sorted(maps.Keys(sample.Labels))
		slices.Reverse(keys)

		labels := make(map[string]string, len(sample.Labels))
		for _, key := range keys {
			labels[key] = sample.Labels[key]
		}
		sample.Labels = labels
		samples[i] = sample
	}
	c.Samples = samples
	return c
}

func flattenChunks(chunks []store.Chunk) []model.Sample {
	var samples []model.Sample
	for _, c := range chunks {
		samples = append(samples, c.Samples...)
	}
	return samples
}

func assertSamplesEqual(t *testing.T, got, want []model.Sample) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d samples, want %d\ngot: %+v\nwant: %+v", len(got), len(want), got, want)
	}
	for i := range got {
		if !got[i].Timestamp.Equal(want[i].Timestamp) ||
			got[i].Metric != want[i].Metric ||
			got[i].Value != want[i].Value ||
			got[i].IntervalMS != want[i].IntervalMS ||
			!maps.Equal(got[i].Labels, want[i].Labels) {
			t.Fatalf("sample %d = %+v, want %+v", i, got[i], want[i])
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

	prefix := path.Join(
		"smob-it",
		"store-equivalence",
		fmt.Sprintf("%d", time.Now().UnixNano()),
	)
	t.Cleanup(func() { deleteS3Prefix(t, client, bucket, prefix) })

	return equivalenceS3{endpoint: endpoint, bucket: bucket, prefix: prefix, client: client}
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
