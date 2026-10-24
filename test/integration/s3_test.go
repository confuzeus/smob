//go:build integration

package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// chunkNamePattern mirrors the documented chunk filename, including the
// optional collision suffix.
var chunkNamePattern = regexp.MustCompile(`^(\d{8})T(\d{6})Z-(\d{8})T(\d{6})Z(-[1-9]\d*)?\.jsonl\.gz$`)

// s3TestEnv is one real S3-compatible endpoint and a unique prefix inside it.
type s3TestEnv struct {
	endpoint string
	bucket   string
	prefix   string
	client   *awss3.Client
}

// requireS3 returns the configured S3 service or skips with the missing
// variables. The AWS SDK is used here only to inspect and clean up the test
// prefix; smob itself is still driven as a black box.
func requireS3(t *testing.T) s3TestEnv {
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
		strings.NewReplacer("/", "-", " ", "-").Replace(strings.ToLower(t.Name())),
		fmt.Sprintf("%d", time.Now().UnixNano()),
	)
	t.Cleanup(func() { deletePrefix(t, client, bucket, prefix) })

	return s3TestEnv{endpoint: endpoint, bucket: bucket, prefix: prefix, client: client}
}

// ensureBucket creates the shared test bucket when it does not exist. Tests
// isolate themselves with unique prefixes rather than unique buckets. The
// store-parity package may create the same bucket concurrently, so a failed
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

func TestS3Unreachable(t *testing.T) {
	// Resolve credentials locally so the request reaches the closed port.
	// Without them the SDK fails on instance metadata before the network path
	// this test is meant to exercise.
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a closed port: %v", err)
	}
	endpoint := "http://" + listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close reserved port: %v", err)
	}

	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
storage:
  primary: s3
  s3:
    bucket: smob-test
    endpoint: %s
`, dataDir, endpoint))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res := runSmob(t, ctx, "collect", "--dry-run", "--config", cfg)
	if res.ExitCode != 3 {
		t.Fatalf("exit = %d, want 3\nstdout:\n%s\nstderr:\n%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	for _, want := range []string{"s3://smob-test", "persistent store failure"} {
		if !strings.Contains(res.Stderr, want) {
			t.Errorf("stderr does not contain %q:\n%s", want, res.Stderr)
		}
	}
}

func TestS3RoundTrip(t *testing.T) {
	env := requireS3(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	diskDir := filepath.Join(t.TempDir(), "data")
	diskCfg := collectConfig(t, diskDir, "/proc")
	if res := runSmob(t, ctx, "collect", "--once", "--config", diskCfg); res.ExitCode != 0 {
		t.Fatalf("disk collect exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	s3Dir := filepath.Join(t.TempDir(), "data")
	s3Cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
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
`, s3Dir, env.endpoint, env.bucket, env.prefix))
	if res := runSmob(t, ctx, "collect", "--once", "--config", s3Cfg); res.ExitCode != 0 {
		t.Fatalf("S3 collect exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	keys := listS3Keys(t, ctx, env.client, env.bucket, env.prefix+"/")
	if len(keys) == 0 {
		t.Fatalf("collect wrote no objects under %s", env.prefix)
	}
	assertS3Layout(t, env.prefix, keys)
	assertDiskLayout(t, diskDir)

	diskOut := runSmob(t, ctx, "query", "--config", diskCfg, "--since", "1h", "--format", "json")
	if diskOut.ExitCode != 0 {
		t.Fatalf("disk query exit = %d\nstderr:\n%s", diskOut.ExitCode, diskOut.Stderr)
	}
	s3Out := runSmob(t, ctx, "query", "--config", s3Cfg, "--since", "1h", "--format", "json")
	if s3Out.ExitCode != 0 {
		t.Fatalf("S3 query exit = %d\nstderr:\n%s", s3Out.ExitCode, s3Out.Stderr)
	}

	diskSamples := decodeJSONSamples(t, diskOut.Stdout)
	s3Samples := decodeJSONSamples(t, s3Out.Stdout)
	if len(diskSamples) == 0 || len(s3Samples) == 0 {
		t.Fatalf("query returned disk=%d samples, S3=%d samples; want both non-empty", len(diskSamples), len(s3Samples))
	}
	assertSampleIdentities(t, "S3", s3Samples, "disk", diskSamples)
	assertOrderedSamples(t, s3Samples)

	limited := runSmob(t, ctx, "query", "--config", s3Cfg, "--since", "1h", "--limit", "1", "--format", "json")
	if limited.ExitCode != 0 {
		t.Fatalf("limited S3 query exit = %d\nstderr:\n%s", limited.ExitCode, limited.Stderr)
	}
	limitedSamples := decodeJSONSamples(t, limited.Stdout)
	if len(limitedSamples) != 1 {
		t.Fatalf("limited S3 query returned %d samples, want 1", len(limitedSamples))
	}
	if got, want := sampleIdentity(limitedSamples[0]), sampleIdentity(s3Samples[0]); got != want {
		t.Fatalf("limited S3 query first sample = %s, want %s", got, want)
	}

	filtered := runSmob(t, ctx, "query", "--config", s3Cfg, "--since", "1h",
		"--metric", "system.memory.total_bytes", "--format", "json")
	if filtered.ExitCode != 0 {
		t.Fatalf("filtered S3 query exit = %d\nstderr:\n%s", filtered.ExitCode, filtered.Stderr)
	}
	filteredSamples := decodeJSONSamples(t, filtered.Stdout)
	if len(filteredSamples) != 1 || filteredSamples[0].Metric != "system.memory.total_bytes" {
		t.Fatalf("metric-filtered S3 query = %+v, want one system.memory.total_bytes sample", filteredSamples)
	}
}

// assertS3Layout checks every object against the documented key layout and that
// the hour directories match the chunk start.
func assertS3Layout(t *testing.T, prefix string, keys []string) {
	t.Helper()

	for _, key := range keys {
		rel := strings.TrimPrefix(key, prefix+"/")
		parts := strings.Split(rel, "/")
		if len(parts) != 7 {
			t.Fatalf("object key %q has %d components, want 7", key, len(parts))
		}
		if parts[0] != "test-host" || parts[1] != "system" {
			t.Fatalf("object key %q = %s/%s, want test-host/system", key, parts[0], parts[1])
		}
		assertChunkName(t, key, parts)
	}
}

func assertDiskLayout(t *testing.T, dataDir string) {
	t.Helper()

	paths := chunkPaths(t, dataDir)
	if len(paths) == 0 {
		t.Fatalf("disk collect wrote no chunks under %s", dataDir)
	}
	for _, chunkPath := range paths {
		rel, err := filepath.Rel(dataDir, chunkPath)
		if err != nil {
			t.Fatalf("relative chunk path %s: %v", chunkPath, err)
		}
		parts := strings.Split(rel, string(os.PathSeparator))
		if len(parts) != 7 {
			t.Fatalf("disk chunk %q has %d components, want 7", rel, len(parts))
		}
		if parts[0] != "test-host" || parts[1] != "system" {
			t.Fatalf("disk chunk %q = %s/%s, want test-host/system", rel, parts[0], parts[1])
		}
		assertChunkName(t, rel, parts)
	}
}

func assertChunkName(t *testing.T, location string, parts []string) {
	t.Helper()

	match := chunkNamePattern.FindStringSubmatch(parts[6])
	if match == nil {
		t.Fatalf("chunk %q has filename %q, want <start>-<end>.jsonl.gz", location, parts[6])
	}
	wantDir := []string{match[1][0:4], match[1][4:6], match[1][6:8], match[2][0:2]}
	if !slices.Equal(parts[2:6], wantDir) {
		t.Fatalf("chunk %q sits in %v, want %v from its start time", location, parts[2:6], wantDir)
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
	sort.Strings(keys)
	return keys
}

// deletePrefix removes every object written by one test. A shared bucket is
// reused, so leaving objects behind would leak state into later runs.
func deletePrefix(t *testing.T, client *awss3.Client, bucket, prefix string) {
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

// assertSampleIdentities compares metric and label sets, not live values or
// timestamps: the disk and S3 runs observe the host at different instants.
func assertSampleIdentities(t *testing.T, gotName string, got []wireSample, wantName string, want []wireSample) {
	t.Helper()

	gotIDs := make([]string, len(got))
	for i, sample := range got {
		gotIDs[i] = sampleIdentity(sample)
	}
	wantIDs := make([]string, len(want))
	for i, sample := range want {
		wantIDs[i] = sampleIdentity(sample)
	}
	sort.Strings(gotIDs)
	sort.Strings(wantIDs)
	if !slices.Equal(gotIDs, wantIDs) {
		t.Fatalf("%s samples differ from %s samples\ngot (%d):\n%s\nwant (%d):\n%s",
			gotName, wantName, len(gotIDs), strings.Join(gotIDs, "\n"), len(wantIDs), strings.Join(wantIDs, "\n"))
	}
}

func sampleIdentity(sample wireSample) string {
	keys := make([]string, 0, len(sample.Labels))
	for name := range sample.Labels {
		keys = append(keys, name)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(sample.Metric)
	b.WriteByte('{')
	for _, name := range keys {
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(sample.Labels[name])
		b.WriteByte(',')
	}
	b.WriteByte('}')
	return b.String()
}

// assertOrderedSamples checks the documented timestamp, metric, then canonical
// label order.
func assertOrderedSamples(t *testing.T, samples []wireSample) {
	t.Helper()

	for i := 1; i < len(samples); i++ {
		prev, cur := samples[i-1], samples[i]
		if cur.Timestamp.Before(prev.Timestamp) {
			t.Fatalf("sample %d timestamp %s is before %s", i, cur.Timestamp, prev.Timestamp)
		}
		if !cur.Timestamp.Equal(prev.Timestamp) {
			continue
		}
		if cur.Metric < prev.Metric {
			t.Fatalf("sample %d metric %s is before %s at the same timestamp", i, cur.Metric, prev.Metric)
		}
		if cur.Metric == prev.Metric && sampleIdentity(cur) < sampleIdentity(prev) {
			t.Fatalf("sample %d labels %s are before %s at the same timestamp and metric",
				i, sampleIdentity(cur), sampleIdentity(prev))
		}
	}
}
