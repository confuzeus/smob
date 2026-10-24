//go:build integration

package docker

// These tests drive the source beside the package. The daemon and containers
// are real; no fake server stands in for either. The CLI contract lives in
// test/integration/docker_test.go.

import (
	"fmt"
	"maps"
	"math"
	"os"
	"testing"
	"time"

	"github.com/confuzeus/smob/internal/model"
	"github.com/docker/docker/api/types/container"
)

// TestSourceOneShot checks the first tick from a real container (memory gauge
// only) and the second tick (rates with a truthful interval).
func TestSourceOneShot(t *testing.T) {
	socket := requireDocker(t)
	c, ctx := connectDocker(t)

	unique := fmt.Sprintf("smob-source-%d-%d", os.Getpid(), time.Now().UnixNano())
	name := unique + "-live"
	id := startNamedContainer(t, ctx, c, name)
	goneID := startNamedContainer(t, ctx, c, unique+"-gone")

	// Wait for a populated stats payload from the daemon before the source's
	// first observation, so the gauge is real rather than an empty response.
	_ = waitForOneShotStats(t, ctx, newTestStatsAcquirer(c), id)

	src, err := New(ctx, Options{
		Socket:         socket,
		RescanInterval: 200 * time.Millisecond,
		Interval:       time.Second,
	}, newSupervisorTestLogger())
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	if got := src.Scope(); got != "container" {
		t.Fatalf("Scope() = %q, want container", got)
	}
	if got := src.Interval(); got != time.Second {
		t.Fatalf("Interval() = %s, want 1s", got)
	}

	// The supervisor discovered the second container at construction. Removing
	// it before the first tick makes the tick see a container that disappeared;
	// the batch must still succeed for the live container.
	if err := c.ContainerRemove(ctx, goneID, container.RemoveOptions{Force: true}); err != nil {
		t.Fatalf("remove container %s: %v", goneID, err)
	}

	firstAt := time.Now()
	first, err := src.Collect(ctx, firstAt)
	if err != nil {
		t.Fatalf("first collect: %v", err)
	}
	matched := samplesForContainer(first, name)
	if len(matched) != 1 {
		t.Fatalf("first tick samples for %s = %d, want one memory gauge: %+v",
			name, len(matched), matched)
	}
	if got := matched[0].Metric; got != metricContainerMemoryUsedBytes {
		t.Fatalf("first tick metric = %q, want %q", got, metricContainerMemoryUsedBytes)
	}
	if matched[0].IntervalMS != 0 {
		t.Fatalf("memory gauge interval_ms = %d, want 0", matched[0].IntervalMS)
	}
	if matched[0].Value <= 0 {
		t.Fatalf("memory gauge = %v, want greater than zero", matched[0].Value)
	}
	if !matched[0].Timestamp.Equal(firstAt) {
		t.Fatalf("one-shot timestamp = %s, want the tick time %s", matched[0].Timestamp, firstAt)
	}
	assertSeriesLabels(t, matched[0], name, testImage, "")

	waitForCondition(t, 10*time.Second, "a full collection interval between ticks", func() bool {
		return time.Since(firstAt) >= 1100*time.Millisecond
	})

	secondAt := time.Now()
	second, err := src.Collect(ctx, secondAt)
	if err != nil {
		t.Fatalf("second collect: %v", err)
	}
	assertRateTick(t, second, name, testImage, 1000)
}

// TestSourceStream checks the bounded bridge end to end: Start runs the
// supervisor and its consumer, Collect drains observations without waiting for
// the tick, and Close joins both goroutines.
func TestSourceStream(t *testing.T) {
	socket := requireDocker(t)
	c, ctx := connectDocker(t)

	unique := fmt.Sprintf("smob-source-%d-%d", os.Getpid(), time.Now().UnixNano())
	name := unique + "-stream"
	id := startNamedContainer(t, ctx, c, name)
	_ = waitForOneShotStats(t, ctx, newTestStatsAcquirer(c), id)

	src, err := New(ctx, Options{
		Socket:         socket,
		RescanInterval: 200 * time.Millisecond,
		StreamStats:    true,
		Interval:       time.Second,
	}, newSupervisorTestLogger())
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	src.Start(ctx)
	src.Start(ctx) // idempotent: a second call starts no extra goroutines

	// The stale tick time proves Collect reports observation time, not tick
	// time, for streamed samples.
	stale := time.Now().Add(-time.Hour)
	var collected []model.Sample
	waitForCondition(t, 60*time.Second, "streamed memory, CPU, and network samples", func() bool {
		batch, err := src.Collect(ctx, stale)
		if err != nil {
			t.Fatalf("stream collect: %v", err)
		}
		if got := countMetric(batch, name, metricContainerMemoryUsedBytes); got > 1 {
			t.Fatalf("one tick returned %d memory gauges for %s, want at most one", got, name)
		}
		collected = append(collected, batch...)
		return hasMetric(collected, name, metricContainerMemoryUsedBytes) &&
			hasMetric(collected, name, metricContainerCPUPercent) &&
			hasNetworkRate(collected, name)
	})

	for _, sample := range samplesForContainer(collected, name) {
		if !sample.Timestamp.After(stale) {
			t.Fatalf("streamed sample timestamp = %s, want the observation time after %s",
				sample.Timestamp, stale)
		}
		if age := time.Since(sample.Timestamp); age > 2*time.Minute {
			t.Fatalf("streamed sample timestamp is %s old", age)
		}
		switch sample.Metric {
		case metricContainerMemoryUsedBytes:
			if sample.IntervalMS != 0 {
				t.Fatalf("memory gauge interval_ms = %d, want 0", sample.IntervalMS)
			}
			if sample.Value <= 0 {
				t.Fatalf("memory gauge = %v, want greater than zero", sample.Value)
			}
			assertSeriesLabels(t, sample, name, testImage, "")
		case metricContainerCPUPercent:
			if sample.IntervalMS <= 0 {
				t.Fatalf("CPU interval_ms = %d, want greater than zero", sample.IntervalMS)
			}
			assertFiniteNonNegative(t, sample.Metric, sample.Value)
			assertSeriesLabels(t, sample, name, testImage, "")
		case metricContainerNetworkRXBytesPerSecond, metricContainerNetworkTXBytesPerSecond:
			if sample.IntervalMS <= 0 {
				t.Fatalf("%s interval_ms = %d, want greater than zero", sample.Metric, sample.IntervalMS)
			}
			assertFiniteNonNegative(t, sample.Metric, sample.Value)
			iface := sample.Labels["interface"]
			if iface == "" {
				t.Fatalf("%s has no interface label", sample.Metric)
			}
			assertSeriesLabels(t, sample, name, testImage, iface)
		default:
			t.Fatalf("unexpected metric %q for container %s", sample.Metric, name)
		}
	}

	closeStart := time.Now()
	if err := src.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}
	t.Logf("Source.Close joined the supervisor and stream bridge in %s", time.Since(closeStart))
}

// assertRateTick checks the second one-shot tick: one memory gauge, one CPU
// rate, and at least one network rate, all with the expected labels.
func assertRateTick(t *testing.T, samples []model.Sample, name, image string, minIntervalMS int64) {
	t.Helper()
	var memory, cpu, network int
	for _, sample := range samplesForContainer(samples, name) {
		switch sample.Metric {
		case metricContainerMemoryUsedBytes:
			memory++
			if sample.IntervalMS != 0 {
				t.Fatalf("memory gauge interval_ms = %d, want 0", sample.IntervalMS)
			}
			if sample.Value <= 0 {
				t.Fatalf("memory gauge = %v, want greater than zero", sample.Value)
			}
			assertSeriesLabels(t, sample, name, image, "")
		case metricContainerCPUPercent:
			cpu++
			if sample.IntervalMS < minIntervalMS {
				t.Fatalf("CPU interval_ms = %d, want at least %d", sample.IntervalMS, minIntervalMS)
			}
			assertFiniteNonNegative(t, sample.Metric, sample.Value)
			assertSeriesLabels(t, sample, name, image, "")
		case metricContainerNetworkRXBytesPerSecond, metricContainerNetworkTXBytesPerSecond:
			network++
			if sample.IntervalMS < minIntervalMS {
				t.Fatalf("%s interval_ms = %d, want at least %d",
					sample.Metric, sample.IntervalMS, minIntervalMS)
			}
			assertFiniteNonNegative(t, sample.Metric, sample.Value)
			assertSeriesLabels(t, sample, name, image, sample.Labels["interface"])
		default:
			t.Fatalf("unexpected metric %q for container %s", sample.Metric, name)
		}
	}
	if memory != 1 {
		t.Fatalf("memory gauges = %d, want 1", memory)
	}
	if cpu != 1 {
		t.Fatalf("CPU rates = %d, want 1", cpu)
	}
	if network == 0 {
		t.Fatal("no network rate emitted")
	}
}

// assertSeriesLabels checks the exact series labels and proves the source does
// not duplicate the collector's host and scope labels.
func assertSeriesLabels(t *testing.T, sample model.Sample, name, image, iface string) {
	t.Helper()
	want := map[string]string{
		"container": name,
		"image":     image,
	}
	if iface != "" {
		want["interface"] = iface
	}
	if !maps.Equal(sample.Labels, want) {
		t.Fatalf("%s labels = %v, want %v", sample.Metric, sample.Labels, want)
	}
	if _, ok := sample.Labels["host"]; ok {
		t.Fatalf("%s sets host; the collector owns it", sample.Metric)
	}
	if _, ok := sample.Labels["scope"]; ok {
		t.Fatalf("%s sets scope; the collector owns it", sample.Metric)
	}
}

func assertFiniteNonNegative(t *testing.T, metric string, value float64) {
	t.Helper()
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		t.Fatalf("%s = %v, want a finite non-negative value", metric, value)
	}
}

func samplesForContainer(samples []model.Sample, name string) []model.Sample {
	var matches []model.Sample
	for _, sample := range samples {
		if sample.Labels["container"] == name {
			matches = append(matches, sample)
		}
	}
	return matches
}

func countMetric(samples []model.Sample, name, metric string) int {
	count := 0
	for _, sample := range samplesForContainer(samples, name) {
		if sample.Metric == metric {
			count++
		}
	}
	return count
}

func hasMetric(samples []model.Sample, name, metric string) bool {
	return countMetric(samples, name, metric) > 0
}

func hasNetworkRate(samples []model.Sample, name string) bool {
	return hasMetric(samples, name, metricContainerNetworkRXBytesPerSecond) ||
		hasMetric(samples, name, metricContainerNetworkTXBytesPerSecond)
}
