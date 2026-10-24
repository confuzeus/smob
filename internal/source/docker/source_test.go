package docker

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/confuzeus/smob/internal/model"
)

func TestContainerSamples(t *testing.T) {
	at := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	container := Container{ID: "id", Name: "web", Image: "busybox:1.36"}

	tests := []struct {
		name         string
		obs          containerMetrics
		wantMetrics  []string
		wantInterval []int64
		wantIfaces   []string
	}{
		{
			name:         "first observation emits the memory gauge only",
			obs:          containerMetrics{memoryUsedBytes: 4096},
			wantMetrics:  []string{metricContainerMemoryUsedBytes},
			wantInterval: []int64{0},
			wantIfaces:   []string{""},
		},
		{
			name: "later observation emits rates with the measured window",
			obs: containerMetrics{
				memoryUsedBytes: 2048,
				window:          2 * time.Second,
				cpuPercent:      12.5,
				cpuKnown:        true,
				networks: []networkRate{
					{name: "eth0", rx: 10, tx: 20, rxKnown: true, txKnown: true},
				},
			},
			wantMetrics: []string{
				metricContainerMemoryUsedBytes,
				metricContainerCPUPercent,
				metricContainerNetworkRXBytesPerSecond,
				metricContainerNetworkTXBytesPerSecond,
			},
			wantInterval: []int64{0, 2000, 2000, 2000},
			wantIfaces:   []string{"", "", "eth0", "eth0"},
		},
		{
			name: "only the known network direction is emitted",
			obs: containerMetrics{
				memoryUsedBytes: 1024,
				window:          time.Second,
				networks: []networkRate{
					{name: "eth1", tx: 5, txKnown: true},
				},
			},
			wantMetrics: []string{
				metricContainerMemoryUsedBytes,
				metricContainerNetworkTXBytesPerSecond,
			},
			wantInterval: []int64{0, 1000},
			wantIfaces:   []string{"", "eth1"},
		},
		{
			name: "unknown CPU emits no CPU sample",
			obs: containerMetrics{
				memoryUsedBytes: 1024,
				window:          time.Second,
				networks:        []networkRate{{name: "eth0", rx: 0, rxKnown: true}},
			},
			wantMetrics: []string{
				metricContainerMemoryUsedBytes,
				metricContainerNetworkRXBytesPerSecond,
			},
			wantInterval: []int64{0, 1000},
			wantIfaces:   []string{"", "eth0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memory, rates := containerSamples(container, tt.obs, at)
			samples := append([]model.Sample{memory}, rates...)

			if len(samples) != len(tt.wantMetrics) {
				t.Fatalf("samples = %d, want %d: %+v", len(samples), len(tt.wantMetrics), samples)
			}
			if memory.Value != float64(tt.obs.memoryUsedBytes) {
				t.Fatalf("memory = %v, want %d", memory.Value, tt.obs.memoryUsedBytes)
			}

			for i, sample := range samples {
				if sample.Metric != tt.wantMetrics[i] {
					t.Fatalf("sample %d metric = %q, want %q", i, sample.Metric, tt.wantMetrics[i])
				}
				if sample.IntervalMS != tt.wantInterval[i] {
					t.Fatalf("sample %d interval_ms = %d, want %d",
						i, sample.IntervalMS, tt.wantInterval[i])
				}
				if sample.Timestamp.Location() != time.UTC || !sample.Timestamp.Equal(at) {
					t.Fatalf("sample %d timestamp = %s, want %s in UTC", i, sample.Timestamp, at)
				}

				wantLabels := map[string]string{
					"container": "web",
					"image":     "busybox:1.36",
				}
				if tt.wantIfaces[i] != "" {
					wantLabels["interface"] = tt.wantIfaces[i]
				}
				if !maps.Equal(sample.Labels, wantLabels) {
					t.Fatalf("sample %d labels = %v, want %v", i, sample.Labels, wantLabels)
				}
				if _, ok := sample.Labels["host"]; ok {
					t.Fatalf("sample %d sets host; the collector owns it", i)
				}
				if _, ok := sample.Labels["scope"]; ok {
					t.Fatalf("sample %d sets scope; the collector owns it", i)
				}
			}

			if len(rates) > 0 && rates[0].Metric == metricContainerCPUPercent {
				if rates[0].Value != tt.obs.cpuPercent {
					t.Fatalf("cpu = %v, want %v", rates[0].Value, tt.obs.cpuPercent)
				}
			}
		})
	}
}

func TestSampleBufferDropsOldest(t *testing.T) {
	tests := []struct {
		name  string
		limit int
		adds  []string
		want  []string
		drops bool
	}{
		{
			name:  "under capacity preserves insertion order",
			limit: 3,
			adds:  []string{"a", "b"},
			want:  []string{"a", "b"},
		},
		{
			name:  "exactly capacity preserves every sample",
			limit: 3,
			adds:  []string{"a", "b", "c"},
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "over capacity evicts the oldest samples",
			limit: 3,
			adds:  []string{"a", "b", "c", "d", "e"},
			want:  []string{"c", "d", "e"},
			drops: true,
		},
		{
			name:  "capacity one keeps only the newest",
			limit: 1,
			adds:  []string{"a", "b"},
			want:  []string{"b"},
			drops: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := newSampleBuffer(tt.limit)
			var dropped bool
			for _, metric := range tt.adds {
				if buffer.add(model.Sample{Metric: metric}) {
					dropped = true
				}
			}
			if dropped != tt.drops {
				t.Fatalf("dropped = %v, want %v", dropped, tt.drops)
			}
			if got := sampleNames(buffer.drain()); !slices.Equal(got, tt.want) {
				t.Fatalf("drain = %v, want %v", got, tt.want)
			}
			if got := buffer.drain(); got != nil {
				t.Fatalf("second drain = %v, want nil", got)
			}
		})
	}
}

func TestSampleBufferReuseAfterDrain(t *testing.T) {
	buffer := newSampleBuffer(2)
	buffer.add(model.Sample{Metric: "a"})
	buffer.add(model.Sample{Metric: "b"})
	_ = buffer.drain()

	buffer.add(model.Sample{Metric: "c"})
	if got := sampleNames(buffer.drain()); !slices.Equal(got, []string{"c"}) {
		t.Fatalf("drain after reuse = %v, want [c]", got)
	}
}

func sampleNames(samples []model.Sample) []string {
	names := make([]string, len(samples))
	for i, sample := range samples {
		names[i] = sample.Metric
	}
	return names
}
