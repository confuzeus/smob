package docker

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"maps"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
)

// loadStatsFixture reads one real StatsResponse captured from the Docker
// daemon. Fixtures are input data; no fake daemon is involved.
func loadStatsFixture(t *testing.T, name string) container.StatsResponse {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	var stats container.StatsResponse
	if err := json.Unmarshal(data, &stats); err != nil {
		t.Fatalf("decode fixture %s: %v", name, err)
	}
	return stats
}

func TestCPUPercent(t *testing.T) {
	first := loadStatsFixture(t, "stats_first.json")
	second := loadStatsFixture(t, "stats_second.json")
	prev := snapshotCPU(first.CPUStats)
	online := second.CPUStats.OnlineCPUs

	baseWant := float64(second.CPUStats.CPUUsage.TotalUsage-prev.totalUsage) /
		float64(second.CPUStats.SystemUsage-prev.systemUsage) * float64(online) * 100
	fallbackWant := float64(second.CPUStats.CPUUsage.TotalUsage-prev.totalUsage) /
		float64(second.CPUStats.SystemUsage-prev.systemUsage) * 4 * 100

	tests := []struct {
		name    string
		prepare func(*container.StatsResponse)
		elapsed time.Duration
		want    float64
		wantOK  bool
	}{
		{
			name:    "normal delta from smob's own previous observation",
			prepare: func(*container.StatsResponse) {},
			elapsed: time.Second,
			want:    baseWant,
			wantOK:  true,
		},
		{
			name: "precpu_stats is ignored",
			prepare: func(stats *container.StatsResponse) {
				stats.PreCPUStats = container.CPUStats{}
				stats.PreRead = time.Time{}
			},
			elapsed: time.Second,
			want:    baseWant,
			wantOK:  true,
		},
		{
			name: "zero system delta",
			prepare: func(stats *container.StatsResponse) {
				stats.CPUStats.SystemUsage = prev.systemUsage
			},
			elapsed: time.Second,
		},
		{
			name: "negative system delta",
			prepare: func(stats *container.StatsResponse) {
				stats.CPUStats.SystemUsage = prev.systemUsage - 1
			},
			elapsed: time.Second,
		},
		{
			name: "cpu counter decrease",
			prepare: func(stats *container.StatsResponse) {
				stats.CPUStats.SystemUsage = prev.systemUsage + 1_000_000
				stats.CPUStats.CPUUsage.TotalUsage = prev.totalUsage - 1
			},
			elapsed: time.Second,
		},
		{
			name: "online cpus fall back to percpu usage",
			prepare: func(stats *container.StatsResponse) {
				stats.CPUStats.OnlineCPUs = 0
				stats.CPUStats.CPUUsage.PercpuUsage = []uint64{1, 2, 3, 4}
			},
			elapsed: time.Second,
			want:    fallbackWant,
			wantOK:  true,
		},
		{
			name: "online cpus and percpu usage both absent",
			prepare: func(stats *container.StatsResponse) {
				stats.CPUStats.OnlineCPUs = 0
				stats.CPUStats.CPUUsage.PercpuUsage = nil
			},
			elapsed: time.Second,
		},
		{
			name: "zero cpu delta is a valid zero",
			prepare: func(stats *container.StatsResponse) {
				stats.CPUStats.SystemUsage = prev.systemUsage + 1_000_000
				stats.CPUStats.CPUUsage.TotalUsage = prev.totalUsage
			},
			elapsed: time.Second,
			wantOK:  true,
		},
		{
			name: "percent is not capped above 100",
			prepare: func(stats *container.StatsResponse) {
				stats.CPUStats.SystemUsage = prev.systemUsage + 1_000_000
				stats.CPUStats.CPUUsage.TotalUsage = prev.totalUsage + 10_000_000
				stats.CPUStats.OnlineCPUs = 12
			},
			elapsed: time.Second,
			want:    12 * 100 * 10,
			wantOK:  true,
		},
		{
			name:    "zero elapsed",
			prepare: func(*container.StatsResponse) {},
			elapsed: 0,
		},
		{
			name:    "negative elapsed",
			prepare: func(*container.StatsResponse) {},
			elapsed: -time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cur := second
			tt.prepare(&cur)

			got, ok := cpuPercent(prev, cur.CPUStats, tt.elapsed)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v (value %v)", ok, tt.wantOK, got)
			}
			if !ok {
				return
			}
			if math.Abs(got-tt.want) > 1e-9 {
				t.Fatalf("percent = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMemoryUsedBytes(t *testing.T) {
	base := loadStatsFixture(t, "stats_first.json").MemoryStats
	usage := base.Usage

	tests := []struct {
		name    string
		prepare func(*container.MemoryStats)
		want    uint64
	}{
		{
			name: "cgroup v2 inactive_file",
			prepare: func(mem *container.MemoryStats) {
				delete(mem.Stats, "total_inactive_file")
				mem.Stats["inactive_file"] = 1000
			},
			want: usage - 1000,
		},
		{
			name: "cgroup v1 total_inactive_file",
			prepare: func(mem *container.MemoryStats) {
				delete(mem.Stats, "inactive_file")
				mem.Stats["total_inactive_file"] = 2000
			},
			want: usage - 2000,
		},
		{
			name: "v1 is preferred when both keys exist",
			prepare: func(mem *container.MemoryStats) {
				mem.Stats["total_inactive_file"] = 1000
				mem.Stats["inactive_file"] = 2000
			},
			want: usage - 1000,
		},
		{
			name: "neither key falls back to usage",
			prepare: func(mem *container.MemoryStats) {
				delete(mem.Stats, "total_inactive_file")
				delete(mem.Stats, "inactive_file")
			},
			want: usage,
		},
		{
			name: "inactive greater than usage floors at zero",
			prepare: func(mem *container.MemoryStats) {
				delete(mem.Stats, "total_inactive_file")
				mem.Stats["inactive_file"] = usage + 100
			},
			want: 0,
		},
		{
			name: "inactive equal to usage floors at zero",
			prepare: func(mem *container.MemoryStats) {
				delete(mem.Stats, "total_inactive_file")
				mem.Stats["inactive_file"] = usage
			},
			want: 0,
		},
		{
			name: "zero inactive is present and subtracts nothing",
			prepare: func(mem *container.MemoryStats) {
				delete(mem.Stats, "total_inactive_file")
				mem.Stats["inactive_file"] = 0
			},
			want: usage,
		},
		{
			name: "oversized v1 value is still preferred and floors",
			prepare: func(mem *container.MemoryStats) {
				mem.Stats["total_inactive_file"] = usage + 100
				mem.Stats["inactive_file"] = 1000
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mem := base
			mem.Stats = maps.Clone(mem.Stats)
			if mem.Stats == nil {
				mem.Stats = make(map[string]uint64)
			}
			tt.prepare(&mem)

			if got := memoryUsedBytes(mem); got != tt.want {
				t.Fatalf("memory used = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestNetworkRates(t *testing.T) {
	base := time.Unix(1_000_000, 0)
	firstStats := loadStatsFixture(t, "stats_first.json")
	secondStats := loadStatsFixture(t, "stats_second.json")

	tests := []struct {
		name      string
		before    container.NetworkStats
		after     container.NetworkStats
		gap       time.Duration
		wantRX    float64
		wantTX    float64
		rxKnown   bool
		txKnown   bool
		wantWarns int
	}{
		{
			name:    "normal delta",
			before:  networkCounters(1000, 2000),
			after:   networkCounters(3000, 5000),
			gap:     2 * time.Second,
			wantRX:  1000,
			wantTX:  1500,
			rxKnown: true,
			txKnown: true,
		},
		{
			name:    "zero traffic is a valid zero",
			before:  networkCounters(1000, 2000),
			after:   networkCounters(1000, 2000),
			gap:     time.Second,
			rxKnown: true,
			txKnown: true,
		},
		{
			name:      "rx decrease drops only rx",
			before:    networkCounters(2000, 2000),
			after:     networkCounters(1000, 3000),
			gap:       time.Second,
			wantTX:    1000,
			txKnown:   true,
			wantWarns: 1,
		},
		{
			name:      "tx decrease drops only tx",
			before:    networkCounters(2000, 2000),
			after:     networkCounters(3000, 1000),
			gap:       time.Second,
			wantRX:    1000,
			rxKnown:   true,
			wantWarns: 1,
		},
		{
			name:      "both directions decrease",
			before:    networkCounters(2000, 2000),
			after:     networkCounters(1000, 1000),
			gap:       time.Second,
			wantWarns: 2,
		},
		{
			name:   "zero elapsed",
			before: networkCounters(1000, 2000),
			after:  networkCounters(3000, 5000),
		},
		{
			name:   "negative elapsed",
			before: networkCounters(1000, 2000),
			after:  networkCounters(3000, 5000),
			gap:    -time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first := firstStats
			first.Networks = map[string]container.NetworkStats{"eth0": tt.before}
			second := secondStats
			second.Networks = map[string]container.NetworkStats{"eth0": tt.after}

			acc, log := newTestAccumulator(t)
			firstObs := acc.observe(rawStats{containerID: "c1", stats: first}, base)
			if len(firstObs.networks) != 0 {
				t.Fatalf("first observation emitted %d network rates, want none", len(firstObs.networks))
			}

			obs := acc.observe(rawStats{containerID: "c1", stats: second}, base.Add(tt.gap))
			if tt.gap <= 0 {
				if len(obs.networks) != 0 {
					t.Fatalf("non-positive window emitted %d network rates, want none", len(obs.networks))
				}
				if got := strings.Count(log.String(), "container network counter reset"); got != 0 {
					t.Fatalf("non-positive window logged %d resets, want 0", got)
				}
				return
			}

			if len(obs.networks) == 0 {
				if tt.rxKnown || tt.txKnown {
					t.Fatal("no network rate emitted, want at least one direction")
				}
			} else {
				rate := obs.networks[0]
				if rate.name != "eth0" {
					t.Fatalf("interface = %q, want eth0", rate.name)
				}
				if rate.rxKnown != tt.rxKnown || rate.txKnown != tt.txKnown {
					t.Fatalf("known = (%v, %v), want (%v, %v)",
						rate.rxKnown, rate.txKnown, tt.rxKnown, tt.txKnown)
				}
				if rate.rxKnown && math.Abs(rate.rx-tt.wantRX) > 1e-9 {
					t.Fatalf("rx = %v, want %v", rate.rx, tt.wantRX)
				}
				if rate.txKnown && math.Abs(rate.tx-tt.wantTX) > 1e-9 {
					t.Fatalf("tx = %v, want %v", rate.tx, tt.wantTX)
				}
			}

			if got := strings.Count(log.String(), "container network counter reset"); got != tt.wantWarns {
				t.Fatalf("reset warnings = %d, want %d\n%s", got, tt.wantWarns, log.String())
			}
		})
	}
}

func TestNetworkInterfaceLifecycle(t *testing.T) {
	base := time.Unix(1_000_000, 0)
	first := loadStatsFixture(t, "stats_first.json")
	first.Networks = map[string]container.NetworkStats{
		"eth0": networkCounters(1000, 1000),
	}
	second := loadStatsFixture(t, "stats_second.json")
	second.Networks = map[string]container.NetworkStats{
		"eth0": networkCounters(2000, 2000),
		"eth1": networkCounters(5000, 5000),
	}
	third := loadStatsFixture(t, "stats_second.json")
	third.Networks = map[string]container.NetworkStats{
		"eth0": networkCounters(3000, 3000),
	}
	fourth := loadStatsFixture(t, "stats_second.json")
	fourth.Networks = map[string]container.NetworkStats{
		"eth0": networkCounters(4000, 4000),
		"eth1": networkCounters(9000, 9000),
	}
	fifth := loadStatsFixture(t, "stats_second.json")
	fifth.Networks = map[string]container.NetworkStats{
		"eth0": networkCounters(5000, 5000),
		"eth1": networkCounters(10_000, 10_000),
	}

	acc, log := newTestAccumulator(t)
	if obs := acc.observe(rawStats{containerID: "c1", stats: first}, base); len(obs.networks) != 0 {
		t.Fatalf("first observation emitted network rates")
	}

	obs := acc.observe(rawStats{containerID: "c1", stats: second}, base.Add(time.Second))
	if rate, ok := networkByName(obs, "eth0"); !ok || !rate.rxKnown || rate.rx != 1000 {
		t.Fatalf("eth0 after one interval = %+v, want 1000 B/s", rate)
	}
	if _, ok := networkByName(obs, "eth1"); ok {
		t.Fatal("new interface eth1 emitted a rate on its first observation")
	}

	obs = acc.observe(rawStats{containerID: "c1", stats: third}, base.Add(2*time.Second))
	if _, ok := networkByName(obs, "eth1"); ok {
		t.Fatal("removed interface eth1 emitted a rate")
	}

	obs = acc.observe(rawStats{containerID: "c1", stats: fourth}, base.Add(3*time.Second))
	if _, ok := networkByName(obs, "eth1"); ok {
		t.Fatal("reappeared interface eth1 reused its discarded baseline")
	}

	obs = acc.observe(rawStats{containerID: "c1", stats: fifth}, base.Add(4*time.Second))
	rate, ok := networkByName(obs, "eth1")
	if !ok || !rate.rxKnown || rate.rx != 1000 {
		t.Fatalf("eth1 after reappearing for one interval = %+v, want 1000 B/s", rate)
	}
	if got := strings.Count(log.String(), "container network counter reset"); got != 0 {
		t.Fatalf("interface churn logged %d resets, want 0\n%s", got, log.String())
	}
}

func TestNetworkResetWarningOnce(t *testing.T) {
	base := time.Unix(1_000_000, 0)
	payloads := []container.NetworkStats{
		networkCounters(1000, 1000),
		networkCounters(900, 1100),
		networkCounters(800, 1200),
		networkCounters(1000, 1300),
		networkCounters(900, 1400),
	}

	acc, log := newTestAccumulator(t)
	stats := loadStatsFixture(t, "stats_second.json")
	for i, counters := range payloads {
		stats.Networks = map[string]container.NetworkStats{"eth0": counters}
		at := base.Add(time.Duration(i) * time.Second)
		acc.observe(rawStats{containerID: "c1", stats: stats}, at)
	}

	if got := strings.Count(log.String(), "container network counter reset"); got != 2 {
		t.Fatalf("reset warnings = %d, want 2 (one per reset episode)\n%s", got, log.String())
	}
}

func TestAccumulatorFirstObservation(t *testing.T) {
	stats := loadStatsFixture(t, "stats_first.json")
	acc, _ := newTestAccumulator(t)

	obs := acc.observe(rawStats{containerID: "c1", stats: stats}, time.Unix(1_000_000, 0))
	if obs.window != 0 {
		t.Fatalf("first observation window = %s, want 0", obs.window)
	}
	if obs.cpuKnown {
		t.Fatal("first observation emitted a CPU sample")
	}
	if len(obs.networks) != 0 {
		t.Fatalf("first observation emitted %d network rates, want none", len(obs.networks))
	}
	if obs.memoryUsedBytes == 0 {
		t.Fatal("first observation did not report a memory gauge")
	}
}

func TestAccumulatorSubMillisecondWindow(t *testing.T) {
	base := time.Unix(1_000_000, 0)
	first := loadStatsFixture(t, "stats_first.json")
	second := loadStatsFixture(t, "stats_second.json")

	acc, _ := newTestAccumulator(t)
	acc.observe(rawStats{containerID: "c1", stats: first}, base)
	obs := acc.observe(rawStats{containerID: "c1", stats: second}, base.Add(500*time.Microsecond))

	if obs.window != 500*time.Microsecond {
		t.Fatalf("window = %s, want 500µs", obs.window)
	}
	if obs.cpuKnown || len(obs.networks) != 0 {
		t.Fatalf("sub-millisecond window emitted rates: cpu=%v networks=%d",
			obs.cpuKnown, len(obs.networks))
	}
}

func TestAccumulatorPrune(t *testing.T) {
	base := time.Unix(1_000_000, 0)
	first := loadStatsFixture(t, "stats_first.json")
	first.Networks = map[string]container.NetworkStats{"eth0": networkCounters(2000, 2000)}
	second := loadStatsFixture(t, "stats_second.json")
	second.Networks = map[string]container.NetworkStats{"eth0": networkCounters(1000, 3000)}

	acc, _ := newTestAccumulator(t)
	acc.observe(rawStats{containerID: "c1", stats: first}, base)
	acc.observe(rawStats{containerID: "c1", stats: second}, base.Add(time.Second))

	other := loadStatsFixture(t, "stats_first.json")
	acc.observe(rawStats{containerID: "c2", stats: other}, base.Add(time.Second))

	if len(acc.warned) == 0 {
		t.Fatal("expected a reset warning for c1 before pruning")
	}

	acc.prune([]string{"c2"})
	if _, ok := acc.containers["c1"]; ok {
		t.Fatal("state for removed container c1 was retained")
	}
	if _, ok := acc.containers["c2"]; !ok {
		t.Fatal("state for active container c2 was pruned")
	}
	if len(acc.warned) != 0 {
		t.Fatalf("warning flags for removed container = %d, want 0", len(acc.warned))
	}

	obs := acc.observe(rawStats{containerID: "c1", stats: second}, base.Add(2*time.Second))
	if obs.window != 0 || obs.cpuKnown || len(obs.networks) != 0 {
		t.Fatalf("rediscovered container reused stale state: window=%s cpu=%v networks=%d",
			obs.window, obs.cpuKnown, len(obs.networks))
	}
}

func TestAccumulatorNewContainerID(t *testing.T) {
	base := time.Unix(1_000_000, 0)
	first := loadStatsFixture(t, "stats_first.json")
	second := loadStatsFixture(t, "stats_second.json")

	acc, _ := newTestAccumulator(t)
	acc.observe(rawStats{containerID: "old", stats: first}, base)
	acc.observe(rawStats{containerID: "old", stats: second}, base.Add(time.Second))

	obs := acc.observe(rawStats{containerID: "new", stats: second}, base.Add(time.Second))
	if obs.window != 0 || obs.cpuKnown || len(obs.networks) != 0 {
		t.Fatalf("re-created container with a new ID reused old state: window=%s cpu=%v networks=%d",
			obs.window, obs.cpuKnown, len(obs.networks))
	}
}

func newTestAccumulator(t *testing.T) (*accumulator, *bytes.Buffer) {
	t.Helper()
	var log bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return newAccumulator(logger), &log
}

func networkCounters(rx, tx uint64) container.NetworkStats {
	return container.NetworkStats{RxBytes: rx, TxBytes: tx}
}

func networkByName(obs containerMetrics, name string) (networkRate, bool) {
	for _, rate := range obs.networks {
		if rate.name == name {
			return rate, true
		}
	}
	return networkRate{}, false
}
