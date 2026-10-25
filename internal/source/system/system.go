package system

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/confuzeus/smob/internal/model"
)

const (
	scope = "system"

	metricCPUPercent              = "system.cpu.percent"
	metricMemoryTotalBytes        = "system.memory.total_bytes"
	metricMemoryAvailableBytes    = "system.memory.available_bytes"
	metricMemoryUsedBytes         = "system.memory.used_bytes"
	metricNetworkRXBytesPerSecond = "system.network.rx_bytes_per_second"
	metricNetworkTXBytesPerSecond = "system.network.tx_bytes_per_second"
)

// snapshot is one consistent-enough reading of the /proc files the system
// source needs. at keeps the monotonic reading for rate math; sample timestamps
// are derived from it separately.
type snapshot struct {
	at     time.Time
	bootID string
	stat   statSnapshot
	mem    meminfoSnapshot
	net    netDevSnapshot
}

// Source collects host-wide CPU, memory, and network metrics from /proc.
type Source struct {
	procPath string
	interval time.Duration
	logger   *slog.Logger

	prev   *snapshot
	warned map[string]bool
}

// New returns a system source rooted at procPath. Every required /proc file is
// read once so startup fails instead of degrading silently; those readings are
// discarded, so the first tick emits gauges only.
func New(procPath string, interval time.Duration, logger *slog.Logger) (*Source, error) {
	if procPath == "" {
		return nil, fmt.Errorf("system: proc path is empty")
	}
	if interval <= 0 {
		return nil, fmt.Errorf("system: interval must be positive; got %s", interval)
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	s := &Source{
		procPath: procPath,
		interval: interval,
		logger:   logger,
		warned:   make(map[string]bool),
	}
	if _, err := s.snapshot(context.Background(), time.Now()); err != nil {
		return nil, err
	}
	return s, nil
}

// Scope implements the collector's Source interface.
func (s *Source) Scope() string { return scope }

// Interval implements the collector's Source interface.
func (s *Source) Interval() time.Duration { return s.interval }

// Collect implements the collector's Source interface. The first call emits
// memory gauges only; CPU and network rates need a previous snapshot.
func (s *Source) Collect(ctx context.Context, now time.Time) ([]model.Sample, error) {
	cur, err := s.snapshot(ctx, now)
	if err != nil {
		return nil, err
	}

	samples := memorySamples(cur.at.UTC(), cur.mem)
	if s.prev != nil {
		elapsed := cur.at.Sub(s.prev.at)
		switch {
		case cur.bootID != s.prev.bootID:
			s.logger.Warn("boot id changed; dropping rate deltas",
				"previous", s.prev.bootID, "current", cur.bootID)
			clear(s.warned)
		case elapsed >= time.Millisecond:
			samples = append(samples, s.rateSamples(cur, elapsed)...)
		}
	}
	s.pruneResetWarnings(cur)
	s.prev = cur
	return samples, nil
}

// rateSamples computes CPU and network rates between the previous snapshot and
// cur. Series that reset are skipped for this tick.
func (s *Source) rateSamples(cur *snapshot, elapsed time.Duration) []model.Sample {
	prev := s.prev
	samples := make([]model.Sample, 0, 1+len(prev.stat.cpus)+2*len(prev.net))

	addCPU := func(name string, before, after cpuTimes) {
		labels := map[string]string{"cpu": name}
		if after.resetFrom(before) {
			s.warnReset(metricCPUPercent, labels)
			return
		}
		percent, ok := cpuPercent(before, after)
		if !ok {
			return
		}
		s.clearReset(metricCPUPercent, labels)
		samples = append(samples, model.Sample{
			Timestamp:  cur.at.UTC(),
			Metric:     metricCPUPercent,
			Value:      percent,
			IntervalMS: model.Millis(elapsed),
			Labels:     labels,
		})
	}

	addCPU("all", prev.stat.all, cur.stat.all)
	for id, prevCPU := range prev.stat.cpus {
		if curCPU, ok := cur.stat.cpus[id]; ok {
			addCPU(strconv.Itoa(id), prevCPU, curCPU)
		}
	}

	for name, prevNet := range prev.net {
		curNet, ok := cur.net[name]
		if !ok {
			continue
		}
		samples = s.appendRate(samples, cur.at.UTC(), elapsed, metricNetworkRXBytesPerSecond,
			map[string]string{"interface": name}, prevNet.rxBytes, curNet.rxBytes)
		samples = s.appendRate(samples, cur.at.UTC(), elapsed, metricNetworkTXBytesPerSecond,
			map[string]string{"interface": name}, prevNet.txBytes, curNet.txBytes)
	}
	return samples
}

// appendRate appends one network rate, or logs the first reset for the series
// and drops the tick.
func (s *Source) appendRate(samples []model.Sample, at time.Time, elapsed time.Duration, metric string, labels map[string]string, prev, cur uint64) []model.Sample {
	value, ok := bytesPerSecond(prev, cur, elapsed)
	if !ok {
		s.warnReset(metric, labels)
		return samples
	}
	s.clearReset(metric, labels)

	return append(samples, model.Sample{
		Timestamp:  at,
		Metric:     metric,
		Value:      value,
		IntervalMS: model.Millis(elapsed),
		Labels:     labels,
	})
}

// warnReset logs one reset per series until a valid delta resumes.
func (s *Source) warnReset(metric string, labels map[string]string) {
	key := seriesKey(metric, labels)
	if s.warned[key] {
		return
	}
	s.warned[key] = true
	s.logger.Warn("counter reset; dropping rate for one tick", "metric", metric, "labels", labels)
}

// clearReset forgets a previously logged reset for the series.
func (s *Source) clearReset(metric string, labels map[string]string) {
	delete(s.warned, seriesKey(metric, labels))
}

// pruneResetWarnings bounds the reset-warning set to the series in cur, so
// interface and CPU churn cannot leak entries or suppress the first warning for
// a series that reappears later.
func (s *Source) pruneResetWarnings(cur *snapshot) {
	live := liveSeries(cur)
	for key := range s.warned {
		if _, ok := live[key]; !ok {
			delete(s.warned, key)
		}
	}
}

// liveSeries returns the reset-warning keys for every series in snap.
func liveSeries(snap *snapshot) map[string]struct{} {
	keys := make(map[string]struct{}, 1+len(snap.stat.cpus)+2*len(snap.net))
	keys[seriesKey(metricCPUPercent, map[string]string{"cpu": "all"})] = struct{}{}
	for id := range snap.stat.cpus {
		keys[seriesKey(metricCPUPercent, map[string]string{"cpu": strconv.Itoa(id)})] = struct{}{}
	}
	for name := range snap.net {
		labels := map[string]string{"interface": name}
		keys[seriesKey(metricNetworkRXBytesPerSecond, labels)] = struct{}{}
		keys[seriesKey(metricNetworkTXBytesPerSecond, labels)] = struct{}{}
	}
	return keys
}

// seriesKey returns a stable identity for one metric and label set.
func seriesKey(metric string, labels map[string]string) string {
	var b strings.Builder
	b.WriteString(metric)
	for _, key := range slices.Sorted(maps.Keys(labels)) {
		b.WriteByte('\x00')
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteString(labels[key])
	}
	return b.String()
}

// memorySamples converts the parsed meminfo figures into gauges.
func memorySamples(at time.Time, mem meminfoSnapshot) []model.Sample {
	var used uint64
	if mem.availableBytes < mem.totalBytes {
		used = mem.totalBytes - mem.availableBytes
	}
	return []model.Sample{
		{Timestamp: at, Metric: metricMemoryTotalBytes, Value: float64(mem.totalBytes)},
		{Timestamp: at, Metric: metricMemoryAvailableBytes, Value: float64(mem.availableBytes)},
		{Timestamp: at, Metric: metricMemoryUsedBytes, Value: float64(used)},
	}
}

// snapshot reads and parses every input the system source needs.
func (s *Source) snapshot(ctx context.Context, at time.Time) (*snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	bootID, err := readBootID(s.procPath)
	if err != nil {
		return nil, err
	}
	stat, err := readStatSnapshot(filepath.Join(s.procPath, "stat"))
	if err != nil {
		return nil, err
	}
	mem, err := readMeminfoSnapshot(filepath.Join(s.procPath, "meminfo"))
	if err != nil {
		return nil, err
	}
	net, err := readNetDevSnapshot(filepath.Join(s.procPath, "net/dev"))
	if err != nil {
		return nil, err
	}

	return &snapshot{at: at, bootID: bootID, stat: stat, mem: mem, net: net}, nil
}

// readBootID reads the kernel's boot identifier, which detects a reboot that
// restarted the counters. It runs on every tick; docs/IMPLEMENTATION.md says
// "at startup", which is weaker than what this does. Keep the per-tick read and
// reconcile the doc separately.
func readBootID(procPath string) (string, error) {
	path := filepath.Join(procPath, "sys/kernel/random/boot_id")
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	id := strings.TrimSpace(string(data))
	if id == "" {
		return "", fmt.Errorf("read %s: empty boot id", path)
	}
	return id, nil
}

func readStatSnapshot(path string) (statSnapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return statSnapshot{}, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	snapshot, err := parseStat(f)
	if err != nil {
		return statSnapshot{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return snapshot, nil
}

func readMeminfoSnapshot(path string) (meminfoSnapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return meminfoSnapshot{}, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	snapshot, err := parseMeminfo(f)
	if err != nil {
		return meminfoSnapshot{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return snapshot, nil
}

func readNetDevSnapshot(path string) (netDevSnapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return netDevSnapshot{}, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	snapshot, err := parseNetDev(f)
	if err != nil {
		return netDevSnapshot{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return snapshot, nil
}
