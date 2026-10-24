package docker

import (
	"log/slog"
	"maps"
	"slices"
	"time"
)

const (
	metricContainerCPUPercent              = "container.cpu.percent"
	metricContainerMemoryUsedBytes         = "container.memory.used_bytes"
	metricContainerNetworkRXBytesPerSecond = "container.network.rx_bytes_per_second"
	metricContainerNetworkTXBytesPerSecond = "container.network.tx_bytes_per_second"

	// minRateWindow keeps every rate's interval_ms positive. The process and
	// system sources use the same lower bound.
	minRateWindow = time.Millisecond
)

// containerMetrics is the metric set computed from one stats payload. Labels
// and sample construction belong to the source; this type only reports values
// and whether each rate is usable for this observation.
type containerMetrics struct {
	containerID     string
	memoryUsedBytes uint64
	// window is the monotonic interval since the previous observation of this
	// container. It is the interval_ms for every rate in this observation and is
	// zero on the first observation.
	window     time.Duration
	cpuPercent float64
	cpuKnown   bool
	networks   []networkRate
}

// networkRate is one interface's rate values for one observation. RX and TX are
// independent because a reset can affect only one direction.
type networkRate struct {
	name    string
	rx      float64
	tx      float64
	rxKnown bool
	txKnown bool
}

// netCounters is the cumulative network counter baseline for one interface.
type netCounters struct {
	rx uint64
	tx uint64
}

// containerRateState is the previous observation of one container. The
// container ID is the map key; interface state lives here so an interface that
// disappears is forgotten with its container.
type containerRateState struct {
	at         time.Time
	cpu        cpuSnapshot
	interfaces map[string]netCounters
}

// networkResetKey identifies one rate series for reset-warning suppression.
type networkResetKey struct {
	containerID   string
	interfaceName string
	metric        string
}

// accumulator computes container metrics across observations. Callers must
// keep one accumulator for the life of the source and call observe from one
// goroutine: one-shot ticks and the stream consumer are each single-threaded.
// prune bounds state to the containers that are currently discovered.
type accumulator struct {
	logger     *slog.Logger
	containers map[string]*containerRateState
	warned     map[networkResetKey]bool
}

// newAccumulator returns an accumulator with empty state.
func newAccumulator(logger *slog.Logger) *accumulator {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &accumulator{
		logger:     logger,
		containers: make(map[string]*containerRateState),
		warned:     make(map[networkResetKey]bool),
	}
}

// observe computes one payload's metrics and advances the baseline for the
// container. at must be time.Now() or another monotonic reading; it is never a
// daemon Read timestamp, so a wall-clock step cannot change a rate window.
//
// The first observation, a window shorter than a millisecond, and a dropped CPU
// baseline produce no CPU sample. Network rates are absent on an interface's
// first observation and when its counters reset; memory is a gauge and is always
// reported.
func (a *accumulator) observe(payload rawStats, at time.Time) containerMetrics {
	id := payload.containerID
	stats := payload.stats

	observed := containerMetrics{
		containerID:     id,
		memoryUsedBytes: memoryUsedBytes(stats.MemoryStats),
	}

	previous, seen := a.containers[id]
	if seen {
		observed.window = at.Sub(previous.at)
	}

	next := &containerRateState{
		at:         at,
		cpu:        snapshotCPU(stats.CPUStats),
		interfaces: make(map[string]netCounters, len(stats.Networks)),
	}

	if seen && observed.window >= minRateWindow {
		if percent, ok := cpuPercent(previous.cpu, stats.CPUStats, observed.window); ok {
			observed.cpuPercent = percent
			observed.cpuKnown = true
		}
	}

	for _, name := range slices.Sorted(maps.Keys(stats.Networks)) {
		current := stats.Networks[name]
		next.interfaces[name] = netCounters{rx: current.RxBytes, tx: current.TxBytes}

		if !seen || observed.window < minRateWindow {
			continue
		}
		prev, ok := previous.interfaces[name]
		if !ok {
			continue
		}

		rate := networkRate{name: name}
		if value, ok := bytesPerSecond(prev.rx, current.RxBytes, observed.window); ok {
			rate.rx = value
			rate.rxKnown = true
			a.clearReset(id, name, metricContainerNetworkRXBytesPerSecond)
		} else {
			a.warnReset(id, name, metricContainerNetworkRXBytesPerSecond)
		}
		if value, ok := bytesPerSecond(prev.tx, current.TxBytes, observed.window); ok {
			rate.tx = value
			rate.txKnown = true
			a.clearReset(id, name, metricContainerNetworkTXBytesPerSecond)
		} else {
			a.warnReset(id, name, metricContainerNetworkTXBytesPerSecond)
		}
		if rate.rxKnown || rate.txKnown {
			observed.networks = append(observed.networks, rate)
		}
	}

	if seen {
		for name := range previous.interfaces {
			if _, ok := stats.Networks[name]; !ok {
				a.clearInterfaceWarnings(id, name)
			}
		}
	}
	a.containers[id] = next
	return observed
}

// prune drops state for containers that are no longer discovered, including
// their reset-warning flags. Callers pass the discovered running IDs, not the
// IDs of a possibly partial stats batch: a failed stats request must not reset
// a live container's baseline.
func (a *accumulator) prune(activeContainerIDs []string) {
	keep := make(map[string]struct{}, len(activeContainerIDs))
	for _, id := range activeContainerIDs {
		keep[id] = struct{}{}
	}
	for id := range a.containers {
		if _, ok := keep[id]; !ok {
			delete(a.containers, id)
		}
	}
	for key := range a.warned {
		if _, ok := keep[key.containerID]; !ok {
			delete(a.warned, key)
		}
	}
}

// warnReset logs one counter reset per series until a valid delta resumes.
func (a *accumulator) warnReset(containerID, interfaceName, metric string) {
	key := networkResetKey{containerID: containerID, interfaceName: interfaceName, metric: metric}
	if a.warned[key] {
		return
	}
	a.warned[key] = true
	a.logger.Warn("container network counter reset; dropping rate for one tick",
		"container", containerID, "interface", interfaceName, "metric", metric)
}

// clearReset forgets a previously logged reset after a valid delta.
func (a *accumulator) clearReset(containerID, interfaceName, metric string) {
	delete(a.warned, networkResetKey{
		containerID:   containerID,
		interfaceName: interfaceName,
		metric:        metric,
	})
}

// clearInterfaceWarnings forgets a removed interface's reset flags, so a later
// reappearance is treated as a new series.
func (a *accumulator) clearInterfaceWarnings(containerID, interfaceName string) {
	for key := range a.warned {
		if key.containerID == containerID && key.interfaceName == interfaceName {
			delete(a.warned, key)
		}
	}
}
