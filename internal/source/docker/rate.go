package docker

import (
	"time"

	"github.com/docker/docker/api/types/container"
)

// cpuSnapshot is the part of a container's cpu_stats needed for a CPU rate.
// The baseline is smob's own previous observation: Docker's precpu_stats is
// empty in one-shot responses, and diffing our own payloads makes the CPU ratio
// and interval_ms cover exactly the same window in both acquisition modes,
// including when stream backpressure drops a payload.
type cpuSnapshot struct {
	totalUsage  uint64
	systemUsage uint64
}

// snapshotCPU extracts the cumulative CPU counters used as the next baseline.
func snapshotCPU(stats container.CPUStats) cpuSnapshot {
	return cpuSnapshot{
		totalUsage:  stats.CPUUsage.TotalUsage,
		systemUsage: stats.SystemUsage,
	}
}

// cpuPercent returns per-core CPU percent between two observations of one
// container. 100 means one core fully busy, so a multi-threaded workload may
// exceed 100 and is never capped.
//
// It reports false, meaning no CPU sample for this tick, when the window is not
// positive, system time did not advance, a counter went backwards, or neither
// online_cpus nor percpu_usage identifies the scaling factor. The caller updates
// its baseline regardless, so a dropped tick resumes from the current snapshot.
func cpuPercent(prev cpuSnapshot, cur container.CPUStats, elapsed time.Duration) (float64, bool) {
	if elapsed <= 0 || cur.SystemUsage <= prev.systemUsage {
		return 0, false
	}
	if cur.CPUUsage.TotalUsage < prev.totalUsage {
		return 0, false
	}

	online := cur.OnlineCPUs
	if online == 0 {
		online = uint32(len(cur.CPUUsage.PercpuUsage))
	}
	if online == 0 {
		return 0, false
	}

	cpuDelta := cur.CPUUsage.TotalUsage - prev.totalUsage
	systemDelta := cur.SystemUsage - prev.systemUsage
	return float64(cpuDelta) / float64(systemDelta) * float64(online) * 100, true
}

// memoryUsedBytes returns the container's working set: usage minus
// inactive_file. cgroup v1 reports total_inactive_file and v2 reports
// inactive_file; v1 is preferred when both are present. The result is floored
// at zero.
//
// When neither key is present the daemon gave us no inactive-file figure, so
// usage is returned unchanged rather than inventing a subtraction. That matches
// Docker's own fallback and is documented in the README; it may include
// reclaimable page cache.
func memoryUsedBytes(mem container.MemoryStats) uint64 {
	if inactive, ok := mem.Stats["total_inactive_file"]; ok {
		return subtractFloor(mem.Usage, inactive)
	}
	if inactive, ok := mem.Stats["inactive_file"]; ok {
		return subtractFloor(mem.Usage, inactive)
	}
	return mem.Usage
}

// subtractFloor returns value - sub, or zero when sub is at least value.
func subtractFloor(value, sub uint64) uint64 {
	if sub >= value {
		return 0
	}
	return value - sub
}

// bytesPerSecond converts a cumulative counter delta into a per-second rate. It
// reports false when the counter reset or the window is not positive.
func bytesPerSecond(prev, cur uint64, elapsed time.Duration) (float64, bool) {
	if elapsed <= 0 || cur < prev {
		return 0, false
	}
	return float64(cur-prev) / elapsed.Seconds(), true
}
