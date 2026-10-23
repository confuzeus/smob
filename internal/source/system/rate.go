package system

import "time"

// values returns the raw CPU counters in /proc/stat field order. guest and
// guest_nice are already included in user and nice, but they are still compared
// when detecting resets.
func (c cpuTimes) values() [10]uint64 {
	return [10]uint64{
		c.user,
		c.nice,
		c.system,
		c.idle,
		c.iowait,
		c.irq,
		c.softirq,
		c.steal,
		c.guest,
		c.guestNice,
	}
}

// resetFrom reports whether any raw CPU counter went backwards, which the
// kernel does when counters restart after a reboot or CPU hotplug.
func (c cpuTimes) resetFrom(prev cpuTimes) bool {
	cur := c.values()
	old := prev.values()
	for i := range cur {
		if cur[i] < old[i] {
			return true
		}
	}
	return false
}

// cpuPercent returns the busy percentage between two snapshots of one CPU
// series. Busy time is total minus idle and iowait. It reports false when the
// counters reset or the total did not advance, so the caller drops the tick
// instead of reporting a bogus rate.
func cpuPercent(prev, cur cpuTimes) (float64, bool) {
	if cur.resetFrom(prev) {
		return 0, false
	}

	totalDelta := cur.total() - prev.total()
	if totalDelta == 0 {
		return 0, false
	}
	busyDelta := cur.busy() - prev.busy()

	percent := float64(busyDelta) / float64(totalDelta) * 100
	if percent < 0 {
		return 0, false
	}
	if percent > 100 {
		percent = 100
	}
	return percent, true
}

// bytesPerSecond converts a cumulative counter delta into a per-second rate. It
// reports false when the counter reset or the window is not positive.
func bytesPerSecond(prev, cur uint64, elapsed time.Duration) (float64, bool) {
	if elapsed <= 0 || cur < prev {
		return 0, false
	}
	return float64(cur-prev) / elapsed.Seconds(), true
}
