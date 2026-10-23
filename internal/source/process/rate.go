package process

import "time"

// userHZ is the unit of utime and stime. The Linux userspace ABI fixes USER_HZ
// at 100 regardless of the kernel's internal CONFIG_HZ.
const userHZ = 100

// cpuPercent returns per-core CPU percent for one process between two stat
// snapshots. 100 means one core fully busy, so a multi-threaded process can
// exceed it.
//
// It reports false, meaning the rate is unavailable for this tick, when the PID
// was reused (starttime changed), a counter went backwards, or the measurement
// window is shorter than a millisecond. A zero tick delta is a valid zero
// percent: an idle process still has a series.
func cpuPercent(prev, cur statSnapshot, elapsed time.Duration) (float64, bool) {
	if cur.starttime != prev.starttime || elapsed < time.Millisecond {
		return 0, false
	}
	if cur.utime < prev.utime || cur.stime < prev.stime {
		return 0, false
	}

	ticks := (cur.utime - prev.utime) + (cur.stime - prev.stime)
	return float64(ticks) / userHZ / elapsed.Seconds() * 100, true
}
