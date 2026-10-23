package collector

import "time"

// windowFor returns the half-open UTC-aligned chunk window that contains ts.
// The interval must be a positive whole number of seconds that divides one hour;
// configuration validation enforces that, so every window starts on a second
// boundary and never crosses an hour directory.
func windowFor(ts time.Time, interval time.Duration) (start, end time.Time) {
	start = ts.UTC().Truncate(interval)
	return start, start.Add(interval)
}
