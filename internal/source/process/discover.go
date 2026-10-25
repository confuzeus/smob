package process

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"time"
)

// pidCache enumerates process IDs below a proc root and refreshes the list at
// rescanInterval. A rescan that fails after the first successful scan keeps the
// previous set and is retried on the next rescan, so one failing proc root does
// not abort every tick. The collector drives it from one goroutine, so it is not
// safe for concurrent use.
type pidCache struct {
	procPath string
	rescan   time.Duration
	logger   *slog.Logger
	pids     []int
	lastScan time.Time
	// scanned is set by the first successful scan. Before that there is no
	// previous set to keep, so a failure is returned to the caller.
	scanned bool
	// scanErr is the message of the persistent rescan failure. It suppresses
	// repeated warnings until a scan succeeds.
	scanErr string
}

// newPIDCache returns a cache rooted at procPath.
func newPIDCache(procPath string, rescan time.Duration, logger *slog.Logger) *pidCache {
	return &pidCache{procPath: procPath, rescan: rescan, logger: logger}
}

// list returns the current PIDs, rescanning the proc root when the rescan
// interval has elapsed. refreshed reports whether this call performed a
// successful scan, so the source can reset its per-PID warning state. A failed
// rescan keeps the previous set and logs once rather than returning an error
// that would abort the tick. now must come from the collector's clock;
// time.Sub then uses the monotonic reading.
func (c *pidCache) list(ctx context.Context, now time.Time) (pids []int, refreshed bool, err error) {
	if !c.lastScan.IsZero() && now.Sub(c.lastScan) < c.rescan {
		return c.pids, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	pids, err = listPIDs(c.procPath)
	if err != nil {
		if !c.scanned {
			// No previous set exists, so this tick has nothing to collect. The
			// caller reports it; the next tick retries immediately.
			return nil, false, err
		}
		// Keep reading the known PIDs and wait a full rescan interval before
		// trying the root again.
		c.lastScan = now
		c.warnScan(err)
		return c.pids, false, nil
	}
	c.clearScanWarning(len(pids))
	c.pids = pids
	c.lastScan = now
	c.scanned = true
	return c.pids, true, nil
}

// warnScan logs the first occurrence of a persistent scan failure at warn and
// later repeats at debug. A different error is a different failure and is
// reported again.
func (c *pidCache) warnScan(err error) {
	message := err.Error()
	if message != c.scanErr {
		c.scanErr = message
		c.logger.Warn("scan processes; keeping the previous set and retrying on the next rescan", "error", err)
		return
	}
	c.logger.Debug("process scan still failing", "error", err)
}

// clearScanWarning re-arms warning suppression after a successful scan.
func (c *pidCache) clearScanWarning(found int) {
	if c.scanErr == "" {
		return
	}
	c.scanErr = ""
	c.logger.Info("process scan recovered", "pids", found)
}

// listPIDs returns the numeric process directories in procPath in ascending
// order. Entries such as "self", "sys", and "net" do not parse as positive
// integers and are skipped.
func listPIDs(procPath string) ([]int, error) {
	entries, err := os.ReadDir(procPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", procPath, err)
	}

	pids := make([]int, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		pids = append(pids, pid)
	}
	slices.Sort(pids)
	return pids, nil
}
