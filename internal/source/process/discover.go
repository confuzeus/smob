package process

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"
)

// pidCache enumerates process IDs below a proc root and refreshes the list at
// rescanInterval. The collector drives it from one goroutine, so it is not safe
// for concurrent use.
type pidCache struct {
	procPath string
	rescan   time.Duration
	pids     []int
	lastScan time.Time
}

// newPIDCache returns a cache rooted at procPath.
func newPIDCache(procPath string, rescan time.Duration) *pidCache {
	return &pidCache{procPath: procPath, rescan: rescan}
}

// list returns the current PIDs, rescanning the proc root when the rescan
// interval has elapsed. refreshed reports whether this call performed a scan,
// so the source can reset its per-PID warning state. now must come from the
// collector's clock; time.Sub then uses the monotonic reading.
func (c *pidCache) list(ctx context.Context, now time.Time) (pids []int, refreshed bool, err error) {
	if !c.lastScan.IsZero() && now.Sub(c.lastScan) < c.rescan {
		return c.pids, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	pids, err = listPIDs(c.procPath)
	if err != nil {
		return nil, false, err
	}
	c.pids = pids
	c.lastScan = now
	return c.pids, true, nil
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
