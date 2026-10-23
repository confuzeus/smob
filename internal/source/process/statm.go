package process

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

// parseStatm parses /proc/[pid]/statm and returns the resident set size in
// pages. statm reports seven page counts; only the second (resident) is
// recorded, so trailing fields are ignored.
func parseStatm(r io.Reader) (uint64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return 0, fmt.Errorf("read statm: %w", err)
	}

	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0, fmt.Errorf("got %d fields, want at least 2", len(fields))
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("resident: %w", err)
	}
	return pages, nil
}
