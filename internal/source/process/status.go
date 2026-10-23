package process

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// parseStatus parses /proc/[pid]/status and returns VmRSS in bytes. A missing
// VmRSS is not an error: zombies have no resident set, and zero is the correct
// gauge value for them.
func parseStatus(r io.Reader) (uint64, error) {
	var rssBytes uint64
	var found bool

	scanner := bufio.NewScanner(r)
	line := 0
	for scanner.Scan() {
		line++
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 || fields[0] != "VmRSS:" {
			continue
		}
		if found {
			return 0, fmt.Errorf("line %d: duplicate VmRSS", line)
		}
		if len(fields) < 2 {
			return 0, fmt.Errorf("line %d: VmRSS: missing value", line)
		}
		if len(fields) >= 3 && fields[2] != "kB" {
			return 0, fmt.Errorf("line %d: VmRSS: unexpected unit %q", line, fields[2])
		}

		kilobytes, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("line %d: VmRSS: %w", line, err)
		}
		if kilobytes > math.MaxUint64/1024 {
			return 0, fmt.Errorf("line %d: VmRSS: value overflows bytes", line)
		}
		rssBytes = kilobytes * 1024
		found = true
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("read status: %w", err)
	}
	return rssBytes, nil
}
