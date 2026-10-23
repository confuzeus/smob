package system

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// meminfoSnapshot holds the memory figures smob records. Both values are bytes;
// /proc/meminfo reports them in kilobytes.
type meminfoSnapshot struct {
	totalBytes     uint64
	availableBytes uint64
}

// parseMeminfo parses /proc/meminfo. It extracts MemTotal and MemAvailable and
// ignores every other field. Both are required: MemAvailable has been present
// since Linux 3.14, so a missing value means the input is not usable.
func parseMeminfo(r io.Reader) (meminfoSnapshot, error) {
	var snapshot meminfoSnapshot
	var foundTotal, foundAvailable bool

	scanner := bufio.NewScanner(r)
	line := 0
	for scanner.Scan() {
		line++
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}

		var dst *uint64
		var found *bool
		switch fields[0] {
		case "MemTotal:":
			dst, found = &snapshot.totalBytes, &foundTotal
		case "MemAvailable:":
			dst, found = &snapshot.availableBytes, &foundAvailable
		default:
			continue
		}

		if *found {
			return meminfoSnapshot{}, fmt.Errorf("line %d: duplicate %s", line, fields[0])
		}
		if len(fields) < 2 {
			return meminfoSnapshot{}, fmt.Errorf("line %d: %s: missing value", line, fields[0])
		}
		if len(fields) >= 3 && fields[2] != "kB" {
			return meminfoSnapshot{}, fmt.Errorf("line %d: %s: unexpected unit %q", line, fields[0], fields[2])
		}

		kilobytes, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return meminfoSnapshot{}, fmt.Errorf("line %d: %s: %w", line, fields[0], err)
		}
		if kilobytes > math.MaxUint64/1024 {
			return meminfoSnapshot{}, fmt.Errorf("line %d: %s: value overflows bytes", line, fields[0])
		}

		*dst = kilobytes * 1024
		*found = true
	}
	if err := scanner.Err(); err != nil {
		return meminfoSnapshot{}, fmt.Errorf("read meminfo: %w", err)
	}

	switch {
	case !foundTotal:
		return meminfoSnapshot{}, errors.New("missing MemTotal")
	case !foundAvailable:
		return meminfoSnapshot{}, errors.New("missing MemAvailable")
	}
	return snapshot, nil
}
