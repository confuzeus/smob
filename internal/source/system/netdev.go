package system

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// netCounters holds the byte counters for one network interface. The values are
// cumulative; only their deltas are meaningful to callers.
type netCounters struct {
	rxBytes uint64
	txBytes uint64
}

// netDevSnapshot maps an interface name to its cumulative counters.
type netDevSnapshot map[string]netCounters

// parseNetDev parses /proc/net/dev. The two leading header lines have no colon
// and are skipped; any later line without a colon is malformed, because it may
// be a data line whose colon was lost. Each data line has sixteen counters, with
// received bytes first and transmitted bytes ninth.
func parseNetDev(r io.Reader) (netDevSnapshot, error) {
	snapshot := make(netDevSnapshot)

	scanner := bufio.NewScanner(r)
	line := 0
	headers := 2
	for scanner.Scan() {
		line++
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}

		name, counters, ok := strings.Cut(text, ":")
		if !ok {
			if headers > 0 {
				headers--
				continue
			}
			return nil, fmt.Errorf("line %d: missing ':' separator", line)
		}
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("line %d: empty interface name", line)
		}

		fields := strings.Fields(counters)
		if len(fields) < 16 {
			return nil, fmt.Errorf("line %d: interface %s: got %d fields, want at least 16", line, name, len(fields))
		}
		rx, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("line %d: interface %s: rx_bytes: %w", line, name, err)
		}
		tx, err := strconv.ParseUint(fields[8], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("line %d: interface %s: tx_bytes: %w", line, name, err)
		}
		if _, ok := snapshot[name]; ok {
			return nil, fmt.Errorf("line %d: duplicate interface %s", line, name)
		}
		snapshot[name] = netCounters{rxBytes: rx, txBytes: tx}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read net/dev: %w", err)
	}
	if len(snapshot) == 0 {
		return nil, errors.New("no interfaces")
	}
	return snapshot, nil
}
