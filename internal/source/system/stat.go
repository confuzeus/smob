package system

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// cpuTimes holds the CPU counters from one /proc/stat cpu line. Values are in
// USER_HZ ticks; only their deltas are meaningful to callers.
type cpuTimes struct {
	user      uint64
	nice      uint64
	system    uint64
	idle      uint64
	iowait    uint64
	irq       uint64
	softirq   uint64
	steal     uint64
	guest     uint64
	guestNice uint64
}

// total returns the sum of the counters excluding guest and guest_nice, which
// the kernel already includes in user and nice.
func (c cpuTimes) total() uint64 {
	return c.user + c.nice + c.system + c.idle + c.iowait + c.irq + c.softirq + c.steal
}

// busy returns the time not spent idle or waiting on I/O.
func (c cpuTimes) busy() uint64 {
	idle := c.idle + c.iowait
	total := c.total()
	if idle > total {
		return 0
	}
	return total - idle
}

// statSnapshot is the parsed form of /proc/stat: the aggregate cpu line plus
// each per-CPU line. Per-CPU indexes may have gaps when CPUs are offlined.
type statSnapshot struct {
	all  cpuTimes
	cpus map[int]cpuTimes
}

// parseStat parses /proc/stat. It requires the aggregate cpu line and accepts
// four or more counter fields per line; kernels have added counters over time,
// so trailing fields beyond the ten this package models are ignored.
func parseStat(r io.Reader) (statSnapshot, error) {
	s := statSnapshot{cpus: make(map[int]cpuTimes)}

	scanner := bufio.NewScanner(r)
	// The intr line lists one counter per IRQ, so hosts with thousands of
	// interrupts exceed the scanner's 64 KiB default token limit.
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	line := 0
	foundAll := false
	for scanner.Scan() {
		line++
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}

		name := fields[0]
		switch {
		case name == "cpu":
			if foundAll {
				return statSnapshot{}, fmt.Errorf("line %d: duplicate cpu line", line)
			}
			times, err := parseCPUTimes(fields[1:])
			if err != nil {
				return statSnapshot{}, fmt.Errorf("line %d: cpu: %w", line, err)
			}
			s.all, foundAll = times, true
		case strings.HasPrefix(name, "cpu"):
			index, err := strconv.Atoi(strings.TrimPrefix(name, "cpu"))
			if err != nil || index < 0 {
				// Not a per-CPU line, such as a future cpu-prefixed key.
				continue
			}
			if _, ok := s.cpus[index]; ok {
				return statSnapshot{}, fmt.Errorf("line %d: duplicate cpu%d line", line, index)
			}
			times, err := parseCPUTimes(fields[1:])
			if err != nil {
				return statSnapshot{}, fmt.Errorf("line %d: cpu%d: %w", line, index, err)
			}
			s.cpus[index] = times
		}
	}
	if err := scanner.Err(); err != nil {
		// Scan stops before consuming the failed line, so it is the next one.
		return statSnapshot{}, fmt.Errorf("line %d: %w", line+1, err)
	}
	if !foundAll {
		return statSnapshot{}, errors.New("missing aggregate cpu line")
	}
	return s, nil
}

// parseCPUTimes decodes the counters after the cpu name. Kernels before 2.6.33
// emit four fields; modern kernels emit ten. A future kernel may append fields,
// which are accepted but not modelled.
func parseCPUTimes(fields []string) (cpuTimes, error) {
	if len(fields) < 4 {
		return cpuTimes{}, fmt.Errorf("got %d fields, want at least 4", len(fields))
	}

	var times cpuTimes
	dst := [...]*uint64{
		&times.user,
		&times.nice,
		&times.system,
		&times.idle,
		&times.iowait,
		&times.irq,
		&times.softirq,
		&times.steal,
		&times.guest,
		&times.guestNice,
	}
	for i, field := range fields {
		if i == len(dst) {
			break
		}
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return cpuTimes{}, fmt.Errorf("field %d: %w", i+1, err)
		}
		*dst[i] = value
	}
	return times, nil
}
