package process

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// statSnapshot holds the /proc/[pid]/stat fields smob records. The three
// counters are USER_HZ ticks; the Linux userspace ABI fixes USER_HZ at 100, so
// callers convert ticks to time with that constant.
type statSnapshot struct {
	pid       int
	comm      string
	utime     uint64
	stime     uint64
	starttime uint64
}

// parseStat parses /proc/[pid]/stat. The comm field is wrapped in parentheses
// and may itself contain spaces, parentheses, or a newline, so the line is cut
// at the last ')' rather than the first; every field after it is numeric.
//
// The fields after the closing parenthesis start at field 3 (state), which
// makes utime field 14, stime field 15, and starttime field 22 of proc(5).
func parseStat(r io.Reader) (statSnapshot, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return statSnapshot{}, fmt.Errorf("read stat: %w", err)
	}
	text := string(data)

	open := strings.IndexByte(text, '(')
	if open < 0 {
		return statSnapshot{}, errors.New("missing '(' after pid")
	}
	pid, err := strconv.Atoi(strings.TrimSpace(text[:open]))
	if err != nil {
		return statSnapshot{}, fmt.Errorf("field 1 (pid): %w", err)
	}
	if pid <= 0 {
		return statSnapshot{}, fmt.Errorf("field 1 (pid): %d is not positive", pid)
	}

	close := strings.LastIndexByte(text, ')')
	if close < open {
		return statSnapshot{}, errors.New("missing ')' after comm")
	}
	comm := text[open+1 : close]

	fields := strings.Fields(text[close+1:])
	const (
		indexUtime     = 11 // field 14
		indexStime     = 12 // field 15
		indexStarttime = 19 // field 22
	)
	if len(fields) <= indexStarttime {
		return statSnapshot{}, fmt.Errorf("got %d fields after comm, want at least %d", len(fields), indexStarttime+1)
	}

	utime, err := statField(fields, indexUtime, "14 (utime)")
	if err != nil {
		return statSnapshot{}, err
	}
	stime, err := statField(fields, indexStime, "15 (stime)")
	if err != nil {
		return statSnapshot{}, err
	}
	starttime, err := statField(fields, indexStarttime, "22 (starttime)")
	if err != nil {
		return statSnapshot{}, err
	}

	return statSnapshot{
		pid:       pid,
		comm:      comm,
		utime:     utime,
		stime:     stime,
		starttime: starttime,
	}, nil
}

// statField decodes one counter after the comm. index is zero-based among the
// fields that follow the closing parenthesis, where /proc field 3 is index 0.
func statField(fields []string, index int, name string) (uint64, error) {
	value, err := strconv.ParseUint(fields[index], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("field %s: %w", name, err)
	}
	return value, nil
}
