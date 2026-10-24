// Package chunk defines the chunk key layout and the deterministic JSONL-gzip
// encoding shared by the disk and S3 store backends.
//
// A chunk key is relative to the store root:
//
//	<host>/<scope>/YYYY/MM/DD/HH/<start>-<end>.jsonl.gz
//
// Both backends derive keys and contents from this package, so a backend cannot
// drift from the documented storage contract.
package chunk

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/confuzeus/smob/internal/model"
)

const (
	// FileSuffix identifies a completed chunk object in the key layout.
	FileSuffix = ".jsonl.gz"
	// TimeLayout is the UTC filename form: 20261023T160000Z.
	TimeLayout = "20060102T150405Z"
)

// Key returns the key of a chunk relative to the store root:
//
//	<host>/<scope>/YYYY/MM/DD/HH/<start>-<end>.jsonl.gz
//
// Start and end are normalized to UTC and must share a second boundary, because
// the filename only carries second precision. The hour directory comes from
// start; configuration guarantees a chunk never crosses an hour boundary.
// The result always uses forward slashes, including on disk.
func Key(host, scope string, start, end time.Time) (string, error) {
	if err := ValidateComponent("host", host); err != nil {
		return "", err
	}
	if err := ValidateComponent("scope", scope); err != nil {
		return "", err
	}

	start, end = start.UTC(), end.UTC()
	switch {
	case start.IsZero():
		return "", errors.New("start: must not be zero")
	case end.IsZero():
		return "", errors.New("end: must not be zero")
	case !end.After(start):
		return "", fmt.Errorf("end: must be after start; got %s and %s", start.Format(time.RFC3339), end.Format(time.RFC3339))
	case start.Nanosecond() != 0:
		return "", fmt.Errorf("start: must be aligned to a second; got %s", start.Format(time.RFC3339Nano))
	case end.Nanosecond() != 0:
		return "", fmt.Errorf("end: must be aligned to a second; got %s", end.Format(time.RFC3339Nano))
	}

	name := start.Format(TimeLayout) + "-" + end.Format(TimeLayout) + FileSuffix
	return path.Join(
		host,
		scope,
		start.Format("2006"),
		start.Format("01"),
		start.Format("02"),
		start.Format("15"),
		name,
	), nil
}

// CollisionKey inserts a numeric suffix before the file extension, turning
// <start>-<end>.jsonl.gz into <start>-<end>-1.jsonl.gz.
func CollisionKey(key string, n int) string {
	base := strings.TrimSuffix(key, FileSuffix)
	return base + "-" + strconv.Itoa(n) + FileSuffix
}

// ParseName parses a chunk filename, accepting the optional collision suffix
// written by the collision guard.
func ParseName(name string) (start, end time.Time, err error) {
	if !strings.HasSuffix(name, FileSuffix) {
		return time.Time{}, time.Time{}, fmt.Errorf("missing %s suffix", FileSuffix)
	}

	parts := strings.Split(strings.TrimSuffix(name, FileSuffix), "-")
	if len(parts) != 2 && len(parts) != 3 {
		return time.Time{}, time.Time{}, fmt.Errorf("want <start>-<end> with an optional collision suffix")
	}

	start, err = time.ParseInLocation(TimeLayout, parts[0], time.UTC)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("start: %w", err)
	}
	end, err = time.ParseInLocation(TimeLayout, parts[1], time.UTC)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("end: %w", err)
	}
	if !end.After(start) {
		return time.Time{}, time.Time{}, fmt.Errorf("end %s is not after start %s", end.Format(time.RFC3339), start.Format(time.RFC3339))
	}
	if len(parts) == 3 {
		n, err := strconv.Atoi(parts[2])
		if err != nil || n < 1 {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid collision suffix %q", parts[2])
		}
	}
	return start, end, nil
}

// ParseHourPath reports whether rel is an hour directory
// <host>/<scope>/YYYY/MM/DD/HH and returns its UTC hour.
func ParseHourPath(rel string) (time.Time, bool) {
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) != 6 {
		return time.Time{}, false
	}
	if !IsComponent(parts[0]) || !IsComponent(parts[1]) {
		return time.Time{}, false
	}
	if len(parts[2]) != 4 || len(parts[3]) != 2 || len(parts[4]) != 2 || len(parts[5]) != 2 {
		return time.Time{}, false
	}

	year, yearErr := strconv.Atoi(parts[2])
	month, monthErr := strconv.Atoi(parts[3])
	day, dayErr := strconv.Atoi(parts[4])
	hour, hourErr := strconv.Atoi(parts[5])
	if yearErr != nil || monthErr != nil || dayErr != nil || hourErr != nil {
		return time.Time{}, false
	}

	at := time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC)
	if at.Year() != year || int(at.Month()) != month || at.Day() != day || at.Hour() != hour {
		return time.Time{}, false
	}
	return at, true
}

// Overlaps reports whether the half-open chunk window intersects the query
// range. A zero range boundary is unbounded.
func Overlaps(start, end time.Time, r model.TimeRange) bool {
	if !r.Start.IsZero() && !end.After(r.Start) {
		return false
	}
	if !r.End.IsZero() && !start.Before(r.End) {
		return false
	}
	return true
}

// ValidateComponent requires a value that can be used unchanged as one path
// component. It keeps a malformed host or scope from escaping the store root.
func ValidateComponent(field, value string) error {
	if !IsComponent(value) {
		return fmt.Errorf("%s: must be a single path component; got %q", field, value)
	}
	return nil
}

// IsComponent reports whether value is a single, non-empty path component that
// is neither "." nor "..".
func IsComponent(value string) bool {
	return value != "" &&
		value != "." &&
		value != ".." &&
		!strings.ContainsRune(value, '/') &&
		!strings.ContainsRune(value, 0)
}

// Encode writes samples as one JSON object per line through gzip. Go's gzip
// writer emits no filename or modification time, and encoding/json sorts map
// keys, so identical samples produce identical bytes.
func Encode(w io.Writer, samples []model.Sample) error {
	gz := gzip.NewWriter(w)
	enc := json.NewEncoder(gz)
	for _, sample := range samples {
		if err := enc.Encode(sample); err != nil {
			_ = gz.Close()
			return err
		}
	}
	return gz.Close()
}
