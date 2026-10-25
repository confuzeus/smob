package disk

import (
	"compress/gzip"
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/store"
	"github.com/confuzeus/smob/internal/store/chunk"
)

// ctxCheckInterval bounds how long a large merge can run without observing
// cancellation. Chunks are small, but filtering may make many loops cheap.
const ctxCheckInterval = 1024

// Query yields matching samples in timestamp, metric, then canonical label
// order. It merges one UTC hour at a time, so memory and open file descriptors
// stay proportional to the chunks in one hour rather than the whole range.
func (s *Store) Query(ctx context.Context, q model.Query, yield func(model.Sample) error) error {
	if err := s.CheckQuery(); err != nil {
		return err
	}
	filter, err := store.NewFilter(q)
	if err != nil {
		return err
	}
	groups, err := s.hourGroups(ctx, q)
	if err != nil {
		return err
	}

	emitted := 0
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return err
		}
		paths, err := listChunks(group.dirs, q.Range)
		if err != nil {
			return err
		}

		limit := 0
		if q.Limit > 0 {
			limit = q.Limit - emitted
		}
		n, err := merge(ctx, paths, filter, limit, yield)
		emitted += n
		if err != nil {
			return err
		}
		if q.Limit > 0 && emitted >= q.Limit {
			return nil
		}
	}
	return nil
}

// CheckQuery verifies that the data directory exists and is a directory. A
// mistyped root would otherwise be indistinguishable from an empty store, so
// the query fails with the path named instead. The check runs once, before the
// walk: a directory or chunk that disappears while the query runs is still
// treated as empty. Query calls it, and the query command calls it before
// opening the output document so a bad path prints nothing.
func (s *Store) CheckQuery() error {
	info, err := os.Stat(s.root)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("data directory %s does not exist; set data_dir or pass --data-dir", s.root)
	case err != nil:
		return fmt.Errorf("data directory %s: %w", s.root, err)
	case !info.IsDir():
		return fmt.Errorf("data directory %s is not a directory; set data_dir or pass --data-dir", s.root)
	}
	return nil
}

// hourGroup is one UTC hour and the host/scope directories that may contain
// chunks for it. Processing groups in hour order keeps output globally sorted.
type hourGroup struct {
	hour time.Time
	dirs []string
}

// hourGroups resolves the hour directories to scan. Host and scope are applied
// first, so a filtered query never touches another host's or scope's chunks. A
// bounded range then yields its hour paths directly. A one-sided range descends
// only the date levels that can still contain in-range data instead of walking
// the whole tree. Only when neither host nor scope is set does an unbounded
// range keep the full-tree walk.
func (s *Store) hourGroups(ctx context.Context, q model.Query) ([]hourGroup, error) {
	if q.Host == "" && q.Scope == "" && (q.Range.Start.IsZero() || q.Range.End.IsZero()) {
		return s.walkHourGroups(ctx)
	}

	hosts, err := s.hosts(q.Host)
	if err != nil {
		return nil, err
	}

	var groups []hourGroup
	for _, host := range hosts {
		scopes, err := s.scopes(host, q.Scope)
		if err != nil {
			return nil, err
		}
		for _, scope := range scopes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			dir := filepath.Join(s.root, host, scope)
			var found []hourGroup
			if q.Range.Start.IsZero() || q.Range.End.IsZero() {
				found, err = scanHourGroups(ctx, dir, q.Range)
			} else {
				found = rangeHourGroups(dir, q.Range)
			}
			if err != nil {
				return nil, err
			}
			groups = append(groups, found...)
		}
	}
	return mergeHourGroups(groups), nil
}

// rangeHourGroups returns one group per hour in r below one host/scope
// directory. Both bounds are set, so the paths are built directly.
func rangeHourGroups(dir string, r model.TimeRange) []hourGroup {
	start := r.Start.UTC().Truncate(time.Hour)
	last := lastHour(r.End)
	// A pathological or inverted range can yield a non-positive or huge count,
	// so clamp the capacity hint instead of reserving it blindly.
	hours := int(last.Sub(start)/time.Hour) + 1
	capacity := min(max(hours, 0), ctxCheckInterval)
	groups := make([]hourGroup, 0, capacity)
	for h := start; !h.After(last); h = h.Add(time.Hour) {
		groups = append(groups, hourGroup{
			hour: h,
			dirs: []string{filepath.Join(dir,
				h.Format("2006"), h.Format("01"), h.Format("02"), h.Format("15"))},
		})
	}
	return groups
}

// scanHourGroups resolves the hour directories below one host/scope directory
// when one range boundary is zero. It descends year, month, day, and hour and
// skips any directory whose whole span is outside r, so the missing bound costs
// only the levels that can still contain data. A directory that disappeared
// between levels is treated as empty.
func scanHourGroups(ctx context.Context, dir string, r model.TimeRange) ([]hourGroup, error) {
	years, err := readComponents(dir)
	if err != nil {
		return nil, err
	}

	var groups []hourGroup
	for _, yearName := range years {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		year, ok := pathNumber(yearName, 4)
		if !ok {
			continue
		}
		yearFirst := time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC)
		yearDir := filepath.Join(dir, yearName)
		if !hoursOverlap(yearFirst, yearFirst.AddDate(1, 0, 0).Add(-time.Hour), r) {
			continue
		}

		months, err := readComponents(yearDir)
		if err != nil {
			return nil, err
		}
		for _, monthName := range months {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			month, ok := pathNumber(monthName, 2)
			if !ok || month < 1 || month > 12 {
				continue
			}
			monthFirst := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
			monthDir := filepath.Join(yearDir, monthName)
			if !hoursOverlap(monthFirst, monthFirst.AddDate(0, 1, 0).Add(-time.Hour), r) {
				continue
			}

			days, err := readComponents(monthDir)
			if err != nil {
				return nil, err
			}
			for _, dayName := range days {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				day, ok := pathNumber(dayName, 2)
				if !ok {
					continue
				}
				dayFirst := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
				if dayFirst.Day() != day {
					continue
				}
				dayDir := filepath.Join(monthDir, dayName)
				if !hoursOverlap(dayFirst, dayFirst.AddDate(0, 0, 1).Add(-time.Hour), r) {
					continue
				}

				hours, err := readComponents(dayDir)
				if err != nil {
					return nil, err
				}
				for _, hourName := range hours {
					hour, ok := pathNumber(hourName, 2)
					if !ok || hour > 23 {
						continue
					}
					at := time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC)
					if at.Hour() != hour || !hoursOverlap(at, at, r) {
						continue
					}
					groups = append(groups, hourGroup{
						hour: at,
						dirs: []string{filepath.Join(dayDir, hourName)},
					})
				}
			}
		}
	}
	return groups, nil
}

// pathNumber parses a fixed-width numeric path component, so a stray directory
// is not read as a date.
func pathNumber(name string, width int) (int, bool) {
	if len(name) != width {
		return 0, false
	}
	for i := 0; i < len(name); i++ {
		if name[i] < '0' || name[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.Atoi(name)
	if err != nil {
		return 0, false
	}
	return n, true
}

// hoursOverlap reports whether the inclusive hour span [first, last] intersects
// r. A zero range boundary is unbounded.
func hoursOverlap(first, last time.Time, r model.TimeRange) bool {
	if !r.Start.IsZero() && last.Before(r.Start.UTC().Truncate(time.Hour)) {
		return false
	}
	if !r.End.IsZero() && first.After(lastHour(r.End)) {
		return false
	}
	return true
}

// lastHour returns the hour containing the last instant before end.
func lastHour(end time.Time) time.Time {
	return end.UTC().Add(-time.Nanosecond).Truncate(time.Hour)
}

// mergeHourGroups orders groups by hour and joins groups for the same hour, so
// one merge reads every chunk in that hour at once.
func mergeHourGroups(groups []hourGroup) []hourGroup {
	sort.Slice(groups, func(i, j int) bool {
		if !groups[i].hour.Equal(groups[j].hour) {
			return groups[i].hour.Before(groups[j].hour)
		}
		return groups[i].dirs[0] < groups[j].dirs[0]
	})

	merged := groups[:0]
	for _, group := range groups {
		if n := len(merged); n > 0 && merged[n-1].hour.Equal(group.hour) {
			merged[n-1].dirs = append(merged[n-1].dirs, group.dirs...)
			continue
		}
		merged = append(merged, group)
	}
	return merged
}

// walkHourGroups discovers hour directories at any depth and groups them by
// hour. It is the fallback for an unbounded start or end when the query selects
// no host or scope.
func (s *Store) walkHourGroups(ctx context.Context) ([]hourGroup, error) {
	var groups []hourGroup
	err := filepath.WalkDir(s.root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		hour, ok := chunk.ParseHourPath(rel)
		if !ok {
			return nil
		}
		groups = append(groups, hourGroup{hour: hour, dirs: []string{path}})
		return filepath.SkipDir
	})
	if err != nil {
		return nil, err
	}

	return mergeHourGroups(groups), nil
}

// hosts returns the host directories to scan. An explicit host is trusted as a
// single component; otherwise every directory under the root is a candidate.
func (s *Store) hosts(host string) ([]string, error) {
	if host != "" {
		if err := chunk.ValidateComponent("host", host); err != nil {
			return nil, err
		}
		return []string{host}, nil
	}
	return readComponents(s.root)
}

// scopes returns the scope directories under host. An explicit scope is
// validated so it cannot escape the root.
func (s *Store) scopes(host, scope string) ([]string, error) {
	if scope != "" {
		if err := chunk.ValidateComponent("scope", scope); err != nil {
			return nil, err
		}
		return []string{scope}, nil
	}
	return readComponents(filepath.Join(s.root, host))
}

func readComponents(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && chunk.IsComponent(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// listChunks returns the chunk files in dirs whose windows intersect r. A file
// that disappeared after listing is skipped; the caller tolerates ENOENT.
func listChunks(dirs []string, r model.TimeRange) ([]string, error) {
	var paths []string
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if !strings.HasSuffix(name, chunk.FileSuffix) {
				continue
			}
			start, end, err := chunk.ParseName(name)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", filepath.Join(dir, name), err)
			}
			if !chunk.Overlaps(start, end, r) {
				continue
			}
			paths = append(paths, filepath.Join(dir, name))
		}
	}
	sort.Strings(paths)
	return paths, nil
}

// stream decodes one chunk file and holds its current sample.
type stream struct {
	file *os.File
	gz   *gzip.Reader
	dec  *json.Decoder
	path string
	cur  model.Sample
}

func openStream(path string) (*stream, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	return &stream{
		file: f,
		gz:   gz,
		dec:  json.NewDecoder(gz),
		path: path,
	}, nil
}

func (s *stream) next() (model.Sample, bool, error) {
	var sample model.Sample
	err := s.dec.Decode(&sample)
	if errors.Is(err, io.EOF) {
		return model.Sample{}, false, nil
	}
	if err != nil {
		return model.Sample{}, false, fmt.Errorf("decode %s: %w", s.path, err)
	}
	if sample.Metric == "" {
		return model.Sample{}, false, fmt.Errorf("decode %s: sample has an empty metric", s.path)
	}
	return sample, true, nil
}

func (s *stream) close() {
	_ = s.gz.Close()
	_ = s.file.Close()
}

// streamHeap orders streams by their current sample.
type streamHeap struct {
	streams []*stream
}

func (h *streamHeap) Len() int { return len(h.streams) }
func (h *streamHeap) Less(i, j int) bool {
	return model.LessSample(h.streams[i].cur, h.streams[j].cur)
}
func (h *streamHeap) Swap(i, j int) {
	h.streams[i], h.streams[j] = h.streams[j], h.streams[i]
}
func (h *streamHeap) Push(x any) {
	h.streams = append(h.streams, x.(*stream))
}
func (h *streamHeap) Pop() any {
	old := h.streams
	n := len(old)
	x := old[n-1]
	h.streams = old[:n-1]
	return x
}

// merge yields matching samples from the sorted chunk files. It stops after
// limit samples when limit is positive.
func merge(ctx context.Context, paths []string, filter *store.Filter, limit int, yield func(model.Sample) error) (int, error) {
	var opened []*stream
	defer func() {
		for _, st := range opened {
			st.close()
		}
	}()

	active := make([]*stream, 0, len(paths))
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		st, err := openStream(path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		opened = append(opened, st)

		sample, ok, err := st.next()
		if err != nil {
			return 0, err
		}
		if !ok {
			continue
		}
		st.cur = sample
		active = append(active, st)
	}

	h := &streamHeap{streams: active}
	heap.Init(h)

	emitted, loops := 0, 0
	for h.Len() > 0 {
		loops++
		if loops%ctxCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return emitted, err
			}
		}

		st := h.streams[0]
		sample := st.cur
		if filter.Match(sample) {
			if err := yield(sample); err != nil {
				return emitted, err
			}
			emitted++
			if limit > 0 && emitted >= limit {
				return emitted, nil
			}
		}

		next, ok, err := st.next()
		if err != nil {
			return emitted, err
		}
		if !ok {
			heap.Pop(h)
			continue
		}
		st.cur = next
		heap.Fix(h, 0)
	}
	return emitted, nil
}
