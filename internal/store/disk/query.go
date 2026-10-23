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
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/confuzeus/smob/internal/model"
)

// ctxCheckInterval bounds how long a large merge can run without observing
// cancellation. Chunks are small, but filtering may make many loops cheap.
const ctxCheckInterval = 1024

// Query yields matching samples in timestamp, metric, then canonical label
// order. It merges one UTC hour at a time, so memory and open file descriptors
// stay proportional to the chunks in one hour rather than the whole range.
func (s *Store) Query(ctx context.Context, q model.Query, yield func(model.Sample) error) error {
	if q.Limit < 0 {
		return errors.New("query: limit must not be negative")
	}
	matchers, err := compileMatchers(q.Labels)
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
		n, err := merge(ctx, paths, q, matchers, limit, yield)
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

// hourGroup is one UTC hour and the host/scope directories that may contain
// chunks for it. Processing groups in hour order keeps output globally sorted.
type hourGroup struct {
	hour time.Time
	dirs []string
}

// hourGroups resolves the hour directories to scan. A bounded range is walked
// hour by hour; an unbounded range needs a full tree walk because the model
// treats a zero range boundary as unbounded.
func (s *Store) hourGroups(ctx context.Context, q model.Query) ([]hourGroup, error) {
	if q.Range.Start.IsZero() || q.Range.End.IsZero() {
		return s.walkHourGroups(ctx)
	}

	hosts, err := s.hosts(q.Host)
	if err != nil {
		return nil, err
	}
	type target struct{ host, scope string }
	var targets []target
	for _, host := range hosts {
		scopes, err := s.scopes(host, q.Scope)
		if err != nil {
			return nil, err
		}
		for _, scope := range scopes {
			targets = append(targets, target{host: host, scope: scope})
		}
	}
	if len(targets) == 0 {
		return nil, nil
	}

	start := q.Range.Start.UTC().Truncate(time.Hour)
	last := q.Range.End.UTC().Add(-time.Nanosecond).Truncate(time.Hour)

	var groups []hourGroup
	for h := start; !h.After(last); h = h.Add(time.Hour) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		dirs := make([]string, 0, len(targets))
		for _, t := range targets {
			dirs = append(dirs, filepath.Join(
				s.root,
				t.host,
				t.scope,
				h.Format("2006"),
				h.Format("01"),
				h.Format("02"),
				h.Format("15"),
			))
		}
		groups = append(groups, hourGroup{hour: h, dirs: dirs})
	}
	return groups, nil
}

// walkHourGroups discovers hour directories at any depth and groups them by
// hour. It is the fallback for an unbounded start or end.
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
		hour, ok := parseHourPath(rel)
		if !ok {
			return nil
		}
		groups = append(groups, hourGroup{hour: hour, dirs: []string{path}})
		return filepath.SkipDir
	})
	if err != nil {
		return nil, err
	}

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
	return merged, nil
}

// hosts returns the host directories to scan. An explicit host is trusted as a
// single component; otherwise every directory under the root is a candidate.
func (s *Store) hosts(host string) ([]string, error) {
	if host != "" {
		if err := validateComponent("host", host); err != nil {
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
		if err := validateComponent("scope", scope); err != nil {
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
		if entry.IsDir() && isComponent(entry.Name()) {
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
			if !strings.HasSuffix(name, chunkFileSuffix) {
				continue
			}
			start, end, err := parseChunkName(name)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", filepath.Join(dir, name), err)
			}
			if !overlaps(start, end, r) {
				continue
			}
			paths = append(paths, filepath.Join(dir, name))
		}
	}
	sort.Strings(paths)
	return paths, nil
}

// overlaps reports whether the half-open chunk window intersects the query
// range. A zero range boundary is unbounded.
func overlaps(start, end time.Time, r model.TimeRange) bool {
	if !r.Start.IsZero() && !end.After(r.Start) {
		return false
	}
	if !r.End.IsZero() && !start.Before(r.End) {
		return false
	}
	return true
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
func merge(ctx context.Context, paths []string, q model.Query, matchers []compiledMatcher, limit int, yield func(model.Sample) error) (int, error) {
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
		if matches(q, matchers, sample) {
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

// compiledMatcher is a label matcher with its regular expression precompiled.
type compiledMatcher struct {
	name  string
	op    model.LabelOp
	value string
	re    *regexp.Regexp
}

func compileMatchers(labels []model.LabelMatcher) ([]compiledMatcher, error) {
	matchers := make([]compiledMatcher, 0, len(labels))
	for _, label := range labels {
		if label.Name == "" {
			return nil, errors.New("query: label matcher has an empty name")
		}

		matcher := compiledMatcher{name: label.Name, op: label.Op, value: label.Value}
		switch label.Op {
		case model.LabelEqual:
		case model.LabelRegex:
			re, err := regexp.Compile(label.Value)
			if err != nil {
				return nil, fmt.Errorf("query: label %s: %w", label.Name, err)
			}
			matcher.re = re
		default:
			return nil, fmt.Errorf("query: label %s: unknown matcher operation %d", label.Name, label.Op)
		}
		matchers = append(matchers, matcher)
	}
	return matchers, nil
}

// matches applies the range, metric, and label filters to one sample. A missing
// label reads as the empty string, so an exact empty match and a regex such as
// .* both match an absent label.
func matches(q model.Query, matchers []compiledMatcher, sample model.Sample) bool {
	if !q.Range.Contains(sample.Timestamp) {
		return false
	}
	if q.Host != "" && sample.Labels["host"] != q.Host {
		return false
	}
	if q.Scope != "" && sample.Labels["scope"] != q.Scope {
		return false
	}
	if q.Metric != "" && sample.Metric != q.Metric {
		return false
	}
	for _, matcher := range matchers {
		value := sample.Labels[matcher.name]
		switch matcher.op {
		case model.LabelEqual:
			if value != matcher.value {
				return false
			}
		case model.LabelRegex:
			if !matcher.re.MatchString(value) {
				return false
			}
		}
	}
	return true
}
