package process

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/confuzeus/smob/internal/model"
)

const (
	scope                = "process"
	metricMemoryRSSBytes = "process.memory.rss_bytes"
)

// Options configures a process source.
type Options struct {
	// ProcPath is the proc filesystem root, normally /proc.
	ProcPath string
	// Filter is a regular expression matched against comm. Empty matches every
	// process. comm is truncated to 15 bytes by the kernel.
	Filter string
	// TopN bounds the number of series per tick. Zero means all matching
	// processes.
	TopN int
	// Interval is the collection cadence.
	Interval time.Duration
	// RescanInterval is how often the proc root is listed for new PIDs.
	RescanInterval time.Duration
}

// Source collects per-process metrics from /proc/[pid].
type Source struct {
	procPath string
	filter   *regexp.Regexp
	topN     int
	interval time.Duration
	pids     *pidCache
	logger   *slog.Logger
	warned   map[int]bool
}

// New returns a process source. The proc root and one process are read once so
// an unusable source fails at startup instead of on the first tick.
func New(opts Options, logger *slog.Logger) (*Source, error) {
	switch {
	case opts.ProcPath == "":
		return nil, errors.New("process: proc path is empty")
	case opts.Interval <= 0:
		return nil, fmt.Errorf("process: interval must be positive; got %s", opts.Interval)
	case opts.RescanInterval <= 0:
		return nil, fmt.Errorf("process: rescan interval must be positive; got %s", opts.RescanInterval)
	case opts.TopN < 0:
		return nil, fmt.Errorf("process: top_n must not be negative; got %d", opts.TopN)
	}

	var filter *regexp.Regexp
	if opts.Filter != "" {
		compiled, err := regexp.Compile(opts.Filter)
		if err != nil {
			return nil, fmt.Errorf("process: filter: %w", err)
		}
		filter = compiled
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	s := &Source{
		procPath: opts.ProcPath,
		filter:   filter,
		topN:     opts.TopN,
		interval: opts.Interval,
		pids:     newPIDCache(opts.ProcPath, opts.RescanInterval),
		logger:   logger,
		warned:   make(map[int]bool),
	}
	if err := s.probe(); err != nil {
		return nil, err
	}
	return s, nil
}

// probe verifies that the proc root and one process are readable.
func (s *Source) probe() error {
	if _, err := listPIDs(s.procPath); err != nil {
		return fmt.Errorf("process: %w", err)
	}
	for _, name := range []string{"stat", "status"} {
		path := filepath.Join(s.procPath, "self", name)
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("process: open %s: %w", path, err)
		}
		_ = f.Close()
	}
	return nil
}

// Scope implements the collector's Source interface.
func (s *Source) Scope() string { return scope }

// Interval implements the collector's Source interface.
func (s *Source) Interval() time.Duration { return s.interval }

// Collect implements the collector's Source interface. RSS is a gauge, so the
// first tick and collect --once emit complete samples for every selected
// process.
func (s *Source) Collect(ctx context.Context, now time.Time) ([]model.Sample, error) {
	pids, refreshed, err := s.pids.list(ctx, now)
	if err != nil {
		return nil, err
	}
	if refreshed {
		clear(s.warned)
	}

	candidates := make([]candidate, 0, len(pids))
	for _, pid := range pids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cand, ok, err := s.readCandidate(pid)
		if err != nil {
			return nil, err
		}
		if ok {
			candidates = append(candidates, cand)
		}
	}

	selected := top(candidates, s.topN)
	samples := make([]model.Sample, 0, len(selected))
	for _, cand := range selected {
		samples = append(samples, model.Sample{
			Timestamp: now.UTC(),
			Metric:    metricMemoryRSSBytes,
			Value:     float64(cand.rss),
			Labels: map[string]string{
				"pid":  strconv.Itoa(cand.pid),
				"comm": cand.comm,
			},
		})
	}
	return samples, nil
}

// candidate is one process that passed the filter and has a known RSS.
type candidate struct {
	pid  int
	comm string
	rss  uint64
}

// readCandidate reads and filters one process. A false result means the process
// exited, became unreadable, or did not match the filter.
func (s *Source) readCandidate(pid int) (candidate, bool, error) {
	statFile, err := s.processFile(pid, "stat")
	if err != nil {
		return candidate{}, false, err
	}
	if statFile == nil {
		return candidate{}, false, nil
	}
	stat, err := parseStat(statFile)
	_ = statFile.Close()
	if err != nil {
		if processGone(err) {
			return candidate{}, false, nil
		}
		return candidate{}, false, fmt.Errorf("parse %s: %w", s.path(pid, "stat"), err)
	}
	if s.filter != nil && !s.filter.MatchString(stat.comm) {
		return candidate{}, false, nil
	}

	statusFile, err := s.processFile(pid, "status")
	if err != nil {
		return candidate{}, false, err
	}
	if statusFile == nil {
		return candidate{}, false, nil
	}
	rss, err := parseStatus(statusFile)
	_ = statusFile.Close()
	if err != nil {
		if processGone(err) {
			return candidate{}, false, nil
		}
		return candidate{}, false, fmt.Errorf("parse %s: %w", s.path(pid, "status"), err)
	}

	return candidate{pid: pid, comm: stat.comm, rss: rss}, true, nil
}

// processFile opens one file below /proc/<pid>. A nil file and nil error mean
// the process exited (ENOENT or ESRCH) or the file is inaccessible; the latter
// is logged once per rescan. Other errors are returned.
func (s *Source) processFile(pid int, name string) (*os.File, error) {
	path := s.path(pid, name)
	f, err := os.Open(path)
	switch {
	case err == nil:
		return f, nil
	case processGone(err):
		return nil, nil
	case errors.Is(err, fs.ErrPermission):
		s.warn(pid, err)
		return nil, nil
	default:
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
}

// processGone reports whether err means the process disappeared while it was
// being read. A process can exit between listing and opening, or between
// opening and reading.
func processGone(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH)
}

// warn logs an unreadable process once per rescan, so a hidepid mount does not
// flood the log or fail collection.
func (s *Source) warn(pid int, err error) {
	if s.warned[pid] {
		return
	}
	s.warned[pid] = true
	s.logger.Warn("skip unreadable process", "pid", pid, "error", err)
}

// path returns the proc file path for one process.
func (s *Source) path(pid int, name string) string {
	return filepath.Join(s.procPath, strconv.Itoa(pid), name)
}

// top returns the highest-ranked candidates: RSS descending, then PID
// ascending. A limit of zero or less returns every candidate.
func top(candidates []candidate, limit int) []candidate {
	slices.SortFunc(candidates, func(a, b candidate) int {
		if a.rss != b.rss {
			return cmp.Compare(b.rss, a.rss)
		}
		return cmp.Compare(a.pid, b.pid)
	})
	if limit > 0 && len(candidates) > limit {
		return candidates[:limit]
	}
	return candidates
}
