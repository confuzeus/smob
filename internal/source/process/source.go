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
	metricCPUPercent     = "process.cpu.percent"
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

// Source collects per-process metrics from /proc/[pid]. The collector drives it
// from one goroutine, so it is not safe for concurrent use.
type Source struct {
	procPath string
	filter   *regexp.Regexp
	topN     int
	interval time.Duration
	pids     *pidCache
	logger   *slog.Logger

	// state holds the previous snapshot per process so the next tick can
	// compute a CPU rate. It is rebuilt from every candidate each tick, which
	// prunes exited processes and PID reuse.
	state map[pidKey]procState
	// resetWarned logs one reset per series until a valid rate resumes.
	resetWarned map[pidKey]bool
	// skipped logs one skipped PID per rescan, whatever the reason: permission,
	// EMFILE, EIO, a layout surprise, or an invalid starttime.
	skipped map[int]bool
}

// pidKey identifies one process across PID reuse: the kernel reuses PIDs, but
// starttime distinguishes the new process from the old one.
type pidKey struct {
	pid       int
	starttime uint64
}

// procState is the previous snapshot of one process and when it was taken.
type procState struct {
	stat statSnapshot
	at   time.Time
}

// New returns a process source. The proc root and one process are read and
// parsed once so an unusable source fails at startup instead of on the first
// tick.
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
		procPath:    opts.ProcPath,
		filter:      filter,
		topN:        opts.TopN,
		interval:    opts.Interval,
		pids:        newPIDCache(opts.ProcPath, opts.RescanInterval, logger),
		logger:      logger,
		state:       make(map[pidKey]procState),
		resetWarned: make(map[pidKey]bool),
		skipped:     make(map[int]bool),
	}
	if err := s.probe(); err != nil {
		return nil, err
	}
	return s, nil
}

// probe verifies that the proc root is listable and that both documented file
// layouts parse. Parsing stat and status at startup turns a kernel or
// filesystem surprise into a startup failure instead of a per-tick skip.
func (s *Source) probe() error {
	if _, err := listPIDs(s.procPath); err != nil {
		return fmt.Errorf("process: %w", err)
	}

	statPath := filepath.Join(s.procPath, "self", "stat")
	stat, err := parseStatPath(statPath)
	if err != nil {
		return fmt.Errorf("process: %w", err)
	}
	if err := validateStat(stat); err != nil {
		return fmt.Errorf("process: %s: %w", statPath, err)
	}

	statusPath := filepath.Join(s.procPath, "self", "status")
	if _, err := parseStatusPath(statusPath); err != nil {
		return fmt.Errorf("process: %w", err)
	}
	return nil
}

// Scope implements the collector's Source interface.
func (s *Source) Scope() string { return scope }

// Interval implements the collector's Source interface.
func (s *Source) Interval() time.Duration { return s.interval }

// Collect implements the collector's Source interface. RSS is a gauge and is
// emitted for every selected process. CPU is a rate: a process seen for the
// first time, or one whose counters reset, emits no CPU sample for that tick.
//
// A failure reading one process (permission, EMFILE, EIO, a layout surprise, an
// invalid observation) skips that PID and is logged at warn once per rescan; the
// rest of the tick continues. An error is returned only when the process list
// itself cannot be read, or when ctx is canceled.
func (s *Source) Collect(ctx context.Context, now time.Time) ([]model.Sample, error) {
	pids, refreshed, err := s.pids.list(ctx, now)
	if err != nil {
		return nil, err
	}
	if refreshed {
		clear(s.skipped)
	}

	candidates := make([]candidate, 0, len(pids))
	for _, pid := range pids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if cand, ok := s.readCandidate(pid); ok {
			candidates = append(candidates, cand)
		}
	}

	state, resets := advanceRates(s.state, candidates, now)
	for _, reset := range resets {
		s.warnReset(reset.key, reset.comm)
	}
	for i := range candidates {
		if candidates[i].cpuKnown {
			s.clearReset(pidKey{pid: candidates[i].pid, starttime: candidates[i].stat.starttime})
		}
	}
	s.state = state
	s.pruneResetWarnings(state)

	selected := top(candidates, s.topN)
	samples := make([]model.Sample, 0, 2*len(selected))
	for _, cand := range selected {
		labels := map[string]string{
			"pid":  strconv.Itoa(cand.pid),
			"comm": cand.comm,
		}
		samples = append(samples, model.Sample{
			Timestamp: now.UTC(),
			Metric:    metricMemoryRSSBytes,
			Value:     float64(cand.rss),
			Labels:    labels,
		})
		if !cand.cpuKnown {
			continue
		}
		samples = append(samples, model.Sample{
			Timestamp:  now.UTC(),
			Metric:     metricCPUPercent,
			Value:      cand.cpu,
			IntervalMS: model.Millis(cand.window),
			Labels:     labels,
		})
	}
	return samples, nil
}

// rateReset is one process whose counters went backwards between ticks. Its
// rate is skipped for this tick and the reset is reported once per series.
type rateReset struct {
	key  pidKey
	comm string
}

// advanceRates rebuilds the per-process rate state from this tick's candidates
// and decides which of them emit a CPU rate. A process is identified by
// (pid, starttime), so a reused PID misses the lookup and its first delta is
// skipped: the new observation replaces the old counters as the baseline
// instead of subtracting one process's counter from another's.
//
// A window shorter than a millisecond keeps the baseline without a rate and
// without a reset. A counter that went backwards also skips the rate and is
// reported as a reset; either way the baseline advances to this tick. The
// returned map has one entry per current process, which prunes processes that
// disappeared. Candidates' rate fields are set in place.
func advanceRates(prev map[pidKey]procState, candidates []candidate, now time.Time) (map[pidKey]procState, []rateReset) {
	state := make(map[pidKey]procState, len(candidates))
	var resets []rateReset
	for i := range candidates {
		cand := &candidates[i]
		key := pidKey{pid: cand.pid, starttime: cand.stat.starttime}
		previous, seen := prev[key]
		state[key] = procState{stat: cand.stat, at: now}
		if !seen {
			continue
		}
		elapsed := now.Sub(previous.at)
		if elapsed < time.Millisecond {
			continue
		}
		cpu, ok := cpuPercent(previous.stat, cand.stat, elapsed)
		if !ok {
			resets = append(resets, rateReset{key: key, comm: cand.comm})
			continue
		}
		cand.cpu = cpu
		cand.cpuKnown = true
		cand.window = elapsed
	}
	return state, resets
}

// candidate is one process that passed the filter and has a known RSS.
type candidate struct {
	pid      int
	comm     string
	rss      uint64
	stat     statSnapshot
	cpu      float64
	cpuKnown bool
	window   time.Duration
}

// readCandidate reads and filters one process. A false result means the process
// exited, became unreadable, did not match the filter, or produced an invalid
// observation. A per-process failure is logged once and skipped, never returned:
// one bad PID must not drop the whole tick.
func (s *Source) readCandidate(pid int) (candidate, bool) {
	stat, ok := s.readStat(pid)
	if !ok {
		return candidate{}, false
	}
	if err := validateStat(stat); err != nil {
		s.warnSkip(pid, fmt.Errorf("%s: %w", s.path(pid, "stat"), err))
		return candidate{}, false
	}
	if s.filter != nil && !s.filter.MatchString(stat.comm) {
		return candidate{}, false
	}

	rss, ok := s.readStatus(pid)
	if !ok {
		return candidate{}, false
	}

	// A PID reused between the stat and status reads would pair the new
	// process's RSS with the old comm and starttime. Re-read stat and drop the
	// candidate when the process changed under us.
	after, ok := s.readStat(pid)
	if !ok {
		return candidate{}, false
	}
	if !sameProcess(stat, after) {
		s.logger.Debug("skip process: pid reused during read",
			"pid", pid, "starttime", stat.starttime, "starttime_after", after.starttime)
		return candidate{}, false
	}

	return candidate{pid: pid, comm: stat.comm, rss: rss, stat: stat}, true
}

// readStat reads one process's stat file, reporting whether it is still there.
// A process that exited is silent; any other failure is logged and skipped.
func (s *Source) readStat(pid int) (statSnapshot, bool) {
	stat, err := parseStatPath(s.path(pid, "stat"))
	switch {
	case err == nil:
		return stat, true
	case processGone(err):
		return statSnapshot{}, false
	default:
		s.warnSkip(pid, err)
		return statSnapshot{}, false
	}
}

// readStatus reads one process's VmRSS, reporting whether it is still there.
func (s *Source) readStatus(pid int) (uint64, bool) {
	rss, err := parseStatusPath(s.path(pid, "status"))
	switch {
	case err == nil:
		return rss, true
	case processGone(err):
		return 0, false
	default:
		s.warnSkip(pid, err)
		return 0, false
	}
}

// parseStatPath opens and parses one proc stat file.
func parseStatPath(path string) (statSnapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return statSnapshot{}, fmt.Errorf("open %s: %w", path, err)
	}
	stat, err := parseStat(f)
	_ = f.Close()
	if err != nil {
		return statSnapshot{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return stat, nil
}

// parseStatusPath opens and parses one proc status file.
func parseStatusPath(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", path, err)
	}
	rss, err := parseStatus(f)
	_ = f.Close()
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", path, err)
	}
	return rss, nil
}

// validateStat rejects an observation the source cannot key on. starttime is
// the process start time in clock ticks since boot, so zero is not a live
// process in /proc. Accepting it would key every such PID on (pid, 0) and let
// PID-reuse detection silently degrade; skipping instead keeps it sound.
func validateStat(stat statSnapshot) error {
	if stat.starttime == 0 {
		return errors.New("starttime is zero")
	}
	return nil
}

// sameProcess reports whether a stat snapshot re-read after status observed the
// same process. A differing starttime means the kernel reused the PID between
// the two reads.
func sameProcess(before, after statSnapshot) bool {
	return after.starttime != 0 && after.starttime == before.starttime
}

// processGone reports whether err means the process disappeared while it was
// being read. A process can exit between listing and opening, or between
// opening and reading.
func processGone(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH)
}

// warnSkip logs one skipped process per rescan, so a hidepid mount, a
// descriptor exhaustion, or one unparseable PID does not flood the log or fail
// collection for every other process.
func (s *Source) warnSkip(pid int, err error) {
	if s.skipped[pid] {
		return
	}
	s.skipped[pid] = true
	s.logger.Warn("skip process", "pid", pid, "error", err)
}

// warnReset logs one reset per series until a valid rate resumes.
func (s *Source) warnReset(key pidKey, comm string) {
	if s.resetWarned[key] {
		return
	}
	s.resetWarned[key] = true
	s.logger.Warn("process counter reset; dropping rate for one tick",
		"pid", key.pid, "comm", comm, "metric", metricCPUPercent)
}

// clearReset forgets a previously logged reset for the series.
func (s *Source) clearReset(key pidKey) {
	delete(s.resetWarned, key)
}

// pruneResetWarnings bounds the warning set to processes still present, so PID
// churn cannot grow it without limit.
func (s *Source) pruneResetWarnings(state map[pidKey]procState) {
	for key := range s.resetWarned {
		if _, ok := state[key]; !ok {
			delete(s.resetWarned, key)
		}
	}
}

// path returns the proc file path for one process.
func (s *Source) path(pid int, name string) string {
	return filepath.Join(s.procPath, strconv.Itoa(pid), name)
}

// top returns the highest-ranked candidates: CPU descending (an unknown CPU
// sorts as zero, so the first tick ranks by RSS), then RSS descending, then PID
// ascending. A limit of zero or less returns every candidate.
func top(candidates []candidate, limit int) []candidate {
	slices.SortFunc(candidates, func(a, b candidate) int {
		if a.cpu != b.cpu {
			return cmp.Compare(b.cpu, a.cpu)
		}
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
