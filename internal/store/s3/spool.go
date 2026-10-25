package s3

import (
	"compress/gzip"
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/store/chunk"
)

const (
	// Spooled chunks carry the same process and container names as disk chunks,
	// so they are group-readable only and never accessible to "other". Existing
	// spool entries keep their modes; the explicit chmod defeats a permissive
	// umask.
	spoolDirPerm  = 0o750
	spoolFilePerm = 0o640
	// maxSpoolCollisions bounds the suffix search so a writer bug cannot spin.
	maxSpoolCollisions = 1000

	// replayInterval is how often an idle replayer checks for new spooled
	// chunks. Failures back off exponentially from replayBackoffBase.
	replayInterval    = 15 * time.Second
	replayBackoffBase = 30 * time.Second
	replayBackoffMax  = 5 * time.Minute
)

// spoolEntry is one chunk file in the local spool.
type spoolEntry struct {
	key  string // relative chunk key, forward slashes
	size int64
}

// spool is a local directory of chunks whose S3 upload exhausted the retry
// budget. Chunk files mirror the relative key layout, so a spool file name is
// the exact key replay uploads.
//
// Writers and the replayer share the process. Writes are atomic; replay marks a
// key in flight before reading it so cap enforcement cannot evict a file that
// is about to be uploaded. The size index is maintained in memory so a spool
// write never walks the whole tree.
type spool struct {
	dir      string
	maxBytes int64
	logger   *slog.Logger

	mu       sync.Mutex
	inFlight map[string]bool
	indexed  bool
	sizes    map[string]int64
	total    int64
	oldest   keyHeap
}

func newSpool(dir string, maxBytes int64, logger *slog.Logger) *spool {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &spool{
		dir:      filepath.Clean(dir),
		maxBytes: maxBytes,
		logger:   logger,
		inFlight: make(map[string]bool),
	}
}

// check verifies that the spool directory exists and is writable without
// leaving a probe or temp file behind, then builds the size index.
func (s *spool) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, spoolDirPerm); err != nil {
		return err
	}
	info, err := os.Stat(s.dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s: not a directory", s.dir)
	}

	f, err := os.CreateTemp(s.dir, ".check-*")
	if err != nil {
		return err
	}
	name := f.Name()
	_, writeErr := f.Write([]byte{0})
	syncErr := f.Sync()
	closeErr := f.Close()
	removeErr := os.Remove(name)
	if err := errors.Join(writeErr, syncErr, closeErr, removeErr); err != nil {
		return err
	}

	if err := s.removeTemps(ctx); err != nil {
		return err
	}
	return s.ensureIndex(ctx)
}

// removeTemps removes temp files left by a crash during an atomic spool write.
// Concurrent collectors are unsupported, so any .spool-* file is stale at
// startup.
func (s *spool) removeTemps(ctx context.Context) error {
	var errs []error
	err := filepath.WalkDir(s.dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() || !strings.HasPrefix(d.Name(), ".spool-") {
			return nil
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
		return nil
	})
	return errors.Join(append(errs, err)...)
}

// write stores body under key atomically and returns the relative key it used.
// If a pending spool entry already occupies key, the next collision suffix is
// reserved so an earlier chunk is never overwritten.
func (s *spool) write(ctx context.Context, key string, body []byte) (string, error) {
	if err := s.ensureIndex(ctx); err != nil {
		return "", err
	}

	final, used, err := s.reserve(key)
	if err != nil {
		return "", err
	}
	dir := filepath.Dir(final)

	tmp, err := os.CreateTemp(dir, ".spool-*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if err := tmp.Chmod(spoolFilePerm); err != nil {
		_ = tmp.Close()
		cleanup()
		return "", err
	}
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		cleanup()
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return "", err
	}
	if err := os.Rename(tmpName, final); err != nil {
		cleanup()
		return "", err
	}

	// The file is in place, so account for it even if the directory fsync
	// fails below. The caller reports the failure and collection exits; the
	// next startup rebuilds the index from disk.
	s.index(used, int64(len(body)))
	if err := syncDir(dir); err != nil {
		return "", err
	}
	return used, nil
}

// reserve returns the first free path for key, adding a collision suffix when a
// pending spool entry already owns that window. Collection is the only spool
// writer and replay only deletes, so a stat-then-rename guard is sufficient.
func (s *spool) reserve(key string) (path, used string, err error) {
	first, err := s.path(key)
	if err != nil {
		return "", "", err
	}
	if err := os.MkdirAll(filepath.Dir(first), spoolDirPerm); err != nil {
		return "", "", err
	}

	for n := 0; n <= maxSpoolCollisions; n++ {
		candidate := key
		if n > 0 {
			candidate = chunk.CollisionKey(key, n)
		}
		candidatePath, err := s.path(candidate)
		if err != nil {
			return "", "", err
		}
		_, statErr := os.Lstat(candidatePath)
		switch {
		case errors.Is(statErr, fs.ErrNotExist):
			return candidatePath, candidate, nil
		case statErr != nil:
			return "", "", statErr
		}
	}
	return "", "", fmt.Errorf("no free spool name after %d collisions for %s", maxSpoolCollisions, key)
}

// path maps a relative chunk key to its spool path and rejects keys that could
// escape the spool root.
func (s *spool) path(key string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(key))
	if clean == "." || clean == ".." || filepath.IsAbs(clean) ||
		strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid spool key %q", key)
	}
	return filepath.Join(s.dir, clean), nil
}

// read returns the bytes of one spooled chunk. A missing file is reported as a
// filesystem error so callers can decide whether it is benign.
func (s *spool) read(key string) ([]byte, error) {
	path, err := s.path(key)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

// verify streams key through the chunk decoder without retaining the samples.
// A truncated or bit-rotted file must be caught before replay uploads it: the
// object would outlive the local copy and could never be repaired. The stream
// is drained to io.Discard so gzip verifies its checksum and trailer.
func (s *spool) verify(key string) error {
	path, err := s.path(key)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("gzip: %w", err)
	}
	defer gz.Close()

	dec := json.NewDecoder(gz)
	for {
		var sample model.Sample
		err := dec.Decode(&sample)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("decode: %w", err)
		}
	}
	// Drain the rest of the compressed stream to io.Discard so gzip reports a
	// bad checksum or a short trailer even if the decoder stopped early.
	if _, err := io.Copy(io.Discard, gz); err != nil {
		return fmt.Errorf("gzip: %w", err)
	}
	return nil
}

// remove deletes one spool entry and updates the size index.
func (s *spool) remove(key string) error {
	path, err := s.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	s.forget(key)
	return nil
}

// entries lists every valid chunk file in the spool in lexicographic key order.
// That order is chronological within a host and scope. Temp files and anything
// that does not match the chunk layout are ignored.
func (s *spool) entries(ctx context.Context) ([]spoolEntry, error) {
	var entries []spoolEntry
	err := filepath.WalkDir(s.dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), chunk.FileSuffix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(s.dir, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if !validSpoolKey(key) {
			return nil
		}
		entries = append(entries, spoolEntry{key: key, size: info.Size()})
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	return entries, nil
}

func validSpoolKey(key string) bool {
	parts := strings.Split(key, "/")
	if len(parts) != 7 {
		return false
	}
	if !chunk.IsComponent(parts[0]) || !chunk.IsComponent(parts[1]) {
		return false
	}
	_, _, err := chunk.ParseName(parts[6])
	return err == nil
}

// listHour returns the spooled keys for one host/scope/hour whose windows
// overlap r. A missing directory is an empty hour, not an error.
func (s *spool) listHour(host, scope string, hour time.Time, r model.TimeRange) ([]string, error) {
	if err := chunk.ValidateComponent("host", host); err != nil {
		return nil, err
	}
	if err := chunk.ValidateComponent("scope", scope); err != nil {
		return nil, err
	}

	dir := filepath.Join(s.dir, host, scope,
		hour.Format("2006"), hour.Format("01"), hour.Format("02"), hour.Format("15"))
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var keys []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), chunk.FileSuffix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		start, end, err := chunk.ParseName(entry.Name())
		if err != nil {
			s.logger.Warn("ignoring malformed spool chunk", "path", filepath.Join(dir, entry.Name()), "error", err)
			continue
		}
		if !chunk.Overlaps(start, end, r) {
			continue
		}
		rel, err := filepath.Rel(s.dir, filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		keys = append(keys, filepath.ToSlash(rel))
	}
	sort.Strings(keys)
	return keys, nil
}

// list walks the spool for an unbounded range and groups matching keys by their
// start hour. A missing spool directory is empty, not an error.
func (s *spool) list(ctx context.Context, r model.TimeRange, host, scope string) (map[time.Time][]string, error) {
	byHour := make(map[time.Time][]string)
	err := filepath.WalkDir(s.dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), chunk.FileSuffix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(s.dir, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if !validSpoolKey(key) {
			return nil
		}
		parts := strings.Split(key, "/")
		if host != "" && parts[0] != host {
			return nil
		}
		if scope != "" && parts[1] != scope {
			return nil
		}
		start, end, err := chunk.ParseName(parts[6])
		if err != nil {
			s.logger.Warn("ignoring malformed spool chunk", "path", path, "error", err)
			return nil
		}
		if !chunk.Overlaps(start, end, r) {
			return nil
		}
		hour := start.UTC().Truncate(time.Hour)
		byHour[hour] = append(byHour[hour], key)
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	for _, keys := range byHour {
		sort.Strings(keys)
	}
	return byHour, nil
}

// hosts returns the host directories present in the spool.
func (s *spool) hosts() ([]string, error) { return s.components(s.dir) }

// scopes returns the scope directories present under host.
func (s *spool) scopes(host string) ([]string, error) {
	if err := chunk.ValidateComponent("host", host); err != nil {
		return nil, err
	}
	return s.components(filepath.Join(s.dir, host))
}

func (s *spool) components(dir string) ([]string, error) {
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

// acquire marks key as being replayed. It returns false when another replay is
// already handling it. Cap enforcement skips in-flight keys.
func (s *spool) acquire(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight[key] {
		return false
	}
	s.inFlight[key] = true
	return true
}

func (s *spool) release(key string) {
	s.mu.Lock()
	delete(s.inFlight, key)
	s.mu.Unlock()
}

// ensureIndex rebuilds the in-memory size index once per process. The directory
// walk is bounded by the spool itself and is not repeated per write.
func (s *spool) ensureIndex(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ensureIndexLocked(ctx)
}

func (s *spool) ensureIndexLocked(ctx context.Context) error {
	if s.indexed {
		return nil
	}
	// entries reads the directory only and does not take s.mu.
	entries, err := s.entries(ctx)
	if err != nil {
		return err
	}
	sizes := make(map[string]int64, len(entries))
	keys := make(keyHeap, 0, len(entries))
	for _, entry := range entries {
		sizes[entry.key] = entry.size
		keys = append(keys, entry.key)
	}
	heap.Init(&keys)
	s.sizes = sizes
	s.oldest = keys
	s.total = 0
	for _, size := range sizes {
		s.total += size
	}
	s.indexed = true
	return nil
}

func (s *spool) index(key string, size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sizes[key]; ok {
		return
	}
	s.sizes[key] = size
	s.total += size
	heap.Push(&s.oldest, key)
}

func (s *spool) forget(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	size, ok := s.sizes[key]
	if !ok {
		return
	}
	delete(s.sizes, key)
	s.total -= size
	s.compactLocked()
}

// compactLocked rebuilds the heap when replay has left too many stale entries.
// Removal from an arbitrary position is not free, so deleted keys are skipped
// lazily and the heap is compacted once it is mostly stale.
func (s *spool) compactLocked() {
	if s.oldest.Len() <= 2*len(s.sizes)+1024 {
		return
	}
	keys := make(keyHeap, 0, len(s.sizes))
	for key := range s.sizes {
		keys = append(keys, key)
	}
	heap.Init(&keys)
	s.oldest = keys
}

// enforceCap drops the lexicographically oldest unprotected spool entries until
// the total is at or below spool_max_bytes. It is best effort: a failed delete
// is logged and the next chunk write tries again. Collection never waits on the
// network or on replay here.
func (s *spool) enforceCap(ctx context.Context, protect string) {
	if err := ctx.Err(); err != nil {
		return
	}
	if err := s.ensureIndex(ctx); err != nil {
		s.logger.Error("scan spool for cap enforcement", "spool_dir", s.dir, "error", err)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.total <= s.maxBytes {
		return
	}

	var deferred []string
	for s.total > s.maxBytes && s.oldest.Len() > 0 {
		key := heap.Pop(&s.oldest).(string)
		size, ok := s.sizes[key]
		if !ok {
			continue
		}
		if key == protect || s.inFlight[key] {
			deferred = append(deferred, key)
			continue
		}

		path, err := s.path(key)
		if err != nil {
			s.logger.Error("drop spooled chunk", "key", key, "error", err)
			deferred = append(deferred, key)
			continue
		}
		if err := os.Remove(path); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				delete(s.sizes, key)
				s.total -= size
				continue
			}
			s.logger.Error("drop spooled chunk", "key", key, "error", err)
			deferred = append(deferred, key)
			continue
		}
		delete(s.sizes, key)
		s.total -= size
		s.logger.Error("dropped oldest spooled chunk over spool_max_bytes",
			"key", key, "bytes", size, "spool_bytes", s.total, "spool_max_bytes", s.maxBytes)
	}
	for _, key := range deferred {
		heap.Push(&s.oldest, key)
	}
	if s.total > s.maxBytes {
		s.logger.Error("spool remains over spool_max_bytes after dropping unprotected chunks",
			"spool_dir", s.dir, "spool_bytes", s.total, "spool_max_bytes", s.maxBytes)
	}
}

// TrimSpool implements store.Replayer. It applies spool_max_bytes once and is
// called by collect synchronously before collection starts, so startup
// enforcement cannot race the first spool write. Enforcement is best effort and
// logs its own failures.
func (s *Store) TrimSpool(ctx context.Context) {
	s.spool.enforceCap(ctx, "")
}

// Replay drains the spool in key order until ctx is canceled. It is started by
// collect only, after TrimSpool has applied the size cap, so it never races the
// startup trim against the first Write. A transient upload failure leaves the
// file in place and backs off; a corrupt file or a permanent conflict is logged
// at error and left for an operator.
func (s *Store) Replay(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	blocked := make(map[string]struct{})
	var backoff time.Duration

	for {
		if ctx.Err() != nil {
			return
		}
		failed, progressed, err := s.replayPass(ctx, blocked)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("scan spool", "spool_dir", s.spool.dir, "error", err)
			if !sleepContext(ctx, nextReplayDelay(&backoff)) {
				return
			}
			continue
		}
		if progressed {
			backoff = 0
		}
		if failed {
			if !sleepContext(ctx, nextReplayDelay(&backoff)) {
				return
			}
			continue
		}
		if !sleepContext(ctx, replayInterval) {
			return
		}
	}
}

// replayPass attempts every unblocked spool entry once. It reports whether a
// transient failure ended the pass early and whether any entry made progress,
// so Replay can choose between backoff and the idle interval. A corrupt file is
// blocked for the rest of the process: its bytes stay on disk for an operator,
// but replay does not retry it every cycle.
func (s *Store) replayPass(ctx context.Context, blocked map[string]struct{}) (failed, progressed bool, err error) {
	entries, err := s.spool.entries(ctx)
	if err != nil {
		return false, false, err
	}

	for _, entry := range entries {
		if ctx.Err() != nil {
			return failed, progressed, ctx.Err()
		}
		if _, ok := blocked[entry.key]; ok {
			continue
		}

		replayed, replayErr := func() (bool, error) {
			if !s.spool.acquire(entry.key) {
				return false, nil
			}
			defer s.spool.release(entry.key)
			return s.replayEntry(ctx, entry.key)
		}()
		switch {
		case replayErr == nil:
			if replayed {
				progressed = true
				s.logger.Info("replayed spooled chunk", "key", entry.key)
			}
		case ctx.Err() != nil:
			return failed, progressed, ctx.Err()
		case errors.Is(replayErr, errSpoolCorrupt):
			blocked[entry.key] = struct{}{}
			progressed = true
			s.logger.Error("spooled chunk failed verification; leaving it for an operator",
				"key", entry.key, "error", replayErr)
		case errors.Is(replayErr, errSpoolConflict):
			blocked[entry.key] = struct{}{}
			progressed = true
			s.logger.Error("spooled chunk conflicts with an existing object; leaving it for an operator",
				"key", entry.key, "error", replayErr)
		default:
			s.logger.Warn("replay spooled chunk", "key", entry.key, "error", replayErr)
			return true, progressed, nil
		}
	}
	return false, progressed, nil
}

// replayEntry verifies then uploads one spool file at its exact key and deletes
// it after a successful (or already-present) upload. It reports false when the
// file disappeared before it could be read. A file that does not decode as a
// chunk is never uploaded: the error wraps errSpoolCorrupt and the bytes stay
// on disk.
func (s *Store) replayEntry(ctx context.Context, key string) (bool, error) {
	if err := s.spool.verify(key); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("%s: %w: %w", key, errSpoolCorrupt, err)
	}
	body, err := s.spool.read(key)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("read spool %s: %w", key, err)
	}
	if err := s.putExact(ctx, key, body); err != nil {
		return false, err
	}
	if err := s.spool.remove(key); err != nil {
		return false, fmt.Errorf("remove replayed spool %s: %w", key, err)
	}
	return true, nil
}

// nextReplayDelay advances backoff and returns the delay before the next replay
// attempt. Equal jitter keeps half the fixed delay and randomizes the rest, so
// hosts that shared an outage do not resynchronise their retries.
func nextReplayDelay(backoff *time.Duration) time.Duration {
	if *backoff == 0 {
		*backoff = replayBackoffBase
	} else {
		*backoff *= 2
		if *backoff > replayBackoffMax {
			*backoff = replayBackoffMax
		}
	}
	half := *backoff / 2
	return half + rand.N(half+1)
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// syncDir makes the rename durable. A filesystem that rejects directory fsync
// is a store failure rather than a silent durability gap.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// keyHeap is a min-heap of relative chunk keys. Entries for deleted files are
// skipped lazily; compactLocked rebuilds it when they dominate.
type keyHeap []string

func (h keyHeap) Len() int           { return len(h) }
func (h keyHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h keyHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *keyHeap) Push(x any)        { *h = append(*h, x.(string)) }
func (h *keyHeap) Pop() any {
	old := *h
	n := len(old)
	key := old[n-1]
	*h = old[:n-1]
	return key
}
