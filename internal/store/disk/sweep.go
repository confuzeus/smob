package disk

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/confuzeus/smob/internal/store"
	"github.com/confuzeus/smob/internal/store/chunk"
)

// sweepChunk is one closed chunk file below the store root. Only metadata is
// kept: the corpus is never opened or decoded.
type sweepChunk struct {
	path  string
	key   string
	start time.Time
	end   time.Time
	size  int64
}

// Sweep implements store.Sweeper. It performs one retention pass over the
// documented chunk layout below the store root: age first, then total size.
// Age is measured from the chunk end, the moment the window is complete, except
// for an end in the future: that chunk is measured from its start so a clock
// jump or a planted name cannot make it immortal. It logs its own failures and
// never reports an error, because retention must not interrupt collection.
//
// A chunk whose end is after the sweep start is left alone by the size pass.
// The partial chunk flushed at shutdown can end a second in the future while a
// sweep is running: the file is written atomically and is complete, but the
// same instant's size pass must not delete it. A later pass treats it normally.
func (s *Store) Sweep(ctx context.Context, r store.Retention) {
	if r.MaxAge <= 0 && r.MaxBytes <= 0 {
		return
	}

	now := time.Now().UTC()
	chunks, err := s.sweepCandidates(ctx, now)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Warn("scan chunks for retention", "data_dir", s.root, "error", err)
		}
		return
	}

	// Age first. A failed delete leaves the chunk in the kept slice and is
	// retried by the next pass.
	kept := chunks[:0]
	for _, c := range chunks {
		if ctx.Err() != nil {
			return
		}
		if r.MaxAge > 0 && !c.ageTime(now).After(now.Add(-r.MaxAge)) {
			if s.deleteChunk(ctx, c, "age") {
				continue
			}
			if ctx.Err() != nil {
				return
			}
		}
		kept = append(kept, c)
	}

	if r.MaxBytes <= 0 {
		return
	}

	var total int64
	for _, c := range kept {
		total += c.size
	}
	for _, c := range kept {
		if ctx.Err() != nil {
			return
		}
		if total <= r.MaxBytes {
			break
		}
		if c.end.After(now) {
			continue
		}
		if s.deleteChunk(ctx, c, "size") {
			total -= c.size
		}
		if ctx.Err() != nil {
			return
		}
	}
	if total > r.MaxBytes {
		s.logger.Warn("retention size limit remains exceeded",
			"data_dir", s.root, "chunk_bytes", total, "max_bytes", r.MaxBytes)
	}
}

// sweepCandidates lists the closed chunks below the store root. The walk is
// metadata-only and ordered oldest first by chunk end, then by key so ties are
// deterministic. A chunk that disappears between listing and stat is skipped,
// and an unreadable subtree is logged and skipped rather than aborting the pass.
//
// The configured spool directory is skipped even when it sits below the root:
// retention must never delete a chunk that is still waiting to be uploaded.
func (s *Store) sweepCandidates(ctx context.Context, now time.Time) ([]sweepChunk, error) {
	// Resolve a symlinked data directory so its chunks remain visible; WalkDir
	// does not follow a symlink at the root.
	root := resolvePath(s.root)
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	spool := ""
	if s.spool != "" {
		spool = resolvePath(s.spool)
	}

	var chunks []sweepChunk
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			s.logger.Warn("walk data directory for retention", "path", path, "error", walkErr)
			return nil
		}
		if d.IsDir() {
			if spool != "" && under(path, spool) {
				// A spool_dir at or above the data directory makes staged
				// uploads indistinguishable from closed chunks, so the whole
				// configured subtree is skipped rather than interpreted.
				return fs.SkipDir
			}
			return nil
		}
		c, ok := s.sweepFile(root, spool, path, d)
		if !ok {
			return nil
		}
		chunks = append(chunks, c)
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	sort.Slice(chunks, func(i, j int) bool {
		ei, ej := orderEnd(chunks[i].end, now), orderEnd(chunks[j].end, now)
		if !ei.Equal(ej) {
			return ei.Before(ej)
		}
		return chunks[i].key < chunks[j].key
	})
	return chunks, nil
}

// ageTime is the timestamp the age limit measures. A completed window is
// measured from its end. A future end cannot be trusted — a clock jump or a
// planted filename can push it out forever — so that chunk is measured from
// its start instead and becomes deletable like any other completed window.
func (c sweepChunk) ageTime(now time.Time) time.Time {
	if c.end.After(now) {
		return c.start
	}
	return c.end
}

// orderEnd caps a future chunk end at now for size-pass ordering. A genuine
// in-flight chunk is still spared by the size pass, but a bogus future end must
// not sort behind every real chunk forever.
func orderEnd(end, now time.Time) time.Time {
	if end.After(now) {
		return now
	}
	return end
}

// sweepFile reports whether path is a completed chunk in the documented
// layout. Anything else — .tmp files, the S3 spool, stray or malformed files —
// is deliberately ignored.
func (s *Store) sweepFile(root, spool, path string, d fs.DirEntry) (sweepChunk, bool) {
	// Defence in depth: the walk skips the spool subtree, but no path below it
	// may ever be classified as a data chunk.
	if spool != "" && under(path, spool) {
		return sweepChunk{}, false
	}

	rel, err := filepath.Rel(root, path)
	if err != nil {
		return sweepChunk{}, false
	}
	rel = filepath.ToSlash(rel)

	parts := strings.Split(rel, "/")
	if len(parts) != 7 {
		return sweepChunk{}, false
	}
	hour, ok := chunk.ParseHourPath(strings.Join(parts[:6], "/"))
	if !ok {
		return sweepChunk{}, false
	}
	start, end, err := chunk.ParseName(parts[6])
	if err != nil || !start.Truncate(time.Hour).Equal(hour) {
		return sweepChunk{}, false
	}

	info, err := d.Info()
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			s.logger.Warn("stat chunk for retention", "key", rel, "error", err)
		}
		return sweepChunk{}, false
	}
	if !info.Mode().IsRegular() {
		return sweepChunk{}, false
	}
	return sweepChunk{path: path, key: rel, start: start, end: end, size: info.Size()}, true
}

// resolvePath resolves symlinks in path. Components that do not exist yet are
// kept as given, so a spool directory created after startup still compares
// equal once it appears.
func resolvePath(path string) string {
	path = filepath.Clean(path)
	rest := ""
	for {
		if resolved, err := filepath.EvalSymlinks(path); err == nil {
			return filepath.Join(resolved, rest)
		}
		parent := filepath.Dir(path)
		if parent == path {
			return filepath.Join(path, rest)
		}
		rest = filepath.Join(filepath.Base(path), rest)
		path = parent
	}
}

// under reports whether path is dir itself or a descendant of it. Both paths
// are cleaned by resolvePath before they reach here.
func under(path, dir string) bool {
	rel, err := filepath.Rel(dir, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

// deleteChunk removes one chunk and logs the outcome. A file that is already
// gone counts as deleted: a query or another sweeper may have won the race.
func (s *Store) deleteChunk(ctx context.Context, c sweepChunk, reason string) bool {
	if ctx.Err() != nil {
		return false
	}
	// Last gate: never remove a spooled chunk, even if a caller finds one.
	if s.spool != "" && under(resolvePath(c.path), resolvePath(s.spool)) {
		s.logger.Warn("refusing to delete a spooled chunk", "key", c.key, "spool_dir", s.spool)
		return false
	}
	if err := os.Remove(c.path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return true
		}
		s.logger.Warn("delete chunk for retention", "key", c.key, "reason", reason, "error", err)
		return false
	}
	s.logger.Info("deleted chunk", "key", c.key, "bytes", c.size, "reason", reason)
	return true
}
