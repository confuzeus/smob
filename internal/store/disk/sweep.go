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
	path string
	key  string
	end  time.Time
	size int64
}

// Sweep implements store.Sweeper. It performs one retention pass over the
// documented chunk layout below the store root: age first, then total size.
// Age is measured from the chunk end, the moment the window is complete. It
// logs its own failures and never reports an error, because retention must not
// interrupt collection.
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
	chunks, err := s.sweepCandidates(ctx)
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
		if r.MaxAge > 0 && !c.end.After(now.Add(-r.MaxAge)) {
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
func (s *Store) sweepCandidates(ctx context.Context) ([]sweepChunk, error) {
	// Resolve a symlinked data directory so its chunks remain visible; WalkDir
	// does not follow a symlink at the root.
	root, err := filepath.EvalSymlinks(s.root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	var chunks []sweepChunk
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
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
			return nil
		}
		c, ok := s.sweepFile(root, path, d)
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
		if !chunks[i].end.Equal(chunks[j].end) {
			return chunks[i].end.Before(chunks[j].end)
		}
		return chunks[i].key < chunks[j].key
	})
	return chunks, nil
}

// sweepFile reports whether path is a completed chunk in the documented
// layout. Anything else — .tmp files, the S3 spool, stray or malformed files —
// is deliberately ignored.
func (s *Store) sweepFile(root, path string, d fs.DirEntry) (sweepChunk, bool) {
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
	return sweepChunk{path: path, key: rel, end: end, size: info.Size()}, true
}

// deleteChunk removes one chunk and logs the outcome. A file that is already
// gone counts as deleted: a query or another sweeper may have won the race.
func (s *Store) deleteChunk(ctx context.Context, c sweepChunk, reason string) bool {
	if ctx.Err() != nil {
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
