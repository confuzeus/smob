// Package disk implements the store contract on the local filesystem.
//
// A chunk is one gzip-compressed JSONL object at
//
//	<root>/<host>/<scope>/YYYY/MM/DD/HH/<start>-<end>.jsonl.gz
//
// Completed chunks are immutable: a write goes to a unique temporary file that
// is fsynced and hard-linked into place. If the target name already exists, the
// writer appends -1, -2, and so on rather than overwriting.
package disk

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/store"
	"github.com/confuzeus/smob/internal/store/chunk"
)

const (
	// dirPerm and filePerm keep the executable bit off and deny access to
	// "other": chunks carry process and container names, so any local user who
	// is not in the service group must not read them. Existing files are never
	// chmodded; the explicit chmod on the temp file also defeats a permissive
	// umask.
	dirPerm  = 0o750
	filePerm = 0o640
	// maxCollisions bounds the suffix search so a writer bug cannot spin.
	maxCollisions = 1000
)

// Store reads and writes chunks below one root directory.
type Store struct {
	root string
	// rootReal is root with its existing symlinks resolved. It is computed once
	// here and is the trust anchor for containment and for the durability chain.
	rootReal string
	// spool is the S3 staging directory. Retention must never delete below it,
	// even when it sits inside root. Empty when no spool is configured.
	spool  string
	logger *slog.Logger
}

// New returns a disk store rooted at root. spoolDir, when non-empty, is the S3
// staging directory that retention must leave alone. New creates nothing:
// querying a fresh install is not an error, and Write creates directories as
// needed. Resolving the root reads the filesystem but writes nothing.
func New(root, spoolDir string, logger *slog.Logger) (*Store, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("disk: data directory is empty")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	root = filepath.Clean(root)
	s := &Store{root: root, rootReal: resolvePath(root), logger: logger}
	if spoolDir = strings.TrimSpace(spoolDir); spoolDir != "" {
		s.spool = filepath.Clean(spoolDir)
	}
	return s, nil
}

// Write persists one completed chunk. Samples are sorted and validated by
// chunk.Prepare before encoding, so the stored chunk satisfies the Query
// ordering contract regardless of the caller. Every filesystem failure is
// wrapped in store.ErrPersistent so the CLI reports a persistent store failure.
func (s *Store) Write(ctx context.Context, c store.Chunk) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := chunk.Key(c.Host, c.Scope, c.Start, c.End)
	if err != nil {
		return fmt.Errorf("write chunk: %w", err)
	}
	samples, err := chunk.Prepare(c.Start, c.End, c.Samples)
	if err != nil {
		return fmt.Errorf("write chunk: %w", err)
	}
	if err := s.write(key, samples); err != nil {
		return fmt.Errorf("write %s: %w: %w", key, store.ErrPersistent, err)
	}
	return nil
}

// Close implements store.Store. A disk store holds no resources between calls.
func (s *Store) Close() error { return nil }

// Check implements store.Checker. It creates the root if needed, writes and
// removes a temporary probe file, and deletes temp files left by a crash, so an
// unusable data directory fails at startup. It never leaves a chunk behind.
func (s *Store) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.probe(); err != nil {
		return fmt.Errorf("check %s: %w: %w", s.root, store.ErrPersistent, err)
	}
	if err := s.removeTemps(ctx); err != nil {
		return fmt.Errorf("clean %s: %w: %w", s.root, store.ErrPersistent, err)
	}
	return nil
}

func (s *Store) probe() error {
	if err := os.MkdirAll(s.root, dirPerm); err != nil {
		return err
	}
	f, err := os.CreateTemp(s.root, ".check-*")
	if err != nil {
		return err
	}
	name := f.Name()
	closeErr := f.Close()
	removeErr := os.Remove(name)
	return errors.Join(closeErr, removeErr)
}

// removeTemps deletes crash leftovers below the store root: the unique
// .smob-*.tmp write temp, the legacy <chunk>.tmp name, and .check-* probe files.
// Only those exact name patterns are matched, so chunks and directories are
// never touched. Concurrent collectors are unsupported, so every match at
// startup is stale. A spool nested below the root is skipped: it owns its own
// temp names.
func (s *Store) removeTemps(ctx context.Context) error {
	root := s.rootReal
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	spool := ""
	if s.spool != "" {
		spool = resolvePath(s.spool)
		// Only a spool nested inside the data directory owns a subtree of it.
		// An equal or enclosing spool must not suppress the cleanup.
		if spool == root || !under(spool, root) {
			spool = ""
		}
	}

	var errs []error
	removed := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() {
			if spool != "" && under(path, spool) {
				return fs.SkipDir
			}
			return nil
		}
		if !isTempName(d.Name()) {
			return nil
		}
		if err := os.Remove(path); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, err)
			}
			return nil
		}
		removed++
		return nil
	})
	if err := errors.Join(append(errs, err)...); err != nil {
		return err
	}
	if removed > 0 {
		s.logger.Info("removed leftover temp files", "data_dir", s.root, "files", removed)
	}
	return nil
}

// isTempName reports whether name is a temp pattern this package can leave
// behind after a crash.
func isTempName(name string) bool {
	if strings.HasPrefix(name, ".smob-") && strings.HasSuffix(name, ".tmp") {
		return true
	}
	if strings.HasSuffix(name, chunk.FileSuffix+".tmp") {
		return true
	}
	return strings.HasPrefix(name, ".check-")
}

func (s *Store) write(key string, samples []model.Sample) error {
	final := filepath.Join(s.root, key)
	if err := os.MkdirAll(filepath.Dir(final), dirPerm); err != nil {
		return err
	}
	// MkdirAll follows symlinks, so a symlinked host or scope component could
	// have created the directory outside the store root. Resolve what was made
	// and keep only paths below the root.
	dir, err := s.contain(filepath.Dir(final))
	if err != nil {
		return err
	}
	final = filepath.Join(dir, filepath.Base(final))

	// A unique temp name means a planted symlink cannot be followed and a
	// concurrent writer cannot truncate someone else's partial chunk.
	tmp, err := os.CreateTemp(dir, ".smob-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if err := tmp.Chmod(filePerm); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := writeSamples(tmp, samples); err != nil {
		cleanup()
		return err
	}
	if err := publish(tmpName, final); err != nil {
		cleanup()
		return err
	}

	// The link keeps the data and the rename fallback moves it, so the temp is
	// already gone on that path.
	if err := os.Remove(tmpName); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return s.syncChain(dir)
}

// contain resolves dir and returns it only when it is the store root or a
// descendant of it.
func (s *Store) contain(dir string) (string, error) {
	real, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return "", err
	}
	if !under(real, s.rootReal) {
		return "", fmt.Errorf("%s resolves to %s, outside data directory %s", dir, real, s.rootReal)
	}
	return real, nil
}

// syncChain makes a publish durable across the directories Write may have
// created. It fsyncs dir and each ancestor up to and including the store root,
// then stops: nothing above the root is part of the store.
func (s *Store) syncChain(dir string) error {
	for under(dir, s.rootReal) {
		if err := syncDir(dir); err != nil {
			return err
		}
		if dir == s.rootReal {
			return nil
		}
		dir = filepath.Dir(dir)
	}
	return fmt.Errorf("%s is outside data directory %s", dir, s.rootReal)
}

// publish hard-links the completed temp file to the first key name that does not
// exist, suffixing the key when a previous write owns the window. link(2) fails
// with EEXIST instead of replacing, so the collision guard is atomic between
// concurrent writers.
func publish(tmp, key string) error {
	for i := 0; i <= maxCollisions; i++ {
		candidate := key
		if i > 0 {
			candidate = chunk.CollisionKey(key, i)
		}
		err := os.Link(tmp, candidate)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, fs.ErrExist):
			continue
		case errors.Is(err, syscall.EOPNOTSUPP),
			errors.Is(err, syscall.EPERM),
			errors.Is(err, syscall.EXDEV):
			// Some filesystems refuse hard links; fall back to reserve+rename.
			return publishByRename(tmp, key)
		default:
			return err
		}
	}
	return fmt.Errorf("no free name after %d collisions for %s", maxCollisions, key)
}

// publishByRename is the fallback for filesystems that refuse hard links. It
// keeps the older stat-then-rename guard, which a concurrent writer can still
// overwrite; correctness on those filesystems is limited to the single-writer
// case.
func publishByRename(tmp, key string) error {
	final, err := reserve(key)
	if err != nil {
		return err
	}
	return os.Rename(tmp, final)
}

// reserve returns the first free path for key, adding a collision suffix when a
// previous write already owns that window. It is only used by the hard-link
// fallback: the stat-then-rename guard cannot stop a writer that appears after
// the check.
func reserve(key string) (string, error) {
	for i := 0; i <= maxCollisions; i++ {
		candidate := key
		if i > 0 {
			candidate = chunk.CollisionKey(key, i)
		}
		_, err := os.Lstat(candidate)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return candidate, nil
		case err != nil:
			return "", err
		}
	}
	return "", fmt.Errorf("no free name after %d collisions for %s", maxCollisions, key)
}

// writeSamples writes JSONL through gzip and fsyncs the result. The encoding is
// shared with the S3 backend, so identical samples produce identical bytes. It
// closes f on every path.
func writeSamples(f *os.File, samples []model.Sample) error {
	if err := chunk.Encode(f, samples); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// syncDir makes the publish durable. Some filesystems reject directory fsync;
// that is a store failure rather than a silent durability gap.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
