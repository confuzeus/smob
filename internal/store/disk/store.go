// Package disk implements the store contract on the local filesystem.
//
// A chunk is one gzip-compressed JSONL object at
//
//	<root>/<host>/<scope>/YYYY/MM/DD/HH/<start>-<end>.jsonl.gz
//
// Completed chunks are immutable: a write goes to a temporary file that is
// fsynced and renamed into place. If the target name already exists, the writer
// appends -1, -2, and so on rather than overwriting.
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

	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/store"
	"github.com/confuzeus/smob/internal/store/chunk"
)

const (
	dirPerm  = 0o755
	filePerm = 0o644
	// maxCollisions bounds the suffix search so a writer bug cannot spin.
	maxCollisions = 1000
)

// Store reads and writes chunks below one root directory.
type Store struct {
	root   string
	logger *slog.Logger
}

// New returns a disk store rooted at root. It creates nothing: querying a fresh
// install is not an error, and Write creates directories as needed.
func New(root string, logger *slog.Logger) (*Store, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("disk: data directory is empty")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Store{root: filepath.Clean(root), logger: logger}, nil
}

// Write persists one completed chunk. Every filesystem failure is wrapped in
// store.ErrPersistent so the CLI reports a persistent store failure.
func (s *Store) Write(ctx context.Context, c store.Chunk) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := chunk.Key(c.Host, c.Scope, c.Start, c.End)
	if err != nil {
		return fmt.Errorf("write chunk: %w", err)
	}
	if err := s.write(key, c.Samples); err != nil {
		return fmt.Errorf("write %s: %w: %w", key, store.ErrPersistent, err)
	}
	return nil
}

// Close implements store.Store. A disk store holds no resources between calls.
func (s *Store) Close() error { return nil }

// Check implements store.Checker. It creates the root if needed and writes and
// removes a temporary probe file, so an unusable data directory fails at
// startup. It never leaves a chunk behind.
func (s *Store) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.probe(); err != nil {
		return fmt.Errorf("check %s: %w: %w", s.root, store.ErrPersistent, err)
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

func (s *Store) write(key string, samples []model.Sample) error {
	final, err := reserve(filepath.Join(s.root, key))
	if err != nil {
		return err
	}
	dir := filepath.Dir(final)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return err
	}

	tmp := final + ".tmp"
	if err := writeSamples(tmp, samples); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDir(dir)
}

// reserve returns the first free path for key, adding a collision suffix when a
// previous write already owns that window. Concurrent writers for the same host
// and prefix are unsupported, so a stat-then-rename guard is sufficient.
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
// shared with the S3 backend, so identical samples produce identical bytes.
func writeSamples(path string, samples []model.Sample) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, filePerm)
	if err != nil {
		return err
	}
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

// syncDir makes the rename durable. Some filesystems reject directory fsync;
// that is a store failure rather than a silent durability gap.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
