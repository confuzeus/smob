package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"syscall"

	"github.com/confuzeus/smob/internal/cli"
	"github.com/confuzeus/smob/internal/collector"
	"github.com/confuzeus/smob/internal/source/docker"
	"github.com/confuzeus/smob/internal/source/process"
	"github.com/confuzeus/smob/internal/source/system"
	"github.com/confuzeus/smob/internal/store"
	"github.com/spf13/cobra"
)

// lowDiskBytes is the free-space threshold for the startup warning.
const lowDiskBytes = 1 << 30

func (a *app) collectCommand() *cobra.Command {
	var dryRun, once bool

	cmd := &cobra.Command{
		Use:   "collect",
		Short: "Collect metrics and write them to the configured store",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := a.load(cmd); err != nil {
				return err
			}
			if dryRun && once {
				return cli.UsageErrorf("collect: --dry-run and --once are mutually exclusive")
			}
			return a.runCollect(cmd.Context(), dryRun, once)
		},
	}

	cmd.Flags().StringVar(&a.dataDir, "data-dir", "", "override data_dir from the configuration")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate the configuration, sources, and store without writing samples")
	cmd.Flags().BoolVar(&once, "once", false, "collect one snapshot from every enabled source and exit")
	return cmd
}

// runCollect builds every enabled source and the store, probes both, then runs
// the collector. The collector owns and closes the store. The Docker source is
// the only source with a lifecycle: it is constructed (and probed) before the
// store, started after a successful dry-run check, and always closed.
func (a *app) runCollect(ctx context.Context, dryRun, once bool) (err error) {
	sources, dockerSrc, err := a.newSources(ctx, once)
	if err != nil {
		return err
	}
	if dockerSrc != nil {
		defer func() {
			if closeErr := dockerSrc.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close docker source: %w", closeErr))
			}
		}()
	}

	st, err := newStore(ctx, a.cfg, a.logger)
	if err != nil {
		return err
	}
	if checker, ok := st.(store.Checker); ok {
		if err := checker.Check(ctx); err != nil {
			_ = st.Close()
			return err
		}
	}

	scopes := make([]string, 0, len(sources))
	for _, src := range sources {
		scopes = append(scopes, src.Scope())
	}
	a.logger.Info("collector ready",
		"host", a.cfg.Host,
		"data_dir", a.cfg.DataDir,
		"chunk_interval", a.cfg.Storage.ChunkInterval.String(),
		"sources", scopes,
	)
	warnFreeSpace(a.logger, a.cfg.DataDir)

	if dryRun {
		a.logger.Info("configuration is valid; no data written")
		return st.Close()
	}

	if dockerSrc != nil {
		dockerSrc.Start(ctx)
	}

	c := collector.New(a.cfg, sources, st, a.logger)
	if once {
		return c.RunOnce(ctx)
	}

	// Drain anything staged by a previous failed upload while collection runs.
	// The replayer shares the command context, so a signal stops it promptly;
	// chunks written by the final flush stay in the spool for the next start.
	// Run closes the store, but S3 Close is a no-op; replay is canceled and
	// waited for as soon as Run returns, including the early-error path.
	if replayer, ok := st.(store.Replayer); ok {
		replayCtx, stopReplay := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			replayer.Replay(replayCtx)
		}()

		err = c.Run(ctx)
		stopReplay()
		<-done
		return err
	}
	return c.Run(ctx)
}

// newSources constructs every enabled source. It returns the Docker source
// separately because it owns a client and background goroutines: the caller
// must Start it before the collector and Close it afterwards. A disabled source
// is never constructed, so its socket is never touched.
//
// once forces one-shot container stats. A single snapshot has no stream
// history, so stream_stats cannot produce samples for collect --once.
func (a *app) newSources(ctx context.Context, once bool) ([]collector.Source, *docker.Source, error) {
	var sources []collector.Source
	if a.cfg.Sources.System.Enabled {
		src, err := system.New(a.cfg.ProcPath, a.cfg.Sources.System.Interval.Std(), a.logger)
		if err != nil {
			return nil, nil, err
		}
		sources = append(sources, src)
	}
	if a.cfg.Sources.Process.Enabled {
		src, err := process.New(process.Options{
			ProcPath:       a.cfg.ProcPath,
			Filter:         a.cfg.Sources.Process.Filter,
			TopN:           a.cfg.Sources.Process.TopN,
			Interval:       a.cfg.Sources.Process.Interval.Std(),
			RescanInterval: a.cfg.Sources.Process.RescanInterval.Std(),
		}, a.logger)
		if err != nil {
			return nil, nil, err
		}
		sources = append(sources, src)
	}

	var dockerSrc *docker.Source
	if a.cfg.Sources.Docker.Enabled {
		streamStats := a.cfg.Sources.Docker.StreamStats
		if once && streamStats {
			a.logger.Info("collect --once: using one-shot container stats because stream_stats needs a running stream")
			streamStats = false
		}
		src, err := docker.New(ctx, docker.Options{
			Socket:         a.cfg.Sources.Docker.Socket,
			Exclude:        a.cfg.Sources.Docker.Exclude,
			Interval:       a.cfg.Sources.Docker.Interval.Std(),
			RescanInterval: a.cfg.Sources.Docker.RescanInterval.Std(),
			StreamStats:    streamStats,
		}, a.logger)
		if err != nil {
			return nil, nil, err
		}
		dockerSrc = src
		sources = append(sources, src)
	}
	return sources, dockerSrc, nil
}

// warnFreeSpace logs a warning when the filesystem under dataDir is low on
// space. It never fails startup: collection writes eventually report the real
// error.
func warnFreeSpace(logger *slog.Logger, dataDir string) {
	free, err := freeBytes(dataDir)
	if err != nil {
		logger.Debug("free space check failed", "data_dir", dataDir, "error", err)
		return
	}
	if free < lowDiskBytes {
		logger.Warn("low disk space", "data_dir", dataDir, "free_bytes", free)
	}
}

// freeBytes reports free bytes on the filesystem containing path, walking up to
// the nearest existing ancestor when the data directory does not exist yet.
func freeBytes(path string) (uint64, error) {
	for {
		var stat syscall.Statfs_t
		err := syscall.Statfs(path, &stat)
		if err == nil {
			return uint64(stat.Bavail) * uint64(stat.Bsize), nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return 0, err
		}
		parent := filepath.Dir(path)
		if parent == path {
			return 0, fmt.Errorf("statfs %s: %w", path, err)
		}
		path = parent
	}
}
