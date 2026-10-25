package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"syscall"
	"time"

	"github.com/confuzeus/smob/internal/cli"
	"github.com/confuzeus/smob/internal/collector"
	"github.com/confuzeus/smob/internal/config"
	"github.com/confuzeus/smob/internal/source/docker"
	"github.com/confuzeus/smob/internal/source/process"
	"github.com/confuzeus/smob/internal/source/system"
	"github.com/confuzeus/smob/internal/store"
	"github.com/confuzeus/smob/internal/store/chunk"
	"github.com/spf13/cobra"
)

const (
	// lowDiskBytes is the free-space threshold for the startup warning.
	lowDiskBytes = 1 << 30
	// sweepInterval is the retention cadence after the startup pass. The tick
	// invokes the same store.Sweeper.Sweep as startup; only the schedule differs.
	sweepInterval = time.Hour
)

func (a *app) collectCommand() *cobra.Command {
	var dryRun, once bool

	cmd := &cobra.Command{
		Use:   "collect",
		Short: "Collect metrics and write them to the configured store",
		Args:  noArgs,
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
	// A source scope becomes a chunk key component, so reject one that cannot
	// be a path component before the store is opened or any sample is written.
	for _, src := range sources {
		if err := chunk.ValidateComponent("scope", src.Scope()); err != nil {
			return fmt.Errorf("source %s: %w", src.Scope(), err)
		}
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

	// Retention runs after Check has made the root usable and before the
	// free-space warning, so the warning reflects space reclaimed at startup.
	// A zero limit disables retention and avoids a needless tree scan. Dry-run
	// must not delete anything.
	retention := store.Retention{
		MaxAge:   a.cfg.Retention.Disk.MaxAge.Std(),
		MaxBytes: a.cfg.Retention.Disk.MaxBytes,
	}
	var sweeper store.Sweeper
	if !dryRun && (retention.MaxAge > 0 || retention.MaxBytes > 0) {
		if s, ok := st.(store.Sweeper); ok {
			sweeper = s
		}
	}
	if sweeper != nil {
		sweeper.Sweep(ctx, retention)
	}

	// Trim a pre-existing over-cap spool before any source can write, because
	// Write spools on failure and an initial trim in the replayer goroutine
	// could race the chunk that write just staged. Dry-run deletes nothing.
	replayer, replayable := st.(store.Replayer)
	if replayable && !dryRun {
		replayer.TrimSpool(ctx)
	}

	a.logger.Info("collector ready", startupAttrs(a.cfg, sources, retention)...)
	warnFreeSpace(a.logger, a.cfg.DataDir, dryRun)

	if dryRun {
		a.logDryRunProbes(sources, retention)
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

	// Sweep hourly while collection runs. The startup pass above already ran;
	// this loop only handles later ticks. Canceling and joining it on every
	// return path keeps the goroutine and ticker from leaking and stops a pass
	// promptly when the process is interrupted.
	if sweeper != nil {
		sweepCtx, stopSweep := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			ticker := time.NewTicker(sweepInterval)
			defer ticker.Stop()
			for {
				select {
				case <-sweepCtx.Done():
					return
				case <-ticker.C:
					sweeper.Sweep(sweepCtx, retention)
				}
			}
		}()
		defer func() {
			stopSweep()
			<-done
		}()
	}

	// Drain anything staged by a previous failed upload while collection runs.
	// The replayer shares the command context, so a signal stops it promptly;
	// chunks written by the final flush stay in the spool for the next start.
	// Run closes the store, but S3 Close is a no-op; replay is canceled and
	// waited for as soon as Run returns, including the early-error path.
	if replayable {
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

// keyRoot returns the root of the chunk key space: the data directory for the
// disk backend, or s3://bucket/prefix for S3. An operator needs it to locate a
// chunk named in a later log line.
func keyRoot(cfg config.Config) string {
	if cfg.Storage.Primary != "s3" {
		return cfg.DataDir
	}
	root := "s3://" + cfg.Storage.S3.Bucket
	if cfg.Storage.S3.Prefix != "" {
		root += "/" + cfg.Storage.S3.Prefix
	}
	return root
}

// startupAttrs returns the effective configuration logged once when the
// collector is ready. Credentials never enter the config, and validation has
// already rejected an endpoint with userinfo, so these values are safe to log.
func startupAttrs(cfg config.Config, sources []collector.Source, retention store.Retention) []any {
	scopes := make([]string, 0, len(sources))
	attrs := []any{
		"host", cfg.Host,
		"data_dir", cfg.DataDir,
		"storage", cfg.Storage.Primary,
		"key_root", keyRoot(cfg),
		"chunk_interval", cfg.Storage.ChunkInterval.String(),
		"retention", slog.GroupValue(
			slog.String("max_age", retention.MaxAge.String()),
			slog.Int64("max_bytes", retention.MaxBytes),
		),
	}
	for _, src := range sources {
		scopes = append(scopes, src.Scope())
		attrs = append(attrs, src.Scope()+"_interval", src.Interval().String())
	}
	attrs = append(attrs, "sources", scopes)
	if cfg.Storage.Primary == "s3" {
		attrs = append(attrs,
			"bucket", cfg.Storage.S3.Bucket,
			"prefix", cfg.Storage.S3.Prefix,
			"endpoint", cfg.Storage.S3.Endpoint,
			"spool", slog.GroupValue(
				slog.String("dir", cfg.Storage.S3.SpoolDir),
				slog.Int64("max_bytes", cfg.Storage.S3.SpoolMaxBytes),
			),
		)
	}
	return attrs
}

// logDryRunProbes reports every check a dry run performed, so an operator can
// see what the verdict covers instead of only the final line.
func (a *app) logDryRunProbes(sources []collector.Source, retention store.Retention) {
	for _, src := range sources {
		a.logger.Info("dry-run: source probe passed",
			"scope", src.Scope(), "interval", src.Interval().String())
	}
	a.logger.Info("dry-run: store check passed",
		"storage", a.cfg.Storage.Primary, "key_root", keyRoot(a.cfg))
	if retention.MaxAge > 0 || retention.MaxBytes > 0 {
		a.logger.Info("dry-run: retention skipped",
			"max_age", retention.MaxAge.String(), "max_bytes", retention.MaxBytes)
		return
	}
	a.logger.Info("dry-run: retention skipped", "reason", "disabled")
}

// warnFreeSpace logs a warning when the filesystem under dataDir is low on
// space, and in dry-run also logs the probe result itself. It never fails
// startup: collection writes eventually report the real error.
func warnFreeSpace(logger *slog.Logger, dataDir string, dryRun bool) {
	free, err := freeBytes(dataDir)
	if err != nil {
		logger.Debug("free space check failed", "data_dir", dataDir, "error", err)
		if dryRun {
			logger.Info("dry-run: free space check failed", "data_dir", dataDir, "error", err)
		}
		return
	}
	if dryRun {
		logger.Info("dry-run: free space checked", "data_dir", dataDir, "free_bytes", free)
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
