package main

import (
	"io"
	"log/slog"

	"github.com/confuzeus/smob/internal/cli"
	"github.com/confuzeus/smob/internal/config"
	"github.com/spf13/cobra"
)

// buildInfo carries the metadata injected at link time.
type buildInfo struct {
	version string
	commit  string
	date    string
}

// app holds the state shared by subcommands for one invocation.
type app struct {
	info       buildInfo
	configPath string
	logLevel   string
	logFormat  string

	cfg    config.Config
	logger *slog.Logger
}

func newRootCommand(info buildInfo) *cobra.Command {
	a := &app{info: info}

	root := &cobra.Command{
		Use:           "smob",
		Short:         "Simple monitoring observability tool",
		Long:          "Record CPU, memory, and network usage over time on Linux and store it on local disk or in S3-compatible object storage.",
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	root.CompletionOptions.DisableDefaultCmd = true
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return cli.UsageErrorf("%v", err)
	})
	root.PersistentFlags().StringVar(&a.configPath, "config", config.DefaultPath, "path to the YAML configuration file")
	root.PersistentFlags().StringVar(&a.logLevel, "log-level", "", "log level override: debug, info, warn, error")
	root.PersistentFlags().StringVar(&a.logFormat, "log-format", "", "log format override: text, json")

	root.AddCommand(
		a.collectCommand(),
		a.queryCommand(),
		a.tailCommand(),
		a.statsCommand(),
		a.configCommand(),
		a.versionCommand(),
	)
	return root
}

// load resolves the configuration for cmd: file defaults, then the file, then
// flag overrides, then validation. On success the effective config and logger
// are available to the command.
func (a *app) load(cmd *cobra.Command) error {
	cfg, err := config.Load(a.configPath)
	if err != nil {
		return cli.UsageErrorf("%v", err)
	}
	if cmd.Flags().Changed("log-level") {
		cfg.LogLevel = a.logLevel
	}
	if cmd.Flags().Changed("log-format") {
		cfg.LogFormat = a.logFormat
	}
	if err := cfg.Validate(); err != nil {
		return cli.UsageErrorf("%v", err)
	}

	a.cfg = cfg
	a.logger = newLogger(cmd.ErrOrStderr(), cfg)
	a.logger.Debug("loaded configuration", "path", a.configPath)
	return nil
}

func newLogger(w io.Writer, cfg config.Config) *slog.Logger {
	level := slog.LevelInfo
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	opts := &slog.HandlerOptions{Level: level}
	if cfg.LogFormat == "json" {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}
