package main

import (
	"io"
	"log/slog"
	"strings"
	"time"

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
	dataDir    string
	logLevel   string
	logFormat  string

	cfg    config.Config
	logger *slog.Logger
}

func newRootCommand(info buildInfo) *cobra.Command {
	a := &app{info: info}

	root := &cobra.Command{
		Use:   "smob",
		Short: "Simple monitoring observability tool",
		Long:  "Record CPU, memory, and network usage over time on Linux and store it on local disk or in S3-compatible object storage.",
		// Cobra's version template only sees .Version, so it holds the full
		// banner built by buildInfo.String, the single format definition.
		Version:       info.String(),
		Args:          noArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	root.CompletionOptions.DisableDefaultCmd = true
	root.SetVersionTemplate("{{.Version}}\n")
	// Without RunE cobra treats the root as not runnable and prints help before
	// validating args, so an unknown subcommand would exit 0. RunE keeps args
	// validation in play and preserves plain `smob` printing help.
	root.RunE = func(cmd *cobra.Command, _ []string) error {
		return cmd.Help()
	}
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return cli.UsageErrorf("%v", err)
	})
	root.SetHelpCommand(helpCommand())
	root.PersistentFlags().StringVar(&a.configPath, "config", config.DefaultPath, "path to the YAML configuration file")
	root.PersistentFlags().StringVar(&a.logLevel, "log-level", "", "log level override: debug, info, warn, error")
	root.PersistentFlags().StringVar(&a.logFormat, "log-format", "", "log format override: text, json")
	// Register --version explicitly so cobra does not add a -v shorthand;
	// -v is reserved for a future verbosity flag. Cobra evaluates this flag
	// before config loading and before any RunE.
	root.Flags().Bool("version", false, "print version, commit, and build date and exit")

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

// noArgs rejects positional arguments as a usage error. It mirrors cobra.NoArgs
// but tags the error, because cobra reports argument rejections as plain errors
// and cli.ExitCode would map them to 1. Used by every leaf command and by the
// root, where a positional argument is an unknown subcommand.
func noArgs(cmd *cobra.Command, args []string) error {
	if err := cobra.NoArgs(cmd, args); err != nil {
		return cli.UsageErrorf("%v", err)
	}
	return nil
}

// helpCommand replaces cobra's default help command so an unknown topic is a
// usage error on stderr instead of a stdout message with exit 0. A known topic
// still prints that command's help to stdout and exits 0.
func helpCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "help [command]",
		Short: "Help about any command",
		Long: `Help provides help for any command in the application.
Simply type smob help [path to command] for full details.`,
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			target, rest, err := cmd.Root().Find(args)
			if err != nil || len(rest) > 0 {
				return cli.UsageErrorf("unknown help topic %q", strings.Join(args, " "))
			}
			// Cobra's default help command initializes these flags before
			// rendering, so the help text matches `smob <cmd> --help`.
			target.InitDefaultHelpFlag()
			target.InitDefaultVersionFlag()
			return target.Help()
		},
	}
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
	// --data-dir is local to collect, query, and stats; config and version do
	// not define it, so Changed on an absent flag is meaningless.
	if flag := cmd.Flags().Lookup("data-dir"); flag != nil && flag.Changed {
		cfg.DataDir = a.dataDir
	}
	cfg.Resolve()
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

	opts := &slog.HandlerOptions{
		Level: level,
		// All stored data is UTC, so log timestamps are too. Without this the
		// handlers render the process's local zone and a log cannot be lined up
		// with chunk keys without converting first.
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key != slog.TimeKey || len(groups) != 0 {
				return a
			}
			if t, ok := a.Value.Any().(time.Time); ok {
				return slog.String(slog.TimeKey, t.UTC().Format(time.RFC3339Nano))
			}
			return a
		},
	}
	if cfg.LogFormat == "json" {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}
