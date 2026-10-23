// Command smob collects CPU, memory, and network metrics on Linux and stores
// them on local disk or in S3-compatible object storage.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confuzeus/smob/internal/cli"
)

// Build metadata, set at link time with -ldflags -X.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	root := newRootCommand(buildInfo{version: version, commit: commit, date: date})
	root.SetOut(os.Stdout)
	root.SetErr(os.Stderr)

	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "smob: %v\n", err)
		os.Exit(cli.ExitCode(err))
	}
}
