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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigs)

	go func() {
		if code := awaitShutdown(sigs, cancel); code != 0 {
			fmt.Fprintln(os.Stderr, "smob: second signal, exiting without flushing")
			os.Exit(code)
		}
	}()

	root := newRootCommand(buildInfo{version: version, commit: commit, date: date})
	root.SetOut(os.Stdout)
	root.SetErr(os.Stderr)

	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "smob: %v\n", err)
		os.Exit(cli.ExitCode(err))
	}
}

// awaitShutdown applies the process signal contract. The first signal cancels
// the command context so collect can flush its active chunks. A second signal
// returns 1, and main exits without waiting for that flush.
func awaitShutdown(sigs <-chan os.Signal, cancel context.CancelFunc) int {
	<-sigs
	cancel()
	<-sigs
	return 1
}
