// Package cli holds helpers shared by smob's subcommands.
package cli

import (
	"errors"
	"fmt"
)

// ErrUsage marks an error caused by invalid input, flags, or configuration.
// The process exits with code 2 for these and code 1 for runtime failures.
var ErrUsage = errors.New("usage")

// ErrStore marks a persistent store failure, including a full disk. The process
// exits with code 3 for these.
var ErrStore = errors.New("store")

// ExitCode maps an error to the process exit code.
func ExitCode(err error) int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, ErrUsage):
		return 2
	case errors.Is(err, ErrStore):
		return 3
	default:
		return 1
	}
}

// UsageErrorf wraps a formatted message so ExitCode reports it as a usage
// failure.
func UsageErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrUsage, fmt.Sprintf(format, args...))
}
