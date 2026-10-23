//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	res := runSmob(t, ctx, "version")
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	const want = "smob test-version (commit test-commit, built test-date)\n"
	if res.Stdout != want {
		t.Fatalf("stdout = %q, want %q", res.Stdout, want)
	}
}

func TestVersionIgnoresConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	missing := filepath.Join(t.TempDir(), "absent.yaml")
	res := runSmob(t, ctx, "version", "--config", missing)
	if res.ExitCode != 0 {
		t.Fatalf("version must not read the config: exit %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
}

func TestJSONLogging(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	path := validConfig(t)
	res := runSmob(t, ctx, "config", "--validate", "--config", path, "--log-level", "debug", "--log-format", "json")
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	var entry struct {
		Level string `json:"level"`
		Msg   string `json:"msg"`
		Path  string `json:"path"`
	}
	if err := json.Unmarshal([]byte(res.Stderr), &entry); err != nil {
		t.Fatalf("stderr is not a JSON log line: %v\nstderr:\n%s", err, res.Stderr)
	}
	if entry.Level != "DEBUG" || entry.Msg != "loaded configuration" || entry.Path != path {
		t.Fatalf("log entry = %+v, want level DEBUG, msg \"loaded configuration\", path %q", entry, path)
	}
}

func TestHelpGolden(t *testing.T) {
	commands := []struct {
		name string
		args []string
	}{
		{"root", nil},
		{"collect", []string{"collect"}},
		{"query", []string{"query"}},
		{"tail", []string{"tail"}},
		{"stats", []string{"stats"}},
		{"config", []string{"config"}},
		{"version", []string{"version"}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	for _, tc := range commands {
		t.Run(tc.name, func(t *testing.T) {
			args := append(append([]string{}, tc.args...), "--help")
			res := runSmob(t, ctx, args...)
			if res.ExitCode != 0 {
				t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
			}
			compareGolden(t, "help_"+tc.name+".txt", res.Stdout)
		})
	}
}
