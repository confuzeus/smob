//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestS3InvalidCredentialsFailLoudly proves that credentials the endpoint
// rejects at startup are a persistent store failure: collect exits 3, names the
// failure, and stages nothing in the spool that a later replay could never
// upload.
func TestS3InvalidCredentialsFailLoudly(t *testing.T) {
	env := requireS3(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dataDir := filepath.Join(t.TempDir(), "data")
	spoolDir := filepath.Join(dataDir, "spool")
	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
proc_path: /proc
sources:
  system:
    enabled: true
    interval: 100ms
storage:
  primary: s3
  chunk_interval: 1s
  s3:
    endpoint: %s
    bucket: %s
    prefix: %s
    spool_dir: %s
`, dataDir, env.endpoint, env.bucket, env.prefix, spoolDir))

	creds := replaceEnv(os.Environ(),
		"AWS_ACCESS_KEY_ID=smob-invalid-access-key",
		"AWS_SECRET_ACCESS_KEY=smob-invalid-secret-key",
	)
	res := runSmobEnv(t, ctx, creds, "collect", "--once", "--config", cfg)
	if res.ExitCode != 3 {
		t.Fatalf("exit = %d, want 3\nstdout:\n%s\nstderr:\n%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	for _, want := range []string{"s3://" + env.bucket, "persistent store failure"} {
		if !strings.Contains(res.Stderr, want) {
			t.Errorf("stderr does not contain %q:\n%s", want, res.Stderr)
		}
	}
	if got := spoolChunkPaths(t, spoolDir); len(got) != 0 {
		t.Fatalf("spool holds %d chunk(s) after a terminal credential failure: %v", len(got), got)
	}
}

// TestS3TerminalUploadFailsLoudly drives the upload path with credentials that
// can reach and read the bucket: the key exceeds the endpoint's limit, so the
// startup check passes and the first PUT is rejected with 400 KeyTooLongError.
// A terminal rejection must fail with exit 3 instead of spooling a chunk that
// every later replay would reject again.
func TestS3TerminalUploadFailsLoudly(t *testing.T) {
	env := requireS3(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dataDir := filepath.Join(t.TempDir(), "data")
	spoolDir := filepath.Join(dataDir, "spool")
	prefix := strings.Repeat("p", 1200)
	cfg := writeConfig(t, fmt.Sprintf(`data_dir: %s
host: test-host
proc_path: /proc
sources:
  system:
    enabled: true
    interval: 100ms
storage:
  primary: s3
  chunk_interval: 1s
  s3:
    endpoint: %s
    bucket: %s
    prefix: %s
    spool_dir: %s
`, dataDir, env.endpoint, env.bucket, prefix, spoolDir))

	res := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if res.ExitCode != 3 {
		t.Fatalf("exit = %d, want 3\nstdout:\n%s\nstderr:\n%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	for _, want := range []string{"persistent store failure", "terminal upload error"} {
		if !strings.Contains(res.Stderr, want) {
			t.Errorf("stderr does not contain %q:\n%s", want, res.Stderr)
		}
	}
	if got := spoolChunkPaths(t, spoolDir); len(got) != 0 {
		t.Fatalf("spool holds %d chunk(s) after a terminal upload failure: %v", len(got), got)
	}
}

// replaceEnv returns environ with each "NAME=value" override applied once,
// dropping any earlier value for that name.
func replaceEnv(environ []string, overrides ...string) []string {
	drop := make(map[string]struct{}, len(overrides))
	for _, override := range overrides {
		name, _, _ := strings.Cut(override, "=")
		drop[name] = struct{}{}
	}
	env := make([]string, 0, len(environ)+len(overrides))
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		if _, ok := drop[name]; ok {
			continue
		}
		env = append(env, kv)
	}
	return append(env, overrides...)
}

// runSmobEnv runs the binary with an explicit environment. It mirrors runSmob
// so a test can control the credential chain without mutating the test process.
func runSmobEnv(t *testing.T, ctx context.Context, env []string, args ...string) result {
	t.Helper()
	cmd := exec.CommandContext(ctx, smobBin, args...)
	cmd.Env = env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	err := cmd.Run()
	res := result{Stdout: stdout.String(), Stderr: stderr.String()}
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("run smob %v: %v", args, err)
		}
		res.ExitCode = exitErr.ExitCode()
	}
	return res
}
