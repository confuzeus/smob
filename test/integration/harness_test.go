//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

type result struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

func runSmob(t *testing.T, ctx context.Context, args ...string) result {
	t.Helper()
	cmd := exec.CommandContext(ctx, smobBin, args...)
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

// writeConfig writes body to a fresh smob.yaml and returns its path.
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "smob.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// validConfig writes the smallest configuration that passes validation and
// returns its path.
func validConfig(t *testing.T) string {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	return writeConfig(t, "data_dir: "+dataDir+"\n")
}

func hasLine(out, line string) bool {
	for _, got := range bytes.Split([]byte(out), []byte("\n")) {
		if string(bytes.TrimSpace(got)) == line {
			return true
		}
	}
	return false
}

func updateGolden() bool {
	return os.Getenv("SMOB_TEST_UPDATE") == "1"
}

func compareGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join(moduleRoot(), "test", "integration", "testdata", "golden", name)

	if updateGolden() {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create golden dir: %v", err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatalf("write golden %s: %v", name, err)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run make test-update)", name, err)
	}
	if got != string(want) {
		t.Errorf("%s does not match its golden file; run make test-update and review the diff\ngot:\n%s", name, got)
	}
}
