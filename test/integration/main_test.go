//go:build integration

package integration

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

var smobBin string

func TestMain(m *testing.M) {
	root := moduleRoot()
	dir, err := os.MkdirTemp("", "smob-it-*")
	if err != nil {
		panic(err)
	}
	smobBin = filepath.Join(dir, "smob")

	args := []string{
		"build",
		"-o", smobBin,
		"-ldflags", "-X main.version=test-version -X main.commit=test-commit -X main.date=test-date",
	}
	if os.Getenv("SMOB_TEST_RACE") == "1" {
		args = append(args, "-race")
	}
	args = append(args, "./cmd/smob")

	cmd := exec.Command("go", args...)
	cmd.Dir = root
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "build smob: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func moduleRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			panic("go.mod not found above test directory")
		}
		dir = parent
	}
}
