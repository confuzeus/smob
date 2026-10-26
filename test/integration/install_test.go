//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// installVersion is the tag the fake release tree is published under. The
// installed binary reports TestMain's ldflags version, not this tag, because the
// installer trusts the release checksum rather than the banner; the release
// workflow's smoke step asserts the tag against the real artifacts.
const installVersion = "v0.0.0"

// installTestArch reports the architecture whose release asset the installer
// would select on this host.
func installTestArch(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skipf("the installer targets Linux; this host is %s", runtime.GOOS)
	}
	switch runtime.GOARCH {
	case "amd64", "arm64":
		return runtime.GOARCH
	default:
		t.Skipf("releases cover linux/amd64 and linux/arm64; this host is linux/%s", runtime.GOARCH)
	}
	return ""
}

func requireInstallTools(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("curl"); err != nil {
		reason := fmt.Sprintf("curl is unavailable (%v); the installer downloads release assets with it", err)
		if os.Getenv("SMOB_TEST_STRICT") == "1" {
			t.Fatal(reason)
		}
		t.Skip(reason)
	}
}

// installRelease lays out the release directory the installer downloads from:
// <base>/<version>/smob-linux-<arch> and SHA256SUMS. It returns the base and the
// asset path so a test can tamper with the binary after the checksums are
// written.
func installRelease(t *testing.T, asset string) (base, assetPath string) {
	t.Helper()
	content, err := os.ReadFile(smobBin)
	if err != nil {
		t.Fatalf("read built smob: %v", err)
	}

	base = t.TempDir()
	versionDir := filepath.Join(base, installVersion)
	if err := os.MkdirAll(versionDir, 0o755); err != nil {
		t.Fatalf("create release directory: %v", err)
	}
	assetPath = filepath.Join(versionDir, asset)
	if err := os.WriteFile(assetPath, content, 0o755); err != nil {
		t.Fatalf("write release asset: %v", err)
	}
	if err := os.WriteFile(filepath.Join(versionDir, "SHA256SUMS"), installChecksums(t, asset, content), 0o644); err != nil {
		t.Fatalf("write SHA256SUMS: %v", err)
	}
	return base, assetPath
}

// installChecksums mirrors the release's two-entry SHA256SUMS: the host's asset
// plus the other architecture, which the installer must ignore.
func installChecksums(t *testing.T, asset string, content []byte) []byte {
	t.Helper()
	sum := sha256.Sum256(content)
	other := "arm64"
	if strings.HasSuffix(asset, "-arm64") {
		other = "amd64"
	}
	return []byte(fmt.Sprintf("%s  %s\n%s  smob-linux-%s\n",
		hex.EncodeToString(sum[:]), asset, strings.Repeat("0", 64), other))
}

// installEnv drops ambient installer overrides so the test controls the
// download source, then points the installer at the fake release tree.
func installEnv(base string) []string {
	drop := []string{
		"SMOB_VERSION=",
		"SMOB_INSTALL_DIR=",
		"SMOB_NO_SERVICE=",
		"SMOB_RELEASES_BASE_URL=",
	}
	env := make([]string, 0, len(os.Environ())+1)
	for _, kv := range os.Environ() {
		skip := false
		for _, prefix := range drop {
			if strings.HasPrefix(kv, prefix) {
				skip = true
				break
			}
		}
		if !skip {
			env = append(env, kv)
		}
	}
	return append(env, "SMOB_RELEASES_BASE_URL="+fileURL(base))
}

func fileURL(path string) string {
	return (&url.URL{Scheme: "file", Path: path}).String()
}

func TestInstallScriptInstallsBinary(t *testing.T) {
	requireInstallTools(t)
	asset := "smob-linux-" + installTestArch(t)
	script := filepath.Join(moduleRoot(), "install.sh")
	base, _ := installRelease(t, asset)
	installDir := filepath.Join(t.TempDir(), "bin")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", script,
		"--version", installVersion,
		"--no-service",
		"--install-dir", installDir,
	)
	cmd.Env = installEnv(base)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install.sh failed: %v\n%s", err, out)
	}

	installed := filepath.Join(installDir, "smob")
	got, err := os.ReadFile(installed)
	if err != nil {
		t.Fatalf("read installed binary: %v\ninstaller output:\n%s", err, out)
	}
	if want, err := os.ReadFile(smobBin); err != nil {
		t.Fatalf("read built smob: %v", err)
	} else if !bytes.Equal(got, want) {
		t.Fatal("installed binary differs from the release asset")
	}
	info, err := os.Stat(installed)
	if err != nil {
		t.Fatalf("stat installed binary: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("installed binary is not executable: mode %o", info.Mode().Perm())
	}

	banner, err := exec.CommandContext(ctx, installed, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("installed binary failed to run: %v\n%s", err, banner)
	}
	if !strings.Contains(string(banner), "test-version") {
		t.Fatalf("installed binary did not report TestMain's build metadata: %s", banner)
	}
}

func TestInstallScriptRejectsChecksumMismatch(t *testing.T) {
	requireInstallTools(t)
	asset := "smob-linux-" + installTestArch(t)
	script := filepath.Join(moduleRoot(), "install.sh")
	base, assetPath := installRelease(t, asset)

	f, err := os.OpenFile(assetPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open release asset: %v", err)
	}
	if _, err := f.WriteString("tampered"); err != nil {
		_ = f.Close()
		t.Fatalf("tamper with release asset: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close release asset: %v", err)
	}

	installDir := filepath.Join(t.TempDir(), "bin")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", script,
		"--version", installVersion,
		"--no-service",
		"--install-dir", installDir,
	)
	cmd.Env = installEnv(base)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("install.sh succeeded with a modified asset\n%s", out)
	}
	if !strings.Contains(string(out), "checksum mismatch") {
		t.Fatalf("installer did not report the checksum mismatch:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(installDir, "smob")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("binary was installed despite the checksum mismatch: %v", err)
	}
}

func TestInstallScriptFailsWhenAssetIsMissing(t *testing.T) {
	requireInstallTools(t)
	asset := "smob-linux-" + installTestArch(t)
	script := filepath.Join(moduleRoot(), "install.sh")
	base := t.TempDir()
	if err := os.MkdirAll(filepath.Join(base, installVersion), 0o755); err != nil {
		t.Fatalf("create release directory: %v", err)
	}

	installDir := filepath.Join(t.TempDir(), "bin")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", script,
		"--version", installVersion,
		"--no-service",
		"--install-dir", installDir,
	)
	cmd.Env = installEnv(base)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("install.sh succeeded without the %s asset\n%s", asset, out)
	}
	if !strings.Contains(string(out), "download failed") {
		t.Fatalf("installer did not report the failed download:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(installDir, "smob")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("binary was installed without a release asset: %v", err)
	}
}

func TestInstallScriptRequiresRoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; the non-root refusal cannot be exercised")
	}
	script := filepath.Join(moduleRoot(), "install.sh")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", script, "--no-service")
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("install.sh succeeded without root and without an explicit install directory\n%s", out)
	}
	if !strings.Contains(string(out), "requires root") {
		t.Fatalf("installer did not explain that root is required:\n%s", out)
	}
}
