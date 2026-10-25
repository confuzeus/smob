//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// goldenDir is the snapshot directory for stable CLI output.
func goldenDir() string {
	return filepath.Join(moduleRoot(), "test", "integration", "testdata", "golden")
}

// goldenFile names one snapshot.
func goldenFile(name string) string {
	return filepath.Join(goldenDir(), name)
}

func TestVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// TestMain injects fixed ldflags, so this golden never depends on the
	// developer's git state.
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"subcommand", []string{"version"}},
		{"flag", []string{"--version"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := runSmob(t, ctx, tc.args...)
			if res.ExitCode != 0 {
				t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
			}
			// Version is a pure stdout command; anything on stderr means a log
			// line or a cobra diagnostic leaked into the wrong stream.
			if res.Stderr != "" {
				t.Fatalf("version wrote to stderr:\n%s", res.Stderr)
			}
			compareGolden(t, "version.txt", res.Stdout)
		})
	}
}

func TestVersionIgnoresConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	missing := filepath.Join(t.TempDir(), "absent.yaml")
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"subcommand", []string{"version", "--config", missing}},
		{"flag", []string{"--version", "--config", missing}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := runSmob(t, ctx, tc.args...)
			if res.ExitCode != 0 {
				t.Fatalf("version must not read the config: exit %d\nstderr:\n%s", res.ExitCode, res.Stderr)
			}
		})
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

// TestHelpGolden snapshots --help for the root and every advertised subcommand.
// The command list comes from cobra's own "Available Commands:" block, which is
// rendered from root.Commands(), so a newly registered subcommand cannot escape
// snapshotted coverage. Cobra's generated completion command is disabled in
// root.go and the help command is ours (root.SetHelpCommand), so help is
// snapshotted too.
func TestHelpGolden(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	for _, name := range advertisedCommands(t, ctx) {
		t.Run(name, func(t *testing.T) {
			res := runSmob(t, ctx, helpArgs(name, true)...)
			if res.ExitCode != 0 {
				t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
			}
			// Help is a pure stdout command; a stray log line or cobra
			// diagnostic on stderr would break scripts that capture it.
			if res.Stderr != "" {
				t.Fatalf("--help wrote to stderr:\n%s", res.Stderr)
			}
			compareGolden(t, "help_"+name+".txt", res.Stdout)
		})
	}
}

// advertisedCommands parses the "Available Commands:" block from `smob --help`
// and returns "root" plus every advertised subcommand, sorted. Cobra renders
// that block from root.Commands(), which is the set this test must cover.
func advertisedCommands(t *testing.T, ctx context.Context) []string {
	t.Helper()

	res := runSmob(t, ctx, "--help")
	if res.ExitCode != 0 {
		t.Fatalf("root --help exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	names := []string{"root"}
	inCommands := false
	for _, line := range strings.Split(res.Stdout, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "Available Commands:" {
			inCommands = true
			continue
		}
		if !inCommands {
			continue
		}
		if trimmed == "" {
			break
		}
		if fields := strings.Fields(trimmed); len(fields) > 0 {
			names = append(names, fields[0])
		}
	}
	if len(names) < 2 {
		t.Fatalf("root --help advertises no subcommands; cannot check help coverage:\n%s", res.Stdout)
	}
	slices.Sort(names)
	return names
}

// helpArgs builds argv for name: the --help flag form (flag true) or the help
// subcommand form. "root" is the binary itself with no subcommand.
func helpArgs(name string, flag bool) []string {
	switch {
	case name == "root" && flag:
		return []string{"--help"}
	case name == "root":
		return []string{"help"}
	case flag:
		return []string{name, "--help"}
	default:
		return []string{"help", name}
	}
}

// TestHelpGoldenCoverage keeps the snapshot set and the command set in step in
// both directions: every advertised command has a help golden, and every help
// golden belongs to an advertised command. A golden left behind by a renamed or
// removed command is an orphan and fails rather than lingering forever.
func TestHelpGoldenCoverage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	commands := advertisedCommands(t, ctx)

	// Literal floor: the parse above is the mechanism, but the documented
	// command set is the contract. A change to help formatting must break this
	// test loudly instead of silently shrinking the covered set.
	for _, want := range []string{"collect", "config", "help", "query", "stats", "tail", "version"} {
		if !slices.Contains(commands, want) {
			t.Errorf("advertised commands %v are missing %q", commands, want)
		}
	}

	entries, err := os.ReadDir(goldenDir())
	if err != nil {
		t.Fatalf("read golden dir: %v", err)
	}

	goldened := make(map[string]bool)
	present := make(map[string]bool)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		present[entry.Name()] = true
		name := entry.Name()
		if !strings.HasPrefix(name, "help_") || !strings.HasSuffix(name, ".txt") {
			continue
		}
		goldened[strings.TrimSuffix(strings.TrimPrefix(name, "help_"), ".txt")] = true
	}

	for _, name := range commands {
		if goldened[name] {
			continue
		}
		// Update mode is about to write the missing snapshot; a stale golden is
		// still an orphan and is checked below.
		if updateGolden() {
			continue
		}
		t.Errorf("command %q has no help golden (want testdata/golden/help_%s.txt); run just test-update and review the diff", name, name)
	}
	for name := range goldened {
		if !slices.Contains(commands, name) {
			t.Errorf("orphan golden testdata/golden/help_%s.txt: no such command is advertised", name)
		}
	}
	// Non-help snapshots are explicit so a renamed subcommand cannot leave a
	// stale file behind unremarked.
	for _, name := range []string{"version.txt", "config_dump.txt"} {
		if !present[name] && !updateGolden() {
			t.Errorf("missing golden testdata/golden/%s", name)
		}
	}
}

// TestUsageErrors run every rejected argument and help topic through the binary.
// A usage error exits 2, writes nothing to stdout, and names the problem on
// stderr; an unknown help topic in particular must not print cobra's default
// stdout notice.
func TestUsageErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for _, tc := range []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{"unknown command", []string{"nonsense"}, "unknown command"},
		{"version extra argument", []string{"version", "extra"}, "unknown command"},
		{"config extra argument", []string{"config", "extra"}, "unknown command"},
		{"tail extra argument", []string{"tail", "extra"}, "unknown command"},
		{"unknown help topic", []string{"help", "nonsense"}, "unknown help topic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := runSmob(t, ctx, tc.args...)
			if res.ExitCode != 2 {
				t.Fatalf("exit = %d, want 2\nstdout:\n%s\nstderr:\n%s", res.ExitCode, res.Stdout, res.Stderr)
			}
			if res.Stdout != "" {
				t.Fatalf("usage error wrote to stdout:\n%s", res.Stdout)
			}
			// A usage error must always explain itself on stderr; an empty
			// stream would leave the user with only an exit code.
			if strings.TrimSpace(res.Stderr) == "" {
				t.Fatal("usage error wrote nothing to stderr")
			}
			if !strings.Contains(res.Stderr, tc.wantStderr) {
				t.Fatalf("stderr does not contain %q:\n%s", tc.wantStderr, res.Stderr)
			}
		})
	}
}

// TestHelpCommand proves `smob help <cmd>` is byte-for-byte the same page as
// `smob <cmd> --help` for every advertised command, and that it matches the
// snapshot rather than merely agreeing with itself.
func TestHelpCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	for _, name := range advertisedCommands(t, ctx) {
		t.Run(name, func(t *testing.T) {
			want := runSmob(t, ctx, helpArgs(name, true)...)
			if want.ExitCode != 0 {
				t.Fatalf("%s --help exit = %d\nstderr:\n%s", name, want.ExitCode, want.Stderr)
			}

			got := runSmob(t, ctx, helpArgs(name, false)...)
			if got.ExitCode != 0 {
				t.Fatalf("help %s exit = %d\nstderr:\n%s", name, got.ExitCode, got.Stderr)
			}
			if got.Stderr != "" {
				t.Fatalf("help %s wrote to stderr:\n%s", name, got.Stderr)
			}
			if got.Stdout != want.Stdout {
				t.Fatalf("help %s differs from --help\nhelp:\n%s\n--help:\n%s", name, got.Stdout, want.Stdout)
			}
			compareGolden(t, "help_"+name+".txt", got.Stdout)
		})
	}
}

// TestTailNotImplemented pins the stub contract: tail reports the missing
// feature before it reads configuration, so a missing config still exits 1 with
// the documented message rather than a config usage error.
func TestTailNotImplemented(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	missing := filepath.Join(t.TempDir(), "absent.yaml")
	res := runSmob(t, ctx, "tail", "--config", missing)
	if res.ExitCode != 1 {
		t.Fatalf("exit = %d, want 1\nstdout:\n%s\nstderr:\n%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "not implemented") {
		t.Fatalf("stderr does not report the stub:\n%s", res.Stderr)
	}
}

// TestMissingGoldenFails proves compareGolden never invents a snapshot in read
// mode: a missing file is a failure that names the remedy, not a silent
// acceptance. compareGolden calls t.Fatalf, so the assertion runs in a child
// process launched from the same test binary.
func TestMissingGoldenFails(t *testing.T) {
	const artifact = "absent_golden.txt"

	if os.Getenv("SMOB_TEST_MISSING_GOLDEN") == "1" {
		compareGolden(t, artifact, "this must not be written\n")
		t.Fatal("compareGolden accepted a missing golden")
	}

	path := goldenFile(artifact)
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("test artifact %s already exists; remove it before running", path)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMissingGoldenFails$")
	cmd.Env = append(os.Environ(), "SMOB_TEST_MISSING_GOLDEN=1", "SMOB_TEST_UPDATE=0")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("child reported success for a missing golden:\n%s", out)
	}
	if !strings.Contains(string(out), artifact) || !strings.Contains(string(out), "test-update") {
		t.Fatalf("failure does not name the golden and the update recipe:\n%s", out)
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("compareGolden created %s in read mode", path)
	}
}

// machineSpecific patterns reject output that depends on the host, the clock,
// or the toolchain rather than on smob. They are matched against every golden.
var machineSpecific = []struct {
	what string
	re   *regexp.Regexp
}{
	{"RFC 3339 timestamp", regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`)},
	{"calendar date", regexp.MustCompile(`\b20\d{2}-\d{2}-\d{2}\b`)},
	{"Go toolchain version", regexp.MustCompile(`\bgo1\.\d+`)},
	{"build target", regexp.MustCompile(`\b(?:linux|darwin|windows)/(?:amd64|arm64|386|arm)\b`)},
	{"temporary path", regexp.MustCompile(`/tmp/`)},
	{"home directory path", regexp.MustCompile(`/(?:home|Users)/[^/\s]+`)},
	{"Go install path", regexp.MustCompile(`/usr/local/go/|GOROOT=`)},
}

// TestGoldensAreMachineIndependent guards the snapshots themselves. A golden
// that embeds a temp dir, the hostname, a date, or a Go build string can never
// match on another machine, and the usual "run just test-update" fix would hide
// the leak rather than remove it.
func TestGoldensAreMachineIndependent(t *testing.T) {
	entries, err := os.ReadDir(goldenDir())
	if err != nil {
		t.Fatalf("read golden dir: %v", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("os.Hostname: %v", err)
	}
	// Words that legitimately appear in smob's own output; a host named one of
	// them says nothing about portability.
	genericHosts := []string{"smob", "test", "localhost", "help", "config", "version", "build"}

	root := moduleRoot()
	checked := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".txt") {
			continue
		}
		checked++
		path := filepath.Join(goldenDir(), entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read golden %s: %v", entry.Name(), err)
		}
		content := string(data)

		if strings.Contains(content, root) {
			t.Errorf("%s embeds the checkout path %q", entry.Name(), root)
		}
		if hostname != "" && len(hostname) >= 4 && !slices.Contains(genericHosts, hostname) &&
			strings.Contains(content, hostname) {
			t.Errorf("%s embeds the machine hostname %q", entry.Name(), hostname)
		}
		for _, pattern := range machineSpecific {
			if loc := pattern.re.FindString(content); loc != "" {
				t.Errorf("%s embeds a machine-specific %s: %q", entry.Name(), pattern.what, loc)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no goldens were checked")
	}
}
