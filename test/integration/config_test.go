//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	base := "data_dir: " + dataDir + "\n"

	cases := []struct {
		name     string
		body     string
		args     []string
		exit     int
		wants    []string
		wantsNot []string
	}{
		{
			name: "minimal",
			body: base,
			exit: 0,
		},
		{
			name:  "unknown field",
			body:  base + "bogus: 1\n",
			exit:  2,
			wants: []string{"bogus"},
		},
		{
			name:  "removed flush interval",
			body:  base + "flush_interval: 30s\n",
			exit:  2,
			wants: []string{"flush_interval"},
		},
		{
			name:  "removed storage disk section",
			body:  base + "storage:\n  disk:\n    enabled: true\n",
			exit:  2,
			wants: []string{"field disk"},
		},
		{
			name:  "removed storage s3 enabled field",
			body:  base + "storage:\n  s3:\n    enabled: true\n",
			exit:  2,
			wants: []string{"field enabled"},
		},
		{
			name:  "removed storage s3 chunk interval",
			body:  base + "storage:\n  s3:\n    chunk_interval: 5m\n",
			exit:  2,
			wants: []string{"chunk_interval"},
		},
		{
			name:  "duplicate key",
			body:  base + base,
			exit:  2,
			wants: []string{"data_dir", "already defined"},
		},
		{
			name:  "second yaml document",
			body:  base + "---\n" + base,
			exit:  2,
			wants: []string{"multiple YAML documents"},
		},
		{
			name:  "invalid duration",
			body:  base + "sources:\n  system:\n    interval: soon\n",
			exit:  2,
			wants: []string{"invalid duration", "line 4"},
		},
		{
			name:  "zero system interval",
			body:  base + "sources:\n  system:\n    interval: 0s\n",
			exit:  2,
			wants: []string{"sources.system.interval"},
		},
		{
			name:  "system interval below one millisecond",
			body:  base + "sources:\n  system:\n    interval: 500us\n",
			exit:  2,
			wants: []string{"sources.system.interval", "at least 1ms"},
		},
		{
			name:  "system interval just below one millisecond",
			body:  base + "sources:\n  system:\n    interval: 999us\n",
			exit:  2,
			wants: []string{"sources.system.interval", "at least 1ms"},
		},
		{
			name: "one millisecond system interval",
			body: base + "sources:\n  system:\n    interval: 1ms\n",
			exit: 0,
		},
		{
			name:  "chunk interval does not divide an hour",
			body:  base + "storage:\n  chunk_interval: 7m\n",
			exit:  2,
			wants: []string{"storage.chunk_interval", "divide one hour evenly"},
		},
		{
			name: "chunk interval divides an hour",
			body: base + "storage:\n  chunk_interval: 15m\n",
			exit: 0,
		},
		{
			name:  "chunk interval below one second",
			body:  base + "storage:\n  chunk_interval: 500ms\n",
			exit:  2,
			wants: []string{"storage.chunk_interval", "whole number of seconds"},
		},
		{
			name:  "chunk interval is a sub-second offset",
			body:  base + "storage:\n  chunk_interval: 1500ms\n",
			exit:  2,
			wants: []string{"storage.chunk_interval", "whole number of seconds"},
		},
		{
			name: "one second chunk interval",
			body: base + "storage:\n  chunk_interval: 1s\n",
			exit: 0,
		},
		{
			name:  "negative retention age",
			body:  base + "retention:\n  disk:\n    max_age: -1s\n",
			exit:  2,
			wants: []string{"retention.disk.max_age", "zero disables the limit"},
		},
		{
			name:  "negative retention size",
			body:  base + "retention:\n  disk:\n    max_bytes: -1\n",
			exit:  2,
			wants: []string{"retention.disk.max_bytes", "zero disables the limit"},
		},
		{
			name: "zero retention limits disable both policies",
			body: base + "retention:\n  disk:\n    max_age: 0s\n    max_bytes: 0\n",
			exit: 0,
		},
		{
			name:  "host is not a path component",
			body:  base + "host: bad/host\n",
			exit:  2,
			wants: []string{"host: must be a single path component"},
		},
		{
			name:  "host contains a newline",
			body:  base + "host: \"a\\nb\"\n",
			exit:  2,
			wants: []string{"host: must be a single path component"},
		},
		{
			name:  "host has leading whitespace",
			body:  base + "host: \" a\"\n",
			exit:  2,
			wants: []string{"host: must be a single path component"},
		},
		{
			name:  "host has trailing whitespace",
			body:  base + "host: \"a \"\n",
			exit:  2,
			wants: []string{"host: must be a single path component"},
		},
		{
			name:  "host is a dot",
			body:  base + "host: \".\"\n",
			exit:  2,
			wants: []string{"host: must be a single path component"},
		},
		{
			name:  "host is a dot dot",
			body:  base + "host: \"..\"\n",
			exit:  2,
			wants: []string{"host: must be a single path component"},
		},
		{
			name:  "host is a slash",
			body:  base + "host: \"/\"\n",
			exit:  2,
			wants: []string{"host: must be a single path component"},
		},
		{
			name:  "host contains NUL",
			body:  base + "host: \"a\\0b\"\n",
			exit:  2,
			wants: []string{"host: must be a single path component"},
		},
		{
			name:  "host contains DEL",
			body:  base + "host: \"a\\x7fb\"\n",
			exit:  2,
			wants: []string{"host: must be a single path component"},
		},
		{
			name:  "host is longer than 255 bytes",
			body:  base + "host: " + strings.Repeat("a", 300) + "\n",
			exit:  2,
			wants: []string{"host: must be a single path component"},
		},
		{
			name: "normal hostname",
			body: base + "host: web-1\n",
			exit: 0,
		},
		{
			name: "s3 primary without bucket",
			body: base + "storage:\n  primary: s3\n",
			exit: 2,
			wants: []string{
				"storage.s3.bucket",
				"storage.s3.region",
			},
		},
		{
			name:  "disk primary without data dir",
			body:  "data_dir: \"\"\nstorage:\n  primary: disk\n",
			exit:  2,
			wants: []string{"data_dir", "storage.primary is disk"},
		},
		{
			name:  "s3 primary without data dir or spool dir",
			body:  "data_dir: \"\"\nstorage:\n  primary: s3\n  s3:\n    bucket: bucket\n    region: us-east-1\n",
			exit:  2,
			wants: []string{"storage.s3.spool_dir"},
		},
		{
			name: "s3 with region",
			body: base + "storage:\n  primary: s3\n  s3:\n    bucket: bucket\n    region: us-east-1\n",
			exit: 0,
		},
		{
			name:     "s3 endpoint replaces region",
			body:     base + "storage:\n  primary: s3\n  s3:\n    bucket: bucket\n    endpoint: http://127.0.0.1:9000\n",
			exit:     0,
			wantsNot: []string{"storage.s3.region"},
		},
		{
			name:  "s3 endpoint is not a URL",
			body:  base + "storage:\n  primary: s3\n  s3:\n    bucket: bucket\n    endpoint: 127.0.0.1:9000\n",
			exit:  2,
			wants: []string{"storage.s3.endpoint"},
		},
		{
			name:  "s3 endpoint with userinfo",
			body:  base + "storage:\n  primary: s3\n  s3:\n    bucket: bucket\n    endpoint: https://user:pass@127.0.0.1:9000\n",
			exit:  2,
			wants: []string{"storage.s3.endpoint", "userinfo"},
		},
		{
			name:  "s3 endpoint with a query",
			body:  base + "storage:\n  primary: s3\n  s3:\n    bucket: bucket\n    endpoint: http://127.0.0.1:9000?region=us-east-1\n",
			exit:  2,
			wants: []string{"storage.s3.endpoint", "query string"},
		},
		{
			name:  "s3 endpoint with a fragment",
			body:  base + "storage:\n  primary: s3\n  s3:\n    bucket: bucket\n    endpoint: http://127.0.0.1:9000/#minio\n",
			exit:  2,
			wants: []string{"storage.s3.endpoint", "fragment"},
		},
		{
			name:  "s3 bucket is whitespace only",
			body:  base + "storage:\n  primary: s3\n  s3:\n    bucket: \"  \"\n    region: us-east-1\n",
			exit:  2,
			wants: []string{"storage.s3.bucket"},
		},
		{
			name:  "s3 bucket contains whitespace",
			body:  base + "storage:\n  primary: s3\n  s3:\n    bucket: \"my bucket\"\n    region: us-east-1\n",
			exit:  2,
			wants: []string{"storage.s3.bucket", "whitespace"},
		},
		{
			name:  "s3 bucket is longer than 255 bytes",
			body:  base + "storage:\n  primary: s3\n  s3:\n    bucket: " + strings.Repeat("b", 256) + "\n    region: us-east-1\n",
			exit:  2,
			wants: []string{"storage.s3.bucket", "255"},
		},
		{
			name:  "s3 region is whitespace only",
			body:  base + "storage:\n  primary: s3\n  s3:\n    bucket: bucket\n    region: \"  \"\n",
			exit:  2,
			wants: []string{"storage.s3.region"},
		},
		{
			name:  "s3 spool dir is whitespace only",
			body:  "data_dir: \"\"\nstorage:\n  primary: s3\n  s3:\n    bucket: bucket\n    region: us-east-1\n    spool_dir: \"  \"\n",
			exit:  2,
			wants: []string{"storage.s3.spool_dir"},
		},
		{
			name:  "s3 prefix with a parent segment",
			body:  base + "storage:\n  primary: s3\n  s3:\n    bucket: bucket\n    region: us-east-1\n    prefix: prod/../other\n",
			exit:  2,
			wants: []string{"storage.s3.prefix", `".."`},
		},
		{
			name:  "s3 prefix with a current segment",
			body:  base + "storage:\n  primary: s3\n  s3:\n    bucket: bucket\n    region: us-east-1\n    prefix: ./prod\n",
			exit:  2,
			wants: []string{"storage.s3.prefix", `"."`},
		},
		{
			name:  "s3 prefix with a repeated slash",
			body:  base + "storage:\n  primary: s3\n  s3:\n    bucket: bucket\n    region: us-east-1\n    prefix: prod//smob\n",
			exit:  2,
			wants: []string{"storage.s3.prefix"},
		},
		{
			name:  "s3 prefix is only a parent segment",
			body:  base + "storage:\n  primary: s3\n  s3:\n    bucket: bucket\n    region: us-east-1\n    prefix: \"..\"\n",
			exit:  2,
			wants: []string{"storage.s3.prefix", `".."`},
		},
		{
			name: "s3 endpoint with a normal prefix",
			body: base + "storage:\n  primary: s3\n  s3:\n    bucket: bucket\n    endpoint: http://127.0.0.1:9000\n    prefix: prod/smob\n",
			exit: 0,
		},
		{
			name:  "zero spool size",
			body:  base + "storage:\n  s3:\n    spool_max_bytes: 0\n",
			exit:  2,
			wants: []string{"storage.s3.spool_max_bytes"},
		},
		{
			name:  "no sources enabled",
			body:  base + "sources:\n  system:\n    enabled: false\n",
			exit:  2,
			wants: []string{"sources: at least one source must be enabled"},
		},
		{
			name: "process source enabled",
			body: base + "sources:\n  process:\n    enabled: true\n",
			exit: 0,
		},
		{
			name:  "process filter is not a regexp",
			body:  base + "sources:\n  process:\n    enabled: true\n    filter: \"[\"\n",
			exit:  2,
			wants: []string{"sources.process.filter"},
		},
		{
			name:  "zero process interval",
			body:  base + "sources:\n  process:\n    enabled: true\n    interval: 0s\n",
			exit:  2,
			wants: []string{"sources.process.interval"},
		},
		{
			name:  "process interval below one millisecond",
			body:  base + "sources:\n  process:\n    enabled: true\n    interval: 500us\n",
			exit:  2,
			wants: []string{"sources.process.interval", "at least 1ms"},
		},
		{
			name:  "process interval just below one millisecond",
			body:  base + "sources:\n  process:\n    enabled: true\n    interval: 999us\n",
			exit:  2,
			wants: []string{"sources.process.interval", "at least 1ms"},
		},
		{
			name: "one millisecond process interval",
			body: base + "sources:\n  process:\n    enabled: true\n    interval: 1ms\n",
			exit: 0,
		},
		{
			name:  "zero process rescan interval",
			body:  base + "sources:\n  process:\n    enabled: true\n    rescan_interval: 0s\n",
			exit:  2,
			wants: []string{"sources.process.rescan_interval"},
		},
		{
			name: "negative top_n",
			body: base + "sources:\n  process:\n    enabled: true\n    top_n: -1\n",
			exit: 2,
			wants: []string{
				"sources.process.top_n",
			},
		},
		{
			name:  "docker socket missing",
			body:  base + "sources:\n  docker:\n    enabled: true\n    socket: \"\"\n",
			exit:  2,
			wants: []string{"sources.docker.socket"},
		},
		{
			name: "docker source enabled",
			body: base + "sources:\n  docker:\n    enabled: true\n",
			exit: 0,
		},
		{
			name:  "docker stream stats without enabled source",
			body:  base + "sources:\n  docker:\n    stream_stats: true\n",
			exit:  2,
			wants: []string{"sources.docker.stream_stats"},
		},
		{
			name:  "zero docker interval",
			body:  base + "sources:\n  docker:\n    enabled: true\n    interval: 0s\n",
			exit:  2,
			wants: []string{"sources.docker.interval"},
		},
		{
			name:  "docker interval below one millisecond",
			body:  base + "sources:\n  docker:\n    enabled: true\n    interval: 500us\n",
			exit:  2,
			wants: []string{"sources.docker.interval", "at least 1ms"},
		},
		{
			name:  "docker interval just below one millisecond",
			body:  base + "sources:\n  docker:\n    enabled: true\n    interval: 999us\n",
			exit:  2,
			wants: []string{"sources.docker.interval", "at least 1ms"},
		},
		{
			name: "one millisecond docker interval",
			body: base + "sources:\n  docker:\n    enabled: true\n    interval: 1ms\n",
			exit: 0,
		},
		{
			name:  "zero docker rescan interval",
			body:  base + "sources:\n  docker:\n    enabled: true\n    rescan_interval: 0s\n",
			exit:  2,
			wants: []string{"sources.docker.rescan_interval"},
		},
		{
			name:  "docker exclude is not a regexp",
			body:  base + "sources:\n  docker:\n    enabled: true\n    exclude: \"[\"\n",
			exit:  2,
			wants: []string{"sources.docker.exclude"},
		},
		{
			name:  "log level override is invalid",
			body:  base,
			args:  []string{"--log-level", "verbose"},
			exit:  2,
			wants: []string{"log_level"},
		},
		{
			name:  "unknown flag",
			body:  base,
			args:  []string{"--bogus"},
			exit:  2,
			wants: []string{"unknown flag", "--bogus"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			path := writeConfig(t, tc.body)
			args := append([]string{"config", "--validate", "--config", path}, tc.args...)
			res := runSmob(t, ctx, args...)

			if res.ExitCode != tc.exit {
				t.Fatalf("exit = %d, want %d\nstdout:\n%s\nstderr:\n%s", res.ExitCode, tc.exit, res.Stdout, res.Stderr)
			}
			for _, want := range tc.wants {
				if !strings.Contains(res.Stderr, want) {
					t.Errorf("stderr does not contain %q\nstderr:\n%s", want, res.Stderr)
				}
			}
			for _, unwanted := range tc.wantsNot {
				if strings.Contains(res.Stderr, unwanted) {
					t.Errorf("stderr unexpectedly contains %q\nstderr:\n%s", unwanted, res.Stderr)
				}
			}
			if tc.exit == 0 && !strings.Contains(res.Stdout, "config valid:") {
				t.Errorf("stdout = %q, want a config valid line", res.Stdout)
			}
			// A rejected config is a usage error: the diagnostic belongs on
			// stderr alone, so a redirect of stdout stays empty.
			if tc.exit != 0 && res.Stdout != "" {
				t.Errorf("rejected config wrote to stdout:\n%s", res.Stdout)
			}
		})
	}
}

func TestConfigDump(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	dataDir := filepath.Join(t.TempDir(), "data")
	path := writeConfig(t, fmt.Sprintf("data_dir: %s\nsources:\n  system:\n    interval: 5s\n", dataDir))

	res := runSmob(t, ctx, "config", "--config", path)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}

	for _, line := range []string{
		"interval: 5s",
		"proc_path: /proc",
		"log_level: info",
		"log_format: text",
		"primary: disk",
		"chunk_interval: 5m0s",
		"rescan_interval: 30s",
		"spool_max_bytes: 1073741824",
		"max_age: 0s",
	} {
		if !hasLine(res.Stdout, line) {
			t.Errorf("config dump is missing %q\noutput:\n%s", line, res.Stdout)
		}
	}

	// The dump must itself be a valid configuration.
	roundTrip := writeConfig(t, res.Stdout)
	validated := runSmob(t, ctx, "config", "--validate", "--config", roundTrip)
	if validated.ExitCode != 0 {
		t.Fatalf("round-tripped dump does not validate: exit %d\nstderr:\n%s", validated.ExitCode, validated.Stderr)
	}

	// The Docker rescan cadence is printed even while the source is disabled,
	// so a dump round-trip proves the field is wired into the schema.
	dockerPath := writeConfig(t, fmt.Sprintf(`data_dir: %s
sources:
  docker:
    rescan_interval: 45s
`, dataDir))
	dockerRes := runSmob(t, ctx, "config", "--config", dockerPath)
	if dockerRes.ExitCode != 0 {
		t.Fatalf("docker config dump exit = %d\nstderr:\n%s", dockerRes.ExitCode, dockerRes.Stderr)
	}
	if !hasLine(dockerRes.Stdout, "rescan_interval: 45s") {
		t.Errorf("config dump is missing docker rescan_interval: 45s\noutput:\n%s", dockerRes.Stdout)
	}
	dockerRoundTrip := writeConfig(t, dockerRes.Stdout)
	validated = runSmob(t, ctx, "config", "--validate", "--config", dockerRoundTrip)
	if validated.ExitCode != 0 {
		t.Fatalf("round-tripped docker dump does not validate: exit %d\nstderr:\n%s", validated.ExitCode, validated.Stderr)
	}
}

// TestConfigDumpGolden pins the effective-config shape and field order. The
// fixture sets every value that would otherwise vary by machine — data_dir,
// host, and proc_path — so the dump is deterministic; normaliseConfigDump is a
// belt-and-braces guard against a future default that interpolates the checkout
// or a temp directory.
func TestConfigDumpGolden(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	path := writeConfig(t, `data_dir: /var/lib/smob
host: golden-host
proc_path: /proc
sources:
  system:
    interval: 5s
`)
	res := runSmob(t, ctx, "config", "--config", path)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	// The dump is pure stdout; a log line here would corrupt a redirect.
	if res.Stderr != "" {
		t.Fatalf("config dump wrote to stderr:\n%s", res.Stderr)
	}

	compareGolden(t, "config_dump.txt", normaliseConfigDump(res.Stdout))
}

// normaliseConfigDump strips anything that could still vary by machine.
func normaliseConfigDump(out string) string {
	for _, repl := range []struct{ from, to string }{
		{moduleRoot(), "<module>"},
		{os.TempDir(), "<tmp>"},
	} {
		if repl.from != "" {
			out = strings.ReplaceAll(out, repl.from, repl.to)
		}
	}
	return out
}

func TestConfigS3Resolution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	dataDir := filepath.Join(t.TempDir(), "data")
	cases := []struct {
		name  string
		body  string
		wants []string
	}{
		{
			name: "endpoint defaults",
			body: fmt.Sprintf(`data_dir: %s
storage:
  primary: s3
  s3:
    bucket: bucket
    endpoint: http://127.0.0.1:9000
`, dataDir),
			wants: []string{
				"region: us-east-1",
				"path_style: true",
				"spool_dir: " + filepath.Join(dataDir, "spool"),
			},
		},
		{
			name: "explicit values are preserved",
			body: fmt.Sprintf(`data_dir: %s
storage:
  primary: s3
  s3:
    bucket: bucket
    region: eu-west-1
    endpoint: http://127.0.0.1:9000
    path_style: false
`, dataDir),
			wants: []string{
				"region: eu-west-1",
				"path_style: false",
				"spool_dir: " + filepath.Join(dataDir, "spool"),
			},
		},
		{
			name: "surrounding whitespace and prefix slashes are trimmed",
			body: fmt.Sprintf(`data_dir: %s
storage:
  primary: s3
  s3:
    bucket: "  bucket  "
    region: " us-east-1 "
    endpoint: " http://127.0.0.1:9000 "
    prefix: /prod/smob/
`, dataDir),
			wants: []string{
				"region: us-east-1",
				"bucket: bucket",
				"endpoint: http://127.0.0.1:9000",
				"prefix: prod/smob",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, tc.body)
			res := runSmob(t, ctx, "config", "--config", path)
			if res.ExitCode != 0 {
				t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
			}
			for _, want := range tc.wants {
				if !hasLine(res.Stdout, want) {
					t.Errorf("config dump is missing %q\noutput:\n%s", want, res.Stdout)
				}
			}

			roundTrip := writeConfig(t, res.Stdout)
			validated := runSmob(t, ctx, "config", "--validate", "--config", roundTrip)
			if validated.ExitCode != 0 {
				t.Fatalf("round-tripped dump does not validate: exit %d\nstderr:\n%s", validated.ExitCode, validated.Stderr)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	host, err := os.Hostname()
	if err != nil {
		t.Fatalf("os.Hostname: %v", err)
	}

	path := writeConfig(t, "")
	res := runSmob(t, ctx, "config", "--config", path)
	if res.ExitCode != 0 {
		t.Fatalf("exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	for _, want := range []struct {
		line  string
		count int
	}{
		{"data_dir: /var/lib/smob", 1},
		{"proc_path: /proc", 1},
		{"host: " + host, 1},
		{"socket: /var/run/docker.sock", 1},
		{"exclude: \"\"", 1},
		{"stream_stats: false", 1},
		{"chunk_interval: 5m0s", 1},
		{"top_n: 20", 1},
		// System and Docker share the 10s default; process alone uses 30s.
		{"interval: 10s", 2},
		{"interval: 30s", 1},
		// Process and Docker share the 30s rescan default.
		{"rescan_interval: 30s", 2},
	} {
		if got := countLines(res.Stdout, want.line); got != want.count {
			t.Errorf("defaults contain %q %d times, want %d\noutput:\n%s",
				want.line, got, want.count, res.Stdout)
		}
	}
}

// countLines counts exact, trimmed lines in a config dump. Defaults are
// asserted by count so a field cannot hide behind another source's value.
func countLines(out, line string) int {
	count := 0
	for _, got := range strings.Split(out, "\n") {
		if strings.TrimSpace(got) == line {
			count++
		}
	}
	return count
}

// TestConfigFileSizeLimit proves a misdirected --config cannot grow the read
// without bound: a file just under the 4 MiB cap still loads, and a file just
// over it is a usage error rather than a decode failure.
func TestConfigFileSizeLimit(t *testing.T) {
	const limit = 4 << 20

	cases := []struct {
		name string
		size int
		exit int
	}{
		{name: "just under the limit", size: limit - 1, exit: 0},
		{name: "just over the limit", size: limit + 1, exit: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			dataDir := filepath.Join(t.TempDir(), "data")
			body := "data_dir: " + dataDir + "\n"
			// Pad with a single comment line so the file is exactly tc.size bytes.
			body += "#" + strings.Repeat("x", tc.size-len(body)-2) + "\n"
			path := writeConfig(t, body)
			if info, err := os.Stat(path); err != nil {
				t.Fatalf("stat config: %v", err)
			} else if got := int(info.Size()); got != tc.size {
				t.Fatalf("config size = %d, want %d", got, tc.size)
			}

			res := runSmob(t, ctx, "config", "--validate", "--config", path)
			if res.ExitCode != tc.exit {
				t.Fatalf("exit = %d, want %d\nstderr:\n%s", res.ExitCode, tc.exit, res.Stderr)
			}
			if tc.exit == 2 {
				for _, want := range []string{path, "config: file", "exceeds 4 MiB limit"} {
					if !strings.Contains(res.Stderr, want) {
						t.Errorf("stderr does not contain %q\nstderr:\n%s", want, res.Stderr)
					}
				}
				return
			}
			if !strings.Contains(res.Stdout, "config valid:") {
				t.Errorf("stdout = %q, want a config valid line", res.Stdout)
			}
		})
	}
}

func TestMissingConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	missing := filepath.Join(t.TempDir(), "absent.yaml")
	res := runSmob(t, ctx, "collect", "--config", missing)
	if res.ExitCode != 2 {
		t.Fatalf("exit = %d, want 2\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if !strings.Contains(res.Stderr, missing) {
		t.Errorf("stderr does not name the config path %q:\n%s", missing, res.Stderr)
	}
}
