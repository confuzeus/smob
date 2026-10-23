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
			name:  "host is not a path component",
			body:  base + "host: bad/host\n",
			exit:  2,
			wants: []string{"host: must be a single path component"},
		},
		{
			name: "s3 primary without bucket",
			body: base + "storage:\n  primary: s3\n",
			exit: 2,
			wants: []string{
				"storage.s3.bucket",
				"storage.s3.region",
				"storage.s3: not implemented in this build",
			},
		},
		{
			name:  "s3 primary without data dir or spool dir",
			body:  "data_dir: \"\"\nstorage:\n  primary: s3\n  s3:\n    bucket: bucket\n    region: us-east-1\n",
			exit:  2,
			wants: []string{"storage.s3.spool_dir"},
		},
		{
			name:     "s3 endpoint replaces region",
			body:     base + "storage:\n  primary: s3\n  s3:\n    bucket: bucket\n    endpoint: http://127.0.0.1:9000\n",
			exit:     2,
			wants:    []string{"storage.s3: not implemented in this build"},
			wantsNot: []string{"storage.s3.region"},
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
			name: "process filter is not a regexp",
			body: base + "sources:\n  process:\n    enabled: true\n    filter: \"[\"\n",
			exit: 2,
			wants: []string{
				"sources.process.filter",
				"sources.process: not implemented in this build",
			},
		},
		{
			name:  "zero process interval",
			body:  base + "sources:\n  process:\n    enabled: true\n    interval: 0s\n",
			exit:  2,
			wants: []string{"sources.process.interval"},
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
			name: "docker socket missing",
			body: base + "sources:\n  docker:\n    enabled: true\n    socket: \"\"\n",
			exit: 2,
			wants: []string{
				"sources.docker.socket",
				"sources.docker: not implemented in this build",
			},
		},
		{
			name:  "log level override is invalid",
			body:  base,
			args:  []string{"--log-level", "verbose"},
			exit:  2,
			wants: []string{"log_level"},
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
	for _, line := range []string{
		"data_dir: /var/lib/smob",
		"proc_path: /proc",
		"host: " + host,
		"socket: /var/run/docker.sock",
		"chunk_interval: 5m0s",
		"top_n: 20",
		"interval: 10s",
		"interval: 30s",
	} {
		if !hasLine(res.Stdout, line) {
			t.Errorf("defaults are missing %q\noutput:\n%s", line, res.Stdout)
		}
	}
}

func TestCollectNotImplemented(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	path := validConfig(t)
	res := runSmob(t, ctx, "collect", "--config", path)
	if res.ExitCode != 1 {
		t.Fatalf("exit = %d, want 1\nstdout:\n%s\nstderr:\n%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "not implemented") {
		t.Errorf("stderr does not explain the missing feature:\n%s", res.Stderr)
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
