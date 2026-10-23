//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// seedQueryData writes two chunks with deterministic samples across hosts and
// scopes. Timestamps are fixed, so tests pass an explicit range.
func seedQueryData(t *testing.T) (cfg string, base time.Time) {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg = writeConfig(t, "data_dir: "+dataDir+"\n")
	base = time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)
	start, end := base, base.Add(5*time.Minute)

	writeChunk(t, dataDir, "alpha", "system", start, end, []wireSample{
		{
			Timestamp:  base.Add(1 * time.Second),
			Metric:     "system.cpu.percent",
			Value:      12.5,
			IntervalMS: 10_000,
			Labels:     map[string]string{"host": "alpha", "scope": "system", "cpu": "all"},
		},
		{
			Timestamp: base.Add(1 * time.Second),
			Metric:    "system.memory.used_bytes",
			Value:     1024,
			Labels:    map[string]string{"host": "alpha", "scope": "system"},
		},
		{
			Timestamp:  base.Add(2 * time.Second),
			Metric:     "system.network.rx_bytes_per_second",
			Value:      2048,
			IntervalMS: 10_000,
			Labels:     map[string]string{"host": "alpha", "scope": "system", "interface": "eth0"},
		},
	})
	writeChunk(t, dataDir, "beta", "process", start, end, []wireSample{
		{
			Timestamp:  base.Add(1 * time.Second),
			Metric:     "process.cpu.percent",
			Value:      50,
			IntervalMS: 30_000,
			Labels:     map[string]string{"host": "beta", "scope": "process", "pid": "42", "comm": "nginx"},
		},
		{
			Timestamp:  base.Add(3 * time.Second),
			Metric:     "process.cpu.percent",
			Value:      150,
			IntervalMS: 30_000,
			Labels:     map[string]string{"host": "beta", "scope": "process", "pid": "43", "comm": "postgres"},
		},
	})
	return cfg, base
}

func queryRange(base time.Time) []string {
	return []string{"--since", base.Add(-time.Hour).Format(time.RFC3339), "--until", base.Add(time.Hour).Format(time.RFC3339)}
}

func queryArgs(cfg string, base time.Time, extra ...string) []string {
	args := []string{"query", "--config", cfg}
	args = append(args, queryRange(base)...)
	return append(args, extra...)
}

func TestQueryFormats(t *testing.T) {
	cfg, base := seedQueryData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	table := runSmob(t, ctx, queryArgs(cfg, base, "--format", "table")...)
	if table.ExitCode != 0 {
		t.Fatalf("table query exit = %d\nstderr:\n%s", table.ExitCode, table.Stderr)
	}
	header := strings.Fields(strings.Split(strings.TrimSpace(table.Stdout), "\n")[0])
	wantHeader := []string{"TIME", "METRIC", "VALUE", "UNIT", "LABELS"}
	if !slices.Equal(header, wantHeader) {
		t.Fatalf("table header = %v, want %v", header, wantHeader)
	}

	jsonOut := runSmob(t, ctx, queryArgs(cfg, base, "--format", "json")...)
	if jsonOut.ExitCode != 0 {
		t.Fatalf("json query exit = %d\nstderr:\n%s", jsonOut.ExitCode, jsonOut.Stderr)
	}
	want := decodeJSONSamples(t, jsonOut.Stdout)
	if len(want) != 5 {
		t.Fatalf("json returned %d samples, want 5", len(want))
	}
	if tableRows(table.Stdout) != len(want) {
		t.Fatalf("table rows = %d, want %d", tableRows(table.Stdout), len(want))
	}

	jsonlOut := runSmob(t, ctx, queryArgs(cfg, base, "--format", "jsonl")...)
	if jsonlOut.ExitCode != 0 {
		t.Fatalf("jsonl query exit = %d\nstderr:\n%s", jsonlOut.ExitCode, jsonlOut.Stderr)
	}
	assertSameSamples(t, decodeJSONLSamples(t, jsonlOut.Stdout), want)

	csvOut := runSmob(t, ctx, queryArgs(cfg, base, "--format", "csv")...)
	if csvOut.ExitCode != 0 {
		t.Fatalf("csv query exit = %d\nstderr:\n%s", csvOut.ExitCode, csvOut.Stderr)
	}
	assertSameSamples(t, decodeCSVSamples(t, csvOut.Stdout), want)
}

func TestQueryOrdering(t *testing.T) {
	cfg, base := seedQueryData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	res := runSmob(t, ctx, queryArgs(cfg, base, "--format", "json")...)
	if res.ExitCode != 0 {
		t.Fatalf("query exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	samples := decodeJSONSamples(t, res.Stdout)

	for i := 1; i < len(samples); i++ {
		if samples[i].Timestamp.Before(samples[i-1].Timestamp) {
			t.Fatalf("sample %d timestamp %s is before %s", i, samples[i].Timestamp, samples[i-1].Timestamp)
		}
		if samples[i].Timestamp.Equal(samples[i-1].Timestamp) && samples[i].Metric < samples[i-1].Metric {
			t.Fatalf("sample %d metric %s is before %s at the same timestamp", i, samples[i].Metric, samples[i-1].Metric)
		}
	}

	if got := samples[0].Metric; got != "process.cpu.percent" {
		t.Fatalf("first metric = %s, want process.cpu.percent", got)
	}
	if got := samples[0].Labels["host"]; got != "beta" {
		t.Fatalf("first host = %s, want beta", got)
	}
	if got := samples[len(samples)-1].Value; got != 150 {
		t.Fatalf("last value = %v, want 150", got)
	}
}

func TestQueryFilters(t *testing.T) {
	cfg, base := seedQueryData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cases := []struct {
		name string
		args []string
		want int
	}{
		{"metric", []string{"--metric", "process.cpu.percent"}, 2},
		{"host", []string{"--host", "alpha"}, 3},
		{"scope", []string{"--scope", "process"}, 2},
		{"exact label", []string{"--label", "cpu=all"}, 1},
		{"regex label", []string{"--label", "comm~nginx|postgres"}, 2},
		{"missing label is empty", []string{"--label", "comm="}, 3},
		{"combined", []string{"--scope", "process", "--label", "comm~nginx"}, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := runSmob(t, ctx, queryArgs(cfg, base, append(tc.args, "--format", "json")...)...)
			if res.ExitCode != 0 {
				t.Fatalf("query exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
			}
			if got := len(decodeJSONSamples(t, res.Stdout)); got != tc.want {
				t.Fatalf("got %d samples, want %d", got, tc.want)
			}
		})
	}
}

func TestQueryLimit(t *testing.T) {
	cfg, base := seedQueryData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	limited := runSmob(t, ctx, queryArgs(cfg, base, "--limit", "2", "--format", "json")...)
	if limited.ExitCode != 0 {
		t.Fatalf("query exit = %d\nstderr:\n%s", limited.ExitCode, limited.Stderr)
	}
	samples := decodeJSONSamples(t, limited.Stdout)
	if len(samples) != 2 {
		t.Fatalf("limited query returned %d samples, want 2", len(samples))
	}
	if !samples[0].Timestamp.Equal(base.Add(time.Second)) {
		t.Fatalf("first limited sample = %s, want %s", samples[0].Timestamp, base.Add(time.Second))
	}

	unlimited := runSmob(t, ctx, queryArgs(cfg, base, "--limit", "0", "--format", "json")...)
	if unlimited.ExitCode != 0 {
		t.Fatalf("query exit = %d\nstderr:\n%s", unlimited.ExitCode, unlimited.Stderr)
	}
	if got := len(decodeJSONSamples(t, unlimited.Stdout)); got != 5 {
		t.Fatalf("unlimited query returned %d samples, want 5", got)
	}
}

func TestQueryDefaults(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	now := time.Now().UTC()

	recent := now.Add(-30 * time.Minute).Truncate(time.Second)
	old := now.Add(-2 * time.Hour).Truncate(time.Second)
	for _, sample := range []wireSample{
		{
			Timestamp: recent,
			Metric:    "recent.metric.value",
			Value:     1,
			Labels:    map[string]string{"host": "h", "scope": "system"},
		},
		{
			Timestamp: old,
			Metric:    "old.metric.value",
			Value:     2,
			Labels:    map[string]string{"host": "h", "scope": "system"},
		},
	} {
		start := sample.Timestamp.Truncate(5 * time.Minute)
		writeChunk(t, dataDir, "h", "system", start, start.Add(5*time.Minute), []wireSample{sample})
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	defaults := runSmob(t, ctx, "query", "--config", cfg, "--format", "json")
	if defaults.ExitCode != 0 {
		t.Fatalf("default query exit = %d\nstderr:\n%s", defaults.ExitCode, defaults.Stderr)
	}
	samples := decodeJSONSamples(t, defaults.Stdout)
	if len(samples) != 1 || samples[0].Metric != "recent.metric.value" {
		t.Fatalf("default query = %+v, want only recent.metric.value", samples)
	}

	until := runSmob(t, ctx, "query", "--config", cfg, "--until", now.Format(time.RFC3339), "--format", "json")
	if until.ExitCode != 0 {
		t.Fatalf("until query exit = %d\nstderr:\n%s", until.ExitCode, until.Stderr)
	}
	if got := len(decodeJSONSamples(t, until.Stdout)); got != 1 {
		t.Fatalf("until-only query returned %d samples, want 1", got)
	}

	wide := runSmob(t, ctx, "query", "--config", cfg, "--since", "3h", "--format", "json")
	if wide.ExitCode != 0 {
		t.Fatalf("wide query exit = %d\nstderr:\n%s", wide.ExitCode, wide.Stderr)
	}
	if got := len(decodeJSONSamples(t, wide.Stdout)); got != 2 {
		t.Fatalf("wide query returned %d samples, want 2", got)
	}
}

func TestQueryEmpty(t *testing.T) {
	cfg := writeConfig(t, "data_dir: "+filepath.Join(t.TempDir(), "missing")+"\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	table := runSmob(t, ctx, "query", "--config", cfg)
	if table.ExitCode != 0 {
		t.Fatalf("empty table query exit = %d\nstderr:\n%s", table.ExitCode, table.Stderr)
	}
	if tableRows(table.Stdout) != 0 {
		t.Fatalf("empty table query = %q, want a header only", table.Stdout)
	}
	// The exact spacing depends on tabwriter; check the fields instead.
	header := strings.Fields(strings.Split(strings.TrimSpace(table.Stdout), "\n")[0])
	want := []string{"TIME", "METRIC", "VALUE", "UNIT", "LABELS"}
	if !slices.Equal(header, want) {
		t.Fatalf("table header = %v, want %v", header, want)
	}

	jsonOut := runSmob(t, ctx, "query", "--config", cfg, "--format", "json")
	if jsonOut.ExitCode != 0 {
		t.Fatalf("empty json query exit = %d\nstderr:\n%s", jsonOut.ExitCode, jsonOut.Stderr)
	}
	if jsonOut.Stdout != "[]\n" {
		t.Fatalf("empty json query = %q, want %q", jsonOut.Stdout, "[]\n")
	}
}

func TestQueryInvalidFlags(t *testing.T) {
	cfg := validConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cases := []struct {
		name string
		args []string
		want string
	}{
		{"bad since", []string{"--since", "nonsense"}, "since"},
		{"zero since", []string{"--since", "0s"}, "since"},
		{"bad until", []string{"--until", "1h"}, "until"},
		{"bad format", []string{"--format", "yaml"}, "format"},
		{"negative limit", []string{"--limit", "-1"}, "limit"},
		{"label without operator", []string{"--label", "host"}, "label"},
		{"empty label name", []string{"--label", "=x"}, "label"},
		{"bad label regex", []string{"--label", "x~["}, "label x"},
		{"bad scope", []string{"--scope", "bogus"}, "scope"},
		{"bad host", []string{"--host", "a/b"}, "host"},
		{"inverted range", []string{"--since", "2026-10-23T17:00:00Z", "--until", "2026-10-23T16:00:00Z"}, "since"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"query", "--config", cfg}, tc.args...)
			res := runSmob(t, ctx, args...)
			if res.ExitCode != 2 {
				t.Fatalf("exit = %d, want 2\nstdout:\n%s\nstderr:\n%s", res.ExitCode, res.Stdout, res.Stderr)
			}
			if !strings.Contains(res.Stderr, tc.want) {
				t.Fatalf("stderr does not mention %q:\n%s", tc.want, res.Stderr)
			}
		})
	}
}

func TestQueryCorruptChunk(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	base := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)

	dir := filepath.Join(dataDir, "h", "system", "2026", "10", "23", "16")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create chunk dir: %v", err)
	}
	path := filepath.Join(dir, "20261023T160000Z-20261023T160500Z.jsonl.gz")
	if err := os.WriteFile(path, []byte("not gzip"), 0o644); err != nil {
		t.Fatalf("write corrupt chunk: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, queryArgs(cfg, base, "--format", "json")...)
	if res.ExitCode != 1 {
		t.Fatalf("exit = %d, want 1\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "20261023T160000Z-20261023T160500Z.jsonl.gz") {
		t.Fatalf("stderr does not name the corrupt chunk:\n%s", res.Stderr)
	}
}

func TestQueryMalformedChunkName(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	base := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)

	dir := filepath.Join(dataDir, "h", "system", "2026", "10", "23", "16")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create chunk dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "junk.jsonl.gz"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write malformed chunk: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res := runSmob(t, ctx, queryArgs(cfg, base, "--format", "json")...)
	if res.ExitCode != 1 {
		t.Fatalf("exit = %d, want 1\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if !strings.Contains(res.Stderr, "junk.jsonl.gz") {
		t.Fatalf("stderr does not name the malformed chunk:\n%s", res.Stderr)
	}
}
