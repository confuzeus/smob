//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
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

// tableCells splits one tabwriter row into its five columns. tabwriter pads
// every column with at least two spaces; the first four cells cannot contain a
// space, and LABELS is last, so a run of two or more spaces separates them. A
// label value with single spaces therefore stays inside the last cell.
func tableCells(t *testing.T, line string) []string {
	t.Helper()
	cells := make([]string, 0, 5)
	for len(cells) < 4 {
		i := strings.Index(line, "  ")
		if i < 0 {
			t.Fatalf("table row has %d columns, want 5:\n%s", len(cells)+1, line)
		}
		cells = append(cells, strings.TrimRight(line[:i], " "))
		line = strings.TrimLeft(line[i:], " ")
	}
	return append(cells, line)
}

// TestQueryTableCells checks the rendered cells, not just the row count. The
// hostile label value carries spaces, '=', ',', and a newline: the newline must
// be escaped so the row stays on one line, while the rest survive verbatim.
// The gauge also pins the `_bytes` unit.
func TestQueryTableCells(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	base := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)
	hostile := "a b=c,d\ne"
	labels := map[string]string{"host": "h", "scope": "process", "pid": "1", "comm": hostile}

	writeChunk(t, dataDir, "h", "process", base, base.Add(5*time.Minute), []wireSample{
		{
			Timestamp:  base.Add(time.Second),
			Metric:     "process.cpu.percent",
			Value:      12.5,
			IntervalMS: 10_000,
			Labels:     labels,
		},
		{
			Timestamp: base.Add(time.Second),
			Metric:    "process.memory.rss_bytes",
			Value:     4096,
			Labels:    labels,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	res := runSmob(t, ctx, queryArgs(cfg, base, "--format", "table")...)
	if res.ExitCode != 0 {
		t.Fatalf("table query exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	lines := strings.Split(strings.TrimSpace(res.Stdout), "\n")
	if len(lines) != 3 {
		t.Fatalf("table output has %d lines, want a header and two rows; a label newline split a row:\n%s", len(lines), res.Stdout)
	}

	wantHeader := []string{"TIME", "METRIC", "VALUE", "UNIT", "LABELS"}
	if got := tableCells(t, lines[0]); !slices.Equal(got, wantHeader) {
		t.Fatalf("table header = %v, want %v", got, wantHeader)
	}

	const wantLabels = "comm=a b=c,d\\ne,host=h,pid=1,scope=process"
	wantRows := [][]string{
		{base.Add(time.Second).Format(time.RFC3339Nano), "process.cpu.percent", "12.5", "%", wantLabels},
		{base.Add(time.Second).Format(time.RFC3339Nano), "process.memory.rss_bytes", "4096", "B", wantLabels},
	}
	for i, want := range wantRows {
		if got := tableCells(t, lines[i+1]); !slices.Equal(got, want) {
			t.Fatalf("table row %d = %v, want %v", i, got, want)
		}
	}
	if strings.ContainsAny(wantRows[1][4], "\r\n") {
		t.Fatalf("escaped labels still contain a line break: %q", wantRows[1][4])
	}
}

// TestQueryCSVQuoting proves encoding/csv keeps a label value containing a
// comma, a quote, and a newline inside one five-column record.
func TestQueryCSVQuoting(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	base := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)
	hostile := "a,b\"c\nd"
	labels := map[string]string{"host": "h", "scope": "process", "pid": "1", "comm": hostile}

	writeChunk(t, dataDir, "h", "process", base, base.Add(5*time.Minute), []wireSample{{
		Timestamp:  base.Add(time.Second),
		Metric:     "process.cpu.percent",
		Value:      12.5,
		IntervalMS: 10_000,
		Labels:     labels,
	}})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	res := runSmob(t, ctx, queryArgs(cfg, base, "--format", "csv")...)
	if res.ExitCode != 0 {
		t.Fatalf("csv query exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	rows, err := csv.NewReader(strings.NewReader(res.Stdout)).ReadAll()
	if err != nil {
		t.Fatalf("decode csv output: %v\n%s", err, res.Stdout)
	}
	wantHeader := []string{"timestamp", "metric", "value", "interval_ms", "labels"}
	if len(rows) != 2 || !slices.Equal(rows[0], wantHeader) {
		t.Fatalf("csv rows = %v, want the header and one sample row", rows)
	}
	if len(rows[1]) != len(wantHeader) {
		t.Fatalf("csv sample row has %d fields, want %d: %v", len(rows[1]), len(wantHeader), rows[1])
	}

	want := []string{
		base.Add(time.Second).Format(time.RFC3339Nano),
		"process.cpu.percent",
		"12.5",
		"10000",
		"comm=" + hostile + ",host=h,pid=1,scope=process",
	}
	if !slices.Equal(rows[1], want) {
		t.Fatalf("csv fields = %q, want %q", rows[1], want)
	}
}

// TestQueryJSONAndJSONLAgree proves the JSON document is one array holding the
// same ordered samples as JSONL, that a gauge omits interval_ms, and that a
// rate includes it.
func TestQueryJSONAndJSONLAgree(t *testing.T) {
	const (
		gaugeMetric = "system.memory.used_bytes"
		rateMetric  = "system.cpu.percent"
	)
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	base := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)
	labels := map[string]string{"host": "h", "scope": "system"}

	writeChunk(t, dataDir, "h", "system", base, base.Add(5*time.Minute), []wireSample{
		{Timestamp: base.Add(time.Second), Metric: rateMetric, Value: 12.5, IntervalMS: 10_000, Labels: labels},
		{Timestamp: base.Add(time.Second), Metric: gaugeMetric, Value: 1024, Labels: labels},
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	jsonOut := runSmob(t, ctx, queryArgs(cfg, base, "--format", "json")...)
	if jsonOut.ExitCode != 0 {
		t.Fatalf("json query exit = %d\nstderr:\n%s", jsonOut.ExitCode, jsonOut.Stderr)
	}
	var document []json.RawMessage
	if err := json.Unmarshal([]byte(jsonOut.Stdout), &document); err != nil {
		t.Fatalf("json output is not an array: %v\n%s", err, jsonOut.Stdout)
	}
	if len(document) != 2 {
		t.Fatalf("json array holds %d samples, want 2", len(document))
	}

	jsonlOut := runSmob(t, ctx, queryArgs(cfg, base, "--format", "jsonl")...)
	if jsonlOut.ExitCode != 0 {
		t.Fatalf("jsonl query exit = %d\nstderr:\n%s", jsonlOut.ExitCode, jsonlOut.Stderr)
	}
	var lines []json.RawMessage
	for _, line := range strings.Split(strings.TrimSpace(jsonlOut.Stdout), "\n") {
		if line != "" {
			lines = append(lines, json.RawMessage(line))
		}
	}
	if len(lines) != len(document) {
		t.Fatalf("jsonl holds %d samples, want %d", len(lines), len(document))
	}

	assertSameSamples(t, decodeJSONLSamples(t, jsonlOut.Stdout), decodeJSONSamples(t, jsonOut.Stdout))

	for _, tc := range []struct {
		name  string
		lines []json.RawMessage
	}{
		{"json", document},
		{"jsonl", lines},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seen := map[string]bool{}
			for _, line := range tc.lines {
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(line, &fields); err != nil {
					t.Fatalf("decode sample: %v\n%s", err, line)
				}
				var metric string
				if err := json.Unmarshal(fields["metric"], &metric); err != nil {
					t.Fatalf("decode metric: %v\n%s", err, line)
				}
				interval, hasInterval := fields["interval_ms"]
				seen[metric] = true
				switch metric {
				case gaugeMetric:
					if hasInterval {
						t.Fatalf("gauge %s carries interval_ms=%s", metric, interval)
					}
				case rateMetric:
					if !hasInterval {
						t.Fatalf("rate %s omits interval_ms", metric)
					}
					if string(interval) != "10000" {
						t.Fatalf("rate %s interval_ms = %s, want 10000", metric, interval)
					}
				}
			}
			if !seen[gaugeMetric] || !seen[rateMetric] {
				t.Fatalf("output does not hold both the gauge and the rate: seen %v", seen)
			}
		})
	}
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
		{"exact pid", []string{"--label", "pid=43"}, 1},
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

// corruptChunk overwrites a chunk with bytes that cannot be gzip-decoded. A
// query that opens it fails, so a successful query proves the file was skipped.
func corruptChunk(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("not gzip"), 0o644); err != nil {
		t.Fatalf("corrupt chunk %s: %v", path, err)
	}
}

// seededChunkPath returns the file path writeChunk uses for a five-minute
// chunk.
func seededChunkPath(dataDir, host, scope string, start time.Time) string {
	return filepath.Join(dataDir, host, scope,
		start.Format("2006"), start.Format("01"), start.Format("02"), start.Format("15"),
		start.Format("20060102T150405Z")+"-"+start.Add(5*time.Minute).Format("20060102T150405Z")+".jsonl.gz")
}

// TestQueryHostPruning proves a host filter never reads another host's chunks:
// beta's chunk is corrupt, so the filtered alpha query succeeds only if beta is
// never opened.
func TestQueryHostPruning(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	base := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)

	writeChunk(t, dataDir, "alpha", "system", base, base.Add(5*time.Minute), []wireSample{{
		Timestamp: base.Add(time.Second),
		Metric:    "system.cpu.percent",
		Value:     1,
		Labels:    map[string]string{"host": "alpha", "scope": "system"},
	}})
	writeChunk(t, dataDir, "beta", "system", base, base.Add(5*time.Minute), []wireSample{{
		Timestamp: base.Add(time.Second),
		Metric:    "system.cpu.percent",
		Value:     2,
		Labels:    map[string]string{"host": "beta", "scope": "system"},
	}})
	betaChunk := seededChunkPath(dataDir, "beta", "system", base)
	corruptChunk(t, betaChunk)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	alpha := runSmob(t, ctx, queryArgs(cfg, base, "--host", "alpha", "--format", "json")...)
	if alpha.ExitCode != 0 {
		t.Fatalf("alpha query exit = %d\nstderr:\n%s", alpha.ExitCode, alpha.Stderr)
	}
	samples := decodeJSONSamples(t, alpha.Stdout)
	if len(samples) != 1 || samples[0].Labels["host"] != "alpha" || samples[0].Value != 1 {
		t.Fatalf("alpha query = %+v, want only alpha's sample", samples)
	}

	// The corruption is real: a query that must read beta fails.
	beta := runSmob(t, ctx, queryArgs(cfg, base, "--host", "beta", "--format", "json")...)
	if beta.ExitCode != 1 {
		t.Fatalf("beta query exit = %d, want 1\nstderr:\n%s", beta.ExitCode, beta.Stderr)
	}
	if !strings.Contains(beta.Stderr, filepath.Base(betaChunk)) {
		t.Fatalf("stderr does not name the corrupt chunk:\n%s", beta.Stderr)
	}

	missing := runSmob(t, ctx, queryArgs(cfg, base, "--host", "gamma", "--format", "json")...)
	if missing.ExitCode != 0 {
		t.Fatalf("missing host query exit = %d\nstderr:\n%s", missing.ExitCode, missing.Stderr)
	}
	if missing.Stdout != "[]\n" {
		t.Fatalf("missing host query = %q, want %q", missing.Stdout, "[]\n")
	}
}

// TestQueryScopePruning proves a scope filter never reads another scope's
// chunks: alpha/process is corrupt, so selecting alpha/system succeeds only if
// the process directory is never opened.
func TestQueryScopePruning(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	base := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)

	writeChunk(t, dataDir, "alpha", "system", base, base.Add(5*time.Minute), []wireSample{{
		Timestamp: base.Add(time.Second),
		Metric:    "system.cpu.percent",
		Value:     1,
		Labels:    map[string]string{"host": "alpha", "scope": "system"},
	}})
	writeChunk(t, dataDir, "alpha", "process", base, base.Add(5*time.Minute), []wireSample{{
		Timestamp: base.Add(time.Second),
		Metric:    "process.cpu.percent",
		Value:     2,
		Labels:    map[string]string{"host": "alpha", "scope": "process"},
	}})
	processChunk := seededChunkPath(dataDir, "alpha", "process", base)
	corruptChunk(t, processChunk)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// No host filter: every host is discovered, but only the system scope is
	// read.
	system := runSmob(t, ctx, queryArgs(cfg, base, "--scope", "system", "--format", "json")...)
	if system.ExitCode != 0 {
		t.Fatalf("system query exit = %d\nstderr:\n%s", system.ExitCode, system.Stderr)
	}
	samples := decodeJSONSamples(t, system.Stdout)
	if len(samples) != 1 || samples[0].Labels["scope"] != "system" || samples[0].Value != 1 {
		t.Fatalf("system query = %+v, want only the system sample", samples)
	}

	process := runSmob(t, ctx, queryArgs(cfg, base, "--host", "alpha", "--scope", "process", "--format", "json")...)
	if process.ExitCode != 1 {
		t.Fatalf("process query exit = %d, want 1\nstderr:\n%s", process.ExitCode, process.Stderr)
	}
	if !strings.Contains(process.Stderr, filepath.Base(processChunk)) {
		t.Fatalf("stderr does not name the corrupt chunk:\n%s", process.Stderr)
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

// TestQueryEmpty proves an existing data directory with no chunks is an empty
// result, not an error. A missing or non-directory root is covered by
// TestQueryBadDataDir.
func TestQueryEmpty(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("create empty data dir: %v", err)
	}
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
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

// TestQueryBadDataDir proves a query root that is missing or not a directory
// fails loudly instead of looking like an empty store. The root is checked
// before the output document opens, so a mistyped path prints nothing at all.
func TestQueryBadDataDir(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "typo")
	notDir := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(notDir, []byte("x"), 0o600); err != nil {
		t.Fatalf("write file root: %v", err)
	}
	cfg := writeConfig(t, "data_dir: "+filepath.Join(dir, "data")+"\n")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cases := []struct {
		name string
		args []string
		want string
	}{
		{"config root", []string{"query", "--config", cfg}, filepath.Join(dir, "data")},
		{"missing flag root", []string{"query", "--config", cfg, "--data-dir", missing}, missing},
		{"missing flag root json", []string{"query", "--config", cfg, "--data-dir", missing, "--format", "json"}, missing},
		{"missing flag root aggregate", []string{"query", "--config", cfg, "--data-dir", missing, "--agg", "avg"}, missing},
		{"file root", []string{"query", "--config", cfg, "--data-dir", notDir}, notDir},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := runSmob(t, ctx, tc.args...)
			if res.ExitCode != 1 {
				t.Fatalf("exit = %d, want 1\nstdout:\n%s\nstderr:\n%s", res.ExitCode, res.Stdout, res.Stderr)
			}
			if !strings.Contains(res.Stderr, tc.want) {
				t.Fatalf("stderr does not name %q:\n%s", tc.want, res.Stderr)
			}
			if !strings.Contains(res.Stderr, "--data-dir") {
				t.Fatalf("stderr does not point at --data-dir:\n%s", res.Stderr)
			}
			if res.Stdout != "" {
				t.Fatalf("stdout = %q, want empty", res.Stdout)
			}
		})
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
	if !json.Valid([]byte(res.Stdout)) {
		t.Fatalf("stdout is not a complete JSON document after a scan error:\n%s", res.Stdout)
	}
}

// TestQueryScanErrorClosesJSON proves a failure after some rows were written
// still produces a complete JSON array instead of a truncated one. The first
// hour yields a sample; the second hour's chunk is corrupt.
func TestQueryScanErrorClosesJSON(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	base := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)

	writeChunk(t, dataDir, "h", "system", base, base.Add(5*time.Minute), []wireSample{{
		Timestamp: base.Add(time.Second),
		Metric:    "system.cpu.percent",
		Value:     1,
		Labels:    map[string]string{"host": "h", "scope": "system"},
	}})
	later := base.Add(time.Hour)
	corrupt := seededChunkPath(dataDir, "h", "system", later)
	if err := os.MkdirAll(filepath.Dir(corrupt), 0o755); err != nil {
		t.Fatalf("create chunk dir: %v", err)
	}
	if err := os.WriteFile(corrupt, []byte("not gzip"), 0o644); err != nil {
		t.Fatalf("write corrupt chunk: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	args := []string{
		"query", "--config", cfg,
		"--since", base.Add(-time.Hour).Format(time.RFC3339),
		"--until", base.Add(2 * time.Hour).Format(time.RFC3339),
		"--format", "json",
	}
	res := runSmob(t, ctx, args...)
	if res.ExitCode != 1 {
		t.Fatalf("exit = %d, want 1\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if !strings.Contains(res.Stderr, filepath.Base(corrupt)) {
		t.Fatalf("stderr does not name the corrupt chunk:\n%s", res.Stderr)
	}
	if !json.Valid([]byte(res.Stdout)) {
		t.Fatalf("stdout is not a complete JSON document after a scan error:\n%s", res.Stdout)
	}
	samples := decodeJSONSamples(t, res.Stdout)
	if len(samples) != 1 {
		t.Fatalf("stdout held %d samples, want the one written before the failure", len(samples))
	}
}

// TestQueryHostileLabels proves a label value containing newlines, tabs, and
// carriage returns stays on one table row and keeps CSV parseable. Process
// comm is the realistic source of such a value.
func TestQueryHostileLabels(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	base := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)
	hostile := "a\nb\tc\\d\r"

	writeChunk(t, dataDir, "h", "process", base, base.Add(5*time.Minute), []wireSample{{
		Timestamp: base.Add(time.Second),
		Metric:    "process.cpu.percent",
		Value:     1,
		Labels: map[string]string{
			"host": "h", "scope": "process", "pid": "1", "comm": hostile,
		},
	}})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	table := runSmob(t, ctx, queryArgs(cfg, base, "--format", "table")...)
	if table.ExitCode != 0 {
		t.Fatalf("table query exit = %d\nstderr:\n%s", table.ExitCode, table.Stderr)
	}
	lines := strings.Split(strings.TrimSpace(table.Stdout), "\n")
	if len(lines) != 2 {
		t.Fatalf("table output has %d lines, want a header and one row:\n%s", len(lines), table.Stdout)
	}
	fields := strings.Fields(lines[1])
	if len(fields) != 5 {
		t.Fatalf("table row has %d columns, want 5:\n%s", len(fields), lines[1])
	}
	wantLabels := `comm=a\nb\tc\\d\r,host=h,pid=1,scope=process`
	if fields[4] != wantLabels {
		t.Fatalf("table labels = %q, want %q", fields[4], wantLabels)
	}

	csvOut := runSmob(t, ctx, queryArgs(cfg, base, "--format", "csv")...)
	if csvOut.ExitCode != 0 {
		t.Fatalf("csv query exit = %d\nstderr:\n%s", csvOut.ExitCode, csvOut.Stderr)
	}
	rows, err := csv.NewReader(strings.NewReader(csvOut.Stdout)).ReadAll()
	if err != nil {
		t.Fatalf("decode csv output: %v\n%s", err, csvOut.Stdout)
	}
	if len(rows) != 2 || len(rows[1]) != 5 {
		t.Fatalf("csv rows = %v, want a header and one five-column row", rows)
	}
	if got := parseLabels(rows[1][4])["comm"]; got != hostile {
		t.Fatalf("csv comm = %q, want %q", got, hostile)
	}
}

// TestQueryByteUnit proves the `_bytes` suffix used by the process source maps
// to the bytes unit in table output.
func TestQueryByteUnit(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	base := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)

	writeChunk(t, dataDir, "h", "process", base, base.Add(5*time.Minute), []wireSample{{
		Timestamp: base.Add(time.Second),
		Metric:    "process.memory.rss_bytes",
		Value:     4096,
		Labels:    map[string]string{"host": "h", "scope": "process", "pid": "1", "comm": "nginx"},
	}})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	res := runSmob(t, ctx, queryArgs(cfg, base, "--format", "table")...)
	if res.ExitCode != 0 {
		t.Fatalf("table query exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	for _, line := range strings.Split(strings.TrimSpace(res.Stdout), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 4 && fields[1] == "process.memory.rss_bytes" {
			if fields[3] != "B" {
				t.Fatalf("unit = %q, want B\n%s", fields[3], line)
			}
			return
		}
	}
	t.Fatalf("table output has no process.memory.rss_bytes row:\n%s", res.Stdout)
}

// TestQueryNonFiniteAggregate proves an aggregate that overflows to +Inf fails
// with the metric named instead of emitting an invalid row. Every format must
// leave a well-formed document behind.
func TestQueryNonFiniteAggregate(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	base := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)
	labels := map[string]string{"host": "h", "scope": "system"}

	writeChunk(t, dataDir, "h", "system", base, base.Add(5*time.Minute), []wireSample{
		{Timestamp: base.Add(time.Second), Metric: "system.memory.used_bytes", Value: 1e308, Labels: labels},
		{Timestamp: base.Add(2 * time.Second), Metric: "system.memory.used_bytes", Value: 1e308, Labels: labels},
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for _, format := range []string{"table", "json", "jsonl", "csv"} {
		t.Run(format, func(t *testing.T) {
			res := runSmob(t, ctx, queryArgs(cfg, base, "--agg", "sum", "--format", format)...)
			if res.ExitCode != 1 {
				t.Fatalf("exit = %d, want 1\nstdout:\n%s\nstderr:\n%s", res.ExitCode, res.Stdout, res.Stderr)
			}
			if !strings.Contains(res.Stderr, "system.memory.used_bytes") || !strings.Contains(res.Stderr, "non-finite") {
				t.Fatalf("stderr does not name the metric and the problem:\n%s", res.Stderr)
			}
			if strings.Contains(res.Stdout, "Inf") {
				t.Fatalf("stdout emitted a non-finite value:\n%s", res.Stdout)
			}
			if format == "json" && !json.Valid([]byte(res.Stdout)) {
				t.Fatalf("stdout is not a complete JSON document:\n%s", res.Stdout)
			}
		})
	}
}

// TestQueryInterrupt proves a signal during a large scan fails the process and
// still leaves a complete JSON document. A truncated array must never exit 0.
func TestQueryInterrupt(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	base := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)

	// Enough rows that the scan is still running when the signal arrives.
	labels := map[string]string{"host": "h", "scope": "system"}
	samples := make([]wireSample, 300_000)
	for i := range samples {
		samples[i] = wireSample{
			Timestamp: base.Add(time.Second + time.Duration(i)*time.Microsecond),
			Metric:    "system.cpu.percent",
			Value:     float64(i % 100),
			Labels:    labels,
		}
	}
	writeChunk(t, dataDir, "h", "system", base, base.Add(5*time.Minute), samples)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, smobBin, queryArgs(cfg, base, "--format", "json", "--limit", "0")...)
	cmd.WaitDelay = 5 * time.Second
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start query: %v", err)
	}

	var output bytes.Buffer
	sawSample := make(chan struct{})
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		reader := bufio.NewReader(stdout)
		for {
			line, readErr := reader.ReadString('\n')
			output.WriteString(line)
			if strings.HasPrefix(line, "{") {
				select {
				case <-sawSample:
				default:
					close(sawSample)
				}
			}
			if readErr != nil {
				return
			}
		}
	}()

	select {
	case <-sawSample:
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		<-readDone
		_ = cmd.Wait()
		t.Fatalf("query wrote no sample before the timeout\nstderr:\n%s", stderr.String())
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("signal query: %v", err)
	}
	select {
	case <-readDone:
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		<-readDone
		t.Fatalf("query did not stop after SIGINT\nstderr:\n%s", stderr.String())
	}

	exitErr := cmd.Wait()
	var execErr *exec.ExitError
	if !errors.As(exitErr, &execErr) {
		t.Fatalf("query exit = %v, want a non-zero exit\nstderr:\n%s", exitErr, stderr.String())
	}
	if execErr.ExitCode() != 1 {
		t.Fatalf("exit = %d, want 1\nstderr:\n%s", execErr.ExitCode(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "query interrupted") {
		t.Fatalf("stderr does not report the interruption:\n%s", stderr.String())
	}

	decoded := decodeJSONSamples(t, output.String())
	if len(decoded) == 0 || len(decoded) >= len(samples) {
		t.Fatalf("interrupted query returned %d samples, want a partial result", len(decoded))
	}
}

// seedLabelMatcherData writes one process chunk whose pid values exercise
// anchoring and zero padding, and one system chunk whose samples carry neither
// pid nor comm.
func seedLabelMatcherData(t *testing.T) (cfg string, base time.Time) {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg = writeConfig(t, "data_dir: "+dataDir+"\n")
	base = time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)

	processLabels := func(pid, comm string) map[string]string {
		return map[string]string{"host": "gamma", "scope": "process", "pid": pid, "comm": comm}
	}
	writeChunk(t, dataDir, "gamma", "process", base, base.Add(5*time.Minute), []wireSample{
		{Timestamp: base.Add(time.Second), Metric: "process.cpu.percent", Value: 1, IntervalMS: 10_000, Labels: processLabels("7", "init")},
		{Timestamp: base.Add(2 * time.Second), Metric: "process.cpu.percent", Value: 2, IntervalMS: 10_000, Labels: processLabels("12", "nginx")},
		{Timestamp: base.Add(3 * time.Second), Metric: "process.cpu.percent", Value: 3, IntervalMS: 10_000, Labels: processLabels("0123", "postgres")},
		{Timestamp: base.Add(4 * time.Second), Metric: "process.cpu.percent", Value: 4, IntervalMS: 10_000, Labels: processLabels("123", "nginx worker")},
	})
	writeChunk(t, dataDir, "alpha", "system", base, base.Add(5*time.Minute), []wireSample{
		{
			Timestamp: base.Add(time.Second),
			Metric:    "system.memory.used_bytes",
			Value:     1024,
			Labels:    map[string]string{"host": "alpha", "scope": "system"},
		},
	})
	return cfg, base
}

// querySamples runs a JSON query and fails on a non-zero exit.
func querySamples(t *testing.T, ctx context.Context, cfg string, base time.Time, extra ...string) []wireSample {
	t.Helper()
	res := runSmob(t, ctx, queryArgs(cfg, base, append(extra, "--format", "json")...)...)
	if res.ExitCode != 0 {
		t.Fatalf("query %v exit = %d\nstderr:\n%s", extra, res.ExitCode, res.Stderr)
	}
	return decodeJSONSamples(t, res.Stdout)
}

// labelValues returns one label value per sample, in output order.
func labelValues(samples []wireSample, name string) []string {
	values := make([]string, 0, len(samples))
	for _, sample := range samples {
		values = append(values, sample.Labels[name])
	}
	return values
}

// TestQueryLabelRegexContract pins regex anchoring, zero-padded pid values,
// comm alternation, a comm with spaces, and AND across repeated --label flags.
func TestQueryLabelRegexContract(t *testing.T) {
	cfg, base := seedLabelMatcherData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cases := []struct {
		name  string
		args  []string
		label string
		want  []string
	}{
		{
			name:  "regex anchored at start skips zero padded",
			args:  []string{"--label", "pid~^12"},
			label: "pid",
			want:  []string{"12", "123"},
		},
		{
			name:  "regex unanchored matches inside zero padded",
			args:  []string{"--label", "pid~12"},
			label: "pid",
			want:  []string{"12", "0123", "123"},
		},
		{
			name:  "exact zero padded value",
			args:  []string{"--label", "pid=0123"},
			label: "pid",
			want:  []string{"0123"},
		},
		{
			name:  "exact unpadded value is not normalised",
			args:  []string{"--label", "pid=123"},
			label: "pid",
			want:  []string{"123"},
		},
		{
			name:  "comm alternation is unanchored",
			args:  []string{"--label", "comm~nginx|postgres"},
			label: "comm",
			want:  []string{"nginx", "postgres", "nginx worker"},
		},
		{
			name:  "comm with spaces is one exact argument",
			args:  []string{"--label", "comm=nginx worker"},
			label: "comm",
			want:  []string{"nginx worker"},
		},
		{
			name:  "repeated labels are ANDed",
			args:  []string{"--label", "comm~nginx", "--label", "pid~^12$"},
			label: "pid",
			want:  []string{"12"},
		},
		{
			name:  "exact pid rejects another comm match",
			args:  []string{"--label", "comm~nginx", "--label", "pid=0123"},
			label: "pid",
			want:  nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			samples := querySamples(t, ctx, cfg, base, tc.args...)
			got := labelValues(samples, tc.label)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("%s values = %v, want %v", tc.label, got, tc.want)
			}
		})
	}
}

// TestQueryLabelLimitFiltersFirst proves --limit counts only samples that pass
// the label matcher: a non-matching pid at an earlier timestamp must not consume
// the one slot.
func TestQueryLabelLimitFiltersFirst(t *testing.T) {
	cfg, base := seedLabelMatcherData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	samples := querySamples(t, ctx, cfg, base, "--label", "pid~12", "--limit", "1")
	if len(samples) != 1 {
		t.Fatalf("limit 1 returned %d samples, want 1", len(samples))
	}
	if got := samples[0].Labels["pid"]; got != "12" {
		t.Fatalf("limited sample pid = %q, want 12", got)
	}
	if want := base.Add(2 * time.Second); !samples[0].Timestamp.Equal(want) {
		t.Fatalf("limited sample timestamp = %s, want %s", samples[0].Timestamp, want)
	}

	// The same limit without the matcher takes the earliest sample, whose pid 7
	// does not match, so the case above really proves filtering precedes limiting.
	unfiltered := querySamples(t, ctx, cfg, base, "--limit", "1")
	if len(unfiltered) != 1 || unfiltered[0].Labels["pid"] != "7" {
		t.Fatalf("unfiltered limit 1 = %+v, want the earliest non-matching sample", unfiltered)
	}
}

// TestQueryHostSelectsPathsNotLabels proves --host is a path selector that also
// filters the host label, while --label host=... is only a label predicate: the
// two channels are independent and ANDed when combined.
func TestQueryHostSelectsPathsNotLabels(t *testing.T) {
	cfg, base := seedLabelMatcherData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cases := []struct {
		name string
		args []string
		want []string
	}{
		{"host flag selects gamma", []string{"--host", "gamma"}, []string{"gamma", "gamma", "gamma", "gamma"}},
		{"host label matcher also matches gamma", []string{"--label", "host=gamma"}, []string{"gamma", "gamma", "gamma", "gamma"}},
		{"host flag selects alpha", []string{"--host", "alpha"}, []string{"alpha"}},
		{"host label can select nothing", []string{"--label", "host=nosuch"}, nil},
		{"host flag and label matcher are ANDed", []string{"--host", "gamma", "--label", "host=alpha"}, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := labelValues(querySamples(t, ctx, cfg, base, tc.args...), "host")
			if !slices.Equal(got, tc.want) {
				t.Fatalf("host values = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestQueryLabelInvalidMatcher pins the exit code for an invalid regex and for
// the unsupported negated operators. `!=` and `!~` must be rejected rather than
// silently read as a label named `comm!`.
func TestQueryLabelInvalidMatcher(t *testing.T) {
	cfg, base := seedLabelMatcherData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	invalid := runSmob(t, ctx, queryArgs(cfg, base, "--label", "comm~[")...)
	if invalid.ExitCode != 2 {
		t.Fatalf("invalid regex exit = %d, want 2\nstderr:\n%s", invalid.ExitCode, invalid.Stderr)
	}
	if !strings.Contains(invalid.Stderr, "label comm") {
		t.Fatalf("stderr does not name the label:\n%s", invalid.Stderr)
	}
	if invalid.Stdout != "" {
		t.Fatalf("invalid regex wrote stdout:\n%s", invalid.Stdout)
	}

	for _, raw := range []string{"comm!=nginx", "comm!~nginx"} {
		t.Run(raw, func(t *testing.T) {
			res := runSmob(t, ctx, queryArgs(cfg, base, "--label", raw, "--format", "json")...)
			if res.ExitCode != 2 {
				t.Fatalf("exit = %d, want 2\nstderr:\n%s", res.ExitCode, res.Stderr)
			}
			if !strings.Contains(res.Stderr, "only name=value and name~regex are supported") {
				t.Fatalf("stderr does not explain the supported operators:\n%s", res.Stderr)
			}
			if res.Stdout != "" {
				t.Fatalf("unsupported matcher wrote stdout:\n%s", res.Stdout)
			}
		})
	}
}

// TestQueryMissingLabelIsEmpty pins that an exact empty label match selects
// samples without that label, and that a non-empty regex excludes them.
func TestQueryMissingLabelIsEmpty(t *testing.T) {
	cfg, base := seedLabelMatcherData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	missing := querySamples(t, ctx, cfg, base, "--label", "comm=")
	if len(missing) != 1 {
		t.Fatalf("--label comm= returned %d samples, want the one system sample", len(missing))
	}
	if _, ok := missing[0].Labels["comm"]; ok {
		t.Fatalf("system sample unexpectedly carries comm=%q", missing[0].Labels["comm"])
	}
	if got := missing[0].Labels["scope"]; got != "system" {
		t.Fatalf("missing-label sample scope = %q, want system", got)
	}

	absent := querySamples(t, ctx, cfg, base, "--scope", "process", "--label", "comm=")
	if len(absent) != 0 {
		t.Fatalf("--scope process --label comm= returned %d samples, want none", len(absent))
	}

	present := querySamples(t, ctx, cfg, base, "--label", "comm~.")
	if got := labelValues(present, "comm"); !slices.Equal(got, []string{"init", "nginx", "postgres", "nginx worker"}) {
		t.Fatalf("--label comm~. comm values = %v, want only the process samples", got)
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
