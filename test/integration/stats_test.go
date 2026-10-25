//go:build integration

package integration

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// wireSummary mirrors the documented stats row schema. The duplication is
// intentional: a schema change must break the contract test.
type wireSummary struct {
	Metric string  `json:"metric"`
	Count  int64   `json:"count"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
	Unit   string  `json:"-"`
}

// seedStatsData writes two chunks with deterministic samples across hosts and
// scopes. The process CPU series carry different interval_ms values so an
// unweighted mean is distinguishable from an interval-weighted one.
func seedStatsData(t *testing.T) (cfg string, base time.Time) {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg = writeConfig(t, "data_dir: "+dataDir+"\n")
	base = time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)
	start, end := base, base.Add(5*time.Minute)

	writeChunk(t, dataDir, "alpha", "system", start, end, []wireSample{
		{
			Timestamp:  base.Add(time.Second),
			Metric:     "system.cpu.percent",
			Value:      10,
			IntervalMS: 10_000,
			Labels:     map[string]string{"host": "alpha", "scope": "system", "cpu": "all"},
		},
		{
			Timestamp: base.Add(time.Second),
			Metric:    "system.memory.used_bytes",
			Value:     100,
			Labels:    map[string]string{"host": "alpha", "scope": "system"},
		},
		{
			Timestamp:  base.Add(2 * time.Second),
			Metric:     "system.cpu.percent",
			Value:      20,
			IntervalMS: 10_000,
			Labels:     map[string]string{"host": "alpha", "scope": "system", "cpu": "0"},
		},
		{
			Timestamp: base.Add(2 * time.Second),
			Metric:    "system.memory.used_bytes",
			Value:     300,
			Labels:    map[string]string{"host": "alpha", "scope": "system"},
		},
		{
			Timestamp:  base.Add(3 * time.Second),
			Metric:     "system.cpu.percent",
			Value:      30,
			IntervalMS: 10_000,
			Labels:     map[string]string{"host": "alpha", "scope": "system", "cpu": "all"},
		},
	})
	writeChunk(t, dataDir, "beta", "process", start, end, []wireSample{
		{
			Timestamp:  base.Add(time.Second),
			Metric:     "process.cpu.percent",
			Value:      50,
			IntervalMS: 30_000,
			Labels:     map[string]string{"host": "beta", "scope": "process", "pid": "42", "comm": "nginx"},
		},
		{
			Timestamp: base.Add(time.Second),
			Metric:    "process.memory.rss_bytes",
			Value:     1024,
			Labels:    map[string]string{"host": "beta", "scope": "process", "pid": "42", "comm": "nginx"},
		},
		{
			Timestamp:  base.Add(2 * time.Second),
			Metric:     "process.cpu.percent",
			Value:      150,
			IntervalMS: 10_000,
			Labels:     map[string]string{"host": "beta", "scope": "process", "pid": "43", "comm": "postgres"},
		},
	})
	return cfg, base
}

func statsArgs(cfg string, base time.Time, extra ...string) []string {
	args := []string{"stats", "--config", cfg}
	args = append(args, queryRange(base)...)
	return append(args, extra...)
}

func runStatsJSON(t *testing.T, ctx context.Context, cfg string, base time.Time, extra ...string) []wireSummary {
	t.Helper()
	args := statsArgs(cfg, base, append(extra, "--format", "json")...)
	res := runSmob(t, ctx, args...)
	if res.ExitCode != 0 {
		t.Fatalf("stats exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	return decodeJSONSummaries(t, res.Stdout)
}

func decodeJSONSummaries(t *testing.T, out string) []wireSummary {
	t.Helper()
	var summaries []wireSummary
	if err := json.Unmarshal([]byte(out), &summaries); err != nil {
		t.Fatalf("decode json stats output: %v\n%s", err, out)
	}
	return summaries
}

func decodeJSONLSummaries(t *testing.T, out string) []wireSummary {
	t.Helper()
	var summaries []wireSummary
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		var summary wireSummary
		if err := json.Unmarshal([]byte(line), &summary); err != nil {
			t.Fatalf("decode jsonl stats line: %v\n%s", err, line)
		}
		summaries = append(summaries, summary)
	}
	return summaries
}

func decodeCSVSummaries(t *testing.T, out string) []wireSummary {
	t.Helper()
	rows, err := csv.NewReader(strings.NewReader(out)).ReadAll()
	if err != nil {
		t.Fatalf("decode csv stats output: %v\n%s", err, out)
	}
	wantHeader := []string{"metric", "count", "min", "max", "mean"}
	if len(rows) == 0 || !slices.Equal(rows[0], wantHeader) {
		t.Fatalf("csv header = %v, want %v", rows, wantHeader)
	}

	var summaries []wireSummary
	for _, row := range rows[1:] {
		if len(row) != len(wantHeader) {
			t.Fatalf("csv row = %v, want %d columns", row, len(wantHeader))
		}
		count, err := strconv.ParseInt(row[1], 10, 64)
		if err != nil {
			t.Fatalf("csv count %q: %v", row[1], err)
		}
		minValue, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			t.Fatalf("csv min %q: %v", row[2], err)
		}
		maxValue, err := strconv.ParseFloat(row[3], 64)
		if err != nil {
			t.Fatalf("csv max %q: %v", row[3], err)
		}
		mean, err := strconv.ParseFloat(row[4], 64)
		if err != nil {
			t.Fatalf("csv mean %q: %v", row[4], err)
		}
		summaries = append(summaries, wireSummary{
			Metric: row[0],
			Count:  count,
			Min:    minValue,
			Max:    maxValue,
			Mean:   mean,
		})
	}
	return summaries
}

func decodeTableSummaries(t *testing.T, out string) []wireSummary {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	header := strings.Fields(lines[0])
	wantHeader := []string{"METRIC", "COUNT", "MIN", "MAX", "MEAN", "UNIT"}
	if !slices.Equal(header, wantHeader) {
		t.Fatalf("table header = %v, want %v", header, wantHeader)
	}

	var summaries []wireSummary
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			t.Fatalf("table row = %q, want at least 5 columns", line)
		}
		count, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			t.Fatalf("table count %q: %v", fields[1], err)
		}
		minValue, err := strconv.ParseFloat(fields[2], 64)
		if err != nil {
			t.Fatalf("table min %q: %v", fields[2], err)
		}
		maxValue, err := strconv.ParseFloat(fields[3], 64)
		if err != nil {
			t.Fatalf("table max %q: %v", fields[3], err)
		}
		mean, err := strconv.ParseFloat(fields[4], 64)
		if err != nil {
			t.Fatalf("table mean %q: %v", fields[4], err)
		}
		unit := ""
		if len(fields) > 5 {
			unit = fields[5]
		}
		summaries = append(summaries, wireSummary{
			Metric: fields[0],
			Count:  count,
			Min:    minValue,
			Max:    maxValue,
			Mean:   mean,
			Unit:   unit,
		})
	}
	return summaries
}

func assertSameSummaries(t *testing.T, got, want []wireSummary) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d summaries, want %d\ngot: %+v", len(got), len(want), got)
	}
	for i := range got {
		if got[i].Metric != want[i].Metric ||
			got[i].Count != want[i].Count ||
			got[i].Min != want[i].Min ||
			got[i].Max != want[i].Max ||
			got[i].Mean != want[i].Mean {
			t.Fatalf("summary %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestStatsFormats(t *testing.T) {
	cfg, base := seedStatsData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	want := []wireSummary{
		{Metric: "process.cpu.percent", Count: 2, Min: 50, Max: 150, Mean: 75},
		{Metric: "process.memory.rss_bytes", Count: 1, Min: 1024, Max: 1024, Mean: 1024},
		{Metric: "system.cpu.percent", Count: 3, Min: 10, Max: 30, Mean: 20},
		{Metric: "system.memory.used_bytes", Count: 2, Min: 100, Max: 300, Mean: 200},
	}

	table := runSmob(t, ctx, statsArgs(cfg, base, "--format", "table")...)
	if table.ExitCode != 0 {
		t.Fatalf("table stats exit = %d\nstderr:\n%s", table.ExitCode, table.Stderr)
	}
	tableSummaries := decodeTableSummaries(t, table.Stdout)
	assertSameSummaries(t, tableSummaries, want)
	wantUnits := map[string]string{
		"process.cpu.percent": "%",
		"system.cpu.percent":  "%",
	}
	for _, summary := range tableSummaries {
		if unit, ok := wantUnits[summary.Metric]; ok && summary.Unit != unit {
			t.Fatalf("unit for %s = %q, want %q", summary.Metric, summary.Unit, unit)
		}
	}

	jsonOut := runSmob(t, ctx, statsArgs(cfg, base, "--format", "json")...)
	if jsonOut.ExitCode != 0 {
		t.Fatalf("json stats exit = %d\nstderr:\n%s", jsonOut.ExitCode, jsonOut.Stderr)
	}
	assertSameSummaries(t, decodeJSONSummaries(t, jsonOut.Stdout), want)

	jsonlOut := runSmob(t, ctx, statsArgs(cfg, base, "--format", "jsonl")...)
	if jsonlOut.ExitCode != 0 {
		t.Fatalf("jsonl stats exit = %d\nstderr:\n%s", jsonlOut.ExitCode, jsonlOut.Stderr)
	}
	assertSameSummaries(t, decodeJSONLSummaries(t, jsonlOut.Stdout), want)

	csvOut := runSmob(t, ctx, statsArgs(cfg, base, "--format", "csv")...)
	if csvOut.ExitCode != 0 {
		t.Fatalf("csv stats exit = %d\nstderr:\n%s", csvOut.ExitCode, csvOut.Stderr)
	}
	assertSameSummaries(t, decodeCSVSummaries(t, csvOut.Stdout), want)
}

func TestStatsMeanMatchesAggAvg(t *testing.T) {
	cfg, base := seedStatsData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cases := []struct {
		name     string
		metric   string
		wantMean float64
	}{
		// The samples are 50 with interval_ms=30000 and 150 with interval_ms=10000.
		// Interval weighting produces 75; the unweighted mean would be 100.
		{"weighted rate", "process.cpu.percent", 75},
		// Gauges carry no interval, so their mean stays arithmetic: (100+300)/2.
		{"unweighted gauge", "system.memory.used_bytes", 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			summaries := runStatsJSON(t, ctx, cfg, base, "--metric", tc.metric)
			if len(summaries) != 1 {
				t.Fatalf("got %d summaries, want 1", len(summaries))
			}
			if summaries[0].Mean != tc.wantMean {
				t.Fatalf("stats mean = %v, want %v", summaries[0].Mean, tc.wantMean)
			}

			aggregates := runAggJSON(t, ctx, cfg, base, "--agg", "avg", "--metric", tc.metric)
			if len(aggregates) != 1 {
				t.Fatalf("got %d aggregates, want 1", len(aggregates))
			}
			if summaries[0].Mean != aggregates[0].Value {
				t.Fatalf("stats mean %v does not equal query --agg avg %v", summaries[0].Mean, aggregates[0].Value)
			}
			if summaries[0].Count != aggregates[0].Count {
				t.Fatalf("stats count %d does not equal query --agg count %d", summaries[0].Count, aggregates[0].Count)
			}
		})
	}
}

func TestStatsRejectsNonFinite(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	base := time.Date(2026, 10, 23, 16, 0, 0, 0, time.UTC)

	// 1e308 x interval_ms overflows float64, so the interval-weighted mean
	// becomes +Inf while min and max stay finite.
	writeChunk(t, dataDir, "alpha", "process", base, base.Add(5*time.Minute), []wireSample{
		{
			Timestamp:  base.Add(time.Second),
			Metric:     "process.cpu.percent",
			Value:      1e308,
			IntervalMS: 30_000,
			Labels:     map[string]string{"host": "alpha", "scope": "process", "pid": "42", "comm": "nginx"},
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	res := runSmob(t, ctx, statsArgs(cfg, base, "--metric", "process.cpu.percent", "--format", "json")...)
	if res.ExitCode != 1 {
		t.Fatalf("non-finite stats exit = %d, want 1\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if res.Stdout != "" {
		t.Fatalf("non-finite stats wrote a partial document: %q", res.Stdout)
	}
	if !strings.Contains(res.Stderr, "process.cpu.percent") || !strings.Contains(res.Stderr, "not finite") {
		t.Fatalf("non-finite stats stderr = %q, want the metric name and a non-finite error", res.Stderr)
	}
}

func TestStatsFilters(t *testing.T) {
	cfg, base := seedStatsData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cases := []struct {
		name string
		args []string
		want []wireSummary
	}{
		{
			name: "metric",
			args: []string{"--metric", "process.cpu.percent"},
			want: []wireSummary{{Metric: "process.cpu.percent", Count: 2, Min: 50, Max: 150, Mean: 75}},
		},
		{
			name: "host",
			args: []string{"--host", "alpha"},
			want: []wireSummary{
				{Metric: "system.cpu.percent", Count: 3, Min: 10, Max: 30, Mean: 20},
				{Metric: "system.memory.used_bytes", Count: 2, Min: 100, Max: 300, Mean: 200},
			},
		},
		{
			name: "scope",
			args: []string{"--scope", "process"},
			want: []wireSummary{
				{Metric: "process.cpu.percent", Count: 2, Min: 50, Max: 150, Mean: 75},
				{Metric: "process.memory.rss_bytes", Count: 1, Min: 1024, Max: 1024, Mean: 1024},
			},
		},
		{
			name: "exact label",
			args: []string{"--label", "cpu=all"},
			want: []wireSummary{{Metric: "system.cpu.percent", Count: 2, Min: 10, Max: 30, Mean: 20}},
		},
		{
			name: "regex label",
			args: []string{"--label", "comm~nginx|postgres"},
			want: []wireSummary{
				{Metric: "process.cpu.percent", Count: 2, Min: 50, Max: 150, Mean: 75},
				{Metric: "process.memory.rss_bytes", Count: 1, Min: 1024, Max: 1024, Mean: 1024},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertSameSummaries(t, runStatsJSON(t, ctx, cfg, base, tc.args...), tc.want)
		})
	}

	t.Run("time range", func(t *testing.T) {
		args := []string{
			"stats", "--config", cfg,
			"--since", base.Add(2 * time.Second).Format(time.RFC3339),
			"--until", base.Add(3 * time.Second).Format(time.RFC3339),
			"--format", "json",
		}
		res := runSmob(t, ctx, args...)
		if res.ExitCode != 0 {
			t.Fatalf("stats exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
		}
		want := []wireSummary{
			{Metric: "process.cpu.percent", Count: 1, Min: 150, Max: 150, Mean: 150},
			{Metric: "system.cpu.percent", Count: 1, Min: 20, Max: 20, Mean: 20},
			{Metric: "system.memory.used_bytes", Count: 1, Min: 300, Max: 300, Mean: 300},
		}
		assertSameSummaries(t, decodeJSONSummaries(t, res.Stdout), want)
	})
}

func TestStatsLimit(t *testing.T) {
	cfg, base := seedStatsData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	limited := runStatsJSON(t, ctx, cfg, base, "--metric", "system.cpu.percent", "--limit", "2")
	want := []wireSummary{{Metric: "system.cpu.percent", Count: 2, Min: 10, Max: 20, Mean: 15}}
	assertSameSummaries(t, limited, want)

	unlimited := runStatsJSON(t, ctx, cfg, base, "--metric", "system.cpu.percent", "--limit", "0")
	want = []wireSummary{{Metric: "system.cpu.percent", Count: 3, Min: 10, Max: 30, Mean: 20}}
	assertSameSummaries(t, unlimited, want)

	// A scan-wide limit stops after the first two samples in query order, so
	// only the metrics those samples belong to appear.
	truncated := runStatsJSON(t, ctx, cfg, base, "--limit", "2")
	want = []wireSummary{
		{Metric: "process.cpu.percent", Count: 1, Min: 50, Max: 50, Mean: 50},
		{Metric: "process.memory.rss_bytes", Count: 1, Min: 1024, Max: 1024, Mean: 1024},
	}
	assertSameSummaries(t, truncated, want)
}

func TestStatsEmpty(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("create empty data dir: %v", err)
	}
	cfg := writeConfig(t, "data_dir: "+dataDir+"\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	table := runSmob(t, ctx, "stats", "--config", cfg)
	if table.ExitCode != 0 {
		t.Fatalf("empty table stats exit = %d\nstderr:\n%s", table.ExitCode, table.Stderr)
	}
	if tableRows(table.Stdout) != 0 {
		t.Fatalf("empty table stats = %q, want a header only", table.Stdout)
	}
	header := strings.Fields(strings.Split(strings.TrimSpace(table.Stdout), "\n")[0])
	wantHeader := []string{"METRIC", "COUNT", "MIN", "MAX", "MEAN", "UNIT"}
	if !slices.Equal(header, wantHeader) {
		t.Fatalf("table header = %v, want %v", header, wantHeader)
	}

	jsonOut := runSmob(t, ctx, "stats", "--config", cfg, "--format", "json")
	if jsonOut.ExitCode != 0 {
		t.Fatalf("empty json stats exit = %d\nstderr:\n%s", jsonOut.ExitCode, jsonOut.Stderr)
	}
	if jsonOut.Stdout != "[]\n" {
		t.Fatalf("empty json stats = %q, want %q", jsonOut.Stdout, "[]\n")
	}

	jsonlOut := runSmob(t, ctx, "stats", "--config", cfg, "--format", "jsonl")
	if jsonlOut.ExitCode != 0 {
		t.Fatalf("empty jsonl stats exit = %d\nstderr:\n%s", jsonlOut.ExitCode, jsonlOut.Stderr)
	}
	if strings.TrimSpace(jsonlOut.Stdout) != "" {
		t.Fatalf("empty jsonl stats = %q, want no rows", jsonlOut.Stdout)
	}

	csvOut := runSmob(t, ctx, "stats", "--config", cfg, "--format", "csv")
	if csvOut.ExitCode != 0 {
		t.Fatalf("empty csv stats exit = %d\nstderr:\n%s", csvOut.ExitCode, csvOut.Stderr)
	}
	if got := decodeCSVSummaries(t, csvOut.Stdout); len(got) != 0 {
		t.Fatalf("empty csv stats returned %d rows, want 0", len(got))
	}
}

// TestStatsInterrupt proves a signal during a large scan fails the process
// instead of printing a summary of the samples it managed to read as if it
// covered the whole range.
func TestStatsInterrupt(t *testing.T) {
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

	d := startSmob(t, ctx, statsArgs(cfg, base,
		"--format", "json", "--limit", "0", "--log-format", "json", "--log-level", "debug")...)
	// The debug record is written while the command runs, before the store is
	// opened, so the signal cannot arrive before the handler is installed.
	waitFor(t, 30*time.Second, "stats to load its configuration", func() bool {
		return hasLogMessage(d.stderr.String(), "loaded configuration")
	})
	d.signal(t, os.Interrupt)

	if code := d.waitExit(t, 10*time.Second); code != 1 {
		t.Fatalf("interrupted stats exit = %d, want 1\nstderr:\n%s", code, d.stderr.String())
	}
	if got := d.stdout.String(); got != "" {
		t.Fatalf("interrupted stats wrote a document: %q", got)
	}
	if !strings.Contains(d.stderr.String(), "stats interrupted") {
		t.Fatalf("stderr does not report the interruption:\n%s", d.stderr.String())
	}
}

func TestStatsLive(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := collectConfig(t, dataDir, "/proc")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	collect := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if collect.ExitCode != 0 {
		t.Fatalf("collect --once exit = %d\nstderr:\n%s", collect.ExitCode, collect.Stderr)
	}

	res := runSmob(t, ctx, "stats", "--config", cfg, "--since", "1h", "--format", "json")
	if res.ExitCode != 0 {
		t.Fatalf("live stats exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	summaries := decodeJSONSummaries(t, res.Stdout)
	if len(summaries) == 0 {
		t.Fatal("live stats returned no summaries")
	}

	var memory *wireSummary
	for i := range summaries {
		summary := &summaries[i]
		if i > 0 && summaries[i-1].Metric > summary.Metric {
			t.Fatalf("summaries are not sorted: %s before %s", summaries[i-1].Metric, summary.Metric)
		}
		if summary.Count < 1 {
			t.Fatalf("%s has count %d, want at least 1", summary.Metric, summary.Count)
		}
		if math.IsNaN(summary.Min) || math.IsInf(summary.Min, 0) ||
			math.IsNaN(summary.Max) || math.IsInf(summary.Max, 0) ||
			math.IsNaN(summary.Mean) || math.IsInf(summary.Mean, 0) {
			t.Fatalf("%s has a non-finite summary: %+v", summary.Metric, summary)
		}
		if summary.Min > summary.Max || summary.Mean < summary.Min || summary.Mean > summary.Max {
			t.Fatalf("%s has inconsistent extrema: %+v", summary.Metric, summary)
		}
		if summary.Metric == "system.memory.used_bytes" {
			memory = summary
		}
	}
	if memory == nil {
		t.Fatalf("live stats did not report system.memory.used_bytes: %+v", summaries)
	}
	if memory.Min <= 0 {
		t.Fatalf("system.memory.used_bytes min = %v, want greater than zero", memory.Min)
	}
}
