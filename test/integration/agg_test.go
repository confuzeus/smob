//go:build integration

package integration

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"maps"
	"math"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// wireAggregate mirrors the documented query --agg row schema. The duplication
// is intentional: a schema change must break the contract test.
type wireAggregate struct {
	Metric string            `json:"metric"`
	Labels map[string]string `json:"labels"`
	Count  int64             `json:"count"`
	Value  float64           `json:"value"`
	Unit   string            `json:"-"`
}

func aggArgs(cfg string, base time.Time, extra ...string) []string {
	args := []string{"query", "--config", cfg}
	args = append(args, queryRange(base)...)
	return append(args, extra...)
}

func runAggJSON(t *testing.T, ctx context.Context, cfg string, base time.Time, extra ...string) []wireAggregate {
	t.Helper()
	args := aggArgs(cfg, base, append(extra, "--format", "json")...)
	res := runSmob(t, ctx, args...)
	if res.ExitCode != 0 {
		t.Fatalf("query --agg exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	return decodeJSONAggregates(t, res.Stdout)
}

func decodeJSONAggregates(t *testing.T, out string) []wireAggregate {
	t.Helper()
	var aggregates []wireAggregate
	if err := json.Unmarshal([]byte(out), &aggregates); err != nil {
		t.Fatalf("decode json aggregate output: %v\n%s", err, out)
	}
	return aggregates
}

func decodeJSONLAggregates(t *testing.T, out string) []wireAggregate {
	t.Helper()
	var aggregates []wireAggregate
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		var aggregate wireAggregate
		if err := json.Unmarshal([]byte(line), &aggregate); err != nil {
			t.Fatalf("decode jsonl aggregate line: %v\n%s", err, line)
		}
		aggregates = append(aggregates, aggregate)
	}
	return aggregates
}

func decodeCSVAggregates(t *testing.T, out string) []wireAggregate {
	t.Helper()
	rows, err := csv.NewReader(strings.NewReader(out)).ReadAll()
	if err != nil {
		t.Fatalf("decode csv aggregate output: %v\n%s", err, out)
	}
	wantHeader := []string{"metric", "labels", "count", "value"}
	if len(rows) == 0 || !slices.Equal(rows[0], wantHeader) {
		t.Fatalf("csv header = %v, want %v", rows, wantHeader)
	}

	var aggregates []wireAggregate
	for _, row := range rows[1:] {
		if len(row) != len(wantHeader) {
			t.Fatalf("csv row = %v, want %d columns", row, len(wantHeader))
		}
		count, err := strconv.ParseInt(row[2], 10, 64)
		if err != nil {
			t.Fatalf("csv count %q: %v", row[2], err)
		}
		value, err := strconv.ParseFloat(row[3], 64)
		if err != nil {
			t.Fatalf("csv value %q: %v", row[3], err)
		}
		aggregates = append(aggregates, wireAggregate{
			Metric: row[0],
			Labels: parseLabels(row[1]),
			Count:  count,
			Value:  value,
		})
	}
	return aggregates
}

func decodeTableAggregates(t *testing.T, out string) []wireAggregate {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	header := strings.Fields(lines[0])
	wantHeader := []string{"METRIC", "COUNT", "VALUE", "UNIT", "LABELS"}
	if !slices.Equal(header, wantHeader) {
		t.Fatalf("table header = %v, want %v", header, wantHeader)
	}

	var aggregates []wireAggregate
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			t.Fatalf("table row = %q, want at least 3 columns", line)
		}
		count, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			t.Fatalf("table count %q: %v", fields[1], err)
		}
		value, err := strconv.ParseFloat(fields[2], 64)
		if err != nil {
			t.Fatalf("table value %q: %v", fields[2], err)
		}
		unit := ""
		labels := fields[3:]
		if len(labels) > 0 && !strings.Contains(labels[0], "=") {
			unit = labels[0]
			labels = labels[1:]
		}
		aggregates = append(aggregates, wireAggregate{
			Metric: fields[0],
			Labels: parseLabels(strings.Join(labels, ",")),
			Count:  count,
			Value:  value,
			Unit:   unit,
		})
	}
	return aggregates
}

// aggregateUnit mirrors the documented unit derivation for test assertions.
func aggregateUnit(metric string) string {
	switch {
	case strings.HasSuffix(metric, ".bytes_per_second"):
		return "B/s"
	case strings.HasSuffix(metric, ".percent"):
		return "%"
	case strings.HasSuffix(metric, ".bytes"):
		return "B"
	default:
		return ""
	}
}

func assertSameAggregates(t *testing.T, got, want []wireAggregate) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d aggregates, want %d\ngot: %+v", len(got), len(want), got)
	}
	for i := range got {
		if got[i].Metric != want[i].Metric ||
			got[i].Count != want[i].Count ||
			math.Abs(got[i].Value-want[i].Value) > 1e-9 ||
			!maps.Equal(got[i].Labels, want[i].Labels) {
			t.Fatalf("aggregate %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// seedAggData writes deterministic samples for grouping tests. The sample at
// cpu=0/mode=user appears on two hosts and another sample for cpu=0 carries a
// different unlisted mode, so grouping by cpu must merge them. One cpu=1
// sample has no mode label, exercising the empty-string value.
func seedAggData(t *testing.T) (cfg string, base time.Time) {
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
			Labels:     map[string]string{"host": "alpha", "scope": "system", "cpu": "0", "mode": "user"},
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
			Labels:     map[string]string{"host": "alpha", "scope": "system", "cpu": "0", "mode": "system"},
		},
		{
			Timestamp:  base.Add(2 * time.Second),
			Metric:     "system.cpu.percent",
			Value:      30,
			IntervalMS: 10_000,
			Labels:     map[string]string{"host": "alpha", "scope": "system", "cpu": "1"},
		},
		{
			Timestamp:  base.Add(3 * time.Second),
			Metric:     "system.cpu.percent",
			Value:      40,
			IntervalMS: 10_000,
			Labels:     map[string]string{"host": "alpha", "scope": "system", "cpu": "1", "mode": "user"},
		},
	})
	writeChunk(t, dataDir, "beta", "system", start, end, []wireSample{
		{
			Timestamp:  base.Add(time.Second),
			Metric:     "system.cpu.percent",
			Value:      70,
			IntervalMS: 10_000,
			Labels:     map[string]string{"host": "beta", "scope": "system", "cpu": "0", "mode": "user"},
		},
	})
	return cfg, base
}

func TestQueryAggValues(t *testing.T) {
	cfg, base := seedStatsData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cases := []struct {
		name   string
		metric string
		op     string
		want   float64
		count  int64
	}{
		// 50@30s + 150@10s is 75 when interval-weighted and 100 unweighted.
		{"weighted rate avg", "process.cpu.percent", "avg", 75, 2},
		{"rate avg with equal intervals", "system.cpu.percent", "avg", 20, 3},
		{"unweighted gauge avg", "system.memory.used_bytes", "avg", 200, 2},
		{"rate min ignores interval", "process.cpu.percent", "min", 50, 2},
		{"rate max ignores interval", "process.cpu.percent", "max", 150, 2},
		// A weighted sum would be 600000 here; 60 pins the unweighted total.
		{"sum ignores interval", "system.cpu.percent", "sum", 60, 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runAggJSON(t, ctx, cfg, base, "--agg", tc.op, "--metric", tc.metric)
			want := []wireAggregate{{
				Metric: tc.metric,
				Labels: map[string]string{},
				Count:  tc.count,
				Value:  tc.want,
			}}
			assertSameAggregates(t, got, want)
		})
	}

	res := runSmob(t, ctx, aggArgs(cfg, base, "--agg", "avg", "--metric", "system.cpu.percent", "--format", "json")...)
	if res.ExitCode != 0 {
		t.Fatalf("query --agg exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	if !strings.Contains(res.Stdout, `"labels":{}`) {
		t.Fatalf("metric-only JSON labels are not an empty object:\n%s", res.Stdout)
	}
}

func TestQueryAggFilters(t *testing.T) {
	cfg, base := seedStatsData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cases := []struct {
		name string
		args []string
		want []wireAggregate
	}{
		{
			name: "host",
			args: []string{"--host", "alpha"},
			want: []wireAggregate{
				{Metric: "system.cpu.percent", Labels: map[string]string{}, Count: 3, Value: 20},
				{Metric: "system.memory.used_bytes", Labels: map[string]string{}, Count: 2, Value: 200},
			},
		},
		{
			name: "scope",
			args: []string{"--scope", "process"},
			want: []wireAggregate{
				{Metric: "process.cpu.percent", Labels: map[string]string{}, Count: 2, Value: 75},
				{Metric: "process.memory.rss_bytes", Labels: map[string]string{}, Count: 1, Value: 1024},
			},
		},
		{
			name: "label",
			args: []string{"--label", "cpu=all"},
			want: []wireAggregate{
				{Metric: "system.cpu.percent", Labels: map[string]string{}, Count: 2, Value: 20},
			},
		},
		{
			name: "metric and label",
			args: []string{"--metric", "system.cpu.percent", "--label", "cpu=all"},
			want: []wireAggregate{
				{Metric: "system.cpu.percent", Labels: map[string]string{}, Count: 2, Value: 20},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runAggJSON(t, ctx, cfg, base, append(tc.args, "--agg", "avg")...)
			assertSameAggregates(t, got, tc.want)
		})
	}
}

func TestQueryAggGrouping(t *testing.T) {
	cfg, base := seedAggData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	t.Run("metric only", func(t *testing.T) {
		got := runAggJSON(t, ctx, cfg, base, "--agg", "avg")
		want := []wireAggregate{
			{Metric: "system.cpu.percent", Labels: map[string]string{}, Count: 5, Value: 34},
			{Metric: "system.memory.used_bytes", Labels: map[string]string{}, Count: 1, Value: 100},
		}
		assertSameAggregates(t, got, want)
	})

	t.Run("one label with unlisted labels merged", func(t *testing.T) {
		got := runAggJSON(t, ctx, cfg, base, "--agg", "avg", "--group-by", "cpu")
		want := []wireAggregate{
			{Metric: "system.cpu.percent", Labels: map[string]string{"cpu": "0"}, Count: 3, Value: 100.0 / 3},
			{Metric: "system.cpu.percent", Labels: map[string]string{"cpu": "1"}, Count: 2, Value: 35},
			{Metric: "system.memory.used_bytes", Labels: map[string]string{"cpu": ""}, Count: 1, Value: 100},
		}
		assertSameAggregates(t, got, want)
	})

	t.Run("missing label is empty string", func(t *testing.T) {
		got := runAggJSON(t, ctx, cfg, base, "--agg", "avg", "--group-by", "mode")
		want := []wireAggregate{
			{Metric: "system.cpu.percent", Labels: map[string]string{"mode": ""}, Count: 1, Value: 30},
			{Metric: "system.cpu.percent", Labels: map[string]string{"mode": "system"}, Count: 1, Value: 20},
			{Metric: "system.cpu.percent", Labels: map[string]string{"mode": "user"}, Count: 3, Value: 40},
			{Metric: "system.memory.used_bytes", Labels: map[string]string{"mode": ""}, Count: 1, Value: 100},
		}
		assertSameAggregates(t, got, want)
	})

	t.Run("multiple labels are ordered canonically", func(t *testing.T) {
		got := runAggJSON(t, ctx, cfg, base, "--agg", "avg", "--group-by", "cpu,mode")
		want := []wireAggregate{
			{Metric: "system.cpu.percent", Labels: map[string]string{"cpu": "0", "mode": "system"}, Count: 1, Value: 20},
			{Metric: "system.cpu.percent", Labels: map[string]string{"cpu": "0", "mode": "user"}, Count: 2, Value: 40},
			{Metric: "system.cpu.percent", Labels: map[string]string{"cpu": "1", "mode": ""}, Count: 1, Value: 30},
			{Metric: "system.cpu.percent", Labels: map[string]string{"cpu": "1", "mode": "user"}, Count: 1, Value: 40},
			{Metric: "system.memory.used_bytes", Labels: map[string]string{"cpu": "", "mode": ""}, Count: 1, Value: 100},
		}
		assertSameAggregates(t, got, want)
	})

	t.Run("metric is implied and may be listed", func(t *testing.T) {
		implicit := runAggJSON(t, ctx, cfg, base, "--agg", "avg", "--group-by", "cpu")
		explicit := runAggJSON(t, ctx, cfg, base, "--agg", "avg", "--group-by", "metric,cpu")
		assertSameAggregates(t, explicit, implicit)

		metricOnly := runAggJSON(t, ctx, cfg, base, "--agg", "avg", "--group-by", "metric")
		noGroup := runAggJSON(t, ctx, cfg, base, "--agg", "avg")
		assertSameAggregates(t, metricOnly, noGroup)
	})
}

func TestQueryAggFormats(t *testing.T) {
	cfg, base := seedAggData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	jsonOut := runSmob(t, ctx, aggArgs(cfg, base, "--agg", "avg", "--group-by", "cpu", "--format", "json")...)
	if jsonOut.ExitCode != 0 {
		t.Fatalf("json aggregate exit = %d\nstderr:\n%s", jsonOut.ExitCode, jsonOut.Stderr)
	}
	want := decodeJSONAggregates(t, jsonOut.Stdout)

	table := runSmob(t, ctx, aggArgs(cfg, base, "--agg", "avg", "--group-by", "cpu", "--format", "table")...)
	if table.ExitCode != 0 {
		t.Fatalf("table aggregate exit = %d\nstderr:\n%s", table.ExitCode, table.Stderr)
	}
	tableAggregates := decodeTableAggregates(t, table.Stdout)
	assertSameAggregates(t, tableAggregates, want)
	for _, aggregate := range tableAggregates {
		if wantUnit := aggregateUnit(aggregate.Metric); aggregate.Unit != wantUnit {
			t.Fatalf("unit for %s = %q, want %q", aggregate.Metric, aggregate.Unit, wantUnit)
		}
	}

	jsonlOut := runSmob(t, ctx, aggArgs(cfg, base, "--agg", "avg", "--group-by", "cpu", "--format", "jsonl")...)
	if jsonlOut.ExitCode != 0 {
		t.Fatalf("jsonl aggregate exit = %d\nstderr:\n%s", jsonlOut.ExitCode, jsonlOut.Stderr)
	}
	assertSameAggregates(t, decodeJSONLAggregates(t, jsonlOut.Stdout), want)

	csvOut := runSmob(t, ctx, aggArgs(cfg, base, "--agg", "avg", "--group-by", "cpu", "--format", "csv")...)
	if csvOut.ExitCode != 0 {
		t.Fatalf("csv aggregate exit = %d\nstderr:\n%s", csvOut.ExitCode, csvOut.Stderr)
	}
	assertSameAggregates(t, decodeCSVAggregates(t, csvOut.Stdout), want)
}

func TestQueryAggLimit(t *testing.T) {
	cfg, base := seedStatsData(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	limited := runAggJSON(t, ctx, cfg, base, "--agg", "avg", "--metric", "system.cpu.percent", "--limit", "2")
	want := []wireAggregate{{
		Metric: "system.cpu.percent",
		Labels: map[string]string{},
		Count:  2,
		Value:  15,
	}}
	assertSameAggregates(t, limited, want)

	unlimited := runAggJSON(t, ctx, cfg, base, "--agg", "avg", "--metric", "system.cpu.percent", "--limit", "0")
	want = []wireAggregate{{
		Metric: "system.cpu.percent",
		Labels: map[string]string{},
		Count:  3,
		Value:  20,
	}}
	assertSameAggregates(t, unlimited, want)
}

func TestQueryAggInvalidFlags(t *testing.T) {
	cfg := validConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cases := []struct {
		name string
		args []string
		want string
	}{
		{"group-by without agg", []string{"--group-by", "cpu"}, "group-by"},
		{"empty group-by without agg", []string{"--group-by", ""}, "group-by"},
		{"empty agg", []string{"--agg", ""}, "agg"},
		{"invalid agg", []string{"--agg", "median"}, "agg"},
		{"empty group label", []string{"--agg", "avg", "--group-by", "cpu,,mode"}, "group-by"},
		{"empty group-by value", []string{"--agg", "avg", "--group-by", ","}, "group-by"},
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

func TestQueryAggEmpty(t *testing.T) {
	cfg := writeConfig(t, "data_dir: "+filepath.Join(t.TempDir(), "missing")+"\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	table := runSmob(t, ctx, "query", "--config", cfg, "--agg", "avg")
	if table.ExitCode != 0 {
		t.Fatalf("empty table aggregate exit = %d\nstderr:\n%s", table.ExitCode, table.Stderr)
	}
	if tableRows(table.Stdout) != 0 {
		t.Fatalf("empty table aggregate = %q, want a header only", table.Stdout)
	}
	header := strings.Fields(strings.Split(strings.TrimSpace(table.Stdout), "\n")[0])
	if !slices.Equal(header, []string{"METRIC", "COUNT", "VALUE", "UNIT", "LABELS"}) {
		t.Fatalf("table header = %v", header)
	}

	jsonOut := runSmob(t, ctx, "query", "--config", cfg, "--agg", "avg", "--format", "json")
	if jsonOut.ExitCode != 0 {
		t.Fatalf("empty json aggregate exit = %d\nstderr:\n%s", jsonOut.ExitCode, jsonOut.Stderr)
	}
	if jsonOut.Stdout != "[]\n" {
		t.Fatalf("empty json aggregate = %q, want %q", jsonOut.Stdout, "[]\n")
	}

	jsonlOut := runSmob(t, ctx, "query", "--config", cfg, "--agg", "avg", "--format", "jsonl")
	if jsonlOut.ExitCode != 0 {
		t.Fatalf("empty jsonl aggregate exit = %d\nstderr:\n%s", jsonlOut.ExitCode, jsonlOut.Stderr)
	}
	if strings.TrimSpace(jsonlOut.Stdout) != "" {
		t.Fatalf("empty jsonl aggregate = %q, want no rows", jsonlOut.Stdout)
	}

	csvOut := runSmob(t, ctx, "query", "--config", cfg, "--agg", "avg", "--format", "csv")
	if csvOut.ExitCode != 0 {
		t.Fatalf("empty csv aggregate exit = %d\nstderr:\n%s", csvOut.ExitCode, csvOut.Stderr)
	}
	if got := decodeCSVAggregates(t, csvOut.Stdout); len(got) != 0 {
		t.Fatalf("empty csv aggregate returned %d rows, want 0", len(got))
	}
}

func TestQueryAggLive(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := collectConfig(t, dataDir, "/proc")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	collect := runSmob(t, ctx, "collect", "--once", "--config", cfg)
	if collect.ExitCode != 0 {
		t.Fatalf("collect --once exit = %d\nstderr:\n%s", collect.ExitCode, collect.Stderr)
	}

	res := runSmob(t, ctx, "query", "--config", cfg, "--since", "1h", "--agg", "avg", "--group-by", "scope", "--format", "json")
	if res.ExitCode != 0 {
		t.Fatalf("live aggregate exit = %d\nstderr:\n%s", res.ExitCode, res.Stderr)
	}
	aggregates := decodeJSONAggregates(t, res.Stdout)
	if len(aggregates) == 0 {
		t.Fatal("live aggregate returned no rows")
	}

	for i, aggregate := range aggregates {
		if i > 0 && aggregates[i-1].Metric > aggregate.Metric {
			t.Fatalf("aggregates are not sorted: %s before %s", aggregates[i-1].Metric, aggregate.Metric)
		}
		if aggregate.Count < 1 {
			t.Fatalf("%s has count %d, want at least 1", aggregate.Metric, aggregate.Count)
		}
		if math.IsNaN(aggregate.Value) || math.IsInf(aggregate.Value, 0) {
			t.Fatalf("%s has a non-finite aggregate: %+v", aggregate.Metric, aggregate)
		}
		if len(aggregate.Labels) != 1 || aggregate.Labels["scope"] == "" {
			t.Fatalf("%s labels = %v, want only a non-empty scope", aggregate.Metric, aggregate.Labels)
		}
	}
}
