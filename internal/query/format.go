// Package query formats stored samples for the query and stats commands.
package query

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/confuzeus/smob/internal/model"
)

// Writer consumes samples in the order the store yields them. Close flushes
// buffered output and finishes the document.
type Writer interface {
	Write(model.Sample) error
	Close() error
}

// NewWriter returns a Writer for format. The header or opening delimiter is
// written immediately, so an empty result still produces valid output.
func NewWriter(w io.Writer, format string) (Writer, error) {
	switch format {
	case "table":
		tw := tabwriter.NewWriter(w, 0, 8, 2, ' ', 0)
		if _, err := io.WriteString(tw, "TIME\tMETRIC\tVALUE\tUNIT\tLABELS\n"); err != nil {
			return nil, err
		}
		return &tableWriter{w: tw}, nil
	case "json":
		if _, err := io.WriteString(w, "["); err != nil {
			return nil, err
		}
		return &jsonWriter{w: w, first: true}, nil
	case "jsonl":
		return &jsonlWriter{enc: json.NewEncoder(w)}, nil
	case "csv":
		cw := csv.NewWriter(w)
		if err := cw.Write([]string{"timestamp", "metric", "value", "interval_ms", "labels"}); err != nil {
			return nil, err
		}
		return &csvWriter{w: cw}, nil
	default:
		return nil, fmt.Errorf("format: must be one of table, json, jsonl, csv; got %q", format)
	}
}

type tableWriter struct {
	w *tabwriter.Writer
}

func (t *tableWriter) Write(sample model.Sample) error {
	if err := validateFinite(sample.Metric, sample.Value); err != nil {
		return err
	}
	_, err := fmt.Fprintf(t.w, "%s\t%s\t%s\t%s\t%s\n",
		formatTime(sample.Timestamp),
		escapeCell(sample.Metric),
		formatValue(sample.Value),
		unitFor(sample.Metric),
		escapeCell(formatLabels(sample.Labels)),
	)
	return err
}

func (t *tableWriter) Close() error {
	return t.w.Flush()
}

type jsonWriter struct {
	w     io.Writer
	first bool
}

func (j *jsonWriter) Write(sample model.Sample) error {
	if err := validateFinite(sample.Metric, sample.Value); err != nil {
		return err
	}
	data, err := json.Marshal(sample)
	if err != nil {
		return err
	}
	prefix := ",\n"
	if j.first {
		prefix = "\n"
		j.first = false
	}
	_, err = io.WriteString(j.w, prefix+string(data))
	return err
}

func (j *jsonWriter) Close() error {
	if j.first {
		_, err := io.WriteString(j.w, "]\n")
		return err
	}
	_, err := io.WriteString(j.w, "\n]\n")
	return err
}

type jsonlWriter struct {
	enc *json.Encoder
}

func (j *jsonlWriter) Write(sample model.Sample) error {
	if err := validateFinite(sample.Metric, sample.Value); err != nil {
		return err
	}
	return j.enc.Encode(sample)
}

func (j *jsonlWriter) Close() error { return nil }

type csvWriter struct {
	w *csv.Writer
}

func (c *csvWriter) Write(sample model.Sample) error {
	if err := validateFinite(sample.Metric, sample.Value); err != nil {
		return err
	}
	return c.w.Write([]string{
		formatTime(sample.Timestamp),
		sample.Metric,
		formatValue(sample.Value),
		strconv.FormatInt(sample.IntervalMS, 10),
		formatLabels(sample.Labels),
	})
}

func (c *csvWriter) Close() error {
	c.w.Flush()
	return c.w.Error()
}

// formatTime renders timestamps in UTC with full precision.
func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// formatValue avoids exponent notation so byte counts stay readable.
func formatValue(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

// validateFinite rejects values that JSON cannot encode and that would
// otherwise render as NaN or +Inf in table and CSV output. It runs before a row
// is written, so a bad sample never leaves partial output.
func validateFinite(metric string, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("metric %s: non-finite value %v", metric, value)
	}
	return nil
}

// tableEscaper keeps a table cell on one line. A label value such as a process
// comm may contain newlines, carriage returns, or tabs; left verbatim they
// split a row or shift its columns.
var tableEscaper = strings.NewReplacer(
	`\`, `\\`,
	"\n", `\n`,
	"\r", `\r`,
	"\t", `\t`,
)

// escapeCell renders value as one table cell with control characters visible.
func escapeCell(value string) string {
	return tableEscaper.Replace(value)
}

// formatLabels renders labels in canonical key order.
func formatLabels(labels map[string]string) string {
	keys := slices.Sorted(maps.Keys(labels))
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+labels[key])
	}
	return strings.Join(parts, ",")
}

// unitFor derives the display unit from the metric name. Both `.bytes` and
// `_bytes` suffixes are byte gauges; the process source names its RSS gauge
// `process.memory.rss_bytes`.
func unitFor(metric string) string {
	switch {
	case strings.HasSuffix(metric, ".bytes_per_second"):
		return "B/s"
	case strings.HasSuffix(metric, ".percent"):
		return "%"
	case strings.HasSuffix(metric, ".bytes"), strings.HasSuffix(metric, "_bytes"):
		return "B"
	default:
		return ""
	}
}
