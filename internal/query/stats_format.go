package query

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"
)

// SummaryWriter consumes summaries in metric order. Close finishes the
// document.
type SummaryWriter interface {
	Write(Summary) error
	Close() error
}

// NewSummaryWriter returns a SummaryWriter for format. The header or opening
// delimiter is written immediately, so an empty result still produces valid
// output.
func NewSummaryWriter(w io.Writer, format string) (SummaryWriter, error) {
	switch format {
	case "table":
		tw := tabwriter.NewWriter(w, 0, 8, 2, ' ', 0)
		if _, err := io.WriteString(tw, "METRIC\tCOUNT\tMIN\tMAX\tMEAN\tUNIT\n"); err != nil {
			return nil, err
		}
		return &summaryTableWriter{w: tw}, nil
	case "json":
		if _, err := io.WriteString(w, "["); err != nil {
			return nil, err
		}
		return &summaryJSONWriter{w: w, first: true}, nil
	case "jsonl":
		return &summaryJSONLWriter{enc: json.NewEncoder(w)}, nil
	case "csv":
		cw := csv.NewWriter(w)
		if err := cw.Write([]string{"metric", "count", "min", "max", "mean"}); err != nil {
			return nil, err
		}
		return &summaryCSVWriter{w: cw}, nil
	default:
		return nil, fmt.Errorf("format: must be one of table, json, jsonl, csv; got %q", format)
	}
}

type summaryTableWriter struct {
	w *tabwriter.Writer
}

func (t *summaryTableWriter) Write(summary Summary) error {
	_, err := fmt.Fprintf(t.w, "%s\t%d\t%s\t%s\t%s\t%s\n",
		summary.Metric,
		summary.Count,
		formatValue(summary.Min),
		formatValue(summary.Max),
		formatValue(summary.Mean),
		unitFor(summary.Metric),
	)
	return err
}

func (t *summaryTableWriter) Close() error {
	return t.w.Flush()
}

type summaryJSONWriter struct {
	w     io.Writer
	first bool
}

func (j *summaryJSONWriter) Write(summary Summary) error {
	data, err := json.Marshal(summary)
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

func (j *summaryJSONWriter) Close() error {
	if j.first {
		_, err := io.WriteString(j.w, "]\n")
		return err
	}
	_, err := io.WriteString(j.w, "\n]\n")
	return err
}

type summaryJSONLWriter struct {
	enc *json.Encoder
}

func (j *summaryJSONLWriter) Write(summary Summary) error {
	return j.enc.Encode(summary)
}

func (j *summaryJSONLWriter) Close() error { return nil }

type summaryCSVWriter struct {
	w *csv.Writer
}

func (c *summaryCSVWriter) Write(summary Summary) error {
	return c.w.Write([]string{
		summary.Metric,
		strconv.FormatInt(summary.Count, 10),
		formatValue(summary.Min),
		formatValue(summary.Max),
		formatValue(summary.Mean),
	})
}

func (c *summaryCSVWriter) Close() error {
	c.w.Flush()
	return c.w.Error()
}
