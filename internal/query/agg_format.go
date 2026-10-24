package query

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"
)

// AggregateWriter consumes aggregates in group order. Close finishes the
// document.
type AggregateWriter interface {
	Write(Aggregate) error
	Close() error
}

// NewAggregateWriter returns an AggregateWriter for format. The header or
// opening delimiter is written immediately, so an empty result still produces
// valid output.
func NewAggregateWriter(w io.Writer, format string) (AggregateWriter, error) {
	switch format {
	case "table":
		tw := tabwriter.NewWriter(w, 0, 8, 2, ' ', 0)
		if _, err := io.WriteString(tw, "METRIC\tCOUNT\tVALUE\tUNIT\tLABELS\n"); err != nil {
			return nil, err
		}
		return &aggregateTableWriter{w: tw}, nil
	case "json":
		if _, err := io.WriteString(w, "["); err != nil {
			return nil, err
		}
		return &aggregateJSONWriter{w: w, first: true}, nil
	case "jsonl":
		return &aggregateJSONLWriter{enc: json.NewEncoder(w)}, nil
	case "csv":
		cw := csv.NewWriter(w)
		if err := cw.Write([]string{"metric", "labels", "count", "value"}); err != nil {
			return nil, err
		}
		return &aggregateCSVWriter{w: cw}, nil
	default:
		return nil, fmt.Errorf("format: must be one of table, json, jsonl, csv; got %q", format)
	}
}

type aggregateTableWriter struct {
	w *tabwriter.Writer
}

func (t *aggregateTableWriter) Write(aggregate Aggregate) error {
	_, err := fmt.Fprintf(t.w, "%s\t%d\t%s\t%s\t%s\n",
		aggregate.Metric,
		aggregate.Count,
		formatValue(aggregate.Value),
		unitFor(aggregate.Metric),
		formatLabels(aggregate.Labels),
	)
	return err
}

func (t *aggregateTableWriter) Close() error {
	return t.w.Flush()
}

type aggregateJSONWriter struct {
	w     io.Writer
	first bool
}

func (j *aggregateJSONWriter) Write(aggregate Aggregate) error {
	data, err := json.Marshal(aggregate)
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

func (j *aggregateJSONWriter) Close() error {
	if j.first {
		_, err := io.WriteString(j.w, "]\n")
		return err
	}
	_, err := io.WriteString(j.w, "\n]\n")
	return err
}

type aggregateJSONLWriter struct {
	enc *json.Encoder
}

func (j *aggregateJSONLWriter) Write(aggregate Aggregate) error {
	return j.enc.Encode(aggregate)
}

func (j *aggregateJSONLWriter) Close() error { return nil }

type aggregateCSVWriter struct {
	w *csv.Writer
}

func (c *aggregateCSVWriter) Write(aggregate Aggregate) error {
	return c.w.Write([]string{
		aggregate.Metric,
		formatLabels(aggregate.Labels),
		strconv.FormatInt(aggregate.Count, 10),
		formatValue(aggregate.Value),
	})
}

func (c *aggregateCSVWriter) Close() error {
	c.w.Flush()
	return c.w.Error()
}
