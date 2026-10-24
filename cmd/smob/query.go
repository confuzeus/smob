package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/confuzeus/smob/internal/cli"
	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/query"
	"github.com/spf13/cobra"
)

// queryOptions holds the raw query flags before they are validated.
type queryOptions struct {
	since  string
	until  string
	host   string
	scope  string
	metric string
	labels []string
	limit  int
	format string
}

func (a *app) queryCommand() *cobra.Command {
	var opts queryOptions

	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query recorded samples",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := a.load(cmd); err != nil {
				return err
			}
			q, err := opts.query(time.Now())
			if err != nil {
				return cli.UsageErrorf("%v", err)
			}
			return a.runQuery(cmd, q, opts.format)
		},
	}

	cmd.Flags().StringVar(&opts.since, "since", "", "start time: a duration such as 1h, or an RFC3339 timestamp (default: last hour)")
	cmd.Flags().StringVar(&opts.until, "until", "", "end time as an RFC3339 timestamp (default: now)")
	cmd.Flags().StringVar(&opts.host, "host", "", "restrict to one host")
	cmd.Flags().StringVar(&opts.scope, "scope", "", "restrict to system, process, or container")
	cmd.Flags().StringVar(&opts.metric, "metric", "", "exact metric name")
	cmd.Flags().StringArrayVar(&opts.labels, "label", nil, "label matcher: name=value or name~regex (repeatable)")
	cmd.Flags().IntVar(&opts.limit, "limit", 10_000, "maximum samples; 0 means unlimited")
	cmd.Flags().StringVar(&opts.format, "format", "table", "output format: table, json, jsonl, or csv")
	cmd.Flags().StringVar(&a.dataDir, "data-dir", "", "override data_dir from the configuration")
	return cmd
}

// query validates the flags and resolves them into a model.Query. The until
// time is resolved first; a since duration is relative to it.
func (o queryOptions) query(now time.Time) (model.Query, error) {
	switch o.format {
	case "table", "json", "jsonl", "csv":
	default:
		return model.Query{}, fmt.Errorf("format: must be one of table, json, jsonl, csv; got %q", o.format)
	}
	switch o.scope {
	case "", "system", "process", "container":
	default:
		return model.Query{}, fmt.Errorf("scope: must be one of system, process, container; got %q", o.scope)
	}
	if o.limit < 0 {
		return model.Query{}, fmt.Errorf("limit: must not be negative; got %d", o.limit)
	}
	if o.host != "" && !validComponent(o.host) {
		return model.Query{}, fmt.Errorf("host: must be a single path component; got %q", o.host)
	}

	until := now.UTC()
	if o.until != "" {
		parsed, err := time.Parse(time.RFC3339, o.until)
		if err != nil {
			return model.Query{}, fmt.Errorf("until: must be an RFC3339 timestamp: %w", err)
		}
		until = parsed.UTC()
	}

	since := until.Add(-time.Hour)
	if o.since != "" {
		if d, err := time.ParseDuration(o.since); err == nil {
			if d <= 0 {
				return model.Query{}, fmt.Errorf("since: duration must be greater than zero; got %s", o.since)
			}
			since = until.Add(-d)
		} else {
			parsed, err := time.Parse(time.RFC3339, o.since)
			if err != nil {
				return model.Query{}, fmt.Errorf("since: must be a duration or RFC3339 timestamp; got %q", o.since)
			}
			since = parsed.UTC()
		}
	}
	if !since.Before(until) {
		return model.Query{}, fmt.Errorf("since: must be before until; got %s and %s", since.Format(time.RFC3339), until.Format(time.RFC3339))
	}

	matchers := make([]model.LabelMatcher, 0, len(o.labels))
	for _, raw := range o.labels {
		matcher, err := parseLabelMatcher(raw)
		if err != nil {
			return model.Query{}, err
		}
		matchers = append(matchers, matcher)
	}

	return model.Query{
		Range:  model.TimeRange{Start: since, End: until},
		Host:   o.host,
		Scope:  o.scope,
		Metric: o.metric,
		Labels: matchers,
		Limit:  o.limit,
	}, nil
}

// parseLabelMatcher splits name=value or name~regex at the first operator.
func parseLabelMatcher(raw string) (model.LabelMatcher, error) {
	equal := strings.IndexByte(raw, '=')
	regex := strings.IndexByte(raw, '~')

	index, op := -1, model.LabelEqual
	switch {
	case equal >= 0 && (regex < 0 || equal < regex):
		index = equal
	case regex >= 0:
		index, op = regex, model.LabelRegex
	}
	if index < 0 {
		return model.LabelMatcher{}, fmt.Errorf("label: want name=value or name~regex; got %q", raw)
	}

	name, value := raw[:index], raw[index+1:]
	if name == "" {
		return model.LabelMatcher{}, fmt.Errorf("label: name must not be empty; got %q", raw)
	}
	if op == model.LabelRegex {
		if _, err := regexp.Compile(value); err != nil {
			return model.LabelMatcher{}, fmt.Errorf("label %s: %w", name, err)
		}
	}
	return model.LabelMatcher{Name: name, Op: op, Value: value}, nil
}

// validComponent reports whether value can be used as one host or scope path
// component.
func validComponent(value string) bool {
	return value != "" && value != "." && value != ".." && !strings.ContainsRune(value, '/')
}

func (a *app) runQuery(cmd *cobra.Command, q model.Query, format string) error {
	st, err := newStore(cmd.Context(), a.cfg, a.logger)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	out, err := query.NewWriter(cmd.OutOrStdout(), format)
	if err != nil {
		if errors.Is(err, syscall.EPIPE) {
			return nil
		}
		return err
	}

	if err := st.Query(cmd.Context(), q, out.Write); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, syscall.EPIPE) {
			return nil
		}
		return err
	}
	if err := out.Close(); err != nil {
		if errors.Is(err, syscall.EPIPE) {
			return nil
		}
		return err
	}
	return nil
}
