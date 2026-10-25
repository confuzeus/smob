package main

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/confuzeus/smob/internal/cli"
	"github.com/confuzeus/smob/internal/model"
	"github.com/confuzeus/smob/internal/query"
	"github.com/spf13/cobra"
)

func (a *app) statsCommand() *cobra.Command {
	var opts queryOptions

	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Summarize samples over a time range",
		Long: `Summarize samples per metric over a time range.

The same range defaults and filters as query apply. Each metric produces one
row with the number of matching samples, the minimum, the maximum, and the
mean. Metrics are sorted alphabetically. Mean is interval-weighted for rate
metrics and unweighted for gauges, so it equals query --agg avg for the same
range and filters.

--limit stops the scan after N matching samples (default 10000, 0 unlimited),
so a limited run reports partial statistics over those samples. An empty result
prints only the header (or [] for JSON) and exits 0.`,
		Args: noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := a.load(cmd); err != nil {
				return err
			}
			q, err := opts.query(time.Now())
			if err != nil {
				return cli.UsageErrorf("%v", err)
			}
			return a.runStats(cmd, q, opts.format)
		},
	}

	addQueryFlags(cmd, &opts, &a.dataDir)
	return cmd
}

func (a *app) runStats(cmd *cobra.Command, q model.Query, format string) error {
	st, err := newStore(cmd.Context(), a.cfg, a.logger)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	var summaries query.Summarizer
	if err := st.Query(cmd.Context(), q, func(sample model.Sample) error {
		summaries.Add(sample)
		return nil
	}); err != nil {
		return statsError(err)
	}

	rows, err := summaries.Summaries()
	if err != nil {
		return err
	}

	out, err := query.NewSummaryWriter(cmd.OutOrStdout(), format)
	if err != nil {
		if errors.Is(err, syscall.EPIPE) {
			return nil
		}
		return err
	}

	for _, summary := range rows {
		if err := out.Write(summary); err != nil {
			if errors.Is(err, syscall.EPIPE) {
				return nil
			}
			return err
		}
	}
	if err := out.Close(); err != nil {
		if errors.Is(err, syscall.EPIPE) {
			return nil
		}
		return err
	}
	return nil
}

// statsError classifies a scan failure. A canceled context means the summary
// covers only part of the range, so it is reported as a failure instead of
// being printed as complete statistics. A broken pipe stays a clean exit.
func statsError(err error) error {
	if errors.Is(err, syscall.EPIPE) {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("stats interrupted: %w", err)
	}
	return err
}
