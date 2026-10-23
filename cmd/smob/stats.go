package main

import (
	"errors"

	"github.com/spf13/cobra"
)

func (a *app) statsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stats",
		Short: "Summarize samples over a time range",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := a.load(cmd); err != nil {
				return err
			}
			return errors.New("stats: summaries are not implemented in this build")
		},
	}
}
