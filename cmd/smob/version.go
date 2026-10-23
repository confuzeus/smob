package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func (a *app) versionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version, commit, and build date",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "smob %s (commit %s, built %s)\n", a.info.version, a.info.commit, a.info.date)
			return err
		},
	}
}
