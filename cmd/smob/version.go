package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

// String renders the version banner shared by the version subcommand and the
// root --version flag.
func (b buildInfo) String() string {
	return fmt.Sprintf("smob %s (commit %s, built %s)", b.version, b.commit, b.date)
}

func (a *app) versionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version, commit, and build date",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintln(cmd.OutOrStdout(), a.info.String())
			return err
		},
	}
}
