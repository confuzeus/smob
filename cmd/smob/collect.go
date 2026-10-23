package main

import (
	"errors"

	"github.com/spf13/cobra"
)

func (a *app) collectCommand() *cobra.Command {
	var dryRun, once bool

	cmd := &cobra.Command{
		Use:   "collect",
		Short: "Collect metrics and write them to the configured store",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := a.load(cmd); err != nil {
				return err
			}
			// dryRun and once gain behavior with the collector in Phase 1.
			return errors.New("collect: metric collection is not implemented in this build")
		},
	}
	cmd.Flags().StringVar(&a.dataDir, "data-dir", "", "override data_dir from the configuration")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate the configuration and exit without writing")
	cmd.Flags().BoolVar(&once, "once", false, "collect one snapshot from every enabled source and exit")
	return cmd
}
