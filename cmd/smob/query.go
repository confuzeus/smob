package main

import (
	"errors"

	"github.com/spf13/cobra"
)

func (a *app) queryCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query",
		Short: "Query recorded samples",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := a.load(cmd); err != nil {
				return err
			}
			return errors.New("query: querying samples is not implemented in this build")
		},
	}
	cmd.Flags().StringVar(&a.dataDir, "data-dir", "", "override data_dir from the configuration")
	return cmd
}
