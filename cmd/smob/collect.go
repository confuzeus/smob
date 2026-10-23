package main

import (
	"errors"

	"github.com/spf13/cobra"
)

func (a *app) collectCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "collect",
		Short: "Collect metrics and write them to the configured store",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := a.load(cmd); err != nil {
				return err
			}
			return errors.New("collect: metric collection is not implemented in this build")
		},
	}
}
