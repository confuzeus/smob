package main

import (
	"errors"

	"github.com/spf13/cobra"
)

func (a *app) tailCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "tail",
		Short: "Follow the most recent samples",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := a.load(cmd); err != nil {
				return err
			}
			return errors.New("tail: tailing samples is not implemented in this build")
		},
	}
}
