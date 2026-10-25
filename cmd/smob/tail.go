package main

import (
	"errors"

	"github.com/spf13/cobra"
)

func (a *app) tailCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "tail",
		Short: "Follow the most recent samples",
		Args:  noArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			// Report the stub before loading configuration: tail must not
			// depend on a readable config to say it is not implemented.
			return errors.New("tail: tailing samples is not implemented in this build")
		},
	}
}
