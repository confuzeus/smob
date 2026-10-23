package main

import (
	"errors"

	"github.com/spf13/cobra"
)

func (a *app) queryCommand() *cobra.Command {
	return &cobra.Command{
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
}
