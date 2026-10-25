package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func (a *app) configCommand() *cobra.Command {
	var validate bool

	cmd := &cobra.Command{
		Use:   "config",
		Short: "Print the effective configuration",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := a.load(cmd); err != nil {
				return err
			}
			if validate {
				_, err := fmt.Fprintf(cmd.OutOrStdout(), "config valid: %s\n", a.configPath)
				return err
			}

			out, err := yaml.Marshal(a.cfg)
			if err != nil {
				return fmt.Errorf("encode config: %w", err)
			}
			if _, err := cmd.OutOrStdout().Write(out); err != nil {
				return fmt.Errorf("write config: %w", err)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&validate, "validate", false, "validate the configuration and exit")
	return cmd
}
