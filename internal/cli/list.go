package cli

import "github.com/spf13/cobra"

func newListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all known daemons across repos",
		RunE:  stubRun("list"),
	}
}
