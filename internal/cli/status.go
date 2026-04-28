package cli

import "github.com/spf13/cobra"

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Print current daemon + clients for one repo (default: cwd)",
		RunE:  stubRun("status"),
	}
}
