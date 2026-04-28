package cli

import "github.com/spf13/cobra"

func newGCCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "gc",
		Short: "Prune central registry of dead/missing repo entries",
		RunE:  stubRun("gc"),
	}
}
