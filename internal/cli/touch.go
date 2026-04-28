package cli

import "github.com/spf13/cobra"

func newTouchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "touch",
		Short: "Heartbeat refresh only (no signal)",
		RunE:  stubRun("touch"),
	}
	cmd.Flags().String("session-id", "", "Session identifier (required)")
	return cmd
}
