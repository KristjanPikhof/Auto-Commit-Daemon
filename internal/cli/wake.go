package cli

import "github.com/spf13/cobra"

func newWakeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "wake",
		Short: "Heartbeat refresh + nudge daemon",
		RunE:  stubRun("wake"),
	}
	cmd.Flags().String("session-id", "", "Session identifier (required)")
	return cmd
}
