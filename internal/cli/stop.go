package cli

import "github.com/spf13/cobra"

func newStopCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop",
		Short: "Deregister a session; daemon exits when refcount hits zero",
		RunE:  stubRun("stop"),
	}
	cmd.Flags().String("session-id", "", "Session identifier to deregister")
	cmd.Flags().Bool("flush", false, "Drain pending events before stopping (with --force)")
	cmd.Flags().Bool("force", false, "Skip refcount and SIGTERM the daemon")
	cmd.Flags().Bool("all", false, "Stop every daemon in the central registry")
	return cmd
}
