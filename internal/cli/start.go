package cli

import "github.com/spf13/cobra"

func newStartCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Register a session and ensure a daemon is running for this repo",
		RunE:  stubRun("start"),
	}
	cmd.Flags().String("session-id", "", "Universal session identifier (UUID, required)")
	cmd.Flags().String("harness", "", "Harness identifier (claude-code|codex|opencode|pi|shell|other)")
	cmd.Flags().Int("watch-pid", 0, "Optional fast-path PID for liveness probe (0 to disable)")
	return cmd
}
