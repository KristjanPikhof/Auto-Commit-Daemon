package cli

import "github.com/spf13/cobra"

func newDaemonCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "daemon",
		Short:  "Daemon mode (long-running). Not normally invoked manually.",
		Hidden: true,
	}
	run := &cobra.Command{
		Use:   "run",
		Short: "Run the long-lived daemon for a single repo",
		RunE:  stubRun("daemon run"),
	}
	run.Flags().String("git-dir", "", "Override .git path (rare)")
	cmd.AddCommand(run)
	return cmd
}
