package cli

import (
	"errors"
	"os"

	"github.com/spf13/cobra"
)

// ErrNoCommand is retained for source compatibility. Bare acd now renders a
// read-only health summary and no longer returns this error.
var ErrNoCommand = errors.New("no command provided")

// Execute builds the root command tree and runs it.
func Execute() error {
	root := newRootCmd()
	root.SetOut(os.Stdout)
	root.SetErr(os.Stderr)
	return root.Execute()
}

func newRootCmd() *cobra.Command {
	var (
		repoPath string
		jsonOut  bool
		quiet    bool
		logLevel string
	)

	cmd := &cobra.Command{
		Use:           "acd",
		Short:         "Atomic Commit Daemon — per-repo, multi-harness",
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(c *cobra.Command, args []string) error {
			return runControlStatus(c.Context(), c.OutOrStdout(), repoPath, jsonOut)
		},
	}
	cmd.CompletionOptions.HiddenDefaultCmd = true
	cmd.SetHelpTemplate(rootHelpTemplate)

	pf := cmd.PersistentFlags()
	pf.StringVar(&repoPath, "repo", "", "Repo root (default: cwd)")
	pf.BoolVar(&jsonOut, "json", false, "Emit JSON output")
	pf.BoolVar(&quiet, "quiet", false, "Suppress non-essential output")
	pf.StringVar(&logLevel, "log-level", "info", "debug|info|warn|error")

	cmd.AddCommand(
		newOnCmd(),
		newOffCmd(),
		newConfigureCmd(),
		newSettingsCmd(),
		newAuthCmd(),
		newVersionCmd(),
		newStartCmd(),
		newStopCmd(),
		newWakeCmd(),
		newTouchCmd(),
		newFlushCmd(),
		newStatusCmd(),
		newEventsCmd(),
		newPromptCmd(),
		newExplainCmd(),
		newFixCmd(),
		newLogsCmd(),
		newRepoCmd(),
		newListCmd(),
		newStatsCmd(),
		newDiagnoseCmd(),
		newRecoverCmd(),
		newPauseCmd(),
		newResumeCmd(),
		newPurgeEventsCmd(),
		newSetupCmd(),
		newHookStdinExtractCmd(),
		newHookCursorExtractCmd(),
		newDoctorCmd(),
		newGCCmd(),
		newCommitAllCmd(),
		newRewriteCommitsCmd(),
		newDaemonCmd(),
	)

	return cmd
}
