package cli

import (
	"errors"
	"os"

	"github.com/spf13/cobra"
)

// ErrNoCommand is returned when acd is invoked with no subcommand.
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
		RunE: func(c *cobra.Command, args []string) error {
			_ = c.Help()
			return ErrNoCommand
		},
	}

	pf := cmd.PersistentFlags()
	pf.StringVar(&repoPath, "repo", "", "Repo root (default: cwd)")
	pf.BoolVar(&jsonOut, "json", false, "Emit JSON output")
	pf.BoolVar(&quiet, "quiet", false, "Suppress non-essential output")
	pf.StringVar(&logLevel, "log-level", "info", "debug|info|warn|error")

	cmd.AddCommand(
		newVersionCmd(),
		newStartCmd(),
		newStopCmd(),
		newWakeCmd(),
		newTouchCmd(),
		newStatusCmd(),
		newListCmd(),
		newStatsCmd(),
		newDiagnoseCmd(),
		newRecoverCmd(),
		newInitCmd(),
		newHookStdinExtractCmd(),
		newDoctorCmd(),
		newGCCmd(),
		newDaemonCmd(),
	)

	return cmd
}
