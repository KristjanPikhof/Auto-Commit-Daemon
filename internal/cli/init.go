package cli

import "github.com/spf13/cobra"

func newInitCmd() *cobra.Command {
	return &cobra.Command{
		Use:       "init <harness>",
		Short:     "Print install snippet for a harness adapter",
		Args:      cobra.ExactArgs(1),
		ValidArgs: []string{"claude-code", "codex", "opencode", "pi", "shell"},
		RunE:      stubRun("init"),
	}
}
