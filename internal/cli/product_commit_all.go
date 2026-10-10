package cli

import (
	"fmt"
	"github.com/spf13/cobra"
)

// Keep the old spelling for one release; finish owns the operation.
func newProductCommitAllCmd() *cobra.Command {
	var yes, dryRun bool
	cmd := &cobra.Command{Use: "commit-all", Short: "Deprecated: use acd finish", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !yes && !dryRun {
				return invalidCommandError("acd commit-all: use `acd finish`, or pass --yes to retain staged-selection consent")
			}
			fmt.Fprintln(cmd.ErrOrStderr(), "acd commit-all is deprecated; use acd finish.")
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			quiet, _ := cmd.Flags().GetBool("quiet")
			return runFinish(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), repo, finishStartParams{DryRun: dryRun, IncludeStaged: yes}, jsonOut, quiet)
		}}
	cmd.Flags().BoolVar(&yes, "yes", false, "Finish, including the exact current staged selection")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Plan and check without moving the branch or index")
	return cmd
}
