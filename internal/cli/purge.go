package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

func newPurgeEventsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "purge-events",
		Short:  "[DEPRECATED] Preserve stuck events safely (use `acd fix --force --yes`)",
		Hidden: true,
		Long: `DEPRECATED: ` + "`acd purge-events`" + ` no longer deletes capture rows.

Use:
  acd fix --force --dry-run   # preview archive-only recovery
  acd fix --force --yes       # protect each exact pair, then settle it

The legacy --blocked, --pending, and --failed selectors are refused because
delegating them to whole-repository recovery can affect unrelated exact pairs.
Use --all explicitly to preview or apply archive-only recovery.`,
		RunE: func(c *cobra.Command, args []string) error {
			repo, _ := c.Flags().GetString("repo")
			blocked, _ := c.Flags().GetBool("blocked")
			pending, _ := c.Flags().GetBool("pending")
			failed, _ := c.Flags().GetBool("failed")
			all, _ := c.Flags().GetBool("all")
			yes, _ := c.Flags().GetBool("yes")
			dryRun, _ := c.Flags().GetBool("dry-run")
			jsonOut, _ := c.Flags().GetBool("json")
			fmt.Fprintln(c.ErrOrStderr(), "acd purge-events is deprecated; use acd fix --force [--yes]. See acd fix --help.")
			return runPurgeEvents(c.Context(), c.OutOrStdout(),
				repo, blocked, pending, failed, all, yes, dryRun, jsonOut)
		},
	}
	cmd.Flags().Bool("blocked", false, "(deprecated) Unsupported selective recovery; use --all")
	cmd.Flags().Bool("pending", false, "(deprecated) Unsupported selective deletion")
	cmd.Flags().Bool("failed", false, "(deprecated) Unsupported selective deletion")
	cmd.Flags().Bool("all", false, "(deprecated) Safely reconcile all stuck exact pairs")
	cmd.Flags().Bool("yes", false, "(deprecated) Apply safe archive-only recovery")
	cmd.Flags().Bool("dry-run", false, "(deprecated) Show the safe recovery plan")
	return cmd
}

func runPurgeEvents(
	ctx context.Context,
	out io.Writer,
	repo string,
	blocked, pending, failed, all, yes, dryRun, jsonOut bool,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if blocked || pending || failed {
		return fmt.Errorf("acd purge-events: selective --blocked/--pending/--failed recovery is no longer supported; pass --all to explicitly preserve every stuck exact pair")
	}
	if !all {
		return fmt.Errorf("acd purge-events: pass --all to delegate whole-repository safe recovery")
	}
	if !dryRun && !yes {
		return fmt.Errorf("acd purge-events: refusing to mutate state without --yes (use --dry-run first)")
	}
	return runFix(ctx, out, repo, dryRun, yes, true, false, jsonOut)
}
