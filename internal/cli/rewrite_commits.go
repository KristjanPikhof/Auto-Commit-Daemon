package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

type rewriteSelectionReport struct {
	OK                bool                      `json:"ok"`
	Repo              string                    `json:"repo"`
	BranchRef         string                    `json:"branch_ref"`
	Head              string                    `json:"head"`
	Selected          []git.RewriteCommitRecord `json:"selected"`
	RecreateUnchanged []git.RewriteCommitRecord `json:"recreate_unchanged"`
	SelectedPositions string                    `json:"selected_positions"`
}

type rewriteCommitsOptions struct {
	selection git.RewriteSelectionOptions
	base      string
	head      string
	planOut   string
	showPlan  string
	applyPlan string
	dryRun    bool
	yes       bool
}

func newRewriteCommitsCmd() *cobra.Command {
	var opts rewriteCommitsOptions
	cmd := &cobra.Command{
		Use:   "rewrite-commits (--range <base>..<head> | --base <base> --head <head>) [--plan-out FILE]",
		Short: "Preview an AI-generated commit rewrite plan for the current branch",
		Long: `Preview an AI-generated rewrite plan for a linear commit range on the
current branch.

Plan generation is intentionally gated: ACD_COMMIT_STRATEGY must resolve to
intent and ACD_AI_PROVIDER must name a usable non-deterministic planner provider
(openai-compat with ACD_AI_API_KEY, or subprocess:<name>). Deterministic fallback
is not enough for rewrite planning. Showing or applying a previously saved plan
may bypass the provider gate because no new plan is generated.

v1 scope: current branch linear ranges only; merge commit rewrites are refused;
there is no daemon automation.`,
		Example: `  ACD_COMMIT_STRATEGY=intent ACD_AI_PROVIDER=openai-compat ACD_AI_API_KEY=... acd rewrite-commits --range main~5..HEAD --plan-out rewrite.json
  acd rewrite-commits --base main~5 --head HEAD --dry-run
  acd rewrite-commits --show-plan rewrite.json
  acd rewrite-commits --apply-plan rewrite.json --yes`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runRewriteCommits(cmd.Context(), cmd.OutOrStdout(), repo, opts, jsonOut)
		},
	}
	cmd.Flags().StringVar(&opts.selection.GitRange, "range", "", "Linear commit range to rewrite (for example main~5..HEAD); current branch only")
	cmd.Flags().StringVar(&opts.base, "base", "", "Exclusive base revision for the linear rewrite range")
	cmd.Flags().StringVar(&opts.head, "head", "", "Inclusive head revision for the linear rewrite range (default HEAD when --base is set)")
	cmd.Flags().StringVar(&opts.planOut, "plan-out", "", "Write the generated rewrite plan to FILE")
	cmd.Flags().StringVar(&opts.showPlan, "show-plan", "", "Display a saved rewrite plan without checking the AI provider gate")
	cmd.Flags().StringVar(&opts.applyPlan, "apply-plan", "", "Apply a saved rewrite plan; bypasses the plan-generation provider gate")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "Validate and preview only; do not rewrite commits")
	cmd.Flags().BoolVar(&opts.yes, "yes", false, "Skip confirmation when applying a saved plan")
	cmd.Flags().StringVar(&opts.selection.From, "from", "", "Compatibility selector: select from commit-ish or 1-based position through HEAD")
	cmd.Flags().IntVar(&opts.selection.Last, "last", 0, "Compatibility selector: select the newest n commits")
	return cmd
}

func runRewriteCommits(ctx context.Context, out io.Writer, repoFlag string, opts rewriteCommitsOptions, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := normalizeAndValidateRewriteOptions(&opts); err != nil {
		return err
	}
	if opts.showPlan != "" {
		fmt.Fprintf(out, "rewrite-commits saved plan display: %s\n", opts.showPlan)
		fmt.Fprintln(out, "Saved plan display is command-contract only in this build; no AI provider check required.")
		return nil
	}
	if opts.applyPlan != "" {
		if !opts.yes && !opts.dryRun {
			return errors.New("acd rewrite-commits: --apply-plan requires --yes or --dry-run")
		}
		fmt.Fprintf(out, "rewrite-commits saved plan apply: %s\n", opts.applyPlan)
		fmt.Fprintln(out, "Saved plan apply is command-contract only in this build; no AI provider check required.")
		return nil
	}
	cfg := ai.LoadProviderConfigFromEnv()
	provider, closer, err := ai.BuildProvider(cfg)
	if err != nil {
		return fmt.Errorf("acd rewrite-commits: build provider: %w", err)
	}
	if closer != nil {
		defer func() { _ = closer.Close() }()
	}
	if err := ai.CheckRewritePlanGenerationGate(cfg, provider); err != nil {
		return fmt.Errorf("acd rewrite-commits: %w", err)
	}

	repo, err := resolveRepo(repoFlag)
	if err != nil {
		return err
	}
	selection, err := git.ResolveRewriteSelection(ctx, repo, opts.selection)
	if err != nil {
		return err
	}
	report := rewriteSelectionReport{
		OK:                true,
		Repo:              repo,
		BranchRef:         selection.BranchRef,
		Head:              selection.Head,
		Selected:          selection.Selected,
		RecreateUnchanged: selection.RecreateUnchanged,
		SelectedPositions: fmt.Sprintf("%d-%d", selection.SelectedNewestIndex, selection.SelectedOldestIndex),
	}
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	fmt.Fprintf(out, "rewrite-commits plan generation accepted for %s\n", rewriteSelectionLabel(opts))
	fmt.Fprintf(out, "rewrite-commits selection for %s (%s @ %s)\n", report.Repo, report.BranchRef, shortenSHA(report.Head))
	fmt.Fprintf(out, "Selected positions: %s\n", report.SelectedPositions)
	fmt.Fprintf(out, "Selected commits (%d):\n", len(report.Selected))
	for _, c := range report.Selected {
		fmt.Fprintf(out, "- %s %s\n", shortenSHA(c.OID), c.Subject)
	}
	if len(report.RecreateUnchanged) > 0 {
		fmt.Fprintf(out, "Recreate unchanged newer commits (%d):\n", len(report.RecreateUnchanged))
		for _, c := range report.RecreateUnchanged {
			fmt.Fprintf(out, "- %s %s\n", shortenSHA(c.OID), c.Subject)
		}
	}
	fmt.Fprintln(out, "Plan storage and git rewrite application are handled by later lanes; no commits were rewritten.")
	return nil
}

func normalizeAndValidateRewriteOptions(opts *rewriteCommitsOptions) error {
	modes := 0
	if strings.TrimSpace(opts.showPlan) != "" {
		modes++
	}
	if strings.TrimSpace(opts.applyPlan) != "" {
		modes++
	}
	generate := opts.selection.GitRange != "" || opts.base != "" || opts.head != "" || opts.selection.From != "" || opts.selection.Last > 0
	if generate {
		modes++
	}
	if modes == 0 {
		return errors.New("acd rewrite-commits: choose --range, --base/--head, --from, --last, --show-plan, or --apply-plan")
	}
	if modes > 1 {
		return errors.New("acd rewrite-commits: choose only one mode: generate, --show-plan, or --apply-plan")
	}
	if opts.selection.GitRange != "" && (opts.base != "" || opts.head != "") {
		return errors.New("acd rewrite-commits: use either --range or --base/--head, not both")
	}
	if opts.head != "" && opts.base == "" {
		return errors.New("acd rewrite-commits: --head requires --base")
	}
	if opts.base != "" {
		head := opts.head
		if head == "" {
			head = "HEAD"
		}
		opts.selection.GitRange = opts.base + ".." + head
	}
	if opts.selection.GitRange != "" && !strings.Contains(opts.selection.GitRange, "..") {
		return errors.New("acd rewrite-commits: --range must be a linear <base>..<head> range")
	}
	return nil
}

func rewriteSelectionLabel(opts rewriteCommitsOptions) string {
	if opts.selection.GitRange != "" {
		return opts.selection.GitRange
	}
	if opts.selection.From != "" {
		return opts.selection.From + "..HEAD"
	}
	return fmt.Sprintf("last %d commit(s)", opts.selection.Last)
}
