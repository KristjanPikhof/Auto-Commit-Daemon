package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
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
	fromSHA   string
	fromNR    int
	rangeNR   string
	rangeSHA  string

	base         string
	head         string
	planOut      string
	showPlan     string
	applyPlan    string
	editPlan     string
	dryRun       bool
	yes          bool
	review       bool
	noReview     bool
	planOnly     bool
	messagesOnly bool
	newBranch    string
	editFormat   string
	progress     string
	progressTo   io.Writer
	quiet        bool
	in           io.Reader
}

func newRewriteCommitsCmd() *cobra.Command {
	var opts rewriteCommitsOptions
	cmd := &cobra.Command{
		Use:     "rewrite-commits (--from-nr <n> | --from-sha <sha> | --range-nr <start-end> | --range-sha <base>..<head> | --last <n>) [--plan-out FILE] | --edit <plan-id-or-file>",
		Aliases: []string{"edit-commits", "edit-commit"},
		Short:   "Review and improve commit messages on this branch",
		Long: `Review and change selected commits on this branch.

1. Select commits with --last, --from-sha, --from-nr, --range-sha or --range-nr.
2. Save a plan with --plan-only or --plan-out FILE.
3. Review it with --show-plan or --edit.
4. Preview with --apply PLAN --dry-run, then apply with --apply PLAN --yes.

Applying changes Git history. ACD creates a private backup first. Merge
commits are not supported. Your working files stay unchanged.

Local mode preserves original order and grouping. Intent mode can regroup
completed work. Use --messages-only to preserve each selected commit,
including empty commits. Use --new-branch NAME to keep the original branch.

An active protection worker must check and apply the plan. Showing, editing,
and applying a saved plan make no AI request. Use --progress plain for
copyable progress, or --quiet to hide progress. See docs/rewrite-commits.md
for details.`,
		Example: `  acd config edit
  acd history rewrite --last 5 --plan-only
  acd history rewrite --from-sha 8f4c2a1 --plan-out rewrite.json
  acd history rewrite --show-plan rewrite.json
  acd history rewrite --edit rewrite.json --plan-only
  acd history rewrite --apply rewrite.json --dry-run
  acd history rewrite --apply rewrite.json --yes`,

		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			quiet, _ := cmd.Flags().GetBool("quiet")
			opts.in = cmd.InOrStdin()
			opts.progressTo = cmd.ErrOrStderr()
			opts.quiet = quiet
			return runRewriteCommits(cmd.Context(), cmd.OutOrStdout(), repo, opts, jsonOut)
		},
	}
	cmd.Flags().StringVar(&opts.fromSHA, "from-sha", "", "Rewrite from commit-ish through HEAD; numeric-looking values are treated as commits, not positions")
	cmd.Flags().IntVar(&opts.fromNR, "from-nr", 0, "Rewrite from 1-based position n through HEAD, where 1 is HEAD")
	cmd.Flags().StringVar(&opts.rangeNR, "range-nr", "", "Rewrite a 1-based position range (start-end, where 1 is HEAD)")
	cmd.Flags().StringVar(&opts.rangeSHA, "range-sha", "", "Rewrite commits selected by a simple git range <base>..<head>")
	cmd.Flags().StringVar(&opts.selection.Range, "range", "", "Compatibility selector: 1-based position range to rewrite (start-end, where 1 is HEAD)")
	cmd.Flags().StringVar(&opts.selection.GitRange, "git-range", "", "Advanced compatibility selector: git rev-list revset; selected commits must be contiguous on the current branch")
	cmd.Flags().StringVar(&opts.base, "base", "", "Deprecated alias for --git-range <base>..<head>: exclusive base revision")
	cmd.Flags().StringVar(&opts.head, "head", "", "Deprecated alias for --git-range <base>..<head>: inclusive head revision (default HEAD when --base is set)")
	cmd.Flags().StringVar(&opts.planOut, "plan-out", "", "Write the generated rewrite plan to FILE")
	cmd.Flags().StringVar(&opts.showPlan, "show-plan", "", "Display a saved plan without creating a new AI request")
	cmd.Flags().StringVar(&opts.editPlan, "edit", "", "Edit a saved plan by plan ID or file path")
	cmd.Flags().StringVar(&opts.applyPlan, "apply-plan", "", "Apply a saved rewrite plan file; bypasses the plan-generation provider gate")
	cmd.Flags().StringVar(&opts.applyPlan, "apply", "", "Apply a saved plan by plan ID or file path")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "Check and preview the apply without rewriting commits")
	cmd.Flags().BoolVar(&opts.yes, "yes", false, "Apply the reviewed plan without another confirmation")
	cmd.Flags().BoolVar(&opts.review, "review", false, "Open EDITOR to review/edit proposed commit messages before apply")
	cmd.Flags().BoolVar(&opts.noReview, "no-review", false, "Skip the review/edit prompt and leave proposed messages unchanged")
	cmd.Flags().BoolVar(&opts.planOnly, "plan-only", false, "Generate or edit and save the rewrite plan without prompting to apply")
	cmd.Flags().BoolVar(&opts.messagesOnly, "messages-only", false, "Keep one output commit per selected commit instead of grouping by intent")
	cmd.Flags().StringVar(&opts.newBranch, "new-branch", "", "Reconstruct complete goals onto a new branch while preserving the original")
	cmd.Flags().StringVar(&opts.progress, "progress", string(rewriteProgressModeAuto), "Progress output mode: auto, plain, json, or off")
	cmd.Flags().StringVar(&opts.editFormat, "format", rewriteEditFormatText, "Review edit format: text or json")
	cmd.Flags().StringVar(&opts.selection.From, "from", "", "Compatibility selector: select from commit-ish or 1-based position through HEAD; prefer --from-sha or --from-nr")
	cmd.Flags().IntVar(&opts.selection.Last, "last", 0, "Select the newest n commits")
	for _, name := range []string{"range", "git-range", "base", "head", "from", "apply-plan"} {
		_ = cmd.Flags().MarkHidden(name)
	}
	return cmd
}

func runRewriteCommits(ctx context.Context, out io.Writer, repoFlag string, opts rewriteCommitsOptions, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := normalizeAndValidateRewriteOptions(&opts); err != nil {
		return err
	}
	progress, err := newRewriteProgressSink(opts.progress, opts.quiet, opts.progressTo)
	if err != nil {
		return err
	}
	if opts.showPlan != "" {
		if repo, repoErr := resolveRepo(repoFlag); repoErr == nil {
			if plan, ok, err := readIntentHistoryPlanRef(ctx, repo, opts.showPlan); err == nil && ok {
				return showIntentHistoryPlan(ctx, out, repo, plan, jsonOut, false)
			}
		}
		return showSavedRewritePlan(ctx, out, repoFlag, opts.showPlan, jsonOut)
	}
	if opts.applyPlan != "" {
		if repo, err := resolveRepo(repoFlag); err == nil {
			if plan, ok, err := readIntentHistoryPlanRef(ctx, repo, opts.applyPlan); err == nil && ok {
				if !opts.yes && !opts.dryRun {
					return errors.New("acd history rewrite: --apply requires --yes or --dry-run")
				}
				if jsonOut {
					if err := applyIntentHistoryPlan(ctx, io.Discard, repo, plan, opts.dryRun); err != nil {
						return err
					}
					return showIntentHistoryPlan(ctx, out, repo, plan, true, opts.dryRun)
				}
				return applyIntentHistoryPlan(ctx, out, repo, plan, opts.dryRun)
			}
		}
		return applySavedRewritePlan(ctx, out, repoFlag, opts)
	}
	if opts.editPlan != "" {
		if repo, err := resolveRepo(repoFlag); err == nil {
			if plan, ok, err := readIntentHistoryPlanRef(ctx, repo, opts.editPlan); err == nil && ok {
				return editFinishHistoryMessages(ctx, out, repo, plan, opts, jsonOut)
			}
		}
		return editSavedRewritePlan(ctx, out, repoFlag, opts)
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
	if err := progress.Emit(rewriteProgressEvent{Phase: "selection", Message: fmt.Sprintf("selected %d commit(s)", len(report.Selected)), Current: len(report.Selected), Total: len(report.Selected)}); err != nil {
		return err
	}
	return generateFinishHistoryPlan(ctx, out, repo, selection, opts, jsonOut)
}

func rewritePlanRefArg(ref string) string {
	if ref == "" {
		return ref
	}
	if strings.ContainsAny(ref, " \t'\"$`\\") {
		return strconv.Quote(ref)
	}
	return ref
}

func printSavedRewritePlanGroups(out io.Writer, plan state.RewritePlan) {
	fmt.Fprintf(out, "Selected commits: %d\n", rewritePlanInputCount(plan))
	fmt.Fprintf(out, "Resulting commits: %d\n", len(plan.Groups))
	for i, group := range plan.Groups {
		firstLine := strings.SplitN(strings.TrimSpace(group.ProposedMessage), "\n", 2)[0]
		fmt.Fprintf(out, "Group %d: %s\n", i+1, firstLine)
		fmt.Fprintf(out, "  Reason: %s\n", group.GroupingReason)
		for _, member := range group.Members {
			subject := strings.SplitN(strings.TrimSpace(member.OriginalMessage), "\n", 2)[0]
			fmt.Fprintf(out, "  - %s %s\n", shortenSHA(member.OldOID), subject)
		}
	}
}

func rewritePlanInputCount(plan state.RewritePlan) int {
	count := 0
	for _, group := range plan.Groups {
		count += len(group.Members)
	}
	return count
}

func editSavedRewritePlan(ctx context.Context, out io.Writer, repoFlag string, opts rewriteCommitsOptions) error {
	return errors.New("history rewrite: legacy plans remain inspectable; create a current local or messages-only plan to edit messages")
}

func applySavedRewritePlan(ctx context.Context, out io.Writer, repoFlag string, opts rewriteCommitsOptions) error {
	return errors.New("acd history rewrite: legacy plans remain inspectable; generate a new current plan before applying")
}

func showSavedRewritePlan(ctx context.Context, out io.Writer, repoFlag, ref string, jsonOut bool) error {
	repo := repoFlag
	if _, err := os.Stat(ref); err != nil {
		var resolveErr error
		repo, resolveErr = resolveRepo(repoFlag)
		if resolveErr != nil {
			return resolveErr
		}
	}
	plan, err := readRewritePlanRef(ctx, repo, ref)
	if err != nil {
		return fmt.Errorf("acd history rewrite: read show plan\nDetails: %w", err)
	}
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(plan)
	}
	fmt.Fprintf(out, "rewrite-commits saved plan display: %s\n", ref)
	fmt.Fprintln(out, "Saved plan display loaded without AI provider check; no AI call was made.")
	fmt.Fprintf(out, "Plan ID: %s\n", plan.ID)
	fmt.Fprintf(out, "Branch: %s @ %s\n", plan.BranchRef, shortenSHA(plan.ExpectedHead))
	fmt.Fprintf(out, "Commit format: %s\n", normalizeRewritePlanCommitFormat(plan.CommitFormat))
	fmt.Fprintf(out, "Validation status: %s\n", plan.ValidationStatus)
	if plan.ValidationError.Valid && strings.TrimSpace(plan.ValidationError.String) != "" {
		fmt.Fprintf(out, "Validation error: %s\n", plan.ValidationError.String)
	}
	fmt.Fprintf(out, "Apply status: %s\n", plan.ApplyStatus)
	printSavedRewritePlanGroups(out, plan)
	return nil
}

func readRewritePlanRef(ctx context.Context, repo, ref string) (state.RewritePlan, error) {
	if _, err := os.Stat(ref); err == nil {
		return readRewritePlanFile(ref)
	}
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		return state.RewritePlan{}, err
	}
	db, err := state.OpenReadOnly(ctx, dbPath)
	if err != nil {
		return state.RewritePlan{}, fmt.Errorf("open state db\nDetails: %w", err)
	}
	defer db.Close()
	plan, ok, err := state.LoadRewritePlan(ctx, db, ref)
	if err != nil {
		return state.RewritePlan{}, err
	}
	if !ok {
		return state.RewritePlan{}, fmt.Errorf("plan %q not found as file or saved plan id", ref)
	}
	return plan, nil
}

func readRewritePlanFile(path string) (state.RewritePlan, error) {
	f, err := os.Open(path)
	if err != nil {
		return state.RewritePlan{}, err
	}
	defer f.Close()
	var plan state.RewritePlan
	if err := json.NewDecoder(f).Decode(&plan); err != nil {
		return state.RewritePlan{}, err
	}
	groups, err := state.RewritePlanGroups(plan)
	if err != nil {
		return state.RewritePlan{}, err
	}
	plan.Groups = groups
	plan.Commits = nil
	plan.CommitFormat = normalizeRewritePlanCommitFormat(plan.CommitFormat)
	return plan, nil
}

func rewriteStateDBPath(ctx context.Context, repo string) (string, error) {
	gitDir, err := git.AbsoluteGitDir(ctx, repo)
	if err != nil {
		return "", fmt.Errorf("acd history rewrite: resolve git dir\nDetails: %w", err)
	}
	return state.DBPathFromGitDir(gitDir), nil
}

func normalizeRewritePlanCommitFormat(format string) string {
	switch ai.CommitFormat(strings.ToLower(strings.TrimSpace(format))) {
	case ai.CommitFormatConventional:
		return string(ai.CommitFormatConventional)
	default:
		return string(ai.CommitFormatImperative)
	}
}

func normalizeAndValidateRewriteOptions(opts *rewriteCommitsOptions) error {
	opts.fromSHA = strings.TrimSpace(opts.fromSHA)
	opts.rangeNR = strings.TrimSpace(opts.rangeNR)
	opts.rangeSHA = strings.TrimSpace(opts.rangeSHA)
	opts.selection.From = strings.TrimSpace(opts.selection.From)
	opts.selection.Range = strings.TrimSpace(opts.selection.Range)
	opts.selection.GitRange = strings.TrimSpace(opts.selection.GitRange)
	opts.base = strings.TrimSpace(opts.base)
	opts.head = strings.TrimSpace(opts.head)
	opts.showPlan = strings.TrimSpace(opts.showPlan)
	opts.applyPlan = strings.TrimSpace(opts.applyPlan)
	opts.editPlan = strings.TrimSpace(opts.editPlan)
	opts.newBranch = strings.TrimSpace(opts.newBranch)
	if opts.newBranch != "" && (opts.messagesOnly || opts.review || opts.editPlan != "" || opts.applyPlan != "" || opts.showPlan != "") {
		return errors.New("acd history rewrite: --new-branch generates a goal plan; use its saved ID for show or apply")
	}

	opts.editFormat = strings.ToLower(strings.TrimSpace(opts.editFormat))
	if opts.editFormat == "" {
		opts.editFormat = rewriteEditFormatText
	}
	opts.progress = strings.ToLower(strings.TrimSpace(opts.progress))
	if opts.progress == "" {
		opts.progress = string(rewriteProgressModeAuto)
	}
	if opts.editFormat != rewriteEditFormatText && opts.editFormat != rewriteEditFormatJSON {
		return fmt.Errorf("acd history rewrite: --format must be text or json")
	}
	if !validRewriteProgressMode(opts.progress) {
		return fmt.Errorf("acd history rewrite: --progress must be auto, plain, json, or off")
	}
	if opts.review && opts.noReview {
		return errors.New("acd history rewrite: choose only one of --review or --no-review")
	}
	if opts.planOnly && opts.applyPlan != "" {
		return errors.New("acd history rewrite: choose only one of --plan-only or --apply/--apply-plan")
	}
	if opts.editPlan != "" && (opts.review || opts.noReview) {
		return errors.New("acd history rewrite: --edit cannot be combined with --review or --no-review")
	}
	if opts.fromNR < 0 {
		return errors.New("acd history rewrite: --from-nr must be positive")
	}
	newSelectorCount := 0
	if opts.fromSHA != "" {
		newSelectorCount++
	}
	if opts.fromNR > 0 {
		newSelectorCount++
	}
	if opts.rangeNR != "" {
		newSelectorCount++
	}
	if opts.rangeSHA != "" {
		newSelectorCount++
	}
	compatSelectorCount := 0
	if opts.selection.From != "" {
		compatSelectorCount++
	}
	if opts.selection.Range != "" {
		compatSelectorCount++
	}
	if opts.selection.GitRange != "" {
		compatSelectorCount++
	}
	if opts.selection.Last > 0 {
		compatSelectorCount++
	}
	if opts.selection.Last < 0 {
		return errors.New("acd history rewrite: --last must be positive")
	}
	if newSelectorCount > 0 && compatSelectorCount > 0 {
		return errors.New("acd history rewrite: use one selector family; prefer --from-sha, --from-nr, --range-nr, or --range-sha instead of mixing compatibility flags")
	}
	if newSelectorCount > 1 {
		return errors.New("acd history rewrite: choose only one of --from-sha, --from-nr, --range-nr, or --range-sha")
	}
	if newSelectorCount > 0 && (opts.base != "" || opts.head != "") {
		return errors.New("acd history rewrite: use either --from-sha/--from-nr/--range-nr/--range-sha or --base/--head, not both")
	}
	if opts.rangeSHA != "" {
		if strings.ContainsAny(opts.rangeSHA, " \t\n\r") || !strings.Contains(opts.rangeSHA, "..") {
			return errors.New("acd history rewrite: --range-sha must be a simple git range like base..head")
		}
		opts.selection.GitRange = opts.rangeSHA
	}
	if opts.rangeNR != "" {
		opts.selection.Range = opts.rangeNR
	}
	if opts.fromNR > 0 {
		opts.selection.FromPosition = opts.fromNR
	}
	if opts.fromSHA != "" {
		opts.selection.FromSHA = opts.fromSHA
	}
	modes := 0
	if opts.showPlan != "" {
		modes++
	}
	if opts.applyPlan != "" {
		modes++
	}
	if opts.editPlan != "" {
		modes++
	}
	generate := opts.selection.Range != "" || opts.selection.GitRange != "" || opts.base != "" || opts.head != "" || opts.selection.From != "" || opts.selection.FromSHA != "" || opts.selection.FromPosition > 0 || opts.selection.Last > 0
	if generate {
		modes++
	}
	if modes == 0 {
		return errors.New("acd history rewrite: choose --from-sha, --from-nr, --range-nr, --range-sha, --last, --from, --range, --git-range, --base/--head, --show-plan, --edit, --apply, or --apply-plan")
	}
	if modes > 1 {
		return errors.New("acd history rewrite: choose only one mode: generate, --show-plan, --edit, or --apply/--apply-plan")
	}
	if opts.messagesOnly && !generate {
		return errors.New("acd history rewrite: --messages-only is only valid when generating a new plan")
	}
	if opts.selection.Range != "" && strings.Contains(opts.selection.Range, "..") {
		return errors.New("acd history rewrite: --range-nr/--range uses 1-based positions like 2-5; use --range-sha or --git-range for git revsets")
	}
	if (opts.selection.Range != "" || opts.selection.GitRange != "") && (opts.base != "" || opts.head != "") {
		return errors.New("acd history rewrite: use either --range-nr/--range-sha/--git-range or --base/--head, not both")
	}
	if opts.head != "" && opts.base == "" {
		return errors.New("acd history rewrite: --head requires --base")
	}
	if opts.base != "" {
		head := opts.head
		if head == "" {
			head = "HEAD"
		}
		opts.selection.GitRange = opts.base + ".." + head
	}
	return nil
}
