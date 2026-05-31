package cli

import (
	"context"
	"database/sql"
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

	base       string
	head       string
	planOut    string
	showPlan   string
	applyPlan  string
	editPlan   string
	dryRun     bool
	yes        bool
	review     bool
	noReview   bool
	planOnly   bool
	editFormat string
	progress   string
	progressTo io.Writer
	quiet      bool
	in         io.Reader
}

func newRewriteCommitsCmd() *cobra.Command {
	var opts rewriteCommitsOptions
	cmd := &cobra.Command{
		Use:     "rewrite-commits (--from-nr <n> | --from-sha <sha> | --range-nr <start-end> | --range-sha <base>..<head> | --last <n>) [--plan-out FILE] | --edit <plan-id-or-file>",
		Aliases: []string{"edit-commits", "edit-commit"},
		Short:   "Generate, review, edit, and optionally apply an AI commit rewrite plan for the current branch",
		Long: `Preview an AI-generated rewrite plan for a linear commit range on the
current branch.

Plan generation is intentionally gated: ACD_COMMIT_STRATEGY must resolve to
intent and ACD_AI_PROVIDER must name a usable non-deterministic planner provider
(openai-compat with ACD_AI_API_KEY, or subprocess:<name>). Deterministic fallback
is not enough for rewrite planning. Showing, editing, or applying a previously
saved plan may bypass the provider gate because no new plan is generated.

Use --edit <plan-id-or-file> to reopen a saved rewrite plan in $EDITOR. The
editor uses --format text by default, or --format json. Edit only the proposed
commit message text. When the target is a saved plan id, changed edits are
validated and saved as a new plan revision, and the new plan id is printed.
When the target is a standalone JSON plan file, changed edits are written back
to that file. Unchanged edits are accepted. After editing, ACD asks whether to
apply the plan unless --plan-only is set; --yes applies after edit, and --dry-run
validates/previews apply without rewriting commits.

v1 scope: current branch linear ranges only; merge commit rewrites are refused;
there is no daemon automation.`,
		Example: `  ACD_COMMIT_STRATEGY=intent ACD_AI_PROVIDER=openai-compat ACD_AI_API_KEY=... acd rewrite-commits --from-sha 8f4c2a1 --plan-out rewrite.json
  acd rewrite-commits --from-nr 5 --plan-only
  acd rewrite-commits --range-nr 5-12 --review --format text
  acd rewrite-commits --range-sha main~12..main~4 --format json
  acd rewrite-commits --last 4 --no-review --yes
  acd rewrite-commits --from 5 --plan-only        # compatibility alias
  acd rewrite-commits --git-range main~12..main~4 --format json
  acd rewrite-commits --show-plan rewrite.json
  acd rewrite-commits --edit <plan-id-or-file> --format text --plan-only
  acd rewrite-commits --edit <plan-id-or-file> --dry-run
  acd rewrite-commits --edit <plan-id-or-file> --yes
  acd rewrite-commits --apply-plan rewrite.json --dry-run
  acd rewrite-commits --apply <plan-id> --yes
  git reset --hard refs/acd/rewrite-backups/<plan-id>  # backup recovery if apply output says to use this ref`,

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
	cmd.Flags().StringVar(&opts.selection.GitRange, "git-range", "", "Advanced git rev-list revset; selected commits must be contiguous on the current branch")
	cmd.Flags().StringVar(&opts.base, "base", "", "Deprecated alias for --git-range <base>..<head>: exclusive base revision")
	cmd.Flags().StringVar(&opts.head, "head", "", "Deprecated alias for --git-range <base>..<head>: inclusive head revision (default HEAD when --base is set)")
	cmd.Flags().StringVar(&opts.planOut, "plan-out", "", "Write the generated rewrite plan to FILE")
	cmd.Flags().StringVar(&opts.showPlan, "show-plan", "", "Display a saved rewrite plan without checking the AI provider gate")
	cmd.Flags().StringVar(&opts.editPlan, "edit", "", "Edit a saved rewrite plan by plan id or file path without checking the AI provider gate")
	cmd.Flags().StringVar(&opts.applyPlan, "apply-plan", "", "Apply a saved rewrite plan file; bypasses the plan-generation provider gate")
	cmd.Flags().StringVar(&opts.applyPlan, "apply", "", "Apply a saved rewrite plan by plan id or file path (alias for --apply-plan when given a file)")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "Validate and preview only; do not rewrite commits")
	cmd.Flags().BoolVar(&opts.yes, "yes", false, "Answer yes to apply prompts and skip confirmation in noninteractive runs")
	cmd.Flags().BoolVar(&opts.review, "review", false, "Open EDITOR to review/edit proposed commit messages before apply")
	cmd.Flags().BoolVar(&opts.noReview, "no-review", false, "Skip the review/edit prompt and leave proposed messages unchanged")
	cmd.Flags().BoolVar(&opts.planOnly, "plan-only", false, "Generate or edit and save the rewrite plan without prompting to apply")
	cmd.Flags().StringVar(&opts.progress, "progress", string(rewriteProgressModeAuto), "Progress output mode: auto, plain, json, or off")
	cmd.Flags().StringVar(&opts.editFormat, "format", rewriteEditFormatText, "Review edit format: text or json")
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
	progress, err := newRewriteProgressSink(opts.progress, opts.quiet, opts.progressTo)
	if err != nil {
		return err
	}
	if opts.showPlan != "" {
		return showSavedRewritePlan(ctx, out, repoFlag, opts.showPlan, jsonOut)
	}
	if opts.applyPlan != "" {
		return applySavedRewritePlan(ctx, out, repoFlag, opts)
	}
	if opts.editPlan != "" {
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
	if err := progress.Emit(rewriteProgressEvent{
		Phase:   "selection",
		Message: fmt.Sprintf("selected %d commit(s)", len(report.Selected)),
		Current: len(report.Selected),
		Total:   len(report.Selected),
	}); err != nil {
		return err
	}
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	fmt.Fprintf(out, "rewrite-commits plan generation accepted for %s\n", rewriteSelectionLabel(opts))
	printRewriteSelectionSummary(out, report)

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
	if err := progress.Emit(rewriteProgressEvent{Phase: "provider", Message: fmt.Sprintf("using %s", ai.PrimaryProviderName(provider))}); err != nil {
		return err
	}

	plan, err := generateRewritePlan(ctx, repo, selection, provider, cfg, progress)
	if err != nil {
		return err
	}
	if opts.planOut != "" {
		if err := writeRewritePlanFile(opts.planOut, plan); err != nil {
			return err
		}
		if err := progress.Emit(rewriteProgressEvent{Phase: "save", Message: fmt.Sprintf("wrote %s", opts.planOut), PlanID: plan.ID}); err != nil {
			return err
		}
	}

	if plan.ValidationStatus == state.RewritePlanValidationInvalid {
		fmt.Fprintf(out, "Plan stored as invalid: %s\n", plan.ValidationError.String)
	} else {
		fmt.Fprintf(out, "Generated valid rewrite plan %s with %d proposal(s).\n", plan.ID, len(plan.Commits))
	}
	if err := progress.Emit(rewriteProgressEvent{Phase: "validation", Message: fmt.Sprintf("status %s", plan.ValidationStatus), PlanID: plan.ID}); err != nil {
		return err
	}
	if opts.planOut != "" {
		fmt.Fprintf(out, "Plan written to %s\n", opts.planOut)
	}
	if plan.ValidationStatus == state.RewritePlanValidationInvalid {
		fmt.Fprintln(out, "No commits were rewritten.")
		return errors.New("acd rewrite-commits: AI proposal validation failed; invalid plan saved and apply is blocked")
	}
	if opts.planOnly {
		planRef := plan.ID
		if opts.planOut != "" {
			planRef = opts.planOut
		}
		if err := progress.Emit(rewriteProgressEvent{Phase: "next", Message: "plan saved; git history unchanged", PlanID: plan.ID}); err != nil {
			return err
		}
		finishRewritePlanOnly(out, planRef)
		return nil
	}
	if opts.dryRun {
		fmt.Fprintln(out, "No commits were rewritten.")
		return nil
	}

	review := opts.review
	if opts.review && opts.noReview {
		return errors.New("acd rewrite-commits: choose only one of --review or --no-review")
	}
	if !opts.yes && !opts.review && !opts.noReview {
		var err error
		review, err = promptRewriteYesNo(inputOrStdin(opts.in), out, "Review or edit proposed messages before applying?", false)
		if err != nil {
			return err
		}
	}
	if review {
		commits, changed, err := editRewritePlanWithEditor(plan, opts.editFormat)
		if err != nil {
			return err
		}
		updated, err := persistEditedRewritePlan(ctx, repo, plan, commits)
		if err != nil {
			return err
		}
		plan = updated
		if changed {
			fmt.Fprintf(out, "Edited rewrite plan saved as %s.\n", plan.ID)
		} else {
			fmt.Fprintln(out, "Review complete; proposed messages unchanged.")
		}
	}

	applyNow := opts.yes
	if !opts.yes {
		var err error
		applyNow, err = promptRewriteYesNo(inputOrStdin(opts.in), out, "Apply this rewrite plan now?", false)
		if err != nil {
			return err
		}
	}
	if !applyNow {
		fmt.Fprintln(out, "No rewrite performed.")
		return nil
	}
	applyOpts := opts
	applyOpts.applyPlan = plan.ID
	applyOpts.yes = true
	return applySavedRewritePlan(ctx, out, repoFlag, applyOpts)
}

func finishRewritePlanOnly(out io.Writer, planRef string) {
	fmt.Fprintln(out, "Plan saved. Git history unchanged.")
	printRewritePlanNextSteps(out, planRef)
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

func printRewritePlanNextSteps(out io.Writer, planRef string) {
	arg := rewritePlanRefArg(planRef)
	fmt.Fprintln(out, "Next:")
	fmt.Fprintf(out, "  acd rewrite-commits --show-plan %s\n", arg)
	fmt.Fprintf(out, "  acd rewrite-commits --apply-plan %s --dry-run\n", arg)
	fmt.Fprintf(out, "  acd rewrite-commits --apply-plan %s --yes\n", arg)
}

func printRewriteSelectionSummary(out io.Writer, report rewriteSelectionReport) {
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
}

func editSavedRewritePlan(ctx context.Context, out io.Writer, repoFlag string, opts rewriteCommitsOptions) error {
	repo, err := resolveRepo(repoFlag)
	if err != nil {
		return err
	}
	plan, err := readRewritePlanRef(ctx, repo, opts.editPlan)
	if err != nil {
		return fmt.Errorf("acd rewrite-commits: read edit plan: %w", err)
	}
	commits, changed, err := editRewritePlanWithEditor(plan, opts.editFormat)
	if err != nil {
		return err
	}
	applyRef := opts.editPlan
	if changed {
		if isRewritePlanFileRef(opts.editPlan) {
			plan.Commits = commits
			plan.ValidationStatus = state.RewritePlanValidationValid
			plan.ValidationError = sql.NullString{}
			plan.ApplyStatus = state.RewritePlanApplyPending
			plan.Edited = true
			if err := writeRewritePlanFile(opts.editPlan, plan); err != nil {
				return err
			}
			fmt.Fprintf(out, "Edited rewrite plan file saved: %s.\n", opts.editPlan)
		} else {
			updated, err := persistEditedRewritePlan(ctx, repo, plan, commits)
			if err != nil {
				return err
			}
			plan = updated
			applyRef = plan.ID
			fmt.Fprintf(out, "Edited rewrite plan saved as %s.\n", plan.ID)
		}
	} else {
		fmt.Fprintln(out, "Edited rewrite plan unchanged; no new revision saved.")
	}
	fmt.Fprintln(out, "Saved plan edit loaded without AI provider check; no AI call was made.")
	if opts.planOnly {
		finishRewritePlanOnly(out, applyRef)
		return nil
	}
	if opts.dryRun {
		applyOpts := opts
		applyOpts.applyPlan = applyRef
		return applySavedRewritePlan(ctx, out, repoFlag, applyOpts)
	}
	applyNow := opts.yes
	if !opts.yes {
		applyNow, err = promptRewriteYesNo(inputOrStdin(opts.in), out, "Apply this edited rewrite plan now?", false)
		if err != nil {
			return err
		}
	}
	if !applyNow {
		fmt.Fprintln(out, "No rewrite performed.")
		return nil
	}
	applyOpts := opts
	applyOpts.applyPlan = applyRef
	applyOpts.yes = true
	return applySavedRewritePlan(ctx, out, repoFlag, applyOpts)
}

func isRewritePlanFileRef(ref string) bool {
	if strings.TrimSpace(ref) == "" {
		return false
	}
	info, err := os.Stat(ref)
	return err == nil && !info.IsDir()
}

func applySavedRewritePlan(ctx context.Context, out io.Writer, repoFlag string, opts rewriteCommitsOptions) error {
	if !opts.yes && !opts.dryRun {
		return errors.New("acd rewrite-commits: --apply-plan requires --yes or --dry-run")
	}
	progress, err := newRewriteProgressSink(opts.progress, opts.quiet, opts.progressTo)
	if err != nil {
		return err
	}
	repo, err := resolveRepo(repoFlag)
	if err != nil {
		return err
	}
	plan, err := readRewritePlanRef(ctx, repo, opts.applyPlan)
	if err != nil {
		return fmt.Errorf("acd rewrite-commits: read apply plan: %w", err)
	}
	if err := validateRewritePlanReadyForApply(plan); err != nil {
		return err
	}
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		return err
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("acd rewrite-commits: open state db: %w", err)
	}
	defer db.Close()
	if daemonState, _, err := state.LoadDaemonState(ctx, db); err != nil {
		return fmt.Errorf("acd rewrite-commits: inspect daemon state: %w", err)
	} else if daemonState.Mode != "" && daemonState.Mode != "stopped" {
		return fmt.Errorf("acd rewrite-commits: daemon must be stopped before applying rewrite plan (current mode: %s)", daemonState.Mode)
	}
	if !opts.dryRun {
		pending, err := state.CountAllPendingCaptureEvents(ctx, db)
		if err != nil {
			return fmt.Errorf("acd rewrite-commits: inspect pending capture queue: %w", err)
		}
		if pending.Count > 0 {
			return fmt.Errorf("acd rewrite-commits: refusing to rewrite while capture queue has pending event(s); flush or clear the ACD queue first (pending: %d, oldest seq: %d)", pending.Count, pending.OldestSeq)
		}
	}

	applyCommits := make([]git.RewriteApplyCommit, 0, len(plan.Commits))
	for _, c := range plan.Commits {
		applyCommits = append(applyCommits, git.RewriteApplyCommit{OldOID: c.OldOID, ProposedMessage: c.ProposedMessage})
	}
	res, err := git.ApplyRewritePlan(ctx, repo, git.RewriteApplyOptions{
		BranchRef:    plan.BranchRef,
		ExpectedHead: plan.ExpectedHead,
		PlanID:       plan.ID,
		Commits:      applyCommits,
		DryRun:       opts.dryRun,
		Progress: func(event git.RewriteApplyProgress) error {
			return progress.Emit(rewriteProgressEvent{
				Phase:        "apply_" + event.Phase,
				Message:      event.Message,
				Current:      event.Current,
				Total:        event.Total,
				CommitOID:    event.OldOID,
				NewCommitOID: event.NewOID,
				BackupRef:    event.BackupRef,
			})
		},
	})
	if err != nil {
		if plan.ID != "" && !opts.dryRun {
			_ = markRewritePlanStatusIfPresent(ctx, db, plan.ID, state.RewritePlanApplyFailed)
		}
		return err
	}

	fmt.Fprintf(out, "rewrite-commits saved plan apply: %s\n", opts.applyPlan)
	fmt.Fprintln(out, "Saved plan apply loaded without AI provider check; no second AI call was made.")
	if opts.dryRun {
		fmt.Fprintf(out, "Dry run: plan can apply to %s at %s; no commits were rewritten.\n", plan.BranchRef, shortenSHA(plan.ExpectedHead))
		return nil
	}
	fmt.Fprintf(out, "Applied rewrite plan to %s: %s -> %s (%d commit(s) recreated).\n", plan.BranchRef, shortenSHA(res.OldHead), shortenSHA(res.NewHead), res.RecreatedCount)
	fmt.Fprintf(out, "Backup branch: %s\n", res.BackupBranchRef)
	if res.InternalBackupRef != "" {
		fmt.Fprintf(out, "Internal backup ref: %s\n", res.InternalBackupRef)
	}
	fmt.Fprintf(out, "Recovery: git reset --hard %s\n", res.BackupBranchRef)
	reconcile, err := state.ReconcileRewriteCommitOIDs(ctx, db, res.CommitMap)
	if err != nil {
		return fmt.Errorf("acd rewrite-commits: reconcile state OIDs after successful git rewrite: %w", err)
	}
	if err := progress.Emit(rewriteProgressEvent{Phase: "apply_reconcile", Message: "reconciled state OIDs", Current: reconcile.CaptureEvents + reconcile.DecisionRecords + reconcile.PublishTargetCommitOID + reconcile.PublishSourceHead}); err != nil {
		return err
	}
	if plan.ID != "" {
		if err := markRewritePlanStatusIfPresent(ctx, db, plan.ID, state.RewritePlanApplyApplied); err != nil {
			return fmt.Errorf("acd rewrite-commits: mark plan applied: %w", err)
		}
	}
	fmt.Fprintf(out, "State OID reconciliation: capture_events=%d decision_records=%d publish_state_target=%d publish_state_source=%d\n", reconcile.CaptureEvents, reconcile.DecisionRecords, reconcile.PublishTargetCommitOID, reconcile.PublishSourceHead)
	return nil
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
		return fmt.Errorf("acd rewrite-commits: read show plan: %w", err)
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
	fmt.Fprintf(out, "Validation status: %s\n", plan.ValidationStatus)
	if plan.ValidationError.Valid && strings.TrimSpace(plan.ValidationError.String) != "" {
		fmt.Fprintf(out, "Validation error: %s\n", plan.ValidationError.String)
	}
	fmt.Fprintf(out, "Apply status: %s\n", plan.ApplyStatus)
	fmt.Fprintf(out, "Commits (%d):\n", len(plan.Commits))
	for _, c := range plan.Commits {
		firstLine := strings.SplitN(strings.TrimSpace(c.ProposedMessage), "\n", 2)[0]
		fmt.Fprintf(out, "- %s %s\n", shortenSHA(c.OldOID), firstLine)
	}
	return nil
}

func validateRewritePlanReadyForApply(plan state.RewritePlan) error {
	if plan.ValidationStatus != state.RewritePlanValidationValid {
		if plan.ValidationStatus == state.RewritePlanValidationInvalid && plan.ValidationError.Valid && strings.TrimSpace(plan.ValidationError.String) != "" {
			return fmt.Errorf("acd rewrite-commits: cannot apply rewrite plan with validation status %q: %s", plan.ValidationStatus, plan.ValidationError.String)
		}
		if strings.TrimSpace(plan.ValidationStatus) == "" {
			return errors.New("acd rewrite-commits: cannot apply rewrite plan without validation status \"valid\"")
		}
		return fmt.Errorf("acd rewrite-commits: cannot apply rewrite plan with validation status %q; expected %q", plan.ValidationStatus, state.RewritePlanValidationValid)
	}
	return nil
}

func markRewritePlanStatusIfPresent(ctx context.Context, db *state.DB, id, status string) error {
	if _, ok, err := state.LoadRewritePlan(ctx, db, id); err != nil {
		return err
	} else if !ok {
		return nil
	}
	return state.MarkRewritePlanApplyStatus(ctx, db, id, status)
}

func readRewritePlanRef(ctx context.Context, repo, ref string) (state.RewritePlan, error) {
	if _, err := os.Stat(ref); err == nil {
		return readRewritePlanFile(ref)
	}
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		return state.RewritePlan{}, err
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return state.RewritePlan{}, fmt.Errorf("open state db: %w", err)
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
	return plan, nil
}

func writeRewritePlanFile(path string, plan state.RewritePlan) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("acd rewrite-commits: create plan file: %w", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(plan)
}

func generateRewritePlan(ctx context.Context, repo string, selection git.RewriteSelection, provider ai.Provider, cfg ai.ProviderConfig, progress rewriteProgressSink) (state.RewritePlan, error) {
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		return state.RewritePlan{}, err
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return state.RewritePlan{}, fmt.Errorf("acd rewrite-commits: open state db: %w", err)
	}
	defer db.Close()

	proposer, ok := provider.(ai.CommitRewriteProposer)
	if !ok {
		return state.RewritePlan{}, fmt.Errorf("acd rewrite-commits: provider %s cannot generate rewrite proposals", ai.PrimaryProviderName(provider))
	}
	plan := state.RewritePlan{
		BranchRef:        selection.BranchRef,
		ExpectedHead:     selection.Head,
		Provider:         sql.NullString{String: ai.PrimaryProviderName(provider), Valid: ai.PrimaryProviderName(provider) != ""},
		Model:            sql.NullString{String: cfg.Model, Valid: cfg.Model != ""},
		ValidationStatus: state.RewritePlanValidationValid,
		ApplyStatus:      state.RewritePlanApplyPending,
	}
	for i, c := range selection.Selected {
		if err := progress.Emit(rewriteProgressEvent{
			Phase:         "proposal",
			Message:       "requesting proposal",
			Current:       i + 1,
			Total:         len(selection.Selected),
			CommitOID:     c.OID,
			CommitSubject: c.Subject,
		}); err != nil {
			return state.RewritePlan{}, err
		}
		req, err := buildCommitRewriteRequest(ctx, repo, db, selection.Selected, i, c, ai.ProviderNeedsDiff(provider))
		if err != nil {
			return state.RewritePlan{}, err
		}
		result, err := proposer.ProposeCommitRewrite(ctx, req)
		if err != nil {
			plan.ValidationStatus = state.RewritePlanValidationInvalid
			plan.ValidationError = sql.NullString{String: rewriteFailureJSON(c.OID, err), Valid: true}
			plan.Commits = append(plan.Commits, state.RewritePlanCommit{OldOID: c.OID, OriginalMessage: c.Message, ProposedMessage: c.Message})
			if emitErr := progress.Emit(rewriteProgressEvent{
				Phase:         "proposal",
				Message:       "proposal failed",
				Current:       i + 1,
				Total:         len(selection.Selected),
				CommitOID:     c.OID,
				CommitSubject: c.Subject,
			}); emitErr != nil {
				return state.RewritePlan{}, emitErr
			}
			break
		}
		message := result.Subject
		if strings.TrimSpace(result.Body) != "" {
			message += "\n\n" + result.Body
		}
		plan.Commits = append(plan.Commits, state.RewritePlanCommit{OldOID: c.OID, OriginalMessage: c.Message, ProposedMessage: message})
		if err := progress.Emit(rewriteProgressEvent{
			Phase:         "proposal",
			Message:       "proposal accepted",
			Current:       i + 1,
			Total:         len(selection.Selected),
			CommitOID:     c.OID,
			CommitSubject: c.Subject,
		}); err != nil {
			return state.RewritePlan{}, err
		}
	}
	if len(plan.Commits) == 0 && len(selection.Selected) > 0 {
		c := selection.Selected[0]
		plan.ValidationStatus = state.RewritePlanValidationInvalid
		plan.ValidationError = sql.NullString{String: rewriteFailureJSON(c.OID, errors.New("no proposal generated")), Valid: true}
		plan.Commits = []state.RewritePlanCommit{{OldOID: c.OID, OriginalMessage: c.Message, ProposedMessage: c.Message}}
	}
	id, err := state.SaveRewritePlan(ctx, db, plan)
	if err != nil {
		return state.RewritePlan{}, fmt.Errorf("acd rewrite-commits: save rewrite plan: %w", err)
	}
	plan.ID = id
	if err := progress.Emit(rewriteProgressEvent{Phase: "save", Message: "saved plan", PlanID: plan.ID}); err != nil {
		return state.RewritePlan{}, err
	}
	return plan, nil
}

func rewriteStateDBPath(ctx context.Context, repo string) (string, error) {
	gitDir, err := git.AbsoluteGitDir(ctx, repo)
	if err != nil {
		return "", fmt.Errorf("acd rewrite-commits: resolve git dir: %w", err)
	}
	return state.DBPathFromGitDir(gitDir), nil
}

func buildCommitRewriteRequest(ctx context.Context, repo string, db *state.DB, all []git.RewriteCommitRecord, idx int, c git.RewriteCommitRecord, providerNeedsDiff bool) (ai.CommitRewriteRequest, error) {
	paths, err := gitOutputLines(ctx, repo, "diff-tree", "--no-commit-id", "--name-only", "-r", c.OID)
	if err != nil {
		return ai.CommitRewriteRequest{}, err
	}
	stat, err := gitOutputString(ctx, repo, "show", "--stat", "--format=", c.OID)
	if err != nil {
		return ai.CommitRewriteRequest{}, err
	}
	req := ai.CommitRewriteRequest{OldOID: c.OID, OriginalMessage: c.Message, ChangedPaths: paths, DiffStat: strings.TrimSpace(stat), DiffIncluded: false}
	if providerNeedsDiff && rewriteDiffEgressOptIn() {
		diff, err := gitOutputString(ctx, repo, "show", "--format=", "--no-ext-diff", "--unified=80", c.OID)
		if err != nil {
			return ai.CommitRewriteRequest{}, err
		}
		req.RedactedDiff = ai.Truncate(ai.RedactDiffSecrets(diff), ai.IntentStageDiffCap)
		req.DiffIncluded = req.RedactedDiff != ""
	}
	for _, n := range rewriteNeighbors(all, idx) {
		req.NeighborCommits = append(req.NeighborCommits, ai.CommitSummary{OID: n.OID, Subject: n.Subject, Body: strings.TrimSpace(strings.TrimPrefix(n.Message, n.Subject))})
	}
	decisions, err := state.DecisionsForCommit(ctx, db, c.OID, 50)
	if err == nil {
		for _, d := range decisions {
			req.DecisionContext = append(req.DecisionContext, decisionContextFromRecord(d))
		}
	}
	return req, nil
}

func rewriteNeighbors(all []git.RewriteCommitRecord, idx int) []git.RewriteCommitRecord {
	var out []git.RewriteCommitRecord
	if idx > 0 {
		out = append(out, all[idx-1])
	}
	if idx+1 < len(all) {
		out = append(out, all[idx+1])
	}
	return out
}

func decisionContextFromRecord(d state.DecisionRecord) ai.RewriteDecisionContext {
	out := ai.RewriteDecisionContext{Kind: d.Kind}
	if d.Path.Valid {
		out.Path = d.Path.String
	}
	if d.Reason.Valid {
		out.Reason = d.Reason.String
	}
	if d.EventSeq.Valid {
		out.EventSeq = d.EventSeq.Int64
	}
	if d.ActionTaken.Valid {
		out.ActionTaken = d.ActionTaken.String
	}
	if d.UserMessage.Valid {
		out.UserMessage = d.UserMessage.String
	}
	return out
}

func gitOutputString(ctx context.Context, repo string, args ...string) (string, error) {
	out, err := git.Run(ctx, git.RunOpts{Dir: repo, Timeout: git.DefaultReadTimeout}, args...)
	if err != nil {
		return "", fmt.Errorf("acd rewrite-commits: git %s: %w", strings.Join(args, " "), err)
	}
	return string(out), nil
}

func gitOutputLines(ctx context.Context, repo string, args ...string) ([]string, error) {
	out, err := gitOutputString(ctx, repo, args...)
	if err != nil {
		return nil, err
	}
	var lines []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines, nil
}

func rewriteDiffEgressOptIn() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("ACD_AI_DIFF_EGRESS"))) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

func rewriteFailureJSON(oid string, err error) string {
	body, _ := json.Marshal(map[string]string{"commit_oid": oid, "error": err.Error()})
	return string(body)
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

	opts.editFormat = strings.ToLower(strings.TrimSpace(opts.editFormat))
	if opts.editFormat == "" {
		opts.editFormat = rewriteEditFormatText
	}
	opts.progress = strings.ToLower(strings.TrimSpace(opts.progress))
	if opts.progress == "" {
		opts.progress = string(rewriteProgressModeAuto)
	}
	if opts.editFormat != rewriteEditFormatText && opts.editFormat != rewriteEditFormatJSON {
		return fmt.Errorf("acd rewrite-commits: --format must be text or json")
	}
	if !validRewriteProgressMode(opts.progress) {
		return fmt.Errorf("acd rewrite-commits: --progress must be auto, plain, json, or off")
	}
	if opts.review && opts.noReview {
		return errors.New("acd rewrite-commits: choose only one of --review or --no-review")
	}
	if opts.planOnly && opts.applyPlan != "" {
		return errors.New("acd rewrite-commits: choose only one of --plan-only or --apply/--apply-plan")
	}
	if opts.editPlan != "" && (opts.review || opts.noReview) {
		return errors.New("acd rewrite-commits: --edit cannot be combined with --review or --no-review")
	}
	if opts.fromNR < 0 {
		return errors.New("acd rewrite-commits: --from-nr must be positive")
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
		return errors.New("acd rewrite-commits: --last must be positive")
	}
	if newSelectorCount > 0 && compatSelectorCount > 0 {
		return errors.New("acd rewrite-commits: use one selector family; prefer --from-sha, --from-nr, --range-nr, or --range-sha instead of mixing compatibility flags")
	}
	if newSelectorCount > 1 {
		return errors.New("acd rewrite-commits: choose only one of --from-sha, --from-nr, --range-nr, or --range-sha")
	}
	if newSelectorCount > 0 && (opts.base != "" || opts.head != "") {
		return errors.New("acd rewrite-commits: use either --from-sha/--from-nr/--range-nr/--range-sha or --base/--head, not both")
	}
	if opts.rangeSHA != "" {
		if strings.ContainsAny(opts.rangeSHA, " \t\n\r") || !strings.Contains(opts.rangeSHA, "..") {
			return errors.New("acd rewrite-commits: --range-sha must be a simple git range like base..head")
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
		return errors.New("acd rewrite-commits: choose --from-sha, --from-nr, --range-nr, --range-sha, --last, --from, --range, --git-range, --base/--head, --show-plan, --edit, --apply, or --apply-plan")
	}
	if modes > 1 {
		return errors.New("acd rewrite-commits: choose only one mode: generate, --show-plan, --edit, or --apply/--apply-plan")
	}
	if opts.selection.Range != "" && strings.Contains(opts.selection.Range, "..") {
		return errors.New("acd rewrite-commits: --range-nr/--range uses 1-based positions like 2-5; use --range-sha or --git-range for git revsets")
	}
	if (opts.selection.Range != "" || opts.selection.GitRange != "") && (opts.base != "" || opts.head != "") {
		return errors.New("acd rewrite-commits: use either --range-nr/--range-sha/--git-range or --base/--head, not both")
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
	return nil
}

func rewriteSelectionLabel(opts rewriteCommitsOptions) string {
	if opts.selection.FromSHA != "" {
		return opts.selection.FromSHA + "..HEAD"
	}
	if opts.selection.FromPosition > 0 {
		return fmt.Sprintf("from position %d", opts.selection.FromPosition)
	}
	if opts.selection.Range != "" {
		return opts.selection.Range
	}
	if opts.selection.GitRange != "" {
		return opts.selection.GitRange
	}
	if opts.selection.From != "" {
		return opts.selection.From + "..HEAD"
	}
	return fmt.Sprintf("last %d commit(s)", opts.selection.Last)
}
