package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
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
	selection  git.RewriteSelectionOptions
	base       string
	head       string
	planOut    string
	showPlan   string
	applyPlan  string
	dryRun     bool
	yes        bool
	review     bool
	noReview   bool
	planOnly   bool
	editFormat string
	in         io.Reader
}

func newRewriteCommitsCmd() *cobra.Command {
	var opts rewriteCommitsOptions
	cmd := &cobra.Command{
		Use:   "rewrite-commits (--from <sha|position> | --range <start-end> | --last <n> | --git-range <revset>) [--plan-out FILE]",
		Short: "Generate, review, and optionally apply an AI commit rewrite plan for the current branch",
		Long: `Preview an AI-generated rewrite plan for a linear commit range on the
current branch.

Plan generation is intentionally gated: ACD_COMMIT_STRATEGY must resolve to
intent and ACD_AI_PROVIDER must name a usable non-deterministic planner provider
(openai-compat with ACD_AI_API_KEY, or subprocess:<name>). Deterministic fallback
is not enough for rewrite planning. Showing or applying a previously saved plan
may bypass the provider gate because no new plan is generated.

v1 scope: current branch linear ranges only; merge commit rewrites are refused;
there is no daemon automation.`,
		Example: `  ACD_COMMIT_STRATEGY=intent ACD_AI_PROVIDER=openai-compat ACD_AI_API_KEY=... acd rewrite-commits --from 8f4c2a1 --plan-out rewrite.json
  acd rewrite-commits --from 5 --plan-only
  acd rewrite-commits --range 5-12 --review --format text
  acd rewrite-commits --last 4 --no-review --yes
  acd rewrite-commits --git-range main~12..main~4 --format json
  acd rewrite-commits --show-plan rewrite.json
  acd rewrite-commits --apply-plan rewrite.json --dry-run
  acd rewrite-commits --apply <plan-id> --yes
  git reset --hard refs/acd/rewrite-backups/<plan-id>  # backup recovery if apply output says to use this ref`,

		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			opts.in = cmd.InOrStdin()
			return runRewriteCommits(cmd.Context(), cmd.OutOrStdout(), repo, opts, jsonOut)
		},
	}
	cmd.Flags().StringVar(&opts.selection.Range, "range", "", "1-based position range to rewrite (start-end, where 1 is HEAD)")
	cmd.Flags().StringVar(&opts.selection.GitRange, "git-range", "", "Advanced git rev-list revset; selected commits must be contiguous on the current branch")
	cmd.Flags().StringVar(&opts.base, "base", "", "Deprecated alias for --git-range <base>..<head>: exclusive base revision")
	cmd.Flags().StringVar(&opts.head, "head", "", "Deprecated alias for --git-range <base>..<head>: inclusive head revision (default HEAD when --base is set)")
	cmd.Flags().StringVar(&opts.planOut, "plan-out", "", "Write the generated rewrite plan to FILE")
	cmd.Flags().StringVar(&opts.showPlan, "show-plan", "", "Display a saved rewrite plan without checking the AI provider gate")
	cmd.Flags().StringVar(&opts.applyPlan, "apply-plan", "", "Apply a saved rewrite plan file; bypasses the plan-generation provider gate")
	cmd.Flags().StringVar(&opts.applyPlan, "apply", "", "Apply a saved rewrite plan by plan id or file path (alias for --apply-plan when given a file)")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "Validate and preview only; do not rewrite commits")
	cmd.Flags().BoolVar(&opts.yes, "yes", false, "Answer yes to apply prompts and skip confirmation in noninteractive runs")
	cmd.Flags().BoolVar(&opts.review, "review", false, "Open EDITOR to review/edit proposed commit messages before apply")
	cmd.Flags().BoolVar(&opts.noReview, "no-review", false, "Skip the review/edit prompt and leave proposed messages unchanged")
	cmd.Flags().BoolVar(&opts.planOnly, "plan-only", false, "Generate, save, and print the rewrite plan summary without prompting to apply")
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
	if opts.showPlan != "" {
		fmt.Fprintf(out, "rewrite-commits saved plan display: %s\n", opts.showPlan)
		fmt.Fprintln(out, "Saved plan display is command-contract only in this build; no AI provider check required.")
		return nil
	}
	if opts.applyPlan != "" {
		return applySavedRewritePlan(ctx, out, repoFlag, opts)
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

	plan, err := generateRewritePlan(ctx, repo, selection, provider, cfg)
	if err != nil {
		return err
	}
	if opts.planOut != "" {
		if err := writeRewritePlanFile(opts.planOut, plan); err != nil {
			return err
		}
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
	if plan.ValidationStatus == state.RewritePlanValidationInvalid {
		fmt.Fprintf(out, "Plan stored as invalid: %s\n", plan.ValidationError.String)
	} else {
		fmt.Fprintf(out, "Generated valid rewrite plan %s with %d proposal(s).\n", plan.ID, len(plan.Commits))
	}
	if opts.planOut != "" {
		fmt.Fprintf(out, "Plan written to %s\n", opts.planOut)
	}
	if plan.ValidationStatus == state.RewritePlanValidationInvalid {
		fmt.Fprintln(out, "No commits were rewritten.")
		return errors.New("acd rewrite-commits: AI proposal validation failed; invalid plan saved and apply is blocked")
	}
	if opts.planOnly || opts.dryRun {
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
			fmt.Fprintln(out, "Edited rewrite plan saved.")
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
		fmt.Fprintln(out, "No commits were rewritten.")
		return nil
	}
	applyOpts := opts
	applyOpts.applyPlan = plan.ID
	applyOpts.yes = true
	return applySavedRewritePlan(ctx, out, repoFlag, applyOpts)
}

func applySavedRewritePlan(ctx context.Context, out io.Writer, repoFlag string, opts rewriteCommitsOptions) error {
	if !opts.yes && !opts.dryRun {
		return errors.New("acd rewrite-commits: --apply-plan requires --yes or --dry-run")
	}
	repo, err := resolveRepo(repoFlag)
	if err != nil {
		return err
	}
	plan, err := readRewritePlanRef(ctx, repo, opts.applyPlan)
	if err != nil {
		return fmt.Errorf("acd rewrite-commits: read apply plan: %w", err)
	}
	if plan.ValidationStatus == state.RewritePlanValidationInvalid {
		return fmt.Errorf("acd rewrite-commits: cannot apply invalid rewrite plan: %s", plan.ValidationError.String)
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
	reconcile, err := state.ReconcileRewriteCommitOIDs(ctx, db, res.CommitMap)
	if err != nil {
		return fmt.Errorf("acd rewrite-commits: reconcile state OIDs after successful git rewrite: %w", err)
	}
	if plan.ID != "" {
		if err := markRewritePlanStatusIfPresent(ctx, db, plan.ID, state.RewritePlanApplyApplied); err != nil {
			return fmt.Errorf("acd rewrite-commits: mark plan applied: %w", err)
		}
	}
	fmt.Fprintf(out, "Applied rewrite plan to %s: %s -> %s (%d commit(s) recreated).\n", plan.BranchRef, shortenSHA(res.OldHead), shortenSHA(res.NewHead), res.RecreatedCount)
	fmt.Fprintf(out, "Backup branch: %s\n", res.BackupBranchRef)
	if res.InternalBackupRef != "" {
		fmt.Fprintf(out, "Internal backup ref: %s\n", res.InternalBackupRef)
	}
	fmt.Fprintf(out, "Recovery: git reset --hard %s\n", res.BackupBranchRef)
	fmt.Fprintf(out, "State OID reconciliation: capture_events=%d decision_records=%d publish_state_target=%d publish_state_source=%d\n", reconcile.CaptureEvents, reconcile.DecisionRecords, reconcile.PublishTargetCommitOID, reconcile.PublishSourceHead)
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

func generateRewritePlan(ctx context.Context, repo string, selection git.RewriteSelection, provider ai.Provider, cfg ai.ProviderConfig) (state.RewritePlan, error) {
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
		req, err := buildCommitRewriteRequest(ctx, repo, db, selection.Selected, i, c, ai.ProviderNeedsDiff(provider))
		if err != nil {
			return state.RewritePlan{}, err
		}
		result, err := proposer.ProposeCommitRewrite(ctx, req)
		if err != nil {
			plan.ValidationStatus = state.RewritePlanValidationInvalid
			plan.ValidationError = sql.NullString{String: rewriteFailureJSON(c.OID, err), Valid: true}
			plan.Commits = append(plan.Commits, state.RewritePlanCommit{OldOID: c.OID, OriginalMessage: c.Message, ProposedMessage: c.Message})
			break
		}
		message := result.Subject
		if strings.TrimSpace(result.Body) != "" {
			message += "\n\n" + result.Body
		}
		plan.Commits = append(plan.Commits, state.RewritePlanCommit{OldOID: c.OID, OriginalMessage: c.Message, ProposedMessage: message})
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
	opts.editFormat = strings.ToLower(strings.TrimSpace(opts.editFormat))
	if opts.editFormat == "" {
		opts.editFormat = rewriteEditFormatText
	}
	if opts.editFormat != rewriteEditFormatText && opts.editFormat != rewriteEditFormatJSON {
		return fmt.Errorf("acd rewrite-commits: --format must be text or json")
	}
	if opts.review && opts.noReview {
		return errors.New("acd rewrite-commits: choose only one of --review or --no-review")
	}
	if opts.planOnly && opts.applyPlan != "" {
		return errors.New("acd rewrite-commits: choose only one of --plan-only or --apply/--apply-plan")
	}
	modes := 0
	if strings.TrimSpace(opts.showPlan) != "" {
		modes++
	}
	if strings.TrimSpace(opts.applyPlan) != "" {
		modes++
	}
	generate := opts.selection.Range != "" || opts.selection.GitRange != "" || opts.base != "" || opts.head != "" || opts.selection.From != "" || opts.selection.Last > 0
	if generate {
		modes++
	}
	if modes == 0 {
		return errors.New("acd rewrite-commits: choose --from, --range, --last, --git-range, --base/--head, --show-plan, --apply, or --apply-plan")
	}
	if modes > 1 {
		return errors.New("acd rewrite-commits: choose only one mode: generate, --show-plan, or --apply/--apply-plan")
	}
	if opts.selection.Range != "" && strings.Contains(opts.selection.Range, "..") {
		return errors.New("acd rewrite-commits: --range uses 1-based positions like 2-5; use --git-range for git revsets")
	}
	if (opts.selection.Range != "" || opts.selection.GitRange != "") && (opts.base != "" || opts.head != "") {
		return errors.New("acd rewrite-commits: use either --range/--git-range or --base/--head, not both")
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
