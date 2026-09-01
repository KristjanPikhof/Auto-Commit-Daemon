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
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
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
	editFormat   string
	progress     string
	progressTo   io.Writer
	quiet        bool
	in           io.Reader
}

type rewriteProviderResolution struct {
	Config         ai.ProviderConfig
	ProviderSource config.Source
	StrategySource config.Source
	GlobalConfig   ai.ProviderConfig
	GlobalSource   config.Source
}

func newRewriteCommitsCmd() *cobra.Command {
	var opts rewriteCommitsOptions
	cmd := &cobra.Command{
		Use:     "rewrite-commits (--from-nr <n> | --from-sha <sha> | --range-nr <start-end> | --range-sha <base>..<head> | --last <n>) [--plan-out FILE] | --edit <plan-id-or-file>",
		Aliases: []string{"edit-commits", "edit-commit"},
		Short:   "Review and improve commit messages on this branch",
		Long: `Create a reviewable plan that improves commit messages in a linear
part of the current branch. Normal ACD protection never runs this command.

Recommended workflow:
  1. Select commits with --last, --from-sha, --from-nr, --range-sha, or
     --range-nr.
  2. Save a plan with --plan-only or --plan-out.
  3. Review it with --show-plan or --edit.
  4. Preview the apply with --dry-run, then confirm it with --yes.

Applying a plan rewrites Git history. ACD first verifies that the selected
commits are safe to rewrite and creates a private backup ref. Merge commits are
not supported. Working-tree files are not changed.

Creating a new plan requires Intent mode and a configured AI provider. Showing,
editing, or applying a saved plan does not create a new AI request.

New plans group adjacent commits by intent. Use --messages-only to keep one
output commit for each selected commit.

Use --edit with a saved plan ID or file to review messages in $EDITOR. Editing a
saved plan ID creates a new revision; editing a standalone plan file updates
that file. The command prints the plan ID or file it saved.

Progress is written to stderr so stdout stays usable for command results and
--json. Use --progress plain for copyable progress, or --quiet to hide it.`,
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
	if jsonOut {
		if err := progress.Emit(rewriteProgressEvent{
			Phase:   "selection",
			Message: fmt.Sprintf("selected %d commit(s)", len(report.Selected)),
			Current: len(report.Selected),
			Total:   len(report.Selected),
		}); err != nil {
			return err
		}
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	providerResolution, err := resolveRewriteProviderConfig(ctx, repo)
	if err != nil {
		return err
	}
	provider, closer, err := ai.BuildProvider(providerResolution.Config)
	if err != nil {
		cause := fmt.Errorf("%w: %v", ai.ErrRewriteRequiresAIProvider, err)
		return rewriteProviderGateError(cause, providerResolution, repo)
	}
	if closer != nil {
		defer func() { _ = closer.Close() }()
	}
	if err := ai.CheckHistoryRewritePlanGenerationGate(providerResolution.Config, provider, opts.messagesOnly); err != nil {
		return rewriteProviderGateError(err, providerResolution, repo)
	}
	if err := progress.Emit(rewriteProgressEvent{
		Phase:   "selection",
		Message: fmt.Sprintf("selected %d commit(s)", len(report.Selected)),
		Current: len(report.Selected),
		Total:   len(report.Selected),
	}); err != nil {
		return err
	}
	fmt.Fprintf(out, "History rewrite plan for %s\n", rewriteSelectionLabel(opts))
	printRewriteSelectionSummary(out, report)
	if err := progress.Emit(rewriteProgressEvent{Phase: "provider", Message: fmt.Sprintf("using %s", displayProvider(ai.PrimaryProviderName(provider)))}); err != nil {
		return err
	}

	plan, err := generateRewritePlan(ctx, repo, selection, provider, providerResolution.Config, progress, opts.messagesOnly)
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
		fmt.Fprintf(out, "Plan stored as invalid (%s): %s\n", plan.ID, plan.ValidationError.String)
	} else {
		fmt.Fprintf(out, "Generated valid rewrite plan %s.\n", plan.ID)
		printRewritePlanSummary(out, plan, selection.Selected)
	}
	if err := progress.Emit(rewriteProgressEvent{Phase: "validation", Message: fmt.Sprintf("status %s", plan.ValidationStatus), PlanID: plan.ID}); err != nil {
		return err
	}
	if opts.planOut != "" {
		fmt.Fprintf(out, "Plan written to %s\n", opts.planOut)
	}
	if plan.ValidationStatus == state.RewritePlanValidationInvalid {
		fmt.Fprintln(out, "No commits were rewritten.")
		planRef := plan.ID
		if opts.planOut != "" {
			planRef = opts.planOut
		}
		fmt.Fprintf(out, "Next: Repair the saved plan with `acd history rewrite --edit %s --plan-only`.\n", rewritePlanRefArg(planRef))
		return errors.New("acd history rewrite: AI proposal validation failed; invalid plan saved and apply is blocked")
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
		return errors.New("acd history rewrite: choose only one of --review or --no-review")
	}
	if !opts.yes && !opts.review && !opts.noReview {
		var err error
		review, err = promptRewriteYesNo(inputOrStdin(opts.in), out, "Review or edit proposed messages before applying?", false)
		if err != nil {
			return err
		}
	}
	if review {
		groups, changed, err := editRewritePlanWithEditor(plan, opts.editFormat)
		if err != nil {
			return err
		}
		updated, err := persistEditedRewritePlan(ctx, repo, plan, groups)
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
		applyNow, err = promptRewriteYesNo(inputOrStdin(opts.in), out, rewriteApplyQuestion(plan), false)
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

func resolveRewriteProviderConfig(ctx context.Context, repo string) (rewriteProviderResolution, error) {
	roots, err := paths.Resolve()
	if err != nil {
		return rewriteProviderResolution{}, fmt.Errorf("acd history rewrite: resolve config paths: %w", err)
	}
	service, err := settings.NewValidationService(ctx, settings.Options{
		Roots:    roots,
		RepoPath: repo,
	})
	if err != nil {
		return rewriteProviderResolution{}, fmt.Errorf("acd history rewrite: resolve settings: %w", err)
	}
	defer func() { _ = service.Close() }()
	preview, err := service.AuthoringPreview()
	if err != nil {
		return rewriteProviderResolution{}, fmt.Errorf("acd history rewrite: resolve settings: %w", err)
	}
	cfg, err := service.AuthoringProviderConfig()
	if err != nil {
		return rewriteProviderResolution{}, fmt.Errorf("acd history rewrite: resolve settings: %w", err)
	}
	globalService, err := settings.NewGlobalService(ctx, settings.Options{Roots: roots})
	if err != nil {
		return rewriteProviderResolution{}, fmt.Errorf("acd history rewrite: resolve global settings: %w", err)
	}
	defer func() { _ = globalService.Close() }()
	globalCfg, err := globalService.AuthoringProviderConfig()
	if err != nil {
		return rewriteProviderResolution{}, fmt.Errorf("acd history rewrite: resolve global settings: %w", err)
	}
	globalPreview, err := globalService.AuthoringPreview()
	if err != nil {
		return rewriteProviderResolution{}, fmt.Errorf("acd history rewrite: resolve global settings: %w", err)
	}
	return rewriteProviderResolution{
		Config: cfg, ProviderSource: preview.Sources[config.FieldProvider],
		StrategySource: preview.Sources[config.FieldCommitStrategy],
		GlobalConfig:   globalCfg, GlobalSource: globalPreview.Sources[config.FieldProvider],
	}, nil
}

func rewriteProviderGateError(cause error, resolved rewriteProviderResolution, repo string) error {
	var guidance strings.Builder
	repositoryOverride := resolved.ProviderSource == config.SourceRepository
	if errors.Is(cause, ai.ErrRewriteRequiresIntentStrategy) {
		repositoryOverride = resolved.StrategySource == config.SourceRepository
		fmt.Fprintf(&guidance, "\n\nCurrent mode: %s (%s)\n",
			displayConfigureWord(string(resolved.Config.CommitStrategy)),
			displayRewriteSettingSource(resolved.StrategySource))
	} else {
		fmt.Fprintf(&guidance, "\n\nCurrent provider: %s (%s)\n",
			displayProvider(resolved.Config.Mode),
			displayRewriteSettingSource(resolved.ProviderSource))
	}
	if errors.Is(cause, ai.ErrRewriteRequiresAIProvider) &&
		strings.TrimSpace(resolved.Config.Mode) == "openai-compat" &&
		strings.TrimSpace(resolved.Config.APIKey) == "" {
		guidance.WriteString("Credential: not configured\n\nConfigure the credential:\n  acd auth set\n")
		guidance.WriteString("\nNo plan was generated. Git history is unchanged.")
		return fmt.Errorf("%w%s", cause, guidance.String())
	}
	if repositoryOverride && rewriteProviderConfigCanPlan(resolved.GlobalConfig) {
		label := "Inherited provider"
		if resolved.GlobalSource == config.SourceGlobal {
			label = "Global default"
		}
		fmt.Fprintf(&guidance, "%s: %s", label, displayProvider(resolved.GlobalConfig.Mode))
		if model := strings.TrimSpace(resolved.GlobalConfig.Model); model != "" {
			fmt.Fprintf(&guidance, " (%s)", model)
		}
		fmt.Fprintf(&guidance, "\n\nTo use every global setting:\n  acd config edit --repo %s --inherit\n", productListShellQuote(repo))
		fmt.Fprintf(&guidance, "\nTo configure only this repository:\n  acd config edit --repo %s\n", productListShellQuote(repo))
	} else {
		fmt.Fprintf(&guidance, "\nConfigure this repository:\n  acd config edit --repo %s\n", productListShellQuote(repo))
		guidance.WriteString("\nConfigure defaults for repositories without overrides:\n  acd config edit\n")
	}
	guidance.WriteString("\nNo plan was generated. Git history is unchanged.")
	return fmt.Errorf("%w%s", cause, guidance.String())
}

func rewriteProviderConfigCanPlan(cfg ai.ProviderConfig) bool {
	mode := strings.TrimSpace(strings.ToLower(cfg.Mode))
	if cfg.CommitStrategy != ai.CommitStrategyIntent || mode == "" || mode == "deterministic" {
		return false
	}
	if mode == "openai-compat" && strings.TrimSpace(cfg.APIKey) == "" {
		return false
	}
	_, err := ai.ValidateProviderConfig(cfg)
	return err == nil
}

func displayRewriteSettingSource(source config.Source) string {
	switch source {
	case config.SourceRepository:
		return "repository override"
	case config.SourceGlobal:
		return "global default"
	default:
		return string(source)
	}
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
	fmt.Fprintf(out, "  acd history rewrite --show-plan %s\n", arg)
	fmt.Fprintf(out, "  acd history rewrite --apply-plan %s --dry-run\n", arg)
	fmt.Fprintf(out, "  acd history rewrite --apply-plan %s --yes\n", arg)
}

func printRewriteSelectionSummary(out io.Writer, report rewriteSelectionReport) {
	fmt.Fprintf(out, "Repository: %s\n", report.Repo)
	fmt.Fprintf(out, "Branch: %s @ %s\n", report.BranchRef, shortenSHA(report.Head))
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

func printRewritePlanSummary(out io.Writer, plan state.RewritePlan, selected []git.RewriteCommitRecord) {
	inputCount := rewritePlanInputCount(plan)
	outputCount := len(plan.Groups)
	fmt.Fprintf(out, "Selected commits: %d\n", inputCount)
	fmt.Fprintf(out, "Resulting commits: %d\n", outputCount)
	fmt.Fprintf(out, "Commit reduction: %d\n", inputCount-outputCount)
	byOID := make(map[string]git.RewriteCommitRecord, len(selected))
	for _, commit := range selected {
		byOID[commit.OID] = commit
	}
	for i, group := range plan.Groups {
		subject := strings.SplitN(strings.TrimSpace(group.ProposedMessage), "\n", 2)[0]
		fmt.Fprintf(out, "Group %d: %s\n", i+1, subject)
		fmt.Fprintf(out, "  Reason: %s\n", group.GroupingReason)
		for _, member := range group.Members {
			originalSubject := strings.SplitN(strings.TrimSpace(member.OriginalMessage), "\n", 2)[0]
			if commit, ok := byOID[member.OldOID]; ok {
				originalSubject = commit.Subject
			}
			fmt.Fprintf(out, "  - %s %s\n", shortenSHA(member.OldOID), originalSubject)
		}
	}
	if inputCount > outputCount {
		fmt.Fprintln(out, "Applying this plan reduces commit count. The final tree remains unchanged.")
	}
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

func rewriteApplyQuestion(plan state.RewritePlan) string {
	inputCount := rewritePlanInputCount(plan)
	outputCount := len(plan.Groups)
	if inputCount == outputCount {
		return fmt.Sprintf("Apply this plan to rewrite %d commit message(s)?", inputCount)
	}
	return fmt.Sprintf("Replace %d selected commits with %d grouped commits? The final tree will stay unchanged.", inputCount, outputCount)
}

func editSavedRewritePlan(ctx context.Context, out io.Writer, repoFlag string, opts rewriteCommitsOptions) error {
	repo, err := resolveRepo(repoFlag)
	if err != nil {
		return err
	}
	plan, err := readRewritePlanRef(ctx, repo, opts.editPlan)
	if err != nil {
		return fmt.Errorf("acd history rewrite: read edit plan: %w", err)
	}
	groups, changed, err := editRewritePlanWithEditor(plan, opts.editFormat)
	if err != nil {
		return err
	}
	applyRef := opts.editPlan
	if changed {
		if isRewritePlanFileRef(opts.editPlan) {
			if err := validateRewritePlanGroupsInRepo(ctx, repo, groups); err != nil {
				return err
			}
			plan.Groups = groups
			plan.Commits = nil
			plan.ValidationStatus = state.RewritePlanValidationValid
			plan.ValidationError = sql.NullString{}
			plan.ApplyStatus = state.RewritePlanApplyPending
			plan.Edited = true
			if err := writeRewritePlanFile(opts.editPlan, plan); err != nil {
				return err
			}
			fmt.Fprintf(out, "Edited rewrite plan file saved: %s.\n", opts.editPlan)
		} else {
			updated, err := persistEditedRewritePlan(ctx, repo, plan, groups)
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
		applyNow, err = promptRewriteYesNo(inputOrStdin(opts.in), out, rewriteApplyQuestion(plan), false)
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
		return errors.New("acd history rewrite: --apply-plan requires --yes or --dry-run")
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
		return fmt.Errorf("acd history rewrite: read apply plan: %w", err)
	}
	if err := validateRewritePlanReadyForApply(plan); err != nil {
		return err
	}
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		return err
	}
	rewriteLock, err := acquireRewriteApplyOwnership(ctx, repo, opts.dryRun)
	if err != nil {
		return err
	}
	if rewriteLock != nil {
		defer func() { _ = rewriteLock.Release() }()
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("acd history rewrite: open state db: %w", err)
	}
	defer db.Close()
	if daemonState, _, err := state.LoadDaemonState(ctx, db); err != nil {
		return fmt.Errorf("acd history rewrite: inspect daemon state: %w", err)
	} else if !opts.dryRun && daemonState.Mode != "" &&
		daemonState.Mode != "stopped" {
		if err := state.SaveDaemonState(ctx, db, state.DaemonState{
			Mode: "stopped",
			Note: sql.NullString{
				String: "history rewrite proved exclusive ownership",
				Valid:  true,
			},
		}); err != nil {
			return fmt.Errorf(
				"acd history rewrite: repair stale daemon state: %w", err)
		}
	}
	if !opts.dryRun {
		pending, err := state.CountAllPendingCaptureEvents(ctx, db)
		if err != nil {
			return fmt.Errorf("acd history rewrite: inspect pending capture queue: %w", err)
		}
		if pending.Count > 0 {
			return fmt.Errorf("acd history rewrite: %d protected change(s) are still waiting to be published; run `acd commit-all`, then try again", pending.Count)
		}
	}

	res, err := git.ApplyRewritePlan(ctx, repo, git.RewriteApplyOptions{
		BranchRef:    plan.BranchRef,
		ExpectedHead: plan.ExpectedHead,
		PlanID:       plan.ID,
		Groups:       rewriteApplyGroups(plan.Groups),
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

	fmt.Fprintf(out, "History rewrite plan: %s\n", opts.applyPlan)
	fmt.Fprintln(out, "The saved messages were reused. No new AI request was made.")
	if opts.dryRun {
		fmt.Fprintf(out, "Changed: no\n")
		fmt.Fprintf(out, "Status: This plan can be applied to %s at %s.\n", plan.BranchRef, shortenSHA(plan.ExpectedHead))
		fmt.Fprintf(out, "Next: Review the plan, then apply it with `acd history rewrite --apply-plan %s --yes`.\n", opts.applyPlan)
		return nil
	}
	fmt.Fprintln(out, "History rewrite complete.")
	fmt.Fprintf(out, "Selected commits: %d -> %d\n", res.SelectedInputCount, res.SelectedOutputCount)
	fmt.Fprintf(out, "Later commits recreated unchanged: %d\n", res.UnchangedDescendantCount)
	fmt.Fprintf(out, "Branch: %s (%s to %s)\n", plan.BranchRef, shortenSHA(res.OldHead), shortenSHA(res.NewHead))
	fmt.Fprintf(out, "Recovery backup: %s\n", res.BackupBranchRef)
	if res.InternalBackupRef != "" {
		fmt.Fprintf(out, "Additional backup: %s\n", res.InternalBackupRef)
	}
	fmt.Fprintf(out, "Undo command: git reset --hard %s\n", res.BackupBranchRef)
	reconcile, err := state.ReconcileRewriteCommitOIDs(ctx, db, res.CommitMap)
	if err != nil {
		return fmt.Errorf("acd history rewrite: reconcile state OIDs after successful git rewrite: %w", err)
	}
	reconciledRows := reconcile.CaptureEvents + reconcile.DecisionRecords + reconcile.PublishTargetCommitOID + reconcile.PublishSourceHead
	if err := progress.Emit(rewriteProgressEvent{Phase: "apply_reconcile", Message: "reconciled state OIDs", Current: int(reconciledRows)}); err != nil {
		return err
	}
	if plan.ID != "" {
		if err := markRewritePlanStatusIfPresent(ctx, db, plan.ID, state.RewritePlanApplyApplied); err != nil {
			return fmt.Errorf("acd history rewrite: mark plan applied: %w", err)
		}
	}
	fmt.Fprintln(out, "Status: Git history and ACD's records now agree.")
	fmt.Fprintln(out, "Next: No action is needed. Keep the recovery backup until you have checked the rewritten history.")
	return nil
}

func acquireRewriteApplyOwnership(
	ctx context.Context,
	repo string,
	dryRun bool,
) (*daemon.DaemonLock, error) {
	lookup, err := loadControlRepo(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("acd history rewrite: inspect ACD lifecycle: %w", err)
	}
	if lookup.Registered && !lookup.Record.LifecycleDisabled() {
		return nil, errors.New(
			"acd history rewrite: ACD must be off before applying this plan " +
				"(current state: enabled); run `acd off`, then try again")
	}
	if dryRun {
		return nil, nil
	}
	lock, err := daemon.AcquireDaemonLock(lookup.Worktree.GitDir)
	if err != nil {
		if errors.Is(err, daemon.ErrDaemonLockHeld) {
			return nil, errors.New(
				"acd history rewrite: ACD must be off before applying this plan " +
					"(a repository worker still owns the writer lock); " +
					"run `acd off`, then try again")
		}
		return nil, fmt.Errorf(
			"acd history rewrite: acquire exclusive repository ownership: %w", err)
	}
	confirmed, confirmErr := loadControlRepo(ctx, repo)
	if confirmErr != nil {
		_ = lock.Release()
		return nil, fmt.Errorf(
			"acd history rewrite: recheck ACD lifecycle: %w", confirmErr)
	}
	if confirmed.Registered && !confirmed.Record.LifecycleDisabled() {
		_ = lock.Release()
		return nil, errors.New(
			"acd history rewrite: ACD became enabled while preparing the rewrite; " +
				"run `acd off`, then try again")
	}
	return lock, nil
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
		return fmt.Errorf("acd history rewrite: read show plan: %w", err)
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

func validateRewritePlanReadyForApply(plan state.RewritePlan) error {
	if plan.ValidationStatus != state.RewritePlanValidationValid {
		if plan.ValidationStatus == state.RewritePlanValidationInvalid && plan.ValidationError.Valid && strings.TrimSpace(plan.ValidationError.String) != "" {
			return fmt.Errorf("acd history rewrite: cannot apply rewrite plan with validation status %q: %s", plan.ValidationStatus, plan.ValidationError.String)
		}
		if strings.TrimSpace(plan.ValidationStatus) == "" {
			return errors.New("acd history rewrite: cannot apply rewrite plan without validation status \"valid\"")
		}
		return fmt.Errorf("acd history rewrite: cannot apply rewrite plan with validation status %q; expected %q", plan.ValidationStatus, state.RewritePlanValidationValid)
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
	groups, err := state.RewritePlanGroups(plan)
	if err != nil {
		return state.RewritePlan{}, err
	}
	plan.Groups = groups
	plan.Commits = nil
	plan.CommitFormat = normalizeRewritePlanCommitFormat(plan.CommitFormat)
	return plan, nil
}

func writeRewritePlanFile(path string, plan state.RewritePlan) error {
	groups, err := state.RewritePlanGroups(plan)
	if err != nil {
		return fmt.Errorf("acd history rewrite: validate plan file: %w", err)
	}
	plan.Groups = groups
	plan.Commits = nil
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("acd history rewrite: create plan file: %w", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(plan)
}

func generateRewritePlan(ctx context.Context, repo string, selection git.RewriteSelection, provider ai.Provider, cfg ai.ProviderConfig, progress rewriteProgressSink, messagesOnly bool) (state.RewritePlan, error) {
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		return state.RewritePlan{}, err
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return state.RewritePlan{}, fmt.Errorf("acd history rewrite: open state db: %w", err)
	}
	defer db.Close()
	plan := newRewritePlan(selection, provider, cfg)
	if messagesOnly {
		generateMessageOnlyRewritePlan(ctx, repo, db, selection, provider, cfg, progress, &plan)
	} else {
		generateGroupedRewritePlan(ctx, repo, db, selection, provider, cfg, progress, &plan)
	}
	id, err := state.SaveRewritePlan(ctx, db, plan)
	if err != nil {
		return state.RewritePlan{}, fmt.Errorf("acd history rewrite: save rewrite plan: %w", err)
	}
	plan.ID = id
	if err := progress.Emit(rewriteProgressEvent{Phase: "save", Message: "saved plan", PlanID: plan.ID}); err != nil {
		return state.RewritePlan{}, err
	}
	return plan, nil
}

func newRewritePlan(selection git.RewriteSelection, provider ai.Provider, cfg ai.ProviderConfig) state.RewritePlan {
	return state.RewritePlan{
		BranchRef:        selection.BranchRef,
		ExpectedHead:     selection.Head,
		Provider:         sql.NullString{String: ai.PrimaryProviderName(provider), Valid: ai.PrimaryProviderName(provider) != ""},
		Model:            sql.NullString{String: cfg.Model, Valid: cfg.Model != ""},
		CommitFormat:     string(cfg.CommitFormat),
		ValidationStatus: state.RewritePlanValidationValid,
		ApplyStatus:      state.RewritePlanApplyPending,
	}
}

func generateMessageOnlyRewritePlan(ctx context.Context, repo string, db *state.DB, selection git.RewriteSelection, provider ai.Provider, cfg ai.ProviderConfig, progress rewriteProgressSink, plan *state.RewritePlan) {
	proposer, ok := provider.(ai.CommitRewriteProposer)
	if !ok {
		invalidateRewritePlan(plan, selection.Selected, errors.New("provider cannot generate rewrite proposals"))
		return
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
			invalidateRewritePlan(plan, selection.Selected, err)
			return
		}
		req, err := buildCommitRewriteRequest(ctx, repo, db, selection.Selected, i, c, ai.ProviderNeedsDiff(provider), cfg.CommitFormat)
		if err != nil {
			invalidateRewritePlan(plan, selection.Selected, err)
			return
		}
		result, err := proposer.ProposeCommitRewrite(ctx, req)
		if err != nil {
			invalidateRewritePlan(plan, selection.Selected, err)
			_ = progress.Emit(rewriteProgressEvent{
				Phase:         "proposal",
				Message:       "proposal failed",
				Current:       i + 1,
				Total:         len(selection.Selected),
				CommitOID:     c.OID,
				CommitSubject: c.Subject,
			})
			return
		}
		message := result.Subject
		if strings.TrimSpace(result.Body) != "" {
			message += "\n\n" + result.Body
		}
		plan.Groups = append(plan.Groups, state.RewritePlanGroup{
			Members:         []state.RewritePlanMember{{OldOID: c.OID, OriginalMessage: c.Message}},
			ProposedMessage: message,
			GroupingReason:  "message-only rewrite",
		})
		if err := progress.Emit(rewriteProgressEvent{
			Phase:         "proposal",
			Message:       "proposal accepted",
			Current:       i + 1,
			Total:         len(selection.Selected),
			CommitOID:     c.OID,
			CommitSubject: c.Subject,
		}); err != nil {
			invalidateRewritePlan(plan, selection.Selected, err)
			return
		}
	}
	if len(plan.Groups) == 0 {
		invalidateRewritePlan(plan, selection.Selected, errors.New("no proposal generated"))
	}
}

func generateGroupedRewritePlan(ctx context.Context, repo string, db *state.DB, selection git.RewriteSelection, provider ai.Provider, cfg ai.ProviderConfig, progress rewriteProgressSink, plan *state.RewritePlan) {
	planner, ok := provider.(ai.HistoryRewritePlanner)
	if !ok {
		invalidateRewritePlan(plan, selection.Selected, errors.New("provider cannot group historical commits"))
		return
	}
	if err := progress.Emit(rewriteProgressEvent{Phase: "grouping", Message: fmt.Sprintf("grouping %d selected commit(s)", len(selection.Selected)), Current: len(selection.Selected), Total: len(selection.Selected)}); err != nil {
		invalidateRewritePlan(plan, selection.Selected, err)
		return
	}
	req, err := buildHistoryRewritePlanRequest(ctx, repo, db, selection.Selected, provider, cfg.CommitFormat)
	if err != nil {
		invalidateRewritePlan(plan, selection.Selected, err)
		return
	}
	proposal, err := planner.ProposeHistoryRewritePlan(ctx, req)
	if err != nil {
		invalidateRewritePlan(plan, selection.Selected, err)
		_ = progress.Emit(rewriteProgressEvent{Phase: "grouping", Message: "grouping failed"})
		return
	}
	byOID := make(map[string]git.RewriteCommitRecord, len(selection.Selected))
	for _, commit := range selection.Selected {
		byOID[commit.OID] = commit
	}
	for _, proposed := range proposal.Groups {
		group := state.RewritePlanGroup{GroupingReason: proposed.GroupingReason}
		group.ProposedMessage = proposed.Subject
		if strings.TrimSpace(proposed.Body) != "" {
			group.ProposedMessage += "\n\n" + proposed.Body
		}
		for _, oid := range proposed.OldOIDs {
			commit := byOID[oid]
			group.Members = append(group.Members, state.RewritePlanMember{OldOID: oid, OriginalMessage: commit.Message})
		}
		plan.Groups = append(plan.Groups, group)
	}
	if err := validateRewritePlanGroupsInRepo(ctx, repo, plan.Groups); err != nil {
		invalidateRewritePlan(plan, selection.Selected, err)
		return
	}
	_ = progress.Emit(rewriteProgressEvent{Phase: "grouping", Message: fmt.Sprintf("%d group(s) ready", len(plan.Groups)), Current: len(plan.Groups), Total: len(plan.Groups)})
}

func buildHistoryRewritePlanRequest(ctx context.Context, repo string, db *state.DB, commits []git.RewriteCommitRecord, provider ai.Provider, format ai.CommitFormat) (ai.HistoryRewritePlanRequest, error) {
	req := ai.HistoryRewritePlanRequest{CommitFormat: format}
	diffCap := 0
	if len(commits) > 0 {
		diffCap = ai.HistoryRewriteTotalDiffCap / len(commits)
		if diffCap > ai.HistoryRewritePerCommitDiffCap {
			diffCap = ai.HistoryRewritePerCommitDiffCap
		}
	}
	for i, commit := range commits {
		paths, err := gitOutputLines(ctx, repo, "diff-tree", "--root", "--no-commit-id", "--name-only", "-r", commit.OID)
		if err != nil {
			return ai.HistoryRewritePlanRequest{}, err
		}
		if len(paths) > 100 {
			paths = paths[:100]
		}
		stat, err := gitOutputString(ctx, repo, "show", "--stat", "--format=", commit.OID)
		if err != nil {
			return ai.HistoryRewritePlanRequest{}, err
		}
		author, err := gitOutputString(ctx, repo, "show", "-s", "--format=%an%x00%ae", commit.OID)
		if err != nil {
			return ai.HistoryRewritePlanRequest{}, err
		}
		parts := strings.Split(strings.TrimRight(author, "\n"), "\x00")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return ai.HistoryRewritePlanRequest{}, fmt.Errorf("acd history rewrite: malformed author metadata for %s", shortenSHA(commit.OID))
		}
		evidence := ai.HistoryRewriteCommit{
			OldOID:          commit.OID,
			Position:        i + 1,
			OriginalMessage: truncateUTF8(commit.Message, 2*1024),
			AuthorName:      parts[0],
			AuthorEmail:     parts[1],
			ChangedPaths:    paths,
			DiffStat:        truncateUTF8(strings.TrimSpace(stat), 4*1024),
		}
		decisions, decisionErr := state.DecisionsForCommit(ctx, db, commit.OID, 20)
		if decisionErr == nil {
			for _, decision := range decisions {
				evidence.DecisionContext = append(evidence.DecisionContext, truncateRewriteDecisionContext(decisionContextFromRecord(decision), 512))
			}
		}
		if ai.ProviderNeedsDiff(provider) && rewriteDiffEgressOptIn() && diffCap > 0 {
			diff, err := gitOutputString(ctx, repo, "show", "--format=", "--no-ext-diff", "--unified=80", commit.OID)
			if err != nil {
				return ai.HistoryRewritePlanRequest{}, err
			}
			evidence.RedactedDiff = ai.Truncate(ai.RedactDiffSecrets(diff), diffCap)
			evidence.DiffIncluded = evidence.RedactedDiff != ""
		}
		req.Commits = append(req.Commits, evidence)
	}
	if _, err := ai.BuildHistoryRewriteUserPrompt(req); err != nil {
		return ai.HistoryRewritePlanRequest{}, err
	}
	return req, nil
}

func truncateRewriteDecisionContext(in ai.RewriteDecisionContext, limit int) ai.RewriteDecisionContext {
	in.Kind = truncateUTF8(in.Kind, limit)
	in.Path = truncateUTF8(in.Path, limit)
	in.Reason = truncateUTF8(in.Reason, limit)
	in.ActionTaken = truncateUTF8(in.ActionTaken, limit)
	in.UserMessage = truncateUTF8(in.UserMessage, limit)
	return in
}

func truncateUTF8(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	end := 0
	for index := range value {
		if index > limit {
			break
		}
		end = index
	}
	if end == 0 {
		return ""
	}
	return value[:end]
}

func invalidateRewritePlan(plan *state.RewritePlan, commits []git.RewriteCommitRecord, cause error) {
	plan.ValidationStatus = state.RewritePlanValidationInvalid
	plan.ValidationError = sql.NullString{String: rewriteFailureJSON("selection", cause), Valid: true}
	plan.Groups = singletonRewritePlanGroups(commits)
}

func singletonRewritePlanGroups(commits []git.RewriteCommitRecord) []state.RewritePlanGroup {
	groups := make([]state.RewritePlanGroup, 0, len(commits))
	for _, commit := range commits {
		groups = append(groups, state.RewritePlanGroup{
			Members:         []state.RewritePlanMember{{OldOID: commit.OID, OriginalMessage: commit.Message}},
			ProposedMessage: commit.Message,
			GroupingReason:  "original commit retained after planning failure",
		})
	}
	return groups
}

func validateRewritePlanGroupsInRepo(ctx context.Context, repo string, groups []state.RewritePlanGroup) error {
	return git.ValidateRewriteGroupSemantics(ctx, repo, rewriteApplyGroups(groups))
}

func rewriteApplyGroups(groups []state.RewritePlanGroup) []git.RewriteApplyGroup {
	applyGroups := make([]git.RewriteApplyGroup, 0, len(groups))
	for _, group := range groups {
		oldOIDs := make([]string, 0, len(group.Members))
		for _, member := range group.Members {
			oldOIDs = append(oldOIDs, member.OldOID)
		}
		applyGroups = append(applyGroups, git.RewriteApplyGroup{OldOIDs: oldOIDs, ProposedMessage: group.ProposedMessage})
	}
	return applyGroups
}

func rewriteStateDBPath(ctx context.Context, repo string) (string, error) {
	gitDir, err := git.AbsoluteGitDir(ctx, repo)
	if err != nil {
		return "", fmt.Errorf("acd history rewrite: resolve git dir: %w", err)
	}
	return state.DBPathFromGitDir(gitDir), nil
}

func buildCommitRewriteRequest(ctx context.Context, repo string, db *state.DB, all []git.RewriteCommitRecord, idx int, c git.RewriteCommitRecord, providerNeedsDiff bool, commitFormat ai.CommitFormat) (ai.CommitRewriteRequest, error) {
	paths, err := gitOutputLines(ctx, repo, "diff-tree", "--no-commit-id", "--name-only", "-r", c.OID)
	if err != nil {
		return ai.CommitRewriteRequest{}, err
	}
	stat, err := gitOutputString(ctx, repo, "show", "--stat", "--format=", c.OID)
	if err != nil {
		return ai.CommitRewriteRequest{}, err
	}
	req := ai.CommitRewriteRequest{OldOID: c.OID, OriginalMessage: c.Message, ChangedPaths: paths, DiffStat: strings.TrimSpace(stat), DiffIncluded: false, CommitFormat: commitFormat}
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

func normalizeRewritePlanCommitFormat(format string) string {
	switch ai.CommitFormat(strings.ToLower(strings.TrimSpace(format))) {
	case ai.CommitFormatConventional:
		return string(ai.CommitFormatConventional)
	default:
		return string(ai.CommitFormatImperative)
	}
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
		return "", fmt.Errorf("acd history rewrite: git %s: %w", strings.Join(args, " "), err)
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
