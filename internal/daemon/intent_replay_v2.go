package daemon

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// replayIntentCandidateBatch connects the durable v2 candidate engine to the
// existing proven Git publication path. Decisions are consumed in the
// planner-validated topological order; a failed/non-publishable prerequisite
// stops its dependants from reaching publishIntentSelection.
func replayIntentCandidateBatch(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	activeCtx CaptureContext,
	opts ReplayOpts,
	cfg intentReplayConfig,
	indexFile string,
	items []intentReplayItem,
	legacyRequest ai.IntentPlanRequest,
	parent, parentTree string,
	forced bool,
	sum ReplaySummary,
) (ReplaySummary, error) {
	diffBySeq := make(map[int64]string, len(legacyRequest.OfferedCaptures))
	for _, offered := range legacyRequest.OfferedCaptures {
		diffBySeq[offered.Seq] = offered.CapturedDiff
	}
	captures := make([]IntentCandidateCapture, 0, len(items))
	for _, item := range items {
		var covered []state.CaptureEvent
		if item.coalesce != nil {
			covered = append(covered, item.coalesce.Covered...)
		}
		captures = append(captures, IntentCandidateCapture{
			Event: item.event, Ops: item.ops, CapturedDiff: diffBySeq[item.event.Seq],
			CoveredEvents: covered,
		})
	}
	retryLimit := resolvedIntentRetryLimit()
	if cfg.retryLimit != nil {
		retryLimit = *cfg.retryLimit
	}
	telemetry := runtimeTelemetryFromContext(ctx)
	presetID := telemetry.presetID
	if presetID == "" {
		presetID = "intent." + string(opts.IntentPreset)
	}
	presetVersion := telemetry.presetVersion
	if presetVersion <= 0 {
		presetVersion = config.PresetCatalogVersion
	}
	evaluationStartedTS := float64(time.Now().UnixNano()) / 1e9
	plannerCtx := ctx
	if opts.PromptTrace != nil {
		plannerCtx = prompttrace.With(ctx, opts.PromptTrace, prompttrace.Metadata{
			Strategy: "intent_v2", Protocol: ai.IntentPlannerProtocolV2,
			Provider: cfg.plannerProvider, Model: cfg.plannerModel,
			OfferedSeqs: intentOfferedSeqs(legacyRequest),
			BranchRef:   activeCtx.BranchRef, Generation: activeCtx.BranchGeneration,
			DiffIncluded:     intentRequestIncludesDiff(legacyRequest),
			DiffCap:          ai.IntentStageDiffCap,
			ConfigRevisionID: telemetry.revisionID,
			ConfigProfile:    telemetry.profile,
		})
	}
	plannerCtx, attemptCounter := ai.WithIntentAttemptCounter(plannerCtx)
	if !forced {
		traceIntentPlannerInput(opts.Trace, repoRoot, activeCtx, items,
			legacyRequest, cfg)
	}
	evaluation, err := EvaluateIntentCandidates(plannerCtx, db, IntentCandidateEvaluation{
		BranchRef: activeCtx.BranchRef, BranchGeneration: activeCtx.BranchGeneration,
		Captures: captures, Planner: cfg.planner, Health: opts.IntentHealth,
		RetryLimit: retryLimit, RetryLimitSet: true,
		Preset:       opts.IntentPreset,
		CommitFormat: cfg.commitFormat, IncludeDiffs: cfg.includeDiffs,
		ForcedAging: forced, Provider: cfg.plannerProvider, Model: cfg.plannerModel,
		ConfigRevisionID: sql.NullInt64{
			Int64: telemetry.revisionID, Valid: telemetry.revisionID > 0,
		},
		ConfigProfile: telemetry.profile,
		PresetID:      presetID,
		PresetVersion: presetVersion,
		LatestCommit:  legacyRequest.LatestCommit,
		PathContext:   legacyRequest.PathCommitContext,
		Hints:         runtimeIntentDependencyHints(captures),
		Materialize: intentCandidateScratchMaterializer(
			repoRoot, opts.GitDir, parent),
		VerificationMode: opts.IntentVerificationMode,
		Verify:           opts.IntentCandidateVerify,
		Now:              time.Now().UTC(),
	})
	if err != nil {
		return sum, err
	}
	if counted := attemptCounter.RetryCount(); counted > evaluation.RetryCount {
		evaluation.RetryCount = counted
	}
	windowCfg := cfg
	windowCfg.retryCount = evaluation.RetryCount
	windowCfg.fallbackUsed = evaluation.Fallback != ""
	windowPlan := intentCandidatePlannerWindowPlan(
		legacyRequest, evaluation)
	if evaluation.PlannerFailure != "" {
		traceIntentPlannerValidationFailure(opts.Trace, repoRoot, activeCtx,
			items, evaluation.PlannerFailure)
	}
	traceIntentPlannerOutput(opts.Trace, repoRoot, activeCtx, items, windowPlan)
	if err := recordIntentPlannerWindow(ctx, db, windowCfg, legacyRequest,
		windowPlan, items, activeCtx, evaluationStartedTS, forced,
		evaluation.PlannerFailure); err != nil {
		return sum, err
	}
	if err := recordIntentDeferrals(
		ctx, db, windowPlan, items, activeCtx, evaluationStartedTS,
	); err != nil {
		return sum, err
	}
	if evaluation.PlannerFailure != "" {
		nowSec := float64(time.Now().UnixNano()) / 1e9
		for _, item := range items {
			if err := state.RecordPlannerError(ctx, db, item.event.Seq, nowSec,
				evaluation.PlannerFailure); err != nil {
				return sum, err
			}
			if err := appendIntentPlannerDecision(
				ctx, db, item.event, activeCtx, nowSec,
				state.DecisionKindIntentPlannerError,
				evaluation.PlannerFailure,
				"Intent v2 planner failed",
				"Intent v2 applied the configured preset fallback policy.",
			); err != nil {
				return sum, err
			}
		}
	}

	itemBySeq := make(map[int64]intentReplayItem, len(items))
	for _, item := range items {
		itemBySeq[item.event.Seq] = item
	}
	publishedCandidates := make(map[string]struct{})
	currentParent, currentTree := parent, parentTree
	publishedAny := false
	for _, decision := range evaluation.Decisions {
		if !decision.Publishable {
			continue
		}
		for _, prerequisite := range decision.Assignment.DependsOnCandidates {
			if _, ok := publishedCandidates[prerequisite]; !ok {
				sum.Skipped = true
				sum.SkippedReason = "intent_v2_prerequisite_pending"
				return sum, nil
			}
		}
		selected := make([]intentReplayItem, 0, len(decision.Candidate.Events))
		hasPublished := false
		for _, member := range decision.Candidate.Events {
			if member.EventRole == "coalesced" {
				continue
			}
			item, ok := itemBySeq[member.EventSeq]
			if !ok {
				event, loadErr := loadIntentCaptureEvent(ctx, db, member.EventSeq)
				if loadErr != nil {
					return sum, loadErr
				}
				ops, loadErr := state.LoadCaptureOps(ctx, db, member.EventSeq)
				if loadErr != nil {
					return sum, loadErr
				}
				item = intentReplayItem{event: event, ops: ops}
				itemBySeq[member.EventSeq] = item
			}
			if item.event.State != state.EventStatePending {
				hasPublished = true
			}
			selected = append(selected, item)
		}
		sort.Slice(selected, func(i, j int) bool {
			return selected[i].event.Seq < selected[j].event.Seq
		})
		plan := ai.IntentPlan{
			SelectedSeqs: append([]int64(nil), decision.Assignment.SelectedSeqs...),
			Subject:      decision.Assignment.Subject, Body: decision.Assignment.Body,
			GroupingReason: decision.Assignment.GroupingReason,
			Source:         evaluation.ProtocolVersion,
		}
		if hasPublished {
			if !opts.IntentRepairEnabled {
				sum.Skipped = true
				sum.SkippedReason = "intent_v2_repair_required"
				return sum, nil
			}
			repaired, publishedCount, repairErr := repairIntentCandidateDecision(
				ctx, repoRoot, opts.GitDir, db, activeCtx, opts, decision,
				selected, currentParent)
			if repairErr != nil {
				return sum, repairErr
			}
			if repaired.Status != state.IntentRepairCompleted {
				sum.Skipped = true
				sum.SkippedReason = "intent_v2_repair_" + repaired.Status
				return sum, nil
			}
			sum.Published += publishedCount
			sum.BaseHead = repaired.NewHead
			publishedCandidates[decision.Candidate.ID] = struct{}{}
			publishedAny = true
			currentParent = repaired.NewHead
			currentTree, err = resolveTreeOID(ctx, repoRoot, currentParent)
			if err != nil {
				return sum, err
			}
			continue
		}
		before := sum
		sum, err = publishIntentSelection(ctx, repoRoot, db, activeCtx, opts,
			indexFile, selected, plan, currentParent, currentTree, sum)
		if err != nil {
			return sum, err
		}
		if sum.Conflicts > before.Conflicts || sum.Failed > before.Failed ||
			sum.Published == before.Published {
			return sum, nil
		}
		if err := markIntentCandidatePublished(ctx, db, decision.Candidate.ID,
			sum.BaseHead, opts.IntentRepairEnabled, opts.IntentRepairHorizon); err != nil {
			return sum, err
		}
		publishedCandidates[decision.Candidate.ID] = struct{}{}
		publishedAny = true
		currentParent = sum.BaseHead
		currentTree, err = resolveTreeOID(ctx, repoRoot, currentParent)
		if err != nil {
			return sum, err
		}
	}
	if !publishedAny {
		sum.Skipped = true
		if evaluation.NeedsAttention {
			sum.SkippedReason = "intent_v2_needs_attention"
		} else {
			sum.SkippedReason = "intent_v2_candidate_wait"
		}
	}
	return sum, nil
}

func intentCandidatePlannerWindowPlan(
	request ai.IntentPlanRequest,
	evaluation IntentCandidateEvaluationResult,
) ai.IntentPlan {
	offered := make(map[int64]struct{}, len(request.OfferedCaptures))
	for _, capture := range request.OfferedCaptures {
		offered[capture.Seq] = struct{}{}
	}
	plan := ai.IntentPlan{Source: evaluation.ProtocolVersion}
	covered := make(map[int64]struct{}, len(offered))
	for _, decision := range evaluation.Decisions {
		seqs := make([]int64, 0, len(decision.Assignment.SelectedSeqs))
		for _, seq := range decision.Assignment.SelectedSeqs {
			if _, ok := offered[seq]; !ok {
				continue
			}
			seqs = append(seqs, seq)
			covered[seq] = struct{}{}
		}
		if len(seqs) == 0 {
			continue
		}
		if decision.Publishable {
			plan.SelectedSeqs = append(plan.SelectedSeqs, seqs...)
			plan.CommitGroups = append(plan.CommitGroups, ai.IntentCommitGroup{
				SelectedSeqs:   append([]int64(nil), seqs...),
				Subject:        decision.Assignment.Subject,
				Body:           decision.Assignment.Body,
				GroupingReason: decision.Assignment.GroupingReason,
			})
			continue
		}
		reason := intentCandidateDeferredReason(decision, evaluation)
		for _, seq := range seqs {
			plan.DeferredSeqs = append(plan.DeferredSeqs, seq)
			plan.DeferredReasons = append(plan.DeferredReasons, ai.DeferredReason{
				Seq: seq, Reason: reason,
			})
		}
	}
	for _, capture := range request.OfferedCaptures {
		if _, ok := covered[capture.Seq]; ok {
			continue
		}
		plan.DeferredSeqs = append(plan.DeferredSeqs, capture.Seq)
		plan.DeferredReasons = append(plan.DeferredReasons, ai.DeferredReason{
			Seq:    capture.Seq,
			Reason: "candidate evaluation retained this capture",
		})
	}
	sort.Slice(plan.SelectedSeqs, func(i, j int) bool {
		return plan.SelectedSeqs[i] < plan.SelectedSeqs[j]
	})
	sort.Slice(plan.DeferredSeqs, func(i, j int) bool {
		return plan.DeferredSeqs[i] < plan.DeferredSeqs[j]
	})
	sort.Slice(plan.DeferredReasons, func(i, j int) bool {
		return plan.DeferredReasons[i].Seq < plan.DeferredReasons[j].Seq
	})
	return plan
}

func intentCandidateDeferredReason(
	decision IntentCandidateDecision,
	evaluation IntentCandidateEvaluationResult,
) string {
	if len(decision.Assignment.MissingCompanions) > 0 {
		return strings.Join(decision.Assignment.MissingCompanions, "; ")
	}
	if evaluation.PlannerFailure != "" {
		return "planner failure retained this candidate"
	}
	if evaluation.NeedsAttention {
		return "candidate requires attention before publication"
	}
	return "candidate is waiting for an atomic publish boundary"
}

func runtimeIntentDependencyHints(
	captures []IntentCandidateCapture,
) []IntentDependencyHint {
	type features struct {
		seq       int64
		stem      string
		module    string
		diff      string
		symbols   map[string]struct{}
		changeIDs map[string]struct{}
		generated bool
	}
	ordered := append([]IntentCandidateCapture(nil), captures...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Event.Seq < ordered[j].Event.Seq
	})
	items := make([]features, 0, len(ordered))
	for _, capture := range ordered {
		diff := strings.ToLower(capture.CapturedDiff)
		item := features{
			seq: capture.Event.Seq, stem: intentSemanticStem(capture),
			module: intentCaptureModule(capture), diff: diff,
			symbols:   runtimeIntentSymbols(diff),
			changeIDs: runtimeIntentChangeIDs(diff),
			generated: strings.Contains(diff, "code generated") ||
				strings.Contains(diff, "generated from") ||
				runtimeIntentGeneratedPath(capture.Event.Path),
		}
		items = append(items, item)
	}
	var hints []IntentDependencyHint
	seen := map[string]struct{}{}
	add := func(from, to int64, strength ai.IntentDependencyStrength, kind, evidence string) {
		if from <= 0 || to <= 0 || from == to || evidence == "" ||
			len(hints) >= state.IntentDependencyMaxPerPair {
			return
		}
		key := fmt.Sprintf("%d:%d:%s:%s", from, to, strength, kind)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		hints = append(hints, IntentDependencyHint{
			PrerequisiteSeq: from, DependentSeq: to,
			Strength: strength, Kind: kind, Evidence: evidence,
		})
	}
	for i := range items {
		for j := i + 1; j < len(items); j++ {
			earlier, later := items[i], items[j]
			if shared := firstRuntimeIntentFeature(
				earlier.symbols, later.symbols); shared != "" {
				add(earlier.seq, later.seq, ai.IntentDependencySoft,
					"symbol_hash", shared)
			}
			if shared := firstRuntimeIntentFeature(
				earlier.changeIDs, later.changeIDs); shared != "" {
				add(earlier.seq, later.seq, ai.IntentDependencySoft,
					"hunk_hash", shared)
			}
			earlierBase := runtimeIntentStemBase(earlier.stem)
			laterBase := runtimeIntentStemBase(later.stem)
			switch {
			case earlierBase != "" && strings.Contains(later.diff, earlierBase):
				add(earlier.seq, later.seq, ai.IntentDependencySoft,
					"import_reference", earlierBase)
			case laterBase != "" && strings.Contains(earlier.diff, laterBase):
				add(earlier.seq, later.seq, ai.IntentDependencySoft,
					"import_reference", laterBase)
			}
			if later.generated && earlier.module == later.module &&
				(earlierBase != "" && strings.Contains(later.diff, earlierBase)) {
				add(earlier.seq, later.seq, ai.IntentDependencyHard,
					"generated_source", earlierBase)
			}
		}
	}
	return hints
}

func runtimeIntentSymbols(diff string) map[string]struct{} {
	const maxSymbols = 128
	common := map[string]struct{}{
		"about": {}, "after": {}, "before": {}, "branch": {}, "candidate": {},
		"commit": {}, "config": {}, "context": {}, "error": {}, "false": {},
		"function": {}, "import": {}, "intent": {}, "package": {}, "return": {},
		"string": {}, "struct": {}, "testing": {}, "true": {}, "value": {},
	}
	out := make(map[string]struct{})
	for _, line := range strings.Split(diff, "\n") {
		if len(line) == 0 || (line[0] != '+' && line[0] != '-') ||
			strings.HasPrefix(line, "+++") || strings.HasPrefix(line, "---") {
			continue
		}
		for _, token := range strings.FieldsFunc(line[1:], func(r rune) bool {
			return r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r)
		}) {
			token = strings.ToLower(token)
			// Short words such as "split", "content", or a directory name
			// are common across unrelated captures and are not reliable
			// symbol evidence. Keep this signal for identifier-like tokens.
			if len(token) < 8 {
				continue
			}
			if _, skip := common[token]; skip {
				continue
			}
			out[token] = struct{}{}
			if len(out) >= maxSymbols {
				return out
			}
		}
	}
	return out
}

func runtimeIntentChangeIDs(diff string) map[string]struct{} {
	const maxChanges = 64
	out := make(map[string]struct{})
	for _, line := range strings.Split(diff, "\n") {
		if len(line) < 8 || (line[0] != '+' && line[0] != '-') ||
			strings.HasPrefix(line, "+++") || strings.HasPrefix(line, "---") {
			continue
		}
		normalized := strings.Join(strings.Fields(line[1:]), " ")
		if len(normalized) < 7 {
			continue
		}
		out[intentEvidenceHash(normalized)] = struct{}{}
		if len(out) >= maxChanges {
			break
		}
	}
	return out
}

func firstRuntimeIntentFeature(
	left, right map[string]struct{},
) string {
	for feature := range left {
		if _, ok := right[feature]; ok {
			return feature
		}
	}
	return ""
}

func runtimeIntentStemBase(stem string) string {
	base := strings.ToLower(path.Base(stem))
	if len(base) < 4 {
		return ""
	}
	return base
}

func runtimeIntentGeneratedPath(value string) bool {
	base := strings.ToLower(path.Base(value))
	return strings.Contains(base, ".generated.") ||
		strings.Contains(base, "_generated.") ||
		strings.Contains(base, ".gen.") ||
		strings.Contains(base, "_gen.")
}

func intentCandidateScratchMaterializer(
	repoRoot, gitDir, parent string,
) IntentCandidateMaterializer {
	currentSeed := parent
	return func(ctx context.Context, captures []IntentCandidateCapture) error {
		tree, err := materializeIntentCandidateTree(
			ctx, repoRoot, gitDir, currentSeed, captures)
		if err == nil {
			currentSeed = tree
		}
		return err
	}
}

func materializeIntentCandidateTree(
	ctx context.Context,
	repoRoot, gitDir, parent string,
	captures []IntentCandidateCapture,
) (string, error) {
	return materializeIntentCandidateTreeFromSeed(
		ctx, repoRoot, gitDir, parent, captures, true)
}

func materializeIntentCandidateTreeFromSeed(
	ctx context.Context,
	repoRoot, gitDir, parent string,
	captures []IntentCandidateCapture,
	usePublishedBase bool,
) (string, error) {
	if gitDir == "" || parent == "" {
		return "", errors.New("daemon: intent v2 materialize: git dir and parent are required")
	}
	sort.Slice(captures, func(i, j int) bool {
		return captures[i].Event.Seq < captures[j].Event.Seq
	})
	seed := parent
	if usePublishedBase {
		for _, capture := range captures {
			if capture.Event.State != state.EventStatePublished ||
				!capture.Event.CommitOID.Valid {
				continue
			}
			out, err := git.Run(ctx, git.RunOpts{Dir: repoRoot, Timeout: git.DefaultReadTimeout},
				"rev-parse", capture.Event.CommitOID.String+"^")
			if err != nil {
				return "", fmt.Errorf("daemon: intent v2 materialize: resolve soft-published base: %w", err)
			}
			seed = string(bytes.TrimSpace(out))
			break
		}
	}
	root := filepath.Join(gitDir, "acd")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", err
	}
	dir, err := os.MkdirTemp(root, "intent-materialize-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	if err := os.Chmod(dir, 0o700); err != nil {
		return "", err
	}
	index := filepath.Join(dir, "idx")
	if err := git.ReadTree(ctx, repoRoot, index, seed); err != nil {
		return "", err
	}
	tree := ""
	for _, capture := range captures {
		if len(capture.Ops) == 0 {
			return "", fmt.Errorf("daemon: intent v2 materialize: capture %d has no ops",
				capture.Event.Seq)
		}
		if reason, err := detectConflict(ctx, repoRoot, index, capture.Ops); err != nil {
			return "", err
		} else if reason != "" {
			return "", errors.New(reason)
		}
		tree, err = applyOpsAndWriteTree(ctx, repoRoot, index, capture.Ops)
		if err != nil {
			return "", err
		}
	}
	return tree, nil
}

func repairIntentCandidateDecision(
	ctx context.Context,
	repoRoot, gitDir string,
	db *state.DB,
	activeCtx CaptureContext,
	opts ReplayOpts,
	decision IntentCandidateDecision,
	selected []intentReplayItem,
	currentParent string,
) (IntentRepairResult, int, error) {
	if !decision.Candidate.PublishedCommitOID.Valid {
		return IntentRepairResult{}, 0,
			errors.New("daemon: intent v2 repair: candidate has no published commit")
	}
	if !decision.Candidate.SoftPublicationDeadline.Valid ||
		decision.Candidate.SoftPublicationDeadline.Float64 <=
			float64(time.Now().UnixNano())/1e9 {
		return IntentRepairResult{
			Status: state.IntentRepairSkipped,
			Reason: "repair_horizon_expired",
		}, 0, nil
	}
	softCommit := decision.Candidate.PublishedCommitOID.String
	repairLimit := opts.IntentRepairMaxCommits
	if repairLimit <= 0 || repairLimit > git.MaxIntentRepairCommits {
		repairLimit = git.MaxIntentRepairCommits
	}
	out, err := git.Run(ctx, git.RunOpts{
		Dir: repoRoot, Timeout: git.DefaultReadTimeout,
	}, "rev-list", "--first-parent",
		fmt.Sprintf("--max-count=%d", repairLimit), currentParent)
	if err != nil {
		return IntentRepairResult{}, 0, err
	}
	newestFirst := strings.Fields(string(out))
	softIndex := -1
	for i, oid := range newestFirst {
		if oid == softCommit {
			softIndex = i
			break
		}
	}
	if softIndex < 0 {
		return IntentRepairResult{
			Status: state.IntentRepairSkipped,
			Reason: "repair_commit_outside_suffix",
		}, 0, nil
	}
	suffix := append([]string(nil), newestFirst[:softIndex+1]...)
	for left, right := 0, len(suffix)-1; left < right; left, right = left+1, right-1 {
		suffix[left], suffix[right] = suffix[right], suffix[left]
	}
	captures := make([]IntentCandidateCapture, 0, len(selected))
	paths := make(map[string]struct{})
	pendingCount := 0
	for _, item := range selected {
		captures = append(captures, IntentCandidateCapture{
			Event: item.event, Ops: item.ops,
		})
		if item.event.State == state.EventStatePending {
			pendingCount += 1 + coverLen(item.coalesce)
		}
		for _, path := range touchedPaths(item.ops) {
			paths[path] = struct{}{}
		}
	}
	tree, err := materializeIntentCandidateTree(ctx, repoRoot, gitDir,
		softCommit, captures)
	if err != nil {
		return IntentRepairResult{}, 0, err
	}
	message := decision.Assignment.Subject
	if decision.Assignment.Body != "" {
		message += "\n\n" + decision.Assignment.Body
	}
	plans := []IntentRepairCandidatePlan{{
		CandidateID: decision.Candidate.ID,
		Replaces:    []string{softCommit},
		TreeOID:     tree, Message: message, AuthorOID: softCommit,
	}}
	seedTree := tree
	for _, oldOID := range suffix[1:] {
		candidate, ok, loadErr := state.IntentCandidateByPublishedCommit(
			ctx, db, activeCtx.BranchRef, activeCtx.BranchGeneration, oldOID)
		if loadErr != nil {
			return IntentRepairResult{}, 0, loadErr
		}
		if !ok {
			return IntentRepairResult{
				Status: state.IntentRepairSkipped,
				Reason: "repair_suffix_not_acd_owned",
			}, 0, nil
		}
		laterCaptures := make([]IntentCandidateCapture, 0, len(candidate.Events))
		for _, member := range candidate.Events {
			event, loadErr := loadIntentCaptureEvent(ctx, db, member.EventSeq)
			if loadErr != nil {
				return IntentRepairResult{}, 0, loadErr
			}
			ops, loadErr := state.LoadCaptureOps(ctx, db, member.EventSeq)
			if loadErr != nil {
				return IntentRepairResult{}, 0, loadErr
			}
			laterCaptures = append(laterCaptures, IntentCandidateCapture{
				Event: event, Ops: ops,
			})
			for _, touched := range touchedPaths(ops) {
				paths[touched] = struct{}{}
			}
		}
		seedTree, err = materializeIntentCandidateTreeFromSeed(
			ctx, repoRoot, gitDir, seedTree, laterCaptures, false)
		if err != nil {
			return IntentRepairResult{}, 0, err
		}
		oldMessage, loadErr := git.Run(ctx, git.RunOpts{
			Dir: repoRoot, Timeout: git.DefaultReadTimeout,
		}, "show", "-s", "--format=%B", oldOID)
		if loadErr != nil {
			return IntentRepairResult{}, 0, loadErr
		}
		plans = append(plans, IntentRepairCandidatePlan{
			CandidateID: candidate.ID,
			Replaces:    []string{oldOID},
			TreeOID:     seedTree,
			Message:     strings.TrimRight(string(oldMessage), "\n"),
			AuthorOID:   oldOID,
		})
	}
	pathList := make([]string, 0, len(paths))
	for path := range paths {
		pathList = append(pathList, path)
	}
	sort.Strings(pathList)
	result, err := ApplyIntentRepairTransaction(ctx, repoRoot, gitDir, db,
		activeCtx, IntentRepairPlan{
			BranchRef: activeCtx.BranchRef, BranchGeneration: activeCtx.BranchGeneration,
			ExpectedHead: currentParent, Paths: pathList,
			MaxCommits: repairLimit,
			Candidates: plans,
		})
	if err == nil && result.Status == state.IntentRepairCompleted {
		repairedCtx := activeCtx
		repairedCtx.BaseHead = result.NewHead
		for _, item := range selected {
			reconcileLiveIndexAfterPublish(
				ctx, repoRoot, opts.Trace, repairedCtx, item.event, item.ops)
		}
	}
	return result, pendingCount, err
}

func markIntentCandidatePublished(
	ctx context.Context,
	db *state.DB,
	candidateID, commitOID string,
	repairEnabled bool,
	horizon time.Duration,
) error {
	status := state.IntentCandidatePublished
	var deadline sql.NullFloat64
	if repairEnabled {
		status = state.IntentCandidateSoftPublished
		if horizon <= 0 {
			horizon = 10 * time.Minute
		}
		deadline = sql.NullFloat64{
			Float64: float64(time.Now().Add(horizon).UnixNano()) / 1e9,
			Valid:   true,
		}
	}
	res, err := db.SQL().ExecContext(ctx, `
UPDATE intent_candidates
SET status=?, published_commit_oid=?, soft_publication_deadline=?,
    updated_ts=?
WHERE id=? AND status='ready'`,
		status, commitOID, deadline,
		float64(time.Now().UnixNano())/1e9, candidateID)
	if err != nil {
		return fmt.Errorf("daemon: mark intent candidate published: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("daemon: mark intent candidate published: candidate %s changed",
			candidateID)
	}
	return nil
}
