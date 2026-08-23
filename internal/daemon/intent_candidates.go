package daemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// IntentCandidateCapture is the immutable capture material made visible to one
// candidate evaluation. CapturedDiff is transient planner context and is never
// copied into candidate or dependency state.
type IntentCandidateCapture struct {
	Event        state.CaptureEvent
	Ops          []state.CaptureOp
	CapturedDiff string
	// CoveredEvents are original captures represented by Event after the
	// opt-in same-path coalescing pass. They remain durable candidate
	// members, while planning and materialization use the merged Event/Ops
	// semantic unit.
	CoveredEvents []state.CaptureEvent
}

// IntentDependencyHint lets upstream analyzers contribute bounded symbol,
// import, reference, hunk, or generated-source evidence. Evidence is hashed
// before it crosses the engine boundary.
type IntentDependencyHint struct {
	PrerequisiteSeq int64
	DependentSeq    int64
	Strength        ai.IntentDependencyStrength
	Kind            string
	Evidence        string
}

// IntentCandidateMaterializer proves that one exact candidate selection can be
// applied independently. P7 supplies the scratch-index/worktree implementation;
// a missing implementation keeps the gate pending.
type IntentCandidateMaterializer func(context.Context, []IntentCandidateCapture) error

// IntentCandidateVerifier runs the already-approved preset command against the
// exact materialized candidate tree. P7 supplies the bounded runner.
type IntentCandidateVerification struct {
	Status    string
	Output    string
	CheckedTS float64
}

type IntentCandidateVerifier func(
	context.Context,
	ai.IntentCandidateAssignment,
	[]IntentCandidateCapture,
) (IntentCandidateVerification, error)

// IntentCandidateEvaluation describes one durable planner evaluation. It does
// not publish commits or mutate Git refs; P8 consumes the publishable decisions.
type IntentCandidateEvaluation struct {
	BranchRef           string
	BranchGeneration    int64
	Captures            []IntentCandidateCapture
	Hints               []IntentDependencyHint
	Planner             interface{ Name() string }
	Health              *IntentPlannerHealth
	RetryLimit          int
	RetryLimitSet       bool
	Preset              config.PresetName
	CommitFormat        ai.CommitFormat
	IncludeDiffs        bool
	ForcedAging         bool
	Provider            string
	Model               string
	ConfigRevisionID    sql.NullInt64
	ConfigProfile       string
	PresetID            string
	PresetVersion       int
	LatestCommit        *ai.CommitSummary
	PathContext         []ai.PathCommitContext
	RecentSoftCommits   []ai.IntentSoftCommitSummary
	PriorFindings       []ai.IntentAtomicityFinding
	Materialize         IntentCandidateMaterializer
	VerificationMode    string
	Verify              IntentCandidateVerifier
	Now                 time.Time
	TargetEventSeqs     []int64
	RejectLocalFallback bool
	allowSemanticPlan   bool
}

// IntentCandidateDecision is one persisted candidate revision plus its exact
// planner message and atomicity result.
type IntentCandidateDecision struct {
	Candidate   state.IntentCandidate
	Assignment  ai.IntentCandidateAssignment
	Atomicity   ai.IntentAtomicityReport
	Publishable bool
}

type IntentCandidateEvaluationResult struct {
	ProtocolVersion        string
	Fallback               string
	PlannerFailure         string
	RetryCount             int
	PlanAttempt            int
	PlanAttemptLimit       int
	UnresolvedCaptureCount int
	PreservedGroupCount    int
	ResolutionMode         string
	PlanFingerprint        string
	PreflightState         string
	FindingCodes           []string
	ProviderCallSkipped    string
	NeedsAttention         bool
	Boundaries             []state.IntentActivityBoundary
	Dependencies           []state.IntentCaptureDependency
	Decisions              []IntentCandidateDecision
	// VisibleCandidateIDs records durable nonterminal candidates included in
	// this exact dependency/planner evaluation, including candidates whose
	// already-published captures needed no new assignment.
	VisibleCandidateIDs []string
}

// IntentSemanticFallbackRequiredError means the bounded semantic path was
// exhausted without producing a valid candidate graph. A durable publication
// drain may respond with one local unlock before replanning its frozen target.
type IntentSemanticFallbackRequiredError struct {
	Failure string
}

func (e *IntentSemanticFallbackRequiredError) Error() string {
	if e == nil || e.Failure == "" {
		return "daemon: intent candidates: semantic fallback required"
	}
	return "daemon: intent candidates: semantic fallback required: " + e.Failure
}

// IntentPlanPreflightError means the durable planning snapshot could not
// produce a locally valid baseline. No provider attempt has been consumed.
type IntentPlanPreflightError struct {
	Failure string
}

func (e *IntentPlanPreflightError) Error() string {
	if e == nil || e.Failure == "" {
		return "daemon: intent candidates: preflight blocked"
	}
	return "daemon: intent candidates: preflight blocked: " + e.Failure
}

const (
	intentBalancedFallbackCaptureCap = 32
	intentBalancedFallbackPathCap    = 12
)

type intentCandidateContinuation struct {
	TargetID   string   `json:"target_id"`
	SourceIDs  []string `json:"source_ids,omitempty"`
	HoldReason string   `json:"hold_reason,omitempty"`
}

type resolvedIntentPlanRun struct {
	Plan          ai.IntentPlanV2               `json:"plan"`
	Continuations []intentCandidateContinuation `json:"continuations,omitempty"`
}

type intentCandidateContinuationOptions struct {
	RewriteDeterministicMessage bool
	IncludePersistedCompanions  bool
	PreservePersistedBoundaries bool
}

// EvaluateIntentCandidates builds and persists the dependency graph, evaluates
// a native-v2 or compatibility plan, runs all seven gates, and revises durable
// candidates. Git publication and history repair deliberately remain outside
// this service.
func EvaluateIntentCandidates(
	ctx context.Context,
	db *state.DB,
	input IntentCandidateEvaluation,
) (IntentCandidateEvaluationResult, error) {
	var result IntentCandidateEvaluationResult
	if db == nil {
		return result, errors.New("daemon: intent candidates: nil state db")
	}
	if input.BranchRef == "" || input.BranchGeneration < 0 {
		return result, errors.New("daemon: intent candidates: invalid branch pair")
	}
	if len(input.Captures) == 0 || len(input.Captures) > state.IntentCandidateMaxCaptures {
		return result, fmt.Errorf("daemon: intent candidates: requires 1..%d captures",
			state.IntentCandidateMaxCaptures)
	}
	if input.Preset == "" {
		input.Preset = config.PresetBalanced
	}
	if input.Now.IsZero() {
		input.Now = time.Now().UTC()
	}
	if input.PresetID == "" {
		input.PresetID = string(config.StrategyIntent) + "." + string(input.Preset)
	}
	if input.PresetVersion <= 0 {
		input.PresetVersion = config.PresetCatalogVersion
	}
	if input.VerificationMode == "" {
		if input.Preset == config.PresetFast {
			input.VerificationMode = "none"
		} else {
			input.VerificationMode = "fast"
		}
	}
	switch input.VerificationMode {
	case "none", "structural", "fast", "full":
	default:
		return result, fmt.Errorf(
			"daemon: intent candidates: unsupported verification mode %q",
			input.VerificationMode)
	}
	if input.Provider == "" && input.Planner != nil {
		input.Provider = input.Planner.Name()
	}
	if !input.RetryLimitSet {
		input.RetryLimit = 2
	}
	if input.RetryLimit < 0 {
		input.RetryLimit = 0
	} else if input.RetryLimit > 2 {
		// The setting counts additional attempts after the initial request.
		// Three total calls are enough to correct a plan without allowing a
		// reconstruction loop.
		input.RetryLimit = 2
	}
	nowSeconds := float64(input.Now.UnixNano()) / 1e9
	if _, err := state.FinalizeExpiredIntentCandidates(
		ctx, db, input.BranchRef, input.BranchGeneration, nowSeconds,
	); err != nil {
		return result, err
	}

	existing, err := state.IntentCandidatesForPair(ctx, db, input.BranchRef,
		input.BranchGeneration, state.IntentCandidateMaxOpenPerPair)
	if err != nil {
		return result, err
	}
	if len(input.TargetEventSeqs) > 0 {
		existing = intentCandidatesWithinTarget(existing, input.TargetEventSeqs)
	}
	for _, candidate := range existing {
		result.VisibleCandidateIDs = append(
			result.VisibleCandidateIDs, candidate.ID)
	}
	allCaptures, err := loadCandidateCaptureContext(ctx, db, input, existing)
	if err != nil {
		return result, err
	}
	if len(input.RecentSoftCommits) == 0 {
		input.RecentSoftCommits = recentIntentSoftCommitSummaries(
			existing, allCaptures, input.Now)
	}
	dependencies, err := BuildIntentCandidateDependencies(input.BranchRef,
		input.BranchGeneration, allCaptures, input.Hints, input.Now)
	if err != nil {
		return result, err
	}
	persistedDependencies, err := state.IntentCaptureDependenciesForPair(
		ctx, db, input.BranchRef, input.BranchGeneration)
	if err != nil {
		return result, err
	}
	dependencies, err = mergeIntentDependencies(persistedDependencies, dependencies)
	if err != nil {
		return result, err
	}
	if err := state.ReplaceIntentCaptureDependencies(ctx, db, input.BranchRef,
		input.BranchGeneration, dependencies); err != nil {
		return result, err
	}
	result.Dependencies = dependencies

	pendingBoundaries, err := state.PendingIntentActivityBoundaries(ctx, db, 0,
		ai.IntentActivityBoundaryCap)
	if err != nil {
		return result, err
	}
	boundaries, throughBoundary := consumableBoundariesForPair(pendingBoundaries,
		input.BranchRef, input.BranchGeneration)
	result.Boundaries = boundaries

	req, err := buildIntentCandidateRequest(input, existing, dependencies, boundaries)
	if err != nil {
		return result, err
	}
	plan, fallback, plannerFailure, retryCount, needsAttention, continuations,
		planRun, err :=
		chooseIntentCandidatePlan(ctx, req, input.Planner, input.Health,
			input.RetryLimit, input.Preset, existing, db, input)
	if err != nil {
		result.PlanAttempt = planRun.AttemptCount
		result.PlanAttemptLimit = planRun.AttemptLimit
		result.UnresolvedCaptureCount = len(planRun.UnresolvedSeqs)
		result.PreservedGroupCount = len(planRun.PreservedGroups)
		result.ResolutionMode = planRun.ResolutionMode.String
		result.PlanFingerprint = planRun.Fingerprint
		result.PreflightState = planRun.ProgressState.String
		result.FindingCodes = append([]string(nil), planRun.FindingCodes...)
		if planRun.ProgressState.String == "preflight_blocked" {
			result.ProviderCallSkipped = "invalid_local_baseline"
		}
		return result, err
	}
	result.ProtocolVersion = plan.ProtocolVersion
	result.Fallback = fallback
	result.PlannerFailure = plannerFailure
	result.RetryCount = retryCount
	result.PlanAttempt = planRun.AttemptCount
	result.PlanAttemptLimit = planRun.AttemptLimit
	result.UnresolvedCaptureCount = len(planRun.UnresolvedSeqs)
	result.PreservedGroupCount = len(planRun.PreservedGroups)
	result.ResolutionMode = planRun.ResolutionMode.String
	result.PlanFingerprint = planRun.Fingerprint
	result.NeedsAttention = needsAttention
	if input.RejectLocalFallback &&
		(result.Fallback != "" || result.PlannerFailure != "") {
		return result, &IntentSemanticFallbackRequiredError{
			Failure: result.PlannerFailure,
		}
	}
	input.allowSemanticPlan = result.ResolutionMode == "provider" ||
		result.ResolutionMode == "local_repair" ||
		result.ResolutionMode == "partial_replan" ||
		result.ResolutionMode == "repair_replan"
	plan, err = stabilizeIntentCandidatePlan(plan, existing, input.BranchRef,
		input.BranchGeneration)
	if err != nil {
		return result, err
	}
	validationBaseRequest := req
	if result.Fallback != "" {
		validationBaseRequest = normalizeIntentFallbackBoundaries(req)
	}
	validationRequest := intentCandidateContinuationValidationRequest(
		validationBaseRequest, continuations)
	if validationErr := ai.ValidateIntentPlanV2(validationRequest, plan); validationErr != nil {
		if input.Preset == config.PresetQuality {
			return result, validationErr
		}
		semanticFailure := ai.SanitizePlannerError(validationErr.Error())
		if result.PlannerFailure == "" {
			result.PlannerFailure = semanticFailure
		} else {
			result.PlannerFailure = ai.SanitizePlannerError(
				result.PlannerFailure + "; normalized plan: " + semanticFailure)
		}
		if input.RejectLocalFallback {
			return result, &IntentSemanticFallbackRequiredError{
				Failure: result.PlannerFailure,
			}
		}
		plan = deterministicIntentCandidatePlan(req, false, false)
		continuations, _, err = continuePersistedIntentCandidates(
			req, &plan, intentCandidateContinuationOptions{
				RewriteDeterministicMessage: true,
			})
		if err == nil {
			plan, err = stabilizeIntentCandidatePlan(plan, existing,
				input.BranchRef, input.BranchGeneration)
		}
		if err == nil {
			validationRequest = intentCandidateContinuationValidationRequest(
				req, continuations)
			err = ai.ValidateIntentPlanV2(validationRequest, plan)
		}
		if err != nil {
			return result, &IntentSemanticFallbackRequiredError{
				Failure: ai.SanitizePlannerError(err.Error()),
			}
		}
		result.Fallback = "deterministic_semantic_rebuild"
	}
	plan, continuations, err = advanceTerminalIntentCandidateIDs(
		ctx, db, input.BranchRef, input.BranchGeneration, plan, continuations)
	if err != nil {
		return result, err
	}

	existingByID := make(map[string]state.IntentCandidate, len(existing))
	for _, candidate := range existing {
		existingByID[candidate.ID] = candidate
	}
	captureBySeq := make(map[int64]IntentCandidateCapture, len(allCaptures))
	for _, capture := range allCaptures {
		captureBySeq[capture.Event.Seq] = capture
	}
	if applyIntentCandidateContinuationLimits(
		&plan, continuations, existingByID, captureBySeq) {
		result.NeedsAttention = true
	}
	existingByID, err = mergeIntentCandidateContinuationViews(
		existingByID, continuations)
	if err != nil {
		return result, err
	}
	if input.Preset == config.PresetBalanced && fallback != "" {
		if applyBalancedFallbackBounds(&plan, existingByID, captureBySeq) {
			result.NeedsAttention = true
		}
	}
	validationRequest = intentCandidateContinuationValidationRequest(
		validationBaseRequest, continuations)
	if err := ai.ValidateIntentPlanV2(validationRequest, plan); err != nil {
		return result, err
	}
	continuationByTarget := make(map[string]intentCandidateContinuation,
		len(continuations))
	for _, continuation := range continuations {
		continuationByTarget[continuation.TargetID] = continuation
	}
	evaluationDependencies := appendIntentCandidateMembershipDependencies(
		dependencies, existing)

	for _, assignment := range plan.Candidates {
		if continuation, ok := continuationByTarget[assignment.CandidateID]; ok && continuation.HoldReason != "" {
			decision, holdErr := heldIntentCandidateContinuationDecision(
				assignment, continuation, existingByID)
			if holdErr != nil {
				return result, holdErr
			}
			result.Decisions = append(result.Decisions, decision)
			result.NeedsAttention = true
			continue
		}
		decision, err := evaluateIntentCandidateAssignment(ctx, input, plan,
			assignment, evaluationDependencies, existingByID, captureBySeq)
		if err != nil {
			return result, err
		}
		if continuation, ok := continuationByTarget[assignment.CandidateID]; ok && len(continuation.SourceIDs) > 0 {
			merged, mergeErr := state.MergeIntentCandidates(ctx, db,
				state.IntentCandidateMergeRequest{
					Target:             decision.Candidate,
					SourceCandidateIDs: continuation.SourceIDs,
					Reason:             "hard_dependency_continuation",
					MergedTS:           nowSeconds,
				})
			if mergeErr != nil {
				return result, mergeErr
			}
			decision.Candidate = merged.Candidate
		} else if err := state.SaveIntentCandidate(
			ctx, db, decision.Candidate); err != nil {
			return result, err
		}
		result.Decisions = append(result.Decisions, decision)
		if !decision.Publishable && assignment.Readiness == ai.IntentCandidateReady {
			result.NeedsAttention = true
		}
	}

	if throughBoundary > 0 {
		if _, err := state.ConsumeIntentActivityBoundaries(ctx, db, throughBoundary,
			float64(input.Now.UnixNano())/1e9); err != nil {
			return result, err
		}
	}
	return result, nil
}

func intentCandidatesWithinTarget(
	candidates []state.IntentCandidate,
	targetSeqs []int64,
) []state.IntentCandidate {
	target := make(map[int64]struct{}, len(targetSeqs))
	for _, seq := range targetSeqs {
		target[seq] = struct{}{}
	}
	filtered := make([]state.IntentCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		inside := len(candidate.Events) > 0
		for _, event := range candidate.Events {
			if _, ok := target[event.EventSeq]; !ok {
				inside = false
				break
			}
		}
		if inside {
			filtered = append(filtered, candidate)
		}
	}
	return filtered
}

// advanceTerminalIntentCandidateIDs keeps planner-stable IDs restart-safe
// without allowing a historical terminal candidate to block new work. The
// first available successor is deterministic, so a crash after saving it will
// reuse that nonterminal candidate on the next pass.
func advanceTerminalIntentCandidateIDs(
	ctx context.Context,
	db *state.DB,
	branchRef string,
	generation int64,
	plan ai.IntentPlanV2,
	continuations []intentCandidateContinuation,
) (ai.IntentPlanV2, []intentCandidateContinuation, error) {
	replacements := make(map[string]string)
	for index := range plan.Candidates {
		assignment := &plan.Candidates[index]
		baseID := assignment.CandidateID
		if replacement, ok := replacements[baseID]; ok {
			assignment.CandidateID = replacement
			continue
		}
		candidate, exists, err := state.IntentCandidateByID(ctx, db, baseID)
		if err != nil {
			return plan, continuations, err
		}
		if !exists || (candidate.BranchRef == branchRef &&
			candidate.BranchGeneration == generation &&
			!terminalIntentCandidateStatus(candidate.Status)) {
			continue
		}
		for attempt := 1; attempt <= state.IntentCandidateMaxOpenPerPair; attempt++ {
			sum := sha256.Sum256([]byte(fmt.Sprintf(
				"%s\x00%s\x00%d\x00%v\x00%d",
				baseID, branchRef, generation, assignment.SelectedSeqs, attempt)))
			successorID := fmt.Sprintf("intent-successor-%x", sum[:12])
			successor, found, loadErr := state.IntentCandidateByID(
				ctx, db, successorID)
			if loadErr != nil {
				return plan, continuations, loadErr
			}
			if found && (successor.BranchRef != branchRef ||
				successor.BranchGeneration != generation ||
				terminalIntentCandidateStatus(successor.Status)) {
				continue
			}
			replacements[baseID] = successorID
			assignment.CandidateID = successorID
			break
		}
		if assignment.CandidateID == baseID {
			return plan, continuations, fmt.Errorf(
				"daemon: intent candidates: exhausted successor IDs for %q", baseID)
		}
	}
	if len(replacements) == 0 {
		return plan, continuations, nil
	}
	for index := range plan.Candidates {
		for dependencyIndex, dependencyID := range plan.Candidates[index].DependsOnCandidates {
			if replacement := replacements[dependencyID]; replacement != "" {
				plan.Candidates[index].DependsOnCandidates[dependencyIndex] = replacement
			}
		}
	}
	for index := range continuations {
		if replacement := replacements[continuations[index].TargetID]; replacement != "" {
			continuations[index].TargetID = replacement
		}
		for sourceIndex, sourceID := range continuations[index].SourceIDs {
			if replacement := replacements[sourceID]; replacement != "" {
				continuations[index].SourceIDs[sourceIndex] = replacement
			}
		}
	}
	return plan, continuations, nil
}

func terminalIntentCandidateStatus(status string) bool {
	return status == state.IntentCandidatePublished ||
		status == state.IntentCandidateSuperseded ||
		status == state.IntentCandidateFailed
}

// BuildIntentCandidateDependencies creates a bounded graph from immutable
// capture metadata. Hard edges preserve replay correctness; soft edges only
// contribute semantic evidence.
func BuildIntentCandidateDependencies(
	branchRef string,
	generation int64,
	captures []IntentCandidateCapture,
	hints []IntentDependencyHint,
	now time.Time,
) ([]state.IntentCaptureDependency, error) {
	if branchRef == "" || generation < 0 {
		return nil, errors.New("daemon: intent dependency graph: invalid branch pair")
	}
	if len(captures) > state.IntentDependencyMaxPerPair {
		return nil, fmt.Errorf("daemon: intent dependency graph: capture context exceeds %d",
			state.IntentDependencyMaxPerPair)
	}
	ordered := append([]IntentCandidateCapture(nil), captures...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Event.Seq < ordered[j].Event.Seq
	})
	known := make(map[int64]struct{}, len(ordered))
	for _, capture := range ordered {
		if capture.Event.Seq <= 0 || capture.Event.BranchRef != branchRef ||
			capture.Event.BranchGeneration != generation {
			return nil, fmt.Errorf("daemon: intent dependency graph: capture %d has wrong provenance",
				capture.Event.Seq)
		}
		if _, duplicate := known[capture.Event.Seq]; duplicate {
			return nil, fmt.Errorf("daemon: intent dependency graph: duplicate capture %d",
				capture.Event.Seq)
		}
		known[capture.Event.Seq] = struct{}{}
	}

	var out []state.IntentCaptureDependency
	seen := make(map[string]struct{})
	add := func(from, to int64, strength, kind, evidence string) error {
		if from <= 0 || to <= 0 || from >= to {
			return fmt.Errorf("daemon: intent dependency graph: invalid edge %d -> %d", from, to)
		}
		key := fmt.Sprintf("%d\x00%d\x00%s\x00%s", from, to, strength, kind)
		if _, ok := seen[key]; ok {
			return nil
		}
		if len(out) >= state.IntentDependencyMaxPerPair {
			return fmt.Errorf("daemon: intent dependency graph: edge cap %d exceeded",
				state.IntentDependencyMaxPerPair)
		}
		seen[key] = struct{}{}
		out = append(out, state.IntentCaptureDependency{
			BranchRef: branchRef, BranchGeneration: generation,
			PrerequisiteSeq: from, DependentSeq: to, Strength: strength,
			Kind: kind, Evidence: intentEvidenceHash(evidence),
			CreatedTS: float64(now.UnixNano()) / 1e9,
		})
		return nil
	}

	lastPath := map[string]int64{}
	lastAfterObject := map[string]int64{}
	createdPath := map[string]int64{}
	lastModule := map[string]int64{}
	lastRole := map[string]int64{}
	for _, capture := range ordered {
		seq := capture.Event.Seq
		paths := intentCapturePaths(capture)
		for _, capturePath := range paths {
			if prior := lastPath[capturePath]; prior > 0 {
				if err := add(prior, seq, state.IntentDependencyHard,
					"same_path", capturePath); err != nil {
					return nil, err
				}
			}
			lastPath[capturePath] = seq
		}
		for _, op := range capture.Ops {
			if op.BeforeOID.Valid {
				if prior := lastAfterObject[op.BeforeOID.String]; prior > 0 && prior < seq {
					if err := add(prior, seq, state.IntentDependencyHard,
						"object_chain", op.BeforeOID.String); err != nil {
						return nil, err
					}
				}
			}
			if op.AfterOID.Valid && op.AfterOID.String != "" {
				lastAfterObject[op.AfterOID.String] = seq
			}
			switch op.Op {
			case "create":
				createdPath[op.Path] = seq
			case "modify", "mode", "delete":
				if prior := createdPath[op.Path]; prior > 0 && prior < seq {
					if err := add(prior, seq, state.IntentDependencyHard,
						"create_before_change", op.Path); err != nil {
						return nil, err
					}
				}
			case "rename":
				if op.OldPath.Valid {
					if prior := createdPath[op.OldPath.String]; prior > 0 && prior < seq {
						if err := add(prior, seq, state.IntentDependencyHard,
							"rename_chain", op.OldPath.String+"->"+op.Path); err != nil {
							return nil, err
						}
					}
				}
				createdPath[op.Path] = seq
			}
		}

		role := intentCaptureRole(capture)
		module := intentCaptureModule(capture)
		if module != "" && role != "documentation" {
			if prior := lastModule[module]; prior > 0 && prior < seq {
				if err := add(prior, seq, state.IntentDependencySoft,
					"module_proximity", module); err != nil {
					return nil, err
				}
			}
			lastModule[module] = seq
		}
		stem := intentSemanticStem(capture)
		if role == "test" {
			if prior := lastRole["code:"+stem]; prior > 0 && prior < seq {
				if err := add(prior, seq, state.IntentDependencySoft,
					"test_source", stem); err != nil {
					return nil, err
				}
			}
			lastRole["test:"+stem] = seq
		} else if role == "code" || role == "migration" {
			if prior := lastRole["test:"+stem]; prior > 0 && prior < seq {
				if err := add(prior, seq, state.IntentDependencySoft,
					"test_source", stem); err != nil {
					return nil, err
				}
			}
			lastRole["code:"+stem] = seq
		}
	}
	for i := 1; i < len(ordered); i++ {
		previous, current := ordered[i-1], ordered[i]
		if current.Event.CapturedTS >= previous.Event.CapturedTS &&
			current.Event.CapturedTS-previous.Event.CapturedTS <= 120 {
			if err := add(previous.Event.Seq, current.Event.Seq,
				state.IntentDependencySoft, "temporal_proximity",
				fmt.Sprintf("%.6f:%.6f", previous.Event.CapturedTS,
					current.Event.CapturedTS)); err != nil {
				return nil, err
			}
		}
	}

	for _, hint := range hints {
		if _, ok := known[hint.PrerequisiteSeq]; !ok {
			return nil, fmt.Errorf("daemon: intent dependency hint references unknown seq %d",
				hint.PrerequisiteSeq)
		}
		if _, ok := known[hint.DependentSeq]; !ok {
			return nil, fmt.Errorf("daemon: intent dependency hint references unknown seq %d",
				hint.DependentSeq)
		}
		strength := string(hint.Strength)
		if hint.Strength != ai.IntentDependencyHard &&
			hint.Strength != ai.IntentDependencySoft {
			return nil, fmt.Errorf("daemon: intent dependency hint has invalid strength %q",
				hint.Strength)
		}
		kind := intentBoundedLabel(hint.Kind, ai.IntentDependencyKindCap)
		if kind == "" {
			return nil, errors.New("daemon: intent dependency hint kind is required")
		}
		if err := add(hint.PrerequisiteSeq, hint.DependentSeq, strength, kind,
			hint.Evidence); err != nil {
			return nil, err
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PrerequisiteSeq != out[j].PrerequisiteSeq {
			return out[i].PrerequisiteSeq < out[j].PrerequisiteSeq
		}
		if out[i].DependentSeq != out[j].DependentSeq {
			return out[i].DependentSeq < out[j].DependentSeq
		}
		if out[i].Strength != out[j].Strength {
			return out[i].Strength < out[j].Strength
		}
		return out[i].Kind < out[j].Kind
	})
	return out, nil
}

func buildIntentCandidateRequest(
	input IntentCandidateEvaluation,
	existing []state.IntentCandidate,
	dependencies []state.IntentCaptureDependency,
	boundaries []state.IntentActivityBoundary,
) (ai.IntentPlanRequestV2, error) {
	offered := make([]ai.OfferedCapture, 0, len(input.Captures))
	visibleSeqs := make(map[int64]struct{}, len(input.Captures))
	for _, capture := range input.Captures {
		op := capture.Event.Operation
		if len(capture.Ops) == 1 && capture.Ops[0].Op != "" {
			op = capture.Ops[0].Op
		}
		offered = append(offered, ai.OfferedCapture{
			Seq: capture.Event.Seq, Path: capture.Event.Path, Op: op,
			Timestamp: time.Unix(0, int64(capture.Event.CapturedTS*1e9)).UTC(),
			Fidelity:  capture.Event.Fidelity, CapturedDiff: capture.CapturedDiff,
		})
		visibleSeqs[capture.Event.Seq] = struct{}{}
	}
	candidates := make([]ai.IntentCandidateSummary, 0, len(existing))
	for _, candidate := range existing {
		seqs := make([]int64, 0, len(candidate.Events))
		for _, event := range candidate.Events {
			if event.EventRole == "coalesced" {
				continue
			}
			seqs = append(seqs, event.EventSeq)
			visibleSeqs[event.EventSeq] = struct{}{}
		}
		candidates = append(candidates, ai.IntentCandidateSummary{
			CandidateID: candidate.ID, Status: candidate.Status,
			Purpose: candidate.Purpose, SelectedSeqs: seqs,
			MissingCompanions: splitIntentSummary(candidate.MissingCompanions),
			Ready:             candidate.Readiness == state.IntentReadinessReady,
			CreatedAt:         secondsTime(candidate.CreatedTS),
			UpdatedAt:         secondsTime(candidate.UpdatedTS),
		})
	}
	edges := make([]ai.IntentCaptureDependency, 0, len(dependencies))
	for _, dependency := range dependencies {
		if _, ok := visibleSeqs[dependency.PrerequisiteSeq]; !ok {
			continue
		}
		if _, ok := visibleSeqs[dependency.DependentSeq]; !ok {
			continue
		}
		edges = append(edges, ai.IntentCaptureDependency{
			FromSeq: dependency.PrerequisiteSeq, ToSeq: dependency.DependentSeq,
			Strength: ai.IntentDependencyStrength(dependency.Strength),
			Kind:     dependency.Kind, EvidenceHash: dependency.Evidence,
		})
	}
	epochs := make([]ai.IntentActivityBoundary, 0, len(boundaries))
	for _, boundary := range boundaries {
		epochs = append(epochs, ai.IntentActivityBoundary{
			Epoch: strconv.FormatInt(boundary.Epoch, 10), Kind: boundary.Kind,
			CreatedAt: secondsTime(boundary.CreatedTS),
		})
	}
	return ai.NewIntentPlanRequestV2(ai.IntentPlanRequestV2Options{
		LatestCommit: input.LatestCommit, PathCommitContext: input.PathContext,
		OfferedCaptures: offered, Candidates: candidates,
		Dependencies: edges, ActivityBoundaries: epochs,
		RecentSoftCommits:      input.RecentSoftCommits,
		PriorAtomicityFindings: input.PriorFindings,
		ForcedAging:            input.ForcedAging,
		IncludeCapturedDiffs:   input.IncludeDiffs,
		CommitFormat:           input.CommitFormat,
	})
}

func recentIntentSoftCommitSummaries(
	candidates []state.IntentCandidate,
	captures []IntentCandidateCapture,
	now time.Time,
) []ai.IntentSoftCommitSummary {
	pathBySeq := make(map[int64]string, len(captures))
	for _, capture := range captures {
		pathBySeq[capture.Event.Seq] = capture.Event.Path
	}
	var summaries []ai.IntentSoftCommitSummary
	for _, candidate := range candidates {
		if candidate.Status != state.IntentCandidateSoftPublished ||
			!candidate.PublishedCommitOID.Valid ||
			!candidate.SoftPublicationDeadline.Valid ||
			candidate.SoftPublicationDeadline.Float64 <=
				float64(now.UnixNano())/1e9 {
			continue
		}
		pathSet := make(map[string]struct{})
		for _, event := range candidate.Events {
			if path := pathBySeq[event.EventSeq]; path != "" {
				pathSet[path] = struct{}{}
			}
		}
		paths := make([]string, 0, len(pathSet))
		for path := range pathSet {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		publishedTS := candidate.UpdatedTS
		if candidate.ReadyTS.Valid {
			publishedTS = candidate.ReadyTS.Float64
		}
		summaries = append(summaries, ai.IntentSoftCommitSummary{
			CandidateID: candidate.ID,
			OID:         candidate.PublishedCommitOID.String,
			Subject:     intentBoundedLabel(candidate.Purpose, ai.SubjectCap),
			Paths:       paths,
			PublishedAt: secondsTime(publishedTS),
			Deadline:    secondsTime(candidate.SoftPublicationDeadline.Float64),
		})
	}
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].PublishedAt.After(summaries[j].PublishedAt)
	})
	if len(summaries) > ai.IntentRecentSoftCommitCap {
		summaries = summaries[:ai.IntentRecentSoftCommitCap]
	}
	return summaries
}

func intentRequestTouchesRepairableSuffix(req ai.IntentPlanRequestV2) bool {
	repairable := make(map[string]struct{}, len(req.RecentSoftCommits))
	for _, commit := range req.RecentSoftCommits {
		repairable[commit.CandidateID] = struct{}{}
	}
	if len(repairable) == 0 {
		return false
	}
	persistedOwner := make(map[int64]string)
	for _, candidate := range req.Candidates {
		if _, ok := repairable[candidate.CandidateID]; !ok {
			continue
		}
		for _, seq := range candidate.SelectedSeqs {
			persistedOwner[seq] = candidate.CandidateID
		}
	}
	offered := make(map[int64]struct{}, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		offered[capture.Seq] = struct{}{}
	}
	for _, edge := range req.Dependencies {
		if edge.Strength != ai.IntentDependencyHard {
			continue
		}
		_, fromPersisted := persistedOwner[edge.FromSeq]
		_, toPersisted := persistedOwner[edge.ToSeq]
		_, fromOffered := offered[edge.FromSeq]
		_, toOffered := offered[edge.ToSeq]
		if (fromPersisted && toOffered) || (fromOffered && toPersisted) {
			return true
		}
	}
	return false
}

func preflightIntentCandidatePlan(
	req ai.IntentPlanRequestV2,
	preset config.PresetName,
) (ai.IntentPlanRequestV2, []intentCandidateContinuation, error) {
	if preset != config.PresetFast && preset != config.PresetBalanced &&
		preset != config.PresetQuality {
		return req, nil, fmt.Errorf(
			"daemon: intent candidates: unsupported preset %q", preset)
	}
	baseline := deterministicIntentCandidatePlan(req, false, false)
	continuations, _, err := continuePersistedIntentCandidates(
		req, &baseline, intentCandidateContinuationOptions{})
	if err != nil {
		return req, continuations, err
	}
	plannerRequest := intentCandidateContinuationValidationRequest(
		req, continuations)
	baseline = declareIntentFallbackDependencies(plannerRequest, baseline)
	if err := ai.ValidateIntentPlanV2(plannerRequest, baseline); err != nil {
		return plannerRequest, continuations, err
	}
	plannerRequest.BaselineCandidates = baseline.Candidates
	if err := ai.ValidateIntentPlanRequestV2(plannerRequest); err != nil {
		return plannerRequest, continuations, err
	}
	return plannerRequest, continuations, nil
}

func chooseIntentCandidatePlan(
	ctx context.Context,
	req ai.IntentPlanRequestV2,
	planner interface{ Name() string },
	health *IntentPlannerHealth,
	retryLimit int,
	preset config.PresetName,
	existing []state.IntentCandidate,
	db *state.DB,
	input IntentCandidateEvaluation,
) (ai.IntentPlanV2, string, string, int, bool, []intentCandidateContinuation, state.IntentPlanRun, error) {
	plannerFailure := ""
	retryCount := 0
	repairSuffixCorrection := false
	plannerRequest, baselineContinuations, preflightErr :=
		preflightIntentCandidatePlan(req, preset)
	fingerprintRequest := req
	fingerprintRequest.BaselineCandidates = plannerRequest.BaselineCandidates
	run, err := newIntentPlanRun(fingerprintRequest, input, retryLimit+1)
	if err != nil {
		return ai.IntentPlanV2{}, "", "", retryCount, false, nil,
			state.IntentPlanRun{}, err
	}
	if preflightErr != nil {
		run, err = state.EnsureIntentPlanRun(ctx, db, run)
		if err != nil {
			return ai.IntentPlanV2{}, "", "", retryCount, false, nil, run, err
		}
		run.ProgressState = sql.NullString{String: "preflight_blocked", Valid: true}
		run.ResolutionMode = sql.NullString{String: "local_preflight", Valid: true}
		run.UnresolvedSeqs = offeredIntentSeqs(req)
		run.FindingCodes = []string{"preflight_invalid"}
		var validation *ai.IntentPlanV2ValidationError
		if errors.As(preflightErr, &validation) {
			run.FindingCodes = intentFindingCodes(validation.Findings)
		}
		if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
			return ai.IntentPlanV2{}, "", "", retryCount, false, nil, run, err
		}
		return ai.IntentPlanV2{}, "", "", retryCount, false,
			baselineContinuations, run, &IntentPlanPreflightError{
				Failure: ai.SanitizePlannerError(preflightErr.Error()),
			}
	}
	var permit IntentPlannerHealthPermit
	permitHeld := false
	if planner != nil {
		if health != nil {
			var acquireErr error
			permit, acquireErr = health.Acquire(ctx)
			if acquireErr != nil {
				var openErr *IntentPlannerCircuitOpenError
				if !errors.As(acquireErr, &openErr) {
					return ai.IntentPlanV2{}, "", "", retryCount, false, nil, run, acquireErr
				}
				planner = nil
			} else {
				permitHeld = true
			}
		}
	}
	if planner != nil {
		var previousSignature string
		var lockedCandidates []ai.IntentCandidateAssignment
		for {
			reserved, allowed, reserveErr := state.ReserveIntentPlanAttempt(ctx, db, run)
			if reserveErr != nil {
				return ai.IntentPlanV2{}, "", "", retryCount, false, nil, run, reserveErr
			}
			run = reserved
			if !allowed {
				if run.Completed && run.ResolvedPlanJSON.Valid {
					plan, continuations, loadErr := loadResolvedIntentPlanRun(
						req, run.ResolvedPlanJSON.String)
					if loadErr != nil {
						run.Completed = false
						run.ResolvedPlanJSON = sql.NullString{}
						run.ProgressState = sql.NullString{
							String: "local_cache_rebuild", Valid: true,
						}
						run.ResolutionMode = sql.NullString{}
						run.FindingCodes = []string{"cached_plan_invalid"}
						if updateErr := state.UpdateIntentPlanRun(
							ctx, db, run); updateErr != nil {
							return ai.IntentPlanV2{}, "", "", retryCount,
								false, nil, run, updateErr
						}
						plannerFailure = ai.SanitizePlannerError(loadErr.Error())
						break
					}
					run.ResolutionMode = sql.NullString{
						String: "completed_plan_reuse", Valid: true,
					}
					return plan, "", "", retryCount, false,
						continuations, run, nil
				}
				break
			}
			if previousSignature == "" && run.AttemptCount > 1 &&
				run.NormalizedPartition.Valid {
				previousSignature = run.NormalizedPartition.String + "\x00" +
					strings.Join(run.FindingCodes, ",")
			}
			retryCount = run.AttemptCount - 1
			attemptCtx := prompttrace.WithRetryCount(ctx, retryCount)
			plan, err := ai.PlanIntentV2WithCompatibility(
				attemptCtx, planner, plannerRequest)
			if rejected, ok := ai.RejectedIntentPlanV2(err); ok {
				plan = rejected
			}
			if len(lockedCandidates) > 0 {
				plan = mergeLockedIntentCandidates(req, lockedCandidates, plan)
			}
			plannerCallFailed := err != nil
			var continuations []intentCandidateContinuation
			if err == nil {
				continuations, _, err =
					continuePersistedIntentCandidates(
						req, &plan, intentCandidateContinuationOptions{})
				if err == nil {
					validationReq := intentCandidateContinuationValidationRequest(
						req, continuations)
					err = ai.ValidateIntentPlanV2(validationReq, plan)
					if err == nil {
						err = validatePlannerSemanticRationale(validationReq, plan)
					}
				}
				plannerCallFailed = false
			}
			if err == nil {
				if err := storeResolvedIntentPlanRun(
					&run, plan, continuations); err != nil {
					return ai.IntentPlanV2{}, "", "", retryCount, false,
						nil, run, err
				}
				run.Completed = true
				mode := "provider"
				if len(lockedCandidates) > 0 {
					mode = "partial_replan"
				} else if repairSuffixCorrection {
					mode = "repair_replan"
				}
				run.ResolutionMode = sql.NullString{String: mode, Valid: true}
				run.ProgressState = sql.NullString{String: "completed", Valid: true}
				run.UnresolvedSeqs = nil
				if len(lockedCandidates) > 0 {
					run.PreservedGroups = intentAssignmentMembership(lockedCandidates)
				} else {
					run.PreservedGroups = nil
				}
				if updateErr := state.UpdateIntentPlanRun(ctx, db, run); updateErr != nil {
					return ai.IntentPlanV2{}, "", "", retryCount, false, nil, run, updateErr
				}
				if health != nil && permitHeld {
					if healthErr := health.Complete(ctx, permit, nil); healthErr != nil {
						return ai.IntentPlanV2{}, "", "", retryCount, false, nil, run, healthErr
					}
				}
				return plan, "", "", retryCount, false, continuations, run, nil
			}
			if ctx.Err() != nil {
				if health != nil && permitHeld {
					_ = health.Complete(ctx, permit, err)
				}
				return ai.IntentPlanV2{}, "", "", retryCount, false, nil, run, ctx.Err()
			}
			var validation *ai.IntentPlanV2ValidationError
			if errors.As(err, &validation) && len(validation.Findings) > 0 {
				localRepairReq := intentCandidateContinuationValidationRequest(
					req, continuations)
				if repaired, repairMode, ok := repairIntentCandidatePlanLocally(
					localRepairReq, plannerRequest.BaselineCandidates,
					plan, validation.Findings,
				); ok {
					if err := storeResolvedIntentPlanRun(
						&run, repaired, continuations); err != nil {
						return ai.IntentPlanV2{}, "", "", retryCount,
							false, nil, run, err
					}
					run.Completed = true
					run.ResolutionMode = sql.NullString{String: "local_repair", Valid: true}
					run.ProgressState = sql.NullString{String: "completed", Valid: true}
					run.UnresolvedSeqs = nil
					run.PreservedGroups = nil
					run.FindingCodes = intentFindingCodes(validation.Findings)
					if updateErr := state.UpdateIntentPlanRun(ctx, db, run); updateErr != nil {
						return ai.IntentPlanV2{}, "", "", retryCount, false, nil, run, updateErr
					}
					if health != nil && permitHeld {
						if healthErr := health.Complete(ctx, permit, nil); healthErr != nil {
							return ai.IntentPlanV2{}, "", "", retryCount, false, nil, run, healthErr
						}
					}
					return repaired, repairMode,
						ai.SanitizePlannerError(err.Error()), retryCount, false,
						continuations, run, nil
				}
				partition := intentPlanPartitionSignature(plan)
				codes := intentFindingCodes(validation.Findings)
				signature := partition + "\x00" + strings.Join(codes, ",")
				run.FindingCodes = codes
				run.UnresolvedSeqs = offeredIntentSeqs(req)
				if preserved, partial, ok := preserveIntentPlanGroups(
					req, plan, validation.Findings); ok &&
					len(preserved) > len(lockedCandidates) {
					lockedCandidates = preserved
					var partialPreflightErr error
					plannerRequest, _, partialPreflightErr =
						preflightIntentCandidatePlan(partial, preset)
					if partialPreflightErr != nil {
						run.ProgressState = sql.NullString{
							String: "preflight_blocked", Valid: true,
						}
						run.ResolutionMode = sql.NullString{
							String: "local_preflight", Valid: true,
						}
						run.FindingCodes = []string{"preflight_invalid"}
						run.UnresolvedSeqs = offeredIntentSeqs(partial)
						if updateErr := state.UpdateIntentPlanRun(
							ctx, db, run); updateErr != nil {
							return ai.IntentPlanV2{}, "", "", retryCount,
								false, nil, run, updateErr
						}
						return ai.IntentPlanV2{}, "", "", retryCount,
							false, nil, run, &IntentPlanPreflightError{
								Failure: ai.SanitizePlannerError(
									partialPreflightErr.Error()),
							}
					}
					run.PreservedGroups = intentAssignmentMembership(preserved)
					run.UnresolvedSeqs = offeredIntentSeqs(partial)
				}
				run.NormalizedPartition = sql.NullString{String: partition, Valid: partition != ""}
				if previousSignature != "" && signature == previousSignature {
					run.ProgressState = sql.NullString{String: "no_progress", Valid: true}
					_ = state.UpdateIntentPlanRun(ctx, db, run)
					plannerFailure = ai.SanitizePlannerError(err.Error())
					break
				}
				previousSignature = signature
				run.ProgressState = sql.NullString{String: "refining", Valid: true}
				if updateErr := state.UpdateIntentPlanRun(ctx, db, run); updateErr != nil {
					return ai.IntentPlanV2{}, "", "", retryCount, false, nil, run, updateErr
				}
				plannerRequest.RetryCorrection = ai.BuildIntentAtomicityCorrection(validation.Findings)
				if intentRequestTouchesRepairableSuffix(req) {
					repairSuffixCorrection = true
					plannerRequest.RetryCorrection = strings.TrimSpace(
						plannerRequest.RetryCorrection + "\n" +
							"Replan the repairable private ACD suffix with the new captures. " +
							"You may merge or repartition only the recent soft commits listed " +
							"in this request. Preserve every other candidate boundary.")
				}
				if len(lockedCandidates) > 0 {
					plannerRequest.RetryCorrection += "\nPreserve these validated locked groups: " +
						intentPlanPartitionSignature(ai.IntentPlanV2{Candidates: lockedCandidates})
				}
				plannerCallFailed = false
				plannerFailure = ai.SanitizePlannerError(err.Error())
				continue
			}
			plannerFailure = ai.SanitizePlannerError(err.Error())
			if health != nil && permitHeld {
				failure := classifyIntentPlannerHealthFailure(err, plannerCallFailed)
				if healthErr := health.Complete(ctx, permit, failure); healthErr != nil {
					return ai.IntentPlanV2{}, "", "", retryCount, false, nil, run, healthErr
				}
				permitHeld = false
			}
			break
		}
	}
	if health != nil && permitHeld {
		// A provider response that failed semantic validation is not a transport
		// outage. Evidence reconstruction below is the successful terminal path.
		if healthErr := health.Complete(ctx, permit, nil); healthErr != nil {
			return ai.IntentPlanV2{}, "", "", retryCount, false, nil, run, healthErr
		}
	}
	if preset != config.PresetFast && preset != config.PresetBalanced && preset != config.PresetQuality {
		return ai.IntentPlanV2{}, "", plannerFailure, retryCount, false, nil, run,
			fmt.Errorf("daemon: intent candidates: unsupported preset %q", preset)
	}
	plan := deterministicIntentCandidatePlan(req, true, false)
	fallbackNeedsAttention := false
	if preset == config.PresetBalanced {
		plan, fallbackNeedsAttention = balancedIntentCandidatePlan(req)
	}
	fallbackReq := normalizeIntentFallbackBoundaries(req)
	continuations, companionNeedsAttention, err := continuePersistedIntentCandidates(
		fallbackReq, &plan, intentCandidateContinuationOptions{
			PreservePersistedBoundaries: true,
			IncludePersistedCompanions:  true,
		})
	if err != nil {
		return ai.IntentPlanV2{}, "", plannerFailure, retryCount, false, nil, run, err
	}
	plan = declareIntentFallbackDependencies(
		intentCandidateContinuationValidationRequest(fallbackReq, continuations), plan)
	validationReq := intentCandidateContinuationValidationRequest(
		fallbackReq, continuations)
	if validationErr := ai.ValidateIntentPlanV2(validationReq, plan); validationErr != nil {
		return ai.IntentPlanV2{}, "", plannerFailure, retryCount, false,
			nil, run, validationErr
	}
	var messageErr error
	var messageReady bool
	plan, plannerFailure, messageReady, messageErr = applyIntentFallbackMessageQuality(
		ctx, planner,
		intentCandidateContinuationValidationRequest(fallbackReq, continuations),
		plan, plannerFailure)
	if messageErr != nil {
		return ai.IntentPlanV2{}, "", plannerFailure, retryCount, false, nil, run, messageErr
	}
	validationErr := ai.ValidateIntentPlanV2(validationReq, plan)
	if validationErr != nil {
		return ai.IntentPlanV2{}, "", plannerFailure, retryCount, false, nil, run, validationErr
	}
	if run.AttemptCount == 0 {
		var ensureErr error
		run, ensureErr = state.EnsureIntentPlanRun(ctx, db, run)
		if ensureErr != nil {
			return ai.IntentPlanV2{}, "", plannerFailure, retryCount, false, nil, run, ensureErr
		}
	}
	if !messageReady {
		holdIntentFallbackForMessage(&plan)
		run.Completed = false
		run.ResolutionMode = sql.NullString{
			String: "waiting_message_rewrite", Valid: true,
		}
		run.ProgressState = sql.NullString{
			String: "waiting_message_rewrite", Valid: true,
		}
		run.UnresolvedSeqs = offeredIntentSeqs(req)
		run.ResolvedPlanJSON = sql.NullString{}
		if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
			return ai.IntentPlanV2{}, "", plannerFailure, retryCount,
				false, nil, run, err
		}
		return plan, "waiting_message_rewrite", plannerFailure, retryCount,
			true, continuations, run, nil
	}
	if err := storeResolvedIntentPlanRun(&run, plan, continuations); err != nil {
		return ai.IntentPlanV2{}, "", plannerFailure, retryCount, false,
			nil, run, err
	}
	run.Completed = true
	resolutionMode := "evidence_partition"
	if intentPlanDependsOnPersistedCandidate(req, plan) {
		resolutionMode = "dependent_message_fallback"
	}
	run.ResolutionMode = sql.NullString{String: resolutionMode, Valid: true}
	run.ProgressState = sql.NullString{String: "completed", Valid: true}
	run.UnresolvedSeqs = nil
	run.PreservedGroups = nil
	if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
		return ai.IntentPlanV2{}, "", plannerFailure, retryCount, false, nil, run, err
	}
	return plan, "evidence_partition", plannerFailure, retryCount,
		fallbackNeedsAttention || companionNeedsAttention, continuations, run, nil
}

func storeResolvedIntentPlanRun(
	run *state.IntentPlanRun,
	plan ai.IntentPlanV2,
	continuations []intentCandidateContinuation,
) error {
	raw, err := json.Marshal(resolvedIntentPlanRun{
		Plan: plan, Continuations: continuations,
	})
	if err != nil {
		return fmt.Errorf("daemon: encode resolved intent plan: %w", err)
	}
	if len(raw) > state.IntentResolvedPlanJSONCap {
		return fmt.Errorf("daemon: resolved intent plan exceeds %d bytes",
			state.IntentResolvedPlanJSONCap)
	}
	run.ResolvedPlanJSON = sql.NullString{String: string(raw), Valid: true}
	return nil
}

func loadResolvedIntentPlanRun(
	req ai.IntentPlanRequestV2,
	raw string,
) (ai.IntentPlanV2, []intentCandidateContinuation, error) {
	var resolved resolvedIntentPlanRun
	if err := json.Unmarshal([]byte(raw), &resolved); err != nil {
		return ai.IntentPlanV2{}, nil,
			fmt.Errorf("daemon: decode resolved intent plan: %w", err)
	}
	validationReq := intentCandidateContinuationValidationRequest(
		req, resolved.Continuations)
	if err := ai.ValidateIntentPlanV2(validationReq, resolved.Plan); err != nil {
		return ai.IntentPlanV2{}, nil,
			fmt.Errorf("daemon: validate resolved intent plan: %w", err)
	}
	return resolved.Plan, resolved.Continuations, nil
}

func validatePlannerSemanticRationale(
	req ai.IntentPlanRequestV2,
	plan ai.IntentPlanV2,
) error {
	for _, candidate := range plan.Candidates {
		if len(candidate.SelectedSeqs) <= 1 ||
			intentRequestSeqsConnected(candidate.SelectedSeqs, req.Dependencies) {
			continue
		}
		rationale := strings.ToLower(strings.TrimSpace(
			candidate.Purpose + " " + candidate.GroupingReason))
		for _, weak := range []string{
			"same evaluation window", "same window", "happened together",
			"module directory", "directory proximity", "unrelated", "disconnected",
		} {
			if strings.Contains(rationale, weak) {
				finding := ai.IntentAtomicityFinding{
					CandidateID: candidate.CandidateID,
					Gate:        ai.IntentAtomicitySeparation,
					Code:        "candidate_disconnected",
					Summary:     "semantic rationale relies on weak proximity rather than one intent",
				}
				return &ai.IntentPlanV2ValidationError{
					Message:  "intent planner v2: candidate_disconnected: semantic rationale relies on weak proximity",
					Findings: []ai.IntentAtomicityFinding{finding},
				}
			}
		}
	}
	return nil
}

func intentRequestSeqsConnected(
	seqs []int64,
	edges []ai.IntentCaptureDependency,
) bool {
	allowed := make(map[int64]struct{}, len(seqs))
	for _, seq := range seqs {
		allowed[seq] = struct{}{}
	}
	adj := make(map[int64][]int64, len(seqs))
	for _, edge := range edges {
		if edge.Strength == ai.IntentDependencySoft &&
			!strongIntentSemanticDependency(edge.Kind) {
			continue
		}
		if _, ok := allowed[edge.FromSeq]; !ok {
			continue
		}
		if _, ok := allowed[edge.ToSeq]; !ok {
			continue
		}
		adj[edge.FromSeq] = append(adj[edge.FromSeq], edge.ToSeq)
		adj[edge.ToSeq] = append(adj[edge.ToSeq], edge.FromSeq)
	}
	seen := map[int64]struct{}{seqs[0]: {}}
	queue := []int64{seqs[0]}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, next := range adj[current] {
			if _, ok := seen[next]; ok {
				continue
			}
			seen[next] = struct{}{}
			queue = append(queue, next)
		}
	}
	return len(seen) == len(seqs)
}

func newIntentPlanRun(
	req ai.IntentPlanRequestV2,
	input IntentCandidateEvaluation,
	attemptLimit int,
) (state.IntentPlanRun, error) {
	if attemptLimit < 1 {
		attemptLimit = 1
	} else if attemptLimit > 3 {
		attemptLimit = 3
	}
	fingerprintRequest := req
	fingerprintRequest.OfferedCaptures = append(
		[]ai.OfferedCapture(nil), req.OfferedCaptures...)
	for i := range fingerprintRequest.OfferedCaptures {
		digest := sha256.Sum256([]byte(
			fingerprintRequest.OfferedCaptures[i].CapturedDiff))
		fingerprintRequest.OfferedCaptures[i].CapturedDiff =
			fmt.Sprintf("sha256:%x", digest)
	}
	fingerprintRequest.CapturedDiffTransform = prompttrace.TransformMetadata{}
	fingerprintRequest.RetryCorrection = ""
	targetEventSeqs := append([]int64(nil), input.TargetEventSeqs...)
	sort.Slice(targetEventSeqs, func(i, j int) bool {
		return targetEventSeqs[i] < targetEventSeqs[j]
	})
	fingerprintInput := struct {
		Domain               string                 `json:"domain"`
		Request              ai.IntentPlanRequestV2 `json:"request"`
		BranchRef            string                 `json:"branch_ref"`
		BranchGeneration     int64                  `json:"branch_generation"`
		Provider             string                 `json:"provider"`
		Model                string                 `json:"model"`
		Preset               config.PresetName      `json:"preset"`
		CommitFormat         ai.CommitFormat        `json:"commit_format"`
		PresetVersion        int                    `json:"preset_version"`
		ConfigRevisionID     int64                  `json:"config_revision_id"`
		ConfigRevisionValid  bool                   `json:"config_revision_valid"`
		VerificationMode     string                 `json:"verification_mode"`
		IncludeCapturedDiffs bool                   `json:"include_captured_diffs"`
		ConfigProfile        string                 `json:"config_profile"`
		PresetID             string                 `json:"preset_id"`
		TargetEventSeqs      []int64                `json:"target_event_seqs"`
		RejectLocalFallback  bool                   `json:"reject_local_fallback"`
		AttemptLimit         int                    `json:"attempt_limit"`
	}{
		Domain:    "acd.intent-plan-run/v2",
		Request:   fingerprintRequest,
		BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration,
		Provider: input.Provider, Model: input.Model, Preset: input.Preset,
		CommitFormat: input.CommitFormat, PresetVersion: input.PresetVersion,
		ConfigRevisionID:     input.ConfigRevisionID.Int64,
		ConfigRevisionValid:  input.ConfigRevisionID.Valid,
		VerificationMode:     input.VerificationMode,
		IncludeCapturedDiffs: input.IncludeDiffs,
		ConfigProfile:        input.ConfigProfile,
		PresetID:             input.PresetID,
		TargetEventSeqs:      targetEventSeqs,
		RejectLocalFallback:  input.RejectLocalFallback,
		AttemptLimit:         attemptLimit,
	}
	encoded, err := json.Marshal(fingerprintInput)
	if err != nil {
		return state.IntentPlanRun{},
			fmt.Errorf("daemon: encode intent plan fingerprint: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return state.IntentPlanRun{
		Fingerprint:      fmt.Sprintf("sha256:%x", digest),
		BranchRef:        input.BranchRef,
		BranchGeneration: input.BranchGeneration,
		Provider: sql.NullString{String: input.Provider,
			Valid: strings.TrimSpace(input.Provider) != ""},
		Model: sql.NullString{String: input.Model,
			Valid: strings.TrimSpace(input.Model) != ""},
		ConfigRevisionID: input.ConfigRevisionID,
		AttemptLimit:     attemptLimit,
		UnresolvedSeqs:   offeredIntentSeqs(req),
	}, nil
}

func offeredIntentSeqs(req ai.IntentPlanRequestV2) []int64 {
	seqs := make([]int64, 0, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		seqs = append(seqs, capture.Seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs
}

func intentPlanMembership(plan ai.IntentPlanV2) [][]int64 {
	groups := make([][]int64, 0, len(plan.Candidates))
	for _, candidate := range plan.Candidates {
		seqs := append([]int64(nil), candidate.SelectedSeqs...)
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		groups = append(groups, seqs)
	}
	sort.Slice(groups, func(i, j int) bool {
		if len(groups[i]) == 0 || len(groups[j]) == 0 {
			return len(groups[i]) < len(groups[j])
		}
		return groups[i][0] < groups[j][0]
	})
	return groups
}

func intentAssignmentMembership(assignments []ai.IntentCandidateAssignment) [][]int64 {
	return intentPlanMembership(ai.IntentPlanV2{Candidates: assignments})
}

func mergeLockedIntentCandidates(
	req ai.IntentPlanRequestV2,
	locked []ai.IntentCandidateAssignment,
	replanned ai.IntentPlanV2,
) ai.IntentPlanV2 {
	merged := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	merged.Candidates = append(merged.Candidates, cloneIntentPlanV2(
		ai.IntentPlanV2{Candidates: locked}).Candidates...)
	merged.Candidates = append(merged.Candidates, replanned.Candidates...)
	addIntentCandidateDependencies(req, &merged)
	return merged
}

// preserveIntentPlanGroups locks only structurally sound groups whose hard
// dependency closure is complete. Everything else is returned as a smaller
// request, so malformed membership can never be salvaged into a commit.
func preserveIntentPlanGroups(
	req ai.IntentPlanRequestV2,
	plan ai.IntentPlanV2,
	findings []ai.IntentAtomicityFinding,
) ([]ai.IntentCandidateAssignment, ai.IntentPlanRequestV2, bool) {
	badIDs := make(map[string]struct{}, len(findings))
	for _, finding := range findings {
		if finding.CandidateID != "" {
			badIDs[finding.CandidateID] = struct{}{}
		}
	}
	offered := make(map[int64]ai.OfferedCapture, len(req.OfferedCaptures))
	counts := make(map[int64]int, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		offered[capture.Seq] = capture
	}
	for _, candidate := range plan.Candidates {
		for _, seq := range candidate.SelectedSeqs {
			counts[seq]++
		}
	}
	owner := make(map[int64]string, len(offered))
	for _, candidate := range plan.Candidates {
		for _, seq := range candidate.SelectedSeqs {
			owner[seq] = candidate.CandidateID
		}
	}
	for _, edge := range req.Dependencies {
		if edge.Strength != ai.IntentDependencyHard {
			continue
		}
		fromID, fromOK := owner[edge.FromSeq]
		toID, toOK := owner[edge.ToSeq]
		if fromOK && toOK && fromID != toID {
			badIDs[fromID] = struct{}{}
			badIDs[toID] = struct{}{}
		}
	}
	var preserved []ai.IntentCandidateAssignment
	preservedSeq := make(map[int64]struct{})
	for _, candidate := range plan.Candidates {
		if _, bad := badIDs[candidate.CandidateID]; bad || candidate.CandidateID == "" ||
			len(candidate.SelectedSeqs) == 0 {
			continue
		}
		valid := true
		for _, seq := range candidate.SelectedSeqs {
			if _, ok := offered[seq]; !ok || counts[seq] != 1 {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}
		preserved = append(preserved, candidate)
		for _, seq := range candidate.SelectedSeqs {
			preservedSeq[seq] = struct{}{}
		}
	}
	if len(preserved) == 0 || len(preservedSeq) == len(offered) {
		return nil, ai.IntentPlanRequestV2{}, false
	}
	partial := req
	partial.OfferedCaptures = make([]ai.OfferedCapture, 0,
		len(req.OfferedCaptures)-len(preservedSeq))
	for _, capture := range req.OfferedCaptures {
		if _, locked := preservedSeq[capture.Seq]; !locked {
			partial.OfferedCaptures = append(partial.OfferedCaptures, capture)
		}
	}
	partial.Candidates = append([]ai.IntentCandidateSummary(nil), req.Candidates...)
	for _, candidate := range preserved {
		partial.Candidates = append(partial.Candidates, ai.IntentCandidateSummary{
			CandidateID:  candidate.CandidateID,
			Status:       "locked_ready",
			Purpose:      candidate.Purpose,
			SelectedSeqs: append([]int64(nil), candidate.SelectedSeqs...),
			Ready:        candidate.Readiness == ai.IntentCandidateReady,
		})
	}
	return preserved, partial, len(partial.OfferedCaptures) > 0
}

func intentPlanPartitionSignature(plan ai.IntentPlanV2) string {
	groups := intentPlanMembership(plan)
	parts := make([]string, 0, len(groups))
	for _, group := range groups {
		parts = append(parts, fmt.Sprint(group))
	}
	return strings.Join(parts, "|")
}

func intentFindingCodes(findings []ai.IntentAtomicityFinding) []string {
	set := make(map[string]struct{}, len(findings))
	for _, finding := range findings {
		code := strings.TrimSpace(finding.Code)
		if code != "" {
			set[code] = struct{}{}
		}
	}
	codes := make([]string, 0, len(set))
	for code := range set {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes
}

func applyIntentFallbackMessageQuality(
	ctx context.Context,
	planner interface{ Name() string },
	req ai.IntentPlanRequestV2,
	plan ai.IntentPlanV2,
	plannerFailure string,
) (ai.IntentPlanV2, string, bool, error) {
	if _, ok := planner.(ai.IntentMessageRewriter); !ok {
		return plan, plannerFailure, false, nil
	}
	rewritten, err := ai.ApplyIntentV2MessageQuality(ctx, planner, req, plan)
	if err == nil {
		return rewritten, plannerFailure, true, nil
	}
	if ctx.Err() != nil {
		return ai.IntentPlanV2{}, plannerFailure, false, ctx.Err()
	}
	messageFailure := "message quality fallback: " + err.Error()
	if plannerFailure != "" {
		messageFailure = plannerFailure + "; " + messageFailure
	}
	return plan, ai.SanitizePlannerError(messageFailure), false, nil
}

func holdIntentFallbackForMessage(plan *ai.IntentPlanV2) {
	for i := range plan.Candidates {
		candidate := &plan.Candidates[i]
		if candidate.Readiness != ai.IntentCandidateReady {
			continue
		}
		candidate.Readiness = ai.IntentCandidateWait
		candidate.Subject = ""
		candidate.Body = ""
		if !containsIntentString(candidate.MissingCompanions,
			"semantic commit message unavailable") {
			candidate.MissingCompanions = append(candidate.MissingCompanions,
				"semantic commit message unavailable")
		}
	}
}

func intentPlanDependsOnPersistedCandidate(
	req ai.IntentPlanRequestV2,
	plan ai.IntentPlanV2,
) bool {
	persisted := make(map[string]struct{}, len(req.Candidates))
	for _, candidate := range req.Candidates {
		persisted[candidate.CandidateID] = struct{}{}
	}
	for _, candidate := range plan.Candidates {
		for _, dependencyID := range candidate.DependsOnCandidates {
			if _, ok := persisted[dependencyID]; ok {
				return true
			}
		}
	}
	return false
}

func repairIntentCandidatePlanLocally(
	req ai.IntentPlanRequestV2,
	baseline []ai.IntentCandidateAssignment,
	plan ai.IntentPlanV2,
	findings []ai.IntentAtomicityFinding,
) (ai.IntentPlanV2, string, bool) {
	current := cloneIntentPlanV2(plan)
	seen := make(map[string]struct{})
	for pass := 0; pass < 3; pass++ {
		signature := localIntentPlanRepairSignature(current)
		if _, repeated := seen[signature]; repeated {
			break
		}
		seen[signature] = struct{}{}

		forced, forcedChanged := repairForcedIntentCandidatesFromBaseline(
			current, baseline, findings)
		if forcedChanged {
			current = forced
			if ai.ValidateIntentPlanV2(req, current) == nil {
				return current, "repaired_forced_aging", true
			}
		}
		if repaired, ok := repairIntentCandidateDependencies(req, current); ok {
			mode := "repaired_dependency_declarations"
			if forcedChanged {
				mode = "repaired_forced_aging"
			}
			return repaired, mode, true
		}
		if !forcedChanged {
			break
		}
	}
	return plan, "", false
}

func repairForcedIntentCandidatesFromBaseline(
	plan ai.IntentPlanV2,
	baseline []ai.IntentCandidateAssignment,
	findings []ai.IntentAtomicityFinding,
) (ai.IntentPlanV2, bool) {
	forcedIDs := make(map[string]struct{})
	for _, finding := range findings {
		if finding.Code == "forced_capture_deferred" && finding.CandidateID != "" {
			forcedIDs[finding.CandidateID] = struct{}{}
		}
	}
	if len(forcedIDs) == 0 || len(baseline) == 0 {
		return plan, false
	}
	baselineByMembership := make(map[string]ai.IntentCandidateAssignment,
		len(baseline))
	for _, candidate := range baseline {
		if candidate.Readiness != ai.IntentCandidateReady ||
			len(candidate.MissingCompanions) > 0 {
			continue
		}
		baselineByMembership[intentSeqMembershipKey(candidate.SelectedSeqs)] = candidate
	}
	repaired := cloneIntentPlanV2(plan)
	changed := false
	for i := range repaired.Candidates {
		candidate := &repaired.Candidates[i]
		if _, repair := forcedIDs[candidate.CandidateID]; !repair {
			continue
		}
		baselineCandidate, ok := baselineByMembership[intentSeqMembershipKey(candidate.SelectedSeqs)]
		if !ok {
			continue
		}
		candidate.Readiness = ai.IntentCandidateReady
		candidate.MissingCompanions = nil
		candidate.DependsOnCandidates = append([]string(nil),
			baselineCandidate.DependsOnCandidates...)
		if strings.TrimSpace(candidate.Subject) == "" {
			candidate.Subject = baselineCandidate.Subject
		}
		changed = true
	}
	return repaired, changed
}

func intentSeqMembershipKey(seqs []int64) string {
	ordered := append([]int64(nil), seqs...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return fmt.Sprint(ordered)
}

func localIntentPlanRepairSignature(plan ai.IntentPlanV2) string {
	raw, err := json.Marshal(plan)
	if err != nil {
		return intentPlanPartitionSignature(plan)
	}
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum)
}

// repairIntentCandidateDependencies adds only dependency declarations already
// proven by hard capture edges. It works on a deep clone and returns success
// only when the complete v2 validator accepts the result, so cycles, unknown
// owners, non-topological plans, and unrelated structural defects leave the
// original plan untouched.
func repairIntentCandidateDependencies(
	req ai.IntentPlanRequestV2,
	plan ai.IntentPlanV2,
) (ai.IntentPlanV2, bool) {
	repaired := cloneIntentPlanV2(plan)
	owner := make(map[int64]string, len(req.OfferedCaptures))
	output := make(map[string]struct{}, len(repaired.Candidates))
	for _, candidate := range repaired.Candidates {
		output[candidate.CandidateID] = struct{}{}
		for _, seq := range candidate.SelectedSeqs {
			owner[seq] = candidate.CandidateID
		}
	}
	for _, candidate := range req.Candidates {
		for _, seq := range candidate.SelectedSeqs {
			if _, exists := owner[seq]; !exists {
				owner[seq] = candidate.CandidateID
			}
		}
	}

	changed := false
	for _, edge := range req.Dependencies {
		if edge.Strength != ai.IntentDependencyHard {
			continue
		}
		fromID, fromOK := owner[edge.FromSeq]
		toID, toOK := owner[edge.ToSeq]
		if !fromOK || !toOK || fromID == toID {
			continue
		}
		if _, toIsOutput := output[toID]; !toIsOutput {
			return plan, false
		}
		for i := range repaired.Candidates {
			candidate := &repaired.Candidates[i]
			if candidate.CandidateID != toID ||
				containsIntentString(candidate.DependsOnCandidates, fromID) {
				continue
			}
			candidate.DependsOnCandidates = append(
				candidate.DependsOnCandidates, fromID)
			changed = true
		}
	}
	if !changed || ai.ValidateIntentPlanV2(req, repaired) != nil {
		return plan, false
	}
	return repaired, true
}

func declareIntentFallbackDependencies(
	req ai.IntentPlanRequestV2,
	plan ai.IntentPlanV2,
) ai.IntentPlanV2 {
	repaired := cloneIntentPlanV2(plan)
	owner := make(map[int64]string, len(req.OfferedCaptures))
	output := make(map[string]int, len(repaired.Candidates))
	for i, candidate := range repaired.Candidates {
		output[candidate.CandidateID] = i
		for _, seq := range candidate.SelectedSeqs {
			owner[seq] = candidate.CandidateID
		}
	}
	for _, candidate := range req.Candidates {
		for _, seq := range candidate.SelectedSeqs {
			if _, exists := owner[seq]; !exists {
				owner[seq] = candidate.CandidateID
			}
		}
	}
	for _, edge := range req.Dependencies {
		if edge.Strength != ai.IntentDependencyHard {
			continue
		}
		fromID, fromOK := owner[edge.FromSeq]
		toID, toOK := owner[edge.ToSeq]
		toIndex, toIsOutput := output[toID]
		if !fromOK || !toOK || fromID == toID || !toIsOutput ||
			containsIntentString(
				repaired.Candidates[toIndex].DependsOnCandidates, fromID) {
			continue
		}
		repaired.Candidates[toIndex].DependsOnCandidates = append(
			repaired.Candidates[toIndex].DependsOnCandidates, fromID)
	}
	return repaired
}

func cloneIntentPlanV2(plan ai.IntentPlanV2) ai.IntentPlanV2 {
	clone := plan
	clone.Candidates = append([]ai.IntentCandidateAssignment(nil), plan.Candidates...)
	for i := range clone.Candidates {
		clone.Candidates[i].SelectedSeqs = append(
			[]int64(nil), plan.Candidates[i].SelectedSeqs...)
		clone.Candidates[i].MissingCompanions = append(
			[]string(nil), plan.Candidates[i].MissingCompanions...)
		clone.Candidates[i].DependsOnCandidates = append(
			[]string(nil), plan.Candidates[i].DependsOnCandidates...)
	}
	return clone
}

func evaluateIntentCandidateAssignment(
	ctx context.Context,
	input IntentCandidateEvaluation,
	plan ai.IntentPlanV2,
	assignment ai.IntentCandidateAssignment,
	dependencies []state.IntentCaptureDependency,
	existing map[string]state.IntentCandidate,
	captures map[int64]IntentCandidateCapture,
) (IntentCandidateDecision, error) {
	var decision IntentCandidateDecision
	decision.Assignment = assignment
	selected := append([]int64(nil), assignment.SelectedSeqs...)
	if prior, ok := existing[assignment.CandidateID]; ok {
		for _, event := range prior.Events {
			if event.EventRole == "coalesced" {
				continue
			}
			if !containsIntentSeq(selected, event.EventSeq) {
				selected = append(selected, event.EventSeq)
			}
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i] < selected[j] })
	candidateCaptures := make([]IntentCandidateCapture, 0, len(selected))
	events := make([]state.IntentCandidateEvent, 0, len(selected))
	for _, seq := range selected {
		capture, ok := captures[seq]
		if !ok {
			return decision, fmt.Errorf("daemon: intent candidate %s missing capture %d",
				assignment.CandidateID, seq)
		}
		if len(events) >= state.IntentCandidateMaxCaptures {
			return decision, fmt.Errorf(
				"daemon: intent candidate %s exceeds capture cap %d",
				assignment.CandidateID, state.IntentCandidateMaxCaptures)
		}
		candidateCaptures = append(candidateCaptures, capture)
		events = append(events, state.IntentCandidateEvent{
			EventSeq: seq, EventRole: intentCaptureRole(capture),
		})
		for _, covered := range capture.CoveredEvents {
			if len(events) >= state.IntentCandidateMaxCaptures {
				return decision, fmt.Errorf(
					"daemon: intent candidate %s exceeds capture cap %d",
					assignment.CandidateID, state.IntentCandidateMaxCaptures)
			}
			events = append(events, state.IntentCandidateEvent{
				EventSeq: covered.Seq, EventRole: "coalesced",
			})
		}
	}

	results := []ai.IntentAtomicityGateResult{
		{Gate: ai.IntentAtomicityCohesion, Status: ai.IntentAtomicityPassed},
		{Gate: ai.IntentAtomicityCompleteness, Status: ai.IntentAtomicityPassed},
		{Gate: ai.IntentAtomicitySeparation, Status: ai.IntentAtomicityPassed},
		{Gate: ai.IntentAtomicityDependency, Status: ai.IntentAtomicityPassed},
		{Gate: ai.IntentAtomicityRevertibility, Status: ai.IntentAtomicityPassed},
	}
	waiting := assignment.Readiness == ai.IntentCandidateWait ||
		len(assignment.MissingCompanions) > 0
	if waiting {
		results[1] = pendingIntentGate(assignment.CandidateID,
			ai.IntentAtomicityCompleteness, "candidate_waiting",
			"candidate is waiting for required companions")
	}
	if err := validateIntentCandidateComponent(selected, dependencies); err != nil &&
		!input.allowSemanticPlan {
		results[0] = failedIntentGate(assignment.CandidateID,
			ai.IntentAtomicityCohesion, "candidate_lacks_semantic_cohesion", err)
		results[2] = failedIntentGate(assignment.CandidateID,
			ai.IntentAtomicitySeparation, "candidate_disconnected", err)
		results[4] = failedIntentGate(assignment.CandidateID,
			ai.IntentAtomicityRevertibility, "unrelated_component", err)
	}
	if waiting {
		results = append(results, pendingIntentGate(assignment.CandidateID,
			ai.IntentAtomicityMaterialization, "candidate_not_sealed",
			"waiting candidate is not eligible for materialization"))
	} else if input.Materialize == nil {
		results = append(results, pendingIntentGate(assignment.CandidateID,
			ai.IntentAtomicityMaterialization, "materializer_unavailable",
			"exact candidate materialization has not run"))
	} else if err := input.Materialize(ctx, candidateCaptures); err != nil {
		results = append(results, failedIntentGate(assignment.CandidateID,
			ai.IntentAtomicityMaterialization, "materialization_failed", err))
	} else {
		results = append(results, ai.IntentAtomicityGateResult{
			Gate: ai.IntentAtomicityMaterialization, Status: ai.IntentAtomicityPassed,
		})
	}

	verificationRequired := input.VerificationMode == "fast" ||
		input.VerificationMode == "full"
	var verificationResult IntentCandidateVerification
	if !verificationRequired {
		results = append(results, ai.IntentAtomicityGateResult{
			Gate:   ai.IntentAtomicityVerification,
			Status: ai.IntentAtomicityNotRequired,
		})
	} else if waiting {
		results = append(results, pendingIntentGate(assignment.CandidateID,
			ai.IntentAtomicityVerification, "candidate_not_sealed",
			"waiting candidate is not eligible for verification"))
	} else if !intentPreVerificationGatesPassed(results) {
		results = append(results, pendingIntentGate(assignment.CandidateID,
			ai.IntentAtomicityVerification, "candidate_not_atomic",
			"candidate must pass structural and materialization gates before verification"))
	} else if input.Verify == nil {
		results = append(results, pendingIntentGate(assignment.CandidateID,
			ai.IntentAtomicityVerification, "verification_unavailable",
			"approved candidate verification has not run"))
	} else {
		var verifyErr error
		verificationResult, verifyErr = input.Verify(
			ctx, assignment, candidateCaptures)
		if verifyErr != nil {
			results = append(results, failedIntentGate(assignment.CandidateID,
				ai.IntentAtomicityVerification, "verification_failed", verifyErr))
		} else {
			results = append(results, ai.IntentAtomicityGateResult{
				Gate: ai.IntentAtomicityVerification, Status: ai.IntentAtomicityPassed,
			})
		}
	}
	report := ai.NewIntentAtomicityReport(assignment.CandidateID, results...)
	if err := ai.ValidateIntentAtomicityReport(report); err != nil {
		return decision, err
	}

	status := state.IntentCandidateReady
	readiness := state.IntentReadinessReady
	if !report.Valid {
		status = state.IntentCandidateWaiting
		readiness = state.IntentReadinessWait
		for _, gate := range report.Gates {
			if gate.Status == ai.IntentAtomicityFailed &&
				(gate.Gate == ai.IntentAtomicityCohesion ||
					gate.Gate == ai.IntentAtomicityCompleteness ||
					gate.Gate == ai.IntentAtomicitySeparation ||
					gate.Gate == ai.IntentAtomicityDependency ||
					gate.Gate == ai.IntentAtomicityRevertibility) {
				status = state.IntentCandidateBlocked
				break
			}
		}
	}
	nowSeconds := float64(input.Now.UnixNano()) / 1e9
	created := nowSeconds
	if prior, ok := existing[assignment.CandidateID]; ok {
		created = prior.CreatedTS
	}
	atomicitySummary := "all required atomicity gates passed"
	if report.Valid && plan.ProtocolVersion == ai.IntentPlannerProtocolV1Compat {
		atomicitySummary = "v1 compatibility plan passed safety gates; native v2 semantic readiness quality is unavailable"
	}
	if !report.Valid {
		var findings []ai.IntentAtomicityFinding
		for _, gate := range report.Gates {
			if gate.Finding != nil {
				findings = append(findings, *gate.Finding)
			}
		}
		atomicitySummary = ai.BuildIntentAtomicityCorrection(findings)
	}
	candidate := state.IntentCandidate{
		ID: assignment.CandidateID, BranchRef: input.BranchRef,
		BranchGeneration: input.BranchGeneration, Status: status,
		Purpose: assignment.Purpose, CreatedTS: created, UpdatedTS: nowSeconds,
		Readiness:         readiness,
		MissingCompanions: strings.Join(assignment.MissingCompanions, "\n"),
		AtomicityStatus: sql.NullString{
			String: string(intentAtomicityReportStatus(report)), Valid: true,
		},
		AtomicitySummary:   atomicitySummary,
		AtomicityCheckedTS: sql.NullFloat64{Float64: nowSeconds, Valid: true},
		Provider:           sql.NullString{String: input.Provider, Valid: input.Provider != ""},
		Model:              sql.NullString{String: input.Model, Valid: input.Model != ""},
		PlannerProtocol:    sql.NullString{String: plan.ProtocolVersion, Valid: true},
		ConfigRevisionID:   input.ConfigRevisionID,
		ConfigProfile:      sql.NullString{String: input.ConfigProfile, Valid: input.ConfigProfile != ""},
		PresetID:           sql.NullString{String: input.PresetID, Valid: true},
		PresetVersion:      sql.NullInt64{Int64: int64(input.PresetVersion), Valid: true},
		Events:             events,
	}
	verificationStatus := verificationResult.Status
	if verificationStatus == "" {
		verificationStatus = string(intentAtomicityGateStatus(
			report, ai.IntentAtomicityVerification))
	}
	candidate.VerificationStatus = sql.NullString{
		String: verificationStatus, Valid: verificationStatus != "",
	}
	candidate.VerificationOutput = verificationResult.Output
	if verificationResult.CheckedTS > 0 {
		candidate.VerificationTS = sql.NullFloat64{
			Float64: verificationResult.CheckedTS, Valid: true,
		}
	}
	if prior, ok := existing[assignment.CandidateID]; ok {
		candidate.PublishedCommitOID = prior.PublishedCommitOID
		candidate.SoftPublicationDeadline = prior.SoftPublicationDeadline
	}
	if report.Valid {
		candidate.ReadyTS = sql.NullFloat64{Float64: nowSeconds, Valid: true}
	}
	decision.Candidate = candidate
	decision.Atomicity = report
	decision.Publishable = report.Valid && assignment.Readiness == ai.IntentCandidateReady
	return decision, nil
}

func intentPreVerificationGatesPassed(
	results []ai.IntentAtomicityGateResult,
) bool {
	for _, result := range results {
		if result.Gate == ai.IntentAtomicityVerification {
			continue
		}
		if result.Status != ai.IntentAtomicityPassed &&
			result.Status != ai.IntentAtomicityNotRequired {
			return false
		}
	}
	return true
}

func deterministicIntentCandidatePlan(
	req ai.IntentPlanRequestV2,
	includeSemantic bool,
	onlySmallestReady bool,
) ai.IntentPlanV2 {
	components := intentDependencyComponents(req, includeSemantic)
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for i, seqs := range components {
		subject, purpose := deterministicIntentCandidateMessage(req, seqs)
		readiness := ai.IntentCandidateReady
		var missing []string
		if onlySmallestReady && i > 0 {
			readiness = ai.IntentCandidateWait
			subject = ""
			missing = []string{"selected deterministic component is published first"}
		}
		plan.Candidates = append(plan.Candidates, ai.IntentCandidateAssignment{
			CandidateID:  stableGeneratedCandidateID(req, seqs),
			SelectedSeqs: seqs, Purpose: purpose, Readiness: readiness,
			MissingCompanions: missing, Subject: subject,
			GroupingReason: "deterministic dependency component",
		})
	}
	addIntentCandidateDependencies(req, &plan)
	return plan
}

// balancedIntentCandidatePlan keeps hard dependency components intact and only
// joins them through one-to-one companion evidence. Weak similarity signals
// cannot turn a planner outage into a broad semantic guess.
func balancedIntentCandidatePlan(
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, bool) {
	hardComponents := intentDependencyComponents(req, false)
	componentBySeq := make(map[int64]int, len(req.OfferedCaptures))
	for component, seqs := range hardComponents {
		for _, seq := range seqs {
			componentBySeq[seq] = component
		}
	}
	parent := make([]int, len(hardComponents))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(component int) int {
		if parent[component] != component {
			parent[component] = find(parent[component])
		}
		return parent[component]
	}
	union := func(left, right int) {
		leftRoot, rightRoot := find(left), find(right)
		if leftRoot != rightRoot {
			parent[rightRoot] = leftRoot
		}
	}

	type companionPair struct{ left, right int }
	var pairs []companionPair
	neighbors := make([]map[int]struct{}, len(hardComponents))
	for _, edge := range req.Dependencies {
		if edge.Strength != ai.IntentDependencySoft ||
			!balancedIntentCompanionDependency(edge.Kind) {
			continue
		}
		left, leftOK := componentBySeq[edge.FromSeq]
		right, rightOK := componentBySeq[edge.ToSeq]
		if !leftOK || !rightOK || left == right {
			continue
		}
		if neighbors[left] == nil {
			neighbors[left] = make(map[int]struct{})
		}
		if neighbors[right] == nil {
			neighbors[right] = make(map[int]struct{})
		}
		neighbors[left][right] = struct{}{}
		neighbors[right][left] = struct{}{}
		pairs = append(pairs, companionPair{left: left, right: right})
	}
	ambiguous := make([]bool, len(hardComponents))
	for _, pair := range pairs {
		if len(neighbors[pair.left]) == 1 && len(neighbors[pair.right]) == 1 {
			union(pair.left, pair.right)
			continue
		}
		ambiguous[pair.left] = true
		ambiguous[pair.right] = true
	}

	seqsByRoot := make(map[int][]int64)
	ambiguousByRoot := make(map[int]bool)
	for component, seqs := range hardComponents {
		root := find(component)
		seqsByRoot[root] = append(seqsByRoot[root], seqs...)
		if ambiguous[component] {
			ambiguousByRoot[root] = true
		}
	}
	groups := make([][]int64, 0, len(seqsByRoot))
	for _, seqs := range seqsByRoot {
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		groups = append(groups, seqs)
	}
	sort.Slice(groups, func(i, j int) bool {
		if len(groups[i]) != len(groups[j]) {
			return len(groups[i]) < len(groups[j])
		}
		return groups[i][0] < groups[j][0]
	})

	needsAttention := false
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for _, seqs := range groups {
		subject, purpose := deterministicIntentCandidateMessage(req, seqs)
		readiness := ai.IntentCandidateReady
		var missing []string
		groupingReason := "bounded deterministic dependency component"
		root := find(componentBySeq[seqs[0]])
		if ambiguousByRoot[root] {
			readiness = ai.IntentCandidateWait
			subject = ""
			missing = []string{
				"ambiguous companion evidence requires planner review",
			}
			groupingReason = "ambiguous companion evidence is retained for planner review"
			needsAttention = true
		}
		plan.Candidates = append(plan.Candidates, ai.IntentCandidateAssignment{
			CandidateID:  stableGeneratedCandidateID(req, seqs),
			SelectedSeqs: seqs, Purpose: purpose, Readiness: readiness,
			MissingCompanions: missing, Subject: subject,
			GroupingReason: groupingReason,
		})
	}
	addIntentCandidateDependencies(req, &plan)
	return plan, needsAttention
}

func balancedIntentCompanionDependency(kind string) bool {
	switch kind {
	case "test_source", "migration_test":
		return true
	default:
		return false
	}
}

func holdIntentCandidatePlan(req ai.IntentPlanRequestV2) ai.IntentPlanV2 {
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for _, capture := range req.OfferedCaptures {
		plan.Candidates = append(plan.Candidates, ai.IntentCandidateAssignment{
			CandidateID:       stableGeneratedCandidateID(req, []int64{capture.Seq}),
			SelectedSeqs:      []int64{capture.Seq},
			Purpose:           "retain capture until quality planning is available",
			Readiness:         ai.IntentCandidateWait,
			MissingCompanions: []string{"quality planner is unavailable"},
			GroupingReason:    "quality preset forbids planner-failure publication",
		})
	}
	return plan
}

func normalizeIntentFallbackBoundaries(
	req ai.IntentPlanRequestV2,
) ai.IntentPlanRequestV2 {
	normalized := req
	normalized.Dependencies = append(
		[]ai.IntentCaptureDependency(nil), req.Dependencies...)
	protected := make(map[int64]struct{})
	for _, candidate := range req.Candidates {
		if candidate.Status != state.IntentCandidateSoftPublished &&
			candidate.Status != state.IntentCandidatePublished {
			continue
		}
		for _, seq := range candidate.SelectedSeqs {
			protected[seq] = struct{}{}
		}
	}
	offered := make(map[int64]struct{}, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		offered[capture.Seq] = struct{}{}
	}
	for i := range normalized.Dependencies {
		edge := &normalized.Dependencies[i]
		if edge.Strength != ai.IntentDependencyHard {
			continue
		}
		_, fromOffered := offered[edge.FromSeq]
		_, toProtected := protected[edge.ToSeq]
		if fromOffered && toProtected {
			edge.FromSeq, edge.ToSeq = edge.ToSeq, edge.FromSeq
		}
	}
	return normalized
}

// continuePersistedIntentCandidates computes the full hard closure across
// offered assignments and durable candidates. When a bridge joins multiple
// candidates it chooses one deterministic survivor and asks the state layer to
// merge every other lineage transactionally after the atomicity gates run.
func continuePersistedIntentCandidates(
	req ai.IntentPlanRequestV2,
	plan *ai.IntentPlanV2,
	options intentCandidateContinuationOptions,
) ([]intentCandidateContinuation, bool, error) {
	parent := make(map[int64]int64)
	add := func(seq int64) {
		if _, ok := parent[seq]; !ok {
			parent[seq] = seq
		}
	}
	var find func(int64) int64
	find = func(seq int64) int64 {
		if parent[seq] != seq {
			parent[seq] = find(parent[seq])
		}
		return parent[seq]
	}
	union := func(left, right int64) {
		leftRoot, rightRoot := find(left), find(right)
		if leftRoot != rightRoot {
			parent[rightRoot] = leftRoot
		}
	}

	outputOwner := make(map[int64]int, len(req.OfferedCaptures))
	for i, candidate := range plan.Candidates {
		var previous int64
		for _, seq := range candidate.SelectedSeqs {
			add(seq)
			outputOwner[seq] = i
			if previous > 0 {
				union(previous, seq)
			}
			previous = seq
		}
	}
	persistedOwner := make(map[int64]string)
	protectedCandidate := make(map[string]struct{})
	for _, candidate := range req.Candidates {
		if candidate.Status == state.IntentCandidateSoftPublished ||
			candidate.Status == state.IntentCandidatePublished {
			protectedCandidate[candidate.CandidateID] = struct{}{}
		}
		var previous int64
		for _, seq := range candidate.SelectedSeqs {
			add(seq)
			persistedOwner[seq] = candidate.CandidateID
			if previous > 0 {
				union(previous, seq)
			}
			previous = seq
		}
	}
	for _, edge := range req.Dependencies {
		if edge.Strength != ai.IntentDependencyHard {
			continue
		}
		if _, leftOK := parent[edge.FromSeq]; !leftOK {
			continue
		}
		if _, rightOK := parent[edge.ToSeq]; !rightOK {
			continue
		}
		if options.PreservePersistedBoundaries {
			fromID := persistedOwner[edge.FromSeq]
			_, toIsOutput := outputOwner[edge.ToSeq]
			_, protected := protectedCandidate[fromID]
			if protected && toIsOutput {
				continue
			}
		}
		union(edge.FromSeq, edge.ToSeq)
	}

	companionNeedsAttention := false
	var companionOutputs map[int]struct{}
	if options.IncludePersistedCompanions {
		companionPersistedOwner := persistedOwner
		if options.PreservePersistedBoundaries {
			companionPersistedOwner = make(map[int64]string, len(persistedOwner))
			for seq, candidateID := range persistedOwner {
				if _, protected := protectedCandidate[candidateID]; !protected {
					companionPersistedOwner[seq] = candidateID
				}
			}
		}
		companionOutputs, companionNeedsAttention =
			connectBalancedPersistedCompanions(
				req, plan, parent, find, union, outputOwner,
				companionPersistedOwner)
	}

	type hardClosure struct {
		outputs   map[int]struct{}
		persisted map[string]struct{}
	}
	closures := make(map[int64]*hardClosure)
	for seq := range parent {
		root := find(seq)
		closure := closures[root]
		if closure == nil {
			closure = &hardClosure{
				outputs:   make(map[int]struct{}),
				persisted: make(map[string]struct{}),
			}
			closures[root] = closure
		}
		if output, ok := outputOwner[seq]; ok {
			closure.outputs[output] = struct{}{}
		}
		if candidateID := persistedOwner[seq]; candidateID != "" {
			closure.persisted[candidateID] = struct{}{}
		}
	}
	firstPersistedSeq := make(map[string]int64, len(req.Candidates))
	for _, candidate := range req.Candidates {
		for _, seq := range candidate.SelectedSeqs {
			if current := firstPersistedSeq[candidate.CandidateID]; current == 0 || seq < current {
				firstPersistedSeq[candidate.CandidateID] = seq
			}
		}
	}
	canonicalID := func(ids map[string]struct{}) string {
		sortedIDs := make([]string, 0, len(ids))
		for candidateID := range ids {
			sortedIDs = append(sortedIDs, candidateID)
		}
		sort.Slice(sortedIDs, func(i, j int) bool {
			leftSeq, rightSeq := firstPersistedSeq[sortedIDs[i]],
				firstPersistedSeq[sortedIDs[j]]
			if leftSeq != rightSeq {
				if leftSeq == 0 {
					return false
				}
				if rightSeq == 0 {
					return true
				}
				return leftSeq < rightSeq
			}
			return sortedIDs[i] < sortedIDs[j]
		})
		if len(sortedIDs) == 0 {
			return ""
		}
		return sortedIDs[0]
	}

	continuedByOutput := make(map[int]string)
	continuationSources := make(map[string]map[string]struct{})
	for _, closure := range closures {
		if len(closure.outputs) == 0 || len(closure.persisted) == 0 {
			continue
		}
		targetID := canonicalID(closure.persisted)
		for output := range closure.outputs {
			continuedByOutput[output] = targetID
		}
		for sourceID := range closure.persisted {
			if sourceID == targetID {
				continue
			}
			if continuationSources[targetID] == nil {
				continuationSources[targetID] = make(map[string]struct{})
			}
			continuationSources[targetID][sourceID] = struct{}{}
		}
	}
	remappedIDs := make(map[string]string, len(continuedByOutput))
	for output, candidateID := range continuedByOutput {
		remappedIDs[plan.Candidates[output].CandidateID] = candidateID
		plan.Candidates[output].CandidateID = candidateID
		if _, ok := companionOutputs[output]; ok {
			plan.Candidates[output].GroupingReason =
				"bounded deterministic persisted companion continuation"
		}
	}
	mergedByID := make(map[string]int, len(plan.Candidates))
	mergedReady := make(map[string]bool, len(plan.Candidates))
	merged := make([]ai.IntentCandidateAssignment, 0, len(plan.Candidates))
	for _, candidate := range plan.Candidates {
		mergedReady[candidate.CandidateID] =
			mergedReady[candidate.CandidateID] ||
				candidate.Readiness == ai.IntentCandidateReady
		if existingIndex, ok := mergedByID[candidate.CandidateID]; ok {
			target := &merged[existingIndex]
			for _, seq := range candidate.SelectedSeqs {
				if !containsIntentSeq(target.SelectedSeqs, seq) {
					target.SelectedSeqs = append(target.SelectedSeqs, seq)
				}
			}
			for _, missing := range candidate.MissingCompanions {
				if !containsIntentString(target.MissingCompanions, missing) {
					target.MissingCompanions = append(
						target.MissingCompanions, missing)
				}
			}
			target.DependsOnCandidates = append(
				target.DependsOnCandidates, candidate.DependsOnCandidates...)
			continue
		}
		mergedByID[candidate.CandidateID] = len(merged)
		merged = append(merged, candidate)
	}
	for i := range merged {
		candidate := &merged[i]
		sort.Slice(candidate.SelectedSeqs, func(i, j int) bool {
			return candidate.SelectedSeqs[i] < candidate.SelectedSeqs[j]
		})
		dependencies := candidate.DependsOnCandidates[:0]
		for _, dependencyID := range candidate.DependsOnCandidates {
			if replacement := remappedIDs[dependencyID]; replacement != "" {
				dependencyID = replacement
			}
			if dependencyID == candidate.CandidateID ||
				containsIntentString(dependencies, dependencyID) {
				continue
			}
			dependencies = append(dependencies, dependencyID)
		}
		candidate.DependsOnCandidates = dependencies
		if mergedReady[candidate.CandidateID] {
			missing := candidate.MissingCompanions[:0]
			for _, companion := range candidate.MissingCompanions {
				if companion != "selected deterministic component is published first" {
					missing = append(missing, companion)
				}
			}
			candidate.MissingCompanions = missing
			if len(missing) == 0 {
				candidate.Readiness = ai.IntentCandidateReady
			} else {
				candidate.Readiness = ai.IntentCandidateWait
			}
		}
		if options.RewriteDeterministicMessage {
			subject, purpose := deterministicIntentCandidateMessage(
				req, candidate.SelectedSeqs)
			candidate.Purpose = purpose
			if candidate.Readiness == ai.IntentCandidateReady {
				candidate.Subject = subject
				candidate.Body = ""
			}
		}
	}
	plan.Candidates = merged
	continuations := make([]intentCandidateContinuation, 0,
		len(continuationSources))
	for targetID, sourceSet := range continuationSources {
		sourceIDs := make([]string, 0, len(sourceSet))
		for sourceID := range sourceSet {
			sourceIDs = append(sourceIDs, sourceID)
		}
		sort.Strings(sourceIDs)
		continuations = append(continuations, intentCandidateContinuation{
			TargetID: targetID, SourceIDs: sourceIDs,
		})
	}
	sort.Slice(continuations, func(i, j int) bool {
		return continuations[i].TargetID < continuations[j].TargetID
	})
	return continuations, companionNeedsAttention, nil
}

func connectBalancedPersistedCompanions(
	req ai.IntentPlanRequestV2,
	plan *ai.IntentPlanV2,
	parent map[int64]int64,
	find func(int64) int64,
	union func(int64, int64),
	outputOwner map[int64]int,
	persistedOwner map[int64]string,
) (map[int]struct{}, bool) {
	outputsByRoot := make(map[int64]map[int]struct{})
	persistedByRoot := make(map[int64]map[string]struct{})
	for seq, output := range outputOwner {
		root := find(seq)
		if outputsByRoot[root] == nil {
			outputsByRoot[root] = make(map[int]struct{})
		}
		outputsByRoot[root][output] = struct{}{}
	}
	for seq, candidateID := range persistedOwner {
		root := find(seq)
		if persistedByRoot[root] == nil {
			persistedByRoot[root] = make(map[string]struct{})
		}
		persistedByRoot[root][candidateID] = struct{}{}
	}

	outputNeighbors := make(map[int64]map[int64]struct{})
	persistedNeighbors := make(map[int64]map[int64]struct{})
	addCompanion := func(outputRoot, persistedRoot int64) {
		if outputNeighbors[outputRoot] == nil {
			outputNeighbors[outputRoot] = make(map[int64]struct{})
		}
		if persistedNeighbors[persistedRoot] == nil {
			persistedNeighbors[persistedRoot] = make(map[int64]struct{})
		}
		outputNeighbors[outputRoot][persistedRoot] = struct{}{}
		persistedNeighbors[persistedRoot][outputRoot] = struct{}{}
	}
	for _, edge := range req.Dependencies {
		if edge.Strength != ai.IntentDependencySoft ||
			!balancedIntentCompanionDependency(edge.Kind) {
			continue
		}
		if _, leftOK := parent[edge.FromSeq]; !leftOK {
			continue
		}
		if _, rightOK := parent[edge.ToSeq]; !rightOK {
			continue
		}
		leftRoot, rightRoot := find(edge.FromSeq), find(edge.ToSeq)
		if leftRoot == rightRoot {
			continue
		}
		leftOutputs, leftPersisted :=
			len(outputsByRoot[leftRoot]), len(persistedByRoot[leftRoot])
		rightOutputs, rightPersisted :=
			len(outputsByRoot[rightRoot]), len(persistedByRoot[rightRoot])
		switch {
		case leftOutputs > 0 && leftPersisted == 0 &&
			rightPersisted > 0 && rightOutputs == 0:
			addCompanion(leftRoot, rightRoot)
		case rightOutputs > 0 && rightPersisted == 0 &&
			leftPersisted > 0 && leftOutputs == 0:
			addCompanion(rightRoot, leftRoot)
		}
	}

	companionOutputs := make(map[int]struct{})
	needsAttention := false
	for outputRoot, neighbors := range outputNeighbors {
		safe := false
		var persistedRoot int64
		if len(neighbors) == 1 {
			for persistedRoot = range neighbors {
			}
			safe = len(persistedNeighbors[persistedRoot]) == 1
		}
		if safe {
			for output := range outputsByRoot[outputRoot] {
				companionOutputs[output] = struct{}{}
			}
			union(outputRoot, persistedRoot)
			continue
		}
		needsAttention = true
		for output := range outputsByRoot[outputRoot] {
			holdBalancedFallbackAssignment(
				&plan.Candidates[output],
				"ambiguous persisted companion evidence requires planner review",
			)
		}
	}
	return companionOutputs, needsAttention
}

func applyIntentCandidateContinuationLimits(
	plan *ai.IntentPlanV2,
	continuations []intentCandidateContinuation,
	existing map[string]state.IntentCandidate,
	captures map[int64]IntentCandidateCapture,
) bool {
	needsAttention := false
	for i := range continuations {
		continuation := &continuations[i]
		members := make(map[int64]struct{})
		addExisting := func(candidateID string) {
			for _, event := range existing[candidateID].Events {
				members[event.EventSeq] = struct{}{}
			}
		}
		addExisting(continuation.TargetID)
		for _, sourceID := range continuation.SourceIDs {
			addExisting(sourceID)
		}
		for _, assignment := range plan.Candidates {
			if assignment.CandidateID != continuation.TargetID {
				continue
			}
			for _, seq := range assignment.SelectedSeqs {
				members[seq] = struct{}{}
				if capture, ok := captures[seq]; ok {
					for _, covered := range capture.CoveredEvents {
						members[covered.Seq] = struct{}{}
					}
				}
			}
		}
		if len(members) <= state.IntentCandidateMaxCaptures {
			continue
		}
		continuation.HoldReason = fmt.Sprintf(
			"hard dependency closure exceeds the durable %d-capture candidate limit",
			state.IntentCandidateMaxCaptures)
		for candidateIndex := range plan.Candidates {
			assignment := &plan.Candidates[candidateIndex]
			if assignment.CandidateID != continuation.TargetID {
				continue
			}
			assignment.Readiness = ai.IntentCandidateWait
			assignment.Subject = ""
			assignment.Body = ""
			if !containsIntentString(
				assignment.MissingCompanions, continuation.HoldReason) {
				assignment.MissingCompanions = append(
					assignment.MissingCompanions,
					continuation.HoldReason)
			}
			assignment.GroupingReason =
				"hard dependency closure is retained for bounded review"
		}
		needsAttention = true
	}
	return needsAttention
}

func heldIntentCandidateContinuationDecision(
	assignment ai.IntentCandidateAssignment,
	continuation intentCandidateContinuation,
	existing map[string]state.IntentCandidate,
) (IntentCandidateDecision, error) {
	candidate, ok := existing[continuation.TargetID]
	if !ok {
		return IntentCandidateDecision{}, fmt.Errorf(
			"daemon: intent candidates: held continuation target %q is missing",
			continuation.TargetID)
	}
	report := ai.NewIntentAtomicityReport(
		assignment.CandidateID,
		pendingIntentGate(
			assignment.CandidateID,
			ai.IntentAtomicityCompleteness,
			"candidate_capture_cap_exceeded",
			continuation.HoldReason),
		pendingIntentGate(
			assignment.CandidateID,
			ai.IntentAtomicityDependency,
			"hard_dependency_closure_held",
			"the complete hard dependency closure must remain unpublished"),
	)
	if err := ai.ValidateIntentAtomicityReport(report); err != nil {
		return IntentCandidateDecision{}, err
	}
	return IntentCandidateDecision{
		Candidate: candidate, Assignment: assignment,
		Atomicity: report, Publishable: false,
	}, nil
}

func mergeIntentCandidateContinuationViews(
	existing map[string]state.IntentCandidate,
	continuations []intentCandidateContinuation,
) (map[string]state.IntentCandidate, error) {
	out := make(map[string]state.IntentCandidate, len(existing))
	for candidateID, candidate := range existing {
		candidate.Events = append([]state.IntentCandidateEvent(nil),
			candidate.Events...)
		out[candidateID] = candidate
	}
	for _, continuation := range continuations {
		if continuation.HoldReason != "" {
			continue
		}
		target, ok := out[continuation.TargetID]
		if !ok {
			return nil, fmt.Errorf(
				"daemon: intent candidates: continuation target %q is missing",
				continuation.TargetID)
		}
		eventsBySeq := make(map[int64]state.IntentCandidateEvent,
			len(target.Events))
		for _, event := range target.Events {
			eventsBySeq[event.EventSeq] = event
		}
		for _, sourceID := range continuation.SourceIDs {
			source, sourceOK := out[sourceID]
			if !sourceOK {
				return nil, fmt.Errorf(
					"daemon: intent candidates: continuation source %q is missing",
					sourceID)
			}
			if source.BranchRef != target.BranchRef ||
				source.BranchGeneration != target.BranchGeneration {
				return nil, fmt.Errorf(
					"daemon: intent candidates: continuation source %q changed branch pair",
					sourceID)
			}
			if target.CreatedTS == 0 ||
				(source.CreatedTS > 0 && source.CreatedTS < target.CreatedTS) {
				target.CreatedTS = source.CreatedTS
			}
			for _, event := range source.Events {
				eventsBySeq[event.EventSeq] = event
			}
		}
		seqs := make([]int64, 0, len(eventsBySeq))
		for seq := range eventsBySeq {
			seqs = append(seqs, seq)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		target.Events = make([]state.IntentCandidateEvent, 0, len(seqs))
		for _, seq := range seqs {
			event := eventsBySeq[seq]
			event.CandidateID = target.ID
			target.Events = append(target.Events, event)
		}
		out[target.ID] = target
	}
	return out, nil
}

func intentCandidateContinuationValidationRequest(
	req ai.IntentPlanRequestV2,
	continuations []intentCandidateContinuation,
) ai.IntentPlanRequestV2 {
	if len(continuations) == 0 {
		return req
	}
	originalCandidates := append([]ai.IntentCandidateSummary(nil),
		req.Candidates...)
	targetBySource := make(map[string]string)
	for _, continuation := range continuations {
		for _, sourceID := range continuation.SourceIDs {
			targetBySource[sourceID] = continuation.TargetID
		}
	}
	originalByID := make(map[string]ai.IntentCandidateSummary,
		len(originalCandidates))
	for _, candidate := range originalCandidates {
		originalByID[candidate.CandidateID] = candidate
	}
	byID := make(map[string]ai.IntentCandidateSummary, len(req.Candidates))
	order := make([]string, 0, len(req.Candidates))
	for _, candidate := range req.Candidates {
		targetID := candidate.CandidateID
		if replacement := targetBySource[targetID]; replacement != "" {
			targetID = replacement
		}
		target, exists := byID[targetID]
		if !exists {
			target = originalByID[targetID]
			target.CandidateID = targetID
			target.SelectedSeqs = nil
			order = append(order, targetID)
		}
		for _, seq := range candidate.SelectedSeqs {
			if !containsIntentSeq(target.SelectedSeqs, seq) {
				target.SelectedSeqs = append(target.SelectedSeqs, seq)
			}
		}
		byID[targetID] = target
	}
	req.Candidates = make([]ai.IntentCandidateSummary, 0, len(byID))
	seen := make(map[string]struct{}, len(order))
	for _, candidateID := range order {
		if _, duplicate := seen[candidateID]; duplicate {
			continue
		}
		seen[candidateID] = struct{}{}
		candidate := byID[candidateID]
		sort.Slice(candidate.SelectedSeqs, func(i, j int) bool {
			return candidate.SelectedSeqs[i] < candidate.SelectedSeqs[j]
		})
		req.Candidates = append(req.Candidates, candidate)
	}
	seenEdges := make(map[string]struct{}, len(req.Dependencies))
	for _, edge := range req.Dependencies {
		seenEdges[fmt.Sprintf("%d\x00%d\x00%s\x00%s",
			edge.FromSeq, edge.ToSeq, edge.Strength, edge.Kind)] = struct{}{}
	}
	for _, candidate := range originalCandidates {
		seqs := append([]int64(nil), candidate.SelectedSeqs...)
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		for i := 1; i < len(seqs); i++ {
			edge := ai.IntentCaptureDependency{
				FromSeq: seqs[i-1], ToSeq: seqs[i],
				Strength: ai.IntentDependencySoft,
				Kind:     "persisted_candidate_membership",
			}
			key := fmt.Sprintf("%d\x00%d\x00%s\x00%s",
				edge.FromSeq, edge.ToSeq, edge.Strength, edge.Kind)
			if _, exists := seenEdges[key]; exists {
				continue
			}
			seenEdges[key] = struct{}{}
			req.Dependencies = append(req.Dependencies, edge)
		}
	}
	return req
}

func appendIntentCandidateMembershipDependencies(
	dependencies []state.IntentCaptureDependency,
	existing []state.IntentCandidate,
) []state.IntentCaptureDependency {
	out := append([]state.IntentCaptureDependency(nil), dependencies...)
	seen := make(map[string]struct{}, len(out))
	for _, edge := range out {
		seen[fmt.Sprintf("%d\x00%d\x00%s\x00%s",
			edge.PrerequisiteSeq, edge.DependentSeq,
			edge.Strength, edge.Kind)] = struct{}{}
	}
	for _, candidate := range existing {
		var seqs []int64
		for _, event := range candidate.Events {
			if event.EventRole != "coalesced" {
				seqs = append(seqs, event.EventSeq)
			}
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		for i := 1; i < len(seqs); i++ {
			edge := state.IntentCaptureDependency{
				BranchRef:        candidate.BranchRef,
				BranchGeneration: candidate.BranchGeneration,
				PrerequisiteSeq:  seqs[i-1],
				DependentSeq:     seqs[i],
				Strength:         state.IntentDependencySoft,
				Kind:             "persisted_candidate_membership",
			}
			key := fmt.Sprintf("%d\x00%d\x00%s\x00%s",
				edge.PrerequisiteSeq, edge.DependentSeq,
				edge.Strength, edge.Kind)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, edge)
		}
	}
	return out
}

func applyBalancedFallbackBounds(
	plan *ai.IntentPlanV2,
	existing map[string]state.IntentCandidate,
	captures map[int64]IntentCandidateCapture,
) bool {
	needsAttention := false
	for i := range plan.Candidates {
		assignment := &plan.Candidates[i]
		selected := make(map[int64]struct{}, len(assignment.SelectedSeqs))
		paths := make(map[string]struct{})
		addCapture := func(capture IntentCandidateCapture) {
			selected[capture.Event.Seq] = struct{}{}
			for _, covered := range capture.CoveredEvents {
				selected[covered.Seq] = struct{}{}
			}
			for _, capturePath := range intentCapturePaths(capture) {
				if capturePath != "" {
					paths[capturePath] = struct{}{}
				}
			}
		}
		for _, seq := range assignment.SelectedSeqs {
			if capture, ok := captures[seq]; ok {
				addCapture(capture)
			} else {
				selected[seq] = struct{}{}
			}
		}
		if prior, ok := existing[assignment.CandidateID]; ok {
			for _, event := range prior.Events {
				selected[event.EventSeq] = struct{}{}
				if event.EventRole == "coalesced" {
					continue
				}
				if capture, captureOK := captures[event.EventSeq]; captureOK {
					addCapture(capture)
				}
			}
		}
		if len(selected) > intentBalancedFallbackCaptureCap {
			holdBalancedFallbackAssignment(assignment,
				fmt.Sprintf("balanced fallback exceeds %d captures",
					intentBalancedFallbackCaptureCap))
			needsAttention = true
		}
		if len(paths) > intentBalancedFallbackPathCap {
			holdBalancedFallbackAssignment(assignment,
				fmt.Sprintf("balanced fallback exceeds %d paths",
					intentBalancedFallbackPathCap))
			needsAttention = true
		}
	}
	return needsAttention
}

func holdBalancedFallbackAssignment(
	assignment *ai.IntentCandidateAssignment,
	reason string,
) {
	assignment.Readiness = ai.IntentCandidateWait
	assignment.Subject = ""
	assignment.Body = ""
	if !containsIntentString(assignment.MissingCompanions, reason) {
		assignment.MissingCompanions = append(
			assignment.MissingCompanions, reason)
	}
	assignment.GroupingReason =
		"bounded fallback requires planner review"
}

func reuseIntentCandidatePartition(
	req ai.IntentPlanRequestV2,
	existing []state.IntentCandidate,
) (ai.IntentPlanV2, bool) {
	offered := make(map[int64]struct{}, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		offered[capture.Seq] = struct{}{}
	}
	assigned := make(map[int64]struct{}, len(offered))
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for _, candidate := range existing {
		if candidate.AtomicityStatus.String != string(ai.IntentAtomicityPassed) ||
			candidate.Readiness != state.IntentReadinessReady {
			continue
		}
		var selected []int64
		for _, event := range candidate.Events {
			if _, ok := offered[event.EventSeq]; ok {
				selected = append(selected, event.EventSeq)
				assigned[event.EventSeq] = struct{}{}
			}
		}
		if len(selected) == 0 {
			continue
		}
		subject, _ := deterministicIntentCandidateMessage(req, selected)
		plan.Candidates = append(plan.Candidates, ai.IntentCandidateAssignment{
			CandidateID: candidate.ID, SelectedSeqs: selected,
			Purpose: candidate.Purpose, Readiness: ai.IntentCandidateReady,
			Subject: subject, GroupingReason: "reuse last valid candidate partition",
		})
	}
	if len(assigned) != len(offered) {
		return ai.IntentPlanV2{}, false
	}
	sort.Slice(plan.Candidates, func(i, j int) bool {
		return plan.Candidates[i].SelectedSeqs[0] < plan.Candidates[j].SelectedSeqs[0]
	})
	addIntentCandidateDependencies(req, &plan)
	if err := ai.ValidateIntentPlanV2(req, plan); err != nil {
		return ai.IntentPlanV2{}, false
	}
	return plan, true
}

func intentDependencyComponents(req ai.IntentPlanRequestV2, includeSemantic bool) [][]int64 {
	parent := make(map[int64]int64, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		parent[capture.Seq] = capture.Seq
	}
	var find func(int64) int64
	find = func(seq int64) int64 {
		if parent[seq] != seq {
			parent[seq] = find(parent[seq])
		}
		return parent[seq]
	}
	union := func(a, b int64) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[rb] = ra
		}
	}
	type pair struct{ left, right int64 }
	pairKinds := make(map[pair]map[string]struct{})
	if includeSemantic {
		for _, edge := range req.Dependencies {
			left, right := edge.FromSeq, edge.ToSeq
			if left > right {
				left, right = right, left
			}
			key := pair{left: left, right: right}
			if pairKinds[key] == nil {
				pairKinds[key] = make(map[string]struct{})
			}
			pairKinds[key][edge.Kind] = struct{}{}
		}
	}
	for _, edge := range req.Dependencies {
		if _, ok := parent[edge.FromSeq]; !ok {
			continue
		}
		if _, ok := parent[edge.ToSeq]; !ok {
			continue
		}
		left, right := edge.FromSeq, edge.ToSeq
		if left > right {
			left, right = right, left
		}
		if edge.Strength == ai.IntentDependencyHard || (includeSemantic &&
			evidencePartitionDependency(edge.Kind, pairKinds[pair{left: left, right: right}])) {
			union(edge.FromSeq, edge.ToSeq)
		}
	}
	byRoot := map[int64][]int64{}
	for seq := range parent {
		root := find(seq)
		byRoot[root] = append(byRoot[root], seq)
	}
	out := make([][]int64, 0, len(byRoot))
	for _, seqs := range byRoot {
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		out = append(out, seqs)
	}
	sort.Slice(out, func(i, j int) bool {
		if len(out[i]) != len(out[j]) {
			return len(out[i]) < len(out[j])
		}
		return out[i][0] < out[j][0]
	})
	return out
}

func evidencePartitionDependency(kind string, corroborating map[string]struct{}) bool {
	switch kind {
	case "activity_epoch", "temporal_proximity", "module_proximity":
		return false
	case "symbol_hash", "hunk_hash":
		_, symbol := corroborating["symbol_hash"]
		_, hunk := corroborating["hunk_hash"]
		return symbol && hunk
	default:
		return true
	}
}

func addIntentCandidateDependencies(req ai.IntentPlanRequestV2, plan *ai.IntentPlanV2) {
	owner := map[int64]string{}
	for _, candidate := range plan.Candidates {
		for _, seq := range candidate.SelectedSeqs {
			owner[seq] = candidate.CandidateID
		}
	}
	for _, edge := range req.Dependencies {
		if edge.Strength != ai.IntentDependencyHard {
			continue
		}
		from, fromOK := owner[edge.FromSeq]
		to, toOK := owner[edge.ToSeq]
		if !fromOK || !toOK || from == to {
			continue
		}
		for i := range plan.Candidates {
			if plan.Candidates[i].CandidateID == to &&
				!containsIntentString(plan.Candidates[i].DependsOnCandidates, from) {
				plan.Candidates[i].DependsOnCandidates = append(
					plan.Candidates[i].DependsOnCandidates, from)
			}
		}
	}
}

func validateIntentCandidateComponent(
	seqs []int64,
	dependencies []state.IntentCaptureDependency,
) error {
	if len(seqs) <= 1 {
		return nil
	}
	allowed := make(map[int64]struct{}, len(seqs))
	for _, seq := range seqs {
		allowed[seq] = struct{}{}
	}
	adj := make(map[int64][]int64, len(seqs))
	for _, edge := range dependencies {
		if edge.Strength == state.IntentDependencySoft &&
			!strongIntentSemanticDependency(edge.Kind) {
			continue
		}
		if _, ok := allowed[edge.PrerequisiteSeq]; !ok {
			continue
		}
		if _, ok := allowed[edge.DependentSeq]; !ok {
			continue
		}
		adj[edge.PrerequisiteSeq] = append(adj[edge.PrerequisiteSeq], edge.DependentSeq)
		adj[edge.DependentSeq] = append(adj[edge.DependentSeq], edge.PrerequisiteSeq)
	}
	seen := map[int64]struct{}{seqs[0]: {}}
	queue := []int64{seqs[0]}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, next := range adj[current] {
			if _, ok := seen[next]; ok {
				continue
			}
			seen[next] = struct{}{}
			queue = append(queue, next)
		}
	}
	if len(seen) != len(seqs) {
		return errors.New("candidate merges independent dependency components")
	}
	return nil
}

func strongIntentSemanticDependency(kind string) bool {
	switch kind {
	case "activity_epoch", "temporal_proximity", "module_proximity":
		return false
	default:
		return true
	}
}

func loadCandidateCaptureContext(
	ctx context.Context,
	db *state.DB,
	input IntentCandidateEvaluation,
	existing []state.IntentCandidate,
) ([]IntentCandidateCapture, error) {
	bySeq := make(map[int64]IntentCandidateCapture)
	for _, capture := range input.Captures {
		if capture.Event.BranchRef != input.BranchRef ||
			capture.Event.BranchGeneration != input.BranchGeneration {
			return nil, fmt.Errorf("daemon: intent candidates: capture %d has wrong branch pair",
				capture.Event.Seq)
		}
		bySeq[capture.Event.Seq] = capture
	}
	for _, candidate := range existing {
		for i := 0; i < len(candidate.Events); i++ {
			member := candidate.Events[i]
			if member.EventRole == "coalesced" {
				continue
			}
			members := []state.IntentCandidateEvent{member}
			for i+1 < len(candidate.Events) &&
				candidate.Events[i+1].EventRole == "coalesced" {
				i++
				members = append(members, candidate.Events[i])
			}
			if len(members) == 1 {
				if _, ok := bySeq[member.EventSeq]; ok {
					continue
				}
				event, err := loadIntentCaptureEvent(ctx, db, member.EventSeq)
				if err != nil {
					return nil, err
				}
				ops, err := state.LoadCaptureOps(ctx, db, member.EventSeq)
				if err != nil {
					return nil, err
				}
				bySeq[member.EventSeq] = IntentCandidateCapture{
					Event: event, Ops: ops,
				}
				continue
			}
			events := make([]state.CaptureEvent, 0, len(members))
			for _, grouped := range members {
				event, err := loadIntentCaptureEvent(ctx, db, grouped.EventSeq)
				if err != nil {
					return nil, err
				}
				events = append(events, event)
			}
			offers, err := coalesceIntentWindow(
				ctx, db, events, true, state.LoadCaptureOps)
			if err != nil {
				return nil, err
			}
			if len(offers) != 1 ||
				len(offers[0].Token.Covered) != len(events)-1 {
				return nil, fmt.Errorf(
					"daemon: intent candidates: cannot reconstruct coalesced candidate %s",
					candidate.ID)
			}
			capture := IntentCandidateCapture{
				Event: offers[0].Primary, Ops: offers[0].MergedOps,
				CoveredEvents: offers[0].Token.Covered,
			}
			if current, ok := bySeq[member.EventSeq]; ok {
				capture.CapturedDiff = current.CapturedDiff
			}
			bySeq[member.EventSeq] = capture
		}
	}
	out := make([]IntentCandidateCapture, 0, len(bySeq))
	for _, capture := range bySeq {
		out = append(out, capture)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Event.Seq < out[j].Event.Seq })
	return out, nil
}

func loadIntentCaptureEvent(ctx context.Context, db *state.DB, seq int64) (state.CaptureEvent, error) {
	var event state.CaptureEvent
	err := db.ReadSQL().QueryRowContext(ctx, `
SELECT seq, branch_ref, branch_generation, base_head, operation, path, old_path,
       fidelity, captured_ts, published_ts, state, commit_oid, error, message
FROM capture_events WHERE seq=?`, seq).Scan(
		&event.Seq, &event.BranchRef, &event.BranchGeneration, &event.BaseHead,
		&event.Operation, &event.Path, &event.OldPath, &event.Fidelity,
		&event.CapturedTS, &event.PublishedTS, &event.State, &event.CommitOID,
		&event.Error, &event.Message,
	)
	if err != nil {
		return state.CaptureEvent{}, fmt.Errorf("daemon: load candidate capture %d: %w", seq, err)
	}
	return event, nil
}

func mergeIntentDependencies(
	prior []state.IntentCaptureDependency,
	current []state.IntentCaptureDependency,
) ([]state.IntentCaptureDependency, error) {
	out := make([]state.IntentCaptureDependency, 0, len(prior)+len(current))
	seen := make(map[string]struct{}, len(prior)+len(current))
	for _, group := range [][]state.IntentCaptureDependency{prior, current} {
		for _, edge := range group {
			key := fmt.Sprintf("%d\x00%d\x00%s\x00%s", edge.PrerequisiteSeq,
				edge.DependentSeq, edge.Strength, edge.Kind)
			if _, ok := seen[key]; ok {
				continue
			}
			if len(out) >= state.IntentDependencyMaxPerPair {
				return nil, fmt.Errorf("daemon: intent dependency graph: persisted edge cap %d exceeded",
					state.IntentDependencyMaxPerPair)
			}
			seen[key] = struct{}{}
			edge.ID = 0
			out = append(out, edge)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PrerequisiteSeq != out[j].PrerequisiteSeq {
			return out[i].PrerequisiteSeq < out[j].PrerequisiteSeq
		}
		if out[i].DependentSeq != out[j].DependentSeq {
			return out[i].DependentSeq < out[j].DependentSeq
		}
		if out[i].Strength != out[j].Strength {
			return out[i].Strength < out[j].Strength
		}
		return out[i].Kind < out[j].Kind
	})
	return out, nil
}

func consumableBoundariesForPair(
	boundaries []state.IntentActivityBoundary,
	branchRef string,
	generation int64,
) ([]state.IntentActivityBoundary, int64) {
	out := make([]state.IntentActivityBoundary, 0, len(boundaries))
	var through int64
	for _, boundary := range boundaries {
		through = boundary.Epoch
		if boundary.BranchRef.Valid &&
			(boundary.BranchRef.String != branchRef ||
				boundary.BranchGeneration.Int64 != generation) {
			continue
		}
		out = append(out, boundary)
	}
	return out, through
}

func stabilizeIntentCandidatePlan(
	plan ai.IntentPlanV2,
	existing []state.IntentCandidate,
	branchRef string,
	generation int64,
) (ai.IntentPlanV2, error) {
	originalIDs := make([]string, len(plan.Candidates))
	overlapCounts := make(map[string]int)
	overlapCandidate := make([]string, len(plan.Candidates))
	for i, assignment := range plan.Candidates {
		for _, candidate := range existing {
			overlap := false
			for _, event := range candidate.Events {
				if event.EventRole == "coalesced" {
					continue
				}
				if containsIntentSeq(assignment.SelectedSeqs, event.EventSeq) {
					overlap = true
					break
				}
			}
			if !overlap {
				continue
			}
			if overlapCandidate[i] != "" {
				overlapCandidate[i] = ""
				break
			}
			overlapCandidate[i] = candidate.ID
		}
	}
	for _, id := range overlapCandidate {
		if id != "" {
			overlapCounts[id]++
		}
	}
	remap := make(map[string]string, len(plan.Candidates))
	used := make(map[string]struct{}, len(plan.Candidates))
	existingIDs := make(map[string]struct{}, len(existing))
	for _, candidate := range existing {
		existingIDs[candidate.ID] = struct{}{}
	}
	for i := range plan.Candidates {
		oldID := plan.Candidates[i].CandidateID
		originalIDs[i] = oldID
		newID := ""
		if _, explicitlyContinued := existingIDs[oldID]; explicitlyContinued {
			newID = oldID
		} else if overlapCandidate[i] != "" &&
			overlapCounts[overlapCandidate[i]] == 1 {
			newID = overlapCandidate[i]
		}
		if newID == "" {
			newID = stableIntentCandidateID(
				branchRef, generation, plan.Candidates[i].SelectedSeqs)
		}
		if _, duplicate := used[newID]; duplicate {
			return ai.IntentPlanV2{}, fmt.Errorf(
				"daemon: intent candidates: stabilized duplicate id %q", newID)
		}
		used[newID] = struct{}{}
		remap[oldID] = newID
		plan.Candidates[i].CandidateID = newID
	}
	for i := range plan.Candidates {
		dependencies := make([]string, 0,
			len(plan.Candidates[i].DependsOnCandidates))
		seen := make(map[string]struct{},
			len(plan.Candidates[i].DependsOnCandidates))
		for _, dependencyID := range plan.Candidates[i].DependsOnCandidates {
			originalDependencyID := dependencyID
			if replacement := remap[dependencyID]; replacement != "" {
				dependencyID = replacement
			}
			// Distinct provider candidates can stabilize to one persisted
			// candidate. Their former dependency then becomes satisfied by
			// that continuation and must not turn into a self edge. Preserve
			// an explicit provider self edge so validation still rejects it.
			if dependencyID == plan.Candidates[i].CandidateID &&
				originalDependencyID != originalIDs[i] {
				continue
			}
			if _, duplicate := seen[dependencyID]; duplicate {
				continue
			}
			seen[dependencyID] = struct{}{}
			dependencies = append(dependencies, dependencyID)
		}
		plan.Candidates[i].DependsOnCandidates = dependencies
	}
	plan.Candidates = stableTopologicalIntentCandidates(plan.Candidates)
	return plan, nil
}

// stableTopologicalIntentCandidates puts every in-plan prerequisite before its
// dependents. External persisted prerequisites do not participate in ordering.
// Cycles remain in their original order so the complete validator reports the
// structural error instead of normalization hiding it.
func stableTopologicalIntentCandidates(
	candidates []ai.IntentCandidateAssignment,
) []ai.IntentCandidateAssignment {
	indexByID := make(map[string]int, len(candidates))
	for i := range candidates {
		indexByID[candidates[i].CandidateID] = i
	}
	indegree := make([]int, len(candidates))
	dependents := make([][]int, len(candidates))
	for i, candidate := range candidates {
		for _, dependencyID := range candidate.DependsOnCandidates {
			dependencyIndex, ok := indexByID[dependencyID]
			if !ok {
				continue
			}
			indegree[i]++
			dependents[dependencyIndex] = append(
				dependents[dependencyIndex], i)
		}
	}
	less := func(left, right int) bool {
		leftSeq, rightSeq := int64(0), int64(0)
		for _, seq := range candidates[left].SelectedSeqs {
			if leftSeq == 0 || seq < leftSeq {
				leftSeq = seq
			}
		}
		for _, seq := range candidates[right].SelectedSeqs {
			if rightSeq == 0 || seq < rightSeq {
				rightSeq = seq
			}
		}
		if leftSeq != rightSeq {
			return leftSeq < rightSeq
		}
		if candidates[left].CandidateID != candidates[right].CandidateID {
			return candidates[left].CandidateID < candidates[right].CandidateID
		}
		return left < right
	}
	ready := make([]int, 0, len(candidates))
	for i := range candidates {
		if indegree[i] == 0 {
			ready = append(ready, i)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return less(ready[i], ready[j]) })
	ordered := make([]ai.IntentCandidateAssignment, 0, len(candidates))
	for len(ready) > 0 {
		current := ready[0]
		ready = ready[1:]
		ordered = append(ordered, candidates[current])
		for _, dependent := range dependents[current] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
		sort.Slice(ready, func(i, j int) bool { return less(ready[i], ready[j]) })
	}
	if len(ordered) != len(candidates) {
		return candidates
	}
	return ordered
}

func stableIntentCandidateID(
	branchRef string,
	generation int64,
	seqs []int64,
) string {
	ordered := append([]int64(nil), seqs...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%v",
		branchRef, generation, ordered)))
	return fmt.Sprintf("intent-%x", sum[:12])
}

func stableGeneratedCandidateID(req ai.IntentPlanRequestV2, seqs []int64) string {
	var branchContext string
	if req.LatestCommit != nil {
		branchContext = req.LatestCommit.OID
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%v", branchContext, seqs)))
	return fmt.Sprintf("intent-%x", sum[:12])
}

func deterministicIntentCandidateMessage(
	req ai.IntentPlanRequestV2,
	seqs []int64,
) (string, string) {
	selected := make(map[int64]struct{}, len(seqs))
	for _, seq := range seqs {
		selected[seq] = struct{}{}
	}
	var paths []string
	for _, capture := range req.OfferedCaptures {
		if _, ok := selected[capture.Seq]; ok {
			paths = append(paths, capture.Path)
		}
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return "Update captured changes", "apply one dependency component"
	}
	label := path.Base(paths[0])
	if len(paths) > 1 {
		label = path.Dir(paths[0])
		if label == "." {
			label = "related changes"
		}
	}
	subject := "Update " + label
	if len([]rune(subject)) > ai.SubjectCap {
		subject = string([]rune(subject)[:ai.SubjectCap])
	}
	return subject, "update " + label
}

func intentCapturePaths(capture IntentCandidateCapture) []string {
	seen := map[string]struct{}{}
	if capture.Event.Path != "" {
		seen[capture.Event.Path] = struct{}{}
	}
	if capture.Event.OldPath.Valid {
		seen[capture.Event.OldPath.String] = struct{}{}
	}
	for _, op := range capture.Ops {
		if op.Path != "" {
			seen[op.Path] = struct{}{}
		}
		if op.OldPath.Valid {
			seen[op.OldPath.String] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for capturePath := range seen {
		if capturePath != "" {
			out = append(out, capturePath)
		}
	}
	sort.Strings(out)
	return out
}

func intentCaptureRole(capture IntentCandidateCapture) string {
	lower := strings.ToLower(capture.Event.Path)
	base := path.Base(lower)
	switch {
	case strings.Contains(lower, "/migrations/") ||
		strings.Contains(base, "migration") || strings.HasSuffix(base, ".sql"):
		return "migration"
	case strings.Contains(lower, "/docs/") || strings.HasSuffix(base, ".md") ||
		strings.HasSuffix(base, ".mdx") || strings.HasSuffix(base, ".rst"):
		return "documentation"
	case strings.Contains(base, "_test.") || strings.Contains(base, ".test.") ||
		strings.Contains(base, ".spec.") || strings.Contains(lower, "/test/") ||
		strings.Contains(lower, "/tests/"):
		return "test"
	case strings.HasSuffix(base, ".json") || strings.HasSuffix(base, ".yaml") ||
		strings.HasSuffix(base, ".yml") || strings.HasSuffix(base, ".toml") ||
		strings.HasSuffix(base, ".ini"):
		return "configuration"
	default:
		return "code"
	}
}

func intentCaptureModule(capture IntentCandidateCapture) string {
	dir := path.Dir(capture.Event.Path)
	if dir == "." || dir == "/" {
		return ""
	}
	return dir
}

func intentSemanticStem(capture IntentCandidateCapture) string {
	base := strings.ToLower(path.Base(capture.Event.Path))
	for _, marker := range []string{"_test.", ".test.", ".spec."} {
		if index := strings.Index(base, marker); index >= 0 {
			return path.Join(path.Dir(capture.Event.Path), base[:index])
		}
	}
	ext := path.Ext(base)
	return path.Join(path.Dir(capture.Event.Path), strings.TrimSuffix(base, ext))
}

func intentEvidenceHash(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return fmt.Sprintf("sha256:%x", sum[:16])
}

func intentBoundedLabel(value string, cap int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) > cap {
		runes = runes[:cap]
	}
	return string(runes)
}

func secondsTime(value float64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(value*1e9)).UTC()
}

func splitIntentSummary(value string) []string {
	var out []string
	for _, line := range strings.Split(value, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			out = append(out, line)
		}
	}
	return out
}

func pendingIntentGate(
	candidateID string,
	gate ai.IntentAtomicityGate,
	code string,
	summary string,
) ai.IntentAtomicityGateResult {
	return ai.IntentAtomicityGateResult{
		Gate: gate, Status: ai.IntentAtomicityPending,
		Finding: &ai.IntentAtomicityFinding{
			CandidateID: candidateID, Gate: gate, Code: code,
			Summary: ai.NormalizeIntentAtomicitySummary(summary),
		},
	}
}

func failedIntentGate(
	candidateID string,
	gate ai.IntentAtomicityGate,
	code string,
	err error,
) ai.IntentAtomicityGateResult {
	return ai.IntentAtomicityGateResult{
		Gate: gate, Status: ai.IntentAtomicityFailed,
		Finding: &ai.IntentAtomicityFinding{
			CandidateID: candidateID, Gate: gate, Code: code,
			Summary: ai.NormalizeIntentAtomicitySummary(err.Error()),
		},
	}
}

func intentAtomicityReportStatus(report ai.IntentAtomicityReport) ai.IntentAtomicityStatus {
	if report.Valid {
		return ai.IntentAtomicityPassed
	}
	status := ai.IntentAtomicityPending
	for _, gate := range report.Gates {
		if gate.Status == ai.IntentAtomicityFailed {
			return ai.IntentAtomicityFailed
		}
	}
	return status
}

func intentAtomicityGateStatus(
	report ai.IntentAtomicityReport,
	gate ai.IntentAtomicityGate,
) ai.IntentAtomicityStatus {
	for _, result := range report.Gates {
		if result.Gate == gate {
			return result.Status
		}
	}
	return ai.IntentAtomicityPending
}

func containsIntentSeq(values []int64, target int64) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsIntentString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
