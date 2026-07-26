package daemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
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
	BranchRef         string
	BranchGeneration  int64
	Captures          []IntentCandidateCapture
	Hints             []IntentDependencyHint
	Planner           interface{ Name() string }
	Health            *IntentPlannerHealth
	RetryLimit        int
	RetryLimitSet     bool
	Preset            config.PresetName
	CommitFormat      ai.CommitFormat
	IncludeDiffs      bool
	ForcedAging       bool
	Provider          string
	Model             string
	ConfigRevisionID  sql.NullInt64
	ConfigProfile     string
	PresetID          string
	PresetVersion     int
	LatestCommit      *ai.CommitSummary
	PathContext       []ai.PathCommitContext
	RecentSoftCommits []ai.IntentSoftCommitSummary
	PriorFindings     []ai.IntentAtomicityFinding
	Materialize       IntentCandidateMaterializer
	VerificationMode  string
	Verify            IntentCandidateVerifier
	Now               time.Time
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
	ProtocolVersion string
	Fallback        string
	PlannerFailure  string
	RetryCount      int
	NeedsAttention  bool
	Boundaries      []state.IntentActivityBoundary
	Dependencies    []state.IntentCaptureDependency
	Decisions       []IntentCandidateDecision
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
		input.RetryLimit = 1
	}
	if input.RetryLimit < 0 {
		input.RetryLimit = 0
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
	allCaptures, err := loadCandidateCaptureContext(ctx, db, input, existing)
	if err != nil {
		return result, err
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
	plan, fallback, plannerFailure, retryCount, needsAttention, err :=
		chooseIntentCandidatePlan(ctx, req, input.Planner, input.Health,
			input.RetryLimit, input.Preset, existing)
	if err != nil {
		return result, err
	}
	result.ProtocolVersion = plan.ProtocolVersion
	result.Fallback = fallback
	result.PlannerFailure = plannerFailure
	result.RetryCount = retryCount
	result.NeedsAttention = needsAttention
	plan, err = stabilizeIntentCandidatePlan(plan, existing, input.BranchRef,
		input.BranchGeneration)
	if err != nil {
		return result, err
	}
	if err := ai.ValidateIntentPlanV2(req, plan); err != nil {
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

	for _, assignment := range plan.Candidates {
		decision, err := evaluateIntentCandidateAssignment(ctx, input, plan,
			assignment, dependencies, existingByID, captureBySeq)
		if err != nil {
			return result, err
		}
		if err := state.SaveIntentCandidate(ctx, db, decision.Candidate); err != nil {
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

func chooseIntentCandidatePlan(
	ctx context.Context,
	req ai.IntentPlanRequestV2,
	planner interface{ Name() string },
	health *IntentPlannerHealth,
	retryLimit int,
	preset config.PresetName,
	existing []state.IntentCandidate,
) (ai.IntentPlanV2, string, string, int, bool, error) {
	plannerFailure := ""
	retryCount := 0
	if planner != nil {
		var permit IntentPlannerHealthPermit
		if health != nil {
			var acquireErr error
			permit, acquireErr = health.Acquire(ctx)
			if acquireErr != nil {
				var openErr *IntentPlannerCircuitOpenError
				if !errors.As(acquireErr, &openErr) {
					return ai.IntentPlanV2{}, "", "", retryCount, false, acquireErr
				}
				planner = nil
			}
		}
		if planner != nil {
			plan, err := ai.PlanIntentV2WithCompatibility(ctx, planner, req)
			if err == nil {
				if health != nil {
					if healthErr := health.Complete(ctx, permit, nil); healthErr != nil {
						return ai.IntentPlanV2{}, "", "", retryCount, false, healthErr
					}
				}
				return plan, "", "", retryCount, false, nil
			}
			if ctx.Err() != nil {
				if health != nil {
					_ = health.Complete(ctx, permit, err)
				}
				return ai.IntentPlanV2{}, "", "", retryCount, false, ctx.Err()
			}
			plannerCallFailed := true
			for attempt := 0; attempt < retryLimit; attempt++ {
				var validation *ai.IntentPlanV2ValidationError
				if !errors.As(err, &validation) ||
					len(validation.Findings) == 0 {
					break
				}
				retry := req
				retry.RetryCorrection = ai.BuildIntentAtomicityCorrection(validation.Findings)
				retryCount++
				retryCtx := prompttrace.WithRetryCount(ctx, retryCount)
				if corrected, retryErr := ai.PlanIntentV2WithCompatibility(retryCtx, planner, retry); retryErr == nil {
					if health != nil {
						if healthErr := health.Complete(ctx, permit, nil); healthErr != nil {
							return ai.IntentPlanV2{}, "", "", retryCount, false, healthErr
						}
					}
					return corrected, "", "", retryCount, false, nil
				} else if ctx.Err() != nil {
					if health != nil {
						_ = health.Complete(ctx, permit, retryErr)
					}
					return ai.IntentPlanV2{}, "", "", retryCount, false, ctx.Err()
				} else {
					err = retryErr
					plannerCallFailed = true
				}
			}
			if health != nil {
				failure := classifyIntentPlannerHealthFailure(err, plannerCallFailed)
				if healthErr := health.Complete(ctx, permit, failure); healthErr != nil {
					return ai.IntentPlanV2{}, "", "", retryCount, false, healthErr
				}
			}
			plannerFailure = ai.SanitizePlannerError(err.Error())
		}
	}
	switch preset {
	case config.PresetFast:
		plan := deterministicIntentCandidatePlan(req, false, true)
		if err := continuePersistedIntentCandidates(req, &plan); err != nil {
			return ai.IntentPlanV2{}, "", plannerFailure, retryCount, false, err
		}
		return plan, "hard_dependency_component", plannerFailure, retryCount, false,
			ai.ValidateIntentPlanV2(req, plan)
	case config.PresetBalanced:
		if plan, ok := reuseIntentCandidatePartition(req, existing); ok {
			return plan, "last_valid_partition", plannerFailure, retryCount, false, nil
		}
		plan := deterministicIntentCandidatePlan(req, true, false)
		if err := continuePersistedIntentCandidates(req, &plan); err != nil {
			return ai.IntentPlanV2{}, "", plannerFailure, retryCount, false, err
		}
		return plan, "verified_dependency_partition", plannerFailure, retryCount, false,
			ai.ValidateIntentPlanV2(req, plan)
	case config.PresetQuality:
		plan := holdIntentCandidatePlan(req)
		if err := continuePersistedIntentCandidates(req, &plan); err != nil {
			return ai.IntentPlanV2{}, "", plannerFailure, retryCount, false, err
		}
		return plan, "needs_attention", plannerFailure, retryCount, true,
			ai.ValidateIntentPlanV2(req, plan)
	default:
		return ai.IntentPlanV2{}, "", plannerFailure, retryCount, false,
			fmt.Errorf("daemon: intent candidates: unsupported preset %q", preset)
	}
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
	if err := validateIntentCandidateComponent(selected, dependencies); err != nil {
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

// continuePersistedIntentCandidates keeps deterministic fallback safe when a
// hard edge connects newly offered work to an existing candidate. The v2
// response carries only offered sequences, so continuing the persisted
// candidate is expressed by reusing its ID; evaluation then restores its prior
// membership before materialization.
func continuePersistedIntentCandidates(
	req ai.IntentPlanRequestV2,
	plan *ai.IntentPlanV2,
) error {
	outputOwner := make(map[int64]int, len(req.OfferedCaptures))
	for i, candidate := range plan.Candidates {
		for _, seq := range candidate.SelectedSeqs {
			outputOwner[seq] = i
		}
	}
	persistedOwner := make(map[int64]string)
	for _, candidate := range req.Candidates {
		for _, seq := range candidate.SelectedSeqs {
			persistedOwner[seq] = candidate.CandidateID
		}
	}
	continuedByOutput := make(map[int]string)
	bind := func(output int, seq int64, candidateID string) error {
		if previous := continuedByOutput[output]; previous != "" &&
			previous != candidateID {
			return fmt.Errorf(
				"daemon: intent candidates: fallback capture %d connects persisted candidates %q and %q",
				seq, previous, candidateID)
		}
		continuedByOutput[output] = candidateID
		return nil
	}
	for _, edge := range req.Dependencies {
		if edge.Strength != ai.IntentDependencyHard {
			continue
		}
		fromOutput, fromVisible := outputOwner[edge.FromSeq]
		toOutput, toVisible := outputOwner[edge.ToSeq]
		fromPersisted, fromPersistedOK := persistedOwner[edge.FromSeq]
		toPersisted, toPersistedOK := persistedOwner[edge.ToSeq]
		switch {
		case fromVisible && !toVisible && toPersistedOK:
			if err := bind(fromOutput, edge.FromSeq, toPersisted); err != nil {
				return err
			}
		case !fromVisible && fromPersistedOK && toVisible:
			if err := bind(toOutput, edge.ToSeq, fromPersisted); err != nil {
				return err
			}
		}
	}
	remappedIDs := make(map[string]string, len(continuedByOutput))
	for output, candidateID := range continuedByOutput {
		remappedIDs[plan.Candidates[output].CandidateID] = candidateID
		plan.Candidates[output].CandidateID = candidateID
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
		subject, purpose := deterministicIntentCandidateMessage(
			req, candidate.SelectedSeqs)
		candidate.Purpose = purpose
		if candidate.Readiness == ai.IntentCandidateReady {
			candidate.Subject = subject
			candidate.Body = ""
		}
	}
	plan.Candidates = merged
	return nil
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
	for _, edge := range req.Dependencies {
		if _, ok := parent[edge.FromSeq]; !ok {
			continue
		}
		if _, ok := parent[edge.ToSeq]; !ok {
			continue
		}
		if edge.Strength == ai.IntentDependencyHard ||
			(includeSemantic && strongIntentSemanticDependency(edge.Kind)) {
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
		for j, dependencyID := range plan.Candidates[i].DependsOnCandidates {
			if replacement := remap[dependencyID]; replacement != "" {
				plan.Candidates[i].DependsOnCandidates[j] = replacement
			}
		}
	}
	return plan, nil
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
