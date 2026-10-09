package daemon

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentCompanionSplitReviewsAfterRestartAndPublishesWholeGoal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newCaptureFixture(t)
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "retry.go", "package app\nfunc RetrySpeech(attempt int) bool { return attempt > 0 }\n")
	writePublicationFile(t, f, "retry_test.go", "package app\nimport \"testing\"\nfunc TestRetrySpeech(t *testing.T) { if !RetrySpeech(1) { t.Fatal(\"retry unavailable\") } }\n")
	if protected := capturePublicationFiles(t, f); !protected.Protected {
		t.Fatalf("source and test unprotected: %+v", protected)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 2 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	input := IntentCandidateEvaluation{
		RepoPath: f.dir, BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		Preset: config.PresetBalanced, PresetID: "intent.balanced", PresetVersion: config.PresetCatalogVersion,
		CommitFormat: ai.CommitFormatImperative, IncludeDiffs: true, VerificationMode: "structural",
		Provider: "intent-v2-test", Now: time.Now().UTC().Truncate(time.Second),
		Materialize: intentCandidateScratchMaterializer(f.dir, f.gitDir, f.cctx.BaseHead),
	}
	for _, event := range pending {
		ops, err := state.LoadCaptureOps(ctx, f.db, event.Seq)
		if err != nil {
			t.Fatal(err)
		}
		diff, err := BuildOpsDiffWithCap(ctx, f.dir, ops, ai.IntentStageDiffCap)
		if err != nil {
			t.Fatal(err)
		}
		capture := IntentCandidateCapture{Event: event, Ops: ops, CapturedDiff: diff}
		input.Captures = append(input.Captures, capture)
		input.TargetEventSeqs = append(input.TargetEventSeqs, event.Seq)
	}
	input.Captures, err = loadFocusedIntentGoalEvidence(ctx, input, nil, input.Captures)
	if err != nil {
		t.Fatal(err)
	}
	if err := attachIntentFileMetadata(ctx, f.dir, input.Captures); err != nil {
		t.Fatal(err)
	}
	bySeq := map[int64]IntentCandidateCapture{}
	for _, capture := range input.Captures {
		bySeq[capture.Event.Seq] = capture
	}
	dependencies, err := BuildIntentCandidateDependencies(input.BranchRef, input.BranchGeneration, input.Captures, runtimeIntentDependencyHints(input.Captures), input.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.ReplaceIntentCaptureDependencies(ctx, f.db, input.BranchRef, input.BranchGeneration, dependencies); err != nil {
		t.Fatal(err)
	}
	req, err := buildIntentCandidateRequest(input, nil, dependencies, nil, input.Captures)
	if err != nil {
		t.Fatal(err)
	}
	split := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for _, capture := range input.Captures {
		split.Candidates = append(split.Candidates, ai.IntentCandidateAssignment{
			CandidateID: capture.Event.Path, SelectedSeqs: []int64{capture.Event.Seq},
			Purpose: "restore speech recognition after pauses", Readiness: ai.IntentCandidateReady,
			Subject: "Restore speech recognition after pauses", Body: "- Keep resumed recognition covered by its regression test",
			GroupingReason: "the older worker incorrectly split the implementation and test",
		})
	}
	result := IntentCandidateEvaluationResult{}
	for _, assignment := range split.Candidates {
		decision, err := evaluateIntentCandidateAssignment(ctx, f.db, input, split, assignment, dependencies, nil, bySeq)
		if err != nil || decision.Publishable || !intentAtomicityNeedsSemanticReview(decision.Atomicity) || decision.Candidate.Status != state.IntentCandidateWaiting {
			t.Fatalf("split companion did not request review: %+v err=%v", decision, err)
		}
		// Reproduce the blocked rows persisted by the old installed worker.
		legacy := decision.Candidate
		legacy.Status = state.IntentCandidateBlocked
		if err := state.SaveIntentCandidate(ctx, f.db, legacy); err != nil {
			t.Fatal(err)
		}
		result.Decisions = append(result.Decisions, decision)
	}
	run, err := newIntentPlanRun(req, input, 3)
	if err != nil {
		t.Fatal(err)
	}
	run, err = state.EnsureIntentPlanRun(ctx, f.db, run)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduleRejectedIntentGoalReview(ctx, f.db, input, req, split, nil, run, &result); err != nil {
		t.Fatal(err)
	}
	run, found, err := state.IntentPlanRunByFingerprint(ctx, f.db, run.Fingerprint)
	if err != nil || !found || !reflect.DeepEqual(run.FindingCodes, []string{"available_companion_split"}) || result.NeedsAttention || result.ResolutionMode != "waiting_semantic_retry" {
		t.Fatalf("split was not scheduled honestly: %+v result=%+v err=%v", run, result, err)
	}
	head := mustGitOutput(t, f.dir, "rev-parse", "HEAD")
	index := mustGitOutput(t, f.dir, "ls-files", "--stage")
	var checkpointID string
	if err := f.db.ReadSQL().QueryRowContext(ctx, "SELECT checkpoint_id FROM checkpoint_events WHERE event_seq=? LIMIT 1", input.TargetEventSeqs[0]).Scan(&checkpointID); err != nil {
		t.Fatal(err)
	}
	ts := intentPlannerHealthTimestamp(time.Now())
	drain := state.PublicationDrain{ID: "review-whole-speech-retry", CheckpointID: checkpointID, WorktreeID: checkpoint.WorktreeID(f.dir), BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration,
		CommitStrategy: "intent", CommitFormat: "imperative", Provider: input.Provider, ProviderFingerprint: "sha256:" + strings.Repeat("0", 64),
		Phase: state.PublicationDrainEventFallback, FallbackMode: publicationFallbackSemanticReplan,
		TargetEventCount: 2, EventSeqs: input.TargetEventSeqs, CreatedTS: ts, UpdatedTS: ts, LastProgressTS: ts}
	if _, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil {
		t.Fatal(err)
	}
	unfrozen := input
	unfrozen.TargetEventSeqs = nil
	differentTarget := input
	differentTarget.TargetEventSeqs = []int64{pending[1].Seq}
	for _, tc := range []struct {
		name  string
		input IntentCandidateEvaluation
		phase string
		mode  string
	}{
		{"unfrozen_input", unfrozen, state.PublicationDrainEventFallback, publicationFallbackSemanticReplan},
		{"different_input_target", differentTarget, state.PublicationDrainEventFallback, publicationFallbackSemanticReplan},
		{"checkpointing", input, state.PublicationDrainCheckpointing, ""},
		{"local_unlock", input, state.PublicationDrainEventFallback, publicationFallbackLocalUnlock},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := f.db.SQL().ExecContext(ctx, "UPDATE publication_drains SET phase=?,fallback_mode=? WHERE id=?", tc.phase, tc.mode, drain.ID); err != nil {
				t.Fatal(err)
			}
			legacy, found, err := state.IntentCandidateByID(ctx, f.db, split.Candidates[0].CandidateID)
			if err != nil || !found {
				t.Fatalf("legacy candidate missing: found=%t err=%v", found, err)
			}
			rows := []state.IntentCandidate{legacy}
			if err := reopenProtectedSemanticIntentCandidates(ctx, f.db, tc.input, rows); err != nil || rows[0].Status != state.IntentCandidateBlocked {
				t.Fatalf("unsafe frozen review reopened: %+v err=%v", rows, err)
			}
		})
	}
	if _, err := f.db.SQL().ExecContext(ctx, "UPDATE publication_drains SET phase=?,fallback_mode=? WHERE id=?", drain.Phase, drain.FallbackMode, drain.ID); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "later.md", "# Later independent work\n")
	if protected := capturePublicationFiles(t, f); !protected.Protected {
		t.Fatalf("later capture unprotected: %+v", protected)
	}
	later, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(later) != 3 {
		t.Fatalf("later protected membership missing: %+v err=%v", later, err)
	}
	legacy, found, err := state.IntentCandidateByID(ctx, f.db, split.Candidates[0].CandidateID)
	if err != nil || !found {
		t.Fatalf("legacy candidate missing: found=%t err=%v", found, err)
	}
	expanded := legacy
	expanded.Events = append(append([]state.IntentCandidateEvent(nil), legacy.Events...), state.IntentCandidateEvent{EventSeq: later[2].Seq, EventRole: "docs"})
	if err := state.SaveIntentCandidate(ctx, f.db, expanded); err != nil {
		t.Fatal(err)
	}
	withLater := input
	withLater.TargetEventSeqs = append(append([]int64(nil), input.TargetEventSeqs...), later[2].Seq)
	rows := []state.IntentCandidate{expanded}
	if err := reopenProtectedSemanticIntentCandidates(ctx, f.db, withLater, rows); err != nil || rows[0].Status != state.IntentCandidateBlocked {
		t.Fatalf("review widened the durable frozen target: %+v err=%v", rows, err)
	}
	if err := state.SaveIntentCandidate(ctx, f.db, legacy); err != nil {
		t.Fatal(err)
	}
	dbPath := f.db.Path()
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	f.db, err = state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.db.Close() })
	whole := split.Candidates[0]
	whole.CandidateID = "speech-retry"
	whole.SelectedSeqs = append([]int64(nil), input.TargetEventSeqs...)
	whole.Readiness = ai.IntentCandidateReady
	whole.MissingCompanions = nil
	whole.GroupingReason = "the actual RetrySpeech implementation and caller test complete one goal"
	planner := &semanticRetryReplayPlanner{intentCandidatePlannerStub: intentCandidatePlannerStub{plan: ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{whole}}}}
	input.Planner = planner
	waiting, err := EvaluateIntentCandidates(ctx, f.db, input)
	if err != nil || waiting.NeedsAttention || waiting.ResolutionMode != "waiting_semantic_retry" || planner.calls != 0 {
		t.Fatalf("restart did not preserve bounded review: %+v calls=%d err=%v", waiting, planner.calls, err)
	}
	if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
		t.Fatalf("protected companion review still requires user action: attention=%t err=%v", attention, err)
	}
	if err := updateIntentV2EvaluationMeta(ctx, f.db, &RuntimeBundle{PresetID: "intent.balanced", PresetVersion: config.PresetCatalogVersion}, ReplaySummary{SkippedReason: "intent_v2_waiting_semantic_retry"}, nil); err != nil {
		t.Fatal(err)
	}
	if raw, _, err := state.MetaGet(ctx, f.db, "intent.v2.needs_attention"); err != nil || raw != "" {
		t.Fatalf("status/list projection asks for configuration: %q err=%v", raw, err)
	}
	input.Now = input.Now.Add(5*time.Minute + time.Second)
	reviewed, err := EvaluateIntentCandidates(ctx, f.db, input)
	if err != nil || reviewed.NeedsAttention || len(reviewed.Decisions) != 1 || !reviewed.Decisions[0].Publishable || planner.calls != 1 {
		t.Fatalf("whole goal did not recover: %+v calls=%d err=%v", reviewed, planner.calls, err)
	}
	if mustGitOutput(t, f.dir, "rev-parse", "HEAD") != head || mustGitOutput(t, f.dir, "ls-files", "--stage") != index {
		t.Fatal("semantic review changed live Git state")
	}
	published, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner, IntentPlannerProvider: planner.Name(), IntentPreset: config.PresetBalanced, IntentWindow: 20, IntentBypassBatchWait: true, IntentVerificationMode: "structural", PublicationDrain: &drain})
	if err != nil || published.Published != 2 || published.Failed != 0 || published.Disposition == ReplayDispositionNeedsAttention {
		t.Fatalf("whole source/test goal did not publish: %+v err=%v", published, err)
	}
	if got := strings.TrimSpace(mustGitOutput(t, f.dir, "show", "--format=%s", "--name-only", "HEAD")); got != "Restore speech recognition after pauses\n\nretry.go\nretry_test.go" {
		t.Fatalf("source and supporting test did not publish together: %q", got)
	}
	pending, err = state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Path != "later.md" {
		t.Fatalf("frozen target consumed later work: %+v err=%v", pending, err)
	}
}

func TestIntentCompanionSplitReviewKeepsOtherSafetyFailuresClosed(t *testing.T) {
	t.Parallel()
	for _, gate := range []ai.IntentAtomicityGate{ai.IntentAtomicityDependency, ai.IntentAtomicityMaterialization, ai.IntentAtomicityVerification} {
		report := ai.NewIntentAtomicityReport("goal", failedIntentGate("goal", ai.IntentAtomicityCompleteness, "available_companion_split", sql.ErrNoRows), failedIntentGate("goal", gate, "unsafe_state", sql.ErrNoRows))
		if intentAtomicityNeedsSemanticReview(report) {
			t.Fatalf("companion split waived %s", gate)
		}
	}
	if intentFindingNeedsSemanticReview(ai.IntentAtomicityDependency, "available_companion_split") || intentFindingNeedsSemanticReview(ai.IntentAtomicityCompleteness, "hard_dependency_undeclared") {
		t.Fatal("semantic review accepted a mismatched code or gate")
	}
}
