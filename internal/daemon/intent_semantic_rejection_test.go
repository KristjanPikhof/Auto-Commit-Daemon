package daemon

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func semanticRejectedFixture(t *testing.T) (*captureFixture, IntentCandidateEvaluation, ai.IntentPlanRequestV2, ai.IntentPlanV2, IntentCandidateDecision) {
	t.Helper()
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "SpeechEngine.swift", "func retrySpeechRecognition(attempt: Int) -> Bool { return attempt > 0 }\n")
	writePublicationFile(t, f, "TranslationEngine.swift", "func selectedTranslationLocale(_ locale: String) -> String { return locale }\n")
	if protection := capturePublicationFiles(t, f); !protection.Protected {
		t.Fatalf("initial captures unprotected: %+v", protection)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 2 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	input := IntentCandidateEvaluation{
		RepoPath: f.dir, BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		Preset: config.PresetBalanced, PresetID: "intent.balanced", PresetVersion: config.PresetCatalogVersion, CommitFormat: ai.CommitFormatImperative,
		IncludeDiffs: true, VerificationMode: "structural", Provider: "intent-v2-test",
		Now:         time.Now().UTC().Truncate(time.Second),
		Materialize: intentCandidateScratchMaterializer(f.dir, f.gitDir, f.cctx.BaseHead),
	}
	for _, event := range pending {
		input.TargetEventSeqs = append(input.TargetEventSeqs, event.Seq)
		ops, err := state.LoadCaptureOps(ctx, f.db, event.Seq)
		if err != nil {
			t.Fatal(err)
		}
		diff, err := BuildOpsDiffWithCap(ctx, f.dir, ops, ai.IntentStageDiffCap)
		if err != nil {
			t.Fatal(err)
		}
		input.Captures = append(input.Captures, IntentCandidateCapture{Event: event, Ops: ops, CapturedDiff: diff})
	}
	if err := attachIntentFileMetadata(ctx, f.dir, input.Captures); err != nil {
		t.Fatal(err)
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
	assignment := ai.IntentCandidateAssignment{
		CandidateID: "unproved-recognition-group", SelectedSeqs: []int64{pending[0].Seq, pending[1].Seq},
		Purpose: "restore speech recognition after pauses", Readiness: ai.IntentCandidateReady,
		Subject: "Restore speech recognition after pauses", Body: "- Keep recognition available after capture resumes",
		GroupingReason: "the proposed relationship needs exact captured proof",
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{assignment}}
	bySeq := map[int64]IntentCandidateCapture{}
	for _, capture := range input.Captures {
		bySeq[capture.Event.Seq] = capture
	}
	decision, err := evaluateIntentCandidateAssignment(ctx, f.db, input, plan, assignment, dependencies, nil, bySeq)
	if err != nil || decision.Publishable || !intentAtomicityNeedsSemanticReview(decision.Atomicity) {
		t.Fatalf("actual candidate gates did not produce the semantic rejection: %+v err=%v", decision, err)
	}
	return f, input, req, plan, decision
}

func correctedSemanticRejectedPlan(input IntentCandidateEvaluation) ai.IntentPlanV2 {
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for i, capture := range input.Captures {
		subject := "Restore speech recognition after pauses"
		if i == 1 {
			subject = "Preserve the selected translation locale"
		}
		plan.Candidates = append(plan.Candidates, ai.IntentCandidateAssignment{
			CandidateID: capture.Event.Path, SelectedSeqs: []int64{capture.Event.Seq},
			Purpose: subject, Readiness: ai.IntentCandidateReady, Subject: subject,
			Body:           "- Keep this complete behavior independently reviewable",
			GroupingReason: "each recorded behavior is independently complete",
		})
	}
	return plan
}

func TestIntentSemanticCandidateRejectionReviewsAfterRestartAndProtectsLaterCapture(t *testing.T) {
	t.Parallel()
	f, input, req, plan, decision := semanticRejectedFixture(t)
	ctx := context.Background()
	head := mustGitOutput(t, f.dir, "rev-parse", "HEAD")
	index := mustGitOutput(t, f.dir, "ls-files", "--stage")
	if decision.Candidate.Status != state.IntentCandidateWaiting {
		t.Fatalf("semantic rejection became permanent: %+v", decision.Candidate)
	}
	if err := state.SaveIntentCandidate(ctx, f.db, decision.Candidate); err != nil {
		t.Fatal(err)
	}
	run, err := newIntentPlanRun(req, input, 3)
	if err != nil {
		t.Fatal(err)
	}
	run, err = state.EnsureIntentPlanRun(ctx, f.db, run)
	if err != nil {
		t.Fatal(err)
	}
	result := IntentCandidateEvaluationResult{PlanFingerprint: run.Fingerprint, Decisions: []IntentCandidateDecision{decision}}
	if err := scheduleRejectedIntentGoalReview(ctx, f.db, input, req, plan, nil, run, &result); err != nil {
		t.Fatal(err)
	}
	if result.NeedsAttention || result.ResolutionMode != "waiting_semantic_retry" || result.UnresolvedCaptureCount != 2 {
		t.Fatalf("semantic rejection did not enter review: %+v", result)
	}
	if err := updateIntentV2EvaluationMeta(ctx, f.db, &RuntimeBundle{PresetID: "intent.balanced", PresetVersion: config.PresetCatalogVersion}, ReplaySummary{SkippedReason: "intent_v2_waiting_semantic_retry"}, nil); err != nil {
		t.Fatal(err)
	}
	if raw, _, err := state.MetaGet(ctx, f.db, "intent.v2.needs_attention"); err != nil || raw != "" {
		t.Fatalf("healthy review still asked for configuration: %q err=%v", raw, err)
	}
	retry, found, err := loadIntentSemanticRetry(ctx, f.db)
	if err != nil || !found || retry.RetryAtTS != intentPlannerHealthTimestamp(input.Now.Add(time.Hour)) {
		t.Fatalf("durable review deadline=%+v found=%t err=%v", retry, found, err)
	}
	planner := &semanticRetryReplayPlanner{intentCandidatePlannerStub: intentCandidatePlannerStub{plan: correctedSemanticRejectedPlan(input)}}
	input.Planner = planner
	input.TargetEventSeqs = offeredIntentSeqs(req)
	writePublicationFile(t, f, "later.md", "# Later capture remains protected\n")
	if protection := capturePublicationFiles(t, f); !protection.Protected {
		t.Fatalf("later capture unprotected: %+v", protection)
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
	waiting, err := EvaluateIntentCandidates(ctx, f.db, input)
	if err != nil || waiting.NeedsAttention || waiting.ResolutionMode != "waiting_semantic_retry" || planner.calls != 0 {
		t.Fatalf("restart skipped semantic cooldown: %+v calls=%d err=%v", waiting, planner.calls, err)
	}
	input.Now = input.Now.Add(time.Hour + time.Second)
	reviewed, err := EvaluateIntentCandidates(ctx, f.db, input)
	if err != nil || reviewed.NeedsAttention || planner.calls != 1 || len(reviewed.Decisions) != 2 {
		t.Fatalf("due review could not safely split rejected grouping: %+v calls=%d err=%v", reviewed, planner.calls, err)
	}
	for _, revised := range reviewed.Decisions {
		if !revised.Publishable {
			t.Fatalf("corrected complete behavior remains blocked: %+v", revised)
		}
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 3 || mustGitOutput(t, f.dir, "rev-parse", "HEAD") != head || mustGitOutput(t, f.dir, "ls-files", "--stage") != index {
		t.Fatalf("review changed captured work or live Git state: pending=%+v err=%v", pending, err)
	}
	old, found, err := state.IntentCandidateByID(ctx, f.db, decision.Candidate.ID)
	if err != nil || !found || old.Status != state.IntentCandidateSuperseded {
		t.Fatalf("rejected grouping retained ownership: %+v err=%v", old, err)
	}
	history, err := state.IntentCandidateEventHistory(ctx, f.db, old.ID)
	if err != nil || len(history) != 2 {
		t.Fatalf("rejected membership provenance was lost: %+v err=%v", history, err)
	}
	var checkpointID string
	if err := f.db.ReadSQL().QueryRowContext(ctx, "SELECT checkpoint_id FROM checkpoint_events WHERE event_seq=?", input.Captures[0].Event.Seq).Scan(&checkpointID); err != nil {
		t.Fatal(err)
	}
	ts := intentPlannerHealthTimestamp(time.Now())
	drain := state.PublicationDrain{
		ID: "review-corrected-goals", CheckpointID: checkpointID, WorktreeID: checkpoint.WorktreeID(f.dir),
		BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration,
		Phase: state.PublicationDrainSemantic, TargetEventCount: 2, EventSeqs: input.TargetEventSeqs,
		CreatedTS: ts, UpdatedTS: ts, LastProgressTS: ts,
	}
	if _, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil {
		t.Fatal(err)
	}
	published, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner,
		IntentPlannerProvider: planner.Name(), IntentPreset: config.PresetBalanced,
		IntentWindow: 20, IntentBypassBatchWait: true, IntentVerificationMode: "structural",
		PublicationDrain: &drain,
	})
	if err != nil || published.Published != 2 || published.Failed != 0 || published.Disposition == ReplayDispositionNeedsAttention {
		t.Fatalf("corrected semantic goals did not publish: %+v err=%v", published, err)
	}
	pending, err = state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Path != "later.md" {
		t.Fatalf("publication consumed later protected work: %+v err=%v", pending, err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "cat-file", "-e", "HEAD:later.md"); err == nil {
		t.Fatal("later capture escaped the frozen publication target")
	}
	if body, err := os.ReadFile(filepath.Join(f.dir, "later.md")); err != nil || string(body) != "# Later capture remains protected\n" {
		t.Fatalf("publication changed later live work: %q err=%v", body, err)
	}
}

func TestIntentSemanticCandidateLegacyBlockReopensOnlyProtectedExactFailure(t *testing.T) {
	t.Parallel()
	f, input, _, _, decision := semanticRejectedFixture(t)
	ctx := context.Background()
	candidate := decision.Candidate
	candidate.Status = state.IntentCandidateBlocked
	if err := state.SaveIntentCandidate(ctx, f.db, candidate); err != nil {
		t.Fatal(err)
	}
	dbPath := f.db.Path()
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	f.db = db
	t.Cleanup(func() { _ = f.db.Close() })
	for _, tc := range []struct {
		name   string
		mutate func(*state.IntentCandidate)
	}{
		{"dependency", func(c *state.IntentCandidate) {
			c.AtomicitySummary += "\ncandidate=" + c.ID + " gate=dependency code=hard_dependency_undeclared: prerequisite is unavailable"
		}},
		{"verification", func(c *state.IntentCandidate) { c.VerificationStatus = sql.NullString{String: "failed", Valid: true} }},
		{"branch", func(c *state.IntentCandidate) { c.BranchGeneration++ }},
		{"unpublished_identity", func(c *state.IntentCandidate) {
			c.PublishedCommitOID = sql.NullString{String: "published", Valid: true}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unsafe := candidate
			tc.mutate(&unsafe)
			rows := []state.IntentCandidate{unsafe}
			if err := reopenProtectedSemanticIntentCandidates(ctx, f.db, input, rows); err != nil || !reflect.DeepEqual(rows[0], unsafe) {
				t.Fatalf("ambiguous block reopened: %+v err=%v", rows, err)
			}
		})
	}
	for _, tc := range []struct{ name, apply, restore string }{
		{"checkpoint_prepared", "UPDATE checkpoints SET phase='prepared'", "UPDATE checkpoints SET phase='completed'"},
		{"checkpoint_incomplete", "UPDATE checkpoints SET coverage_complete=0", "UPDATE checkpoints SET coverage_complete=1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := f.db.SQL().ExecContext(ctx, tc.apply); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := f.db.SQL().ExecContext(ctx, tc.restore); err != nil {
					t.Fatal(err)
				}
			}()
			rows := []state.IntentCandidate{candidate}
			if err := reopenProtectedSemanticIntentCandidates(ctx, f.db, input, rows); err != nil || rows[0].Status != state.IntentCandidateBlocked {
				t.Fatalf("unproved checkpoint released legacy block: %+v err=%v", rows, err)
			}
		})
	}
	if err := state.MetaSet(ctx, f.db, MetaKeyBranchTransitionNeedsAttention, "ambiguous branch transition"); err != nil {
		t.Fatal(err)
	}
	rows := []state.IntentCandidate{candidate}
	if err := reopenProtectedSemanticIntentCandidates(ctx, f.db, input, rows); err != nil || rows[0].Status != state.IntentCandidateBlocked {
		t.Fatalf("branch ambiguity released legacy block: %+v err=%v", rows, err)
	}
	if err := state.MetaSet(ctx, f.db, MetaKeyBranchTransitionNeedsAttention, ""); err != nil {
		t.Fatal(err)
	}
	rows = []state.IntentCandidate{candidate}
	if err := reopenProtectedSemanticIntentCandidates(ctx, f.db, input, rows); err != nil || rows[0].Status != state.IntentCandidateWaiting {
		t.Fatalf("protected semantic-only legacy rejection stayed blocked: %+v err=%v", rows, err)
	}
	if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
		t.Fatalf("safe legacy review still requires configuration: attention=%t err=%v", attention, err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "diff", "--exit-code"); err != nil {
		t.Fatalf("classification-only recovery changed tracked files: %v", err)
	}
}
