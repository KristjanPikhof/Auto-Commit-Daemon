package daemon

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func remoteWaitingGoal() ai.IntentCandidateAssignment {
	return ai.IntentCandidateAssignment{CandidateID: "review-speech", SelectedSeqs: []int64{1},
		Purpose: "recognition restart behavior needs a grounded goal", Readiness: ai.IntentCandidateWait,
		MissingCompanions: []string{"the captured evidence needs a meaningful goal review"},
		GroupingReason:    "retain the protected capture while its completed goal is reviewed"}
}

func TestIntentRemoteWaitReviewsAfterRestartAndPublishes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newCaptureFixture(t)
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "SpeechEngine.swift", "let retryRecognition = true\n")
	captured := capturePublicationFiles(t, f)
	if !captured.Protected {
		t.Fatalf("capture was not checkpoint protected: %+v", captured)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("initial pending=%+v err=%v", pending, err)
	}
	planner := &semanticRetryReplayPlanner{intentCandidatePlannerStub: intentCandidatePlannerStub{
		plan: ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{remoteWaitingGoal()}},
	}}
	opts := ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner,
		IntentPreset: config.PresetBalanced, IntentPlannerProvider: planner.Name(), IntentWindow: 20,
		IntentMinPending: 1, IntentBypassBatchWait: true, IntentVerificationMode: "structural"}
	now := float64(time.Now().Unix())
	drain := state.PublicationDrain{ID: "frozen-remote-wait", CheckpointID: captured.CheckpointID, WorktreeID: checkpoint.WorktreeID(f.dir),
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration, CommitStrategy: "intent", CommitFormat: "imperative",
		Provider: planner.Name(), ProviderFingerprint: "sha256:" + strings.Repeat("0", 64), Phase: state.PublicationDrainSemantic,
		TargetEventCount: 1, EventSeqs: []int64{pending[0].Seq}, CreatedTS: now, UpdatedTS: now, LastProgressTS: now}
	if _, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil {
		t.Fatal(err)
	}
	opts.PublicationDrain = &drain
	first, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || first.Disposition != ReplayDispositionTransientWait || first.SkippedReason != "intent_v2_waiting_semantic_retry" || first.Published != 0 || first.Failed != 0 || planner.calls != 1 {
		t.Fatalf("accepted remote WAIT bypassed goal review: %+v calls=%d err=%v", first, planner.calls, err)
	}
	retry, found, err := loadIntentSemanticRetry(ctx, f.db)
	if err != nil || !found || retry.ReviewCount != 1 || retry.RetryAtTS-retry.ScheduledAtTS != (5*time.Minute).Seconds() {
		t.Fatalf("remote WAIT did not schedule five minutes: %+v found=%t err=%v", retry, found, err)
	}
	writePublicationFile(t, f, "release_checklist.md", "# Release readiness checks\n")
	if later := capturePublicationFiles(t, f); !later.Protected {
		t.Fatalf("later work was not protected: %+v", later)
	}
	path := f.db.Path()
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	f.db, err = state.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.db.Close() })
	planner.plan = restoredSemanticPlan()
	for i := 0; i < 3; i++ {
		waiting, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
		if err != nil || waiting.Disposition != ReplayDispositionTransientWait || waiting.Published != 0 || waiting.Failed != 0 || planner.calls != 1 {
			t.Fatalf("restart/poll reset remote WAIT: %+v calls=%d err=%v", waiting, planner.calls, err)
		}
		saved, found, err := loadIntentSemanticRetry(ctx, f.db)
		// The plan fingerprint may move when the durable candidate first
		// becomes visible; the capture deadline and count cannot move.
		if err != nil || !found || saved.RetryAtTS != retry.RetryAtTS || saved.ScheduledAtTS != retry.ScheduledAtTS || saved.ReviewCount != retry.ReviewCount {
			t.Fatalf("unchanged wait drifted: before=%+v after=%+v err=%v", retry, saved, err)
		}
		retry = saved
	}
	if pending, err := state.PendingEvents(ctx, f.db, 0); err != nil || len(pending) != 2 {
		t.Fatalf("remote WAIT lost protected membership: %+v err=%v", pending, err)
	}
	if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
		t.Fatalf("healthy review became required action: %t err=%v", attention, err)
	}
	retry.RetryAtTS = intentPlannerHealthTimestamp(time.Now().Add(-time.Second))
	retry.ScheduledAtTS = retry.RetryAtTS - intentSemanticReviewDelay(retry.ReviewCount).Seconds()
	if err := saveIntentSemanticRetry(ctx, f.db, retry); err != nil {
		t.Fatal(err)
	}
	completed, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || completed.Published != 1 || completed.Failed != 0 || planner.calls != 2 {
		t.Fatalf("due remote review did not create a commit: %+v calls=%d err=%v", completed, planner.calls, err)
	}
	if pending, err := state.PendingEvents(ctx, f.db, 0); err != nil || len(pending) != 1 || pending[0].Path != "release_checklist.md" {
		t.Fatalf("published goal remained queued or consumed later work: %+v err=%v", pending, err)
	}
	if final, err := UpdatePublicationDrainAfterReplay(ctx, f.db, drain, completed, nil, time.Now()); err != nil || final.Phase != state.PublicationDrainCompleted || !reflect.DeepEqual(final.EventSeqs, drain.EventSeqs) {
		t.Fatalf("remote review widened or failed its frozen target: %+v err=%v", final, err)
	}
	if _, found, err := loadIntentSemanticRetry(ctx, f.db); err != nil || found {
		t.Fatalf("resolved remote wait retained cooldown: found=%t err=%v", found, err)
	}
	message, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "log", "-1", "--format=%s")
	if err != nil || strings.TrimSpace(string(message)) != "Restore speech recognition after pauses" {
		t.Fatalf("due review created a generic commit: %q err=%v", message, err)
	}
}

func TestIntentRemoteWaitLegacyCacheRetainsReadyGoal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	req, input, planner := semanticRetryRequest(t)
	req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: 2, Path: "recognition.md", Op: "modify"})
	ready := restoredSemanticPlan().Candidates[0]
	waiting := remoteWaitingGoal()
	waiting.CandidateID, waiting.SelectedSeqs = "review-documentation", []int64{2}
	planner.err, planner.plan = nil, ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{ready, waiting}}
	_, _, _, _, _, _, run, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	if err != nil || planner.calls != 1 {
		t.Fatalf("initial remote plan failed: calls=%d err=%v", planner.calls, err)
	}
	retry, found, err := loadIntentSemanticRetry(ctx, db)
	if err != nil || !found {
		t.Fatalf("fresh remote wait has no review: found=%t err=%v", found, err)
	}
	if err := clearIntentSemanticRetry(ctx, db, retry.EvidenceFingerprint); err != nil {
		t.Fatal(err)
	}
	// Recreate the older worker's accepted provider cache: WAIT was completed
	// with no review record, while its valid ready goal remained in the plan.
	run.ProgressState = sql.NullString{String: "completed", Valid: true}
	run.ResolutionMode = sql.NullString{String: "provider", Valid: true}
	run.UnresolvedSeqs = nil
	if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, "UPDATE intent_plan_runs SET updated_ts=? WHERE fingerprint=?", intentPlannerHealthTimestamp(input.Now.Add(-2*time.Minute)), run.Fingerprint); err != nil {
		t.Fatal(err)
	}
	plan, _, _, _, _, _, repaired, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	if err != nil || planner.calls != 1 || repaired.ProgressState.String != "waiting_semantic_retry" || len(plan.Candidates) != 2 || !reflect.DeepEqual(plan.Candidates[0], ready) {
		t.Fatalf("cached remote WAIT stayed permanently complete: plan=%+v run=%+v calls=%d err=%v", plan, repaired, planner.calls, err)
	}
	retry, found, err = loadIntentSemanticRetry(ctx, db)
	if err != nil || !found || retry.ReviewCount != 1 || retry.RetryAtTS != intentPlannerHealthTimestamp(input.Now.Add(3*time.Minute)) {
		t.Fatalf("legacy cache did not use original review time: %+v found=%t err=%v", retry, found, err)
	}
	_, _, _, _, _, _, _, err = chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	var wait *IntentSemanticRetryWaitError
	if !errors.As(err, &wait) || planner.calls != 1 {
		t.Fatalf("cached remote wait spent another attempt: calls=%d err=%v", planner.calls, err)
	}
	input.Now = secondsTime(retry.RetryAtTS)
	doc := restoredSemanticPlan().Candidates[0]
	doc.CandidateID, doc.SelectedSeqs = "recognition-guide", []int64{2}
	doc.Purpose, doc.Subject = "document speech recognition recovery", "Document speech recognition recovery"
	planner.plan = ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{doc}}
	plan, _, _, _, _, _, resumed, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	if err != nil || planner.calls != 2 || len(plan.Candidates) != 2 || !reflect.DeepEqual(resumed.PreservedGroups, [][]int64{{1}}) || len(planner.req.OfferedCaptures) != 1 || planner.req.OfferedCaptures[0].Seq != 2 || !reflect.DeepEqual(plan.Candidates[0], ready) {
		t.Fatalf("due review lost valid ready goal: plan=%+v request=%+v run=%+v calls=%d err=%v", plan, planner.req, resumed, planner.calls, err)
	}
}
