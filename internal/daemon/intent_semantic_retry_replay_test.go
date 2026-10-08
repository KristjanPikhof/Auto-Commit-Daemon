package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type semanticRetryReplayPlanner struct{ intentCandidatePlannerStub }

func (*semanticRetryReplayPlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("v2 required")
}

func (p *semanticRetryReplayPlanner) PlanIntentV2(ctx context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	plan, err := p.intentCandidatePlannerStub.PlanIntentV2(ctx, req)
	if err == nil && len(req.OfferedCaptures) == 1 {
		plan.Candidates[0].SelectedSeqs = []int64{req.OfferedCaptures[0].Seq}
	}
	return plan, err
}

func TestIntentSemanticRetryProtectsWithoutEscalationAndPublishesAfterRestart(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "SpeechEngine.swift", "let retryRecognition = true\n")
	protected := capturePublicationFiles(t, f)
	if !protected.Protected {
		t.Fatalf("capture was not protected: %+v", protected)
	}
	planner := &semanticRetryReplayPlanner{intentCandidatePlannerStub: intentCandidatePlannerStub{
		err: &ai.IntentPlanV2ValidationError{Message: "intent planner v2: invalid response payload"},
	}}
	opts := ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner,
		IntentPreset: config.PresetBalanced, IntentPlannerProvider: planner.Name(),
		IntentWindow: 20, IntentMinPending: 1, IntentBypassBatchWait: true,
		IntentVerificationMode: "structural",
	}
	first, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || first.Disposition != ReplayDispositionTransientWait || first.SkippedReason != "intent_v2_waiting_semantic_retry" || first.Published != 0 || first.HasMore || first.Failed != 0 {
		t.Fatalf("semantic wait escalated or published: %+v err=%v", first, err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("held capture lost protection: pending=%+v err=%v", pending, err)
	}
	retry, found, err := loadIntentSemanticRetry(ctx, f.db)
	if err != nil || !found {
		t.Fatalf("durable retry missing: %+v found=%t err=%v", retry, found, err)
	}
	calls := planner.calls
	dbPath := f.db.Path()
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	f.db, err = state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.db.Close() })
	planner.err, planner.plan = nil, restoredSemanticPlan()
	waiting, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || waiting.Disposition != ReplayDispositionTransientWait || planner.calls != calls || waiting.Published != 0 || waiting.HasMore {
		t.Fatalf("restart spent cooldown: %+v calls=%d err=%v", waiting, planner.calls, err)
	}
	// Advance the persisted deadline instead of sleeping for an hour. The
	// provider now returns a useful goal for the same protected capture.
	retry.RetryAtTS = intentPlannerHealthTimestamp(time.Now().Add(-time.Second))
	if err := saveIntentSemanticRetry(ctx, f.db, retry); err != nil {
		t.Fatal(err)
	}
	recovered, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || recovered.Published != 1 || recovered.Failed != 0 || planner.calls != calls+1 {
		t.Fatalf("due review did not publish: %+v calls=%d err=%v", recovered, planner.calls, err)
	}
	pending, err = state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 0 {
		t.Fatalf("semantic review left protected work pending: %+v err=%v", pending, err)
	}
	message, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "log", "-1", "--format=%s")
	if err != nil || strings.TrimSpace(string(message)) != "Restore speech recognition after pauses" {
		t.Fatalf("published generic message: %q err=%v", message, err)
	}
}
