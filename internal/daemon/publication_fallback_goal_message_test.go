package daemon

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestPublicationDrainLocalUnlockRetainsHeadingLabelUntilSemanticReview(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	seedTrackedFileCommit(t, ctx, f, "CONTRIBUTING.md", "# Local setup\nUse Pi0.85 for development.\n")
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	const contents = "# Local setup\nUse Pi1.1 for development.\n"
	writePublicationFile(t, f, "CONTRIBUTING.md", contents)
	captured := capturePublicationFiles(t, f)
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	now := time.Now().UTC()
	ts := intentPlannerHealthTimestamp(now)
	drain := state.PublicationDrain{ID: "drain-heading-goal", CheckpointID: captured.CheckpointID, WorktreeID: checkpoint.WorktreeID(f.dir),
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration, Phase: state.PublicationDrainEventFallback,
		FallbackMode: publicationFallbackLocalUnlock, TargetEventCount: 1, EventSeqs: []int64{pending[0].Seq},
		CommitStrategy: string(ai.CommitStrategyIntent), Provider: "intent-v2-test", CommitFormat: string(ai.CommitFormatImperative),
		ProviderFingerprint: publicationDrainTestDigest, CreatedTS: ts, UpdatedTS: ts, LastProgressTS: ts}
	if created, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil || !created {
		t.Fatalf("prepare=(%t,%v)", created, err)
	}
	targetSeqs := append([]int64(nil), drain.EventSeqs...)
	writePublicationFile(t, f, "later.md", "# Separate later goal\nKeep this capture outside the publication target.\n")
	capturePublicationFiles(t, f)
	planner := &semanticRetryReplayPlanner{intentCandidatePlannerStub: intentCandidatePlannerStub{plan: ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{CandidateID: "development-baseline", SelectedSeqs: drain.EventSeqs,
			Purpose: "Explain the Pi1.1 development requirement", Readiness: ai.IntentCandidateReady,
			Subject: "Require Pi1.1 for local development", Body: "- Align local setup guidance with the supported runtime",
			GroupingReason: "The captured documentation completes the development prerequisite change"}}}}}
	opts := ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner,
		IntentPreset: config.PresetFast, IntentBypassBatchWait: true, IntentIncludeDiffs: true, IntentWindow: 10, PublicationDrain: &drain}
	before := f.cctx.BaseHead
	sum, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || sum.Published != 0 || sum.Disposition != ReplayDispositionTransientWait ||
		sum.SkippedReason != "intent_v2_waiting_semantic_retry" || planner.calls != 0 {
		t.Fatalf("local heading became a commit: sum=%+v calls=%d err=%v", sum, planner.calls, err)
	}
	if head, err := gitpkg.RevParse(ctx, f.dir, "HEAD"); err != nil || head != before {
		t.Fatalf("label-only recovery moved HEAD=%s err=%v", head, err)
	}
	run, found, err := state.IntentPlanRunByFingerprint(ctx, f.db, sum.PlanFingerprint)
	if err != nil || !found || run.ResolutionMode.String != "waiting_semantic_retry" || run.ProgressState.String != "waiting_semantic_retry" {
		t.Fatalf("local evidence claimed provider authority: run=%+v found=%t err=%v", run, found, err)
	}
	review, found, err := loadIntentSemanticRetry(ctx, f.db)
	if err != nil || !found || review.ReviewCount != 1 || review.RetryAtTS-review.ScheduledAtTS != (5*time.Minute).Seconds() {
		t.Fatalf("local goal wait did not schedule review: %+v found=%t err=%v", review, found, err)
	}
	drain, err = UpdatePublicationDrainAfterReplay(ctx, f.db, drain, sum, nil, time.Now().UTC())
	if err != nil || drain.FallbackMode != publicationFallbackSemanticReplan || !reflect.DeepEqual(drain.EventSeqs, targetSeqs) {
		t.Fatalf("local wait cannot return to semantic planning: drain=%+v err=%v", drain, err)
	}
	// Advance the isolated fixture's persisted clock by exactly one cadence;
	// restart/retry deadline preservation is exercised by semantic retry tests.
	review.ScheduledAtTS = intentPlannerHealthTimestamp(time.Now().UTC().Add(-5*time.Minute - time.Second))
	review.RetryAtTS = review.ScheduledAtTS + (5 * time.Minute).Seconds()
	if err := saveIntentSemanticRetry(ctx, f.db, review); err != nil {
		t.Fatal(err)
	}
	opts.PublicationDrain = &drain
	sum, err = Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || sum.Published != 1 || planner.calls != 1 {
		t.Fatalf("meaningful semantic review did not publish: sum=%+v calls=%d err=%v", sum, planner.calls, err)
	}
	if subject := strings.TrimSpace(mustGitOutput(t, f.dir, "show", "-s", "--format=%s", "HEAD")); subject != planner.plan.Candidates[0].Subject {
		t.Fatalf("recovery kept a location label: %q", subject)
	}
	if got := mustGitOutput(t, f.dir, "show", "HEAD:CONTRIBUTING.md"); got != contents {
		t.Fatalf("semantic recovery changed captured bytes: %q", got)
	}
	if remaining, err := state.PendingEvents(ctx, f.db, 0); err != nil || len(remaining) != 1 || remaining[0].Path != "later.md" {
		t.Fatalf("later work left the frozen target: %+v err=%v", remaining, err)
	}
}

func TestPublicationDrainVerifiedPrefixRetainsSemanticAuthority(t *testing.T) {
	t.Parallel()
	req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2, OfferedCaptures: []ai.OfferedCapture{
		{Seq: 1, Path: "label.go", Op: "create", CapturedDiff: "+const DefaultLabel = \"complete\"\n"},
		{Seq: 2, Path: "value.go", Op: "create", CapturedDiff: "+func DefaultValue() string { return DefaultLabel }\n"},
	}}
	cached := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: "default-values", SelectedSeqs: []int64{1, 2}, Purpose: "return the default value label", Readiness: ai.IntentCandidateReady,
		Subject: "Add labeled default values", Body: "- Resolve the default label through the value API",
		GroupingReason: "The declared default label and its consumer complete the default value behavior",
	}}}
	planner := publicationDrainAtomicFallbackPlanner{semanticPrefix: &cached}
	input := IntentCandidateEvaluation{BranchRef: "refs/heads/main", BranchGeneration: 1,
		Preset: config.PresetBalanced, CommitFormat: ai.CommitFormatImperative, Now: time.Now().UTC()}
	plan, fallback, _, _, attention, _, run, err := chooseIntentCandidatePlan(context.Background(), req, planner, nil, 0,
		input.Preset, nil, openIntentCandidateTestDB(t), input)
	if err != nil || attention || fallback != "" || !run.Completed || run.ResolutionMode.String != "local_repair" ||
		len(plan.Candidates) != 1 || plan.Candidates[0].Readiness != ai.IntentCandidateReady || plan.Candidates[0].Subject != cached.Candidates[0].Subject {
		t.Fatalf("verified goal lost its semantic authority: plan=%+v run=%+v fallback=%q err=%v", plan, run, fallback, err)
	}
}
