package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentUnassignedReplayPublishesCompleteGoalAndRetainsProvisionalMember(t *testing.T) {
	t.Parallel()
	testIntentUnassignedReplay(t, "", true)
}

func testIntentUnassignedReplay(t *testing.T, legacySubject string, frozen bool) {
	t.Helper()
	ctx := context.Background()
	f := newCaptureFixture(t)
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "retry.go", "package app\nfunc RetrySpeech() bool { return false }\n")
	writePublicationFile(t, f, "retry_test.go", "package app\nimport \"testing\"\nfunc TestRetrySpeech(t *testing.T) { if RetrySpeech() { t.Fatal(\"unsupported retry\") } }\n")
	if !frozen {
		// A normal window starts with fresh work and already includes the
		// later protected member; legacy repair must not widen the window.
		if first := capturePublicationFiles(t, f); !first.Protected {
			t.Fatalf("initial capture not protected: %+v", first)
		}
	}
	writePublicationFile(t, f, "release_checklist.md", "# Release readiness checks\n")
	protected := capturePublicationFiles(t, f)
	if !protected.Protected {
		t.Fatalf("capture not protected: %+v", protected)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 3 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	var selected, target []int64
	var omitted int64
	for _, event := range pending {
		target = append(target, event.Seq)
		if event.Path == "release_checklist.md" {
			omitted = event.Seq
		} else {
			selected = append(selected, event.Seq)
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i] < selected[j] })
	now := float64(time.Now().Unix())
	unknown := ai.IntentCandidateAssignment{CandidateID: "previous-unclassified-doc", SelectedSeqs: []int64{omitted},
		Purpose: "retain dependency component until its goal is known", Readiness: ai.IntentCandidateWait,
		MissingCompanions: []string{unclassifiedIntentCompanion}, GroupingReason: "bounded fallback requires planner review", Subject: legacySubject}
	if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{
		ID: unknown.CandidateID, BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		Status: state.IntentCandidateWaiting, Readiness: state.IntentReadinessWait, Purpose: unknown.Purpose,
		MissingCompanions: unclassifiedIntentCompanion, AtomicityStatus: sql.NullString{String: "pending", Valid: true},
		VerificationStatus: sql.NullString{String: "not_required", Valid: true}, CreatedTS: now, UpdatedTS: now,
		Events: []state.IntentCandidateEvent{{EventSeq: omitted, EventRole: "documentation"}},
	}); err != nil {
		t.Fatal(err)
	}
	old, err := state.EnsureIntentPlanRun(ctx, f.db, state.IntentPlanRun{
		Fingerprint: "sha256:" + strings.Repeat("a", 64), BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration, AttemptLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	old.Completed = true
	old.ProgressState = sql.NullString{String: "waiting_semantic_retry", Valid: true}
	old.ResolutionMode = old.ProgressState
	old.UnresolvedSeqs = []int64{omitted}
	if err := storeResolvedIntentPlanRun(&old, ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{unknown}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := state.UpdateIntentPlanRun(ctx, f.db, old); err != nil {
		t.Fatal(err)
	}
	ready := ai.IntentCandidateAssignment{CandidateID: "safe-speech-retry", SelectedSeqs: selected,
		Purpose: "reject unsupported speech recognition retries", Readiness: ai.IntentCandidateReady,
		Subject: "Reject unsupported speech recognition retries", Body: "- Keep the recognition guard and its direct regression complete",
		GroupingReason: "the changed RetrySpeech implementation and its direct test establish one complete behavior"}
	planner := &semanticRetryReplayPlanner{intentCandidatePlannerStub: intentCandidatePlannerStub{
		plan: ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{ready}},
	}}
	drain := state.PublicationDrain{ID: "frozen-omitted-provisional", CheckpointID: protected.CheckpointID, WorktreeID: checkpoint.WorktreeID(f.dir),
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration, CommitStrategy: "intent", CommitFormat: "imperative",
		Provider: planner.Name(), ProviderFingerprint: "sha256:" + strings.Repeat("0", 64), Phase: state.PublicationDrainSemantic,
		TargetEventCount: 3, EventSeqs: target, CreatedTS: now, UpdatedTS: now, LastProgressTS: now}
	if frozen {
		if _, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil {
			t.Fatal(err)
		}
	}
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	before := revListCount(t, ctx, f.dir, "HEAD")
	opts := ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner,
		IntentPreset: config.PresetBalanced, IntentWindow: 20, IntentMinPending: 1, IntentBypassBatchWait: true,
		IntentIncludeDiffs: true, IntentVerificationMode: "structural"}
	if frozen {
		opts.PublicationDrain = &drain
	}
	first, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || first.Published != 2 || first.Failed != 0 || planner.calls != 1 || revListCount(t, ctx, f.dir, "HEAD") != before+1 {
		t.Fatalf("omission discarded a complete goal: summary=%+v calls=%d err=%v", first, planner.calls, err)
	}
	for _, candidate := range planner.req.Candidates {
		if candidate.CandidateID == unknown.CandidateID {
			t.Fatal("provisional ownership was not safely released before provider planning")
		}
	}
	var offered []int64
	for _, capture := range planner.req.OfferedCaptures {
		offered = append(offered, capture.Seq)
	}
	if !reflect.DeepEqual(offered, target) {
		t.Fatalf("repair changed the frozen offer: %v want %v", offered, target)
	}
	if message := strings.TrimSpace(mustGitOutput(t, f.dir, "log", "-1", "--format=%s")); message != ready.Subject {
		t.Fatalf("completion changed the goal message: %q", message)
	}
	pending, err = state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Seq != omitted {
		t.Fatalf("omitted capture was lost or published: %+v err=%v", pending, err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "cat-file", "-e", "HEAD:release_checklist.md"); err == nil {
		t.Fatal("WAIT capture leaked into the ready commit")
	}
	var waitingID string
	if err := f.db.ReadSQL().QueryRowContext(ctx, "SELECT candidate_id FROM intent_candidate_events WHERE event_seq=? AND membership_state='active'", omitted).Scan(&waitingID); err != nil {
		t.Fatal(err)
	}
	waiting, found, err := state.IntentCandidateByID(ctx, f.db, waitingID)
	if err != nil || !found || waiting.Status != state.IntentCandidateWaiting || waiting.Readiness != state.IntentReadinessWait {
		t.Fatalf("omission is not durably waiting: %+v found=%t err=%v", waiting, found, err)
	}
	previous, found, err := state.IntentCandidateByID(ctx, f.db, unknown.CandidateID)
	if err != nil || !found || previous.Status != state.IntentCandidateSuperseded {
		t.Fatalf("provisional boundary survived safe reassignment: %+v found=%t err=%v", previous, found, err)
	}
	if history, err := state.IntentCandidateEventHistory(ctx, f.db, unknown.CandidateID); err != nil || len(history) != 1 || history[0].EventSeq != omitted {
		t.Fatalf("legacy membership provenance was lost: %+v err=%v", history, err)
	}
	if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
		t.Fatalf("safe omitted work requires action: %t err=%v", attention, err)
	}
	if head == first.BaseHead {
		t.Fatal("complete goal did not move HEAD")
	}
	if frozen {
		progress, err := UpdatePublicationDrainAfterReplay(ctx, f.db, drain, first, nil, time.Now())
		if err != nil || progress.PublishedEventCount != 2 || progress.TargetEventCount != 3 ||
			!reflect.DeepEqual(progress.EventSeqs, target) || progress.Phase == state.PublicationDrainNeedsAction {
			t.Fatalf("visible frozen progress lost the protected omission: %+v err=%v", progress, err)
		}
	}
	review, found, err := loadIntentSemanticRetry(ctx, f.db)
	if err != nil || !found || review.ReviewCount != 1 || review.RetryAtTS-review.ScheduledAtTS != (5*time.Minute).Seconds() {
		t.Fatalf("omission has no bounded goal review: %+v found=%t err=%v", review, found, err)
	}
}

func TestIntentUnassignedCompletionCannotSplitAvailableTest(t *testing.T) {
	t.Parallel()
	req, err := ai.NewIntentPlanRequestV2(ai.IntentPlanRequestV2Options{
		IncludeCapturedDiffs: true,
		OfferedCaptures: []ai.OfferedCapture{
			{Seq: 1, Path: "retry.go", Op: "create", CapturedDiff: "+package app\n+func RetrySpeech() bool { return false }\n"},
			{Seq: 2, Path: "retry_test.go", Op: "create", CapturedDiff: "+package app\n+import \"testing\"\n+func TestRetrySpeech(t *testing.T) { if RetrySpeech() { t.Fatal(\"unsupported retry\") } }\n"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: "retry-guard", SelectedSeqs: []int64{1}, Purpose: "reject unsupported speech recognition retries", Readiness: ai.IntentCandidateReady,
		Subject: "Reject unsupported speech recognition retries", Body: "- Reject unsupported restart attempts", GroupingReason: "the recognition retry guard changes",
	}}}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := ai.DecodeIntentPlanV2(raw, req)
	if err != nil || len(completed.Candidates) != 2 || completed.Candidates[1].Readiness != ai.IntentCandidateWait {
		t.Fatalf("omitted test is not retained: %+v err=%v", completed, err)
	}
	if err := ValidateIntentGoalPlan(req, completed); err == nil || !strings.Contains(err.Error(), "available_companion_split") {
		t.Fatalf("WAIT completion waived available implementation/test completeness: %v", err)
	}
}
