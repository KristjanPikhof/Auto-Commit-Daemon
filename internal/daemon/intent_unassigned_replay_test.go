package daemon

import (
	"context"
	"database/sql"
	"fmt"
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
	ctx := context.Background()
	f := newCaptureFixture(t)
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "retry.go", "package app\nfunc RetrySpeech() bool { return false }\n")
	writePublicationFile(t, f, "retry_test.go", "package app\nimport \"testing\"\nfunc TestRetrySpeech(t *testing.T) { if RetrySpeech() { t.Fatal(\"unsupported retry\") } }\n")
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
		MissingCompanions: []string{unclassifiedIntentCompanion}, GroupingReason: "bounded fallback requires planner review"}
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
	if _, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil {
		t.Fatal(err)
	}
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	before := revListCount(t, ctx, f.dir, "HEAD")
	opts := ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner,
		IntentPreset: config.PresetBalanced, IntentWindow: 20, IntentMinPending: 1, IntentBypassBatchWait: true,
		IntentIncludeDiffs: true, IntentVerificationMode: "structural", PublicationDrain: &drain}
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
	waiting, found, err := state.IntentCandidateByID(ctx, f.db, fmt.Sprintf("host-wait-%d", omitted))
	if err != nil || !found || waiting.Status != state.IntentCandidateWaiting || waiting.Readiness != state.IntentReadinessWait {
		t.Fatalf("omission is not durably waiting: %+v found=%t err=%v", waiting, found, err)
	}
	if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
		t.Fatalf("safe omitted work requires action: %t err=%v", attention, err)
	}
	if head == first.BaseHead {
		t.Fatal("complete goal did not move HEAD")
	}
	if outcome, err := state.ReadPublicationOutcome(ctx, f.db.ReadSQL(), f.cctx.BranchRef, f.cctx.BranchGeneration); err != nil || outcome.WaitingChanges != 1 || outcome.BranchChanges != 2 || outcome.BranchCommitted {
		t.Fatalf("visible queue outcome lost the protected omission: %+v err=%v", outcome, err)
	}
}
