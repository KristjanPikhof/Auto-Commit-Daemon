package daemon

import (
	"context"
	"os"
	"path/filepath"
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

func TestIntentForcedSingletonReoffersProtectedGoalAndPublishesFrozenTarget(t *testing.T) {
	t.Parallel()
	f, input, _, _, rejected := semanticRejectedFixture(t)
	ctx := context.Background()
	if err := state.SaveIntentCandidate(ctx, f.db, rejected.Candidate); err != nil {
		t.Fatal(err)
	}
	for _, capture := range input.Captures {
		if err := state.RecordPlannerDefer(ctx, f.db, capture.Event.Seq, 1, "captured relationships need a grounded goal review"); err != nil {
			t.Fatal(err)
		}
	}
	var checkpointID string
	if err := f.db.ReadSQL().QueryRowContext(ctx, "SELECT checkpoint_id FROM checkpoint_events WHERE event_seq=?", input.Captures[0].Event.Seq).Scan(&checkpointID); err != nil {
		t.Fatal(err)
	}
	ts := intentPlannerHealthTimestamp(time.Now())
	drain := state.PublicationDrain{
		ID: "forced-reoffer-frozen-goals", CheckpointID: checkpointID, WorktreeID: checkpoint.WorktreeID(f.dir),
		BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration,
		CommitStrategy: "intent", CommitFormat: "imperative", Provider: "intent-v2-test",
		ProviderFingerprint: "sha256:" + strings.Repeat("0", 64),
		Phase:               state.PublicationDrainSemantic, TargetEventCount: 2, EventSeqs: input.TargetEventSeqs,
		CreatedTS: ts, UpdatedTS: ts, LastProgressTS: ts,
	}
	if _, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "later.md", "# Later capture remains protected\n")
	if protection := capturePublicationFiles(t, f); !protection.Protected {
		t.Fatalf("later capture unprotected: %+v", protection)
	}
	// Confirm the original overdue selection is still a singleton; the
	// candidate evaluator must safely reoffer its other protected member.
	selected, forced, _, err := selectIntentWindow(ctx, f.db, []state.CaptureEvent{input.Captures[0].Event, input.Captures[1].Event}, intentReplayConfig{window: 1, deferLimit: 1, candidateMode: true, targetEventSeqs: input.TargetEventSeqs})
	if err != nil || !forced || len(selected) != 1 {
		t.Fatalf("original forced selection=%+v forced=%t err=%v", selected, forced, err)
	}
	planner := &semanticRetryReplayPlanner{intentCandidatePlannerStub: intentCandidatePlannerStub{plan: correctedSemanticRejectedPlan(input)}}
	head := mustGitOutput(t, f.dir, "rev-parse", "HEAD")
	published, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner,
		IntentPlannerProvider: planner.Name(), IntentPreset: config.PresetBalanced,
		IntentWindow: 1, IntentDeferLimit: 1, IntentBypassBatchWait: true,
		IntentIncludeDiffs: true, IntentVerificationMode: "structural", PublicationDrain: &drain,
	})
	if err != nil || published.Published != 2 || published.Failed != 0 || published.Disposition == ReplayDispositionNeedsAttention || planner.calls != 1 {
		t.Fatalf("protected reoffer did not publish: %+v calls=%d err=%v", published, planner.calls, err)
	}
	var offered []int64
	for _, capture := range planner.req.OfferedCaptures {
		offered = append(offered, capture.Seq)
	}
	if planner.req.ForcedAging || !reflect.DeepEqual(offered, input.TargetEventSeqs) {
		t.Fatalf("expanded planning request retained singleton constraint or changed target: forced=%t offered=%v want=%v", planner.req.ForcedAging, offered, input.TargetEventSeqs)
	}
	progress, err := UpdatePublicationDrainAfterReplay(ctx, f.db, drain, published, nil, time.Now())
	if err != nil || progress.Phase != state.PublicationDrainCompleted || progress.PublishedEventCount != 2 || !reflect.DeepEqual(progress.EventSeqs, input.TargetEventSeqs) {
		t.Fatalf("frozen publication did not complete: %+v err=%v", progress, err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Path != "later.md" {
		t.Fatalf("later capture was consumed: %+v err=%v", pending, err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "cat-file", "-e", "HEAD:later.md"); err == nil {
		t.Fatal("later capture escaped the frozen target")
	}
	if body, err := os.ReadFile(filepath.Join(f.dir, "later.md")); err != nil || string(body) != "# Later capture remains protected\n" {
		t.Fatalf("publication changed later work: %q err=%v", body, err)
	}
	if mustGitOutput(t, f.dir, "rev-parse", "HEAD") == head {
		t.Fatal("publication failed to advance HEAD")
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "diff", "--cached", "--exit-code"); err != nil {
		t.Fatalf("publication changed user staging: %v", err)
	}
	if history, err := state.IntentCandidateEventHistory(ctx, f.db, rejected.Candidate.ID); err != nil || len(history) != 2 {
		t.Fatalf("rejected membership provenance lost: %+v err=%v", history, err)
	}
	if err := updateIntentV2EvaluationMeta(ctx, f.db, &RuntimeBundle{PresetID: "intent.balanced", PresetVersion: config.PresetCatalogVersion}, published, nil); err != nil {
		t.Fatal(err)
	}
	if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
		t.Fatalf("status/list still require action: %t err=%v", attention, err)
	}
	if got := strings.TrimSpace(mustGitOutput(t, f.dir, "log", "-2", "--format=%s")); got != "Preserve the selected translation locale\nRestore speech recognition after pauses" {
		t.Fatalf("reoffer did not publish purposeful goals: %q", got)
	}
}

func TestIntentReofferPreservesForcedSingletonOnRefusedExpansion(t *testing.T) {
	t.Parallel()
	first := IntentCandidateCapture{Event: state.CaptureEvent{Seq: 1, BranchRef: "refs/heads/main", BranchGeneration: 1, State: state.EventStatePending}}
	second := first
	second.Event.Seq = 2
	candidate := state.IntentCandidate{Events: []state.IntentCandidateEvent{{EventSeq: 1}, {EventSeq: 2}}}
	input := IntentCandidateEvaluation{BranchRef: first.Event.BranchRef, BranchGeneration: 1, Captures: []IntentCandidateCapture{first}, TargetEventSeqs: []int64{1}, ForcedAging: true}
	if reofferIntentCandidateMembers(&input, candidate, []IntentCandidateCapture{first, second}) || !input.ForcedAging || len(input.Captures) != 1 {
		t.Fatalf("refused target expansion changed singleton: %+v", input)
	}
	candidate.Events = candidate.Events[:1]
	if !reofferIntentCandidateMembers(&input, candidate, []IntentCandidateCapture{first}) || !input.ForcedAging || len(input.Captures) != 1 {
		t.Fatalf("unchanged singleton lost forced aging: %+v", input)
	}
}
