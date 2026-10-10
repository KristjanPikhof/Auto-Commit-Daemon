package daemon

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type frozenRemainderPlanner struct {
	t       *testing.T
	seqs    []int64
	calls   int
	request ai.IntentPlanRequestV2
}

func (*frozenRemainderPlanner) Name() string { return "frozen-remainder-test" }
func (*frozenRemainderPlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("native v2 required")
}
func (p *frozenRemainderPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.calls++
	p.request = req
	if req.ForcedAging || !reflect.DeepEqual(offeredIntentSeqs(req), p.seqs) {
		p.t.Fatalf("complete frozen goal narrowed or widened: offered=%v forced=%t want=%v", offeredIntentSeqs(req), req.ForcedAging, p.seqs)
	}
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: "complete-value-goal", SelectedSeqs: p.seqs, Readiness: ai.IntentCandidateReady,
		Purpose: "return and verify the updated source value", Subject: "Return the updated source value",
		Body:           "- Keep the source implementation and matching regression together",
		GroupingReason: "the implementation and its regression complete the same behavior",
	}}}, nil
}

func TestReplayIntentFrozenRemainderReoffersSeparateWaitingGoals(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newCaptureFixture(t)
	seedTrackedFileCommit(t, ctx, f, "source.go", "package source\nfunc Value() int { return 1 }\n")
	seedTrackedFileCommit(t, ctx, f, "source_test.go", "package source\nimport \"testing\"\nfunc TestValue(t *testing.T) { if Value() != 1 { t.Fatal(Value()) } }\n")
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "source.go", "package source\nfunc Value() int { return 2 }\n")
	writePublicationFile(t, f, "source_test.go", "package source\nimport \"testing\"\nfunc TestValue(t *testing.T) { if Value() != 2 { t.Fatal(Value()) } }\n")
	protection := capturePublicationFiles(t, f)
	if !protection.Protected {
		t.Fatalf("target not protected: %+v", protection)
	}
	pending, err := state.PublishableEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 2 {
		t.Fatalf("eligible target=%+v err=%v", pending, err)
	}
	var seqs []int64
	for _, event := range pending {
		seqs = append(seqs, event.Seq)
		candidate := state.IntentCandidate{ID: "waiting-" + event.Path,
			BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
			Status: state.IntentCandidateWaiting, Readiness: state.IntentReadinessWait,
			Purpose:            "complete the updated source value with its regression",
			MissingCompanions:  "the other captured implementation or regression is not offered",
			AtomicityStatus:    sql.NullString{String: string(ai.IntentAtomicityPending), Valid: true},
			VerificationStatus: sql.NullString{String: "not_required", Valid: true},
			Events:             []state.IntentCandidateEvent{{EventSeq: event.Seq, EventRole: "code"}},
		}
		if intentCandidateNeedsSemanticReview(candidate) {
			t.Fatal("fixture accidentally used a rejected READY instead of valid WAIT")
		}
		if err := state.SaveIntentCandidate(ctx, f.db, candidate); err != nil {
			t.Fatal(err)
		}
		if err := state.RecordPlannerDefer(ctx, f.db, event.Seq, 1, "waiting for the other captured companion"); err != nil {
			t.Fatal(err)
		}
	}
	cfg := intentReplayConfig{candidateMode: true, window: 2, deferLimit: 1, targetEventSeqs: seqs}
	window, forced, reason, err := selectIntentWindow(ctx, f.db, pending, cfg)
	if err != nil || forced || reason != "" || !reflect.DeepEqual(window, pending) {
		t.Fatalf("aged WAIT owners still offered separately: window=%+v forced=%t reason=%q err=%v", window, forced, reason, err)
	}
	ts := intentPlannerHealthTimestamp(time.Now())
	drain := state.PublicationDrain{ID: "separate-wait-frozen-remainder", CheckpointID: protection.CheckpointID,
		WorktreeID: checkpoint.WorktreeID(f.dir), BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		CommitStrategy: "intent", CommitFormat: "imperative", Provider: "frozen-remainder-test",
		ProviderFingerprint: "sha256:" + strings.Repeat("0", 64), Phase: state.PublicationDrainSemantic,
		TargetEventCount: 2, EventSeqs: seqs, CreatedTS: ts, UpdatedTS: ts, LastProgressTS: ts}
	if _, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "later.md", "# Later independent work\n")
	mustGitOutput(t, f.dir, "add", "later.md")
	staging := mustGitOutput(t, f.dir, "diff", "--cached")
	if later := capturePublicationFiles(t, f); !later.Protected {
		t.Fatalf("later capture unprotected: %+v", later)
	}
	planner := &frozenRemainderPlanner{t: t, seqs: seqs}
	before := revListCount(t, ctx, f.dir, "HEAD")
	result, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir,
		CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner, IntentPreset: config.PresetBalanced,
		IntentIncludeDiffs: true, IntentWindow: 2, IntentDeferLimit: 1, IntentBypassBatchWait: true,
		IntentVerificationMode: "structural", RequireCompletedCheckpoint: true, PublicationDrain: &drain})
	if err != nil || result.Published != 2 || result.Failed != 0 || result.Disposition == ReplayDispositionNeedsAttention ||
		planner.calls != 1 || revListCount(t, ctx, f.dir, "HEAD") != before+1 {
		t.Fatalf("complete frozen source goal did not publish: result=%+v calls=%d err=%v", result, planner.calls, err)
	}
	if got := strings.TrimSpace(mustGitOutput(t, f.dir, "log", "-1", "--format=%s")); got != "Return the updated source value" {
		t.Fatalf("publication lost purposeful message: %q", got)
	}
	if got := strings.Fields(mustGitOutput(t, f.dir, "show", "--format=", "--name-only", "HEAD")); !reflect.DeepEqual(got, []string{"source.go", "source_test.go"}) {
		t.Fatalf("commit crossed frozen goal: %v", got)
	}
	progress, err := UpdatePublicationDrainAfterReplay(ctx, f.db, drain, result, nil, time.Now())
	if err != nil || progress.Phase != state.PublicationDrainCompleted || progress.PublishedEventCount != 2 || !reflect.DeepEqual(progress.EventSeqs, seqs) {
		t.Fatalf("frozen target did not complete unchanged: %+v err=%v", progress, err)
	}
	remaining, err := state.PublishableEvents(ctx, f.db, 0)
	if err != nil || len(remaining) != 1 || remaining[0].Path != "later.md" {
		t.Fatalf("later protected capture was consumed: %+v err=%v", remaining, err)
	}
	completed, err := state.PublicationDrainByID(ctx, f.db, drain.ID)
	if err != nil || completed.Phase != state.PublicationDrainCompleted || completed.PublishedEventCount != 2 || !reflect.DeepEqual(completed.EventSeqs, seqs) {
		t.Fatalf("durable drain projection stayed incomplete: %+v err=%v", completed, err)
	}
	if err := updateIntentV2EvaluationMeta(ctx, f.db, &RuntimeBundle{PresetID: "intent.balanced", PresetVersion: config.PresetCatalogVersion}, result, nil); err != nil {
		t.Fatal(err)
	}
	if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
		t.Fatalf("published WAIT owners still require user action: attention=%t err=%v", attention, err)
	}
	if got := mustGitOutput(t, f.dir, "diff", "--cached"); got != staging {
		t.Fatalf("user staging changed: before=%q after=%q", staging, got)
	}
	if body, err := os.ReadFile(filepath.Join(f.dir, "later.md")); err != nil || string(body) != "# Later independent work\n" {
		t.Fatalf("later worktree changed: %q err=%v", body, err)
	}
	for _, event := range pending {
		var owners int
		if err := f.db.ReadSQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM intent_candidate_events WHERE event_seq=? AND membership_state='active'", event.Seq).Scan(&owners); err != nil || owners != 1 {
			t.Fatalf("capture %d ownership=%d err=%v", event.Seq, owners, err)
		}
	}
}

func TestIntentFrozenWindowRequiresCompleteProtectedMembership(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newCaptureFixture(t)
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"first.md", "second.md"} {
		writePublicationFile(t, f, path, "# Protected independent work\n")
	}
	if captured := capturePublicationFiles(t, f); !captured.Protected {
		t.Fatalf("fixture unprotected: %+v", captured)
	}
	pending, err := state.PublishableEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 2 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	target := []int64{pending[0].Seq, pending[1].Seq}
	if complete, err := completeProtectedIntentFrozenWindow(ctx, f.db, pending, target); err != nil || !complete {
		t.Fatalf("complete protected target refused: complete=%t err=%v", complete, err)
	}
	for _, resolved := range []string{state.EventStatePublished, state.EventStateRecovered} {
		if _, err := f.db.SQL().ExecContext(ctx, "UPDATE capture_events SET state=? WHERE seq=?", resolved, target[1]); err != nil {
			t.Fatal(err)
		}
		if complete, err := completeProtectedIntentFrozenWindow(ctx, f.db, pending[:1], target); err != nil || !complete {
			t.Fatalf("resolved %s member prevented complete remainder: complete=%t err=%v", resolved, complete, err)
		}
		if _, err := f.db.SQL().ExecContext(ctx, "UPDATE capture_events SET state='pending' WHERE seq=?", target[1]); err != nil {
			t.Fatal(err)
		}
	}
	for name, seqs := range map[string][]int64{
		"missing target row": {target[0], target[1], target[1] + 1000},
		"later capture":      {target[0]},
		"duplicate target":   {target[0], target[1], target[1]},
	} {
		t.Run(name, func(t *testing.T) {
			if complete, err := completeProtectedIntentFrozenWindow(ctx, f.db, pending, seqs); err != nil || complete {
				t.Fatalf("unsafe membership accepted: complete=%t err=%v", complete, err)
			}
		})
	}
	if complete, err := completeProtectedIntentFrozenWindow(ctx, f.db, pending[:1], target); err != nil || complete {
		t.Fatalf("gated/uncheckpointed remainder accepted: complete=%t err=%v", complete, err)
	}
	for _, event := range pending {
		if err := state.RecordPlannerDefer(ctx, f.db, event.Seq, 1, "aged captured work"); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("aged singleton retains forced review", func(t *testing.T) {
		window, forced, _, err := selectIntentWindow(ctx, f.db, pending[:1], intentReplayConfig{candidateMode: true, window: 2, deferLimit: 1, targetEventSeqs: target[:1]})
		if err != nil || !forced || !reflect.DeepEqual(window, pending[:1]) {
			t.Fatalf("complete singleton lost forced review: window=%+v forced=%t err=%v", window, forced, err)
		}
	})
	window, forced, _, err := selectIntentWindow(ctx, f.db, pending, intentReplayConfig{candidateMode: true, window: 1, deferLimit: 1, targetEventSeqs: target})
	if err != nil || !forced || len(window) != 1 {
		t.Fatalf("window cap was widened: window=%+v forced=%t err=%v", window, forced, err)
	}
	for _, update := range []string{
		"UPDATE capture_events SET branch_generation=2 WHERE seq=?",
		"UPDATE capture_events SET state='blocked_conflict' WHERE seq=?",
	} {
		if _, err := f.db.SQL().ExecContext(ctx, update, target[1]); err != nil {
			t.Fatal(err)
		}
		if complete, err := completeProtectedIntentFrozenWindow(ctx, f.db, pending, target); err != nil || complete {
			t.Fatalf("stale or unsafe member accepted: complete=%t err=%v", complete, err)
		}
		if _, err := f.db.SQL().ExecContext(ctx, "UPDATE capture_events SET branch_generation=?,state='pending' WHERE seq=?", f.cctx.BranchGeneration, target[1]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.db.SQL().ExecContext(ctx, "DELETE FROM checkpoint_events WHERE event_seq=?", target[1]); err != nil {
		t.Fatal(err)
	}
	if complete, err := completeProtectedIntentFrozenWindow(ctx, f.db, pending, target); err != nil || complete {
		t.Fatalf("unprotected target member accepted: complete=%t err=%v", complete, err)
	}
	oversized := make([]int64, ai.IntentCandidateCaptureCap+1)
	if complete, err := completeProtectedIntentFrozenWindow(ctx, nil, pending, oversized); err != nil || complete {
		t.Fatalf("target cap exceeded: complete=%t err=%v", complete, err)
	}
}
