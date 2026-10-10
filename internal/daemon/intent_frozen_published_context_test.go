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
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type frozenPublishedDocPlanner struct {
	requests  []ai.IntentPlanRequestV2
	finishDoc bool
}

func (*frozenPublishedDocPlanner) Name() string { return "frozen-published-doc-test" }
func (*frozenPublishedDocPlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	panic("native v2 planner required")
}
func (p *frozenPublishedDocPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.requests = append(p.requests, req)
	implementation, regression := false, false
	for _, candidate := range req.Candidates {
		if candidate.Status != state.IntentCandidatePublished {
			continue
		}
		for _, capture := range candidate.CapturedEvidence {
			implementation = implementation || strings.Contains(capture.CapturedDiff, "func FrozenRemainderReady() bool { return true }")
			regression = regression || strings.Contains(capture.CapturedDiff, "func TestFrozenRemainder(t *testing.T)") && strings.Contains(capture.CapturedDiff, "!FrozenRemainderReady()")
		}
	}
	code := ai.IntentCandidateAssignment{CandidateID: "complete-frozen-review", Purpose: "complete and verify small frozen target remainders", Readiness: ai.IntentCandidateReady,
		Subject: "Complete frozen target reviews", Body: "- Keep the remainder implementation and its focused regression together", GroupingReason: "the implementation and its direct regression complete one behavior"}
	doc := ai.IntentCandidateAssignment{CandidateID: "document-frozen-review", Purpose: "document review of small frozen target remainders", Readiness: ai.IntentCandidateWait,
		MissingCompanions: []string{"the implementation and focused regression described by this documentation were not supplied"}, GroupingReason: "this documentation describes the frozen review behavior"}
	for _, capture := range req.OfferedCaptures {
		if capture.Path == "docs/review.md" {
			doc.SelectedSeqs = append(doc.SelectedSeqs, capture.Seq)
		} else {
			code.SelectedSeqs = append(code.SelectedSeqs, capture.Seq)
		}
	}
	if p.finishDoc && implementation && regression {
		doc.Readiness, doc.MissingCompanions = ai.IntentCandidateReady, nil
		doc.Subject = "Document completed frozen target reviews"
		doc.Body = "- Explain the published remainder behavior without consuming later work"
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	if len(code.SelectedSeqs) > 0 {
		plan.Candidates = append(plan.Candidates, code)
	}
	if len(doc.SelectedSeqs) > 0 {
		plan.Candidates = append(plan.Candidates, doc)
	}
	return plan, nil
}

func frozenPublishedDocFixture(t *testing.T) (*captureFixture, state.PublicationDrain, state.CaptureEvent) {
	t.Helper()
	ctx := context.Background()
	f := newCaptureFixture(t)
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "review.go", "package review\nfunc FrozenRemainderReady() bool { return true }\n")
	writePublicationFile(t, f, "review_test.go", "package review\nimport \"testing\"\nfunc TestFrozenRemainder(t *testing.T) { if !FrozenRemainderReady() { t.Fatal(\"remainder was not ready\") } }\n")
	if err := os.Mkdir(filepath.Join(f.dir, "docs"), 0755); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "docs/review.md", "# Frozen target reviews\n\nACD reviews a small frozen remainder together. Later work stays protected for the next target.\n")
	protection := capturePublicationFiles(t, f)
	if !protection.Protected {
		t.Fatalf("target not protected: %+v", protection)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 3 {
		t.Fatalf("captures=%+v err=%v", pending, err)
	}
	var doc state.CaptureEvent
	var seqs []int64
	for _, event := range pending {
		seqs = append(seqs, event.Seq)
		if event.Path == "docs/review.md" {
			doc = event
		}
	}
	stamp := intentPlannerHealthTimestamp(time.Now())
	planner := &frozenPublishedDocPlanner{}
	drain := state.PublicationDrain{ID: "frozen-published-doc", CheckpointID: protection.CheckpointID, WorktreeID: checkpoint.WorktreeID(f.dir), BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		CommitStrategy: "intent", CommitFormat: "imperative", Provider: planner.Name(), ProviderFingerprint: "sha256:" + strings.Repeat("0", 64), Phase: state.PublicationDrainSemantic,
		TargetEventCount: 3, EventSeqs: seqs, CreatedTS: stamp, UpdatedTS: stamp, LastProgressTS: stamp}
	if _, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil {
		t.Fatal(err)
	}
	result, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner, IntentPreset: config.PresetBalanced,
		IntentIncludeDiffs: true, IntentWindow: 3, IntentBypassBatchWait: true, IntentVerificationMode: "structural", RequireCompletedCheckpoint: true, PublicationDrain: &drain})
	if err != nil || result.Published != 2 || result.Failed != 0 || result.Disposition == ReplayDispositionNeedsAttention {
		t.Fatalf("implementation/test prefix did not publish: %+v err=%v", result, err)
	}
	f.cctx.BaseHead = result.BaseHead
	drain, err = UpdatePublicationDrainAfterReplay(ctx, f.db, drain, result, nil, time.Now())
	if err != nil || drain.PublishedEventCount != 2 || drain.Phase == state.PublicationDrainCompleted {
		t.Fatalf("documentation wait lost its target: %+v err=%v", drain, err)
	}
	doc, err = loadIntentCaptureEvent(ctx, f.db, doc.Seq)
	if err != nil || doc.State != state.EventStatePending {
		t.Fatalf("documentation was not retained: %+v err=%v", doc, err)
	}
	return f, drain, doc
}

func TestIntentFrozenPublishedBaselineCompletesDocumentationAfterRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, drain, doc := frozenPublishedDocFixture(t)
	var baselineID string
	if err := f.db.ReadSQL().QueryRowContext(ctx, `
SELECT owner.candidate_id FROM intent_candidate_events owner
JOIN capture_events event ON event.seq=owner.event_seq
WHERE owner.membership_state='active' AND event.path='review.go'
 AND event.branch_ref=? AND event.branch_generation=? AND event.state='published'`,
		f.cctx.BranchRef, f.cctx.BranchGeneration).Scan(&baselineID); err != nil {
		t.Fatal(err)
	}
	baseline, found, err := state.IntentCandidateByID(ctx, f.db, baselineID)
	if err != nil || !found || baseline.Status != state.IntentCandidatePublished || len(baseline.Events) != 2 || !baseline.PublishedCommitOID.Valid || baseline.PublishedCommitOID.String != f.cctx.BaseHead {
		t.Fatalf("prefix is not published baseline: %+v err=%v", baseline, err)
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
	writePublicationFile(t, f, "later.md", "# Later independent work\n")
	mustGitOutput(t, f.dir, "add", "later.md")
	staging := mustGitOutput(t, f.dir, "diff", "--cached", "--binary")
	if protection := capturePublicationFiles(t, f); !protection.Protected {
		t.Fatal("later staged work is not protected")
	}
	planner := &frozenPublishedDocPlanner{finishDoc: true}
	before := revListCount(t, ctx, f.dir, "HEAD")
	result, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner, IntentPreset: config.PresetBalanced,
		IntentIncludeDiffs: true, IntentWindow: 1, IntentBypassBatchWait: true, IntentVerificationMode: "structural", RequireCompletedCheckpoint: true, PublicationDrain: &drain})
	if err != nil || result.Published != 1 || result.Failed != 0 || result.Disposition == ReplayDispositionNeedsAttention || len(planner.requests) != 1 || revListCount(t, ctx, f.dir, "HEAD") != before+1 {
		t.Fatalf("published support did not complete documentation: %+v calls=%d err=%v", result, len(planner.requests), err)
	}
	req := planner.requests[0]
	if !reflect.DeepEqual(offeredIntentSeqs(req), []int64{doc.Seq}) {
		t.Fatalf("baseline or later work became selectable: %v", offeredIntentSeqs(req))
	}
	for _, dependency := range req.Dependencies {
		if dependency.FromSeq != doc.Seq && dependency.ToSeq != doc.Seq {
			continue
		}
		// Existing proximity metadata is not used as proof of causal cohesion.
		if dependency.Strength == ai.IntentDependencySoft {
			switch dependency.Kind {
			case "activity_epoch", "temporal_proximity", "module_proximity":
				continue
			}
		}
		t.Fatalf("prose acquired a fabricated dependency: %+v", dependency)
	}
	current, _, err := state.IntentCandidateByID(ctx, f.db, baseline.ID)
	if err != nil || !reflect.DeepEqual(current, baseline) {
		t.Fatalf("published provenance changed: %+v err=%v", current, err)
	}
	if got := strings.TrimSpace(mustGitOutput(t, f.dir, "log", "-1", "--format=%s")); got != "Document completed frozen target reviews" {
		t.Fatalf("documentation lost its purpose: %q", got)
	}
	completed, err := UpdatePublicationDrainAfterReplay(ctx, f.db, drain, result, nil, time.Now())
	if err != nil || completed.Phase != state.PublicationDrainCompleted || completed.PublishedEventCount != 3 || !reflect.DeepEqual(completed.EventSeqs, drain.EventSeqs) {
		t.Fatalf("frozen target did not complete unchanged: %+v err=%v", completed, err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Path != "later.md" {
		t.Fatalf("later work escaped target: %+v err=%v", pending, err)
	}
	if got := mustGitOutput(t, f.dir, "diff", "--cached", "--binary"); got != staging {
		t.Fatal("documentation publication changed staging")
	}
	if got, err := os.ReadFile(filepath.Join(f.dir, "later.md")); err != nil || string(got) != "# Later independent work\n" {
		t.Fatalf("later worktree changed: %q err=%v", got, err)
	}
	if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
		t.Fatalf("status/list incorrectly requires action: attention=%t err=%v", attention, err)
	}
}

func TestIntentFrozenPublishedBaselineRejectsUnprovedAndExpandedContext(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, drain, doc := frozenPublishedDocFixture(t)
	ops, err := state.LoadCaptureOps(ctx, f.db, doc.Seq)
	if err != nil {
		t.Fatal(err)
	}
	input := IntentCandidateEvaluation{RepoPath: f.dir, BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration, IncludeDiffs: true,
		LatestCommit: &ai.CommitSummary{OID: f.cctx.BaseHead}, TargetEventSeqs: drain.EventSeqs, Captures: []IntentCandidateCapture{{Event: doc, Ops: ops}}}
	for _, name := range []string{"no target", "different target", "over cap", "foreign branch", "privacy disabled"} {
		t.Run(name, func(t *testing.T) {
			copy := input
			switch name {
			case "no target":
				copy.TargetEventSeqs = nil
			case "different target":
				copy.TargetEventSeqs = []int64{doc.Seq}
			case "over cap":
				copy.TargetEventSeqs = make([]int64, state.IntentCandidateMaxCaptures+1)
			case "foreign branch":
				copy.BranchGeneration++
			case "privacy disabled":
				copy.IncludeDiffs = false
			}
			got, err := loadPublishedIntentFormerCompanions(ctx, f.db, &copy, nil)
			if err != nil || len(got) != 0 || len(copy.frozenPublishedContext) != 0 {
				t.Fatalf("unsafe baseline admitted: candidates=%+v marker=%v err=%v", got, copy.frozenPublishedContext, err)
			}
		})
	}
	if _, err := f.db.SQL().ExecContext(ctx, "UPDATE capture_ops SET after_mode='100755' WHERE event_seq IN (SELECT seq FROM capture_events WHERE path='review.go')"); err != nil {
		t.Fatal(err)
	}
	unproved := input
	if got, err := loadPublishedIntentFormerCompanions(ctx, f.db, &unproved, nil); err != nil || len(got) != 0 || len(unproved.frozenPublishedContext) != 0 {
		t.Fatalf("unproved captured mode was supplied: candidates=%+v marker=%v err=%v", got, unproved.frozenPublishedContext, err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, "UPDATE capture_ops SET after_mode='100644' WHERE event_seq IN (SELECT seq FROM capture_events WHERE path='review.go')"); err != nil {
		t.Fatal(err)
	}
	// HEAD movement invalidates the exact recorded implementation post-image.
	input.LatestCommit.OID = mustCommitPath(t, f.dir, "review.go", "package review\nfunc FrozenRemainderReady() bool { return false }\n", "Change the published remainder behavior")
	got, err := loadPublishedIntentFormerCompanions(ctx, f.db, &input, nil)
	if err != nil || len(got) != 0 || len(input.frozenPublishedContext) != 0 {
		t.Fatalf("moved implementation was supplied: candidates=%+v marker=%v err=%v", got, input.frozenPublishedContext, err)
	}
}
