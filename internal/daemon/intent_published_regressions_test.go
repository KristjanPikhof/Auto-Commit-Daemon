package daemon

import (
	"context"
	"database/sql"
	"fmt"
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

const publishedHistorySource = "package app\nfunc PlanHistory() int { return historyEvidence() }\nfunc historyEvidence() int { return 1 }\n"
const publishedHistoryTest = "package app\nimport \"testing\"\nfunc TestRecordedHistory(t *testing.T) { if PlanHistory() != 2 { t.Fatal(\"wrong recorded evidence\") } }\n"

func publishedRegressionFixture(t *testing.T) (*captureFixture, IntentCandidateEvaluation, state.IntentCandidate) {
	t.Helper()
	ctx := context.Background()
	f := newCaptureFixture(t)
	f.cctx.BaseHead = mustCommitPath(t, f.dir, "history.go", publishedHistorySource, "Add recorded history planning")
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "history_typescript_reference_test.go", publishedHistoryTest)
	if captured := capturePublicationFiles(t, f); !captured.Protected {
		t.Fatalf("published regression was not protected: %+v", captured)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("regression captures=%+v err=%v", pending, err)
	}
	committed, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyEvent})
	if err != nil || committed.Published != 1 {
		t.Fatalf("earlier regression did not publish: %+v err=%v", committed, err)
	}
	f.cctx.BaseHead = committed.BaseHead
	baseline := state.IntentCandidate{ID: "published-history-regression", BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		Status: state.IntentCandidatePublished, Readiness: state.IntentReadinessReady, Purpose: "verify recorded history evidence",
		PublishedCommitOID: sql.NullString{String: committed.BaseHead, Valid: true},
		Events:             []state.IntentCandidateEvent{{EventSeq: pending[0].Seq, EventRole: "test"}}}
	if err := state.SaveIntentCandidate(ctx, f.db, baseline); err != nil {
		t.Fatal(err)
	}
	baseline, _, err = state.IntentCandidateByID(ctx, f.db, baseline.ID)
	if err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "history.go", strings.Replace(publishedHistorySource, "return 1", "return 2", 1))
	if captured := capturePublicationFiles(t, f); !captured.Protected {
		t.Fatalf("private implementation unprotected: %+v", captured)
	}
	pending, err = state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("implementation captures=%+v err=%v", pending, err)
	}
	ops, err := state.LoadCaptureOps(ctx, f.db, pending[0].Seq)
	if err != nil {
		t.Fatal(err)
	}
	return f, IntentCandidateEvaluation{RepoPath: f.dir, BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		IncludeDiffs: true, LatestCommit: &ai.CommitSummary{OID: committed.BaseHead[:8]},
		Captures: []IntentCandidateCapture{{Event: pending[0], Ops: ops}}, Now: time.Now().UTC()}, baseline
}

type publishedRegressionPlanner struct{ intentCandidatePlannerStub }

func (p *publishedRegressionPlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	panic("native v2 planner required")
}

func (p *publishedRegressionPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.calls++
	p.req = req
	found := false
	for _, candidate := range req.Candidates {
		for _, evidence := range candidate.CapturedEvidence {
			found = found || candidate.Status == state.IntentCandidatePublished && strings.Contains(evidence.CapturedDiff, "PlanHistory()")
		}
	}
	assignment := ai.IntentCandidateAssignment{CandidateID: "recorded-history-evidence", Readiness: ai.IntentCandidateWait,
		Purpose: "reconstruct exact recorded history evidence", MissingCompanions: []string{"focused historical regression is not available"}, GroupingReason: "the historical regression must explain the completed goal"}
	for _, capture := range req.OfferedCaptures {
		assignment.SelectedSeqs = append(assignment.SelectedSeqs, capture.Seq)
		found = found && strings.Contains(capture.CapturedDiff, " func PlanHistory()")
	}
	if found {
		assignment.Readiness = ai.IntentCandidateReady
		assignment.MissingCompanions = nil
		assignment.Subject = "Reconstruct recorded history evidence"
		assignment.Body = "- Reuse the published public API regression for the private change"
		assignment.GroupingReason = "the recorded public entry point calls the changed private implementation and its exact regression is already in HEAD"
	}
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{assignment}}, nil
}

func TestIntentPublishedPublicRegressionCompletesPrivateImplementation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, input, baseline := publishedRegressionFixture(t)
	var checkpointID string
	if err := f.db.ReadSQL().QueryRowContext(ctx, "SELECT checkpoint_id FROM checkpoint_events WHERE event_seq=?", input.Captures[0].Event.Seq).Scan(&checkpointID); err != nil {
		t.Fatal(err)
	}
	seq := input.Captures[0].Event.Seq
	ts := intentPlannerHealthTimestamp(time.Now())
	drain := state.PublicationDrain{ID: "published-public-regression", CheckpointID: checkpointID, WorktreeID: checkpoint.WorktreeID(f.dir),
		BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration, CommitStrategy: "intent", CommitFormat: "imperative",
		Provider: "intent-v2-test", ProviderFingerprint: "sha256:" + strings.Repeat("0", 64),
		Phase: state.PublicationDrainSemantic, TargetEventCount: 1, EventSeqs: []int64{seq}, CreatedTS: ts, UpdatedTS: ts, LastProgressTS: ts}
	if _, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "later.md", "# Later work stays protected\n")
	if captured := capturePublicationFiles(t, f); !captured.Protected {
		t.Fatalf("later work unprotected: %+v", captured)
	}
	planner := &publishedRegressionPlanner{}
	published, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetBalanced, IntentIncludeDiffs: true,
		IntentWindow: 1, IntentBypassBatchWait: true, IntentVerificationMode: "structural", PublicationDrain: &drain})
	if err != nil || published.Published != 1 || published.Failed != 0 || planner.calls != 1 || published.Disposition == ReplayDispositionNeedsAttention {
		t.Fatalf("published regression did not release implementation: %+v calls=%d err=%v", published, planner.calls, err)
	}
	if len(planner.req.OfferedCaptures) != 1 || planner.req.OfferedCaptures[0].Seq != seq {
		t.Fatalf("published support gained selection authority: %+v", planner.req.OfferedCaptures)
	}
	after, found, err := state.IntentCandidateByID(ctx, f.db, baseline.ID)
	if err != nil || !found || !reflect.DeepEqual(after, baseline) {
		t.Fatalf("published provenance changed: %+v err=%v", after, err)
	}
	progress, err := UpdatePublicationDrainAfterReplay(ctx, f.db, drain, published, nil, time.Now())
	if err != nil || progress.Phase != state.PublicationDrainCompleted || progress.TargetEventCount != 1 || !reflect.DeepEqual(progress.EventSeqs, []int64{seq}) {
		t.Fatalf("frozen progress incomplete: %+v err=%v", progress, err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Path != "later.md" {
		t.Fatalf("later work escaped frozen target: %+v err=%v", pending, err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "cat-file", "-e", "HEAD:later.md"); err == nil {
		t.Fatal("later work entered branch history")
	}
	if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
		t.Fatalf("status/list require action after safe publication: %t err=%v", attention, err)
	}
	if got := strings.TrimSpace(mustGitOutput(t, f.dir, "log", "-1", "--format=%s")); got != "Reconstruct recorded history evidence" {
		t.Fatalf("implementation was published without its purpose: %q", got)
	}
}

func TestIntentRecordedGoCallerOwnershipRejectsFalseRelationships(t *testing.T) {
	t.Parallel()
	diff := "-func historyEvidence() int { return 1 }\n+func historyEvidence() int { return 2 }\n"
	for _, tc := range []struct {
		name, caller string
		want         bool
	}{
		{"direct", "func PlanHistory() int { return historyEvidence() }", true},
		{"comment", "// func PlanHistory() int { return historyEvidence() }", false},
		{"quoted", "func PlanHistory() string { return \"historyEvidence()\" }", false},
		{"parameter", "func PlanHistory(historyEvidence func() int) int { return historyEvidence() }", false},
		{"local_callback", "func PlanHistory() int { historyEvidence := func() int { return 4 }; return historyEvidence() }", false},
		{"selector", "func PlanHistory() int { return foreign.historyEvidence() }", false},
		{"closure", "func PlanHistory() func() int { return func() int { return historyEvidence() } }", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			contents := "package app\n" + tc.caller + "\nfunc historyEvidence() int { return 2 }\n"
			calls := intentGoRecordedCallContext("history.go", contents, diff, intentSourceReferenceContextCap)
			if got := strings.Contains(calls.context, " func PlanHistory"); got != tc.want {
				t.Fatalf("reverse ownership=%t want=%t context=%q", got, tc.want, calls.context)
			}
			if got := intentGoRecordedCallContext("history.go", contents, diff, 1); got.context != "" {
				t.Fatalf("caller context exceeded byte budget: %q", got.context)
			}
		})
	}
	for _, body := range []string{
		"package app_test\nfunc TestHistory() { PlanHistory() }\n",
		"package app\nfunc TestHistory(PlanHistory func()) { PlanHistory() }\n",
		"package app\nfunc PlanHistory() {}\nfunc TestHistory() { PlanHistory() }\n",
		"package app\nfunc TestHistory() { _ = \"PlanHistory()\" }\n",
		"package app\nfunc TestHistory() { foreign.PlanHistory() }\n",
	} {
		if intentGoPublishedReferenceContext("history_test.go", []byte(body), intentGoRegressionSource{packageName: "app", names: map[string]bool{"PlanHistory": true}}) != "" {
			t.Fatalf("unowned test reference was admitted: %s", body)
		}
	}
}

func TestIntentPublishedRegressionRequiresExactAvailableVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, input, _ := publishedRegressionFixture(t)
	for _, tc := range []struct{ name, contents string }{
		{"stale_body", "package app\nfunc TestOtherHistory() {}\n"},
		{"wrong_mode", publishedHistoryTest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			head := mustCommitPath(t, f.dir, "history_typescript_reference_test.go", tc.contents, "Change unrelated regression")
			if tc.name == "wrong_mode" {
				if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "update-index", "--chmod=+x", "history_typescript_reference_test.go"); err != nil {
					t.Fatal(err)
				}
				if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "commit", "-q", "-m", "Change regression mode"); err != nil {
					t.Fatal(err)
				}
				head = mustGitOutput(t, f.dir, "rev-parse", "HEAD")
			}
			copy := input
			copy.LatestCommit = &ai.CommitSummary{OID: strings.TrimSpace(head)}
			if candidates, err := loadPublishedIntentFormerCompanions(ctx, f.db, &copy, nil); err != nil || len(candidates) != 0 || len(copy.publishedContext) != 0 {
				t.Fatalf("unavailable test version was supplied: %+v err=%v", candidates, err)
			}
		})
	}
}

func TestIntentPublishedRegressionContextRespectsOpenCandidateCap(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, input, _ := publishedRegressionFixture(t)
	oid, err := git.HashObjectStdin(ctx, f.dir, []byte(publishedHistoryTest))
	if err != nil {
		t.Fatal(err)
	}
	test := appendIntentCandidateCapture(t, f.db, "history_second_test.go", "create", "", oid)
	head := mustCommitPath(t, f.dir, test.Event.Path, publishedHistoryTest, "Add another history regression")
	if err := state.MarkEventPublished(ctx, f.db, test.Event.Seq, state.EventStatePublished, sql.NullString{String: strings.TrimSpace(head), Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
		t.Fatal(err)
	}
	if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{ID: "second-history-regression", BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration, Status: state.IntentCandidatePublished, Readiness: state.IntentReadinessReady, Purpose: "verify history planning", PublishedCommitOID: sql.NullString{String: strings.TrimSpace(head), Valid: true}, Events: []state.IntentCandidateEvent{{EventSeq: test.Event.Seq, EventRole: "test"}}}); err != nil {
		t.Fatal(err)
	}
	input.LatestCommit = &ai.CommitSummary{OID: strings.TrimSpace(head)}
	// Real unrelated pending memberships occupy all but one context slot.
	var captures []IntentCandidateCapture
	for i := 0; i < state.IntentCandidateMaxOpenPerPair-1; i++ {
		captures = append(captures, intentCandidateCaptureFixture(0, fmt.Sprintf("held-%03d.md", i), "create", "", oid))
	}
	seedIntentCandidateCaptureBatch(t, f.db, captures)
	var existing []state.IntentCandidate
	for i, capture := range captures {
		candidate := state.IntentCandidate{ID: fmt.Sprintf("held-%03d", i), BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration, Status: state.IntentCandidateWaiting, Readiness: state.IntentReadinessWait, Purpose: "await a complete independent goal", Events: []state.IntentCandidateEvent{{EventSeq: capture.Event.Seq, EventRole: "documentation"}}}
		if err := state.SaveIntentCandidate(ctx, f.db, candidate); err != nil {
			t.Fatal(err)
		}
		existing = append(existing, candidate)
	}
	result, err := loadPublishedIntentFormerCompanions(ctx, f.db, &input, existing)
	if err != nil || len(result) != state.IntentCandidateMaxOpenPerPair || len(input.publishedContext) != 1 {
		t.Fatalf("published context exceeded candidate cap: candidates=%d contexts=%d err=%v", len(result), len(input.publishedContext), err)
	}
	if !reflect.DeepEqual(result[:len(existing)], existing) {
		t.Fatal("context budgeting displaced pending owners")
	}
}
