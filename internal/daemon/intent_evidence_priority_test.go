package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func evidenceTestDiff(contents string) string {
	return "diff --git a/example.go b/example.go\nnew file mode 100644\n--- /dev/null\n+++ b/example.go\n@@ -0,0 +1,500 @@\n+" + strings.ReplaceAll(contents, "\n", "\n+")
}

func TestIntentEvidenceBudgetPreservesCurrentTestsAndPublishedWitness(t *testing.T) {
	t.Parallel()
	const assertion = `if got != 2 { t.Fatal("missing recorded history") }`
	var captures []IntentCandidateCapture
	add := func(seq int64, path, stateName, diff string) {
		captures = append(captures, IntentCandidateCapture{Event: state.CaptureEvent{Seq: seq, Path: path, Operation: "create", State: stateName}, CapturedDiff: diff})
	}
	source := "package app\nfunc PlanIntentHistory() int { return historyEvidence() }\nfunc historyEvidence() int { return 2 }\n"
	add(1, "history.go", state.EventStatePending, evidenceTestDiff(source))
	body := "package app\nimport \"testing\"\nfunc TestPendingHistory(t *testing.T) {\n got := PlanIntentHistory()\n" + strings.Repeat(" _ = 1 // recorded pending regression setup\n", 140) + assertion + "\n" + strings.Repeat(" _ = 2 // recorded pending regression cleanup\n", 140) + "}\n"
	add(2, "pending_history_test.go", state.EventStatePending, evidenceTestDiff(body))
	for i := 3; i <= 20; i++ {
		content := fmt.Sprintf("package app\nfunc TestCurrent%d() {\n", i) + strings.Repeat(" _ = 1 // independently recorded current behavior\n", 85) + " _ = PlanIntentHistory()\n}\n"
		add(int64(i), fmt.Sprintf("current_%d_test.go", i), state.EventStatePending, evidenceTestDiff(content))
	}
	sourceOwner := intentGoRegressionSource{packageName: "app", names: map[string]bool{"PlanIntentHistory": true}}
	for i := 0; i < 10; i++ {
		content := fmt.Sprintf("package app\nfunc TestPublished%d() {\n", i) + strings.Repeat(" FixtureHelper() // older already-published baseline setup\n", 80) + " _ = PlanIntentHistory()\n" + strings.Repeat(" FixtureHelper() // older already-published baseline cleanup\n", 80) + "}\n"
		references := intentGoPublishedReferenceContext("history_published_test.go", []byte(content), sourceOwner)
		if !strings.Contains(references, "PlanIntentHistory()") || strings.Contains(references, "FixtureHelper") {
			t.Fatalf("exact published witness lost: %q", references)
		}
		add(int64(100+i), fmt.Sprintf("history_published_%d_test.go", i), state.EventStatePublished, prependIntentRecordedReferenceContext(evidenceTestDiff(content), references))
	}
	prioritized := prioritizeIntentRelationshipEvidence(captures)
	legacy := allocateIntentEvidenceDiffs(prioritized, ai.HistoryRewriteTotalDiffCap)
	if strings.Contains(legacy[1], assertion) {
		t.Fatal("fixture did not reproduce old pending-test starvation")
	}
	preferred := map[int]bool{}
	for i := 0; i < 20; i++ {
		preferred[i] = true
	}
	allocated := allocateIntentEvidenceDiffsPrioritized(prioritized, ai.HistoryRewriteTotalDiffCap, preferred)
	if !strings.Contains(allocated[1], assertion) {
		t.Fatal("current pending test assertion was lost")
	}
	var offered, readonly []ai.OfferedCapture
	total := 0
	for i, capture := range captures {
		total += len(allocated[i])
		if len(allocated[i]) > ai.IntentStageDiffCap {
			t.Fatal("per-file budget exceeded")
		}
		item := ai.OfferedCapture{Seq: capture.Event.Seq, Path: capture.Event.Path, Op: "create", CapturedDiff: allocated[i]}
		if i < 20 {
			offered = append(offered, item)
		} else {
			if !strings.Contains(allocated[i], "PlanIntentHistory()") {
				t.Fatal("published exact call was clipped")
			}
			readonly = append(readonly, item)
		}
	}
	if total > ai.HistoryRewriteTotalDiffCap {
		t.Fatalf("global evidence budget exceeded: %d", total)
	}
	candidate := ai.IntentCandidateSummary{CandidateID: "published-history-support", Status: state.IntentCandidatePublished, CapturedEvidence: readonly}
	for _, capture := range readonly {
		candidate.SelectedSeqs = append(candidate.SelectedSeqs, capture.Seq)
	}
	req, err := ai.NewIntentPlanRequestV2(ai.IntentPlanRequestV2Options{OfferedCaptures: offered, Candidates: []ai.IntentCandidateSummary{candidate}, IncludeCapturedDiffs: true})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := ai.BuildIntentPlanV2UserPrompt(req)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(wire, "readonly_evidence") || !strings.Contains(req.OfferedCaptures[1].CapturedDiff, assertion) {
		t.Fatal("provider wire lost protected test or published support")
	}
	if len(req.OfferedCaptures) != 20 {
		t.Fatal("evidence priority changed offered membership")
	}
}

func TestIntentPublishedReferenceContextKeepsTypedOwnershipAndBounds(t *testing.T) {
	t.Parallel()
	contents := "package app\nfunc Proof(value *Metadata) *Metadata {\n" + strings.Repeat(" FixtureHelper()\n", 400) + " return &Metadata{}\n}\n"
	references := intentGoPublishedReferenceContext("proof.go", []byte(contents), intentGoRegressionSource{packageName: "app", types: map[string]bool{"Metadata": true}})
	if !strings.Contains(references, "*Metadata") || !strings.Contains(references, "&Metadata{}") || strings.Contains(references, "FixtureHelper") || len(references) > intentSourceReferenceContextCap {
		t.Fatalf("typed witness not precise/bounded: %q", references)
	}
	oversized := "package app\nfunc TestHistory(){ PlanIntentHistory(" + strings.Repeat("1,", intentSourceReferenceContextCap) + ") }\n"
	if got := intentGoPublishedReferenceContext("history_test.go", []byte(oversized), intentGoRegressionSource{packageName: "app", names: map[string]bool{"PlanIntentHistory": true}}); got != "" {
		t.Fatal("partial oversized call was retained")
	}
}

func TestIntentEvidenceContractRechecksLegacyWaitOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	req, input, planner := semanticRetryRequest(t)
	// Recorded v4 fingerprint of this fixed request, before offered-body and
	// exact published-reference retention. Observation clocks are excluded.
	const legacy = "sha256:e4c64b0edaef7d0986de3d5ccbda520771d05c5e0b51f539c5eae64c4d888b5f"
	current, err := intentSemanticRetryEvidence(req, input, 1)
	if err != nil || current == legacy {
		t.Fatalf("old evidence contract reused: %s err=%v", current, err)
	}
	oldPlan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{CandidateID: "old-clipped-context", SelectedSeqs: []int64{1}, Purpose: "restore speech recognition", Readiness: ai.IntentCandidateWait, MissingCompanions: []string{"old planner context is clipped"}, GroupingReason: "recorded context could not explain the completed goal"}}}
	raw, err := json.Marshal(resolvedIntentPlanRun{Plan: oldPlan})
	if err != nil {
		t.Fatal(err)
	}
	old, err := state.EnsureIntentPlanRun(ctx, db, state.IntentPlanRun{Fingerprint: legacy, BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration, AttemptLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	old.Completed = true
	old.ProgressState = sql.NullString{String: "waiting_semantic_retry", Valid: true}
	old.ResolutionMode = old.ProgressState
	old.ResolvedPlanJSON = sql.NullString{String: string(raw), Valid: true}
	old.UnresolvedSeqs = []int64{1}
	if err := state.UpdateIntentPlanRun(ctx, db, old); err != nil {
		t.Fatal(err)
	}
	legacyRetry := IntentSemanticRetrySnapshot{Version: 1, BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration, EvidenceFingerprint: legacy, PlanFingerprint: legacy, ReviewCount: 3, ScheduledAtTS: intentPlannerHealthTimestamp(input.Now), RetryAtTS: intentPlannerHealthTimestamp(input.Now.Add(time.Hour))}
	if err := saveIntentSemanticRetry(ctx, db, legacyRetry); err != nil {
		t.Fatal(err)
	}
	planner.err = nil
	planner.plan = oldPlan
	_, _, _, _, _, _, _, err = chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	if err != nil || planner.calls != 1 {
		t.Fatalf("new evidence contract did not review old wait: calls=%d err=%v", planner.calls, err)
	}
	retry, found, err := loadIntentSemanticRetryForEvidence(ctx, db, current)
	if err != nil || !found || retry.ReviewCount != 1 || retry.RetryAtTS != intentPlannerHealthTimestamp(input.Now.Add(5*time.Minute)) {
		t.Fatalf("new review schedule=%+v found=%t err=%v", retry, found, err)
	}
	path := db.Path()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = state.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, _, _, _, _, _, _, err = chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	var wait *IntentSemanticRetryWaitError
	if !errors.As(err, &wait) || planner.calls != 1 {
		t.Fatalf("unchanged v7 evidence repeated review: calls=%d err=%v", planner.calls, err)
	}
	unchanged, _, err := loadIntentSemanticRetryForEvidence(ctx, db, current)
	if err != nil || unchanged != retry {
		t.Fatalf("restart moved the deadline: before=%+v after=%+v err=%v", retry, unchanged, err)
	}
}
