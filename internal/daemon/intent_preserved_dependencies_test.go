package daemon

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentPreservationRetainsOnlyCompletePrerequisiteClosure(t *testing.T) {
	t.Parallel()
	req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateSummary{{CandidateID: "published-baseline", Status: "published", Ready: true}}}
	var candidates []ai.IntentCandidateAssignment
	for i, id := range []string{"rejected-baseline", "worker-signals", "worker-guide", "independent-goal", "unknown-consumer", "baseline-consumer"} {
		seq := int64(i + 1)
		req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: seq})
		candidates = append(candidates, ai.IntentCandidateAssignment{CandidateID: id, SelectedSeqs: []int64{seq}, Readiness: ai.IntentCandidateReady})
	}
	candidates[1].DependsOnCandidates = []string{candidates[0].CandidateID}
	candidates[2].DependsOnCandidates = []string{candidates[1].CandidateID}
	candidates[4].DependsOnCandidates = []string{"discarded-response-id"}
	candidates[5].DependsOnCandidates = []string{"published-baseline"}
	preserved, partial, ok := preserveIntentPlanGroups(req, ai.IntentPlanV2{Candidates: candidates},
		[]ai.IntentAtomicityFinding{{CandidateID: candidates[0].CandidateID, Code: "hard_dependency_undeclared"}})
	if !ok || !reflect.DeepEqual(intentAssignmentMembership(preserved), [][]int64{{4}, {6}}) ||
		!reflect.DeepEqual(offeredIntentSeqs(partial), []int64{1, 2, 3, 5}) {
		t.Fatalf("dependent outlived its rejected prerequisite: preserved=%+v partial=%+v", preserved, partial)
	}
	// A restart sees only the previously locked dependent, not the discarded
	// prerequisite. Its unknown response-local ID must still lose authority.
	if kept, _, ok := preserveIntentPlanGroups(req, ai.IntentPlanV2{Candidates: candidates[1:3]}, nil); ok || len(kept) > 0 {
		t.Fatalf("stale partial cache retained orphaned dependencies: %+v", kept)
	}
}

func TestIntentIncompleteSemanticRetryResumesAfterRestartOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	req, input, planner := semanticRetryRequest(t)
	planner.err, planner.plan = nil, restoredSemanticPlan()
	plannerRequest, _, err := preflightIntentCandidatePlan(ctx, req, input.Preset, input.Captures, input.PreflightMaterialize)
	if err != nil {
		t.Fatal(err)
	}
	fingerprintReq := req
	fingerprintReq.BaselineCandidates = plannerRequest.BaselineCandidates
	run, err := newIntentPlanRun(fingerprintReq, input, 1)
	if err != nil {
		t.Fatal(err)
	}
	run, err = state.EnsureIntentPlanRun(ctx, db, run)
	if err != nil {
		t.Fatal(err)
	}
	run.AttemptCount = run.AttemptLimit
	run.ResolutionMode = sql.NullString{String: "waiting_semantic_retry", Valid: true}
	run.ProgressState = run.ResolutionMode
	run.UnresolvedSeqs = offeredIntentSeqs(req)
	if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
		t.Fatal(err)
	}
	evidence, err := intentSemanticRetryEvidence(req, input, run.AttemptLimit)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := scheduleIntentSemanticRetry(ctx, db, input, run, evidence, input.Now)
	if err != nil {
		t.Fatal(err)
	}
	retryAt := secondsTime(retry.RetryAtTS)
	path := db.Path()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = state.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, _, _, _, _, _, waiting, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	var wait *IntentSemanticRetryWaitError
	if !errors.As(err, &wait) || waiting.AttemptCount != 1 || planner.calls != 0 {
		t.Fatalf("restart spent the cooldown: run=%+v calls=%d err=%v", waiting, planner.calls, err)
	}
	input.Now = retryAt.Add(time.Second)
	for i := 0; i < 2; i++ {
		plan, _, _, _, attention, _, resumed, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
		if err != nil || attention || planner.calls != 1 || !resumed.Completed || resumed.AttemptCount != 1 ||
			len(plan.Candidates) != 1 || plan.Candidates[0].Readiness != ai.IntentCandidateReady {
			t.Fatalf("due review did not resolve once: plan=%+v run=%+v calls=%d err=%v", plan, resumed, planner.calls, err)
		}
	}
}
