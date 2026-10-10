package daemon

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentStalePurposeRepartitionsThirtyProtectedCaptures(t *testing.T) {
	t.Parallel()
	testIntentUnclassifiedFallbackRepartition(t, 28, false, true)
}

func TestIntentNewestNativePurposeKeepsWaitingBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	source := appendIntentCandidateCapture(t, db, "retry.go", "create", "", "source-after")
	source.CapturedDiff = "+package app\n+func RetrySpeech() bool { return false }\n"
	test := appendIntentCandidateCapture(t, db, "retry_test.go", "create", "", "test-after")
	test.CapturedDiff = "+package app\n+import \"testing\"\n+func TestRetrySpeech(t *testing.T) { if RetrySpeech() { t.Fatal(\"unsupported retry\") } }\n"
	captures := []IntentCandidateCapture{source, test}
	seqs := []int64{source.Event.Seq, test.Event.Seq}
	candidate := state.IntentCandidate{ID: "waiting-recognition", BranchRef: source.Event.BranchRef, BranchGeneration: source.Event.BranchGeneration,
		Status: state.IntentCandidateWaiting, Readiness: state.IntentReadinessWait, Purpose: "complete speech retry behavior",
		MissingCompanions: "the companion behavior needs review", AtomicityStatus: sql.NullString{String: "pending", Valid: true},
		VerificationStatus: sql.NullString{String: "not_required", Valid: true}, CreatedTS: 100, UpdatedTS: 100,
		Events: []state.IntentCandidateEvent{{EventSeq: seqs[0], EventRole: "code"}, {EventSeq: seqs[1], EventRole: "test"}}}
	if err := state.SaveIntentCandidate(ctx, db, candidate); err != nil {
		t.Fatal(err)
	}
	input := IntentCandidateEvaluation{BranchRef: candidate.BranchRef, BranchGeneration: candidate.BranchGeneration, Captures: captures, TargetEventSeqs: seqs}
	unknown := ai.IntentCandidateAssignment{CandidateID: candidate.ID, SelectedSeqs: seqs, Purpose: "retain dependency component until its goal is known",
		Readiness: ai.IntentCandidateWait, MissingCompanions: []string{unclassifiedIntentCompanion}, GroupingReason: "protected dependency component needs a meaningful goal message"}
	save := func(fingerprint, mode string, at float64, assignment ai.IntentCandidateAssignment) {
		t.Helper()
		run, err := state.EnsureIntentPlanRun(ctx, db, state.IntentPlanRun{Fingerprint: fingerprint, BranchRef: candidate.BranchRef, BranchGeneration: candidate.BranchGeneration, AttemptLimit: 3})
		if err != nil {
			t.Fatal(err)
		}
		run.Completed = true
		run.ResolutionMode = sql.NullString{String: mode, Valid: true}
		if err := storeResolvedIntentPlanRun(&run, ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{assignment}}, nil); err != nil {
			t.Fatal(err)
		}
		if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
			t.Fatal(err)
		}
		if _, err := db.SQL().ExecContext(ctx, "UPDATE intent_plan_runs SET updated_ts=? WHERE fingerprint=?", at, fingerprint); err != nil {
			t.Fatal(err)
		}
	}
	save("sha256:"+strings.Repeat("a", 64), "waiting_semantic_retry", 200, unknown)
	newest := unknown
	newest.Purpose = "reject unsupported speech retries"
	newest.GroupingReason = "the captured retry guard and its direct regression complete one behavior"
	newest.MissingCompanions = []string{"review the recognition restart behavior before publishing"}
	req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for _, capture := range captures {
		req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: capture.Event.Seq, Path: capture.Event.Path, Op: "create", CapturedDiff: capture.CapturedDiff})
	}
	for _, readiness := range []ai.IntentCandidateReadiness{ai.IntentCandidateWait, ai.IntentCandidateReady} {
		t.Run(string(readiness), func(t *testing.T) {
			newest.Readiness = readiness
			if readiness == ai.IntentCandidateReady {
				newest.MissingCompanions = nil
				newest.Subject, newest.Body = "Reject unsupported speech retries", "- Keep the retry guard and its direct regression together"
			}
			plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{newest}}
			if err := ValidateIntentGoalPlan(req, plan); err != nil {
				t.Fatalf("newer purposeful plan lacks actual source/test proof: %v", err)
			}
			save("sha256:"+strings.Repeat("b", 64), "provider", 300, newest)
			copy := input
			before := append([]IntentCandidateCapture(nil), copy.Captures...)
			ids, err := unclassifiedIntentCandidatesForRepartition(ctx, db, &copy, []state.IntentCandidate{candidate}, captures)
			if err != nil || len(ids) != 0 || !reflect.DeepEqual(before, copy.Captures) {
				t.Fatalf("older unknown plan overrode newer purpose: ids=%v captures=%+v err=%v", ids, copy.Captures, err)
			}
		})
	}
	// A newer purposeful state observation also closes a stale fallback,
	// even when its corresponding native run is no longer in the bounded scan.
	if _, err := db.SQL().ExecContext(ctx, "DELETE FROM intent_plan_runs WHERE fingerprint=?", "sha256:"+strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	candidate.UpdatedTS = 400
	copy := input
	ids, err := unclassifiedIntentCandidatesForRepartition(ctx, db, &copy, []state.IntentCandidate{candidate}, captures)
	if err != nil || len(ids) != 0 {
		t.Fatalf("older fallback reinterpreted newer state: ids=%v err=%v", ids, err)
	}
}
