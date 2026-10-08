package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func semanticRetryRequest(t *testing.T) (ai.IntentPlanRequestV2, IntentCandidateEvaluation, *intentCandidatePlannerStub) {
	t.Helper()
	req, err := ai.NewIntentPlanRequestV2(ai.IntentPlanRequestV2Options{
		OfferedCaptures: []ai.OfferedCapture{{Seq: 1, Path: "SpeechEngine.swift", Op: "modify"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	planner := &intentCandidatePlannerStub{err: &ai.IntentPlanV2ValidationError{Message: "intent planner v2: invalid response payload"}}
	input := IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1, Provider: planner.Name(),
		Preset: config.PresetBalanced, Now: time.Now().UTC().Truncate(time.Second),
	}
	return req, input, planner
}

func TestIntentSemanticRetryPrunesOnlyExpiredSupersededCooldowns(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	a := appendIntentCandidateCapture(t, db, "SpeechEngine.swift", "modify", "before", "after")
	b := appendIntentCandidateCapture(t, db, "CameraController.swift", "modify", "before", "after")
	var runs []state.IntentPlanRun
	for i, seq := range []int64{a.Event.Seq, a.Event.Seq, b.Event.Seq} {
		run, err := state.EnsureIntentPlanRun(ctx, db, state.IntentPlanRun{
			Fingerprint: fmt.Sprintf("sha256:%064x", i+1), BranchRef: "refs/heads/main", BranchGeneration: 1,
			AttemptLimit: 1, UnresolvedSeqs: []int64{seq},
		})
		if err != nil {
			t.Fatal(err)
		}
		run.Completed = true
		run.UnresolvedSeqs = []int64{seq}
		run.ProgressState = sql.NullString{String: "waiting_semantic_retry", Valid: true}
		run.ResolutionMode = run.ProgressState
		if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
			t.Fatal(err)
		}
		runs = append(runs, run)
	}
	if _, err := db.SQL().ExecContext(ctx, "UPDATE intent_plan_runs SET updated_ts=? WHERE fingerprint=?", float64(now.Unix())+1, runs[1].Fingerprint); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{}
	for i := 1; i <= 143; i++ {
		fingerprint := fmt.Sprintf("sha256:%064x", i)
		retryAt := now.Add(-time.Minute)
		if i == 2 {
			retryAt = now.Add(time.Hour)
		}
		raw, err := json.Marshal(IntentSemanticRetrySnapshot{
			Version: 1, BranchRef: "refs/heads/main", BranchGeneration: 1,
			EvidenceFingerprint: fingerprint, PlanFingerprint: fingerprint,
			RetryAtTS: intentPlannerHealthTimestamp(retryAt),
		})
		if err != nil {
			t.Fatal(err)
		}
		values[intentSemanticRetryKey(fingerprint)] = string(raw)
	}
	if err := state.MetaSetMany(ctx, db, values); err != nil {
		t.Fatal(err)
	}
	if err := pruneIntentSemanticRetries(ctx, db, runs[1].Fingerprint, now); err != nil {
		t.Fatal(err)
	}
	var cooldowns, plans, captures int
	if err := db.SQL().QueryRowContext(ctx, "SELECT count(*) FROM daemon_meta WHERE key LIKE ?", MetaKeyIntentSemanticRetry+".%").Scan(&cooldowns); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRowContext(ctx, "SELECT count(*) FROM intent_plan_runs").Scan(&plans); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRowContext(ctx, "SELECT count(*) FROM capture_events WHERE state='pending'").Scan(&captures); err != nil {
		t.Fatal(err)
	}
	if cooldowns != 2 || plans != 3 || captures != 2 {
		t.Fatalf("prune lost active retry or provenance: cooldowns=%d plans=%d captures=%d", cooldowns, plans, captures)
	}
	for _, index := range []int{1, 2} {
		if _, found, err := loadIntentSemanticRetryForEvidence(ctx, db, runs[index].Fingerprint); err != nil || !found {
			t.Fatalf("active record %d pruned: found=%t err=%v", index, found, err)
		}
	}
}

func restoredSemanticPlan() ai.IntentPlanV2 {
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "restore-speech", SelectedSeqs: []int64{1},
			Purpose: "restore speech recognition after pauses", Readiness: ai.IntentCandidateReady,
			Subject:        "Restore speech recognition after pauses",
			Body:           "- Resume recognition when the speaker continues talking",
			GroupingReason: "the capture completes the recognition restart behavior",
		}},
	}
}

func TestIntentSemanticRetryResumesUnchangedEvidenceAfterRestart(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	req, input, planner := semanticRetryRequest(t)
	plan, _, _, _, attention, _, first, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	if err != nil || attention || !first.Completed || first.AttemptCount != 1 || first.ResolutionMode.String != "waiting_semantic_retry" || len(plan.Candidates) != 1 || plan.Candidates[0].Readiness != ai.IntentCandidateWait {
		t.Fatalf("unresolved goals were not held safely: plan=%+v run=%+v attention=%t err=%v", plan, first, attention, err)
	}
	retry, found, err := loadIntentSemanticRetry(ctx, db)
	if err != nil || !found || retry.RetryAtTS != intentPlannerHealthTimestamp(input.Now.Add(time.Hour)) {
		t.Fatalf("retry=%+v found=%t err=%v", retry, found, err)
	}
	planner.err, planner.plan = nil, restoredSemanticPlan()
	for i := 0; i < 3; i++ {
		dbPath := db.Path()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = state.Open(ctx, dbPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		_, _, _, _, _, _, waiting, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
		var wait *IntentSemanticRetryWaitError
		if !errors.As(err, &wait) || !wait.RetryAt.Equal(input.Now.Add(time.Hour)) || waiting.AttemptCount != 1 || waiting.Fingerprint != first.Fingerprint || planner.calls != 1 {
			t.Fatalf("restart reset semantic cooldown: run=%+v calls=%d err=%v", waiting, planner.calls, err)
		}
		after, _, err := loadIntentSemanticRetry(ctx, db)
		if err != nil || after != retry {
			t.Fatalf("retry drifted across restart: before=%+v after=%+v err=%v", retry, after, err)
		}
	}
	input.Now = input.Now.Add(time.Hour)
	plan, fallback, _, _, _, _, resumed, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	if err != nil || fallback != "" || resumed.ResolutionMode.String != "provider" || resumed.AttemptCount != 1 || resumed.Fingerprint != first.Fingerprint || planner.calls != 2 || !reflect.DeepEqual(plan, planner.plan) {
		t.Fatalf("due review did not resume same evidence: plan=%+v run=%+v calls=%d err=%v", plan, resumed, planner.calls, err)
	}
	if _, found, err := loadIntentSemanticRetry(ctx, db); err != nil || found {
		t.Fatalf("completed goal retained retry: found=%t err=%v", found, err)
	}
}

func TestIntentSemanticRetryRebuildsLegacyGenericResolution(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	req, input, planner := semanticRetryRequest(t)
	_, _, _, _, _, _, run, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	if err != nil {
		t.Fatal(err)
	}
	legacy := restoredSemanticPlan()
	legacy.Candidates[0].Subject = "Update SpeechEngine.swift"
	legacy.Candidates[0].Body = "- Preserve the captured modify of SpeechEngine.swift"
	if err := storeResolvedIntentPlanRun(&run, legacy, nil); err != nil {
		t.Fatal(err)
	}
	run.ResolutionMode = sql.NullString{String: "evidence_partition", Valid: true}
	run.ProgressState = sql.NullString{String: "completed", Valid: true}
	if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
		t.Fatal(err)
	}
	retry, _, err := loadIntentSemanticRetry(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := clearIntentSemanticRetry(ctx, db, retry.EvidenceFingerprint); err != nil {
		t.Fatal(err)
	}
	planner.err, planner.plan = nil, restoredSemanticPlan()
	plan, _, _, _, _, _, rebuilt, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	if err != nil || len(plan.Candidates) != 1 || plan.Candidates[0].Readiness != ai.IntentCandidateWait || rebuilt.ResolutionMode.String != "waiting_semantic_retry" || planner.calls != 1 {
		t.Fatalf("generic cached resolution bypassed safe review: plan=%+v run=%+v calls=%d err=%v", plan, rebuilt, planner.calls, err)
	}
	input.Now = input.Now.Add(time.Hour)
	plan, _, _, _, _, _, repaired, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	if err != nil || repaired.Fingerprint != run.Fingerprint || planner.calls != 2 || plan.Candidates[0].Subject != planner.plan.Candidates[0].Subject {
		t.Fatalf("legacy generic goal stayed blocked: plan=%+v run=%+v calls=%d err=%v", plan, repaired, planner.calls, err)
	}
}

func TestIntentSemanticRetryKeepsIndependentWindowCooldowns(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	reqA, input, planner := semanticRetryRequest(t)
	reqB := reqA
	reqB.OfferedCaptures = []ai.OfferedCapture{{Seq: 2, Path: "CameraController.swift", Op: "modify"}}
	for _, req := range []ai.IntentPlanRequestV2{reqA, reqB} {
		if _, _, _, _, _, _, _, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input); err != nil {
			t.Fatal(err)
		}
	}
	evidenceA, _ := intentSemanticRetryEvidence(reqA, input, 1)
	evidenceB, _ := intentSemanticRetryEvidence(reqB, input, 1)
	beforeA, foundA, errA := loadIntentSemanticRetryForEvidence(ctx, db, evidenceA)
	beforeB, foundB, errB := loadIntentSemanticRetryForEvidence(ctx, db, evidenceB)
	if errA != nil || errB != nil || !foundA || !foundB || planner.calls != 2 {
		t.Fatalf("window cooldowns missing: A=%+v B=%+v calls=%d errors=%v/%v", beforeA, beforeB, planner.calls, errA, errB)
	}
	dbPath := db.Path()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// Scheduling pressure creates a different plan run, but does not grant
	// another attempt on the same unchanged capture evidence.
	reqA.ForcedAging = true
	for _, req := range []ai.IntentPlanRequestV2{reqA, reqB, reqA, reqB} {
		_, _, _, _, _, _, _, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
		var wait *IntentSemanticRetryWaitError
		if !errors.As(err, &wait) || !wait.RetryAt.Equal(input.Now.Add(time.Hour)) || planner.calls != 2 {
			t.Fatalf("another window or pressure escaped cooldown: calls=%d err=%v", planner.calls, err)
		}
	}
	input.Now = input.Now.Add(time.Hour)
	planner.err, planner.plan = nil, restoredSemanticPlan()
	planner.plan.Candidates[0].SelectedSeqs = []int64{2}
	if _, _, _, _, _, _, run, err := chooseIntentCandidatePlan(ctx, reqB, planner, nil, 0, input.Preset, nil, db, input); err != nil || run.AttemptCount != 1 || planner.calls != 3 {
		t.Fatalf("B failed due retry: run=%+v calls=%d err=%v", run, planner.calls, err)
	}
	afterA, foundA, errA := loadIntentSemanticRetryForEvidence(ctx, db, evidenceA)
	if errA != nil || !foundA || afterA.RetryAtTS != beforeA.RetryAtTS {
		t.Fatalf("B completion erased A: before=%+v after=%+v found=%t err=%v", beforeA, afterA, foundA, errA)
	}
	planner.plan = restoredSemanticPlan()
	if _, _, _, _, _, _, run, err := chooseIntentCandidatePlan(ctx, reqA, planner, nil, 0, input.Preset, nil, db, input); err != nil || run.AttemptCount != 1 || planner.calls != 4 {
		t.Fatalf("A failed independent due retry: run=%+v calls=%d err=%v", run, planner.calls, err)
	}
}

func TestIntentSemanticRetryRetainsValidGroups(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	req, input, planner := semanticRetryRequest(t)
	req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: 2, Path: "recognition.md", Op: "modify"})
	_, _, _, _, _, _, run, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	if err != nil {
		t.Fatal(err)
	}
	known := restoredSemanticPlan().Candidates[0]
	mixed := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{known, {
		CandidateID: "unknown-document", SelectedSeqs: []int64{2},
		Purpose: "retain captured evidence until its goal is known", Readiness: ai.IntentCandidateWait,
		MissingCompanions: []string{"captured evidence needs a meaningful goal message"}, GroupingReason: "independent captured document",
	}}}
	if err := storeResolvedIntentPlanRun(&run, mixed, nil); err != nil {
		t.Fatal(err)
	}
	if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
		t.Fatal(err)
	}
	input.Now = input.Now.Add(time.Hour)
	planner.err, planner.plan = nil, restoredSemanticPlan()
	planner.plan.Candidates[0].CandidateID = "document-recognition"
	planner.plan.Candidates[0].SelectedSeqs = []int64{2}
	planner.plan.Candidates[0].Subject = "Document speech recognition recovery"
	plan, _, _, _, _, _, resumed, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	if err != nil || len(plan.Candidates) != 2 || len(planner.req.OfferedCaptures) != 1 || planner.req.OfferedCaptures[0].Seq != 2 || !reflect.DeepEqual(resumed.PreservedGroups, [][]int64{{1}}) {
		t.Fatalf("retry discarded valid groups: plan=%+v request=%+v run=%+v err=%v", plan, planner.req, resumed, err)
	}
	for _, candidate := range plan.Candidates {
		if candidate.CandidateID == known.CandidateID && !reflect.DeepEqual(candidate, known) {
			t.Fatalf("valid group changed during partial retry: %+v != %+v", candidate, known)
		}
	}
}

func TestIntentSemanticRetryEvidenceIgnoresCandidateObservationClocks(t *testing.T) {
	req, input, _ := semanticRetryRequest(t)
	req.Candidates = []ai.IntentCandidateSummary{{CandidateID: "previous", SelectedSeqs: []int64{1}, CreatedAt: input.Now, UpdatedAt: input.Now}}
	before, err := newIntentPlanRun(req, input, 1)
	if err != nil {
		t.Fatal(err)
	}
	req.Candidates[0].UpdatedAt = input.Now.Add(time.Hour)
	after, err := newIntentPlanRun(req, input, 1)
	if err != nil || before.Fingerprint != after.Fingerprint {
		t.Fatalf("observation clock reset the semantic attempt cap: before=%s after=%s err=%v", before.Fingerprint, after.Fingerprint, err)
	}
	stable, err := intentSemanticRetryEvidence(req, input, 1)
	if err != nil {
		t.Fatal(err)
	}
	req.Candidates = nil
	req.ForcedAging = true
	observed, err := intentSemanticRetryEvidence(req, input, 1)
	if err != nil || stable != observed {
		t.Fatalf("derived candidate pressure reset the semantic cooldown: %s %s err=%v", stable, observed, err)
	}
	req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: 2, Path: "SpeechEngineTests.swift", Op: "create"})
	changed, err := intentSemanticRetryEvidence(req, input, 1)
	if err != nil || changed == stable {
		t.Fatalf("new evidence did not invalidate cooldown: %s %s err=%v", stable, changed, err)
	}
}
