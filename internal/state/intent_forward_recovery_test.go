package state

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func TestForwardRecoverIntentCandidatePreservesPublishedState(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t)
	ctx := context.Background()
	published := appendIntentV2Event(t, db, "refs/heads/main", 9, "feature.go")
	pending := appendIntentV2Event(t, db, "refs/heads/main", 9, "feature_test.go")
	linked := appendIntentV2Event(t, db, "refs/heads/main", 9, "fixture.txt")
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events
SET state='published',commit_oid='published-head',published_ts=1
WHERE seq=?`, published); err != nil {
		t.Fatal(err)
	}
	candidate := IntentCandidate{
		ID: "mixed-candidate", BranchRef: "refs/heads/main", BranchGeneration: 9,
		Status: IntentCandidateReady, Readiness: IntentReadinessReady,
		Events: []IntentCandidateEvent{
			{EventSeq: published, EventRole: "implementation"},
			{EventSeq: pending, EventRole: "test"},
		},
	}
	if err := SaveIntentCandidate(ctx, db, candidate); err != nil {
		t.Fatal(err)
	}
	if err := ReplaceIntentCaptureDependencies(ctx, db,
		"refs/heads/main", 9, []IntentCaptureDependency{{
			BranchRef: "refs/heads/main", BranchGeneration: 9,
			PrerequisiteSeq: pending, DependentSeq: linked,
			Strength: "hard", Kind: "fixture",
		}}); err != nil {
		t.Fatal(err)
	}
	recovery := IntentForwardRecovery{
		BranchRef: "refs/heads/main", BranchGeneration: 9,
		CandidateID: candidate.ID, Reason: "repair_horizon_expired",
	}
	pendingCount, changed, err := ForwardRecoverIntentCandidate(ctx, db, recovery)
	if err != nil || !changed || pendingCount != 1 {
		t.Fatalf("forward recovery=(pending=%d changed=%t err=%v)",
			pendingCount, changed, err)
	}
	got, ok, err := IntentCandidateByID(ctx, db, candidate.ID)
	if err != nil || !ok || got.Status != IntentCandidateSuperseded {
		t.Fatalf("candidate=(%+v ok=%t err=%v)", got, ok, err)
	}
	if len(got.Events) != 0 {
		t.Fatalf("active candidate events=%+v want none", got.Events)
	}
	for seq, want := range map[int64]string{
		published: EventStatePublished,
		pending:   EventStatePending,
		linked:    EventStatePending,
	} {
		var eventState string
		if err := db.ReadSQL().QueryRowContext(ctx,
			`SELECT state FROM capture_events WHERE seq=?`, seq).Scan(&eventState); err != nil {
			t.Fatal(err)
		}
		if eventState != want {
			t.Fatalf("event %d state=%q want=%q", seq, eventState, want)
		}
	}
	decisions, err := DecisionsForEvent(ctx, db, pending, 10)
	if err != nil || len(decisions) != 1 ||
		decisions[0].Kind != DecisionKindIntentForwardRecovery {
		t.Fatalf("pending decisions=%+v err=%v", decisions, err)
	}
	marker, active, err := IntentForwardRecoveryForPair(
		ctx, db, recovery.BranchRef, recovery.BranchGeneration)
	if err != nil || !active || marker.CandidateID != candidate.ID {
		t.Fatalf("forward marker=(%+v active=%t err=%v)", marker, active, err)
	}
	if marker.Stage != "semantic_replan" ||
		!reflect.DeepEqual(marker.TargetEventSeqs, []int64{pending, linked}) {
		t.Fatalf("forward marker=%+v, want frozen dependency target [%d %d]",
			marker, pending, linked)
	}
	marker, err = AdvanceIntentForwardRecovery(
		ctx, db, marker, "local_unlock", 0)
	if err != nil || marker.Stage != "local_unlock" || marker.UnlockCount != 0 {
		t.Fatalf("local unlock marker=(%+v,%v)", marker, err)
	}
	marker, err = AdvanceIntentForwardRecovery(
		ctx, db, marker, "semantic_replan", 1)
	if err != nil || marker.Stage != "semantic_replan" ||
		marker.UnlockCount != 1 || marker.LastProgressTS <= 0 {
		t.Fatalf("semantic marker=(%+v,%v)", marker, err)
	}
	if err := CompleteIntentForwardRecovery(ctx, db, marker, 1); err != nil {
		t.Fatal(err)
	}
	if _, active, err := IntentForwardRecoveryForPair(
		ctx, db, recovery.BranchRef, recovery.BranchGeneration,
	); err != nil || active {
		t.Fatalf("marker after completion active=%t err=%v", active, err)
	}
}

func TestIntentForwardRecoveryOldMarkerDefaultsToSemanticReplan(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t)
	ctx := context.Background()
	legacy := IntentForwardRecovery{
		BranchRef: "refs/heads/main", BranchGeneration: 3,
		CandidateID: "legacy", Reason: "repair_horizon_expired",
	}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO daemon_meta(key,value,updated_ts) VALUES(?,?,1)`,
		intentForwardRecoveryMetaKey, string(raw)); err != nil {
		t.Fatal(err)
	}
	got, active, err := IntentForwardRecoveryForPair(
		ctx, db, legacy.BranchRef, legacy.BranchGeneration)
	if err != nil || !active || got.Stage != "semantic_replan" {
		t.Fatalf("legacy marker=(%+v active=%t err=%v)", got, active, err)
	}
}

func TestForwardRecoverIntentCandidateRejectsActivePublication(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t)
	ctx := context.Background()
	seq := appendIntentV2Event(t, db, "refs/heads/main", 4, "feature.go")
	if err := SaveIntentCandidate(ctx, db, IntentCandidate{
		ID: "candidate", BranchRef: "refs/heads/main", BranchGeneration: 4,
		Status: IntentCandidateReady, Readiness: IntentReadinessReady,
		Events: []IntentCandidateEvent{{EventSeq: seq, EventRole: "implementation"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO self_publications(
 id,branch_ref,branch_generation,source_head,target_commit_oid,target_tree_oid,
 membership_digest,member_count,phase,created_ts,updated_ts,
 completion_published_ts,completion_candidate_status
) VALUES(
 'sp','refs/heads/main',4,'source','target','tree',
 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
 1,'prepared',1,1,0,'unknown'
)`); err != nil {
		t.Fatal(err)
	}
	_, changed, err := ForwardRecoverIntentCandidate(ctx, db,
		IntentForwardRecovery{
			BranchRef: "refs/heads/main", BranchGeneration: 4,
			CandidateID: "candidate", Reason: "repair_horizon_expired",
		})
	if err == nil || changed {
		t.Fatalf("active publication recovery changed=%t err=%v", changed, err)
	}
}
