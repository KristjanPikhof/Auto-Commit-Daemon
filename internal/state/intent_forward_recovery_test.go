package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
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
	if err := CompleteIntentForwardRecovery(ctx, db, marker, 1); err == nil {
		t.Fatal("completion cleared an unresolved recovery target")
	}
	if _, active, err := IntentForwardRecoveryForPair(
		ctx, db, recovery.BranchRef, recovery.BranchGeneration,
	); err != nil || !active {
		t.Fatalf("unresolved marker active=%t err=%v", active, err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events
SET state='published',commit_oid='recovery-head',published_ts=2
WHERE seq IN (?,?)`, pending, linked); err != nil {
		t.Fatal(err)
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

func TestStartFailedIntentCheckpointRecoveryFreezesCompleteCheckpoint(t *testing.T) {
	for _, candidateStatus := range []string{
		IntentCandidateWaiting,
		IntentCandidateBlocked,
	} {
		t.Run(candidateStatus, func(t *testing.T) {
			fixture := seedFailedIntentCheckpointFixture(t, 3, 2)
			ctx := context.Background()
			if _, err := fixture.db.SQL().ExecContext(ctx, `
UPDATE intent_candidates SET status=? WHERE id=?`,
				candidateStatus, fixture.candidateID); err != nil {
				t.Fatal(err)
			}
			if candidateStatus == IntentCandidateBlocked {
				drain := PublicationDrain{
					ID: "needs-action-drain", CheckpointID: fixture.checkpoint.ID,
					WorktreeID:       fixture.checkpoint.WorktreeID,
					BranchRef:        fixture.checkpoint.ObservedRef,
					BranchGeneration: failedIntentCheckpointGeneration,
					Phase:            PublicationDrainNeedsAction,
					TargetEventCount: int64(len(fixture.seqs)),
					LastError:        "forced_capture_deferred", CreatedTS: 2,
					UpdatedTS: 2, LastProgressTS: 2,
				}
				if _, err := PreparePublicationDrain(ctx, fixture.db, drain); err != nil {
					t.Fatal(err)
				}
			}

			recovery, changed, err := StartFailedIntentCheckpointRecovery(
				ctx, fixture.db, failedIntentCheckpointBranch,
				failedIntentCheckpointGeneration, fixture.seqs[0])
			if err != nil || !changed {
				t.Fatalf("checkpoint recovery=(%+v changed=%t err=%v)",
					recovery, changed, err)
			}
			if recovery.CandidateID != fixture.candidateID ||
				recovery.Reason != failedIntentCheckpointRecoveryReason ||
				recovery.Stage != "semantic_replan" || recovery.LastProgressTS <= 0 ||
				!reflect.DeepEqual(recovery.TargetEventSeqs, fixture.seqs) {
				t.Fatalf("recovery=%+v want complete checkpoint %+v",
					recovery, fixture.seqs)
			}

			candidate, ok, err := IntentCandidateByID(
				ctx, fixture.db, fixture.candidateID)
			if err != nil || !ok || candidate.Status != IntentCandidateSuperseded ||
				len(candidate.Events) != 0 {
				t.Fatalf("candidate=(%+v ok=%t err=%v)", candidate, ok, err)
			}
			for _, seq := range fixture.seqs {
				var eventState string
				if err := fixture.db.ReadSQL().QueryRowContext(ctx,
					`SELECT state FROM capture_events WHERE seq=?`, seq).
					Scan(&eventState); err != nil {
					t.Fatal(err)
				}
				if eventState != EventStatePending {
					t.Fatalf("event %d state=%q want pending", seq, eventState)
				}
			}
			for _, seq := range fixture.seqs[:2] {
				decisions, err := DecisionsForEvent(ctx, fixture.db, seq, 10)
				if err != nil || len(decisions) != 1 ||
					decisions[0].Kind != DecisionKindIntentForwardRecovery ||
					decisions[0].ActionTaken.String !=
						"retired_failed_candidate_for_checkpoint_recovery" {
					t.Fatalf("event %d decisions=%+v err=%v", seq, decisions, err)
				}
			}
			if decisions, err := DecisionsForEvent(
				ctx, fixture.db, fixture.seqs[2], 10); err != nil || len(decisions) != 0 {
				t.Fatalf("unowned checkpoint decisions=%+v err=%v", decisions, err)
			}

			again, changed, err := StartFailedIntentCheckpointRecovery(
				ctx, fixture.db, failedIntentCheckpointBranch,
				failedIntentCheckpointGeneration, fixture.seqs[0])
			if err != nil || changed || !reflect.DeepEqual(again, recovery) {
				t.Fatalf("idempotent recovery=(%+v changed=%t err=%v), want %+v",
					again, changed, err, recovery)
			}
			var decisionCount int
			if err := fixture.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM decision_records WHERE kind=?`,
				DecisionKindIntentForwardRecovery).Scan(&decisionCount); err != nil {
				t.Fatal(err)
			}
			if decisionCount != 2 {
				t.Fatalf("decision count=%d want 2", decisionCount)
			}
		})
	}
}

func TestOldestOverdueFailedIntentEventStopsAtTerminalBarrier(t *testing.T) {
	db, _ := openTestDB(t)
	ctx := context.Background()
	barrierSeq := appendIntentV2Event(
		t, db, failedIntentCheckpointBranch,
		failedIntentCheckpointGeneration, "terminal.go")
	heldSeq := appendIntentV2Event(
		t, db, failedIntentCheckpointBranch,
		failedIntentCheckpointGeneration, "held.go")
	seedFailedIntentCompletedCheckpoint(t, db, "main", []int64{heldSeq})
	const candidateID = "failed-behind-terminal"
	if err := SaveIntentCandidate(ctx, db, IntentCandidate{
		ID: candidateID, BranchRef: failedIntentCheckpointBranch,
		BranchGeneration: failedIntentCheckpointGeneration,
		Status:           IntentCandidateWaiting, Readiness: IntentReadinessWait,
		Purpose: "failed candidate behind terminal event",
		VerificationStatus: sql.NullString{
			String: "failed", Valid: true,
		},
		Events: []IntentCandidateEvent{{
			EventSeq: heldSeq, EventRole: "implementation",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := RecordPlannerDefer(
		ctx, db, heldSeq, 1, "verification failed"); err != nil {
		t.Fatal(err)
	}
	if seq, ok, err := OldestOverdueFailedIntentEventSeq(
		ctx, db, failedIntentCheckpointBranch,
		failedIntentCheckpointGeneration, 1,
	); err != nil || !ok || seq != heldSeq {
		t.Fatalf("visible failed event=(%d,%t,%v), want %d", seq, ok, err, heldSeq)
	}
	if err := MarkEventPublished(
		ctx, db, barrierSeq, EventStateFailed, sql.NullString{},
		sql.NullString{String: "terminal barrier", Valid: true},
		sql.NullString{}, 2,
	); err != nil {
		t.Fatal(err)
	}
	if seq, ok, err := OldestOverdueFailedIntentEventSeq(
		ctx, db, failedIntentCheckpointBranch,
		failedIntentCheckpointGeneration, 1,
	); err != nil || ok || seq != 0 {
		t.Fatalf("failed event behind barrier=(%d,%t,%v), want hidden", seq, ok, err)
	}
	candidate, ok, err := IntentCandidateByID(ctx, db, candidateID)
	if err != nil || !ok || candidate.Status != IntentCandidateWaiting ||
		len(candidate.Events) != 1 {
		t.Fatalf("candidate changed behind barrier=(%+v,%t,%v)", candidate, ok, err)
	}
	if _, active, err := IntentForwardRecoveryForPair(
		ctx, db, failedIntentCheckpointBranch,
		failedIntentCheckpointGeneration,
	); err != nil || active {
		t.Fatalf("recovery marker behind barrier active=%t err=%v", active, err)
	}
}

func TestStartFailedIntentCheckpointRecoveryFreezesOnlyPendingCheckpointRows(
	t *testing.T,
) {
	fixture := seedFailedIntentCheckpointFixture(t, 5, 2)
	ctx := context.Background()
	execFailedIntentCheckpointTest(t, fixture.db, `
UPDATE capture_events
SET state='published',commit_oid='published-before-recovery',published_ts=2
WHERE seq=?`, fixture.seqs[2])
	execFailedIntentCheckpointTest(t, fixture.db, `
UPDATE capture_events SET state='recovered',published_ts=2 WHERE seq=?`,
		fixture.seqs[3])

	recovery, changed, err := StartFailedIntentCheckpointRecovery(
		ctx, fixture.db, failedIntentCheckpointBranch,
		failedIntentCheckpointGeneration, fixture.seqs[0])
	if err != nil || !changed {
		t.Fatalf("mixed checkpoint recovery=(%+v changed=%t err=%v)",
			recovery, changed, err)
	}
	wantTarget := []int64{fixture.seqs[0], fixture.seqs[1], fixture.seqs[4]}
	if !reflect.DeepEqual(recovery.TargetEventSeqs, wantTarget) {
		t.Fatalf("recovery target=%v want pending rows %v",
			recovery.TargetEventSeqs, wantTarget)
	}
	wantStates := map[int64]string{
		fixture.seqs[0]: EventStatePending,
		fixture.seqs[1]: EventStatePending,
		fixture.seqs[2]: EventStatePublished,
		fixture.seqs[3]: EventStateRecovered,
		fixture.seqs[4]: EventStatePending,
	}
	for seq, want := range wantStates {
		var got string
		if err := fixture.db.ReadSQL().QueryRowContext(ctx,
			`SELECT state FROM capture_events WHERE seq=?`, seq).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("event %d state=%q want %q", seq, got, want)
		}
	}
	candidate, ok, err := IntentCandidateByID(ctx, fixture.db, fixture.candidateID)
	if err != nil || !ok || candidate.Status != IntentCandidateSuperseded ||
		len(candidate.Events) != 0 {
		t.Fatalf("candidate=(%+v ok=%t err=%v)", candidate, ok, err)
	}
}

func TestStartFailedIntentCheckpointRecoveryExpandsFailedClosure(t *testing.T) {
	fixture := seedFailedIntentCheckpointClosureFixture(t, false)
	ctx := context.Background()
	recovery, changed, err := StartFailedIntentCheckpointRecovery(
		ctx, fixture.db, failedIntentCheckpointBranch,
		failedIntentCheckpointGeneration, fixture.seqs[0])
	if err != nil || !changed {
		t.Fatalf("checkpoint closure recovery=(%+v changed=%t err=%v)",
			recovery, changed, err)
	}
	if recovery.CandidateID != fixture.rootCandidateID ||
		recovery.Reason != failedIntentCheckpointRecoveryReason ||
		recovery.Stage != "semantic_replan" ||
		!reflect.DeepEqual(recovery.TargetEventSeqs, fixture.seqs) {
		t.Fatalf("recovery=%+v want complete closure %v", recovery, fixture.seqs)
	}
	for _, candidateID := range []string{
		fixture.rootCandidateID,
		fixture.overlapCandidateID,
	} {
		candidate, ok, err := IntentCandidateByID(ctx, fixture.db, candidateID)
		if err != nil || !ok || candidate.Status != IntentCandidateSuperseded ||
			len(candidate.Events) != 0 {
			t.Fatalf("candidate %s=(%+v ok=%t err=%v)",
				candidateID, candidate, ok, err)
		}
	}
	var decisions int
	if err := fixture.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM decision_records WHERE kind=?`,
		DecisionKindIntentForwardRecovery).Scan(&decisions); err != nil {
		t.Fatal(err)
	}
	if decisions != 5 {
		t.Fatalf("closure decisions=%d want 5 candidate members", decisions)
	}
	for _, seq := range fixture.seqs {
		var eventState string
		if err := fixture.db.ReadSQL().QueryRowContext(ctx,
			`SELECT state FROM capture_events WHERE seq=?`, seq).
			Scan(&eventState); err != nil {
			t.Fatal(err)
		}
		if eventState != EventStatePending {
			t.Fatalf("event %d state=%q want pending", seq, eventState)
		}
	}
}

func TestStartFailedIntentCheckpointRecoveryRejectsHealthyClosureCandidate(
	t *testing.T,
) {
	fixture := seedFailedIntentCheckpointClosureFixture(t, true)
	ctx := context.Background()
	recovery, changed, err := StartFailedIntentCheckpointRecovery(
		ctx, fixture.db, failedIntentCheckpointBranch,
		failedIntentCheckpointGeneration, fixture.seqs[0])
	if err != nil || changed || recovery.CandidateID != "" {
		t.Fatalf("healthy closure recovery=(%+v changed=%t err=%v)",
			recovery, changed, err)
	}
	for candidateID, want := range map[string]struct {
		status string
		active int
	}{
		fixture.rootCandidateID:    {IntentCandidateWaiting, 2},
		fixture.overlapCandidateID: {IntentCandidateReady, 3},
	} {
		var status string
		var active int
		if err := fixture.db.ReadSQL().QueryRowContext(ctx,
			`SELECT status FROM intent_candidates WHERE id=?`, candidateID).
			Scan(&status); err != nil {
			t.Fatal(err)
		}
		if err := fixture.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_candidate_events
WHERE candidate_id=? AND membership_state='active'`, candidateID).
			Scan(&active); err != nil {
			t.Fatal(err)
		}
		if status != want.status || active != want.active {
			t.Fatalf("candidate %s status=%q active=%d want %q/%d",
				candidateID, status, active, want.status, want.active)
		}
	}
	var markers, decisions int
	if err := fixture.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM daemon_meta WHERE key=?`,
		intentForwardRecoveryMetaKey).Scan(&markers); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM decision_records WHERE kind=?`,
		DecisionKindIntentForwardRecovery).Scan(&decisions); err != nil {
		t.Fatal(err)
	}
	if markers != 0 || decisions != 0 {
		t.Fatalf("healthy closure markers=%d decisions=%d want 0/0",
			markers, decisions)
	}
}

func TestStartFailedIntentCheckpointRecoveryIncludesIncompleteClosureCandidate(
	t *testing.T,
) {
	fixture := seedFailedIntentCheckpointClosureFixture(t, true)
	ctx := context.Background()
	if _, err := fixture.db.SQL().ExecContext(ctx, `
UPDATE intent_candidates
SET status='waiting',readiness='wait',
    missing_companions='production companion is not in this planner window',
    verification_status='pending'
WHERE id=?`, fixture.overlapCandidateID); err != nil {
		t.Fatal(err)
	}

	recovery, changed, err := StartFailedIntentCheckpointRecovery(
		ctx, fixture.db, failedIntentCheckpointBranch,
		failedIntentCheckpointGeneration, fixture.seqs[0])
	if err != nil || !changed ||
		!reflect.DeepEqual(recovery.TargetEventSeqs, fixture.seqs) {
		t.Fatalf("incomplete closure recovery=(%+v changed=%t err=%v)",
			recovery, changed, err)
	}
	for _, candidateID := range []string{
		fixture.rootCandidateID,
		fixture.overlapCandidateID,
	} {
		candidate, ok, err := IntentCandidateByID(ctx, fixture.db, candidateID)
		if err != nil || !ok || candidate.Status != IntentCandidateSuperseded ||
			len(candidate.Events) != 0 {
			t.Fatalf("candidate %s=(%+v ok=%t err=%v)",
				candidateID, candidate, ok, err)
		}
	}
}

func TestCompleteResolvedIntentForwardRecoveryProvesAndClearsMarker(t *testing.T) {
	fixture := seedFailedIntentCheckpointFixture(t, 3, 2)
	ctx := context.Background()
	recovery, changed, err := StartFailedIntentCheckpointRecovery(
		ctx, fixture.db, failedIntentCheckpointBranch,
		failedIntentCheckpointGeneration, fixture.seqs[0])
	if err != nil || !changed {
		t.Fatalf("start recovery=(%+v changed=%t err=%v)", recovery, changed, err)
	}
	execFailedIntentCheckpointTest(t, fixture.db, `
UPDATE capture_events
SET state=CASE WHEN seq=? THEN 'published' ELSE 'recovered' END,
    commit_oid=CASE WHEN seq=? THEN 'published-commit' ELSE NULL END
WHERE seq IN (?,?,?)`, fixture.seqs[0], fixture.seqs[0],
		fixture.seqs[0], fixture.seqs[1], fixture.seqs[2])
	loaded, active, err := IntentForwardRecoveryForPair(
		ctx, fixture.db, failedIntentCheckpointBranch,
		failedIntentCheckpointGeneration)
	if err != nil || !active || !reflect.DeepEqual(loaded, recovery) {
		t.Fatalf("loaded recovery=(%+v active=%t err=%v), want %+v",
			loaded, active, err, recovery)
	}
	completed, err := CompleteResolvedIntentForwardRecovery(ctx, fixture.db, loaded)
	if err != nil || !completed {
		t.Fatalf("complete resolved recovery=(%t,%v)", completed, err)
	}
	if _, active, err := IntentForwardRecoveryForPair(
		ctx, fixture.db, failedIntentCheckpointBranch,
		failedIntentCheckpointGeneration); err != nil || active {
		t.Fatalf("marker after resolved completion active=%t err=%v", active, err)
	}
	for key, want := range map[string]string{
		"intent.v2.last_fallback_mode":   "semantic_replan",
		"intent.v2.last_fallback_size":   "3",
		"intent.v2.last_fallback_reason": failedIntentCheckpointRecoveryReason,
	} {
		var got string
		if err := fixture.db.ReadSQL().QueryRowContext(ctx,
			`SELECT value FROM daemon_meta WHERE key=?`, key).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("diagnostic %s=%q want %q", key, got, want)
		}
	}
	completed, err = CompleteResolvedIntentForwardRecovery(ctx, fixture.db, loaded)
	if err != nil || completed {
		t.Fatalf("idempotent resolved completion=(%t,%v)", completed, err)
	}
}

func TestCompleteAnyResolvedIntentForwardRecoveryClearsOlderGeneration(t *testing.T) {
	fixture := seedFailedIntentCheckpointFixture(t, 3, 2)
	ctx := context.Background()
	recovery, changed, err := StartFailedIntentCheckpointRecovery(
		ctx, fixture.db, failedIntentCheckpointBranch,
		failedIntentCheckpointGeneration, fixture.seqs[0])
	if err != nil || !changed {
		t.Fatalf("start recovery=(%+v changed=%t err=%v)",
			recovery, changed, err)
	}
	execFailedIntentCheckpointTest(t, fixture.db, `
UPDATE capture_events
SET state='recovered',published_ts=2
WHERE seq IN (?,?,?)`, fixture.seqs[0], fixture.seqs[1], fixture.seqs[2])

	loaded, completed, err := CompleteAnyResolvedIntentForwardRecovery(
		ctx, fixture.db)
	if err != nil {
		t.Fatal(err)
	}
	if !completed || loaded.BranchRef != recovery.BranchRef ||
		loaded.BranchGeneration != recovery.BranchGeneration ||
		!sameEventSeqs(loaded.TargetEventSeqs, recovery.TargetEventSeqs) {
		t.Fatalf("completed=%t loaded=%+v want=%+v",
			completed, loaded, recovery)
	}
	if _, active, err := IntentForwardRecoveryForPair(
		ctx, fixture.db, recovery.BranchRef, recovery.BranchGeneration,
	); err != nil || active {
		t.Fatalf("old generation marker active=%t err=%v", active, err)
	}

	newGeneration := recovery.BranchGeneration + 1
	newSeqs := []int64{
		appendIntentV2Event(t, fixture.db, recovery.BranchRef,
			newGeneration, "new-source.go"),
		appendIntentV2Event(t, fixture.db, recovery.BranchRef,
			newGeneration, "new-source_test.go"),
	}
	seedFailedIntentCompletedCheckpoint(t, fixture.db, "split", newSeqs)
	const newCandidateID = "new-generation-failed-candidate"
	if err := SaveIntentCandidate(ctx, fixture.db, IntentCandidate{
		ID: newCandidateID, BranchRef: recovery.BranchRef,
		BranchGeneration: newGeneration,
		Status:           IntentCandidateWaiting, Readiness: IntentReadinessWait,
		Purpose: "complete the new generation change",
		VerificationStatus: sql.NullString{
			String: "failed", Valid: true,
		},
		Events: []IntentCandidateEvent{{
			EventSeq: newSeqs[0], EventRole: "implementation",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	started, changed, err := StartFailedIntentCheckpointRecovery(
		ctx, fixture.db, recovery.BranchRef, newGeneration, newSeqs[0])
	if err != nil || !changed || started.CandidateID != newCandidateID ||
		!sameEventSeqs(started.TargetEventSeqs, newSeqs) {
		t.Fatalf("new generation recovery=(%+v changed=%t err=%v)",
			started, changed, err)
	}
}

func TestCompleteAnyResolvedIntentForwardRecoveryKeepsUnresolvedMarker(t *testing.T) {
	fixture := seedFailedIntentCheckpointFixture(t, 3, 2)
	ctx := context.Background()
	recovery, changed, err := StartFailedIntentCheckpointRecovery(
		ctx, fixture.db, failedIntentCheckpointBranch,
		failedIntentCheckpointGeneration, fixture.seqs[0])
	if err != nil || !changed {
		t.Fatalf("start recovery=(%+v changed=%t err=%v)",
			recovery, changed, err)
	}

	loaded, completed, err := CompleteAnyResolvedIntentForwardRecovery(
		ctx, fixture.db)
	if err != nil || completed || !reflect.DeepEqual(loaded, recovery) {
		t.Fatalf("unresolved completion=(%+v completed=%t err=%v), want %+v",
			loaded, completed, err, recovery)
	}
	if marker, active, err := IntentForwardRecoveryForPair(
		ctx, fixture.db, recovery.BranchRef, recovery.BranchGeneration,
	); err != nil || !active || !reflect.DeepEqual(marker, recovery) {
		t.Fatalf("unresolved marker=(%+v active=%t err=%v), want %+v",
			marker, active, err, recovery)
	}
	if _, changed, err := StartFailedIntentCheckpointRecovery(
		ctx, fixture.db, recovery.BranchRef,
		recovery.BranchGeneration+1, fixture.seqs[0],
	); err == nil || changed || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("new generation with unresolved marker changed=%t err=%v",
			changed, err)
	}
}

func TestCompleteAnyResolvedIntentForwardRecoveryKeepsLegacyTargetlessMarker(
	t *testing.T,
) {
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

	loaded, completed, err := CompleteAnyResolvedIntentForwardRecovery(ctx, db)
	if err != nil || completed || loaded.Stage != "semantic_replan" ||
		loaded.CandidateID != legacy.CandidateID {
		t.Fatalf("legacy completion=(%+v completed=%t err=%v)",
			loaded, completed, err)
	}
	if marker, active, err := IntentForwardRecoveryForPair(
		ctx, db, legacy.BranchRef, legacy.BranchGeneration,
	); err != nil || !active || marker.CandidateID != legacy.CandidateID {
		t.Fatalf("legacy marker=(%+v active=%t err=%v)", marker, active, err)
	}
}

func TestCompleteResolvedIntentForwardRecoveryRejectsUnprovedTarget(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*testing.T, failedIntentCheckpointFixture)
		change    func(*IntentForwardRecovery)
		wantError bool
	}{
		{
			name: "pending member",
			mutate: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db, `
UPDATE capture_events SET state='recovered' WHERE seq IN (?,?)`,
					fixture.seqs[1], fixture.seqs[2])
			},
		},
		{
			name: "terminal member",
			mutate: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db,
					`UPDATE capture_events SET state='recovered' WHERE seq IN (?,?,?)`,
					fixture.seqs[0], fixture.seqs[1], fixture.seqs[2])
				execFailedIntentCheckpointTest(t, fixture.db,
					`UPDATE capture_events SET state='failed' WHERE seq=?`, fixture.seqs[1])
			},
		},
		{
			name: "missing member",
			mutate: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db,
					`UPDATE capture_events SET state='recovered' WHERE seq IN (?,?,?)`,
					fixture.seqs[0], fixture.seqs[1], fixture.seqs[2])
				execFailedIntentCheckpointTest(t, fixture.db,
					`DELETE FROM checkpoint_events WHERE event_seq=?`, fixture.seqs[2])
				execFailedIntentCheckpointTest(t, fixture.db,
					`DELETE FROM capture_events WHERE seq=?`, fixture.seqs[2])
			},
		},
		{
			name: "changed frozen target",
			mutate: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db,
					`UPDATE capture_events SET state='recovered' WHERE seq IN (?,?,?)`,
					fixture.seqs[0], fixture.seqs[1], fixture.seqs[2])
			},
			change: func(recovery *IntentForwardRecovery) {
				recovery.TargetEventSeqs = append(
					[]int64(nil), recovery.TargetEventSeqs[:2]...)
			},
			wantError: true,
		},
		{
			name: "empty supplied target",
			change: func(recovery *IntentForwardRecovery) {
				recovery.TargetEventSeqs = nil
			},
			wantError: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := seedFailedIntentCheckpointFixture(t, 3, 2)
			ctx := context.Background()
			recovery, changed, err := StartFailedIntentCheckpointRecovery(
				ctx, fixture.db, failedIntentCheckpointBranch,
				failedIntentCheckpointGeneration, fixture.seqs[0])
			if err != nil || !changed {
				t.Fatalf("start recovery=(%+v changed=%t err=%v)",
					recovery, changed, err)
			}
			frozen := append([]int64(nil), recovery.TargetEventSeqs...)
			if test.mutate != nil {
				test.mutate(t, fixture)
			}
			if test.change != nil {
				test.change(&recovery)
			}
			completed, err := CompleteResolvedIntentForwardRecovery(
				ctx, fixture.db, recovery)
			if completed || (err != nil) != test.wantError {
				t.Fatalf("unproved completion=(%t,%v), want error=%t",
					completed, err, test.wantError)
			}
			marker, active, markerErr := IntentForwardRecoveryForPair(
				ctx, fixture.db, failedIntentCheckpointBranch,
				failedIntentCheckpointGeneration)
			if markerErr != nil || !active ||
				!reflect.DeepEqual(marker.TargetEventSeqs, frozen) {
				t.Fatalf("preserved marker=(%+v active=%t err=%v), want target=%v",
					marker, active, markerErr, frozen)
			}
			var diagnostics int
			if err := fixture.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM daemon_meta WHERE key LIKE 'intent.v2.last_fallback_%'`).
				Scan(&diagnostics); err != nil {
				t.Fatal(err)
			}
			if diagnostics != 0 {
				t.Fatalf("fallback diagnostics=%d want 0", diagnostics)
			}
		})
	}
}

func TestStartFailedIntentCheckpointRecoveryRejectsIncompleteProof(t *testing.T) {
	tests := []struct {
		name   string
		events int
		mutate func(*testing.T, failedIntentCheckpointFixture)
	}{
		{
			name: "candidate is ready",
			mutate: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db, `
UPDATE intent_candidates SET status='ready',readiness='ready' WHERE id=?`,
					fixture.candidateID)
			},
		},
		{
			name: "verification is not failed",
			mutate: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db, `
UPDATE intent_candidates SET verification_status='timed_out' WHERE id=?`,
					fixture.candidateID)
			},
		},
		{
			name: "candidate has published oid",
			mutate: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db, `
UPDATE intent_candidates SET published_commit_oid='published' WHERE id=?`,
					fixture.candidateID)
			},
		},
		{
			name: "candidate has soft deadline",
			mutate: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db, `
UPDATE intent_candidates SET soft_publication_deadline=99 WHERE id=?`,
					fixture.candidateID)
			},
		},
		{
			name: "candidate member is not pending",
			mutate: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db, `
UPDATE capture_events SET state='published',commit_oid='published' WHERE seq=?`,
					fixture.seqs[0])
			},
		},
		{
			name: "candidate member has no checkpoint",
			mutate: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db,
					`DELETE FROM checkpoint_events WHERE event_seq=?`, fixture.seqs[1])
			},
		},
		{
			name: "checkpoint is not completed",
			mutate: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db,
					`UPDATE checkpoints SET phase='prepared',completed_ts=NULL WHERE id=?`,
					fixture.checkpoint.ID)
			},
		},
		{
			name: "checkpoint member failed",
			mutate: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db,
					`UPDATE capture_events SET state='failed' WHERE seq=?`,
					fixture.seqs[2])
			},
		},
		{
			name: "checkpoint member blocked",
			mutate: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db,
					`UPDATE capture_events SET state='blocked_conflict' WHERE seq=?`,
					fixture.seqs[2])
			},
		},
		{
			name: "checkpoint crosses branch pair",
			mutate: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db, `
UPDATE capture_events SET branch_ref='refs/heads/other' WHERE seq=?`,
					fixture.seqs[2])
			},
		},
		{
			name:   "checkpoint exceeds candidate cap",
			events: IntentCandidateMaxCaptures + 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			eventCount := test.events
			if eventCount == 0 {
				eventCount = 3
			}
			fixture := seedFailedIntentCheckpointFixture(t, eventCount, 2)
			if test.mutate != nil {
				test.mutate(t, fixture)
			}
			ctx := context.Background()
			var statusBefore string
			if err := fixture.db.ReadSQL().QueryRowContext(ctx,
				`SELECT status FROM intent_candidates WHERE id=?`,
				fixture.candidateID).Scan(&statusBefore); err != nil {
				t.Fatal(err)
			}
			recovery, changed, err := StartFailedIntentCheckpointRecovery(
				ctx, fixture.db, failedIntentCheckpointBranch,
				failedIntentCheckpointGeneration, fixture.seqs[0])
			if err != nil || changed || recovery.CandidateID != "" {
				t.Fatalf("rejected proof recovery=(%+v changed=%t err=%v)",
					recovery, changed, err)
			}
			assertFailedIntentCheckpointUnchanged(
				t, fixture, statusBefore, 2)
		})
	}
}

func TestStartFailedIntentCheckpointRecoveryRejectsConcurrentTransition(t *testing.T) {
	tests := []struct {
		name string
		seed func(*testing.T, failedIntentCheckpointFixture)
	}{
		{
			name: "self publication",
			seed: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db, `
INSERT INTO self_publications(
 id,branch_ref,branch_generation,source_head,target_commit_oid,target_tree_oid,
 membership_digest,member_count,phase,created_ts,updated_ts,
 completion_published_ts,completion_candidate_status
) VALUES(
 'failed-candidate-publication',?,?, 'source','target','tree',
 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
 1,'prepared',1,1,0,'unknown'
)`, failedIntentCheckpointBranch, failedIntentCheckpointGeneration)
			},
		},
		{
			name: "intent repair",
			seed: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				execFailedIntentCheckpointTest(t, fixture.db, `
INSERT INTO intent_repairs(
 id,branch_ref,branch_generation,status,expected_head,plan_digest,
 created_ts,updated_ts
) VALUES('failed-candidate-repair',?,?,'prepared','head','digest',1,1)`,
					failedIntentCheckpointBranch, failedIntentCheckpointGeneration)
			},
		},
		{
			name: "publication drain",
			seed: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				drain := PublicationDrain{
					ID: "active-drain", CheckpointID: fixture.checkpoint.ID,
					WorktreeID:       fixture.checkpoint.WorktreeID,
					BranchRef:        fixture.checkpoint.ObservedRef,
					BranchGeneration: failedIntentCheckpointGeneration,
					Phase:            PublicationDrainSemantic,
					TargetEventCount: int64(len(fixture.seqs)),
					CreatedTS:        2, UpdatedTS: 2, LastProgressTS: 2,
				}
				if _, err := PreparePublicationDrain(
					context.Background(), fixture.db, drain); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "forward marker",
			seed: func(t *testing.T, fixture failedIntentCheckpointFixture) {
				marker := IntentForwardRecovery{
					BranchRef: "refs/heads/other", BranchGeneration: 1,
					CandidateID: "other", Reason: "other",
					Stage: "semantic_replan", TargetEventSeqs: []int64{999},
				}
				raw, err := json.Marshal(marker)
				if err != nil {
					t.Fatal(err)
				}
				execFailedIntentCheckpointTest(t, fixture.db, `
INSERT INTO daemon_meta(key,value,updated_ts) VALUES(?,?,1)`,
					intentForwardRecoveryMetaKey, string(raw))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := seedFailedIntentCheckpointFixture(t, 3, 2)
			test.seed(t, fixture)
			recovery, changed, err := StartFailedIntentCheckpointRecovery(
				context.Background(), fixture.db, failedIntentCheckpointBranch,
				failedIntentCheckpointGeneration, fixture.seqs[0])
			if err == nil || changed || recovery.CandidateID != "" {
				t.Fatalf("concurrent recovery=(%+v changed=%t err=%v)",
					recovery, changed, err)
			}
			assertFailedIntentCheckpointUnchanged(
				t, fixture, IntentCandidateWaiting, 2)
		})
	}
}

const (
	failedIntentCheckpointBranch     = "refs/heads/main"
	failedIntentCheckpointGeneration = int64(7)
)

type failedIntentCheckpointFixture struct {
	db          *DB
	checkpoint  Checkpoint
	candidateID string
	seqs        []int64
	memberCount int
}

type failedIntentCheckpointClosureFixture struct {
	db                 *DB
	rootCandidateID    string
	overlapCandidateID string
	seqs               []int64
}

func seedFailedIntentCheckpointClosureFixture(
	t *testing.T,
	healthyOverlap bool,
) failedIntentCheckpointClosureFixture {
	t.Helper()
	db, _ := openTestDB(t)
	seqs := make([]int64, 0, 6)
	for index := range 6 {
		seqs = append(seqs, appendIntentV2Event(
			t, db, failedIntentCheckpointBranch,
			failedIntentCheckpointGeneration,
			fmt.Sprintf("closure-%03d.go", index)))
	}
	seedFailedIntentCompletedCheckpoint(t, db, "closure-a", seqs[:3])
	seedFailedIntentCompletedCheckpoint(t, db, "closure-b", seqs[3:5])
	seedFailedIntentCompletedCheckpoint(t, db, "closure-c", seqs[5:])

	const rootCandidateID = "failed-closure-root"
	if err := SaveIntentCandidate(context.Background(), db, IntentCandidate{
		ID: rootCandidateID, BranchRef: failedIntentCheckpointBranch,
		BranchGeneration: failedIntentCheckpointGeneration,
		Status:           IntentCandidateWaiting, Readiness: IntentReadinessWait,
		Purpose: "failed root candidate",
		VerificationStatus: sql.NullString{
			String: "failed", Valid: true,
		},
		Events: []IntentCandidateEvent{
			{EventSeq: seqs[0], EventRole: "implementation"},
			{EventSeq: seqs[1], EventRole: "implementation"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	const overlapCandidateID = "failed-closure-overlap"
	overlapStatus := IntentCandidateWaiting
	overlapReadiness := IntentReadinessWait
	overlapVerification := "failed"
	if healthyOverlap {
		overlapStatus = IntentCandidateReady
		overlapReadiness = IntentReadinessReady
		overlapVerification = "passed"
	}
	if err := SaveIntentCandidate(context.Background(), db, IntentCandidate{
		ID: overlapCandidateID, BranchRef: failedIntentCheckpointBranch,
		BranchGeneration: failedIntentCheckpointGeneration,
		Status:           overlapStatus, Readiness: overlapReadiness,
		Purpose: "checkpoint-spanning candidate",
		VerificationStatus: sql.NullString{
			String: overlapVerification, Valid: true,
		},
		Events: []IntentCandidateEvent{
			{EventSeq: seqs[2], EventRole: "implementation"},
			{EventSeq: seqs[3], EventRole: "implementation"},
			{EventSeq: seqs[5], EventRole: "verification"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	return failedIntentCheckpointClosureFixture{
		db: db, rootCandidateID: rootCandidateID,
		overlapCandidateID: overlapCandidateID, seqs: seqs,
	}
}

func seedFailedIntentCheckpointFixture(
	t *testing.T,
	eventCount int,
	memberCount int,
) failedIntentCheckpointFixture {
	t.Helper()
	if eventCount < memberCount || memberCount <= 0 {
		t.Fatal("invalid failed intent checkpoint fixture size")
	}
	db, _ := openTestDB(t)
	seqs := make([]int64, 0, eventCount)
	for index := range eventCount {
		seqs = append(seqs, appendIntentV2Event(
			t, db, failedIntentCheckpointBranch,
			failedIntentCheckpointGeneration,
			fmt.Sprintf("checkpoint-%03d.go", index)))
	}
	checkpoint := seedFailedIntentCompletedCheckpoint(t, db, "main", seqs)
	candidateID := "failed-checkpoint-candidate"
	events := make([]IntentCandidateEvent, 0, memberCount)
	for _, seq := range seqs[:memberCount] {
		events = append(events, IntentCandidateEvent{
			EventSeq: seq, EventRole: "implementation",
		})
	}
	if err := SaveIntentCandidate(context.Background(), db, IntentCandidate{
		ID: candidateID, BranchRef: failedIntentCheckpointBranch,
		BranchGeneration: failedIntentCheckpointGeneration,
		Status:           IntentCandidateWaiting, Readiness: IntentReadinessWait,
		Purpose:            "failed checkpoint candidate",
		VerificationStatus: sql.NullString{String: "failed", Valid: true},
		Events:             events,
	}); err != nil {
		t.Fatal(err)
	}
	return failedIntentCheckpointFixture{
		db: db, checkpoint: checkpoint, candidateID: candidateID,
		seqs: seqs, memberCount: memberCount,
	}
}

func seedFailedIntentCompletedCheckpoint(
	t *testing.T,
	db *DB,
	suffix string,
	eventSeqs []int64,
) Checkpoint {
	t.Helper()
	checkpointNumber := "1788000000000"
	checkpointHex := "aaaaaaaaaaaaaaaa"
	switch suffix {
	case "split":
		checkpointNumber = "1788000000001"
		checkpointHex = "bbbbbbbbbbbbbbbb"
	case "closure-a":
		checkpointNumber = "1788000000002"
		checkpointHex = "cccccccccccccccc"
	case "closure-b":
		checkpointNumber = "1788000000003"
		checkpointHex = "dddddddddddddddd"
	case "closure-c":
		checkpointNumber = "1788000000004"
		checkpointHex = "eeeeeeeeeeeeeeee"
	}
	checkpointID := "cp-" + checkpointNumber + "-" + checkpointHex
	checkpoint := Checkpoint{
		ID:          checkpointID,
		OperationID: "op-failed-intent-" + suffix,
		WorktreeID:  "0123456789abcdef", Reason: CheckpointReasonManualBarrier,
		ObservationEpoch: 1, CoverageEpoch: 1, ObservedHead: "head",
		ObservedRef: failedIntentCheckpointBranch,
		TreeOID:     "tree-" + suffix, CommitOID: "commit-" + suffix,
		Ref:       "refs/acd/checkpoints/v1/0123456789abcdef/" + checkpointID,
		CreatedTS: 1, EventSeqs: append([]int64(nil), eventSeqs...),
	}
	ctx := context.Background()
	if created, err := PrepareCheckpoint(
		ctx, db, checkpoint, checkpointTestDigest); err != nil || !created {
		t.Fatalf("prepare checkpoint=(%t,%v)", created, err)
	}
	if err := CompleteCheckpoint(
		ctx, db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 2); err != nil {
		t.Fatal(err)
	}
	return checkpoint
}

func execFailedIntentCheckpointTest(
	t *testing.T,
	db *DB,
	statement string,
	args ...any,
) {
	t.Helper()
	if _, err := db.SQL().ExecContext(
		context.Background(), statement, args...); err != nil {
		t.Fatal(err)
	}
}

func assertFailedIntentCheckpointUnchanged(
	t *testing.T,
	fixture failedIntentCheckpointFixture,
	wantStatus string,
	wantActive int,
) {
	t.Helper()
	ctx := context.Background()
	var status string
	if err := fixture.db.ReadSQL().QueryRowContext(ctx,
		`SELECT status FROM intent_candidates WHERE id=?`, fixture.candidateID).
		Scan(&status); err != nil {
		t.Fatal(err)
	}
	var active, decisions int
	if err := fixture.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_candidate_events
WHERE candidate_id=? AND membership_state='active'`,
		fixture.candidateID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM decision_records WHERE kind=?`,
		DecisionKindIntentForwardRecovery).Scan(&decisions); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || active != wantActive || decisions != 0 {
		t.Fatalf("candidate status=%q active=%d decisions=%d, want %q/%d/0",
			status, active, decisions, wantStatus, wantActive)
	}
}
