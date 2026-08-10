package state

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

func TestReconcileCheckpointIntentMembershipsRetiresRecoveredMember(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t)
	ctx := context.Background()
	pending := appendIntentV2Event(t, db, "refs/heads/main", 1, "current.go")
	recoveredCreate := appendIntentV2Event(t, db, "refs/heads/main", 1, "legacy.go")
	recoveredModify := appendIntentV2Event(t, db, "refs/heads/main", 1, "legacy.go")
	publishedCreate := appendIntentV2Event(t, db, "refs/heads/main", 1, "published.go")
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='recovered' WHERE seq IN (?,?)`,
		recoveredCreate, recoveredModify); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='published',commit_oid='published-commit',published_ts=2 WHERE seq=?`,
		publishedCreate); err != nil {
		t.Fatal(err)
	}
	seedCompletedCheckpointEvent(t, db, pending)
	if err := SaveIntentCandidate(ctx, db, IntentCandidate{
		ID: "legacy-mixed", BranchRef: "refs/heads/main", BranchGeneration: 1,
		Status: IntentCandidateWaiting, Readiness: IntentReadinessWait,
		Purpose: "legacy mixed membership",
		Events: []IntentCandidateEvent{
			{EventSeq: recoveredCreate, EventRole: "code"},
			{EventSeq: recoveredModify, EventRole: "code"},
			{EventSeq: publishedCreate, EventRole: "code"},
			{EventSeq: pending, EventRole: "code"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	result, err := ReconcileCheckpointIntentMemberships(ctx, db, "0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.RetiredCandidates) != 1 || result.RetiredCandidates[0] != "legacy-mixed" || result.OperationID == "" {
		t.Fatalf("result=%+v", result)
	}
	var status, membership string
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT status FROM intent_candidates WHERE id='legacy-mixed'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRowContext(ctx, `
SELECT membership_state FROM intent_candidate_events
WHERE candidate_id='legacy-mixed' AND event_seq=?`, pending).Scan(&membership); err != nil {
		t.Fatal(err)
	}
	if status != IntentCandidateSuperseded || membership != IntentMembershipSuperseded {
		t.Fatalf("status=%q membership=%q", status, membership)
	}
	var pendingState, recoveredState, publishedState string
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT state FROM capture_events WHERE seq=?`, pending).Scan(&pendingState); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT state FROM capture_events WHERE seq=?`, recoveredCreate).Scan(&recoveredState); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT state FROM capture_events WHERE seq=?`, publishedCreate).Scan(&publishedState); err != nil {
		t.Fatal(err)
	}
	if pendingState != EventStatePending || recoveredState != EventStateRecovered || publishedState != EventStatePublished {
		t.Fatalf("event states changed: pending=%q recovered=%q published=%q",
			pendingState, recoveredState, publishedState)
	}

	again, err := ReconcileCheckpointIntentMemberships(ctx, db, "0123456789abcdef")
	if err != nil || len(again.RetiredCandidates) != 0 || again.OperationID != "" {
		t.Fatalf("idempotent result=%+v err=%v", again, err)
	}
}

func TestReconcileCheckpointIntentMembershipsKeepsPendingCheckpointCandidate(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t)
	ctx := context.Background()
	seq := appendIntentV2Event(t, db, "refs/heads/main", 1, "valid.go")
	seedCompletedCheckpointEvent(t, db, seq)
	if err := SaveIntentCandidate(ctx, db, IntentCandidate{
		ID: "valid", BranchRef: "refs/heads/main", BranchGeneration: 1,
		Status: IntentCandidateWaiting, Readiness: IntentReadinessWait,
		Purpose: "valid checkpoint candidate",
		Events:  []IntentCandidateEvent{{EventSeq: seq, EventRole: "code"}},
	}); err != nil {
		t.Fatal(err)
	}
	result, err := ReconcileCheckpointIntentMemberships(ctx, db, "0123456789abcdef")
	if err != nil || len(result.RetiredCandidates) != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	var status string
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT status FROM intent_candidates WHERE id='valid'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != IntentCandidateWaiting {
		t.Fatalf("status=%q", status)
	}
}

func seedCompletedCheckpointEvent(t *testing.T, db *DB, eventSeq int64) {
	t.Helper()
	ctx := context.Background()
	suffix := strconv.FormatInt(eventSeq, 10)
	opID := "checkpoint-op-" + suffix
	cpID := "cp-test-" + suffix
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO operations(id,kind,worktree_id,phase,status,plan_digest,error,created_ts,updated_ts)
VALUES(?, 'checkpoint', '0123456789abcdef', 'completed', 'completed', ?, '', 1, 1)`,
		opID, "sha256:"+strings.Repeat("0", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO checkpoints(id,seq,operation_id,worktree_id,reason,observation_epoch,coverage_epoch,
 observed_head,observed_ref,tree_oid,commit_oid,checkpoint_ref,phase,created_ts,completed_ts)
VALUES(?, ?, ?, '0123456789abcdef', 'poll', 1, 1, '', 'refs/heads/main', 'tree', 'commit', ?, 'completed', 1, 1)`,
		cpID, eventSeq, opID, "refs/acd/checkpoints/v1/0123456789abcdef/"+cpID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx,
		`INSERT INTO checkpoint_events(checkpoint_id,ord,event_seq) VALUES(?,0,?)`, cpID, eventSeq); err != nil {
		t.Fatal(err)
	}
}
