package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func TestRecoverySnapshotSchemaV12MigrationRoundTrip(t *testing.T) {
	ctx := context.Background()
	dbPath := DBPathFromGitDir(filepath.Join(t.TempDir(), ".git"))
	d, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open v12 seed: %v", err)
	}
	seq := appendRecoveryTestEvent(t, ctx, d, "refs/heads/main", 7,
		"base-before-upgrade", "pending.txt", EventStatePending)
	if _, err := AppendDecision(ctx, d, DecisionRecord{
		DecisionTS: 1,
		Kind:       DecisionKindCaptured,
		EventSeq:   sql.NullInt64{Int64: seq, Valid: true},
	}); err != nil {
		t.Fatalf("AppendDecision before downgrade: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close v12 seed: %v", err)
	}

	// Model an on-disk v11 database: all pre-v12 data remains, while the v12
	// pure-DDL tables are absent and user_version requests the upgrade.
	raw, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatalf("open raw v11 fixture: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
DROP TABLE recovery_snapshot_events;
DROP TABLE recovery_snapshots;
PRAGMA user_version = 11;`); err != nil {
		_ = raw.Close()
		t.Fatalf("downgrade fixture to v11: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw v11 fixture: %v", err)
	}

	d, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open migrated v11 fixture: %v", err)
	}
	if version, err := d.UserVersion(ctx); err != nil || version != SchemaVersion {
		t.Fatalf("migrated user_version=(%d,%v), want (%d,nil)", version, err, SchemaVersion)
	}
	var eventCount, decisionCount int
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE seq = ?`, seq).Scan(&eventCount); err != nil {
		t.Fatalf("count preserved event: %v", err)
	}
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM decision_records WHERE event_seq = ?`, seq).Scan(&decisionCount); err != nil {
		t.Fatalf("count preserved decision: %v", err)
	}
	if eventCount != 1 || decisionCount != 1 {
		t.Fatalf("v11 rows not preserved: events=%d decisions=%d", eventCount, decisionCount)
	}

	chain, err := LoadUnpublishedRecoveryChain(ctx, d, "refs/heads/main", 7, seq)
	if err != nil {
		t.Fatalf("LoadUnpublishedRecoveryChain after migration: %v", err)
	}
	snapshot, err := TransitionRecoveryChain(ctx, d, RecoveryChainTransition{
		Expected:     chain,
		TargetState:  EventStateRecovered,
		CommitOID:    "recovery-commit-v12",
		RecoveryRef:  "refs/acd/recovery/migration-round-trip",
		Reason:       "migration round trip",
		TransitionTS: 12,
	})
	if err != nil {
		t.Fatalf("TransitionRecoveryChain after migration: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close migrated db: %v", err)
	}

	d, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("reopen migrated db: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	got, ok, err := RecoverySnapshotByRef(ctx, d, "refs/acd/recovery/migration-round-trip")
	if err != nil || !ok {
		t.Fatalf("RecoverySnapshotByRef after reopen: ok=%v err=%v", ok, err)
	}
	if got.ID != snapshot.ID || got.EventCount != 1 || got.Outcome != EventStateRecovered {
		t.Fatalf("snapshot after reopen=%+v want id=%d count=1 recovered", got, snapshot.ID)
	}
	members, err := RecoverySnapshotEvents(ctx, d, got.ID)
	if err != nil {
		t.Fatalf("RecoverySnapshotEvents after reopen: %v", err)
	}
	if len(members) != 1 || members[0].Ord != 0 || members[0].EventSeq != seq {
		t.Fatalf("snapshot membership=%+v want exact seq %d", members, seq)
	}
}

func TestRecoveryChainLoadScopedOrdered(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	_ = appendRecoveryTestEvent(t, ctx, d, "refs/heads/other", 4,
		"other-base", "other-before.txt", EventStatePending)
	first := appendRecoveryTestEvent(t, ctx, d, "refs/heads/main", 4,
		"base-one", "one.txt", EventStateBlockedConflict)
	_ = appendRecoveryTestEvent(t, ctx, d, "refs/heads/other", 4,
		"other-base", "other-after.txt", EventStateFailed)
	second := appendRecoveryTestEvent(t, ctx, d, "refs/heads/main", 4,
		"base-two", "two.txt", EventStatePending)
	_ = appendRecoveryTestEvent(t, ctx, d, "refs/heads/main", 5,
		"new-generation", "wrong-generation.txt", EventStatePending)
	_ = appendRecoveryTestEvent(t, ctx, d, "refs/heads/main", 4,
		"base-three", "already-recovered.txt", EventStateRecovered)
	_ = appendRecoveryTestEvent(t, ctx, d, "refs/heads/main", 4,
		"base-four", "already-published.txt", EventStatePublished)

	chain, err := LoadUnpublishedRecoveryChain(ctx, d, "refs/heads/main", 4, first)
	if err != nil {
		t.Fatalf("LoadUnpublishedRecoveryChain: %v", err)
	}
	if len(chain) != 2 || chain[0].Event.Seq != first || chain[1].Event.Seq != second {
		t.Fatalf("scoped chain=%+v want ordered seqs [%d %d]", chain, first, second)
	}
	if chain[0].Event.BaseHead != "base-one" || chain[1].Event.BaseHead != "base-two" {
		t.Fatalf("per-event base heads lost: %+v", chain)
	}
	for _, member := range chain {
		if len(member.Ops) != 1 || member.Ops[0].EventSeq != member.Event.Seq {
			t.Fatalf("ops not loaded exactly for seq %d: %+v", member.Event.Seq, member.Ops)
		}
	}
}

func TestPublishedRecoveryContextLoadScopedOrdered(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	_ = appendRecoveryTestEvent(t, ctx, d, "refs/heads/main", 4,
		"other-base", "unrelated-prefix.txt", EventStatePublished)
	prefix := appendRecoveryTestEvent(t, ctx, d, "refs/heads/main", 4,
		"first-base", "prefix.txt", EventStatePublished)
	first := appendRecoveryTestEvent(t, ctx, d, "refs/heads/main", 4,
		"first-base", "prefix.txt", EventStatePending)
	interleaved := appendRecoveryTestEvent(t, ctx, d, "refs/heads/main", 4,
		"second-base", "last.txt", EventStatePublished)
	last := appendRecoveryTestEvent(t, ctx, d, "refs/heads/main", 4,
		"second-base", "last.txt", EventStateFailed)
	_ = appendRecoveryTestEvent(t, ctx, d, "refs/heads/main", 4,
		"second-base", "after-suffix.txt", EventStatePublished)
	_ = appendRecoveryTestEvent(t, ctx, d, "refs/heads/main", 5,
		"first-base", "wrong-generation.txt", EventStatePublished)

	recoveryContext, err := LoadPublishedRecoveryContext(
		ctx, d, "refs/heads/main", 4, first, last,
	)
	if err != nil {
		t.Fatalf("LoadPublishedRecoveryContext: %v", err)
	}
	if len(recoveryContext) != 2 || recoveryContext[0].Event.Seq != prefix ||
		recoveryContext[1].Event.Seq != interleaved {
		t.Fatalf("recovery context=%+v want path-scoped ordered seqs [%d %d]",
			recoveryContext, prefix, interleaved)
	}
	for _, member := range recoveryContext {
		if len(member.Ops) != 1 || member.Ops[0].EventSeq != member.Event.Seq {
			t.Fatalf("ops not loaded exactly for seq %d: %+v", member.Event.Seq, member.Ops)
		}
	}
}

func TestPublishedRecoveryContextLoadIncludesRenameClosure(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	const branchRef = "refs/heads/main"
	const generation = int64(4)

	appendRename := func(oldPath, path string) int64 {
		t.Helper()
		seq, err := AppendCaptureEvent(ctx, d, CaptureEvent{
			BranchRef: branchRef, BranchGeneration: generation,
			BaseHead: "base", Operation: "rename", Path: path,
			OldPath: sqlNullStr(oldPath), Fidelity: "exact",
			State: EventStatePublished,
		}, []CaptureOp{{
			Op: "rename", Path: path, OldPath: sqlNullStr(oldPath),
			BeforeOID: sqlNullStr("before-" + oldPath), BeforeMode: sqlNullStr("100644"),
			AfterOID: sqlNullStr("after-" + path), AfterMode: sqlNullStr("100644"),
			Fidelity: "exact",
		}})
		if err != nil {
			t.Fatalf("AppendCaptureEvent rename %s -> %s: %v", oldPath, path, err)
		}
		return seq
	}

	firstRename := appendRename("a.txt", "b.txt")
	secondRename := appendRename("b.txt", "c.txt")
	first := appendRecoveryTestEvent(t, ctx, d, branchRef, generation,
		"advanced-base", "anchor.txt", EventStateBlockedConflict)
	last := appendRecoveryTestEvent(t, ctx, d, branchRef, generation,
		"advanced-base", "c.txt", EventStatePending)

	recoveryContext, err := LoadPublishedRecoveryContext(
		ctx, d, branchRef, generation, first, last,
	)
	if err != nil {
		t.Fatalf("LoadPublishedRecoveryContext: %v", err)
	}
	if len(recoveryContext) != 2 ||
		recoveryContext[0].Event.Seq != firstRename ||
		recoveryContext[1].Event.Seq != secondRename {
		t.Fatalf("recovery context=%+v want transitive rename seqs [%d %d]",
			recoveryContext, firstRename, secondRename)
	}
}

func TestRecoveryChainTransitionPublishedAtomic(t *testing.T) {
	t.Parallel()
	d, chain := seedRecoveryTestChain(t)
	ctx := context.Background()

	const proofRef = "refs/acd/recovery/main-g3-1-2/published"
	snapshot, err := TransitionRecoveryChain(ctx, d, RecoveryChainTransition{
		Expected:     chain,
		TargetState:  EventStatePublished,
		CommitOID:    "external-head-oid",
		RecoveryRef:  proofRef,
		Reason:       "composed final state matches stable HEAD",
		TransitionTS: 42,
	})
	if err != nil {
		t.Fatalf("TransitionRecoveryChain published: %v", err)
	}
	if snapshot.Outcome != EventStatePublished || !snapshot.RecoveryRef.Valid || snapshot.RecoveryRef.String != proofRef ||
		snapshot.EventCount != len(chain) {
		t.Fatalf("published snapshot=%+v", snapshot)
	}
	assertRecoveryTransitionState(t, d, chain, EventStatePublished, "external-head-oid")
	assertRecoveryBookkeeping(t, d, snapshot, chain, DecisionKindRecoveryPublished)
}

func TestRecoveryChainTransitionPublishedPrefixPreservesLaterCapture(t *testing.T) {
	t.Parallel()
	d, chain := seedRecoveryTestChain(t)
	ctx := context.Background()
	later := appendRecoveryTestEvent(
		t, ctx, d, chain[0].Event.BranchRef,
		chain[0].Event.BranchGeneration,
		"later-base", "later.txt", EventStatePending)

	snapshot, err := TransitionRecoveryChain(ctx, d, RecoveryChainTransition{
		Expected:              chain,
		TargetState:           EventStatePublished,
		CommitOID:             "frozen-external-target",
		RecoveryRef:           "refs/acd/recovery/frozen-prefix",
		Reason:                "settle frozen target only",
		TransitionTS:          42,
		AllowLaterUnpublished: true,
	})
	if err != nil {
		t.Fatalf("TransitionRecoveryChain prefix: %v", err)
	}
	if snapshot.EventCount != len(chain) {
		t.Fatalf("snapshot=%+v want %d frozen events", snapshot, len(chain))
	}
	assertRecoveryTransitionState(
		t, d, chain, EventStatePublished, "frozen-external-target")
	var laterState string
	var laterCommit sql.NullString
	if err := d.ReadSQL().QueryRowContext(ctx, `
SELECT state,commit_oid FROM capture_events WHERE seq=?`, later).Scan(
		&laterState, &laterCommit); err != nil {
		t.Fatalf("load later capture: %v", err)
	}
	if laterState != EventStatePending || laterCommit.Valid {
		t.Fatalf("later capture state=%q commit=%v want pending", laterState, laterCommit)
	}
}

func TestRecoveryChainTransitionRecoveredIsNonBarrier(t *testing.T) {
	t.Parallel()
	d, chain := seedRecoveryTestChain(t)
	ctx := context.Background()

	const recoveryRef = "refs/acd/recovery/main-g3-1-2"
	snapshot, err := TransitionRecoveryChain(ctx, d, RecoveryChainTransition{
		Expected:     chain,
		TargetState:  EventStateRecovered,
		CommitOID:    "hidden-recovery-commit",
		RecoveryRef:  recoveryRef,
		Reason:       "ambiguous chain archived before unblock",
		TransitionTS: 43,
	})
	if err != nil {
		t.Fatalf("TransitionRecoveryChain recovered: %v", err)
	}
	if !snapshot.RecoveryRef.Valid || snapshot.RecoveryRef.String != recoveryRef {
		t.Fatalf("recovered snapshot ref=%+v want %q", snapshot.RecoveryRef, recoveryRef)
	}
	assertRecoveryTransitionState(t, d, chain, EventStateRecovered, "hidden-recovery-commit")
	assertRecoveryBookkeeping(t, d, snapshot, chain, DecisionKindRecoveryArchived)

	third := appendRecoveryTestEvent(t, ctx, d, chain[0].Event.BranchRef,
		chain[0].Event.BranchGeneration, "base-three", "after-recovery.txt", EventStatePending)
	visible, err := PendingEvents(ctx, d, 0)
	if err != nil {
		t.Fatalf("PendingEvents after recovered chain: %v", err)
	}
	if len(visible) != 1 || visible[0].Seq != third {
		t.Fatalf("recovered rows formed a replay barrier: visible=%+v want seq=%d", visible, third)
	}
}

func TestRecoveryChainTransitionSupersedesIntentCandidate(t *testing.T) {
	t.Parallel()
	d, chain := seedRecoveryTestChain(t)
	ctx := context.Background()

	// A candidate can carry prior context in addition to the rows selected by
	// recovery. Every active membership must be released when any owned event
	// leaves pending state, or a later plan can pull recovered rows back in.
	contextSeq := appendRecoveryTestEvent(t, ctx, d,
		chain[0].Event.BranchRef, chain[0].Event.BranchGeneration,
		"context-base", "candidate-context.txt", EventStatePublished)
	if err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID:               "recovery-candidate",
		BranchRef:        chain[0].Event.BranchRef,
		BranchGeneration: chain[0].Event.BranchGeneration,
		Status:           IntentCandidateWaiting,
		Readiness:        IntentReadinessWait,
		Purpose:          "candidate spanning a recovered chain",
		Events: []IntentCandidateEvent{
			{EventSeq: chain[0].Event.Seq, EventRole: "code"},
			{EventSeq: chain[1].Event.Seq, EventRole: "test"},
			{EventSeq: contextSeq, EventRole: "context"},
		},
	}); err != nil {
		t.Fatalf("SaveIntentCandidate: %v", err)
	}

	if _, err := TransitionRecoveryChain(ctx, d, RecoveryChainTransition{
		Expected:     chain,
		TargetState:  EventStateRecovered,
		CommitOID:    "candidate-recovery-commit",
		RecoveryRef:  "refs/acd/recovery/candidate-membership",
		Reason:       "candidate membership recovery",
		TransitionTS: 45,
	}); err != nil {
		t.Fatalf("TransitionRecoveryChain: %v", err)
	}

	var status, readiness string
	var deadline sql.NullFloat64
	if err := d.SQL().QueryRowContext(ctx, `
SELECT status,readiness,soft_publication_deadline
FROM intent_candidates WHERE id='recovery-candidate'`).Scan(
		&status, &readiness, &deadline); err != nil {
		t.Fatalf("load recovery candidate: %v", err)
	}
	if status != IntentCandidateSuperseded || readiness != IntentReadinessWait || deadline.Valid {
		t.Fatalf("candidate after recovery=(%q,%q,%+v), want superseded,wait,NULL",
			status, readiness, deadline)
	}
	var active, superseded int
	if err := d.SQL().QueryRowContext(ctx, `
SELECT COALESCE(SUM(membership_state='active'),0),
       COALESCE(SUM(membership_state='superseded'),0)
FROM intent_candidate_events WHERE candidate_id='recovery-candidate'`).Scan(
		&active, &superseded); err != nil {
		t.Fatalf("count recovery candidate membership: %v", err)
	}
	if active != 0 || superseded != 3 {
		t.Fatalf("candidate membership active=%d superseded=%d want 0,3",
			active, superseded)
	}
}

func TestRecoveryChainTransitionRollsBackCandidateRetirement(t *testing.T) {
	t.Parallel()
	d, chain := seedRecoveryTestChain(t)
	ctx := context.Background()
	if err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID:               "rollback-candidate",
		BranchRef:        chain[0].Event.BranchRef,
		BranchGeneration: chain[0].Event.BranchGeneration,
		Status:           IntentCandidateWaiting,
		Readiness:        IntentReadinessWait,
		Purpose:          "candidate retirement rollback",
		Events: []IntentCandidateEvent{
			{EventSeq: chain[0].Event.Seq, EventRole: "code"},
			{EventSeq: chain[1].Event.Seq, EventRole: "test"},
		},
	}); err != nil {
		t.Fatalf("SaveIntentCandidate: %v", err)
	}
	if _, err := d.SQL().ExecContext(ctx, `
CREATE TRIGGER recovery_abort_candidate_retirement
BEFORE UPDATE OF status ON intent_candidates
WHEN OLD.id = 'rollback-candidate' AND NEW.status = 'superseded'
BEGIN
    SELECT RAISE(ABORT, 'synthetic candidate retirement failure');
END`); err != nil {
		t.Fatalf("create candidate rollback trigger: %v", err)
	}

	_, err := TransitionRecoveryChain(ctx, d, RecoveryChainTransition{
		Expected:     chain,
		TargetState:  EventStateRecovered,
		CommitOID:    "must-roll-back-candidate",
		RecoveryRef:  "refs/acd/recovery/candidate-rollback",
		TransitionTS: 46,
	})
	if err == nil {
		t.Fatal("TransitionRecoveryChain unexpectedly retired candidate through abort trigger")
	}
	assertRecoveryNoPartialMutation(t, d, chain, 0)

	var status string
	var active int
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT status FROM intent_candidates WHERE id='rollback-candidate'`).Scan(
		&status); err != nil {
		t.Fatalf("load rollback candidate: %v", err)
	}
	if err := d.SQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_candidate_events
WHERE candidate_id='rollback-candidate' AND membership_state='active'`).Scan(
		&active); err != nil {
		t.Fatalf("count rollback candidate membership: %v", err)
	}
	if status != IntentCandidateWaiting || active != 2 {
		t.Fatalf("candidate changed despite rollback: status=%q active=%d",
			status, active)
	}
}

func TestRecoveryChainTransitionPreservesUnrelatedBookkeeping(t *testing.T) {
	t.Parallel()
	d, chain := seedRecoveryTestChain(t)
	ctx := context.Background()

	// Recovery may settle only its own planner rows and publish breadcrumb.
	// Seed an unrelated planner row and replace the singleton breadcrumb with
	// another anchor; both must survive this chain's transition.
	unrelatedSeq := appendRecoveryTestEvent(t, ctx, d, "refs/heads/other", 9,
		"other-base", "other.txt", EventStatePending)
	if _, err := d.SQL().ExecContext(ctx, `
INSERT INTO planner_state(event_seq, defer_count, last_planned_ts)
VALUES (?, 2, 2)`, unrelatedSeq); err != nil {
		t.Fatalf("seed unrelated planner row: %v", err)
	}
	if _, err := d.SQL().ExecContext(ctx, `
UPDATE publish_state
SET event_seq = ?, branch_ref = 'refs/heads/other', branch_generation = 9,
    status = 'blocked_conflict', error = 'other conflict'
WHERE id = 1`, unrelatedSeq); err != nil {
		t.Fatalf("seed unrelated publish breadcrumb: %v", err)
	}

	if _, err := TransitionRecoveryChain(ctx, d, RecoveryChainTransition{
		Expected:     chain,
		TargetState:  EventStateRecovered,
		CommitOID:    "recovery-with-unrelated-state",
		RecoveryRef:  "refs/acd/recovery/preserve-unrelated",
		Reason:       "preserve unrelated bookkeeping",
		TransitionTS: 44,
	}); err != nil {
		t.Fatalf("TransitionRecoveryChain: %v", err)
	}
	var plannerCount int
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM planner_state WHERE event_seq = ?`, unrelatedSeq).Scan(&plannerCount); err != nil {
		t.Fatalf("count unrelated planner row: %v", err)
	}
	var pubSeq int64
	var pubRef, pubStatus string
	if err := d.SQL().QueryRowContext(ctx, `
SELECT event_seq, branch_ref, status FROM publish_state WHERE id = 1`).Scan(
		&pubSeq, &pubRef, &pubStatus); err != nil {
		t.Fatalf("load unrelated publish breadcrumb: %v", err)
	}
	if plannerCount != 1 || pubSeq != unrelatedSeq || pubRef != "refs/heads/other" || pubStatus != "blocked_conflict" {
		t.Fatalf("unrelated bookkeeping changed: planner=%d pub=(%d,%q,%q)",
			plannerCount, pubSeq, pubRef, pubStatus)
	}
	for _, key := range replayRecoveryMetaTestKeys() {
		if value, ok, err := MetaGet(ctx, d, key); err != nil || !ok || value != "stale" {
			t.Fatalf("unrelated meta %s=(%q,%v,%v), want stale,true,nil", key, value, ok, err)
		}
	}
}

func TestRecoveryChainRaceRefusesChangedSuffix(t *testing.T) {
	t.Parallel()
	d, chain := seedRecoveryTestChain(t)
	ctx := context.Background()

	extra := appendRecoveryTestEvent(t, ctx, d, chain[0].Event.BranchRef,
		chain[0].Event.BranchGeneration, "racing-base", "racing.txt", EventStatePending)
	_, err := TransitionRecoveryChain(ctx, d, RecoveryChainTransition{
		Expected:     chain,
		TargetState:  EventStateRecovered,
		CommitOID:    "must-not-land",
		RecoveryRef:  "refs/acd/recovery/stale-expected-chain",
		TransitionTS: 50,
	})
	if !errors.Is(err, ErrRecoveryChainChanged) {
		t.Fatalf("TransitionRecoveryChain race err=%v want ErrRecoveryChainChanged", err)
	}
	assertRecoveryNoPartialMutation(t, d, chain, extra)
}

func TestRecoveryChainTransitionRollsBackOnPartialFailure(t *testing.T) {
	t.Parallel()
	d, chain := seedRecoveryTestChain(t)
	ctx := context.Background()
	secondSeq := chain[1].Event.Seq
	if _, err := d.SQL().ExecContext(ctx, fmt.Sprintf(`
CREATE TRIGGER recovery_abort_second
BEFORE UPDATE OF state ON capture_events
WHEN OLD.seq = %d
BEGIN
    SELECT RAISE(ABORT, 'synthetic recovery failure');
END`, secondSeq)); err != nil {
		t.Fatalf("create rollback trigger: %v", err)
	}

	_, err := TransitionRecoveryChain(ctx, d, RecoveryChainTransition{
		Expected:     chain,
		TargetState:  EventStateRecovered,
		CommitOID:    "must-roll-back",
		RecoveryRef:  "refs/acd/recovery/rollback",
		TransitionTS: 51,
	})
	if err == nil {
		t.Fatalf("TransitionRecoveryChain unexpectedly succeeded through abort trigger")
	}
	assertRecoveryNoPartialMutation(t, d, chain, 0)
}

func seedRecoveryTestChain(t *testing.T) (*DB, []RecoveryChainEvent) {
	t.Helper()
	d, _ := openTestDB(t)
	ctx := context.Background()
	const branchRef = "refs/heads/main"
	const generation = int64(3)

	first := appendRecoveryTestEvent(t, ctx, d, branchRef, generation,
		"base-one", "first.txt", EventStatePending)
	if err := MarkEventBlocked(ctx, d, first, "before-state mismatch", 10,
		sqlNullStr(branchRef), sql.NullInt64{Int64: generation, Valid: true},
		sqlNullStr("base-one")); err != nil {
		t.Fatalf("MarkEventBlocked: %v", err)
	}
	second := appendRecoveryTestEvent(t, ctx, d, branchRef, generation,
		"base-two", "second.txt", EventStatePending)
	for _, seq := range []int64{first, second} {
		if _, err := d.SQL().ExecContext(ctx, `
INSERT INTO planner_state(event_seq, defer_count, last_planned_ts)
VALUES (?, 1, 1)`, seq); err != nil {
			t.Fatalf("seed planner state seq=%d: %v", seq, err)
		}
	}
	for _, key := range replayRecoveryMetaTestKeys() {
		if err := MetaSet(ctx, d, key, "stale"); err != nil {
			t.Fatalf("seed meta %s: %v", key, err)
		}
	}
	chain, err := LoadUnpublishedRecoveryChain(ctx, d, branchRef, generation, first)
	if err != nil {
		t.Fatalf("LoadUnpublishedRecoveryChain fixture: %v", err)
	}
	if len(chain) != 2 || chain[0].Event.Seq != first || chain[1].Event.Seq != second {
		t.Fatalf("fixture chain=%+v", chain)
	}
	return d, chain
}

func appendRecoveryTestEvent(t *testing.T, ctx context.Context, d *DB, branchRef string, generation int64, baseHead, path, eventState string) int64 {
	t.Helper()
	seq, err := AppendCaptureEvent(ctx, d, CaptureEvent{
		BranchRef:        branchRef,
		BranchGeneration: generation,
		BaseHead:         baseHead,
		Operation:        "modify",
		Path:             path,
		Fidelity:         "exact",
		State:            eventState,
	}, []CaptureOp{{
		Op:         "modify",
		Path:       path,
		BeforeOID:  sqlNullStr("before-" + path),
		BeforeMode: sqlNullStr("100644"),
		AfterOID:   sqlNullStr("after-" + path),
		AfterMode:  sqlNullStr("100644"),
		Fidelity:   "exact",
	}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent %s: %v", path, err)
	}
	return seq
}

func assertRecoveryTransitionState(t *testing.T, d *DB, original []RecoveryChainEvent, wantState, wantCommit string) {
	t.Helper()
	ctx := context.Background()
	for _, expected := range original {
		var stateName, commitOID, branchRef, baseHead, operation, path, fidelity string
		var generation int64
		var oldPath sql.NullString
		if err := d.SQL().QueryRowContext(ctx, `
SELECT state, commit_oid, branch_ref, branch_generation, base_head,
       operation, path, old_path, fidelity
FROM capture_events WHERE seq = ?`, expected.Event.Seq).Scan(
			&stateName, &commitOID, &branchRef, &generation, &baseHead,
			&operation, &path, &oldPath, &fidelity); err != nil {
			t.Fatalf("load transitioned seq=%d: %v", expected.Event.Seq, err)
		}
		if stateName != wantState || commitOID != wantCommit {
			t.Fatalf("seq=%d lifecycle=(%q,%q) want (%q,%q)",
				expected.Event.Seq, stateName, commitOID, wantState, wantCommit)
		}
		if branchRef != expected.Event.BranchRef || generation != expected.Event.BranchGeneration ||
			baseHead != expected.Event.BaseHead || operation != expected.Event.Operation ||
			path != expected.Event.Path || oldPath != expected.Event.OldPath || fidelity != expected.Event.Fidelity {
			t.Fatalf("seq=%d provenance changed: got branch=%q gen=%d base=%q op=%q path=%q old=%+v fidelity=%q want event=%+v",
				expected.Event.Seq, branchRef, generation, baseHead, operation, path,
				oldPath, fidelity, expected.Event)
		}
		ops, err := LoadCaptureOps(ctx, d, expected.Event.Seq)
		if err != nil {
			t.Fatalf("LoadCaptureOps seq=%d: %v", expected.Event.Seq, err)
		}
		if !sameRecoveryChain(
			[]RecoveryChainEvent{{Event: expected.Event, Ops: ops}},
			[]RecoveryChainEvent{{Event: expected.Event, Ops: expected.Ops}},
		) {
			t.Fatalf("seq=%d capture ops changed: got=%+v want=%+v", expected.Event.Seq, ops, expected.Ops)
		}
	}
}

func assertRecoveryBookkeeping(t *testing.T, d *DB, snapshot RecoverySnapshot, chain []RecoveryChainEvent, decisionKind string) {
	t.Helper()
	ctx := context.Background()
	loaded, ok, err := RecoverySnapshotByID(ctx, d, snapshot.ID)
	if err != nil || !ok || loaded != snapshot {
		t.Fatalf("RecoverySnapshotByID=(%+v,%v,%v) want (%+v,true,nil)", loaded, ok, err, snapshot)
	}
	members, err := RecoverySnapshotEvents(ctx, d, snapshot.ID)
	if err != nil {
		t.Fatalf("RecoverySnapshotEvents: %v", err)
	}
	if len(members) != len(chain) {
		t.Fatalf("membership count=%d want %d", len(members), len(chain))
	}
	for i := range chain {
		if members[i].Ord != i || members[i].EventSeq != chain[i].Event.Seq {
			t.Fatalf("membership[%d]=%+v want seq=%d", i, members[i], chain[i].Event.Seq)
		}
	}
	var plannerCount, decisionCount int
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM planner_state`).Scan(&plannerCount); err != nil {
		t.Fatalf("count planner state: %v", err)
	}
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM decision_records WHERE kind = ?`, decisionKind).Scan(&decisionCount); err != nil {
		t.Fatalf("count recovery decisions: %v", err)
	}
	if plannerCount != 0 || decisionCount != len(chain) {
		t.Fatalf("bookkeeping planner=%d decisions=%d want 0,%d", plannerCount, decisionCount, len(chain))
	}
	var status string
	var eventSeq sql.NullInt64
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT status, event_seq FROM publish_state WHERE id = 1`).Scan(&status, &eventSeq); err != nil {
		t.Fatalf("load publish_state: %v", err)
	}
	wantStatus := "ok"
	if snapshot.Outcome == EventStatePublished {
		wantStatus = EventStatePublished
	}
	if status != wantStatus || eventSeq.Valid {
		t.Fatalf("publish breadcrumb=(%q,%+v) want %s,NULL", status, eventSeq, wantStatus)
	}
	for _, key := range replayRecoveryMetaTestKeys() {
		if _, ok, err := MetaGet(ctx, d, key); err != nil || ok {
			t.Fatalf("meta %s after transition: ok=%v err=%v want absent", key, ok, err)
		}
	}
}

func replayRecoveryMetaTestKeys() []string {
	return []string{
		"last_replay_conflict",
		"last_replay_conflict_legacy",
		"last_replay_error",
		"replay.error_repeat_count",
		"replay.error_last_seen_ts",
	}
}

func assertRecoveryNoPartialMutation(t *testing.T, d *DB, chain []RecoveryChainEvent, extraSeq int64) {
	t.Helper()
	ctx := context.Background()
	for _, expected := range chain {
		var gotState string
		if err := d.SQL().QueryRowContext(ctx,
			`SELECT state FROM capture_events WHERE seq = ?`, expected.Event.Seq).Scan(&gotState); err != nil {
			t.Fatalf("load unchanged seq=%d: %v", expected.Event.Seq, err)
		}
		if gotState != expected.Event.State {
			t.Fatalf("seq=%d state=%q want original %q", expected.Event.Seq, gotState, expected.Event.State)
		}
	}
	if extraSeq > 0 {
		var gotState string
		if err := d.SQL().QueryRowContext(ctx,
			`SELECT state FROM capture_events WHERE seq = ?`, extraSeq).Scan(&gotState); err != nil {
			t.Fatalf("load racing seq=%d: %v", extraSeq, err)
		}
		if gotState != EventStatePending {
			t.Fatalf("racing seq=%d state=%q want pending", extraSeq, gotState)
		}
	}
	var snapshots, memberships, decisions, planner int
	for query, dest := range map[string]*int{
		`SELECT COUNT(*) FROM recovery_snapshots`:       &snapshots,
		`SELECT COUNT(*) FROM recovery_snapshot_events`: &memberships,
		`SELECT COUNT(*) FROM decision_records`:         &decisions,
		`SELECT COUNT(*) FROM planner_state`:            &planner,
	} {
		if err := d.SQL().QueryRowContext(ctx, query).Scan(dest); err != nil {
			t.Fatalf("rollback count %q: %v", query, err)
		}
	}
	if snapshots != 0 || memberships != 0 || decisions != 0 || planner != len(chain) {
		t.Fatalf("partial recovery write: snapshots=%d memberships=%d decisions=%d planner=%d want 0,0,0,%d",
			snapshots, memberships, decisions, planner, len(chain))
	}
	var pubStatus string
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT status FROM publish_state WHERE id = 1`).Scan(&pubStatus); err != nil {
		t.Fatalf("load unchanged publish_state: %v", err)
	}
	if pubStatus != "blocked_conflict" {
		t.Fatalf("publish_state=%q want blocked_conflict after rollback", pubStatus)
	}
}
