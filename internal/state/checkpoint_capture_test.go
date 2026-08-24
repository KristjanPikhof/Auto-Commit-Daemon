package state

import (
	"context"
	"path/filepath"
	"testing"
)

func TestCheckpointCaptureCancellationCannotOrphanEventOrShadow(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	checkpoint := Checkpoint{
		ID: "cp-1-0123456789abcdef", OperationID: "op-cp-atomic", WorktreeID: "0123456789abcdef",
		Reason: CheckpointReasonPoll, ObservationEpoch: 1, CoverageEpoch: 1,
		TreeOID: "tree", CommitOID: "commit", Ref: "refs/acd/checkpoints/v1/0123456789abcdef/cp-1-0123456789abcdef",
	}
	const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if _, err := PrepareCheckpoint(ctx, db, checkpoint, digest); err != nil {
		t.Fatal(err)
	}
	if err := CompleteCheckpoint(ctx, db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 1); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = AttachCheckpointCaptures(cancelled, db, checkpoint.ID, []CheckpointCapture{{
		Event: CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: "base", Operation: "modify", Path: "file.txt", Fidelity: "exact"},
		Ops: []CaptureOp{{Op: "modify", Path: "file.txt", Fidelity: "exact"}},
		ShadowUpsert: &ShadowPath{BranchRef: "refs/heads/main", BranchGeneration: 1,
			Path: "file.txt", Operation: "modify", BaseHead: "base", Fidelity: "exact"},
	}})
	if err == nil {
		t.Fatal("cancelled attach succeeded")
	}
	var events, memberships, shadows int
	for query, destination := range map[string]*int{
		`SELECT COUNT(*) FROM capture_events`:    &events,
		`SELECT COUNT(*) FROM checkpoint_events`: &memberships,
		`SELECT COUNT(*) FROM shadow_paths`:      &shadows,
	} {
		if err := db.SQL().QueryRowContext(ctx, query).Scan(destination); err != nil {
			t.Fatal(err)
		}
	}
	if events != 0 || memberships != 0 || shadows != 0 {
		t.Fatalf("events=%d memberships=%d shadows=%d", events, memberships, shadows)
	}
}

func TestCheckpointRetentionDoesNotPublishZeroMemberProtection(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cp := Checkpoint{
		ID: "cp-2-0123456789abcdef", OperationID: "op-cp-retention",
		WorktreeID: "0123456789abcdef", Reason: CheckpointReasonManualBarrier,
		ObservationEpoch: 2, CoverageEpoch: 2, TreeOID: "tree", CommitOID: "commit",
		Ref: "refs/acd/checkpoints/v1/0123456789abcdef/cp-2-0123456789abcdef",
	}
	const digest = "sha256:1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if _, err := PrepareCheckpoint(ctx, db, cp, digest); err != nil {
		t.Fatal(err)
	}
	if err := CompleteCheckpoint(ctx, db, cp.ID, cp.Ref, cp.CommitOID, 2); err != nil {
		t.Fatal(err)
	}
	items, err := RetentionCheckpoints(ctx, db, cp.WorktreeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Published {
		t.Fatalf("retention=%+v", items)
	}
}

func TestPublishedEventPruningWaitsForCheckpointRetention(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cp := Checkpoint{
		ID: "cp-3-0123456789abcdef", OperationID: "op-cp-event-retention",
		WorktreeID: "0123456789abcdef", Reason: CheckpointReasonPoll,
		ObservationEpoch: 3, CoverageEpoch: 3, TreeOID: "tree", CommitOID: "commit",
		Ref: "refs/acd/checkpoints/v1/0123456789abcdef/cp-3-0123456789abcdef",
	}
	const digest = "sha256:2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if _, err := PrepareCheckpoint(ctx, db, cp, digest); err != nil {
		t.Fatal(err)
	}
	if err := CompleteCheckpoint(ctx, db, cp.ID, cp.Ref, cp.CommitOID, 3); err != nil {
		t.Fatal(err)
	}
	seqs, err := AttachCheckpointCaptures(ctx, db, cp.ID, []CheckpointCapture{{
		Event: CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: "base", Operation: "create", Path: "old.txt",
			Fidelity: "exact", CapturedTS: 10},
		Ops: []CaptureOp{{Op: "create", Path: "old.txt", Fidelity: "exact"}},
	}})
	if err != nil || len(seqs) != 1 {
		t.Fatalf("AttachCheckpointCaptures: seqs=%v err=%v", seqs, err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events
SET state='published', published_ts=20, commit_oid='published'
WHERE seq=?`, seqs[0]); err != nil {
		t.Fatal(err)
	}
	if pruned, err := PrunePublishedEventsBefore(ctx, db, 100); err != nil || pruned != 0 {
		t.Fatalf("retained checkpoint prune=(%d,%v), want (0,nil)", pruned, err)
	}

	items, err := RetentionCheckpoints(ctx, db, cp.WorktreeID)
	if err != nil || len(items) != 1 {
		t.Fatalf("RetentionCheckpoints: items=%v err=%v", items, err)
	}
	operationID, err := PrepareCheckpointPrune(ctx, db, items[0], digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := CompleteCheckpointPrune(
		ctx, db, operationID, cp.ID, cp.Ref, cp.CommitOID); err != nil {
		t.Fatal(err)
	}
	if pruned, err := PrunePublishedEventsBefore(ctx, db, 100); err != nil || pruned != 1 {
		t.Fatalf("expired checkpoint prune=(%d,%v), want (1,nil)", pruned, err)
	}
}
