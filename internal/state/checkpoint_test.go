package state

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

const checkpointTestDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestCheckpointPrepareCompleteAndReadOnlyProjection(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	result, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
    branch_ref, branch_generation, base_head, operation, path,
    fidelity, captured_ts, state
) VALUES ('refs/heads/main', 1, 'base', 'modify', 'safe.txt',
          'exact', 1, 'pending')`)
	if err != nil {
		t.Fatal(err)
	}
	eventSeq, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}

	want := Checkpoint{
		ID:               "cp-1786060000000-0123456789abcdef",
		OperationID:      "op-checkpoint-1",
		WorktreeID:       "0123456789abcdef",
		Reason:           CheckpointReasonPoll,
		ObservationEpoch: 7,
		CoverageEpoch:    7,
		ObservedHead:     "head",
		ObservedRef:      "refs/heads/main",
		TreeOID:          "tree",
		CommitOID:        "commit",
		Ref:              "refs/acd/checkpoints/v1/0123456789abcdef/cp-1786060000000-0123456789abcdef",
		CreatedTS:        10,
		EventSeqs:        []int64{eventSeq},
		Exclusions: []CheckpointExclusion{
			{Category: "ignored", Count: 3},
			{Category: "sensitive", Count: 1},
		},
	}
	created, err := PrepareCheckpoint(ctx, db, want, checkpointTestDigest)
	if err != nil || !created {
		t.Fatalf("prepare=(%t,%v), want (true,nil)", created, err)
	}
	created, err = PrepareCheckpoint(ctx, db, want, checkpointTestDigest)
	if err != nil || created {
		t.Fatalf("idempotent prepare=(%t,%v), want (false,nil)", created, err)
	}

	projection, err := ReadCheckpointProjection(ctx, dbPath, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !projection.Available || projection.Prepared != 1 || projection.Completed != 0 ||
		len(projection.Recoverable) != 1 || projection.Latest == nil {
		t.Fatalf("prepared projection = %+v", projection)
	}
	if got := projection.Recoverable[0].ID; got != want.ID {
		t.Fatalf("recoverable id=%q want=%q", got, want.ID)
	}

	if err := CompleteCheckpoint(ctx, db, want.ID, want.Ref, want.CommitOID, 20); err != nil {
		t.Fatal(err)
	}
	if err := CompleteCheckpoint(ctx, db, want.ID, want.Ref, want.CommitOID, 30); err != nil {
		t.Fatalf("idempotent completion: %v", err)
	}
	projection, err = ReadCheckpointProjection(ctx, dbPath, 10)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Prepared != 0 || projection.Completed != 1 || len(projection.Recoverable) != 0 {
		t.Fatalf("completed projection = %+v", projection)
	}
	if projection.Latest == nil || projection.Latest.Phase != CheckpointCompleted {
		t.Fatalf("latest = %+v", projection.Latest)
	}
	var operationStatus string
	if err := db.ReadSQL().QueryRowContext(ctx,
		`SELECT status FROM operations WHERE id=?`, want.OperationID).Scan(&operationStatus); err != nil {
		t.Fatal(err)
	}
	if operationStatus != "completed" {
		t.Fatalf("operation status=%q want completed", operationStatus)
	}
	if sync, err := db.PragmaInt(ctx, "synchronous"); err != nil || sync != 1 {
		t.Fatalf("synchronous=(%d,%v), want NORMAL(1)", sync, err)
	}
}

func TestCheckpointIdentityAndAmbiguityFailClosed(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	want := Checkpoint{
		ID:               "cp-1786060000001-fedcba9876543210",
		OperationID:      "op-checkpoint-2",
		WorktreeID:       "fedcba9876543210",
		Reason:           CheckpointReasonManualBarrier,
		ObservationEpoch: 1,
		CoverageEpoch:    1,
		TreeOID:          "tree",
		CommitOID:        "commit",
		Ref:              "refs/acd/checkpoints/v1/fedcba9876543210/cp-1786060000001-fedcba9876543210",
	}
	if _, err := PrepareCheckpoint(ctx, db, want, checkpointTestDigest); err != nil {
		t.Fatal(err)
	}
	changed := want
	changed.TreeOID = "other-tree"
	if _, err := PrepareCheckpoint(ctx, db, changed, checkpointTestDigest); !errors.Is(err, ErrCheckpointIdentityMismatch) {
		t.Fatalf("identity mismatch error=%v", err)
	}
	if err := CompleteCheckpoint(ctx, db, want.ID, want.Ref, "other-commit", 2); !errors.Is(err, ErrCheckpointIdentityMismatch) {
		t.Fatalf("completion mismatch error=%v", err)
	}
	if err := MarkCheckpointNeedsAction(ctx, db, want.ID, "private ref points at an unexpected object"); err != nil {
		t.Fatal(err)
	}
	if err := CompleteCheckpoint(ctx, db, want.ID, want.Ref, want.CommitOID, 2); !errors.Is(err, ErrCheckpointPhaseConflict) {
		t.Fatalf("needs-action completion error=%v", err)
	}
}

func TestCompletedCheckpointForBarrierMatchesExactIdentity(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	checkpoints := []Checkpoint{
		{
			ID: "cp-1786060000009-dddddddddddddddd", OperationID: "op-barrier-stale",
			WorktreeID: "0123456789abcdef", Reason: CheckpointReasonPoll,
			ObservationEpoch: 100, CoverageEpoch: 100,
			ObservedHead: "stale-main-head", ObservedRef: "refs/heads/main",
			TreeOID: "stale-main-tree", CommitOID: "stale-main-commit",
			Ref:       "refs/acd/checkpoints/v1/0123456789abcdef/cp-1786060000009-dddddddddddddddd",
			CreatedTS: 9,
		},
		{
			ID: "cp-1786060000010-aaaaaaaaaaaaaaaa", OperationID: "op-barrier-feature",
			WorktreeID: "0123456789abcdef", Reason: CheckpointReasonPoll,
			ObservationEpoch: 10, CoverageEpoch: 10,
			ObservedHead: "feature-head", ObservedRef: "refs/heads/feature",
			TreeOID: "feature-tree", CommitOID: "feature-commit",
			Ref:       "refs/acd/checkpoints/v1/0123456789abcdef/cp-1786060000010-aaaaaaaaaaaaaaaa",
			CreatedTS: 10,
		},
		{
			ID: "cp-1786060000011-bbbbbbbbbbbbbbbb", OperationID: "op-barrier-peer",
			WorktreeID: "fedcba9876543210", Reason: CheckpointReasonPoll,
			ObservationEpoch: 11, CoverageEpoch: 11,
			ObservedHead: "main-head", ObservedRef: "refs/heads/main",
			TreeOID: "peer-tree", CommitOID: "peer-commit",
			Ref:       "refs/acd/checkpoints/v1/fedcba9876543210/cp-1786060000011-bbbbbbbbbbbbbbbb",
			CreatedTS: 11,
		},
		{
			ID: "cp-1786060000012-cccccccccccccccc", OperationID: "op-barrier-main",
			WorktreeID: "0123456789abcdef", Reason: CheckpointReasonPoll,
			ObservationEpoch: 12, CoverageEpoch: 12,
			ObservedHead: "main-head", ObservedRef: "refs/heads/main",
			TreeOID: "main-tree", CommitOID: "main-commit",
			Ref:       "refs/acd/checkpoints/v1/0123456789abcdef/cp-1786060000012-cccccccccccccccc",
			CreatedTS: 12,
		},
	}
	for _, checkpoint := range checkpoints {
		if created, err := PrepareCheckpoint(
			ctx, db, checkpoint, checkpointTestDigest); err != nil || !created {
			t.Fatalf("prepare %s=(%t,%v)", checkpoint.ID, created, err)
		}
		if err := CompleteCheckpoint(
			ctx, db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID,
			checkpoint.CreatedTS+1); err != nil {
			t.Fatal(err)
		}
	}

	checkpoint, ok, err := CompletedCheckpointForBarrier(
		ctx, db, "0123456789abcdef", 10, 0, "refs/heads/main")
	if err != nil || !ok || checkpoint.ID != checkpoints[3].ID {
		t.Fatalf("checkpoint=(%+v,%t,%v)", checkpoint, ok, err)
	}
	if _, ok, err := CompletedCheckpointForBarrier(
		ctx, db, "0123456789abcdef", 13, int64(len(checkpoints)),
		"refs/heads/main"); err != nil || ok {
		t.Fatalf("future checkpoint=(%t,%v), want false,nil", ok, err)
	}
	if _, _, err := CompletedCheckpointForBarrier(
		ctx, db, "short", 1, 0,
		"refs/heads/main"); !errors.Is(err, ErrCheckpointIdentityMismatch) {
		t.Fatalf("invalid identity error=%v", err)
	}
}

func TestCheckpointProjectionDoesNotMigrateV19(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v19.db")
	conn, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA user_version=19"); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	projection, err := ReadCheckpointProjection(ctx, path, 10)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Available || projection.SchemaVersion != 19 {
		t.Fatalf("projection=%+v, want unavailable v19", projection)
	}

	conn, err = sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var version int
	if err := conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 19 {
		t.Fatalf("projection migrated database to v%d", version)
	}
}
