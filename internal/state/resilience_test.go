package state

import (
	"context"
	"errors"
	"testing"
)

func TestResiliencePartialCheckpointRejectsRestoreAndPreservesCoverage(t *testing.T) {
	ctx := context.Background()
	db, _ := openLegacyTestDB(t)
	cp := Checkpoint{ID: "cp-123-0123456789abcdef", OperationID: "partial-op", WorktreeID: "0123456789abcdef", Reason: CheckpointReasonPoll, ObservationEpoch: 9, CoverageEpoch: 9, ObservedRef: "refs/heads/main", TreeOID: "tree", CommitOID: "commit", Ref: "refs/acd/checkpoints/v1/0123456789abcdef/cp-123-0123456789abcdef", Partial: true, CaptureIssues: []CheckpointCaptureIssue{{Path: "missing.go", Reason: "unreadable"}}}
	if _, err := PrepareCheckpoint(ctx, db, cp, checkpointTestDigest); err != nil {
		t.Fatal(err)
	}
	if err := CompleteCheckpoint(ctx, db, cp.ID, cp.Ref, cp.CommitOID, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveCheckpoint(ctx, db.Path(), cp.ID); !errors.Is(err, ErrCheckpointPartial) {
		t.Fatalf("restore err=%v", err)
	}
	if _, err := PrepareCheckpoint(ctx, db, cp, checkpointTestDigest); err != nil {
		t.Fatal(err)
	}
	cp.CaptureIssues[0].Path = "different.go"
	if _, err := PrepareCheckpoint(ctx, db, cp, checkpointTestDigest); !errors.Is(err, ErrCheckpointIdentityMismatch) {
		t.Fatalf("changed immutable issue err=%v", err)
	}
}

func TestResilienceV28ReaderDoesNotMigrateCoverage(t *testing.T) {
	ctx := context.Background()
	db, path := openLegacyTestDB(t)
	cp := seedLegacyCheckpoint(t, db, []string{"legacy"})
	for _, query := range []string{`DROP TABLE checkpoint_capture_issues`, `ALTER TABLE checkpoints DROP COLUMN coverage_complete`, `ALTER TABLE intent_plan_runs DROP COLUMN provider_deadline_ts`, `PRAGMA user_version=28`} {
		if _, err := db.SQL().ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	projection, err := ReadCheckpointProjection(ctx, path, 1)
	if err != nil || projection.SchemaVersion != 28 || projection.Latest == nil || projection.Latest.Partial {
		t.Fatalf("legacy reader=%+v err=%v", projection, err)
	}
	if _, err := ResolveCheckpoint(ctx, path, cp.ID); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	projection, err = ReadCheckpointProjection(ctx, path, 1)
	if err != nil || projection.SchemaVersion != SchemaVersion || projection.Latest.Partial {
		t.Fatalf("migrated reader=%+v err=%v", projection, err)
	}
}

func TestResilienceRetainsLastCompleteBehindPartial(t *testing.T) {
	ctx := context.Background()
	db, _ := openLegacyTestDB(t)
	complete := seedLegacyCheckpoint(t, db, []string{"complete"})
	partial := complete
	partial.ID = "cp-124-0123456789abcdef"
	partial.OperationID = "partial-retention"
	partial.Ref = "refs/acd/checkpoints/v1/" + partial.WorktreeID + "/" + partial.ID
	partial.Partial = true
	partial.CaptureIssues = []CheckpointCaptureIssue{{Path: "unreadable.go", Reason: "unreadable"}}
	partial.EventSeqs = nil
	if _, err := PrepareCheckpoint(ctx, db, partial, checkpointTestDigest); err != nil {
		t.Fatal(err)
	}
	if err := CompleteCheckpoint(ctx, db, partial.ID, partial.Ref, partial.CommitOID, 2); err != nil {
		t.Fatal(err)
	}
	checkpoints, err := RetentionCheckpoints(ctx, db, complete.WorktreeID)
	if err != nil || len(checkpoints) != 2 || checkpoints[0].LatestComplete || !checkpoints[1].LatestComplete {
		t.Fatalf("complete retention=%+v err=%v", checkpoints, err)
	}

}
