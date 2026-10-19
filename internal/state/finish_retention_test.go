package state

import (
	"context"
	"strings"
	"testing"
)

func TestFinishRetentionFailedAttemptsAndLatestComplete(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	first := finishFixture(t, db, 1)
	first.Target.TargetRef = "refs/acd/finish/v1/first/target"
	first.Target.TargetCommitOID = strings.Repeat("a", 40)
	r := startFinishFixture(t, db, first)
	r = failFinishFixture(t, db, r)
	retry, err := RetryFinishRequest(ctx, db, r.ID, nowSeconds()+60, false)
	if err != nil {
		t.Fatal(err)
	}
	failFinishFixture(t, db, retry)
	newer := finishFixture(t, db, 2)
	items, err := RetentionCheckpoints(ctx, db, first.Target.WorktreeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || !items[0].LatestComplete || items[0].ID != newer.Target.CheckpointID || !items[1].Unresolved || !items[1].Superseded {
		t.Fatalf("retention %+v", items)
	}
	pins, err := RetentionObjectPins(ctx, db, first.Target.WorktreeID)
	if err != nil || len(pins) != 1 || pins[0].CommitOID != first.Target.TargetCommitOID {
		t.Fatalf("attempt pins %+v %v", pins, err)
	}
	if _, err = PrepareCheckpointPrune(ctx, db, items[1], checkpointTestDigest); err == nil {
		t.Fatal("failed immutable attempt allowed source pruning")
	}
}

func TestFinishRetentionIncompleteNewerDoesNotSupersede(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	first := finishFixture(t, db, 1)
	newer := finishFixture(t, db, 2)
	if _, err := db.SQL().ExecContext(ctx, "UPDATE checkpoints SET coverage_complete=0 WHERE id=?", newer.Target.CheckpointID); err != nil {
		t.Fatal(err)
	}
	items, err := RetentionCheckpoints(ctx, db, first.Target.WorktreeID)
	if err != nil {
		t.Fatal(err)
	}
	if !items[1].LatestComplete || items[1].Superseded {
		t.Fatalf("partial checkpoint replaced complete protection: %+v", items)
	}
}
