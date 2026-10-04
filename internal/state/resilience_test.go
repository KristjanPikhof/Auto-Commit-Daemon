package state

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestResiliencePrunePublishedPreservesCompletedDrain(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	cp := seedPublicationDrainCheckpoint(t, db, []string{"retained"})
	drain := PublicationDrain{ID: "retained-drain", CheckpointID: cp.ID, WorktreeID: cp.WorktreeID, BranchRef: cp.ObservedRef, BranchGeneration: 7, Phase: PublicationDrainCheckpointing, TargetEventCount: 1, CreatedTS: 1, UpdatedTS: 1, LastProgressTS: 1}
	if _, err := PreparePublicationDrain(ctx, db, drain); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{`UPDATE capture_events SET state='published',published_ts=2`, `UPDATE publication_drains SET phase='completed',published_event_count=1,completed_ts=2`, `UPDATE checkpoints SET retained=0,pruned_ts=3`} {
		if _, err := db.SQL().ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	seq, err := AppendCaptureEvent(ctx, db, CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 7, BaseHead: "head", Operation: "create", Path: "unreferenced", Fidelity: "exact", CapturedTS: 1, State: EventStatePublished}, []CaptureOp{{Op: "create", Path: "unreferenced", Fidelity: "exact"}})
	if err != nil {
		t.Fatal(err)
	}
	pruned, err := PrunePublishedEventsBefore(ctx, db, 100)
	if err != nil || pruned != 1 {
		t.Fatalf("pruned=%d err=%v", pruned, err)
	}
	var count int
	if err := db.ReadSQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events WHERE seq=?`, cp.EventSeqs[0]).Scan(&count); err != nil || count != 1 {
		t.Fatalf("drain capture count=%d err=%v", count, err)
	}
	if err := db.ReadSQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events WHERE seq=?`, seq).Scan(&count); err != nil || count != 0 {
		t.Fatalf("unreferenced capture count=%d err=%v", count, err)
	}
	if err := db.ReadSQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_foreign_key_check`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("foreign keys=%d err=%v", count, err)
	}
	if pruned, err := PrunePublishedEventsBefore(ctx, db, 100); err != nil || pruned != 0 {
		t.Fatalf("repeat prune=%d err=%v", pruned, err)
	}
}

func TestResiliencePartialCheckpointRejectsFullBarrierAndRestore(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	cp := Checkpoint{ID: "cp-123-0123456789abcdef", OperationID: "partial-op", WorktreeID: "0123456789abcdef", Reason: CheckpointReasonPoll, ObservationEpoch: 9, CoverageEpoch: 9, ObservedRef: "refs/heads/main", TreeOID: "tree", CommitOID: "commit", Ref: "refs/acd/checkpoints/v1/partial", Partial: true, CaptureIssues: []CheckpointCaptureIssue{{Path: "missing.go", Reason: "unreadable"}}}
	if _, err := PrepareCheckpoint(ctx, db, cp, checkpointTestDigest); err != nil {
		t.Fatal(err)
	}
	if err := CompleteCheckpoint(ctx, db, cp.ID, cp.Ref, cp.CommitOID, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ResolveCheckpoint(ctx, db.Path(), cp.ID); !errors.Is(err, ErrCheckpointPartial) {
		t.Fatalf("restore err=%v", err)
	}
	if _, ok, err := CompletedCheckpointForBarrier(ctx, db, cp.WorktreeID, 1, 0, cp.ObservedRef); err != nil || ok {
		t.Fatalf("barrier accepted partial: ok=%t err=%v", ok, err)
	}
	if _, err := PrepareCheckpoint(ctx, db, cp, checkpointTestDigest); err != nil {
		t.Fatal(err)
	}
	cp.CaptureIssues[0].Path = "different.go"
	if _, err := PrepareCheckpoint(ctx, db, cp, checkpointTestDigest); !errors.Is(err, ErrCheckpointIdentityMismatch) {
		t.Fatalf("changed immutable issue err=%v", err)
	}
}

func TestResilienceProviderDeadlineSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	db, path := openTestDB(t)
	run, err := EnsureIntentPlanRun(ctx, db, IntentPlanRun{Fingerprint: "budget", BranchRef: "refs/heads/main", AttemptLimit: 3})
	if err != nil {
		t.Fatal(err)
	}
	deadline := float64(time.Now().Add(time.Minute).Unix())
	run, err = StartIntentProviderBudget(ctx, db, run, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	run, err = StartIntentProviderBudget(ctx, db, run, deadline+600)
	if err != nil || run.ProviderDeadlineTS != deadline {
		t.Fatalf("budget reset across restart: %+v err=%v", run, err)
	}
}
