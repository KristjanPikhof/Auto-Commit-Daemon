package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPublicationDrainPersistsFrozenMembershipAndTransitions(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	checkpoint := seedPublicationDrainCheckpoint(t, db, []string{"a", "b"})
	drain := PublicationDrain{
		ID: "drain-1", CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 7, Phase: PublicationDrainCheckpointing,
		TargetEventCount: int64(len(checkpoint.EventSeqs)),
		CreatedTS:        10, UpdatedTS: 10, LastProgressTS: 10,
	}
	created, err := PreparePublicationDrain(ctx, db, drain)
	if err != nil || !created {
		t.Fatalf("prepare=(%t,%v)", created, err)
	}
	if created, err = PreparePublicationDrain(ctx, db, drain); err != nil || created {
		t.Fatalf("idempotent prepare=(%t,%v)", created, err)
	}

	semantic, err := AdvancePublicationDrain(ctx, db, drain.ID,
		PublicationDrainUpdate{
			ExpectedPhase: PublicationDrainCheckpointing,
			Phase:         PublicationDrainSemantic, PublishedEventCount: 0,
			UpdatedTS: 11, LastProgressTS: 10,
		})
	if err != nil || semantic.Phase != PublicationDrainSemantic {
		t.Fatalf("semantic=(%+v,%v)", semantic, err)
	}
	normalizing, err := AdvancePublicationDrain(ctx, db, drain.ID,
		PublicationDrainUpdate{
			ExpectedPhase: PublicationDrainSemantic,
			Phase:         PublicationDrainNormalizing, SemanticRebuildAttempts: 1,
			FallbackMode: "deterministic_semantic", LastError: "invalid\x00 graph",
			UpdatedTS: 12, LastProgressTS: 12,
		})
	if err != nil || normalizing.LastError != "invalid graph" {
		t.Fatalf("normalizing=(%+v,%v)", normalizing, err)
	}
	fallbackUpdate := PublicationDrainUpdate{
		ExpectedPhase: PublicationDrainNormalizing,
		Phase:         PublicationDrainEventFallback, PublishedEventCount: 1,
		SemanticRebuildAttempts: 1, EventFallbackCount: 1, CommitCount: 1,
		FallbackMode: "dependency_ordered_events", LastError: normalizing.LastError,
		StagedConsumed: true, UpdatedTS: 13, LastProgressTS: 13,
	}
	fallback, err := AdvancePublicationDrain(ctx, db, drain.ID, fallbackUpdate)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := AdvancePublicationDrain(ctx, db, drain.ID, fallbackUpdate); err != nil || !reflect.DeepEqual(replay, fallback) {
		t.Fatalf("idempotent fallback=(%+v,%v) want=%+v", replay, err, fallback)
	}
	completed, err := AdvancePublicationDrain(ctx, db, drain.ID,
		PublicationDrainUpdate{
			ExpectedPhase: PublicationDrainEventFallback,
			Phase:         PublicationDrainCompleted, PublishedEventCount: 2,
			SemanticRebuildAttempts: 1, EventFallbackCount: 2, CommitCount: 2,
			FallbackMode: "dependency_ordered_events", LastError: normalizing.LastError,
			StagedConsumed: true, UpdatedTS: 14, LastProgressTS: 14,
			CompletedTS: sql.NullFloat64{Float64: 14, Valid: true},
		})
	if err != nil || completed.Phase != PublicationDrainCompleted ||
		!reflect.DeepEqual(completed.EventSeqs, checkpoint.EventSeqs) {
		t.Fatalf("completed=(%+v,%v)", completed, err)
	}
	if active, err := ActivePublicationDrains(ctx, db); err != nil || len(active) != 0 {
		t.Fatalf("active=%+v err=%v", active, err)
	}
}

func TestPublicationDrainRejectsIdentityPhaseAndProgressRegression(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	checkpoint := seedPublicationDrainCheckpoint(t, db, []string{"a"})
	drain := PublicationDrain{
		ID: "drain-guarded", CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 7, Phase: PublicationDrainSemantic,
		TargetEventCount: 1, PublishedEventCount: 1,
		CreatedTS: 20, UpdatedTS: 20, LastProgressTS: 20,
	}
	if _, err := PreparePublicationDrain(ctx, db, drain); err != nil {
		t.Fatal(err)
	}
	changed := drain
	changed.BranchGeneration = 2
	if _, err := PreparePublicationDrain(ctx, db, changed); !errors.Is(err, ErrPublicationDrainIdentity) {
		t.Fatalf("identity error=%v", err)
	}
	if _, err := AdvancePublicationDrain(ctx, db, drain.ID,
		PublicationDrainUpdate{
			ExpectedPhase: PublicationDrainCheckpointing,
			Phase:         PublicationDrainEventFallback, PublishedEventCount: 1,
			UpdatedTS: 21, LastProgressTS: 21,
		}); !errors.Is(err, ErrPublicationDrainPhase) {
		t.Fatalf("CAS error=%v", err)
	}
	if _, err := AdvancePublicationDrain(ctx, db, drain.ID,
		PublicationDrainUpdate{
			ExpectedPhase: PublicationDrainSemantic,
			Phase:         PublicationDrainNormalizing, PublishedEventCount: 0,
			UpdatedTS: 21, LastProgressTS: 21,
		}); !errors.Is(err, ErrPublicationDrainProgress) {
		t.Fatalf("progress error=%v", err)
	}
	loaded, err := PublicationDrainByID(ctx, db, drain.ID)
	if err != nil || loaded.Phase != PublicationDrainSemantic ||
		loaded.PublishedEventCount != 1 {
		t.Fatalf("loaded after rejection=%+v err=%v", loaded, err)
	}
}

func TestPublicationDrainRequiresCompleteCheckpointMembership(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	checkpoint := seedPublicationDrainCheckpoint(t, db, []string{"a", "b"})
	drain := PublicationDrain{
		ID: "drain-incomplete", CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 7, Phase: PublicationDrainCheckpointing,
		TargetEventCount: 1, EventSeqs: checkpoint.EventSeqs[:1],
		CreatedTS: 20, UpdatedTS: 20, LastProgressTS: 20,
	}
	if _, err := PreparePublicationDrain(ctx, db, drain); !errors.Is(err, ErrPublicationDrainIdentity) {
		t.Fatalf("incomplete checkpoint membership error=%v", err)
	}
}

func TestReadOnlyDrainProjectionDoesNotMigrateV20(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, "PRAGMA user_version=20"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	beforeBody, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	before := sha256.Sum256(beforeBody)
	projection, err := ReadPublicationDrainProjection(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	afterBody, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	after := sha256.Sum256(afterBody)
	if projection.Available || projection.SchemaVersion != 20 || before != after {
		t.Fatalf("projection=%+v sha before=%x after=%x",
			projection, before, after)
	}
}

func TestMigrateSchema20ToCurrentAddsPublicationDrains(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx,
		"DROP INDEX idx_publication_drains_active"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx,
		"DROP TABLE publication_drains"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, "PRAGMA user_version=20"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if version, err := db.UserVersion(ctx); err != nil || version != SchemaVersion {
		t.Fatalf("version=(%d,%v)", version, err)
	}
	var table string
	if err := db.ReadSQL().QueryRowContext(ctx, `
SELECT name FROM sqlite_master WHERE type='table' AND name='publication_drains'`).
		Scan(&table); err != nil || table != "publication_drains" {
		t.Fatalf("publication_drains=(%q,%v)", table, err)
	}
}

func seedPublicationDrainCheckpoint(
	t *testing.T,
	db *DB,
	paths []string,
) Checkpoint {
	t.Helper()
	ctx := context.Background()
	checkpoint := Checkpoint{
		ID:          "cp-1786486000000-0123456789abcdef",
		OperationID: "op-publication-drain",
		WorktreeID:  "0123456789abcdef", Reason: CheckpointReasonManualBarrier,
		ObservationEpoch: 1, CoverageEpoch: 1, ObservedHead: "head",
		ObservedRef: "refs/heads/main", TreeOID: "tree", CommitOID: "commit",
		Ref:       "refs/acd/checkpoints/v1/0123456789abcdef/cp-1786486000000-0123456789abcdef",
		CreatedTS: 1,
	}
	for i, path := range paths {
		result, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state
) VALUES('refs/heads/main',7,'head','modify',?,'exact',?,'pending')`, path, i+1)
		if err != nil {
			t.Fatal(err)
		}
		seq, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		checkpoint.EventSeqs = append(checkpoint.EventSeqs, seq)
	}
	if created, err := PrepareCheckpoint(
		ctx, db, checkpoint, checkpointTestDigest); err != nil || !created {
		t.Fatalf("checkpoint prepare=(%t,%v)", created, err)
	}
	if err := CompleteCheckpoint(
		ctx, db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 2); err != nil {
		t.Fatal(err)
	}
	return checkpoint
}
