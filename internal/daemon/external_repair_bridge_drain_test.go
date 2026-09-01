package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestExternalBridgeFrozenDrainLookupIsExactAndFailsClosed(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	if _, err := f.capture.db.SQL().ExecContext(ctx, `
UPDATE publication_drains SET phase='completed'
WHERE id='drain-external-repair-bridge'`); err != nil {
		t.Fatalf("temporarily complete original drain: %v", err)
	}
	checkpoint := state.Checkpoint{
		ID:               "cp-1786487000001-abcdef0123456789",
		OperationID:      "op-external-repair-bridge-competing",
		WorktreeID:       "0123456789abcdef",
		Reason:           state.CheckpointReasonManualBarrier,
		ObservationEpoch: 1,
		CoverageEpoch:    1,
		ObservedHead:     f.source,
		ObservedRef:      f.capture.cctx.BranchRef,
		TreeOID:          f.source,
		CommitOID:        f.source,
		CreatedTS:        10,
	}
	checkpoint.Ref = "refs/acd/checkpoints/v1/" + checkpoint.WorktreeID +
		"/" + checkpoint.ID
	if created, err := state.PrepareCheckpoint(
		ctx, f.capture.db, checkpoint, publicationDrainTestDigest,
	); err != nil || !created {
		t.Fatalf("prepare competing checkpoint=(%t,%v)", created, err)
	}
	if err := state.CompleteCheckpoint(
		ctx, f.capture.db, checkpoint.ID, checkpoint.Ref,
		checkpoint.CommitOID, 11,
	); err != nil {
		t.Fatalf("complete competing checkpoint: %v", err)
	}
	drain := state.PublicationDrain{
		ID:               "drain-external-repair-bridge-competing",
		CheckpointID:     checkpoint.ID,
		WorktreeID:       checkpoint.WorktreeID,
		BranchRef:        checkpoint.ObservedRef,
		BranchGeneration: f.capture.cctx.BranchGeneration,
		Phase:            state.PublicationDrainCheckpointing,
		CreatedTS:        12,
		UpdatedTS:        12,
		LastProgressTS:   12,
	}
	if created, err := state.PreparePublicationDrain(
		ctx, f.capture.db, drain,
	); err != nil || !created {
		t.Fatalf("prepare competing drain=(%t,%v)", created, err)
	}
	if _, err := f.capture.db.SQL().ExecContext(ctx, `
UPDATE publication_drains SET phase='event_fallback'
WHERE id='drain-external-repair-bridge'`); err != nil {
		t.Fatalf("restore original drain: %v", err)
	}

	chain, err := state.LoadUnpublishedRecoveryChain(
		ctx, f.capture.db, f.capture.cctx.BranchRef,
		f.capture.cctx.BranchGeneration, f.pendingSeqs[0])
	if err != nil {
		t.Fatalf("LoadUnpublishedRecoveryChain: %v", err)
	}
	opts := externalBridgeTestRecoveryOptions(f)
	count, matched, err := externalBridgeMatchesFrozenDrain(
		ctx, f.capture.db, opts, chain)
	if err != nil || matched || count != 0 {
		t.Fatalf("duplicate exact drains=(count=%d matched=%t err=%v), want deterministic no-match",
			count, matched, err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = externalBridgeMatchesFrozenDrain(
		canceled, f.capture.db, opts, chain)
	if !errors.Is(err, context.Canceled) ||
		errors.Is(err, state.ErrCompletedBranchTransitionProof) {
		t.Fatalf("canceled lookup err=%v, want transient context cancellation", err)
	}
}
