package daemon

import (
	"context"
	"database/sql"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestReconcileExternalRepairBridgePublishesFirstChildAndPreservesDescendant(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, false)
	ctx := context.Background()
	opts := RecoveryReconcileOptions{
		GitDir:             f.capture.gitDir,
		BranchRef:          f.capture.cctx.BranchRef,
		BranchGeneration:   f.capture.cctx.BranchGeneration,
		FirstSeq:           f.pendingSeqs[0],
		Trigger:            "test_external_repair_bridge",
		ExternalParentHead: f.source,
	}

	proved, err := ProveUnpublishedChain(
		ctx, f.capture.dir, f.capture.db, opts)
	if err != nil {
		t.Fatalf("ProveUnpublishedChain: %v", err)
	}
	if !proved.Handled || proved.Outcome != state.EventStatePublished ||
		proved.CommitOID != f.target || proved.EventCount != len(f.pendingSeqs) {
		t.Fatalf("proof=%+v want target %s", proved, f.target)
	}
	for _, seq := range f.pendingSeqs {
		if eventState, commit := readEventState(t, ctx, f.capture.db, seq); eventState != state.EventStatePending || commit.Valid {
			t.Fatalf("read-only proof changed seq=%d state=%q commit=%v",
				seq, eventState, commit)
		}
	}

	statusBefore := gitRawOutput(t, ctx, f.capture.dir, "status", "--short")
	result, err := ReconcileUnpublishedChain(
		ctx, f.capture.dir, f.capture.db, opts)
	if err != nil {
		t.Fatalf("ReconcileUnpublishedChain: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStatePublished ||
		result.CommitOID != f.target || result.EventCount != len(f.pendingSeqs) {
		t.Fatalf("result=%+v want target %s", result, f.target)
	}
	if head, err := git.RevParse(ctx, f.capture.dir, "HEAD"); err != nil || head != f.live {
		t.Fatalf("live HEAD=%s err=%v want untouched %s", head, err, f.live)
	}
	if statusAfter := gitRawOutput(t, ctx, f.capture.dir, "status", "--short"); statusAfter != statusBefore {
		t.Fatalf("reconciliation changed worktree/index:\nbefore=%q\nafter=%q",
			statusBefore, statusAfter)
	}
	for _, seq := range f.pendingSeqs {
		if eventState, commit := readEventState(t, ctx, f.capture.db, seq); eventState != state.EventStatePublished ||
			!commit.Valid || commit.String != f.target {
			t.Fatalf("seq=%d state=%q commit=%v want published at %s",
				seq, eventState, commit, f.target)
		}
	}
	reconciled, err := state.ReconcileResolvedPublicationDrains(
		ctx, f.capture.db, 3)
	if err != nil || len(reconciled) != 1 ||
		reconciled[0].ResolvedEvents != int64(len(f.pendingSeqs)) {
		t.Fatalf("resolved drains=%+v err=%v", reconciled, err)
	}
	protected, err := git.RevParse(ctx, f.capture.dir, result.RecoveryRef)
	if err != nil || protected != f.target {
		t.Fatalf("proof ref=%s err=%v want target %s", protected, err, f.target)
	}
	if ancestor, err := git.IsAncestor(ctx, f.capture.dir, f.target, f.live); err != nil || !ancestor {
		t.Fatalf("target ancestor of later live commit=%t err=%v", ancestor, err)
	}
}

func TestExternalRepairBridgeRejectsUnownedTargetPath(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, true)
	ctx := context.Background()
	live, err := currentRecoveryLiveState(ctx, f.capture.dir, nil, nil)
	if err != nil {
		t.Fatalf("currentRecoveryLiveState: %v", err)
	}
	chain, err := state.LoadUnpublishedRecoveryChain(
		ctx, f.capture.db, f.capture.cctx.BranchRef,
		f.capture.cctx.BranchGeneration, f.pendingSeqs[0])
	if err != nil {
		t.Fatalf("LoadUnpublishedRecoveryChain: %v", err)
	}
	proof, matched, err := proveExternalRepairBridge(
		ctx, f.capture.dir, f.capture.db, RecoveryReconcileOptions{
			BranchRef:          f.capture.cctx.BranchRef,
			BranchGeneration:   f.capture.cctx.BranchGeneration,
			FirstSeq:           f.pendingSeqs[0],
			ExternalParentHead: f.source,
		}, live, chain)
	if err != nil {
		t.Fatalf("proveExternalRepairBridge: %v", err)
	}
	if matched {
		t.Fatalf("unowned target path matched proof %+v", proof)
	}
	for _, seq := range f.pendingSeqs {
		if eventState, _ := readEventState(t, ctx, f.capture.db, seq); eventState != state.EventStatePending {
			t.Fatalf("rejected proof changed seq=%d state=%q", seq, eventState)
		}
	}
}

type externalRepairBridgeFixture struct {
	capture     *captureFixture
	source      string
	target      string
	live        string
	pendingSeqs []int64
}

func newExternalRepairBridgeFixture(
	t *testing.T,
	includeUnownedPath bool,
) externalRepairBridgeFixture {
	t.Helper()
	f := newCaptureFixture(t)
	ctx := context.Background()
	blob := func(content string) string {
		oid, err := git.HashObjectStdin(ctx, f.dir, []byte(content))
		if err != nil {
			t.Fatalf("HashObjectStdin: %v", err)
		}
		return oid
	}
	pendingOld := blob("pending old\n")
	pendingBridge := blob("pending repaired\n")
	pendingMiddle := blob("pending middle\n")
	pendingFinal := blob("pending final\n")
	restoredOld := blob("restored old\n")
	restoredFinal := blob("restored final\n")
	notesFinal := blob("notes final\n")
	laterBlob := blob("later external work\n")
	unownedBlob := blob("unowned target work\n")

	preRepair := commitTreeWithIndexUpdates(t, ctx, f, f.cctx.BaseHead, "pre-repair",
		git.RegularFileMode+" "+pendingOld+"\tpending.txt",
		git.RegularFileMode+" "+restoredOld+"\trestored.txt")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, preRepair, f.cctx.BaseHead); err != nil {
		t.Fatalf("install pre-repair head: %v", err)
	}
	f.cctx.BaseHead = preRepair

	repairPending := appendRecoveryEvent(t, ctx, f, preRepair, state.CaptureOp{
		Op: "modify", Path: "pending.txt",
		BeforeOID: oidValue(pendingOld), BeforeMode: oidValue(git.RegularFileMode),
		AfterOID: oidValue(pendingBridge), AfterMode: oidValue(git.RegularFileMode),
	})
	repairRestored := appendRecoveryEvent(t, ctx, f, preRepair, state.CaptureOp{
		Op: "modify", Path: "restored.txt",
		BeforeOID: oidValue(restoredOld), BeforeMode: oidValue(git.RegularFileMode),
		AfterOID: oidValue(restoredFinal), AfterMode: oidValue(git.RegularFileMode),
	})
	repairCommit := commitTreeWithIndexUpdates(t, ctx, f, preRepair, "completed repair",
		git.RegularFileMode+" "+pendingBridge+"\tpending.txt",
		git.RegularFileMode+" "+restoredFinal+"\trestored.txt")
	for _, seq := range []int64{repairPending, repairRestored} {
		if err := state.MarkEventPublished(
			ctx, f.db, seq, state.EventStatePublished, oidValue(repairCommit),
			sql.NullString{}, sql.NullString{}, 1); err != nil {
			t.Fatalf("mark repair member %d published: %v", seq, err)
		}
	}
	seedCompletedExternalBridgeRepair(
		t, ctx, f, preRepair, repairCommit,
		[]int64{repairPending, repairRestored})

	// Reproduce the stale-index publication: the commit descends from the
	// repaired head but writes the pre-repair entries back into its tree.
	source := commitTreeWithIndexUpdates(t, ctx, f, repairCommit, "stale source",
		git.RegularFileMode+" "+pendingOld+"\tpending.txt",
		git.RegularFileMode+" "+restoredOld+"\trestored.txt")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, source, preRepair); err != nil {
		t.Fatalf("install stale source: %v", err)
	}
	f.cctx.BaseHead = source

	pendingOne := appendRecoveryEvent(t, ctx, f, source, state.CaptureOp{
		Op: "modify", Path: "pending.txt",
		BeforeOID: oidValue(pendingBridge), BeforeMode: oidValue(git.RegularFileMode),
		AfterOID: oidValue(pendingMiddle), AfterMode: oidValue(git.RegularFileMode),
	})
	pendingTwo := appendRecoveryEvent(t, ctx, f, source, state.CaptureOp{
		Op: "modify", Path: "pending.txt",
		BeforeOID: oidValue(pendingMiddle), BeforeMode: oidValue(git.RegularFileMode),
		AfterOID: oidValue(pendingFinal), AfterMode: oidValue(git.RegularFileMode),
	})
	pendingNotes := appendRecoveryEvent(t, ctx, f, source, state.CaptureOp{
		Op: "create", Path: "notes.txt",
		AfterOID: oidValue(notesFinal), AfterMode: oidValue(git.RegularFileMode),
	})
	pendingSeqs := []int64{pendingOne, pendingTwo, pendingNotes}
	seedExternalRepairBridgeDrain(t, ctx, f, source, pendingSeqs)

	targetUpdates := []string{
		git.RegularFileMode + " " + pendingFinal + "\tpending.txt",
		git.RegularFileMode + " " + restoredFinal + "\trestored.txt",
		git.RegularFileMode + " " + notesFinal + "\tnotes.txt",
	}
	if includeUnownedPath {
		targetUpdates = append(targetUpdates,
			git.RegularFileMode+" "+unownedBlob+"\tunowned.txt")
	}
	target := commitTreeWithIndexUpdates(
		t, ctx, f, source, "external target", targetUpdates...)
	live := commitTreeWithIndexUpdates(t, ctx, f, target, "later external work",
		git.RegularFileMode+" "+laterBlob+"\tlater.txt")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, live, source); err != nil {
		t.Fatalf("install live descendant: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "reset", "--hard", live); err != nil {
		t.Fatalf("reset isolated fixture to live descendant: %v", err)
	}
	return externalRepairBridgeFixture{
		capture: f, source: source, target: target, live: live,
		pendingSeqs: pendingSeqs,
	}
}

func seedExternalRepairBridgeDrain(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	head string,
	eventSeqs []int64,
) {
	t.Helper()
	tree, err := git.RevParse(ctx, f.dir, head+"^{tree}")
	if err != nil {
		t.Fatalf("resolve checkpoint tree: %v", err)
	}
	checkpoint := state.Checkpoint{
		ID:               "cp-1786487000000-fedcba9876543210",
		OperationID:      "op-external-repair-bridge",
		WorktreeID:       "0123456789abcdef",
		Reason:           state.CheckpointReasonManualBarrier,
		ObservationEpoch: 1,
		CoverageEpoch:    1,
		ObservedHead:     head,
		ObservedRef:      f.cctx.BranchRef,
		TreeOID:          tree,
		CommitOID:        head,
		Ref:              "refs/acd/checkpoints/v1/0123456789abcdef/cp-1786487000000-fedcba9876543210",
		CreatedTS:        1,
		EventSeqs:        append([]int64(nil), eventSeqs...),
	}
	if created, err := state.PrepareCheckpoint(
		ctx, f.db, checkpoint, publicationDrainTestDigest); err != nil || !created {
		t.Fatalf("prepare bridge checkpoint=(%t,%v)", created, err)
	}
	if err := state.CompleteCheckpoint(
		ctx, f.db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 2); err != nil {
		t.Fatalf("complete bridge checkpoint: %v", err)
	}
	drain := state.PublicationDrain{
		ID:                  "drain-external-repair-bridge",
		CheckpointID:        checkpoint.ID,
		WorktreeID:          checkpoint.WorktreeID,
		BranchRef:           checkpoint.ObservedRef,
		BranchGeneration:    f.cctx.BranchGeneration,
		Phase:               state.PublicationDrainEventFallback,
		TargetEventCount:    int64(len(eventSeqs)),
		PublishedEventCount: 0,
		CreatedTS:           2,
		UpdatedTS:           2,
		LastProgressTS:      2,
	}
	if created, err := state.PreparePublicationDrain(
		ctx, f.db, drain); err != nil || !created {
		t.Fatalf("prepare bridge drain=(%t,%v)", created, err)
	}
}

func seedCompletedExternalBridgeRepair(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	oldHead string,
	newHead string,
	eventSeqs []int64,
) {
	t.Helper()
	const repairID = "external-repair-bridge"
	const candidateID = "external-repair-candidate"
	if _, err := f.db.SQL().ExecContext(ctx, `
INSERT INTO intent_repairs(
    id,branch_ref,branch_generation,status,expected_head,plan_digest,
    old_head,new_head,created_ts,updated_ts,error
) VALUES (?, ?, ?, 'prepared', ?, 'bridge-digest', ?, ?, 1, 1, '')`,
		repairID, f.cctx.BranchRef, f.cctx.BranchGeneration,
		oldHead, oldHead, newHead); err != nil {
		t.Fatalf("insert repair: %v", err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, `
INSERT INTO intent_repair_commits(
    repair_id,ord,candidate_id,old_oid,new_oid
) VALUES (?, 0, ?, ?, ?)`, repairID, candidateID, oldHead, newHead); err != nil {
		t.Fatalf("insert repair commit: %v", err)
	}
	for ord, seq := range eventSeqs {
		if _, err := f.db.SQL().ExecContext(ctx, `
INSERT INTO intent_repair_members(
    repair_id,ord,candidate_id,event_seq,prior_state
) VALUES (?, ?, ?, ?, 'pending')`, repairID, ord, candidateID, seq); err != nil {
			t.Fatalf("insert repair member %d: %v", seq, err)
		}
	}
	if _, err := f.db.SQL().ExecContext(ctx, `
INSERT INTO intent_repair_member_seals(
    repair_id,membership_mode,member_count
) VALUES (?, 'frozen', ?)`, repairID, len(eventSeqs)); err != nil {
		t.Fatalf("seal repair membership: %v", err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, `
UPDATE intent_repairs
SET status='completed',git_applied_ts=2,completed_ts=2,updated_ts=2
WHERE id=? AND status='prepared'`, repairID); err != nil {
		t.Fatalf("complete repair: %v", err)
	}
}

func oidValue(value string) sql.NullString {
	return sql.NullString{String: value, Valid: true}
}
