package daemon

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestReconcileExternalRepairBridgePublishesFirstChildAndPreservesDescendant(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
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
		reconciled[0].ResolvedEvents != int64(f.drainEventCount) {
		t.Fatalf("resolved drains=%+v err=%v", reconciled, err)
	}
	protected, err := git.RevParse(ctx, f.capture.dir, result.RecoveryRef)
	if err != nil || protected != f.target {
		t.Fatalf("proof ref=%s err=%v want target %s", protected, err, f.target)
	}
	if ancestor, err := git.IsAncestor(ctx, f.capture.dir, f.target, f.live); err != nil || !ancestor {
		t.Fatalf("target ancestor of later live commit=%t err=%v", ancestor, err)
	}
	targetPending, err := git.LsTreeBlobOID(ctx, f.capture.dir, f.target, "pending.txt")
	if err != nil {
		t.Fatalf("read target pending blob: %v", err)
	}
	livePending, err := git.LsTreeBlobOID(ctx, f.capture.dir, f.live, "pending.txt")
	if err != nil || livePending == targetPending {
		t.Fatalf("later commit pending blob=%s err=%v want different from target %s",
			livePending, err, targetPending)
	}
	for seq, wantCommit := range f.repairCommits {
		if eventState, commit := readEventState(t, ctx, f.capture.db, seq); eventState != state.EventStatePublished || !commit.Valid ||
			commit.String != wantCommit {
			t.Fatalf("repair seq=%d changed state=%q commit=%v",
				seq, eventState, commit)
		}
	}
}

func TestExternalRepairBridgeRejectsUnownedTargetPath(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{
		includeUnownedPath: true,
	})
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

func TestExternalRepairBridgeRejectsIncompleteRepairRestoration(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{
		omitRestoredPath: true,
	})
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
		t.Fatalf("incomplete repair restoration matched proof %+v", proof)
	}
}

func TestExternalRepairBridgeUsesCumulativeAdjacentRepairState(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{
		includePriorRepair: true,
	})
	result, err := ProveUnpublishedChain(
		context.Background(), f.capture.dir, f.capture.db,
		RecoveryReconcileOptions{
			GitDir:             f.capture.gitDir,
			BranchRef:          f.capture.cctx.BranchRef,
			BranchGeneration:   f.capture.cctx.BranchGeneration,
			FirstSeq:           f.pendingSeqs[0],
			ExternalParentHead: f.source,
		})
	if err != nil || !result.Handled ||
		result.Outcome != state.EventStatePublished ||
		result.CommitOID != f.target {
		t.Fatalf("cumulative repair proof=%+v err=%v want target %s",
			result, err, f.target)
	}
}

func TestExternalRepairBridgeRejectsEquivalentLaterProofRef(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	laterBlob, err := git.HashObjectStdin(ctx, f.capture.dir, []byte("other later work\n"))
	if err != nil {
		t.Fatalf("HashObjectStdin: %v", err)
	}
	equivalentLater := commitTreeWithIndexUpdates(
		t, ctx, f.capture, f.target, "equivalent later proof",
		git.RegularFileMode+" "+laterBlob+"\tother-later.txt")
	ref := recoveryProofRefName(
		f.capture.cctx.BranchRef, f.capture.cctx.BranchGeneration,
		f.pendingSeqs[0], f.pendingSeqs[len(f.pendingSeqs)-1], f.target)
	if _, err := git.EnsureRecoveryRef(
		ctx, f.capture.dir, ref, equivalentLater); err != nil {
		t.Fatalf("seed equivalent later proof ref: %v", err)
	}

	result, err := ReconcileUnpublishedChain(
		ctx, f.capture.dir, f.capture.db, RecoveryReconcileOptions{
			GitDir:             f.capture.gitDir,
			BranchRef:          f.capture.cctx.BranchRef,
			BranchGeneration:   f.capture.cctx.BranchGeneration,
			FirstSeq:           f.pendingSeqs[0],
			ExternalParentHead: f.source,
		})
	if !errors.Is(err, git.ErrRecoveryRefCollision) {
		t.Fatalf("ReconcileUnpublishedChain result=%+v err=%v want collision",
			result, err)
	}
	for _, seq := range f.pendingSeqs {
		if eventState, commit := readEventState(t, ctx, f.capture.db, seq); eventState != state.EventStatePending || commit.Valid {
			t.Fatalf("rejected proof changed seq=%d state=%q commit=%v",
				seq, eventState, commit)
		}
	}
}

func TestExternalBridgeFirstParentProofRejectsBackwardReset(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	target, matched, err := firstParentChildSince(
		context.Background(), f.capture.dir, f.live, f.target)
	if err != nil || matched || target != "" {
		t.Fatalf("target=%q matched=%t err=%v want ordinary no-match",
			target, matched, err)
	}
}

type externalRepairBridgeFixture struct {
	capture         *captureFixture
	source          string
	target          string
	live            string
	pendingSeqs     []int64
	drainEventCount int
	repairCommits   map[int64]string
}

type externalRepairBridgeFixtureOptions struct {
	includeUnownedPath bool
	omitRestoredPath   bool
	includePriorRepair bool
}

func newExternalRepairBridgeFixture(
	t *testing.T,
	opts externalRepairBridgeFixtureOptions,
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
	publishedFinal := blob("published suffix\n")
	laterBlob := blob("later external work\n")
	laterPending := blob("pending changed by later commit\n")
	unownedBlob := blob("unowned target work\n")
	priorOld := blob("prior repair old\n")
	priorFinal := blob("prior repair final\n")

	preRepair := commitTreeWithIndexUpdates(t, ctx, f, f.cctx.BaseHead, "pre-repair",
		git.RegularFileMode+" "+pendingOld+"\tpending.txt",
		git.RegularFileMode+" "+restoredOld+"\trestored.txt",
		git.RegularFileMode+" "+priorOld+"\tprior.txt")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, preRepair, f.cctx.BaseHead); err != nil {
		t.Fatalf("install pre-repair head: %v", err)
	}
	f.cctx.BaseHead = preRepair
	repairEvents := make([]int64, 0, 3)
	repairCommits := make(map[int64]string)
	if opts.includePriorRepair {
		priorEvent := appendRecoveryEvent(t, ctx, f, preRepair, state.CaptureOp{
			Op: "modify", Path: "prior.txt",
			BeforeOID: oidValue(priorOld), BeforeMode: oidValue(git.RegularFileMode),
			AfterOID: oidValue(priorFinal), AfterMode: oidValue(git.RegularFileMode),
		})
		priorCommit := commitTreeWithIndexUpdates(
			t, ctx, f, preRepair, "prior completed repair",
			git.RegularFileMode+" "+priorFinal+"\tprior.txt")
		if err := state.MarkEventPublished(
			ctx, f.db, priorEvent, state.EventStatePublished,
			oidValue(priorCommit), sql.NullString{}, sql.NullString{}, 1); err != nil {
			t.Fatalf("mark prior repair member published: %v", err)
		}
		seedCompletedExternalBridgeRepair(
			t, ctx, f, "external-prior-repair", "external-prior-candidate",
			preRepair, priorCommit, []int64{priorEvent})
		if err := git.UpdateRef(
			ctx, f.dir, f.cctx.BranchRef, priorCommit, preRepair); err != nil {
			t.Fatalf("install prior repair head: %v", err)
		}
		preRepair = priorCommit
		f.cctx.BaseHead = priorCommit
		repairEvents = append(repairEvents, priorEvent)
		repairCommits[priorEvent] = priorCommit
	}

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
		t, ctx, f, "external-repair-bridge", "external-repair-candidate",
		preRepair, repairCommit,
		[]int64{repairPending, repairRestored})
	if err := git.UpdateRef(
		ctx, f.dir, f.cctx.BranchRef, repairCommit, preRepair); err != nil {
		t.Fatalf("install repair head: %v", err)
	}
	repairEvents = append(repairEvents, repairPending, repairRestored)
	repairCommits[repairPending] = repairCommit
	repairCommits[repairRestored] = repairCommit
	publishedSuffix := appendRecoveryEvent(t, ctx, f, repairCommit, state.CaptureOp{
		Op: "create", Path: "published.txt",
		AfterOID: oidValue(publishedFinal), AfterMode: oidValue(git.RegularFileMode),
	})

	// Reproduce the stale-index publication: the commit descends from the
	// repaired head but writes the pre-repair entries back into its tree.
	sourceUpdates := []string{
		git.RegularFileMode + " " + pendingOld + "\tpending.txt",
		git.RegularFileMode + " " + restoredOld + "\trestored.txt",
		git.RegularFileMode + " " + publishedFinal + "\tpublished.txt",
	}
	if opts.includePriorRepair {
		sourceUpdates = append(sourceUpdates,
			git.RegularFileMode+" "+priorOld+"\tprior.txt")
	}
	source := commitTreeWithIndexUpdates(
		t, ctx, f, repairCommit, "stale source", sourceUpdates...)
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, source, repairCommit); err != nil {
		t.Fatalf("install stale source: %v", err)
	}
	seedCompletedExternalBridgePublication(
		t, ctx, f, repairCommit, source, publishedSuffix)
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
	drainSeqs := append(append(
		append([]int64(nil), repairEvents...), publishedSuffix), pendingSeqs...)
	seedExternalRepairBridgeDrain(
		t, ctx, f, source, drainSeqs, int64(len(repairEvents)+1))

	targetUpdates := []string{
		git.RegularFileMode + " " + pendingFinal + "\tpending.txt",
		git.RegularFileMode + " " + notesFinal + "\tnotes.txt",
	}
	if !opts.omitRestoredPath {
		targetUpdates = append(targetUpdates,
			git.RegularFileMode+" "+restoredFinal+"\trestored.txt")
	}
	if opts.includePriorRepair {
		targetUpdates = append(targetUpdates,
			git.RegularFileMode+" "+priorFinal+"\tprior.txt")
	}
	if opts.includeUnownedPath {
		targetUpdates = append(targetUpdates,
			git.RegularFileMode+" "+unownedBlob+"\tunowned.txt")
	}
	target := commitTreeWithIndexUpdates(
		t, ctx, f, source, "external target", targetUpdates...)
	live := commitTreeWithIndexUpdates(t, ctx, f, target, "later external work",
		git.RegularFileMode+" "+laterPending+"\tpending.txt",
		git.RegularFileMode+" "+laterBlob+"\tlater.txt")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, live, source); err != nil {
		t.Fatalf("install live descendant: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "reset", "--hard", live); err != nil {
		t.Fatalf("reset isolated fixture to live descendant: %v", err)
	}
	return externalRepairBridgeFixture{
		capture: f, source: source, target: target, live: live,
		pendingSeqs: pendingSeqs, drainEventCount: len(drainSeqs),
		repairCommits: repairCommits,
	}
}

func seedCompletedExternalBridgePublication(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	sourceHead string,
	targetHead string,
	eventSeq int64,
) {
	t.Helper()
	tree, err := git.RevParse(ctx, f.dir, targetHead+"^{tree}")
	if err != nil {
		t.Fatalf("resolve publication tree: %v", err)
	}
	members := []state.SelfPublicationMember{{EventSeq: eventSeq}}
	digest, err := state.SelfPublicationMembershipDigest(members)
	if err != nil {
		t.Fatalf("publication membership digest: %v", err)
	}
	publication := state.SelfPublication{
		ID: "external-repair-publication",
		OperationID: sql.NullString{
			String: "op-external-repair-publication", Valid: true,
		},
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		SourceHead:       sourceHead,
		TargetCommitOID:  targetHead,
		TargetTreeOID:    tree,
		MembershipDigest: digest,
		MemberCount:      len(members),
		CreatedTS:        2,
		Members:          members,
		Completion: state.SelfPublicationCompletion{
			PublishedTS: 3, CandidateStatus: state.IntentCandidatePublished,
		},
	}
	if created, err := state.PrepareSelfPublication(
		ctx, f.db, publication); err != nil || !created {
		t.Fatalf("prepare publication=(%t,%v)", created, err)
	}
	if changed, err := state.MarkSelfPublicationGitApplied(
		ctx, f.db, publication, 3); err != nil || !changed {
		t.Fatalf("mark publication applied=(%t,%v)", changed, err)
	}
	if completed, err := state.CompleteSelfPublication(
		ctx, f.db, publication, state.SelfPublicationCompletion{
			PublishedTS: 3, CandidateStatus: state.IntentCandidatePublished,
		}); err != nil || !completed {
		t.Fatalf("complete publication=(%t,%v)", completed, err)
	}
}

func seedExternalRepairBridgeDrain(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	head string,
	eventSeqs []int64,
	publishedCount int64,
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
		PublishedEventCount: publishedCount,
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
	repairID string,
	candidateID string,
	oldHead string,
	newHead string,
	eventSeqs []int64,
) {
	t.Helper()
	if _, err := f.db.SQL().ExecContext(ctx, `
INSERT INTO intent_repairs(
    id,branch_ref,branch_generation,status,expected_head,plan_digest,
    backup_ref,old_head,new_head,created_ts,updated_ts,error
) VALUES (?, ?, ?, 'prepared', ?, 'bridge-digest', ?, ?, ?, 1, 1, '')`,
		repairID, f.cctx.BranchRef, f.cctx.BranchGeneration,
		oldHead, "refs/acd/intent-repair/test/"+repairID+"/backup",
		oldHead, newHead); err != nil {
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
