package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestExternalRepairBridgePublishesFrozenPrefixAndPreservesLaterCapture(
	t *testing.T,
) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	laterBlob := externalBridgeTestBlob(t, ctx, f.capture, "later capture\n")
	laterSeq := appendRecoveryEvent(t, ctx, f.capture, f.source, state.CaptureOp{
		Op: "create", Path: "later-capture.txt",
		AfterOID: oidValue(laterBlob), AfterMode: oidValue(git.RegularFileMode),
	})
	opts := externalBridgeTestRecoveryOptions(f)

	proved, err := ProveUnpublishedChain(
		ctx, f.capture.dir, f.capture.db, opts)
	if err != nil {
		t.Fatalf("ProveUnpublishedChain: %v", err)
	}
	if !proved.Handled || proved.Outcome != state.EventStatePublished ||
		proved.CommitOID != f.target || proved.EventCount != len(f.pendingSeqs) {
		t.Fatalf("proof=%+v want only %d frozen events at %s",
			proved, len(f.pendingSeqs), f.target)
	}
	if eventState, commit := readEventState(
		t, ctx, f.capture.db, laterSeq); eventState != state.EventStatePending || commit.Valid {
		t.Fatalf("read-only proof changed later seq=%d state=%q commit=%v",
			laterSeq, eventState, commit)
	}

	result, err := ReconcileUnpublishedChain(
		ctx, f.capture.dir, f.capture.db, opts)
	if err != nil {
		t.Fatalf("ReconcileUnpublishedChain: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStatePublished ||
		result.CommitOID != f.target || result.EventCount != len(f.pendingSeqs) {
		t.Fatalf("result=%+v want only %d frozen events at %s",
			result, len(f.pendingSeqs), f.target)
	}
	for _, seq := range f.pendingSeqs {
		if eventState, commit := readEventState(
			t, ctx, f.capture.db, seq); eventState != state.EventStatePublished ||
			!commit.Valid || commit.String != f.target {
			t.Fatalf("frozen seq=%d state=%q commit=%v want published at %s",
				seq, eventState, commit, f.target)
		}
	}
	if eventState, commit := readEventState(
		t, ctx, f.capture.db, laterSeq); eventState != state.EventStatePending || commit.Valid {
		t.Fatalf("later seq=%d state=%q commit=%v want pending",
			laterSeq, eventState, commit)
	}
	resolved, err := state.ReconcileResolvedPublicationDrains(
		ctx, f.capture.db, 3)
	if err != nil || len(resolved) != 1 ||
		resolved[0].ResolvedEvents != int64(f.drainEventCount) {
		t.Fatalf("resolved drains=%+v err=%v", resolved, err)
	}
}

func TestRunExternalRepairBridgePreservesLaterCapturedShadow(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyEvent))
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	registerLiveClient(t, f.capture.db)
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.capture.dir},
		"reset", "--hard", f.source); err != nil {
		t.Fatalf("reset fixture to persisted source: %v", err)
	}

	shadowCtx := f.capture.cctx
	shadowCtx.BaseHead = f.source
	if _, err := ReseedShadowFromHead(
		ctx, f.capture.dir, f.capture.db, shadowCtx); err != nil {
		t.Fatalf("seed live shadow: %v", err)
	}
	const laterPath = "captured-after-frozen-drain.txt"
	laterBody := []byte("protected later edit\n")
	laterBlob, err := git.HashObjectStdin(ctx, f.capture.dir, laterBody)
	if err != nil {
		t.Fatalf("hash later edit: %v", err)
	}
	laterSeq := appendRecoveryEvent(
		t, ctx, f.capture, f.source, state.CaptureOp{
			Op: "create", Path: laterPath,
			AfterOID:  oidValue(laterBlob),
			AfterMode: oidValue(git.RegularFileMode),
		})
	if err := os.WriteFile(
		filepath.Join(f.capture.dir, laterPath), laterBody, 0o644); err != nil {
		t.Fatalf("write later edit: %v", err)
	}
	if err := state.UpsertShadowPath(ctx, f.capture.db, state.ShadowPath{
		BranchRef:        f.capture.cctx.BranchRef,
		BranchGeneration: f.capture.cctx.BranchGeneration,
		Path:             laterPath,
		Operation:        "create",
		Mode:             oidValue(git.RegularFileMode),
		OID:              oidValue(laterBlob),
		BaseHead:         f.source,
		Fidelity:         "rescan",
	}); err != nil {
		t.Fatalf("record later shadow: %v", err)
	}

	wakeCh := make(chan struct{}, 8)
	shutdownCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	checkHook, checkEntered, releaseCheck := oneShotBranchTokenCheckGate()
	defer releaseCheck()
	var wg sync.WaitGroup
	var runErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = Run(runCtx, Options{
			RepoPath:        f.capture.dir,
			GitDir:          f.capture.gitDir,
			DB:              f.capture.db,
			Scheduler:       fastScheduler(),
			BootGrace:       30 * time.Second,
			WakeCh:          wakeCh,
			ShutdownCh:      shutdownCh,
			SkipSignals:     true,
			MessageFn:       DeterministicMessage,
			beforeBranchTokenCheck: checkHook,
			replay: func(_ context.Context, _ string, _ *state.DB,
				cctx CaptureContext, _ ReplayOpts) (ReplaySummary, error) {
				return ReplaySummary{BaseHead: cctx.BaseHead, Skipped: true}, nil
			},
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		if runErr != nil {
			t.Fatalf("Run: %v", runErr)
		}
	})

	waitForDaemonMode(t, f.capture.db, "running", 2*time.Second)
	waitForBranchTokenCheckGate(t, checkEntered)
	if err := git.UpdateRef(ctx, f.capture.dir, f.capture.cctx.BranchRef,
		f.live, f.source); err != nil {
		t.Fatalf("advance branch to external descendant: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.capture.dir},
		"reset", "--hard", f.live); err != nil {
		t.Fatalf("materialize external descendant: %v", err)
	}
	releaseCheck()
	waitForMetaValue(t, f.capture.db, MetaKeyBranchHead, f.live, 10*time.Second)
	if eventState, commit := readEventState(
		t, ctx, f.capture.db, laterSeq); eventState != state.EventStatePending || commit.Valid {
		t.Fatalf("later seq=%d state=%q commit=%v want pending after transition",
			laterSeq, eventState, commit)
	}
	for i := 0; i < 4; i++ {
		wakeCh <- struct{}{}
		time.Sleep(50 * time.Millisecond)
	}
	waitFor(t, 5*time.Second, "post-transition capture pass", func() bool {
		var baseHead string
		err := f.capture.db.ReadSQL().QueryRowContext(ctx, `
SELECT base_head FROM shadow_paths
WHERE branch_ref=? AND branch_generation=? AND path=?`,
			f.capture.cctx.BranchRef, f.capture.cctx.BranchGeneration,
			laterPath).Scan(&baseHead)
		return err == nil && baseHead == f.live
	})
	var eventCount int
	if err := f.capture.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM capture_events WHERE path=?`, laterPath).Scan(
		&eventCount); err != nil {
		t.Fatalf("count later capture: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("later capture rows=%d want exactly one", eventCount)
	}
	var externalCount int
	if err := f.capture.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM capture_events WHERE path='later.txt'`).Scan(
		&externalCount); err != nil {
		t.Fatalf("count external descendant capture: %v", err)
	}
	if externalCount != 0 {
		t.Fatalf("external descendant capture rows=%d want zero", externalCount)
	}
}

func TestExternalRepairBridgeUsesAuthoritativeRecaptureReset(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	var repairHead string
	if err := f.capture.db.ReadSQL().QueryRowContext(ctx, `
SELECT source_head
FROM self_publications
WHERE id='external-repair-publication'`).Scan(&repairHead); err != nil {
		t.Fatalf("load repair head: %v", err)
	}
	repairBlob, err := git.LsTreeBlobOID(
		ctx, f.capture.dir, repairHead, "pending.txt")
	if err != nil || repairBlob == "" {
		t.Fatalf("repair pending blob=%q err=%v", repairBlob, err)
	}
	recaptureSeq := f.pendingSeqs[1]
	if _, err := f.capture.db.SQL().ExecContext(ctx, `
UPDATE capture_events SET base_head=? WHERE seq=?`,
		repairHead, recaptureSeq); err != nil {
		t.Fatalf("set recapture base: %v", err)
	}
	if _, err := f.capture.db.SQL().ExecContext(ctx, `
UPDATE capture_ops SET before_oid=?,before_mode=?
WHERE event_seq=? AND ord=0`,
		repairBlob, git.RegularFileMode, recaptureSeq); err != nil {
		t.Fatalf("set recapture before-state: %v", err)
	}

	result, err := ReconcileUnpublishedChain(
		ctx, f.capture.dir, f.capture.db,
		externalBridgeTestRecoveryOptions(f))
	if err != nil {
		t.Fatalf("ReconcileUnpublishedChain: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStatePublished ||
		result.CommitOID != f.target || result.EventCount != len(f.pendingSeqs) {
		t.Fatalf("result=%+v want authoritative recapture at %s",
			result, f.target)
	}
}

func TestExternalRepairPublicationSuffixReconstructsCoalescedOps(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	const path = "coalesced.txt"
	firstBlob := externalBridgeTestBlob(t, ctx, f, "first candidate save\n")
	finalBlob := externalBridgeTestBlob(t, ctx, f, "final candidate save\n")
	firstSeq := appendRecoveryEvent(t, ctx, f, f.cctx.BaseHead, state.CaptureOp{
		Op: "create", Path: path,
		AfterOID: oidValue(firstBlob), AfterMode: oidValue(git.RegularFileMode),
	})
	finalSeq := appendRecoveryEvent(t, ctx, f, f.cctx.BaseHead, state.CaptureOp{
		Op: "create", Path: path,
		AfterOID: oidValue(finalBlob), AfterMode: oidValue(git.RegularFileMode),
	})
	const candidateID = "external-bridge-coalesced-candidate"
	if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{
		ID: candidateID, BranchRef: f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		Status:           state.IntentCandidateReady,
		Purpose:          "publish the final save",
		Readiness:        state.IntentReadinessReady,
		Events: []state.IntentCandidateEvent{
			{EventSeq: firstSeq, EventRole: "code"},
			{EventSeq: finalSeq, EventRole: "code"},
		},
	}); err != nil {
		t.Fatalf("SaveIntentCandidate: %v", err)
	}
	target := commitTreeWithIndexUpdates(
		t, ctx, f, f.cctx.BaseHead, "coalesced publication",
		git.RegularFileMode+" "+finalBlob+"\t"+path)
	publication := state.SelfPublication{
		ID:               "external-bridge-coalesced-publication",
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		SourceHead:       f.cctx.BaseHead,
		TargetCommitOID:  target,
		TargetTreeOID:    externalBridgeTestTree(t, ctx, f, target),
		CreatedTS:        1,
		Members: []state.SelfPublicationMember{
			{EventSeq: firstSeq, CandidateID: oidValue(candidateID)},
			{EventSeq: finalSeq, CandidateID: oidValue(candidateID)},
		},
		Completion: state.SelfPublicationCompletion{
			PublishedTS: 3, CandidateStatus: state.IntentCandidatePublished,
		},
	}
	completeExternalBridgeTestPublication(t, ctx, f, publication)

	suffix, owned, err := loadExternalRepairPublicationSuffix(
		ctx, f.dir, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
		f.cctx.BaseHead, target)
	if err != nil {
		t.Fatalf("loadExternalRepairPublicationSuffix: %v", err)
	}
	if !owned || len(suffix) != 1 || len(suffix[0].Ops) != 1 {
		t.Fatalf("suffix=%+v owned=%t want one merged operation", suffix, owned)
	}
	merged := suffix[0].Ops[0]
	if merged.Op != "create" || merged.Path != path ||
		!merged.AfterOID.Valid || merged.AfterOID.String != finalBlob {
		t.Fatalf("merged op=%+v want final create %s", merged, finalBlob)
	}
	index := make(map[string]git.IndexEntry)
	if conflict := applyRecoveryOpsInMemory(index, suffix[0].Ops); conflict != "" {
		t.Fatalf("merged publication conflicts: %s", conflict)
	}
	if got := index[path]; got.OID != finalBlob || got.Mode != git.RegularFileMode {
		t.Fatalf("merged index entry=%+v want %s %s",
			got, git.RegularFileMode, finalBlob)
	}

	rawIndex := make(map[string]git.IndexEntry)
	for _, seq := range []int64{firstSeq, finalSeq} {
		ops, err := state.LoadCaptureOps(ctx, f.db, seq)
		if err != nil {
			t.Fatalf("LoadCaptureOps seq=%d: %v", seq, err)
		}
		if conflict := applyRecoveryOpsInMemory(rawIndex, ops); conflict != "" {
			if seq != finalSeq {
				t.Fatalf("first raw operation conflicts: %s", conflict)
			}
			return
		}
	}
	t.Fatal("raw conflicting creates unexpectedly applied without coalescing")
}

func TestExternalRepairPublicationSuffixRejectsGitParentMismatch(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	const path = "bad-parent.txt"
	blob := externalBridgeTestBlob(t, ctx, f, "bad parent\n")
	seq := appendRecoveryEvent(t, ctx, f, f.cctx.BaseHead, state.CaptureOp{
		Op: "create", Path: path,
		AfterOID: oidValue(blob), AfterMode: oidValue(git.RegularFileMode),
	})
	intermediate := commitTreeWithIndexUpdates(
		t, ctx, f, f.cctx.BaseHead, "unrecorded parent")
	target := commitTreeWithIndexUpdates(
		t, ctx, f, intermediate, "mismatched publication parent",
		git.RegularFileMode+" "+blob+"\t"+path)
	publication := state.SelfPublication{
		ID:               "external-bridge-parent-mismatch",
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		SourceHead:       f.cctx.BaseHead,
		TargetCommitOID:  target,
		TargetTreeOID:    externalBridgeTestTree(t, ctx, f, target),
		CreatedTS:        1,
		Members:          []state.SelfPublicationMember{{EventSeq: seq}},
		Completion: state.SelfPublicationCompletion{
			PublishedTS: 3, CandidateStatus: state.IntentCandidatePublished,
		},
	}
	completeExternalBridgeTestPublication(t, ctx, f, publication)

	_, _, err := loadExternalRepairPublicationSuffix(
		ctx, f.dir, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
		f.cctx.BaseHead, target)
	if !errors.Is(err, state.ErrCompletedBranchTransitionProof) {
		t.Fatalf("load suffix err=%v want completed transition proof failure", err)
	}
}

func TestExternalRepairPublicationSuffixRejectsMembershipDigestDrift(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	const publicationID = "external-repair-publication"
	var repairHead string
	if err := f.capture.db.ReadSQL().QueryRowContext(ctx, `
SELECT source_head FROM self_publications WHERE id=?`, publicationID).Scan(
		&repairHead); err != nil {
		t.Fatalf("load publication source: %v", err)
	}
	if _, err := f.capture.db.SQL().ExecContext(ctx,
		`DROP TRIGGER self_publications_identity_immutable`); err != nil {
		t.Fatalf("disable isolated fixture identity trigger: %v", err)
	}
	if _, err := f.capture.db.SQL().ExecContext(ctx, `
UPDATE self_publications
SET membership_digest='sha256:0000000000000000000000000000000000000000000000000000000000000000'
WHERE id=?`, publicationID); err != nil {
		t.Fatalf("drift publication membership digest: %v", err)
	}

	_, _, err := loadExternalRepairPublicationSuffix(
		ctx, f.capture.dir, f.capture.db,
		f.capture.cctx.BranchRef, f.capture.cctx.BranchGeneration,
		repairHead, f.source)
	if !errors.Is(err, state.ErrCompletedBranchTransitionProof) {
		t.Fatalf("load suffix err=%v want completed transition proof failure", err)
	}
}

func TestExternalBridgeChangedPathsClassifiesPathLimit(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	blob := externalBridgeTestBlob(t, ctx, f, "bounded path\n")
	updates := make([]string, 0, maxExternalRepairBridgePaths+1)
	for i := 0; i <= maxExternalRepairBridgePaths; i++ {
		updates = append(updates, fmt.Sprintf(
			"%s %s\tbounded/%04d.txt", git.RegularFileMode, blob, i))
	}
	target := commitTreeWithIndexUpdates(
		t, ctx, f, f.cctx.BaseHead, "oversized bridge target", updates...)

	_, err := externalBridgeChangedPaths(
		ctx, f.dir, f.cctx.BaseHead, target)
	if !errors.Is(err, state.ErrCompletedBranchTransitionProof) {
		t.Fatalf("externalBridgeChangedPaths err=%v want completed transition proof failure", err)
	}
}

func externalBridgeTestRecoveryOptions(
	f externalRepairBridgeFixture,
) RecoveryReconcileOptions {
	return RecoveryReconcileOptions{
		GitDir:             f.capture.gitDir,
		BranchRef:          f.capture.cctx.BranchRef,
		BranchGeneration:   f.capture.cctx.BranchGeneration,
		FirstSeq:           f.pendingSeqs[0],
		Trigger:            "test_external_repair_bridge_edge",
		ExternalParentHead: f.source,
	}
}

func externalBridgeTestBlob(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	contents string,
) string {
	t.Helper()
	oid, err := git.HashObjectStdin(ctx, f.dir, []byte(contents))
	if err != nil {
		t.Fatalf("HashObjectStdin: %v", err)
	}
	return oid
}

func externalBridgeTestTree(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	commit string,
) string {
	t.Helper()
	tree, err := git.RevParse(ctx, f.dir, commit+"^{tree}")
	if err != nil {
		t.Fatalf("resolve tree for %s: %v", commit, err)
	}
	return tree
}

func completeExternalBridgeTestPublication(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	publication state.SelfPublication,
) {
	t.Helper()
	if created, err := state.PrepareSelfPublication(
		ctx, f.db, publication); err != nil || !created {
		t.Fatalf("PrepareSelfPublication=(%t,%v)", created, err)
	}
	if changed, err := state.MarkSelfPublicationGitApplied(
		ctx, f.db, publication, 2); err != nil || !changed {
		t.Fatalf("MarkSelfPublicationGitApplied=(%t,%v)", changed, err)
	}
	if completed, err := state.CompleteSelfPublication(
		ctx, f.db, publication, state.SelfPublicationCompletion{
			PublishedTS: 3, CandidateStatus: state.IntentCandidatePublished,
		}); err != nil || !completed {
		t.Fatalf("CompleteSelfPublication=(%t,%v)", completed, err)
	}
}
