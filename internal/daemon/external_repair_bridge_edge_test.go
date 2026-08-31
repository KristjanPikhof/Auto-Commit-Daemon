package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestExternalRepairBridgeRejectsRepairPlanDigestDrift(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	if _, err := f.capture.db.SQL().ExecContext(ctx, `
UPDATE intent_repairs SET plan_digest=? WHERE id='external-repair-bridge'`,
		"sha256:"+strings.Repeat("f", 64)); err != nil {
		t.Fatalf("drift repair plan digest: %v", err)
	}

	_, err := ProveUnpublishedChain(
		ctx, f.capture.dir, f.capture.db,
		externalBridgeTestRecoveryOptions(f))
	if !errors.Is(err, state.ErrCompletedBranchTransitionProof) {
		t.Fatalf("plan digest drift err=%v want completed-transition proof error", err)
	}
}

func TestExternalRepairBridgeRejectsFinalRepairMappingMismatch(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	if _, err := f.capture.db.SQL().ExecContext(ctx, `
UPDATE intent_repair_commits
SET new_oid=old_oid
WHERE repair_id='external-repair-bridge'
  AND ord=(SELECT MAX(ord) FROM intent_repair_commits
           WHERE repair_id='external-repair-bridge')`); err != nil {
		t.Fatalf("drift final repair mapping: %v", err)
	}

	_, err := ProveUnpublishedChain(
		ctx, f.capture.dir, f.capture.db,
		externalBridgeTestRecoveryOptions(f))
	if !errors.Is(err, state.ErrCompletedBranchTransitionProof) {
		t.Fatalf("final mapping drift err=%v want completed-transition proof error", err)
	}
}

func TestRecoveredIntentRepairPlanDigestBoundsSealedMetadata(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	tree := externalBridgeTestTree(t, ctx, f, f.cctx.BaseHead)
	tests := []struct {
		name   string
		commit func(t *testing.T) string
	}{
		{
			name: "message",
			commit: func(t *testing.T) string {
				return commitTreeWithIndexUpdates(
					t, ctx, f, f.cctx.BaseHead,
					strings.Repeat("m", int(intentRepairMessageReadCap)+1))
			},
		},
		{
			name: "author",
			commit: func(t *testing.T) string {
				oid, err := git.CommitTreeWithIdentity(
					ctx, f.dir, tree, "bounded author",
					strings.Repeat("a", int(intentRepairAuthorReadCap)+1),
					"bounded@example.com", f.cctx.BaseHead)
				if err != nil {
					t.Fatalf("CommitTreeWithIdentity: %v", err)
				}
				return oid
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newOID := tt.commit(t)
			_, err := recoveredIntentRepairPlanDigest(
				ctx, f.dir, state.IntentRepair{ID: "bounded-" + tt.name},
				[]state.IntentRepairCommit{{
					CandidateID: oidValue("bounded-candidate"),
					OldOID:      f.cctx.BaseHead,
					NewOID:      oidValue(newOID),
				}})
			if !errors.Is(err, ErrIntentRepairRecoveryProof) {
				t.Fatalf("recovered digest err=%v want recovery-proof error", err)
			}
		})
	}
}

func TestRecoveredIntentRepairPlanDigestRejectsMissingMappedCommit(t *testing.T) {
	f := newCaptureFixture(t)
	missingOID := strings.Repeat("f", len(f.cctx.BaseHead))
	_, err := recoveredIntentRepairPlanDigest(
		context.Background(), f.dir,
		state.IntentRepair{ID: "missing-mapped-commit"},
		[]state.IntentRepairCommit{{
			CandidateID: oidValue("missing-candidate"),
			OldOID:      f.cctx.BaseHead,
			NewOID:      oidValue(missingOID),
		}})
	if !errors.Is(err, ErrIntentRepairRecoveryProof) {
		t.Fatalf("missing mapped commit err=%v want recovery-proof error", err)
	}
}

func TestRecoveredIntentRepairPlanDigestKeepsCancellationRetryable(t *testing.T) {
	f := newCaptureFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := recoveredIntentRepairPlanDigest(
		ctx, f.dir, state.IntentRepair{ID: "canceled-mapped-commit"},
		[]state.IntentRepairCommit{{
			CandidateID: oidValue("canceled-candidate"),
			OldOID:      f.cctx.BaseHead,
			NewOID:      oidValue(f.cctx.BaseHead),
		}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled mapped commit err=%v want context cancellation", err)
	}
	if errors.Is(err, ErrIntentRepairRecoveryProof) {
		t.Fatalf("canceled mapped commit err=%v was classified permanent", err)
	}
}

func TestExternalRepairBridgeRejectsMissingMappedCommit(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	missingOID := strings.Repeat("f", len(f.source))
	if _, err := f.capture.db.SQL().ExecContext(ctx, `
UPDATE intent_repair_commits SET new_oid=?
WHERE repair_id='external-repair-bridge'`, missingOID); err != nil {
		t.Fatalf("drift repair mapping: %v", err)
	}
	if _, err := f.capture.db.SQL().ExecContext(ctx, `
UPDATE intent_repairs SET new_head=?
WHERE id='external-repair-bridge'`, missingOID); err != nil {
		t.Fatalf("drift repair head: %v", err)
	}

	evidence := newExternalRepairEvidence(
		f.capture.db, f.capture.dir, f.capture.cctx.BranchRef,
		f.capture.cctx.BranchGeneration)
	_, err := evidence.loadRepair(ctx, "external-repair-bridge")
	if !errors.Is(err, state.ErrCompletedBranchTransitionProof) {
		t.Fatalf("missing mapped commit err=%v want completed proof error", err)
	}
}

func TestCanonicalRepairMappingValidatesSealedPlan(t *testing.T) {
	tests := []struct {
		name       string
		driftPlan  bool
		driftFinal bool
	}{
		{name: "plan digest", driftPlan: true},
		{name: "final mapping", driftFinal: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newExternalRepairBridgeFixture(
				t, externalRepairBridgeFixtureOptions{})
			ctx := context.Background()
			const repairID = "historical-canonical-mapping"
			if _, err := f.capture.db.SQL().ExecContext(ctx, `
INSERT INTO intent_repairs(
    id,branch_ref,branch_generation,status,expected_head,plan_digest,
    backup_ref,old_head,new_head,created_ts,updated_ts,error
) VALUES (?, ?, ?, 'prepared', ?, ?, ?, ?, ?, 10, 10, '')`,
				repairID, f.capture.cctx.BranchRef,
				f.capture.cctx.BranchGeneration, f.live,
				"sha256:"+strings.Repeat("0", 64),
				"refs/acd/intent-repair/test/"+repairID+"/backup",
				f.live, f.target); err != nil {
				t.Fatalf("insert historical repair: %v", err)
			}
			if _, err := f.capture.db.SQL().ExecContext(ctx, `
INSERT INTO intent_repair_commits(
    repair_id,ord,candidate_id,old_oid,new_oid
) VALUES (?, 0, 'historical-candidate', ?, ?)`,
				repairID, f.source, f.target); err != nil {
				t.Fatalf("insert historical mapping: %v", err)
			}
			if _, err := f.capture.db.SQL().ExecContext(ctx, `
INSERT INTO intent_repair_member_seals(
    repair_id,membership_mode,member_count
) VALUES (?, 'legacy', 0)`, repairID); err != nil {
				t.Fatalf("seal historical mapping: %v", err)
			}
			repair, ok, err := state.IntentRepairByID(
				ctx, f.capture.db, repairID)
			if err != nil || !ok {
				t.Fatalf("load historical repair=(%t,%v)", ok, err)
			}
			digest, err := recoveredIntentRepairPlanDigest(
				ctx, f.capture.dir, repair, repair.Commits)
			if err != nil {
				t.Fatalf("digest historical repair: %v", err)
			}
			if tt.driftPlan {
				digest = "sha256:" + strings.Repeat("f", 64)
			}
			newHead := f.target
			if tt.driftFinal {
				newHead = f.live
			}
			if _, err := f.capture.db.SQL().ExecContext(ctx, `
UPDATE intent_repairs
SET status='completed',plan_digest=?,new_head=?,git_applied_ts=11,
    completed_ts=11,updated_ts=11
WHERE id=? AND status='prepared'`, digest, newHead, repairID); err != nil {
				t.Fatalf("complete historical repair: %v", err)
			}

			evidence := newExternalRepairEvidence(
				f.capture.db, f.capture.dir, f.capture.cctx.BranchRef,
				f.capture.cctx.BranchGeneration)
			_, err = evidence.canonicalCommitChain(ctx, f.source)
			if !errors.Is(err, state.ErrCompletedBranchTransitionProof) {
				t.Fatalf("canonical mapping err=%v want completed proof error", err)
			}
		})
	}
}

func TestExternalRepairBridgeClassifiesHistoricalObjectDrift(t *testing.T) {
	tests := []struct {
		name      string
		memberSQL string
		memberID  string
	}{
		{
			name: "repair member",
			memberSQL: `SELECT event_seq FROM intent_repair_members
WHERE repair_id=? ORDER BY ord LIMIT 1`,
			memberID: "external-repair-bridge",
		},
		{
			name: "publication member",
			memberSQL: `SELECT event_seq FROM self_publication_members
WHERE publication_id=? ORDER BY ord LIMIT 1`,
			memberID: "external-repair-publication",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newExternalRepairBridgeFixture(
				t, externalRepairBridgeFixtureOptions{})
			ctx := context.Background()
			var seq int64
			if err := f.capture.db.ReadSQL().QueryRowContext(
				ctx, tt.memberSQL, tt.memberID).Scan(&seq); err != nil {
				t.Fatalf("load historical member: %v", err)
			}
			if _, err := f.capture.db.SQL().ExecContext(ctx, `
UPDATE capture_ops SET after_oid=? WHERE event_seq=? AND ord=0`,
				strings.Repeat("f", 40), seq); err != nil {
				t.Fatalf("drift captured object: %v", err)
			}

			_, err := ProveUnpublishedChain(
				ctx, f.capture.dir, f.capture.db,
				externalBridgeTestRecoveryOptions(f))
			if !errors.Is(err, state.ErrCompletedBranchTransitionProof) {
				t.Fatalf("historical object drift err=%v want completed proof error", err)
			}
		})
	}
}

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
			RepoPath:               f.capture.dir,
			GitDir:                 f.capture.gitDir,
			DB:                     f.capture.db,
			Scheduler:              fastScheduler(),
			BootGrace:              30 * time.Second,
			WakeCh:                 wakeCh,
			ShutdownCh:             shutdownCh,
			SkipSignals:            true,
			MessageFn:              DeterministicMessage,
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
			{EventSeq: firstSeq},
			{EventSeq: finalSeq},
		},
		Completion: state.SelfPublicationCompletion{
			PublishedTS: 3, CandidateStatus: state.IntentCandidatePublished,
		},
	}
	completeExternalBridgeTestPublication(t, ctx, f, publication)
	evidence := newExternalRepairEvidence(
		f.db, f.dir, f.cctx.BranchRef, f.cctx.BranchGeneration)

	suffix, owned, err := loadExternalRepairPublicationSuffix(
		ctx, f.dir, evidence,
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

func TestExternalRepairPublicationSuffixSharesEvidenceBudget(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	blob := externalBridgeTestBlob(t, ctx, f, "bounded publication\n")
	seq := appendRecoveryEvent(t, ctx, f, f.cctx.BaseHead, state.CaptureOp{
		Op: "create", Path: "bounded.txt",
		AfterOID: oidValue(blob), AfterMode: oidValue(git.RegularFileMode),
	})
	target := commitTreeWithIndexUpdates(
		t, ctx, f, f.cctx.BaseHead, "bounded publication",
		git.RegularFileMode+" "+blob+"\tbounded.txt")
	completeExternalBridgeTestPublication(t, ctx, f, state.SelfPublication{
		ID:        "external-bridge-bounded-publication",
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		SourceHead: f.cctx.BaseHead, TargetCommitOID: target,
		TargetTreeOID: externalBridgeTestTree(t, ctx, f, target),
		CreatedTS:     1,
		Members:       []state.SelfPublicationMember{{EventSeq: seq}},
		Completion: state.SelfPublicationCompletion{
			PublishedTS: 3, CandidateStatus: state.IntentCandidatePublished,
		},
	})
	evidence := newExternalRepairEvidence(
		f.db, f.dir, f.cctx.BranchRef, f.cctx.BranchGeneration)
	// One transition, one member, one capture event, and one operation require
	// four shared evidence rows. A budget of three must fail before an
	// unbounded per-member operation read can occur.
	evidence.budget.remaining = 3
	_, _, err := loadExternalRepairPublicationSuffix(
		ctx, f.dir, evidence, f.cctx.BaseHead, target)
	if !errors.Is(err, state.ErrCompletedBranchTransitionProof) {
		t.Fatalf("publication suffix err=%v want shared proof-budget error", err)
	}
}

func TestExternalRepairPublicationSuffixKeepsCancellationRetryable(t *testing.T) {
	f := newCaptureFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	evidence := newExternalRepairEvidence(
		f.db, f.dir, f.cctx.BranchRef, f.cctx.BranchGeneration)
	_, _, err := loadExternalRepairPublicationSuffix(
		ctx, f.dir, evidence, f.cctx.BaseHead, "unreached-target")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled suffix err=%v want context cancellation", err)
	}
	if errors.Is(err, state.ErrCompletedBranchTransitionProof) {
		t.Fatalf("canceled suffix err=%v was misclassified as permanent proof failure", err)
	}
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
	evidence := newExternalRepairEvidence(
		f.db, f.dir, f.cctx.BranchRef, f.cctx.BranchGeneration)

	_, _, err := loadExternalRepairPublicationSuffix(
		ctx, f.dir, evidence,
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
	evidence := newExternalRepairEvidence(
		f.capture.db, f.capture.dir, f.capture.cctx.BranchRef,
		f.capture.cctx.BranchGeneration)

	_, _, err := loadExternalRepairPublicationSuffix(
		ctx, f.capture.dir, evidence,
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

func TestExternalBridgeIndexStateChunksPathspecArguments(t *testing.T) {
	f := newCaptureFixture(t)
	paths := make([]string, 0, maxExternalRepairBridgePaths)
	for i := 0; i < maxExternalRepairBridgePaths-1; i++ {
		paths = append(paths, fmt.Sprintf(
			"missing-%04d-%s", i, strings.Repeat("x", 235)))
	}
	paths = append(paths, ".gitignore")
	unchunkedBytes := argvBytes("git", "ls-tree", "-z", f.cctx.BaseHead, "--")
	for _, path := range paths {
		unchunkedBytes += len(git.LiteralPathspec(path)) + 1
	}
	if unchunkedBytes <= 1<<20 {
		t.Fatalf("fixture argv=%d bytes does not exceed macOS ARG_MAX", unchunkedBytes)
	}

	index, err := externalBridgeIndexState(
		context.Background(), f.dir, f.cctx.BaseHead, paths)
	if err != nil {
		t.Fatalf("externalBridgeIndexState: %v", err)
	}
	if len(index) != 1 || index[".gitignore"].OID == "" {
		t.Fatalf("index=%+v want only the existing .gitignore", index)
	}
}

func TestExternalBridgePathspecChunksRejectInvalidPathSets(t *testing.T) {
	tests := []struct {
		name  string
		paths []string
	}{
		{name: "duplicate", paths: []string{"same.txt", "same.txt"}},
		{name: "overlong", paths: []string{
			strings.Repeat("x", externalBridgeGitArgvByteCap),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := externalBridgePathspecChunks("HEAD", tt.paths)
			if !errors.Is(err, state.ErrCompletedBranchTransitionProof) {
				t.Fatalf("pathspec chunks err=%v want completed proof error", err)
			}
		})
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
