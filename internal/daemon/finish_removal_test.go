package daemon

import (
	"context"
	"database/sql"
	"errors"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

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
		if err := testutil.SeedLegacyCaptureOutcome(
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
		if err := testutil.SeedLegacyCaptureOutcome(
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
	if created, err := testutil.SeedLegacySelfPublication(
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
		ctx, f.db, checkpoint, "sha256:"+strings.Repeat("a", 64)); err != nil || !created {
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
	if created, err := testutil.SeedLegacyDrain(
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
) VALUES (?, ?, ?, 'prepared', ?, ?, ?, ?, ?, 1, 1, '')`,
		repairID, f.cctx.BranchRef, f.cctx.BranchGeneration,
		oldHead, "sha256:"+strings.Repeat("0", 64),
		"refs/acd/intent-repair/test/"+repairID+"/backup",
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
	repair, ok, err := state.IntentRepairByID(ctx, f.db, repairID)
	if err != nil || !ok {
		t.Fatalf("load prepared repair=(%t,%v)", ok, err)
	}
	digest, err := recoveredIntentRepairPlanDigest(
		ctx, f.dir, repair, repair.Commits)
	if err != nil {
		t.Fatalf("digest prepared repair: %v", err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, `
UPDATE intent_repairs SET plan_digest=? WHERE id=? AND status='prepared'`,
		digest, repairID); err != nil {
		t.Fatalf("seal repair plan digest: %v", err)
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
func appendRecoveryEvent(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	baseHead string,
	op state.CaptureOp,
) int64 {
	t.Helper()
	if op.Fidelity == "" {
		op.Fidelity = "rescan"
	}
	ev := state.CaptureEvent{
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		BaseHead:         baseHead,
		Operation:        op.Op,
		Path:             op.Path,
		OldPath:          op.OldPath,
		Fidelity:         op.Fidelity,
	}
	seq, err := testutil.SeedLegacyCapture(ctx, f.db, ev, []state.CaptureOp{op})
	if err != nil {
		t.Fatalf("AppendCaptureEvent %s %s: %v", op.Op, op.Path, err)
	}
	return seq
}
func commitTreeWithIndexUpdates(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	baseHead string,
	message string,
	lines ...string,
) string {
	t.Helper()
	indexFile := filepath.Join(t.TempDir(), "idx")
	if err := git.ReadTree(ctx, f.dir, indexFile, baseHead); err != nil {
		t.Fatalf("ReadTree %s: %v", baseHead, err)
	}
	if err := git.UpdateIndexInfo(ctx, f.dir, indexFile, lines); err != nil {
		t.Fatalf("UpdateIndexInfo: %v", err)
	}
	tree, err := git.WriteTree(ctx, f.dir, indexFile)
	if err != nil {
		t.Fatalf("WriteTree: %v", err)
	}
	commit, err := git.CommitTree(ctx, f.dir, tree, message, baseHead)
	if err != nil {
		t.Fatalf("CommitTree: %v", err)
	}
	return commit
}
func gitRawOutput(t *testing.T, ctx context.Context, repoDir string, args ...string) string {
	t.Helper()
	out, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, args...)
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return string(out)
}

// readEventState returns the (state, commit_oid) for a capture_events row.
func readEventState(t *testing.T, ctx context.Context, db *state.DB, seq int64) (string, sql.NullString) {
	t.Helper()
	var s string
	var oid sql.NullString
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT state, commit_oid FROM capture_events WHERE seq = ?`, seq).Scan(&s, &oid); err != nil {
		t.Fatalf("query capture_events seq=%d: %v", seq, err)
	}
	return s, oid
}
func TestFinishRemovalEmptyLegacyClientsKeepProtectionRunning(t *testing.T) {
	f := newCaptureFixture(t)
	if err := testutil.InitLegacy(t.Context(), f.db); err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	var seconds atomic.Int64
	wake, _ := finishProtectionRun(t, f, func(o *Options) {
		o.Now = func() time.Time { return base.Add(time.Duration(seconds.Load()) * time.Second) }
		o.progressHeartbeatEvery = 10 * time.Millisecond
	})
	var clients int
	if err := f.db.ReadSQL().QueryRow("SELECT COUNT(*) FROM daemon_clients").Scan(&clients); err != nil || clients != 0 {
		t.Fatalf("legacy clients=%d: %v", clients, err)
	}
	for step := int64(1); step <= 3; step++ {
		before, _, err := state.MetaGet(context.Background(), f.db, MetaKeyProtectionObservationEpoch)
		if err != nil {
			t.Fatal(err)
		}
		seconds.Store(step * 3600)
		wake <- struct{}{}
		finishProtectionWait(t, func() bool {
			epoch, _, _ := state.MetaGet(context.Background(), f.db, MetaKeyProtectionCoveredEpoch)
			return epoch != before
		})
		finishProtectionWait(t, func() bool {
			st, ok, _ := state.LoadDaemonState(context.Background(), f.db)
			return ok && st.Mode == "running" && st.HeartbeatTS >= float64(base.Unix()+step*3600)
		})
	}
	finishWrite(t, f.dir, "after-idle.txt", "protected after the old grace period")
	wake <- struct{}{}
	finishProtectionWait(t, func() bool {
		cp := finishProtectionLatest(t, f)
		if cp == nil {
			return false
		}
		oid, err := git.LsTreeBlobOID(context.Background(), f.dir, cp.CommitOID, "after-idle.txt")
		return err == nil && oid != "" && finishProtectionHas(t, f, "after-idle.txt", "protected after the old grace period")
	})
}

func TestFinishRemovalLegacyBridgeRequiresActivationWithoutMutation(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	opts := RecoveryReconcileOptions{GitDir: f.capture.gitDir, BranchRef: f.capture.cctx.BranchRef, BranchGeneration: f.capture.cctx.BranchGeneration, FirstSeq: f.pendingSeqs[0], ExternalParentHead: f.source}
	refs := gitRawOutput(t, ctx, f.capture.dir, "show-ref")
	indexPath := filepath.Join(f.capture.gitDir, "index")
	index, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	work := gitRawOutput(t, ctx, f.capture.dir, "status", "--porcelain")
	dbBytes, err := os.ReadFile(f.capture.db.Path())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReconcileUnpublishedChain(ctx, f.capture.dir, f.capture.db, opts); !errors.Is(err, errLegacyBridgeActivationRequired) {
		t.Fatalf("mutable legacy bridge accepted: %v", err)
	}
	if refs != gitRawOutput(t, ctx, f.capture.dir, "show-ref") || work != gitRawOutput(t, ctx, f.capture.dir, "status", "--porcelain") {
		t.Fatal("legacy refusal changed refs or worktree")
	}
	afterIndex, err := os.ReadFile(indexPath)
	if err != nil || string(index) != string(afterIndex) {
		t.Fatal("legacy refusal changed raw index")
	}
	afterDB, err := os.ReadFile(f.capture.db.Path())
	if err != nil || string(dbBytes) != string(afterDB) {
		t.Fatal("legacy refusal changed state")
	}
	for _, seq := range f.pendingSeqs {
		eventState, commit := readEventState(t, ctx, f.capture.db, seq)
		if eventState != state.EventStatePending || commit.Valid {
			t.Fatal("legacy refusal rewrote capture provenance")
		}
	}
}
