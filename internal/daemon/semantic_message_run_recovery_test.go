package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRunAppliesAlternativeRuntimeBeforeSemanticMessageRecovery(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "test-only-key")
	t.Setenv(ai.EnvProvider, "deterministic")
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyEvent))

	f := newCaptureFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatalf("bootstrap shadow: %v", err)
	}
	if err := state.MetaSetMany(ctx, f.db, map[string]string{
		MetaKeyBranchGeneration: strconv.FormatInt(f.cctx.BranchGeneration, 10),
		MetaKeyBranchHead:       f.cctx.BaseHead,
		MetaKeyBranchToken:      branchTokenRev(f.cctx.BaseHead, f.cctx.BranchRef),
	}); err != nil {
		t.Fatal(err)
	}

	firstBlob, err := git.HashObjectStdin(ctx, f.dir, []byte("first\n"))
	if err != nil {
		t.Fatal(err)
	}
	laterBlob, err := git.HashObjectStdin(ctx, f.dir, []byte("later\n"))
	if err != nil {
		t.Fatal(err)
	}
	firstSeq := appendRecoveryEvent(t, ctx, f, f.cctx.BaseHead, state.CaptureOp{
		Op: "create", Path: "first.txt",
		AfterOID:  sql.NullString{String: firstBlob, Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	laterSeq := appendRecoveryEvent(t, ctx, f, f.cctx.BaseHead, state.CaptureOp{
		Op: "create", Path: "later.txt",
		AfterOID:  sql.NullString{String: laterBlob, Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})

	frozen := insertPublicationRuntimeRevision(t, f.db, 1,
		"openai-compat", "https://frozen.example/v1", "frozen-model")
	activatePublicationRuntimeRevision(t, f.db, frozen.ID, sql.NullInt64{})
	strategy, format, provider, fingerprint, err :=
		publicationRuntimeRevisionContract(frozen)
	if err != nil {
		t.Fatal(err)
	}
	if provider != "openai-compat" {
		t.Fatalf("frozen provider=%q want openai-compat", provider)
	}

	tree, err := git.RevParse(ctx, f.dir, f.cctx.BaseHead+"^{tree}")
	if err != nil {
		t.Fatal(err)
	}
	worktreeID := checkpointpkg.WorktreeID(f.dir)
	checkpointID := "cp-1788213600000-0123456789abcdef"
	checkpointRef := fmt.Sprintf(
		"refs/acd/checkpoints/v1/%s/%s", worktreeID, checkpointID)
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir},
		"update-ref", checkpointRef, f.cctx.BaseHead); err != nil {
		t.Fatal(err)
	}
	checkpoint := state.Checkpoint{
		ID: checkpointID, OperationID: "op-run-semantic-message-recovery",
		WorktreeID: worktreeID, Reason: state.CheckpointReasonManualBarrier,
		ObservationEpoch: 1, CoverageEpoch: 1,
		ObservedHead: f.cctx.BaseHead, ObservedRef: f.cctx.BranchRef,
		TreeOID: tree, CommitOID: f.cctx.BaseHead, Ref: checkpointRef,
		CreatedTS: 1.5, EventSeqs: []int64{firstSeq},
	}
	if created, err := state.PrepareCheckpoint(
		ctx, f.db, checkpoint, publicationDrainTestDigest); err != nil || !created {
		t.Fatalf("prepare checkpoint=(%t,%v)", created, err)
	}
	if err := state.CompleteCheckpoint(
		ctx, f.db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 1.75); err != nil {
		t.Fatal(err)
	}
	drain := state.PublicationDrain{
		ID: "drain-" + checkpointID, CheckpointID: checkpoint.ID,
		WorktreeID: worktreeID, BranchRef: f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		Phase:            state.PublicationDrainCheckpointing,
		TargetEventCount: 1, CreatedTS: 2, UpdatedTS: 2,
		LastProgressTS: 2, EventSeqs: []int64{firstSeq},
		CommitStrategy: strategy, CommitFormat: format,
		ConfigRevisionID: frozen.ID, Provider: provider,
		ProviderModel: "frozen-model", ProviderFingerprint: fingerprint,
	}
	if created, err := state.PreparePublicationDrain(
		ctx, f.db, drain); err != nil || !created {
		t.Fatalf("prepare drain=(%t,%v)", created, err)
	}
	blockedUpdate := PublicationDrainUpdateFrom(drain, 2.5, 2)
	blockedUpdate.Phase = state.PublicationDrainNeedsAction
	blockedUpdate.FallbackMode = publicationFallbackLocalUnlock
	blockedUpdate.LastError = PublicationDrainSemanticMessageUnavailableReason
	blocked, err := state.AdvancePublicationDrain(
		ctx, f.db, drain.ID, blockedUpdate)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Phase != state.PublicationDrainNeedsAction ||
		blocked.ConfigRevisionID != frozen.ID ||
		blocked.LastError != PublicationDrainSemanticMessageUnavailableReason {
		t.Fatalf("frozen drain=%+v", blocked)
	}

	type replayCall struct {
		revisionID int64
		drainID    string
	}
	var replayCalls []replayCall
	recovered := make(chan struct{}, 1)
	wakeCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(runCtx, Options{
			RepoPath: f.dir, GitDir: f.gitDir, DB: f.db,
			Scheduler: fastScheduler(), BootGrace: 30 * time.Second,
			MessageFn: DeterministicMessage, WakeCh: wakeCh,
			ShutdownCh: make(chan struct{}, 1), SkipSignals: true,
			runtimeBuildProvider: runtimeBuilder(
				f.db, map[string]*runtimeTestCloser{}).BuildProvider,
			replay: func(callCtx context.Context, _ string, _ *state.DB,
				cctx CaptureContext, opts ReplayOpts,
			) (ReplaySummary, error) {
				call := replayCall{revisionID: runtimeTelemetryFromContext(callCtx).revisionID}
				if opts.PublicationDrain != nil {
					call.drainID = opts.PublicationDrain.ID
				}
				replayCalls = append(replayCalls, call)
				return ReplaySummary{BaseHead: cctx.BaseHead, Skipped: true}, nil
			},
			afterRunLoopWorkDecision: func(_, recoveryFollowup bool) {
				if recoveryFollowup {
					select {
					case recovered <- struct{}{}:
					default:
					}
				}
			},
		})
	}()
	stopped := false
	defer func() {
		if stopped {
			return
		}
		cancel()
		<-done
	}()

	waitForDaemonMode(t, f.db, "running", 2*time.Second)
	alternative := insertPublicationRuntimeRevision(t, f.db, 3,
		"deterministic", "", "")
	request, ok, err := state.RequestConfigActivation(
		ctx, f.db, alternative.ID,
		sql.NullInt64{Int64: frozen.ID, Valid: true})
	if err != nil || !ok {
		t.Fatalf("request alternative revision=(%+v,%t,%v)", request, ok, err)
	}
	select {
	case wakeCh <- struct{}{}:
	default:
	}
	select {
	case <-recovered:
	case err := <-done:
		stopped = true
		t.Fatalf("Run exited before recovery: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not recover the unavailable semantic publication")
	}
	cancel()
	if err := <-done; err != nil {
		stopped = true
		t.Fatal(err)
	}
	stopped = true

	runtimeState, err := state.RuntimeConfigActivationState(ctx, f.db)
	if err != nil {
		t.Fatal(err)
	}
	if !runtimeState.DesiredRevisionID.Valid ||
		!runtimeState.AppliedRevisionID.Valid ||
		!runtimeState.LastKnownGoodRevisionID.Valid ||
		runtimeState.DesiredRevisionID.Int64 != alternative.ID ||
		runtimeState.AppliedRevisionID.Int64 != alternative.ID ||
		runtimeState.LastKnownGoodRevisionID.Int64 != alternative.ID {
		t.Fatalf("runtime state after recovery=%+v want revision %d applied",
			runtimeState, alternative.ID)
	}
	request, err = state.ActivationRequestByID(ctx, f.db, request.ID)
	if err != nil || request.Status != state.ActivationApplied {
		t.Fatalf("alternative activation request=%+v err=%v", request, err)
	}

	completed, err := state.PublicationDrainByID(ctx, f.db, drain.ID)
	if err != nil || completed.Phase != state.PublicationDrainCompleted {
		t.Fatalf("completed drain=%+v err=%v", completed, err)
	}
	var snapshotID int64
	var snapshotOutcome, recoveryRef string
	var firstArchived, lastArchived int64
	var archivedCount int
	if err := f.db.SQL().QueryRowContext(ctx, `
SELECT id,outcome,recovery_ref,first_event_seq,last_event_seq,event_count
FROM recovery_snapshots
WHERE first_event_seq=?`, firstSeq).Scan(
		&snapshotID, &snapshotOutcome, &recoveryRef,
		&firstArchived, &lastArchived, &archivedCount); err != nil {
		t.Fatal(err)
	}
	if snapshotOutcome != state.EventStateRecovered ||
		firstArchived != firstSeq || lastArchived != laterSeq || archivedCount != 2 {
		t.Fatalf("recovery snapshot outcome=%q range=%d..%d count=%d",
			snapshotOutcome, firstArchived, lastArchived, archivedCount)
	}
	members, err := state.RecoverySnapshotEvents(ctx, f.db, snapshotID)
	if err != nil || len(members) != 2 ||
		members[0].EventSeq != firstSeq || members[1].EventSeq != laterSeq {
		t.Fatalf("recovery members=%+v err=%v", members, err)
	}
	if _, err := git.RevParse(ctx, f.dir, recoveryRef); err != nil {
		t.Fatalf("resolve recovery ref %q: %v", recoveryRef, err)
	}
	for _, seq := range []int64{firstSeq, laterSeq} {
		got, _ := readEventState(t, ctx, f.db, seq)
		if got != state.EventStateRecovered {
			t.Fatalf("event %d state=%q want recovered", seq, got)
		}
	}
	for _, call := range replayCalls {
		if call.revisionID == alternative.ID && call.drainID == drain.ID {
			t.Fatalf("replay used alternative revision %d against frozen drain %s",
				call.revisionID, call.drainID)
		}
	}
}
