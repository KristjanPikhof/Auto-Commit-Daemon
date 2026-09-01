package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRecoveryReconciliationEvidenceLimitIsEnforced(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	blob, err := git.HashObjectStdin(ctx, f.dir, []byte("bounded\n"))
	if err != nil {
		t.Fatal(err)
	}
	firstSeq := appendRecoveryEvent(t, ctx, f, f.cctx.BaseHead, state.CaptureOp{
		Op: "create", Path: "first.txt",
		AfterOID:  sql.NullString{String: blob, Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	appendRecoveryEvent(t, ctx, f, f.cctx.BaseHead, state.CaptureOp{
		Op: "create", Path: "second.txt",
		AfterOID:  sql.NullString{String: blob, Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})

	_, err = loadRecoveryChainForReconciliation(ctx, f.db,
		RecoveryReconcileOptions{
			BranchRef:        f.cctx.BranchRef,
			BranchGeneration: f.cctx.BranchGeneration,
			FirstSeq:         firstSeq,
			EvidenceLimit:    1,
		})
	if !errors.Is(err, state.ErrCompletedBranchTransitionProof) {
		t.Fatalf("load recovery chain error=%v, want proof limit", err)
	}
}

func TestRecoverUnavailableSemanticMessageArchivesWholeSuffix(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
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
	// A newer immutable revision remains causally newer even if the wall clock
	// moved backward before its creation or activation.
	alternative := insertPublicationRuntimeRevision(t, f.db, 1.25,
		"deterministic", "", "")
	activatePublicationRuntimeRevision(t, f.db, frozen.ID, sql.NullInt64{})
	strategy, format, _, fingerprint, err :=
		publicationRuntimeRevisionContract(frozen)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := git.RevParse(ctx, f.dir, f.cctx.BaseHead+"^{tree}")
	if err != nil {
		t.Fatal(err)
	}
	worktreeID := checkpointpkg.WorktreeID(f.dir)
	checkpointID := "cp-1788210000000-0123456789abcdef"
	checkpointRef := fmt.Sprintf(
		"refs/acd/checkpoints/v1/%s/%s", worktreeID, checkpointID)
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir},
		"update-ref", checkpointRef, f.cctx.BaseHead); err != nil {
		t.Fatal(err)
	}
	checkpoint := state.Checkpoint{
		ID: checkpointID, OperationID: "op-semantic-message-recovery",
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
		ConfigRevisionID: frozen.ID, Provider: "openai-compat",
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
	activatePublicationRuntimeRevision(t, f.db, alternative.ID, sql.NullInt64{
		Int64: frozen.ID, Valid: true,
	})
	if _, err := f.db.SQL().ExecContext(ctx,
		`UPDATE runtime_config_state SET applied_ts=1 WHERE id=1`); err != nil {
		t.Fatal(err)
	}

	completed, result, err :=
		RecoverUnavailableSemanticMessagePublicationDrain(
			ctx, f.dir, f.gitDir, f.db, blocked, nil, time.Unix(5, 0))
	if err != nil {
		t.Fatal(err)
	}
	if completed == nil || completed.Phase != state.PublicationDrainCompleted ||
		result.Outcome != state.EventStateRecovered || result.EventCount != 2 ||
		result.FirstSeq != firstSeq || result.LastSeq != laterSeq ||
		result.RecoveryRef == "" {
		t.Fatalf("completed=%+v result=%+v", completed, result)
	}
	for _, seq := range []int64{firstSeq, laterSeq} {
		got, _ := readEventState(t, ctx, f.db, seq)
		if got != state.EventStateRecovered {
			t.Fatalf("event %d state=%q want recovered", seq, got)
		}
	}
	if _, err := git.RevParse(ctx, f.dir, result.RecoveryRef); err != nil {
		t.Fatalf("recovery ref %s: %v", result.RecoveryRef, err)
	}
}

func TestAppliedRemoteReplacementIsNotAutomaticRecoveryProof(t *testing.T) {
	ctx := context.Background()
	db, _, drain := openPublicationDrainTestState(t, 1, 1)
	frozen := insertPublicationRuntimeRevision(t, db, 1,
		"openai-compat", "https://frozen.example/v1", "frozen-model")
	replacement := insertPublicationRuntimeRevision(t, db, 20,
		"openai-compat", "https://replacement.example/v1", "replacement-model")
	strategy, format, _, fingerprint, err :=
		publicationRuntimeRevisionContract(frozen)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE publication_drains
SET commit_strategy=?,commit_format=?,config_revision_id=?,
    provider='openai-compat',provider_fingerprint=?
WHERE id=?`, strategy, format, frozen.ID, fingerprint, drain.ID); err != nil {
		t.Fatal(err)
	}
	drain, err = state.PublicationDrainByID(ctx, db, drain.ID)
	if err != nil {
		t.Fatal(err)
	}
	activatePublicationRuntimeRevision(t, db, frozen.ID, sql.NullInt64{})
	activatePublicationRuntimeRevision(t, db, replacement.ID, sql.NullInt64{
		Int64: frozen.ID, Valid: true,
	})

	ready, err := publicationDrainHasAppliedAlternativeRuntime(ctx, db, drain)
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("remote replacement was treated as proven automatic recovery")
	}
}

func insertPublicationRuntimeRevision(
	t *testing.T,
	db *state.DB,
	createdTS float64,
	provider string,
	baseURL string,
	model string,
) state.ConfigRevision {
	t.Helper()
	values := map[string]any{
		config.FieldProvider:            provider,
		config.FieldBaseURL:             baseURL,
		config.FieldModel:               model,
		config.FieldDiffEgress:          provider != "deterministic",
		config.FieldCommitStrategy:      "intent",
		config.FieldCommitFormat:        "imperative",
		config.FieldCommitPreset:        "fast",
		config.FieldIntentVerification:  "structural",
		config.FieldIntentRepairEnabled: false,
		"preset_id":                     "intent.fast",
		"preset_version":                config.PresetCatalogVersion,
		"customized":                    true,
		"confirmations":                 []string{},
	}
	if provider != "deterministic" {
		values["confirmations"] = []string{"diff_egress"}
	}
	body, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := state.InsertConfigRevision(context.Background(), db,
		state.ConfigRevisionInput{
			Snapshot: body, Profile: "default", Scope: "repository",
			CreatedTS: createdTS,
		})
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func activatePublicationRuntimeRevision(
	t *testing.T,
	db *state.DB,
	revisionID int64,
	expected sql.NullInt64,
) {
	t.Helper()
	ctx := context.Background()
	request, ok, err := state.RequestConfigActivation(
		ctx, db, revisionID, expected)
	if err != nil || !ok {
		t.Fatalf("request activation=(%+v,%t,%v)", request, ok, err)
	}
	if ok, err := state.AcknowledgeConfigActivation(
		ctx, db, request.ID, revisionID); err != nil || !ok {
		t.Fatalf("ack activation=(%t,%v)", ok, err)
	}
	if ok, err := state.ApplyConfigActivation(
		ctx, db, request.ID, revisionID); err != nil || !ok {
		t.Fatalf("apply activation=(%t,%v)", ok, err)
	}
}
