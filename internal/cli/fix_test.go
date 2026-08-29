package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const fixCheckpointTestDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestFix_DefaultsToDryRunWhenNoFlagsPassed(t *testing.T) {
	repo, stateDB, _ := makeRegisteredGitRepoStateDB(t)
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}

	var out bytes.Buffer
	if err := runFix(context.Background(), &out, repo, false, false, false, false, true); err != nil {
		t.Fatalf("runFix: %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !plan.DryRun || plan.BackupPath != "" {
		t.Fatalf("default fix was not a pure dry-run: %+v", plan)
	}
	after, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if before != after {
		t.Fatalf("dry-run mutated state.db: before=%s after=%s", before, after)
	}
}

func TestFix_DryRunPlansExactPairWithoutMutation(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	seedPurgeFixtureRows(t, db)
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}

	plan := runFixJSON(t, repo, true, false, false, false)
	action := findFixAction(plan, fixActionReconcileUnpublishedChain)
	if action == nil {
		t.Fatalf("plan lacks exact-pair reconciliation: %+v", plan.Actions)
	}
	if action.BranchRef != "refs/heads/main" || action.BranchGeneration != 1 || action.Seq < 1 {
		t.Fatalf("action lost exact provenance: %+v", *action)
	}
	after, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if before != after {
		t.Fatalf("dry-run mutated state.db: before=%s after=%s", before, after)
	}
}

func TestFix_ReconcilesResolvedPublicationDrain(t *testing.T) {
	ctx := context.Background()
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	drainID, checkpointID := seedResolvedFixPublicationDrain(t, ctx, repo, db)
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatal(err)
	}

	dryRun := runFixJSON(t, repo, true, false, false, false)
	action := findFixAction(dryRun, fixActionCompleteResolvedDrain)
	if action == nil || action.DrainID != drainID ||
		action.CheckpointID != checkpointID || valueOrZero(action.TargetEvents) != 1 ||
		valueOrZero(action.ResolvedEvents) != 1 || action.Applied {
		t.Fatalf("resolved drain dry-run action=%+v", action)
	}
	afterDryRun, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatal(err)
	}
	if before != afterDryRun {
		t.Fatalf("resolved drain dry-run mutated state: before=%s after=%s",
			before, afterDryRun)
	}

	applied := runFixJSON(t, repo, false, true, false, false)
	action = findFixAction(applied, fixActionCompleteResolvedDrain)
	if action == nil || !action.Applied || action.RowsChanged != 1 ||
		applied.RowsChanged != 1 || applied.BackupPath == "" {
		t.Fatalf("resolved drain apply=%+v action=%+v", applied, action)
	}
	drain, err := state.PublicationDrainByID(ctx, db, drainID)
	if err != nil || drain.Phase != state.PublicationDrainCompleted ||
		drain.PublishedEventCount != 1 || drain.LastError != "" {
		t.Fatalf("resolved drain after fix=%+v err=%v", drain, err)
	}
}

func TestFix_DryRunToleratesPreV5DB(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	seedPurgeFixtureRows(t, db)
	if _, err := db.SQL().ExecContext(context.Background(), `DROP TABLE decision_records`); err != nil {
		t.Fatalf("drop decision_records: %v", err)
	}

	plan := runFixJSON(t, repo, true, false, false, false)
	if findFixAction(plan, fixActionReconcileUnpublishedChain) == nil {
		t.Fatalf("pre-v5 plan lacks exact-pair reconciliation: %+v", plan.Actions)
	}
}

func TestFix_ApplyClearsExpiredPauseAndDrainedBackpressure(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	if err := state.MetaSet(ctx, db, daemon.MetaKeyCaptureBackpressurePausedAt, time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)); err != nil {
		t.Fatalf("seed backpressure: %v", err)
	}
	markerPath := filepath.Join(repo, ".git", "acd", "paused")
	expiredAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	if _, err := pausepkg.Write(markerPath, pausepkg.Marker{
		Reason: "old maintenance", SetAt: time.Now().UTC().Add(-time.Hour).Format(time.RFC3339),
		SetBy: "test", ExpiresAt: &expiredAt,
	}, true); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	plan := runFixJSON(t, repo, false, true, false, false)
	if plan.BackupPath == "" || !plan.ManualPauseRemoved {
		t.Fatalf("safe housekeeping did not apply: %+v", plan)
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("manual pause marker still exists: %v", err)
	}
	if _, ok, err := state.MetaGet(ctx, db, daemon.MetaKeyCaptureBackpressurePausedAt); err != nil || ok {
		t.Fatalf("backpressure meta remains: ok=%v err=%v", ok, err)
	}
}

func TestFix_ClearPauseRemovesActiveMarkerOnlyWhenRequested(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	markerPath := filepath.Join(repo, ".git", "acd", "paused")
	if _, err := pausepkg.Write(markerPath, pausepkg.Marker{
		Reason: "maintenance", SetAt: time.Now().UTC().Format(time.RFC3339), SetBy: "test",
	}, true); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	plan := runFixJSON(t, repo, false, true, false, true)
	if !plan.ManualPauseRemoved {
		t.Fatalf("--clear-pause did not remove marker: %+v", plan)
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("manual pause marker still exists: %v", err)
	}
}

func TestClearPublishBarrierClearsReplayErrorMetadata(t *testing.T) {
	_, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	for _, key := range []string{
		"last_replay_conflict",
		"last_replay_conflict_legacy",
		"last_replay_error",
		"replay.error_repeat_count",
		"replay.error_last_seen_ts",
	} {
		if err := state.MetaSet(ctx, db, key, "stale"); err != nil {
			t.Fatalf("seed replay metadata %s: %v", key, err)
		}
	}
	if err := state.SavePublishState(ctx, db, state.Publish{
		Status: "blocked_conflict",
		Error:  sql.NullString{String: "stale replay failure", Valid: true},
	}); err != nil {
		t.Fatal(err)
	}

	tx, err := db.SQL().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := clearPublishBarrierIfSafe(ctx, tx, nowFloat()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	publish, ok, err := state.LoadPublishState(ctx, db)
	if err != nil || !ok || publish.Status != "ok" || publish.Error.Valid {
		t.Fatalf("publish state=%+v ok=%v err=%v", publish, ok, err)
	}
	for _, key := range []string{
		"last_replay_conflict",
		"last_replay_conflict_legacy",
		"last_replay_error",
		"replay.error_repeat_count",
		"replay.error_last_seen_ts",
	} {
		if value, ok, err := state.MetaGet(ctx, db, key); err != nil || ok {
			t.Fatalf("replay metadata %s=(%q,%v,%v), want absent",
				key, value, ok, err)
		}
	}
}

func TestFix_CommitFailureDoesNotReportTransactionalActionApplied(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	if err := state.MetaSet(ctx, db, daemon.MetaKeyCaptureBackpressurePausedAt,
		time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)); err != nil {
		t.Fatalf("seed backpressure: %v", err)
	}
	plan, err := buildFixPlan(ctx, repo, stateDB, false, false, false)
	if err != nil {
		t.Fatalf("buildFixPlan: %v", err)
	}
	injected := errors.New("injected transaction commit failure")
	plan.commitTransaction = func(*sql.Tx) error { return injected }

	err = applyFixPlan(ctx, stateDB, &plan)
	if !errors.Is(err, injected) {
		t.Fatalf("applyFixPlan err=%v want injected commit failure", err)
	}
	markFixIncomplete(&plan, err)
	action := findFixAction(plan, fixActionClearDrainedBackpressure)
	if action == nil || action.Applied || action.RowsChanged != 0 || plan.RowsChanged != 0 {
		t.Fatalf("rolled-back action reported applied: action=%+v plan=%+v", action, plan)
	}
	if _, ok, err := state.MetaGet(ctx, db, daemon.MetaKeyCaptureBackpressurePausedAt); err != nil || !ok {
		t.Fatalf("rollback lost backpressure marker: ok=%v err=%v", ok, err)
	}
	if _, ok, err := state.MetaGet(ctx, db, "capture.backpressure_overridden_at"); err != nil || ok {
		t.Fatalf("rollback retained override marker: ok=%v err=%v", ok, err)
	}

	var out bytes.Buffer
	if err := renderFix(&out, plan, true); err != nil {
		t.Fatalf("render commit failure: %v", err)
	}
	var rendered fixPlan
	if err := json.Unmarshal(out.Bytes(), &rendered); err != nil {
		t.Fatalf("unmarshal commit failure result: %v\n%s", err, out.String())
	}
	renderedAction := findFixAction(rendered, fixActionClearDrainedBackpressure)
	if !rendered.Incomplete || renderedAction == nil || renderedAction.Applied ||
		renderedAction.RowsChanged != 0 || rendered.RowsChanged != 0 {
		t.Fatalf("rendered rollback claimed applied rows: action=%+v plan=%+v", renderedAction, rendered)
	}
}

func TestFix_PreservesAtomicallyReplacedPauseMarker(t *testing.T) {
	repo, stateDB, _ := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	markerPath := filepath.Join(repo, ".git", "acd", "paused")
	original := pausepkg.Marker{
		Reason: "planned maintenance",
		SetAt:  time.Now().UTC().Add(-time.Hour).Format(time.RFC3339),
		SetBy:  "test",
	}
	if _, err := pausepkg.Write(markerPath, original, false); err != nil {
		t.Fatalf("write original marker: %v", err)
	}
	plan, err := buildFixPlan(ctx, repo, stateDB, false, false, true)
	if err != nil {
		t.Fatalf("buildFixPlan: %v", err)
	}
	plannedInfo := plan.plannedPauseInfo
	var replaceErr error
	plan.beforePauseRemove = func() {
		_, replaceErr = pausepkg.Write(markerPath, original, true)
	}

	err = applyFixPlan(ctx, stateDB, &plan)
	if replaceErr != nil {
		t.Fatalf("replace pause marker: %v", replaceErr)
	}
	if err == nil || !strings.Contains(err.Error(), "changed since planning") {
		t.Fatalf("applyFixPlan err=%v want changed-marker refusal", err)
	}
	current, ok, readErr := pausepkg.Read(filepath.Join(repo, ".git"))
	if readErr != nil || !ok || !samePauseMarker(original, current) {
		t.Fatalf("replacement marker not preserved: marker=%+v ok=%v err=%v", current, ok, readErr)
	}
	currentInfo, statErr := os.Lstat(markerPath)
	if statErr != nil {
		t.Fatalf("stat replacement marker: %v", statErr)
	}
	if os.SameFile(plannedInfo, currentInfo) {
		t.Fatal("fixture did not atomically replace the planned marker inode")
	}
	action := findFixAction(plan, fixActionClearExpiredManualPause)
	if plan.ManualPauseRemoved || action == nil || action.Applied {
		t.Fatalf("replacement marker reported removed: action=%+v plan=%+v", action, plan)
	}
}

func TestFix_PreservesPauseCreatedAfterQuarantineRename(t *testing.T) {
	repo, stateDB, _ := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	markerPath := filepath.Join(repo, ".git", "acd", "paused")
	original := pausepkg.Marker{
		Reason: "planned maintenance",
		SetAt:  time.Now().UTC().Add(-time.Hour).Format(time.RFC3339),
		SetBy:  "test",
	}
	if _, err := pausepkg.Write(markerPath, original, false); err != nil {
		t.Fatalf("write original marker: %v", err)
	}
	plan, err := buildFixPlan(ctx, repo, stateDB, false, false, true)
	if err != nil {
		t.Fatalf("buildFixPlan: %v", err)
	}
	replacement := pausepkg.Marker{
		Reason: "new operator pause",
		SetAt:  time.Now().UTC().Format(time.RFC3339),
		SetBy:  "replacement-test",
	}
	var quarantinePath string
	var replaceErr error
	plan.afterPauseQuarantine = func(path string) {
		quarantinePath = path
		_, replaceErr = pausepkg.Write(markerPath, replacement, false)
	}

	err = applyFixPlan(ctx, stateDB, &plan)
	if replaceErr != nil {
		t.Fatalf("create replacement pause marker: %v", replaceErr)
	}
	if err == nil || !strings.Contains(err.Error(), "new marker was created and preserved") {
		t.Fatalf("applyFixPlan err=%v want new-marker preservation signal", err)
	}
	current, ok, readErr := pausepkg.Read(filepath.Join(repo, ".git"))
	if readErr != nil || !ok || !samePauseMarker(replacement, current) {
		t.Fatalf("new marker did not survive quarantine removal: marker=%+v ok=%v err=%v", current, ok, readErr)
	}
	if quarantinePath == "" {
		t.Fatal("quarantine hook did not run")
	}
	if _, statErr := os.Stat(quarantinePath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("matched planned marker remained quarantined: %v", statErr)
	}
	action := findFixAction(plan, fixActionClearExpiredManualPause)
	if plan.ManualPauseRemoved || action == nil || action.Applied {
		t.Fatalf("new pause marker reported removed: action=%+v plan=%+v", action, plan)
	}
}

func TestFix_ApplyPublishesWholePairAgainstHEAD(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	parent, head, beforeOID, afterOID := commitExternalSeedChange(t, ctx, repo)
	seq := appendFixEvent(t, ctx, db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: parent,
		Operation: "modify", Path: "seed.txt", Fidelity: "exact",
		State: state.EventStateBlockedConflict,
		Error: sql.NullString{String: "modify before-state mismatch", Valid: true},
	}, []state.CaptureOp{{
		Op: "modify", Path: "seed.txt", Fidelity: "exact",
		BeforeOID:  sql.NullString{String: beforeOID, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: afterOID, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
	}})

	plan := runFixJSON(t, repo, false, true, false, false)
	action := findFixAction(plan, fixActionReconcileUnpublishedChain)
	if action == nil || !action.Applied || action.State != state.EventStatePublished || action.RecoveryRef == "" {
		t.Fatalf("published reconciliation missing proof: %+v actions=%+v", action, plan.Actions)
	}
	var gotState, gotCommit, branchRef string
	var generation int64
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT state, commit_oid, branch_ref, branch_generation FROM capture_events WHERE seq = ?`, seq,
	).Scan(&gotState, &gotCommit, &branchRef, &generation); err != nil {
		t.Fatalf("query event: %v", err)
	}
	if gotState != state.EventStatePublished || gotCommit != head || branchRef != "refs/heads/main" || generation != 1 {
		t.Fatalf("event=%s commit=%s pair=%s/g%d want published %s original pair", gotState, gotCommit, branchRef, generation, head)
	}
}

func TestFix_ForceArchivesWholePairWithoutPeelingBarrier(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)

	plan := runFixJSON(t, repo, false, true, true, false)
	action := findFixAction(plan, fixActionReconcileUnpublishedChain)
	if action == nil || !action.ArchiveOnly || action.State != state.EventStateRecovered || action.RowsChanged != 2 || action.RecoveryRef == "" {
		t.Fatalf("archive-only whole-pair action=%+v plan=%+v", action, plan)
	}
	var firstState, secondState, firstCommit, secondCommit string
	if err := db.SQL().QueryRowContext(ctx, `SELECT state, commit_oid FROM capture_events WHERE seq = ?`, first).Scan(&firstState, &firstCommit); err != nil {
		t.Fatalf("query first: %v", err)
	}
	if err := db.SQL().QueryRowContext(ctx, `SELECT state, commit_oid FROM capture_events WHERE seq = ?`, second).Scan(&secondState, &secondCommit); err != nil {
		t.Fatalf("query second: %v", err)
	}
	if firstState != state.EventStateRecovered || secondState != state.EventStateRecovered || firstCommit != secondCommit {
		t.Fatalf("chain peeled or split: first=%s/%s second=%s/%s", firstState, firstCommit, secondState, secondCommit)
	}
	if got := countRowsWhere(t, db, "capture_events", "seq IN (?, ?)", first, second); got != 2 {
		t.Fatalf("reconciliation deleted capture rows: %d", got)
	}
	if plan.BackupPath == "" {
		t.Fatal("archive-only recovery did not back up state.db")
	}
}

func TestFix_ForceRecoversUnreconstructibleDrainAndRecaptures(t *testing.T) {
	for _, reason := range []string{
		"publication_drain_runtime_contract_unavailable",
		"publication_drain_environment_runtime_changed",
	} {
		t.Run(reason, func(t *testing.T) {
			ctx := context.Background()
			repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
			head, err := git.RevParse(ctx, repo, "HEAD")
			if err != nil {
				t.Fatal(err)
			}

			const legacyPath = "legacy-runtime.txt"
			if err := os.WriteFile(filepath.Join(repo, legacyPath),
				[]byte("protected legacy work\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			legacyBlob, err := git.HashObjectStdin(
				ctx, repo, []byte("protected legacy work\n"))
			if err != nil {
				t.Fatal(err)
			}
			seq := appendFixEvent(t, ctx, db, state.CaptureEvent{
				BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
				Operation: "create", Path: legacyPath, Fidelity: "exact",
				State: state.EventStatePending,
			}, []state.CaptureOp{{
				Op: "create", Path: legacyPath, Fidelity: "exact",
				AfterOID:  sql.NullString{String: legacyBlob, Valid: true},
				AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
			}})
			checkpoint := state.Checkpoint{
				ID:               "cp-1787525400000-0123456789abcdef",
				OperationID:      "op-legacy-runtime-recovery",
				WorktreeID:       "0123456789abcdef",
				Reason:           state.CheckpointReasonManualBarrier,
				ObservationEpoch: 1, CoverageEpoch: 1,
				ObservedHead: head, ObservedRef: "refs/heads/main",
				TreeOID: head, CommitOID: head,
				Ref:       "refs/acd/checkpoints/v1/0123456789abcdef/cp-1787525400000-0123456789abcdef",
				CreatedTS: 1, EventSeqs: []int64{seq},
			}
			if created, err := state.PrepareCheckpoint(
				ctx, db, checkpoint, fixCheckpointTestDigest); err != nil || !created {
				t.Fatalf("prepare checkpoint=(%t,%v)", created, err)
			}
			if err := state.CompleteCheckpoint(
				ctx, db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 2); err != nil {
				t.Fatal(err)
			}
			const drainID = "drain-cp-1787525400000-0123456789abcdef"
			if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO publication_drains(
 id,checkpoint_id,worktree_id,branch_ref,branch_generation,phase,
 target_event_count,created_ts,updated_ts,last_progress_ts
) VALUES(?,?,?,?,1,'semantic',1,3,3,3)`,
				drainID, checkpoint.ID, checkpoint.WorktreeID,
				checkpoint.ObservedRef); err != nil {
				t.Fatal(err)
			}
			if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO publication_drain_events(drain_id,ord,event_seq) VALUES(?,0,?)`,
				drainID, seq); err != nil {
				t.Fatal(err)
			}
			if _, err := db.SQL().ExecContext(ctx, `PRAGMA user_version=24`); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			preMigrationSHA, err := fileSHA256(stateDB)
			if err != nil {
				t.Fatal(err)
			}
			preMigrationPlan := runFixJSON(t, repo, true, false, false, false)
			if !preMigrationPlan.ForceRequired || len(preMigrationPlan.Unsafe) == 0 ||
				findFixAction(preMigrationPlan, fixActionReconcileUnpublishedChain) != nil {
				t.Fatalf("pre-v25 recovery plan=%+v", preMigrationPlan)
			}
			postPlanSHA, err := fileSHA256(stateDB)
			if err != nil || postPlanSHA != preMigrationSHA {
				t.Fatalf("pre-v25 dry-run mutated DB: before=%s after=%s err=%v",
					preMigrationSHA, postPlanSHA, err)
			}
			if version, err := state.ReadUserVersion(ctx, stateDB); err != nil || version != 24 {
				t.Fatalf("pre-v25 dry-run schema=%d err=%v", version, err)
			}

			migrated, err := state.OpenRuntime(ctx, stateDB)
			if err != nil {
				t.Fatalf("migrate v24 drain: %v", err)
			}
			legacy, err := state.PublicationDrainByID(ctx, migrated, drainID)
			if err != nil || legacy.CommitStrategy != "" ||
				legacy.CommitFormat != "" || legacy.Provider != "" {
				t.Fatalf("unproved migration contract=%+v err=%v", legacy, err)
			}
			if reason == "publication_drain_environment_runtime_changed" {
				if _, err := migrated.SQL().ExecContext(ctx, `
UPDATE publication_drains
SET commit_strategy='intent',commit_format='imperative',
    provider='openai-compat',provider_model='legacy-model'
WHERE id=?`, drainID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := migrated.SQL().ExecContext(ctx, `
UPDATE publication_drains SET phase='needs_action',last_error=? WHERE id=?`,
				reason, drainID); err != nil {
				t.Fatal(err)
			}
			if err := migrated.Close(); err != nil {
				t.Fatal(err)
			}

			const stagedPath = "staged-runtime.txt"
			if err := os.WriteFile(filepath.Join(repo, stagedPath),
				[]byte("staged version\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "add", stagedPath); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(repo, stagedPath),
				[]byte("worktree version\n"), 0o644); err != nil {
				t.Fatal(err)
			}

			type gitSnapshot struct {
				head, status, cachedDiff, worktreeDiff string
				indexSHA, legacySHA, stagedSHA         string
			}
			snapshot := func() gitSnapshot {
				t.Helper()
				gotHead, err := git.RevParse(ctx, repo, "HEAD")
				if err != nil {
					t.Fatal(err)
				}
				statusBody, err := git.Run(ctx, git.RunOpts{Dir: repo},
					"status", "--porcelain=v1", "--untracked-files=all")
				if err != nil {
					t.Fatal(err)
				}
				cached, err := git.Run(ctx, git.RunOpts{Dir: repo},
					"diff", "--cached", "--binary", "--no-ext-diff")
				if err != nil {
					t.Fatal(err)
				}
				worktree, err := git.Run(ctx, git.RunOpts{Dir: repo},
					"diff", "--binary", "--no-ext-diff")
				if err != nil {
					t.Fatal(err)
				}
				indexSHA, err := fileSHA256(filepath.Join(repo, ".git", "index"))
				if err != nil {
					t.Fatal(err)
				}
				legacySHA, err := fileSHA256(filepath.Join(repo, legacyPath))
				if err != nil {
					t.Fatal(err)
				}
				stagedSHA, err := fileSHA256(filepath.Join(repo, stagedPath))
				if err != nil {
					t.Fatal(err)
				}
				return gitSnapshot{
					head: gotHead, status: string(statusBody),
					cachedDiff: string(cached), worktreeDiff: string(worktree),
					indexSHA: indexSHA, legacySHA: legacySHA, stagedSHA: stagedSHA,
				}
			}
			beforeGit := snapshot()
			beforeDB, err := fileSHA256(stateDB)
			if err != nil {
				t.Fatal(err)
			}

			withoutForce := runFixJSON(t, repo, true, false, false, false)
			if !withoutForce.ForceRequired || len(withoutForce.Unsafe) == 0 ||
				findFixAction(withoutForce, fixActionReconcileUnpublishedChain) != nil {
				t.Fatalf("non-force plan=%+v", withoutForce)
			}
			forceDryRun := runFixJSON(t, repo, true, false, true, false)
			action := findFixAction(forceDryRun, fixActionReconcileUnpublishedChain)
			if action == nil || !action.ArchiveOnly || !action.RequiresForce || action.Applied {
				t.Fatalf("force dry-run action=%+v plan=%+v", action, forceDryRun)
			}
			afterDryRunDB, err := fileSHA256(stateDB)
			if err != nil || afterDryRunDB != beforeDB || snapshot() != beforeGit {
				t.Fatalf("dry-run mutated state: db=%s/%s err=%v git=%+v/%+v",
					beforeDB, afterDryRunDB, err, beforeGit, snapshot())
			}

			applied := runFixJSON(t, repo, false, true, true, false)
			action = findFixAction(applied, fixActionReconcileUnpublishedChain)
			if action == nil || !action.Applied || action.State != state.EventStateRecovered ||
				action.RecoveryRef == "" || action.RowsChanged != 1 {
				t.Fatalf("applied recovery action=%+v plan=%+v", action, applied)
			}
			if got := snapshot(); got != beforeGit {
				t.Fatalf("recovery changed HEAD/index/worktree: before=%+v after=%+v",
					beforeGit, got)
			}
			if recoveredCommit, err := git.RevParse(ctx, repo, action.RecoveryRef); err != nil || recoveredCommit != action.CommitOID {
				t.Fatalf("recovery ref=%s commit=%s err=%v want %s",
					action.RecoveryRef, recoveredCommit, err, action.CommitOID)
			}

			live, err := state.Open(ctx, stateDB)
			if err != nil {
				t.Fatal(err)
			}
			defer live.Close()
			assertFixEventState(t, ctx, live, seq, state.EventStateRecovered)
			drain, err := state.PublicationDrainByID(ctx, live, drainID)
			if err != nil || drain.Phase != state.PublicationDrainCompleted ||
				drain.PublishedEventCount != 1 || drain.LastError != "" {
				t.Fatalf("settled drain=%+v err=%v", drain, err)
			}
			barrier, err := daemon.PublicationDrainBarrierForPair(
				ctx, live, "refs/heads/main", 1)
			if err != nil || barrier != nil {
				t.Fatalf("publication barrier after recovery=%+v err=%v", barrier, err)
			}
			if bootstrapped, err := daemon.IsShadowBootstrapped(
				ctx, live, "refs/heads/main", 1); err != nil || bootstrapped {
				t.Fatalf("shadow after archive=%t err=%v", bootstrapped, err)
			}

			cctx := daemon.CaptureContext{
				BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
			}
			if _, err := daemon.BootstrapShadow(ctx, repo, live, cctx); err != nil {
				t.Fatal(err)
			}
			checker := git.NewIgnoreChecker(repo)
			defer checker.Close()
			if _, err := daemon.Capture(ctx, repo, live, cctx, daemon.CaptureOpts{
				IgnoreChecker: checker, SensitiveMatcher: state.NewSensitiveMatcher(),
			}); err != nil {
				t.Fatalf("recapture dirty work: %v", err)
			}
			var recapturedSeq int64
			if err := live.SQL().QueryRowContext(ctx, `
SELECT seq FROM capture_events
WHERE seq>? AND branch_ref='refs/heads/main' AND branch_generation=1
  AND path=? AND state='pending'
ORDER BY seq DESC LIMIT 1`, seq, legacyPath).Scan(&recapturedSeq); err != nil {
				t.Fatalf("find recaptured legacy work: %v", err)
			}
			if recapturedSeq == seq {
				t.Fatalf("legacy event was not recaptured: old=%d new=%d", seq, recapturedSeq)
			}
		})
	}
}

func TestFix_BackupIncludesWALOnlyCommittedRows(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	if _, err := db.SQL().ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("checkpoint baseline: %v", err)
	}
	reader, err := db.ReadSQL().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin snapshot reader: %v", err)
	}
	defer reader.Rollback()
	var baseline int
	if err := reader.QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events`).Scan(&baseline); err != nil {
		t.Fatalf("establish snapshot reader: %v", err)
	}

	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	walInfo, err := os.Stat(stateDB + "-wal")
	if err != nil || walInfo.Size() == 0 {
		t.Fatalf("fixture did not retain committed WAL frames: info=%v err=%v", walInfo, err)
	}

	// A raw main-file copy must not contain the pair, proving the committed rows
	// are visible only through the live WAL snapshot at backup time.
	rawMain := filepath.Join(t.TempDir(), "raw-main.db")
	rawBytes, err := os.ReadFile(stateDB)
	if err != nil {
		t.Fatalf("read raw main DB: %v", err)
	}
	if err := os.WriteFile(rawMain, rawBytes, 0o600); err != nil {
		t.Fatalf("write raw main DB copy: %v", err)
	}
	rawConn, err := sql.Open("sqlite", "file:"+rawMain+"?immutable=1")
	if err != nil {
		t.Fatalf("open raw main DB: %v", err)
	}
	defer rawConn.Close()
	var rawRows int
	if err := rawConn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE seq IN (?, ?)`, first, second,
	).Scan(&rawRows); err != nil {
		t.Fatalf("query raw main DB: %v", err)
	}
	if rawRows != 0 {
		t.Fatalf("fixture rows unexpectedly checkpointed into main DB: %d", rawRows)
	}

	plan := runFixJSON(t, repo, false, true, true, false)
	if plan.BackupPath == "" {
		t.Fatal("fix did not report a backup path")
	}
	backup, err := openStateDBReadOnly(ctx, plan.BackupPath)
	if err != nil {
		t.Fatalf("open WAL-safe backup: %v", err)
	}
	defer backup.Close()
	var backedUpRows int
	if err := backup.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE seq IN (?, ?)`, first, second,
	).Scan(&backedUpRows); err != nil {
		t.Fatalf("query WAL-safe backup: %v", err)
	}
	if backedUpRows != 2 {
		t.Fatalf("WAL-safe backup rows=%d want 2", backedUpRows)
	}
	var integrity string
	if err := backup.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&integrity); err != nil {
		t.Fatalf("quick_check backup: %v", err)
	}
	if integrity != "ok" {
		t.Fatalf("backup quick_check=%q want ok", integrity)
	}
}

func TestFix_BackupPrecedesSchemaMigration(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	if err := state.MetaSet(ctx, db, daemon.MetaKeyCaptureBackpressurePausedAt,
		time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)); err != nil {
		t.Fatalf("seed backpressure: %v", err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
DROP TABLE recovery_snapshot_events;
DROP TABLE recovery_snapshots;
PRAGMA user_version = 11;`); err != nil {
		t.Fatalf("downgrade recovery schema: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close downgraded DB: %v", err)
	}

	plan := runFixJSON(t, repo, false, true, false, false)
	if plan.BackupPath == "" {
		t.Fatal("fix did not report a backup path")
	}
	backup, err := openStateDBReadOnly(ctx, plan.BackupPath)
	if err != nil {
		t.Fatalf("open pre-migration backup: %v", err)
	}
	defer backup.Close()
	var backupVersion, backupRecoveryTables int
	if err := backup.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&backupVersion); err != nil {
		t.Fatalf("read backup user_version: %v", err)
	}
	if err := backup.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sqlite_master
WHERE type = 'table' AND name IN ('recovery_snapshots', 'recovery_snapshot_events')`).Scan(&backupRecoveryTables); err != nil {
		t.Fatalf("count backup recovery tables: %v", err)
	}
	if backupVersion != 11 || backupRecoveryTables != 0 {
		t.Fatalf("backup captured post-migration schema: version=%d tables=%d", backupVersion, backupRecoveryTables)
	}

	live, err := openStateDBReadOnly(ctx, stateDB)
	if err != nil {
		t.Fatalf("open migrated live DB: %v", err)
	}
	defer live.Close()
	var liveVersion, liveRecoveryTables int
	if err := live.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&liveVersion); err != nil {
		t.Fatalf("read live user_version: %v", err)
	}
	if err := live.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sqlite_master
WHERE type = 'table' AND name IN ('recovery_snapshots', 'recovery_snapshot_events')`).Scan(&liveRecoveryTables); err != nil {
		t.Fatalf("count live recovery tables: %v", err)
	}
	if liveVersion != state.SchemaVersion || liveRecoveryTables != 2 {
		t.Fatalf("live schema not migrated: version=%d tables=%d", liveVersion, liveRecoveryTables)
	}
}

func TestFix_ReconcileIsIdempotent(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)

	firstPlan := runFixJSON(t, repo, false, true, false, false)
	firstAction := findFixAction(firstPlan, fixActionReconcileUnpublishedChain)
	if firstAction == nil || !firstAction.Applied || firstAction.RecoveryRef == "" {
		t.Fatalf("first reconciliation=%+v", firstAction)
	}
	if got := countRowsWhere(t, db, "recovery_snapshots", "1 = 1"); got != 1 {
		t.Fatalf("snapshots after first run=%d want 1", got)
	}

	secondPlan := runFixJSON(t, repo, false, true, false, false)
	if action := findFixAction(secondPlan, fixActionReconcileUnpublishedChain); action != nil {
		t.Fatalf("idempotent rerun planned reconciliation: %+v", *action)
	}
	if got := countRowsWhere(t, db, "recovery_snapshots", "1 = 1"); got != 1 {
		t.Fatalf("idempotent rerun created snapshot: %d", got)
	}
	assertFixEventState(t, ctx, db, first, state.EventStateRecovered)
	assertFixEventState(t, ctx, db, second, state.EventStateRecovered)
}

func TestFix_HeadChangeBetweenPlanAndApplyFailsClosed(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	plan, err := buildFixPlan(ctx, repo, stateDB, false, false, false)
	if err != nil {
		t.Fatalf("buildFixPlan: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repo, "advance-head.txt"), []byte("advance\n"), 0o644); err != nil {
		t.Fatalf("write advance file: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "add", "advance-head.txt"); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo},
		"-c", "user.name=ACD Test", "-c", "user.email=acd@example.invalid",
		"commit", "-m", "advance head"); err != nil {
		t.Fatalf("git commit: %v", err)
	}

	err = applyFixPlan(ctx, stateDB, &plan)
	if err == nil || !strings.Contains(err.Error(), "HEAD changed during planning") {
		t.Fatalf("applyFixPlan err=%v want HEAD race refusal", err)
	}
	assertFixEventState(t, ctx, db, first, state.EventStateBlockedConflict)
	assertFixEventState(t, ctx, db, second, state.EventStatePending)
	if got := countRowsWhere(t, db, "recovery_snapshots", "1 = 1"); got != 0 {
		t.Fatalf("HEAD race wrote recovery snapshot: %d", got)
	}
}

func TestFix_GitOperationAppearsBetweenPlanAndApplyFailsClosed(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	plan, err := buildFixPlan(ctx, repo, stateDB, false, false, false)
	if err != nil {
		t.Fatalf("buildFixPlan: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repo, ".git", "MERGE_HEAD"), []byte(strings.Repeat("0", 40)+"\n"), 0o600); err != nil {
		t.Fatalf("write MERGE_HEAD: %v", err)
	}

	err = applyFixPlan(ctx, stateDB, &plan)
	if err == nil || !strings.Contains(err.Error(), "Git operation merge") {
		t.Fatalf("applyFixPlan err=%v want merge-operation refusal", err)
	}
	if plan.BackupPath != "" {
		t.Fatalf("pre-backup Git-operation refusal created backup: %s", plan.BackupPath)
	}
	assertFixEventState(t, ctx, db, first, state.EventStateBlockedConflict)
	assertFixEventState(t, ctx, db, second, state.EventStatePending)
	if got := countRowsWhere(t, db, "recovery_snapshots", "1 = 1"); got != 0 {
		t.Fatalf("Git-operation race wrote recovery snapshot: %d", got)
	}
}

func TestFix_GitOperationAppearsAfterBackupFailsClosed(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	plan, err := buildFixPlan(ctx, repo, stateDB, false, false, false)
	if err != nil {
		t.Fatalf("buildFixPlan: %v", err)
	}
	var markerErr error
	plan.afterBackup = func() {
		markerErr = os.Mkdir(filepath.Join(repo, ".git", "rebase-merge"), 0o700)
	}

	err = applyFixPlan(ctx, stateDB, &plan)
	if markerErr != nil {
		t.Fatalf("create rebase-merge marker: %v", markerErr)
	}
	if err == nil || !strings.Contains(err.Error(), "Git operation rebase-merge") {
		t.Fatalf("applyFixPlan err=%v want post-backup rebase refusal", err)
	}
	if plan.BackupPath == "" {
		t.Fatal("post-backup Git-operation fixture did not reach backup boundary")
	}
	assertFixEventState(t, ctx, db, first, state.EventStateBlockedConflict)
	assertFixEventState(t, ctx, db, second, state.EventStatePending)
	if got := countRowsWhere(t, db, "recovery_snapshots", "1 = 1"); got != 0 {
		t.Fatalf("post-backup Git-operation race wrote recovery snapshot: %d", got)
	}
}

func TestFix_PauseAppearsBetweenPlanAndApplyFailsClosed(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	plan, err := buildFixPlan(ctx, repo, stateDB, false, false, false)
	if err != nil {
		t.Fatalf("buildFixPlan: %v", err)
	}
	markerPath := pausepkg.Path(filepath.Join(repo, ".git"))
	if _, err := pausepkg.Write(markerPath, pausepkg.Marker{
		Reason: "new maintenance", SetAt: time.Now().UTC().Format(time.RFC3339), SetBy: "test",
	}, false); err != nil {
		t.Fatalf("write new pause marker: %v", err)
	}

	err = applyFixPlan(ctx, stateDB, &plan)
	if err == nil || !strings.Contains(err.Error(), "pause marker appeared since planning") {
		t.Fatalf("applyFixPlan err=%v want new-pause refusal", err)
	}
	if plan.BackupPath != "" {
		t.Fatalf("pre-backup pause refusal created backup: %s", plan.BackupPath)
	}
	assertFixEventState(t, ctx, db, first, state.EventStateBlockedConflict)
	assertFixEventState(t, ctx, db, second, state.EventStatePending)
}

func TestFix_StableManualPauseAllowsRecovery(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	markerPath := pausepkg.Path(filepath.Join(repo, ".git"))
	marker := pausepkg.Marker{
		Reason: "stable maintenance", SetAt: time.Now().UTC().Format(time.RFC3339), SetBy: "test",
	}
	if _, err := pausepkg.Write(markerPath, marker, false); err != nil {
		t.Fatalf("write stable pause marker: %v", err)
	}
	plan, err := buildFixPlan(ctx, repo, stateDB, false, false, false)
	if err != nil {
		t.Fatalf("buildFixPlan: %v", err)
	}

	if err := applyFixPlan(ctx, stateDB, &plan); err != nil {
		t.Fatalf("applyFixPlan with stable pause: %v", err)
	}
	assertFixEventState(t, ctx, db, first, state.EventStateRecovered)
	assertFixEventState(t, ctx, db, second, state.EventStateRecovered)
	current, ok, err := pausepkg.Read(filepath.Join(repo, ".git"))
	if err != nil || !ok || !samePauseMarker(marker, current) {
		t.Fatalf("stable pause was not preserved: marker=%+v ok=%v err=%v", current, ok, err)
	}
}

func TestFix_PlannedPauseDisappearsAfterBackupFailsClosed(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	markerPath := pausepkg.Path(filepath.Join(repo, ".git"))
	if _, err := pausepkg.Write(markerPath, pausepkg.Marker{
		Reason: "planned maintenance", SetAt: time.Now().UTC().Format(time.RFC3339), SetBy: "test",
	}, false); err != nil {
		t.Fatalf("write planned pause marker: %v", err)
	}
	plan, err := buildFixPlan(ctx, repo, stateDB, false, false, false)
	if err != nil {
		t.Fatalf("buildFixPlan: %v", err)
	}
	var removeErr error
	plan.afterBackup = func() { removeErr = os.Remove(markerPath) }

	err = applyFixPlan(ctx, stateDB, &plan)
	if removeErr != nil {
		t.Fatalf("remove planned pause marker: %v", removeErr)
	}
	if err == nil || !strings.Contains(err.Error(), "pause marker disappeared since planning") {
		t.Fatalf("applyFixPlan err=%v want disappeared-pause refusal", err)
	}
	if plan.BackupPath == "" {
		t.Fatal("disappeared-pause fixture did not reach backup boundary")
	}
	assertFixEventState(t, ctx, db, first, state.EventStateBlockedConflict)
	assertFixEventState(t, ctx, db, second, state.EventStatePending)
	if got := countRowsWhere(t, db, "recovery_snapshots", "1 = 1"); got != 0 {
		t.Fatalf("pause-state race wrote recovery snapshot: %d", got)
	}
}

func TestFix_FirstPairFailureDoesNotMutateOtherPair(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	bad := appendFixEvent(t, ctx, db, state.CaptureEvent{
		BranchRef: "refs/heads/a-bad", BranchGeneration: 2, BaseHead: head,
		Operation: "create", Path: "bad.txt", Fidelity: "exact",
		State: state.EventStateFailed, Error: sql.NullString{String: "missing object", Valid: true},
	}, []state.CaptureOp{{
		Op: "create", Path: "bad.txt", Fidelity: "exact",
		AfterOID:  sql.NullString{String: strings.Repeat("f", 40), Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	}})
	goodFirst, goodSecond := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/z-good", 3)

	var out bytes.Buffer
	err = runFix(ctx, &out, repo, false, true, false, false, true)
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("runFix err=%v want first-pair failure\n%s", err, out.String())
	}
	assertFixEventState(t, ctx, db, bad, state.EventStateFailed)
	assertFixEventState(t, ctx, db, goodFirst, state.EventStateBlockedConflict)
	assertFixEventState(t, ctx, db, goodSecond, state.EventStatePending)
	if got := countRowsWhere(t, db, "recovery_snapshots", "1 = 1"); got != 0 {
		t.Fatalf("cross-pair failure wrote snapshots: %d", got)
	}
}

func TestFix_SecondPairFailureReportsIncompleteApply(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	goodFirst, goodSecond := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/a-good", 2)
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	bad := appendFixEvent(t, ctx, db, state.CaptureEvent{
		BranchRef: "refs/heads/z-bad", BranchGeneration: 3, BaseHead: head,
		Operation: "create", Path: "missing.txt", Fidelity: "exact",
		State: state.EventStateFailed, Error: sql.NullString{String: "missing object", Valid: true},
	}, []state.CaptureOp{{
		Op: "create", Path: "missing.txt", Fidelity: "exact",
		AfterOID:  sql.NullString{String: strings.Repeat("f", 40), Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	}})

	var out bytes.Buffer
	err = runFix(ctx, &out, repo, false, true, false, false, true)
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("runFix err=%v want second-pair failure\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal incomplete plan: %v\n%s", err, out.String())
	}
	if !plan.Incomplete || plan.RowsChanged != 2 || len(plan.VerifyErrors) == 0 ||
		!strings.Contains(strings.Join(plan.VerifyErrors, " "), "missing") {
		t.Fatalf("partial failure not reported as incomplete: %+v", plan)
	}
	var goodAction, badAction *fixAction
	for i := range plan.Actions {
		switch plan.Actions[i].BranchRef {
		case "refs/heads/a-good":
			goodAction = &plan.Actions[i]
		case "refs/heads/z-bad":
			badAction = &plan.Actions[i]
		}
	}
	if goodAction == nil || !goodAction.Applied || goodAction.RowsChanged != 2 || goodAction.RecoveryRef == "" {
		t.Fatalf("first pair success not retained in result: %+v", goodAction)
	}
	if badAction == nil || badAction.Applied {
		t.Fatalf("failed second pair reported applied: %+v", badAction)
	}
	assertFixEventState(t, ctx, db, goodFirst, state.EventStateRecovered)
	assertFixEventState(t, ctx, db, goodSecond, state.EventStateRecovered)
	assertFixEventState(t, ctx, db, bad, state.EventStateFailed)

	var human bytes.Buffer
	if err := renderFix(&human, plan, false); err != nil {
		t.Fatalf("render incomplete human result: %v", err)
	}
	if !strings.Contains(human.String(), "Fix incomplete") || !strings.Contains(human.String(), "missing") {
		t.Fatalf("human output hid partial failure:\n%s", human.String())
	}
}

func TestFix_ReconciliationLeavesLiveGitStateUntouched(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	if err := os.WriteFile(filepath.Join(repo, "staged-user.txt"), []byte("staged\n"), 0o644); err != nil {
		t.Fatalf("write staged file: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "add", "staged-user.txt"); err != nil {
		t.Fatalf("git add: %v", err)
	}
	headBefore, _ := git.RevParse(ctx, repo, "HEAD")
	indexBefore, err := git.Run(ctx, git.RunOpts{Dir: repo}, "diff", "--cached", "--binary")
	if err != nil {
		t.Fatalf("index before: %v", err)
	}
	statusBefore, err := git.Run(ctx, git.RunOpts{Dir: repo}, "status", "--porcelain=v1")
	if err != nil {
		t.Fatalf("status before: %v", err)
	}
	worktreeBefore, err := os.ReadFile(filepath.Join(repo, "staged-user.txt"))
	if err != nil {
		t.Fatalf("read worktree before: %v", err)
	}

	runFixJSON(t, repo, false, true, false, false)
	headAfter, _ := git.RevParse(ctx, repo, "HEAD")
	indexAfter, _ := git.Run(ctx, git.RunOpts{Dir: repo}, "diff", "--cached", "--binary")
	statusAfter, _ := git.Run(ctx, git.RunOpts{Dir: repo}, "status", "--porcelain=v1")
	worktreeAfter, _ := os.ReadFile(filepath.Join(repo, "staged-user.txt"))
	if headAfter != headBefore || string(indexAfter) != string(indexBefore) ||
		string(statusAfter) != string(statusBefore) || string(worktreeAfter) != string(worktreeBefore) {
		t.Fatalf("fix mutated live Git state: HEAD %s->%s index_equal=%v status %q->%q worktree_equal=%v",
			headBefore, headAfter, string(indexAfter) == string(indexBefore), statusBefore, statusAfter,
			string(worktreeAfter) == string(worktreeBefore))
	}
}

func TestFix_RecoveredGeneratedDeleteKeepsCaptureOps(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	generatedPath := ".derivedData-overlap/cache.db"
	if err := os.MkdirAll(filepath.Dir(filepath.Join(repo, generatedPath)), 0o755); err != nil {
		t.Fatalf("mkdir generated root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repo, generatedPath), []byte("generated before\n"), 0o644); err != nil {
		t.Fatalf("write generated file: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "add", "-f", generatedPath); err != nil {
		t.Fatalf("git add generated file: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo},
		"-c", "user.name=ACD Test", "-c", "user.email=acd@example.invalid",
		"commit", "-m", "seed generated file"); err != nil {
		t.Fatalf("git commit generated file: %v", err)
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	createdOID, err := git.HashObjectStdin(ctx, repo, []byte("captured trigger\n"))
	if err != nil {
		t.Fatalf("hash trigger: %v", err)
	}
	first := appendFixEvent(t, ctx, db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
		Operation: "create", Path: "trigger.txt", Fidelity: "exact",
		State: state.EventStateBlockedConflict,
		Error: sql.NullString{String: "before-state mismatch", Valid: true},
	}, []state.CaptureOp{{
		Op: "create", Path: "trigger.txt", Fidelity: "exact",
		AfterOID:  sql.NullString{String: createdOID, Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	}})
	generatedOID, err := git.LsTreeBlobOID(ctx, repo, head, generatedPath)
	if err != nil {
		t.Fatalf("ls-tree generated file: %v", err)
	}
	generated := appendFixEvent(t, ctx, db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
		Operation: "delete", Path: generatedPath, Fidelity: "exact",
		State: state.EventStatePending,
	}, []state.CaptureOp{{
		Op: "delete", Path: generatedPath, Fidelity: "exact",
		BeforeOID:  sql.NullString{String: generatedOID, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	}})
	if err := state.RecordPlannerOffer(ctx, db, generated, 123); err != nil {
		t.Fatalf("RecordPlannerOffer: %v", err)
	}

	plan := runFixJSON(t, repo, false, true, false, false)
	generatedAction := findFixAction(plan, fixActionDropGeneratedPending)
	if generatedAction == nil || !generatedAction.Applied || generatedAction.RowsChanged != 0 {
		t.Fatalf("generated cleanup should skip recovered row: %+v", generatedAction)
	}
	assertFixEventState(t, ctx, db, first, state.EventStateRecovered)
	assertFixEventState(t, ctx, db, generated, state.EventStateRecovered)
	if got := countRowsWhere(t, db, "capture_ops", "event_seq = ?", generated); got != 1 {
		t.Fatalf("generated recovery lost capture ops: %d", got)
	}
}

func TestFix_ReconcilePreservesUnrelatedPublishBreadcrumb(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	unrelated := appendFixEvent(t, ctx, db, state.CaptureEvent{
		BranchRef: "refs/heads/other", BranchGeneration: 9, BaseHead: head,
		Operation: "create", Path: "other.txt", Fidelity: "exact", State: state.EventStatePublished,
		CommitOID: sql.NullString{String: head, Valid: true},
	}, []state.CaptureOp{{Op: "create", Path: "other.txt", Fidelity: "exact"}})
	if err := state.SavePublishState(ctx, db, state.Publish{
		EventSeq:         sql.NullInt64{Int64: unrelated, Valid: true},
		BranchRef:        sql.NullString{String: "refs/heads/other", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 9, Valid: true},
		SourceHead:       sql.NullString{String: head, Valid: true},
		Status:           "blocked_conflict", Error: sql.NullString{String: "unrelated", Valid: true},
	}); err != nil {
		t.Fatalf("SavePublishState: %v", err)
	}

	runFixJSON(t, repo, false, true, false, false)
	publish, ok, err := state.LoadPublishState(ctx, db)
	if err != nil || !ok {
		t.Fatalf("LoadPublishState: ok=%v err=%v", ok, err)
	}
	if !publish.EventSeq.Valid || publish.EventSeq.Int64 != unrelated || publish.Status != "blocked_conflict" ||
		!publish.BranchRef.Valid || publish.BranchRef.String != "refs/heads/other" {
		t.Fatalf("unrelated publish breadcrumb changed: %+v", publish)
	}
}

func TestFix_StalePairPreservesOriginalProvenance(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, _ := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/stale", 7)

	plan := runFixJSON(t, repo, false, true, false, false)
	action := findFixAction(plan, fixActionReconcileUnpublishedChain)
	if action == nil || action.BranchRef != "refs/heads/stale" || action.BranchGeneration != 7 {
		t.Fatalf("stale pair not planned exactly: %+v", plan.Actions)
	}
	var branchRef, eventState string
	var generation int64
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT branch_ref, branch_generation, state FROM capture_events WHERE seq = ?`, first,
	).Scan(&branchRef, &generation, &eventState); err != nil {
		t.Fatalf("query stale event: %v", err)
	}
	if branchRef != "refs/heads/stale" || generation != 7 || eventState != state.EventStateRecovered {
		t.Fatalf("stale provenance rewritten: %s/g%d state=%s", branchRef, generation, eventState)
	}
}

func TestFix_ReconcileAcceptsDuplicateIdempotentCapture(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	beforeOID, err := git.LsTreeBlobOID(ctx, repo, head, "seed.txt")
	if err != nil {
		t.Fatal(err)
	}
	afterOID, err := git.HashObjectStdin(ctx, repo, []byte("duplicate captured state\n"))
	if err != nil {
		t.Fatal(err)
	}
	event := state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
		Operation: "modify", Path: "seed.txt", Fidelity: "rescan",
	}
	op := state.CaptureOp{
		Op: "modify", Path: "seed.txt", Fidelity: "rescan",
		BeforeOID: sql.NullString{String: beforeOID, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: afterOID, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	}
	event.State = state.EventStateBlockedConflict
	event.Error = sql.NullString{String: "history moved", Valid: true}
	first := appendFixEvent(t, ctx, db, event, []state.CaptureOp{op})
	event.State = state.EventStatePending
	event.Error = sql.NullString{}
	second := appendFixEvent(t, ctx, db, event, []state.CaptureOp{op})

	plan := runFixJSON(t, repo, false, true, false, false)
	assertFixEventState(t, ctx, db, first, state.EventStateRecovered)
	assertFixEventState(t, ctx, db, second, state.EventStateRecovered)
	action := findFixAction(plan, fixActionReconcileUnpublishedChain)
	if action == nil || action.RecoveryRef == "" || action.RowsChanged != 2 {
		t.Fatalf("duplicate recovery action=%+v", action)
	}
	got, err := git.LsTreeBlobOID(ctx, repo, action.RecoveryRef, "seed.txt")
	if err != nil || got != afterOID {
		t.Fatalf("recovery ref seed oid=%q err=%v want %s", got, err, afterOID)
	}
}

func TestFix_MissingObjectLeavesWholePairUnchanged(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	first := appendFixEvent(t, ctx, db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
		Operation: "create", Path: "missing.txt", Fidelity: "exact",
		State: state.EventStateFailed, Error: sql.NullString{String: "missing object", Valid: true},
	}, []state.CaptureOp{{
		Op: "create", Path: "missing.txt", Fidelity: "exact",
		AfterOID:  sql.NullString{String: strings.Repeat("f", 40), Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	}})
	second := appendFixEvent(t, ctx, db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
		Operation: "create", Path: "successor.txt", Fidelity: "exact", State: state.EventStatePending,
	}, []state.CaptureOp{{
		Op: "create", Path: "successor.txt", Fidelity: "exact",
		AfterOID:  sql.NullString{String: strings.Repeat("e", 40), Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	}})

	var out bytes.Buffer
	err = runFix(ctx, &out, repo, false, true, false, false, true)
	if err == nil || !strings.Contains(err.Error(), "missing blob object") {
		t.Fatalf("runFix err=%v want missing-object refusal\n%s", err, out.String())
	}
	assertFixEventState(t, ctx, db, first, state.EventStateFailed)
	assertFixEventState(t, ctx, db, second, state.EventStatePending)
	if got := countRowsWhere(t, db, "recovery_snapshots", "1 = 1"); got != 0 {
		t.Fatalf("missing-object failure wrote snapshot: %d", got)
	}
}

func TestFix_QuiescesManagedRuntimeWhenDaemonIsLive(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{PID: os.Getpid(), Mode: "running"}); err != nil {
		t.Fatalf("SaveDaemonState: %v", err)
	}
	oldPrepare := prepareFixMutationSupervisor
	oldQuiesce := withQuiescedFixRuntime
	t.Cleanup(func() {
		prepareFixMutationSupervisor = oldPrepare
		withQuiescedFixRuntime = oldQuiesce
	})
	prepareFixMutationSupervisor = func(context.Context, paths.Roots) error { return nil }
	quiesceCalls := 0
	withQuiescedFixRuntime = func(operationCtx context.Context, _ paths.Roots, _ string, operation func(context.Context) error) error {
		quiesceCalls++
		if err := state.SaveDaemonState(operationCtx, db, state.DaemonState{Mode: "stopped"}); err != nil {
			return err
		}
		return operation(operationCtx)
	}

	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, false, true, false, false, true); err != nil {
		t.Fatalf("runFix: %v\n%s", err, out.String())
	}
	if quiesceCalls != 1 {
		t.Fatalf("quiesce calls=%d want 1", quiesceCalls)
	}
	assertFixEventState(t, ctx, db, first, state.EventStateRecovered)
	assertFixEventState(t, ctx, db, second, state.EventStateRecovered)
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("decode plan: %v\n%s", err, out.String())
	}
	if plan.RowsChanged != 2 {
		t.Fatalf("rows changed=%d want 2", plan.RowsChanged)
	}
}

func TestFix_ApplyRefusesDaemonLockAcquiredAfterPlan(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	plan, err := buildFixPlan(ctx, repo, stateDB, false, false, false)
	if err != nil {
		t.Fatalf("buildFixPlan: %v", err)
	}
	gitDir := filepath.Join(repo, ".git")
	daemonLock, err := daemon.AcquireDaemonLock(gitDir)
	if err != nil {
		t.Fatalf("simulate daemon start after planning: %v", err)
	}
	defer daemonLock.Release()

	err = applyFixPlan(ctx, stateDB, &plan)
	if !errors.Is(err, daemon.ErrDaemonLockHeld) {
		t.Fatalf("applyFixPlan err=%v want daemon-lock refusal", err)
	}
	if plan.BackupPath != "" || plan.RowsChanged != 0 {
		t.Fatalf("daemon-lock refusal mutated plan: %+v", plan)
	}
	assertFixEventState(t, ctx, db, first, state.EventStateBlockedConflict)
	assertFixEventState(t, ctx, db, second, state.EventStatePending)
	if got := countRowsWhere(t, db, "recovery_snapshots", "1 = 1"); got != 0 {
		t.Fatalf("daemon-lock refusal wrote recovery snapshot: %d", got)
	}
}

func TestFix_QuiescesSharedRuntimeWhenLinkedWorktreeHoldsCanonicalLock(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	lock, err := daemon.AcquireDaemonLock(filepath.Join(repo, ".git"))
	if err != nil {
		t.Fatalf("hold canonical lock: %v", err)
	}
	lockReleased := false
	t.Cleanup(func() {
		if !lockReleased {
			_ = lock.Release()
		}
	})

	oldPrepare := prepareFixMutationSupervisor
	oldQuiesce := withQuiescedFixRuntime
	t.Cleanup(func() {
		prepareFixMutationSupervisor = oldPrepare
		withQuiescedFixRuntime = oldQuiesce
	})
	prepareFixMutationSupervisor = func(context.Context, paths.Roots) error { return nil }
	quiesceCalls := 0
	withQuiescedFixRuntime = func(operationCtx context.Context, _ paths.Roots, _ string, operation func(context.Context) error) error {
		quiesceCalls++
		if err := lock.Release(); err != nil {
			return err
		}
		lockReleased = true
		return operation(operationCtx)
	}

	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, false, true, false, false, true); err != nil {
		t.Fatalf("runFix: %v\n%s", err, out.String())
	}
	if quiesceCalls != 1 {
		t.Fatalf("quiesce calls=%d want 1", quiesceCalls)
	}
	assertFixEventState(t, ctx, db, first, state.EventStateRecovered)
	assertFixEventState(t, ctx, db, second, state.EventStateRecovered)
}

func runFixJSON(t *testing.T, repo string, dryRun, yes, force, clearPause bool) fixPlan {
	t.Helper()
	var out bytes.Buffer
	if err := runFix(context.Background(), &out, repo, dryRun, yes, force, clearPause, true); err != nil {
		t.Fatalf("runFix: %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal fix result: %v\n%s", err, out.String())
	}
	return plan
}

func findFixAction(plan fixPlan, kind string) *fixAction {
	for i := range plan.Actions {
		if plan.Actions[i].Kind == kind {
			return &plan.Actions[i]
		}
	}
	return nil
}

func seedResolvedFixPublicationDrain(
	t *testing.T,
	ctx context.Context,
	repo string,
	db *state.DB,
) (string, string) {
	t.Helper()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,
 state,commit_oid,published_ts
) VALUES('refs/heads/main',1,?,'modify','resolved.txt','exact',1,
         'published',?,2)`, head, head)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	const checkpointID = "cp-1787439000000-0123456789abcdef"
	checkpoint := state.Checkpoint{
		ID: checkpointID, OperationID: "op-fix-resolved-drain",
		WorktreeID: "0123456789abcdef", Reason: state.CheckpointReasonManualBarrier,
		ObservationEpoch: 1, CoverageEpoch: 1, ObservedHead: head,
		ObservedRef: "refs/heads/main", TreeOID: head, CommitOID: head,
		Ref:       "refs/acd/checkpoints/v1/0123456789abcdef/" + checkpointID,
		CreatedTS: 1, EventSeqs: []int64{seq},
	}
	if created, err := state.PrepareCheckpoint(
		ctx, db, checkpoint, fixCheckpointTestDigest); err != nil || !created {
		t.Fatalf("prepare checkpoint=(%t,%v)", created, err)
	}
	if err := state.CompleteCheckpoint(
		ctx, db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 2); err != nil {
		t.Fatal(err)
	}
	const drainID = "drain-cp-1787439000000-0123456789abcdef"
	drain := state.PublicationDrain{
		ID: drainID, CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 1, Phase: state.PublicationDrainNeedsAction,
		TargetEventCount: 1, LastError: "forced_capture_deferred",
		StagedConsent: true, StagedConsumed: true,
		CreatedTS: 3, UpdatedTS: 3, LastProgressTS: 3,
		EventSeqs: []int64{seq},
	}
	if created, err := state.PreparePublicationDrain(ctx, db, drain); err != nil || !created {
		t.Fatalf("prepare drain=(%t,%v)", created, err)
	}
	return drainID, checkpointID
}

func commitExternalSeedChange(t *testing.T, ctx context.Context, repo string) (parent, head, beforeOID, afterOID string) {
	t.Helper()
	var err error
	parent, err = git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse parent: %v", err)
	}
	beforeOID, err = git.LsTreeBlobOID(ctx, repo, parent, "seed.txt")
	if err != nil {
		t.Fatalf("ls-tree parent: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repo, "seed.txt"), []byte("seed\nlanded externally\n"), 0o644); err != nil {
		t.Fatalf("write seed.txt: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "add", "seed.txt"); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "-c", "user.name=ACD Test", "-c", "user.email=acd@example.invalid", "commit", "-m", "external modify"); err != nil {
		t.Fatalf("git commit: %v", err)
	}
	head, err = git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	afterOID, err = git.LsTreeBlobOID(ctx, repo, head, "seed.txt")
	if err != nil {
		t.Fatalf("ls-tree HEAD: %v", err)
	}
	return parent, head, beforeOID, afterOID
}

func stageRecoverableBarrierPair(t *testing.T, ctx context.Context, repo string, db *state.DB, branchRef string, generation int64) (int64, int64) {
	t.Helper()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	firstBlob, err := git.HashObjectStdin(ctx, repo, []byte("first captured\n"))
	if err != nil {
		t.Fatalf("hash first: %v", err)
	}
	secondBlob, err := git.HashObjectStdin(ctx, repo, []byte("second captured\n"))
	if err != nil {
		t.Fatalf("hash second: %v", err)
	}
	first := appendFixEvent(t, ctx, db, state.CaptureEvent{
		BranchRef: branchRef, BranchGeneration: generation, BaseHead: head,
		Operation: "create", Path: "first-recovery.txt", Fidelity: "exact",
		State: state.EventStateBlockedConflict,
		Error: sql.NullString{String: "before-state mismatch", Valid: true},
	}, []state.CaptureOp{{
		Op: "create", Path: "first-recovery.txt", Fidelity: "exact",
		AfterOID:  sql.NullString{String: firstBlob, Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	}})
	second := appendFixEvent(t, ctx, db, state.CaptureEvent{
		BranchRef: branchRef, BranchGeneration: generation, BaseHead: head,
		Operation: "create", Path: "second-recovery.txt", Fidelity: "exact", State: state.EventStatePending,
	}, []state.CaptureOp{{
		Op: "create", Path: "second-recovery.txt", Fidelity: "exact",
		AfterOID:  sql.NullString{String: secondBlob, Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	}})
	return first, second
}

func appendFixEvent(t *testing.T, ctx context.Context, db *state.DB, ev state.CaptureEvent, ops []state.CaptureOp) int64 {
	t.Helper()
	seq, err := state.AppendCaptureEvent(ctx, db, ev, ops)
	if err != nil {
		t.Fatalf("AppendCaptureEvent(%s): %v", ev.Path, err)
	}
	return seq
}

func assertFixEventState(t *testing.T, ctx context.Context, db *state.DB, seq int64, want string) {
	t.Helper()
	var got string
	if err := db.SQL().QueryRowContext(ctx, `SELECT state FROM capture_events WHERE seq = ?`, seq).Scan(&got); err != nil {
		t.Fatalf("query seq=%d: %v", seq, err)
	}
	if got != want {
		t.Fatalf("seq=%d state=%s want %s", seq, got, want)
	}
}

func countRowsWhere(t *testing.T, db *state.DB, table, where string, args ...any) int {
	t.Helper()
	var n int
	query := "SELECT COUNT(*) FROM " + table + " WHERE " + where
	if err := db.SQL().QueryRowContext(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// seedGeneratedPendingFixFixture is shared with diagnose tests so both
// surfaces describe the same protected generated-delete incident.
func seedGeneratedPendingFixFixture(t *testing.T, ctx context.Context, repo string, db *state.DB) []int64 {
	t.Helper()
	write := func(rel, body string) {
		t.Helper()
		full := filepath.Join(repo, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
	write(".derivedData-provider-core/Index.noindex/a.db", "a")
	write(".derivedData-provider-core/Index.noindex/b.db", "b")
	write("build/output.js", "ignored but not safe-ignore")
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "add", "-f",
		".derivedData-provider-core/Index.noindex/a.db",
		".derivedData-provider-core/Index.noindex/b.db"); err != nil {
		t.Fatalf("git add forced generated files: %v", err)
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	seed := func(path, op string) int64 {
		t.Helper()
		seq := appendFixEvent(t, ctx, db, state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
			Operation: op, Path: path, Fidelity: "full", State: state.EventStatePending,
		}, []state.CaptureOp{{
			Op: op, Path: path, Fidelity: "full",
			BeforeOID:  sql.NullString{String: strings.Repeat("a", 40), Valid: op == "delete"},
			BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: op == "delete"},
			AfterOID:   sql.NullString{String: strings.Repeat("b", 40), Valid: op != "delete"},
			AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: op != "delete"},
		}})
		if err := state.RecordPlannerOffer(ctx, db, seq, 123); err != nil {
			t.Fatalf("RecordPlannerOffer(%d): %v", seq, err)
		}
		return seq
	}
	seqs := []int64{
		seed(".derivedData-provider-core/Index.noindex/a.db", "delete"),
		seed(".derivedData-provider-core/Index.noindex/b.db", "delete"),
	}
	seed("build/output.js", "delete")
	seed("src/ordinary.txt", "modify")
	return seqs
}
