// Package restore plans and applies full checkpoint restoration as a new
// working-tree change. It never moves HEAD or mutates the live Git index.
package restore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type Counts struct {
	Created            int `json:"created"`
	Modified           int `json:"modified"`
	Deleted            int `json:"deleted"`
	ModeChanged        int `json:"mode_changed"`
	Symlinks           int `json:"symlinks"`
	UntrackedOverwrite int `json:"untracked_overwrite"`
	StagedOverlap      int `json:"staged_overlap"`
}

type Plan struct {
	CheckpointID     string `json:"checkpoint_id"`
	PlanDigest       string `json:"plan_digest"`
	RepoRoot         string `json:"repo_root"`
	WorktreeID       string `json:"worktree_id"`
	HeadToken        string `json:"head_token"`
	BranchGeneration int64  `json:"branch_generation"`
	Detached         bool   `json:"detached"`
	IndexDigest      string `json:"index_digest"`
	Counts           Counts `json:"counts"`
	CanApply         bool   `json:"can_apply"`
	Refusal          string `json:"refusal,omitempty"`

	target  state.Checkpoint
	changes []change
	current []checkpoint.Entry
	policy  ProtectionPolicy
}

// ProtectionPolicy is one immutable worker-resolved exclusion snapshot used
// throughout preview, apply revalidation, checkpoint creation, and repair.
type ProtectionPolicy struct {
	Sensitive    *state.SensitiveMatcher
	SafeIgnore   *state.SafeIgnoreMatcher
	MaxFileBytes int64
}

type Result struct {
	OperationID        string `json:"operation_id"`
	RestoredCheckpoint string `json:"restored_checkpoint"`
	UndoCheckpoint     string `json:"undo_checkpoint"`
	ResultCheckpoint   string `json:"result_checkpoint"`
}

type RepairPlan struct {
	OperationID      string `json:"operation_id"`
	TargetCheckpoint string `json:"target_checkpoint"`
	PreCheckpoint    string `json:"pre_restore_checkpoint"`
	CanRepair        bool   `json:"can_repair"`
	Refusal          string `json:"refusal,omitempty"`
}

type change struct {
	Path   string
	Before *checkpoint.Entry
	After  *checkpoint.Entry
}

type backupManifest struct {
	Version int                   `json:"version"`
	Context durableRestoreContext `json:"context"`
	Entries []backupEntry         `json:"entries"`
}

const backupManifestVersion = 1

type durableRestoreContext struct {
	PlanDigest       string `json:"plan_digest"`
	WorktreeID       string `json:"worktree_id"`
	HeadToken        string `json:"head_token"`
	BranchGeneration int64  `json:"branch_generation"`
	Detached         bool   `json:"detached"`
	IndexDigest      string `json:"index_digest"`
}

type backupEntry struct {
	Path       string      `json:"path"`
	Exists     bool        `json:"exists"`
	Mode       fs.FileMode `json:"mode"`
	Symlink    bool        `json:"symlink"`
	Directory  bool        `json:"directory,omitempty"`
	BackupFile string      `json:"backup_file,omitempty"`
}

func Preview(ctx context.Context, repoRoot, gitDir, dbPath, id string) (Plan, error) {
	return PreviewWithPolicy(ctx, repoRoot, gitDir, dbPath, id, ProtectionPolicy{})
}

// PreviewWithPolicy plans a restore using the same resolved exclusion policy
// as the repository worker that will apply it.
func PreviewWithPolicy(
	ctx context.Context,
	repoRoot, gitDir, dbPath, id string,
	policy ProtectionPolicy,
) (Plan, error) {
	policy = normalizedProtectionPolicy(policy)
	target, err := state.ResolveCheckpoint(ctx, dbPath, id)
	if err != nil {
		return Plan{}, err
	}
	if marker, active := daemon.GitOperationInProgress(gitDir); active {
		return Plan{CheckpointID: target.ID, Refusal: "Git " + marker + " is in progress"}, nil
	}
	unmerged, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repoRoot}, "ls-files", "-u", "-z")
	if err != nil {
		return Plan{}, err
	}
	if len(unmerged) > 0 {
		return Plan{CheckpointID: target.ID, Refusal: "Git conflicts are present"}, nil
	}
	readDB, err := state.OpenReadOnly(ctx, dbPath)
	if err != nil {
		return Plan{}, err
	}
	generation, err := daemon.LoadBranchGeneration(ctx, readDB)
	_ = readDB.Close()
	if err != nil {
		return Plan{}, err
	}
	headToken, err := headToken(ctx, repoRoot)
	if err != nil {
		return Plan{}, err
	}
	checker := gitpkg.NewIgnoreChecker(repoRoot)
	defer checker.Close()
	current, _, _, err := daemon.ScanProtectedEntries(ctx, repoRoot, policy.captureOpts(checker, nil))
	if err != nil {
		return Plan{}, err
	}
	targetTree, err := gitpkg.LsTree(ctx, repoRoot, target.TreeOID, true)
	if err != nil {
		return Plan{}, err
	}
	targetEntries := make([]checkpoint.Entry, 0, len(targetTree))
	for _, entry := range targetTree {
		if entry.Type == "blob" {
			targetEntries = append(targetEntries, checkpoint.Entry{Path: entry.Path, Mode: entry.Mode, OID: entry.OID})
		}
	}
	if excluded, err := targetExcluded(ctx, checker, targetEntries, policy); err != nil {
		return Plan{}, err
	} else if excluded {
		return Plan{
			CheckpointID: target.ID, RepoRoot: repoRoot, WorktreeID: target.WorktreeID,
			HeadToken: headToken, BranchGeneration: generation,
			Detached: observedRef(headToken) == "", CanApply: false,
			Refusal: "restore target includes paths excluded by current protection policy",
		}, nil
	}
	changes := diffEntries(current, targetEntries)
	staged, err := stagedPaths(ctx, repoRoot)
	if err != nil {
		return Plan{}, err
	}
	tracked, err := trackedPaths(ctx, repoRoot)
	if err != nil {
		return Plan{}, err
	}
	counts := countChanges(changes, staged, tracked)
	indexDigest, err := digestIndex(ctx, repoRoot)
	if err != nil {
		return Plan{}, err
	}
	plan := Plan{
		CheckpointID: target.ID, RepoRoot: repoRoot,
		WorktreeID: target.WorktreeID, HeadToken: headToken,
		BranchGeneration: generation, Detached: observedRef(headToken) == "",
		IndexDigest: indexDigest, Counts: counts, CanApply: counts.StagedOverlap == 0,
		target: target, changes: changes, current: current, policy: policy,
	}
	if counts.StagedOverlap > 0 {
		plan.Refusal = "restore overlaps staged paths; commit or unstage them first"
	}
	plan.PlanDigest = digestPlan(plan)
	return plan, nil
}

func targetExcluded(
	ctx context.Context,
	checker *gitpkg.IgnoreChecker,
	entries []checkpoint.Entry,
	policy ProtectionPolicy,
) (bool, error) {
	paths := make([]string, len(entries))
	for i, entry := range entries {
		paths[i] = entry.Path
		if policy.Sensitive.Match(entry.Path) || sensitiveAncestorExcluded(entry.Path, policy.Sensitive) ||
			policy.SafeIgnore.MatchFile(entry.Path) {
			return true, nil
		}
	}
	ignored, err := checker.Check(ctx, paths)
	if err != nil {
		return false, err
	}
	for _, match := range ignored {
		if match {
			return true, nil
		}
	}
	return false, nil
}

func sensitiveAncestorExcluded(relative string, matcher *state.SensitiveMatcher) bool {
	for ancestor := path.Dir(relative); ancestor != "."; ancestor = path.Dir(ancestor) {
		if matcher.MatchDirectory(ancestor) {
			return true
		}
	}
	return false
}

func normalizedProtectionPolicy(policy ProtectionPolicy) ProtectionPolicy {
	if policy.Sensitive == nil {
		policy.Sensitive = state.NewSensitiveMatcher()
	}
	if policy.SafeIgnore == nil {
		policy.SafeIgnore = state.NewSafeIgnoreMatcher()
	}
	return policy
}

func (policy ProtectionPolicy) captureOpts(
	checker *gitpkg.IgnoreChecker,
	store *checkpoint.Store,
) daemon.CaptureOpts {
	return daemon.CaptureOpts{
		IgnoreChecker: checker, SensitiveMatcher: policy.Sensitive,
		SafeIgnoreMatcher: policy.SafeIgnore, MaxFileBytes: policy.MaxFileBytes,
		CheckpointStore: store,
	}
}

func Apply(ctx context.Context, db *state.DB, plan Plan) (Result, error) {
	if db == nil || !plan.CanApply || plan.Refusal != "" {
		return Result{}, errors.New("restore: plan is not safe to apply")
	}
	pending, err := state.RestoreRepairPending(ctx, db)
	if err != nil {
		return Result{}, fmt.Errorf("restore: inspect interrupted operation: %w", err)
	}
	if pending {
		return Result{}, errors.New("restore: repair the interrupted restore before applying another")
	}
	revalidated, err := PreviewWithPolicy(ctx, plan.RepoRoot,
		filepath.Dir(filepath.Dir(db.Path())), db.Path(), plan.CheckpointID, plan.policy)
	if err != nil {
		return Result{}, err
	}
	if revalidated.PlanDigest != plan.PlanDigest || revalidated.HeadToken != plan.HeadToken || revalidated.IndexDigest != plan.IndexDigest {
		return Result{}, errors.New("restore: working tree or Git state changed after preview")
	}
	plan = revalidated
	operationID, err := newOperationID()
	if err != nil {
		return Result{}, err
	}
	steps := make([]state.OperationStep, 0, len(plan.changes))
	for i, item := range plan.changes {
		steps = append(steps, state.OperationStep{Sequence: i + 1, Kind: "restore_path", Target: item.Path,
			BeforeDigest: entryDigest(item.Before), AfterDigest: entryDigest(item.After), Phase: state.OperationPrepared})
	}
	if err := state.PrepareOperation(ctx, db, state.Operation{ID: operationID, Kind: "restore", WorktreeID: plan.WorktreeID, PlanDigest: plan.PlanDigest}, steps); err != nil {
		return Result{}, err
	}
	if err := state.PrepareRestoreOperation(ctx, db, operationID, plan.CheckpointID, plan.PlanDigest); err != nil {
		_ = state.AdvanceOperation(context.Background(), db, operationID, state.OperationNeedsAttention, state.OperationNeedsAttention, "restore journal could not be prepared")
		return Result{}, err
	}

	gitDir := filepath.Dir(filepath.Dir(db.Path()))
	store := checkpoint.Store{DB: db}
	epoch := nextEpoch(ctx, db)
	preChecker := gitpkg.NewIgnoreChecker(plan.RepoRoot)
	preEntries, _, _, scanErr := daemon.ScanProtectedEntries(ctx, plan.RepoRoot,
		plan.policy.captureOpts(preChecker, &store))
	_ = preChecker.Close()
	if scanErr != nil {
		_ = state.AdvanceOperation(context.Background(), db, operationID, state.OperationNeedsAttention, state.OperationNeedsAttention, "pre-restore protection scan failed")
		return Result{}, fmt.Errorf("restore: pre-restore protection scan: %w", scanErr)
	}
	pre, err := store.Create(ctx, checkpoint.Request{
		RepoRoot: plan.RepoRoot, WorktreeID: plan.WorktreeID,
		Reason: state.CheckpointReasonPreRestore, ObservationEpoch: epoch, CoverageEpoch: epoch,
		ObservedHead: strings.TrimPrefix(strings.Split(plan.HeadToken, " ")[0], "rev:"),
		ObservedRef:  observedRef(plan.HeadToken), Entries: preEntries,
	})
	if err != nil {
		_ = state.AdvanceOperation(context.Background(), db, operationID, state.OperationNeedsAttention, state.OperationNeedsAttention, "pre-restore checkpoint failed")
		return Result{}, fmt.Errorf("restore: pre-restore checkpoint: %w", err)
	}
	backupDir := filepath.Join(gitDir, "acd", "restore", operationID)
	manifest, err := createBackups(ctx, plan.RepoRoot, backupDir, plan)
	if err != nil {
		_ = state.FinishRestoreOperation(context.Background(), db, operationID, pre.Checkpoint.ID, "",
			state.OperationRolledBack, state.OperationRolledBack, "restore preimage backup failed")
		return Result{}, err
	}
	if err := state.BeginRestoreApply(ctx, db, operationID, pre.Checkpoint.ID); err != nil {
		return Result{}, err
	}
	applyErr := applyChanges(ctx, plan.RepoRoot, plan.changes)
	if applyErr == nil && afterRestoreApplyForTest != nil {
		applyErr = afterRestoreApplyForTest()
	}
	if applyErr != nil {
		rollbackErr := rollback(plan.RepoRoot, backupDir, manifest, steps)
		phase, status := state.OperationRolledBack, state.OperationRolledBack
		if rollbackErr != nil {
			phase, status = state.OperationApplying, state.OperationNeedsAttention
		}
		_ = state.FinishRestoreOperation(context.Background(), db, operationID, pre.Checkpoint.ID, "",
			phase, status, "restore application failed")
		return Result{}, errors.Join(applyErr, rollbackErr)
	}
	if got, digestErr := digestIndex(ctx, plan.RepoRoot); digestErr != nil || got != plan.IndexDigest {
		rollbackErr := rollback(plan.RepoRoot, backupDir, manifest, steps)
		phase, status := state.OperationRolledBack, state.OperationRolledBack
		if rollbackErr != nil {
			phase, status = state.OperationApplying, state.OperationNeedsAttention
		}
		_ = state.FinishRestoreOperation(context.Background(), db, operationID, pre.Checkpoint.ID, "",
			phase, status, "Git index changed during restore")
		return Result{}, errors.Join(errors.New("restore: Git index changed during apply"), digestErr, rollbackErr)
	}
	if got, headErr := headToken(ctx, plan.RepoRoot); headErr != nil || got != plan.HeadToken {
		rollbackErr := rollback(plan.RepoRoot, backupDir, manifest, steps)
		phase, status := state.OperationRolledBack, state.OperationRolledBack
		if rollbackErr != nil {
			phase, status = state.OperationApplying, state.OperationNeedsAttention
		}
		_ = state.FinishRestoreOperation(context.Background(), db, operationID, pre.Checkpoint.ID, "",
			phase, status, "Git HEAD changed during restore")
		return Result{}, errors.Join(errors.New("restore: Git HEAD changed during apply"), headErr, rollbackErr)
	}
	if err := state.AdvanceRestoreOperation(ctx, db, operationID, pre.Checkpoint.ID, "", "applied"); err != nil {
		rollbackErr := rollback(plan.RepoRoot, backupDir, manifest, steps)
		_ = state.AdvanceOperation(context.Background(), db, operationID, state.OperationNeedsAttention, state.OperationNeedsAttention, "restore journal could not record filesystem application")
		return Result{}, errors.Join(err, rollbackErr)
	}
	postCheckpointID, err := createResultCheckpoint(ctx, db, store, plan, epoch+1)
	if err != nil || postCheckpointID == "" {
		_ = state.AdvanceOperation(context.Background(), db, operationID, state.OperationNeedsAttention, state.OperationNeedsAttention, "post-restore checkpoint failed")
		return Result{}, fmt.Errorf("restore: files restored but post-restore checkpoint needs repair: %w", err)
	}
	if err := state.FinishRestoreOperation(ctx, db, operationID, pre.Checkpoint.ID,
		postCheckpointID, state.OperationCompleted, state.OperationCompleted, ""); err != nil {
		return Result{}, err
	}
	return Result{OperationID: operationID, RestoredCheckpoint: plan.CheckpointID,
		UndoCheckpoint: pre.Checkpoint.ID, ResultCheckpoint: postCheckpointID}, nil
}

func createResultCheckpoint(ctx context.Context, db *state.DB, store checkpoint.Store, plan Plan, epoch int64) (string, error) {
	checker := gitpkg.NewIgnoreChecker(plan.RepoRoot)
	defer checker.Close()
	head := strings.TrimPrefix(strings.Split(plan.HeadToken, " ")[0], "rev:")
	if !plan.Detached {
		opts := plan.policy.captureOpts(checker, &store)
		opts.WorktreeID = plan.WorktreeID
		opts.ObservationEpoch = epoch
		opts.CheckpointReason = state.CheckpointReasonRestore
		summary, err := daemon.Capture(ctx, plan.RepoRoot, db, daemon.CaptureContext{
			BranchRef: observedRef(plan.HeadToken), BranchGeneration: plan.BranchGeneration, BaseHead: head,
		}, opts)
		return summary.CheckpointID, err
	}
	entries, exclusions, _, err := daemon.ScanProtectedEntries(ctx, plan.RepoRoot,
		plan.policy.captureOpts(checker, &store))
	if err != nil {
		return "", err
	}
	created, err := store.Create(ctx, checkpoint.Request{
		RepoRoot: plan.RepoRoot, WorktreeID: plan.WorktreeID,
		Reason: state.CheckpointReasonRestore, ObservationEpoch: epoch, CoverageEpoch: epoch,
		ObservedHead: head, Entries: entries, Exclusions: exclusions,
	})
	if err != nil {
		return "", err
	}
	if err := daemon.CompleteProtectionCoverage(ctx, db, epoch, created.Checkpoint.ID, entries); err != nil {
		return "", err
	}
	return created.Checkpoint.ID, nil
}

// PreviewRepair identifies an interrupted restore with durable preimages.
// Repair proves whether to complete the exact target or restore the preimages.
func PreviewRepair(ctx context.Context, db *state.DB) (RepairPlan, error) {
	operation, err := state.RepairableRestoreOperation(ctx, db)
	if err != nil {
		return RepairPlan{}, err
	}
	plan := RepairPlan{OperationID: operation.OperationID,
		TargetCheckpoint: operation.TargetCheckpointID}
	if operation.PreCheckpointID.Valid {
		plan.PreCheckpoint = operation.PreCheckpointID.String
	}
	if plan.PreCheckpoint == "" {
		plan.Refusal = "restore pre-checkpoint identity is missing"
		return plan, nil
	}
	plan.CanRepair = true
	return plan, nil
}

// Repair completes the missing post-restore checkpoint after proving the
// current protected tree still exactly matches the requested restore target.
func Repair(ctx context.Context, repoRoot string, db *state.DB, plan RepairPlan) (Result, error) {
	return RepairWithPolicy(ctx, repoRoot, db, plan, ProtectionPolicy{})
}

// RepairWithPolicy reconciles an interrupted restore under the same immutable
// exclusion policy used by the worker before the crash.
func RepairWithPolicy(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	plan RepairPlan,
	policy ProtectionPolicy,
) (Result, error) {
	policy = normalizedProtectionPolicy(policy)
	currentPlan, err := PreviewRepair(ctx, db)
	if err != nil {
		return Result{}, err
	}
	if !plan.CanRepair || currentPlan != plan {
		return Result{}, errors.New("restore: repair plan changed; preview again")
	}
	operation, err := state.RepairableRestoreOperation(ctx, db)
	if err != nil || operation.OperationID != plan.OperationID {
		return Result{}, errors.New("restore: repair operation changed; preview again")
	}
	steps, err := state.RestoreOperationSteps(ctx, db, plan.OperationID)
	if err != nil {
		return Result{}, err
	}
	backupDir := filepath.Join(filepath.Dir(filepath.Dir(db.Path())), "acd", "restore", plan.OperationID)
	manifest, err := loadBackupManifest(backupDir)
	if err != nil {
		return Result{}, fmt.Errorf("restore: load durable preimages: %w", err)
	}
	restorePlan, err := repairRestorePlan(ctx, repoRoot, db, operation, manifest, policy)
	if err != nil {
		_ = state.AdvanceOperation(context.Background(), db, plan.OperationID,
			state.OperationNeedsAttention, state.OperationNeedsAttention,
			"restore context changed after the interrupted operation")
		return Result{}, err
	}
	proof, err := inspectRestoreState(ctx, repoRoot, backupDir, steps, manifest)
	if err != nil {
		return Result{}, err
	}
	if !proof.Safe {
		_ = state.AdvanceOperation(context.Background(), db, plan.OperationID,
			state.OperationNeedsAttention, state.OperationNeedsAttention,
			"restore paths include changes not authored by the interrupted operation")
		return Result{}, errors.New("restore: working tree includes third-party changes; refusing repair")
	}
	if proof.ExactBefore {
		if err := state.FinishRestoreOperation(ctx, db, plan.OperationID, plan.PreCheckpoint, "",
			state.OperationRolledBack, state.OperationRolledBack, ""); err != nil {
			return Result{}, err
		}
		return Result{OperationID: plan.OperationID, RestoredCheckpoint: plan.TargetCheckpoint,
			UndoCheckpoint: plan.PreCheckpoint}, nil
	}
	if !proof.ExactAfter {
		if err := rollback(repoRoot, backupDir, manifest, steps); err != nil {
			_ = state.AdvanceOperation(context.Background(), db, plan.OperationID,
				state.OperationNeedsAttention, state.OperationNeedsAttention, "interrupted restore rollback failed")
			return Result{}, fmt.Errorf("restore: interrupted rollback: %w", err)
		}
		rolledBack, err := inspectRestoreState(ctx, repoRoot, backupDir, steps, manifest)
		if err != nil || !rolledBack.ExactBefore {
			_ = state.AdvanceOperation(context.Background(), db, plan.OperationID,
				state.OperationNeedsAttention, state.OperationNeedsAttention, "interrupted restore rollback proof failed")
			return Result{}, errors.Join(errors.New("restore: rollback did not reproduce durable preimages"), err)
		}
		if err := state.FinishRestoreOperation(ctx, db, plan.OperationID, plan.PreCheckpoint, "",
			state.OperationRolledBack, state.OperationRolledBack, ""); err != nil {
			return Result{}, err
		}
		return Result{OperationID: plan.OperationID, RestoredCheckpoint: plan.TargetCheckpoint,
			UndoCheckpoint: plan.PreCheckpoint}, nil
	}
	target, err := state.ResolveCheckpoint(ctx, db.Path(), plan.TargetCheckpoint)
	if err != nil {
		return Result{}, err
	}
	checker := gitpkg.NewIgnoreChecker(repoRoot)
	entries, _, _, err := daemon.ScanProtectedEntries(ctx, repoRoot,
		policy.captureOpts(checker, &checkpoint.Store{DB: db}))
	_ = checker.Close()
	if err != nil {
		return Result{}, fmt.Errorf("restore: repair protection scan: %w", err)
	}
	targetEntries, err := treeEntries(ctx, repoRoot, target.TreeOID)
	if err != nil {
		return Result{}, err
	}
	if !entriesEqual(entries, targetEntries) {
		return Result{}, errors.New("restore: working tree no longer matches the interrupted restore target")
	}
	epoch := nextEpoch(ctx, db)
	store := checkpoint.Store{DB: db}
	postCheckpointID, err := createResultCheckpoint(ctx, db, store, restorePlan, epoch)
	if err != nil || postCheckpointID == "" {
		return Result{}, err
	}
	if err := state.FinishRestoreOperation(ctx, db, plan.OperationID, plan.PreCheckpoint,
		postCheckpointID, state.OperationCompleted, state.OperationCompleted, ""); err != nil {
		return Result{}, err
	}
	return Result{OperationID: plan.OperationID, RestoredCheckpoint: plan.TargetCheckpoint,
		UndoCheckpoint: plan.PreCheckpoint, ResultCheckpoint: postCheckpointID}, nil
}

func repairRestorePlan(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	operation state.RestoreOperation,
	manifest backupManifest,
	policy ProtectionPolicy,
) (Plan, error) {
	context := manifest.Context
	if manifest.Version != backupManifestVersion || context.PlanDigest == "" ||
		context.PlanDigest != operation.PlanDigest || context.WorktreeID != operation.WorktreeID ||
		context.WorktreeID != checkpoint.WorktreeID(repoRoot) || context.BranchGeneration < 1 {
		return Plan{}, errors.New("restore: durable restore context is missing or inconsistent")
	}
	currentHead, err := headToken(ctx, repoRoot)
	if err != nil {
		return Plan{}, err
	}
	currentIndex, err := digestIndex(ctx, repoRoot)
	if err != nil {
		return Plan{}, err
	}
	currentGeneration, err := daemon.LoadBranchGeneration(ctx, db)
	if err != nil {
		return Plan{}, err
	}
	if currentGeneration != context.BranchGeneration {
		return Plan{}, errors.New("restore: branch generation changed after the interrupted restore")
	}
	if currentHead != context.HeadToken || currentIndex != context.IndexDigest ||
		context.Detached != (observedRef(context.HeadToken) == "") {
		return Plan{}, errors.New("restore: Git HEAD or index changed after the interrupted restore")
	}
	return Plan{
		CheckpointID: operation.TargetCheckpointID,
		PlanDigest:   context.PlanDigest, RepoRoot: repoRoot, WorktreeID: context.WorktreeID,
		HeadToken: context.HeadToken, BranchGeneration: context.BranchGeneration,
		Detached: context.Detached, IndexDigest: context.IndexDigest, policy: policy,
	}, nil
}

type restoreProof struct {
	Safe        bool
	ExactBefore bool
	ExactAfter  bool
}

func inspectRestoreState(
	ctx context.Context,
	repoRoot string,
	backupDir string,
	steps []state.OperationStep,
	manifest backupManifest,
) (restoreProof, error) {
	if len(steps) != len(manifest.Entries) {
		return restoreProof{}, errors.New("restore: path proof and backup manifest differ")
	}
	backups := make(map[string]backupEntry, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		if _, duplicate := backups[entry.Path]; duplicate {
			return restoreProof{}, errors.New("restore: duplicate backup manifest path")
		}
		backups[entry.Path] = entry
	}
	hasher, err := gitpkg.NewBlobHasher(ctx, repoRoot)
	if err != nil {
		return restoreProof{}, err
	}
	worktree, err := openSafeTree(repoRoot)
	if err != nil {
		return restoreProof{}, err
	}
	defer worktree.Close()
	backupTree, backupRelative, err := openBackupTree(backupDir, false)
	if err != nil {
		return restoreProof{}, err
	}
	defer backupTree.Close()
	proof := restoreProof{Safe: true, ExactBefore: true, ExactAfter: true}
	for _, step := range steps {
		backup, ok := backups[step.Target]
		if !ok || step.Kind != "restore_path" {
			return restoreProof{}, errors.New("restore: incomplete path proof ledger")
		}
		before, err := backupDigest(hasher, backupTree, backupRelative, backup)
		if err != nil {
			return restoreProof{}, err
		}
		if before != actualBeforeDigest(step, steps) {
			return restoreProof{}, errors.New("restore: durable preimage does not match the operation journal")
		}
		after := actualAfterDigest(step, steps)
		current, err := currentPathDigest(ctx, worktree, step.Target, hasher)
		if err != nil && manifestHasAncestor(manifest.Entries, step.Target) && isNonDirectoryPathError(err) {
			current, err = "absent", nil
		}
		if err != nil {
			return restoreProof{}, err
		}
		matchesBefore := current == before
		matchesAfter := current == after
		proof.ExactBefore = proof.ExactBefore && matchesBefore
		proof.ExactAfter = proof.ExactAfter && matchesAfter
		if !matchesBefore && !matchesAfter && !(current == "absent" && before != "absent") {
			proof.Safe = false
		}
	}
	return proof, nil
}

func actualBeforeDigest(step state.OperationStep, steps []state.OperationStep) string {
	if step.BeforeDigest != "absent" {
		return step.BeforeDigest
	}
	prefix := step.Target + "/"
	for _, candidate := range steps {
		if strings.HasPrefix(candidate.Target, prefix) && candidate.BeforeDigest != "absent" {
			return "directory"
		}
	}
	return "absent"
}

func actualAfterDigest(step state.OperationStep, steps []state.OperationStep) string {
	if step.AfterDigest != "absent" {
		return step.AfterDigest
	}
	prefix := step.Target + "/"
	for _, candidate := range steps {
		if strings.HasPrefix(candidate.Target, prefix) && candidate.AfterDigest != "absent" {
			return "directory"
		}
	}
	return "absent"
}

func backupDigest(
	hasher gitpkg.BlobHasher,
	backupTree *safeTree,
	backupRelative string,
	entry backupEntry,
) (string, error) {
	if !entry.Exists {
		return "absent", nil
	}
	if entry.Directory {
		return "directory", nil
	}
	backupPath, err := backupFilePath(backupRelative, entry)
	if err != nil {
		return "", err
	}
	stored, err := backupTree.read(backupPath)
	if err != nil {
		return "", err
	}
	if !stored.Exists || !stored.Mode.IsRegular() {
		return "", errors.New("restore: durable preimage is missing")
	}
	oid, err := hasher.BlobOID(stored.Body)
	if err != nil {
		return "", err
	}
	mode := gitpkg.RegularFileMode
	if entry.Symlink {
		mode = gitpkg.SymlinkMode
	} else if entry.Mode.Perm()&0o111 != 0 {
		mode = gitpkg.ExecutableFileMode
	}
	return mode + ":" + oid, nil
}

func currentPathDigest(
	ctx context.Context,
	worktree *safeTree,
	relative string,
	hasher gitpkg.BlobHasher,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	current, err := worktree.read(relative)
	if err != nil {
		return "", err
	}
	return pathStateDigest(hasher, current)
}

func pathStateDigest(hasher gitpkg.BlobHasher, current safePathState) (string, error) {
	if !current.Exists {
		return "absent", nil
	}
	if current.Directory {
		return "directory", nil
	}
	mode := gitpkg.RegularFileMode
	if current.Symlink {
		mode = gitpkg.SymlinkMode
	} else if current.Mode.IsRegular() {
		if current.Mode.Perm()&0o111 != 0 {
			mode = gitpkg.ExecutableFileMode
		}
	} else {
		return "unsupported", nil
	}
	oid, err := hasher.BlobOID(current.Body)
	if err != nil {
		return "", err
	}
	return mode + ":" + oid, nil
}

func treeEntries(ctx context.Context, repoRoot, treeOID string) ([]checkpoint.Entry, error) {
	tree, err := gitpkg.LsTree(ctx, repoRoot, treeOID, true)
	if err != nil {
		return nil, err
	}
	entries := make([]checkpoint.Entry, 0, len(tree))
	for _, entry := range tree {
		if entry.Type == "blob" {
			entries = append(entries, checkpoint.Entry{Path: entry.Path, Mode: entry.Mode, OID: entry.OID})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries, nil
}

func entriesEqual(left, right []checkpoint.Entry) bool {
	if len(left) != len(right) {
		return false
	}
	left = append([]checkpoint.Entry(nil), left...)
	right = append([]checkpoint.Entry(nil), right...)
	sort.Slice(left, func(i, j int) bool { return left[i].Path < left[j].Path })
	sort.Slice(right, func(i, j int) bool { return right[i].Path < right[j].Path })
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func diffEntries(before, after []checkpoint.Entry) []change {
	left := make(map[string]checkpoint.Entry, len(before))
	right := make(map[string]checkpoint.Entry, len(after))
	for _, entry := range before {
		left[entry.Path] = entry
	}
	for _, entry := range after {
		right[entry.Path] = entry
	}
	paths := make(map[string]struct{}, len(left)+len(right))
	for path := range left {
		paths[path] = struct{}{}
	}
	for path := range right {
		paths[path] = struct{}{}
	}
	ordered := make([]string, 0, len(paths))
	for path := range paths {
		ordered = append(ordered, path)
	}
	sort.Strings(ordered)
	changes := make([]change, 0)
	for _, path := range ordered {
		beforeEntry, beforeOK := left[path]
		afterEntry, afterOK := right[path]
		if beforeOK && afterOK && beforeEntry.Mode == afterEntry.Mode && beforeEntry.OID == afterEntry.OID {
			continue
		}
		item := change{Path: path}
		if beforeOK {
			copy := beforeEntry
			item.Before = &copy
		}
		if afterOK {
			copy := afterEntry
			item.After = &copy
		}
		changes = append(changes, item)
	}
	return changes
}

func countChanges(changes []change, staged, tracked map[string]bool) Counts {
	var counts Counts
	for _, item := range changes {
		switch {
		case item.Before == nil:
			counts.Created++
		case item.After == nil:
			counts.Deleted++
		default:
			counts.Modified++
		}
		if item.Before != nil && item.After != nil && item.Before.Mode != item.After.Mode {
			counts.ModeChanged++
		}
		if item.After != nil && item.After.Mode == gitpkg.SymlinkMode {
			counts.Symlinks++
		}
		if staged[item.Path] {
			counts.StagedOverlap++
		}
		if item.After != nil && item.Before != nil && !tracked[item.Path] {
			counts.UntrackedOverwrite++
		}
	}
	return counts
}

func stagedPaths(ctx context.Context, repo string) (map[string]bool, error) {
	out, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo,
		ExtraEnv: map[string]string{"GIT_OPTIONAL_LOCKS": "0"}},
		"diff", "--cached", "--name-only", "-z", "--")
	if err != nil {
		return nil, err
	}
	return nulSet(out), nil
}

func trackedPaths(ctx context.Context, repo string) (map[string]bool, error) {
	out, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo,
		ExtraEnv: map[string]string{"GIT_OPTIONAL_LOCKS": "0"}},
		"ls-files", "-z")
	if err != nil {
		return nil, err
	}
	return nulSet(out), nil
}

func nulSet(body []byte) map[string]bool {
	result := make(map[string]bool)
	for _, item := range strings.Split(string(body), "\x00") {
		if item != "" {
			result[item] = true
		}
	}
	return result
}

func headToken(ctx context.Context, repo string) (string, error) {
	head, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		return "", err
	}
	ref, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "symbolic-ref", "-q", "HEAD")
	if err != nil {
		return "rev:" + head, nil
	}
	return "rev:" + head + " " + strings.TrimSpace(string(ref)), nil
}

func observedRef(token string) string {
	parts := strings.SplitN(token, " ", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return ""
}

func digestIndex(ctx context.Context, repo string) (string, error) {
	path, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "rev-parse", "--git-path", "index")
	if err != nil {
		return "", err
	}
	indexPath := strings.TrimSpace(string(path))
	if !filepath.IsAbs(indexPath) {
		indexPath = filepath.Join(repo, indexPath)
	}
	body, err := os.ReadFile(indexPath)
	if errors.Is(err, os.ErrNotExist) {
		return "absent", nil
	}
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func digestPlan(plan Plan) string {
	hash := sha256.New()
	fields := []string{
		plan.CheckpointID, plan.RepoRoot, plan.WorktreeID, plan.HeadToken,
		strconv.FormatInt(plan.BranchGeneration, 10), strconv.FormatBool(plan.Detached),
		plan.IndexDigest, strconv.Itoa(plan.Counts.StagedOverlap),
	}
	for _, field := range fields {
		hash.Write([]byte(field))
		hash.Write([]byte{0})
	}
	for _, item := range plan.changes {
		hash.Write([]byte(item.Path))
		hash.Write([]byte{0})
		hash.Write([]byte(entryDigest(item.Before)))
		hash.Write([]byte{0})
		hash.Write([]byte(entryDigest(item.After)))
		hash.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func entryDigest(entry *checkpoint.Entry) string {
	if entry == nil {
		return "absent"
	}
	return entry.Mode + ":" + entry.OID
}

func nextEpoch(ctx context.Context, db *state.DB) int64 {
	value, ok, _ := state.MetaGet(ctx, db, daemon.MetaKeyProtectionObservationEpoch)
	if !ok {
		return 1
	}
	epoch, _ := strconv.ParseInt(value, 10, 64)
	return epoch + 1
}

func newOperationID() (string, error) {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	return fmt.Sprintf("restore-%d-%s", time.Now().UnixMilli(), hex.EncodeToString(random)), nil
}

func createBackups(ctx context.Context, repoRoot, backupDir string, plan Plan) (backupManifest, error) {
	worktree, err := openSafeTree(repoRoot)
	if err != nil {
		return backupManifest{}, err
	}
	defer worktree.Close()
	backups, relativeDir, err := openBackupTree(backupDir, true)
	if err != nil {
		return backupManifest{}, err
	}
	defer backups.Close()
	hasher, err := gitpkg.NewBlobHasher(ctx, repoRoot)
	if err != nil {
		return backupManifest{}, err
	}

	manifest := backupManifest{
		Version: backupManifestVersion,
		Context: durableRestoreContext{
			PlanDigest: plan.PlanDigest, WorktreeID: plan.WorktreeID,
			HeadToken: plan.HeadToken, BranchGeneration: plan.BranchGeneration,
			Detached: plan.Detached, IndexDigest: plan.IndexDigest,
		},
		Entries: make([]backupEntry, 0, len(plan.changes)),
	}
	for i, item := range plan.changes {
		current, err := worktree.read(item.Path)
		if err != nil {
			return manifest, err
		}
		currentDigest, err := pathStateDigest(hasher, current)
		if err != nil {
			return manifest, err
		}
		if currentDigest != changeBeforeDigest(item, plan.changes) {
			return manifest, fmt.Errorf("restore: working tree changed after preview: %s", item.Path)
		}
		entry := backupEntry{
			Path: item.Path, Exists: current.Exists, Mode: current.Mode,
			Directory: current.Directory, Symlink: current.Symlink,
		}
		if current.Exists && !current.Directory {
			entry.BackupFile = fmt.Sprintf("%06d", i)
			backupPath, err := backupFilePath(relativeDir, entry)
			if err != nil {
				return manifest, err
			}
			if err := backups.writeFile(backupPath, current.Body, 0o600); err != nil {
				return manifest, err
			}
		}
		manifest.Entries = append(manifest.Entries, entry)
	}
	body, err := json.Marshal(manifest)
	if err != nil {
		return manifest, err
	}
	if err := backups.writeFile(relativeDir+"/manifest.json", body, 0o600); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func changeBeforeDigest(item change, changes []change) string {
	if item.Before != nil {
		return entryDigest(item.Before)
	}
	prefix := item.Path + "/"
	for _, candidate := range changes {
		if candidate.Before != nil && strings.HasPrefix(candidate.Path, prefix) {
			return "directory"
		}
	}
	return "absent"
}

func openBackupTree(backupDir string, create bool) (*safeTree, string, error) {
	stateRoot := filepath.Dir(filepath.Dir(backupDir))
	relative, err := filepath.Rel(stateRoot, backupDir)
	if err != nil {
		return nil, "", err
	}
	relative, err = cleanRelativePath(filepath.ToSlash(relative))
	if err != nil {
		return nil, "", err
	}
	tree, err := openSafeTree(stateRoot)
	if err != nil {
		return nil, "", err
	}
	if create {
		if err := tree.ensureDir(relative, 0o700); err != nil {
			_ = tree.Close()
			return nil, "", err
		}
	}
	return tree, relative, nil
}

func loadBackupManifest(backupDir string) (backupManifest, error) {
	backups, relativeDir, err := openBackupTree(backupDir, false)
	if err != nil {
		return backupManifest{}, err
	}
	defer backups.Close()
	stored, err := backups.read(relativeDir + "/manifest.json")
	if err != nil {
		return backupManifest{}, err
	}
	if !stored.Exists || !stored.Mode.IsRegular() {
		return backupManifest{}, errors.New("restore: durable preimage manifest is missing")
	}
	var manifest backupManifest
	if err := json.Unmarshal(stored.Body, &manifest); err != nil {
		return backupManifest{}, err
	}
	return manifest, nil
}

func applyChanges(ctx context.Context, repoRoot string, changes []change) error {
	worktree, err := openSafeTree(repoRoot)
	if err != nil {
		return err
	}
	defer worktree.Close()
	deletes := append([]change(nil), changes...)
	sort.Slice(deletes, func(i, j int) bool { return strings.Count(deletes[i].Path, "/") > strings.Count(deletes[j].Path, "/") })
	for _, item := range deletes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if item.Before == nil {
			continue
		}
		if err := worktree.remove(item.Path); err != nil {
			return fmt.Errorf("restore: remove %s: %w", item.Path, err)
		}
	}
	for _, item := range changes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if item.After == nil {
			continue
		}
		current, err := worktree.read(item.Path)
		if err != nil {
			return err
		}
		if current.Exists {
			if !current.Directory || !provenTransitionDirectory(changes, item.Path) {
				return fmt.Errorf("restore: path changed during apply: %s", item.Path)
			}
			if err := worktree.remove(item.Path); err != nil {
				return fmt.Errorf("restore: replace directory %s: %w", item.Path, err)
			}
		}
		body, err := gitpkg.CatFileBlobLimited(ctx, repoRoot, item.After.OID, 0)
		if err != nil {
			return err
		}
		if item.After.Mode == gitpkg.SymlinkMode {
			err = worktree.writeSymlink(item.Path, string(body))
		} else {
			mode := fs.FileMode(0o644)
			if item.After.Mode == gitpkg.ExecutableFileMode {
				mode = 0o755
			}
			err = worktree.writeFile(item.Path, body, mode)
		}
		if err != nil {
			return fmt.Errorf("restore: write %s: %w", item.Path, err)
		}
	}
	return nil
}

func provenTransitionDirectory(changes []change, path string) bool {
	prefix := path + "/"
	for _, item := range changes {
		if item.Before != nil && strings.HasPrefix(item.Path, prefix) {
			return true
		}
	}
	return false
}

var beforeRollbackClaimForTest func(string)
var afterRestoreApplyForTest func() error

func rollback(
	repoRoot string,
	backupDir string,
	manifest backupManifest,
	steps []state.OperationStep,
) error {
	worktree, err := openSafeTree(repoRoot)
	if err != nil {
		return err
	}
	defer worktree.Close()
	backups, relativeDir, err := openBackupTree(backupDir, false)
	if err != nil {
		return err
	}
	defer backups.Close()
	hasher, err := gitpkg.NewBlobHasher(context.Background(), repoRoot)
	if err != nil {
		return err
	}
	proofs := make(map[string]state.OperationStep, len(steps))
	for _, step := range steps {
		if step.Kind != "restore_path" {
			return errors.New("restore: incomplete rollback path proof")
		}
		if _, duplicate := proofs[step.Target]; duplicate {
			return errors.New("restore: duplicate rollback path proof")
		}
		proofs[step.Target] = step
	}
	if len(proofs) != len(manifest.Entries) {
		return errors.New("restore: rollback path proof and manifest differ")
	}

	entries := append([]backupEntry(nil), manifest.Entries...)
	sort.Slice(entries, func(i, j int) bool {
		return strings.Count(entries[i].Path, "/") > strings.Count(entries[j].Path, "/")
	})
	claims := make([]*safePathClaim, 0, len(entries))
	for _, entry := range entries {
		step, ok := proofs[entry.Path]
		if !ok {
			return errors.Join(errors.New("restore: missing rollback path proof"), restoreClaims(claims))
		}
		before, err := backupDigest(hasher, backups, relativeDir, entry)
		if err != nil {
			return errors.Join(err, restoreClaims(claims))
		}
		if before != actualBeforeDigest(step, steps) {
			return errors.Join(errors.New("restore: durable rollback preimage differs from path proof"), restoreClaims(claims))
		}
		if beforeRollbackClaimForTest != nil {
			beforeRollbackClaimForTest(entry.Path)
		}
		claim, err := worktree.claim(entry.Path)
		if err != nil && manifestHasAncestor(entries, entry.Path) && isNonDirectoryPathError(err) {
			claim, err = &safePathClaim{parent: -1}, nil
		}
		if err != nil {
			return errors.Join(fmt.Errorf("restore: claim %s during rollback: %w", entry.Path, err), restoreClaims(claims))
		}
		claims = append(claims, claim)
		current, err := pathStateDigest(hasher, claim.state)
		if err != nil {
			return errors.Join(err, restoreClaims(claims))
		}
		after := actualAfterDigest(step, steps)
		if !rollbackStateAllowed(current, before, after) {
			return errors.Join(fmt.Errorf("restore: %s changed during rollback", entry.Path), restoreClaims(claims))
		}
	}
	for i, claim := range claims {
		if err := claim.discard(); err != nil {
			return errors.Join(
				fmt.Errorf("restore: discard claimed rollback path: %w", err),
				restoreClaims(claims[i+1:]),
			)
		}
	}

	var combined error
	sort.Slice(entries, func(i, j int) bool {
		return strings.Count(entries[i].Path, "/") < strings.Count(entries[j].Path, "/")
	})
	for _, entry := range entries {
		if !entry.Exists || !entry.Directory {
			continue
		}
		if err := worktree.ensureDir(entry.Path, entry.Mode.Perm()); err != nil {
			combined = errors.Join(combined, err)
		}
	}
	for _, entry := range entries {
		if !entry.Exists || entry.Directory {
			continue
		}
		backupPath, pathErr := backupFilePath(relativeDir, entry)
		if pathErr != nil {
			combined = errors.Join(combined, pathErr)
			continue
		}
		stored, err := backups.read(backupPath)
		if err != nil {
			combined = errors.Join(combined, err)
			continue
		}
		if !stored.Exists || !stored.Mode.IsRegular() {
			combined = errors.Join(combined, fmt.Errorf("restore: durable preimage for %s is missing", entry.Path))
			continue
		}
		if entry.Symlink {
			err = worktree.writeSymlink(entry.Path, string(stored.Body))
		} else {
			err = worktree.writeFile(entry.Path, stored.Body, entry.Mode.Perm())
		}
		combined = errors.Join(combined, err)
	}
	return combined
}

func rollbackStateAllowed(current, before, after string) bool {
	return current == before || current == after || (current == "absent" && before != "absent")
}

func restoreClaims(claims []*safePathClaim) error {
	var combined error
	for i := len(claims) - 1; i >= 0; i-- {
		combined = errors.Join(combined, claims[i].restore())
	}
	return combined
}

func manifestHasAncestor(entries []backupEntry, relative string) bool {
	for _, entry := range entries {
		if strings.HasPrefix(relative, entry.Path+"/") {
			return true
		}
	}
	return false
}

func backupFilePath(backupRelative string, entry backupEntry) (string, error) {
	if entry.BackupFile == "" || filepath.Base(entry.BackupFile) != entry.BackupFile {
		return "", fmt.Errorf("restore: unsafe backup identity for %s", entry.Path)
	}
	return backupRelative + "/" + entry.BackupFile, nil
}
