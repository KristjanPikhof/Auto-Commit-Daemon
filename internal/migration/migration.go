// Package migration performs the forward-only repository portion of the
// transactional v19 to v20 setup cutover. Global setup owns service/config
// rollback around these repository-local transactions.
package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type RepositoryPlan struct {
	Record          central.RepoRecord            `json:"record"`
	FromVersion     int                           `json:"from_version"`
	Pairs           []state.UnresolvedCapturePair `json:"unresolved_pairs"`
	Proofs          []daemon.RecoveryChainResult  `json:"proofs"`
	BridgeSnapshots []BridgeSnapshot              `json:"bridge_snapshots,omitempty"`
	BackupPath      string                        `json:"backup_path"`
	Missing         bool                          `json:"missing_worktree"`
}

type Result struct {
	Record            central.RepoRecord           `json:"record"`
	CheckpointID      string                       `json:"checkpoint_id"`
	RecoveryResults   []daemon.RecoveryChainResult `json:"recovery_results"`
	BridgeCheckpoints []string                     `json:"bridge_checkpoints,omitempty"`
	CreatedRefs       []RefProof                   `json:"created_refs"`
}

type Progress struct {
	Completed int
	Total     int
	Repo      string
}

type RefProof struct {
	Repo      string `json:"repo"`
	Ref       string `json:"ref"`
	CommitOID string `json:"commit_oid"`
}

type heldLock struct{ lock *daemon.DaemonLock }

var scanMigrationEntries = daemon.ScanProtectedEntries

func Preflight(ctx context.Context, record central.RepoRecord, backupPath string) (RepositoryPlan, error) {
	wt, err := gitpkg.ResolveWorktree(ctx, record.Path)
	if err != nil {
		if !record.LifecycleDisabled() {
			return RepositoryPlan{}, err
		}
		summary, summaryErr := state.ReadLegacyWorkSummary(ctx, record.StateDB)
		if summaryErr != nil {
			return RepositoryPlan{}, summaryErr
		}
		if summary.Unpublished > 0 || summary.Terminal > 0 || summary.OpenPublication > 0 {
			return RepositoryPlan{}, fmt.Errorf("migration: missing disabled worktree %s has unresolved captured work", record.Path)
		}
		version, versionErr := state.ReadUserVersion(ctx, record.StateDB)
		if errors.Is(versionErr, os.ErrNotExist) {
			return RepositoryPlan{Record: record, FromVersion: 0, BackupPath: backupPath, Missing: true}, nil
		}
		if versionErr != nil {
			return RepositoryPlan{}, versionErr
		}
		if err := state.QuickCheck(ctx, record.StateDB); err != nil {
			return RepositoryPlan{}, err
		}
		return RepositoryPlan{Record: record, FromVersion: version, BackupPath: backupPath, Missing: true}, nil
	}
	if record.StateDB == "" {
		record.StateDB = state.DBPathFromGitDir(wt.GitDir)
	}
	version, err := state.ReadUserVersion(ctx, record.StateDB)
	if errors.Is(err, os.ErrNotExist) {
		return RepositoryPlan{Record: record, FromVersion: 0, BackupPath: backupPath}, nil
	}
	if err != nil {
		return RepositoryPlan{}, err
	}
	if version > state.SchemaVersion {
		return RepositoryPlan{}, fmt.Errorf("migration: future schema v%d", version)
	}
	if err := state.QuickCheck(ctx, record.StateDB); err != nil {
		return RepositoryPlan{}, err
	}
	if marker, active := daemon.GitOperationInProgress(wt.GitDir); active {
		return RepositoryPlan{}, fmt.Errorf("migration: Git %s is in progress in %s", marker, wt.Root)
	}
	pairs, err := state.ReadUnresolvedCapturePairs(ctx, record.StateDB)
	if err != nil {
		return RepositoryPlan{}, err
	}
	proofs, err := proveChains(ctx, wt.Root, record.StateDB, pairs)
	if err != nil {
		return RepositoryPlan{}, err
	}
	return RepositoryPlan{Record: record, FromVersion: version, Pairs: pairs, Proofs: proofs, BackupPath: backupPath}, nil
}

func proveChains(ctx context.Context, repoRoot, dbPath string, pairs []state.UnresolvedCapturePair) ([]daemon.RecoveryChainResult, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	db, err := state.OpenReadOnly(ctx, dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	proofs := make([]daemon.RecoveryChainResult, 0, len(pairs))
	for _, pair := range pairs {
		proof, proofErr := daemon.ProveUnpublishedChain(ctx, repoRoot, db, daemon.RecoveryReconcileOptions{
			BranchRef: pair.BranchRef, BranchGeneration: pair.Generation,
			FirstSeq: pair.FirstSeq, Trigger: "v20_migration_preflight", InvalidateShadow: true,
		})
		if proofErr != nil {
			return nil, fmt.Errorf("migration: exact-chain preflight %s generation %d: %w", pair.BranchRef, pair.Generation, proofErr)
		}
		if !proof.Handled {
			return nil, fmt.Errorf("migration: exact-chain preflight %s generation %d was not provable", pair.BranchRef, pair.Generation)
		}
		proofs = append(proofs, proof)
	}
	return proofs, nil
}

// ApplyAll acquires canonical repository locks in sorted common-directory
// order, backs up every database before the first schema change, and rolls all
// database files back if any proof or checkpoint fails. Newly created refs are
// deliberately retained on rollback unless their exact target is known by the
// caller's global ref manifest; retaining provenance is safer than deletion.
func ApplyAll(ctx context.Context, plans []RepositoryPlan) ([]Result, error) {
	return ApplyAllWithProgress(ctx, plans, nil)
}

// ApplyAllWithProgress performs the same all-or-nothing migration while
// reporting the repository about to be applied. Progress is observational;
// callers must not mutate the plans or repository state from the callback.
func ApplyAllWithProgress(ctx context.Context, plans []RepositoryPlan, progress func(Progress)) ([]Result, error) {
	plans = append([]RepositoryPlan(nil), plans...)
	sort.Slice(plans, func(i, j int) bool { return plans[i].Record.CommonDir < plans[j].Record.CommonDir })
	locks := make([]heldLock, 0)
	seenCommon := make(map[string]bool)
	for _, plan := range plans {
		if plan.Missing {
			continue
		}
		wt, err := gitpkg.ResolveWorktree(ctx, plan.Record.Path)
		if err != nil {
			releaseLocks(locks)
			return nil, err
		}
		if seenCommon[wt.CommonDir] {
			continue
		}
		lock, err := daemon.AcquireDaemonLock(wt.GitDir)
		if err != nil {
			releaseLocks(locks)
			return nil, fmt.Errorf("migration: quiesce %s: %w", wt.Root, err)
		}
		seenCommon[wt.CommonDir] = true
		locks = append(locks, heldLock{lock: lock})
	}
	defer releaseLocks(locks)
	for _, plan := range plans {
		if err := revalidateRepositoryPlan(ctx, plan); err != nil {
			return nil, err
		}
	}

	for _, plan := range plans {
		_ = os.Remove(createdRefsPath(plan))
		if plan.FromVersion > 0 {
			if err := state.BackupDatabase(ctx, plan.Record.StateDB, plan.BackupPath); err != nil {
				return nil, fmt.Errorf("migration: back up %s: %w", plan.Record.Path, err)
			}
		}
	}
	var results []Result
	for index, plan := range plans {
		if progress != nil {
			progress(Progress{Completed: index + 1, Total: len(plans), Repo: plan.Record.Path})
		}
		result, err := applyRepository(ctx, plan)
		if err != nil {
			rollbackErr := Rollback(plans)
			return nil, errors.Join(fmt.Errorf("migration: apply %s: %w", plan.Record.Path, err), rollbackErr)
		}
		results = append(results, result)
	}
	return results, nil
}

func revalidateRepositoryPlan(ctx context.Context, reviewed RepositoryPlan) error {
	current, err := Preflight(ctx, reviewed.Record, reviewed.BackupPath)
	if err != nil {
		return fmt.Errorf("migration: revalidate %s: %w", reviewed.Record.Path, err)
	}
	for _, snapshot := range reviewed.BridgeSnapshots {
		operationID := strings.Split(strings.TrimPrefix(snapshot.Ref, migrationRefPrefix), "/")[0]
		if err := validateBridgeSnapshot(ctx, operationID, snapshot); err != nil {
			return fmt.Errorf("migration: revalidate retained bridge %s: %w", snapshot.Ref, err)
		}
	}
	current.BridgeSnapshots = reviewed.BridgeSnapshots
	reviewedBody, _ := json.Marshal(reviewed)
	currentBody, _ := json.Marshal(current)
	if string(currentBody) != string(reviewedBody) {
		return fmt.Errorf("migration: repository state changed after preview: %s", reviewed.Record.Path)
	}
	return nil
}

func applyRepository(ctx context.Context, plan RepositoryPlan) (Result, error) {
	if plan.Missing {
		result := Result{Record: plan.Record}
		if plan.FromVersion == 0 {
			return result, nil
		}
		db, err := state.Open(ctx, plan.Record.StateDB)
		if err != nil {
			return Result{}, err
		}
		return result, db.Close()
	}
	wt, err := gitpkg.ResolveWorktree(ctx, plan.Record.Path)
	if err != nil {
		return Result{}, err
	}
	db, err := state.Open(ctx, plan.Record.StateDB)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	result := Result{Record: plan.Record}
	if err := daemon.RecoverSelfPublicationsBeforePlanning(ctx, wt.Root, db); err != nil {
		return Result{}, fmt.Errorf("migration: recover publication journal before membership repair: %w", err)
	}
	for index, pair := range plan.Pairs {
		recovered, err := daemon.ReconcileUnpublishedChain(ctx, wt.Root, db, daemon.RecoveryReconcileOptions{
			GitDir: wt.GitDir, BranchRef: pair.BranchRef, BranchGeneration: pair.Generation,
			FirstSeq: pair.FirstSeq, Trigger: "v20_migration", InvalidateShadow: true,
		})
		if err != nil {
			return Result{}, fmt.Errorf("migration: ambiguous chain %s generation %d: %w", pair.BranchRef, pair.Generation, err)
		}
		if !recovered.Handled {
			return Result{}, fmt.Errorf("migration: chain %s generation %d changed during cutover", pair.BranchRef, pair.Generation)
		}
		if index >= len(plan.Proofs) || !sameRecoveryProof(plan.Proofs[index], recovered) {
			return Result{}, fmt.Errorf("migration: chain %s generation %d no longer matches the reviewed proof", pair.BranchRef, pair.Generation)
		}
		result.RecoveryResults = append(result.RecoveryResults, recovered)
		if recovered.Outcome == state.EventStateRecovered {
			imported, err := importRecoveryCheckpoint(ctx, wt, db, plan.Record.WorktreeID, recovered)
			if err != nil {
				return Result{}, err
			}
			proof := RefProof{Repo: wt.Root, Ref: imported.Ref, CommitOID: imported.CommitOID}
			if err := appendCreatedRef(plan, proof); err != nil {
				deleteErr := gitpkg.DeletePrivateRefDurable(context.Background(), wt.Root, gitpkg.CheckpointRefPrefix, imported.Ref, imported.CommitOID)
				return Result{}, errors.Join(err, deleteErr)
			}
			result.CreatedRefs = append(result.CreatedRefs, proof)
		}
	}
	for _, snapshot := range plan.BridgeSnapshots {
		imported, err := importBridgeCheckpoint(ctx, wt, db, plan.Record.WorktreeID, snapshot)
		if err != nil {
			return Result{}, fmt.Errorf("migration: import retained bridge ref %s: %w", snapshot.Ref, err)
		}
		proof := RefProof{Repo: wt.Root, Ref: imported.Ref, CommitOID: imported.CommitOID}
		if err := appendCreatedRef(plan, proof); err != nil {
			deleteErr := gitpkg.DeletePrivateRefDurable(context.Background(), wt.Root, gitpkg.CheckpointRefPrefix, imported.Ref, imported.CommitOID)
			return Result{}, errors.Join(err, deleteErr)
		}
		result.CreatedRefs = append(result.CreatedRefs, proof)
		result.BridgeCheckpoints = append(result.BridgeCheckpoints, imported.ID)
	}
	if _, err := state.ReconcileCheckpointIntentMemberships(ctx, db, plan.Record.WorktreeID); err != nil {
		return Result{}, fmt.Errorf("migration: reconcile checkpoint publication membership: %w", err)
	}
	store := checkpoint.Store{DB: db}
	entries, exclusions, err := scanProtectedEntriesForMigration(ctx, wt.Root, &store)
	if err != nil {
		return Result{}, err
	}
	head, _ := gitpkg.RevParse(ctx, wt.Root, "HEAD")
	refOut, _ := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: wt.Root}, "symbolic-ref", "-q", "HEAD")
	created, err := store.Create(ctx, checkpoint.Request{
		RepoRoot: wt.Root, WorktreeID: plan.Record.WorktreeID,
		Reason: state.CheckpointReasonMigration, ObservationEpoch: 1, CoverageEpoch: 1,
		ObservedHead: head, ObservedRef: strings.TrimSpace(string(refOut)), Entries: entries, Exclusions: exclusions,
	})
	if err != nil {
		return Result{}, err
	}
	result.CheckpointID = created.Checkpoint.ID
	proof := RefProof{Repo: wt.Root, Ref: created.Checkpoint.Ref, CommitOID: created.Checkpoint.CommitOID}
	if err := appendCreatedRef(plan, proof); err != nil {
		deleteErr := gitpkg.DeletePrivateRefDurable(context.Background(), wt.Root, gitpkg.CheckpointRefPrefix, created.Checkpoint.Ref, created.Checkpoint.CommitOID)
		return Result{}, errors.Join(err, deleteErr)
	}
	result.CreatedRefs = append(result.CreatedRefs, proof)
	if err := state.MetaSetMany(ctx, db, map[string]string{
		daemon.MetaKeyProtectionObservationEpoch: "1", daemon.MetaKeyProtectionCoveredEpoch: "1",
		daemon.MetaKeyProtectionCheckpointID: created.Checkpoint.ID, daemon.MetaKeyProtectionComplete: "true",
	}); err != nil {
		return Result{}, err
	}
	return result, nil
}

const migrationScanAttempts = 3

func scanProtectedEntriesForMigration(ctx context.Context, repoRoot string, store *checkpoint.Store) ([]checkpoint.Entry, []state.CheckpointExclusion, error) {
	var lastErr error
	for attempt := 1; attempt <= migrationScanAttempts; attempt++ {
		checker := gitpkg.NewIgnoreChecker(repoRoot)
		entries, exclusions, summary, err := scanMigrationEntries(ctx, repoRoot,
			daemon.CaptureOpts{IgnoreChecker: checker, CheckpointStore: store})
		closeErr := checker.Close()
		if err == nil && closeErr == nil {
			return entries, exclusions, nil
		}
		lastErr = errors.Join(err, closeErr)
		if err == nil || (summary.Errors == 0 && summary.Oversize == 0) || attempt == migrationScanAttempts {
			break
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, nil, fmt.Errorf("protection scan remained incomplete after %d attempts: %w", migrationScanAttempts, lastErr)
}

func importRecoveryCheckpoint(ctx context.Context, wt gitpkg.Worktree, db *state.DB, worktreeID string, recovered daemon.RecoveryChainResult) (state.Checkpoint, error) {
	snapshot, ok, err := state.RecoverySnapshotByID(ctx, db, recovered.SnapshotID)
	if err != nil || !ok {
		return state.Checkpoint{}, fmt.Errorf("migration: load recovery snapshot %d: %w", recovered.SnapshotID, err)
	}
	members, err := state.RecoverySnapshotEvents(ctx, db, snapshot.ID)
	if err != nil {
		return state.Checkpoint{}, err
	}
	treeOID, err := gitpkg.RevParse(ctx, wt.Root, recovered.CommitOID+"^{tree}")
	if err != nil {
		return state.Checkpoint{}, err
	}
	now := time.Now()
	id, err := checkpoint.NewID(now)
	if err != nil {
		return state.Checkpoint{}, err
	}
	commitOID, err := gitpkg.CommitTreeDurable(ctx, wt.Root, treeOID,
		"acd checkpoint "+id+"\n", checkpoint.IdentityName, checkpoint.IdentityEmail)
	if err != nil {
		return state.Checkpoint{}, err
	}
	ref := gitpkg.CheckpointRefPrefix + worktreeID + "/" + id
	eventSeqs := make([]int64, 0, len(members))
	for _, member := range members {
		eventSeqs = append(eventSeqs, member.EventSeq)
	}
	eventSeqs, err = state.UncheckpointedEventSeqs(ctx, db, eventSeqs)
	if err != nil {
		return state.Checkpoint{}, fmt.Errorf(
			"migration: inspect recovery checkpoint ownership: %w", err)
	}
	head, _ := gitpkg.RevParse(ctx, wt.Root, "HEAD")
	refOut, _ := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: wt.Root}, "symbolic-ref", "-q", "HEAD")
	cp := state.Checkpoint{ID: id, OperationID: "op-" + id, WorktreeID: worktreeID,
		Reason: state.CheckpointReasonMigrationRecovery, ObservedHead: head,
		ObservedRef: strings.TrimSpace(string(refOut)), TreeOID: treeOID,
		CommitOID: commitOID, Ref: ref, CreatedTS: float64(now.UnixNano()) / float64(time.Second),
		EventSeqs: eventSeqs}
	digestBytes := sha256.Sum256([]byte(strings.Join([]string{
		id, worktreeID, treeOID, commitOID, recovered.RecoveryRef,
		strconv.FormatInt(recovered.FirstSeq, 10), strconv.FormatInt(recovered.LastSeq, 10),
	}, "\x00")))
	planDigest := "sha256:" + hex.EncodeToString(digestBytes[:])
	if _, err := state.PrepareCheckpoint(ctx, db, cp, planDigest); err != nil {
		return state.Checkpoint{}, err
	}
	if _, err := gitpkg.EnsureCheckpointRef(ctx, wt.Root, ref, commitOID); err != nil {
		return state.Checkpoint{}, err
	}
	if err := state.CompleteCheckpoint(ctx, db, id, ref, commitOID, 0); err != nil {
		return state.Checkpoint{}, err
	}
	cp.Phase = state.CheckpointCompleted
	return cp, nil
}

func sameRecoveryProof(planned, applied daemon.RecoveryChainResult) bool {
	return planned.Handled == applied.Handled && planned.Outcome == applied.Outcome &&
		planned.FirstSeq == applied.FirstSeq && planned.LastSeq == applied.LastSeq &&
		planned.EventCount == applied.EventCount
}

func releaseLocks(locks []heldLock) {
	for i := len(locks) - 1; i >= 0; i-- {
		_ = locks[i].lock.Release()
	}
}

func restoreBackups(plans []RepositoryPlan) error {
	var combined error
	for _, plan := range plans {
		if plan.FromVersion == 0 || plan.BackupPath == "" {
			continue
		}
		_ = os.Remove(plan.Record.StateDB + "-wal")
		_ = os.Remove(plan.Record.StateDB + "-shm")
		if err := replaceFile(plan.BackupPath, plan.Record.StateDB); err != nil {
			combined = errors.Join(combined, err)
		}
	}
	return combined
}

// Rollback restores pre-cutover databases after a later global setup stage
// fails. Fresh databases are removed only when setup created them at the exact
// planned path.
func Rollback(plans []RepositoryPlan) error {
	var combined error
	for _, plan := range plans {
		if plan.FromVersion == 0 {
			if err := os.Remove(plan.Record.StateDB); err != nil && !errors.Is(err, os.ErrNotExist) {
				combined = errors.Join(combined, err)
			}
			_ = os.Remove(plan.Record.StateDB + "-wal")
			_ = os.Remove(plan.Record.StateDB + "-shm")
			continue
		}
		if err := replaceFile(plan.BackupPath, plan.Record.StateDB); err != nil {
			combined = errors.Join(combined, err)
		}
	}
	return errors.Join(combined, cleanupCreatedRefs(context.Background(), plans))
}

func createdRefsPath(plan RepositoryPlan) string { return plan.BackupPath + ".created-refs.json" }

func appendCreatedRef(plan RepositoryPlan, proof RefProof) error {
	path := createdRefsPath(plan)
	var proofs []RefProof
	if body, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(body, &proofs); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	proofs = append(proofs, proof)
	body, err := json.MarshalIndent(proofs, "", "  ")
	if err != nil {
		return err
	}
	return replaceBytes(path, append(body, '\n'), 0o600)
}

func cleanupCreatedRefs(ctx context.Context, plans []RepositoryPlan) error {
	var combined error
	for _, plan := range plans {
		path := createdRefsPath(plan)
		body, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			combined = errors.Join(combined, err)
			continue
		}
		var proofs []RefProof
		if err := json.Unmarshal(body, &proofs); err != nil {
			combined = errors.Join(combined, fmt.Errorf("migration: decode created-ref manifest: %w", err))
			continue
		}
		var planErr error
		for _, proof := range proofs {
			if err := gitpkg.DeletePrivateRefDurable(ctx, proof.Repo, gitpkg.CheckpointRefPrefix, proof.Ref, proof.CommitOID); err != nil {
				planErr = errors.Join(planErr, err)
			}
		}
		combined = errors.Join(combined, planErr)
		if planErr == nil {
			_ = os.Remove(path)
		}
	}
	return combined
}

func replaceBytes(path string, body []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".acd-migration-refs-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func replaceFile(source, target string) error {
	body, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".acd-migration-rollback-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := removeDatabaseSidecars(target); err != nil {
		return err
	}
	if err := os.Rename(name, target); err != nil {
		return err
	}
	if err := removeDatabaseSidecars(target); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(target))
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		dir.Close()
		return err
	}
	if err := dir.Close(); err != nil {
		return err
	}
	return state.QuickCheck(context.Background(), target)
}

func removeDatabaseSidecars(target string) error {
	var combined error
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if err := os.Remove(target + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			combined = errors.Join(combined, err)
		}
	}
	return combined
}
