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
	CheckpointID string `json:"checkpoint_id"`
	PlanDigest   string `json:"plan_digest"`
	RepoRoot     string `json:"repo_root"`
	WorktreeID   string `json:"worktree_id"`
	HeadToken    string `json:"head_token"`
	IndexDigest  string `json:"index_digest"`
	Counts       Counts `json:"counts"`
	CanApply     bool   `json:"can_apply"`
	Refusal      string `json:"refusal,omitempty"`

	target  state.Checkpoint
	changes []change
	current []checkpoint.Entry
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
	Entries []backupEntry `json:"entries"`
}

type backupEntry struct {
	Path       string      `json:"path"`
	Exists     bool        `json:"exists"`
	Mode       fs.FileMode `json:"mode"`
	Symlink    bool        `json:"symlink"`
	BackupFile string      `json:"backup_file,omitempty"`
}

func Preview(ctx context.Context, repoRoot, gitDir, dbPath, id string) (Plan, error) {
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
	checker := gitpkg.NewIgnoreChecker(repoRoot)
	defer checker.Close()
	current, _, _, err := daemon.ScanProtectedEntries(ctx, repoRoot, daemon.CaptureOpts{IgnoreChecker: checker})
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
	headToken, err := headToken(ctx, repoRoot)
	if err != nil {
		return Plan{}, err
	}
	indexDigest, err := digestIndex(ctx, repoRoot)
	if err != nil {
		return Plan{}, err
	}
	plan := Plan{
		CheckpointID: target.ID, RepoRoot: repoRoot,
		WorktreeID: target.WorktreeID, HeadToken: headToken,
		IndexDigest: indexDigest, Counts: counts, CanApply: counts.StagedOverlap == 0,
		target: target, changes: changes, current: current,
	}
	if counts.StagedOverlap > 0 {
		plan.Refusal = "restore overlaps staged paths; commit or unstage them first"
	}
	plan.PlanDigest = digestPlan(plan)
	return plan, nil
}

func Apply(ctx context.Context, db *state.DB, plan Plan) (Result, error) {
	if db == nil || !plan.CanApply || plan.Refusal != "" {
		return Result{}, errors.New("restore: plan is not safe to apply")
	}
	revalidated, err := Preview(ctx, plan.RepoRoot, filepath.Dir(filepath.Dir(db.Path())), db.Path(), plan.CheckpointID)
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
		daemon.CaptureOpts{IgnoreChecker: preChecker, CheckpointStore: &store})
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
	manifest, err := createBackups(plan.RepoRoot, backupDir, plan.changes)
	if err != nil {
		_ = state.AdvanceOperation(context.Background(), db, operationID, state.OperationNeedsAttention, state.OperationNeedsAttention, "restore preimage backup failed")
		return Result{}, err
	}
	if err := state.AdvanceOperation(ctx, db, operationID, state.OperationApplying, state.OperationActive, ""); err != nil {
		return Result{}, err
	}
	if err := applyChanges(ctx, plan.RepoRoot, plan.changes); err != nil {
		rollbackErr := rollback(plan.RepoRoot, backupDir, manifest)
		status := state.OperationRolledBack
		if rollbackErr != nil {
			status = state.OperationNeedsAttention
		}
		_ = state.AdvanceOperation(context.Background(), db, operationID, status, status, "restore application failed")
		return Result{}, errors.Join(err, rollbackErr)
	}
	if got, digestErr := digestIndex(ctx, plan.RepoRoot); digestErr != nil || got != plan.IndexDigest {
		rollbackErr := rollback(plan.RepoRoot, backupDir, manifest)
		_ = state.AdvanceOperation(context.Background(), db, operationID, state.OperationNeedsAttention, state.OperationNeedsAttention, "Git index changed during restore")
		return Result{}, errors.Join(errors.New("restore: Git index changed during apply"), digestErr, rollbackErr)
	}
	if got, headErr := headToken(ctx, plan.RepoRoot); headErr != nil || got != plan.HeadToken {
		rollbackErr := rollback(plan.RepoRoot, backupDir, manifest)
		_ = state.AdvanceOperation(context.Background(), db, operationID, state.OperationNeedsAttention, state.OperationNeedsAttention, "Git HEAD changed during restore")
		return Result{}, errors.Join(errors.New("restore: Git HEAD changed during apply"), headErr, rollbackErr)
	}
	if err := state.AdvanceRestoreOperation(ctx, db, operationID, pre.Checkpoint.ID, "", state.OperationApplying); err != nil {
		rollbackErr := rollback(plan.RepoRoot, backupDir, manifest)
		_ = state.AdvanceOperation(context.Background(), db, operationID, state.OperationNeedsAttention, state.OperationNeedsAttention, "restore journal could not record filesystem application")
		return Result{}, errors.Join(err, rollbackErr)
	}
	checker := gitpkg.NewIgnoreChecker(plan.RepoRoot)
	defer checker.Close()
	branchRef := observedRef(plan.HeadToken)
	postSummary, err := daemon.Capture(ctx, plan.RepoRoot, db, daemon.CaptureContext{
		BranchRef: branchRef, BranchGeneration: 1, BaseHead: strings.TrimPrefix(strings.Split(plan.HeadToken, " ")[0], "rev:"),
	}, daemon.CaptureOpts{CheckpointStore: &store, WorktreeID: plan.WorktreeID,
		ObservationEpoch: epoch + 1, CheckpointReason: state.CheckpointReasonRestore, IgnoreChecker: checker})
	if err != nil || postSummary.CheckpointID == "" {
		_ = state.AdvanceOperation(context.Background(), db, operationID, state.OperationNeedsAttention, state.OperationNeedsAttention, "post-restore checkpoint failed")
		return Result{}, fmt.Errorf("restore: files restored but post-restore checkpoint needs repair: %w", err)
	}
	if err := state.CompleteRestoreOperation(ctx, db, operationID, pre.Checkpoint.ID, postSummary.CheckpointID, state.OperationCompleted); err != nil {
		return Result{}, err
	}
	if err := state.AdvanceOperation(ctx, db, operationID, state.OperationCompleted, state.OperationCompleted, ""); err != nil {
		return Result{}, err
	}
	return Result{OperationID: operationID, RestoredCheckpoint: plan.CheckpointID,
		UndoCheckpoint: pre.Checkpoint.ID, ResultCheckpoint: postSummary.CheckpointID}, nil
}

// PreviewRepair identifies a restore whose filesystem application completed
// but whose post-restore checkpoint did not. Earlier restore failures are not
// repairable through this path because their preimages must be rolled back.
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
	currentPlan, err := PreviewRepair(ctx, db)
	if err != nil {
		return Result{}, err
	}
	if !plan.CanRepair || currentPlan != plan {
		return Result{}, errors.New("restore: repair plan changed; preview again")
	}
	target, err := state.ResolveCheckpoint(ctx, db.Path(), plan.TargetCheckpoint)
	if err != nil {
		return Result{}, err
	}
	checker := gitpkg.NewIgnoreChecker(repoRoot)
	entries, exclusions, _, err := daemon.ScanProtectedEntries(ctx, repoRoot,
		daemon.CaptureOpts{IgnoreChecker: checker, CheckpointStore: &checkpoint.Store{DB: db}})
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
	operation, err := state.RepairableRestoreOperation(ctx, db)
	if err != nil || operation.OperationID != plan.OperationID {
		return Result{}, errors.New("restore: repair operation changed; preview again")
	}
	eventSeqs, err := state.UncheckpointedEventSeqsSince(ctx, db, operation.OperationCreatedTS)
	if err != nil {
		return Result{}, err
	}
	token, err := headToken(ctx, repoRoot)
	if err != nil {
		return Result{}, err
	}
	epoch := nextEpoch(ctx, db)
	store := checkpoint.Store{DB: db}
	created, err := store.Create(ctx, checkpoint.Request{
		RepoRoot: repoRoot, WorktreeID: target.WorktreeID,
		Reason: state.CheckpointReasonRestore, ObservationEpoch: epoch,
		CoverageEpoch: epoch, ObservedHead: strings.TrimPrefix(strings.Split(token, " ")[0], "rev:"),
		ObservedRef: observedRef(token), Entries: entries, EventSeqs: eventSeqs,
		Exclusions: exclusions,
	})
	if err != nil {
		return Result{}, err
	}
	if err := daemon.CompleteProtectionCoverage(ctx, db, epoch, created.Checkpoint.ID, entries); err != nil {
		return Result{}, err
	}
	if err := state.CompleteRestoreOperation(ctx, db, plan.OperationID, plan.PreCheckpoint,
		created.Checkpoint.ID, state.OperationCompleted); err != nil {
		return Result{}, err
	}
	if err := state.AdvanceOperation(ctx, db, plan.OperationID, state.OperationCompleted, state.OperationCompleted, ""); err != nil {
		return Result{}, err
	}
	return Result{OperationID: plan.OperationID, RestoredCheckpoint: plan.TargetCheckpoint,
		UndoCheckpoint: plan.PreCheckpoint, ResultCheckpoint: created.Checkpoint.ID}, nil
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
	out, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "diff", "--cached", "--name-only", "-z", "--")
	if err != nil {
		return nil, err
	}
	return nulSet(out), nil
}

func trackedPaths(ctx context.Context, repo string) (map[string]bool, error) {
	out, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "ls-files", "-z")
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
	fields := []string{plan.CheckpointID, plan.RepoRoot, plan.WorktreeID, plan.HeadToken, plan.IndexDigest, strconv.Itoa(plan.Counts.StagedOverlap)}
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

func createBackups(repoRoot, backupDir string, changes []change) (backupManifest, error) {
	if err := os.MkdirAll(backupDir, 0o700); err != nil {
		return backupManifest{}, err
	}
	manifest := backupManifest{Entries: make([]backupEntry, 0, len(changes))}
	for i, item := range changes {
		full, err := containedPath(repoRoot, item.Path)
		if err != nil {
			return manifest, err
		}
		info, err := os.Lstat(full)
		entry := backupEntry{Path: item.Path}
		if errors.Is(err, os.ErrNotExist) {
			manifest.Entries = append(manifest.Entries, entry)
			continue
		}
		if err != nil {
			return manifest, err
		}
		entry.Exists, entry.Mode = true, info.Mode()
		backupPath := filepath.Join(backupDir, fmt.Sprintf("%06d", i))
		entry.BackupFile = filepath.Base(backupPath)
		if info.Mode()&os.ModeSymlink != 0 {
			entry.Symlink = true
			target, readErr := os.Readlink(full)
			if readErr != nil {
				return manifest, readErr
			}
			if writeErr := os.WriteFile(backupPath, []byte(target), 0o600); writeErr != nil {
				return manifest, writeErr
			}
		} else if info.Mode().IsRegular() {
			body, readErr := os.ReadFile(full)
			if readErr != nil {
				return manifest, readErr
			}
			if writeErr := os.WriteFile(backupPath, body, 0o600); writeErr != nil {
				return manifest, writeErr
			}
		} else {
			return manifest, fmt.Errorf("restore: unsupported preimage at %s", item.Path)
		}
		manifest.Entries = append(manifest.Entries, entry)
	}
	body, err := json.Marshal(manifest)
	if err != nil {
		return manifest, err
	}
	if err := os.WriteFile(filepath.Join(backupDir, "manifest.json"), body, 0o600); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func applyChanges(ctx context.Context, repoRoot string, changes []change) error {
	deletes := append([]change(nil), changes...)
	sort.Slice(deletes, func(i, j int) bool { return strings.Count(deletes[i].Path, "/") > strings.Count(deletes[j].Path, "/") })
	for _, item := range deletes {
		if item.Before == nil {
			continue
		}
		full, err := containedPath(repoRoot, item.Path)
		if err != nil {
			return err
		}
		if err := os.Remove(full); err != nil && !errors.Is(err, os.ErrNotExist) {
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
		full, err := containedPath(repoRoot, item.Path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return err
		}
		body, err := gitpkg.CatFileBlobLimited(ctx, repoRoot, item.After.OID, 0)
		if err != nil {
			return err
		}
		if item.After.Mode == gitpkg.SymlinkMode {
			tmp := full + ".acd-restore-tmp"
			_ = os.Remove(tmp)
			if err := os.Symlink(string(body), tmp); err != nil {
				return err
			}
			if err := os.Rename(tmp, full); err != nil {
				_ = os.Remove(tmp)
				return err
			}
		} else {
			mode := fs.FileMode(0o644)
			if item.After.Mode == gitpkg.ExecutableFileMode {
				mode = 0o755
			}
			if err := writeAtomic(full, body, mode); err != nil {
				return err
			}
		}
	}
	return nil
}

func rollback(repoRoot, backupDir string, manifest backupManifest) error {
	var combined error
	for i := len(manifest.Entries) - 1; i >= 0; i-- {
		entry := manifest.Entries[i]
		full, err := containedPath(repoRoot, entry.Path)
		if err != nil {
			combined = errors.Join(combined, err)
			continue
		}
		_ = os.Remove(full)
		if !entry.Exists {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			combined = errors.Join(combined, err)
			continue
		}
		body, err := os.ReadFile(filepath.Join(backupDir, entry.BackupFile))
		if err != nil {
			combined = errors.Join(combined, err)
			continue
		}
		if entry.Symlink {
			err = os.Symlink(string(body), full)
		} else {
			err = writeAtomic(full, body, entry.Mode.Perm())
		}
		combined = errors.Join(combined, err)
	}
	return combined
}

func writeAtomic(path string, body []byte, mode fs.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".acd-restore-")
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

func containedPath(root, relative string) (string, error) {
	clean := filepath.ToSlash(filepath.Clean(relative))
	if relative == "" || filepath.IsAbs(relative) || clean != relative || clean == "." || strings.HasPrefix(clean, "../") || strings.ContainsRune(relative, 0) {
		return "", fmt.Errorf("restore: unsafe path %q", relative)
	}
	full := filepath.Join(root, filepath.FromSlash(relative))
	rel, err := filepath.Rel(root, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("restore: path escapes worktree")
	}
	return full, nil
}
