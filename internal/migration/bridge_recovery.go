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
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const bridgeRecoveryManifestName = "migration-bridge-recovery.json"

type bridgeRecoveryManifest struct {
	OperationID string           `json:"operation_id"`
	Retained    []BridgeSnapshot `json:"retained_for_recovery"`
}

// AttachBridgeRecoveries discovers prior rolled-back bridge manifests,
// validates every ref and commit tree, and attaches them to the matching
// repository plan. Planning remains read-only.
func AttachBridgeRecoveries(ctx context.Context, setupRoot string, plans []RepositoryPlan) ([]RepositoryPlan, []string, error) {
	plans = append([]RepositoryPlan(nil), plans...)
	entries, err := os.ReadDir(setupRoot)
	if errors.Is(err, os.ErrNotExist) {
		return plans, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	byWorktree := make(map[string]int, len(plans))
	for index := range plans {
		plans[index].BridgeSnapshots = append([]BridgeSnapshot(nil), plans[index].BridgeSnapshots...)
		byWorktree[plans[index].Record.WorktreeID] = index
	}
	var manifests []string
	seenRefs := make(map[string]bool)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(setupRoot, entry.Name(), bridgeRecoveryManifestName)
		manifest, err := loadBridgeRecoveryManifest(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("migration: read bridge recovery manifest %s: %w", path, err)
		}
		if manifest.OperationID != entry.Name() {
			return nil, nil, fmt.Errorf("migration: bridge recovery operation mismatch in %s", path)
		}
		if len(manifest.Retained) == 0 {
			continue
		}
		for _, snapshot := range manifest.Retained {
			if seenRefs[snapshot.Ref] {
				return nil, nil, fmt.Errorf("migration: duplicate retained bridge ref %s", snapshot.Ref)
			}
			seenRefs[snapshot.Ref] = true
			index, ok := byWorktree[snapshot.WorktreeID]
			if !ok {
				return nil, nil, fmt.Errorf("migration: retained bridge ref %s has no registered worktree", snapshot.Ref)
			}
			plan := &plans[index]
			if plan.Missing || plan.Record.RepositoryID != snapshot.RepositoryID ||
				!central.SameRepoPath(plan.Record.Path, snapshot.Repo) {
				return nil, nil, fmt.Errorf("migration: retained bridge ref %s worktree is unavailable or changed", snapshot.Ref)
			}
			if err := validateBridgeSnapshot(ctx, manifest.OperationID, snapshot); err != nil {
				return nil, nil, err
			}
			plan.BridgeSnapshots = append(plan.BridgeSnapshots, snapshot)
		}
		manifests = append(manifests, path)
	}
	for index := range plans {
		sort.Slice(plans[index].BridgeSnapshots, func(i, j int) bool {
			left, right := plans[index].BridgeSnapshots[i], plans[index].BridgeSnapshots[j]
			if left.CreatedTS == right.CreatedTS {
				return left.Ref < right.Ref
			}
			return left.CreatedTS < right.CreatedTS
		})
	}
	sort.Strings(manifests)
	return plans, manifests, nil
}

func loadBridgeRecoveryManifest(path string) (bridgeRecoveryManifest, error) {
	var manifest bridgeRecoveryManifest
	info, err := os.Lstat(path)
	if err != nil {
		return manifest, err
	}
	if !info.Mode().IsRegular() {
		return manifest, errors.New("manifest is not a regular file")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return manifest, err
	}
	if len(body) > 16<<20 {
		return manifest, errors.New("manifest exceeds 16 MiB")
	}
	if err := json.Unmarshal(body, &manifest); err != nil {
		return manifest, err
	}
	if strings.TrimSpace(manifest.OperationID) == "" {
		return manifest, errors.New("manifest operation id is empty")
	}
	return manifest, nil
}

func validateBridgeSnapshot(ctx context.Context, operationID string, snapshot BridgeSnapshot) error {
	prefix := migrationRefPrefix + operationID + "/"
	if snapshot.WorktreeID == "" || snapshot.Repo == "" || snapshot.CommitOID == "" || snapshot.TreeOID == "" ||
		!strings.HasPrefix(snapshot.Ref, prefix) {
		return fmt.Errorf("migration: invalid retained bridge snapshot %s", snapshot.Ref)
	}
	commitOID, err := gitpkg.RevParse(ctx, snapshot.Repo, snapshot.Ref)
	if err != nil {
		return fmt.Errorf("migration: resolve retained bridge ref %s: %w", snapshot.Ref, err)
	}
	if commitOID != snapshot.CommitOID {
		return fmt.Errorf("migration: retained bridge ref %s points at %s, want %s", snapshot.Ref, commitOID, snapshot.CommitOID)
	}
	treeOID, err := gitpkg.RevParse(ctx, snapshot.Repo, snapshot.CommitOID+"^{tree}")
	if err != nil {
		return fmt.Errorf("migration: resolve retained bridge tree %s: %w", snapshot.Ref, err)
	}
	if treeOID != snapshot.TreeOID {
		return fmt.Errorf("migration: retained bridge ref %s tree is %s, want %s", snapshot.Ref, treeOID, snapshot.TreeOID)
	}
	return nil
}

func importBridgeCheckpoint(ctx context.Context, wt gitpkg.Worktree, db *state.DB, worktreeID string, snapshot BridgeSnapshot) (state.Checkpoint, error) {
	operationID := strings.Split(strings.TrimPrefix(snapshot.Ref, migrationRefPrefix), "/")[0]
	if err := validateBridgeSnapshot(ctx, operationID, snapshot); err != nil {
		return state.Checkpoint{}, err
	}
	now := time.Unix(0, int64(snapshot.CreatedTS*float64(time.Second)))
	if snapshot.CreatedTS <= 0 {
		now = time.Now()
		snapshot.CreatedTS = float64(now.UnixNano()) / float64(time.Second)
	}
	id, err := checkpoint.NewID(now)
	if err != nil {
		return state.Checkpoint{}, err
	}
	commitOID, err := gitpkg.CommitTreeDurable(ctx, wt.Root, snapshot.TreeOID,
		"acd checkpoint "+id+"\n", checkpoint.IdentityName, checkpoint.IdentityEmail)
	if err != nil {
		return state.Checkpoint{}, err
	}
	ref := gitpkg.CheckpointRefPrefix + worktreeID + "/" + id
	cp := state.Checkpoint{ID: id, OperationID: "op-" + id, WorktreeID: worktreeID,
		Reason: state.CheckpointReasonMigrationRecovery, TreeOID: snapshot.TreeOID,
		CommitOID: commitOID, Ref: ref, CreatedTS: snapshot.CreatedTS}
	digestBytes := sha256.Sum256([]byte(strings.Join([]string{
		id, worktreeID, snapshot.Ref, snapshot.CommitOID, snapshot.TreeOID, commitOID,
		strconv.FormatFloat(snapshot.CreatedTS, 'f', -1, 64),
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

// CleanupBridgeRecoveryManifests CAS-deletes only refs proved by their
// manifests, then removes each fully-consumed manifest. It is called only
// after the imported v20 checkpoints and global setup commit are durable.
func CleanupBridgeRecoveryManifests(ctx context.Context, manifestPaths []string) error {
	for _, path := range manifestPaths {
		manifest, err := loadBridgeRecoveryManifest(path)
		if err != nil {
			return err
		}
		for _, snapshot := range manifest.Retained {
			if err := gitpkg.DeletePrivateRefDurable(ctx, snapshot.Repo, migrationRefPrefix, snapshot.Ref, snapshot.CommitOID); err != nil {
				return fmt.Errorf("migration: clean retained bridge ref %s: %w", snapshot.Ref, err)
			}
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			return fmt.Errorf("migration: sync bridge recovery manifest directory: %w", err)
		}
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
