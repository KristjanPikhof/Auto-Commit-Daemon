package daemon

import (
	"context"
	"errors"
	"fmt"
	"maps"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func finishVersionPaths(changes []finish.Change) []string {
	var paths []string
	for _, change := range changes {
		// Keep renames and deletions as complete final transitions. An absent
		// entry in an older snapshot could also mean an earlier exclusion.
		if change.AtomicGroup == "" && change.After.OID != "" && change.Before.Mode != "160000" && change.After.Mode != "160000" {
			paths = append(paths, change.Path)
		}
	}
	return paths
}

func finishVersionTree(current, snapshot finish.Tree, paths []string) finish.Tree {
	next := finish.CloneTree(current)
	for _, path := range paths {
		if entry, exists := snapshot[path]; exists {
			next[path] = entry
		}
	}
	return next
}

// withFinishVersion proves the pin before reading any recorded path versions.
func withFinishVersion(ctx context.Context, repo string, cp state.FinishCheckpoint, use func(context.Context, finish.Tree) error) error {
	return git.WithLockedPrivateRef(ctx, repo, cp.Ref, cp.CommitOID, func(locked context.Context) error {
		tree, err := git.RevParse(locked, repo, cp.CommitOID+"^{tree}")
		if err != nil || tree != cp.TreeOID {
			return errors.Join(err, state.ErrFinishIdentity)
		}
		snapshot, err := readFinishTree(locked, repo, tree)
		if err != nil {
			return err
		}
		return use(locked, snapshot)
	})
}

func (w *FinishWorker) freezeFinishCheckpoints(ctx context.Context, wt git.Worktree, cp state.Checkpoint, target state.FinishTarget, id string, base, frozen finish.Tree) ([]state.FinishCheckpoint, error) {
	net := finish.NetChanges(base, frozen, "author-1")
	baseTree := target.ExpectedHead
	if baseTree == "" {
		var err error
		baseTree, err = materializeFinishTree(ctx, wt, w.DB, base)
		if err != nil {
			return nil, err
		}
	}
	if err := finishRenameGroups(ctx, wt.Root, baseTree, target.TargetTree, net); err != nil {
		return nil, err
	}
	paths := finishVersionPaths(net)
	if len(paths) == 0 {
		return nil, nil
	}
	checkpoints, err := state.FinishCheckpoints(ctx, w.DB, cp)
	if err != nil {
		return nil, err
	}
	current := base
	var versions []state.FinishCheckpoint
	for _, source := range checkpoints {
		proof := state.FinishCheckpoint{ID: source.ID, Ref: source.Ref, CommitOID: source.CommitOID, TreeOID: source.TreeOID}
		err := withFinishVersion(ctx, wt.Root, proof, func(locked context.Context, snapshot finish.Tree) error {
			next := finishVersionTree(current, snapshot, paths)
			if maps.Equal(current, next) || maps.Equal(next, frozen) {
				return nil
			}
			ref := fmt.Sprintf("%s%s/%s/versions/%d", git.FinishRefPrefix, target.WorktreeID, id, len(versions))
			if _, err := git.EnsurePrivateRefDurable(locked, wt.Root, git.FinishRefPrefix, ref, source.CommitOID); err != nil {
				return err
			}
			proof.Ref = ref
			versions = append(versions, proof)
			current = next
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return versions, nil
}

func finishCheckpointChanges(ctx context.Context, repo, baseTree string, target state.FinishTarget, evidence finish.Evidence) ([]finish.Change, error) {
	paths := finishVersionPaths(evidence.Changes)
	current := evidence.Base
	previous := make(map[string]string)
	var changes []finish.Change
	for index, cp := range target.Checkpoints {
		err := withFinishVersion(ctx, repo, cp, func(_ context.Context, snapshot finish.Tree) error {
			next := finishVersionTree(current, snapshot, paths)
			for _, change := range finish.NetChanges(current, next, "author-1") {
				change.ID = fmt.Sprintf("checkpoint-%d:%s", index, change.Path)
				change.Provenance = cp.ID
				change.BeforeTree = previous[change.Path]
				if change.BeforeTree == "" {
					change.BeforeTree = baseTree
				}
				change.AfterTree = cp.TreeOID
				changes = append(changes, change)
				previous[change.Path] = cp.TreeOID
			}
			current = next
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	for _, change := range finish.NetChanges(current, evidence.Target, "author-1") {
		change.BeforeTree = previous[change.Path]
		if change.BeforeTree == "" {
			change.BeforeTree = baseTree
		}
		change.AfterTree = target.TargetTree
		changes = append(changes, change)
	}
	return changes, nil
}
