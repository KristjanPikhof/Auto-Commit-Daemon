package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const fixActionPreserveFinish = "preserve_interrupted_finish"

func planFinishRecovery(ctx context.Context, conn *sql.DB, plan *fixPlan) error {
	hasFinish, err := sqliteTableExists(ctx, conn, "finish_requests")
	if err != nil || !hasFinish {
		return err
	}
	r, err := state.ReadActiveFinishRequest(ctx, conn, checkpoint.WorktreeID(plan.Repo))
	if errors.Is(err, state.ErrFinishNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if r.State != state.FinishNeedsAttention {
		plan.Unsafe = append(plan.Unsafe, "A finish is still running or prepared; wait for its result before recovery.")
		return nil
	}
	plan.ForceRequired = true
	if !plan.Force {
		plan.Unsafe = append(plan.Unsafe, "An interrupted finish still owns this worktree: "+r.ID)
		plan.Suggestions = append(plan.Suggestions, "Run `acd support recover --force --dry-run` to review preserving the interrupted finish, then `acd support recover --force --yes` to release it. Current files, staging, and branches stay unchanged.")
		return nil
	}
	plan.Actions = append(plan.Actions, fixAction{
		ID:   fixActionPreserveFinish + ":" + r.ID,
		Kind: fixActionPreserveFinish, RequestID: r.ID, Attempt: r.Attempt,
		Description: "preserve the interrupted finish and release its block on new requests",
		Reason:      "Publication may have happened; keep the saved evidence without claiming completion or consuming staging.",
		BranchRef:   r.Target.BranchRef, CheckpointID: r.Target.CheckpointID,
		RequiresForce: true, ArchiveOnly: true, finishRequest: &r,
	})
	plan.Suggestions = append(plan.Suggestions, "After recovery, review `git diff` and `git diff --cached`, then run a new `acd finish` for the current work. The old request remains saved and cannot be retried.")
	return nil
}

func applyFinishRecovery(ctx context.Context, db *state.DB, plan *fixPlan, action *fixAction) error {
	r := action.finishRequest
	if r == nil || r.ID != action.RequestID || r.Attempt != action.Attempt || r.Target.WorktreeID != checkpoint.WorktreeID(plan.Repo) {
		return state.ErrFinishIdentity
	}
	p, err := state.LoadFinishPublication(ctx, db, r.ID)
	if err != nil {
		return err
	}
	if p.GitDir != plan.GitDir {
		return state.ErrFinishIdentity
	}
	cp, err := state.ResolveCheckpoint(ctx, plan.StateDB, r.Target.CheckpointID)
	if err != nil {
		return err
	}
	if cp.TreeOID != r.Target.CheckpointTree || cp.WorktreeID != r.Target.WorktreeID || cp.Partial || cp.Phase != state.CheckpointCompleted {
		return state.ErrFinishIdentity
	}
	// Preserve every frozen tree under its existing immutable private pins.
	// Holding them through the state transition also excludes Git pruning.
	err = git.WithLockedPrivateRef(ctx, plan.Repo, cp.Ref, cp.CommitOID, func(lockedCtx context.Context) error {
		preserve := func(targetCtx context.Context) error {
			return git.WithPreservedFinishPublication(targetCtx, plan.Repo, p, func(chainCtx context.Context) error {
				if err := revalidateFixApplySafety(chainCtx, plan); err != nil {
					return err
				}
				return state.PreserveFinishRequest(chainCtx, db, *r)
			})
		}
		if r.Target.TargetRef == "" {
			return preserve(lockedCtx)
		}
		tree, err := git.RevParse(lockedCtx, plan.Repo, r.Target.TargetCommitOID+"^{tree}")
		if err != nil {
			return err
		}
		if tree != r.Target.TargetTree {
			return state.ErrFinishIdentity
		}
		return git.WithLockedPrivateRef(lockedCtx, plan.Repo, r.Target.TargetRef, r.Target.TargetCommitOID, preserve)
	})
	if err != nil {
		return fmt.Errorf("preserve finish %s: %w", r.ID, err)
	}
	action.Applied, action.RowsChanged, action.State = true, 1, "preserved"
	plan.RowsChanged++
	return nil
}

func verifyFinishRecovery(ctx context.Context, conn *sql.DB, plan *fixPlan) error {
	hasFinish, err := sqliteTableExists(ctx, conn, "finish_requests")
	if err != nil || !hasFinish {
		return err
	}
	r, err := state.ReadActiveFinishRequest(ctx, conn, checkpoint.WorktreeID(plan.Repo))
	if errors.Is(err, state.ErrFinishNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("finish request %s still requires review (%s)", r.ID, r.State)
}
