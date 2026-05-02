package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const DefaultLiveIndexRepairLimit = 128

type LiveIndexRepairSummary struct {
	Candidates int
	Applied    int
	Skipped    []git.LiveIndexSkip
}

func RepairPublishedLiveIndex(ctx context.Context, repoRoot string, db *state.DB, head string, limit int) (LiveIndexRepairSummary, error) {
	var sum LiveIndexRepairSummary
	if repoRoot == "" || db == nil || head == "" {
		return sum, nil
	}
	if limit <= 0 {
		limit = DefaultLiveIndexRepairLimit
	}
	events, err := recentPublishedEvents(ctx, db, limit)
	if err != nil {
		return sum, err
	}
	slices.Reverse(events)
	for _, ev := range events {
		if !ev.CommitOID.Valid || ev.CommitOID.String == "" {
			continue
		}
		ok, err := git.IsAncestor(ctx, repoRoot, ev.CommitOID.String, head)
		if err != nil || !ok {
			continue
		}
		ops, err := state.LoadCaptureOps(ctx, db, ev.Seq)
		if err != nil {
			return sum, err
		}
		liveOps, skips, err := repairOpsIfHeadAndWorktreeMatch(ctx, repoRoot, ops)
		if err != nil {
			return sum, err
		}
		sum.Skipped = append(sum.Skipped, skips...)
		if len(liveOps) == 0 {
			continue
		}
		sum.Candidates += len(liveOps)
		res, err := git.ReconcileLiveIndex(ctx, repoRoot, liveOps)
		if err != nil {
			return sum, err
		}
		sum.Applied += len(res.Applied)
		sum.Skipped = append(sum.Skipped, res.Skipped...)
	}
	return sum, nil
}

func recentPublishedEvents(ctx context.Context, db *state.DB, limit int) ([]state.CaptureEvent, error) {
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT seq, branch_ref, branch_generation, base_head, operation, path, old_path,
       fidelity, captured_ts, published_ts, state, commit_oid, error, message
FROM capture_events
WHERE state = ? AND commit_oid IS NOT NULL AND commit_oid != ''
ORDER BY seq DESC
LIMIT ?`, state.EventStatePublished, limit)
	if err != nil {
		return nil, fmt.Errorf("daemon: query published events for live-index repair: %w", err)
	}
	defer rows.Close()
	var out []state.CaptureEvent
	for rows.Next() {
		var ev state.CaptureEvent
		if err := rows.Scan(&ev.Seq, &ev.BranchRef, &ev.BranchGeneration, &ev.BaseHead,
			&ev.Operation, &ev.Path, &ev.OldPath, &ev.Fidelity,
			&ev.CapturedTS, &ev.PublishedTS, &ev.State, &ev.CommitOID, &ev.Error, &ev.Message); err != nil {
			return nil, fmt.Errorf("daemon: scan published event for live-index repair: %w", err)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("daemon: iterate published events for live-index repair: %w", err)
	}
	return out, nil
}

func repairOpsIfHeadAndWorktreeMatch(ctx context.Context, repoRoot string, ops []state.CaptureOp) ([]git.LiveIndexOp, []git.LiveIndexSkip, error) {
	var skips []git.LiveIndexSkip
	for _, op := range ops {
		switch op.Op {
		case "create", "modify", "mode":
			if ok, err := headAndWorktreeMatchAfter(ctx, repoRoot, op.Path, op.AfterOID.String, op.AfterMode.String); err != nil {
				return nil, nil, err
			} else if !ok {
				skips = append(skips, git.LiveIndexSkip{Path: op.Path, Reason: "head_or_worktree_mismatch"})
				return nil, skips, nil
			}
		case "delete":
			if ok, err := headAndWorktreeAbsent(ctx, repoRoot, op.Path); err != nil {
				return nil, nil, err
			} else if !ok {
				skips = append(skips, git.LiveIndexSkip{Path: op.Path, Reason: "head_or_worktree_mismatch"})
				return nil, skips, nil
			}
		case "rename":
			if op.OldPath.Valid && op.OldPath.String != "" {
				if ok, err := headAndWorktreeAbsent(ctx, repoRoot, op.OldPath.String); err != nil {
					return nil, nil, err
				} else if !ok {
					skips = append(skips, git.LiveIndexSkip{Path: op.OldPath.String, Reason: "head_or_worktree_mismatch"})
					return nil, skips, nil
				}
			}
			if ok, err := headAndWorktreeMatchAfter(ctx, repoRoot, op.Path, op.AfterOID.String, op.AfterMode.String); err != nil {
				return nil, nil, err
			} else if !ok {
				skips = append(skips, git.LiveIndexSkip{Path: op.Path, Reason: "head_or_worktree_mismatch"})
				return nil, skips, nil
			}
		}
	}
	return liveIndexOpsFromCaptureOps(ops), skips, nil
}

func headAndWorktreeMatchAfter(ctx context.Context, repoRoot, path, oid, mode string) (bool, error) {
	entries, err := git.LsTree(ctx, repoRoot, "HEAD", false, path)
	if err != nil {
		return false, err
	}
	headOK := false
	for _, entry := range entries {
		if entry.Path == path && entry.Type == "blob" && entry.OID == oid && entry.Mode == mode {
			headOK = true
			break
		}
	}
	if !headOK {
		return false, nil
	}
	fi, err := os.Lstat(filepath.Join(repoRoot, path))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	entry, ok, err := hashCandidate(ctx, repoRoot, candidateLike{
		rel:  path,
		full: filepath.Join(repoRoot, path),
		fi:   fi,
	}, walkOpts{maxBytes: DefaultMaxFileBytes})
	if err != nil || !ok {
		return false, err
	}
	return entry.OID == oid && entry.Mode == mode, nil
}

func headAndWorktreeAbsent(ctx context.Context, repoRoot, path string) (bool, error) {
	entries, err := git.LsTree(ctx, repoRoot, "HEAD", false, path)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.Path == path {
			return false, nil
		}
	}
	if _, err := os.Lstat(filepath.Join(repoRoot, path)); err == nil {
		return false, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return true, nil
	} else {
		return false, err
	}
}
