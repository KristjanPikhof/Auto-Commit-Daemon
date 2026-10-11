package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	DefaultPublishedRetention = 30 * 24 * time.Hour
	BudgetMinimumAge          = 7 * 24 * time.Hour
	DefaultMinimumRetained    = 100
	DefaultContentBudget      = int64(5 << 30)
)

type RetentionSummary struct {
	Pruned         int   `json:"pruned"`
	Retained       int   `json:"retained"`
	ContentBytes   int64 `json:"content_bytes"`
	OverBudget     bool  `json:"over_budget"`
	ProtectedBytes int64 `json:"protected_bytes"`
}

func (s Store) RecoverRetention(ctx context.Context, repoRoot string) error {
	if err := state.CheckRetentionLegacyProof(ctx, s.DB); err != nil {
		return err
	}
	items, err := state.PreparedCheckpointPrunes(ctx, s.DB)
	if err != nil {
		return err
	}
	for _, item := range items {
		var worktree string
		if err := s.DB.ReadSQL().QueryRowContext(ctx, "SELECT worktree_id FROM checkpoints WHERE id=?", item.CheckpointID).Scan(&worktree); err != nil {
			return err
		}
		checkpoints, err := state.RetentionCheckpoints(ctx, s.DB, worktree)
		if err != nil {
			return err
		}
		allowed := false
		retainedRank := 0
		for _, cp := range checkpoints {
			if cp.ID == item.CheckpointID {
				allowed = retainedRank >= DefaultMinimumRetained && cp.Ref == item.Ref && cp.CommitOID == item.CommitOID && (cp.Published || cp.Superseded) && !cp.Unresolved && !cp.LatestComplete && cp.Reason != state.CheckpointReasonPreRestore
			}
			if cp.Retained {
				retainedRank++
			}
		}
		if !allowed {
			_ = state.AdvanceOperation(ctx, s.DB, item.OperationID, state.OperationNeedsAttention, state.OperationNeedsAttention, "checkpoint acquired a new retention obligation")
			return errors.New("checkpoint: interrupted prune acquired a new retention obligation")
		}
		if err := gitpkg.DeletePrivateRefDurable(ctx, repoRoot, gitpkg.CheckpointRefPrefix, item.Ref, item.CommitOID); err != nil {
			_ = state.AdvanceOperation(context.Background(), s.DB, item.OperationID,
				state.OperationNeedsAttention, state.OperationNeedsAttention, "checkpoint retention ref changed")
			return err
		}
		if err := state.CompleteCheckpointPrune(ctx, s.DB, item.OperationID, item.CheckpointID, item.Ref, item.CommitOID); err != nil {
			return err
		}
	}
	return nil
}

func (s Store) ApplyRetention(ctx context.Context, repoRoot, worktreeID string, now time.Time) (RetentionSummary, error) {
	var summary RetentionSummary
	if s.DB == nil {
		return summary, errors.New("checkpoint: retention requires state")
	}
	if now.IsZero() {
		now = time.Now()
	}
	if err := s.RecoverRetention(ctx, repoRoot); err != nil {
		return summary, retentionSafetyError{err}
	}
	checkpoints, err := state.RetentionCheckpoints(ctx, s.DB, worktreeID)
	if err != nil {
		return summary, err
	}
	extraRefs, err := s.retentionEvidenceRefs(ctx, repoRoot, worktreeID, checkpoints)
	if err != nil {
		return summary, retentionSafetyError{err}
	}
	objects, err := s.inventoryRetentionRefs(ctx, repoRoot,
		append(retentionRefs(checkpoints, nil), extraRefs...))
	if err != nil {
		return summary, fmt.Errorf("checkpoint: inventory retained refs: %w", err)
	}
	summary.ContentBytes = inventoryBytes(objects)
	candidates, err := s.retentionCandidates(
		ctx, repoRoot, checkpoints, extraRefs, now, summary.ContentBytes)
	if err != nil {
		return summary, err
	}
	candidateIDs := make(map[string]bool, len(candidates))
	for _, item := range candidates {
		candidateIDs[item.ID] = true
	}
	if len(candidates) > 0 {
		objects, err = s.inventoryRetentionRefs(ctx, repoRoot,
			append(retentionRefs(checkpoints, candidateIDs), extraRefs...))
		if err != nil {
			return summary, fmt.Errorf("checkpoint: inventory retained refs after pruning: %w", err)
		}
		summary.ContentBytes = inventoryBytes(objects)
	}
	protectedRefs := append([]string(nil), extraRefs...)
	for _, item := range checkpoints {
		if !item.Retained || candidateIDs[item.ID] {
			continue
		}
		if (!item.Published && !item.Superseded) || item.Unresolved || item.LatestComplete || item.Reason == state.CheckpointReasonPreRestore {
			protectedRefs = append(protectedRefs, item.Ref)
		}
	}
	protectedObjects, err := s.inventoryRetentionRefs(ctx, repoRoot, protectedRefs)
	if err != nil {
		return summary, fmt.Errorf("checkpoint: inventory protected refs: %w", err)
	}
	summary.ProtectedBytes = inventoryBytes(protectedObjects)
	for _, item := range candidates {
		digest := sha256.Sum256([]byte(item.ID + "\x00" + item.Ref + "\x00" + item.CommitOID))
		operationID, err := state.PrepareCheckpointPrune(ctx, s.DB, item,
			"sha256:"+hex.EncodeToString(digest[:]))
		if err != nil {
			return summary, retentionSafetyError{err}
		}
		if err := gitpkg.DeletePrivateRefDurable(ctx, repoRoot, gitpkg.CheckpointRefPrefix, item.Ref, item.CommitOID); err != nil {
			_ = state.AdvanceOperation(context.Background(), s.DB, operationID,
				state.OperationNeedsAttention, state.OperationNeedsAttention, "checkpoint retention ref changed")
			return summary, retentionSafetyError{err}
		}
		if err := state.CompleteCheckpointPrune(ctx, s.DB, operationID, item.ID, item.Ref, item.CommitOID); err != nil {
			return summary, retentionSafetyError{err}
		}
		summary.Pruned++
	}
	summary.Retained = 0
	for _, item := range checkpoints {
		if item.Retained && !candidateIDs[item.ID] {
			summary.Retained++
		}
	}
	if _, err := state.CompactPrunedCheckpoints(ctx, s.DB, worktreeID, state.CheckpointRetentionBatchLimit); err != nil {
		return summary, retentionSafetyError{err}
	}
	summary.OverBudget = summary.ContentBytes > DefaultContentBudget
	return summary, nil
}

type retentionCandidate struct {
	checkpoint state.RetentionCheckpoint
	expired    bool
}

func (s Store) retentionCandidates(
	ctx context.Context,
	repoRoot string,
	checkpoints []state.RetentionCheckpoint,
	extraRefs []string,
	now time.Time,
	contentBytes int64,
) ([]state.RetentionCheckpoint, error) {
	eligible := make([]retentionCandidate, 0)
	retainedRank := map[string]int{}
	rank := 0
	for _, cp := range checkpoints {
		if cp.Retained {
			retainedRank[cp.ID] = rank
			rank++
		}
	}
	for index := len(checkpoints) - 1; index >= 0; index-- {
		item := checkpoints[index]
		if !item.Retained || (!item.Published && !item.Superseded) || item.Unresolved || item.LatestComplete ||
			retainedRank[item.ID] < DefaultMinimumRetained ||
			item.Reason == state.CheckpointReasonPreRestore {
			continue
		}
		age := now.Sub(time.Unix(0,
			int64(item.CreatedTS*float64(time.Second))))
		if age < BudgetMinimumAge {
			continue
		}
		eligible = append(eligible, retentionCandidate{
			checkpoint: item,
			expired:    age >= DefaultPublishedRetention,
		})
		if len(eligible) == state.CheckpointRetentionBatchLimit {
			break
		}
	}
	if contentBytes <= DefaultContentBudget {
		return expiredRetentionCandidates(eligible), nil
	}
	if len(eligible) == 0 {
		return nil, nil
	}

	// Before the budget is met, the legacy oldest-first loop removes every
	// age-eligible checkpoint it visits. Find the first prefix whose removal
	// brings the exact retained-object union within budget. This preserves the
	// same decision boundary with logarithmic Git traversals instead of one
	// traversal per retained checkpoint.
	firstWithinBudget := -1
	low, high := 1, len(eligible)
	for low <= high {
		middle := low + (high-low)/2
		excluded := make(map[string]bool, middle)
		for _, candidate := range eligible[:middle] {
			excluded[candidate.checkpoint.ID] = true
		}
		objects, err := s.inventoryRetentionRefs(ctx, repoRoot,
			append(retentionRefs(checkpoints, excluded), extraRefs...))
		if err != nil {
			return nil, fmt.Errorf("checkpoint: evaluate retention budget: %w", err)
		}
		if inventoryBytes(objects) <= DefaultContentBudget {
			firstWithinBudget = middle
			high = middle - 1
		} else {
			low = middle + 1
		}
	}
	if firstWithinBudget < 0 {
		firstWithinBudget = len(eligible)
	}
	candidates := make([]state.RetentionCheckpoint, 0, firstWithinBudget)
	for _, candidate := range eligible[:firstWithinBudget] {
		candidates = append(candidates, candidate.checkpoint)
	}
	for _, candidate := range eligible[firstWithinBudget:] {
		if candidate.expired {
			candidates = append(candidates, candidate.checkpoint)
		}
	}
	return candidates, nil
}

func expiredRetentionCandidates(eligible []retentionCandidate) []state.RetentionCheckpoint {
	candidates := make([]state.RetentionCheckpoint, 0)
	for _, candidate := range eligible {
		if candidate.expired {
			candidates = append(candidates, candidate.checkpoint)
		}
	}
	return candidates
}

func retentionRefs(
	checkpoints []state.RetentionCheckpoint,
	excluded map[string]bool,
) []string {
	refs := make([]string, 0, len(checkpoints))
	for _, item := range checkpoints {
		if item.Retained && !excluded[item.ID] {
			refs = append(refs, item.Ref)
		}
	}
	return refs
}

func (s Store) inventoryRetentionRefs(
	ctx context.Context,
	repoRoot string,
	refs []string,
) (map[string]int64, error) {
	if len(refs) == 0 {
		return map[string]int64{}, nil
	}
	if s.retentionInventory != nil {
		return s.retentionInventory(ctx, repoRoot, refs)
	}
	return gitpkg.ReachableObjectSizesForRefs(ctx, repoRoot, refs)
}

func inventoryBytes(objects map[string]int64) int64 {
	var total int64
	for _, size := range objects {
		total += size
	}
	return total
}

// Retain every private proof namespace, including durable refs left by a crash
// before its SQLite journal pointer. Known journal refs must still match exactly.
func (s Store) retentionEvidenceRefs(ctx context.Context, repo, worktree string, checkpoints []state.RetentionCheckpoint) ([]string, error) {
	pins, err := state.RetentionObjectPins(ctx, s.DB, worktree)
	if err != nil {
		return nil, err
	}
	raw, err := gitpkg.RunWithLimit(ctx, gitpkg.RunOpts{Dir: repo}, 8<<20, "for-each-ref", "--format=%(refname) %(objectname) %(symref)", "refs/acd/")
	if err != nil {
		return nil, err
	}
	refs := map[string]string{}
	checkpointRefs := map[string]string{}
	for _, cp := range checkpoints {
		if cp.Retained {
			checkpointRefs[cp.Ref] = cp.CommitOID
		}
	}
	var extras []string
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("checkpoint: private proof ref is symbolic or malformed: %s", line)
		}
		refs[fields[0]] = fields[1]
		if _, known := checkpointRefs[fields[0]]; !known {
			extras = append(extras, fields[0])
		}
	}
	for ref, oid := range checkpointRefs {
		if refs[ref] != oid {
			return nil, fmt.Errorf("checkpoint: retained checkpoint ref %s is missing or moved", ref)
		}
	}
	for _, p := range pins {
		oid, exists := refs[p.Ref]
		if !exists && p.Optional {
			continue
		}
		if !exists || oid != p.CommitOID {
			return nil, fmt.Errorf("checkpoint: retained evidence ref %s is missing or moved; restore its exact saved proof", p.Ref)
		}
	}
	return extras, nil
}
