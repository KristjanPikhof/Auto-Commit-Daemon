package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
	items, err := state.PreparedCheckpointPrunes(ctx, s.DB)
	if err != nil {
		return err
	}
	for _, item := range items {
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
		return summary, err
	}
	checkpoints, err := state.RetentionCheckpoints(ctx, s.DB, worktreeID)
	if err != nil {
		return summary, err
	}
	inventories := make(map[string]map[string]int64)
	objectSizes := make(map[string]int64)
	refCounts := make(map[string]int)
	for _, item := range checkpoints {
		if !item.Retained {
			continue
		}
		objects, err := gitpkg.ReachableObjectSizes(ctx, repoRoot, item.Ref)
		if err != nil {
			return summary, fmt.Errorf("checkpoint: retained ref %s: %w", item.Ref, err)
		}
		inventories[item.ID] = objects
		for oid, size := range objects {
			objectSizes[oid] = size
			refCounts[oid]++
		}
	}
	for oid := range refCounts {
		summary.ContentBytes += objectSizes[oid]
	}
	candidates := make([]state.RetentionCheckpoint, 0)
	for index := len(checkpoints) - 1; index >= 0; index-- {
		item := checkpoints[index]
		if !item.Retained || !item.Published || item.Unresolved || index < DefaultMinimumRetained ||
			item.Reason == state.CheckpointReasonPreRestore {
			continue
		}
		age := now.Sub(time.Unix(0, int64(item.CreatedTS*float64(time.Second))))
		expired := age >= DefaultPublishedRetention
		budgetEligible := summary.ContentBytes > DefaultContentBudget && age >= BudgetMinimumAge
		if !expired && !budgetEligible {
			continue
		}
		candidates = append(candidates, item)
		for oid := range inventories[item.ID] {
			refCounts[oid]--
			if refCounts[oid] == 0 {
				summary.ContentBytes -= objectSizes[oid]
			}
		}
	}
	candidateIDs := make(map[string]bool, len(candidates))
	for _, item := range candidates {
		candidateIDs[item.ID] = true
	}
	protectedObjects := make(map[string]bool)
	for _, item := range checkpoints {
		if !item.Retained || candidateIDs[item.ID] {
			continue
		}
		if !item.Published || item.Unresolved || item.Reason == state.CheckpointReasonPreRestore {
			for oid := range inventories[item.ID] {
				protectedObjects[oid] = true
			}
		}
	}
	for oid := range protectedObjects {
		summary.ProtectedBytes += objectSizes[oid]
	}
	for _, item := range candidates {
		digest := sha256.Sum256([]byte(item.ID + "\x00" + item.Ref + "\x00" + item.CommitOID))
		operationID, err := state.PrepareCheckpointPrune(ctx, s.DB, item,
			"sha256:"+hex.EncodeToString(digest[:]))
		if err != nil {
			return summary, err
		}
		if err := gitpkg.DeletePrivateRefDurable(ctx, repoRoot, gitpkg.CheckpointRefPrefix, item.Ref, item.CommitOID); err != nil {
			_ = state.AdvanceOperation(context.Background(), s.DB, operationID,
				state.OperationNeedsAttention, state.OperationNeedsAttention, "checkpoint retention ref changed")
			return summary, err
		}
		if err := state.CompleteCheckpointPrune(ctx, s.DB, operationID, item.ID, item.Ref, item.CommitOID); err != nil {
			return summary, err
		}
		summary.Pruned++
	}
	summary.Retained = 0
	for _, item := range checkpoints {
		if item.Retained && !candidateIDs[item.ID] {
			summary.Retained++
		}
	}
	summary.OverBudget = summary.ContentBytes > DefaultContentBudget
	return summary, nil
}
