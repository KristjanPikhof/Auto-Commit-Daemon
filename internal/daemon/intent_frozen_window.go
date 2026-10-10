package daemon

import (
	"context"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// The eligible loader can omit an unprotected or quiescent target member.
// Prove the exact bounded remainder before overriding singleton aging.
func completeProtectedIntentFrozenWindow(ctx context.Context, db *state.DB, pending []state.CaptureEvent, targetSeqs []int64) (bool, error) {
	if len(pending) == 0 || len(pending) > ai.IntentCandidateCaptureCap ||
		len(targetSeqs) == 0 || len(targetSeqs) > ai.IntentCandidateCaptureCap {
		return false, nil
	}
	first := pending[0]
	if first.BranchRef == "" || first.BranchGeneration <= 0 {
		return false, nil
	}
	wanted := make(map[int64]bool, len(pending))
	for _, event := range pending {
		if event.Seq <= 0 || wanted[event.Seq] || event.State != state.EventStatePending ||
			event.BranchRef != first.BranchRef || event.BranchGeneration != first.BranchGeneration {
			return false, nil
		}
		wanted[event.Seq] = true
	}
	args := make([]any, 0, len(targetSeqs))
	seen := make(map[int64]bool, len(targetSeqs))
	for _, seq := range targetSeqs {
		if seq <= 0 || seen[seq] {
			return false, nil
		}
		seen[seq] = true
		args = append(args, seq)
	}
	for seq := range wanted {
		if !seen[seq] {
			return false, nil
		}
	}
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT event.seq,event.state,event.branch_ref,event.branch_generation,
 EXISTS (SELECT 1 FROM checkpoint_events member JOIN checkpoints checkpoint ON checkpoint.id=member.checkpoint_id
  WHERE member.event_seq=event.seq AND checkpoint.phase='completed' AND checkpoint.retained=1
   AND checkpoint.coverage_complete=1 AND checkpoint.observed_ref=event.branch_ref)
FROM capture_events event WHERE event.seq IN (`+strings.TrimSuffix(strings.Repeat("?,", len(targetSeqs)), ",")+`)`, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	count, remaining := 0, 0
	for rows.Next() {
		var seq, generation int64
		var status, branch string
		var protected bool
		if err := rows.Scan(&seq, &status, &branch, &generation, &protected); err != nil {
			return false, err
		}
		count++
		if branch != first.BranchRef || generation != first.BranchGeneration {
			return false, nil
		}
		switch status {
		case state.EventStatePublished, state.EventStateRecovered:
			if wanted[seq] {
				return false, nil
			}
		case state.EventStatePending:
			if !protected || !wanted[seq] {
				return false, nil
			}
			remaining++
		default:
			return false, nil
		}
	}
	return count == len(targetSeqs) && remaining == len(pending), rows.Err()
}
