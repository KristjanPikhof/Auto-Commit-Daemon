package state

import (
	"context"
	"errors"
)

// HeldIntentCapturesForPair returns a bounded membership overview without
// loading goal payloads or changing planner state.
func HeldIntentCapturesForPair(ctx context.Context, db *DB, branchRef string, generation int64) (map[int64]bool, error) {
	if db == nil || branchRef == "" || generation < 0 {
		return nil, errors.New("state: invalid held goal scope")
	}
	rows, err := db.readSQL().QueryContext(ctx, `
SELECT membership.event_seq
FROM intent_candidate_events membership
JOIN intent_candidates candidate ON candidate.id=membership.candidate_id
WHERE membership.membership_state='active'
  AND candidate.branch_ref=? AND candidate.branch_generation=?
  AND candidate.status IN ('open','waiting','blocked')
ORDER BY membership.event_seq
LIMIT ?`, branchRef, generation, IntentCandidateMaxCaptures*IntentCandidateMaxOpenPerPair)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	held := make(map[int64]bool)
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, err
		}
		held[seq] = true
	}
	return held, rows.Err()
}
