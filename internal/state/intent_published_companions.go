package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// IntentPublishedFormerCompanions follows immutable superseded memberships,
// rather than scanning unrelated published history for possible companions.
func IntentPublishedFormerCompanions(ctx context.Context, d *DB, branch string, generation int64, candidateIDs []string, limit int) ([]IntentCandidate, error) {
	if d == nil || branch == "" || generation < 0 || len(candidateIDs) > IntentCandidateMaxOpenPerPair {
		return nil, errors.New("state: published former companions: invalid input")
	}
	if len(candidateIDs) == 0 || limit <= 0 {
		return nil, nil
	}
	limit = min(limit, IntentCandidateMaxOpenPerPair)
	args := []any{branch, generation, branch, generation}
	for _, id := range candidateIDs {
		args = append(args, id)
	}
	args = append(args, limit)
	rows, err := d.readSQL().QueryContext(ctx, `
SELECT DISTINCT published.id
FROM intent_candidate_events former
JOIN capture_events known ON known.seq=former.event_seq
JOIN capture_events event ON event.path=known.path
JOIN intent_candidate_events active ON active.event_seq=event.seq AND active.membership_state='active'
JOIN intent_candidates published ON published.id=active.candidate_id
WHERE former.membership_state='superseded'
 AND published.branch_ref=? AND published.branch_generation=? AND published.status='published'
 AND event.branch_ref=? AND event.branch_generation=? AND event.state='published'
 AND event.commit_oid=published.published_commit_oid
 AND event.seq=(SELECT MAX(latest.seq) FROM capture_events latest
  WHERE latest.path=known.path AND latest.branch_ref=event.branch_ref
   AND latest.branch_generation=event.branch_generation AND latest.state='published')
 AND former.candidate_id IN (`+strings.TrimSuffix(strings.Repeat("?,", len(candidateIDs)), ",")+`)
ORDER BY published.updated_ts DESC,published.id
LIMIT ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("state: query published former companions: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	readErr := rows.Err()
	_ = rows.Close()
	if readErr != nil {
		return nil, readErr
	}
	var companions []IntentCandidate
	for _, id := range ids {
		candidate, found, err := IntentCandidateByID(ctx, d, id)
		if err != nil {
			return nil, err
		}
		if found {
			companions = append(companions, candidate)
		}
	}
	return companions, nil
}
