package daemon

import (
	"context"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// A documentation-only remainder may describe behavior already published in
// its own frozen target. Target membership admits bounded baseline lookup; it
// does not establish a semantic relationship or authorize a new assignment.
func discoverIntentPublishedFrozenContext(ctx context.Context, db *state.DB, input IntentCandidateEvaluation) ([]state.IntentCandidate, error) {
	if len(input.Captures) == 0 || len(input.TargetEventSeqs) == 0 || len(input.TargetEventSeqs) > state.IntentCandidateMaxCaptures {
		return nil, nil
	}
	target := make(map[int64]bool, len(input.TargetEventSeqs))
	for _, seq := range input.TargetEventSeqs {
		if seq <= 0 || target[seq] {
			return nil, nil
		}
		target[seq] = true
	}
	offered := make(map[int64]bool, len(input.Captures))
	for _, capture := range input.Captures {
		if !target[capture.Event.Seq] || capture.Event.State != state.EventStatePending ||
			capture.Event.BranchRef != input.BranchRef || capture.Event.BranchGeneration != input.BranchGeneration ||
			intentCaptureRole(capture) != "documentation" {
			return nil, nil
		}
		offered[capture.Event.Seq] = true
		for _, event := range capture.CoveredEvents {
			if !target[event.Seq] || event.State != state.EventStatePending || event.BranchRef != input.BranchRef || event.BranchGeneration != input.BranchGeneration {
				return nil, nil
			}
			offered[event.Seq] = true
		}
	}
	drains, err := state.ActivePublicationDrainsForPair(ctx, db, input.BranchRef, input.BranchGeneration)
	if err != nil {
		return nil, err
	}
	if len(drains) != 1 || drains[0].TargetEventCount != int64(len(target)) {
		return nil, nil
	}
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT member.event_seq,event.branch_ref,event.branch_generation,event.state,
 owner.candidate_id,candidate.status
FROM publication_drain_events member
JOIN capture_events event ON event.seq=member.event_seq
LEFT JOIN intent_candidate_events owner ON owner.event_seq=event.seq AND owner.membership_state='active'
LEFT JOIN intent_candidates candidate ON candidate.id=owner.candidate_id
WHERE member.drain_id=? ORDER BY member.ord LIMIT ?`, drains[0].ID, state.IntentCandidateMaxCaptures+1)
	if err != nil {
		return nil, err
	}
	count := 0
	seen := make(map[int64]bool)
	ids := make([]string, 0)
	known := make(map[string]bool)
	for rows.Next() {
		var seq, generation int64
		var branch, status string
		var id, candidateStatus *string
		if err := rows.Scan(&seq, &branch, &generation, &status, &id, &candidateStatus); err != nil {
			rows.Close()
			return nil, err
		}
		count++
		if !target[seq] || seen[seq] || branch != input.BranchRef || generation != input.BranchGeneration ||
			(status == state.EventStatePending && !offered[seq]) {
			rows.Close()
			return nil, nil
		}
		seen[seq] = true
		if status == state.EventStatePublished && id != nil && candidateStatus != nil && *candidateStatus == state.IntentCandidatePublished && !known[*id] {
			ids = append(ids, *id)
			known[*id] = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if count != len(target) {
		return nil, nil
	}
	var result []state.IntentCandidate
	for _, id := range ids {
		candidate, found, err := state.IntentCandidateByID(ctx, db, id)
		if err != nil {
			return nil, err
		}
		if !found || candidate.BranchRef != input.BranchRef || candidate.BranchGeneration != input.BranchGeneration || len(candidate.Events) == 0 || len(candidate.Events) > state.IntentCandidateMaxCaptures {
			continue
		}
		complete := true
		for _, member := range candidate.Events {
			complete = complete && target[member.EventSeq]
		}
		if complete {
			result = append(result, candidate)
		}
	}
	return result, nil
}
