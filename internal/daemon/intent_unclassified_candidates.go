package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const unclassifiedIntentCompanion = "captured evidence cannot yet explain a meaningful commit goal"

func isUnclassifiedIntentFallbackAssignment(candidate ai.IntentCandidateAssignment) bool {
	// Earlier local WAIT plans retained this unused generic label. It cannot
	// establish an approved goal or authorize publication.
	return candidate.Readiness == ai.IntentCandidateWait &&
		(candidate.Subject == "" || candidate.Subject == "Update files") && candidate.Body == "" &&
		candidate.Purpose == "retain dependency component until its goal is known" &&
		containsIntentString(candidate.MissingCompanions, unclassifiedIntentCompanion) &&
		(candidate.GroupingReason == "protected dependency component needs a meaningful goal message" ||
			candidate.GroupingReason == "bounded fallback requires planner review")
}

// A local evidence partition protects ownership; it has not established a
// semantic goal. Release its planning boundary only when the complete durable
// membership is reoffered and protected. Saving a corrected goal supersedes
// old membership through the normal state writer without changing captures.
func unclassifiedIntentCandidatesForRepartition(ctx context.Context, db *state.DB, input *IntentCandidateEvaluation, candidates []state.IntentCandidate, allCaptures []IntentCandidateCapture) (map[string]bool, error) {
	eligible := make(map[string]state.IntentCandidate)
	if len(input.PriorFindings) >= ai.IntentPriorFindingCap {
		return nil, nil
	}
	offered := map[int64]int64{}
	for _, capture := range allCaptures {
		offered[capture.Event.Seq] = capture.Event.Seq
		for _, covered := range capture.CoveredEvents {
			offered[covered.Seq] = capture.Event.Seq
		}
	}
	for _, candidate := range candidates {
		if candidate.Status != state.IntentCandidateWaiting || candidate.Readiness != state.IntentReadinessWait ||
			candidate.BranchRef != input.BranchRef || candidate.BranchGeneration != input.BranchGeneration ||
			candidate.PublishedCommitOID.Valid || candidate.SoftPublicationDeadline.Valid ||
			candidate.AtomicityStatus.String != string(ai.IntentAtomicityPending) ||
			(candidate.VerificationStatus.String != "not_required" && candidate.VerificationStatus.String != "pending") || len(candidate.Events) == 0 || len(candidate.Events) > state.IntentCandidateMaxCaptures {
			continue
		}
		complete := true
		for _, member := range candidate.Events {
			complete = complete && offered[member.EventSeq] != 0
		}
		if complete {
			eligible[candidate.ID] = candidate
		}
	}
	if len(eligible) == 0 {
		return nil, nil
	}
	attention, _, err := state.MetaGet(ctx, db, MetaKeyBranchTransitionNeedsAttention)
	if err != nil || attention != "" {
		return nil, err
	}
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT resolved_plan_json,resolution_mode,updated_ts FROM intent_plan_runs
WHERE branch_ref=? AND branch_generation=? AND completed=1
 AND resolved_plan_json IS NOT NULL
ORDER BY updated_ts DESC,fingerprint LIMIT 32`, input.BranchRef, input.BranchGeneration)
	if err != nil {
		return nil, err
	}
	proven := map[string]bool{}
	seen := map[string]bool{}
	remainingBytes := 4 * state.IntentResolvedPlanJSONCap
	for rows.Next() {
		var raw string
		var mode sql.NullString
		var updated float64
		if err := rows.Scan(&raw, &mode, &updated); err != nil {
			rows.Close()
			return nil, err
		}
		if len(raw) > state.IntentResolvedPlanJSONCap || len(raw) > remainingBytes {
			// A skipped newer record cannot authorize an older boundary.
			break
		}
		remainingBytes -= len(raw)
		var resolved resolvedIntentPlanRun
		if json.Unmarshal([]byte(raw), &resolved) != nil || resolved.Plan.ProtocolVersion != ai.IntentPlannerProtocolV2 {
			continue
		}
		for _, assignment := range resolved.Plan.Candidates {
			candidate, exists := eligible[assignment.CandidateID]
			if !exists || seen[candidate.ID] {
				continue
			}
			// The newest native assignment is decisive. An older fallback
			// cannot override a later purposeful WAIT or READY proposal.
			seen[candidate.ID] = true
			if !isUnclassifiedIntentFallbackAssignment(assignment) ||
				(mode.String != "evidence_partition" && mode.String != "dependent_message_fallback" && mode.String != "waiting_semantic_retry") {
				continue
			}
			canonicalState := candidate.Purpose == "retain dependency component until its goal is known" &&
				containsIntentString(splitIntentSummary(candidate.MissingCompanions), unclassifiedIntentCompanion)
			if !canonicalState && updated <= candidate.UpdatedTS {
				continue
			}
			// The controlled constructor markers and local run mode establish
			// fallback provenance; a provider name or purpose alone cannot.
			members := map[int64]bool{}
			for _, member := range candidate.Events {
				members[offered[member.EventSeq]] = true
			}
			var seqs []int64
			for seq := range members {
				seqs = append(seqs, seq)
			}
			if intentSeqMembershipKey(assignment.SelectedSeqs) == intentSeqMembershipKey(seqs) {
				proven[candidate.ID] = true
			}
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var ids []string
	for id := range proven {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		protected, err := intentCandidateReviewProtected(ctx, db, *input, eligible[id])
		if err != nil {
			return nil, err
		}
		if !protected || strings.Contains(eligible[id].MissingCompanions, "hard dependency closure") || !reofferIntentCandidateMembers(input, eligible[id], allCaptures) {
			delete(proven, id)
		}
	}
	return proven, nil
}

// Add only recorded pending members already inside the frozen target. A
// partial view stays context; it cannot silently discard or widen membership.
func reofferIntentCandidateMembers(input *IntentCandidateEvaluation, candidate state.IntentCandidate, allCaptures []IntentCandidateCapture) bool {
	known := map[int64]bool{}
	for _, capture := range input.Captures {
		known[capture.Event.Seq] = true
		for _, covered := range capture.CoveredEvents {
			known[covered.Seq] = true
		}
	}
	target := map[int64]bool{}
	for _, seq := range input.TargetEventSeqs {
		target[seq] = true
	}
	var added []IntentCandidateCapture
	for _, member := range candidate.Events {
		if known[member.EventSeq] {
			continue
		}
		if !target[member.EventSeq] {
			return false
		}
		found := false
		for _, capture := range allCaptures {
			matches := capture.Event.Seq == member.EventSeq
			for _, covered := range capture.CoveredEvents {
				matches = matches || covered.Seq == member.EventSeq
			}
			if !matches {
				continue
			}
			if capture.Event.State != state.EventStatePending || capture.Event.BranchRef != input.BranchRef || capture.Event.BranchGeneration != input.BranchGeneration || !target[capture.Event.Seq] {
				return false
			}
			for _, covered := range capture.CoveredEvents {
				if covered.State != state.EventStatePending || covered.BranchRef != input.BranchRef || covered.BranchGeneration != input.BranchGeneration || !target[covered.Seq] {
					return false
				}
			}
			added = append(added, capture)
			known[capture.Event.Seq] = true
			for _, covered := range capture.CoveredEvents {
				known[covered.Seq] = true
			}
			found = true
			break
		}
		if !found {
			return false
		}
	}
	if len(input.Captures)+len(added) > ai.IntentCandidateCaptureCap {
		return false
	}
	input.Captures = append(input.Captures, added...)
	sort.Slice(input.Captures, func(i, j int) bool { return input.Captures[i].Event.Seq < input.Captures[j].Event.Seq })
	return true
}
