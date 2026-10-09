package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// Unknown response-local IDs ask for another plan. A joined Git or ownership
// failure must not become a semantic wait just because one leaf has this code.
func intentPlanHasUnknownCandidateDependency(err error) bool {
	if validation, ok := err.(*ai.IntentPlanV2ValidationError); ok {
		if len(validation.Findings) == 0 {
			return false
		}
		for _, finding := range validation.Findings {
			if finding.Gate != ai.IntentAtomicityDependency || finding.Code != "candidate_dependency_unknown" {
				return false
			}
		}
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !intentPlanHasUnknownCandidateDependency(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return intentPlanHasUnknownCandidateDependency(wrapped.Unwrap())
	}
	return false
}

// Legacy partial preservation kept a dependent after dropping its rejected
// prerequisite. Match the complete error against the actual bounded saved
// plan, rather than granting authority to an error string alone.
func publicationDrainUnknownPlanDependency(ctx context.Context, db *state.DB, drain state.PublicationDrain) (bool, error) {
	if drain.Phase != state.PublicationDrainNeedsAction || drain.ReasonCode != "publication_failed" ||
		drain.ReasonEvidence != "" || drain.CommitStrategy != string(ai.CommitStrategyIntent) ||
		drain.LastError == "" || len(drain.EventSeqs) == 0 || len(drain.EventSeqs) > state.IntentCandidateMaxCaptures {
		return false, nil
	}
	var safe bool
	if err := db.ReadSQL().QueryRowContext(ctx, `SELECT
 NOT EXISTS(SELECT 1 FROM self_publications WHERE branch_ref=? AND branch_generation=? AND phase IN ('prepared','git_applied'))
 AND NOT EXISTS(SELECT 1 FROM intent_repairs WHERE branch_ref=? AND branch_generation=? AND status IN ('prepared','git_applied'))
 AND NOT EXISTS(SELECT 1 FROM operations WHERE worktree_id=? AND status IN ('prepared','active'))
 AND (SELECT COUNT(*) FROM publication_drain_events target JOIN capture_events event ON event.seq=target.event_seq
  WHERE target.drain_id=? AND event.state='pending' AND event.branch_ref=? AND event.branch_generation=?
   AND EXISTS(SELECT 1 FROM checkpoint_events member JOIN checkpoints checkpoint ON checkpoint.id=member.checkpoint_id
    WHERE member.event_seq=event.seq AND checkpoint.phase='completed' AND checkpoint.retained=1
     AND checkpoint.coverage_complete=1 AND checkpoint.observed_ref=event.branch_ref))=?`,
		drain.BranchRef, drain.BranchGeneration, drain.BranchRef, drain.BranchGeneration, drain.WorktreeID,
		drain.ID, drain.BranchRef, drain.BranchGeneration, len(drain.EventSeqs)).Scan(&safe); err != nil || !safe {
		return false, err
	}
	candidateRows, err := db.ReadSQL().QueryContext(ctx, `SELECT id,status,readiness FROM intent_candidates
WHERE branch_ref=? AND branch_generation=? ORDER BY id LIMIT ?`, drain.BranchRef, drain.BranchGeneration, state.IntentCandidateMaxOpenPerPair+1)
	if err != nil {
		return false, err
	}
	known := make(map[string]ai.IntentCandidateSummary)
	for candidateRows.Next() {
		var summary ai.IntentCandidateSummary
		var readiness string
		if err := candidateRows.Scan(&summary.CandidateID, &summary.Status, &readiness); err != nil {
			candidateRows.Close()
			return false, err
		}
		summary.Ready = readiness == state.IntentReadinessReady
		known[summary.CandidateID] = summary
	}
	err = candidateRows.Err()
	candidateRows.Close()
	if err != nil || len(known) > state.IntentCandidateMaxOpenPerPair {
		return false, err
	}
	target := make(map[int64]bool, len(drain.EventSeqs))
	for _, seq := range drain.EventSeqs {
		target[seq] = true
	}
	rows, err := db.ReadSQL().QueryContext(ctx, `SELECT resolved_plan_json,preserved_groups
FROM intent_plan_runs WHERE branch_ref=? AND branch_generation=? AND completed=0
 AND resolved_plan_json IS NOT NULL AND updated_ts>=? AND updated_ts<=?
ORDER BY updated_ts DESC,fingerprint LIMIT 16`, drain.BranchRef, drain.BranchGeneration, drain.LastProgressTS, drain.UpdatedTS)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw sql.NullString
		var memberships string
		if err := rows.Scan(&raw, &memberships); err != nil {
			return false, err
		}
		var stored resolvedIntentPlanRun
		var groups [][]int64
		if !raw.Valid || len(raw.String) > state.IntentResolvedPlanJSONCap ||
			json.Unmarshal([]byte(raw.String), &stored) != nil || len(stored.Continuations) > 0 ||
			json.Unmarshal([]byte(memberships), &groups) != nil || len(groups) == 0 ||
			!reflect.DeepEqual(groups, intentAssignmentMembership(stored.Plan.Candidates)) {
			continue
		}
		req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
		within := true
		seen := make(map[int64]bool)
		listed := make(map[string]bool)
		for _, candidate := range stored.Plan.Candidates {
			for _, seq := range candidate.SelectedSeqs {
				within = within && target[seq] && !seen[seq]
				seen[seq] = true
				req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: seq})
			}
			for _, dependency := range candidate.DependsOnCandidates {
				if summary, found := known[dependency]; found && !listed[dependency] {
					req.Candidates = append(req.Candidates, summary)
					listed[dependency] = true
				}
			}
		}
		if !within {
			continue
		}
		validationErr := ai.ValidateIntentPlanV2(req, stored.Plan)
		if intentPlanHasUnknownCandidateDependency(validationErr) && validationErr.Error() == drain.LastError {
			return true, nil
		}
	}
	return false, rows.Err()
}

func RecoverUnknownIntentDependencyPublicationDrain(ctx context.Context, repo string, db *state.DB, drain state.PublicationDrain, now time.Time) (*state.PublicationDrain, error) {
	proved, err := publicationDrainUnknownPlanDependency(ctx, db, drain)
	if err != nil || !proved {
		return nil, err
	}
	resumed, err := ResumePublicationDrainCheckpointing(ctx, repo, db, drain, now)
	if err != nil || resumed.Phase == state.PublicationDrainNeedsAction {
		return nil, err
	}
	return &resumed, nil
}
