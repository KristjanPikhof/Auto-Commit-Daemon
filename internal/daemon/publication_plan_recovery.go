package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
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
		output := make(map[string]bool)
		for _, candidate := range stored.Plan.Candidates {
			output[candidate.CandidateID] = true
		}
		for _, candidate := range stored.Plan.Candidates {
			for _, seq := range candidate.SelectedSeqs {
				within = within && target[seq] && !seen[seq]
				seen[seq] = true
				req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: seq})
			}
			for _, dependency := range candidate.DependsOnCandidates {
				if output[dependency] || listed[dependency] {
					continue
				}
				if len(listed) >= state.IntentCandidateMaxOpenPerPair {
					within = false
					break
				}
				listed[dependency] = true
				prior, found, err := state.IntentCandidateByID(ctx, db, dependency)
				if err != nil {
					return false, err
				}
				if found && prior.BranchRef == drain.BranchRef && prior.BranchGeneration == drain.BranchGeneration {
					req.Candidates = append(req.Candidates, ai.IntentCandidateSummary{
						CandidateID: prior.ID, Status: prior.Status,
						Ready: prior.Readiness == state.IntentReadinessReady,
					})
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
		// Ordering a proven saved DAG changes no membership or readiness.
		// Reopen only when that exact repair passes the complete validator.
		if validationErr != nil && validationErr.Error() == drain.LastError {
			repaired := cloneIntentPlanV2(stored.Plan)
			repaired.Candidates = stableTopologicalIntentCandidates(repaired.Candidates)
			if ai.ValidateIntentPlanV2(req, repaired) == nil {
				return true, nil
			}
		}
	}
	return false, rows.Err()
}

// A saved local baseline could use continuation IDs with the old ownership
// context. Reopen only a protected target whose named prerequisite is already
// published and whose exact recorded result still exists in HEAD.
func publicationDrainPublishedPlanDependency(ctx context.Context, repo string, db *state.DB, drain state.PublicationDrain) (bool, error) {
	if drain.Phase != state.PublicationDrainNeedsAction || drain.ReasonCode != "publication_failed" ||
		drain.CommitStrategy != string(ai.CommitStrategyIntent) || len(drain.EventSeqs) == 0 || len(drain.EventSeqs) > state.IntentCandidateMaxCaptures {
		return false, nil
	}
	const format = "intent planner v2: hard_dependency_undeclared: hard dependency %d -> %d crosses candidates without depends_on_candidates"
	var from, to int64
	if n, err := fmt.Sscanf(drain.LastError, format, &from, &to); err != nil || n != 2 || from <= 0 || to <= 0 || fmt.Sprintf(format, from, to) != drain.LastError {
		const waitingFormat = "intent planner v2: dependency_not_ready: ready candidate depends on waiting persisted candidate %q"
		var id string
		if n, err := fmt.Sscanf(drain.LastError, waitingFormat, &id); err != nil || n != 1 || fmt.Sprintf(waitingFormat, id) != drain.LastError {
			return false, nil
		}
		err := db.ReadSQL().QueryRowContext(ctx, `SELECT edge.prerequisite_seq,edge.dependent_seq
FROM intent_capture_dependencies edge JOIN capture_events event ON event.seq=edge.prerequisite_seq
JOIN intent_candidates candidate ON candidate.published_commit_oid=event.commit_oid
JOIN publication_drain_events target ON target.event_seq=edge.dependent_seq
WHERE candidate.id=? AND candidate.status IN ('published','superseded') AND event.state='published'
 AND candidate.branch_ref=? AND candidate.branch_generation=?
 AND edge.branch_ref=candidate.branch_ref AND edge.branch_generation=candidate.branch_generation
 AND edge.strength='hard' AND target.drain_id=? ORDER BY edge.prerequisite_seq DESC LIMIT 1`,
			id, drain.BranchRef, drain.BranchGeneration, drain.ID).Scan(&from, &to)
		if err == sql.ErrNoRows {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
	var safe bool
	if err := db.ReadSQL().QueryRowContext(ctx, `SELECT
 EXISTS(SELECT 1 FROM intent_capture_dependencies edge JOIN capture_events prerequisite ON prerequisite.seq=edge.prerequisite_seq
  JOIN publication_drain_events target ON target.event_seq=edge.dependent_seq
  WHERE edge.branch_ref=? AND edge.branch_generation=? AND edge.prerequisite_seq=? AND edge.dependent_seq=?
   AND edge.strength='hard' AND prerequisite.state='published' AND prerequisite.branch_ref=edge.branch_ref
   AND prerequisite.branch_generation=edge.branch_generation AND target.drain_id=?)
 AND NOT EXISTS(SELECT 1 FROM self_publications WHERE branch_ref=? AND branch_generation=? AND phase IN ('prepared','git_applied'))
 AND NOT EXISTS(SELECT 1 FROM intent_repairs WHERE branch_ref=? AND branch_generation=? AND status IN ('prepared','git_applied'))
 AND NOT EXISTS(SELECT 1 FROM operations WHERE worktree_id=? AND status IN ('prepared','active'))
 AND (SELECT COUNT(*) FROM publication_drain_events target JOIN capture_events event ON event.seq=target.event_seq
  WHERE target.drain_id=? AND event.state='pending' AND event.branch_ref=? AND event.branch_generation=?
   AND EXISTS(SELECT 1 FROM checkpoint_events member JOIN checkpoints checkpoint ON checkpoint.id=member.checkpoint_id
    WHERE member.event_seq=event.seq AND checkpoint.phase='completed' AND checkpoint.retained=1
     AND checkpoint.coverage_complete=1 AND checkpoint.observed_ref=event.branch_ref))=?`,
		drain.BranchRef, drain.BranchGeneration, from, to, drain.ID,
		drain.BranchRef, drain.BranchGeneration, drain.BranchRef, drain.BranchGeneration, drain.WorktreeID,
		drain.ID, drain.BranchRef, drain.BranchGeneration, len(drain.EventSeqs)).Scan(&safe); err != nil || !safe {
		return false, err
	}
	ops, err := state.LoadCaptureOpsBounded(ctx, db, from, state.IntentCandidateMaxCaptures)
	if err != nil || len(ops) == 0 {
		return false, err
	}
	head, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		return false, err
	}
	_, proven, err := gitpkg.ProvePublicationAtHEAD(ctx, repo, "", head, publicationProofOps(ops), gitpkg.PublicationProofPolicy{MissingRefIsMismatch: true})
	return proven, err
}

func RecoverUnknownIntentDependencyPublicationDrain(ctx context.Context, repo string, db *state.DB, drain state.PublicationDrain, now time.Time) (*state.PublicationDrain, error) {
	proved, err := publicationDrainUnknownPlanDependency(ctx, db, drain)
	if err == nil && !proved {
		proved, err = publicationDrainPublishedPlanDependency(ctx, repo, db, drain)
	}
	if err != nil || !proved {
		return nil, err
	}
	resumed, err := ResumePublicationDrainCheckpointing(ctx, repo, db, drain, now)
	if err != nil || resumed.Phase == state.PublicationDrainNeedsAction {
		return nil, err
	}
	return &resumed, nil
}
