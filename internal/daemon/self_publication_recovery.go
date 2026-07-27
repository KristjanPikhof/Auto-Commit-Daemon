package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	selfPublicationRecoveryLimit       = 50
	selfPublicationRecoveryOutputLimit = 64 << 10
)

// ErrSelfPublicationRecoveryAmbiguous identifies durable publication evidence
// that cannot be completed or abandoned without guessing about an external Git
// or SQLite transition.
var ErrSelfPublicationRecoveryAmbiguous = errors.New(
	"daemon: self-publication recovery is ambiguous")

// SelfPublicationRecoveryResult records one restart-recovery decision.
type SelfPublicationRecoveryResult struct {
	ID        string
	Phase     string
	Outcome   string
	TargetOID string
}

// SelfPublicationRecoverySummary is bounded to
// selfPublicationRecoveryLimit rows per invocation. FinalTargetOID is the last
// exactly proved commit completed by this pass and can be adopted by the run
// loop without classifying it as an external transition.
type SelfPublicationRecoverySummary struct {
	Inspected      int
	Completed      int
	Abandoned      int
	HasMore        bool
	FinalTargetOID string
	Results        []SelfPublicationRecoveryResult
}

// RecoverSelfPublications converges interrupted publication journal rows. It
// mutates only the journal for a definitely-not-applied prepared attempt, or
// atomically completes exact event/candidate ownership for a definitely-applied
// target. Any ambiguous Git or SQLite state fails closed.
func RecoverSelfPublications(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	cctx CaptureContext,
	opts ReplayOpts,
) (SelfPublicationRecoverySummary, error) {
	var summary SelfPublicationRecoverySummary
	if ctx == nil {
		ctx = context.Background()
	}
	if repoRoot == "" || db == nil {
		return summary, errors.New(
			"daemon: recover self-publications: repo and state db are required")
	}
	if cctx.BranchRef == "" || cctx.BranchGeneration < 1 {
		return summary, errors.New(
			"daemon: recover self-publications: active branch context is required")
	}
	if err := ctx.Err(); err != nil {
		return summary, err
	}

	limit := opts.Limit
	if limit <= 0 || limit > selfPublicationRecoveryLimit {
		limit = selfPublicationRecoveryLimit
	}
	publications, hasMore, err := recoverableSelfPublicationsForPair(
		ctx, db, cctx.BranchRef, cctx.BranchGeneration,
		limit)
	if err != nil {
		return summary, fmt.Errorf(
			"daemon: load recoverable self-publications: %w", err)
	}
	summary.HasMore = hasMore
	for _, publication := range publications {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		summary.Inspected++
		result, recoverErr := recoverSelfPublication(
			ctx, repoRoot, db, publication)
		summary.Results = append(summary.Results, result)
		if recoverErr != nil {
			return summary, recoverErr
		}
		switch result.Outcome {
		case state.SelfPublicationCompleted:
			summary.Completed++
			summary.FinalTargetOID = result.TargetOID
		case state.SelfPublicationAbandoned:
			summary.Abandoned++
		}
	}
	return summary, nil
}

func recoverableSelfPublicationsForPair(
	ctx context.Context,
	db *state.DB,
	branchRef string,
	branchGeneration int64,
	limit int,
) ([]state.SelfPublication, bool, error) {
	if limit <= 0 || limit > selfPublicationRecoveryLimit {
		limit = selfPublicationRecoveryLimit
	}
	rows, err := db.SQL().QueryContext(ctx, `
SELECT id
FROM self_publications
WHERE branch_ref=? AND branch_generation=?
  AND phase IN ('prepared','git_applied')
ORDER BY created_ts, id
LIMIT ?`, branchRef, branchGeneration, limit)
	if err != nil {
		return nil, false, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, false, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, false, err
	}
	if err := rows.Close(); err != nil {
		return nil, false, err
	}
	var hasMore bool
	if len(ids) == limit {
		if err := db.SQL().QueryRowContext(ctx, `
SELECT EXISTS(
	SELECT 1
	FROM self_publications
	WHERE branch_ref=? AND branch_generation=?
	  AND phase IN ('prepared','git_applied')
	  AND (created_ts, id) > (
	      SELECT created_ts, id
	      FROM self_publications
	      WHERE id=?
	  )
)`, branchRef, branchGeneration, ids[len(ids)-1]).Scan(&hasMore); err != nil {
			return nil, false, err
		}
	}
	publications := make([]state.SelfPublication, 0, len(ids))
	for _, id := range ids {
		publication, ok, err := state.SelfPublicationByID(ctx, db, id)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf(
				"self-publication %s changed during recovery load", id)
		}
		publications = append(publications, publication)
	}
	return publications, hasMore, nil
}

func recoverSelfPublication(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	publication state.SelfPublication,
) (SelfPublicationRecoveryResult, error) {
	result := SelfPublicationRecoveryResult{
		ID: publication.ID, Phase: publication.Phase,
		TargetOID: publication.TargetCommitOID,
	}
	refOID, refExists, err := resolveSelfPublicationRef(
		ctx, repoRoot, publication.BranchRef)
	if err != nil {
		return result, ambiguousSelfPublication(publication,
			"resolve literal branch ref", err)
	}

	if publication.Phase == state.SelfPublicationPrepared {
		sourceMatches := refExists && refOID == publication.SourceHead
		if publication.SourceHead == "" {
			sourceMatches = !refExists
		}
		if sourceMatches {
			changed := false
			abandon := func() error {
				var abandonErr error
				changed, abandonErr = state.AbandonSelfPublication(
					ctx, db, publication,
					"restart proved publication target was not applied",
					selfPublicationNow())
				return abandonErr
			}
			var abandonErr error
			if publication.SourceHead == "" {
				abandonErr = git.WithLockedAbsentRef(
					ctx, repoRoot, publication.BranchRef, abandon)
			} else {
				abandonErr = git.WithLockedExpectedRef(
					ctx, repoRoot, publication.BranchRef,
					publication.SourceHead, abandon)
			}
			if abandonErr != nil {
				return result, ambiguousSelfPublication(publication,
					"protect definitely-unapplied abandon", abandonErr)
			}
			if !changed {
				return result, ambiguousSelfPublication(publication,
					"prepared journal changed before abandon", nil)
			}
			result.Outcome = state.SelfPublicationAbandoned
			return result, nil
		}
		if !refExists || refOID != publication.TargetCommitOID {
			return result, ambiguousSelfPublication(publication,
				"prepared publication branch is neither source nor target", nil)
		}
		if err := proveSelfPublication(
			ctx, repoRoot, db, publication); err != nil {
			return result, err
		}
		changed, markErr := state.MarkSelfPublicationGitApplied(
			ctx, db, publication, selfPublicationNow())
		if markErr != nil {
			return result, fmt.Errorf(
				"daemon: promote recovered self-publication %s: %w",
				publication.ID, markErr)
		}
		if !changed {
			return result, ambiguousSelfPublication(publication,
				"prepared journal changed before Git-applied promotion", nil)
		}
		publication.Phase = state.SelfPublicationGitApplied
	}
	if publication.Phase != state.SelfPublicationGitApplied {
		return result, ambiguousSelfPublication(publication,
			"unexpected recoverable phase "+publication.Phase, nil)
	}
	if !refExists || refOID != publication.TargetCommitOID {
		return result, ambiguousSelfPublication(publication,
			"Git-applied publication branch does not equal target", nil)
	}
	if err := proveSelfPublication(
		ctx, repoRoot, db, publication); err != nil {
		return result, err
	}
	message, err := selfPublicationCommitMessage(
		ctx, repoRoot, publication.TargetCommitOID)
	if err != nil {
		return result, ambiguousSelfPublication(publication,
			"read target commit message", err)
	}
	completion := publication.Completion
	completion.Message = sql.NullString{
		String: message, Valid: strings.TrimSpace(message) != "",
	}
	completed := false
	err = git.WithLockedExpectedRef(
		ctx, repoRoot, publication.BranchRef,
		publication.TargetCommitOID,
		func() error {
			var completeErr error
			completed, completeErr = state.CompleteSelfPublication(
				ctx, db, publication, completion)
			return completeErr
		})
	if err != nil {
		return result, fmt.Errorf(
			"daemon: complete protected self-publication %s: %w",
			publication.ID, err)
	}
	if !completed {
		latest, ok, loadErr := state.SelfPublicationByID(
			ctx, db, publication.ID)
		if loadErr != nil {
			return result, fmt.Errorf(
				"daemon: reload recovered self-publication %s: %w",
				publication.ID, loadErr)
		}
		if !ok || latest.Phase != state.SelfPublicationCompleted {
			return result, ambiguousSelfPublication(publication,
				"journal changed before completion", nil)
		}
	}
	result.Outcome = state.SelfPublicationCompleted
	return result, nil
}

func proveSelfPublication(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	publication state.SelfPublication,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	commitOID, err := git.RevParse(
		ctx, repoRoot, publication.TargetCommitOID+"^{commit}")
	if err != nil || commitOID != publication.TargetCommitOID {
		return ambiguousSelfPublication(publication,
			"target commit is missing or does not resolve exactly", err)
	}
	treeOID, err := resolveTreeOID(
		ctx, repoRoot, publication.TargetCommitOID)
	if err != nil || treeOID != publication.TargetTreeOID {
		return ambiguousSelfPublication(publication,
			"target tree does not match journal", err)
	}
	parents, err := selfPublicationParents(
		ctx, repoRoot, publication.TargetCommitOID)
	if err != nil {
		return ambiguousSelfPublication(publication,
			"inspect target parents", err)
	}
	if publication.SourceHead == "" {
		if len(parents) != 0 {
			return ambiguousSelfPublication(publication,
				"initial target unexpectedly has a parent", nil)
		}
	} else {
		if len(parents) != 1 || parents[0] != publication.SourceHead {
			return ambiguousSelfPublication(publication,
				"target is a merge or has the wrong parent", nil)
		}
		sourceOID, sourceErr := git.RevParse(
			ctx, repoRoot, publication.SourceHead+"^{commit}")
		if sourceErr != nil || sourceOID != publication.SourceHead {
			return ambiguousSelfPublication(publication,
				"source commit is missing", sourceErr)
		}
	}
	refs, err := selfPublicationRefsPointingAt(
		ctx, repoRoot, publication.TargetCommitOID)
	if err != nil {
		return ambiguousSelfPublication(publication,
			"inspect refs pointing at target", err)
	}
	if len(refs) != 1 || refs[0] != publication.BranchRef {
		return ambiguousSelfPublication(publication,
			"target has unexpected branch, remote, or tag ownership", nil)
	}
	if err := verifySelfPublicationMembership(
		ctx, db, publication); err != nil {
		return ambiguousSelfPublication(publication,
			"journal membership no longer owns exact pending state", err)
	}
	return nil
}

func verifySelfPublicationMembership(
	ctx context.Context,
	db *state.DB,
	publication state.SelfPublication,
) error {
	if len(publication.Members) == 0 ||
		len(publication.Members) > state.SelfPublicationMaxMembers {
		return errors.New("invalid journal member count")
	}
	for _, member := range publication.Members {
		var eventState, branchRef string
		var generation int64
		if err := db.SQL().QueryRowContext(ctx, `
SELECT state, branch_ref, branch_generation
FROM capture_events WHERE seq=?`, member.EventSeq).Scan(
			&eventState, &branchRef, &generation); err != nil {
			return fmt.Errorf("event %d: %w", member.EventSeq, err)
		}
		if eventState != state.EventStatePending ||
			branchRef != publication.BranchRef ||
			generation != publication.BranchGeneration {
			return fmt.Errorf("event %d changed ownership", member.EventSeq)
		}
		if member.CandidateID.Valid {
			var candidateStatus, membershipState string
			if err := db.SQL().QueryRowContext(ctx, `
SELECT candidate.status, member.membership_state
FROM intent_candidates candidate
JOIN intent_candidate_events member ON member.candidate_id=candidate.id
WHERE candidate.id=? AND member.event_seq=?
  AND candidate.branch_ref=? AND candidate.branch_generation=?`,
				member.CandidateID.String, member.EventSeq,
				publication.BranchRef,
				publication.BranchGeneration).Scan(
				&candidateStatus, &membershipState); err != nil {
				return fmt.Errorf("candidate %s event %d: %w",
					member.CandidateID.String, member.EventSeq, err)
			}
			if candidateStatus != state.IntentCandidateReady ||
				membershipState != state.IntentMembershipActive {
				return fmt.Errorf("candidate %s event %d changed ownership",
					member.CandidateID.String, member.EventSeq)
			}
			continue
		}
		var activeCandidateCount int
		if err := db.SQL().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM intent_candidate_events
WHERE event_seq=? AND membership_state='active'`,
			member.EventSeq).Scan(&activeCandidateCount); err != nil {
			return fmt.Errorf("event-only member %d: %w",
				member.EventSeq, err)
		}
		if activeCandidateCount != 0 {
			return fmt.Errorf("event-only member %d gained candidate ownership",
				member.EventSeq)
		}
	}
	return nil
}

func resolveSelfPublicationRef(
	ctx context.Context,
	repoRoot, branchRef string,
) (string, bool, error) {
	oid, err := git.RevParse(ctx, repoRoot, branchRef)
	if errors.Is(err, git.ErrRefNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return oid, true, nil
}

func selfPublicationParents(
	ctx context.Context,
	repoRoot, oid string,
) ([]string, error) {
	out, err := git.RunWithLimit(ctx, git.RunOpts{
		Dir: repoRoot, Timeout: git.DefaultReadTimeout,
	}, selfPublicationRecoveryOutputLimit,
		"show", "-s", "--format=%P", oid)
	if err != nil {
		return nil, err
	}
	return strings.Fields(string(out)), nil
}

func selfPublicationRefsPointingAt(
	ctx context.Context,
	repoRoot, oid string,
) ([]string, error) {
	out, err := git.RunWithLimit(ctx, git.RunOpts{
		Dir: repoRoot, Timeout: git.DefaultReadTimeout,
	}, selfPublicationRecoveryOutputLimit,
		"for-each-ref", "--points-at="+oid, "--format=%(refname)",
		"refs/heads", "refs/remotes", "refs/tags")
	if err != nil {
		return nil, err
	}
	var refs []string
	for _, ref := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if ref != "" {
			refs = append(refs, ref)
		}
	}
	return refs, nil
}

func selfPublicationCommitMessage(
	ctx context.Context,
	repoRoot, oid string,
) (string, error) {
	out, err := git.RunWithLimit(ctx, git.RunOpts{
		Dir: repoRoot, Timeout: git.DefaultReadTimeout,
	}, selfPublicationRecoveryOutputLimit,
		"show", "-s", "--format=%B", oid)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(out), "\n"), nil
}

func ambiguousSelfPublication(
	publication state.SelfPublication,
	reason string,
	cause error,
) error {
	err := fmt.Errorf(
		"%w: publication=%s phase=%s target=%s: %s",
		ErrSelfPublicationRecoveryAmbiguous, publication.ID,
		publication.Phase, publication.TargetCommitOID, reason)
	if cause != nil {
		return errors.Join(err, cause)
	}
	return err
}
