package daemon

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	intentRepairBackupRetention = 7 * 24 * time.Hour
	intentRepairBackupCap       = 50
)

// IntentRepairCandidatePlan is one rebuilt semantic candidate. Replaces may
// select non-contiguous commits when IntentRepairPlan.OldChain is present,
// but must preserve their relative order within that old chain.
type IntentRepairCandidatePlan struct {
	CandidateID string
	Replaces    []string
	// EventSeqs is the exact active membership used to materialize TreeOID.
	// Apply rejects any membership drift before Git can move.
	EventSeqs []int64
	TreeOID     string
	Message     string
	AuthorOID   string
}

// IntentRepairPlan is the fully gated input to automatic repair. The caller
// must already have passed candidate dependency, atomicity, materialization,
// and preset verification gates; this coordinator rechecks live repository
// safety under the control lock before any mutation.
type IntentRepairPlan struct {
	ID               string
	BranchRef        string
	BranchGeneration int64
	ExpectedHead     string
	// OldChain is the authoritative oldest-to-newest first-parent suffix.
	// An empty value retains the legacy consecutive-partition contract.
	OldChain   []string
	PlanDigest string
	Paths      []string
	MaxCommits int
	Candidates []IntentRepairCandidatePlan
	// VerifyCommit runs the already-approved repository check against each
	// exact rebuilt commit before Git changes any refs.
	VerifyCommit git.IntentRepairCommitVerifier
}

type IntentRepairResult struct {
	ID           string
	Status       string
	Reason       string
	OldHead      string
	NewHead      string
	BackupRef    string
	Recovered    bool
	PrunedRefs   int
	CommitMap    map[string]string
	CandidateMap map[string]string
}

// intentRepairAfterGitApply is a test hook for the process-death window after
// the atomic Git transaction and before the SQL git_applied transition.
var intentRepairAfterGitApply func(IntentRepairResult) error

// ApplyIntentRepairTransaction performs one crash-recoverable automatic repair.
// It never checks out files and relies on the Git primitive's literal-ref CAS
// and live index/worktree preservation.
func ApplyIntentRepairTransaction(
	ctx context.Context,
	repoRoot, gitDir string,
	db *state.DB,
	cctx CaptureContext,
	plan IntentRepairPlan,
) (IntentRepairResult, error) {
	var result IntentRepairResult
	if err := validateIntentRepairPlan(plan, cctx); err != nil {
		return result, err
	}
	if repoRoot == "" || gitDir == "" || db == nil {
		return result, errors.New("daemon: intent repair: repo, git dir, and state db are required")
	}
	if plan.ID == "" {
		id, err := newIntentRepairID()
		if err != nil {
			return result, err
		}
		plan.ID = id
	}
	result.ID = plan.ID
	digest, digestErr := intentRepairPlanDigest(ctx, repoRoot, plan)
	if digestErr != nil {
		return result, digestErr
	}
	if plan.PlanDigest != "" && plan.PlanDigest != digest {
		return result, errors.New(
			"daemon: intent repair: supplied plan digest does not match plan")
	}
	plan.PlanDigest = digest

	lock, err := AcquireControlLock(gitDir)
	if err != nil {
		return result, fmt.Errorf("daemon: intent repair: acquire control lock: %w", err)
	}
	defer lock.Release()
	if reason, err := intentRepairMutationBarrier(ctx, gitDir, db); err != nil {
		return result, err
	} else if reason != "" {
		return persistSkippedIntentRepair(ctx, db, plan, reason)
	}

	eligibility := intentRepairEligibility(plan)
	check, err := git.CheckIntentRepairEligibility(ctx, repoRoot, eligibility)
	if err != nil {
		return result, err
	}
	if !check.Eligible {
		return persistSkippedIntentRepair(ctx, db, plan, check.Reason)
	}
	members, err := state.SnapshotIntentRepairMembers(
		ctx, db, plan.ID, plan.BranchRef, plan.BranchGeneration,
		intentRepairCandidateIDs(plan))
	if err != nil {
		return result, fmt.Errorf(
			"daemon: intent repair: snapshot immutable membership: %w", err)
	}
	if err := validateIntentRepairMemberSnapshot(plan, members); err != nil {
		return result, err
	}

	prepared := state.IntentRepair{
		ID: plan.ID, BranchRef: plan.BranchRef,
		BranchGeneration: plan.BranchGeneration,
		Status:           state.IntentRepairPrepared, ExpectedHead: plan.ExpectedHead,
		PlanDigest: plan.PlanDigest,
		OldHead:    sql.NullString{String: plan.ExpectedHead, Valid: true},
		MembershipMode: state.IntentRepairMembershipFrozen,
		Commits:    intentRepairStateCommits(plan, nil),
		Members:    members,
	}
	if err := state.SaveIntentRepair(ctx, db, prepared); err != nil {
		return result, fmt.Errorf("daemon: intent repair: persist prepared transaction: %w", err)
	}

	if reason, err := intentRepairMutationBarrier(ctx, gitDir, db); err != nil {
		return result, failPreparedIntentRepair(ctx, db, plan.ID, err)
	} else if reason != "" {
		return result, failPreparedIntentRepair(ctx, db, plan.ID, errors.New(reason))
	}
	applied, err := git.ApplyIntentRepair(ctx, repoRoot, git.IntentRepairApplyOptions{
		Eligibility:      eligibility,
		RepairID:         plan.ID,
		Replacements:     intentRepairGitReplacements(plan),
		AllowRepartition: len(plan.OldChain) > 0,
		VerifyCommit:     plan.VerifyCommit,
	})
	if err != nil {
		return result, failPreparedIntentRepair(ctx, db, plan.ID, err)
	}
	if !applied.Eligible {
		skipped, transErr := transitionPreparedIntentRepair(ctx, db, plan.ID,
			state.IntentRepairSkipped, applied.Reason, nil, "", "", "")
		if transErr != nil {
			return result, transErr
		}
		return skipped, nil
	}
	result = intentRepairResultFromApply(plan.ID, applied)
	if intentRepairAfterGitApply != nil {
		if hookErr := intentRepairAfterGitApply(result); hookErr != nil {
			return result, hookErr
		}
	}

	mappings := intentRepairStateCommits(plan, result.CommitMap)
	ok, err := state.TransitionIntentRepair(ctx, db, plan.ID, state.IntentRepairTransition{
		ExpectedStatus: state.IntentRepairPrepared,
		Status:         state.IntentRepairGitApplied,
		BackupRef:      sql.NullString{String: applied.BackupRef, Valid: true},
		OldHead:        sql.NullString{String: applied.OldHead, Valid: true},
		NewHead:        sql.NullString{String: applied.NewHead, Valid: true},
		Commits:        mappings,
	})
	if err != nil {
		return result, fmt.Errorf("daemon: intent repair: persist Git-applied transaction: %w", err)
	}
	if !ok {
		return result, errors.New("daemon: intent repair: prepared transaction changed after Git CAS")
	}
	if err := completeIntentRepair(ctx, repoRoot, db, cctx, plan.ID, mappings,
		applied.NewHead); err != nil {
		return result, err
	}
	result.Status = state.IntentRepairCompleted
	result.PrunedRefs, err = PruneIntentRepairBackups(ctx, repoRoot, db, time.Now())
	if err != nil {
		return result, err
	}
	return result, nil
}

// RecoverIntentRepairs finishes transactions whose process stopped after the
// Git CAS or after the git_applied ledger transition.
func RecoverIntentRepairs(
	ctx context.Context,
	repoRoot, gitDir string,
	db *state.DB,
	cctx CaptureContext,
) ([]IntentRepairResult, error) {
	if repoRoot == "" || gitDir == "" || db == nil {
		return nil, errors.New("daemon: recover intent repairs: repo, git dir, and state db are required")
	}
	lock, err := AcquireControlLock(gitDir)
	if err != nil {
		return nil, fmt.Errorf("daemon: recover intent repairs: acquire control lock: %w", err)
	}
	defer lock.Release()
	if reason, err := intentRepairMutationBarrier(ctx, gitDir, db); err != nil {
		return nil, err
	} else if reason != "" {
		return nil, fmt.Errorf("daemon: recover intent repairs: %s", reason)
	}
	repairs, err := state.RecoverableIntentRepairs(ctx, db, intentRepairBackupCap)
	if err != nil {
		return nil, err
	}
	var out []IntentRepairResult
	for _, repair := range repairs {
		if repair.BranchRef != cctx.BranchRef ||
			repair.BranchGeneration != cctx.BranchGeneration {
			continue
		}
		result, recoverErr := recoverIntentRepair(ctx, repoRoot, db, cctx, repair)
		if recoverErr != nil {
			return out, recoverErr
		}
		out = append(out, result)
	}
	return out, nil
}

func recoverIntentRepair(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	cctx CaptureContext,
	repair state.IntentRepair,
) (IntentRepairResult, error) {
	result := IntentRepairResult{
		ID: repair.ID, Status: repair.Status, OldHead: repair.ExpectedHead,
		CommitMap: make(map[string]string), CandidateMap: make(map[string]string),
		Recovered: true,
	}
	backupRef, err := git.IntentRepairBackupRef(repair.BranchRef, repair.ID)
	if err != nil {
		return result, err
	}
	result.BackupRef = backupRef
	if repair.Status == state.IntentRepairPrepared {
		backupOID, backupErr := git.RevParse(ctx, repoRoot, backupRef)
		head, headErr := git.RevParse(ctx, repoRoot, repair.BranchRef)
		switch {
		case errors.Is(backupErr, git.ErrRefNotFound) && headErr == nil && head == repair.ExpectedHead:
			_, transErr := transitionPreparedIntentRepair(ctx, db, repair.ID,
				state.IntentRepairFailed, "prepared repair never reached Git CAS",
				nil, "", "", "")
			if transErr != nil {
				return result, transErr
			}
			result.Status = state.IntentRepairFailed
			result.Reason = "prepared repair never reached Git CAS"
			return result, nil
		case backupErr != nil:
			return result, fmt.Errorf("daemon: recover intent repair %s: resolve backup: %w",
				repair.ID, backupErr)
		case headErr != nil:
			return result, fmt.Errorf("daemon: recover intent repair %s: resolve branch: %w",
				repair.ID, headErr)
		case backupOID != repair.ExpectedHead:
			return result, fmt.Errorf("daemon: recover intent repair %s: backup points at unexpected commit", repair.ID)
		}
		mappings, mapErr := reconstructIntentRepairMappings(ctx, repoRoot, repair, head)
		if mapErr != nil {
			return result, mapErr
		}
		digest, digestErr := recoveredIntentRepairPlanDigest(
			ctx, repoRoot, repair, mappings)
		if digestErr != nil {
			return result, digestErr
		}
		if digest != repair.PlanDigest {
			return result, fmt.Errorf(
				"daemon: recover intent repair %s: rebuilt chain does not match prepared plan; backup retained at %s",
				repair.ID, backupRef)
		}
		ok, transErr := state.TransitionIntentRepair(ctx, db, repair.ID,
			state.IntentRepairTransition{
				ExpectedStatus: state.IntentRepairPrepared,
				Status:         state.IntentRepairGitApplied,
				BackupRef:      sql.NullString{String: backupRef, Valid: true},
				OldHead:        sql.NullString{String: repair.ExpectedHead, Valid: true},
				NewHead:        sql.NullString{String: head, Valid: true},
				Commits:        mappings,
			})
		if transErr != nil {
			return result, transErr
		}
		if !ok {
			return result, errors.New("daemon: recover intent repair: prepared row changed")
		}
		repair.Status = state.IntentRepairGitApplied
		repair.BackupRef = sql.NullString{String: backupRef, Valid: true}
		repair.NewHead = sql.NullString{String: head, Valid: true}
		repair.Commits = mappings
	}
	if !repair.NewHead.Valid || repair.NewHead.String == "" {
		return result, errors.New("daemon: recover intent repair: Git-applied row has no new head")
	}
	head, err := git.RevParse(ctx, repoRoot, repair.BranchRef)
	if err != nil {
		return result, err
	}
	if head != repair.NewHead.String {
		return result, fmt.Errorf("daemon: recover intent repair %s: branch moved from repaired head; backup retained at %s",
			repair.ID, backupRef)
	}
	for _, mapping := range repair.Commits {
		if !mapping.NewOID.Valid || mapping.NewOID.String == "" {
			return result, errors.New("daemon: recover intent repair: incomplete commit mapping")
		}
		result.CommitMap[mapping.OldOID] = mapping.NewOID.String
		if mapping.CandidateID.Valid {
			result.CandidateMap[mapping.CandidateID.String] = mapping.NewOID.String
		}
	}
	if err := completeIntentRepair(ctx, repoRoot, db, cctx, repair.ID,
		repair.Commits, repair.NewHead.String); err != nil {
		return result, err
	}
	result.Status = state.IntentRepairCompleted
	result.NewHead = repair.NewHead.String
	return result, nil
}

func completeIntentRepair(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	cctx CaptureContext,
	repairID string,
	mappings []state.IntentRepairCommit,
	newHead string,
) error {
	repair, exists, err := state.IntentRepairByID(ctx, db, repairID)
	if err != nil {
		return err
	}
	if !exists || (repair.Status != state.IntentRepairGitApplied &&
		repair.Status != state.IntentRepairCompleted) {
		return errors.New(
			"daemon: complete intent repair: durable Git-applied row is unavailable")
	}
	if repair.BranchRef != cctx.BranchRef ||
		repair.BranchGeneration != cctx.BranchGeneration {
		return errors.New("daemon: complete intent repair: exact branch pair changed")
	}
	if !repair.NewHead.Valid || repair.NewHead.String == "" ||
		repair.NewHead.String != newHead {
		return errors.New("daemon: complete intent repair: durable new head changed")
	}
	if len(repair.Commits) > 0 {
		mappings = repair.Commits
	}
	reconcile := make(map[string]string, len(mappings))
	candidates := make(map[string]string)
	for _, mapping := range mappings {
		if !mapping.NewOID.Valid {
			return errors.New("daemon: complete intent repair: missing new commit mapping")
		}
		reconcile[mapping.OldOID] = mapping.NewOID.String
		if mapping.CandidateID.Valid {
			candidateID := mapping.CandidateID.String
			if existing := candidates[candidateID]; existing != "" &&
				existing != mapping.NewOID.String {
				return fmt.Errorf(
					"daemon: complete intent repair: candidate %s maps to multiple commits",
					candidateID)
			}
			candidates[candidateID] = mapping.NewOID.String
		}
	}
	if err := reconcileIntentRepairLedger(ctx, db, repairID, reconcile, candidates,
		repair.Members, cctx, newHead); err != nil {
		return err
	}
	reseed := cctx
	reseed.BaseHead = newHead
	if _, err := ReseedShadowFromHead(ctx, repoRoot, db, reseed); err != nil {
		return fmt.Errorf("daemon: complete intent repair: reseed exact shadow pair: %w", err)
	}
	ok, err := state.TransitionIntentRepair(ctx, db, repairID,
		state.IntentRepairTransition{
			ExpectedStatus: state.IntentRepairGitApplied,
			Status:         state.IntentRepairCompleted,
		})
	if err != nil {
		return err
	}
	if !ok {
		repair, exists, loadErr := state.IntentRepairByID(ctx, db, repairID)
		if loadErr != nil {
			return loadErr
		}
		if !exists || repair.Status != state.IntentRepairCompleted {
			return errors.New("daemon: complete intent repair: Git-applied row changed")
		}
	}
	return nil
}

func reconcileIntentRepairLedger(
	ctx context.Context,
	db *state.DB,
	repairID string,
	commitMap, candidateMap map[string]string,
	members []state.IntentRepairMember,
	cctx CaptureContext,
	newHead string,
) error {
	tx, err := db.SQL().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("daemon: intent repair ledger: begin: %w", err)
	}
	defer tx.Rollback()
	var status string
	if err := tx.QueryRowContext(ctx,
		`SELECT status FROM intent_repairs WHERE id=?`, repairID).Scan(&status); err != nil {
		return fmt.Errorf("daemon: intent repair ledger: load transaction: %w", err)
	}
	if status != state.IntentRepairGitApplied && status != state.IntentRepairCompleted {
		return fmt.Errorf("daemon: intent repair ledger: transaction is %s", status)
	}
	memberCounts := make(map[string]int)
	if len(members) > 0 {
		for _, member := range members {
			if candidateMap[member.CandidateID] == "" {
				return fmt.Errorf(
					"daemon: intent repair ledger: member candidate %s has no new commit",
					member.CandidateID)
			}
			memberCounts[member.CandidateID]++
		}
		if len(memberCounts) != len(candidateMap) {
			return errors.New(
				"daemon: intent repair ledger: durable membership and candidate mapping differ")
		}
		if err := validateIntentRepairSettlement(
			ctx, tx, repairID, len(members), cctx); err != nil {
			return err
		}
	}
	for oldOID, newOID := range commitMap {
		queries := []string{
			`UPDATE decision_records SET commit_oid=? WHERE commit_oid=?`,
			`UPDATE publish_state SET target_commit_oid=? WHERE target_commit_oid=?`,
			`UPDATE publish_state SET source_head=? WHERE source_head=?`,
		}
		if len(members) == 0 {
			queries = append(queries,
				`UPDATE capture_events SET commit_oid=? WHERE commit_oid=?`)
		}
		for _, query := range queries {
			if _, err := tx.ExecContext(ctx, query, newOID, oldOID); err != nil {
				return fmt.Errorf("daemon: intent repair ledger: reconcile oid: %w", err)
			}
		}
	}
	now := float64(time.Now().UnixNano()) / 1e9
	for candidateID, commitOID := range candidateMap {
		res, err := tx.ExecContext(ctx, `
UPDATE intent_candidates
SET status='published', readiness='ready', published_commit_oid=?,
    updated_ts=?, soft_publication_deadline=NULL
WHERE id=? AND branch_ref=? AND branch_generation=?
  AND status IN ('ready','soft_published','published')`,
			commitOID, now, candidateID, cctx.BranchRef, cctx.BranchGeneration)
		if err != nil {
			return fmt.Errorf("daemon: intent repair ledger: publish candidate: %w", err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("daemon: intent repair ledger: candidate %s is not repairable", candidateID)
		}
		if len(members) > 0 {
			res, err := tx.ExecContext(ctx, `
UPDATE capture_events
SET state='published', commit_oid=?, published_ts=?, error=NULL
WHERE branch_ref=? AND branch_generation=?
  AND seq IN (
      SELECT event_seq FROM intent_repair_members
      WHERE repair_id=? AND candidate_id=?
  )`, commitOID, now, cctx.BranchRef, cctx.BranchGeneration,
				repairID, candidateID)
			if err != nil {
				return fmt.Errorf(
					"daemon: intent repair ledger: settle immutable candidate captures: %w",
					err)
			}
			if n, countErr := res.RowsAffected(); countErr != nil {
				return fmt.Errorf(
					"daemon: intent repair ledger: count immutable candidate captures: %w",
					countErr)
			} else if n != int64(memberCounts[candidateID]) {
				return fmt.Errorf(
					"daemon: intent repair ledger: settled candidate %s captures=%d want=%d",
					candidateID, n, memberCounts[candidateID])
			}
			continue
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE capture_events
SET state='published', commit_oid=?, published_ts=?,
    error=NULL
WHERE seq IN (
    SELECT event_seq FROM intent_candidate_events
    WHERE candidate_id=? AND membership_state='active'
)
  AND branch_ref=? AND branch_generation=?
  AND state IN ('pending','published')`,
			commitOID, now, candidateID, cctx.BranchRef,
			cctx.BranchGeneration); err != nil {
			return fmt.Errorf("daemon: intent repair ledger: settle candidate captures: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE publish_state
SET source_head=?, target_commit_oid=?
WHERE id=1 AND status='succeeded'`, newHead, newHead); err != nil {
		return fmt.Errorf("daemon: intent repair ledger: update publish breadcrumb: %w", err)
	}
	return tx.Commit()
}

func validateIntentRepairSettlement(
	ctx context.Context,
	tx *sql.Tx,
	repairID string,
	memberCount int,
	cctx CaptureContext,
) error {
	var invalidMappings int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM (
    SELECT mapped.candidate_id
    FROM intent_repair_commits mapped
    WHERE mapped.repair_id=?
    GROUP BY mapped.candidate_id
    HAVING mapped.candidate_id IS NULL
       OR COUNT(mapped.new_oid)<>COUNT(*)
       OR COUNT(DISTINCT mapped.new_oid)<>1
)`, repairID).Scan(&invalidMappings); err != nil {
		return fmt.Errorf(
			"daemon: intent repair ledger: inspect immutable candidate mappings: %w",
			err)
	}
	if invalidMappings != 0 {
		return fmt.Errorf(
			"daemon: intent repair ledger: %d candidate mappings are incomplete",
			invalidMappings)
	}

	var exactActive int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM intent_repair_members owned
JOIN intent_candidates candidate
  ON candidate.id=owned.candidate_id
 AND candidate.branch_ref=? AND candidate.branch_generation=?
 AND candidate.status IN ('ready','soft_published','published')
JOIN intent_candidate_events membership
  ON membership.candidate_id=owned.candidate_id
 AND membership.event_seq=owned.event_seq
 AND membership.membership_state='active'
JOIN capture_events event
  ON event.seq=owned.event_seq
 AND event.branch_ref=? AND event.branch_generation=?
WHERE owned.repair_id=?`, cctx.BranchRef, cctx.BranchGeneration,
		cctx.BranchRef, cctx.BranchGeneration, repairID).Scan(&exactActive); err != nil {
		return fmt.Errorf(
			"daemon: intent repair ledger: inspect immutable active membership: %w",
			err)
	}
	if exactActive != memberCount {
		return fmt.Errorf(
			"daemon: intent repair ledger: active membership changed: exact=%d want=%d",
			exactActive, memberCount)
	}

	var incomplete int
	if err := tx.QueryRowContext(ctx, `
WITH expected AS (
    SELECT candidate_id, COUNT(*) AS member_count
    FROM intent_repair_members
    WHERE repair_id=?
    GROUP BY candidate_id
), active AS (
    SELECT membership.candidate_id, COUNT(*) AS member_count
    FROM intent_candidate_events membership
    JOIN expected ON expected.candidate_id=membership.candidate_id
    WHERE membership.membership_state='active'
    GROUP BY membership.candidate_id
)
SELECT COUNT(*)
FROM expected
LEFT JOIN active USING(candidate_id)
WHERE COALESCE(active.member_count, 0)<>expected.member_count`, repairID).
		Scan(&incomplete); err != nil {
		return fmt.Errorf(
			"daemon: intent repair ledger: inspect complete active membership: %w",
			err)
	}
	if incomplete != 0 {
		return fmt.Errorf(
			"daemon: intent repair ledger: %d candidates changed active membership",
			incomplete)
	}

	var invalidState, priorOnly, settledOnly int
	if err := tx.QueryRowContext(ctx, `
WITH classified AS (
    SELECT
        CASE
            WHEN owned.prior_state='pending'
             AND event.state='pending' AND event.commit_oid IS NULL THEN 1
            WHEN owned.prior_state='published'
             AND event.state='published'
             AND EXISTS (
                 SELECT 1 FROM intent_repair_commits mapped
                 WHERE mapped.repair_id=owned.repair_id
                   AND mapped.candidate_id=owned.candidate_id
                   AND mapped.old_oid=event.commit_oid
             ) THEN 1
            ELSE 0
        END AS prior_match,
        CASE
            WHEN event.state='published'
             AND EXISTS (
                 SELECT 1 FROM intent_repair_commits mapped
                 WHERE mapped.repair_id=owned.repair_id
                   AND mapped.candidate_id=owned.candidate_id
                   AND mapped.new_oid=event.commit_oid
             ) THEN 1
            ELSE 0
        END AS settled_match
    FROM intent_repair_members owned
    JOIN capture_events event ON event.seq=owned.event_seq
    WHERE owned.repair_id=?
)
SELECT
    COALESCE(SUM(prior_match=0 AND settled_match=0), 0),
    COALESCE(SUM(prior_match=1 AND settled_match=0), 0),
    COALESCE(SUM(prior_match=0 AND settled_match=1), 0)
FROM classified`, repairID).Scan(&invalidState, &priorOnly, &settledOnly); err != nil {
		return fmt.Errorf(
			"daemon: intent repair ledger: inspect immutable capture state: %w", err)
	}
	if invalidState != 0 {
		return fmt.Errorf(
			"daemon: intent repair ledger: %d immutable captures changed state",
			invalidState)
	}
	if priorOnly != 0 && settledOnly != 0 {
		return errors.New(
			"daemon: intent repair ledger: immutable capture settlement is partial")
	}
	return nil
}

func intentRepairMutationBarrier(ctx context.Context, gitDir string, db *state.DB) (string, error) {
	if marker, active := GitOperationInProgress(gitDir); active {
		return "Git operation in progress: " + marker, nil
	}
	paused, err := daemonPauseState(ctx, gitDir, db)
	if err != nil {
		return "", err
	}
	if paused.Active {
		return "manual or rewind pause is active", nil
	}
	return "", nil
}

func validateIntentRepairPlan(plan IntentRepairPlan, cctx CaptureContext) error {
	if plan.BranchRef == "" || plan.ExpectedHead == "" ||
		plan.BranchGeneration < 0 || len(plan.Candidates) == 0 {
		return errors.New("daemon: intent repair: incomplete plan")
	}
	if cctx.BranchRef != plan.BranchRef ||
		cctx.BranchGeneration != plan.BranchGeneration {
		return errors.New("daemon: intent repair: exact branch pair changed")
	}
	limit := plan.MaxCommits
	if limit == 0 {
		limit = git.MaxIntentRepairCommits
	}
	var count int
	seen := make(map[string]struct{})
	seenCandidates := make(map[string]struct{}, len(plan.Candidates))
	seenEvents := make(map[int64]struct{})
	oldPositions := make(map[string]int, len(plan.OldChain))
	for position, oid := range plan.OldChain {
		if oid == "" {
			return errors.New("daemon: intent repair: empty old-chain oid")
		}
		if _, duplicate := oldPositions[oid]; duplicate {
			return fmt.Errorf("daemon: intent repair: duplicate old-chain oid %s", oid)
		}
		oldPositions[oid] = position
	}
	lastCandidatePosition := -1
	for _, candidate := range plan.Candidates {
		if strings.TrimSpace(candidate.CandidateID) == "" ||
			len(candidate.Replaces) == 0 || candidate.TreeOID == "" ||
			strings.TrimSpace(candidate.Message) == "" {
			return errors.New("daemon: intent repair: incomplete candidate replacement")
		}
		if _, duplicate := seenCandidates[candidate.CandidateID]; duplicate {
			return fmt.Errorf(
				"daemon: intent repair: duplicate candidate id %s",
				candidate.CandidateID)
		}
		seenCandidates[candidate.CandidateID] = struct{}{}
		if len(candidate.EventSeqs) == 0 {
			return fmt.Errorf(
				"daemon: intent repair: candidate %s has no materialized membership",
				candidate.CandidateID)
		}
		for _, seq := range candidate.EventSeqs {
			if seq <= 0 {
				return fmt.Errorf(
					"daemon: intent repair: candidate %s has invalid event %d",
					candidate.CandidateID, seq)
			}
			if _, duplicate := seenEvents[seq]; duplicate {
				return fmt.Errorf(
					"daemon: intent repair: duplicate materialized event %d", seq)
			}
			seenEvents[seq] = struct{}{}
		}
		firstPosition := -1
		previousPosition := -1
		for _, oid := range candidate.Replaces {
			if oid == "" {
				return errors.New("daemon: intent repair: empty replaced oid")
			}
			if len(oldPositions) > 0 {
				position, ok := oldPositions[oid]
				if !ok {
					return fmt.Errorf(
						"daemon: intent repair: replacement %s is outside old chain",
						oid,
					)
				}
				if previousPosition >= position {
					return errors.New(
						"daemon: intent repair: candidate replacements do not preserve old-chain order",
					)
				}
				if firstPosition < 0 {
					firstPosition = position
				}
				previousPosition = position
			}
			if _, duplicate := seen[oid]; duplicate {
				return fmt.Errorf("daemon: intent repair: duplicate replaced oid %s", oid)
			}
			seen[oid] = struct{}{}
			count++
		}
		if firstPosition >= 0 {
			if firstPosition <= lastCandidatePosition {
				return errors.New(
					"daemon: intent repair: candidates are not ordered by their earliest old commit",
				)
			}
			lastCandidatePosition = firstPosition
		}
	}
	if count > limit || count > git.MaxIntentRepairCommits {
		return fmt.Errorf("daemon: intent repair: %d commits exceed limit %d", count, limit)
	}
	if len(plan.OldChain) > 0 {
		if len(plan.OldChain) != count {
			return errors.New(
				"daemon: intent repair: candidates must partition the complete old chain",
			)
		}
		if plan.OldChain[len(plan.OldChain)-1] != plan.ExpectedHead {
			return errors.New("daemon: intent repair: old chain does not end at expected HEAD")
		}
		if _, ok := seen[plan.OldChain[0]]; !ok ||
			len(plan.Candidates[0].Replaces) == 0 ||
			plan.Candidates[0].Replaces[0] != plan.OldChain[0] {
			return errors.New(
				"daemon: intent repair: first candidate must own the oldest commit",
			)
		}
		return nil
	}
	last := plan.Candidates[len(plan.Candidates)-1]
	if last.Replaces[len(last.Replaces)-1] != plan.ExpectedHead {
		return errors.New("daemon: intent repair: replacement chain does not end at expected HEAD")
	}
	return nil
}

func intentRepairEligibility(plan IntentRepairPlan) git.IntentRepairEligibilityOptions {
	candidateByOID := make(map[string]string)
	for _, candidate := range plan.Candidates {
		for _, oid := range candidate.Replaces {
			candidateByOID[oid] = candidate.CandidateID
		}
	}
	chain := plan.OldChain
	if len(chain) == 0 {
		for _, candidate := range plan.Candidates {
			chain = append(chain, candidate.Replaces...)
		}
	}
	commits := make([]git.IntentRepairOwnedCommit, 0, len(chain))
	for _, oid := range chain {
		commits = append(commits, git.IntentRepairOwnedCommit{
			OID: oid, CandidateID: candidateByOID[oid],
		})
	}
	return git.IntentRepairEligibilityOptions{
		BranchRef: plan.BranchRef, ExpectedHead: plan.ExpectedHead,
		Commits: commits, Paths: append([]string(nil), plan.Paths...),
		MaxCommits: plan.MaxCommits,
	}
}

func intentRepairGitReplacements(plan IntentRepairPlan) []git.IntentRepairReplacement {
	out := make([]git.IntentRepairReplacement, 0, len(plan.Candidates))
	for _, candidate := range plan.Candidates {
		out = append(out, git.IntentRepairReplacement{
			Replaces: append([]string(nil), candidate.Replaces...),
			TreeOID:  candidate.TreeOID, Message: candidate.Message,
			AuthorOID: candidate.AuthorOID,
		})
	}
	return out
}

func intentRepairCandidateIDs(plan IntentRepairPlan) []string {
	seen := make(map[string]struct{}, len(plan.Candidates))
	out := make([]string, 0, len(plan.Candidates))
	for _, candidate := range plan.Candidates {
		if _, exists := seen[candidate.CandidateID]; exists {
			continue
		}
		seen[candidate.CandidateID] = struct{}{}
		out = append(out, candidate.CandidateID)
	}
	return out
}

func intentRepairCandidateEventSeqs(
	events []state.IntentCandidateEvent,
) []int64 {
	seqs := make([]int64, 0, len(events))
	for _, event := range events {
		seqs = append(seqs, event.EventSeq)
	}
	return seqs
}

func validateIntentRepairMemberSnapshot(
	plan IntentRepairPlan,
	members []state.IntentRepairMember,
) error {
	expected := make(map[string]map[int64]struct{}, len(plan.Candidates))
	expectedCount := 0
	for _, candidate := range plan.Candidates {
		seqs := make(map[int64]struct{}, len(candidate.EventSeqs))
		for _, seq := range candidate.EventSeqs {
			seqs[seq] = struct{}{}
		}
		expected[candidate.CandidateID] = seqs
		expectedCount += len(seqs)
	}
	if len(members) != expectedCount {
		return fmt.Errorf(
			"daemon: intent repair: membership changed since materialization: got %d events, want %d",
			len(members), expectedCount)
	}
	for _, member := range members {
		seqs, ok := expected[member.CandidateID]
		if !ok {
			return fmt.Errorf(
				"daemon: intent repair: membership changed since materialization: unexpected candidate %s",
				member.CandidateID)
		}
		if _, ok := seqs[member.EventSeq]; !ok {
			return fmt.Errorf(
				"daemon: intent repair: membership changed since materialization: unexpected event %d",
				member.EventSeq)
		}
		delete(seqs, member.EventSeq)
	}
	for candidateID, seqs := range expected {
		if len(seqs) != 0 {
			return fmt.Errorf(
				"daemon: intent repair: membership changed since materialization for candidate %s",
				candidateID)
		}
	}
	return nil
}

func intentRepairStateCommits(plan IntentRepairPlan, commitMap map[string]string) []state.IntentRepairCommit {
	var out []state.IntentRepairCommit
	for _, candidate := range plan.Candidates {
		for _, oid := range candidate.Replaces {
			mapping := state.IntentRepairCommit{
				CandidateID: sql.NullString{String: candidate.CandidateID, Valid: true},
				OldOID:      oid,
			}
			if newOID := commitMap[oid]; newOID != "" {
				mapping.NewOID = sql.NullString{String: newOID, Valid: true}
			}
			out = append(out, mapping)
		}
	}
	return out
}

func intentRepairResultFromApply(id string, applied git.IntentRepairApplyResult) IntentRepairResult {
	result := IntentRepairResult{
		ID: id, Status: state.IntentRepairGitApplied,
		OldHead: applied.OldHead, NewHead: applied.NewHead,
		BackupRef: applied.BackupRef, CommitMap: make(map[string]string),
		CandidateMap: make(map[string]string),
	}
	for _, mapping := range applied.CommitMappings {
		result.CommitMap[mapping.OldOID] = mapping.NewOID
	}
	return result
}

func persistSkippedIntentRepair(ctx context.Context, db *state.DB, plan IntentRepairPlan, reason string) (IntentRepairResult, error) {
	if plan.ID == "" {
		id, err := newIntentRepairID()
		if err != nil {
			return IntentRepairResult{}, err
		}
		plan.ID = id
	}
	if err := state.SaveIntentRepair(ctx, db, state.IntentRepair{
		ID: plan.ID, BranchRef: plan.BranchRef,
		BranchGeneration: plan.BranchGeneration,
		Status:           state.IntentRepairPrepared, ExpectedHead: plan.ExpectedHead,
		PlanDigest: plan.PlanDigest,
		MembershipMode: state.IntentRepairMembershipNone,
		Commits:    intentRepairStateCommits(plan, nil),
	}); err != nil {
		return IntentRepairResult{}, err
	}
	return transitionPreparedIntentRepair(ctx, db, plan.ID,
		state.IntentRepairSkipped, reason, nil, "", "", "")
}

func transitionPreparedIntentRepair(
	ctx context.Context,
	db *state.DB,
	id, status, reason string,
	commits []state.IntentRepairCommit,
	backup, oldHead, newHead string,
) (IntentRepairResult, error) {
	ok, err := state.TransitionIntentRepair(ctx, db, id, state.IntentRepairTransition{
		ExpectedStatus: state.IntentRepairPrepared, Status: status,
		Error: reason, Commits: commits,
		BackupRef: sql.NullString{String: backup, Valid: backup != ""},
		OldHead:   sql.NullString{String: oldHead, Valid: oldHead != ""},
		NewHead:   sql.NullString{String: newHead, Valid: newHead != ""},
	})
	if err != nil {
		return IntentRepairResult{}, err
	}
	if !ok {
		return IntentRepairResult{}, errors.New("daemon: intent repair: prepared row changed")
	}
	return IntentRepairResult{ID: id, Status: status, Reason: reason}, nil
}

func failPreparedIntentRepair(ctx context.Context, db *state.DB, id string, cause error) error {
	reason := cause.Error()
	if len(reason) > state.IntentCandidateSummaryMaxChars {
		reason = reason[:state.IntentCandidateSummaryMaxChars]
	}
	_, err := transitionPreparedIntentRepair(ctx, db, id,
		state.IntentRepairFailed, reason, nil, "", "", "")
	if err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func reconstructIntentRepairMappings(
	ctx context.Context,
	repoRoot string,
	repair state.IntentRepair,
	head string,
) ([]state.IntentRepairCommit, error) {
	if len(repair.Commits) == 0 {
		return nil, errors.New("daemon: recover intent repair: no old commit mapping")
	}
	baseOut, err := git.Run(ctx, git.RunOpts{Dir: repoRoot, Timeout: git.DefaultReadTimeout},
		"rev-parse", repair.Commits[0].OldOID+"^")
	if err != nil {
		return nil, fmt.Errorf("daemon: recover intent repair: resolve repair base: %w", err)
	}
	base := strings.TrimSpace(string(baseOut))
	out, err := git.Run(ctx, git.RunOpts{Dir: repoRoot, Timeout: git.DefaultReadTimeout},
		"rev-list", "--first-parent", "--reverse", base+".."+head)
	if err != nil {
		return nil, fmt.Errorf("daemon: recover intent repair: inspect rebuilt chain: %w", err)
	}
	var newOIDs []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line != "" {
			newOIDs = append(newOIDs, line)
		}
	}
	var groups [][]int
	for i, mapping := range repair.Commits {
		if !mapping.CandidateID.Valid {
			return nil, errors.New("daemon: recover intent repair: mapping has no candidate")
		}
		if len(groups) == 0 ||
			repair.Commits[groups[len(groups)-1][0]].CandidateID.String != mapping.CandidateID.String {
			groups = append(groups, nil)
		}
		groups[len(groups)-1] = append(groups[len(groups)-1], i)
	}
	if len(newOIDs) != len(groups) {
		return nil, fmt.Errorf("daemon: recover intent repair: rebuilt chain has %d commits, want %d",
			len(newOIDs), len(groups))
	}
	mappings := append([]state.IntentRepairCommit(nil), repair.Commits...)
	for groupIndex, positions := range groups {
		for _, position := range positions {
			mappings[position].NewOID = sql.NullString{
				String: newOIDs[groupIndex], Valid: true,
			}
		}
	}
	return mappings, nil
}

type intentRepairDigestCandidate struct {
	candidateID string
	replaces    []string
	eventSeqs   []int64
	treeOID     string
	message     string
	authorOID   string
}

func intentRepairPlanDigest(
	ctx context.Context,
	repoRoot string,
	plan IntentRepairPlan,
) (string, error) {
	candidates := make([]intentRepairDigestCandidate, 0, len(plan.Candidates))
	for _, candidate := range plan.Candidates {
		authorOID := candidate.AuthorOID
		if authorOID == "" {
			authorOID = candidate.Replaces[0]
		}
		candidates = append(candidates, intentRepairDigestCandidate{
			candidateID: candidate.CandidateID,
			replaces:    append([]string(nil), candidate.Replaces...),
			eventSeqs:   append([]int64(nil), candidate.EventSeqs...),
			treeOID:     candidate.TreeOID,
			message:     candidate.Message,
			authorOID:   authorOID,
		})
	}
	return hashIntentRepairCandidates(ctx, repoRoot, candidates)
}

func recoveredIntentRepairPlanDigest(
	ctx context.Context,
	repoRoot string,
	repair state.IntentRepair,
	mappings []state.IntentRepairCommit,
) (string, error) {
	membersByCandidate := make(map[string][]int64)
	for _, member := range repair.Members {
		membersByCandidate[member.CandidateID] = append(
			membersByCandidate[member.CandidateID], member.EventSeq)
	}
	var candidates []intentRepairDigestCandidate
	for i := 0; i < len(mappings); {
		candidateID := mappings[i].CandidateID.String
		newOID := mappings[i].NewOID.String
		candidate := intentRepairDigestCandidate{
			candidateID: candidateID,
			authorOID:   newOID,
			eventSeqs:   append([]int64(nil), membersByCandidate[candidateID]...),
		}
		for i < len(mappings) &&
			mappings[i].CandidateID.Valid &&
			mappings[i].CandidateID.String == candidateID {
			if !mappings[i].NewOID.Valid || mappings[i].NewOID.String != newOID {
				return "", errors.New(
					"daemon: recover intent repair: inconsistent candidate mapping")
			}
			candidate.replaces = append(candidate.replaces, mappings[i].OldOID)
			i++
		}
		tree, err := resolveTreeOID(ctx, repoRoot, newOID)
		if err != nil {
			return "", err
		}
		message, err := git.Run(ctx, git.RunOpts{
			Dir: repoRoot, Timeout: git.DefaultReadTimeout,
		}, "show", "-s", "--format=%B", newOID)
		if err != nil {
			return "", fmt.Errorf(
				"daemon: recover intent repair: read rebuilt message: %w", err)
		}
		candidate.treeOID = tree
		candidate.message = string(message)
		candidates = append(candidates, candidate)
	}
	return hashIntentRepairCandidates(ctx, repoRoot, candidates)
}

func hashIntentRepairCandidates(
	ctx context.Context,
	repoRoot string,
	candidates []intentRepairDigestCandidate,
) (string, error) {
	hash := sha256.New()
	writeField := func(value string) {
		_, _ = fmt.Fprintf(hash, "%d:", len(value))
		_, _ = hash.Write([]byte(value))
	}
	version := "acd-intent-repair-plan-v1"
	for _, candidate := range candidates {
		if len(candidate.eventSeqs) > 0 {
			version = "acd-intent-repair-plan-v2"
			break
		}
	}
	writeField(version)
	for _, candidate := range candidates {
		author, err := git.Run(ctx, git.RunOpts{
			Dir: repoRoot, Timeout: git.DefaultReadTimeout,
		}, "show", "-s", "--format=%an%x00%ae%x00%aI", candidate.authorOID)
		if err != nil {
			return "", fmt.Errorf("daemon: intent repair: read author identity: %w", err)
		}
		writeField(candidate.candidateID)
		eventSeqs := append([]int64(nil), candidate.eventSeqs...)
		sort.Slice(eventSeqs, func(i, j int) bool { return eventSeqs[i] < eventSeqs[j] })
		for _, seq := range eventSeqs {
			writeField(strconv.FormatInt(seq, 10))
		}
		for _, oldOID := range candidate.replaces {
			writeField(oldOID)
		}
		writeField(candidate.treeOID)
		writeField(strings.TrimRight(candidate.message, "\n"))
		writeField(strings.TrimRight(string(author), "\n"))
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

// PruneIntentRepairBackups enforces the seven-day/fifty-ref retention bound.
// A completed repair remains protected while its branch still points at the
// repaired head; advancing the branch makes the backup eligible for pruning.
func PruneIntentRepairBackups(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	now time.Time,
) (int, error) {
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT id, branch_ref, backup_ref, old_head, new_head, completed_ts
FROM intent_repairs
WHERE status='completed' AND backup_ref IS NOT NULL
ORDER BY completed_ts DESC, id DESC`)
	if err != nil {
		return 0, fmt.Errorf("daemon: prune intent repair backups: query: %w", err)
	}
	defer rows.Close()
	type backup struct {
		id, branchRef, ref, oldHead, newHead string
		completed                            float64
	}
	var backups []backup
	for rows.Next() {
		var item backup
		if err := rows.Scan(&item.id, &item.branchRef, &item.ref,
			&item.oldHead, &item.newHead, &item.completed); err != nil {
			return 0, err
		}
		backups = append(backups, item)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	pruned := 0
	for i, item := range backups {
		expired := now.Sub(time.Unix(0, int64(item.completed*1e9))) >=
			intentRepairBackupRetention
		if i < intentRepairBackupCap && !expired {
			continue
		}
		branchHead, err := git.RevParse(ctx, repoRoot, item.branchRef)
		if err == nil && branchHead == item.newHead {
			continue
		}
		current, err := git.RevParse(ctx, repoRoot, item.ref)
		if errors.Is(err, git.ErrRefNotFound) {
			continue
		}
		if err != nil {
			return pruned, err
		}
		if current != item.oldHead {
			return pruned, fmt.Errorf("daemon: prune intent repair backup %s changed target", item.ref)
		}
		if _, err := git.Run(ctx, git.RunOpts{Dir: repoRoot},
			"update-ref", "-d", item.ref, item.oldHead); err != nil {
			return pruned, fmt.Errorf("daemon: prune intent repair backup %s: %w", item.ref, err)
		}
		pruned++
	}
	return pruned, nil
}

func newIntentRepairID() (string, error) {
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("daemon: intent repair: allocate id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func sortedIntentRepairCandidateIDs(values map[string]string) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
