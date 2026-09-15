package cli

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// blockedSelfHealCandidate describes a blocked_conflict row whose captured
// after-state already matches HEAD. The fix planner and diagnose probe share
// this shape so the "auto-resolvable" count surfaced to operators is computed
// from one predicate.
type blockedSelfHealCandidate struct {
	Seq        int64
	Path       string
	BranchRef  string
	Generation int64
	BaseHead   string
	HeadOID    string
	Ops        []state.CaptureOp
}

// scanAutoResolvableBlockedRows enumerates blocked_conflict rows on the active
// (branch_ref, generation) whose captured ops would self-heal at HEAD. The
// predicate mirrors daemon-side probeBlockedSelfHeal exactly:
//   - capture_events.error classifies as before_state_mismatch (caller passes
//     the predicate function — daemon-internal classifier is not exported);
//   - every op is modify/mode OR rename-with-BeforeOID;
//   - alreadyPublishedAtHEAD-equivalent check passes: ancestry from
//     capture_events.base_head to HEAD, per-op HEAD blob OID/mode/absent
//     match, and HEAD did not drift between first and last read.
//
// The function NEVER mutates state. Callers in the planner use this to plan
// resolve_already_landed_barrier; diagnose uses it to report a count.
func scanAutoResolvableBlockedRows(ctx context.Context, conn *sql.DB, repo, head, branchRef string, generation int64) ([]blockedSelfHealCandidate, error) {
	if branchRef == "" {
		return nil, nil
	}
	rows, err := conn.QueryContext(ctx, `
SELECT seq, path, branch_ref, branch_generation, base_head, COALESCE(error, '')
FROM capture_events
WHERE state = ?
  AND branch_ref = ?
  AND branch_generation = ?
ORDER BY seq ASC`, state.EventStateBlockedConflict, branchRef, generation)
	if err != nil {
		return nil, fmt.Errorf("scan blocked rows: %w", err)
	}
	defer rows.Close()

	type rowCandidate struct {
		seq        int64
		path       string
		branchRef  string
		generation int64
		baseHead   string
		errMsg     string
	}
	var raw []rowCandidate
	for rows.Next() {
		var r rowCandidate
		if err := rows.Scan(&r.seq, &r.path, &r.branchRef, &r.generation, &r.baseHead, &r.errMsg); err != nil {
			return nil, fmt.Errorf("scan blocked row: %w", err)
		}
		raw = append(raw, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iter blocked rows: %w", err)
	}

	var out []blockedSelfHealCandidate
	for _, r := range raw {
		if !cliSelfHealEligibleByErrorMessage(r.errMsg) {
			continue
		}
		ops, err := loadFixCaptureOps(ctx, conn, r.seq)
		if err != nil {
			return nil, err
		}
		if !cliSelfHealEligibleByOps(ops) {
			continue
		}
		headOID, alreadyPublished, err := cliAlreadyPublishedAtHEAD(ctx, repo, r.baseHead, head, ops)
		if err != nil {
			// Probe errors fail closed (treat as not-resolvable) so we never
			// over-report self-heal capacity to operators.
			continue
		}
		if !alreadyPublished {
			continue
		}
		out = append(out, blockedSelfHealCandidate{
			Seq:        r.seq,
			Path:       r.path,
			BranchRef:  r.branchRef,
			Generation: r.generation,
			BaseHead:   r.baseHead,
			HeadOID:    headOID,
			Ops:        ops,
		})
	}
	return out, nil
}

// cliSelfHealEligibleByErrorMessage defers to daemon.IsBeforeStateMismatchError
// so the CLI self-heal probe stays in lock-step with the daemon's classifier.
// Any expansion of the daemon's before_state_mismatch class (additional
// substrings) is automatically picked up here.
func cliSelfHealEligibleByErrorMessage(errMsg string) bool {
	if errMsg == "" {
		return false
	}
	return daemon.IsBeforeStateMismatchError(errMsg)
}

// cliSelfHealEligibleByOps is the CLI-side mirror of daemon
// selfHealEligibleByOps. Keep predicate parity: every op must be modify or
// mode, or rename with present BeforeOID. create/delete/other rows are
// ineligible.
func cliSelfHealEligibleByOps(ops []state.CaptureOp) bool {
	if len(ops) == 0 {
		return false
	}
	for _, op := range ops {
		switch op.Op {
		case "modify", "mode":
			// ok
		case "rename":
			if !op.BeforeOID.Valid || op.BeforeOID.String == "" {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// cliAlreadyPublishedAtHEAD proves the HEAD observed during the read-only plan.
// The caller admits only modify/mode/rename captures; deletes stay in recovery.
func cliAlreadyPublishedAtHEAD(ctx context.Context, repo, sourceHead, headOID string, ops []state.CaptureOp) (string, bool, error) {
	proofOps := make([]git.PublicationOp, 0, len(ops))
	for _, op := range ops {
		proof := git.PublicationOp{Operation: op.Op, Path: op.Path}
		if op.OldPath.Valid {
			proof.OldPath = op.OldPath.String
		}
		if op.BeforeOID.Valid {
			proof.BeforeOID = op.BeforeOID.String
		}
		if op.AfterOID.Valid {
			proof.AfterOID = op.AfterOID.String
		}
		if op.AfterMode.Valid {
			proof.AfterMode = op.AfterMode.String
		}
		proofOps = append(proofOps, proof)
	}
	return git.ProvePublicationAtHEAD(ctx, repo, sourceHead, headOID, proofOps, git.PublicationProofPolicy{MissingPathIsMismatch: true})
}

// recoveryBlockerCounts centralizes the read-only blocker predicates used by
// status/list/diagnose/fix-facing code so their counts do not drift.
type recoveryBlockerCounts struct {
	// TotalBlockedConflicts is every terminal blocked_conflict row in the DB,
	// regardless of branch. This is the operator-visible stuck-row total used
	// by status and list.
	TotalBlockedConflicts int
	// ActiveBlockedBarriersWithSuccessors is the active daemon anchor subset of
	// blocked_conflict rows that have later pending rows on the same
	// (branch_ref, branch_generation). This is the force-fix barrier count.
	ActiveBlockedBarriersWithSuccessors int
	// ActiveTerminalEvents is every blocked_conflict or failed row for the
	// daemon's current (branch_ref, branch_generation), including a terminal
	// row at the tail with no pending successor. Bare health uses this to avoid
	// reporting a stuck active pair as healthy.
	ActiveTerminalEvents int
	// FailedBarriersWithSuccessors is every failed terminal row that has later
	// pending rows on the same (branch_ref, branch_generation).
	FailedBarriersWithSuccessors int
	// PendingOnlyIntentDepth is pending depth visible before the first terminal
	// barrier on each (branch_ref, branch_generation); pending rows hidden behind
	// blocked_conflict/failed barriers are excluded.
	PendingOnlyIntentDepth int
}

// loadRecoveryBlockerCounts is read-only and performs only SELECTs. Pass an
// empty activeBranchRef when no daemon anchor is known; active barrier counts
// then stay zero while global totals still populate.
func loadRecoveryBlockerCounts(ctx context.Context, conn *sql.DB, activeBranchRef string, activeGeneration int64) (recoveryBlockerCounts, error) {
	var c recoveryBlockerCounts
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
		state.EventStateBlockedConflict).Scan(&c.TotalBlockedConflicts); err != nil {
		return c, fmt.Errorf("total blocked conflicts: %w", err)
	}
	if activeBranchRef != "" {
		n, err := countTerminalBarriersWithSuccessors(ctx, conn, state.EventStateBlockedConflict, activeBranchRef, activeGeneration)
		if err != nil {
			return c, fmt.Errorf("active blocked barriers with successors: %w", err)
		}
		c.ActiveBlockedBarriersWithSuccessors = n
		if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM capture_events
WHERE branch_ref = ?
  AND branch_generation = ?
  AND state IN (?, ?)`,
			activeBranchRef, activeGeneration,
			state.EventStateBlockedConflict, state.EventStateFailed,
		).Scan(&c.ActiveTerminalEvents); err != nil {
			return c, fmt.Errorf("active terminal events: %w", err)
		}
	}
	failed, err := countTerminalBarriersWithSuccessors(ctx, conn, state.EventStateFailed, "", 0)
	if err != nil {
		return c, fmt.Errorf("failed barriers with successors: %w", err)
	}
	c.FailedBarriersWithSuccessors = failed
	pendingOnly, err := countPendingOnlyIntentDepth(ctx, conn)
	if err != nil {
		return c, fmt.Errorf("pending-only intent depth: %w", err)
	}
	c.PendingOnlyIntentDepth = pendingOnly
	return c, nil
}

func countTerminalBarriersWithSuccessors(ctx context.Context, conn *sql.DB, terminalState, branchRef string, generation int64) (int, error) {
	whereAnchor := ""
	args := []any{terminalState, state.EventStatePending}
	if branchRef != "" {
		whereAnchor = "\n  AND e.branch_ref = ?\n  AND e.branch_generation = ?"
		args = []any{terminalState, branchRef, generation, state.EventStatePending}
	}
	var n int
	err := conn.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM capture_events e
WHERE e.state = ?`+whereAnchor+`
  AND EXISTS (
      SELECT 1
      FROM capture_events p
      WHERE p.branch_ref = e.branch_ref
        AND p.branch_generation = e.branch_generation
        AND p.seq > e.seq
        AND p.state = ?
  )`, args...).Scan(&n)
	return n, err
}

func countPendingOnlyIntentDepth(ctx context.Context, conn *sql.DB) (int, error) {
	var n int
	err := conn.QueryRowContext(ctx, `
WITH barriers AS (
    SELECT branch_ref, branch_generation, MIN(seq) AS first_seq
    FROM capture_events
    WHERE state IN (?, ?)
    GROUP BY branch_ref, branch_generation
)
SELECT COUNT(*)
FROM capture_events e
LEFT JOIN barriers b
       ON b.branch_ref = e.branch_ref
      AND b.branch_generation = e.branch_generation
WHERE e.state = ?
  AND (b.first_seq IS NULL OR e.seq < b.first_seq)`, state.EventStateBlockedConflict, state.EventStateFailed, state.EventStatePending).Scan(&n)
	return n, err
}

// terminalBarrierRowWithSuccessors describes a terminal replay barrier whose
// seq still blocks pending rows on the same (branch_ref, generation). Used by
// the fix planner to enumerate purge_barrier_with_successors candidates.
type terminalBarrierRowWithSuccessors struct {
	Seq        int64
	Path       string
	State      string
	BranchRef  string
	Generation int64
}

// scanTerminalBarrierRowsWithSuccessors returns terminal replay barriers that
// have pending successors. blocked_conflict rows are limited to the active
// anchor because they can represent conflict state on other live branches;
// failed rows are global because failed barrier counts are global and a failed
// replay barrier does not carry a self-heal path.
func scanTerminalBarrierRowsWithSuccessors(ctx context.Context, conn *sql.DB, branchRef string, generation int64) ([]terminalBarrierRowWithSuccessors, error) {
	if branchRef == "" {
		return nil, nil
	}
	rows, err := conn.QueryContext(ctx, `
SELECT e.seq, e.path, e.state, e.branch_ref, e.branch_generation
FROM capture_events e
WHERE (
      (e.state = ? AND e.branch_ref = ? AND e.branch_generation = ?)
   OR e.state = ?
)
  AND EXISTS (
      SELECT 1
      FROM capture_events p
      WHERE p.branch_ref = e.branch_ref
        AND p.branch_generation = e.branch_generation
        AND p.seq > e.seq
        AND p.state = ?
  )
ORDER BY e.seq ASC`,
		state.EventStateBlockedConflict, branchRef, generation,
		state.EventStateFailed,
		state.EventStatePending)
	if err != nil {
		return nil, fmt.Errorf("scan barrier rows with successors: %w", err)
	}
	defer rows.Close()
	var out []terminalBarrierRowWithSuccessors
	for rows.Next() {
		var r terminalBarrierRowWithSuccessors
		if err := rows.Scan(&r.Seq, &r.Path, &r.State, &r.BranchRef, &r.Generation); err != nil {
			return nil, fmt.Errorf("scan barrier row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iter barrier rows: %w", err)
	}
	return out, nil
}
