package cli

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

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
	hasCaptures, err := state.LegacyTableExists(ctx, conn, "capture_events")
	if err != nil || !hasCaptures {
		return c, err
	}
	if err := conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
		state.EventStateBlockedConflict).Scan(&c.TotalBlockedConflicts); err != nil {
		return c, fmt.Errorf("total blocked conflicts\nDetails: %w", err)
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
			return c, fmt.Errorf("active terminal events\nDetails: %w", err)
		}
	}
	failed, err := countTerminalBarriersWithSuccessors(ctx, conn, state.EventStateFailed, "", 0)
	if err != nil {
		return c, fmt.Errorf("failed barriers with successors: %w", err)
	}
	c.FailedBarriersWithSuccessors = failed
	pendingOnly, err := countPendingOnlyIntentDepth(ctx, conn)
	if err != nil {
		return c, fmt.Errorf("pending-only intent depth\nDetails: %w", err)
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
