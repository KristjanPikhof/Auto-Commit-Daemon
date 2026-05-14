package state

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
)

const (
	DecisionKindCaptured        = "captured"
	DecisionKindCommitted       = "committed"
	DecisionKindSkipped         = "skipped"
	DecisionKindProtected       = "protected"
	DecisionKindHandledExternal = "handled_external"
	// DecisionKindHandledExternalAfterBlock signals that a previously
	// blocked_conflict row was self-healed by the replay-time settle probe:
	// an external committer had already landed the captured change at HEAD,
	// so the daemon promotes the row to published without minting a commit.
	// Distinct from DecisionKindHandledExternal so operators can spot rows
	// that cleared a barrier (vs idempotent settles taken while still
	// pending). See internal/daemon/replay.go probeBlockedSelfHeal.
	DecisionKindHandledExternalAfterBlock = "handled_external_after_block"
	DecisionKindSupersededExternal        = "superseded_external"
	DecisionKindBlocked                   = "blocked"
	DecisionKindPaused                    = "paused"
	DecisionKindResumed                   = "resumed"
	DecisionKindIntentDeferred            = "intent_deferred"
	DecisionKindIntentForced              = "intent_forced"
	DecisionKindIntentPlannerError        = "intent_planner_error"
)

const (
	defaultDecisionLimit = 100
	maxDecisionLimit     = 1000
)

// DecisionRecord is one append-only product-facing decision. Callers use it to
// explain why ACD captured, skipped, committed, blocked, or reconciled work.
type DecisionRecord struct {
	ID               int64
	DecisionTS       float64
	Kind             string
	Path             sql.NullString
	Reason           sql.NullString
	EventSeq         sql.NullInt64
	HeadSHA          sql.NullString
	CommitOID        sql.NullString
	BranchRef        sql.NullString
	BranchGeneration sql.NullInt64
	ActionTaken      sql.NullString
	UserMessage      sql.NullString
}

// DecisionCommitGroup summarizes all decision rows attached to a commit. It is
// intentionally derived from decision_records instead of capture_events so it
// still works after historical capture rows are pruned.
type DecisionCommitGroup struct {
	CommitOID string
	EventSeqs []int64
	Paths     []string
	Count     int
}

// AppendDecision inserts a decision row and returns its monotonic cursor ID.
func AppendDecision(ctx context.Context, d *DB, rec DecisionRecord) (int64, error) {
	if rec.Kind == "" {
		return 0, fmt.Errorf("state: AppendDecision: empty kind")
	}
	if rec.DecisionTS == 0 {
		rec.DecisionTS = nowSeconds()
	}
	const q = `
INSERT INTO decision_records(
    decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
    branch_ref, branch_generation, action_taken, user_message
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	res, err := d.conn.ExecContext(ctx, q,
		rec.DecisionTS, rec.Kind, rec.Path, rec.Reason, rec.EventSeq, rec.HeadSHA,
		rec.CommitOID, rec.BranchRef, rec.BranchGeneration, rec.ActionTaken, rec.UserMessage,
	)
	if err != nil {
		return 0, fmt.Errorf("state: append decision: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("state: decision id: %w", err)
	}
	return id, nil
}

// RecentDecisions returns the newest decisions first.
func RecentDecisions(ctx context.Context, d *DB, limit int) ([]DecisionRecord, error) {
	return queryDecisions(ctx, d, `
SELECT id, decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
       branch_ref, branch_generation, action_taken, user_message
FROM decision_records
ORDER BY id DESC
LIMIT ?`, clampDecisionLimit(limit))
}

// DecisionsForPath returns the newest decisions for path first.
func DecisionsForPath(ctx context.Context, d *DB, path string, limit int) ([]DecisionRecord, error) {
	if path == "" {
		return nil, fmt.Errorf("state: DecisionsForPath: empty path")
	}
	return queryDecisions(ctx, d, `
SELECT id, decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
       branch_ref, branch_generation, action_taken, user_message
FROM decision_records
WHERE path = ?
ORDER BY id DESC
	LIMIT ?`, path, clampDecisionLimit(limit))
}

// DecisionsForPathSince returns path decisions with id greater than cursorID in
// insertion order. This supports watch polling with a path filter.
func DecisionsForPathSince(ctx context.Context, d *DB, path string, cursorID int64, limit int) ([]DecisionRecord, error) {
	if path == "" {
		return nil, fmt.Errorf("state: DecisionsForPathSince: empty path")
	}
	if cursorID < 0 {
		return nil, fmt.Errorf("state: DecisionsForPathSince: negative cursor %d", cursorID)
	}
	return queryDecisions(ctx, d, `
SELECT id, decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
       branch_ref, branch_generation, action_taken, user_message
FROM decision_records
WHERE path = ? AND id > ?
ORDER BY id ASC
LIMIT ?`, path, cursorID, clampDecisionLimit(limit))
}

// DecisionsForEvent returns decisions linked to a capture event in insertion
// order so callers can narrate the event lifecycle.
func DecisionsForEvent(ctx context.Context, d *DB, eventSeq int64, limit int) ([]DecisionRecord, error) {
	if eventSeq <= 0 {
		return nil, fmt.Errorf("state: DecisionsForEvent: invalid event seq %d", eventSeq)
	}
	return queryDecisions(ctx, d, `
SELECT id, decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
       branch_ref, branch_generation, action_taken, user_message
FROM decision_records
WHERE event_seq = ?
ORDER BY id ASC
LIMIT ?`, eventSeq, clampDecisionLimit(limit))
}

// DecisionsForCommit returns decisions linked to a commit, newest first.
func DecisionsForCommit(ctx context.Context, d *DB, commitOID string, limit int) ([]DecisionRecord, error) {
	if commitOID == "" {
		return nil, fmt.Errorf("state: DecisionsForCommit: empty commit oid")
	}
	return queryDecisions(ctx, d, `
SELECT id, decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
       branch_ref, branch_generation, action_taken, user_message
FROM decision_records
WHERE commit_oid = ?
ORDER BY id DESC
LIMIT ?`, commitOID, clampDecisionLimit(limit))
}

// DecisionGroupForCommit returns the unique event seqs and paths attached to a
// commit decision. Rows are loaded through the append-only ledger so callers can
// explain grouped commits even after capture_events pruning.
func DecisionGroupForCommit(ctx context.Context, d *DB, commitOID string, limit int) (DecisionCommitGroup, error) {
	rows, err := DecisionsForCommit(ctx, d, commitOID, limit)
	if err != nil {
		return DecisionCommitGroup{}, err
	}
	group := DecisionCommitGroup{CommitOID: commitOID, Count: len(rows)}
	seqs := map[int64]struct{}{}
	paths := map[string]struct{}{}
	for _, row := range rows {
		if row.EventSeq.Valid {
			seqs[row.EventSeq.Int64] = struct{}{}
		}
		if row.Path.Valid && row.Path.String != "" {
			paths[row.Path.String] = struct{}{}
		}
	}
	group.EventSeqs = make([]int64, 0, len(seqs))
	for seq := range seqs {
		group.EventSeqs = append(group.EventSeqs, seq)
	}
	sort.Slice(group.EventSeqs, func(i, j int) bool {
		return group.EventSeqs[i] < group.EventSeqs[j]
	})
	group.Paths = make([]string, 0, len(paths))
	for path := range paths {
		group.Paths = append(group.Paths, path)
	}
	sort.Strings(group.Paths)
	return group, nil
}

// DecisionsSince returns decisions with id greater than cursorID in insertion
// order. This is the polling primitive for watch-style CLI output.
func DecisionsSince(ctx context.Context, d *DB, cursorID int64, limit int) ([]DecisionRecord, error) {
	if cursorID < 0 {
		return nil, fmt.Errorf("state: DecisionsSince: negative cursor %d", cursorID)
	}
	return queryDecisions(ctx, d, `
SELECT id, decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
       branch_ref, branch_generation, action_taken, user_message
FROM decision_records
WHERE id > ?
ORDER BY id ASC
	LIMIT ?`, cursorID, clampDecisionLimit(limit))
}

// LatestDecisionID returns the highest decision cursor ID, or 0 when no
// decisions exist.
func LatestDecisionID(ctx context.Context, d *DB) (int64, error) {
	var id sql.NullInt64
	if err := d.readSQL().QueryRowContext(ctx, `SELECT MAX(id) FROM decision_records`).Scan(&id); err != nil {
		return 0, fmt.Errorf("state: latest decision id: %w", err)
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
}

func queryDecisions(ctx context.Context, d *DB, q string, args ...any) ([]DecisionRecord, error) {
	rows, err := d.readSQL().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("state: query decisions: %w", err)
	}
	defer rows.Close()

	var out []DecisionRecord
	for rows.Next() {
		var rec DecisionRecord
		if err := rows.Scan(&rec.ID, &rec.DecisionTS, &rec.Kind, &rec.Path, &rec.Reason,
			&rec.EventSeq, &rec.HeadSHA, &rec.CommitOID, &rec.BranchRef,
			&rec.BranchGeneration, &rec.ActionTaken, &rec.UserMessage); err != nil {
			return nil, fmt.Errorf("state: scan decision: %w", err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iter decisions: %w", err)
	}
	return out, nil
}

func clampDecisionLimit(limit int) int {
	if limit <= 0 {
		return defaultDecisionLimit
	}
	if limit > maxDecisionLimit {
		return maxDecisionLimit
	}
	return limit
}
