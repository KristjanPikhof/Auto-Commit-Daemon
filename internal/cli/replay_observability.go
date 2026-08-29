package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
)

const (
	replayCandidateIDLimit = 8
	replayRepeatCountCap   = 1_000_000
)

var replayErrorSeqPattern = regexp.MustCompile(
	`(?i)\b(?:capture|event|seq(?:uence)?)\s*[=:]?\s*(\d+)\b`,
)

type replayObservabilityReport struct {
	State            string   `json:"state"`
	LastError        string   `json:"last_error,omitempty"`
	ErrorRepeatCount int      `json:"error_repeat_count,omitempty"`
	ErrorLastSeenTS  int64    `json:"error_last_seen_ts,omitempty"`
	BlockedSeq       int64    `json:"blocked_seq,omitempty"`
	CandidateIDs     []string `json:"candidate_ids,omitempty"`
	LastFallbackMode string   `json:"last_fallback_mode,omitempty"`
	LastFallbackSize int      `json:"last_fallback_size,omitempty"`
}

func loadReplayObservabilityReport(
	ctx context.Context,
	conn *sql.DB,
) (replayObservabilityReport, error) {
	report := replayObservabilityReport{State: "active"}
	if conn == nil {
		return report, nil
	}
	hasMeta, err := sqliteTableExists(ctx, conn, "daemon_meta")
	if err != nil {
		return report, fmt.Errorf("daemon metadata table check: %w", err)
	}
	if !hasMeta {
		return report, nil
	}
	if value, ok, err := metaLookup(ctx, conn, "last_replay_error"); err != nil {
		return report, fmt.Errorf("last replay error: %w", err)
	} else if ok {
		report.LastError = sanitizeObservabilityText(value)
	}
	if report.LastError != "" {
		if value, ok, err := metaLookup(
			ctx, conn, "replay.error_repeat_count",
		); err != nil {
			return report, fmt.Errorf("replay error repeat count: %w", err)
		} else if ok {
			if count, parseErr := strconv.Atoi(strings.TrimSpace(value)); parseErr == nil {
				if count < 0 {
					count = 0
				}
				if count > replayRepeatCountCap {
					count = replayRepeatCountCap
				}
				report.ErrorRepeatCount = count
			}
		}
	}
	if report.LastError != "" {
		if value, ok, err := metaLookup(
			ctx, conn, "replay.error_last_seen_ts",
		); err != nil {
			return report, fmt.Errorf("replay error last seen time: %w", err)
		} else if ok {
			if seen, parseErr := strconv.ParseInt(
				strings.TrimSpace(value), 10, 64,
			); parseErr == nil && seen > 0 {
				report.ErrorLastSeenTS = seen
			}
		}
		report.State = "degraded"
		report.BlockedSeq = replayErrorSeq(report.LastError)
	}
	durableAttention := false
	if value, ok, err := metaLookup(
		ctx, conn, "intent.v2.needs_attention",
	); err != nil {
		return report, fmt.Errorf("intent replay blocked reason: %w", err)
	} else if ok && sanitizeObservabilityText(value) != "" {
		durableAttention = true
	}
	if value, ok, err := metaLookup(
		ctx, conn, daemon.MetaKeyBranchTransitionNeedsAttention,
	); err != nil {
		return report, fmt.Errorf("branch transition blocked reason: %w", err)
	} else if attention := sanitizeObservabilityText(value); ok && attention != "" {
		durableAttention = true
		report.LastError = attention
	}

	hasCandidates, err := sqliteTableExists(ctx, conn, "intent_candidates")
	if err != nil {
		return report, fmt.Errorf("intent candidate table check: %w", err)
	}
	hasMembership, err := sqliteTableExists(
		ctx, conn, "intent_candidate_events")
	if err != nil {
		return report, fmt.Errorf("intent candidate membership check: %w", err)
	}
	hasEvents, err := sqliteTableExists(ctx, conn, "capture_events")
	if err != nil {
		return report, fmt.Errorf("capture event table check: %w", err)
	}
	if report.LastError != "" && hasCandidates && hasMembership && hasEvents {
		if report.BlockedSeq == 0 {
			var blocked sql.NullInt64
			if scanErr := conn.QueryRowContext(ctx, `
SELECT MIN(e.seq)
FROM capture_events e
JOIN intent_candidate_events ce
  ON ce.event_seq=e.seq AND ce.membership_state='active'
JOIN intent_candidates c ON c.id=ce.candidate_id
WHERE e.state='pending'
  AND c.status IN ('open','waiting','ready','soft_published','blocked')`,
			).Scan(&blocked); scanErr == nil && blocked.Valid {
				report.BlockedSeq = blocked.Int64
			}
		}
		report.CandidateIDs, err = replayCandidateIDs(
			ctx, conn, report.BlockedSeq, report.LastError)
		if err != nil {
			return report, err
		}
	}
	if hasCandidates && hasMembership && hasEvents {
		blockedSeq, blocked, blockErr := durableReplayAttention(
			ctx, conn,
		)
		if blockErr != nil {
			return report, blockErr
		}
		if blocked {
			durableAttention = true
			if report.BlockedSeq == 0 {
				report.BlockedSeq = blockedSeq
			}
		}
	}
	if durableAttention {
		report.State = "needs_attention"
	}

	hasWindows, err := sqliteTableExists(ctx, conn, "intent_planner_windows")
	if err != nil {
		return report, fmt.Errorf("intent planner window table check: %w", err)
	}
	var schemaVersion int
	if err := conn.QueryRowContext(ctx, `PRAGMA user_version`).Scan(
		&schemaVersion); err != nil {
		return report, fmt.Errorf("replay schema version: %w", err)
	}
	if hasWindows && schemaVersion >= 14 {
		if value, ok, metaErr := metaLookup(
			ctx, conn, "intent.v2.last_fallback_mode"); metaErr != nil {
			return report, fmt.Errorf("last forward fallback mode: %w", metaErr)
		} else if ok {
			report.LastFallbackMode = sanitizeObservabilityText(value)
		}
		if value, ok, metaErr := metaLookup(
			ctx, conn, "intent.v2.last_fallback_size"); metaErr != nil {
			return report, fmt.Errorf("last forward fallback size: %w", metaErr)
		} else if ok {
			report.LastFallbackSize, _ = strconv.Atoi(strings.TrimSpace(value))
		}
		var outcome, source, selectedGroups sql.NullString
		err = conn.QueryRowContext(ctx, `
SELECT outcome, source, selected_groups
FROM intent_planner_windows
WHERE fallback_used=1
ORDER BY id DESC LIMIT 1`).Scan(&outcome, &source, &selectedGroups)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return report, fmt.Errorf("latest fallback window: %w", err)
		}
		if err == nil && report.LastFallbackMode == "" {
			report.LastFallbackMode = sanitizeObservabilityText(outcome.String)
			if report.LastFallbackMode == "" {
				report.LastFallbackMode = sanitizeObservabilityText(source.String)
			}
			var groups []struct {
				SelectedSeqs []int64 `json:"selected_seqs"`
			}
			if json.Unmarshal([]byte(selectedGroups.String), &groups) == nil {
				for _, group := range groups {
					report.LastFallbackSize += len(group.SelectedSeqs)
				}
			}
		}
	}
	return report, nil
}

// durableReplayAttention returns only persisted publication barriers. A
// repeated replay error is deliberately absent: the daemon is still retrying
// until one of these durable predicates says otherwise.
func durableReplayAttention(
	ctx context.Context,
	conn *sql.DB,
) (int64, bool, error) {
	branchRef, branchGeneration, ok, err := currentReplayPair(ctx, conn)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}
	var seq sql.NullInt64
	err = conn.QueryRowContext(ctx, `
SELECT MIN(seq) FROM (
    SELECT terminal.seq AS seq
    FROM capture_events terminal
    WHERE terminal.state IN ('blocked_conflict','failed')
      AND terminal.branch_ref=?
      AND terminal.branch_generation=?
    UNION ALL
    SELECT pending.seq AS seq
    FROM intent_candidates candidate
    JOIN intent_candidate_events membership
      ON membership.candidate_id=candidate.id
     AND membership.membership_state='active'
    JOIN capture_events pending
      ON pending.seq=membership.event_seq
     AND pending.state='pending'
	    WHERE (candidate.status='blocked'
	       OR candidate.verification_status IN
	          ('timed_out','needs_attention')
	       OR (candidate.verification_status='failed'
	           AND candidate.status<>'waiting'))
      AND candidate.branch_ref=?
      AND candidate.branch_generation=?
)`, branchRef, branchGeneration, branchRef, branchGeneration).Scan(&seq)
	if err != nil {
		return 0, false, fmt.Errorf("durable replay attention: %w", err)
	}
	return seq.Int64, seq.Valid, nil
}

// currentReplayPair reads the durable branch observation maintained by the
// run loop. Production heartbeat writes leave daemon_state's branch columns
// NULL, so those columns are only a compatibility fallback for older state.
func currentReplayPair(
	ctx context.Context,
	conn *sql.DB,
) (string, int64, bool, error) {
	var token, rawGeneration sql.NullString
	var tokenPresent, generationPresent int
	err := conn.QueryRowContext(ctx, `
SELECT
    (SELECT value FROM daemon_meta WHERE key='branch_token'),
    EXISTS(SELECT 1 FROM daemon_meta WHERE key='branch_token'),
    (SELECT value FROM daemon_meta WHERE key='branch.generation'),
    EXISTS(SELECT 1 FROM daemon_meta WHERE key='branch.generation')`,
	).Scan(&token, &tokenPresent, &rawGeneration, &generationPresent)
	if err != nil {
		return "", 0, false, fmt.Errorf("current replay metadata: %w", err)
	}
	if tokenPresent != 0 {
		// A present durable token is authoritative, including a valid
		// detached token. Incomplete or malformed durable metadata fails
		// closed instead of reviving a stale legacy daemon_state pair.
		if !token.Valid || generationPresent == 0 || !rawGeneration.Valid {
			return "", 0, false, nil
		}
		branchRef := replayTokenBranchRef(token.String)
		generation, parseErr := strconv.ParseInt(
			strings.TrimSpace(rawGeneration.String), 10, 64,
		)
		if branchRef != "" && parseErr == nil && generation >= 1 {
			return branchRef, generation, true, nil
		}
		return "", 0, false, nil
	}

	var branchRef sql.NullString
	var generation sql.NullInt64
	err = conn.QueryRowContext(ctx, `
SELECT branch_ref, branch_generation
FROM daemon_state
WHERE id=1`).Scan(&branchRef, &generation)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, false, nil
	}
	if err != nil {
		return "", 0, false, fmt.Errorf("legacy replay branch anchor: %w", err)
	}
	if !branchRef.Valid || strings.TrimSpace(branchRef.String) == "" ||
		!generation.Valid || generation.Int64 < 1 {
		return "", 0, false, nil
	}
	return branchRef.String, generation.Int64, true, nil
}

func replayTokenBranchRef(token string) string {
	token = strings.TrimSpace(token)
	if rest, ok := strings.CutPrefix(token, "rev:"); ok {
		_, branchRef, found := strings.Cut(rest, " ")
		if found {
			return strings.TrimSpace(branchRef)
		}
		return ""
	}
	if rest, ok := strings.CutPrefix(token, "missing "); ok {
		return strings.TrimSpace(rest)
	}
	return ""
}

func replayErrorSeq(value string) int64 {
	match := replayErrorSeqPattern.FindStringSubmatch(value)
	if len(match) != 2 {
		return 0
	}
	seq, _ := strconv.ParseInt(match[1], 10, 64)
	if seq < 0 {
		return 0
	}
	return seq
}

func replayCandidateIDs(
	ctx context.Context,
	conn *sql.DB,
	seq int64,
	lastError string,
) ([]string, error) {
	seen := make(map[string]struct{})
	var ids []string
	add := func(id string) {
		id = sanitizeObservabilityText(id)
		if id == "" || len(ids) >= replayCandidateIDLimit {
			return
		}
		if _, exists := seen[id]; exists {
			return
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if seq > 0 {
		rows, err := conn.QueryContext(ctx, `
SELECT DISTINCT c.id
FROM intent_candidates c
JOIN intent_candidate_events ce
  ON ce.candidate_id=c.id AND ce.membership_state='active'
WHERE ce.event_seq=?
ORDER BY c.updated_ts DESC, c.id
LIMIT ?`, seq, replayCandidateIDLimit)
		if err != nil {
			return nil, fmt.Errorf("replay candidate IDs for seq: %w", err)
		}
		for rows.Next() {
			var id string
			if scanErr := rows.Scan(&id); scanErr != nil {
				rows.Close()
				return nil, fmt.Errorf("scan replay candidate ID: %w", scanErr)
			}
			add(id)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			rows.Close()
			return nil, fmt.Errorf("iterate replay candidate IDs: %w", rowsErr)
		}
		rows.Close()
	}
	if len(ids) >= replayCandidateIDLimit {
		return ids, nil
	}
	rows, err := conn.QueryContext(ctx, `
SELECT id
FROM intent_candidates
ORDER BY updated_ts DESC, id
LIMIT 128`)
	if err != nil {
		return nil, fmt.Errorf("candidate IDs for replay error: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			return nil, fmt.Errorf("scan replay error candidate ID: %w", scanErr)
		}
		if strings.Contains(lastError, id) {
			add(id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate replay error candidate IDs: %w", err)
	}
	return ids, nil
}

func renderReplayObservabilityHuman(
	out io.Writer,
	report replayObservabilityReport,
) {
	if report.State == "" {
		return
	}
	fmt.Fprintf(out, "Replay: %s", valueOrUnset(report.State))
	if report.ErrorRepeatCount > 0 {
		fmt.Fprintf(out, " repeats=%d", report.ErrorRepeatCount)
	}
	if report.ErrorLastSeenTS > 0 {
		fmt.Fprintf(out, " last_seen=%d", report.ErrorLastSeenTS)
	}
	if report.BlockedSeq > 0 {
		fmt.Fprintf(out, " blocked_seq=%d", report.BlockedSeq)
	}
	if len(report.CandidateIDs) > 0 {
		fmt.Fprintf(out, " candidates=%s", strings.Join(report.CandidateIDs, ","))
	}
	if report.LastFallbackMode != "" {
		fmt.Fprintf(out, " fallback=%s size=%d",
			report.LastFallbackMode, report.LastFallbackSize)
	}
	fmt.Fprintln(out)
	if report.LastError != "" {
		fmt.Fprintf(out, "  Last replay error: %s\n", report.LastError)
	}
}
