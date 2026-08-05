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
		report.State = "degraded"
		if report.ErrorRepeatCount >= 2 {
			report.State = "needs_attention"
		}
		report.BlockedSeq = replayErrorSeq(report.LastError)
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
		var outcome, source, selectedGroups sql.NullString
		err = conn.QueryRowContext(ctx, `
SELECT outcome, source, selected_groups
FROM intent_planner_windows
WHERE fallback_used=1
ORDER BY id DESC LIMIT 1`).Scan(&outcome, &source, &selectedGroups)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return report, fmt.Errorf("latest fallback window: %w", err)
		}
		if err == nil {
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
