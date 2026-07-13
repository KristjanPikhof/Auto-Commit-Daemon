package settings

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"unicode"
)

const ComparisonInterpretation = "Descriptive sequential profile results; workloads differ over time and are not causal A/B samples."

type RevisionMetrics struct {
	RevisionID            int64   `json:"revision_id"`
	Profile               string  `json:"profile"`
	PlannerWindows        int     `json:"planner_windows"`
	PrimarySuccessWindows int     `json:"primary_success_windows"`
	FallbackWindows       int     `json:"fallback_windows"`
	RetryCount            int     `json:"retry_count"`
	MedianLatencyMS       float64 `json:"median_latency_ms"`
	DeferredEvents        int     `json:"deferred_events"`
	ForcedSingletons      int     `json:"forced_singletons"`
	DistinctCommits       int     `json:"distinct_commits"`
}

type Comparison struct {
	Interpretation string            `json:"interpretation"`
	Revisions      []RevisionMetrics `json:"revisions"`
}

type comparisonWindow struct {
	revision int64
	profile  string
	duration sql.NullInt64
	retries  int
	fallback bool
	outcome  sql.NullString
	forced   bool
	deferred string
	groups   string
}

// CompareRevisions derives privacy-safe operational summaries from exact
// revision-stamped planner windows and decision commit OIDs. It never returns
// provider/model text, prompts, diffs, errors, or decision messages.
func CompareRevisions(ctx context.Context, conn *sql.DB, revisionIDs ...int64) (Comparison, error) {
	out := Comparison{Interpretation: ComparisonInterpretation, Revisions: []RevisionMetrics{}}
	if conn == nil {
		return out, nil
	}
	hasWindows, err := comparisonTableExists(ctx, conn, "intent_planner_windows")
	if err != nil {
		return out, errors.New("acd settings: inspect planner metrics table")
	}
	hasDecisions, err := comparisonTableExists(ctx, conn, "decision_records")
	if err != nil {
		return out, errors.New("acd settings: inspect decision metrics table")
	}
	if !hasWindows && !hasDecisions {
		return out, nil
	}
	filter, args := comparisonRevisionFilter(revisionIDs)
	byRevision := map[int64]*RevisionMetrics{}
	latencies := map[int64][]int64{}
	if hasWindows {
		columns, err := comparisonColumns(ctx, conn, "intent_planner_windows")
		if err != nil {
			return out, errors.New("acd settings: inspect planner metrics columns")
		}
		for _, required := range []string{"config_revision_id", "config_profile", "duration_ms", "retry_count", "fallback_used", "outcome"} {
			if !columns[required] {
				return out, nil
			}
		}
		rows, err := conn.QueryContext(ctx, `
SELECT config_revision_id, config_profile, duration_ms, retry_count,
       fallback_used, outcome, forced, deferred_seqs, selected_groups
FROM intent_planner_windows
WHERE config_revision_id IS NOT NULL`+filter+`
ORDER BY config_revision_id, id`, args...)
		if err != nil {
			return out, errors.New("acd settings: query planner comparison metrics")
		}
		for rows.Next() {
			var row comparisonWindow
			var fallback, forced int
			if err := rows.Scan(&row.revision, &row.profile, &row.duration, &row.retries,
				&fallback, &row.outcome, &forced, &row.deferred, &row.groups); err != nil {
				rows.Close()
				return out, errors.New("acd settings: scan planner comparison metrics")
			}
			row.fallback, row.forced = fallback != 0, forced != 0
			metric := byRevision[row.revision]
			if metric == nil {
				metric = &RevisionMetrics{RevisionID: row.revision, Profile: sanitizeMetricLabel(row.profile)}
				byRevision[row.revision] = metric
			}
			metric.PlannerWindows++
			metric.RetryCount += row.retries
			if row.fallback {
				metric.FallbackWindows++
			} else if row.outcome.Valid && !isFailedOutcome(row.outcome.String) {
				metric.PrimarySuccessWindows++
			}
			if row.duration.Valid && row.duration.Int64 >= 0 {
				latencies[row.revision] = append(latencies[row.revision], row.duration.Int64)
			}
			var deferred []int64
			if json.Unmarshal([]byte(row.deferred), &deferred) == nil {
				metric.DeferredEvents += len(deferred)
			}
			if row.forced && containsSingletonGroup(row.groups) {
				metric.ForcedSingletons++
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return out, errors.New("acd settings: iterate planner comparison metrics")
		}
		rows.Close()
	}
	if hasDecisions {
		columns, err := comparisonColumns(ctx, conn, "decision_records")
		if err != nil {
			return out, errors.New("acd settings: inspect decision metrics columns")
		}
		if columns["config_revision_id"] && columns["config_profile"] {
			rows, err := conn.QueryContext(ctx, `
SELECT config_revision_id, COALESCE(config_profile,''), COUNT(DISTINCT commit_oid)
FROM decision_records
WHERE config_revision_id IS NOT NULL AND commit_oid IS NOT NULL`+filter+`
GROUP BY config_revision_id, config_profile`, args...)
			if err != nil {
				return out, errors.New("acd settings: query distinct commit metrics")
			}
			for rows.Next() {
				var revision int64
				var profile string
				var commits int
				if err := rows.Scan(&revision, &profile, &commits); err != nil {
					rows.Close()
					return out, errors.New("acd settings: scan distinct commit metrics")
				}
				metric := byRevision[revision]
				if metric == nil {
					metric = &RevisionMetrics{RevisionID: revision, Profile: sanitizeMetricLabel(profile)}
					byRevision[revision] = metric
				}
				if metric.Profile == "" {
					metric.Profile = sanitizeMetricLabel(profile)
				}
				metric.DistinctCommits += commits
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return out, errors.New("acd settings: iterate distinct commit metrics")
			}
			rows.Close()
		}
	}
	ids := make([]int64, 0, len(byRevision))
	for id := range byRevision {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		values := latencies[id]
		if len(values) > 0 {
			sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
			middle := len(values) / 2
			if len(values)%2 == 0 {
				byRevision[id].MedianLatencyMS = float64(values[middle-1]+values[middle]) / 2
			} else {
				byRevision[id].MedianLatencyMS = float64(values[middle])
			}
		}
		out.Revisions = append(out.Revisions, *byRevision[id])
	}
	return out, nil
}

func comparisonRevisionFilter(ids []int64) (string, []any) {
	valid := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id > 0 {
			valid = append(valid, id)
		}
	}
	if len(valid) == 0 {
		return "", nil
	}
	marks := make([]string, len(valid))
	args := make([]any, len(valid))
	for i, id := range valid {
		marks[i], args[i] = "?", id
	}
	return " AND config_revision_id IN (" + strings.Join(marks, ",") + ")", args
}

func comparisonTableExists(ctx context.Context, conn *sql.DB, name string) (bool, error) {
	var count int
	err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&count)
	return count > 0, err
}

func comparisonColumns(ctx context.Context, conn *sql.DB, table string) (map[string]bool, error) {
	rows, err := conn.QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = true
	}
	return out, rows.Err()
}

func containsSingletonGroup(raw string) bool {
	var groups []struct {
		Selected []int64 `json:"selected_seqs"`
	}
	if json.Unmarshal([]byte(raw), &groups) != nil {
		return false
	}
	for _, group := range groups {
		if len(group.Selected) == 1 {
			return true
		}
	}
	return false
}

func isFailedOutcome(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "failed", "error", "fallback", "rejected":
		return true
	default:
		return false
	}
}

func sanitizeMetricLabel(value string) string {
	value = stripANSI(value)
	value = strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) && r != '\u007f' {
			return r
		}
		return -1
	}, value)
	if len(value) > 128 {
		value = value[:128]
	}
	return strings.TrimSpace(value)
}

func stripANSI(value string) string {
	var out strings.Builder
	for i := 0; i < len(value); {
		if value[i] != 0x1b {
			out.WriteByte(value[i])
			i++
			continue
		}
		i++
		if i < len(value) && value[i] == '[' {
			i++
			for i < len(value) {
				c := value[i]
				i++
				if c >= 0x40 && c <= 0x7e {
					break
				}
			}
		}
	}
	return out.String()
}
