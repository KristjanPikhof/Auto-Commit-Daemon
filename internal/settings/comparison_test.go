package settings

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestComparisonExactMetricsDistinctCommitsAndNonCausalLabel(t *testing.T) {
	svc, _ := testService(t, nil, nil, nil)
	ctx := context.Background()
	profile := "fast"
	windows := []state.IntentPlannerWindow{
		{PlannedTS: 1, BranchRef: "refs/heads/main", BranchGeneration: 1,
			ConfigRevisionID: sql.NullInt64{Int64: 7, Valid: true}, ConfigProfile: sql.NullString{String: profile, Valid: true},
			DurationMS: sql.NullInt64{Int64: 100, Valid: true}, RetryCount: 1,
			Outcome: sql.NullString{String: "success", Valid: true}, DeferredSeqs: []int64{1, 2},
			Forced: true, SelectedGroups: []state.IntentPlannerWindowGroup{{SelectedSeqs: []int64{1}}}},
		{PlannedTS: 2, BranchRef: "refs/heads/main", BranchGeneration: 1,
			ConfigRevisionID: sql.NullInt64{Int64: 7, Valid: true}, ConfigProfile: sql.NullString{String: profile, Valid: true},
			DurationMS: sql.NullInt64{Int64: 200, Valid: true}, RetryCount: 2,
			Outcome: sql.NullString{String: "fallback", Valid: true}, FallbackUsed: true,
			DeferredSeqs: []int64{3}},
	}
	for _, window := range windows {
		if _, err := state.AppendIntentPlannerWindow(ctx, svc.db, window); err != nil {
			t.Fatal(err)
		}
	}
	for _, oid := range []string{"commit-a", "commit-a", "commit-b"} {
		if _, err := state.AppendDecision(ctx, svc.db, state.DecisionRecord{
			Kind: "committed", CommitOID: sql.NullString{String: oid, Valid: true},
			ConfigRevisionID: sql.NullInt64{Int64: 7, Valid: true}, ConfigProfile: sql.NullString{String: profile, Valid: true},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.db.SQL().Exec(`UPDATE intent_planner_windows SET config_profile='fast' || char(27) || '[31m'; UPDATE decision_records SET config_profile='fast' || char(27) || '[31m'`); err != nil {
		t.Fatal(err)
	}
	comparison, err := CompareRevisions(ctx, svc.db.ReadSQL(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(comparison.Revisions) != 1 {
		t.Fatalf("comparison = %+v", comparison)
	}
	got := comparison.Revisions[0]
	if got.RevisionID != 7 || got.Profile != "fast" || got.PlannerWindows != 2 ||
		got.PrimarySuccessWindows != 1 || got.FallbackWindows != 1 || got.RetryCount != 3 ||
		got.MedianLatencyMS != 150 || got.DeferredEvents != 3 || got.ForcedSingletons != 1 ||
		got.DistinctCommits != 2 {
		t.Fatalf("metrics = %+v", got)
	}
	if !strings.Contains(strings.ToLower(comparison.Interpretation), "not causal") ||
		!strings.Contains(strings.ToLower(comparison.Interpretation), "sequential") {
		t.Fatalf("interpretation = %q", comparison.Interpretation)
	}
	encoded, _ := json.Marshal(comparison)
	for _, forbidden := range []string{"prompt", "diff", "raw response", "credential", "\x1b"} {
		if strings.Contains(strings.ToLower(string(encoded)), strings.ToLower(forbidden)) {
			t.Fatalf("comparison leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestComparisonPreV14IsEmptyAndReadOnly(t *testing.T) {
	conn, err := sql.Open("sqlite", "file:"+t.TempDir()+"/old.db")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Exec(`
CREATE TABLE intent_planner_windows(
 id INTEGER PRIMARY KEY, planned_ts REAL, branch_ref TEXT, branch_generation INTEGER);
CREATE TABLE decision_records(id INTEGER PRIMARY KEY, decision_ts REAL, kind TEXT);
PRAGMA user_version=13;`); err != nil {
		t.Fatal(err)
	}
	var beforeVersion int
	_ = conn.QueryRow(`PRAGMA user_version`).Scan(&beforeVersion)
	comparison, err := CompareRevisions(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(comparison.Revisions) != 0 || comparison.Interpretation == "" {
		t.Fatalf("old comparison = %+v", comparison)
	}
	var afterVersion, tables int
	_ = conn.QueryRow(`PRAGMA user_version`).Scan(&afterVersion)
	_ = conn.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table'`).Scan(&tables)
	if beforeVersion != 13 || afterVersion != 13 || tables != 2 {
		t.Fatalf("old DB mutated: version %d->%d tables=%d", beforeVersion, afterVersion, tables)
	}
}

func TestComparisonMissingTablesReturnsValidEmptyProjection(t *testing.T) {
	conn, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	comparison, err := CompareRevisions(context.Background(), conn, 1)
	if err != nil || comparison.Revisions == nil || comparison.Interpretation == "" {
		t.Fatalf("missing table comparison = %+v err=%v", comparison, err)
	}
}
