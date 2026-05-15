package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// TestStatus_PlannerErrorRateRecent_EmptyLedger asserts that with no
// decision_records rows, the planner error rate reports 0.0 (not NaN).
// Edge case for the "fixed denominator" policy: empty / window_size = 0.
func TestStatus_PlannerErrorRateRecent_EmptyLedger(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, _ := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")

	report := runStatusJSON(ctx, t, repo)
	if report.IntentStrategy.PlannerErrorRateRecent != 0 {
		t.Fatalf("PlannerErrorRateRecent=%v want 0.0", report.IntentStrategy.PlannerErrorRateRecent)
	}
	if report.IntentStrategy.SingletonCommitRateRecent != 0 {
		t.Fatalf("SingletonCommitRateRecent=%v want 0.0", report.IntentStrategy.SingletonCommitRateRecent)
	}
	if report.IntentStrategy.PlannerErrorRateRecentWarn {
		t.Fatalf("PlannerErrorRateRecentWarn must stay false on empty ledger")
	}
}

// TestStatus_PlannerErrorRateRecent_HalfWindow_FixedDenominator asserts
// that with 50 decisions of which all 50 are planner errors, the rate is
// 50/100 = 0.5 (NOT 50/50 = 1.0). Documents the fixed-denominator policy
// chosen for sub-window ledgers.
func TestStatus_PlannerErrorRateRecent_HalfWindow_FixedDenominator(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")

	// 50 planner-error decisions; ledger size < window denominator.
	for i := 0; i < 50; i++ {
		if _, err := state.AppendDecision(ctx, d, state.DecisionRecord{
			DecisionTS:  float64(1_000_000 + i),
			Kind:        state.DecisionKindIntentPlannerError,
			Path:        sqlNullStr("internal/foo.go"),
			Reason:      sqlNullStr(fmt.Sprintf("planner failure %d", i)),
			ActionTaken: sqlNullStr("intent_planner_error"),
		}); err != nil {
			t.Fatalf("AppendDecision %d: %v", i, err)
		}
	}

	report := runStatusJSON(ctx, t, repo)
	want := 0.5
	if got := report.IntentStrategy.PlannerErrorRateRecent; got != want {
		t.Fatalf("PlannerErrorRateRecent=%v want %v (fixed denominator: 50/%d)",
			got, want, IntentRecentDecisionWindow)
	}
	if !report.IntentStrategy.PlannerErrorRateRecentWarn {
		t.Fatalf("PlannerErrorRateRecentWarn must be true when rate=%v exceeds %v",
			report.IntentStrategy.PlannerErrorRateRecent,
			IntentPlannerErrorRateWarnThreshold)
	}
}

// TestStatus_PlannerErrorRateRecent_Exactly100Decisions asserts the rate
// at the exact window boundary: 100 decisions, 7 planner errors → 0.07.
// Locks the boundary case where the LIMIT and the ledger size match.
func TestStatus_PlannerErrorRateRecent_Exactly100Decisions(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")

	// 100 decisions, 7 of them planner errors. The newest 100 should
	// include all rows since we appended exactly 100.
	for i := 0; i < 100; i++ {
		kind := state.DecisionKindCommitted
		if i < 7 {
			kind = state.DecisionKindIntentPlannerError
		}
		if _, err := state.AppendDecision(ctx, d, state.DecisionRecord{
			DecisionTS:  float64(2_000_000 + i),
			Kind:        kind,
			Path:        sqlNullStr(fmt.Sprintf("file-%d.go", i)),
			CommitOID:   sqlNullStr(commitOIDForIndex(i)),
			ActionTaken: sqlNullStr("test"),
		}); err != nil {
			t.Fatalf("AppendDecision %d: %v", i, err)
		}
	}

	report := runStatusJSON(ctx, t, repo)
	if got := report.IntentStrategy.PlannerErrorRateRecent; got != 0.07 {
		t.Fatalf("PlannerErrorRateRecent=%v want 0.07", got)
	}
	if !report.IntentStrategy.PlannerErrorRateRecentWarn {
		t.Fatalf("PlannerErrorRateRecentWarn must be true at rate=0.07 (>0.05)")
	}
}

// TestStatus_PlannerErrorRateRecent_OnlyMostRecentWindowCounted asserts
// that older planner errors past the IntentRecentDecisionWindow boundary
// do not contribute to the rate. The query must use ORDER BY id DESC
// LIMIT N before counting kind=intent_planner_error.
func TestStatus_PlannerErrorRateRecent_OnlyMostRecentWindowCounted(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")

	// 200 rows: first 100 are all planner errors (the OLDEST), next 100
	// are committed. The newest-100-window contains zero planner errors,
	// so the rate must be 0.0 even though the ledger holds 100 errors.
	for i := 0; i < 100; i++ {
		if _, err := state.AppendDecision(ctx, d, state.DecisionRecord{
			DecisionTS:  float64(3_000_000 + i),
			Kind:        state.DecisionKindIntentPlannerError,
			Path:        sqlNullStr(fmt.Sprintf("old-%d.go", i)),
			ActionTaken: sqlNullStr("intent_planner_error"),
		}); err != nil {
			t.Fatalf("AppendDecision old %d: %v", i, err)
		}
	}
	for i := 0; i < 100; i++ {
		if _, err := state.AppendDecision(ctx, d, state.DecisionRecord{
			DecisionTS:  float64(4_000_000 + i),
			Kind:        state.DecisionKindCommitted,
			Path:        sqlNullStr(fmt.Sprintf("new-%d.go", i)),
			CommitOID:   sqlNullStr(commitOIDForIndex(100 + i)),
			ActionTaken: sqlNullStr("event published"),
		}); err != nil {
			t.Fatalf("AppendDecision new %d: %v", i, err)
		}
	}

	report := runStatusJSON(ctx, t, repo)
	if got := report.IntentStrategy.PlannerErrorRateRecent; got != 0 {
		t.Fatalf("PlannerErrorRateRecent=%v want 0.0 (older errors must not count)", got)
	}
}

// TestStatus_SingletonCommitRateRecent_GroupedVsSingle asserts that grouped
// commits (multi-event sharing the same commit_oid) are NOT counted as
// singletons, while single-event commits are.
func TestStatus_SingletonCommitRateRecent_GroupedVsSingle(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")

	// 2 grouped commits (3 events each) + 6 singleton commits. Total = 8
	// distinct commit_oids; 6 are singletons. Rate = 6/100 = 0.06.
	tsBase := float64(5_000_000)
	const groupedPerOID = 3
	for i, oid := range []string{commitOIDForIndex(1000), commitOIDForIndex(1001)} {
		for j := 0; j < groupedPerOID; j++ {
			seq := int64(i*groupedPerOID + j + 1)
			if _, err := state.AppendDecision(ctx, d, state.DecisionRecord{
				DecisionTS: tsBase,
				Kind:       state.DecisionKindCommitted,
				Path:       sqlNullStr(fmt.Sprintf("grouped-%d-%d.go", i, j)),
				EventSeq:   sql.NullInt64{Int64: seq, Valid: true},
				CommitOID:  sqlNullStr(oid),
				Reason:     sqlNullStr("intent_group: same intent"),
			}); err != nil {
				t.Fatalf("AppendDecision grouped: %v", err)
			}
			tsBase++
		}
	}
	for i := 0; i < 6; i++ {
		if _, err := state.AppendDecision(ctx, d, state.DecisionRecord{
			DecisionTS: tsBase,
			Kind:       state.DecisionKindCommitted,
			Path:       sqlNullStr(fmt.Sprintf("solo-%d.go", i)),
			EventSeq:   sql.NullInt64{Int64: int64(2000 + i), Valid: true},
			CommitOID:  sqlNullStr(commitOIDForIndex(2000 + i)),
			Reason:     sqlNullStr("event published"),
		}); err != nil {
			t.Fatalf("AppendDecision singleton: %v", err)
		}
		tsBase++
	}

	report := runStatusJSON(ctx, t, repo)
	if got := report.IntentStrategy.SingletonCommitRateRecent; got != 0.06 {
		t.Fatalf("SingletonCommitRateRecent=%v want 0.06 (6 singletons over fixed window 100)", got)
	}
}

// TestDiagnose_PlannerErrorRateRecentWarnRemediation asserts the
// remediation hint surfaces when the planner-error rate exceeds the 5%
// threshold. The hint must reference the rejects-log filename so an
// operator knows where to look.
func TestDiagnose_PlannerErrorRateRecentWarnRemediation(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")

	// 20 planner errors + 80 committed decisions: rate = 0.20 > 0.05.
	for i := 0; i < 20; i++ {
		if _, err := state.AppendDecision(ctx, d, state.DecisionRecord{
			DecisionTS:  float64(6_000_000 + i),
			Kind:        state.DecisionKindIntentPlannerError,
			Path:        sqlNullStr(fmt.Sprintf("err-%d.go", i)),
			ActionTaken: sqlNullStr("intent_planner_error"),
		}); err != nil {
			t.Fatalf("AppendDecision err: %v", err)
		}
	}
	for i := 0; i < 80; i++ {
		if _, err := state.AppendDecision(ctx, d, state.DecisionRecord{
			DecisionTS: float64(7_000_000 + i),
			Kind:       state.DecisionKindCommitted,
			Path:       sqlNullStr(fmt.Sprintf("ok-%d.go", i)),
			EventSeq:   sql.NullInt64{Int64: int64(i + 1), Valid: true},
			CommitOID:  sqlNullStr(commitOIDForIndex(3000 + i)),
		}); err != nil {
			t.Fatalf("AppendDecision committed: %v", err)
		}
	}

	var jsonOut bytes.Buffer
	if err := runDiagnose(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runDiagnose json: %v", err)
	}
	var report diagnoseReport
	if err := json.Unmarshal(jsonOut.Bytes(), &report); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if report.IntentStrategy.PlannerErrorRateRecent != 0.20 {
		t.Fatalf("PlannerErrorRateRecent=%v want 0.2", report.IntentStrategy.PlannerErrorRateRecent)
	}
	if !report.IntentStrategy.PlannerErrorRateRecentWarn {
		t.Fatalf("PlannerErrorRateRecentWarn must be true; report=%+v", report.IntentStrategy)
	}
	var found string
	for _, hint := range report.Remediation {
		if strings.Contains(hint, "planner_error_rate_recent") {
			found = hint
			break
		}
	}
	if found == "" {
		t.Fatalf("remediation missing planner_error_rate_recent warn hint; got %v", report.Remediation)
	}
	if !strings.Contains(found, ai.IntentRejectsFileName) {
		t.Fatalf("remediation must reference rejects log filename %q; got %q", ai.IntentRejectsFileName, found)
	}
}

// TestStatus_IntentStageDiffCapExposedInJSON asserts the per-stage planner
// diff cap surfaces in the status JSON so operators inspecting the
// effective budget without re-deriving the constant can verify it.
func TestStatus_IntentStageDiffCapExposedInJSON(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, _ := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")

	report := runStatusJSON(ctx, t, repo)
	if got := report.IntentStrategy.IntentStageDiffCap; got != ai.IntentStageDiffCap {
		t.Fatalf("IntentStageDiffCap=%d want %d", got, ai.IntentStageDiffCap)
	}
}

// TestDiagnose_IntentStageDiffCapExposedInJSON mirrors the status check
// against `acd diagnose --json`. Both commands surface the same
// intent_strategy block — enforce parity.
func TestDiagnose_IntentStageDiffCapExposedInJSON(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, _ := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")

	var jsonOut bytes.Buffer
	if err := runDiagnose(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runDiagnose json: %v", err)
	}
	var report diagnoseReport
	if err := json.Unmarshal(jsonOut.Bytes(), &report); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if got := report.IntentStrategy.IntentStageDiffCap; got != ai.IntentStageDiffCap {
		t.Fatalf("diagnose IntentStageDiffCap=%d want %d", got, ai.IntentStageDiffCap)
	}
}

// TestDefaultIntentDeferLimit_TunedToOne asserts the tuned default for
// ACD_INTENT_DEFER_LIMIT is 1 (Wave 2 lowering from 2). Catches a
// regression that would silently revert the constant.
func TestDefaultIntentDeferLimit_TunedToOne(t *testing.T) {
	if ai.DefaultIntentDeferLimit != 1 {
		t.Fatalf("DefaultIntentDeferLimit=%d want 1", ai.DefaultIntentDeferLimit)
	}
}

// TestIntentStageDiffCap_TunedTo16K asserts the per-stage planner diff cap
// constant is 16000. Locks the contract documented in CLAUDE.md and
// CHANGELOG so regressions surface in CI.
func TestIntentStageDiffCap_TunedTo16K(t *testing.T) {
	if ai.IntentStageDiffCap != 16000 {
		t.Fatalf("IntentStageDiffCap=%d want 16000", ai.IntentStageDiffCap)
	}
	if ai.DiffCap != 4000 {
		t.Fatalf("DiffCap=%d want 4000 (per-event path must NOT change)", ai.DiffCap)
	}
}

// runStatusJSON is a shared helper for tests that need to inspect the
// JSON-shaped status report.
func runStatusJSON(ctx context.Context, t *testing.T, repo string) statusReport {
	t.Helper()
	var out bytes.Buffer
	if err := runStatus(ctx, &out, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var report statusReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	return report
}

// commitOIDForIndex returns a deterministic 40-char hex commit OID for an
// integer seed. Used by rate-calc tests that need distinct OIDs without
// running git commit.
func commitOIDForIndex(i int) string {
	return fmt.Sprintf("%040x", i)
}
