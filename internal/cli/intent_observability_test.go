package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

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

func TestStatus_IntentStrategyUsesEffectiveRuntimeProvider(t *testing.T) {
	t.Setenv(ai.EnvProvider, "deterministic")
	t.Setenv(ai.EnvModel, "stale-model")
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
		BranchRef: sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{
			Int64: 1, Valid: true,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSetMany(ctx, d, map[string]string{
		"ai.provider": "openai-compat",
		"ai.model":    "gpt-runtime",
	}); err != nil {
		t.Fatal(err)
	}

	report := runStatusJSON(ctx, t, repo)
	if report.IntentStrategy.EffectiveProvider != "openai-compat" ||
		report.IntentStrategy.EffectiveModel != "gpt-runtime" {
		t.Fatalf("intent strategy=%+v", report.IntentStrategy)
	}
}

func TestStatus_MessageQualitySummary(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")

	if _, err := state.AppendDecision(ctx, d, state.DecisionRecord{
		DecisionTS:  1,
		Kind:        state.DecisionKindMessageQualityRewrite,
		EventSeq:    sql.NullInt64{Int64: 10, Valid: true},
		Path:        sqlNullStr("internal/ai/provider.go"),
		Reason:      sqlNullStr("generic_subject"),
		ActionTaken: sqlNullStr("message quality rewrite"),
	}); err != nil {
		t.Fatalf("AppendDecision rewrite: %v", err)
	}
	if _, err := state.AppendDecision(ctx, d, state.DecisionRecord{
		DecisionTS:  2,
		Kind:        state.DecisionKindMessageQualityFallback,
		EventSeq:    sql.NullInt64{Int64: 11, Valid: true},
		Path:        sqlNullStr("internal/daemon/replay.go"),
		Reason:      sqlNullStr("body_required"),
		ActionTaken: sqlNullStr("message quality fallback"),
	}); err != nil {
		t.Fatalf("AppendDecision fallback: %v", err)
	}

	report := runStatusJSON(ctx, t, repo)
	if got := report.IntentStrategy.MessageQualityRewriteCountRecent; got != 1 {
		t.Fatalf("MessageQualityRewriteCountRecent=%d want 1", got)
	}
	if got := report.IntentStrategy.MessageQualityFallbackCountRecent; got != 1 {
		t.Fatalf("MessageQualityFallbackCountRecent=%d want 1", got)
	}
	if report.IntentStrategy.LastMessageQualityEventSeq != 11 ||
		report.IntentStrategy.LastMessageQualityPath != "internal/daemon/replay.go" ||
		report.IntentStrategy.LastMessageQualityAction != "message quality fallback" ||
		report.IntentStrategy.LastMessageQualityReason != "body_required" {
		t.Fatalf("last message quality summary=%+v", report.IntentStrategy)
	}
}

func TestStatus_LastPlannerWindowSummary(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
		BranchRef: sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{
			Int64: 1, Valid: true,
		},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := state.AppendIntentPlannerWindow(ctx, d, state.IntentPlannerWindow{
		PlannedTS:           100,
		Provider:            sqlNullStr("openai-compat"),
		Model:               sqlNullStr("gpt-test"),
		BranchRef:           "refs/heads/main",
		BranchGeneration:    1,
		Source:              sqlNullStr("deterministic"),
		CommitFormat:        sqlNullStr("imperative"),
		ValidationFailure:   sqlNullStr(`planner validation failed {"api_key":"legacy-window-secret"}`),
		OfferedSeqs:         []int64{4, 5},
		VisibleOriginalSeqs: []int64{4, 5, 6},
		HiddenSeqs:          []int64{6},
		SelectedGroups: []state.IntentPlannerWindowGroup{{
			SelectedSeqs:   []int64{4},
			OriginalSeqs:   []int64{4, 6},
			Subject:        "Update parser",
			GroupingReason: "related parser edits",
		}},
		DeferredSeqs: []int64{5},
		DeferredReasons: []state.IntentPlannerWindowDeferredReason{{
			Seq:    5,
			Reason: "separate docs change",
		}},
	}); err != nil {
		t.Fatalf("AppendIntentPlannerWindow: %v", err)
	}
	run, err := state.EnsureIntentPlanRun(ctx, d, state.IntentPlanRun{
		Fingerprint: "sha256:preflight", BranchRef: "refs/heads/main",
		BranchGeneration: 1, Provider: sqlNullStr("openai-compat"),
		Model:        sqlNullStr("gpt-test"),
		AttemptLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	run.ProgressState = sqlNullStr("preflight_blocked")
	run.ResolutionMode = sqlNullStr("local_preflight")
	run.FindingCodes = []string{"open_candidate_cap_exceeded"}
	if err := state.UpdateIntentPlanRun(ctx, d, run); err != nil {
		t.Fatal(err)
	}
	if _, err := state.AppendIntentPlannerWindow(ctx, d, state.IntentPlannerWindow{
		PlannedTS:           200,
		BranchRef:           "refs/heads/other",
		BranchGeneration:    2,
		OfferedSeqs:         []int64{99},
		VisibleOriginalSeqs: []int64{99},
	}); err != nil {
		t.Fatal(err)
	}
	otherRun, err := state.EnsureIntentPlanRun(ctx, d, state.IntentPlanRun{
		Fingerprint: "sha256:other", BranchRef: "refs/heads/other",
		BranchGeneration: 2, AttemptLimit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	otherRun.ProgressState = sqlNullStr("waiting_message_rewrite")
	otherRun.ResolutionMode = sqlNullStr("waiting_message_rewrite")
	if err := state.UpdateIntentPlanRun(ctx, d, otherRun); err != nil {
		t.Fatal(err)
	}

	report := runStatusJSON(ctx, t, repo)
	win := report.IntentStrategy.LastPlannerWindow
	if win == nil || win.Provider != "openai-compat" || win.Model != "gpt-test" ||
		win.BranchRef != "refs/heads/main" ||
		report.IntentStrategy.ResolutionMode != "local_preflight" {
		t.Fatalf("last planner window = %+v", win)
	}
	if len(win.OfferedSeqs) != 2 || win.OfferedSeqs[0] != 4 ||
		len(win.HiddenSeqs) != 1 || win.HiddenSeqs[0] != 6 ||
		strings.Contains(win.ValidationFailure, "legacy-window-secret") ||
		!strings.Contains(win.ValidationFailure, "[REDACTED]") {
		t.Fatalf("last planner window seq/failure fields = %+v", win)
	}
	if win.PreflightState != "preflight_blocked" ||
		win.ProviderCallSkipped != "invalid_local_baseline" ||
		!reflect.DeepEqual(win.FindingCodes,
			[]string{"open_candidate_cap_exceeded"}) {
		t.Fatalf("last planner preflight fields = %+v", win)
	}

	var human bytes.Buffer
	if err := runStatus(ctx, &human, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	for _, want := range []string{"Last planner window", "offered=4,5", "Hidden/coalesced seqs: 6", "Validation fallback", "Plan preflight", "provider_call_skipped=invalid_local_baseline"} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("status human missing %q:\n%s", want, human.String())
		}
	}
}

// TestStatus_PlannerErrorRateRecent_HalfWindow_FixedDenominator asserts
// that with 50 decisions of which all 50 are planner errors, the rate is
// 50/100 = 0.5 (NOT 50/50 = 1.0). Documents the fixed-denominator policy
// chosen for sub-window ledgers. The warn flag MUST stay false at this
// row count: see TestStatus_PlannerErrorRateWarnRequiresFullWindow for
// the gating rationale.
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
	if report.IntentStrategy.PlannerErrorRateRecentWarn {
		t.Fatalf("PlannerErrorRateRecentWarn must stay false until ledger reaches %d rows; got true with 50",
			IntentRecentDecisionWindow)
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
	if got, want := report.IntentStrategy.SettleWindowSeconds, int64(ai.DefaultIntentSettleWindow/time.Second); got != want {
		t.Fatalf("SettleWindowSeconds=%d want %d", got, want)
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
	if got, want := report.IntentStrategy.SettleWindowSeconds, int64(ai.DefaultIntentSettleWindow/time.Second); got != want {
		t.Fatalf("diagnose SettleWindowSeconds=%d want %d", got, want)
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

// TestStatus_PathQuiescenceGatedCountAdjustsVisiblePending stamps a
// path_quiescence.gated_count snapshot directly into daemon_meta and
// asserts the status JSON adjusts VisiblePendingEvents downward while
// leaving OldestPendingAgeSeconds anchored to the persistence timestamp
// of the oldest pending row. The semantics mirror what the daemon writes
// on every replay pass — see persistPathQuiescenceSnapshot in
// internal/daemon/replay.go.
func TestStatus_PathQuiescenceGatedCountAdjustsVisiblePending(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")

	// Seed two pending capture events so VisiblePendingEvents=2 before the
	// gated_count adjustment.
	now := float64(1_000_000)
	for i := 0; i < 2; i++ {
		ev := state.CaptureEvent{
			BranchRef:        "refs/heads/main",
			BranchGeneration: 1,
			BaseHead:         "deadbeef",
			Operation:        "modify",
			Path:             fmt.Sprintf("file-%d.go", i),
			Fidelity:         "full",
			CapturedTS:       now + float64(i),
		}
		if _, err := state.AppendCaptureEvent(ctx, d, ev, []state.CaptureOp{{
			Op: "modify", Path: ev.Path, Fidelity: "full",
		}}); err != nil {
			t.Fatalf("AppendCaptureEvent: %v", err)
		}
	}

	// Stamp a gated_count snapshot — daemon would normally do this at the
	// end of every replay pass under the gate. The freshness gate in
	// pathQuiescenceSnapshotFresh requires both a live daemon (we use
	// our own pid so identity.Alive returns true) and a recent
	// path_quiescence.updated_at timestamp.
	if err := state.MetaSet(ctx, d, "path_quiescence.gated_count", "1"); err != nil {
		t.Fatalf("MetaSet gated_count: %v", err)
	}
	if err := state.MetaSet(ctx, d, "path_quiescence.updated_at", time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("MetaSet updated_at: %v", err)
	}
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running",
		HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
	}); err != nil {
		t.Fatalf("SaveDaemonState: %v", err)
	}

	report := runStatusJSON(ctx, t, repo)
	if report.IntentStrategy.PathQuiescenceGatedEvents != 1 {
		t.Fatalf("PathQuiescenceGatedEvents=%d want 1; report=%+v", report.IntentStrategy.PathQuiescenceGatedEvents, report.IntentStrategy)
	}
	// 2 raw pending - 1 gated = 1 visible to the planner.
	if report.IntentStrategy.VisiblePendingEvents != 1 {
		t.Fatalf("VisiblePendingEvents=%d want 1 after gated adjustment; report=%+v",
			report.IntentStrategy.VisiblePendingEvents, report.IntentStrategy)
	}
	// OldestPendingPath is still derived from the oldest pending row — the
	// gate does not change persistence semantics.
	if report.IntentStrategy.OldestPendingPath != "file-0.go" {
		t.Fatalf("OldestPendingPath=%q want file-0.go (gate must not change persistence-derived fields)",
			report.IntentStrategy.OldestPendingPath)
	}
}

// TestStatus_PathQuiescenceStaleGatedCountIgnored covers the freshness
// gate added with P2 #17: when the gated_count value is present but the
// path_quiescence.updated_at snapshot is older than
// pathQuiescenceStaleness (or the daemon is not alive), status must
// still surface PathQuiescenceGatedEvents (the raw value is useful for
// forensic inspection) but MUST NOT subtract it from
// VisiblePendingEvents. Otherwise a dead daemon's frozen snapshot would
// chronically under-count pending work.
func TestStatus_PathQuiescenceStaleGatedCountIgnored(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")

	now := float64(1_000_000)
	for i := 0; i < 2; i++ {
		ev := state.CaptureEvent{
			BranchRef:        "refs/heads/main",
			BranchGeneration: 1,
			BaseHead:         "deadbeef",
			Operation:        "modify",
			Path:             fmt.Sprintf("file-%d.go", i),
			Fidelity:         "full",
			CapturedTS:       now + float64(i),
		}
		if _, err := state.AppendCaptureEvent(ctx, d, ev, []state.CaptureOp{{
			Op: "modify", Path: ev.Path, Fidelity: "full",
		}}); err != nil {
			t.Fatalf("AppendCaptureEvent: %v", err)
		}
	}

	if err := state.MetaSet(ctx, d, "path_quiescence.gated_count", "1"); err != nil {
		t.Fatalf("MetaSet gated_count: %v", err)
	}
	// updated_at is present but ancient — well past pathQuiescenceStaleness.
	if err := state.MetaSet(ctx, d, "path_quiescence.updated_at",
		time.Now().Add(-1*time.Hour).UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("MetaSet stale updated_at: %v", err)
	}
	// Daemon state stamped with our pid so identity.Alive returns true;
	// the staleness gate must still trip on the timestamp alone.
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running",
		HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
	}); err != nil {
		t.Fatalf("SaveDaemonState: %v", err)
	}

	report := runStatusJSON(ctx, t, repo)
	if report.IntentStrategy.PathQuiescenceGatedEvents != 1 {
		t.Fatalf("PathQuiescenceGatedEvents=%d want 1 (raw value still surfaced for forensics); report=%+v",
			report.IntentStrategy.PathQuiescenceGatedEvents, report.IntentStrategy)
	}
	if report.IntentStrategy.VisiblePendingEvents != 2 {
		t.Fatalf("VisiblePendingEvents=%d want 2 (stale gated_count must not be subtracted); report=%+v",
			report.IntentStrategy.VisiblePendingEvents, report.IntentStrategy)
	}
}

// TestStatus_PlannerErrorRateWarnRequiresFullWindow asserts the warn
// flag stays false while decision_records holds fewer than
// IntentRecentDecisionWindow rows even when the rate exceeds the
// threshold. With a fixed denominator early errors are easily
// indistinguishable from sustained noise (5 errors in 5 decisions =
// 0.05 = threshold), so we wait for a representative sample.
func TestStatus_PlannerErrorRateWarnRequiresFullWindow(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")

	// 50 rows, all planner errors. Rate = 50/100 = 0.5, well above
	// IntentPlannerErrorRateWarnThreshold (0.05). But with only 50 rows
	// in the ledger the warn flag MUST stay false.
	for i := 0; i < 50; i++ {
		if _, err := state.AppendDecision(ctx, d, state.DecisionRecord{
			DecisionTS:  float64(8_000_000 + i),
			Kind:        state.DecisionKindIntentPlannerError,
			Path:        sqlNullStr(fmt.Sprintf("err-%d.go", i)),
			ActionTaken: sqlNullStr("intent_planner_error"),
		}); err != nil {
			t.Fatalf("AppendDecision: %v", err)
		}
	}

	report := runStatusJSON(ctx, t, repo)
	if got := report.IntentStrategy.PlannerErrorRateRecent; got != 0.5 {
		t.Fatalf("PlannerErrorRateRecent=%v want 0.5", got)
	}
	if report.IntentStrategy.PlannerErrorRateRecentWarn {
		t.Fatalf("PlannerErrorRateRecentWarn must stay false until ledger reaches %d rows; got true with 50",
			IntentRecentDecisionWindow)
	}

	// Add 50 more decisions to cross the window threshold. After the
	// 100-row mark the warn flag MUST flip on (rate is still >threshold
	// in the most recent window).
	for i := 0; i < 50; i++ {
		if _, err := state.AppendDecision(ctx, d, state.DecisionRecord{
			DecisionTS:  float64(9_000_000 + i),
			Kind:        state.DecisionKindIntentPlannerError,
			Path:        sqlNullStr(fmt.Sprintf("err-late-%d.go", i)),
			ActionTaken: sqlNullStr("intent_planner_error"),
		}); err != nil {
			t.Fatalf("AppendDecision late: %v", err)
		}
	}
	report = runStatusJSON(ctx, t, repo)
	if !report.IntentStrategy.PlannerErrorRateRecentWarn {
		t.Fatalf("PlannerErrorRateRecentWarn must flip true after ledger reaches %d rows; report=%+v",
			IntentRecentDecisionWindow, report.IntentStrategy)
	}
}
