package state

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func testConfigRevision(t *testing.T, d *DB, model string, generation int64) ConfigRevision {
	t.Helper()
	rev, err := InsertConfigRevision(context.Background(), d, ConfigRevisionInput{
		Snapshot:         []byte(fmt.Sprintf(`{"model":%q,"intent":{"window":10}}`, model)),
		Profile:          "default",
		Scope:            "repo",
		SourceGeneration: generation,
	})
	if err != nil {
		t.Fatalf("InsertConfigRevision: %v", err)
	}
	return rev
}

func TestConfigRevisionCanonicalImmutableAndSecretSafe(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	rev, err := InsertConfigRevision(ctx, d, ConfigRevisionInput{
		Snapshot:         []byte(" {\n \"z\": 2, \"a\": {\"enabled\": true} } "),
		Profile:          "work",
		Scope:            "repo",
		SourceGeneration: 7,
		Reason:           "operator\x1b[31m request",
	})
	if err != nil {
		t.Fatalf("InsertConfigRevision: %v", err)
	}
	if rev.SnapshotJSON != `{"a":{"enabled":true},"z":2}` {
		t.Fatalf("canonical snapshot = %q", rev.SnapshotJSON)
	}
	if len(rev.SnapshotHash) != 64 || rev.Profile != "work" || rev.SourceGeneration != 7 {
		t.Fatalf("revision metadata = %+v", rev)
	}
	if strings.Contains(rev.Reason.String, "\x1b") {
		t.Fatalf("reason retained terminal escape: %q", rev.Reason.String)
	}
	if _, err := d.SQL().ExecContext(ctx, `UPDATE config_revisions SET profile='other' WHERE id=?`, rev.ID); err == nil {
		t.Fatal("immutable revision UPDATE succeeded")
	}
	if _, err := d.SQL().ExecContext(ctx, `DELETE FROM config_revisions WHERE id=?`, rev.ID); err == nil {
		t.Fatal("immutable revision DELETE succeeded")
	}
	secretKeys := []string{"api_key", "credentials", "nested-password", "accessToken"}
	for _, key := range secretKeys {
		_, err := InsertConfigRevision(ctx, d, ConfigRevisionInput{
			Snapshot: []byte(fmt.Sprintf(`{"nested":{%q:"do-not-store"}}`, key)),
			Profile:  "default", Scope: "repo",
		})
		if err == nil || !strings.Contains(err.Error(), "forbidden secret key") {
			t.Fatalf("secret key %q error = %v", key, err)
		}
	}
}

func TestConfigRevisionRejectsTrailingJSON(t *testing.T) {
	d, _ := openTestDB(t)
	_, err := InsertConfigRevision(context.Background(), d, ConfigRevisionInput{
		Snapshot: []byte(`{} {}`), Profile: "default", Scope: "repo",
	})
	if err == nil {
		t.Fatal("trailing JSON value accepted")
	}
}

func TestActivationCASRaceAndTransitions(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	a := testConfigRevision(t, d, "a", 1)
	b := testConfigRevision(t, d, "b", 2)

	type result struct {
		req ConfigActivationRequest
		ok  bool
		err error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, id := range []int64{a.ID, b.ID} {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			<-start
			req, ok, err := RequestConfigActivation(ctx, d, id, sql.NullInt64{})
			results <- result{req: req, ok: ok, err: err}
		}(id)
	}
	close(start)
	wg.Wait()
	close(results)
	var winner ConfigActivationRequest
	wins := 0
	for got := range results {
		if got.err != nil {
			t.Fatalf("RequestConfigActivation: %v", got.err)
		}
		if got.ok {
			winner = got.req
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("CAS winners = %d, want 1", wins)
	}
	if ok, err := AcknowledgeConfigActivation(ctx, d, winner.ID, winner.RevisionID); err != nil || !ok {
		t.Fatalf("AcknowledgeConfigActivation = %v, %v", ok, err)
	}
	if ok, err := ApplyConfigActivation(ctx, d, winner.ID, winner.RevisionID); err != nil || !ok {
		t.Fatalf("ApplyConfigActivation = %v, %v", ok, err)
	}
	state, err := RuntimeConfigActivationState(ctx, d)
	if err != nil || state.AppliedRevisionID.Int64 != winner.RevisionID ||
		state.LastKnownGoodRevisionID.Int64 != winner.RevisionID {
		t.Fatalf("applied state = %+v, err=%v", state, err)
	}

	loserID := a.ID
	if loserID == winner.RevisionID {
		loserID = b.ID
	}
	req, ok, err := RequestConfigActivation(ctx, d, loserID,
		sql.NullInt64{Int64: winner.RevisionID, Valid: true})
	if err != nil || !ok {
		t.Fatalf("second RequestConfigActivation = %+v, %v, %v", req, ok, err)
	}
	if ok, err := RejectConfigActivation(ctx, d, req.ID, loserID, "bad\x1b[2J response"); err != nil || !ok {
		t.Fatalf("RejectConfigActivation = %v, %v", ok, err)
	}
	state, _ = RuntimeConfigActivationState(ctx, d)
	if state.AppliedRevisionID.Int64 != winner.RevisionID || strings.Contains(state.LastError.String, "\x1b") {
		t.Fatalf("rejection changed known-good or retained escape: %+v", state)
	}
}

func TestActivationCancelAndRevertThroughNewRevision(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	a := testConfigRevision(t, d, "a", 1)
	req, ok, err := RequestConfigActivation(ctx, d, a.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatalf("request = %+v %v %v", req, ok, err)
	}
	if ok, err := CancelConfigActivation(ctx, d, req.ID, a.ID, "operator cancelled"); err != nil || !ok {
		t.Fatalf("cancel = %v %v", ok, err)
	}
	state, _ := RuntimeConfigActivationState(ctx, d)
	if state.DesiredRevisionID.Valid || state.DesiredRequestID.Valid {
		t.Fatalf("cancelled desired pointers = %+v", state)
	}

	req, ok, _ = RequestConfigActivation(ctx, d, a.ID, sql.NullInt64{})
	_, _ = AcknowledgeConfigActivation(ctx, d, req.ID, a.ID)
	_, _ = ApplyConfigActivation(ctx, d, req.ID, a.ID)
	revert, revertReq, ok, err := RevertConfigActivation(ctx, d, a.ID,
		sql.NullInt64{Int64: a.ID, Valid: true}, 2)
	if err != nil || !ok || revert.ID == a.ID || revertReq.RevisionID != revert.ID ||
		revert.SnapshotHash != a.SnapshotHash {
		t.Fatalf("revert = %+v req=%+v ok=%v err=%v", revert, revertReq, ok, err)
	}
}

func TestActivationCrashStateRecovery(t *testing.T) {
	gitDir := filepath.Join(t.TempDir(), ".git")
	dbPath := DBPathFromGitDir(gitDir)
	ctx := context.Background()
	d, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rev := testConfigRevision(t, d, "candidate", 1)
	req, ok, err := RequestConfigActivation(ctx, d, rev.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatalf("request: %v %v", ok, err)
	}
	if ok, err := AcknowledgeConfigActivation(ctx, d, req.ID, rev.ID); err != nil || !ok {
		t.Fatalf("ack: %v %v", ok, err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	state, err := RuntimeConfigActivationState(ctx, d)
	if err != nil || state.DesiredRevisionID.Int64 != rev.ID || state.AppliedRevisionID.Valid {
		t.Fatalf("recovered state = %+v err=%v", state, err)
	}
	gotReq, err := ActivationRequestByID(ctx, d, req.ID)
	if err != nil || gotReq.Status != ActivationAcknowledged {
		t.Fatalf("recovered request = %+v err=%v", gotReq, err)
	}
}

func TestRuntimeConfigV13MigrationPreservesDataAndDailyRollups(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	raw, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.Exec(`
CREATE TABLE intent_planner_windows(
 id INTEGER PRIMARY KEY AUTOINCREMENT, planned_ts REAL NOT NULL, provider TEXT,
 model TEXT, branch_ref TEXT NOT NULL, branch_generation INTEGER NOT NULL,
 source TEXT, commit_format TEXT, forced INTEGER NOT NULL DEFAULT 0,
 forced_reason TEXT, validation_failure TEXT, offered_seqs TEXT NOT NULL,
 visible_original_seqs TEXT NOT NULL, hidden_seqs TEXT NOT NULL,
 selected_groups TEXT NOT NULL, deferred_seqs TEXT NOT NULL,
 deferred_reasons TEXT NOT NULL);
CREATE TABLE decision_records(
 id INTEGER PRIMARY KEY AUTOINCREMENT, decision_ts REAL NOT NULL, kind TEXT NOT NULL,
 path TEXT, reason TEXT, event_seq INTEGER, head_sha TEXT, commit_oid TEXT,
 branch_ref TEXT, branch_generation INTEGER, action_taken TEXT, user_message TEXT);
CREATE TABLE daily_rollups(
 day TEXT NOT NULL, repo_root TEXT NOT NULL, events_total INTEGER NOT NULL DEFAULT 0,
 commits_total INTEGER NOT NULL DEFAULT 0, files_changed INTEGER NOT NULL DEFAULT 0,
 bytes_changed INTEGER NOT NULL DEFAULT 0, errors_total INTEGER NOT NULL DEFAULT 0,
 sessions_seen INTEGER NOT NULL DEFAULT 0, daemon_uptime_seconds INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(day, repo_root));
INSERT INTO decision_records(decision_ts,kind,reason) VALUES(1,'captured','keep');
INSERT INTO daily_rollups(day,repo_root,events_total) VALUES('2026-07-13','/repo',9);
PRAGMA user_version=13;`)
	if err != nil {
		t.Fatalf("seed v13: %v", err)
	}
	_ = raw.Close()
	d, err := Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("Open migrated v13: %v", err)
	}
	defer d.Close()
	if got, _ := d.UserVersion(context.Background()); got != SchemaVersion {
		t.Fatalf("user_version=%d want=%d", got, SchemaVersion)
	}
	var reason string
	if err := d.SQL().QueryRow(`SELECT reason FROM decision_records WHERE id=1`).Scan(&reason); err != nil || reason != "keep" {
		t.Fatalf("decision preserved=%q err=%v", reason, err)
	}
	var events int
	if err := d.SQL().QueryRow(`SELECT events_total FROM daily_rollups WHERE day='2026-07-13'`).Scan(&events); err != nil || events != 9 {
		t.Fatalf("daily rollup preserved=%d err=%v", events, err)
	}
	for _, table := range []string{"config_revisions", "runtime_config_state", "config_activation_requests", "config_experiments"} {
		var found string
		if err := d.SQL().QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&found); err != nil {
			t.Fatalf("v14 table %s: %v", table, err)
		}
	}
}

func TestExperimentPlannerWindowExactConsumptionAndMetadata(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	baseline := testConfigRevision(t, d, "baseline", 1)
	candidate := testConfigRevision(t, d, "candidate", 2)
	exp, err := CreateConfigExperiment(ctx, d, ConfigExperimentInput{
		BaselineRevisionID: baseline.ID, CandidateRevisionID: candidate.ID,
		WindowBudget: 2, FailurePolicy: "continue",
	})
	if err != nil {
		t.Fatalf("CreateConfigExperiment: %v", err)
	}
	appendWindow := func(ts float64, retries int, fallback bool) int64 {
		t.Helper()
		id, err := AppendIntentPlannerWindow(ctx, d, IntentPlannerWindow{
			PlannedTS: ts, BranchRef: "refs/heads/main", BranchGeneration: 1,
			ConfigRevisionID: sql.NullInt64{Int64: candidate.ID, Valid: true},
			ConfigProfile:    sql.NullString{String: "candidate", Valid: true},
			DurationMS:       sql.NullInt64{Int64: 125, Valid: true},
			RetryCount:       retries, FallbackUsed: fallback,
			Outcome:      sql.NullString{String: "selected", Valid: true},
			ExperimentID: sql.NullInt64{Int64: exp.ID, Valid: true},
		})
		if err != nil {
			t.Fatalf("AppendIntentPlannerWindow: %v", err)
		}
		return id
	}
	appendWindow(100, 1, false)
	got, _ := ConfigExperimentByID(ctx, d, exp.ID)
	if got.CompletedWindows != 1 || got.Status != ExperimentActive {
		t.Fatalf("after first window = %+v", got)
	}
	lastID := appendWindow(101, 2, true)
	got, _ = ConfigExperimentByID(ctx, d, exp.ID)
	if got.CompletedWindows != 2 || got.Status != ExperimentCompleted || !got.CompletedTS.Valid {
		t.Fatalf("after exact budget = %+v", got)
	}
	recent, err := RecentIntentPlannerWindows(ctx, d, 1)
	if err != nil || len(recent) != 1 {
		t.Fatalf("RecentIntentPlannerWindows = %+v %v", recent, err)
	}
	win := recent[0]
	if win.ID != lastID || win.ConfigRevisionID.Int64 != candidate.ID ||
		win.ConfigProfile.String != "candidate" || win.DurationMS.Int64 != 125 ||
		win.RetryCount != 2 || !win.FallbackUsed || win.Outcome.String != "selected" ||
		win.ExperimentID.Int64 != exp.ID || !win.ExperimentConsumed {
		t.Fatalf("planner outcome metadata = %+v", win)
	}
	// A terminal experiment is not incremented again.
	appendWindow(102, 0, false)
	got, _ = ConfigExperimentByID(ctx, d, exp.ID)
	if got.CompletedWindows != 2 {
		t.Fatalf("terminal experiment over-consumed = %+v", got)
	}
}

func TestExperimentPlannerWindowMismatchRollsBack(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	baseline := testConfigRevision(t, d, "baseline", 1)
	candidate := testConfigRevision(t, d, "candidate", 2)
	other := testConfigRevision(t, d, "other", 3)
	exp, err := CreateConfigExperiment(ctx, d, ConfigExperimentInput{
		BaselineRevisionID: baseline.ID, CandidateRevisionID: candidate.ID,
		WindowBudget: 1, FailurePolicy: "continue",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = AppendIntentPlannerWindow(ctx, d, IntentPlannerWindow{
		PlannedTS: 100, BranchRef: "refs/heads/main", BranchGeneration: 1,
		ConfigRevisionID: sql.NullInt64{Int64: other.ID, Valid: true},
		ExperimentID:     sql.NullInt64{Int64: exp.ID, Valid: true},
		Outcome:          sql.NullString{String: "selected", Valid: true},
	})
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mismatch error = %v", err)
	}
	var windows int
	if err := d.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM intent_planner_windows`).Scan(&windows); err != nil || windows != 0 {
		t.Fatalf("rolled-back windows=%d err=%v", windows, err)
	}
	got, _ := ConfigExperimentByID(ctx, d, exp.ID)
	if got.CompletedWindows != 0 || got.Status != ExperimentActive {
		t.Fatalf("mismatch changed experiment = %+v", got)
	}
}

func TestExperimentExpiryAndFailurePolicy(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	baseline := testConfigRevision(t, d, "baseline", 1)
	candidate := testConfigRevision(t, d, "candidate", 2)
	exp, err := CreateConfigExperiment(ctx, d, ConfigExperimentInput{
		BaselineRevisionID: baseline.ID, CandidateRevisionID: candidate.ID,
		WindowBudget: 3, ExpiresTS: sql.NullFloat64{Float64: 50, Valid: true},
		FailurePolicy: "revert",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = AppendIntentPlannerWindow(ctx, d, IntentPlannerWindow{
		PlannedTS: 50, BranchRef: "refs/heads/main", BranchGeneration: 1,
		ConfigRevisionID: sql.NullInt64{Int64: candidate.ID, Valid: true},
		ExperimentID:     sql.NullInt64{Int64: exp.ID, Valid: true},
		Outcome:          sql.NullString{String: "failed", Valid: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := ConfigExperimentByID(ctx, d, exp.ID)
	if got.Status != ExperimentExpired || got.CompletedWindows != 0 {
		t.Fatalf("expired experiment = %+v", got)
	}
}

func TestDecisionConfigRevisionRoundTrip(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	rev := testConfigRevision(t, d, "model", 1)
	_, err := AppendDecision(ctx, d, DecisionRecord{
		Kind:             DecisionKindIntentForced,
		ConfigRevisionID: sql.NullInt64{Int64: rev.ID, Valid: true},
		ConfigProfile:    sql.NullString{String: "default", Valid: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := RecentDecisions(ctx, d, 1)
	if err != nil || len(rows) != 1 || rows[0].ConfigRevisionID.Int64 != rev.ID ||
		rows[0].ConfigProfile.String != "default" {
		t.Fatalf("revision-stamped decision = %+v err=%v", rows, err)
	}
}
