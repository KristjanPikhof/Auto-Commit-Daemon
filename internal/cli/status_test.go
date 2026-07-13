package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestStatus_RegisteredRepoWithClientsAndCommit(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db, "claude-code")

	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 12345, Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}

	// Two clients.
	now := nowFloat()
	if err := state.RegisterClient(ctx, d, state.Client{
		SessionID: "8c7d0000-aaaa-bbbb-cccc-000000000001", Harness: "claude-code",
		LastSeenTS: now,
	}); err != nil {
		t.Fatalf("register A: %v", err)
	}
	if err := state.RegisterClient(ctx, d, state.Client{
		SessionID: "9f3e0000-aaaa-bbbb-cccc-000000000002", Harness: "pi",
		LastSeenTS: now - 14,
	}); err != nil {
		t.Fatalf("register B: %v", err)
	}

	// One commit.
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "auth.py",
		Fidelity: "exact", CapturedTS: now - 47,
	}, nil)
	if err != nil {
		t.Fatalf("append event: %v", err)
	}
	if err := state.MarkEventPublished(ctx, d, seq, "published",
		sql.NullString{String: "a1b2c3deeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Valid: true},
		sql.NullString{}, sql.NullString{String: "Update auth.py", Valid: true},
		now-47); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Branch generation token in meta.
	if err := state.MetaSet(ctx, d, "branch.generation_token", "rev:deadbeef"); err != nil {
		t.Fatalf("meta set: %v", err)
	}

	var out bytes.Buffer
	if err := runStatus(ctx, &out, repo, false); err != nil {
		t.Fatalf("runStatus: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Repo: " + repo,
		"running",
		"pid 12345",
		"Clients (2):",
		"claude-code",
		"pi ",
		"a1b2c3d",
		"Update auth.py",
		"rev:deadbeef",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q in:\n%s", want, got)
		}
	}
}

func TestStatusRuntimeConfigHumanJSONAndRedaction(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Global[config.FieldModel] = json.RawMessage(`"saved-model"`)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	baseline := cliRuntimeRevision(t, d, "baseline", 1)
	candidate := cliRuntimeRevision(t, d, "candidate", 1)
	request, ok, err := state.RequestConfigActivation(ctx, d, baseline.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatalf("baseline request: %v %v", ok, err)
	}
	_, _ = state.AcknowledgeConfigActivation(ctx, d, request.ID, baseline.ID)
	_, _ = state.ApplyConfigActivation(ctx, d, request.ID, baseline.ID)
	pending, ok, err := state.RequestConfigActivation(ctx, d, candidate.ID, sql.NullInt64{Int64: baseline.ID, Valid: true})
	if err != nil || !ok {
		t.Fatalf("candidate request: %v %v", ok, err)
	}
	_, _ = state.RejectConfigActivation(ctx, d, pending.ID, candidate.ID, "rejected")
	unsafe := "https://user:password@provider.invalid api_key=sk-visible prompt=private repository_diff=secret provider_response=raw\x1b[31m"
	if _, err := d.SQL().Exec(`UPDATE runtime_config_state SET last_error=? WHERE id=1`, unsafe); err != nil {
		t.Fatal(err)
	}
	experiment, err := state.CreateConfigExperiment(ctx, d, state.ConfigExperimentInput{
		BaselineRevisionID: baseline.ID, CandidateRevisionID: candidate.ID,
		WindowBudget: 10, ExpiresTS: sql.NullFloat64{Float64: float64(time.Now().Add(time.Hour).Unix()), Valid: true},
		FailurePolicy: "continue",
	})
	if err != nil {
		t.Fatal(err)
	}
	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatal(err)
	}
	var report statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	runtime := report.RuntimeConfig
	if runtime.SavedGeneration != 1 || runtime.DesiredRevisionID != candidate.ID ||
		runtime.AppliedRevisionID != baseline.ID || runtime.LastKnownGoodRevisionID != baseline.ID ||
		runtime.Profile != "profile-a" || runtime.ApplyState != "rejected" ||
		runtime.ApplyBoundary != "next_work_boundary" || runtime.Experiment == nil ||
		runtime.Experiment.ID != experiment.ID || runtime.Experiment.WindowBudget != 10 {
		t.Fatalf("runtime JSON projection = %+v", runtime)
	}
	var human bytes.Buffer
	if err := runStatus(ctx, &human, repo, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Runtime settings: rejected", "desired=", "known_good=", "boundary=next_work_boundary", "saved_generation=1", "Experiment #"} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("human runtime output missing %q:\n%s", want, human.String())
		}
	}
	combined := jsonOut.String() + human.String()
	for _, forbidden := range []string{"user:password", "sk-visible", "private", "repository_diff=secret", "provider_response=raw", "\x1b"} {
		if strings.Contains(combined, forbidden) {
			t.Fatalf("runtime observability leaked %q:\n%s", forbidden, combined)
		}
	}
}

func TestStatusRuntimeConfigPreV14MissingTablesReadOnly(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if _, err := d.SQL().Exec(`
DROP TABLE config_experiments;
DROP TABLE config_activation_requests;
DROP TABLE runtime_config_state;
DROP TRIGGER config_revisions_no_update;
DROP TRIGGER config_revisions_no_delete;
DROP TABLE config_revisions;
PRAGMA user_version=13;
PRAGMA wal_checkpoint(TRUNCATE);`); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := fileSHA256(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runStatus(ctx, &out, repo, true); err != nil {
		t.Fatal(err)
	}
	var report statusReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.RuntimeConfig.ApplyState != "unset" || report.RuntimeConfig.DesiredRevisionID != 0 {
		t.Fatalf("old schema runtime projection = %+v", report.RuntimeConfig)
	}
	after, _ := fileSHA256(dbPath)
	if before != after {
		t.Fatalf("status mutated pre-v14 DB: %s -> %s", before, after)
	}
	conn, err := openStateDBReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var version, tables int
	_ = conn.QueryRow(`PRAGMA user_version`).Scan(&version)
	_ = conn.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'config_%'`).Scan(&tables)
	if version != 13 || tables != 0 {
		t.Fatalf("old schema changed: version=%d config_tables=%d", version, tables)
	}
}

func cliRuntimeRevision(t *testing.T, d *state.DB, model string, generation int64) state.ConfigRevision {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"ai.model": model, "confirmations": []string{}})
	revision, err := state.InsertConfigRevision(context.Background(), d, state.ConfigRevisionInput{
		Snapshot: body, Profile: "profile-a", Scope: "repository", SourceGeneration: generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func TestStatus_StaleHeartbeatOverlay(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db, "claude-code")

	stale := float64(time.Now().Add(-2 * time.Hour).Unix())
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 1, Mode: "running", HeartbeatTS: stale,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	var out bytes.Buffer
	if err := runStatus(ctx, &out, repo, false); err != nil {
		t.Fatalf("runStatus: %v", err)
	}
	if !strings.Contains(out.String(), "stale") {
		t.Fatalf("expected stale daemon line, got:\n%s", out.String())
	}
}

func TestStatus_UnregisteredRepoErrors(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()

	stranger := initCLIResolverRepo(t)
	var out bytes.Buffer
	err := runStatus(ctx, &out, stranger, false)
	if err == nil {
		t.Fatal("expected error for unregistered repo")
	}
	if !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("error should mention 'not registered': %v", err)
	}
}

// TestStatus_BlockedConflictCount verifies `acd status` reports a non-zero
// blocked_conflicts count when the replay loop has terminally settled an
// event in state.EventStateBlockedConflict, and renders a "Blocked
// conflicts:" line in human output. Keeps the CLI surface honest about
// stuck rows that will not retry on their own.
func TestStatus_BlockedConflictCount(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db, "claude-code")

	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 99, Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}

	// Append a blocker event and settle it directly via MarkEventBlocked.
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "ghost.txt",
		Fidelity: "rescan",
	}, []state.CaptureOp{{
		Op: "modify", Path: "ghost.txt", Fidelity: "rescan",
	}})
	if err != nil {
		t.Fatalf("append event: %v", err)
	}
	if err := state.MarkEventBlocked(ctx, d, seq, "before-state mismatch", nowFloat(),
		sql.NullString{String: "refs/heads/main", Valid: true},
		sql.NullInt64{Int64: 1, Valid: true},
		sql.NullString{String: "deadbeef", Valid: true},
	); err != nil {
		t.Fatalf("MarkEventBlocked: %v", err)
	}

	// Human output mentions the blocker.
	var humanOut bytes.Buffer
	if err := runStatus(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	if !strings.Contains(humanOut.String(), "Blocked conflicts: 1") {
		t.Fatalf("missing 'Blocked conflicts: 1' in:\n%s", humanOut.String())
	}

	// JSON shape exposes the field as an integer count.
	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if rep.BlockedConflicts != 1 {
		t.Fatalf("BlockedConflicts = %d, want 1", rep.BlockedConflicts)
	}
	// Pending must be 0 — blocked rows leave the FIFO.
	if rep.PendingEvents != 0 {
		t.Fatalf("PendingEvents = %d, want 0 (blocker is terminal)", rep.PendingEvents)
	}
}

func TestStatus_BlockedBarrierGuidance(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
		BranchRef:        sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "barrier.go",
		Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "barrier.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append blocked event: %v", err)
	}
	if err := state.MarkEventBlocked(ctx, d, seq, "before-state mismatch", nowFloat(),
		sql.NullString{String: "refs/heads/main", Valid: true},
		sql.NullInt64{Int64: 1, Valid: true},
		sql.NullString{String: "deadbeef", Valid: true},
	); err != nil {
		t.Fatalf("mark blocked: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "later.go",
		Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "later.go", Fidelity: "exact"}}); err != nil {
		t.Fatalf("append pending successor: %v", err)
	}

	var humanOut bytes.Buffer
	if err := runStatus(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	human := humanOut.String()
	for _, want := range []string{"Blocked conflicts: 1", "acd fix --dry-run", "Blocked barriers with pending replay: 1", "acd fix --force --dry-run"} {
		if !strings.Contains(human, want) {
			t.Fatalf("status human missing %q in:\n%s", want, human)
		}
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if rep.ActiveBarriers != 1 || rep.BlockedConflicts != 1 || rep.PendingEvents != 1 {
		t.Fatalf("status counts = blocked %d active %d pending %d, want 1/1/1", rep.BlockedConflicts, rep.ActiveBarriers, rep.PendingEvents)
	}
}

func TestStatus_FailedBarrierGuidance(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "bad.go",
		Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "bad.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append failed event: %v", err)
	}
	if err := state.MarkEventPublished(ctx, d, seq, state.EventStateFailed,
		sql.NullString{}, sql.NullString{String: "commit-tree failed", Valid: true},
		sql.NullString{}, nowFloat()); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "later.go",
		Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "later.go", Fidelity: "exact"}}); err != nil {
		t.Fatalf("append pending successor: %v", err)
	}

	var humanOut bytes.Buffer
	if err := runStatus(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	human := humanOut.String()
	for _, want := range []string{"Failed terminal events: 1", "Failed barriers blocking pending replay: 1", "acd fix --dry-run"} {
		if !strings.Contains(human, want) {
			t.Fatalf("status human missing %q in:\n%s", want, human)
		}
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal status: %v\n%s", err, jsonOut.String())
	}
	if rep.FailedEvents != 1 || rep.FailedBlockingPending != 1 {
		t.Fatalf("failed fields = events %d blocking %d, want 1/1", rep.FailedEvents, rep.FailedBlockingPending)
	}
}

func TestStatus_BodyRendersPauseSection(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	expiresAt := time.Now().UTC().Add(time.Minute).Format(time.RFC3339)
	writePauseMarkerForStateDB(t, dbPath, pausepkg.Marker{
		Reason:    "deploy window",
		SetAt:     time.Now().UTC().Format(time.RFC3339),
		SetBy:     "test",
		ExpiresAt: &expiresAt,
	})

	var humanOut bytes.Buffer
	if err := runStatus(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	human := humanOut.String()
	for _, want := range []string{"Pause:", "Source: manual", "Reason: deploy window", "Expires at:"} {
		if !strings.Contains(human, want) {
			t.Fatalf("status output missing %q in:\n%s", want, human)
		}
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if !rep.Paused || rep.Pause == nil || rep.Pause.Source != "manual" || rep.Pause.Reason != "deploy window" {
		t.Fatalf("unexpected pause JSON: %+v", rep.Pause)
	}
}

func TestStatus_DecisionSummary(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	firstID, err := state.AppendDecision(ctx, d, state.DecisionRecord{
		DecisionTS:  10,
		Kind:        state.DecisionKindProtected,
		Path:        sqlNullStr("secrets.env"),
		Reason:      sqlNullStr("sensitive"),
		ActionTaken: sqlNullStr("no_delete_generated"),
	})
	if err != nil {
		t.Fatalf("AppendDecision protected: %v", err)
	}
	secondID, err := state.AppendDecision(ctx, d, state.DecisionRecord{
		DecisionTS:  11,
		Kind:        state.DecisionKindHandledExternal,
		Path:        sqlNullStr("src/app.go"),
		Reason:      sqlNullStr("already_published_by_external_committer"),
		ActionTaken: sqlNullStr("marked_published"),
	})
	if err != nil {
		t.Fatalf("AppendDecision handled: %v", err)
	}

	var humanOut bytes.Buffer
	if err := runStatus(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	human := humanOut.String()
	for _, want := range []string{
		"Decisions: protected=1 handled_external=1",
		"Recent decisions:",
		"#" + strconv.FormatInt(secondID, 10) + " handled_external src/app.go (marked_published)",
		"#" + strconv.FormatInt(firstID, 10) + " protected secrets.env (no_delete_generated)",
		"acd explain --path FILE",
		"acd events --watch",
	} {
		if !strings.Contains(human, want) {
			t.Fatalf("status output missing %q in:\n%s", want, human)
		}
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if rep.DecisionCursor != secondID {
		t.Fatalf("DecisionCursor = %d, want %d", rep.DecisionCursor, secondID)
	}
	if rep.DecisionCounts[state.DecisionKindProtected] != 1 || rep.DecisionCounts[state.DecisionKindHandledExternal] != 1 {
		t.Fatalf("DecisionCounts = %#v, want protected=1 handled_external=1", rep.DecisionCounts)
	}
	if len(rep.RecentDecisions) != 2 || rep.RecentDecisions[0].ID != secondID || rep.RecentDecisions[1].ID != firstID {
		t.Fatalf("RecentDecisions = %#v, want newest first", rep.RecentDecisions)
	}
}

func TestStatus_IntentStrategyUsesDaemonMetadata(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	for k, v := range map[string]string{
		"commit.strategy":       "intent",
		"commit.format":         "conventional",
		"intent.window":         "7",
		"intent.settle_window":  "15s",
		"intent.recent_commits": "3",
		"intent.defer_limit":    "1",
	} {
		if err := state.MetaSet(ctx, d, k, v); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}

	t.Setenv("ACD_COMMIT_STRATEGY", "event")
	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if !rep.IntentStrategy.Active || rep.IntentStrategy.Strategy != "intent" ||
		rep.IntentStrategy.CommitFormat != "conventional" ||
		rep.IntentStrategy.Window != 7 || rep.IntentStrategy.RecentCommits != 3 ||
		rep.IntentStrategy.SettleWindowSeconds != 15 ||
		rep.IntentStrategy.DeferLimit != 1 {
		t.Fatalf("intent strategy = %+v, want daemon metadata", rep.IntentStrategy)
	}
}

func TestStatus_IntentStrategyReportsPlannerHealth(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.MetaSet(ctx, d, "commit.strategy", "intent"); err != nil {
		t.Fatalf("set commit strategy: %v", err)
	}
	nextProbe := time.Date(2026, 7, 13, 3, 21, 0, 0, time.UTC)
	health := daemon.IntentPlannerHealthSnapshot{
		State:               daemon.IntentPlannerCircuitOpen,
		ProviderFingerprint: testPlannerHealthFingerprint(),
		ConsecutiveFailures: 3,
		BackoffLevel:        1,
		NextProbeTS:         float64(nextProbe.Unix()),
		LastFailureClass:    daemon.IntentPlannerFailureValidation,
		LastError:           "planner returned an invalid group",
		BypassCount:         7,
	}
	if err := state.MetaSetJSON(ctx, d, daemon.MetaKeyIntentPlannerHealth, struct {
		Version int `json:"version"`
		daemon.IntentPlannerHealthSnapshot
	}{Version: 1, IntentPlannerHealthSnapshot: health}); err != nil {
		t.Fatalf("set planner health: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if rep.IntentStrategy.PlannerHealth == nil ||
		rep.IntentStrategy.PlannerHealth.State != daemon.IntentPlannerCircuitOpen ||
		rep.IntentStrategy.PlannerHealth.ConsecutiveFailures != 3 ||
		rep.IntentStrategy.PlannerHealth.BypassCount != 7 ||
		rep.IntentStrategy.PlannerHealth.LastFailureClass != daemon.IntentPlannerFailureValidation {
		t.Fatalf("planner health=%+v", rep.IntentStrategy.PlannerHealth)
	}
	if !bytes.Contains(jsonOut.Bytes(), []byte(`"planner_health"`)) ||
		!bytes.Contains(jsonOut.Bytes(), []byte(`"next_probe_ts"`)) {
		t.Fatalf("status JSON missing planner health fields: %s", jsonOut.String())
	}

	var human bytes.Buffer
	if err := runStatus(ctx, &human, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	for _, want := range []string{
		"Intent planner health: open failures=3 bypasses=7",
		"next_probe=2026-07-13T03:21:00Z",
		"last_failure_class=validation",
		"Last circuit failure: planner returned an invalid group",
	} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("status human missing %q:\n%s", want, human.String())
		}
	}
}

func TestStatus_IntentPlannerHealthWarningIsReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name    string
		raw     string
		warning string
	}{
		{name: "empty", raw: "", warning: plannerHealthInvalidWarning},
		{name: "invalid", raw: `{"version":1,"last_error":"sk-secret`, warning: plannerHealthInvalidWarning},
		{name: "unsupported", raw: `{"version":99,"state":"open","last_error":"sk-secret"}`, warning: plannerHealthVersionWarning},
	} {
		t.Run(tc.name, func(t *testing.T) {
			roots := withIsolatedHome(t)
			ctx := context.Background()
			repo, dbPath, d := makeRepoStateDB(t)
			registerRepo(t, roots, repo, dbPath, "codex")
			if err := state.MetaSet(ctx, d, daemon.MetaKeyIntentPlannerHealth, tc.raw); err != nil {
				t.Fatalf("set planner health: %v", err)
			}
			if err := d.Close(); err != nil {
				t.Fatalf("close db: %v", err)
			}
			before, err := fileSHA256(dbPath)
			if err != nil {
				t.Fatalf("checksum before: %v", err)
			}

			var jsonOut bytes.Buffer
			if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
				t.Fatalf("runStatus json: %v", err)
			}
			var rep statusReport
			if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
				t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
			}
			if rep.IntentStrategy.PlannerHealth != nil || rep.IntentStrategy.PlannerHealthWarning != tc.warning {
				t.Fatalf("intent strategy=%+v", rep.IntentStrategy)
			}
			if strings.Contains(jsonOut.String(), "sk-secret") {
				t.Fatalf("status leaked malformed metadata: %s", jsonOut.String())
			}

			var human bytes.Buffer
			if err := runStatus(ctx, &human, repo, false); err != nil {
				t.Fatalf("runStatus human: %v", err)
			}
			if !strings.Contains(human.String(), "Intent planner health warning: "+tc.warning) {
				t.Fatalf("status human missing safe warning:\n%s", human.String())
			}
			after, err := fileSHA256(dbPath)
			if err != nil {
				t.Fatalf("checksum after: %v", err)
			}
			if before != after {
				t.Fatalf("status mutated state.db: before=%q after=%q", before, after)
			}
		})
	}
}

func testPlannerHealthFingerprint() string {
	return "sha256:" + strings.Repeat("0", 64)
}

func TestStatus_IntentStrategyReportsBatchWaitState(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	for k, v := range map[string]string{
		"commit.strategy":        "intent",
		"intent.window":          "7",
		"intent.min_pending":     "3",
		"intent.settle_window":   "0s",
		"intent.max_pending_age": "2m",
		"intent.recent_commits":  "3",
		"intent.defer_limit":     "1",
	} {
		if err := state.MetaSet(ctx, d, k, v); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}
	seq := appendIntentPendingEvent(t, ctx, d, "wait-a.go", nowFloat()-30)
	appendIntentPendingEvent(t, ctx, d, "wait-b.go", nowFloat()-20)

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if !rep.IntentStrategy.BatchWaitActive ||
		rep.IntentStrategy.BatchWaitReason != "skipped_due_intent_batch_wait" ||
		rep.IntentStrategy.VisiblePendingEvents != 2 ||
		rep.IntentStrategy.MinPending != 3 ||
		rep.IntentStrategy.SettleWindowSeconds != 0 ||
		rep.IntentStrategy.MaxPendingAgeSeconds != 120 ||
		rep.IntentStrategy.OldestPendingEventSeq != seq ||
		rep.IntentStrategy.OldestPendingPath != "wait-a.go" {
		t.Fatalf("intent strategy = %+v, want active batch wait", rep.IntentStrategy)
	}
	if rep.IntentStrategy.OldestPendingAgeSeconds <= 0 || rep.IntentStrategy.AgeTriggerInSeconds <= 0 {
		t.Fatalf("intent ages = %+v, want positive oldest age and trigger countdown", rep.IntentStrategy)
	}

	var humanOut bytes.Buffer
	if err := runStatus(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	if !strings.Contains(humanOut.String(), "Intent batch wait: pending=2 min_pending=3") {
		t.Fatalf("status human missing batch wait line:\n%s", humanOut.String())
	}
}

func TestStatus_IntentStrategyReportsSettleWaitState(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	for k, v := range map[string]string{
		"commit.strategy":        "intent",
		"intent.window":          "7",
		"intent.min_pending":     "2",
		"intent.settle_window":   "1m",
		"intent.max_pending_age": "2m",
		"intent.recent_commits":  "3",
		"intent.defer_limit":     "1",
	} {
		if err := state.MetaSet(ctx, d, k, v); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}
	appendIntentPendingEvent(t, ctx, d, "settle-a.go", nowFloat()-10)
	newest := appendIntentPendingEvent(t, ctx, d, "settle-b.go", nowFloat()-5)

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if !rep.IntentStrategy.BatchWaitActive ||
		rep.IntentStrategy.BatchWaitReason != "skipped_due_intent_settle_window" ||
		rep.IntentStrategy.VisiblePendingEvents != 2 ||
		rep.IntentStrategy.MinPending != 2 ||
		rep.IntentStrategy.SettleWindowSeconds != 60 ||
		rep.IntentStrategy.NewestPendingEventSeq != newest {
		t.Fatalf("intent strategy = %+v, want active settle wait", rep.IntentStrategy)
	}
	if rep.IntentStrategy.NewestPendingAgeSeconds <= 0 || rep.IntentStrategy.SettleTriggerInSeconds <= 0 {
		t.Fatalf("intent settle ages = %+v, want positive newest age and trigger countdown", rep.IntentStrategy)
	}

	var humanOut bytes.Buffer
	if err := runStatus(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	if !strings.Contains(humanOut.String(), "Intent settle wait: pending=2") {
		t.Fatalf("status human missing settle wait line:\n%s", humanOut.String())
	}
}

func TestStatus_IntentStrategyUsesDurablePlannerErrorLedger(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	if err := state.MetaSet(ctx, d, "commit.strategy", "intent"); err != nil {
		t.Fatalf("set commit.strategy: %v", err)
	}
	if _, err := state.AppendDecision(ctx, d, state.DecisionRecord{
		DecisionTS:  20,
		Kind:        state.DecisionKindIntentPlannerError,
		Path:        sqlNullStr("src/app.go"),
		Reason:      sqlNullStr(`planner returned unsafe seq {"token":"legacy-secret"}`),
		EventSeq:    sql.NullInt64{Int64: 42, Valid: true},
		ActionTaken: sqlNullStr("planner validation failed"),
		UserMessage: sqlNullStr("fallback used"),
	}); err != nil {
		t.Fatalf("AppendDecision planner error: %v", err)
	}
	if _, err := d.SQL().ExecContext(ctx, `DROP TABLE planner_state`); err != nil {
		t.Fatalf("drop planner_state: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if rep.IntentStrategy.LastPlannerErrorEventSeq != 42 ||
		rep.IntentStrategy.LastPlannerErrorPath != "src/app.go" ||
		strings.Contains(rep.IntentStrategy.LastPlannerError, "legacy-secret") ||
		!strings.Contains(rep.IntentStrategy.LastPlannerError, "[REDACTED]") {
		t.Fatalf("last planner error = %+v, want durable decision_records error", rep.IntentStrategy)
	}
}

func TestStatus_IntentStrategyPlannerSummaryIsBarrierAware(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	for k, v := range map[string]string{
		"commit.strategy":    "intent",
		"intent.defer_limit": "2",
	} {
		if err := state.MetaSet(ctx, d, k, v); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}

	barrierSeq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "blocked.go", Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "blocked.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append barrier event: %v", err)
	}
	if err := state.MarkEventPublished(ctx, d, barrierSeq, state.EventStateFailed,
		sql.NullString{}, sqlNullStr("commit failed"), sql.NullString{}, nowFloat()); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	hiddenSeq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "hidden.go", Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "hidden.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append hidden event: %v", err)
	}
	visibleSeq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 2,
		BaseHead: "feedface", Operation: "modify", Path: "visible.go", Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "visible.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append visible event: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := state.RecordPlannerDefer(ctx, d, hiddenSeq, 100+float64(i), "hidden behind barrier"); err != nil {
			t.Fatalf("defer hidden %d: %v", i, err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := state.RecordPlannerDefer(ctx, d, visibleSeq, 50+float64(i), "visible defer"); err != nil {
			t.Fatalf("defer visible %d: %v", i, err)
		}
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if rep.IntentStrategy.DeferredEvents != 1 ||
		rep.IntentStrategy.MaxDeferCount != 2 ||
		rep.IntentStrategy.ForcedAgingReady != 1 ||
		rep.IntentStrategy.LastDeferredEventSeq != visibleSeq ||
		rep.IntentStrategy.LastDeferredPath != "visible.go" {
		t.Fatalf("intent strategy = %+v, want only visible pending planner row", rep.IntentStrategy)
	}
}

func TestStatus_SkipsDecisionSummaryForPreV5DB(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	if _, err := d.SQL().ExecContext(ctx, `DROP TABLE decision_records`); err != nil {
		t.Fatalf("drop decision_records: %v", err)
	}

	var out bytes.Buffer
	if err := runStatus(ctx, &out, repo, false); err != nil {
		t.Fatalf("runStatus should tolerate missing decision_records: %v\n%s", err, out.String())
	}
	if strings.Contains(out.String(), "Decisions:") {
		t.Fatalf("pre-v5 status rendered decisions unexpectedly:\n%s", out.String())
	}
}

func TestStatusWatchRejectsNonPositiveInterval(t *testing.T) {
	var out bytes.Buffer
	if err := runStatusWatch(context.Background(), &out, ".", 0); err == nil {
		t.Fatal("runStatusWatch with zero interval succeeded")
	}
}

// TestList_Status_Doctor_AgreeOnCounts asserts that when the same repo is
// inspected by acd list, acd status, and acd doctor they all report the
// same pending + blocked_conflict counts. This is the contract the cli-lane
// task is meant to enforce: list must not say "PENDING 0" while status sees
// pending events, and doctor must agree with both.
func TestList_Status_Doctor_AgreeOnCounts(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}

	// 3 pending + 2 blocked.
	for _, p := range []string{"a.go", "b.go", "c.go"} {
		if _, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: "deadbeef", Operation: "modify", Path: p, Fidelity: "exact",
		}, []state.CaptureOp{{Op: "modify", Path: p, Fidelity: "exact"}}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	for _, p := range []string{"x.go", "y.go"} {
		seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: "deadbeef", Operation: "modify", Path: p, Fidelity: "rescan",
		}, []state.CaptureOp{{Op: "modify", Path: p, Fidelity: "rescan"}})
		if err != nil {
			t.Fatalf("append blocker: %v", err)
		}
		if err := state.MarkEventBlocked(ctx, d, seq, "before-state mismatch", nowFloat(),
			sql.NullString{String: "refs/heads/main", Valid: true},
			sql.NullInt64{Int64: 1, Valid: true},
			sql.NullString{String: "deadbeef", Valid: true},
		); err != nil {
			t.Fatalf("block: %v", err)
		}
	}

	// list (json)
	var lOut, lErr bytes.Buffer
	if err := runList(ctx, &lOut, &lErr, true, false); err != nil {
		t.Fatalf("runList: %v", err)
	}
	var listGot struct {
		Repos []listEntry `json:"repos"`
	}
	if err := json.Unmarshal(lOut.Bytes(), &listGot); err != nil {
		t.Fatalf("list unmarshal: %v\n%s", err, lOut.String())
	}
	if len(listGot.Repos) != 1 {
		t.Fatalf("list: want 1 repo, got %d", len(listGot.Repos))
	}

	// status (json)
	var sOut bytes.Buffer
	if err := runStatus(ctx, &sOut, repo, true); err != nil {
		t.Fatalf("runStatus: %v", err)
	}
	var statusGot statusReport
	if err := json.Unmarshal(sOut.Bytes(), &statusGot); err != nil {
		t.Fatalf("status unmarshal: %v\n%s", err, sOut.String())
	}

	// doctor (json)
	var docOut bytes.Buffer
	if err := runDoctor(ctx, &docOut, false, "", true); err != nil {
		t.Fatalf("runDoctor: %v", err)
	}
	var docGot doctorReport
	if err := json.Unmarshal(docOut.Bytes(), &docGot); err != nil {
		t.Fatalf("doctor unmarshal: %v\n%s", err, docOut.String())
	}
	if len(docGot.Repos) != 1 {
		t.Fatalf("doctor: want 1 repo, got %d", len(docGot.Repos))
	}

	// Pending + blocked must all match (3, 2).
	if listGot.Repos[0].PendingEvents != 3 {
		t.Errorf("list pending=%d want 3", listGot.Repos[0].PendingEvents)
	}
	if statusGot.PendingEvents != 3 {
		t.Errorf("status pending=%d want 3", statusGot.PendingEvents)
	}
	if docGot.Repos[0].PendingEvents != 3 {
		t.Errorf("doctor pending=%d want 3", docGot.Repos[0].PendingEvents)
	}
	if listGot.Repos[0].BlockedConflicts != 2 {
		t.Errorf("list blocked=%d want 2", listGot.Repos[0].BlockedConflicts)
	}
	if statusGot.BlockedConflicts != 2 {
		t.Errorf("status blocked=%d want 2", statusGot.BlockedConflicts)
	}
	if docGot.Repos[0].BlockedConflicts != 2 {
		t.Errorf("doctor blocked=%d want 2", docGot.Repos[0].BlockedConflicts)
	}
}

func TestStatus_JSONShape(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db, "claude-code")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	var out bytes.Buffer
	if err := runStatus(ctx, &out, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if rep.Repo != repo {
		t.Fatalf("repo = %q, want %q", rep.Repo, repo)
	}
	if rep.PID != 7 {
		t.Fatalf("pid = %d, want 7", rep.PID)
	}
	if rep.Daemon != "running" {
		t.Fatalf("daemon = %q, want running", rep.Daemon)
	}
}

func appendIntentPendingEvent(t *testing.T, ctx context.Context, d *state.DB, path string, capturedTS float64) int64 {
	t.Helper()
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: path,
		Fidelity: "exact", CapturedTS: capturedTS,
	}, []state.CaptureOp{{Op: "modify", Path: path, Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	return seq
}
