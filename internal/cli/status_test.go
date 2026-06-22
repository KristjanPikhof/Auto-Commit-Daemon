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
		Reason:      sqlNullStr("planner returned unsafe seq"),
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
		rep.IntentStrategy.LastPlannerError != "planner returned unsafe seq" {
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
