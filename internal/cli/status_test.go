package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

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

	stranger := t.TempDir()
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
		PID: 12345, Mode: "running", HeartbeatTS: nowFloat(),
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
	if err := runList(ctx, &lOut, &lErr, true); err != nil {
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
