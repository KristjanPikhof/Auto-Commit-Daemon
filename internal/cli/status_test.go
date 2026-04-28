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
