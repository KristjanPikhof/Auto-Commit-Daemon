package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestList_Human_TwoRepos(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repoA, dbA, dA := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, dA, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save A: %v", err)
	}

	repoB, dbB, dB := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, dB, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save B: %v", err)
	}

	registerRepo(t, roots, repoA, dbA, "claude-code")
	registerRepo(t, roots, repoB, dbB, "codex")

	var stdout, stderr bytes.Buffer
	if err := runList(ctx, &stdout, &stderr, false); err != nil {
		t.Fatalf("runList: %v", err)
	}
	out := stdout.String()
	if !strings.Contains(out, "REPO") || !strings.Contains(out, "DAEMON") {
		t.Fatalf("missing header in output:\n%s", out)
	}
	// Two body rows: count newlines minus header.
	lines := 0
	for _, l := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if l == "" {
			continue
		}
		lines++
	}
	if lines != 3 {
		t.Fatalf("expected 1 header + 2 rows, got %d:\n%s", lines, out)
	}
}

func TestList_JSON_TwoRepos(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repoA, dbA, dA := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, dA, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save A: %v", err)
	}
	repoB, dbB, dB := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, dB, state.DaemonState{
		PID: 2, Mode: "sleeping", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save B: %v", err)
	}

	registerRepo(t, roots, repoA, dbA, "claude-code")
	registerRepo(t, roots, repoB, dbB, "codex")

	var stdout, stderr bytes.Buffer
	if err := runList(ctx, &stdout, &stderr, true); err != nil {
		t.Fatalf("runList json: %v", err)
	}
	var got struct {
		Repos []listEntry `json:"repos"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, stdout.String())
	}
	if len(got.Repos) != 2 {
		t.Fatalf("want 2 repos, got %d", len(got.Repos))
	}
	for _, r := range got.Repos {
		if r.Path == "" || r.RepoHash == "" {
			t.Fatalf("missing fields in %+v", r)
		}
	}
}

func TestList_StatusColumnShowsManualPause(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon: %v", err)
	}
	writePauseMarkerForStateDB(t, dbPath, pausepkg.Marker{
		Reason: "deploy",
		SetAt:  time.Now().UTC().Format(time.RFC3339),
		SetBy:  "test",
	})
	registerRepo(t, roots, repo, dbPath, "codex")

	var stdout, stderr bytes.Buffer
	if err := runList(ctx, &stdout, &stderr, false); err != nil {
		t.Fatalf("runList: %v", err)
	}
	if !strings.Contains(stdout.String(), "paused (manual)") {
		t.Fatalf("missing manual pause status:\n%s", stdout.String())
	}
}

func TestList_StatusColumnShowsRewindGrace(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon: %v", err)
	}
	if err := state.MetaSet(ctx, d, replayPausedUntilMetaKey, time.Now().UTC().Add(time.Minute).Format(time.RFC3339)); err != nil {
		t.Fatalf("MetaSet: %v", err)
	}
	registerRepo(t, roots, repo, dbPath, "codex")

	var stdout, stderr bytes.Buffer
	if err := runList(ctx, &stdout, &stderr, false); err != nil {
		t.Fatalf("runList: %v", err)
	}
	got := stdout.String()
	if !strings.Contains(got, "paused (rewind grace, expires in") {
		t.Fatalf("missing rewind grace pause status:\n%s", got)
	}
}

func TestList_NoPauseShowsOK(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon: %v", err)
	}
	registerRepo(t, roots, repo, dbPath, "codex")

	var stdout, stderr bytes.Buffer
	if err := runList(ctx, &stdout, &stderr, true); err != nil {
		t.Fatalf("runList: %v", err)
	}
	var got struct {
		Repos []listEntry `json:"repos"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, stdout.String())
	}
	if len(got.Repos) != 1 {
		t.Fatalf("repos=%d want 1", len(got.Repos))
	}
	if got.Repos[0].Status != "OK" || got.Repos[0].Paused || got.Repos[0].Pause != nil {
		t.Fatalf("unexpected clean repo status: %+v", got.Repos[0])
	}
}

func TestList_StaleHeartbeatMarked(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	stale := float64(time.Now().Add(-3 * time.Hour).Unix())
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 1, Mode: "running", HeartbeatTS: stale,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := state.RegisterClient(ctx, d, state.Client{
		SessionID: "live-client", Harness: "codex", LastSeenTS: nowFloat(),
	}); err != nil {
		t.Fatalf("register client: %v", err)
	}
	registerRepo(t, roots, repo, db, "claude-code")

	var stdout, stderr bytes.Buffer
	if err := runList(ctx, &stdout, &stderr, false); err != nil {
		t.Fatalf("runList: %v", err)
	}
	if !strings.Contains(stdout.String(), "stale") {
		t.Fatalf("expected stale marker, got:\n%s", stdout.String())
	}
}

func TestList_HidesStaleDaemonWithoutLiveClients(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	stale := float64(time.Now().Add(-3 * time.Hour).Unix())
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 1, Mode: "running", HeartbeatTS: stale,
	}); err != nil {
		t.Fatalf("save daemon: %v", err)
	}
	if err := state.RegisterClient(ctx, d, state.Client{
		SessionID:    "old-client",
		Harness:      "codex",
		RegisteredTS: float64(time.Now().Add(-3 * time.Hour).Unix()),
		LastSeenTS:   float64(time.Now().Add(-3 * time.Hour).Unix()),
	}); err != nil {
		t.Fatalf("register old client: %v", err)
	}
	registerRepo(t, roots, repo, db, "codex")

	var stdout, stderr bytes.Buffer
	if err := runList(ctx, &stdout, &stderr, true); err != nil {
		t.Fatalf("runList json: %v", err)
	}
	var got struct {
		Repos []listEntry `json:"repos"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, stdout.String())
	}
	if len(got.Repos) != 0 {
		t.Fatalf("repos=%d, want inactive stale repo hidden: %+v", len(got.Repos), got.Repos)
	}
}

func TestList_CountsOnlyLiveClients(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	now := time.Now()
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: float64(now.Unix()),
	}); err != nil {
		t.Fatalf("save daemon: %v", err)
	}
	if err := state.RegisterClient(ctx, d, state.Client{
		SessionID:    "old-client",
		Harness:      "codex",
		RegisteredTS: float64(now.Add(-2 * time.Hour).Unix()),
		LastSeenTS:   float64(now.Add(-2 * time.Hour).Unix()),
	}); err != nil {
		t.Fatalf("register old client: %v", err)
	}
	if err := state.RegisterClient(ctx, d, state.Client{
		SessionID:    "live-client",
		Harness:      "codex",
		RegisteredTS: float64(now.Unix()),
		LastSeenTS:   float64(now.Unix()),
	}); err != nil {
		t.Fatalf("register live client: %v", err)
	}
	registerRepo(t, roots, repo, dbPath, "codex")

	var stdout, stderr bytes.Buffer
	if err := runList(ctx, &stdout, &stderr, true); err != nil {
		t.Fatalf("runList json: %v", err)
	}
	var got struct {
		Repos []listEntry `json:"repos"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, stdout.String())
	}
	if len(got.Repos) != 1 {
		t.Fatalf("repos=%d, want 1", len(got.Repos))
	}
	if got.Repos[0].Clients != 1 {
		t.Fatalf("clients=%d, want 1 live client", got.Repos[0].Clients)
	}
}

// TestList_PendingAndBlockedFromState verifies that `acd list` reads
// pending + blocked_conflict counts from state.db rather than rendering
// hardcoded zeros, and that human + JSON output agree.
func TestList_PendingAndBlockedFromState(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon: %v", err)
	}

	// Two pending events.
	for _, p := range []string{"a.go", "b.go"} {
		if _, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: "deadbeef", Operation: "modify", Path: p,
			Fidelity: "exact", CapturedTS: nowFloat(),
		}, []state.CaptureOp{{Op: "modify", Path: p, Fidelity: "exact"}}); err != nil {
			t.Fatalf("append pending: %v", err)
		}
	}

	// One blocked-conflict event.
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "ghost.txt",
		Fidelity: "rescan",
	}, []state.CaptureOp{{Op: "modify", Path: "ghost.txt", Fidelity: "rescan"}})
	if err != nil {
		t.Fatalf("append blocker: %v", err)
	}
	if err := state.MarkEventBlocked(ctx, d, seq, "before-state mismatch", nowFloat(),
		sql.NullString{String: "refs/heads/main", Valid: true},
		sql.NullInt64{Int64: 1, Valid: true},
		sql.NullString{String: "deadbeef", Valid: true},
	); err != nil {
		t.Fatalf("MarkEventBlocked: %v", err)
	}

	registerRepo(t, roots, repo, dbPath, "claude-code")

	// Human output exposes both columns and counts.
	var humanOut, humanErr bytes.Buffer
	if err := runList(ctx, &humanOut, &humanErr, false); err != nil {
		t.Fatalf("runList human: %v", err)
	}
	human := humanOut.String()
	if !strings.Contains(human, "PENDING") || !strings.Contains(human, "BLOCKED") {
		t.Fatalf("human output missing PENDING/BLOCKED columns:\n%s", human)
	}

	// JSON shape exposes counts as integers and matches the state we wrote.
	var jsonOut, jsonErr bytes.Buffer
	if err := runList(ctx, &jsonOut, &jsonErr, true); err != nil {
		t.Fatalf("runList json: %v", err)
	}
	var got struct {
		Repos []listEntry `json:"repos"`
	}
	if err := json.Unmarshal(jsonOut.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if len(got.Repos) != 1 {
		t.Fatalf("repos=%d, want 1", len(got.Repos))
	}
	if got.Repos[0].PendingEvents != 2 {
		t.Fatalf("PendingEvents=%d, want 2", got.Repos[0].PendingEvents)
	}
	if got.Repos[0].BlockedConflicts != 1 {
		t.Fatalf("BlockedConflicts=%d, want 1", got.Repos[0].BlockedConflicts)
	}
}

func TestList_MissingStateDB_Reported(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	// Register a repo whose state.db never existed.
	repo, db, d := makeRepoStateDB(t)
	_ = d.Close()
	registerRepo(t, roots, repo, db+".doesnotexist", "claude-code")

	var stdout, stderr bytes.Buffer
	if err := runList(ctx, &stdout, &stderr, false); err != nil {
		t.Fatalf("runList: %v", err)
	}
	if !strings.Contains(stdout.String(), "missing") {
		t.Fatalf("expected 'missing' marker, got:\n%s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "state.db missing") {
		t.Fatalf("expected slog/log warn for missing state.db, got stderr:\n%s", stderr.String())
	}
}

func writePauseMarkerForStateDB(t *testing.T, stateDBPath string, marker pausepkg.Marker) {
	t.Helper()
	gitDir := filepath.Dir(filepath.Dir(stateDBPath))
	if err := pausepkg.Write(pausepkg.Path(gitDir), marker, true); err != nil {
		t.Fatalf("write pause marker: %v", err)
	}
}
