package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

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
