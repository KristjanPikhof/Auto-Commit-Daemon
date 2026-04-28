package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestGC_DropsMissingRepoDir(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	// Build a repo that exists, then remove it.
	repo, db, d := makeRepoStateDB(t)
	_ = d.Close()
	registerRepo(t, roots, repo, db, "claude-code")
	if err := os.RemoveAll(repo); err != nil {
		t.Fatalf("rm repo: %v", err)
	}

	var out bytes.Buffer
	if err := runGC(ctx, &out, true); err != nil {
		t.Fatalf("runGC: %v", err)
	}
	var rep gcReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rep.Dropped) != 1 || rep.Dropped[0].Reason != "repo-missing" {
		t.Fatalf("expected repo-missing, got %+v", rep.Dropped)
	}
	if rep.Kept != 0 {
		t.Fatalf("kept = %d, want 0", rep.Kept)
	}
}

func TestGC_DropsMissingStateDB(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo := t.TempDir()
	// state.db path under .git/acd that we never create.
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	registerRepo(t, roots, repo, dbPath, "codex")

	var out bytes.Buffer
	if err := runGC(ctx, &out, true); err != nil {
		t.Fatalf("runGC: %v", err)
	}
	var rep gcReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rep.Dropped) != 1 || rep.Dropped[0].Reason != "state-db-missing" {
		t.Fatalf("expected state-db-missing, got %+v", rep.Dropped)
	}
}

func TestGC_DropsDeadDaemon30dOld(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db, "claude-code")

	// Heartbeat older than 30 days, PID guaranteed not alive.
	old := float64(time.Now().Add(-40 * 24 * time.Hour).Unix())
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 1, Mode: "running", HeartbeatTS: old,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	_ = d.Close()

	var out bytes.Buffer
	if err := runGC(ctx, &out, true); err != nil {
		t.Fatalf("runGC: %v", err)
	}
	var rep gcReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rep.Dropped) != 1 || rep.Dropped[0].Reason != "daemon-dead-30d" {
		t.Fatalf("expected daemon-dead-30d, got %+v", rep.Dropped)
	}
}

func TestGC_KeepsLiveDaemon(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db, "claude-code")

	// Use our own pid so identity.Alive returns true.
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	_ = d.Close()

	var out bytes.Buffer
	if err := runGC(ctx, &out, true); err != nil {
		t.Fatalf("runGC: %v", err)
	}
	var rep gcReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rep.Dropped) != 0 || rep.Kept != 1 {
		t.Fatalf("expected 0 dropped, 1 kept; got %+v kept=%d", rep.Dropped, rep.Kept)
	}
}

func TestGC_Idempotent(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	_ = d.Close()
	registerRepo(t, roots, repo, db, "claude-code")
	if err := os.RemoveAll(repo); err != nil {
		t.Fatalf("rm: %v", err)
	}

	var out1 bytes.Buffer
	if err := runGC(ctx, &out1, true); err != nil {
		t.Fatalf("runGC #1: %v", err)
	}

	// Second run should be a no-op.
	var out2 bytes.Buffer
	if err := runGC(ctx, &out2, true); err != nil {
		t.Fatalf("runGC #2: %v", err)
	}
	var rep2 gcReport
	if err := json.Unmarshal(out2.Bytes(), &rep2); err != nil {
		t.Fatalf("unmarshal #2: %v", err)
	}
	if len(rep2.Dropped) != 0 {
		t.Fatalf("second run dropped %d entries (should be 0)", len(rep2.Dropped))
	}

	// Registry should now be empty.
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(reg.Repos) != 0 {
		t.Fatalf("registry has %d entries after gc, want 0", len(reg.Repos))
	}
}
