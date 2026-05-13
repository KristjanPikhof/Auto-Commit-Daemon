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

func TestGC_MergesDuplicateSubdirRowAndReportsJSON(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, db, d := makeRepoStateDB(t)
	_ = d.Close()
	subdir := filepath.Join(repo, "nested")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}
	now := time.Now().Unix()
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.Repos = []central.RepoRecord{
			{Path: repo, RepoHash: "h1", StateDB: db, FirstRegisteredTS: now - 20, LastSeenTS: now - 10, Harnesses: []string{"codex"}},
			{Path: subdir, RepoHash: "h1", StateDB: db, FirstRegisteredTS: now - 30, LastSeenTS: now, Harnesses: []string{"claude-code"}},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	var out bytes.Buffer
	if err := runGC(ctx, &out, true); err != nil {
		t.Fatalf("runGC: %v", err)
	}
	var rep gcReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rep.Merged) != 1 || rep.Merged[0].Reason != "same-git-toplevel" {
		t.Fatalf("merged=%+v, want one same-git-toplevel merge", rep.Merged)
	}
	if len(rep.Dropped) != 0 || rep.Kept != 1 {
		t.Fatalf("report dropped=%+v kept=%d, want 0 dropped, 1 kept", rep.Dropped, rep.Kept)
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(reg.Repos) != 1 || reg.Repos[0].Path != repo {
		t.Fatalf("registry=%+v, want one canonical repo row", reg.Repos)
	}
	if reg.Repos[0].FirstRegisteredTS != now-30 || reg.Repos[0].LastSeenTS != now {
		t.Fatalf("timestamps not merged: %+v", reg.Repos[0])
	}
}

func TestGC_MergesSameStateDBRowsAndIsIdempotent(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo1, db, d := makeRepoStateDB(t)
	_ = d.Close()
	repo2 := t.TempDir()
	now := time.Now().Unix()
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.Repos = []central.RepoRecord{
			{Path: repo1, RepoHash: "h1", StateDB: db, FirstRegisteredTS: now - 20, LastSeenTS: now - 10, Harnesses: []string{"codex"}},
			{Path: repo2, RepoHash: "h2", StateDB: db, FirstRegisteredTS: now - 15, LastSeenTS: now, Harnesses: []string{"pi"}},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	var out bytes.Buffer
	if err := runGC(ctx, &out, true); err != nil {
		t.Fatalf("runGC #1: %v", err)
	}
	var rep gcReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal #1: %v", err)
	}
	if len(rep.Merged) != 1 || rep.Merged[0].Reason != "same-state-db" {
		t.Fatalf("merged=%+v, want one same-state-db merge", rep.Merged)
	}
	if len(rep.Dropped) != 0 || rep.Kept != 1 {
		t.Fatalf("report dropped=%+v kept=%d, want 0 dropped, 1 kept", rep.Dropped, rep.Kept)
	}

	out.Reset()
	if err := runGC(ctx, &out, true); err != nil {
		t.Fatalf("runGC #2: %v", err)
	}
	var rep2 gcReport
	if err := json.Unmarshal(out.Bytes(), &rep2); err != nil {
		t.Fatalf("unmarshal #2: %v", err)
	}
	if len(rep2.Merged) != 0 || len(rep2.Dropped) != 0 || rep2.Kept != 1 {
		t.Fatalf("second run not idempotent: %+v", rep2)
	}
}

func TestGC_MergePreservesExistingStateDBWhenDuplicateHasMissingDB(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, db, d := makeRepoStateDB(t)
	_ = d.Close()
	subdir := filepath.Join(repo, "legacy")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}
	missingDB := filepath.Join(repo, ".git", "acd", "missing-state.db")
	now := time.Now().Unix()
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.Repos = []central.RepoRecord{
			{Path: repo, RepoHash: "h1", StateDB: db, FirstRegisteredTS: now - 20, LastSeenTS: now - 10, Harnesses: []string{"codex"}},
			{Path: subdir, RepoHash: "h1", StateDB: missingDB, FirstRegisteredTS: now - 15, LastSeenTS: now, Harnesses: []string{"claude-code"}},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	var out bytes.Buffer
	if err := runGC(ctx, &out, true); err != nil {
		t.Fatalf("runGC: %v", err)
	}
	var rep gcReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rep.Merged) != 1 || len(rep.Dropped) != 0 || rep.Kept != 1 {
		t.Fatalf("report=%+v, want merge without stale missing-db drop", rep)
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(reg.Repos) != 1 || reg.Repos[0].StateDB != db {
		t.Fatalf("registry=%+v, want existing state DB preserved", reg.Repos)
	}
}

func TestGC_DropsDeadDaemon30dOld(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db, "claude-code")

	// Heartbeat older than 30 days, PID guaranteed not alive (out of pid_max range).
	old := float64(time.Now().Add(-40 * 24 * time.Hour).Unix())
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 0x7fffffff, Mode: "running", HeartbeatTS: old,
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
