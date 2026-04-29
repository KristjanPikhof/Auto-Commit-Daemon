//go:build integration
// +build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRecoverReplaysIncidentFixture(t *testing.T) {
	repo := tempRepo(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("XDG_DATA_HOME", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	env := make([]string, 0, len(os.Environ())+4)
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "HOME=") ||
			strings.HasPrefix(kv, "XDG_STATE_HOME=") ||
			strings.HasPrefix(kv, "XDG_DATA_HOME=") ||
			strings.HasPrefix(kv, "XDG_CONFIG_HOME=") {
			continue
		}
		env = append(env, kv)
	}
	env = append(env, "HOME="+home, "XDG_STATE_HOME=", "XDG_DATA_HOME=", "XDG_CONFIG_HOME=")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	roots, err := paths.Resolve()
	if err != nil {
		t.Fatalf("paths.Resolve: %v", err)
	}
	repoHash, err := paths.RepoHash(repo)
	if err != nil {
		t.Fatalf("RepoHash: %v", err)
	}
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.UpsertRepo(repo, repoHash, dbPath, "test", time.Now().Unix())
		return nil
	}); err != nil {
		t.Fatalf("register repo: %v", err)
	}

	head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID:              999999,
		Mode:             "stopped",
		BranchRef:        sql.NullString{String: "refs/heads/stale", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 3, Valid: true},
	}); err != nil {
		t.Fatalf("SaveDaemonState: %v", err)
	}
	afterOID := gitHashObjectStdin(t, repo, "recovered\n")
	now := nowFloatSeconds()
	inject := fmt.Sprintf(`
INSERT INTO daemon_meta(key, value, updated_ts) VALUES('branch.generation', '3', %f)
  ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_ts = excluded.updated_ts;
INSERT INTO daemon_meta(key, value, updated_ts) VALUES('last_replay_conflict', '{"seq":1,"error_class":"cas_fail"}', %f)
  ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_ts = excluded.updated_ts;
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state, error)
VALUES ('refs/heads/stale', 3, '%s', 'create', 'recover.txt', 'exact', %f, 'blocked_conflict', 'old conflict');
INSERT INTO capture_ops(event_seq, ord, op, path, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'create', 'recover.txt', '%s', '100644', 'exact');
`, now, now, head, now, afterOID)
	if out, err := exec.Command("sqlite3", dbPath, inject).CombinedOutput(); err != nil {
		t.Fatalf("inject incident fixture: %v\n%s", err, out)
	}

	dry := runAcd(t, ctx, env, "recover", "--repo", repo, "--auto", "--dry-run", "--json")
	if dry.ExitCode != 0 {
		t.Fatalf("acd recover dry-run exit=%d\nstdout=%s\nstderr=%s", dry.ExitCode, dry.Stdout, dry.Stderr)
	}
	if state := sqliteScalar(t, dbPath, "SELECT state FROM capture_events WHERE path = 'recover.txt'"); state != "blocked_conflict" {
		t.Fatalf("dry-run mutated event state=%q", state)
	}

	applied := runAcd(t, ctx, env, "recover", "--repo", repo, "--auto", "--yes", "--json")
	if applied.ExitCode != 0 {
		t.Fatalf("acd recover apply exit=%d\nstdout=%s\nstderr=%s", applied.ExitCode, applied.Stdout, applied.Stderr)
	}
	var payload struct {
		BackupPath string `json:"backup_path"`
	}
	if err := json.Unmarshal([]byte(applied.Stdout), &payload); err != nil {
		t.Fatalf("decode recover output: %v\n%s", err, applied.Stdout)
	}
	if payload.BackupPath == "" {
		t.Fatalf("recover output missing backup path: %s", applied.Stdout)
	}
	if _, err := os.Stat(payload.BackupPath); err != nil {
		t.Fatalf("backup missing: %v", err)
	}
	if got := sqliteScalar(t, dbPath, "SELECT branch_ref || '|' || state FROM capture_events WHERE path = 'recover.txt'"); got != "refs/heads/main|pending" {
		t.Fatalf("event after recover=%q want refs/heads/main|pending", got)
	}
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM daemon_meta WHERE key = 'last_replay_conflict'"); got != "0" {
		t.Fatalf("last_replay_conflict rows=%s want 0", got)
	}
}
