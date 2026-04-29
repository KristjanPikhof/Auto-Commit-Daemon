//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRecoverReplaysIncidentFixture(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "recover-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	stopSessionForce(t, env, repo)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	afterOID := gitHashObjectStdin(t, repo, "recovered\n")
	now := nowFloatSeconds()
	inject := fmt.Sprintf(`
UPDATE daemon_state SET branch_ref = 'refs/heads/stale', branch_generation = 3, mode = 'stopped' WHERE id = 1;
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
