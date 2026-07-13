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

func TestFixArchivesIncidentFixtureWithoutRetargeting(t *testing.T) {
	buildAcdBinary(t)

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

	refsBefore := recoveryRefList(t, repo)
	dry := runAcd(t, ctx, env, "fix", "--repo", repo, "--dry-run", "--json")
	if dry.ExitCode != 0 {
		t.Fatalf("acd fix dry-run exit=%d\nstdout=%s\nstderr=%s", dry.ExitCode, dry.Stdout, dry.Stderr)
	}
	if state := sqliteScalar(t, dbPath, "SELECT state FROM capture_events WHERE path = 'recover.txt'"); state != "blocked_conflict" {
		t.Fatalf("dry-run mutated event state=%q", state)
	}
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM recovery_snapshots"); got != "0" {
		t.Fatalf("dry-run created recovery snapshots=%s", got)
	}
	if refsAfter := recoveryRefList(t, repo); refsAfter != refsBefore {
		t.Fatalf("dry-run mutated recovery refs:\nbefore=%s\nafter=%s", refsBefore, refsAfter)
	}

	applied := runAcd(t, ctx, env, "fix", "--repo", repo, "--yes", "--json")
	if applied.ExitCode != 0 {
		t.Fatalf("acd fix apply exit=%d\nstdout=%s\nstderr=%s", applied.ExitCode, applied.Stdout, applied.Stderr)
	}
	var payload reliabilityFixPlan
	if err := json.Unmarshal([]byte(applied.Stdout), &payload); err != nil {
		t.Fatalf("decode fix output: %v\n%s", err, applied.Stdout)
	}
	if payload.BackupPath == "" {
		t.Fatalf("fix output missing backup path: %s", applied.Stdout)
	}
	if _, err := os.Stat(payload.BackupPath); err != nil {
		t.Fatalf("backup missing: %v", err)
	}
	if len(payload.Actions) != 1 {
		t.Fatalf("fix actions=%d want 1\n%s", len(payload.Actions), applied.Stdout)
	}
	action := payload.Actions[0]
	if action.Kind != "reconcile_unpublished_chain" || !action.Applied ||
		action.State != "recovered" || action.RowsChanged != 1 ||
		!strings.HasPrefix(action.RecoveryRef, "refs/acd/recovery/") ||
		!strings.HasSuffix(action.RecoveryRef, "/archive") {
		t.Fatalf("unexpected recovery action=%+v\n%s", action, applied.Stdout)
	}
	if got := sqliteScalar(t, dbPath, "SELECT branch_ref || '|' || branch_generation || '|' || state FROM capture_events WHERE path = 'recover.txt'"); got != "refs/heads/stale|3|recovered" {
		t.Fatalf("event after fix=%q want refs/heads/stale|3|recovered", got)
	}
	seq := sqliteScalar(t, dbPath, "SELECT seq FROM capture_events WHERE path = 'recover.txt'")
	snapshotID := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT snapshot_id FROM recovery_snapshot_events WHERE event_seq = %s", seq))
	if snapshotID == "" {
		t.Fatal("recovered event has no protected snapshot membership")
	}
	if got := sqliteScalar(t, dbPath, fmt.Sprintf(`
SELECT outcome || '|' || event_count || '|' || first_event_seq || '|' || last_event_seq || '|' || recovery_ref
FROM recovery_snapshots WHERE id = %s`, snapshotID)); got !=
		fmt.Sprintf("recovered|1|%s|%s|%s", seq, seq, action.RecoveryRef) {
		t.Fatalf("recovery snapshot=%q", got)
	}
	recoveryCommit := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT commit_oid FROM recovery_snapshots WHERE id = %s", snapshotID))
	if got := strings.TrimSpace(runGitOK(t, repo, "show-ref", "--hash", "--verify", action.RecoveryRef)); got != recoveryCommit {
		t.Fatalf("recovery ref resolves to %q want %q", got, recoveryCommit)
	}
	if got := strings.TrimSpace(runGitOK(t, repo, "rev-parse", action.RecoveryRef+":recover.txt")); got != afterOID {
		t.Fatalf("archived recover.txt oid=%s want %s", got, afterOID)
	}
	// The fixture intentionally has no matching publish_state breadcrumb. Exact
	// recovery must not erase unrelated diagnostic metadata merely because its
	// text mentions the recovered seq.
	if got := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'last_replay_conflict'"); got != `{"seq":1,"error_class":"cas_fail"}` {
		t.Fatalf("unrelated last_replay_conflict changed=%q", got)
	}
}

func TestFixLeavesPublishedStaleLiveIndexUntouched(t *testing.T) {
	buildAcdBinary(t)

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

	baseHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(repo, "legacy.txt"), "legacy published\n")
	afterOID := gitHashObjectStdin(t, repo, "legacy published\n")
	runGitOK(t, repo, "add", "legacy.txt")
	runGitOK(t, repo, "commit", "-q", "-m", "legacy acd publish")
	publishedHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	runGitOK(t, repo, "update-index", "--force-remove", "--", "legacy.txt")
	status := strings.TrimSpace(runGitOK(t, repo, "status", "--porcelain"))
	if !strings.Contains(status, "D  legacy.txt") || !strings.Contains(status, "?? legacy.txt") {
		t.Fatalf("test did not create stale live-index shape:\n%s", status)
	}
	statusBefore := status

	now := nowFloatSeconds()
	inject := fmt.Sprintf(`
INSERT INTO daemon_meta(key, value, updated_ts) VALUES('branch.generation', '1', %f)
  ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_ts = excluded.updated_ts;
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, published_ts, state, commit_oid)
VALUES ('refs/heads/main', 1, '%s', 'create', 'legacy.txt', 'exact', %f, %f, 'published', '%s');
INSERT INTO capture_ops(event_seq, ord, op, path, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'create', 'legacy.txt', '%s', '100644', 'exact');
`, now, baseHead, now, now, publishedHead, afterOID)
	if out, err := exec.Command("sqlite3", dbPath, inject).CombinedOutput(); err != nil {
		t.Fatalf("inject published fixture: %v\n%s", err, out)
	}

	applied := runAcd(t, ctx, env, "fix", "--repo", repo, "--yes", "--json")
	if applied.ExitCode != 0 {
		t.Fatalf("acd fix apply exit=%d\nstdout=%s\nstderr=%s", applied.ExitCode, applied.Stdout, applied.Stderr)
	}
	var payload reliabilityFixPlan
	if err := json.Unmarshal([]byte(applied.Stdout), &payload); err != nil {
		t.Fatalf("decode fix output: %v\n%s", err, applied.Stdout)
	}
	if len(payload.Actions) != 0 || payload.RowsChanged != 0 || payload.BackupPath != "" {
		t.Fatalf("published-only fixture should need no fix actions: %+v\n%s", payload, applied.Stdout)
	}
	if statusAfter := strings.TrimSpace(runGitOK(t, repo, "status", "--porcelain")); statusAfter != statusBefore {
		t.Fatalf("fix mutated the user's stale live index:\nbefore:\n%s\nafter:\n%s", statusBefore, statusAfter)
	}
	if got := sqliteScalar(t, dbPath, "SELECT state || '|' || commit_oid FROM capture_events WHERE path = 'legacy.txt'"); got != "published|"+publishedHead {
		t.Fatalf("published event changed=%q", got)
	}
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM recovery_snapshots"); got != "0" {
		t.Fatalf("published-only fixture created recovery snapshots=%s", got)
	}
}

type reliabilityFixPlan struct {
	BackupPath  string                 `json:"backup_path"`
	RowsChanged int64                  `json:"rows_changed"`
	Actions     []reliabilityFixAction `json:"actions"`
}

type reliabilityFixAction struct {
	Kind        string `json:"kind"`
	Applied     bool   `json:"applied"`
	RowsChanged int64  `json:"rows_changed"`
	State       string `json:"state"`
	RecoveryRef string `json:"recovery_ref"`
}
