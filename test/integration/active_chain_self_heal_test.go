//go:build integration
// +build integration

package integration_test

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestActiveChainSelfHeal_ArchivesRecapturesAndPublishes proves the complete
// unattended recovery loop through the production CLI and daemon: an active
// blocked chain is archived without losing its dirty final worktree, shadow is
// rebuilt from HEAD, and fresh captures publish that work normally.
func TestActiveChainSelfHeal_ArchivesRecapturesAndPublishes(t *testing.T) {
	requireSQLite(t)
	t.Parallel()

	repo := tempRepo(t)
	env := envWith(withIsolatedHome(t),
		"ACD_COMMIT_STRATEGY=event",
		"ACD_AI_PROVIDER=deterministic",
		"ACD_PATH_QUIESCENCE_SECONDS=0",
		"ACD_REWIND_GRACE_SECONDS=0",
	)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	baseHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	dbPath := initStateDBSchema(t, ctx, env, repo, "active-chain-bootstrap")
	generation := sqliteScalar(t, dbPath,
		"SELECT value FROM daemon_meta WHERE key = 'branch.generation'")
	if generation == "" {
		generation = "1"
	}

	const (
		firstPath  = "self-heal-a.txt"
		secondPath = "self-heal-b.txt"
		firstBody  = "final a\n"
		secondBody = "final b\n"
	)
	writeFile(t, filepath.Join(repo, firstPath), firstBody)
	writeFile(t, filepath.Join(repo, secondPath), secondBody)
	firstOID := gitHashObjectStdin(t, repo, firstBody)
	secondOID := gitHashObjectStdin(t, repo, secondBody)

	original := seedCreateRecoveryPair(t, dbPath, "refs/heads/main", generation, baseHead,
		firstPath, firstOID, secondPath, secondOID)
	seedSQL := fmt.Sprintf(`
BEGIN;
INSERT INTO shadow_paths(branch_ref, branch_generation, path, operation, mode, oid, base_head, fidelity, updated_ts)
VALUES ('refs/heads/main', %s, '%s', 'create', '100644', '%s', '%s', 'exact', %f)
ON CONFLICT(branch_ref, branch_generation, path) DO UPDATE SET
    operation=excluded.operation, mode=excluded.mode, oid=excluded.oid,
    base_head=excluded.base_head, fidelity=excluded.fidelity, updated_ts=excluded.updated_ts;
INSERT INTO shadow_paths(branch_ref, branch_generation, path, operation, mode, oid, base_head, fidelity, updated_ts)
VALUES ('refs/heads/main', %s, '%s', 'create', '100644', '%s', '%s', 'exact', %f)
ON CONFLICT(branch_ref, branch_generation, path) DO UPDATE SET
    operation=excluded.operation, mode=excluded.mode, oid=excluded.oid,
    base_head=excluded.base_head, fidelity=excluded.fidelity, updated_ts=excluded.updated_ts;
INSERT INTO publish_state(id, event_seq, branch_ref, branch_generation, source_head, status, error, updated_ts)
VALUES (1, %s, 'refs/heads/main', %s, '%s', 'blocked_conflict', 'integration active-chain barrier', %f)
ON CONFLICT(id) DO UPDATE SET
    event_seq=excluded.event_seq, branch_ref=excluded.branch_ref,
    branch_generation=excluded.branch_generation, source_head=excluded.source_head,
    status=excluded.status, error=excluded.error, updated_ts=excluded.updated_ts;
COMMIT;`,
		generation, firstPath, firstOID, baseHead, nowFloatSeconds(),
		generation, secondPath, secondOID, baseHead, nowFloatSeconds(),
		original.FirstSeq, generation, baseHead, nowFloatSeconds())
	if out, err := exec.Command("sqlite3", dbPath, seedSQL).CombinedOutput(); err != nil {
		t.Fatalf("seed active-chain shadow and barrier: %v\n%s", err, out)
	}

	startSession(t, ctx, env, repo, "active-chain-self-heal", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	deadline := time.Now().Add(25 * time.Second)
	settled := false
	for time.Now().Before(deadline) {
		wakeSession(t, ctx, env, repo, "active-chain-self-heal")
		query := fmt.Sprintf(`
SELECT
  (SELECT COUNT(*) FROM capture_events WHERE seq IN (%s,%s) AND state = 'recovered') || '|' ||
  (SELECT COUNT(*) FROM capture_events WHERE seq NOT IN (%s,%s) AND path IN ('%s','%s') AND state = 'published') || '|' ||
  (SELECT COUNT(*) FROM capture_events WHERE state IN ('pending','blocked_conflict','failed'));`,
			original.FirstSeq, original.SecondSeq, original.FirstSeq, original.SecondSeq,
			firstPath, secondPath)
		out, err := exec.Command("sqlite3", dbPath, query).CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) == "2|2|0" {
			settled = true
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	if !settled {
		dump, _ := exec.Command("sqlite3", dbPath, `
SELECT seq,path,state,commit_oid,error FROM capture_events ORDER BY seq;
SELECT id,outcome,event_count,commit_oid,recovery_ref FROM recovery_snapshots ORDER BY id;
SELECT id,event_seq,status,error FROM publish_state;`).CombinedOutput()
		t.Fatalf("active-chain self-heal did not settle\nstate:\n%s\nlog:\n%s",
			dump, runGitOK(t, repo, "log", "--oneline", "--decorate", "--all"))
	}

	snapshotID := sqliteScalar(t, dbPath, fmt.Sprintf(
		"SELECT snapshot_id FROM recovery_snapshot_events WHERE event_seq = %s", original.FirstSeq))
	if snapshotID == "" {
		t.Fatal("original blocked pair has no recovery snapshot membership")
	}
	if got := sqliteScalar(t, dbPath, fmt.Sprintf(`
SELECT outcome || '|' || event_count || '|' || first_event_seq || '|' || last_event_seq
FROM recovery_snapshots WHERE id = %s`, snapshotID)); got !=
		fmt.Sprintf("recovered|2|%s|%s", original.FirstSeq, original.SecondSeq) {
		t.Fatalf("recovery snapshot summary=%q", got)
	}
	if got := sqliteScalar(t, dbPath, fmt.Sprintf(`
SELECT group_concat(event_seq, ',')
FROM (SELECT event_seq FROM recovery_snapshot_events WHERE snapshot_id = %s ORDER BY ord)`, snapshotID)); got !=
		original.FirstSeq+","+original.SecondSeq {
		t.Fatalf("recovery snapshot membership=%q", got)
	}
	recoveryRef := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT recovery_ref FROM recovery_snapshots WHERE id = %s", snapshotID))
	recoveryCommit := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT commit_oid FROM recovery_snapshots WHERE id = %s", snapshotID))
	if !strings.HasPrefix(recoveryRef, "refs/acd/recovery/") || !strings.HasSuffix(recoveryRef, "/archive") {
		t.Fatalf("recovery ref=%q want refs/acd/recovery/.../archive", recoveryRef)
	}
	if got := strings.TrimSpace(runGitOK(t, repo, "show-ref", "--hash", "--verify", recoveryRef)); got != recoveryCommit {
		t.Fatalf("recovery ref resolves to %q want snapshot commit %q", got, recoveryCommit)
	}
	if got := strings.TrimSpace(runGitOK(t, repo, "rev-parse", recoveryRef+":"+firstPath)); got != firstOID {
		t.Fatalf("archived %s oid=%s want %s", firstPath, got, firstOID)
	}
	if got := strings.TrimSpace(runGitOK(t, repo, "rev-parse", recoveryRef+":"+secondPath)); got != secondOID {
		t.Fatalf("archived %s oid=%s want %s", secondPath, got, secondOID)
	}

	marker := "shadow.bootstrapped:refs/heads/main:" + generation
	if got := sqliteScalar(t, dbPath, fmt.Sprintf(
		"SELECT COUNT(*) FROM daemon_meta WHERE key = '%s'", marker)); got != "1" {
		t.Fatalf("recaptured shadow marker count=%s want 1", got)
	}
	if got := sqliteScalar(t, dbPath, fmt.Sprintf(`
SELECT COUNT(*) FROM shadow_paths
WHERE branch_ref = 'refs/heads/main' AND branch_generation = %s
  AND ((path = '%s' AND oid = '%s') OR (path = '%s' AND oid = '%s'))`,
		generation, firstPath, firstOID, secondPath, secondOID)); got != "2" {
		t.Fatalf("recaptured final shadow rows=%s want 2", got)
	}

	if got := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD:"+firstPath)); got != firstOID {
		t.Fatalf("HEAD %s oid=%s want %s", firstPath, got, firstOID)
	}
	if got := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD:"+secondPath)); got != secondOID {
		t.Fatalf("HEAD %s oid=%s want %s", secondPath, got, secondOID)
	}
	if status := strings.TrimSpace(runGitOK(t, repo, "status", "--porcelain=v1")); status != "" {
		t.Fatalf("worktree not clean after replacement publish:\n%s", status)
	}
	if got := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM capture_events WHERE state IN ('pending','blocked_conflict','failed')"); got != "0" {
		t.Fatalf("terminal/pending blockers remain=%s", got)
	}
}
