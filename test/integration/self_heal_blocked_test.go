//go:build integration
// +build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestSelfHeal_BlockedPromotesToPublishedOnReplay drives Wave 2's idempotent
// self-heal probe (probeBlockedSelfHeal in internal/daemon/replay.go) through
// the real daemon. Scenario:
//
//  1. Seed a baseline commit so the tracked path exists in HEAD with
//     before_oid resolvable to a real blob.
//  2. Hand-inject a blocked_conflict row whose ops describe the modify
//     before -> after for that path, error string classifies as
//     "before-state mismatch".
//  3. Land an EXTERNAL commit (raw git plumbing: write-tree + commit-tree +
//     update-ref) that contains exactly after_oid at the same path. This
//     advances HEAD without going through acd, so daemon sees its captured
//     intent already on disk.
//  4. Wake the session. probeBlockedSelfHeal must:
//     * promote the row state blocked_conflict -> published with
//     commit_oid = HEAD,
//     * append decision_records kind=handled_external_after_block,
//     * upsert publish_state singleton status=published,
//     * NOT mint a new commit (HEAD unchanged after settle).
func TestSelfHeal_BlockedPromotesToPublishedOnReplay(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Baseline commit: write "before" content so the captured before_oid
	// resolves to a real blob the self-heal probe will accept.
	target := filepath.Join(repo, "self-heal.txt")
	writeFile(t, target, "before\n")
	baseHead := gitCommitAll(t, repo, "self-heal baseline", "self-heal.txt")
	beforeOID := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD:self-heal.txt"))
	if beforeOID == "" {
		t.Fatalf("baseline before_oid empty")
	}

	// ACD_REWIND_GRACE_SECONDS=0 disables the same-branch rewind grace so the
	// external commit + wake here does not trigger a 60s replay pause that
	// could time-out the 8-second self-heal wait loop on slow CI runners.
	startSession(t, ctx, env, repo, "selfheal-blocked", "shell", "ACD_REWIND_GRACE_SECONDS=0")
	waitMode(t, repo, "running", 5*time.Second)
	dbPath := selfHealStateDB(repo)

	// Pause replay so seeding the row is not raced by an in-flight pass.
	pauseReplay(t, ctx, env, repo, "selfheal blocked seed")

	// Hand-craft an after blob (must not match before) and the captured
	// blocked_conflict row that points at it.
	afterOID := gitHashObjectStdin(t, repo, "after-external\n")
	if afterOID == beforeOID {
		t.Fatalf("after_oid equal to before_oid; pick distinct content")
	}
	gen := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'branch.generation'")
	if gen == "" {
		gen = "1"
	}
	now := nowFloatSeconds()

	// error='before-state mismatch' is the literal token classifyReplayIssue
	// recognises as replayErrorBeforeStateMismatch — required by
	// selfHealEligibleByClass.
	seedSQL := fmt.Sprintf(`
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state, error)
VALUES ('refs/heads/main', %s, '%s', 'modify', 'self-heal.txt', 'rescan', %f, 'blocked_conflict', 'modify before-state mismatch for self-heal.txt');
INSERT INTO capture_ops(event_seq, ord, op, path, before_oid, before_mode, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'modify', 'self-heal.txt', '%s', '100644', '%s', '100644', 'rescan');
`, gen, baseHead, now, beforeOID, afterOID)
	if out, err := exec.Command("sqlite3", dbPath, seedSQL).CombinedOutput(); err != nil {
		t.Fatalf("seed blocked row: %v\n%s", err, out)
	}
	blockedSeq := sqliteScalar(t, dbPath,
		"SELECT seq FROM capture_events WHERE path = 'self-heal.txt' AND state = 'blocked_conflict' ORDER BY seq DESC LIMIT 1")
	if blockedSeq == "" {
		t.Fatalf("seeded blocked row not found")
	}

	// Land the EXTERNAL commit that already contains after_oid for the
	// same path. Use raw plumbing so this is a true parallel publish, not a
	// daemon-mediated capture.
	externalHead := externalCommitAtPath(t, repo, baseHead, "self-heal.txt", afterOID, "external lands after")
	if externalHead == baseHead {
		t.Fatalf("external commit did not advance HEAD")
	}

	resumeReplay(t, ctx, env, repo)
	// Wake repeatedly: rewind grace may pause replay briefly. The probe runs
	// every replay tick so a few wakes are sufficient.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		wakeSession(t, ctx, env, repo, "selfheal-blocked")
		if latestEventState(t, dbPath, "self-heal.txt") == "published" {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}

	st := latestEventState(t, dbPath, "self-heal.txt")
	if st != "published" {
		dump, _ := exec.Command("sqlite3", dbPath,
			"SELECT seq,operation,path,state,error,commit_oid FROM capture_events WHERE path='self-heal.txt' ORDER BY seq").CombinedOutput()
		t.Fatalf("blocked row state=%q want published\nrows:\n%s", st, dump)
	}

	// commit_oid must equal HEAD; self-heal does NOT mint a new commit.
	publishedOID := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT commit_oid FROM capture_events WHERE seq = %s", blockedSeq))
	if publishedOID != externalHead {
		t.Fatalf("published commit_oid=%q want external HEAD %q", publishedOID, externalHead)
	}

	// decision_records: exactly one handled_external_after_block for this seq.
	decisionCount := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT COUNT(*) FROM decision_records WHERE event_seq = %s AND kind = 'handled_external_after_block'", blockedSeq))
	if decisionCount != "1" {
		dump, _ := exec.Command("sqlite3", dbPath,
			"SELECT id,kind,event_seq,reason FROM decision_records ORDER BY id").CombinedOutput()
		t.Fatalf("handled_external_after_block decision count=%s want 1\nrows:\n%s", decisionCount, dump)
	}

	// publish_state singleton flips to status='published'.
	pubStatus := sqliteScalar(t, dbPath, "SELECT status FROM publish_state WHERE id = 1")
	if pubStatus != "published" {
		dump, _ := exec.Command("sqlite3", dbPath,
			"SELECT id,status,event_seq,target_commit_oid,error FROM publish_state").CombinedOutput()
		t.Fatalf("publish_state.status=%q want published\nrows:\n%s", pubStatus, dump)
	}

	// HEAD must NOT advance past the external commit. Self-heal cannot mint
	// a fresh commit.
	headAfter := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	if headAfter != externalHead {
		log := runGitOK(t, repo, "log", "--oneline", "--decorate", "--all")
		t.Fatalf("HEAD=%s after self-heal; want unchanged external HEAD %s\nlog:\n%s",
			headAfter, externalHead, log)
	}

	// Commit count = seed + baseline + external. Self-heal must not have
	// minted an extra commit.
	if count := strings.TrimSpace(runGitOK(t, repo, "rev-list", "--count", "HEAD")); count != "3" {
		log := runGitOK(t, repo, "log", "--oneline", "--decorate", "--all")
		t.Fatalf("commit count=%s want 3 (seed + baseline + external)\nlog:\n%s", count, log)
	}
}

// externalCommitAtPath stages `path` with the supplied blob oid against
// parent, builds a tree + commit via plumbing, and fast-forwards HEAD to it.
// Returns the new HEAD oid. Used to simulate a parallel committer landing
// our captured after-state without going through the daemon.
func externalCommitAtPath(t *testing.T, repo, parent, path, blobOID, message string) string {
	t.Helper()
	// Seed a scratch index from parent's tree so unrelated paths survive.
	tmpIndex := filepath.Join(t.TempDir(), "external.index")
	envForIndex := []string{"GIT_INDEX_FILE=" + tmpIndex}
	runGitOKEnv(t, repo, envForIndex, "read-tree", parent)
	runGitOKEnv(t, repo, envForIndex,
		"update-index", "--add", "--cacheinfo", "100644,"+blobOID+","+path)
	tree := strings.TrimSpace(runGitOKEnv(t, repo, envForIndex, "write-tree"))
	if tree == "" {
		t.Fatalf("external write-tree produced empty oid")
	}
	commit := strings.TrimSpace(runGitOK(t, repo,
		"commit-tree", tree, "-p", parent, "-m", message))
	if commit == "" {
		t.Fatalf("external commit-tree produced empty oid")
	}
	runGitOK(t, repo, "update-ref", "refs/heads/main", commit, parent)
	return commit
}

// runGitOKEnv is runGitOK with extra env (GIT_INDEX_FILE for plumbing).
// extraEnv entries override the inherited environment because Cmd.Env
// evaluates later duplicates as overrides on macOS/Linux execve semantics.
func runGitOKEnv(t *testing.T, repo string, extraEnv []string, args ...string) string {
	t.Helper()
	full := append([]string{"-C", repo}, args...)
	cmd := exec.Command("git", full...)
	cmd.Env = append(os.Environ(), extraEnv...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}
