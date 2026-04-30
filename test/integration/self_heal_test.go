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

func TestSelfHeal_ParallelCommitterDoesNotBlock(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	target := filepath.Join(repo, "parallel.txt")
	writeFile(t, target, "before\n")
	baselineHead := gitCommitAll(t, repo, "baseline parallel file", "parallel.txt")

	startSession(t, ctx, env, repo, "selfheal-parallel", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	dbPath := selfHealStateDB(repo)
	initialHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	if initialHead != baselineHead {
		t.Fatalf("initial HEAD=%s want baseline %s", initialHead, baselineHead)
	}

	pauseReplay(t, ctx, env, repo, "parallel committer test")
	writeFile(t, target, "same change\n")
	wakeSession(t, ctx, env, repo, "selfheal-parallel")
	waitForEventState(t, dbPath, "parallel.txt", "pending", 8*time.Second)

	externalHead := gitCommitAll(t, repo, "external parallel commit", "parallel.txt")
	if externalHead == initialHead {
		t.Fatal("external commit did not advance HEAD")
	}

	resumeReplay(t, ctx, env, repo)
	wakeSession(t, ctx, env, repo, "selfheal-parallel")
	waitForEventState(t, dbPath, "parallel.txt", "published", 8*time.Second)

	publishedOID := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path = 'parallel.txt' ORDER BY seq DESC LIMIT 1")
	if publishedOID != externalHead {
		t.Fatalf("published commit_oid=%q want external HEAD %q", publishedOID, externalHead)
	}
	assertNoSelfHealTerminalRows(t, dbPath)

	head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	if head != externalHead {
		t.Fatalf("HEAD=%s want unchanged external commit %s", head, externalHead)
	}
	if count := strings.TrimSpace(runGitOK(t, repo, "rev-list", "--count", "HEAD")); count != "3" {
		log := runGitOK(t, repo, "log", "--oneline", "--decorate")
		t.Fatalf("commit count=%s want 3 (seed + baseline + external only)\nlog:\n%s", count, log)
	}
}

func TestSelfHeal_ManualPauseAndResume(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "selfheal-pause", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	dbPath := selfHealStateDB(repo)
	initialHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	pauseReplay(t, ctx, env, repo, "manual pause integration")

	writeFile(t, filepath.Join(repo, "pause-one.txt"), "one\n")
	wakeSession(t, ctx, env, repo, "selfheal-pause")
	waitForEventState(t, dbPath, "pause-one.txt", "pending", 8*time.Second)

	writeFile(t, filepath.Join(repo, "pause-two.txt"), "two\n")
	wakeSession(t, ctx, env, repo, "selfheal-pause")
	waitForEventState(t, dbPath, "pause-two.txt", "pending", 8*time.Second)

	if head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); head != initialHead {
		t.Fatalf("HEAD advanced while manually paused: got %s want %s", head, initialHead)
	}
	if pending := selfHealCount(t, dbPath, "state = 'pending'"); pending < 2 {
		t.Fatalf("pending events=%d want at least 2", pending)
	}

	resumeReplay(t, ctx, env, repo)
	wakeSession(t, ctx, env, repo, "selfheal-pause")
	waitForEventState(t, dbPath, "pause-one.txt", "published", 8*time.Second)
	waitForEventState(t, dbPath, "pause-two.txt", "published", 8*time.Second)
	assertNoSelfHealTerminalRows(t, dbPath)
	assertPublishedOrder(t, repo, dbPath, []string{"pause-one.txt", "pause-two.txt"})
}

func TestSelfHeal_PauseSurvivesDaemonRestart(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "selfheal-restart-a", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	dbPath := selfHealStateDB(repo)
	initialHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	pauseReplay(t, ctx, env, repo, "restart durability")

	writeFile(t, filepath.Join(repo, "restart-paused.txt"), "queued before restart\n")
	wakeSession(t, ctx, env, repo, "selfheal-restart-a")
	waitForEventState(t, dbPath, "restart-paused.txt", "pending", 8*time.Second)

	stopSessionForce(t, env, repo)
	waitMode(t, repo, "stopped", 5*time.Second)

	startSession(t, ctx, env, repo, "selfheal-restart-b", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	wakeSession(t, ctx, env, repo, "selfheal-restart-b")

	// Positive assertion 1: the manual pause marker file must survive the restart.
	markerPath := filepath.Join(repo, ".git", "acd", "paused")
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("pause marker file missing after daemon restart: %v", err)
	}

	// Positive assertion 2: acd status --json must report Paused=true, Source=manual.
	assertStatusPaused(t, ctx, env, repo, "manual")

	// The queued event must still be pending (replay is blocked by the marker).
	if state := latestEventState(t, dbPath, "restart-paused.txt"); state != "pending" {
		t.Fatalf("event state after restart wake=%q want pending", state)
	}
	if head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); head != initialHead {
		t.Fatalf("HEAD advanced while restart pause marker was active: got %s want %s", head, initialHead)
	}

	resumeReplay(t, ctx, env, repo)
	wakeSession(t, ctx, env, repo, "selfheal-restart-b")
	waitForEventState(t, dbPath, "restart-paused.txt", "published", 8*time.Second)
	assertNoSelfHealTerminalRows(t, dbPath)
}

func TestSelfHeal_RewindGracePausesReplay(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "selfheal-rewind", "shell", "ACD_REWIND_GRACE_SECONDS=2")
	waitMode(t, repo, "running", 5*time.Second)

	dbPath := selfHealStateDB(repo)
	seedHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))

	writeFile(t, filepath.Join(repo, "rewind.txt"), "before rewind\n")
	wakeSession(t, ctx, envWith(env, "ACD_REWIND_GRACE_SECONDS=2"), repo, "selfheal-rewind")
	firstCommit := waitForCommitContaining(t, repo, "rewind.txt", 8*time.Second)
	if firstCommit == seedHead {
		t.Fatalf("daemon did not create a first rewind.txt commit")
	}

	runGitOK(t, repo, "reset", "--soft", "HEAD~1")
	if head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); head != seedHead {
		t.Fatalf("soft reset HEAD=%s want seed %s", head, seedHead)
	}

	wakeSession(t, ctx, envWith(env, "ACD_REWIND_GRACE_SECONDS=2"), repo, "selfheal-rewind")
	waitFor(t, "replay.paused_until set", 8*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'replay.paused_until'") != ""
	})
	waitForEventState(t, dbPath, "rewind.txt", "pending", 8*time.Second)
	if head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); head != seedHead {
		t.Fatalf("HEAD advanced during rewind grace: got %s want %s", head, seedHead)
	}

	time.Sleep(2500 * time.Millisecond)
	wakeSession(t, ctx, envWith(env, "ACD_REWIND_GRACE_SECONDS=2"), repo, "selfheal-rewind")
	waitForEventState(t, dbPath, "rewind.txt", "published", 8*time.Second)
	assertNoSelfHealTerminalRows(t, dbPath)
	if head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); head == seedHead {
		t.Fatalf("HEAD did not advance after rewind grace expired")
	}
}

func requireSQLite(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required for self-heal integration assertions")
	}
}

func selfHealStateDB(repo string) string {
	return filepath.Join(repo, ".git", "acd", "state.db")
}

func pauseReplay(t *testing.T, ctx context.Context, env []string, repo, reason string) {
	t.Helper()
	res := runAcd(t, ctx, env,
		"pause", "--repo", repo, "--reason", reason, "--yes", "--json",
	)
	if res.ExitCode != 0 {
		t.Fatalf("acd pause exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
}

func resumeReplay(t *testing.T, ctx context.Context, env []string, repo string) {
	t.Helper()
	res := runAcd(t, ctx, env,
		"resume", "--repo", repo, "--yes", "--json",
	)
	if res.ExitCode != 0 {
		t.Fatalf("acd resume exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
}

func waitForEventState(t *testing.T, dbPath, path, want string, timeout time.Duration) {
	t.Helper()
	waitFor(t, fmt.Sprintf("%s state=%s", path, want), timeout, func() bool {
		return latestEventState(t, dbPath, path) == want
	})
}

func latestEventState(t *testing.T, dbPath, path string) string {
	t.Helper()
	return sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT state FROM capture_events WHERE path = %s ORDER BY seq DESC LIMIT 1", sqliteQuote(path)))
}

func selfHealCount(t *testing.T, dbPath, where string) int {
	t.Helper()
	var n int
	got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM capture_events WHERE "+where)
	if _, err := fmt.Sscanf(got, "%d", &n); err != nil {
		t.Fatalf("parse count %q: %v", got, err)
	}
	return n
}

func assertNoSelfHealTerminalRows(t *testing.T, dbPath string) {
	t.Helper()
	if n := selfHealCount(t, dbPath, "state IN ('blocked_conflict', 'failed')"); n != 0 {
		rows := sqliteScalar(t, dbPath,
			"SELECT group_concat(seq || ':' || path || ':' || state, char(10)) FROM capture_events WHERE state IN ('blocked_conflict', 'failed') ORDER BY seq")
		t.Fatalf("terminal failed/blocked rows=%d want 0\n%s", n, rows)
	}
}

func assertPublishedOrder(t *testing.T, repo, dbPath string, paths []string) {
	t.Helper()
	prevCommit := ""
	for _, path := range paths {
		commit := sqliteScalar(t, dbPath,
			fmt.Sprintf("SELECT commit_oid FROM capture_events WHERE path = %s AND state = 'published' ORDER BY seq DESC LIMIT 1", sqliteQuote(path)))
		if commit == "" {
			t.Fatalf("%s has no published commit_oid", path)
		}
		if _, err := runGit(repo, "cat-file", "-e", commit+"^{commit}"); err != nil {
			t.Fatalf("%s commit_oid %s is not a commit: %v", path, commit, err)
		}
		if prevCommit != "" {
			if _, err := runGit(repo, "merge-base", "--is-ancestor", prevCommit, commit); err != nil {
				log := runGitOK(t, repo, "log", "--oneline", "--decorate", "--all")
				t.Fatalf("commit for %s (%s) is not after previous event commit %s\nlog:\n%s", path, commit, prevCommit, log)
			}
		}
		prevCommit = commit
	}
	head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	if head != prevCommit {
		t.Fatalf("HEAD=%s want last published commit %s", head, prevCommit)
	}
}

func sqliteQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
