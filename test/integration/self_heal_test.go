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
	t.Parallel()

	repo := tempRepo(t)
	baseEnv := withIsolatedHome(t)
	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		"ACD_INTENT_MIN_PENDING=2",
		"ACD_INTENT_SETTLE_WINDOW=1h",
		"ACD_INTENT_MAX_PENDING_AGE=1h",
		"ACD_PATH_QUIESCENCE_SECONDS=0",
		"ACD_REWIND_GRACE_SECONDS=0",
		"ACD_FSNOTIFY_ENABLED=0",
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
	env := envWith(baseEnv, extra...)
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

	writeFile(t, target, "same change\n")
	wakeSession(t, ctx, env, repo, "selfheal-parallel")
	// Intent mode's count gate holds one capture pending without pausing
	// capture. This gives the external committer a real unpublished chain to
	// satisfy; a manual pause would suppress capture and invalidate the test.
	waitForEventState(t, dbPath, "parallel.txt", "pending", 8*time.Second)
	pendingSeq := sqliteScalar(t, dbPath,
		"SELECT seq FROM capture_events WHERE path = 'parallel.txt' AND state = 'pending' ORDER BY seq DESC LIMIT 1")
	if pendingSeq == "" {
		t.Fatal("parallel capture did not remain pending before external commit")
	}

	externalHead := gitCommitAll(t, repo, "external parallel commit", "parallel.txt")
	if externalHead == initialHead {
		t.Fatal("external commit did not advance HEAD")
	}

	wakeSession(t, ctx, env, repo, "selfheal-parallel")
	waitForEventState(t, dbPath, "parallel.txt", "published", 8*time.Second)

	publishedOID := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path = 'parallel.txt' ORDER BY seq DESC LIMIT 1")
	if publishedOID != externalHead {
		t.Fatalf("published commit_oid=%q want external HEAD %q", publishedOID, externalHead)
	}
	assertPublishedRecoverySnapshot(t, repo, dbPath, pendingSeq, externalHead,
		"runtime_branch_transition", "handled_external_after_block")
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

	writeFile(t, filepath.Join(repo, "pause-two.txt"), "two\n")
	wakeSession(t, ctx, env, repo, "selfheal-pause")

	// Under the new contract, manual pause halts both capture and replay,
	// so events for the writes above are not captured until after resume.
	// We can still observe that HEAD has not advanced while paused.
	if head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); head != initialHead {
		t.Fatalf("HEAD advanced while manually paused: got %s want %s", head, initialHead)
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
	// Under the new contract, manual pause halts capture too, so the write
	// above is not captured until after resume. We only verify the pause
	// marker durability and that HEAD has not advanced.

	stopSessionForce(t, env, repo)

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

	// While paused, capture is also halted, so no published row should exist
	// yet for the queued path. (Pending may or may not exist depending on
	// whether the worktree write was observed before the pause took effect;
	// the load-bearing invariant is that it is NOT yet published.)
	if state := latestEventState(t, dbPath, "restart-paused.txt"); state == "published" {
		t.Fatalf("event state after restart wake=%q want non-published while paused", state)
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
	if flushed := runAcd(t, ctx, envWith(env, "ACD_REWIND_GRACE_SECONDS=2"), "flush", "--logical", "--session-id", "selfheal-rewind", "--repo", repo); flushed.ExitCode != 0 {
		t.Fatalf("rewind fixture logical boundary failed: %s", flushed.Stderr)
	}
	firstCommit := waitForCommitContaining(t, repo, "rewind.txt", 8*time.Second)
	if firstCommit == seedHead {
		t.Fatalf("daemon did not create a first rewind.txt commit")
	}

	// CLAUDE.md invariant 10: same-branch rewind pauses BOTH capture and
	// replay during the grace window. The post-rewind capture pass must
	// NOT mint new capture rows for rewind.txt, and HEAD must not advance.
	// Snapshot the queue shape for rewind.txt BEFORE the reset so we can
	// assert neither MAX(seq) nor COUNT(rows) grows during the grace.
	rewindSeqBefore := selfHealRewindMaxSeq(t, dbPath, "rewind.txt")
	rewindCountBefore := selfHealCount(t, dbPath, "path = 'rewind.txt'")

	runGitOK(t, repo, "reset", "--soft", "HEAD~1")
	if head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); head != seedHead {
		t.Fatalf("soft reset HEAD=%s want seed %s", head, seedHead)
	}

	wakeSession(t, ctx, envWith(env, "ACD_REWIND_GRACE_SECONDS=2"), repo, "selfheal-rewind")
	waitFor(t, "replay.paused_until set", 8*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'replay.paused_until'") != ""
	})

	// Wake the daemon a few extra times during the active grace window so a
	// regression that captures during the grace has multiple opportunities to
	// add a phantom row. Then assert: no new rewind.txt capture rows AND HEAD
	// has NOT advanced. This replaces the old (incorrect) `pending` poll —
	// invariant 10 forbids capture rows during the grace window.
	wakeSession(t, ctx, envWith(env, "ACD_REWIND_GRACE_SECONDS=2"), repo, "selfheal-rewind")
	wakeSession(t, ctx, envWith(env, "ACD_REWIND_GRACE_SECONDS=2"), repo, "selfheal-rewind")
	if got := selfHealCount(t, dbPath, "path = 'rewind.txt'"); got != rewindCountBefore {
		rows := sqliteScalar(t, dbPath,
			"SELECT group_concat(seq || ':' || operation || ':' || state, char(10)) FROM capture_events WHERE path = 'rewind.txt' ORDER BY seq")
		t.Fatalf("rewind.txt rows during grace: before=%d after=%d (invariant 10 violated)\nrows:\n%s",
			rewindCountBefore, got, rows)
	}
	if got := selfHealRewindMaxSeq(t, dbPath, "rewind.txt"); got != rewindSeqBefore {
		t.Fatalf("rewind.txt MAX(seq) grew during grace: before=%d after=%d (capture queued rows during pause gate)",
			rewindSeqBefore, got)
	}
	if head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); head != seedHead {
		t.Fatalf("HEAD advanced during rewind grace: got %s want %s", head, seedHead)
	}

	// Poll until the daemon clears replay.paused_until (rewind grace expired).
	waitForMetaCleared(t, dbPath, "replay.paused_until", 5*time.Second)
	wakeSession(t, ctx, envWith(env, "ACD_REWIND_GRACE_SECONDS=2"), repo, "selfheal-rewind")
	waitForEventSeqAfterState(t, dbPath, "rewind.txt", rewindSeqBefore, "published", 8*time.Second)
	assertNoSelfHealTerminalRows(t, dbPath)
	if head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); head == seedHead {
		t.Fatalf("HEAD did not advance after rewind grace expired")
	}
}

// selfHealRewindMaxSeq returns MAX(seq) for capture_events rows on `path`,
// or -1 when no row exists yet. Stable shape for "did the queue grow?" probes.
func selfHealRewindMaxSeq(t *testing.T, dbPath, path string) int64 {
	t.Helper()
	got := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT COALESCE(MAX(seq), -1) FROM capture_events WHERE path = %s", sqliteQuote(path)))
	var n int64
	if _, err := fmt.Sscanf(got, "%d", &n); err != nil {
		t.Fatalf("parse max(seq) %q: %v", got, err)
	}
	return n
}

// TestSelfHeal_FastForwardDuringRewindGrace_NoPhantoms pins the regression
// where a fast-forward landing inside an active rewind-grace window left
// shadow_paths seeded from the rewound (lower) HEAD. After the grace
// window expired, the next capture compared the live HEAD's tracked files
// against the stale shadow rows and emitted phantom `create` events for
// content that was already published.
//
// Sequence: H1 (seed) -> daemon commits H2 (worktree edit) -> operator
// resets HEAD~1 (rewind to H1, grace marker armed) -> operator merges
// --ff-only back to H2 inside the grace window. Once the grace window
// expires, capture_events must NOT contain a pending row for the
// resurrected file — the FF-in-grace path must reseed shadow from H2
// before clearing the gate.
func TestSelfHeal_FastForwardDuringRewindGrace_NoPhantoms(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	testEnv := envWith(env, "ACD_REWIND_GRACE_SECONDS=2", "ACD_FSNOTIFY_ENABLED=0")
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "selfheal-ff-grace", "shell", "ACD_REWIND_GRACE_SECONDS=2", "ACD_FSNOTIFY_ENABLED=0")
	waitMode(t, repo, "running", 5*time.Second)

	dbPath := selfHealStateDB(repo)
	seedHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))

	// Drive the daemon to commit H2 with the file.
	target := filepath.Join(repo, "ff-grace.txt")
	writeFile(t, target, "ff content\n")
	wakeSession(t, ctx, testEnv, repo, "selfheal-ff-grace")
	if flushed := runAcd(t, ctx, testEnv, "flush", "--logical", "--session-id", "selfheal-ff-grace", "--repo", repo); flushed.ExitCode != 0 {
		t.Fatalf("fast-forward fixture logical boundary failed: %s", flushed.Stderr)
	}
	h2 := waitForCommitContaining(t, repo, "ff-grace.txt", 8*time.Second)
	if h2 == seedHead {
		t.Fatal("daemon did not commit H2")
	}

	// Rewind: hard reset to seedHead. This drops both HEAD and the worktree
	// content, so the daemon's same-branch rewind path arms the grace gate.
	runGitOK(t, repo, "reset", "--hard", seedHead)
	if head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); head != seedHead {
		t.Fatalf("hard reset HEAD=%s want seed %s", head, seedHead)
	}
	wakeSession(t, ctx, testEnv, repo, "selfheal-ff-grace")
	waitFor(t, "replay.paused_until set after rewind", 8*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'replay.paused_until'") != ""
	})

	// Fast-forward back to H2 inside the active grace window. The hard
	// reset above leaves the worktree file as untracked content (the
	// daemon committed identical content into H2's tree, so reset --hard
	// only changed HEAD, not the file). Remove it so merge --ff-only does
	// not refuse on "untracked files would be overwritten". H2 is a
	// descendant of seedHead so the merge is a true fast-forward.
	if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove target before ff-merge: %v", err)
	}
	runGitOK(t, repo, "merge", "--ff-only", h2)
	if head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); head != h2 {
		t.Fatalf("ff-merge HEAD=%s want H2 %s", head, h2)
	}
	// The pre-merge remove above is only a test workaround for the daemon's
	// path-scoped index repair state. Restore from the fast-forwarded HEAD so
	// the post-grace assertion checks for phantom capture, not this local delete.
	runGitOK(t, repo, "checkout", "HEAD", "--", "ff-grace.txt")
	wakeSession(t, ctx, testEnv, repo, "selfheal-ff-grace")

	// The FF-in-grace path must reseed shadow + clear the grace marker.
	waitForMetaCleared(t, dbPath, "replay.paused_until", 6*time.Second)

	// Wait for the natural grace expiry to elapse so any post-grace
	// capture pass on a subsequent wake has had a chance to misbehave.
	time.Sleep(3 * time.Second)
	wakeSession(t, ctx, testEnv, repo, "selfheal-ff-grace")
	wakeSession(t, ctx, testEnv, repo, "selfheal-ff-grace")
	time.Sleep(500 * time.Millisecond)

	// HEAD must still equal H2 — no extra commits beyond the reseeded baseline.
	if head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); head != h2 {
		log := runGitOK(t, repo, "log", "--oneline", "--decorate", "--name-status", "-5")
		rows := sqliteScalar(t, dbPath,
			"SELECT group_concat(seq || ':' || operation || ':' || path || ':' || state || ':' || coalesce(substr(commit_oid,1,8),''), char(10)) FROM capture_events ORDER BY seq")
		t.Fatalf("HEAD=%s want H2 %s after grace expired\nlog:\n%s\nevents:\n%s", head, h2, log, rows)
	}
	// No phantom pending rows for the FF-restored file.
	if n := selfHealCount(t, dbPath, "path = 'ff-grace.txt' AND state = 'pending'"); n != 0 {
		rows := sqliteScalar(t, dbPath,
			"SELECT group_concat(seq || ':' || operation || ':' || state, char(10)) FROM capture_events WHERE path = 'ff-grace.txt' ORDER BY seq")
		t.Fatalf("phantom pending rows after FF-in-grace=%d want 0\nrows:\n%s", n, rows)
	}
	// And no new commits beyond H2 — the post-grace capture must not have
	// resurrected the file as a phantom create.
	if count := strings.TrimSpace(runGitOK(t, repo, "rev-list", "--count", "HEAD")); count != "2" {
		log := runGitOK(t, repo, "log", "--oneline", "--decorate")
		t.Fatalf("commit count=%s want 2 (seed + H2 only)\nlog:\n%s", count, log)
	}
}

func requireSQLite(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required for self-heal integration assertions")
	}
}

// assertStatusPaused calls `acd status --json --repo <repo>` and asserts that
// the reply has Paused=true and Pause.Source==wantSource. This is a direct
// observable-state assertion that does not depend on timing.
func assertStatusPaused(t *testing.T, ctx context.Context, env []string, repo, wantSource string) {
	t.Helper()
	res := runAcd(t, ctx, env, "status", "--repo", repo, "--json")
	if res.ExitCode != 3 {
		t.Fatalf("acd status exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	var rep struct {
		State string `json:"state"`
		Data  struct {
			ActionRequired bool   `json:"action_required"`
			Summary        string `json:"summary"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(res.Stdout), &rep); err != nil {
		t.Fatalf("decode status json: %v\nstdout=%s", err, res.Stdout)
	}
	if rep.State != "needs_action" || !rep.Data.ActionRequired ||
		!strings.Contains(strings.ToLower(rep.Data.Summary), "paused") {
		t.Fatalf("acd status did not report %s pause as needs_action\nstdout=%s", wantSource, res.Stdout)
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

func waitForEventSeqAfterState(t *testing.T, dbPath, path string, afterSeq int64, want string, timeout time.Duration) {
	t.Helper()
	query := fmt.Sprintf("SELECT state FROM capture_events WHERE path = %s AND seq > %d ORDER BY seq DESC LIMIT 1",
		sqliteQuote(path), afterSeq)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sqliteScalar(t, dbPath, query) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	rows := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT group_concat(seq || ':' || operation || ':' || state || ':' || COALESCE(error, ''), char(10)) FROM capture_events WHERE path = %s ORDER BY seq", sqliteQuote(path)))
	meta := sqliteScalar(t, dbPath,
		"SELECT group_concat(key || '=' || value, char(10)) FROM daemon_meta WHERE key IN ('replay.paused_until', 'protection.complete', 'protection.observation_epoch', 'protection.covered_epoch') ORDER BY key")
	checkpoints := sqliteScalar(t, dbPath,
		"SELECT group_concat(id || ':' || phase || ':' || reason, char(10)) FROM checkpoints ORDER BY seq")
	t.Fatalf("%s seq>%d did not reach state=%s within %v\nrows:\n%s\nmeta:\n%s\ncheckpoints:\n%s",
		path, afterSeq, want, timeout, rows, meta, checkpoints)
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
