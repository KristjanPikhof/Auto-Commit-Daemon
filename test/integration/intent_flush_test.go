//go:build integration
// +build integration

package integration_test

// intent_flush_test.go — verification-lane Wave 3 acceptance tests for the
// d1 (logical flush) and b2 (per-path quiescence) outcomes:
//
//   - acd flush --logical against a live daemon publishes the visible window
//     within ~2s even when IntentMinPending has not been reached. The first
//     half of this contract lives in flush_logical_test.go (which the d1
//     lane authored). This file asserts a complementary check: flush
//     --logical with the deterministic provider lands a single deterministic
//     commit so the bypass works without any AI provider configured.
//
//   - With ACD_PATH_QUIESCENCE_SECONDS=N and two same-path writes <N apart,
//     the second write extends the quiet window. The capture rows persist
//     for durability immediately, but the planner-visible offer (and the
//     resulting commit) only surfaces after N seconds of silence. The
//     daemon also stamps path_quiescence.gated_count in daemon_meta so
//     `acd status` can show the held-back count — we read the same
//     daemon_meta key here to prove the gate fired.
//
// Companion daemon-package coverage:
//   - test/integration/flush_logical_test.go (intent strategy budget assertion)
//   - internal/daemon/replay_test.go TestReplay_PathQuiescenceGateDefersOfferUntilWindowElapses
//   - internal/cli/intent_observability_test.go TestStatus_PathQuiescenceGatedCountAdjustsVisiblePending

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestFlush_LogicalCommitsSingleEditWithUnavailableProvider exercises the
// d1 bypass on Intent Fast with an unavailable local provider. A single edit
// followed by `acd flush
// --logical` must land within the 2s budget the d1 spec asserts. This
// catches a regression where the bypass plumbing depends on a network
// provider being configured (it must not).
func TestFlush_LogicalCommitsSingleEditWithUnavailableProvider(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	bin := buildAcdBinary(t)
	binDir := filepath.Dir(bin)

	repo := tempRepo(t)
	sessionID := "intent-flush-deterministic"
	env := adapterEnv(t, binDir, "CLAUDE_PROJECT_DIR="+repo)
	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_MIN_PENDING=10",
		"ACD_INTENT_MAX_PENDING_AGE=5m",
		"ACD_INTENT_WINDOW=10",
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
	env = envWith(env, extra...)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	startRes := runAcd(t, ctx, env,
		"start", "--repo", repo,
		"--session-id", sessionID,
		"--harness", "claude-code",
	)
	if startRes.ExitCode != 0 {
		t.Fatalf("acd start exit=%d\nstdout=%s\nstderr=%s",
			startRes.ExitCode, startRes.Stdout, startRes.Stderr)
	}
	t.Cleanup(func() { shutdownDaemon(t, env, repo, sessionID) })
	waitFor(t, "daemon mode==running", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	assertIntentV2RuntimeActive(t, repo)

	headBefore := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	target := filepath.Join(repo, "deterministic-flush.txt")
	writeFile(t, target, "flush me\n")

	// Wake first so capture observes the file before the flush. flush
	// --logical only forces the planner past the count gate; it does not
	// itself create capture rows.
	wakeRes := runAcd(t, ctx, env, "wake",
		"--repo", repo, "--session-id", sessionID,
	)
	if wakeRes.ExitCode != 0 {
		t.Fatalf("acd wake exit=%d\nstdout=%s\nstderr=%s",
			wakeRes.ExitCode, wakeRes.Stdout, wakeRes.Stderr)
	}
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	waitForEventState(t, dbPath, "deterministic-flush.txt", "pending", 5*time.Second)
	if headAfterWake := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); headAfterWake != headBefore {
		t.Fatalf("wake-only drain bypassed intent batch gate: HEAD=%s want %s", headAfterWake, headBefore)
	}

	flushStart := time.Now()
	flushRes := runAcd(t, ctx, env, "flush",
		"--repo", repo, "--session-id", sessionID, "--logical",
	)
	if flushRes.ExitCode != 0 {
		t.Fatalf("acd flush --logical exit=%d\nstdout=%s\nstderr=%s",
			flushRes.ExitCode, flushRes.Stdout, flushRes.Stderr)
	}

	deadline := flushStart.Add(2 * time.Second)
	advanced := false
	for time.Now().Before(deadline) {
		head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
		if head != "" && head != headBefore {
			advanced = true
			t.Logf("HEAD advanced after flush --logical in %s: %s -> %s",
				time.Since(flushStart), headBefore[:12], head[:12])
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !advanced {
		t.Fatalf("flush --logical did not advance HEAD within 2s\nbefore=%s\nstill=%s\nflush stdout=%s\nflush stderr=%s",
			headBefore,
			strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")),
			flushRes.Stdout, flushRes.Stderr)
	}

	// One commit, deterministic subject.
	subj := headSubject(t, repo)
	if subj != "Add deterministic-flush.txt" {
		t.Fatalf("HEAD subject=%q want %q (deterministic provider must produce Add <basename>)",
			subj, "Add deterministic-flush.txt")
	}
}

// TestPathQuiescence_TwoSavesWithinWindowBecomeOneCapture asserts the b2
// quiescence opt-in: with ACD_PATH_QUIESCENCE_SECONDS=2, two writes 500ms
// apart on the same path are gated until the per-path quiet window
// elapses. The capture rows themselves are durable immediately (the gate
// is on planner offer, not on capture append). After the quiet window
// expires plus one wake, the daemon publishes a single commit covering
// both writes.
//
// Mid-test, daemon_meta.path_quiescence.gated_count must report >=1 to
// prove the gate actually held back the offer.
func TestPathQuiescence_TwoSavesWithinWindowBecomeOneCapture(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	bin := buildAcdBinary(t)
	binDir := filepath.Dir(bin)

	repo := tempRepo(t)
	sessionID := "intent-quiescence"

	// Seed the file under version control so the second write captures as
	// a modify (not a create-then-modify) — the quiescence gate keys on
	// path-touch recency regardless of op kind, but consistent ops keep
	// the assertions about commit count clean.
	target := filepath.Join(repo, "quiet.txt")
	writeFile(t, target, "v0\n")
	gitCommitAll(t, repo, "seed quiet.txt", "quiet.txt")

	env := adapterEnv(t, binDir, "CLAUDE_PROJECT_DIR="+repo)
	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		"ACD_INTENT_MIN_PENDING=1",
		"ACD_INTENT_SETTLE_WINDOW=0",
		"ACD_INTENT_MAX_PENDING_AGE=5m",
		"ACD_INTENT_PATH_COALESCE=1",
		"ACD_PATH_QUIESCENCE_SECONDS=2",
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
	env = envWith(env, extra...)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	startRes := runAcd(t, ctx, env,
		"start", "--repo", repo,
		"--session-id", sessionID,
		"--harness", "claude-code",
	)
	if startRes.ExitCode != 0 {
		t.Fatalf("acd start exit=%d\nstdout=%s\nstderr=%s",
			startRes.ExitCode, startRes.Stdout, startRes.Stderr)
	}
	t.Cleanup(func() { shutdownDaemon(t, env, repo, sessionID) })
	waitFor(t, "daemon mode==running", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})

	headBefore := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")

	// Two writes 500ms apart. Each write must register as a capture (we
	// wake between them so the daemon observes each transition); the
	// quiescence gate must hold the planner offer until both are quiet
	// for >= 2s.
	writeFile(t, target, "v1\n")
	wakeSession(t, ctx, env, repo, sessionID)
	waitFor(t, "first capture pending", 5*time.Second, func() bool {
		return sqliteScalar(t, dbPath,
			"SELECT COUNT(*) FROM capture_events WHERE path='quiet.txt' AND state='pending'") == "1"
	})
	time.Sleep(500 * time.Millisecond)
	writeFile(t, target, "v2\n")
	wakeSession(t, ctx, env, repo, sessionID)
	waitFor(t, "second capture pending", 5*time.Second, func() bool {
		return sqliteScalar(t, dbPath,
			"SELECT COUNT(*) FROM capture_events WHERE path='quiet.txt' AND state='pending'") == "2"
	})

	// HEAD must NOT advance yet: the quiescence gate (2s) holds the
	// planner offer. Wait ~1s of additional silence to let a replay tick
	// fire and the gate stamp daemon_meta.
	time.Sleep(1200 * time.Millisecond)
	wakeSession(t, ctx, env, repo, sessionID)
	time.Sleep(300 * time.Millisecond)

	// HEAD still must equal headBefore — the planner has not been offered
	// the gated captures yet because <2s have elapsed since the second
	// write.
	intermediate := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	if intermediate != headBefore {
		t.Fatalf("HEAD advanced too early: was %s now %s; quiescence gate should still be holding offers (only ~1.5s elapsed)",
			headBefore[:12], intermediate[:12])
	}
	// daemon_meta gated_count must be >= 1.
	gated := sqliteScalar(t, dbPath,
		"SELECT value FROM daemon_meta WHERE key='path_quiescence.gated_count'")
	if gated == "" || gated == "0" {
		t.Fatalf("daemon_meta.path_quiescence.gated_count=%q want >=1 (gate must report held-back captures)", gated)
	}

	// Sleep past the 2s window relative to the LAST write, then wake.
	// Total elapsed since v2 write: ~1.5s + 1.2s sleep below = ~2.7s.
	time.Sleep(1200 * time.Millisecond)
	wakeSession(t, ctx, env, repo, sessionID)

	// Now the planner can see the captures and publish.
	startCount := commitCount(t, repo)
	_ = startCount
	waitFor(t, "HEAD advances after quiescence elapses", 10*time.Second, func() bool {
		head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
		return head != headBefore && head != ""
	})

	// Both captures must be published, sharing one commit_oid (the planner
	// saw a single coalesced offer). With deterministic provider the
	// daemon's deterministic-coalesce path also produces one commit.
	waitForEventState(t, dbPath, "quiet.txt", "published", 10*time.Second)
	distinct := sqliteScalar(t, dbPath,
		"SELECT COUNT(DISTINCT commit_oid) FROM capture_events WHERE path='quiet.txt' AND state='published'")
	if distinct != "1" {
		t.Fatalf("distinct commit_oid for quiet.txt published rows=%s want 1 (two saves must surface as one window)",
			distinct)
	}
	rows := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM capture_events WHERE path='quiet.txt' AND state='published'")
	if rows != "2" {
		t.Fatalf("published rows for quiet.txt=%s want 2", rows)
	}
	members := sqliteScalar(t, dbPath, `
SELECT COUNT(*)
FROM intent_candidate_events member
JOIN capture_events capture ON capture.seq=member.event_seq
WHERE capture.path='quiet.txt' AND member.membership_state='active'`)
	if members != "2" {
		t.Fatalf("active candidate members for quiet.txt=%s want 2", members)
	}
	coalesced := sqliteScalar(t, dbPath, `
SELECT COUNT(*)
FROM intent_candidate_events member
JOIN capture_events capture ON capture.seq=member.event_seq
WHERE capture.path='quiet.txt' AND member.event_role='coalesced'
  AND member.membership_state='active'`)
	if coalesced != "1" {
		t.Fatalf("coalesced candidate members for quiet.txt=%s want 1", coalesced)
	}
}
