//go:build integration
// +build integration

package integration_test

// flush_logical_test.go — d1 acceptance: an `acd flush --logical` invocation
// against a live daemon must publish a commit promptly (well under the 5m
// IntentMaxPendingAge default), proving that the flush request bypasses the
// IntentMinPending gate via the daemon's existing IntentBypassBatchWait
// path. The 2s budget below is the d1 task's stated acceptance criterion.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestFlush_LogicalCommitsWithin2s spins a real daemon under a claude-code
// adapter env, edits one file (which is below the default IntentMinPending
// threshold of 10), fires `acd flush --logical`, and asserts HEAD advances
// within 2 seconds. The whole point of the d1 rewire is that idle/Stop
// hooks should not have to wait for IntentMaxPendingAge to commit partial
// work — flush --logical signals "treat the visible window as age-trigger-now"
// to the daemon, which forces a commit on the next replay tick.
func TestFlush_LogicalCommitsWithin2s(t *testing.T) {
	bin := buildAcdBinary(t)
	binDir := filepath.Dir(bin)

	repo := tempRepo(t)
	sessionID := "e2e-flush-logical"
	env := adapterEnv(t, binDir, "CLAUDE_PROJECT_DIR="+repo)
	// Force intent strategy with a min_pending high enough that a single
	// edit cannot trip the count gate — the only path to a commit is via
	// the bypass that flush --logical enqueues.
	env = envWith(env,
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_MIN_PENDING=10",
		"ACD_INTENT_MAX_PENDING_AGE=5m",
		"ACD_INTENT_WINDOW=10",
	)

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
	t.Cleanup(func() {
		shutdownDaemon(t, env, repo, sessionID)
	})

	waitFor(t, "daemon mode==running", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	headBefore := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))

	// Drop a file change under the threshold. A single edit cannot reach
	// IntentMinPending=10, so without the flush bypass the daemon would
	// wait the full IntentMaxPendingAge (5m) before publishing.
	target := filepath.Join(repo, "flush-target.txt")
	if err := os.WriteFile(target, []byte("flush me\n"), 0o644); err != nil {
		t.Fatalf("write target file: %v", err)
	}

	// Wake the daemon so capture observes the new file before we ask it
	// to flush. Without an explicit wake the next poll tick handles capture
	// on its own; we want the test budget tight so we drive both edges.
	wakeRes := runAcd(t, ctx, env, "wake",
		"--repo", repo, "--session-id", sessionID,
	)
	if wakeRes.ExitCode != 0 {
		t.Fatalf("acd wake exit=%d\nstdout=%s\nstderr=%s",
			wakeRes.ExitCode, wakeRes.Stdout, wakeRes.Stderr)
	}

	flushStart := time.Now()
	flushRes := runAcd(t, ctx, env, "flush",
		"--repo", repo, "--session-id", sessionID,
		"--logical",
	)
	if flushRes.ExitCode != 0 {
		t.Fatalf("acd flush --logical exit=%d\nstdout=%s\nstderr=%s",
			flushRes.ExitCode, flushRes.Stdout, flushRes.Stderr)
	}

	// Wait for HEAD to advance. The 2s budget is intentionally tight so a
	// regression that drops the IntentBypassBatchWait wiring (or the new
	// flush_logical command label that triggers it) surfaces here. The
	// daemon's poll interval and replay cycle complete well under that
	// when bypass is in effect; if the bypass falls back to the
	// MinPending count gate, this would time out at the IntentMaxPendingAge
	// boundary instead.
	deadline := flushStart.Add(2 * time.Second)
	advanced := false
	for time.Now().Before(deadline) {
		head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
		if head != headBefore && head != "" {
			advanced = true
			elapsed := time.Since(flushStart)
			t.Logf("HEAD advanced after flush --logical in %s: %s -> %s", elapsed, headBefore[:12], head[:12])
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !advanced {
		dbPath := filepath.Join(repo, ".git", "acd", "state.db")
		t.Fatalf("flush --logical did not advance HEAD within 2s\nbefore=%s\nstill=%s\nflush stdout=%s\nflush stderr=%s\nstate.db=%s",
			headBefore,
			strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")),
			flushRes.Stdout, flushRes.Stderr, dbPath)
	}
}

// TestFlush_LogicalRefusalsAreNoops covers the refusal contract: a manual
// pause marker, detached HEAD, or in-progress git operation must NOT
// enqueue a flush_request and must NOT signal the daemon. The exit code
// stays zero so the harness Stop hook never surfaces a spurious nonzero
// to the surrounding agent.
func TestFlush_LogicalRefusalsAreNoops(t *testing.T) {
	bin := buildAcdBinary(t)
	binDir := filepath.Dir(bin)

	repo := tempRepo(t)
	sessionID := "e2e-flush-refusal"
	env := adapterEnv(t, binDir, "CLAUDE_PROJECT_DIR="+repo)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if startRes := runAcd(t, ctx, env, "start",
		"--repo", repo,
		"--session-id", sessionID,
		"--harness", "claude-code",
	); startRes.ExitCode != 0 {
		t.Fatalf("acd start exit=%d\nstdout=%s\nstderr=%s",
			startRes.ExitCode, startRes.Stdout, startRes.Stderr)
	}
	t.Cleanup(func() {
		shutdownDaemon(t, env, repo, sessionID)
	})
	waitFor(t, "daemon mode==running", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})

	// Pause the daemon.
	if pauseRes := runAcd(t, ctx, env, "pause",
		"--repo", repo, "--reason", "flush refusal test", "--yes",
	); pauseRes.ExitCode != 0 {
		t.Fatalf("acd pause exit=%d\nstdout=%s\nstderr=%s",
			pauseRes.ExitCode, pauseRes.Stdout, pauseRes.Stderr)
	}
	defer func() {
		_ = runAcd(t, ctx, env, "resume", "--repo", repo, "--yes")
	}()

	// flush --logical against a paused repo: ok=true, refused_reason set,
	// no flush_request enqueued. The hook hot path must stay quiet.
	flushRes := runAcd(t, ctx, env, "flush",
		"--repo", repo, "--session-id", sessionID,
		"--logical", "--json",
	)
	if flushRes.ExitCode != 0 {
		t.Fatalf("paused acd flush --logical must exit 0, got %d\nstdout=%s\nstderr=%s",
			flushRes.ExitCode, flushRes.Stdout, flushRes.Stderr)
	}
	if !strings.Contains(flushRes.Stdout, `"refused_reason"`) {
		t.Fatalf("expected refused_reason in flush output, got:\n%s", flushRes.Stdout)
	}
	if !strings.Contains(flushRes.Stdout, `"manual_pause"`) {
		t.Fatalf("expected refused_reason=manual_pause, got:\n%s", flushRes.Stdout)
	}

	// State.db must have no flush_request rows from the refused call.
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	pending := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM flush_requests WHERE command = 'flush_logical'")
	if pending != "0" {
		// Drain query for diagnostics.
		body := sqliteScalar(t, dbPath,
			"SELECT id||':'||command||':'||status FROM flush_requests")
		t.Fatalf("refused flush enqueued %s rows; expected 0\nrows: %s", pending, body)
	}
}
