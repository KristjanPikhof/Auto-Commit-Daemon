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
	ensureCheckpointRuntime(t, env, repo, bin)

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

// TestFlush_LogicalRefusalsAreNoops covers the checkpoint-first contract:
// semantic hints remain accepted while publication is paused. Unsafe Git
// state can delay publication, but it cannot block protection observations.
func TestFlush_LogicalRefusalsAreNoops(t *testing.T) {
	t.Parallel()
	bin := buildAcdBinary(t)
	binDir := filepath.Dir(bin)

	repo := tempRepo(t)
	sessionID := "e2e-flush-refusal"
	env := adapterEnv(t, binDir, "CLAUDE_PROJECT_DIR="+repo)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ensureCheckpointRuntime(t, env, repo, bin)

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

	// flush --logical against a paused repo remains an accepted semantic hint.
	flushRes := runAcd(t, ctx, env, "flush",
		"--repo", repo, "--session-id", sessionID,
		"--logical", "--json",
	)
	if flushRes.ExitCode != 0 {
		t.Fatalf("paused acd flush --logical must exit 0, got %d\nstdout=%s\nstderr=%s",
			flushRes.ExitCode, flushRes.Stdout, flushRes.Stderr)
	}
	// The worker records a hard boundary and a non-blocking publication request;
	// the publication scheduler remains responsible for the pause safety gate.
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	pending := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM flush_requests WHERE command = 'flush_logical'")
	if pending == "0" {
		t.Fatal("paused logical hint did not enqueue a publication boundary")
	}
}
