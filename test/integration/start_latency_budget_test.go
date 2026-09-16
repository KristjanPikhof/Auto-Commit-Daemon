//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestStartLatencyBudget_RepeatedActiveHooks keeps compatibility session
// responses correct while measuring the unified event command installed by
// current integrations. Timing includes the production CLI and supervisor IPC;
// diagnostic reads and legacy JSON reporting stay outside the hook budget.
func TestStartLatencyBudget_RepeatedActiveHooks(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary not found in PATH; required for daemon_state probes")
	}

	repo := tempRepo(t)
	env := withIsolatedHome(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	ensureCheckpointRuntime(t, env, repo, buildAcdBinary(t))

	const (
		hooks           = 10
		perCallBudget   = 200 * time.Millisecond
		aggregateBudget = 1500 * time.Millisecond
	)

	// Establish the session and retain compatibility response coverage.
	coldStart := time.Now()
	cold := runAcd(t, ctx, env,
		"start",
		"--session-id", "session-budget",
		"--repo", repo,
		"--harness", "claude-code",
		"--json",
	)
	coldDuration := time.Since(coldStart)
	if cold.ExitCode != 0 {
		t.Fatalf("cold start exit=%d after %v\nstdout=%s\nstderr=%s",
			cold.ExitCode, coldDuration, cold.Stdout, cold.Stderr)
	}
	var coldJSON struct {
		Started   bool `json:"started"`
		DaemonPID int  `json:"daemon_pid"`
	}
	if err := json.Unmarshal([]byte(cold.Stdout), &coldJSON); err != nil {
		t.Fatalf("decode cold json: %v\n%s", err, cold.Stdout)
	}
	if !coldJSON.Started || coldJSON.DaemonPID <= 0 {
		t.Fatalf("first session registration failed: %+v", coldJSON)
	}

	// Wait until daemon_state.mode == "running" so the daemon is fully
	// alive when we time the active-hook calls.
	waitFor(t, "daemon mode=running", 5*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})

	t.Logf("first session open took %v; repeated supervisor IPC calls must remain <= %v",
		coldDuration, perCallBudget)

	durations := make([]time.Duration, 0, hooks)
	for i := 0; i < hooks; i++ {
		dbPath := filepath.Join(repo, ".git", "acd", "state.db")
		const lastSeenQuery = "SELECT last_seen_ts FROM daemon_clients WHERE session_id='session-budget'"
		before := sqliteScalar(t, dbPath, lastSeenQuery)
		start := time.Now()
		hook := runAcd(t, ctx, env, "internal", "integration", "event",
			"--harness", "claude-code", "--event", "session_open",
			"--repo", repo, "--session-id", "session-budget")
		dur := time.Since(start)
		if hook.ExitCode != 0 {
			t.Fatalf("hook %d exit=%d: %s", i, hook.ExitCode, hook.Stderr)
		}
		// Integration hooks fail open; a zero exit alone cannot prove delivery.
		if after := sqliteScalar(t, dbPath, lastSeenQuery); after == "" || after == before {
			t.Fatalf("hook %d did not refresh its registered session: before=%q after=%q", i, before, after)
		}
		if dur > perCallBudget {
			t.Fatalf("hook %d took %v; per-call budget is %v", i, dur, perCallBudget)
		}
		durations = append(durations, dur)
	}

	// Check legacy responses separately so diagnostic calls do not add wakes
	// between the production hooks whose latency is being measured.

	for i := 0; i < hooks; i++ {
		res := runAcd(t, ctx, env,
			"start",
			"--session-id", "session-budget",
			"--repo", repo,
			"--harness", "claude-code",
			"--json",
		)
		if res.ExitCode != 0 {
			t.Fatalf("compatibility call %d exit=%d\nstdout=%s\nstderr=%s",
				i, res.ExitCode, res.Stdout, res.Stderr)
		}
		var hot struct {
			Started   bool `json:"started"`
			Duplicate bool `json:"duplicate"`
			DaemonPID int  `json:"daemon_pid"`
		}
		if err := json.Unmarshal([]byte(res.Stdout), &hot); err != nil {
			t.Fatalf("hook %d decode json: %v\n%s", i, err, res.Stdout)
		}
		if hot.Started {
			t.Fatalf("hook %d unexpectedly registered the existing session again: %+v", i, hot)
		}
		if !hot.Duplicate {
			t.Fatalf("hook %d not flagged duplicate (short-circuit may be misfiring): %+v", i, hot)
		}
		if hot.DaemonPID != coldJSON.DaemonPID {
			t.Fatalf("hook %d daemon pid=%d want %d (worker identity must remain stable)",
				i, hot.DaemonPID, coldJSON.DaemonPID)
		}

	}

	total := time.Duration(0)
	for _, d := range durations {
		total += d
	}
	t.Logf("ten-hook latency: first=%v per-call=%v total=%v (caps: per=%v total=%v)",
		coldDuration, durations, total, perCallBudget, aggregateBudget)
	if total > aggregateBudget {
		t.Fatalf("aggregate latency %v exceeds budget %v across %d hooks",
			total, aggregateBudget, hooks)
	}

	// Cleanup: stop the daemon so we do not leak processes between tests.
	stop := runAcd(t, ctx, env,
		"stop",
		"--session-id", "session-budget",
		"--repo", repo,
		"--json",
	)
	if stop.ExitCode != 0 {
		t.Logf("stop exit=%d\nstdout=%s\nstderr=%s",
			stop.ExitCode, stop.Stdout, stop.Stderr)
	}
}
