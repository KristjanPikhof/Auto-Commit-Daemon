//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestStartLatencyBudget_RepeatedActiveHooks asserts that the registry-
// read short-circuit is wired AND meaningfully faster than the cold
// path. The test runs in two phases:
//
//  1. Cold path: stop any prior daemon, then time `acd start` against an
//     empty registry. This call performs flock(control.lock) +
//     state.Open(SQLite) + RegisterClient + LoadDaemonState +
//     spawnDaemon (fork/exec) + spawn-poll + central registry rewrite.
//     We measure this so the hot path can be compared apples-to-apples
//     under the same hardware/IO conditions.
//
//  2. Hot path: 10 sequential `acd start` calls under the same
//     session_id. Each must short-circuit (Started=false, Duplicate=true)
//     and complete within `perCallBudget`. Aggregate must beat the
//     `aggregateBudget`. Each individual hot call must also beat
//     `cold / minSpeedupFactor`, pinning the documented 50ms-vs-1s
//     budget claim.
//
// Budget rationale: cold path on a quiet macOS box is ~30-200ms (mostly
// fork+exec). Hot path replaces all of that with one os.ReadFile, one
// central.Load (no flock), one kill(0), one ps, and one TouchClient
// UPDATE — comfortably under 50ms on the same hardware. We pick a
// per-call budget of 200ms (4x measured) so the test is robust on noisy
// CI runners but still flags genuine regressions, and require hot/cold
// speedup of at least 1.5x so a regression that makes the hot path
// equivalent to the cold path fails loudly.
func TestStartLatencyBudget_RepeatedActiveHooks(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary not found in PATH; required for daemon_state probes")
	}

	repo := tempRepo(t)
	env := withIsolatedHome(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const (
		hooks            = 10
		perCallBudget    = 200 * time.Millisecond
		aggregateBudget  = 1500 * time.Millisecond
		minSpeedupFactor = 1.5 // hot < cold / minSpeedupFactor
	)

	// Phase 1: time the cold path. We force a real cold start by
	// stopping any pre-existing daemon (none should exist in a fresh
	// $HOME, but the call is idempotent) and timing a fresh `acd start`.
	stopRes := runAcd(t, ctx, env, "stop", "--repo", repo, "--force", "--json")
	if stopRes.ExitCode != 0 && !strings.Contains(stopRes.Stderr, "no daemon") {
		t.Logf("pre-cold stop exit=%d (ignored): %s", stopRes.ExitCode, stopRes.Stderr)
	}

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
		t.Fatalf("cold start did not spawn daemon: %+v", coldJSON)
	}

	// Wait until daemon_state.mode == "running" so the daemon is fully
	// alive when we time the active-hook calls.
	waitFor(t, "daemon mode=running", 5*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})

	// Phase 2: hot-path measurement. The first hot call exercises the
	// full short-circuit path (cache read + central.Load + kill(0) + ps
	// fingerprint + TouchClient UPDATE).
	hotBudget := time.Duration(float64(coldDuration) / minSpeedupFactor)
	t.Logf("cold path took %v; hot must be <= %v (cold/%g) AND <= %v (per-call cap)",
		coldDuration, hotBudget, minSpeedupFactor, perCallBudget)

	durations := make([]time.Duration, 0, hooks)
	for i := 0; i < hooks; i++ {
		start := time.Now()
		res := runAcd(t, ctx, env,
			"start",
			"--session-id", "session-budget",
			"--repo", repo,
			"--harness", "claude-code",
			"--json",
		)
		dur := time.Since(start)
		if res.ExitCode != 0 {
			t.Fatalf("hook %d exit=%d after %v\nstdout=%s\nstderr=%s",
				i, res.ExitCode, dur, res.Stdout, res.Stderr)
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
			t.Fatalf("hook %d unexpectedly reported started=true (daemon respawned?): %+v", i, hot)
		}
		if !hot.Duplicate {
			t.Fatalf("hook %d not flagged duplicate (short-circuit may be misfiring): %+v", i, hot)
		}
		if hot.DaemonPID != coldJSON.DaemonPID {
			t.Fatalf("hook %d daemon pid=%d want %d (cache should mirror cold-path pid)",
				i, hot.DaemonPID, coldJSON.DaemonPID)
		}
		if dur > perCallBudget {
			t.Fatalf("hook %d took %v; per-call budget is %v", i, dur, perCallBudget)
		}
		if dur > hotBudget {
			t.Fatalf("hook %d took %v; cold-path was %v so hot must be <= %v (cold/%g)",
				i, dur, coldDuration, hotBudget, minSpeedupFactor)
		}
		durations = append(durations, dur)
	}

	total := time.Duration(0)
	for _, d := range durations {
		total += d
	}
	t.Logf("ten-hook latency: cold=%v per-call=%v total=%v (caps: per=%v cold/%.1f=%v total=%v)",
		coldDuration, durations, total, perCallBudget, minSpeedupFactor, hotBudget, aggregateBudget)
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
	waitFor(t, "daemon mode=stopped", 5*time.Second, func() bool {
		return readDaemonStateMode(repo) == "stopped" || strings.TrimSpace(readDaemonStateMode(repo)) == ""
	})
}
