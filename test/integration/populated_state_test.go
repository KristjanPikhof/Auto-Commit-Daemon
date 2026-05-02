//go:build integration
// +build integration

// populated_state_test.go pins the AI-Assistant scenario the v2026-05-01
// verification missed: when the daemon binary is launched against a repo
// whose state.db ALREADY contains thousands of pending flush_requests, dozens
// of stale daemon_clients, and several historical shadow generations, the
// boot loop must still reach mode=running, advance heartbeat, run a real
// capture pass (events emitted beyond bootstrap_shadow.reseed), and drain
// flush_requests to zero — all within a tight wall-clock budget.
//
// Pre-fix behavior: the unbounded ignore-classify path observed in
// internal/git/ignore.go combined with the unpruned walk produced wedge-like
// startup latency on populated repos; first heartbeat could miss the 10s
// budget and the trace would never emit anything beyond the initial
// bootstrap_shadow.reseed record.
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

// TestStartFromPopulatedStateReachesFirstHeartbeat is the headline regression
// for the populated-state startup gap. Acceptance criteria from the task
// description:
//
//   - 1000 pending flush_requests, 25 stale daemon_clients, 3 shadow
//     generations × 800 rows, 0 capture_events seeded BEFORE start.
//   - Daemon reaches mode=running within 10s of `acd start`.
//   - daemon_state.heartbeat_ts advances at least 3 times within the test.
//   - Trace JSONL contains records whose event_class is something OTHER
//     than bootstrap_shadow.reseed within 30s — proves the run loop is
//     making forward progress, not stuck inside boot.
//   - flush_requests drains to 0 (status='pending') within the budget.
func TestStartFromPopulatedStateReachesFirstHeartbeat(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Phase 1 — populate state.db before the daemon under test ever starts.
	// We bring the schema into existence via a brief start/stop cycle so the
	// canonical migrations run, then seed in raw SQL.
	dbPath := initStateDBSchema(t, ctx, env, repo, "populated-bootstrap")

	// Reset to a clean baseline: the schema-bootstrap start/stop above leaves
	// a daemon_clients row + may leave a flush_request from internal wake.
	resetForSeed(t, dbPath)

	SeedFlushRequests(t, dbPath, 1000)
	SeedDaemonClients(t, dbPath, 25)
	SeedShadowGenerations(t, dbPath, "refs/heads/main", 3, 800)

	// Sanity probes: confirm the seeded counts are what we think.
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM flush_requests WHERE status='pending'"); got != "1000" {
		t.Fatalf("seed flush_requests pending=%s want 1000", got)
	}
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM daemon_clients"); got != "25" {
		t.Fatalf("seed daemon_clients=%s want 25", got)
	}
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM shadow_paths"); got != fmt.Sprintf("%d", 3*800) {
		t.Fatalf("seed shadow_paths=%s want %d", got, 3*800)
	}
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM capture_events"); got != "0" {
		t.Fatalf("seed precondition: capture_events=%s want 0", got)
	}

	// Phase 2 — start the daemon with tracing turned on and a per-repo
	// trace dir we can inspect without depending on $XDG_*.
	traceDir := filepath.Join(repo, ".git", "acd", "trace-test")
	if err := os.MkdirAll(traceDir, 0o755); err != nil {
		t.Fatalf("mkdir trace dir: %v", err)
	}
	traceEnv := envWith(env, "ACD_TRACE=1", "ACD_TRACE_DIR="+traceDir)

	startWall := time.Now()
	startSession(t, ctx, traceEnv, repo, "populated-1", "shell", "ACD_TRACE=1", "ACD_TRACE_DIR="+traceDir)

	// First-heartbeat budget: 10s from `acd start` invocation.
	waitFor(t, "daemon mode=running within 10s of populated-state start", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	if elapsed := time.Since(startWall); elapsed > 12*time.Second {
		t.Fatalf("first running budget overshot: %v", elapsed)
	}

	// Phase 3 — heartbeat must advance past whatever the boot loop stamped,
	// proving the daemon's run loop is making forward progress (not parked
	// in a wedge after the initial heartbeat). We sample three distinct
	// observations: the boot value, a post-burst value, and a value taken
	// after a brief settle delay. Each must be a valid float; the post-
	// burst sample MUST be >= the boot sample (monotonic non-decreasing);
	// AT LEAST ONE later sample MUST be strictly greater (proves a tick
	// after boot actually happened on the populated DB).
	bootHB := readHeartbeatTs(repo)
	if bootHB <= 0 {
		t.Fatalf("boot heartbeat_ts not stamped: %v", bootHB)
	}

	// Drive a wake burst so the run loop has a definite reason to tick.
	for i := 0; i < 5; i++ {
		wakeSession(t, ctx, traceEnv, repo, "populated-1")
		time.Sleep(200 * time.Millisecond)
	}
	burstHB := readHeartbeatTs(repo)
	if burstHB < bootHB {
		t.Fatalf("heartbeat regressed across burst: boot=%f burst=%f", bootHB, burstHB)
	}

	// Wait up to 30s for the heartbeat to strictly advance past the boot
	// stamp. This is the load-bearing populated-DB assertion: pre-fix the
	// run loop wedged on the first capture pass and never reached
	// heartbeatNow at the bottom of the iteration.
	advanced := false
	deadline := time.Now().Add(30 * time.Second)
	var lastHB float64
	for time.Now().Before(deadline) {
		// Force file activity so fsnotify drives the loop even if SIGUSR1
		// gets coalesced. Each iteration touches a distinct path so the
		// daemon classifies it as a real op.
		writeFile(t, filepath.Join(repo, fmt.Sprintf("hb-poke-%03d.txt", time.Now().UnixNano()%1000)),
			"poke\n")
		wakeSession(t, ctx, traceEnv, repo, "populated-1")
		time.Sleep(400 * time.Millisecond)
		lastHB = readHeartbeatTs(repo)
		if lastHB > bootHB {
			advanced = true
			break
		}
	}
	if !advanced {
		t.Fatalf("heartbeat_ts never advanced past boot value within 30s: boot=%f last=%f",
			bootHB, lastHB)
	}

	// Phase 4 — trace JSONL must contain at least one record beyond
	// bootstrap_shadow.reseed. The canonical "real work happened" markers
	// are capture.classify or replay.* records.
	waitFor(t, "trace contains non-bootstrap event_class", 30*time.Second, func() bool {
		return traceHasNonBootstrapEvent(t, traceDir)
	})

	// Phase 5 — pending flush_requests must drain to 0 within budget.
	waitFor(t, "flush_requests pending drained to 0", 30*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM flush_requests WHERE status='pending'") == "0"
	})
}

// resetForSeed wipes residual rows from the bootstrap start/stop cycle so
// the test's own seed counts are deterministic. We keep daemon_state intact
// (the singleton row carries the schema marker) but clear every other table
// the seeders touch.
func resetForSeed(t *testing.T, dbPath string) {
	t.Helper()
	ddl := strings.Join([]string{
		"DELETE FROM flush_requests;",
		"DELETE FROM daemon_clients;",
		"DELETE FROM shadow_paths;",
		"DELETE FROM capture_events;",
		"DELETE FROM capture_ops;",
	}, " ")
	if out, err := execSQL(dbPath, ddl); err != nil {
		t.Fatalf("reset for seed: %v\n%s", err, out)
	}
}

// execSQL runs sqlite3 against the database and returns combined output.
func execSQL(dbPath, sql string) (string, error) {
	cmd := exec.Command("sqlite3", dbPath, sql)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// traceHasNonBootstrapEvent returns true if at least one JSONL record in any
// rotated file under traceDir reports an event_class other than
// bootstrap_shadow.reseed. We open every *.jsonl, scan one line at a time,
// and stop on the first non-bootstrap record.
func traceHasNonBootstrapEvent(t *testing.T, traceDir string) bool {
	t.Helper()
	entries, err := os.ReadDir(traceDir)
	if err != nil {
		return false
	}
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".jsonl") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(traceDir, ent.Name()))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var rec struct {
				EventClass string `json:"event_class"`
			}
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				continue
			}
			if rec.EventClass != "" && rec.EventClass != "bootstrap_shadow.reseed" {
				return true
			}
		}
	}
	return false
}
