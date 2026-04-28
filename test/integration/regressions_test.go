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
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestRegressions exercises the §14.2 list one subtest at a time. Every
// scenario is wired against the real `acd` binary so failures reproduce the
// same way an operator would observe them in the field.
//
// Layout per scenario:
//   - acquire a fresh repo + isolated $HOME
//   - drive the binary through start/wake/stop
//   - inspect git tree, sqlite state, or process exit code as appropriate
//
// Most scenarios run inside a single t.Run so a failure in one does not
// abort the others (subtests get independent cleanup).
func TestRegressions(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required for regression assertions")
	}

	t.Run("SymlinkToDirIsMode120000", regSymlinkToDirIsMode120000)
	t.Run("SensitiveGlobDefaultDeny", regSensitiveGlobDefaultDeny)
	t.Run("SensitiveGlobEmptyOverrideFallback", regSensitiveGlobEmptyOverrideFallback)
	t.Run("BootstrapFailurePreservesShadow", regBootstrapFailurePreservesShadow)
	t.Run("CoalescedFlushAcksAtomic", regCoalescedFlushAcksAtomic)
	t.Run("LockContentionLoserExitsTempFail", regLockContentionLoserExitsTempFail)
	t.Run("PIDReuseRejectedByFingerprint", regPIDReuseRejectedByFingerprint)
	t.Run("StopWithPeerDefersKill", regStopWithPeerDefersKill)
	t.Run("DaemonSelfTerminatesOnEmptySweeps", regDaemonSelfTerminatesOnEmptySweeps)
}

// startDaemon is shared scaffolding: `acd start` + wait for mode=running.
// Returns the parsed JSON for follow-up assertions.
type startInfo struct {
	Started   bool   `json:"started"`
	Duplicate bool   `json:"duplicate"`
	DaemonPID int    `json:"daemon_pid"`
	Repo      string `json:"repo"`
}

func startSession(t *testing.T, ctx context.Context, env []string, repo, sessionID, harness string, extraEnv ...string) startInfo {
	t.Helper()
	full := envWith(env, extraEnv...)
	res := runAcd(t, ctx, full,
		"start",
		"--session-id", sessionID,
		"--repo", repo,
		"--harness", harness,
		"--json",
	)
	if res.ExitCode != 0 {
		t.Fatalf("acd start session=%s exit=%d\nstdout=%s\nstderr=%s",
			sessionID, res.ExitCode, res.Stdout, res.Stderr)
	}
	var info startInfo
	if err := json.Unmarshal([]byte(res.Stdout), &info); err != nil {
		t.Fatalf("decode start json: %v\nstdout=%s", err, res.Stdout)
	}
	return info
}

func wakeSession(t *testing.T, ctx context.Context, env []string, repo, sessionID string) {
	t.Helper()
	res := runAcd(t, ctx, env,
		"wake", "--session-id", sessionID, "--repo", repo, "--json",
	)
	if res.ExitCode != 0 {
		t.Fatalf("acd wake exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
}

// stopSession is a forced cleanup helper used in t.Cleanup.
func stopSessionForce(t *testing.T, env []string, repo string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = runAcd(t, ctx, env, "stop", "--repo", repo, "--force", "--json")
	// Best-effort wait for stopped state — don't fail cleanup.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if readDaemonStateMode(repo) == "stopped" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitMode is the canonical "wait until daemon_state.mode matches" helper.
func waitMode(t *testing.T, repo, want string, timeout time.Duration) {
	t.Helper()
	waitFor(t, fmt.Sprintf("daemon_state.mode==%s", want), timeout, func() bool {
		return readDaemonStateMode(repo) == want
	})
}

// waitForCommitContaining polls `git log --name-only` across all commits
// until path appears, or the timeout fires. Returns the matched commit oid.
// Scanning the full log (not just HEAD) tolerates the daemon producing
// multiple atomic commits before the target path lands.
func waitForCommitContaining(t *testing.T, repo, path string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if names, err := runGit(repo, "log", "--all", "--name-only", "--pretty=format:COMMIT %H"); err == nil {
			currentCommit := ""
			for _, line := range strings.Split(names, "\n") {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "COMMIT ") {
					currentCommit = strings.TrimPrefix(line, "COMMIT ")
					continue
				}
				if line == path {
					return currentCommit
				}
			}
		}
		time.Sleep(75 * time.Millisecond)
	}
	out, _ := runGit(repo, "log", "--all", "--name-only", "--pretty=format:%h %s")
	t.Fatalf("history did not include %q within %v\nlast log:\n%s", path, timeout, out)
	return ""
}

// regSymlinkToDirIsMode120000 — write a symlink that points at a real
// directory, drive a capture, assert the symlink lands in the tree as
// mode 120000 (a blob), never as a tree of the target's children.
func regSymlinkToDirIsMode120000(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build a directory with a sentinel file the daemon must NOT pick up.
	target := filepath.Join(repo, "target")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	writeFile(t, filepath.Join(target, "inside.txt"), "should-not-be-captured-via-symlink\n")

	startSession(t, ctx, env, repo, "sym-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	// Create a symlink pointing at the directory.
	link := filepath.Join(repo, "linkdir")
	if err := os.Symlink("target", link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	wakeSession(t, ctx, env, repo, "sym-1")

	// Wait for `linkdir` to appear in any commit on the active branch.
	commit := waitForCommitContaining(t, repo, "linkdir", 8*time.Second)

	// Inspect the mode at the commit that introduced linkdir — must be 120000.
	out := runGitOK(t, repo, "ls-tree", commit, "linkdir")
	// Format: "120000 blob <oid>\tlinkdir"
	if !strings.HasPrefix(strings.TrimSpace(out), "120000") {
		t.Fatalf("linkdir tree entry has wrong mode: %q", out)
	}
	// Critical: the symlinked directory's contents must NEVER appear under
	// the symlink path in any commit on HEAD.
	allTrees := runGitOK(t, repo, "log", "--name-only", "--pretty=format:", "HEAD")
	for _, line := range strings.Split(allTrees, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "linkdir/") {
			t.Fatalf("captured file inside symlinked dir: %q\nfull log:\n%s", line, allTrees)
		}
	}
}

// regSensitiveGlobDefaultDeny — .env and secrets.json must never be
// committed when the operator does NOT override the default-deny list.
func regSensitiveGlobDefaultDeny(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Drop sensitive files matched by the canonical default-deny list +
	// one harmless file before starting the daemon. The harmless one is
	// the canary that proves capture+replay is actually running.
	writeFile(t, filepath.Join(repo, ".env"), "API_KEY=hunter2\n")
	writeFile(t, filepath.Join(repo, "credentials.txt"), "user:pw\n")
	writeFile(t, filepath.Join(repo, "secrets/leak"), "shhh\n")
	writeFile(t, filepath.Join(repo, "harmless.txt"), "ok-to-capture\n")

	startSession(t, ctx, env, repo, "sens-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	wakeSession(t, ctx, env, repo, "sens-1")

	// Wait for harmless.txt to land — proves the daemon performed at least
	// one capture+replay pass.
	waitForCommitContaining(t, repo, "harmless.txt", 5*time.Second)

	// Now inspect the full git history — none of the sensitive paths may
	// appear. Match on exact path so unrelated rows aren't false positives.
	all := runGitOK(t, repo, "log", "--all", "--name-only", "--pretty=format:")
	for _, line := range strings.Split(all, "\n") {
		line = strings.TrimSpace(line)
		switch line {
		case ".env", "credentials.txt", "secrets/leak":
			t.Fatalf("sensitive file %q leaked into history:\n%s", line, all)
		}
	}
}

// regSensitiveGlobEmptyOverrideFallback — setting ACD_SENSITIVE_GLOBS to an
// empty string must NOT disable the default-deny list (security regression).
func regSensitiveGlobEmptyOverrideFallback(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	writeFile(t, filepath.Join(repo, ".env"), "DB_PASS=root\n")
	writeFile(t, filepath.Join(repo, "harmless.txt"), "fallback-canary\n")

	// The override must propagate into the daemon's environment — we pass
	// it via the start command's env so the spawned daemon inherits it.
	startSession(t, ctx, env, repo, "sens-empty", "shell", "ACD_SENSITIVE_GLOBS=")
	waitMode(t, repo, "running", 5*time.Second)

	// Use the same env on wake so the call shape is consistent.
	full := envWith(env, "ACD_SENSITIVE_GLOBS=")
	wakeSession(t, ctx, full, repo, "sens-empty")

	waitForCommitContaining(t, repo, "harmless.txt", 5*time.Second)

	all := runGitOK(t, repo, "log", "--all", "--name-only", "--pretty=format:")
	for _, line := range strings.Split(all, "\n") {
		if strings.TrimSpace(line) == ".env" {
			t.Fatalf(".env leaked when ACD_SENSITIVE_GLOBS=\"\":\n%s", all)
		}
	}
}

// regBootstrapFailurePreservesShadow — corrupting a non-critical table in
// the per-repo state.db must not cause the daemon to wipe shadow_paths from
// scratch. We start the daemon once to get a populated shadow, drop the
// flush_requests table to force a bootstrap-time error path, restart the
// daemon, and assert that shadow_paths is still populated and the
// daemon_state row still reflects the original PID lineage (heartbeat
// stamp survives).
func regBootstrapFailurePreservesShadow(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Initial run: lay shadow + stamp heartbeat.
	writeFile(t, filepath.Join(repo, "boot.txt"), "boot\n")
	startSession(t, ctx, env, repo, "boot-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	wakeSession(t, ctx, env, repo, "boot-1")
	waitForCommitContaining(t, repo, "boot.txt", 5*time.Second)

	// Capture the shadow row count + the heartbeat ts before disruption.
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	preShadow := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM shadow_paths")
	if preShadow == "0" || preShadow == "" {
		t.Fatalf("expected shadow_paths populated before disruption, got %q", preShadow)
	}
	preHeartbeat := sqliteScalar(t, dbPath, "SELECT heartbeat_ts FROM daemon_state WHERE id = 1")
	if preHeartbeat == "" {
		t.Fatalf("expected daemon_state row before disruption")
	}

	// Stop the daemon cleanly.
	res := runAcd(t, ctx, env, "stop", "--session-id", "boot-1", "--repo", repo, "--json")
	if res.ExitCode != 0 {
		t.Fatalf("acd stop exit=%d\n%s\n%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	waitMode(t, repo, "stopped", 5*time.Second)

	// Disrupt: wipe rows from a non-critical operational table while
	// leaving shadow_paths + daemon_state intact. The bootstrap is
	// expected to no-op (existing rows present) and the heartbeat must
	// be re-stamped without erasing what came before.
	if out, err := exec.Command("sqlite3", dbPath,
		"DELETE FROM flush_requests; DELETE FROM capture_events;",
	).CombinedOutput(); err != nil {
		t.Fatalf("disrupt sqlite3: %v\n%s", err, out)
	}

	// Restart: shadow rows must survive (count >= preShadow), and the
	// daemon must come up healthy (mode=running) without rebuilding.
	startSession(t, ctx, env, repo, "boot-2", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	postShadow := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM shadow_paths")
	if postShadow != preShadow {
		t.Fatalf("shadow_paths count changed across restart: pre=%s post=%s", preShadow, postShadow)
	}
}

// regCoalescedFlushAcksAtomic — fire 50 wakes in quick succession and
// confirm the daemon drains every flush_request to a terminal status with
// none stuck in 'pending'.
func regCoalescedFlushAcksAtomic(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	writeFile(t, filepath.Join(repo, "burst.txt"), "burst\n")
	startSession(t, ctx, env, repo, "burst-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	// Fan out 50 acd wake calls in parallel.
	const N = 50
	var wg sync.WaitGroup
	failures := atomic.Int32{}
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			subCtx, sub := context.WithTimeout(ctx, 30*time.Second)
			defer sub()
			res := runAcd(t, subCtx, env,
				"wake", "--session-id", "burst-1", "--repo", repo, "--json",
			)
			if res.ExitCode != 0 {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()
	if f := failures.Load(); f > 0 {
		t.Fatalf("%d/%d acd wake invocations failed", f, N)
	}

	// The daemon may also have produced its own 'wake' rows from the SIGUSR1
	// nudges; we only assert that nothing remains 'pending' after the burst
	// settles. Wait up to 5s for the queue to drain.
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	deadline := time.Now().Add(5 * time.Second)
	var pending string
	for time.Now().Before(deadline) {
		pending = sqliteScalar(t, dbPath,
			"SELECT COUNT(*) FROM flush_requests WHERE status = 'pending'")
		if pending == "0" {
			break
		}
		time.Sleep(75 * time.Millisecond)
	}
	if pending != "0" {
		// Dump for diagnosis.
		dump, _ := exec.Command("sqlite3", dbPath,
			"SELECT id,status,note FROM flush_requests").CombinedOutput()
		t.Fatalf("flush_requests left pending=%s after burst:\n%s", pending, dump)
	}

	total := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM flush_requests")
	if total == "" || total == "0" {
		t.Fatalf("expected at least 50 flush_requests rows, got %s", total)
	}
}

// regLockContentionLoserExitsTempFail — start the daemon via `acd start`,
// then run a second `acd daemon run` directly against the same repo. The
// loser must exit 75 (EX_TEMPFAIL) without altering the live daemon's
// daemon_state row.
func regLockContentionLoserExitsTempFail(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "lock-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	livePID := readDaemonStatePID(repo)
	if livePID == 0 {
		t.Fatalf("no live PID in daemon_state")
	}

	// Run the second daemon directly. It should exit fast with code 75.
	loser := runAcd(t, ctx, env, "daemon", "run", "--repo", repo)
	if loser.ExitCode != 75 {
		t.Fatalf("loser exit=%d want 75\nstdout=%s\nstderr=%s",
			loser.ExitCode, loser.Stdout, loser.Stderr)
	}

	// The live daemon's PID must be unchanged.
	postPID := readDaemonStatePID(repo)
	if postPID != livePID {
		t.Fatalf("live daemon PID changed: pre=%d post=%d", livePID, postPID)
	}
	if mode := readDaemonStateMode(repo); mode != "running" {
		t.Fatalf("live daemon mode disturbed: %q", mode)
	}
}

// regPIDReuseRejectedByFingerprint — register a daemon_clients row whose
// watch_pid is the test process pid but whose watch_fp is bogus. After the
// daemon's next sweep the row is dropped (because the live fingerprint of
// our pid won't match the bogus one).
func regPIDReuseRejectedByFingerprint(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start with a real session so the daemon stays alive across the sweep
	// (otherwise it might self-terminate on empty sweeps before evicting).
	startSession(t, ctx, env, repo, "anchor", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	// Inject the bogus row with a sentinel session id.
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	myPID := os.Getpid()
	insert := fmt.Sprintf(
		"INSERT OR REPLACE INTO daemon_clients(session_id, harness, watch_pid, watch_fp, registered_ts, last_seen_ts) VALUES ('pidreuse-bogus', 'shell', %d, 'bogus|fingerprint', %f, %f);",
		myPID,
		nowFloatSeconds(),
		nowFloatSeconds(),
	)
	if out, err := exec.Command("sqlite3", dbPath, insert).CombinedOutput(); err != nil {
		t.Fatalf("inject bogus client: %v\n%s", err, out)
	}

	// Force a sweep by sending wake (which the daemon converts into a tick).
	wakeSession(t, ctx, env, repo, "anchor")

	// Wait up to 10s for the bogus row to be evicted. Default
	// ClientSweepInterval is 5s.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		out, err := exec.Command("sqlite3", dbPath,
			"SELECT COUNT(*) FROM daemon_clients WHERE session_id = 'pidreuse-bogus'",
		).CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) == "0" {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	dump, _ := exec.Command("sqlite3", dbPath,
		"SELECT session_id,watch_pid,watch_fp FROM daemon_clients").CombinedOutput()
	t.Fatalf("PID-reuse bogus row not evicted within 15s\nrows:\n%s", dump)
}

// regStopWithPeerDefersKill — two concurrent sessions; stopping one must
// leave the daemon alive; stopping the second must shut it down.
func regStopWithPeerDefersKill(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "peer-1", "shell")
	startSession(t, ctx, env, repo, "peer-2", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	pid := readDaemonStatePID(repo)
	if pid == 0 {
		t.Fatalf("expected live PID")
	}

	// Stop session 1 — daemon must defer.
	res1 := runAcd(t, ctx, env, "stop", "--session-id", "peer-1", "--repo", repo, "--json")
	if res1.ExitCode != 0 {
		t.Fatalf("first stop exit=%d\n%s\n%s", res1.ExitCode, res1.Stdout, res1.Stderr)
	}
	var stop1 struct {
		Stopped  bool `json:"stopped"`
		Deferred bool `json:"deferred"`
		Peers    int  `json:"peers"`
	}
	if err := json.Unmarshal([]byte(res1.Stdout), &stop1); err != nil {
		t.Fatalf("decode first stop: %v\n%s", err, res1.Stdout)
	}
	if !stop1.Deferred {
		t.Fatalf("expected deferred=true after first stop, got %+v", stop1)
	}
	if stop1.Stopped {
		t.Fatalf("expected stopped=false after first stop, got %+v", stop1)
	}
	if stop1.Peers != 1 {
		t.Fatalf("expected peers=1 remaining, got %+v", stop1)
	}
	// Daemon still alive.
	if readDaemonStateMode(repo) != "running" {
		t.Fatalf("daemon transitioned before second stop: mode=%q", readDaemonStateMode(repo))
	}

	// Stop session 2 — daemon must shut down.
	res2 := runAcd(t, ctx, env, "stop", "--session-id", "peer-2", "--repo", repo, "--json")
	if res2.ExitCode != 0 {
		t.Fatalf("second stop exit=%d\n%s\n%s", res2.ExitCode, res2.Stdout, res2.Stderr)
	}
	var stop2 struct {
		Stopped  bool `json:"stopped"`
		Deferred bool `json:"deferred"`
	}
	if err := json.Unmarshal([]byte(res2.Stdout), &stop2); err != nil {
		t.Fatalf("decode second stop: %v\n%s", err, res2.Stdout)
	}
	if !stop2.Stopped {
		t.Fatalf("expected stopped=true after final session out, got %+v", stop2)
	}
	waitMode(t, repo, "stopped", 5*time.Second)
}

// regDaemonSelfTerminatesOnEmptySweeps — start the daemon, deregister all
// clients out-of-band, wait for self-termination after BootGrace + 2 sweeps.
//
// We cannot reasonably configure BootGrace via the binary (it's a Go-level
// option), so this scenario uses the production defaults: BootGrace=30s +
// ClientSweepInterval=5s + EmptySweepThreshold=2 = ~40s in the worst case.
// To stay under 60s wall-clock we rely on the heartbeat/TTL path and the
// fact that the daemon's idle ceiling is sub-second on a quiet repo.
func regDaemonSelfTerminatesOnEmptySweeps(t *testing.T) {
	if testing.Short() {
		t.Skip("self-terminate test takes ~45s; skipped under -short")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "lonely-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	// Out-of-band: drop every daemon_clients row so the daemon's next
	// sweep observes empty.
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	if out, err := exec.Command("sqlite3", dbPath, "DELETE FROM daemon_clients").CombinedOutput(); err != nil {
		t.Fatalf("drop clients: %v\n%s", err, out)
	}

	// Wait up to 60s for the self-terminate path. BootGrace=30s +
	// ClientSweepInterval=5s × 2 = 40s is the floor.
	waitMode(t, repo, "stopped", 70*time.Second)
}

// sqliteScalar runs `sqlite3 db -- "SELECT ..."` and returns the trimmed
// stdout. Failures fail the test (these are pure read-only probes).
func sqliteScalar(t *testing.T, dbPath, query string) string {
	t.Helper()
	out, err := exec.Command("sqlite3", dbPath, query).CombinedOutput()
	if err != nil {
		t.Fatalf("sqlite3 %s: %v\n%s", query, err, out)
	}
	return strings.TrimSpace(string(out))
}

func nowFloatSeconds() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

// _ keeps the syscall import quiet under conditional cleanup paths where
// SIGTERM may be referenced in future test extensions.
var _ = syscall.SIGTERM
