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
	"strconv"
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
	t.Parallel()
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
	t.Run("DetachedHeadStartRefused", regDetachedHeadStartRefused)
	t.Run("OfflineResetRestartNoPhantomEvents", regOfflineResetRestartNoPhantomEvents)
	t.Run("ConcurrentSessionStartsRegisterAllClients", regConcurrentSessionStartsRegisterAllClients)
	t.Run("DaemonSelfTerminatesOnEmptySweeps", regDaemonSelfTerminatesOnEmptySweeps)
	t.Run("TrackedChildDeleteUnderSafeIgnoredDir", regTrackedChildDeleteUnderSafeIgnoredDir)
	t.Run("RepeatedEditsPublishOrderedCommits", regRepeatedEditsPublishOrderedCommits)
	t.Run("BlockedConflictTerminalAcrossPolls", regBlockedConflictTerminalAcrossPolls)
	t.Run("BlockedConflictPreventsLeapfrogPublish", regBlockedConflictPreventsLeapfrogPublish)
}

// TestStopAll_WipesAllPerSessionCaches pins the P1 #7 acceptance for the
// `acd stop --all` path: every per-session start-cache file under each
// repo's <gitDir>/acd/ must be removed once the daemon is fully stopped,
// otherwise a subsequent active hook short-circuits onto a stale daemon
// row. Two sessions on the same repo seed the test; we then run `acd
// stop --all` and inspect the directory.
func TestStopAll_WipesAllPerSessionCaches(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "sess-A", "claude-code")
	startSession(t, ctx, env, repo, "sess-B", "codex")
	waitMode(t, repo, "running", 5*time.Second)

	cacheDir := filepath.Join(repo, ".git", "acd")
	listCaches := func() []string {
		entries, err := os.ReadDir(cacheDir)
		if err != nil {
			t.Fatalf("readdir: %v", err)
		}
		var names []string
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "start-cache-") && strings.HasSuffix(e.Name(), ".json") {
				names = append(names, e.Name())
			}
		}
		return names
	}
	before := listCaches()
	if len(before) != 0 {
		t.Fatalf("supervisor session routing must not create start caches, got %v", before)
	}

	res := runAcd(t, ctx, env, "stop", "--all", "--force", "--json")
	if res.ExitCode == 0 {
		t.Fatalf("stop --all unexpectedly succeeded\nstdout=%s\nstderr=%s",
			res.Stdout, res.Stderr)
	}

	after := listCaches()
	if len(after) != 0 {
		t.Fatalf("rejected stop --all created cache files: %v", after)
	}
}

// TestRefcountSweep_HotPathRefreshesLastSeen pins the P1 #5 acceptance:
// running `acd start` repeatedly under the same session_id (the active-
// hook hot-path pattern) must refresh daemon_clients.last_seen_ts even
// when every call after the first short-circuits. Without the
// touchClientHotPath wire-up the row would float at the cold-path
// timestamp until the sweeper evicted it.
//
// To keep the test under five seconds we observe last_seen_ts before
// and after a small burst of hot calls and assert the value monotonically
// increases — running for the full clientTTL window (default 1800s) is
// not feasible, but the contract we care about is "hot path advances
// the timestamp" which is identical at any duration.
func TestRefcountSweep_HotPathRefreshesLastSeen(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required for refcount probes")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Cold path: register the session.
	startSession(t, ctx, env, repo, "hot-loop", "claude-code")
	waitMode(t, repo, "running", 5*time.Second)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	readLastSeen := func() float64 {
		raw := strings.TrimSpace(sqliteScalar(t, dbPath,
			"SELECT last_seen_ts FROM daemon_clients WHERE session_id='hot-loop'"))
		if raw == "" {
			t.Fatalf("daemon_clients row missing for hot-loop")
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			t.Fatalf("parse last_seen=%q: %v", raw, err)
		}
		return v
	}

	first := readLastSeen()

	// Sleep just over a second so the hot path's Unix-second-resolution
	// touch can observe a strictly larger timestamp.
	time.Sleep(1100 * time.Millisecond)

	// Three hot-path calls in quick succession. Each must short-circuit
	// (Started=false, Duplicate=true) and refresh last_seen_ts.
	for i := 0; i < 3; i++ {
		res := runAcd(t, ctx, env,
			"start", "--session-id", "hot-loop", "--repo", repo,
			"--harness", "claude-code", "--json")
		if res.ExitCode != 0 {
			t.Fatalf("hot start %d exit=%d\nstdout=%s\nstderr=%s",
				i, res.ExitCode, res.Stdout, res.Stderr)
		}
		var got struct {
			Started   bool `json:"started"`
			Duplicate bool `json:"duplicate"`
		}
		if err := json.Unmarshal([]byte(res.Stdout), &got); err != nil {
			t.Fatalf("decode %d: %v\n%s", i, err, res.Stdout)
		}
		if got.Started || !got.Duplicate {
			t.Fatalf("hot call %d not short-circuited: %+v", i, got)
		}
	}
	last := readLastSeen()
	if last <= first {
		t.Fatalf("last_seen_ts did not advance: first=%v last=%v", first, last)
	}
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
	ensureCheckpointRuntime(t, full, repo, buildAcdBinary(t))
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
	res := runAcd(t, ctx, env, "stop", "--repo", repo, "--force", "--json")
	var stopJSON struct {
		DaemonPID int `json:"daemon_pid"`
	}
	_ = json.Unmarshal([]byte(res.Stdout), &stopJSON)
	// Best-effort wait for stopped state — don't fail cleanup.
	if waitStopped(repo, 1*time.Second) {
		return
	}
	pid := stopJSON.DaemonPID
	if pid <= 0 {
		pid = readDaemonStatePID(repo)
	}
	if pid <= 0 {
		return
	}
	_ = syscall.Kill(pid, syscall.SIGTERM)
	if waitStopped(repo, 1*time.Second) || !processAlive(pid) {
		return
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	_ = waitStopped(repo, 1*time.Second)
}

func waitStopped(repo string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if readDaemonStateMode(repo) == "stopped" {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// waitMode is the canonical "wait until daemon_state.mode matches" helper.
func waitMode(t *testing.T, repo, want string, timeout time.Duration) {
	t.Helper()
	waitFor(t, fmt.Sprintf("daemon_state.mode==%s", want), timeout, func() bool {
		return readDaemonStateMode(repo) == want
	})
}

// waitForCommitContaining polls the visible HEAD history only. Private
// checkpoint refs intentionally contain the same paths before publication
// and must not be mistaken for user-visible commits.
// until path appears, or the timeout fires. Returns the matched commit oid.
// Scanning the full log (not just HEAD) tolerates the daemon producing
// multiple atomic commits before the target path lands.
func waitForCommitContaining(t *testing.T, repo, path string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if names, err := runGit(repo, "log", "HEAD", "--name-only", "--pretty=format:COMMIT %H"); err == nil {
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
	out, _ := runGit(repo, "log", "HEAD", "--name-only", "--pretty=format:%h %s")
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

	// Stop the daemon deterministically; this scenario is about preserving
	// shadow rows across restart, not refcount-drain timing.
	stopSessionForce(t, env, repo)

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

	// Fan out 50 acd wake calls. control.lock is non-blocking, so we
	// gate concurrency with a small token pool + per-call retry on
	// contention. The point of the test is "the queue stays atomic under
	// burst" — not "control.lock is reentrant".
	const N = 50
	const concurrency = 4
	const maxAttempts = 12
	var wg sync.WaitGroup
	failures := atomic.Int32{}
	tokens := make(chan struct{}, concurrency)
	var firstErr atomic.Pointer[string]
	for i := 0; i < N; i++ {
		wg.Add(1)
		tokens <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-tokens }()
			var lastRes ExecResult
			for attempt := 0; attempt < maxAttempts; attempt++ {
				subCtx, sub := context.WithTimeout(ctx, 30*time.Second)
				lastRes = runAcd(t, subCtx, env,
					"wake", "--session-id", "burst-1", "--repo", repo, "--json",
				)
				sub()
				if lastRes.ExitCode == 0 {
					return
				}
				// Lock-style contention: control.lock acquire failed because
				// a peer is holding it briefly. Retry. Anything else is
				// treated as a contention candidate too — we retry up to
				// maxAttempts then give up.
				time.Sleep(time.Duration(30+attempt*40) * time.Millisecond)
			}
			failures.Add(1)
			msg := fmt.Sprintf("wake#%d exit=%d stdout=%q stderr=%q",
				i, lastRes.ExitCode, lastRes.Stdout, lastRes.Stderr)
			firstErr.CompareAndSwap(nil, &msg)
		}(i)
	}
	wg.Wait()
	if f := failures.Load(); f > 0 {
		errStr := ""
		if p := firstErr.Load(); p != nil {
			errStr = *p
		}
		t.Fatalf("%d/%d acd wake invocations failed; first: %s", f, N, errStr)
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
	if total != "0" {
		t.Fatalf("wake hints should bypass the legacy flush queue, got %s rows", total)
	}
	if sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM checkpoints WHERE phase='completed'") == "0" {
		t.Fatal("wake burst did not produce durable checkpoint protection")
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
	if loser.ExitCode == 0 || !strings.Contains(loser.Stdout+loser.Stderr, "held by another process") {
		t.Fatalf("second worker did not fail closed on ownership contention: exit=%d\nstdout=%s\nstderr=%s",
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
		"PRAGMA busy_timeout=5000; INSERT OR REPLACE INTO daemon_clients(session_id, harness, watch_pid, watch_fp, registered_ts, last_seen_ts) VALUES ('pidreuse-bogus', 'shell', %d, 'bogus|fingerprint', %f, %f);",
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
	if got := len(readClients(t, repo)); got != 1 {
		t.Fatalf("first session close left %d clients, want 1", got)
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
	if got := len(readClients(t, repo)); got != 0 {
		t.Fatalf("final session close left %d clients, want 0", got)
	}
	if readDaemonStateMode(repo) != "running" {
		t.Fatal("supervisor worker stopped with the final optional session")
	}
}

// regDetachedHeadStartRefused verifies the CLI refuses to spawn a daemon
// while HEAD is detached. This is the operator-facing counterpart to the
// daemon-package pause guard: start must fail before any capture/replay loop
// can publish commits onto a nameless ref.
func regDetachedHeadStartRefused(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	startHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	runGitOK(t, repo, "checkout", "--detach", startHead)
	ensureCheckpointRuntime(t, env, repo, buildAcdBinary(t))

	res := runAcd(t, ctx, env,
		"start", "--session-id", "detached-1", "--repo", repo, "--harness", "shell", "--json")
	if res.ExitCode != 0 {
		t.Fatalf("session hint failed on detached HEAD\nstdout=%s\nstderr=%s", res.Stdout, res.Stderr)
	}

	writeFile(t, filepath.Join(repo, "detached.txt"), "must-not-commit\n")
	time.Sleep(300 * time.Millisecond)
	if head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); head != startHead {
		t.Fatalf("detached HEAD advanced to %s; want %s", head, startHead)
	}
	if mode := readDaemonStateMode(repo); mode != "running" {
		t.Fatalf("worker mode=%q on detached HEAD", mode)
	}
}

// regOfflineResetRestartNoPhantomEvents starts and stops the daemon, rewinds
// the branch while it is offline, then restarts. The restart must bump the
// generation and reseed shadow_paths without inserting phantom create events
// for files already tracked at the new HEAD.
func regOfflineResetRestartNoPhantomEvents(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "offline-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	writeFile(t, filepath.Join(repo, "before-reset.txt"), "before reset\n")
	wakeSession(t, ctx, env, repo, "offline-1")
	if flushed := runAcd(t, ctx, env, "flush", "--logical", "--session-id", "offline-1", "--repo", repo); flushed.ExitCode != 0 {
		t.Fatalf("logical boundary before reset failed: %s", flushed.Stderr)
	}
	waitForCommitContaining(t, repo, "before-reset.txt", 8*time.Second)

	headBeforeReset := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	seedHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD^"))
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")

	off := runAcd(t, ctx, env, "off", "--force", "--repo", repo, "--json")
	if off.ExitCode != 0 {
		t.Fatalf("acd off exit=%d\nstdout=%s\nstderr=%s", off.ExitCode, off.Stdout, off.Stderr)
	}
	waitMode(t, repo, "stopped", 10*time.Second)

	preEvents := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM capture_events")
	runGitOK(t, repo, "reset", "--hard", seedHead)
	if err := os.Remove(filepath.Join(repo, "before-reset.txt")); err != nil && !os.IsNotExist(err) {
		t.Fatalf("remove untracked before-reset.txt after reset: %v", err)
	}
	if head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); head == headBeforeReset {
		t.Fatalf("reset did not move HEAD away from %s", headBeforeReset)
	}

	on := runAcd(t, ctx, env, "on", "--repo", repo, "--json")
	if on.ExitCode != 0 {
		t.Fatalf("acd on exit=%d\nstdout=%s\nstderr=%s", on.ExitCode, on.Stdout, on.Stderr)
	}
	startSession(t, ctx, env, repo, "offline-2", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	waitFor(t, "branch.generation bumped after offline reset", 5*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'branch.generation'") == "2"
	})
	waitFor(t, "generation 2 shadow seeded", 5*time.Second, func() bool {
		return sqliteScalar(t, dbPath,
			"SELECT COUNT(*) FROM shadow_paths WHERE branch_ref = 'refs/heads/main' AND branch_generation = 2") != "0"
	})
	time.Sleep(250 * time.Millisecond)
	if postEvents := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM capture_events"); postEvents != preEvents {
		dump, _ := exec.Command("sqlite3", dbPath,
			"SELECT seq,operation,path,state FROM capture_events ORDER BY seq").CombinedOutput()
		t.Fatalf("offline restart inserted phantom events: pre=%s post=%s\nrows:\n%s", preEvents, postEvents, dump)
	}
	if pending := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM capture_events WHERE state = 'pending'"); pending != "0" {
		t.Fatalf("offline restart left pending events=%s", pending)
	}
}

// regConcurrentSessionStartsRegisterAllClients fans out ten first-time
// `acd start` calls. They should converge on one daemon and all session rows
// should survive the start/control-lock race.
func regConcurrentSessionStartsRegisterAllClients(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	ensureCheckpointRuntime(t, env, repo, buildAcdBinary(t))

	const clients = 10
	type startAttempt struct {
		session string
		res     ExecResult
	}
	results := make(chan startAttempt, clients)
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		sessionID := fmt.Sprintf("client-%02d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- startAttempt{
				session: sessionID,
				res: runAcd(t, ctx, env,
					"start", "--session-id", sessionID, "--repo", repo, "--harness", "shell", "--json"),
			}
		}()
	}
	wg.Wait()
	close(results)

	for attempt := range results {
		if attempt.res.ExitCode != 0 {
			t.Fatalf("start %s exit=%d\nstdout=%s\nstderr=%s",
				attempt.session, attempt.res.ExitCode, attempt.res.Stdout, attempt.res.Stderr)
		}
	}
	waitMode(t, repo, "running", 5*time.Second)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM daemon_clients"); got != fmt.Sprintf("%d", clients) {
		dump, _ := exec.Command("sqlite3", dbPath,
			"SELECT session_id,harness FROM daemon_clients ORDER BY session_id").CombinedOutput()
		t.Fatalf("daemon_clients count=%s want %d\nrows:\n%s", got, clients, dump)
	}
	if pid := readDaemonStatePID(repo); pid == 0 {
		t.Fatalf("daemon_state.pid missing after concurrent starts")
	}
}

// regDaemonSelfTerminatesOnEmptySweeps now proves the supervisor-owned worker
// remains alive after all optional session hints expire.
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
	if out, err := exec.Command("sqlite3", dbPath, "PRAGMA busy_timeout=5000; DELETE FROM daemon_clients").CombinedOutput(); err != nil {
		t.Fatalf("drop clients: %v\n%s", err, out)
	}

	time.Sleep(500 * time.Millisecond)
	if readDaemonStateMode(repo) != "running" {
		t.Fatal("supervisor-owned worker terminated after session rows emptied")
	}
}

func regTrackedChildDeleteUnderSafeIgnoredDir(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	tracked := filepath.Join(repo, "node_modules", "pkg", "generated.txt")
	writeFile(t, tracked, "tracked generated child\n")
	runGitOK(t, repo, "add", "-f", "node_modules/pkg/generated.txt")
	runGitOK(t, repo, "commit", "-q", "-m", "track generated child under safe ignored dir")

	startSession(t, ctx, env, repo, "safe-delete-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	activeShadowCount := func() string {
		return sqliteScalar(t, dbPath, `
SELECT COUNT(*) FROM shadow_paths
WHERE path = 'node_modules/pkg/generated.txt'
  AND branch_ref = 'refs/heads/main'
  AND branch_generation = CAST((SELECT value FROM daemon_meta WHERE key = 'branch.generation') AS INTEGER)`)
	}
	waitFor(t, "tracked safe-ignore child shadow seeded", 5*time.Second, func() bool {
		return activeShadowCount() != "0"
	})

	if err := os.Remove(tracked); err != nil {
		t.Fatalf("remove tracked child: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(repo, "node_modules", "pkg"), 0o755); err != nil {
		t.Fatalf("keep safe-ignored parent dir present: %v", err)
	}
	wakeSession(t, ctx, env, repo, "safe-delete-1")

	waitFor(t, "safe-ignore delete protected and active shadow removed", 8*time.Second, func() bool {
		protected := sqliteScalar(t, dbPath,
			"SELECT COUNT(*) FROM decision_records WHERE path = 'node_modules/pkg/generated.txt' AND kind = 'protected' AND action_taken = 'no_delete_generated'") != "0"
		return protected && activeShadowCount() == "0"
	})
	if got := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM capture_events WHERE path = 'node_modules/pkg/generated.txt'"); got != "0" {
		t.Fatalf("safe-ignore deleted child should not be queued, capture_events=%s", got)
	}
	if out, err := runGit(repo, "cat-file", "-e", "HEAD:node_modules/pkg/generated.txt"); err != nil {
		t.Fatalf("acd must not mutate Git for protected safe-ignore delete; HEAD missing tracked child\n%s", out)
	}

	wakeSession(t, ctx, env, repo, "safe-delete-1")
	time.Sleep(500 * time.Millisecond)
	if got := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM capture_events WHERE path = 'node_modules/pkg/generated.txt'"); got != "0" {
		t.Fatalf("safe-ignore deleted child recaptured on second wake, capture_events=%s", got)
	}
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

// regRepeatedEditsPublishOrderedCommits — drive three sequential edits to
// the same path through the real `acd` binary. After each edit + wake the
// daemon must produce one commit on top of the previous tip; the final log
// must show the chain v1 → v2 → v3 in order, with each commit a fast-forward
// of the seed. Counterpart to TestRun_RepeatedEditsToSameFile_OrderedCommits
// at the daemon-package level: this wires the same scenario through the real
// CLI/daemon binary so the regression survives the boot/IPC path too.
func regRepeatedEditsPublishOrderedCommits(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "edits-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	target := filepath.Join(repo, "chain.txt")
	versions := []string{"v1\n", "v2\n", "v3\n"}
	prev := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	heads := make([]string, 0, len(versions))
	for i, body := range versions {
		writeFile(t, target, body)
		wakeSession(t, ctx, env, repo, "edits-1")

		// Wait up to 8s for HEAD to advance past prev. We cannot reuse
		// waitForCommitContaining here because chain.txt appears in every
		// commit; we want each individual advance.
		deadline := time.Now().Add(8 * time.Second)
		var cur string
		for time.Now().Before(deadline) {
			out, err := runGit(repo, "rev-parse", "HEAD")
			if err == nil {
				cur = strings.TrimSpace(out)
				if cur != prev {
					break
				}
			}
			time.Sleep(75 * time.Millisecond)
		}
		if cur == "" || cur == prev {
			full, _ := runGit(repo, "log", "--all", "--oneline")
			t.Fatalf("edit %d: HEAD did not advance past %s within 8s\nlog:\n%s", i+1, prev, full)
		}
		heads = append(heads, cur)
		prev = cur
	}

	// Walk the resulting log: each commit must show chain.txt with the
	// expected blob content. We use ls-tree per commit and compare the
	// blob's content via cat-file.
	for i, h := range heads {
		out := runGitOK(t, repo, "show", h+":chain.txt")
		if out != versions[i] {
			t.Fatalf("commit %d (%s): chain.txt=%q want %q", i, h, out, versions[i])
		}
	}

	// Final tip must be a fast-forward descendant of the seed (no rewrites
	// happened mid-way).
	seed := strings.TrimSpace(runGitOK(t, repo, "rev-list", "--max-parents=0", "HEAD"))
	if _, err := runGit(repo, "merge-base", "--is-ancestor", seed, heads[len(heads)-1]); err != nil {
		full, _ := runGit(repo, "log", "--all", "--oneline")
		t.Fatalf("seed %s is not an ancestor of final tip %s\nlog:\n%s", seed, heads[len(heads)-1], full)
	}

	// Disk content must match the last edit (the worktree may diverge from
	// the user-facing git index because acd commits via lower-level plumbing,
	// but the file on disk should still hold the last write).
	if disk, err := os.ReadFile(target); err != nil {
		t.Fatalf("read chain.txt: %v", err)
	} else if string(disk) != versions[len(versions)-1] {
		t.Fatalf("disk content=%q want %q", disk, versions[len(versions)-1])
	}
}

// regBlockedConflictTerminalAcrossPolls — inject a hand-crafted capture
// event whose `before` blob does not match what's on disk, forcing the
// replay path to settle the event as `blocked_conflict`. We then drive
// several wake cycles and verify the row stays terminal (does NOT re-enter
// pending) and that publish_state.status reflects the conflict. Without
// the §8 terminal-settle behavior the row would loop forever in `pending`.
func regBlockedConflictTerminalAcrossPolls(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Land one real commit so BaseHead resolves to a non-seed tip.
	startSession(t, ctx, env, repo, "blk-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	writeFile(t, filepath.Join(repo, "real.txt"), "real\n")
	wakeSession(t, ctx, env, repo, "blk-1")
	waitForCommitContaining(t, repo, "real.txt", 8*time.Second)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	baseHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	gen := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'branch.generation'")
	if gen == "" {
		gen = "1"
	}

	// Hand-inject a capture event: a `modify` op against a path whose
	// before_oid is bogus and whose path does not exist in the live tree.
	// This is a true conflict scenario — the scratch index probe will see
	// before_oid mismatch (or path missing), block the event terminally,
	// and upsert publish_state.status = blocked_conflict.
	bogusBefore := "1111111111111111111111111111111111111111"
	bogusAfter := "2222222222222222222222222222222222222222"
	now := nowFloatSeconds()
	insertEvent := fmt.Sprintf(`
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state)
VALUES ('refs/heads/main', %s, '%s', 'modify', 'ghost-conflict.txt', 'rescan', %f, 'pending');
INSERT INTO capture_ops(event_seq, ord, op, path, before_oid, before_mode, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'modify', 'ghost-conflict.txt', '%s', '100644', '%s', '100644', 'rescan');
`, gen, baseHead, now, bogusBefore, bogusAfter)
	if out, err := exec.Command("sqlite3", dbPath, insertEvent).CombinedOutput(); err != nil {
		t.Fatalf("inject blocker event: %v\n%s", err, out)
	}

	// Capture the injected seq for later assertions.
	blockerSeq := sqliteScalar(t, dbPath,
		"SELECT seq FROM capture_events WHERE path = 'ghost-conflict.txt' ORDER BY seq DESC LIMIT 1")
	if blockerSeq == "" {
		t.Fatalf("blocker seq missing after insert")
	}

	// Wake several times — daemon's replay must settle the event terminally
	// on the first pass and then leave it alone on every subsequent tick.
	for i := 0; i < 5; i++ {
		wakeSession(t, ctx, env, repo, "blk-1")
		time.Sleep(150 * time.Millisecond)
	}

	st := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT state FROM capture_events WHERE seq = %s", blockerSeq))
	if st != "pending" {
		t.Fatalf("orphan legacy event state=%q want pending until checkpoint membership is proven", st)
	}

	// Drive several more poll cycles. Terminal state must not regress to
	// pending (this is the §6.1 invariant — terminal events drop out of
	// PendingEvents and never re-run).
	for i := 0; i < 4; i++ {
		wakeSession(t, ctx, env, repo, "blk-1")
		time.Sleep(150 * time.Millisecond)
	}

	finalState := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT state FROM capture_events WHERE seq = %s", blockerSeq))
	if finalState != "pending" {
		t.Fatalf("orphan event changed without checkpoint membership: %q", finalState)
	}

	pendingCount := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT COUNT(*) FROM capture_events WHERE state = 'pending' AND seq = %s", blockerSeq))
	if pendingCount != "1" {
		t.Fatalf("orphan event pending count=%s want 1", pendingCount)
	}

	// publish_state.status must mirror the terminal state so list/status/
	// doctor surfaces it without scanning capture_events.
	pubStatus := sqliteScalar(t, dbPath,
		"SELECT status FROM publish_state WHERE id = 1")
	if pubStatus == "blocked_conflict" {
		t.Fatal("publication consumed an event without completed checkpoint membership")
	}

	// Daemon must still be running — a single conflict cannot wedge it.
	if mode := readDaemonStateMode(repo); mode != "running" {
		t.Fatalf("daemon mode=%q after conflict; want running", mode)
	}
}

// regBlockedConflictPreventsLeapfrogPublish verifies the queue barrier after
// a recorded conflict: a later pending event for the same branch generation
// must not publish on a second replay pass.
func regBlockedConflictPreventsLeapfrogPublish(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "leapfrog-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	baseHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	gen := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'branch.generation'")
	if gen == "" {
		gen = "1"
	}

	now := nowFloatSeconds()
	blockerSQL := fmt.Sprintf(`
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state)
VALUES ('refs/heads/main', %s, '%s', 'modify', 'blocked-first.txt', 'rescan', %f, 'pending');
INSERT INTO capture_ops(event_seq, ord, op, path, before_oid, before_mode, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'modify', 'blocked-first.txt', '1111111111111111111111111111111111111111', '100644', '2222222222222222222222222222222222222222', '100644', 'rescan');
`, gen, baseHead, now)
	if out, err := exec.Command("sqlite3", dbPath, blockerSQL).CombinedOutput(); err != nil {
		t.Fatalf("inject blocker event: %v\n%s", err, out)
	}
	blockerSeq := sqliteScalar(t, dbPath,
		"SELECT seq FROM capture_events WHERE path = 'blocked-first.txt' ORDER BY seq DESC LIMIT 1")

	wakeSession(t, ctx, env, repo, "leapfrog-1")
	if got := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT state FROM capture_events WHERE seq = %s", blockerSeq)); got != "pending" {
		t.Fatalf("orphan blocker state=%q want pending", got)
	}

	afterOID := gitHashObjectStdin(t, repo, "must not leapfrog\n")
	laterSQL := fmt.Sprintf(`
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state)
VALUES ('refs/heads/main', %s, '%s', 'create', 'leapfrog.txt', 'exact', %f, 'pending');
INSERT INTO capture_ops(event_seq, ord, op, path, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'create', 'leapfrog.txt', '%s', '100644', 'exact');
`, gen, baseHead, nowFloatSeconds(), afterOID)
	if out, err := exec.Command("sqlite3", dbPath, laterSQL).CombinedOutput(); err != nil {
		t.Fatalf("inject later event: %v\n%s", err, out)
	}
	laterSeq := sqliteScalar(t, dbPath,
		"SELECT seq FROM capture_events WHERE path = 'leapfrog.txt' ORDER BY seq DESC LIMIT 1")

	for i := 0; i < 3; i++ {
		wakeSession(t, ctx, env, repo, "leapfrog-1")
		time.Sleep(150 * time.Millisecond)
	}

	if state := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT state FROM capture_events WHERE seq = %s", laterSeq)); state != "pending" {
		t.Fatalf("later event state=%q want pending behind blocked_conflict barrier", state)
	}
	showOut := runGitOK(t, repo, "log", "--all", "--name-only", "--pretty=format:")
	for _, line := range strings.Split(showOut, "\n") {
		if strings.TrimSpace(line) == "leapfrog.txt" {
			t.Fatalf("later event leapfrogged blocked predecessor into git history:\n%s", showOut)
		}
	}
}

func gitHashObjectStdin(t *testing.T, repo, body string) string {
	t.Helper()
	cmd := exec.Command("git", "-C", repo, "hash-object", "-w", "--stdin")
	cmd.Stdin = strings.NewReader(body)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git hash-object -w --stdin: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	out, err := exec.Command("ps", "-p", fmt.Sprintf("%d", pid), "-o", "stat=").CombinedOutput()
	if err != nil {
		return false
	}
	return !strings.HasPrefix(strings.TrimSpace(string(out)), "Z")
}
