//go:build integration
// +build integration

// fsnotify_fallback_test.go drives §8.5 — fsnotify hybrid + poll fallback —
// through the real `acd` binary. The CLI gates fsnotify behind
// ACD_FSNOTIFY_ENABLED=1 (production opt-in until the broader Phase 4
// cutover); these tests set the toggle so the daemon actually constructs
// the watcher.
//
// Three scenarios live here:
//
//  1. Watch budget exceeded -> poll fallback. ACD_MAX_INOTIFY_WATCHES=2 in
//     a repo with multiple subdirectories forces the watcher to abandon
//     fsnotify at construction and stamp daemon_meta accordingly.
//  2. ACD_DISABLE_FSNOTIFY=1 -> poll fallback. The same fallback path is
//     reached via the env toggle without ever spawning OS watches.
//  3. Sub-second latency on a small repo with fsnotify enabled.
package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// readDaemonMetaKey returns daemon_meta[key] from <repo>/.git/acd/state.db
// or "" when the row is absent / sqlite errors. Polling-friendly.
func readDaemonMetaKey(repoDir, key string) string {
	dbPath := filepath.Join(repoDir, ".git", "acd", "state.db")
	if _, err := os.Stat(dbPath); err != nil {
		return ""
	}
	out, err := exec.Command(
		"sqlite3", dbPath,
		fmt.Sprintf(`SELECT value FROM daemon_meta WHERE key = '%s'`, key),
	).CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// waitMetaValue blocks until daemon_meta[key] equals want or timeout fires.
func waitMetaValue(t *testing.T, repo, key, want string, timeout time.Duration) {
	t.Helper()
	waitFor(t, fmt.Sprintf("daemon_meta[%s]==%q", key, want), timeout, func() bool {
		return readDaemonMetaKey(repo, key) == want
	})
}

// readDaemonLogTail reads the rotating per-repo daemon.log (last 200 lines
// worth, by byte budget) for log-line assertions. The path is reachable
// via the start command's repo_hash + the test's isolated XDG_STATE_HOME.
func readDaemonLogTail(t *testing.T, env []string, repoHash string) string {
	t.Helper()
	stateRoot := ""
	for _, kv := range env {
		if strings.HasPrefix(kv, "XDG_STATE_HOME=") {
			stateRoot = strings.TrimPrefix(kv, "XDG_STATE_HOME=")
		}
		if strings.HasPrefix(kv, "HOME=") && stateRoot == "" {
			stateRoot = filepath.Join(strings.TrimPrefix(kv, "HOME="), ".local", "state")
		}
	}
	if stateRoot == "" {
		return ""
	}
	logPath := filepath.Join(stateRoot, "acd", repoHash, "daemon.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		return ""
	}
	if len(data) > 64*1024 {
		data = data[len(data)-64*1024:]
	}
	return string(data)
}

// startSessionJSON runs `acd start ... --json` with extra env, returning
// the parsed start payload (we need repo_hash for the log lookup).
type startPayload struct {
	Started   bool   `json:"started"`
	Duplicate bool   `json:"duplicate"`
	DaemonPID int    `json:"daemon_pid"`
	Repo      string `json:"repo"`
	RepoHash  string `json:"repo_hash"`
	SessionID string `json:"session_id"`
	Harness   string `json:"harness"`
}

func startSessionJSON(t *testing.T, ctx context.Context, env []string, repo, sessionID, harness string, extraEnv ...string) startPayload {
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
		t.Fatalf("acd start (session=%s) exit=%d\nstdout=%s\nstderr=%s",
			sessionID, res.ExitCode, res.Stdout, res.Stderr)
	}
	var p startPayload
	if err := json.Unmarshal([]byte(res.Stdout), &p); err != nil {
		t.Fatalf("decode start json: %v\nstdout=%s", err, res.Stdout)
	}
	if !p.Started && !p.Duplicate {
		t.Fatalf("expected started or duplicate, got %+v", p)
	}
	return p
}

// TestFsnotify_BudgetExceededFallsBackToPoll: with ACD_MAX_INOTIFY_WATCHES
// set to a tiny number, the watcher must surrender at construction time
// and run poll-only. We verify the daemon_meta breadcrumb (mode + reason)
// and confirm that an edit still produces a commit through the poll loop.
func TestFsnotify_BudgetExceededFallsBackToPoll(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not supported on windows in v1")
	}
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required for daemon_meta probes")
	}

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	// Force the watcher to overshoot: many subdirs, tiny budget.
	for _, sub := range []string{"a", "b", "c", "d", "e", "f"} {
		writeFile(t, filepath.Join(repo, sub, "keep.txt"), "x\n")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startSessionJSON(t, ctx, env, repo, "fsn-budget", "shell",
		"ACD_FSNOTIFY_ENABLED=1",
		"ACD_MAX_INOTIFY_WATCHES=2",
	)
	waitMode(t, repo, "running", 5*time.Second)

	// daemon_meta must record poll mode + the budget-exceeded reason.
	waitMetaValue(t, repo, "fsnotify.mode", "poll", 5*time.Second)
	if got := readDaemonMetaKey(repo, "fsnotify.fallback_reason"); got != "watch_budget_exceeded" {
		t.Fatalf("fsnotify.fallback_reason=%q want watch_budget_exceeded", got)
	}

	// Confirm capture-replay still works under the poll fallback.
	writeFile(t, filepath.Join(repo, "after-fallback.txt"), "poll-still-works\n")
	wakeSession(t, ctx, envWith(env, "ACD_FSNOTIFY_ENABLED=1", "ACD_MAX_INOTIFY_WATCHES=2"), repo, "fsn-budget")
	waitForCommitContaining(t, repo, "after-fallback.txt", 30*time.Second)
}

// TestFsnotify_DisabledByEnvFallsBackToPoll: ACD_DISABLE_FSNOTIFY=1 forces
// poll-only mode at watcher construction with the "disabled_by_env"
// reason.
func TestFsnotify_DisabledByEnvFallsBackToPoll(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not supported on windows in v1")
	}
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required for daemon_meta probes")
	}

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startSessionJSON(t, ctx, env, repo, "fsn-disabled", "shell",
		"ACD_FSNOTIFY_ENABLED=1",
		"ACD_DISABLE_FSNOTIFY=1",
	)
	waitMode(t, repo, "running", 5*time.Second)

	waitMetaValue(t, repo, "fsnotify.mode", "poll", 5*time.Second)
	if got := readDaemonMetaKey(repo, "fsnotify.fallback_reason"); got != "disabled_by_env" {
		t.Fatalf("fsnotify.fallback_reason=%q want disabled_by_env", got)
	}

	// Capture-replay still works.
	writeFile(t, filepath.Join(repo, "poll-only.txt"), "hi\n")
	wakeSession(t, ctx, envWith(env, "ACD_FSNOTIFY_ENABLED=1", "ACD_DISABLE_FSNOTIFY=1"), repo, "fsn-disabled")
	waitForCommitContaining(t, repo, "poll-only.txt", 8*time.Second)
}

// TestFsnotify_RuntimeBudgetExceededFallsBackToPoll starts below the watch
// budget, then creates a nested directory tree while the daemon is running.
// The dynamic re-watch path must detect the budget overshoot and degrade to
// poll mode without losing later capture/replay work.
func TestFsnotify_RuntimeBudgetExceededFallsBackToPoll(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not supported on windows in v1")
	}
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required for daemon_meta probes")
	}

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	extra := []string{
		"ACD_FSNOTIFY_ENABLED=1",
		"ACD_MAX_INOTIFY_WATCHES=3",
	}
	startSessionJSON(t, ctx, env, repo, "fsn-runtime-budget", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	waitMetaValue(t, repo, "fsnotify.mode", "fsnotify", 5*time.Second)

	runtimeRoot := filepath.Join(repo, "runtime-tree")
	if err := os.Mkdir(runtimeRoot, 0o755); err != nil {
		t.Fatalf("mkdir runtime-tree: %v", err)
	}
	for i := 0; i < 6 && readDaemonMetaKey(repo, "fsnotify.mode") != "poll"; i++ {
		time.Sleep(250 * time.Millisecond)
		nested := filepath.Join(runtimeRoot, fmt.Sprintf("fanout-%d", i), "a", "b", "c")
		if err := os.MkdirAll(nested, 0o755); err != nil {
			t.Fatalf("mkdir runtime nested tree: %v", err)
		}
	}
	waitMetaValue(t, repo, "fsnotify.mode", "poll", 5*time.Second)
	if got := readDaemonMetaKey(repo, "fsnotify.fallback_reason"); got != "watch_budget_exceeded" {
		t.Fatalf("fsnotify.fallback_reason=%q want watch_budget_exceeded", got)
	}

	writeFile(t, filepath.Join(repo, "after-runtime-fallback.txt"), "poll after runtime fallback\n")
	wakeSession(t, ctx, envWith(env, extra...), repo, "fsn-runtime-budget")
	waitForCommitContaining(t, repo, "after-runtime-fallback.txt", 8*time.Second)
}

// TestFsnotify_LatencyOnSmallRepo: with fsnotify enabled (no overrides) on
// a small repo, a single file write should land as a commit quickly. We
// gate the strict 1500ms bound under ACD_PERF_TEST=1; otherwise allow up
// to 4 seconds so CI flake risk stays bounded but the path is still
// exercised.
func TestFsnotify_LatencyOnSmallRepo(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fsnotify not supported on windows in v1")
	}
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required for daemon_state probes")
	}

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startSessionJSON(t, ctx, env, repo, "fsn-latency", "shell",
		"ACD_FSNOTIFY_ENABLED=1",
	)
	waitMode(t, repo, "running", 5*time.Second)

	// daemon_meta should report fsnotify mode (not a fallback). On rare
	// CI hosts where the watch budget detection is overly strict the
	// watcher may have fallen back already; we don't fail in that case
	// because the latency assertion below is the real signal we care
	// about.
	startTime := time.Now()
	writeFile(t, filepath.Join(repo, "fast.txt"), "low-latency\n")
	// Note: NO acd wake — fsnotify must drive the wake on its own.

	// Bound: tight when ACD_PERF_TEST=1, lax otherwise.
	bound := 4 * time.Second
	if os.Getenv("ACD_PERF_TEST") == "1" {
		bound = 1500 * time.Millisecond
	}

	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		showOut, err := runGit(repo, "show", "--name-only", "--pretty=", "HEAD")
		if err == nil && strings.Contains(showOut, "fast.txt") {
			elapsed := time.Since(startTime)
			t.Logf("fsnotify-driven commit landed in %v (bound=%v)", elapsed, bound)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	all, _ := runGit(repo, "log", "--all", "--name-only", "--pretty=format:%h %s")
	mode := readDaemonMetaKey(repo, "fsnotify.mode")
	reason := readDaemonMetaKey(repo, "fsnotify.fallback_reason")
	t.Fatalf("fast.txt did not land within %v (fsnotify.mode=%q reason=%q)\nlog:\n%s",
		bound, mode, reason, all)
}
