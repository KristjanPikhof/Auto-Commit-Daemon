//go:build integration
// +build integration

// deadlock_probe_test.go is the safety-net regression for the cr-expert
// finding: under populated state the boot-loop's bounded channels could in
// principle deadlock on a slow first capture pass. The probe drives the real
// `acd` binary through a full lifecycle (start + wake bursts + stop) under a
// 30s wall-clock budget. If the daemon binary fails to exit within the
// budget, we send SIGQUIT to the daemon process so the Go runtime dumps
// every goroutine to the test artifact, then fail the test with the dump
// attached for diagnosis.
package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestDaemon_GoroutineDeadlockProbe runs the binary across enough state
// surface that any leftover deadlock paths surface quickly. Acceptance:
//
//   - Full start → wake burst → stop completes within 30s wall clock.
//   - On hang: SIGQUIT the daemon, capture the stack dump as a test
//     artifact, and t.Fatalf so CI flags it.
func TestDaemon_GoroutineDeadlockProbe(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	// Hard budget: 30s for the entire lifecycle. If we blow past it, we go
	// straight to the goroutine dump.
	const budget = 30 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), budget+10*time.Second)
	defer cancel()

	// Phase 1 — start the daemon. We keep the trace dir for diagnostic
	// breadcrumbs even though the assertion does not depend on it.
	traceDir := filepath.Join(repo, ".git", "acd", "trace-test")
	if err := os.MkdirAll(traceDir, 0o755); err != nil {
		t.Fatalf("mkdir trace dir: %v", err)
	}
	traceEnv := envWith(env, "ACD_TRACE=1", "ACD_TRACE_DIR="+traceDir)

	startSession(t, ctx, traceEnv, repo, "deadlock-1", "shell",
		"ACD_TRACE=1", "ACD_TRACE_DIR="+traceDir)
	waitMode(t, repo, "running", 10*time.Second)
	daemonPID := readDaemonStatePID(repo)
	if daemonPID <= 0 {
		t.Fatalf("daemon PID missing after start")
	}

	// Phase 2 — drive a small wake burst + a few writes so the run loop
	// exercises capture, replay, and refcount sweeps. Anything that
	// deadlocks in the wild needs more than zero work to expose itself.
	for i := 0; i < 5; i++ {
		writeFile(t, filepath.Join(repo, fmt.Sprintf("dead-%02d.txt", i)),
			fmt.Sprintf("body %d\n", i))
		wakeSession(t, ctx, traceEnv, repo, "deadlock-1")
	}

	// Phase 3 — issue stop with the full budget. Run it on a goroutine so
	// the test process can either celebrate a fast stop or fall through to
	// the SIGQUIT path on timeout.
	stopDone := make(chan error, 1)

	go func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), budget)
		defer stopCancel()
		res := runAcd(t, stopCtx, traceEnv, "stop", "--repo", repo, "--json")
		if res.ExitCode != 0 {
			stopDone <- fmt.Errorf("acd stop exit=%d\nstdout=%s\nstderr=%s",
				res.ExitCode, res.Stdout, res.Stderr)
			return
		}
		stopDone <- nil
	}()

	deadline := time.NewTimer(budget)
	defer deadline.Stop()

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("stop failed: %v", err)
		}
		if !waitStopped(repo, 5*time.Second) {
			t.Fatalf("daemon_state.mode never reached 'stopped' after acd stop succeeded")
		}
		// Happy path — the daemon shut down cleanly inside the 30s budget.
		return
	case <-deadline.C:
		// Phase 4 — hang detected. Capture goroutine state by sending
		// SIGQUIT (Go runtime prints all goroutines and exits 2) before
		// failing the test. We wait briefly for the dump to materialize.
		artifact := captureGoroutineDump(t, daemonPID)
		// Drain stopDone (best-effort) so the goroutine is not leaked.
		var hangErr error
		select {
		case hangErr = <-stopDone:
		case <-time.After(3 * time.Second):
			hangErr = errors.New("acd stop never returned post-SIGQUIT")
		}
		if hangErr == nil {
			hangErr = errors.New("acd stop returned post-SIGQUIT but lifecycle exceeded 30s budget")
		}
		t.Fatalf("daemon lifecycle exceeded 30s budget; goroutine dump captured at %s\nstop result: %v",
			artifact, hangErr)
	}
}

// captureGoroutineDump SIGQUITs the daemon process to force the Go runtime
// to print all goroutines on its stderr (which acd's start command captured
// into ~/.local/state/acd/logs/<repo>.log) and then writes the most recent
// log we can locate into an artifact file under the test's temp dir.
//
// On hang, a partial artifact is still vastly more useful than nothing — a
// deadlock signature like "sync.Mutex.Lock... acd/internal/git.ignore" tells
// the operator exactly where to look.
func captureGoroutineDump(t *testing.T, pid int) string {
	t.Helper()

	// Send SIGQUIT to the daemon so it prints all goroutines to stderr.
	// The Go runtime intercepts SIGQUIT and dumps before exiting with code 2.
	if pid > 0 {
		if err := syscall.Kill(pid, syscall.SIGQUIT); err != nil {
			t.Logf("kill -QUIT %d: %v", pid, err)
		}
	}

	// Give the runtime a moment to flush the dump.
	time.Sleep(2 * time.Second)

	// Collect any candidate log file under HOME/.local/state/acd or
	// XDG_STATE_HOME/acd. Best-effort. We also serialize a dump of our own
	// goroutines so the test artifact is never empty.
	var sb sync.Mutex
	var collected []string
	collect := func(path, body string) {
		sb.Lock()
		defer sb.Unlock()
		collected = append(collected, fmt.Sprintf("=== %s ===\n%s\n", path, body))
	}

	// Always include our own snapshot so the artifact never lies.
	collect("test-process-stacks", goroutineStacks())

	if home, err := os.UserHomeDir(); err == nil {
		candidates := []string{
			filepath.Join(home, ".local", "state", "acd", "logs"),
		}
		if xdg := os.Getenv("XDG_STATE_HOME"); xdg != "" {
			candidates = append(candidates, filepath.Join(xdg, "acd", "logs"))
		}
		for _, dir := range candidates {
			ents, err := os.ReadDir(dir)
			if err != nil {
				continue
			}
			for _, e := range ents {
				if e.IsDir() {
					continue
				}
				p := filepath.Join(dir, e.Name())
				if data, err := os.ReadFile(p); err == nil {
					collect(p, string(data))
				}
			}
		}
	}

	artifact := filepath.Join(t.TempDir(), "deadlock-stacks.log")
	if err := os.WriteFile(artifact, []byte(joinLines(collected)), 0o644); err != nil {
		t.Logf("write artifact: %v", err)
	}
	return artifact
}

// goroutineStacks returns a stack dump of the current process's goroutines.
// Sized for "way more than enough" so a hang signature is never truncated.
func goroutineStacks() string {
	const maxBuf = 1 << 20
	buf := make([]byte, maxBuf)
	n := runtimeStack(buf, true)
	return string(buf[:n])
}

// runtimeStack is a thin wrapper around runtime.Stack so the function is
// trivial to substitute under tests if needed.
func runtimeStack(buf []byte, all bool) int {
	return runtime.Stack(buf, all)
}

func joinLines(parts []string) string {
	out := ""
	for _, p := range parts {
		out += p
	}
	return out
}
