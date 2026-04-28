//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestLifecycle_StartEditWakeCommitStop drives a full happy path against the
// real `acd` binary:
//
//  1. `acd start --session-id s1 --repo <repo> --harness shell --json` —
//     spawn the daemon and assert started=true, daemon_pid > 0.
//  2. Write a file inside the repo.
//  3. `acd wake --session-id s1 --repo <repo> --json` — flush the daemon.
//  4. Within 3s the file lands as a commit on HEAD.
//  5. `acd stop --session-id s1 --repo <repo> --json` — daemon shuts down
//     within 5s; daemon_state.mode == "stopped".
//
// The test reuses the buildAcdBinary cache so the compile cost is paid once
// per `go test ./test/integration/...` invocation.
func TestLifecycle_StartEditWakeCommitStop(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary not found in PATH; required for daemon_state probes")
	}

	repo := tempRepo(t)
	env := withIsolatedHome(t)

	// Capture the seed HEAD so we can detect when the daemon advances it.
	startHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. acd start
	startRes := runAcd(t, ctx, env,
		"start",
		"--session-id", "s1",
		"--repo", repo,
		"--harness", "shell",
		"--json",
	)
	if startRes.ExitCode != 0 {
		t.Fatalf("acd start exit=%d\nstdout=%s\nstderr=%s", startRes.ExitCode, startRes.Stdout, startRes.Stderr)
	}
	var startJSON struct {
		Started     bool   `json:"started"`
		Duplicate   bool   `json:"duplicate"`
		DaemonPID   int    `json:"daemon_pid"`
		Repo        string `json:"repo"`
		SessionID   string `json:"session_id"`
		Harness     string `json:"harness"`
		ClientCount int    `json:"client_count"`
	}
	if err := json.Unmarshal([]byte(startRes.Stdout), &startJSON); err != nil {
		t.Fatalf("decode start json: %v\nstdout=%s", err, startRes.Stdout)
	}
	if !startJSON.Started {
		t.Fatalf("expected started=true, got %+v", startJSON)
	}
	if startJSON.DaemonPID <= 0 {
		t.Fatalf("expected daemon_pid>0, got %+v", startJSON)
	}
	if startJSON.Harness != "shell" {
		t.Fatalf("harness=%q want shell", startJSON.Harness)
	}
	if startJSON.ClientCount != 1 {
		t.Fatalf("client_count=%d want 1", startJSON.ClientCount)
	}

	// Ensure the daemon stamped mode=running before we drop a file. The CLI
	// already polls for this for up to 3s, but be defensive.
	waitFor(t, "daemon_state.mode==running", 5*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})

	// 2. Write a fresh file.
	writeFile(t, filepath.Join(repo, "hello.txt"), "hi from integration\n")

	// 3. acd wake — push a flush request and SIGUSR1 the daemon.
	wakeRes := runAcd(t, ctx, env,
		"wake",
		"--session-id", "s1",
		"--repo", repo,
		"--json",
	)
	if wakeRes.ExitCode != 0 {
		t.Fatalf("acd wake exit=%d\nstdout=%s\nstderr=%s", wakeRes.ExitCode, wakeRes.Stdout, wakeRes.Stderr)
	}
	var wakeJSON struct {
		OK         bool   `json:"ok"`
		DaemonPID  int    `json:"daemon_pid"`
		SentSignal bool   `json:"sent_signal"`
		Repo       string `json:"repo"`
		SessionID  string `json:"session_id"`
	}
	if err := json.Unmarshal([]byte(wakeRes.Stdout), &wakeJSON); err != nil {
		t.Fatalf("decode wake json: %v\nstdout=%s", err, wakeRes.Stdout)
	}
	if !wakeJSON.OK {
		t.Fatalf("wake ok=false: %+v", wakeJSON)
	}
	if !wakeJSON.SentSignal {
		t.Fatalf("expected sent_signal=true (daemon should be alive): %+v", wakeJSON)
	}

	// 4. HEAD advances within 3s.
	waitFor(t, "HEAD advanced past seed", 3*time.Second, func() bool {
		head, err := runGit(repo, "rev-parse", "HEAD")
		if err != nil {
			return false
		}
		return strings.TrimSpace(head) != startHead
	})
	// Confirm hello.txt landed in the commit.
	logOut := runGitOK(t, repo, "log", "-1", "--pretty=%H %s", "HEAD")
	if !strings.Contains(logOut, "hello.txt") && !strings.Contains(logOut, "hello") {
		// The deterministic message format may use repo-relative paths or
		// summary forms — fall back to checking the tree contains hello.txt.
		showOut := runGitOK(t, repo, "show", "--name-only", "--pretty=", "HEAD")
		if !strings.Contains(showOut, "hello.txt") {
			t.Fatalf("HEAD does not contain hello.txt; show:\n%s", showOut)
		}
	}

	// 5. acd stop — last session out, daemon should shut down.
	stopRes := runAcd(t, ctx, env,
		"stop",
		"--session-id", "s1",
		"--repo", repo,
		"--json",
	)
	if stopRes.ExitCode != 0 {
		t.Fatalf("acd stop exit=%d\nstdout=%s\nstderr=%s", stopRes.ExitCode, stopRes.Stdout, stopRes.Stderr)
	}
	var stopJSON struct {
		Stopped   bool   `json:"stopped"`
		Deferred  bool   `json:"deferred"`
		Peers     int    `json:"peers"`
		Reason    string `json:"reason"`
		DaemonPID int    `json:"daemon_pid"`
	}
	if err := json.Unmarshal([]byte(stopRes.Stdout), &stopJSON); err != nil {
		t.Fatalf("decode stop json: %v\nstdout=%s", err, stopRes.Stdout)
	}
	if !stopJSON.Stopped && !stopJSON.Deferred {
		t.Fatalf("expected stopped or deferred, got %+v", stopJSON)
	}

	// daemon_state.mode == "stopped" within 5s. Either acd stop already
	// confirmed it (Stopped=true) or the run-loop is finishing up.
	waitFor(t, "daemon_state.mode==stopped", 5*time.Second, func() bool {
		return readDaemonStateMode(repo) == "stopped"
	})
}

// TestLifecycle_StartTwiceSameSession verifies the duplicate flag and that
// no second daemon spawns.
func TestLifecycle_StartTwiceSameSession(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary not found in PATH")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	first := runAcd(t, ctx, env,
		"start", "--session-id", "dup1", "--repo", repo, "--harness", "shell", "--json")
	if first.ExitCode != 0 {
		t.Fatalf("first start exit=%d\n%s\n%s", first.ExitCode, first.Stdout, first.Stderr)
	}
	var firstJSON struct {
		Started   bool `json:"started"`
		Duplicate bool `json:"duplicate"`
		DaemonPID int  `json:"daemon_pid"`
	}
	if err := json.Unmarshal([]byte(first.Stdout), &firstJSON); err != nil {
		t.Fatalf("decode first: %v\n%s", err, first.Stdout)
	}

	// Wait until daemon stamped mode=running so the second start observes a
	// live daemon and skips the spawn path.
	waitFor(t, "first daemon running", 5*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})

	second := runAcd(t, ctx, env,
		"start", "--session-id", "dup1", "--repo", repo, "--harness", "shell", "--json")
	if second.ExitCode != 0 {
		t.Fatalf("second start exit=%d\n%s\n%s", second.ExitCode, second.Stdout, second.Stderr)
	}
	var secondJSON struct {
		Started   bool `json:"started"`
		Duplicate bool `json:"duplicate"`
		DaemonPID int  `json:"daemon_pid"`
	}
	if err := json.Unmarshal([]byte(second.Stdout), &secondJSON); err != nil {
		t.Fatalf("decode second: %v\n%s", err, second.Stdout)
	}
	if secondJSON.Started {
		t.Fatalf("expected second start started=false, got %+v", secondJSON)
	}
	if !secondJSON.Duplicate {
		t.Fatalf("expected second start duplicate=true, got %+v", secondJSON)
	}
	if secondJSON.DaemonPID != firstJSON.DaemonPID {
		t.Fatalf("daemon PID drifted: first=%d second=%d", firstJSON.DaemonPID, secondJSON.DaemonPID)
	}

	// Cleanup — stop with --force so we don't leak a daemon if assertions
	// above failed.
	_ = runAcd(t, ctx, env, "stop", "--session-id", "dup1", "--repo", repo, "--force", "--json")
	waitFor(t, "post-cleanup mode==stopped", 5*time.Second, func() bool {
		return readDaemonStateMode(repo) == "stopped"
	})
}
