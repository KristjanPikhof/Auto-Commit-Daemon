//go:build integration
// +build integration

package integration_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
)

func TestCaptureOwnedCheckpointProcessReapsChildOnFailure(t *testing.T) {
	childPIDPath := filepath.Join(t.TempDir(), "child.pid")
	command := exec.Command("/bin/sh", "-c", `sleep 30 & child=$!; echo "$child" > "$1"; wait`, "sh", childPIDPath)
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	pid := command.Process.Pid
	var childPID int
	waitFor(t, "capture-failure child PID", time.Second, func() bool {
		body, err := os.ReadFile(childPIDPath)
		if err != nil {
			return false
		}
		childPID, err = strconv.Atoi(strings.TrimSpace(string(body)))
		return err == nil && childPID > 0
	})
	_, err := captureOwnedCheckpointProcess(command, func(int) (checkpointProcessIdentity, error) {
		return checkpointProcessIdentity{}, errors.New("forced capture failure")
	})
	if err == nil {
		t.Fatal("capture failure unexpectedly succeeded")
	}
	if command.ProcessState == nil {
		t.Fatalf("owned child was not waited: state=%v", command.ProcessState)
	}
	if identity.Alive(pid) {
		t.Fatal("owned process-group leader remained live after failed capture cleanup")
	}
	waitFor(t, "capture-failure descendant reaped", 2*time.Second, func() bool {
		return !identity.Alive(childPID)
	})
}

func TestStartCheckpointWatchdogReapsOnCaptureFailure(t *testing.T) {
	command := exec.Command("sleep", "30")
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	_, err := startOwnedCheckpointWatchdog(command, func(int) (checkpointProcessIdentity, error) {
		return checkpointProcessIdentity{}, errors.New("forced watchdog capture failure")
	})
	if err == nil {
		t.Fatal("watchdog capture failure unexpectedly succeeded")
	}
	if command.ProcessState == nil {
		t.Fatalf("watchdog was not waited: state=%v", command.ProcessState)
	}
	if identity.Alive(command.Process.Pid) {
		t.Fatal("watchdog remained live after failed capture cleanup")
	}
}

func TestCheckpointWatchdogRetriesInitialAndEscalationCaptureErrors(t *testing.T) {
	process := checkpointProcessIdentity{PID: 4242, StartTime: "start", ArgvHash: "argv"}
	matchCalls := 0
	alive := true
	termSignals := 0
	killSignals := 0
	cleanupCheckpointWatchdogProcess(
		process, 0, time.Millisecond,
		func(checkpointProcessIdentity) (bool, error) {
			matchCalls++
			if matchCalls == 1 {
				return false, errors.New("transient initial capture failure")
			}
			if !alive {
				return false, errors.New("process exited")
			}
			return true, nil
		},
		func(int) bool { return alive },
		func(_ checkpointProcessIdentity, signal syscall.Signal) error {
			switch signal {
			case syscall.SIGTERM:
				termSignals++
				return nil
			case syscall.SIGKILL:
				killSignals++
				if killSignals == 1 {
					return errors.New("transient escalation capture failure")
				}
				alive = false
				return nil
			default:
				return errors.New("unexpected signal")
			}
		},
	)
	if termSignals != 1 || killSignals != 2 || alive {
		t.Fatalf("cleanup term=%d kill=%d alive=%v match_calls=%d", termSignals, killSignals, alive, matchCalls)
	}
}

func TestCheckpointRuntimeIdentityRejectsReusedPID(t *testing.T) {
	process, err := captureCheckpointProcessIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	reused := process
	reused.StartTime += " reused"
	if matches, matchErr := checkpointProcessMatches(reused); matchErr != nil || matches {
		t.Fatal("mismatched process start time matched a recycled PID")
	}
	if err := signalCheckpointProcess(reused, 0); err == nil {
		t.Fatal("signal accepted a recycled PID identity")
	}
}

func TestCheckpointRuntimeLimiterHoldsFifthUntilRelease(t *testing.T) {
	limiter := make(checkpointRuntimeLimiter, 4)
	releases := make([]func(), 0, 4)
	for range 4 {
		releases = append(releases, limiter.acquire())
	}
	for _, release := range releases {
		defer release()
	}

	fifth := make(chan func(), 1)
	go func() { fifth <- limiter.acquire() }()
	select {
	case release := <-fifth:
		release()
		t.Fatal("fifth runtime acquired capacity while four remained live")
	case <-time.After(100 * time.Millisecond):
	}

	releases[0]()
	select {
	case release := <-fifth:
		release()
	case <-time.After(time.Second):
		t.Fatal("fifth runtime did not acquire capacity after cleanup released a slot")
	}
}

func TestCheckpointRuntimeUsesSessionOwnershipOnDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("session ownership is a macOS runtime contract")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	ensureProductionCheckpointRuntime(t, env, repo, buildAcdBinary(t))
	roots, _ := prepareCheckpointRegistration(t, env, repo)
	assertCheckpointRuntimeOwnership(t, roots)
}
