package supervisor

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestEnsureSessionLeavesNonDarwinServiceLifecycleUnchanged(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("non-macOS compatibility contract")
	}
	if err := EnsureSession(context.Background(), paths.Roots{}, "", ""); err != nil {
		t.Fatalf("EnsureSession on %s: %v", runtime.GOOS, err)
	}
}

func TestSessionSupervisorShutdownFailsUntilWorkersStop(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires POSIX process signals")
	}
	command := exec.Command("/bin/sh", "-c", "trap '' TERM; echo ready; while :; do sleep 1; done")
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	if line, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("worker readiness line=%q err=%v", line, err)
	}

	server := &Server{workers: map[string]*workerProcess{
		"0123456789abcdef": {id: "0123456789abcdef", cmd: command},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, protocolErr := server.handle(ctx, Request{Method: "shutdown"})
	if protocolErr == nil || protocolErr.Code != "shutdown_incomplete" {
		t.Fatalf("shutdown error=%+v, want shutdown_incomplete", protocolErr)
	}
}

func TestEnsureSessionCleansUpNeverReadyChild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires POSIX process signals")
	}
	root, err := os.MkdirTemp("/tmp", "acd-session-cleanup-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	roots := paths.Roots{State: filepath.Join(root, "state", "acd")}
	pidPath := filepath.Join(root, "session.pid")
	t.Setenv("ACD_TEST_SESSION_PID", pidPath)
	binary := filepath.Join(root, "never-ready.sh")
	script := "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$ACD_TEST_SESSION_PID\"\ntrap '' TERM\nexec /bin/sleep 60\n"
	if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type pidResult struct {
		body []byte
		err  error
	}
	pidReady := make(chan pidResult, 1)
	go func() {
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			body, readErr := os.ReadFile(pidPath)
			if readErr == nil && strings.TrimSpace(string(body)) != "" {
				pidReady <- pidResult{body: body}
				cancel()
				return
			}
			if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
				pidReady <- pidResult{err: readErr}
				cancel()
				return
			}
			select {
			case <-deadline.C:
				pidReady <- pidResult{err: errors.New("session child did not record its PID")}
				cancel()
				return
			case <-ticker.C:
			}
		}
	}()
	err = ensureSessionForOwner(ctx, roots, binary, filepath.Join(root, "session.log"), "owner-a")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ensure session error=%v, want cancellation", err)
	}
	result := <-pidReady
	if result.err != nil {
		t.Fatalf("read spawned session PID: %v", result.err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(result.body)))
	if err != nil {
		t.Fatalf("parse spawned session PID %q: %v", result.body, err)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("spawned session PID %d remains live after readiness failure: %v", pid, err)
	}
}

func TestSessionReadyRefusesAnotherResponsibilitySession(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "acd-session-owner-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	roots := paths.Roots{State: filepath.Join(root, "state", "acd")}
	if err := os.MkdirAll(filepath.Dir(roots.SupervisorSocketPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", roots.SupervisorSocketPath())
	if err != nil {
		t.Fatal(err)
	}
	var advertised atomic.Value
	advertised.Store(Status{
		PID: os.Getpid(), Version: "v2026-08-07-161-ge473be00-dirty", Ownership: "session:owner-a",
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			var request Request
			if json.NewDecoder(bufio.NewReader(conn)).Decode(&request) == nil {
				_ = json.NewEncoder(conn).Encode(Response{
					Version: ProtocolVersion,
					ID:      request.ID,
					OK:      true,
					Data:    advertised.Load().(Status),
				})
			}
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
	})

	ready, err := sessionReady(context.Background(), roots, "owner-b")
	if ready || err == nil || !strings.Contains(err.Error(), "another macOS application or terminal session") ||
		!strings.Contains(err.Error(), "run `acd setup`") || strings.HasPrefix(err.Error(), "supervisor:") {
		t.Fatalf("cross-owner readiness=(%v, %v), want refusal", ready, err)
	}

	advertised.Store(Status{PID: os.Getpid(), Version: "v2026-08-07-161-ge473be00-dirty"})
	ready, err = sessionReady(context.Background(), roots, "owner-a")
	if ready || err == nil || !strings.Contains(err.Error(), "older ACD background process") ||
		!strings.Contains(err.Error(), "v2026-08-07-161-ge473be00-dirty") ||
		!strings.Contains(err.Error(), "run `acd setup`") || strings.HasPrefix(err.Error(), "supervisor:") {
		t.Fatalf("old-supervisor readiness=(%v, %v), want actionable upgrade refusal", ready, err)
	}

	advertised.Store(Status{PID: os.Getpid(), Ownership: "session:owner-a"})
	ready, err = sessionReady(context.Background(), roots, "owner-a")
	if err != nil || !ready {
		t.Fatalf("matching-owner readiness=(%v, %v), want ready", ready, err)
	}
}
