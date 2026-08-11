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

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestRepositoryMaintenanceLeaseStopsWorkerAndFencesRestart(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires POSIX process signals")
	}
	root := t.TempDir()
	roots := paths.Roots{
		State: filepath.Join(root, "state", "acd"), Share: filepath.Join(root, "share", "acd"),
		Config: filepath.Join(root, "config", "acd"),
	}
	const repositoryID = "0123456789abcdef"
	registry := central.NewRegistry()
	registry.Repos = []central.RepoRecord{{
		Path: "/repo", StateDB: "/repo/state.db", RepositoryID: repositoryID, WorktreeID: "fedcba9876543210",
	}}
	if err := central.Save(roots, registry); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("/bin/sleep", "60")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	worker := &workerProcess{id: repositoryID, cmd: command, signature: "registered", desired: "registered"}
	server := &Server{Roots: roots, BinaryPath: "/does/not/exist", workers: map[string]*workerProcess{repositoryID: worker}}
	go func() {
		_ = command.Wait()
		server.mu.Lock()
		if current := server.workers[repositoryID]; current != nil && current.cmd == command {
			current.cmd = nil
			current.intentional = false
		}
		server.mu.Unlock()
	}()
	t.Cleanup(func() { _ = command.Process.Kill() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, protocolErr := server.beginMaintenance(ctx, repositoryID)
	if protocolErr != nil {
		t.Fatalf("begin maintenance: %+v", protocolErr)
	}
	lease := raw.(MaintenanceLease)
	if lease.Token == "" || lease.RepositoryID != repositoryID {
		t.Fatalf("lease=%+v", lease)
	}
	if err := syscall.Kill(command.Process.Pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("worker pid %d survived maintenance begin: %v", command.Process.Pid, err)
	}
	if err := server.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	if heldWorker := server.workers[repositoryID]; heldWorker != nil && heldWorker.cmd != nil {
		server.mu.Unlock()
		t.Fatal("maintenance hold allowed worker restart")
	}
	server.mu.Unlock()

	params, _ := json.Marshal(map[string]string{"token": lease.Token})
	if _, protocolErr := server.renewMaintenance(repositoryID, params); protocolErr != nil {
		t.Fatalf("renew maintenance: %+v", protocolErr)
	}
	if _, protocolErr := server.endMaintenance(ctx, repositoryID, params); protocolErr != nil {
		t.Fatalf("end maintenance: %+v", protocolErr)
	}
	server.mu.Lock()
	_, held := server.maintenance[repositoryID]
	restarts := server.workers[repositoryID].restarts
	server.mu.Unlock()
	if held || restarts == 0 {
		t.Fatalf("maintenance held=%v restart attempts=%d", held, restarts)
	}
}

func TestRepositoryMaintenanceLeaseRequiresOwnerTokenAndExpires(t *testing.T) {
	root := t.TempDir()
	roots := paths.Roots{
		State: filepath.Join(root, "state", "acd"), Share: filepath.Join(root, "share", "acd"),
		Config: filepath.Join(root, "config", "acd"),
	}
	const repositoryID = "0123456789abcdef"
	registry := central.NewRegistry()
	registry.Repos = []central.RepoRecord{{
		Path: "/repo", StateDB: "/repo/state.db", RepositoryID: repositoryID, WorktreeID: "fedcba9876543210",
	}}
	if err := central.Save(roots, registry); err != nil {
		t.Fatal(err)
	}
	server := &Server{
		Roots: roots, BinaryPath: "/does/not/exist", workers: make(map[string]*workerProcess),
		maintenance: map[string]MaintenanceLease{
			repositoryID: {RepositoryID: repositoryID, Token: "owner-token", ExpiresAt: time.Now().Add(time.Minute)},
		},
	}
	wrongParams, _ := json.Marshal(map[string]string{"token": "wrong-token"})
	if _, protocolErr := server.renewMaintenance(repositoryID, wrongParams); protocolErr == nil || protocolErr.Code != "maintenance_lease_lost" {
		t.Fatalf("renew with wrong token error=%+v", protocolErr)
	}
	if _, protocolErr := server.endMaintenance(context.Background(), repositoryID, wrongParams); protocolErr == nil || protocolErr.Code != "maintenance_lease_lost" {
		t.Fatalf("end with wrong token error=%+v", protocolErr)
	}
	server.mu.Lock()
	lease := server.maintenance[repositoryID]
	lease.ExpiresAt = time.Now().Add(-time.Second)
	server.maintenance[repositoryID] = lease
	server.mu.Unlock()
	if err := server.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	_, held := server.maintenance[repositoryID]
	worker := server.workers[repositoryID]
	server.mu.Unlock()
	if held {
		t.Fatal("expired maintenance lease remained active")
	}
	if worker == nil || worker.restarts == 0 {
		t.Fatalf("expired lease did not restore worker start attempt: %+v", worker)
	}
}

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

func TestSessionReadyReusesOnlyMatchingUserSupervisor(t *testing.T) {
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
		PID: os.Getpid(), Version: "v2026-08-07-161-ge473be00-dirty", Ownership: "user:501",
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

	ready, err := sessionReady(context.Background(), roots, "user:502")
	if ready || err == nil || !strings.Contains(err.Error(), "incompatible with this user") ||
		!strings.Contains(err.Error(), "run `acd setup` once") || strings.HasPrefix(err.Error(), "supervisor:") {
		t.Fatalf("cross-user readiness=(%v, %v), want refusal", ready, err)
	}

	advertised.Store(Status{PID: os.Getpid(), Version: "v2026-08-07-161-ge473be00-dirty"})
	ready, err = sessionReady(context.Background(), roots, "user:501")
	if ready || err == nil || !strings.Contains(err.Error(), "older ACD background process") ||
		!strings.Contains(err.Error(), "v2026-08-07-161-ge473be00-dirty") ||
		!strings.Contains(err.Error(), "run `acd setup`") || strings.HasPrefix(err.Error(), "supervisor:") {
		t.Fatalf("old-supervisor readiness=(%v, %v), want actionable upgrade refusal", ready, err)
	}

	advertised.Store(Status{PID: os.Getpid(), Ownership: "user:501"})
	ready, err = sessionReady(context.Background(), roots, "user:501")
	if err != nil || !ready {
		t.Fatalf("matching-owner readiness=(%v, %v), want ready", ready, err)
	}
}
