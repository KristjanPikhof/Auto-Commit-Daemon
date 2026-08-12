package installer

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestUninstallShutdownRequiresStoppedSupervisorProof(t *testing.T) {
	roots, stop := startShutdownProofServer(t, os.Getpid(), 0, false)
	defer stop()
	client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 100 * time.Millisecond}
	err := stopSupervisorForUninstall(context.Background(), roots, client, 100*time.Millisecond)
	if err == nil {
		t.Fatal("shutdown failure was accepted while the supervisor remained live")
	}
}

func TestUninstallShutdownAcceptsProvenStoppedAfterTransportLoss(t *testing.T) {
	roots, stop := startShutdownProofServer(t, os.Getpid()+1_000_000, 0, true)
	defer stop()
	client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 100 * time.Millisecond}
	if err := stopSupervisorForUninstall(context.Background(), roots, client, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestUninstallShutdownRequiresStoppedWorkerProof(t *testing.T) {
	roots, stop := startShutdownProofServer(t, os.Getpid()+1_000_000, os.Getpid(), true)
	defer stop()
	client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 100 * time.Millisecond}
	err := stopSupervisorForUninstall(context.Background(), roots, client, 100*time.Millisecond)
	if err == nil {
		t.Fatal("shutdown was accepted while a canonical worker remained live")
	}
}

func TestUninstallShutdownWaitsForSessionStartWindow(t *testing.T) {
	root := t.TempDir()
	roots := paths.Roots{State: filepath.Join(root, "state", "acd")}
	startLock, err := supervisor.AcquireSessionLifecycleLock(context.Background(), roots)
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 100 * time.Millisecond}
		result <- stopSupervisorForUninstall(context.Background(), roots, client, time.Second)
	}()
	select {
	case err := <-result:
		startLock.Release()
		t.Fatalf("uninstall crossed an active session start window: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	startLock.Release()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("uninstall did not resume after the session start window closed")
	}
}

func startShutdownProofServer(t *testing.T, reportedPID, workerPID int, stopOnShutdown bool) (paths.Roots, func()) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "acd-shutdown-proof-")
	if err != nil {
		t.Fatal(err)
	}
	roots := paths.Roots{State: filepath.Join(root, "state")}
	if err := os.MkdirAll(filepath.Dir(roots.SupervisorSocketPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", roots.SupervisorSocketPath())
	if err != nil {
		t.Fatal(err)
	}
	var workers []supervisor.WorkerStatus
	if workerPID > 0 {
		repositoryID := "0123456789abcdef"
		workers = append(workers, supervisor.WorkerStatus{RepositoryID: repositoryID, PID: workerPID, State: "running"})
		if err := os.WriteFile(supervisor.WorkerSocketPath(roots, repositoryID), []byte("live"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			var request supervisor.Request
			if json.NewDecoder(bufio.NewReader(conn)).Decode(&request) != nil {
				_ = conn.Close()
				continue
			}
			response := supervisor.Response{Version: supervisor.ProtocolVersion, ID: request.ID, OK: true}
			switch request.Method {
			case "status":
				response.Data = supervisor.Status{PID: reportedPID, Version: "test", Workers: workers}
				_ = json.NewEncoder(conn).Encode(response)
				_ = conn.Close()
			case "shutdown":
				if stopOnShutdown {
					_ = conn.Close()
					_ = listener.Close()
					_ = os.Remove(roots.SupervisorSocketPath())
					return
				}
				response.OK = false
				response.Error = &supervisor.ProtocolError{Code: "shutdown_failed", Message: "still running"}
				_ = json.NewEncoder(conn).Encode(response)
				_ = conn.Close()
			}
		}
	}()
	stop := func() {
		_ = listener.Close()
		<-stopped
		_ = os.RemoveAll(root)
	}
	return roots, stop
}
