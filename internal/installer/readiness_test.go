package installer

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestWaitSupervisorWorkersReadyWaitsForEveryWorkerSocket(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "acd-ready-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	roots := paths.Roots{State: filepath.Join(root, "s")}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	workers := []supervisor.WorkerStatus{
		{RepositoryID: "repo-one", State: "running"},
		{RepositoryID: "repo-two", State: "running"},
	}
	startReadinessServer(t, ctx, roots.SupervisorSocketPath(), func(request supervisor.Request) supervisor.Response {
		return supervisor.Response{Version: supervisor.ProtocolVersion, ID: request.ID, OK: true,
			Data: supervisor.Status{Version: "test", Workers: workers}}
	})
	startReadinessServer(t, ctx, supervisor.WorkerSocketPath(roots, "repo-one"), readyWorkerResponse)
	var secondAttempts atomic.Int32
	startReadinessServer(t, ctx, supervisor.WorkerSocketPath(roots, "repo-two"), func(request supervisor.Request) supervisor.Response {
		if secondAttempts.Add(1) == 1 {
			return supervisor.Response{ID: request.ID, OK: true}
		}
		return readyWorkerResponse(request)
	})

	var counts []int
	err = waitSupervisorWorkersReady(ctx, roots,
		map[string]bool{"repo-one": true, "repo-two": true}, 2*time.Second,
		func(ready, _ int) { counts = append(counts, ready) })
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(counts, 1) || counts[len(counts)-1] != 2 {
		t.Fatalf("readiness progress=%v", counts)
	}
}

func readyWorkerResponse(request supervisor.Request) supervisor.Response {
	return supervisor.Response{Version: supervisor.ProtocolVersion, ID: request.ID, OK: true,
		Data: map[string]bool{"ready": true}}
}

func startReadinessServer(
	t *testing.T,
	ctx context.Context,
	path string,
	respond func(supervisor.Request) supervisor.Response,
) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer conn.Close()
				var request supervisor.Request
				if json.NewDecoder(bufio.NewReader(conn)).Decode(&request) == nil {
					_ = json.NewEncoder(conn).Encode(respond(request))
				}
			}()
		}
	}()
}
