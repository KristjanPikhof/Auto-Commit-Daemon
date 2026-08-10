package supervisor

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestRequestValidationCoversRequiredMethods(t *testing.T) {
	methods := []string{"status", "enable_repository", "disable_repository", "checkpoint_barrier", "hint", "history", "restore_plan", "restore_apply", "repair", "shutdown", "worker_environment"}
	for _, method := range methods {
		request := Request{Version: ProtocolVersion, ID: "request", Method: method}
		if err := request.Validate(); err != nil {
			t.Fatalf("method %s: %v", method, err)
		}
	}
	if err := (Request{Version: 2, ID: "request", Method: "status"}).Validate(); err == nil {
		t.Fatal("future protocol accepted")
	}
}

func TestSupervisorWorkerSocketPathRequiresCanonicalRepositoryID(t *testing.T) {
	roots := paths.Roots{State: t.TempDir()}
	for _, repositoryID := range []string{"", "  ", "repo-one", "0123456789ABCDEf", "0123456789abcdef0"} {
		if got := WorkerSocketPath(roots, repositoryID); got != "" {
			t.Fatalf("repository %q socket=%q", repositoryID, got)
		}
	}
	if got := WorkerSocketPath(roots, "0123456789abcdef"); got == "" {
		t.Fatal("canonical repository id did not produce a socket path")
	}
}

func TestClientUsesOneJSONLineAndValidatesIdentity(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "acd-supervisor-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "supervisor.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		var request Request
		if json.NewDecoder(conn).Decode(&request) == nil {
			_ = json.NewEncoder(conn).Encode(Response{Version: ProtocolVersion, ID: request.ID, OK: true, Data: map[string]bool{"ready": true}})
		}
	}()
	response, err := (Client{SocketPath: socket}).Do(context.Background(), Request{ID: "abc", Method: "status"})
	if err != nil {
		t.Fatal(err)
	}
	if !response.OK {
		t.Fatalf("response=%+v", response)
	}
}

func TestServerSocketPermissionsAndStatus(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "acd-supervisor-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, "state"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(home, "data"))
	roots, err := paths.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	server := &Server{Roots: roots, BinaryPath: "/bin/false", Version: "test"}
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		info, statErr := os.Stat(roots.SupervisorSocketPath())
		if statErr == nil {
			if info.Mode().Perm() != 0o600 {
				t.Fatalf("socket mode=%o", info.Mode().Perm())
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket not ready: %v", statErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	response, err := (Client{SocketPath: roots.SupervisorSocketPath()}).Do(context.Background(), Request{ID: "status", Method: "status"})
	if err != nil || !response.OK {
		t.Fatalf("status=(%+v,%v)", response, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("server did not stop")
	}
}

func TestServerRefusesLegacyRegistry(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "acd-legacy-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	roots := paths.Roots{State: filepath.Join(root, "s"), Share: filepath.Join(root, "d"), Config: filepath.Join(root, "c")}
	registry := central.NewRegistry()
	registry.Version = 1
	if err := central.Save(roots, registry); err != nil {
		t.Fatal(err)
	}
	err = (&Server{Roots: roots, BinaryPath: "/bin/false", Version: "test"}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "registry v1 requires `acd setup`") {
		t.Fatalf("legacy registry startup error=%v", err)
	}
}

func TestRestartBackoffContract(t *testing.T) {
	want := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second, 30 * time.Second}
	for i, expected := range want {
		if got := restartDelay(i + 1); got != expected {
			t.Fatalf("restart %d=%s want %s", i+1, got, expected)
		}
	}
}

func TestStartableWorkerIDsBoundsAndOrdersStartupBatch(t *testing.T) {
	now := time.Now()
	workers := map[string]*workerProcess{
		"repo-f": {id: "repo-f"},
		"repo-e": {id: "repo-e"},
		"repo-d": {id: "repo-d"},
		"repo-c": {id: "repo-c"},
		"repo-b": {id: "repo-b"},
		"repo-a": {id: "repo-a"},
		"running": {
			id: "running", cmd: &exec.Cmd{},
		},
		"backoff": {
			id: "backoff", nextStart: now.Add(time.Minute),
		},
	}
	want := []string{"repo-a", "repo-b", "repo-c", "repo-d"}
	if got := startableWorkerIDs(workers, now, maxWorkerStartsPerReconcile); !slices.Equal(got, want) {
		t.Fatalf("startable workers=%v want %v", got, want)
	}
}

func TestStartingLaunchdWorkersBoundsConcurrentProtection(t *testing.T) {
	home := t.TempDir()
	roots := paths.Roots{State: filepath.Join(home, "state"), Share: filepath.Join(home, "share"), Config: filepath.Join(home, "config")}
	workers := make(map[string]*workerProcess)
	for index := 0; index < maxConcurrentWorkerStarts; index++ {
		id := fmt.Sprintf("%016d", index)
		workers[id] = &workerProcess{id: id, launchd: true}
		if err := WriteWorkerRuntimeStatus(roots, WorkerRuntimeStatus{RepositoryID: id, State: "starting"}); err != nil {
			t.Fatal(err)
		}
	}
	if got := startingLaunchdWorkers(roots, workers); got != maxConcurrentWorkerStarts {
		t.Fatalf("starting launchd workers=%d want %d", got, maxConcurrentWorkerStarts)
	}
	workers["9999999999999999"] = &workerProcess{id: "9999999999999999", launchd: true}
	if err := WriteWorkerRuntimeStatus(roots, WorkerRuntimeStatus{RepositoryID: "9999999999999999", State: "running"}); err != nil {
		t.Fatal(err)
	}
	if got := startingLaunchdWorkers(roots, workers); got != maxConcurrentWorkerStarts {
		t.Fatalf("running worker consumed admission slot: got %d", got)
	}
}

func TestDarwinUsesSessionOwnedSupervisorWithoutServiceFile(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS-only contract")
	}
	definition, err := RenderService("/Users/test", "/Users/test/acd", "/Users/test/acd.log")
	if err != nil {
		t.Fatal(err)
	}
	if definition.Platform != "session" || len(definition.Content) != 0 {
		t.Fatalf("service=%+v, want session mode without installable content", definition)
	}
	if definition.Binary != "/Users/test/acd" || definition.LogPath != "/Users/test/acd.log" {
		t.Fatalf("session paths=%+v", definition)
	}
	if err := ValidateService(definition, "/Users/test/acd"); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyDarwinWorkerLaunchdWrapperRemainsCleanable(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("launchd-only contract")
	}
	home := t.TempDir()
	roots := paths.Roots{State: filepath.Join(home, "state"), Share: filepath.Join(home, "share"), Config: filepath.Join(home, "config")}
	var name string
	var args []string
	server := &Server{
		Roots: roots, BinaryPath: "/managed/acd", LaunchdWorkers: true,
		command: func(_ context.Context, command string, commandArgs ...string) ([]byte, error) {
			name = command
			args = append([]string(nil), commandArgs...)
			return nil, nil
		},
	}
	worker := &workerProcess{id: "0123456789abcdef"}
	server.startWorkerLocked(context.Background(), worker)
	if !worker.launchd || worker.cmd != nil {
		t.Fatalf("worker launch state=(launchd=%t cmd=%v)", worker.launchd, worker.cmd)
	}
	if name != "launchctl" {
		t.Fatalf("command=%q", name)
	}
	servicePath, err := workerServicePath(roots, worker.id)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"bootstrap", fmt.Sprintf("gui/%d", os.Getuid()), servicePath}
	if !slices.Equal(args, want) {
		t.Fatalf("launch args=%q want %q", args, want)
	}
	content, err := os.ReadFile(servicePath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(content)
	for _, required := range []string{
		"<key>ProcessType</key><string>Interactive</string>",
		"<key>ThrottleInterval</key><integer>1</integer>",
		"<string>/managed/acd</string>",
		"<string>supervise</string>",
		"<string>" + roots.State + "</string>",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("worker service missing %q: %s", required, text)
		}
	}
}

func TestWorkerRuntimeStatusIsPrivateAndIdentityBound(t *testing.T) {
	home := t.TempDir()
	roots := paths.Roots{State: filepath.Join(home, "state"), Share: filepath.Join(home, "share"), Config: filepath.Join(home, "config")}
	want := WorkerRuntimeStatus{RepositoryID: "0123456789abcdef", PID: 42, State: "backoff", Restarts: 3, LastError: "test failure"}
	if err := WriteWorkerRuntimeStatus(roots, want); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(roots.WorkerStatusPath(want.RepositoryID))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("status mode=%o", info.Mode().Perm())
	}
	got, err := ReadWorkerRuntimeStatus(roots, want.RepositoryID)
	if err != nil {
		t.Fatal(err)
	}
	got.UpdatedTS = 0
	if got != want {
		t.Fatalf("status=%+v want %+v", got, want)
	}
	if _, err := workerLabel("../../unsafe"); err == nil {
		t.Fatal("unsafe repository id accepted")
	}
}
