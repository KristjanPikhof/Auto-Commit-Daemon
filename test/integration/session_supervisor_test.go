//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestMacOSSessionSupervisorStartsWithoutLaunchd(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS session supervisor contract")
	}
	root, err := os.MkdirTemp("/tmp", "acd-session-supervisor-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	roots := paths.Roots{
		State:  filepath.Join(root, "state", "acd"),
		Share:  filepath.Join(root, "data", "acd"),
		Config: filepath.Join(root, "config", "acd"),
	}
	t.Setenv("XDG_STATE_HOME", filepath.Dir(roots.State))
	t.Setenv("XDG_DATA_HOME", filepath.Dir(roots.Share))
	t.Setenv("XDG_CONFIG_HOME", filepath.Dir(roots.Config))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	logPath := roots.SupervisorLogPath()
	if err := supervisor.EnsureSession(ctx, roots, buildAcdBinary(t), logPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownSupervisorScaleTest(roots) })
	first := readSessionSupervisorStatus(t, ctx, roots)
	if err := supervisor.EnsureSession(ctx, roots, buildAcdBinary(t), logPath); err != nil {
		t.Fatal(err)
	}
	second := readSessionSupervisorStatus(t, ctx, roots)
	if first.PID <= 0 || second.PID != first.PID {
		t.Fatalf("session supervisor PIDs=(%d,%d), want one stable owner", first.PID, second.PID)
	}
	parentOutput, err := exec.Command("/bin/ps", "-o", "ppid=", "-p", strconv.Itoa(first.PID)).Output()
	if err != nil {
		t.Fatalf("inspect session supervisor parent: %v", err)
	}
	if parent := strings.TrimSpace(string(parentOutput)); parent != strconv.Itoa(os.Getpid()) {
		t.Fatalf("session supervisor parent=%s, want caller PID %d", parent, os.Getpid())
	}
	target := fmt.Sprintf("gui/%d/%s", os.Getuid(), supervisor.ServiceLabel)
	if exec.Command("launchctl", "print", target).Run() == nil {
		t.Fatalf("session supervisor unexpectedly registered with launchd: %s", target)
	}
	shutdownSupervisorScaleTest(roots)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := syscall.Kill(first.PID, 0); errors.Is(err, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("session supervisor PID %d was not reaped", first.PID)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func readSessionSupervisorStatus(t *testing.T, ctx context.Context, roots paths.Roots) supervisor.Status {
	t.Helper()
	response, err := (&supervisor.Client{
		SocketPath: roots.SupervisorSocketPath(), Timeout: time.Second,
	}).Do(ctx, supervisor.Request{Version: supervisor.ProtocolVersion, ID: "session-status", Method: "status"})
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(response.Data)
	if err != nil {
		t.Fatal(err)
	}
	var status supervisor.Status
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatal(err)
	}
	return status
}
