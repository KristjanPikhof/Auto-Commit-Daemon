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

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version"
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

func TestMacOSCompatibleRuntimeUpgradeRestartsWithoutSetupMigration(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS session supervisor contract")
	}
	root, err := os.MkdirTemp("/tmp", "acd-compatible-upgrade-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	roots := paths.Roots{
		State: filepath.Join(root, "state", "acd"), Share: filepath.Join(root, "data", "acd"),
		Config: filepath.Join(root, "config", "acd"),
	}
	t.Setenv("XDG_STATE_HOME", filepath.Dir(roots.State))
	t.Setenv("XDG_DATA_HOME", filepath.Dir(roots.Share))
	t.Setenv("XDG_CONFIG_HOME", filepath.Dir(roots.Config))
	repo := tempRepo(t)
	runGitOK(t, repo, "checkout", "--quiet", "--detach", "HEAD")
	worktree, err := gitpkg.ResolveWorktree(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(context.Background(), state.DBPathFromGitDir(worktree.GitDir))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	registry := central.NewRegistry()
	registration, err := registry.RegisterResolvedRepo(worktree, "runtime-upgrade-test", time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	if err := central.Save(roots, registry); err != nil {
		t.Fatal(err)
	}
	original := buildAcdBinary(t)
	body, err := os.ReadFile(original)
	if err != nil {
		t.Fatal(err)
	}
	managed := roots.ManagedBinaryPath()
	if err := os.MkdirAll(filepath.Dir(managed), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(managed, body, 0o755); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "new-acd")
	if err := os.WriteFile(source, append(body, []byte("compatible-runtime-upgrade")...), 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := supervisor.EnsureSession(ctx, roots, managed, roots.SupervisorLogPath()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { shutdownSupervisorScaleTest(roots) })
	before := readSessionSupervisorStatus(t, ctx, roots)
	beforeWorker := waitSessionSupervisorWorker(t, ctx, roots, registration.Record.RepositoryID, registration.Record.WorktreeID)
	if err := os.WriteFile(filepath.Join(repo, "protected-during-upgrade.txt"), []byte("preserve me\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	changed, err := installer.ApplyCompatibleRuntime(ctx, roots, installer.RuntimeUpgradeOptions{
		SourceExecutable: source, SourceVersion: version.String(),
		Compatibility: installer.RuntimeCompatibility(), Integrations: "none",
	})
	if err != nil {
		t.Fatal(err)
	}
	after := readSessionSupervisorStatus(t, ctx, roots)
	afterWorker := waitSessionSupervisorWorker(t, ctx, roots, registration.Record.RepositoryID, registration.Record.WorktreeID)
	wantDigest, err := version.FileDigest(source)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || after.PID == before.PID || after.BinaryDigest != wantDigest || !after.Compatibility.Equal(installer.RuntimeCompatibility()) {
		t.Fatalf("upgrade changed=%v before=%+v after=%+v want_digest=%s", changed, before, after, wantDigest)
	}
	if beforeWorker.PID <= 0 || afterWorker.PID <= 0 || beforeWorker.PID == afterWorker.PID {
		t.Fatalf("detached worker was not replaced: before=%+v after=%+v", beforeWorker, afterWorker)
	}
	projection, err := state.ReadCheckpointProjection(ctx, registration.Record.StateDB, 10)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Latest == nil || projection.Latest.Phase != state.CheckpointCompleted || projection.Latest.Ref == "" {
		t.Fatalf("detached dirty worktree was not checkpointed before upgrade: %+v", projection)
	}
	if got := runGitOK(t, repo, "show", projection.Latest.Ref+":protected-during-upgrade.txt"); got != "preserve me\n" {
		t.Fatalf("checkpointed detached content=%q want %q", got, "preserve me\\n")
	}
	waitProcessExited(t, before.PID, "old supervisor")
	waitProcessExited(t, beforeWorker.PID, "old detached worker")
}

func waitProcessExited(t *testing.T, pid int, description string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s PID %d survived compatible upgrade", description, pid)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitSessionSupervisorWorker(t *testing.T, ctx context.Context, roots paths.Roots, repositoryID, worktreeID string) supervisor.WorkerStatus {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		status := readSessionSupervisorStatus(t, ctx, roots)
		for _, worker := range status.Workers {
			if worker.RepositoryID == repositoryID && worker.State == "running" && worker.PID > 0 {
				response, err := supervisor.DoWorker(ctx, supervisor.WorkerSocketPath(roots, repositoryID), supervisor.Request{
					Version: supervisor.ProtocolVersion,
					ID:      fmt.Sprintf("runtime-upgrade-worker-ready-%d", time.Now().UnixNano()),
					Method:  "status", RepositoryID: repositoryID,
					WorktreeID: worktreeID,
				}, time.Second)
				if err == nil && response.OK {
					return worker
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("supervisor worker %s did not become ready: %+v", repositoryID, status.Workers)
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
