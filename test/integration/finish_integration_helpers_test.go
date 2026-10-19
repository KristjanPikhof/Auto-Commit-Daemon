//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

type finishFixture struct {
	Repo, Bin, RepositoryID, WorktreeID, BaseHead, DBPath string
	Env                                                   []string
	Roots                                                 paths.Roots
}

// This fixture explicitly preinstalls an isolated runtime. Fresh bootstrap is
// tested separately by invoking on before any registration or installation.
func newFinishFixture(t *testing.T, extraEnv ...string) *finishFixture {
	t.Helper()
	wt, err := gitpkg.ResolveWorktree(t.Context(), tempRepo(t))
	if err != nil {
		t.Fatal(err)
	}
	env := finishIsolatedEnv(t, extraEnv...)
	if runtime.GOOS == "darwin" {
		env = finishLaunchctlEnv(t, env)
	}
	bin := buildAcdBinary(t)
	roots, repositoryID := prepareCheckpointRegistration(t, env, wt.Root)
	if err := os.MkdirAll(filepath.Dir(roots.ManagedBinaryPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(bin, roots.ManagedBinaryPath()); err != nil {
		t.Fatal(err)
	}
	f := &finishFixture{
		Repo: wt.Root, Bin: bin, Env: env, Roots: roots, RepositoryID: repositoryID,
		WorktreeID: checkpoint.WorktreeID(wt.Root), DBPath: state.DBPathFromGitDir(wt.GitDir),
		BaseHead: strings.TrimSpace(runGitOK(t, wt.Root, "rev-parse", "HEAD")),
	}
	release := checkpointRuntimeStartSlots.acquire()
	t.Cleanup(release)
	startCheckpointSupervisorRuntime(t, env, roots, repositoryID, supervisor.WorkerSocketPath(roots, repositoryID))
	f.Barrier(t, t.Context())
	return f
}

func finishIsolatedEnv(t *testing.T, extraEnv ...string) []string {
	t.Helper()
	var env []string
	for _, item := range withIsolatedHome(t) {
		if !strings.HasPrefix(item, "ACD_") {
			env = append(env, item)
		}
	}
	return envWith(env, extraEnv...)
}

func (f *finishFixture) Run(t *testing.T, ctx context.Context, args ...string) ExecResult {
	t.Helper()
	return runAcd(t, ctx, f.Env, append([]string{"--repo", f.Repo}, args...)...)
}

func (f *finishFixture) Barrier(t *testing.T, ctx context.Context) {
	t.Helper()
	response, err := supervisor.DoWorker(ctx, supervisor.WorkerSocketPath(f.Roots, f.RepositoryID), supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: "finish-test-barrier", Method: "checkpoint_barrier",
		RepositoryID: f.RepositoryID, WorktreeID: f.WorktreeID,
	}, supervisor.CheckpointBarrierTimeout)
	if err != nil || !response.OK {
		t.Fatalf("checkpoint barrier: response=%+v err=%v", response, err)
	}
}

func (f *finishFixture) OpenDB(t *testing.T) *state.DB {
	t.Helper()
	db, err := state.OpenReadOnly(t.Context(), f.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func (f *finishFixture) Restart(t *testing.T, ctx context.Context) {
	t.Helper()
	response, err := (supervisor.Client{SocketPath: f.Roots.SupervisorSocketPath(), Timeout: 30 * time.Second}).Do(ctx, supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: "finish-test-restart", Method: "restart_repository",
		RepositoryID: f.RepositoryID, WorktreeID: f.WorktreeID,
	})
	if err != nil || !response.OK {
		t.Fatalf("restart worker: response=%+v err=%v", response, err)
	}
	// Restart acceptance precedes the replacement worker binding its socket.
	// Retry only connection absence; an accepted worker error is meaningful.
	waitFor(t, "restarted worker checkpoint barrier", 30*time.Second, func() bool {
		response, err := supervisor.DoWorker(ctx, supervisor.WorkerSocketPath(f.Roots, f.RepositoryID), supervisor.Request{
			Version: supervisor.ProtocolVersion, ID: "finish-test-restart-barrier", Method: "checkpoint_barrier",
			RepositoryID: f.RepositoryID, WorktreeID: f.WorktreeID,
		}, supervisor.CheckpointBarrierTimeout)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
			return false
		}
		if err != nil || !response.OK {
			t.Fatalf("restarted worker barrier: response=%+v err=%v", response, err)
		}
		return true
	})
}

func (f *finishFixture) Apply(t *testing.T, ctx context.Context, draft map[string]string, confirmed ...ai.ConfirmationRequirement) settings.ApplyResult {
	t.Helper()
	lookup := func(key string) (string, bool) {
		for index := len(f.Env) - 1; index >= 0; index-- {
			if value, found := strings.CutPrefix(f.Env[index], key+"="); found {
				return value, true
			}
		}
		return "", false
	}
	service, err := settings.NewService(ctx, settings.Options{Roots: f.Roots, RepoPath: f.Repo, LookupEnv: lookup, CredentialLookup: lookup})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	snapshot, err := service.Snapshot(ctx, settings.ScopeRepository, "")
	if err != nil {
		t.Fatal(err)
	}
	validation, err := service.Validate(ctx, draft, confirmed)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Apply(ctx, settings.ApplyRequest{
		Values: draft, Confirmations: confirmed, TestedFingerprint: validation.Fingerprint,
		ExpectedGeneration: snapshot.SavedGeneration, ExpectedDesiredRevision: snapshot.DesiredRevisionID,
	})
	if err != nil || !result.Applied {
		t.Fatalf("apply approved settings: result=%+v err=%v", result, err)
	}
	return result
}

// The real host launchctl must never inspect or mutate its global ACD label.
// The session backend needs only an absent legacy service; unexpected calls
// fail the test even when production treats a failed command as best effort.
func finishLaunchctlEnv(t *testing.T, env []string) []string {
	t.Helper()
	dir := t.TempDir()
	violations := filepath.Join(dir, "unexpected-launchctl")
	target := fmt.Sprintf("gui/%d/%s", os.Getuid(), supervisor.ServiceLabel)
	body := fmt.Sprintf("#!/bin/sh\nif [ \"$#\" -eq 2 ] && [ \"$1\" = print ] && [ \"$2\" = '%s' ]; then exit 1; fi\nprintf '%%s\\n' \"$@\" >> '%s'\nexit 97\n", target, violations)
	if err := os.WriteFile(filepath.Join(dir, "launchctl"), []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if raw, err := os.ReadFile(violations); err == nil {
			t.Errorf("unexpected service manager call: %s", raw)
		} else if !os.IsNotExist(err) {
			t.Error(err)
		}
	})
	return envWith(env, "PATH="+dir+string(os.PathListSeparator)+envValue(env, "PATH"))
}

func finishDecodeJSON(t *testing.T, result ExecResult, target any) {
	t.Helper()
	if result.ExitCode != 0 {
		t.Fatalf("command exit=%d stdout=%s stderr=%s", result.ExitCode, result.Stdout, result.Stderr)
	}
	if err := json.Unmarshal([]byte(result.Stdout), target); err != nil {
		t.Fatalf("decode command JSON: %v; output=%s", err, result.Stdout)
	}
}
