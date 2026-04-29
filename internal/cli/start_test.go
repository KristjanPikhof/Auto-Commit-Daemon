package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// installFakeSpawn replaces spawnDaemon with a stub that simulates a healthy
// daemon by stamping daemon_state(pid=fakePID, mode="running") into the
// per-repo DB. Returns the call count + restore func.
func installFakeSpawn(t *testing.T, fakePID int) (*atomic.Int32, func()) {
	t.Helper()
	prev := spawnDaemon
	var count atomic.Int32
	spawnDaemon = func(ctx context.Context, repoAbs string) (int, error) {
		count.Add(1)
		gitDir := filepath.Join(repoAbs, ".git")
		dbPath := state.DBPathFromGitDir(gitDir)
		db, err := state.Open(ctx, dbPath)
		if err != nil {
			return 0, err
		}
		defer db.Close()
		if err := state.SaveDaemonState(ctx, db, state.DaemonState{
			PID:         fakePID,
			Mode:        "running",
			HeartbeatTS: nowFloat(),
			UpdatedTS:   nowFloat(),
		}); err != nil {
			return 0, err
		}
		return fakePID, nil
	}
	return &count, func() { spawnDaemon = prev }
}

func makeStartRepo(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	repoDir := t.TempDir()
	if err := git.Init(ctx, repoDir); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref HEAD: %v", err)
	}
	return repoDir
}

func commitStartRepoSeed(t *testing.T, repoDir string) string {
	t.Helper()
	ctx := context.Background()
	for _, kv := range [][]string{
		{"user.email", "acd-test@example.com"},
		{"user.name", "ACD Test"},
		{"commit.gpgsign", "false"},
	} {
		if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "config", kv[0], kv[1]); err != nil {
			t.Fatalf("git config %s: %v", kv[0], err)
		}
	}
	if err := os.WriteFile(filepath.Join(repoDir, "seed.txt"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "add", "seed.txt"); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "commit", "-q", "-m", "seed"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	head, err := git.RevParse(ctx, repoDir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	return head
}

func TestStart_FirstCall_StartsDaemon(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)
	t.Logf("roots=%+v", roots)

	count, restore := installFakeSpawn(t, os.Getpid())
	defer restore()

	var stdout bytes.Buffer
	if err := runStart(ctx, &stdout, repoDir, "session-1", "claude-code", 0, true); err != nil {
		t.Fatalf("runStart: %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("expected spawn count 1, got %d", count.Load())
	}
	var got startResult
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, stdout.String())
	}
	if !got.Started || got.Duplicate {
		t.Fatalf("expected started=true duplicate=false, got %+v", got)
	}
	if got.SessionID != "session-1" || got.Harness != "claude-code" {
		t.Fatalf("session/harness mismatch: %+v", got)
	}
	if got.ClientCount != 1 {
		t.Fatalf("expected client_count=1, got %d", got.ClientCount)
	}
}

func TestStart_DuplicateSession_NoRespawn(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)

	count, restore := installFakeSpawn(t, os.Getpid())
	defer restore()

	var stdout bytes.Buffer
	if err := runStart(ctx, &stdout, repoDir, "session-1", "claude-code", 0, true); err != nil {
		t.Fatalf("first runStart: %v", err)
	}
	stdout.Reset()
	if err := runStart(ctx, &stdout, repoDir, "session-1", "claude-code", 0, true); err != nil {
		t.Fatalf("second runStart: %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("expected spawn count to remain 1, got %d", count.Load())
	}
	var got startResult
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Started {
		t.Fatalf("expected started=false on duplicate, got %+v", got)
	}
	if !got.Duplicate {
		t.Fatalf("expected duplicate=true on second call, got %+v", got)
	}
}

func TestStart_RegistryUpdated(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)
	_, restore := installFakeSpawn(t, os.Getpid())
	defer restore()

	var stdout bytes.Buffer
	if err := runStart(ctx, &stdout, repoDir, "session-x", "codex", 0, true); err != nil {
		t.Fatalf("runStart: %v", err)
	}
	regPath := roots.RegistryPath()
	body, err := os.ReadFile(regPath)
	if err != nil {
		t.Fatalf("read registry: %v", err)
	}
	if !bytes.Contains(body, []byte(repoDir)) || !bytes.Contains(body, []byte("codex")) {
		t.Fatalf("registry missing repo or harness:\n%s", body)
	}
}

func TestStart_DetachedHEADRefused(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)
	head := commitStartRepoSeed(t, repoDir)
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "checkout", "--detach", head); err != nil {
		t.Fatalf("git checkout --detach: %v", err)
	}

	count, restore := installFakeSpawn(t, os.Getpid())
	defer restore()

	var stdout bytes.Buffer
	err := runStart(ctx, &stdout, repoDir, "session-detached", "codex", 0, true)
	if err == nil {
		t.Fatalf("runStart succeeded on detached HEAD")
	}
	if !strings.Contains(err.Error(), "detached HEAD") {
		t.Fatalf("error %q does not mention detached HEAD", err)
	}
	if count.Load() != 0 {
		t.Fatalf("spawn count=%d want 0", count.Load())
	}
}
