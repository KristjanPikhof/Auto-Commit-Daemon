package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

func withSpawnPollSettings(t *testing.T, timeout, interval time.Duration) {
	t.Helper()
	prevTimeout := daemonSpawnPollTimeout
	prevInterval := daemonSpawnPollInterval
	prevAfterDeadline := afterDaemonSpawnPollDeadline
	daemonSpawnPollTimeout = timeout
	daemonSpawnPollInterval = interval
	afterDaemonSpawnPollDeadline = nil
	t.Cleanup(func() {
		daemonSpawnPollTimeout = prevTimeout
		daemonSpawnPollInterval = prevInterval
		afterDaemonSpawnPollDeadline = prevAfterDeadline
	})
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

func openStartDB(t *testing.T, repoDir string) *state.DB {
	t.Helper()
	ctx := context.Background()
	db, err := state.Open(ctx, state.DBPathFromGitDir(filepath.Join(repoDir, ".git")))
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
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

func TestStart_DefaultWatchPIDDisabledPersistsNullWatchPID(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)

	_, restore := installFakeSpawn(t, os.Getpid())
	defer restore()

	var stdout bytes.Buffer
	if err := runStart(ctx, &stdout, repoDir, "session-no-watch", "codex", 0, true); err != nil {
		t.Fatalf("runStart: %v", err)
	}
	db := openStartDB(t, repoDir)
	clients, err := state.ListClients(ctx, db)
	if err != nil {
		t.Fatalf("ListClients: %v", err)
	}
	if len(clients) != 1 {
		t.Fatalf("clients=%d, want 1", len(clients))
	}
	if clients[0].WatchPID.Valid {
		t.Fatalf("watch_pid valid=%v value=%d, want NULL", clients[0].WatchPID.Valid, clients[0].WatchPID.Int64)
	}
	if clients[0].WatchFP.Valid {
		t.Fatalf("watch_fp valid=%v value=%q, want NULL", clients[0].WatchFP.Valid, clients[0].WatchFP.String)
	}
}

func TestStart_AlreadyExitedWatchPIDPersistsNullWatchPID(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)

	cmd := exec.Command("/bin/sh", "-c", "true")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start short-lived child: %v", err)
	}
	deadPID := cmd.Process.Pid
	if err := cmd.Wait(); err != nil {
		t.Fatalf("wait short-lived child: %v", err)
	}

	_, restore := installFakeSpawn(t, os.Getpid())
	defer restore()

	var stdout bytes.Buffer
	if err := runStart(ctx, &stdout, repoDir, "session-dead-watch", "codex", deadPID, true); err != nil {
		t.Fatalf("runStart: %v", err)
	}
	db := openStartDB(t, repoDir)
	clients, err := state.ListClients(ctx, db)
	if err != nil {
		t.Fatalf("ListClients: %v", err)
	}
	if len(clients) != 1 {
		t.Fatalf("clients=%d, want 1", len(clients))
	}
	if clients[0].WatchPID.Valid {
		t.Fatalf("watch_pid valid=%v value=%d, want NULL", clients[0].WatchPID.Valid, clients[0].WatchPID.Int64)
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

func TestStart_RereadsDaemonStateAfterSpawnPollDeadline(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)
	withSpawnPollSettings(t, time.Nanosecond, time.Nanosecond)

	spawnedPID := 111
	finalPID := os.Getpid()
	prevSpawn := spawnDaemon
	spawnDaemon = func(context.Context, string) (int, error) {
		return spawnedPID, nil
	}
	t.Cleanup(func() { spawnDaemon = prevSpawn })
	afterDaemonSpawnPollDeadline = func(ctx context.Context, db *state.DB) {
		if err := state.SaveDaemonState(ctx, db, state.DaemonState{
			PID:         finalPID,
			Mode:        "running",
			HeartbeatTS: nowFloat(),
			UpdatedTS:   nowFloat(),
		}); err != nil {
			t.Fatalf("SaveDaemonState: %v", err)
		}
	}

	var stdout bytes.Buffer
	if err := runStart(ctx, &stdout, repoDir, "session-reread", "codex", 0, true); err != nil {
		t.Fatalf("runStart: %v", err)
	}
	var got startResult
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, stdout.String())
	}
	if got.DaemonPID != finalPID {
		t.Fatalf("daemon_pid=%d, want final daemon_state pid %d instead of spawned pid %d", got.DaemonPID, finalPID, spawnedPID)
	}
}
