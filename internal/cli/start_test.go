package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

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

func TestStart_FirstCall_StartsDaemon(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	_ = db.Close() // start.go reopens the DB itself
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
	repoDir, _, db := makeRepoStateDB(t)
	_ = db.Close()

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
	repoDir, _, db := makeRepoStateDB(t)
	_ = db.Close()
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
