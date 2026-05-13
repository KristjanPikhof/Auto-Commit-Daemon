package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
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

func TestStart_ManualEmptySessionUsesDeterministicHumanSession(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)

	count, restore := installFakeSpawn(t, os.Getpid())
	defer restore()

	var stdout bytes.Buffer
	if err := runStart(ctx, &stdout, repoDir, "", "", 0, true); err != nil {
		t.Fatalf("first runStart: %v", err)
	}
	var first startResult
	if err := json.Unmarshal(stdout.Bytes(), &first); err != nil {
		t.Fatalf("unmarshal first: %v\n%s", err, stdout.String())
	}
	if first.SessionID != humanStartSessionID(first.RepoHash) {
		t.Fatalf("session_id=%q, want deterministic human session for repo hash %q", first.SessionID, first.RepoHash)
	}
	if first.Harness != "other" {
		t.Fatalf("harness=%q, want other", first.Harness)
	}
	if !first.Started || first.Duplicate || first.ClientCount != 1 {
		t.Fatalf("unexpected first start result: %+v", first)
	}

	stdout.Reset()
	if err := runStart(ctx, &stdout, repoDir, "", "", 0, true); err != nil {
		t.Fatalf("second runStart: %v", err)
	}
	var second startResult
	if err := json.Unmarshal(stdout.Bytes(), &second); err != nil {
		t.Fatalf("unmarshal second: %v\n%s", err, stdout.String())
	}
	if second.SessionID != first.SessionID {
		t.Fatalf("second session_id=%q, want %q", second.SessionID, first.SessionID)
	}
	if second.Started || !second.Duplicate || second.ClientCount != 1 {
		t.Fatalf("unexpected second start result: %+v", second)
	}
	if count.Load() != 1 {
		t.Fatalf("spawn count=%d, want 1", count.Load())
	}

	db := openStartDB(t, repoDir)
	clients, err := state.ListClients(ctx, db)
	if err != nil {
		t.Fatalf("ListClients: %v", err)
	}
	if len(clients) != 1 {
		t.Fatalf("clients=%d, want 1", len(clients))
	}
	if clients[0].SessionID != first.SessionID || clients[0].Harness != "other" {
		t.Fatalf("client row mismatch: %+v", clients[0])
	}
}

func TestStart_EmptySessionWithHarnessRequiresExplicitSession(t *testing.T) {
	var stdout bytes.Buffer
	err := runStart(context.Background(), &stdout, ".", "", "codex", 0, true)
	if err == nil {
		t.Fatalf("runStart succeeded without session_id for harness start")
	}
	if !strings.Contains(err.Error(), "--session-id is required when --harness is set") {
		t.Fatalf("error %q does not explain explicit harness session requirement", err)
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

func TestStart_CanonicalizesSubdirectoryForIdentityAndRegistry(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)
	wt, err := git.ResolveWorktree(ctx, repoDir)
	if err != nil {
		t.Fatalf("resolve worktree: %v", err)
	}
	repoDir = wt.Root
	nestedA := filepath.Join(repoDir, "nested", "a")
	nestedB := filepath.Join(repoDir, "nested", "b")
	if err := os.MkdirAll(nestedA, 0o755); err != nil {
		t.Fatalf("mkdir nestedA: %v", err)
	}
	if err := os.MkdirAll(nestedB, 0o755); err != nil {
		t.Fatalf("mkdir nestedB: %v", err)
	}

	var spawnedRepo string
	prevSpawn := spawnDaemon
	var count atomic.Int32
	spawnDaemon = func(ctx context.Context, repoAbs string) (int, error) {
		count.Add(1)
		spawnedRepo = repoAbs
		gitDir := filepath.Join(repoAbs, ".git")
		dbPath := state.DBPathFromGitDir(gitDir)
		db, err := state.Open(ctx, dbPath)
		if err != nil {
			return 0, err
		}
		defer db.Close()
		if err := state.SaveDaemonState(ctx, db, state.DaemonState{
			PID:         os.Getpid(),
			Mode:        "running",
			HeartbeatTS: nowFloat(),
			UpdatedTS:   nowFloat(),
		}); err != nil {
			return 0, err
		}
		return os.Getpid(), nil
	}
	t.Cleanup(func() { spawnDaemon = prevSpawn })

	var stdout bytes.Buffer
	if err := runStart(ctx, &stdout, nestedA, "session-a", "codex", 0, true); err != nil {
		t.Fatalf("first runStart: %v", err)
	}
	var first startResult
	if err := json.Unmarshal(stdout.Bytes(), &first); err != nil {
		t.Fatalf("unmarshal first: %v\n%s", err, stdout.String())
	}
	wantHash, err := paths.RepoHash(repoDir)
	if err != nil {
		t.Fatalf("RepoHash: %v", err)
	}
	if first.Repo != repoDir || first.RepoHash != wantHash || first.SessionID != "session-a" {
		t.Fatalf("first result = %+v, want canonical repo %q hash %q", first, repoDir, wantHash)
	}
	if spawnedRepo != repoDir {
		t.Fatalf("spawned repo = %q, want canonical root %q", spawnedRepo, repoDir)
	}

	stdout.Reset()
	if err := runStart(ctx, &stdout, nestedB, "session-b", "pi", 0, true); err != nil {
		t.Fatalf("second runStart: %v", err)
	}
	var second startResult
	if err := json.Unmarshal(stdout.Bytes(), &second); err != nil {
		t.Fatalf("unmarshal second: %v\n%s", err, stdout.String())
	}
	if second.Repo != repoDir || second.RepoHash != wantHash {
		t.Fatalf("second result = %+v, want canonical repo %q hash %q", second, repoDir, wantHash)
	}
	if count.Load() != 1 {
		t.Fatalf("spawn count = %d, want 1", count.Load())
	}

	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if len(reg.Repos) != 1 {
		t.Fatalf("registry rows = %d, want 1: %+v", len(reg.Repos), reg.Repos)
	}
	row := reg.Repos[0]
	if row.Path != repoDir {
		t.Fatalf("registry path = %q, want canonical root %q", row.Path, repoDir)
	}
	if row.RepoHash != wantHash {
		t.Fatalf("registry hash = %q, want %q", row.RepoHash, wantHash)
	}
	wantStateDB := state.DBPathFromGitDir(filepath.Join(repoDir, ".git"))
	if row.StateDB != wantStateDB {
		t.Fatalf("registry state_db = %q, want %q", row.StateDB, wantStateDB)
	}
	for _, bad := range []string{nestedA, nestedB} {
		if central.SameRepoPath(row.Path, bad) {
			t.Fatalf("registry path %q unexpectedly matched subdir %q", row.Path, bad)
		}
	}
}

func TestStart_MergesLegacySubdirRegistryRowByStateDB(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)
	wt, err := git.ResolveWorktree(ctx, repoDir)
	if err != nil {
		t.Fatalf("resolve worktree: %v", err)
	}
	repoDir = wt.Root
	legacySubdir := filepath.Join(repoDir, "legacy", "subdir")
	if err := os.MkdirAll(legacySubdir, 0o755); err != nil {
		t.Fatalf("mkdir legacy subdir: %v", err)
	}
	stateDB := state.DBPathFromGitDir(wt.GitDir)
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.Repos = []central.RepoRecord{{
			Path:              legacySubdir,
			RepoHash:          "legacy-hash",
			StateDB:           stateDB,
			FirstRegisteredTS: 10,
			LastSeenTS:        20,
			Harnesses:         []string{"codex"},
		}}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy registry: %v", err)
	}

	count, restore := installFakeSpawn(t, os.Getpid())
	defer restore()
	var stdout bytes.Buffer
	if err := runStart(ctx, &stdout, repoDir, "root-session", "pi", 0, true); err != nil {
		t.Fatalf("runStart: %v", err)
	}
	var res startResult
	if err := json.Unmarshal(stdout.Bytes(), &res); err != nil {
		t.Fatalf("unmarshal start: %v\n%s", err, stdout.String())
	}
	if res.Repo != repoDir {
		t.Fatalf("start repo=%q want canonical root %q", res.Repo, repoDir)
	}
	if count.Load() != 1 {
		t.Fatalf("spawn count=%d want 1", count.Load())
	}

	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if len(reg.Repos) != 1 {
		t.Fatalf("registry rows=%d want 1: %+v", len(reg.Repos), reg.Repos)
	}
	rec := reg.Repos[0]
	if rec.Path != repoDir || rec.StateDB != stateDB {
		t.Fatalf("registry record=%+v want canonical path %q state_db %q", rec, repoDir, stateDB)
	}
	if !reflect.DeepEqual(rec.Harnesses, []string{"codex", "pi"}) {
		t.Fatalf("harnesses=%v want [codex pi]", rec.Harnesses)
	}
}

func TestStart_LinkedWorktreeGitFileCanonicalizesSubdirAndStateDB(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	mainRepo := makeStartRepo(t)
	for _, kv := range [][2]string{
		{"user.email", "acd-test@example.com"},
		{"user.name", "ACD Test"},
		{"commit.gpgsign", "false"},
	} {
		if _, err := git.Run(ctx, git.RunOpts{Dir: mainRepo}, "config", kv[0], kv[1]); err != nil {
			t.Fatalf("git config %s: %v", kv[0], err)
		}
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: mainRepo}, "commit", "--allow-empty", "-m", "init"); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	linked := filepath.Join(t.TempDir(), "linked")
	if _, err := git.Run(ctx, git.RunOpts{Dir: mainRepo}, "worktree", "add", "-q", "-b", "linked-start", linked); err != nil {
		t.Fatalf("git worktree add: %v", err)
	}
	if info, err := os.Stat(filepath.Join(linked, ".git")); err != nil {
		t.Fatalf("stat linked .git: %v", err)
	} else if info.IsDir() {
		t.Fatalf("linked worktree .git is a directory, want git-file")
	}
	wt, err := git.ResolveWorktree(ctx, linked)
	if err != nil {
		t.Fatalf("resolve linked worktree: %v", err)
	}
	nested := filepath.Join(linked, "pkg", "deep")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}

	var spawnedRepo string
	prevSpawn := spawnDaemon
	spawnDaemon = func(ctx context.Context, repoAbs string) (int, error) {
		spawnedRepo = repoAbs
		spawnWT, err := git.ResolveWorktree(ctx, repoAbs)
		if err != nil {
			return 0, err
		}
		db, err := state.Open(ctx, state.DBPathFromGitDir(spawnWT.GitDir))
		if err != nil {
			return 0, err
		}
		defer db.Close()
		if err := state.SaveDaemonState(ctx, db, state.DaemonState{
			PID:         os.Getpid(),
			Mode:        "running",
			HeartbeatTS: nowFloat(),
			UpdatedTS:   nowFloat(),
		}); err != nil {
			return 0, err
		}
		return os.Getpid(), nil
	}
	t.Cleanup(func() { spawnDaemon = prevSpawn })

	var stdout bytes.Buffer
	if err := runStart(ctx, &stdout, nested, "linked-session", "codex", 0, true); err != nil {
		t.Fatalf("runStart linked subdir: %v", err)
	}
	var res startResult
	if err := json.Unmarshal(stdout.Bytes(), &res); err != nil {
		t.Fatalf("unmarshal start result: %v\n%s", err, stdout.String())
	}
	if res.Repo != wt.Root {
		t.Fatalf("start repo=%q want linked worktree root %q", res.Repo, wt.Root)
	}
	if spawnedRepo != wt.Root {
		t.Fatalf("spawned repo=%q want linked worktree root %q", spawnedRepo, wt.Root)
	}

	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if len(reg.Repos) != 1 {
		t.Fatalf("registry rows=%d, want 1: %+v", len(reg.Repos), reg.Repos)
	}
	row := reg.Repos[0]
	if row.Path != wt.Root {
		t.Fatalf("registry path=%q want linked worktree root %q", row.Path, wt.Root)
	}
	wantStateDB := state.DBPathFromGitDir(wt.GitDir)
	if row.StateDB != wantStateDB {
		t.Fatalf("registry state_db=%q want resolved git dir state DB %q", row.StateDB, wantStateDB)
	}
	if row.StateDB == state.DBPathFromGitDir(filepath.Join(wt.Root, ".git")) {
		t.Fatalf("registry state_db used literal .git file path: %q", row.StateDB)
	}
}

func TestStart_NonWorktreeFailsBeforeRegistryOrDaemonSpawn(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	nonRepo := t.TempDir()

	count, restore := installFakeSpawn(t, os.Getpid())
	defer restore()

	var stdout bytes.Buffer
	err := runStart(ctx, &stdout, nonRepo, "session-x", "codex", 0, true)
	if err == nil {
		t.Fatalf("runStart succeeded for non-worktree")
	}
	if !strings.Contains(err.Error(), "not inside a Git worktree") {
		t.Fatalf("error %q does not mention non-worktree", err)
	}
	if count.Load() != 0 {
		t.Fatalf("spawn count = %d, want 0", count.Load())
	}
	if _, statErr := os.Stat(roots.RegistryPath()); !os.IsNotExist(statErr) {
		t.Fatalf("registry stat err = %v, want not exist before upsert", statErr)
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

func TestStart_TenConcurrentReportsSurvivingDaemonPID(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)
	withSpawnPollSettings(t, 5*time.Second, 5*time.Millisecond)

	const clients = 10
	survivorPID := os.Getpid()
	var spawnCount atomic.Int32
	var survivorSaved atomic.Bool
	prevSpawn := spawnDaemon
	spawnDaemon = func(ctx context.Context, repoAbs string) (int, error) {
		n := spawnCount.Add(1)
		if n == clients && survivorSaved.CompareAndSwap(false, true) {
			db, err := state.Open(ctx, state.DBPathFromGitDir(filepath.Join(repoAbs, ".git")))
			if err != nil {
				return 0, err
			}
			defer db.Close()
			if err := state.SaveDaemonState(ctx, db, state.DaemonState{
				PID:         survivorPID,
				Mode:        "running",
				HeartbeatTS: nowFloat(),
				UpdatedTS:   nowFloat(),
			}); err != nil {
				return 0, err
			}
		}
		if n == 1 {
			return survivorPID, nil
		}
		return 900000 + int(n), nil
	}
	t.Cleanup(func() { spawnDaemon = prevSpawn })

	var wg sync.WaitGroup
	results := make(chan startResult, clients)
	errs := make(chan error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var stdout bytes.Buffer
			if err := runStart(ctx, &stdout, repoDir, "session-concurrent-"+string(rune('a'+i)), "codex", 0, true); err != nil {
				errs <- err
				return
			}
			var got startResult
			if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
				errs <- err
				return
			}
			results <- got
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Fatalf("runStart concurrent: %v", err)
	}
	var seen int
	var started int
	for got := range results {
		seen++
		if got.DaemonPID != survivorPID {
			t.Fatalf("session %s daemon_pid=%d, want survivor %d", got.SessionID, got.DaemonPID, survivorPID)
		}
		if got.Started {
			started++
		}
	}
	if seen != clients {
		t.Fatalf("results=%d, want %d", seen, clients)
	}
	if started != 1 {
		t.Fatalf("started results=%d, want 1 winner", started)
	}
}
