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
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// Decision matrix coverage for evaluateShortCircuit. Each row drives the
// pure decision function with an explicit cache + registry snapshot so
// none of the cases need a real daemon, real SQLite, or real flock.
func TestEvaluateShortCircuit_Matrix(t *testing.T) {
	const repoHash = "deadbeef00"
	const sessionID = "sess-A"
	const harness = "claude-code"
	const livePID = 4242
	const deadPID = 4243

	now := time.Unix(1_700_000_000, 0)
	ttl := 30 * time.Minute
	freshTS := now.Add(-1 * time.Minute).Unix()
	staleTS := now.Add(-31 * time.Minute).Unix()

	pidAlive := func(pid int) bool { return pid == livePID }

	freshRegistry := &central.RepoRecord{
		Path:       "/tmp/x",
		RepoHash:   repoHash,
		LastSeenTS: freshTS,
	}

	tests := []struct {
		name      string
		cache     *startCache
		registry  *central.RepoRecord
		wantOK    bool
		wantBlame string // substring expected in Reason on escalation
	}{
		{
			name: "same_session_and_fresh_short_circuits",
			cache: &startCache{
				Version: startCacheVersion, RepoHash: repoHash,
				SessionID: sessionID, Harness: harness,
				DaemonPID: livePID, UpdatedAt: freshTS,
			},
			registry: freshRegistry,
			wantOK:   true,
		},
		{
			name:      "no_cache_escalates",
			cache:     nil,
			registry:  freshRegistry,
			wantOK:    false,
			wantBlame: "no_cache",
		},
		{
			name: "different_session_escalates",
			cache: &startCache{
				Version: startCacheVersion, RepoHash: repoHash,
				SessionID: "other-session", Harness: harness,
				DaemonPID: livePID, UpdatedAt: freshTS,
			},
			registry:  freshRegistry,
			wantOK:    false,
			wantBlame: "session_mismatch",
		},
		{
			name: "stale_heartbeat_escalates",
			cache: &startCache{
				Version: startCacheVersion, RepoHash: repoHash,
				SessionID: sessionID, Harness: harness,
				DaemonPID: livePID, UpdatedAt: staleTS,
			},
			registry:  freshRegistry,
			wantOK:    false,
			wantBlame: "stale_heartbeat",
		},
		{
			name: "missing_registry_entry_escalates",
			cache: &startCache{
				Version: startCacheVersion, RepoHash: repoHash,
				SessionID: sessionID, Harness: harness,
				DaemonPID: livePID, UpdatedAt: freshTS,
			},
			registry:  nil,
			wantOK:    false,
			wantBlame: "registry_missing_repo",
		},
		{
			name: "harness_mismatch_escalates",
			cache: &startCache{
				Version: startCacheVersion, RepoHash: repoHash,
				SessionID: sessionID, Harness: "codex",
				DaemonPID: livePID, UpdatedAt: freshTS,
			},
			registry:  freshRegistry,
			wantOK:    false,
			wantBlame: "harness_mismatch",
		},
		{
			name: "dead_daemon_pid_escalates",
			cache: &startCache{
				Version: startCacheVersion, RepoHash: repoHash,
				SessionID: sessionID, Harness: harness,
				DaemonPID: deadPID, UpdatedAt: freshTS,
			},
			registry:  freshRegistry,
			wantOK:    false,
			wantBlame: "daemon_pid_dead",
		},
		{
			name: "repo_hash_mismatch_in_cache_escalates",
			cache: &startCache{
				Version: startCacheVersion, RepoHash: "differenthash",
				SessionID: sessionID, Harness: harness,
				DaemonPID: livePID, UpdatedAt: freshTS,
			},
			registry:  freshRegistry,
			wantOK:    false,
			wantBlame: "repo_hash_mismatch",
		},
		{
			name: "registry_hash_mismatch_escalates",
			cache: &startCache{
				Version: startCacheVersion, RepoHash: repoHash,
				SessionID: sessionID, Harness: harness,
				DaemonPID: livePID, UpdatedAt: freshTS,
			},
			registry: &central.RepoRecord{
				Path: "/tmp/x", RepoHash: "wronghash", LastSeenTS: freshTS,
			},
			wantOK:    false,
			wantBlame: "registry_hash_mismatch",
		},
		{
			name: "zero_updated_at_escalates",
			cache: &startCache{
				Version: startCacheVersion, RepoHash: repoHash,
				SessionID: sessionID, Harness: harness,
				DaemonPID: livePID, UpdatedAt: 0,
			},
			registry:  freshRegistry,
			wantOK:    false,
			wantBlame: "stale_heartbeat",
		},
		{
			name: "no_daemon_pid_escalates",
			cache: &startCache{
				Version: startCacheVersion, RepoHash: repoHash,
				SessionID: sessionID, Harness: harness,
				DaemonPID: 0, UpdatedAt: freshTS,
			},
			registry:  freshRegistry,
			wantOK:    false,
			wantBlame: "no_daemon_pid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := evaluateShortCircuit(tt.cache, repoHash, sessionID, harness,
				tt.registry, now, ttl, pidAlive)
			if d.OK != tt.wantOK {
				t.Fatalf("OK=%v want %v (reason=%q)", d.OK, tt.wantOK, d.Reason)
			}
			if !tt.wantOK && !strings.Contains(d.Reason, tt.wantBlame) {
				t.Fatalf("Reason=%q does not contain %q", d.Reason, tt.wantBlame)
			}
			if tt.wantOK && d.DaemonPID != livePID {
				t.Fatalf("DaemonPID=%d want %d", d.DaemonPID, livePID)
			}
		})
	}
}

// readStartCache must tolerate missing files, empty files, malformed JSON,
// and a future schema version. None of these should bubble up an error;
// they all return nil so the caller takes the cold path.
func TestReadStartCache_TolerantOfBadInputs(t *testing.T) {
	dir := t.TempDir()
	// Missing.
	if got := readStartCache(filepath.Join(dir, "nope.json")); got != nil {
		t.Fatalf("missing file should yield nil, got %+v", got)
	}
	// Empty.
	emptyPath := filepath.Join(dir, "empty.json")
	if err := os.WriteFile(emptyPath, []byte{}, 0o600); err != nil {
		t.Fatalf("write empty: %v", err)
	}
	if got := readStartCache(emptyPath); got != nil {
		t.Fatalf("empty file should yield nil, got %+v", got)
	}
	// Malformed.
	badPath := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(badPath, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write bad: %v", err)
	}
	if got := readStartCache(badPath); got != nil {
		t.Fatalf("bad json should yield nil, got %+v", got)
	}
	// Future version.
	futurePath := filepath.Join(dir, "v999.json")
	body, _ := json.Marshal(map[string]any{"version": 999, "session_id": "x"})
	if err := os.WriteFile(futurePath, body, 0o600); err != nil {
		t.Fatalf("write future: %v", err)
	}
	if got := readStartCache(futurePath); got != nil {
		t.Fatalf("future version should yield nil, got %+v", got)
	}
}

// writeStartCache + readStartCache round-trip including the parent dir
// being created on demand under <gitDir>/acd.
func TestWriteStartCache_RoundTripCreatesParent(t *testing.T) {
	gitDir := t.TempDir()
	in := startCache{
		Version: startCacheVersion, RepoHash: "abc",
		SessionID: "s", Harness: "claude-code",
		DaemonPID: 9999, WatchPID: 1234, UpdatedAt: 42,
	}
	if err := writeStartCache(gitDir, in); err != nil {
		t.Fatalf("writeStartCache: %v", err)
	}
	got := readStartCache(startCachePath(gitDir))
	if got == nil {
		t.Fatalf("expected cache hit, got nil")
	}
	if got.SessionID != in.SessionID || got.DaemonPID != in.DaemonPID ||
		got.RepoHash != in.RepoHash || got.WatchPID != in.WatchPID ||
		got.UpdatedAt != in.UpdatedAt {
		t.Fatalf("round-trip mismatch:\n in=%+v\ngot=%+v", in, *got)
	}
}

// tryShortCircuitStart end-to-end: a fresh cache + a registered repo lets
// the call return OK without us needing to mock anything else.
func TestTryShortCircuitStart_HappyPath(t *testing.T) {
	roots := withIsolatedHome(t)
	repoDir := t.TempDir()
	gitDir := filepath.Join(repoDir, ".git")
	if err := os.MkdirAll(filepath.Join(gitDir, "acd"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	repoHash, err := paths.RepoHash(repoDir)
	if err != nil {
		t.Fatalf("RepoHash: %v", err)
	}
	// Stamp a registry row for this repo.
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.UpsertRepo(repoDir, repoHash, "/state/db", "claude-code", time.Now().Unix())
		return nil
	}); err != nil {
		t.Fatalf("registry WithLock: %v", err)
	}
	// Stamp a fresh cache pointing at our own pid (always alive).
	if err := writeStartCache(gitDir, startCache{
		Version: startCacheVersion, RepoHash: repoHash,
		SessionID: "sess-A", Harness: "claude-code",
		DaemonPID: os.Getpid(), UpdatedAt: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("writeStartCache: %v", err)
	}

	ok, pid, _, reason := tryShortCircuitStart(context.Background(), gitDir,
		repoHash, "sess-A", "claude-code", repoDir)
	if !ok {
		t.Fatalf("short-circuit failed: reason=%q", reason)
	}
	if pid != os.Getpid() {
		t.Fatalf("daemon pid=%d want %d", pid, os.Getpid())
	}
}

// tryShortCircuitStart escalates when no cache is on disk yet.
func TestTryShortCircuitStart_NoCacheEscalates(t *testing.T) {
	_ = withIsolatedHome(t)
	repoDir := t.TempDir()
	gitDir := filepath.Join(repoDir, ".git")
	if err := os.MkdirAll(gitDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	repoHash, err := paths.RepoHash(repoDir)
	if err != nil {
		t.Fatalf("RepoHash: %v", err)
	}
	ok, _, _, reason := tryShortCircuitStart(context.Background(), gitDir,
		repoHash, "sess-A", "claude-code", repoDir)
	if ok {
		t.Fatalf("expected escalation when cache missing")
	}
	if !strings.Contains(reason, "no_cache") {
		t.Fatalf("reason=%q want no_cache", reason)
	}
}

// Repeated runStart calls under the same session_id must NOT spawn a new
// daemon NOR reach the SQLite path: spawnCount stays at 1 and the
// short-circuit path returns success directly.
func TestRunStart_RepeatedActiveHooks_ShortCircuit(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)

	count, restore := installFakeSpawn(t, os.Getpid())
	defer restore()

	// First call takes the cold path (writes the cache).
	var first bytes.Buffer
	if err := runStart(ctx, &first, repoDir, "sess-active-hook", "claude-code", 0, true); err != nil {
		t.Fatalf("first runStart: %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("first call spawn count=%d want 1", count.Load())
	}

	// Replace state.Open / control.lock with shims that detonate if the
	// short-circuit ever falls through. The simplest "did we touch
	// SQLite?" tripwire is to delete .git/acd/state.db before the second
	// call — if the cold path runs, state.Open would re-create it and
	// the test would observe the regenerated file. The short-circuit
	// must skip that entirely.
	gitDir := filepath.Join(repoDir, ".git")
	dbPath := filepath.Join(gitDir, "acd", "state.db")
	if err := os.Remove(dbPath); err != nil {
		t.Fatalf("remove state.db: %v", err)
	}

	// Second call must short-circuit without re-opening SQLite.
	var second bytes.Buffer
	if err := runStart(ctx, &second, repoDir, "sess-active-hook", "claude-code", 0, true); err != nil {
		t.Fatalf("second runStart: %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("second call spawn count=%d want 1 (no respawn)", count.Load())
	}
	// state.db must still be absent — the short-circuit did not call state.Open.
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("state.db reappeared at %s err=%v — short-circuit hit cold path", dbPath, err)
	}

	// JSON output sanity: non-error, daemon pid surfaced from the cache.
	var got startResult
	if err := json.Unmarshal(second.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal second: %v\n%s", err, second.String())
	}
	if got.SessionID != "sess-active-hook" || got.Harness != "claude-code" {
		t.Fatalf("session/harness drift: %+v", got)
	}
	if got.DaemonPID != os.Getpid() {
		t.Fatalf("daemon pid=%d want %d", got.DaemonPID, os.Getpid())
	}
}

// A different session_id MUST escalate to the cold path; the short-circuit
// is keyed on session_id and harness.
func TestRunStart_DifferentSession_NoShortCircuit(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)

	count, restore := installFakeSpawn(t, os.Getpid())
	defer restore()

	// Cold path: register session-A.
	var first bytes.Buffer
	if err := runStart(ctx, &first, repoDir, "sess-A", "claude-code", 0, true); err != nil {
		t.Fatalf("runStart sess-A: %v", err)
	}
	// session-B must escalate (different session_id) and run the cold
	// path; spawn count stays at 1 because the daemon row is healthy.
	var second bytes.Buffer
	if err := runStart(ctx, &second, repoDir, "sess-B", "claude-code", 0, true); err != nil {
		t.Fatalf("runStart sess-B: %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("spawn count=%d want 1 (daemon healthy across both calls)", count.Load())
	}
	// Expect the registry to show two distinct sessions registered;
	// confirm via an SQLite read in the test harness (not the short-
	// circuit path).
	db := openStartDB(t, repoDir)
	clients, err := state.ListClients(ctx, db)
	if err != nil {
		t.Fatalf("ListClients: %v", err)
	}
	if len(clients) != 2 {
		t.Fatalf("client count=%d want 2", len(clients))
	}
}

// shortCircuitNow override knob — keep it from leaking across tests.
func TestShortCircuitNow_Overridable(t *testing.T) {
	prev := shortCircuitNow
	t.Cleanup(func() { shortCircuitNow = prev })
	called := atomic.Int32{}
	shortCircuitNow = func() time.Time {
		called.Add(1)
		return time.Unix(1, 0)
	}
	if got := shortCircuitNow(); got.Unix() != 1 {
		t.Fatalf("override clock not applied: %v", got)
	}
	if called.Load() != 1 {
		t.Fatalf("clock invocations=%d want 1", called.Load())
	}
}
