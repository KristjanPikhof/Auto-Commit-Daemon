package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// fpStub returns a stub captureDaemonFingerprint that always reports the
// supplied (start, argv) tuple. Tests pass it through evaluateShortCircuit's
// fpCapture parameter to pin a deterministic fingerprint.
func fpStub(startTS, argvHash string) func(context.Context, int) (identity.Fingerprint, error) {
	return func(context.Context, int) (identity.Fingerprint, error) {
		return identity.Fingerprint{StartTime: startTS, ArgvHash: argvHash}, nil
	}
}

// installFakeDaemonFingerprint replaces captureDaemonFingerprint for the
// duration of the test with a stub returning fp. The package-level var is
// the entry point used by tryShortCircuitStart and the cold-path writer.
func installFakeDaemonFingerprint(t *testing.T, fp identity.Fingerprint) {
	t.Helper()
	prev := captureDaemonFingerprint
	captureDaemonFingerprint = func(context.Context, int) (identity.Fingerprint, error) {
		return fp, nil
	}
	t.Cleanup(func() { captureDaemonFingerprint = prev })
}

// Decision matrix coverage for evaluateShortCircuit. Each row drives the
// pure decision function with an explicit cache + registry snapshot so
// none of the cases need a real daemon, real SQLite, or real flock.
func TestEvaluateShortCircuit_Matrix(t *testing.T) {
	const repoHash = "deadbeef00"
	const sessionID = "sess-A"
	const harness = "claude-code"
	const livePID = 4242
	const deadPID = 4243
	const stampedStartTS = "Mon May  5 12:00:00 2026"
	const stampedArgvHash = "argv-hash-original"

	now := time.Unix(1_700_000_000, 0)
	ttl := 30 * time.Minute
	freshTS := now.Add(-1 * time.Minute).Unix()
	staleTS := now.Add(-31 * time.Minute).Unix()

	pidAlive := func(pid int) bool { return pid == livePID }
	matchingFP := fpStub(stampedStartTS, stampedArgvHash)

	freshRegistry := &central.RepoRecord{
		Path:       "/tmp/x",
		RepoHash:   repoHash,
		LastSeenTS: freshTS,
	}

	// stampedCache returns a happy-path cache that should short-circuit
	// when paired with matchingFP. Tests that only need to flip one
	// field clone via a copy + tweak.
	stampedCache := func() *startCache {
		return &startCache{
			Version: startCacheVersion, RepoHash: repoHash,
			SessionID: sessionID, Harness: harness,
			DaemonPID: livePID, UpdatedAt: freshTS,
			DaemonStartTS:  stampedStartTS,
			DaemonArgvHash: stampedArgvHash,
		}
	}

	tests := []struct {
		name      string
		cache     *startCache
		registry  *central.RepoRecord
		fp        func(context.Context, int) (identity.Fingerprint, error)
		wantOK    bool
		wantBlame string // substring expected in Reason on escalation
	}{
		{
			name:     "same_session_and_fresh_short_circuits",
			cache:    stampedCache(),
			registry: freshRegistry,
			fp:       matchingFP,
			wantOK:   true,
		},
		{
			name:      "no_cache_escalates",
			cache:     nil,
			registry:  freshRegistry,
			fp:        matchingFP,
			wantOK:    false,
			wantBlame: "no_cache",
		},
		{
			name: "different_session_escalates",
			cache: func() *startCache {
				c := stampedCache()
				c.SessionID = "other-session"
				return c
			}(),
			registry:  freshRegistry,
			fp:        matchingFP,
			wantOK:    false,
			wantBlame: "session_mismatch",
		},
		{
			name: "stale_heartbeat_escalates",
			cache: func() *startCache {
				c := stampedCache()
				c.UpdatedAt = staleTS
				return c
			}(),
			registry:  freshRegistry,
			fp:        matchingFP,
			wantOK:    false,
			wantBlame: "stale_heartbeat",
		},
		{
			name:      "missing_registry_entry_escalates",
			cache:     stampedCache(),
			registry:  nil,
			fp:        matchingFP,
			wantOK:    false,
			wantBlame: "registry_missing_repo",
		},
		{
			name:  "disabled_registry_entry_escalates",
			cache: stampedCache(),
			registry: &central.RepoRecord{
				Path:           "/tmp/x",
				RepoHash:       repoHash,
				LastSeenTS:     freshTS,
				LifecycleState: central.RepoLifecycleDisabled,
			},
			fp:        matchingFP,
			wantOK:    false,
			wantBlame: repoAutodiscoverySkipRepoDisabled,
		},
		{
			name: "harness_mismatch_escalates",
			cache: func() *startCache {
				c := stampedCache()
				c.Harness = "codex"
				return c
			}(),
			registry:  freshRegistry,
			fp:        matchingFP,
			wantOK:    false,
			wantBlame: "harness_mismatch",
		},
		{
			name: "dead_daemon_pid_escalates",
			cache: func() *startCache {
				c := stampedCache()
				c.DaemonPID = deadPID
				return c
			}(),
			registry:  freshRegistry,
			fp:        matchingFP,
			wantOK:    false,
			wantBlame: "daemon_pid_dead",
		},
		{
			name: "repo_hash_mismatch_in_cache_escalates",
			cache: func() *startCache {
				c := stampedCache()
				c.RepoHash = "differenthash"
				return c
			}(),
			registry:  freshRegistry,
			fp:        matchingFP,
			wantOK:    false,
			wantBlame: "repo_hash_mismatch",
		},
		{
			name:      "registry_hash_mismatch_escalates",
			cache:     stampedCache(),
			registry:  &central.RepoRecord{Path: "/tmp/x", RepoHash: "wronghash", LastSeenTS: freshTS},
			fp:        matchingFP,
			wantOK:    false,
			wantBlame: "registry_hash_mismatch",
		},
		{
			name: "zero_updated_at_escalates",
			cache: func() *startCache {
				c := stampedCache()
				c.UpdatedAt = 0
				return c
			}(),
			registry:  freshRegistry,
			fp:        matchingFP,
			wantOK:    false,
			wantBlame: "stale_heartbeat",
		},
		{
			name: "no_daemon_pid_escalates",
			cache: func() *startCache {
				c := stampedCache()
				c.DaemonPID = 0
				return c
			}(),
			registry:  freshRegistry,
			fp:        matchingFP,
			wantOK:    false,
			wantBlame: "no_daemon_pid",
		},
		{
			name: "fingerprint_mismatch_escalates",
			// Cache stamped from one daemon; live ps returns a
			// different start time + argv (PID was recycled by an
			// unrelated process).
			cache:     stampedCache(),
			registry:  freshRegistry,
			fp:        fpStub("Tue May  6 09:30:00 2026", "argv-hash-recycled"),
			wantOK:    false,
			wantBlame: "fingerprint_mismatch",
		},
		{
			name: "fingerprint_missing_escalates",
			// Legacy / partial cache that lacks the fingerprint
			// stamp must escalate; schema v2 requires it.
			cache: func() *startCache {
				c := stampedCache()
				c.DaemonStartTS = ""
				c.DaemonArgvHash = ""
				return c
			}(),
			registry:  freshRegistry,
			fp:        matchingFP,
			wantOK:    false,
			wantBlame: "fingerprint_missing",
		},
		{
			name:     "fingerprint_capture_failure_escalates",
			cache:    stampedCache(),
			registry: freshRegistry,
			fp: func(context.Context, int) (identity.Fingerprint, error) {
				return identity.Fingerprint{}, errors.New("ps: no such process")
			},
			wantOK:    false,
			wantBlame: "fingerprint_capture_failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := evaluateShortCircuit(tt.cache, repoHash, sessionID, harness,
				tt.registry, now, ttl, pidAlive, tt.fp)
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

// TestEvaluateShortCircuit_FingerprintMatchAndMismatch is the explicit
// matched/mismatched pair the task requires (P1 #3 acceptance). It uses
// the stub fingerprint capturer to pin both sides without spawning a
// real ps invocation.
func TestEvaluateShortCircuit_FingerprintMatchAndMismatch(t *testing.T) {
	const repoHash = "abcd1234"
	const sessionID = "sess-fp"
	const harness = "claude-code"
	const livePID = 12345
	const startTS = "Wed May  7 08:15:00 2026"
	const argvHash = "stamped-argv"

	now := time.Unix(1_700_000_000, 0)
	freshTS := now.Add(-30 * time.Second).Unix()
	pidAlive := func(int) bool { return true }
	registry := &central.RepoRecord{Path: "/tmp/x", RepoHash: repoHash, LastSeenTS: freshTS}

	cache := &startCache{
		Version: startCacheVersion, RepoHash: repoHash,
		SessionID: sessionID, Harness: harness, DaemonPID: livePID, UpdatedAt: freshTS,
		DaemonStartTS: startTS, DaemonArgvHash: argvHash,
	}

	t.Run("matching_fingerprint_short_circuits", func(t *testing.T) {
		d := evaluateShortCircuit(cache, repoHash, sessionID, harness, registry,
			now, 30*time.Minute, pidAlive, fpStub(startTS, argvHash))
		if !d.OK {
			t.Fatalf("expected OK, got reason=%q", d.Reason)
		}
	})

	t.Run("mismatched_fingerprint_escalates", func(t *testing.T) {
		d := evaluateShortCircuit(cache, repoHash, sessionID, harness, registry,
			now, 30*time.Minute, pidAlive, fpStub("different start", "different argv"))
		if d.OK {
			t.Fatalf("expected escalation, got OK")
		}
		if !strings.Contains(d.Reason, "fingerprint_mismatch") {
			t.Fatalf("reason=%q want fingerprint_mismatch", d.Reason)
		}
	})
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
// being created on demand under <gitDir>/acd. Schema v2 also round-trips
// the daemon fingerprint stamp.
func TestWriteStartCache_RoundTripCreatesParent(t *testing.T) {
	gitDir := t.TempDir()
	in := startCache{
		Version: startCacheVersion, RepoHash: "abc",
		SessionID: "s", Harness: "claude-code",
		DaemonPID: 9999, WatchPID: 1234, UpdatedAt: 42,
		DaemonStartTS: "Mon May  5 12:00:00 2026", DaemonArgvHash: "argv-hash-roundtrip",
	}
	if err := writeStartCache(gitDir, in); err != nil {
		t.Fatalf("writeStartCache: %v", err)
	}
	got := readStartCache(startCachePath(gitDir, in.SessionID))
	if got == nil {
		t.Fatalf("expected cache hit, got nil")
	}
	if got.SessionID != in.SessionID || got.DaemonPID != in.DaemonPID ||
		got.RepoHash != in.RepoHash || got.WatchPID != in.WatchPID ||
		got.UpdatedAt != in.UpdatedAt ||
		got.DaemonStartTS != in.DaemonStartTS || got.DaemonArgvHash != in.DaemonArgvHash {
		t.Fatalf("round-trip mismatch:\n in=%+v\ngot=%+v", in, *got)
	}
}

// TestWriteStartCache_ConcurrentWritersProduceParseableFile pins that
// N concurrent writeStartCache calls (active-hook-style storm) leave the
// final start-cache file as parseable JSON, never a half-written tail
// from one writer interleaved with another's truncate. The fix is the
// per-call tmp filename via os.CreateTemp; without it the asserts below
// fail intermittently with json: invalid character / unexpected EOF.
func TestWriteStartCache_ConcurrentWritersProduceParseableFile(t *testing.T) {
	t.Parallel()
	gitDir := t.TempDir()
	const writers = 20
	var wg sync.WaitGroup
	wg.Add(writers)
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			err := writeStartCache(gitDir, startCache{
				Version: startCacheVersion, RepoHash: "abc",
				SessionID: "shared-session", Harness: "claude-code",
				DaemonPID: 10000 + i, UpdatedAt: int64(i + 1),
				DaemonStartTS: "Mon May  5 12:00:00 2026", DaemonArgvHash: "argv-x",
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("writeStartCache concurrent: %v", err)
	}
	got := readStartCache(startCachePath(gitDir, "shared-session"))
	if got == nil {
		t.Fatalf("final cache unreadable / unparseable JSON")
	}
	// The winning writer's UpdatedAt must be in [1, writers].
	if got.UpdatedAt < 1 || got.UpdatedAt > writers {
		t.Fatalf("winning UpdatedAt=%d outside [1,%d] — possibly bytes-interleaved", got.UpdatedAt, writers)
	}
	// No leaked .tmp files in the directory once writers are done.
	entries, err := os.ReadDir(filepath.Join(gitDir, "acd"))
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("tmp leak: %s", e.Name())
		}
	}
}

// TestReadStartCache_LegacyV1Rejected pins that v1 cache files (before the
// fingerprint schema bump) are treated as missing so the cold path is
// forced to repopulate the cache with v2 fields.
func TestReadStartCache_LegacyV1Rejected(t *testing.T) {
	dir := t.TempDir()
	v1Path := filepath.Join(dir, "v1.json")
	body, _ := json.Marshal(map[string]any{
		"version": 1, "repo_hash": "abc", "session_id": "s",
		"harness": "claude-code", "daemon_pid": 12345, "updated_at_unix": 42,
	})
	if err := os.WriteFile(v1Path, body, 0o600); err != nil {
		t.Fatalf("write v1: %v", err)
	}
	if got := readStartCache(v1Path); got != nil {
		t.Fatalf("v1 cache must be rejected, got %+v", got)
	}
}

// tryShortCircuitStart end-to-end: a fresh cache + a registered repo lets
// the call return OK without us needing to mock anything else (besides the
// fingerprint capturer, which is stubbed to a deterministic value so the
// cached and recaptured stamps agree).
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
	// Pin a deterministic daemon fingerprint so the cached stamp matches
	// the live re-capture inside tryShortCircuitStart.
	stamped := identity.Fingerprint{StartTime: "Mon May  5 12:00:00 2026", ArgvHash: "argv-hash"}
	installFakeDaemonFingerprint(t, stamped)
	// Stamp a fresh cache pointing at our own pid (always alive).
	if err := writeStartCache(gitDir, startCache{
		Version: startCacheVersion, RepoHash: repoHash,
		SessionID: "sess-A", Harness: "claude-code",
		DaemonPID: os.Getpid(), UpdatedAt: time.Now().Unix(),
		DaemonStartTS: stamped.StartTime, DaemonArgvHash: stamped.ArgvHash,
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

// TestTryShortCircuitStart_FingerprintMismatchEscalates exercises the live
// PID-reuse defense end-to-end: a cached fingerprint that disagrees with
// the (stubbed) live capture forces the cold path with reason
// fingerprint_mismatch.
func TestTryShortCircuitStart_FingerprintMismatchEscalates(t *testing.T) {
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
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.UpsertRepo(repoDir, repoHash, "/state/db", "claude-code", time.Now().Unix())
		return nil
	}); err != nil {
		t.Fatalf("registry WithLock: %v", err)
	}
	// Cached fingerprint from the original daemon.
	if err := writeStartCache(gitDir, startCache{
		Version: startCacheVersion, RepoHash: repoHash,
		SessionID: "sess-A", Harness: "claude-code",
		DaemonPID: os.Getpid(), UpdatedAt: time.Now().Unix(),
		DaemonStartTS: "Mon May  5 12:00:00 2026", DaemonArgvHash: "original-argv",
	}); err != nil {
		t.Fatalf("writeStartCache: %v", err)
	}
	// Live re-capture returns a different stamp (PID was recycled).
	installFakeDaemonFingerprint(t, identity.Fingerprint{
		StartTime: "Tue May  6 09:30:00 2026", ArgvHash: "recycled-argv",
	})
	ok, _, _, reason := tryShortCircuitStart(context.Background(), gitDir,
		repoHash, "sess-A", "claude-code", repoDir)
	if ok {
		t.Fatalf("expected escalation, got OK")
	}
	if !strings.Contains(reason, "fingerprint_mismatch") {
		t.Fatalf("reason=%q want fingerprint_mismatch", reason)
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
	installFakeDaemonFingerprint(t, identity.Fingerprint{
		StartTime: "Mon May  5 12:00:00 2026", ArgvHash: "test-argv",
	})

	// First call takes the cold path (writes the cache).
	var first bytes.Buffer
	if err := runStart(ctx, &first, repoDir, "sess-active-hook", "claude-code", 0, true); err != nil {
		t.Fatalf("first runStart: %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("first call spawn count=%d want 1", count.Load())
	}

	// Tripwire: stub touchClientHotPath so we can prove the hot path
	// (a) actually fires, and (b) does not re-spawn the daemon. The
	// short-circuit MUST take this path; if it falls through to the
	// cold path the spawn-count assertion below catches it.
	var hotTouches atomic.Int32
	prevTouch := touchClientHotPath
	touchClientHotPath = func(ctx context.Context, gitDir, sessionID string) error {
		hotTouches.Add(1)
		return prevTouch(ctx, gitDir, sessionID)
	}
	t.Cleanup(func() { touchClientHotPath = prevTouch })

	// Second call must short-circuit and bump last_seen_ts via the
	// hot-path touch helper (no daemon respawn).
	var second bytes.Buffer
	if err := runStart(ctx, &second, repoDir, "sess-active-hook", "claude-code", 0, true); err != nil {
		t.Fatalf("second runStart: %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("second call spawn count=%d want 1 (no respawn)", count.Load())
	}
	if hotTouches.Load() != 1 {
		t.Fatalf("hot-path touchClient invocations=%d want 1", hotTouches.Load())
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

func TestRunStart_DisabledRepoRejectsStartCacheHotPath(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)

	count, restore := installFakeSpawn(t, os.Getpid())
	defer restore()
	installFakeDaemonFingerprint(t, identity.Fingerprint{
		StartTime: "Mon May  5 12:00:00 2026", ArgvHash: "test-argv",
	})

	var first bytes.Buffer
	if err := runStart(ctx, &first, repoDir, "sess-disabled-hot", "claude-code", 0, true); err != nil {
		t.Fatalf("first runStart: %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("first call spawn count=%d want 1", count.Load())
	}

	if err := central.WithLock(roots, func(reg *central.Registry) error {
		res := reg.DisableRepo(central.RepoRemovalTarget{Path: repoDir}, time.Now().Unix())
		if res.NotFound {
			return errors.New("registered repo not found")
		}
		return nil
	}); err != nil {
		t.Fatalf("disable repo: %v", err)
	}

	var hotTouches atomic.Int32
	prevTouch := touchClientHotPath
	touchClientHotPath = func(ctx context.Context, gitDir, sessionID string) error {
		hotTouches.Add(1)
		return prevTouch(ctx, gitDir, sessionID)
	}
	t.Cleanup(func() { touchClientHotPath = prevTouch })

	var second bytes.Buffer
	if err := runStart(ctx, &second, repoDir, "sess-disabled-hot", "claude-code", 0, true); err != nil {
		t.Fatalf("second runStart: %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("disabled hot-path call spawned daemon: count=%d want 1", count.Load())
	}
	if hotTouches.Load() != 0 {
		t.Fatalf("disabled repo touched hot-path client cache %d times", hotTouches.Load())
	}
	var got startResult
	if err := json.Unmarshal(second.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal second: %v\n%s", err, second.String())
	}
	if !got.Skipped || got.SkipReason != repoAutodiscoverySkipRepoDisabled {
		t.Fatalf("second result=%+v, want repo_disabled skip", got)
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
	installFakeDaemonFingerprint(t, identity.Fingerprint{
		StartTime: "Mon May  5 12:00:00 2026", ArgvHash: "test-argv",
	})

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

// TestMultiSession_PerSessionCacheKeepsBothOnHotPath exercises the per-
// session cache filename switch (P1 #4 acceptance). Two sessions register
// against the same repo; after the cold-path setup, each session's
// subsequent runStart must short-circuit without evicting the other.
func TestMultiSession_PerSessionCacheKeepsBothOnHotPath(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)

	count, restore := installFakeSpawn(t, os.Getpid())
	defer restore()
	installFakeDaemonFingerprint(t, identity.Fingerprint{
		StartTime: "Mon May  5 12:00:00 2026", ArgvHash: "test-argv",
	})

	// Cold path for two distinct sessions.
	for _, s := range []struct {
		session, harness string
	}{
		{"sess-A", "claude-code"},
		{"sess-B", "codex"},
	} {
		var buf bytes.Buffer
		if err := runStart(ctx, &buf, repoDir, s.session, s.harness, 0, true); err != nil {
			t.Fatalf("cold runStart %s: %v", s.session, err)
		}
	}
	if count.Load() != 1 {
		t.Fatalf("spawn count after two cold sessions=%d want 1", count.Load())
	}

	// Both per-session cache files must exist.
	gitDir := filepath.Join(repoDir, ".git")
	pathA := startCachePath(gitDir, "sess-A")
	pathB := startCachePath(gitDir, "sess-B")
	if pathA == pathB {
		t.Fatalf("per-session paths collided: %s", pathA)
	}
	if _, err := os.Stat(pathA); err != nil {
		t.Fatalf("session-A cache missing: %v", err)
	}
	if _, err := os.Stat(pathB); err != nil {
		t.Fatalf("session-B cache missing: %v", err)
	}

	// Tripwire: stub touchClientHotPath so we can record each hot-path
	// touch by session_id. A fall-through to the cold path would
	// re-spawn the daemon (caught by the spawn-count assertion below)
	// and would NOT record a hot-path touch for that call.
	var touchedSessions sync.Map
	prevTouch := touchClientHotPath
	touchClientHotPath = func(ctx context.Context, gitDir, sessionID string) error {
		v, _ := touchedSessions.LoadOrStore(sessionID, new(atomic.Int32))
		v.(*atomic.Int32).Add(1)
		return prevTouch(ctx, gitDir, sessionID)
	}
	t.Cleanup(func() { touchClientHotPath = prevTouch })

	// Interleave hot calls. Both sessions must short-circuit and each
	// must record one hot-path touch per call.
	for _, sess := range []string{"sess-A", "sess-B", "sess-A", "sess-B"} {
		harness := "claude-code"
		if sess == "sess-B" {
			harness = "codex"
		}
		var buf bytes.Buffer
		if err := runStart(ctx, &buf, repoDir, sess, harness, 0, true); err != nil {
			t.Fatalf("hot runStart %s: %v", sess, err)
		}
		var got startResult
		if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
			t.Fatalf("unmarshal %s: %v\n%s", sess, err, buf.String())
		}
		if !got.Duplicate {
			t.Fatalf("session %s did not short-circuit: %+v", sess, got)
		}
	}
	if count.Load() != 1 {
		t.Fatalf("spawn count after interleaved hot calls=%d want 1", count.Load())
	}
	for _, sess := range []string{"sess-A", "sess-B"} {
		v, ok := touchedSessions.Load(sess)
		if !ok {
			t.Fatalf("hot-path touch never recorded for %s", sess)
		}
		if got := v.(*atomic.Int32).Load(); got != 2 {
			t.Fatalf("hot-path touch count for %s=%d want 2", sess, got)
		}
	}

	// Stop session A only; session B's cache must survive. Use the
	// stop path with no kill (peer remains -> Deferred). We need the
	// daemon_state row alive so the stop logic enters the refcount
	// branch. Recreate state.db by re-running a cold call against a
	// throwaway session, then register A and B again so we can stop
	// A explicitly.
	//
	// Simpler: directly invoke os.Remove(startCachePath(gitDir, "sess-A"))
	// would beg the question. Instead drive runStop with a peer alive.
	dbA, err := state.Open(ctx, state.DBPathFromGitDir(gitDir))
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	for _, sid := range []string{"sess-A", "sess-B"} {
		if err := state.RegisterClient(ctx, dbA, state.Client{
			SessionID: sid, Harness: "claude-code",
		}); err != nil {
			t.Fatalf("register %s: %v", sid, err)
		}
	}
	if err := state.SaveDaemonState(ctx, dbA, state.DaemonState{
		PID: 99999, Mode: "running", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	_ = dbA.Close()

	signalCount, restoreSig := installStopSignal(t, repoDir)
	defer restoreSig()

	var stopOut bytes.Buffer
	if err := runStop(ctx, &stopOut, repoDir, "sess-A", false, false, true); err != nil {
		t.Fatalf("runStop sess-A: %v", err)
	}
	if signalCount.Load() != 0 {
		t.Fatalf("stop sess-A signaled daemon despite peer alive: count=%d", signalCount.Load())
	}
	if _, err := os.Stat(pathA); !os.IsNotExist(err) {
		t.Fatalf("session-A cache survived stop: %v", err)
	}
	if _, err := os.Stat(pathB); err != nil {
		t.Fatalf("session-B cache must still exist after stopping A: %v", err)
	}
}

func TestRunStart_LegacySubdirRegistryRowDoesNotRewriteConsent(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)
	subdir := filepath.Join(repoDir, "nested", "pkg")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}
	repoHash, err := paths.RepoHash(repoDir)
	if err != nil {
		t.Fatalf("RepoHash: %v", err)
	}
	gitDir := filepath.Join(repoDir, ".git")
	dbPath := state.DBPathFromGitDir(gitDir)
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.Repos = []central.RepoRecord{{
			Path:              subdir,
			RepoHash:          repoHash,
			StateDB:           dbPath,
			FirstRegisteredTS: 1,
			LastSeenTS:        1,
			Harnesses:         []string{"claude-code"},
		}}
		return nil
	}); err != nil {
		t.Fatalf("write legacy registry row: %v", err)
	}
	registryBefore, err := os.ReadFile(roots.RegistryPath())
	if err != nil {
		t.Fatalf("read legacy registry: %v", err)
	}

	stamped := identity.Fingerprint{StartTime: "Mon May  5 12:00:00 2026", ArgvHash: "legacy-cache-argv"}
	installFakeDaemonFingerprint(t, stamped)
	if err := writeStartCache(gitDir, startCache{
		Version:        startCacheVersion,
		RepoHash:       repoHash,
		SessionID:      "sess-legacy-subdir",
		Harness:        "claude-code",
		DaemonPID:      os.Getpid(),
		ClientCount:    1,
		UpdatedAt:      time.Now().Unix(),
		DaemonStartTS:  stamped.StartTime,
		DaemonArgvHash: stamped.ArgvHash,
	}); err != nil {
		t.Fatalf("write legacy v2 start-cache: %v", err)
	}

	count, restore := installFakeSpawn(t, os.Getpid())
	defer restore()
	var hotTouches atomic.Int32
	prevTouch := touchClientHotPath
	touchClientHotPath = func(ctx context.Context, gitDir, sessionID string) error {
		hotTouches.Add(1)
		return prevTouch(ctx, gitDir, sessionID)
	}
	t.Cleanup(func() { touchClientHotPath = prevTouch })

	var stdout bytes.Buffer
	if err := runStart(ctx, &stdout, subdir, "sess-legacy-subdir", "claude-code", 0, true); err != nil {
		t.Fatalf("runStart from legacy subdir: %v", err)
	}
	if hotTouches.Load() != 0 {
		t.Fatalf("legacy registry hook touched state %d times", hotTouches.Load())
	}
	if count.Load() != 0 {
		t.Fatalf("legacy registered row spawned a replacement daemon: count=%d", count.Load())
	}
	var got startResult
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal start result: %v\n%s", err, stdout.String())
	}
	if !got.Skipped || got.SkipReason != repoAutodiscoverySkipRegistry || !central.SameRepoPath(got.Repo, repoDir) {
		t.Fatalf("result=%+v want registry-error skip at canonical root", got)
	}
	registryAfter, err := os.ReadFile(roots.RegistryPath())
	if err != nil {
		t.Fatalf("read registry after hook: %v", err)
	}
	if !bytes.Equal(registryAfter, registryBefore) {
		t.Fatalf("hook rewrote legacy registry\nbefore=%s\nafter=%s", registryBefore, registryAfter)
	}
	if sc := readStartCache(startCachePath(gitDir, "sess-legacy-subdir")); sc == nil || sc.Version != startCacheVersion {
		t.Fatalf("v2 start-cache was not present after fallback repair: %+v", sc)
	}
}

func TestRunStart_CanonicalRegistryRowUsesEarlyShortCircuitFromSubdir(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir := makeStartRepo(t)
	subdir := filepath.Join(repoDir, "nested", "pkg")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}
	count, restore := installFakeSpawn(t, os.Getpid())
	defer restore()
	installFakeDaemonFingerprint(t, identity.Fingerprint{
		StartTime: "Mon May  5 12:00:00 2026", ArgvHash: "canonical-cache-argv",
	})

	var first bytes.Buffer
	if err := runStart(ctx, &first, repoDir, "sess-canonical", "claude-code", 0, true); err != nil {
		t.Fatalf("cold runStart: %v", err)
	}

	var hotTouches atomic.Int32
	prevTouch := touchClientHotPath
	touchClientHotPath = func(ctx context.Context, gitDir, sessionID string) error {
		hotTouches.Add(1)
		return prevTouch(ctx, gitDir, sessionID)
	}
	t.Cleanup(func() { touchClientHotPath = prevTouch })

	var second bytes.Buffer
	if err := runStart(ctx, &second, subdir, "sess-canonical", "claude-code", 0, true); err != nil {
		t.Fatalf("hot runStart from subdir: %v", err)
	}
	if count.Load() != 1 {
		t.Fatalf("spawn count=%d want 1", count.Load())
	}
	if hotTouches.Load() != 1 {
		t.Fatalf("early short-circuit hot touches=%d want 1", hotTouches.Load())
	}
	var got startResult
	if err := json.Unmarshal(second.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal hot result: %v\n%s", err, second.String())
	}
	if !got.Duplicate || !central.SameRepoPath(got.Repo, repoDir) {
		t.Fatalf("hot result should be duplicate at canonical root: %+v", got)
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
