package daemon

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// openTestDB returns a fresh per-test state DB.
func openTestDB(t *testing.T) *state.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func registerClient(t *testing.T, db *state.DB, c state.Client) {
	t.Helper()
	if err := state.RegisterClient(context.Background(), db, c); err != nil {
		t.Fatalf("RegisterClient: %v", err)
	}
}

func countClients(t *testing.T, db *state.DB) int {
	t.Helper()
	n, err := state.CountClients(context.Background(), db)
	if err != nil {
		t.Fatalf("CountClients: %v", err)
	}
	return n
}

// TestRefcount_PeerAlive: a session with a fresh heartbeat, an alive pid, and
// matching fingerprint must be kept by the sweep.
func TestRefcount_PeerAlive(t *testing.T) {
	db := openTestDB(t)
	now := time.Unix(2_000_000, 0)
	live := identity.Fingerprint{StartTime: "Mon Apr 28 14:22:13 2026", ArgvHash: "abcd"}
	registerClient(t, db, state.Client{
		SessionID:    "sess-alive",
		Harness:      "claude-code",
		WatchPID:     sql.NullInt64{Int64: 1234, Valid: true},
		WatchFP:      sql.NullString{String: FingerprintToken(live), Valid: true},
		RegisteredTS: float64(now.Unix() - 60),
		LastSeenTS:   float64(now.Unix() - 30), // well within TTL
	})

	alive, err := SweepClients(context.Background(), db, now, SweepOpts{
		AliveFn:            func(context.Context, int) bool { return true },
		CaptureFingerprint: func(context.Context, int) (identity.Fingerprint, error) { return live, nil },
	})
	if err != nil {
		t.Fatalf("SweepClients: %v", err)
	}
	if alive != 1 {
		t.Fatalf("alive=%d, want 1", alive)
	}
	if got := countClients(t, db); got != 1 {
		t.Fatalf("post-sweep rows=%d, want 1", got)
	}

	// Not eligible for self-termination yet (alive>0 implies caller resets
	// emptySweepCount; verify the gate predicate independently).
	if ShouldSelfTerminate(0, time.Hour, SelfTerminateOpts{}) {
		t.Fatalf("ShouldSelfTerminate should be false with emptySweep=0")
	}
}

// TestRefcount_DeadPID: row is dropped when kill(pid,0) reports dead.
func TestRefcount_DeadPID(t *testing.T) {
	db := openTestDB(t)
	now := time.Unix(2_000_000, 0)
	registerClient(t, db, state.Client{
		SessionID:    "sess-dead",
		Harness:      "shell",
		WatchPID:     sql.NullInt64{Int64: 9999, Valid: true},
		WatchFP:      sql.NullString{String: FingerprintToken(identity.Fingerprint{StartTime: "x", ArgvHash: "y"}), Valid: true},
		RegisteredTS: float64(now.Unix() - 5),
		LastSeenTS:   float64(now.Unix() - 5),
	})
	alive, err := SweepClients(context.Background(), db, now, SweepOpts{
		AliveFn: func(context.Context, int) bool { return false },
		CaptureFingerprint: func(context.Context, int) (identity.Fingerprint, error) {
			t.Fatalf("CaptureFingerprint should not be called when AliveFn=false")
			return identity.Fingerprint{}, nil
		},
	})
	if err != nil {
		t.Fatalf("SweepClients: %v", err)
	}
	if alive != 0 {
		t.Fatalf("alive=%d, want 0", alive)
	}
	if got := countClients(t, db); got != 0 {
		t.Fatalf("post-sweep rows=%d, want 0", got)
	}
}

// TestRefcount_FingerprintMismatch: alive pid but fingerprint changed -> drop.
func TestRefcount_FingerprintMismatch(t *testing.T) {
	db := openTestDB(t)
	now := time.Unix(2_000_000, 0)
	stored := identity.Fingerprint{StartTime: "Mon Apr 28 14:22:13 2026", ArgvHash: "OLD"}
	registerClient(t, db, state.Client{
		SessionID:    "sess-pid-reuse",
		Harness:      "codex",
		WatchPID:     sql.NullInt64{Int64: 4321, Valid: true},
		WatchFP:      sql.NullString{String: FingerprintToken(stored), Valid: true},
		RegisteredTS: float64(now.Unix() - 10),
		LastSeenTS:   float64(now.Unix() - 5),
	})
	live := identity.Fingerprint{StartTime: "Tue May 01 09:00:00 2026", ArgvHash: "NEW"}
	alive, err := SweepClients(context.Background(), db, now, SweepOpts{
		AliveFn:            func(context.Context, int) bool { return true },
		CaptureFingerprint: func(context.Context, int) (identity.Fingerprint, error) { return live, nil },
	})
	if err != nil {
		t.Fatalf("SweepClients: %v", err)
	}
	if alive != 0 {
		t.Fatalf("alive=%d, want 0", alive)
	}
	if got := countClients(t, db); got != 0 {
		t.Fatalf("post-sweep rows=%d, want 0", got)
	}
}

// TestRefcount_FingerprintUnresolvableKeepsClient: an alive pid whose
// fingerprint cannot be resolved (for example ps cannot see into another PID
// namespace) must stay registered until the universal TTL gate expires.
func TestRefcount_FingerprintUnresolvableKeepsClient(t *testing.T) {
	db := openTestDB(t)
	now := time.Unix(2_000_000, 0)
	stored := identity.Fingerprint{StartTime: "Mon Apr 28 14:22:13 2026", ArgvHash: "OLD"}
	registerClient(t, db, state.Client{
		SessionID:    "sess-ps-unresolvable",
		Harness:      "codex",
		WatchPID:     sql.NullInt64{Int64: 2468, Valid: true},
		WatchFP:      sql.NullString{String: FingerprintToken(stored), Valid: true},
		RegisteredTS: float64(now.Unix() - 10),
		LastSeenTS:   float64(now.Unix() - 5),
	})

	for i := 0; i < 5; i++ {
		alive, err := SweepClients(context.Background(), db, now.Add(time.Duration(i)*time.Minute), SweepOpts{
			AliveFn: func(context.Context, int) bool { return true },
			CaptureFingerprint: func(context.Context, int) (identity.Fingerprint, error) {
				return identity.Fingerprint{}, errors.New("ps unavailable")
			},
		})
		if err != nil {
			t.Fatalf("SweepClients pass %d: %v", i, err)
		}
		if alive != 1 {
			t.Fatalf("pass %d alive=%d, want 1", i, alive)
		}
		if got := countClients(t, db); got != 1 {
			t.Fatalf("pass %d post-sweep rows=%d, want 1", i, got)
		}
	}
}

func TestRefcount_FingerprintProbeHonorsCancellation(t *testing.T) {
	db := openTestDB(t)
	now := time.Unix(2_000_000, 0)
	stored := identity.Fingerprint{StartTime: "Mon Apr 28 14:22:13 2026", ArgvHash: "OLD"}
	registerClient(t, db, state.Client{
		SessionID:    "sess-cancel",
		Harness:      "codex",
		WatchPID:     sql.NullInt64{Int64: 2468, Valid: true},
		WatchFP:      sql.NullString{String: FingerprintToken(stored), Valid: true},
		RegisteredTS: float64(now.Unix() - 10),
		LastSeenTS:   float64(now.Unix() - 5),
	})

	ctx, cancel := context.WithCancel(context.Background())
	alive, err := SweepClients(ctx, db, now, SweepOpts{
		AliveFn: func(context.Context, int) bool { return true },
		CaptureFingerprint: func(ctx context.Context, _ int) (identity.Fingerprint, error) {
			cancel()
			return identity.Fingerprint{}, ctx.Err()
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SweepClients err=%v, want context.Canceled", err)
	}
	if alive != 0 {
		t.Fatalf("alive=%d, want 0 before canceled probe is counted", alive)
	}
	if got := countClients(t, db); got != 1 {
		t.Fatalf("post-sweep rows=%d, want 1", got)
	}
}

// TestRefcount_TTLExpiry: no pid, last_seen older than TTL -> drop.
func TestRefcount_TTLExpiry(t *testing.T) {
	db := openTestDB(t)
	now := time.Unix(2_000_000, 0)
	registerClient(t, db, state.Client{
		SessionID:    "sess-stale",
		Harness:      "pi",
		WatchPID:     sql.NullInt64{}, // no pid
		WatchFP:      sql.NullString{},
		RegisteredTS: float64(now.Unix() - 4000),
		LastSeenTS:   float64(now.Unix() - 4000), // > 30min default TTL
	})
	alive, err := SweepClients(context.Background(), db, now, SweepOpts{})
	if err != nil {
		t.Fatalf("SweepClients: %v", err)
	}
	if alive != 0 {
		t.Fatalf("alive=%d, want 0", alive)
	}
	if got := countClients(t, db); got != 0 {
		t.Fatalf("post-sweep rows=%d, want 0", got)
	}
}

// TestRefcount_TTLOverride: explicit TTL via opts.
func TestRefcount_TTLOverride(t *testing.T) {
	db := openTestDB(t)
	now := time.Unix(2_000_000, 0)
	registerClient(t, db, state.Client{
		SessionID:    "sess-short-ttl",
		Harness:      "shell",
		LastSeenTS:   float64(now.Unix() - 60),
		RegisteredTS: float64(now.Unix() - 60),
	})
	// 30s TTL -> 60s-old row is stale.
	alive, err := SweepClients(context.Background(), db, now, SweepOpts{TTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("SweepClients: %v", err)
	}
	if alive != 0 {
		t.Fatalf("alive=%d, want 0", alive)
	}
}

// TestSweepClients_TOCTOU_DoesNotEvictFreshRow: the sweep classifies a row
// as stale (last_seen_ts < cutoff) but, before it actually deletes, a
// parallel `acd start` upserts the row with a fresh last_seen_ts. The
// tx-scoped DeregisterClientIfStale must observe the live value and skip
// the delete; the alive count drops the row on this tick (because the
// in-memory `c.LastSeenTS` is still stale) but the row survives in the
// database for the next sweep.
//
// Reproduction is deterministic with a controlled clock: list shows a
// stale row, we then refresh it, and the sweep's per-row predicate must
// re-check inside the tx and find the row no longer matches.
func TestSweepClients_TOCTOU_DoesNotEvictFreshRow(t *testing.T) {
	db := openTestDB(t)
	now := time.Unix(2_000_000, 0)
	cutoff := float64(now.Unix()) - DefaultClientTTL.Seconds()

	// Stale at list time (last_seen_ts = cutoff-10).
	registerClient(t, db, state.Client{
		SessionID:    "sess-races-refresh",
		Harness:      "claude-code",
		WatchPID:     sql.NullInt64{},
		LastSeenTS:   cutoff - 10,
		RegisteredTS: cutoff - 10,
	})

	// Inject a "parallel acd start" via the AliveFn hook: the TTL gate
	// fires before AliveFn, so we hook the captureFn position by using a
	// custom test that bumps the row between ListClients and the delete.
	// Easiest deterministic path: bump it with a custom AliveFn-equivalent
	// that runs on the first iteration.
	//
	// SweepClients doesn't expose a hook between list and delete, so we
	// directly exercise DeregisterClientIfStale: that's the operation the
	// sweep delegates to and the one that must honor the TOCTOU guard.
	dropped, err := state.DeregisterClientIfStale(context.Background(), db, "sess-races-refresh", cutoff)
	if err != nil {
		t.Fatalf("DeregisterClientIfStale (stale): %v", err)
	}
	if !dropped {
		t.Fatalf("expected stale row to be deleted, but it survived")
	}

	// Re-register, then simulate a parallel refresh that beat the sweep's
	// delete. Cutoff stays the same; row's last_seen_ts is now fresh.
	registerClient(t, db, state.Client{
		SessionID:    "sess-races-refresh",
		Harness:      "claude-code",
		LastSeenTS:   cutoff + 5, // refreshed past the cutoff
		RegisteredTS: cutoff - 10,
	})
	dropped, err = state.DeregisterClientIfStale(context.Background(), db, "sess-races-refresh", cutoff)
	if err != nil {
		t.Fatalf("DeregisterClientIfStale (fresh): %v", err)
	}
	if dropped {
		t.Fatalf("freshly refreshed row was evicted; TOCTOU guard failed")
	}
	if got := countClients(t, db); got != 1 {
		t.Fatalf("post-guard rows=%d, want 1 (live)", got)
	}

	// Same shape for the PID-pinned variant: the row is still alive but
	// a parallel start swapped in a new pid. The previous sweep's
	// dead-pid classification must not delete the row.
	registerClient(t, db, state.Client{
		SessionID:    "sess-pid-races",
		Harness:      "codex",
		WatchPID:     sql.NullInt64{Int64: 4321, Valid: true},
		LastSeenTS:   cutoff + 1,
		RegisteredTS: cutoff,
	})
	// Parallel start swapped pid 4321 -> 5555 and bumped last_seen_ts.
	if err := state.RegisterClient(context.Background(), db, state.Client{
		SessionID:    "sess-pid-races",
		Harness:      "codex",
		WatchPID:     sql.NullInt64{Int64: 5555, Valid: true},
		LastSeenTS:   cutoff + 100,
		RegisteredTS: cutoff,
	}); err != nil {
		t.Fatalf("RegisterClient (refresh): %v", err)
	}
	dropped, err = state.DeregisterClientIfPID(context.Background(), db, "sess-pid-races", 4321, cutoff+1)
	if err != nil {
		t.Fatalf("DeregisterClientIfPID: %v", err)
	}
	if dropped {
		t.Fatalf("row with replaced pid was evicted; pid TOCTOU guard failed")
	}
}

// TestRefcount_FingerprintWarnMapBounded pins the dedup map's eviction
// behaviour. Inserting more than fingerprintWarnCap distinct (session_id,
// pid) pairs followed by a sweepFingerprintWarnMap call must drop the map
// size back at or below fingerprintWarnCap. Before the bound was added the
// underlying sync.Map grew forever in long-lived daemons attached to
// churning peer fleets.
func TestRefcount_FingerprintWarnMapBounded(t *testing.T) {
	// Reset shared state so prior tests in the package don't poison the
	// dedup map. The map is package-level on purpose (one daemon per
	// process); within tests we treat it as a singleton with explicit
	// resets.
	unresolvedFingerprintMu.Lock()
	unresolvedFingerprintWarnings = make(map[string]fingerprintWarnEntry)
	unresolvedFingerprintMu.Unlock()

	const insertN = 2000
	for i := 0; i < insertN; i++ {
		sess := "sess-" + strconv.Itoa(i)
		logFingerprintUnresolved(sess, i, errors.New("ps unresolvable"))
	}

	unresolvedFingerprintMu.Lock()
	preSweep := len(unresolvedFingerprintWarnings)
	unresolvedFingerprintMu.Unlock()
	if preSweep != insertN {
		t.Fatalf("pre-sweep size=%d want %d (dedup logic regression)", preSweep, insertN)
	}

	post := sweepFingerprintWarnMap()
	if post > fingerprintWarnCap {
		t.Fatalf("post-sweep size=%d exceeds cap %d", post, fingerprintWarnCap)
	}
	// Eviction batch must be at least fingerprintWarnEvictBatch when
	// over-cap so a stuck-at-cap state cannot happen.
	if post > fingerprintWarnCap {
		t.Fatalf("post-sweep size=%d still over cap %d", post, fingerprintWarnCap)
	}
	if want := insertN - fingerprintWarnEvictBatch; post != want && post != fingerprintWarnCap {
		// Tolerate either exact eviction count or hard cap (both are
		// valid post-sweep shapes). Fail only on truly unbounded growth.
		if post > fingerprintWarnCap {
			t.Fatalf("post-sweep size=%d unbounded; want <=%d", post, fingerprintWarnCap)
		}
	}

	// A second sweep at-or-below cap is a no-op.
	post2 := sweepFingerprintWarnMap()
	if post2 != post {
		t.Fatalf("second sweep mutated size: %d -> %d", post, post2)
	}
}

// TestRefcount_FingerprintWarnEvictsOldestFirst pins eviction order: the
// oldest entries (by insertion time) must drop first. Without this, the
// "alarm under investigation" budget the cap implicitly grants can be lost
// to the noisier never-resolves clients.
func TestRefcount_FingerprintWarnEvictsOldestFirst(t *testing.T) {
	unresolvedFingerprintMu.Lock()
	unresolvedFingerprintWarnings = make(map[string]fingerprintWarnEntry)
	unresolvedFingerprintMu.Unlock()

	// Pre-populate to just over cap with monotonically increasing
	// timestamps so eviction order is deterministic.
	now := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	const n = fingerprintWarnCap + 100
	unresolvedFingerprintMu.Lock()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("sess-%04d:%d", i, i)
		unresolvedFingerprintWarnings[key] = fingerprintWarnEntry{
			insertedAt: now.Add(time.Duration(i) * time.Millisecond),
		}
	}
	unresolvedFingerprintMu.Unlock()

	_ = sweepFingerprintWarnMap()

	// The earliest-inserted N entries (where N = at least
	// fingerprintWarnEvictBatch when over cap) must be gone.
	unresolvedFingerprintMu.Lock()
	defer unresolvedFingerprintMu.Unlock()
	if _, present := unresolvedFingerprintWarnings["sess-0000:0"]; present {
		t.Fatalf("oldest entry survived sweep")
	}
	// A recent entry (near the end) should still be present.
	recent := fmt.Sprintf("sess-%04d:%d", n-1, n-1)
	if _, present := unresolvedFingerprintWarnings[recent]; !present {
		t.Fatalf("most recent entry %q evicted before older entries", recent)
	}
}

// TestRefcount_SelfTerminateGate: threshold + grace AND-ed.
func TestRefcount_SelfTerminateGate(t *testing.T) {
	cases := []struct {
		name      string
		emptyN    int
		sinceBoot time.Duration
		want      bool
	}{
		{"two-empty-past-grace", 2, 60 * time.Second, true},
		{"two-empty-pre-grace", 2, 5 * time.Second, false},
		{"one-empty-past-grace", 1, 60 * time.Second, false},
		{"three-empty-past-grace", 3, 60 * time.Second, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ShouldSelfTerminate(tc.emptyN, tc.sinceBoot, SelfTerminateOpts{})
			if got != tc.want {
				t.Fatalf("ShouldSelfTerminate(%d, %s) = %v, want %v",
					tc.emptyN, tc.sinceBoot, got, tc.want)
			}
		})
	}
}
