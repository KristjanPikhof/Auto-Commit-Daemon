// refcount.go implements the daemon_clients GC sweep + self-terminate gate
// per §3.4 + §8.4.
//
// A row is dead when ANY of these holds:
//  1. last_seen_ts + TTL < now              (TTL refresh expired)
//  2. watch_pid != 0 AND !identity.Alive    (peer died)
//  3. watch_pid != 0 AND fingerprint != stored (PID reuse defense)
//
// The daemon self-terminates after EmptySweepThreshold consecutive sweeps
// returning alive==0, but only past BootGrace from boot — a short-lived
// `acd start` that races the daemon's first sweep must not get evicted.
package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// DefaultClientTTL is the heartbeat staleness ceiling (D21).
const DefaultClientTTL = 30 * time.Minute

// DefaultBootGrace is the window after daemon boot during which empty sweeps
// do NOT count toward self-termination. Allows an `acd start` that races the
// daemon's first sweep to register without getting evicted on the spot.
const DefaultBootGrace = 30 * time.Second

// DefaultEmptySweepThreshold is the count of consecutive empty sweeps past
// BootGrace required before the run-loop self-terminates (§8.4).
const DefaultEmptySweepThreshold = 2

// SweepOpts configures one GC pass.
type SweepOpts struct {
	// TTL overrides DefaultClientTTL. Zero falls back to default.
	TTL time.Duration
	// CaptureFingerprint resolves a live fingerprint for a pid. Defaulted to
	// identity.Capture; tests inject a deterministic stub so they don't need
	// a real `ps`. A nil function defaults to identity.Capture.
	CaptureFingerprint func(context.Context, int) (identity.Fingerprint, error)
	// AliveFn checks pid liveness. Defaulted to identity.Alive; tests inject.
	AliveFn func(context.Context, int) bool
}

// SelfTerminateOpts configures ShouldSelfTerminate.
type SelfTerminateOpts struct {
	BootGrace           time.Duration
	EmptySweepThreshold int
}

// SweepClients runs one refcount-GC pass over daemon_clients (§8.4).
//
// Returns the number of rows that survived (alive). Drops every row that
// fails the §3.4 liveness predicate. now is passed in (rather than read from
// time.Now) so tests can advance a deterministic clock; production callers
// pass time.Now().
//
// The fingerprint comparison treats an absent stored fingerprint as "no
// fingerprint check" — peers that registered without one fall back to the
// pid liveness probe alone, matching legacy behaviour.
func SweepClients(ctx context.Context, db *state.DB, now time.Time, opts SweepOpts) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("daemon: SweepClients: nil db")
	}
	ttl := opts.TTL
	if ttl <= 0 {
		ttl = DefaultClientTTL
	}
	aliveFn := opts.AliveFn
	if aliveFn == nil {
		aliveFn = identity.AliveContext
	}
	captureFn := opts.CaptureFingerprint
	if captureFn == nil {
		captureFn = identity.CaptureContext
	}

	clients, err := state.ListClients(ctx, db)
	if err != nil {
		return 0, fmt.Errorf("daemon: list clients: %w", err)
	}

	nowSec := float64(now.UnixNano()) / 1e9
	cutoff := nowSec - ttl.Seconds()
	alive := 0

	for _, c := range clients {
		if err := ctx.Err(); err != nil {
			return alive, err
		}
		// Each per-row eviction goes through a single-statement
		// transaction whose WHERE clause re-asserts the staleness predicate
		// against the live row. This closes the TOCTOU window between
		// ListClients and the DELETE: a parallel `acd start` that upserts
		// last_seen_ts (or replaces watch_pid) wins automatically because
		// the predicate inside the tx no longer matches.

		// (1) TTL gate — universal fallback. Re-check last_seen_ts inside
		// the delete tx so a freshly refreshed registration is preserved.
		if c.LastSeenTS < cutoff {
			if dropped, derr := state.DeregisterClientIfStale(ctx, db, c.SessionID, cutoff); derr != nil {
				return alive, fmt.Errorf("daemon: drop ttl client %q: %w", c.SessionID, derr)
			} else if dropped {
				continue
			}
			// Predicate no longer matches — peer refreshed under us. Fall
			// through into the liveness path with stale `c.LastSeenTS`;
			// the row will survive the tick (and be re-evaluated next sweep).
		}

		// (2) PID liveness — fast-path eviction.
		if c.WatchPID.Valid && c.WatchPID.Int64 > 0 {
			pid := int(c.WatchPID.Int64)
			pidAlive := aliveFn(ctx, pid)
			if err := ctx.Err(); err != nil {
				return alive, err
			}
			if !pidAlive {
				// Pin the delete to the (session_id, watch_pid) we just
				// classified dead, AND require last_seen_ts <= the value
				// we read. A parallel `acd start` that swapped in a fresh
				// pid + bumped last_seen_ts will not match this predicate
				// and the row survives.
				if dropped, derr := state.DeregisterClientIfPID(ctx, db, c.SessionID, c.WatchPID.Int64, c.LastSeenTS); derr != nil {
					return alive, fmt.Errorf("daemon: drop dead-pid client %q: %w", c.SessionID, derr)
				} else if dropped {
					continue
				}
				// Refreshed under us — keep alive count untouched and let
				// the next sweep re-classify.
				alive++
				continue
			}
			// (3) Fingerprint mismatch — PID reuse defense. Only checked
			// when a stored fingerprint is present.
			if c.WatchFP.Valid && c.WatchFP.String != "" {
				live, ferr := captureFn(ctx, pid)
				if err := ctx.Err(); err != nil {
					return alive, err
				}
				if ferr != nil || live.Empty() {
					logFingerprintUnresolved(c.SessionID, pid, ferr)
					alive++
					continue
				}
				if storedHashOf(live) != c.WatchFP.String {
					if dropped, derr := state.DeregisterClientIfPID(ctx, db, c.SessionID, c.WatchPID.Int64, c.LastSeenTS); derr != nil {
						return alive, fmt.Errorf("daemon: drop fp-mismatch client %q: %w", c.SessionID, derr)
					} else if dropped {
						continue
					}
					alive++
					continue
				}
			}
		}

		alive++
	}

	return alive, nil
}

// ShouldSelfTerminate is the run-loop's exit gate per §8.4.
//
// True iff EmptySweepCount >= threshold AND sinceBoot >= BootGrace. The two
// conditions are AND-ed: a race between `acd start` registering its row and
// the daemon's first sweep would otherwise evict the row before the wake
// reached the loop.
func ShouldSelfTerminate(emptySweepCount int, sinceBoot time.Duration, opts SelfTerminateOpts) bool {
	threshold := opts.EmptySweepThreshold
	if threshold <= 0 {
		threshold = DefaultEmptySweepThreshold
	}
	grace := opts.BootGrace
	if grace <= 0 {
		grace = DefaultBootGrace
	}
	return emptySweepCount >= threshold && sinceBoot >= grace
}

// storedHashOf reduces a Fingerprint to the canonical string form persisted
// in daemon_clients.watch_fp. Format: "<startTime>|<argvHash>". Mirrors the
// legacy register/refresh paths so the GC compares apples to apples.
func storedHashOf(fp identity.Fingerprint) string {
	return fp.StartTime + "|" + fp.ArgvHash
}

// FingerprintToken is the canonical persisted form of a Fingerprint. Exposed
// for callers (e.g. `acd start`) that need to write watch_fp consistently
// with what the GC compares against.
func FingerprintToken(fp identity.Fingerprint) string {
	if fp.Empty() {
		return ""
	}
	return storedHashOf(fp)
}

// fingerprintWarnCap is the maximum number of distinct (session_id, pid)
// pairs the deduplication map for "client fingerprint unresolved" warnings
// retains across sweeps. A long-lived daemon attached to a churning fleet of
// session/pids would otherwise grow this map without bound — every unique
// pair we ever warned about stays in memory forever. When the map exceeds
// fingerprintWarnCap, we evict the oldest fingerprintWarnEvictBatch entries
// (by insertion timestamp). The dedup window is therefore "the most recent
// ≤fingerprintWarnCap unique pairs", which is tight enough that legitimate
// alarm clusters under investigation are not flushed mid-analysis.
const (
	fingerprintWarnCap        = 1024
	fingerprintWarnEvictBatch = 256
)

// fingerprintWarnEntry records when a (session_id, pid) was first warned so
// the bounded-eviction step can drop the oldest entries first.
type fingerprintWarnEntry struct {
	insertedAt time.Time
}

var (
	unresolvedFingerprintMu       sync.Mutex
	unresolvedFingerprintWarnings = make(map[string]fingerprintWarnEntry)
)

func logFingerprintUnresolved(sessionID string, pid int, err error) {
	key := fmt.Sprintf("%s:%d", sessionID, pid)
	unresolvedFingerprintMu.Lock()
	if _, loaded := unresolvedFingerprintWarnings[key]; loaded {
		unresolvedFingerprintMu.Unlock()
		return
	}
	unresolvedFingerprintWarnings[key] = fingerprintWarnEntry{insertedAt: time.Now()}
	unresolvedFingerprintMu.Unlock()
	attrs := []any{"session_id", sessionID, "pid", pid}
	if err != nil {
		attrs = append(attrs, "err", err.Error())
	}
	slog.Default().Warn("client fingerprint unresolved; keeping row until ttl", attrs...)
}

// sweepFingerprintWarnMap caps the dedup map at fingerprintWarnCap entries.
// When over the cap, it evicts oldest entries (by insertion timestamp) until
// the surviving set is at most fingerprintWarnCap - fingerprintWarnEvictBatch
// — i.e. it always frees a full batch's worth of headroom rather than
// trimming by a single batch from whatever the current size happens to be.
// This keeps a single sweep deterministic (target = cap - batch) regardless
// of how badly the map overflowed between ticks. Called once per refcount
// sweep tick from the run loop so growth is bounded by the sweep cadence.
//
// Eviction is intentionally batch-based (rather than 1-at-a-time on insert)
// so the common case (under-cap) costs only the size check; the O(n) sort
// runs at most once per sweep tick when the cap is exceeded.
func sweepFingerprintWarnMap() int {
	unresolvedFingerprintMu.Lock()
	defer unresolvedFingerprintMu.Unlock()
	cur := len(unresolvedFingerprintWarnings)
	if cur <= fingerprintWarnCap {
		return cur
	}
	// Collect (key, insertedAt) and sort by age. fingerprintWarnCap is small
	// (1024) so the worst-case overflow slice is bounded by sweep cadence.
	type kv struct {
		key string
		ts  time.Time
	}
	entries := make([]kv, 0, cur)
	for k, v := range unresolvedFingerprintWarnings {
		entries = append(entries, kv{key: k, ts: v.insertedAt})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ts.Before(entries[j].ts)
	})
	// Evict oldest entries until size <= (cap - evictBatch). target floors
	// at zero in case batch exceeds cap (defensive; not the configured
	// shape).
	target := fingerprintWarnCap - fingerprintWarnEvictBatch
	if target < 0 {
		target = 0
	}
	evict := cur - target
	if evict > len(entries) {
		evict = len(entries)
	}
	for i := 0; i < evict; i++ {
		delete(unresolvedFingerprintWarnings, entries[i].key)
	}
	return len(unresolvedFingerprintWarnings)
}
