// refcount.go implements the daemon_clients GC sweep + self-terminate gate
// per §3.4 + §8.4.
//
// A row is dead when ANY of these holds:
//   1. last_seen_ts + TTL < now              (TTL refresh expired)
//   2. watch_pid != 0 AND !identity.Alive    (peer died)
//   3. watch_pid != 0 AND fingerprint != stored (PID reuse defense)
//
// The daemon self-terminates after EmptySweepThreshold consecutive sweeps
// returning alive==0, but only past BootGrace from boot — a short-lived
// `acd start` that races the daemon's first sweep must not get evicted.
package daemon

import (
	"context"
	"fmt"
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
	CaptureFingerprint func(pid int) (identity.Fingerprint, error)
	// AliveFn checks pid liveness. Defaulted to identity.Alive; tests inject.
	AliveFn func(pid int) bool
}

// SelfTerminateOpts configures ShouldSelfTerminate.
type SelfTerminateOpts struct {
	BootGrace            time.Duration
	EmptySweepThreshold  int
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
		aliveFn = identity.Alive
	}
	captureFn := opts.CaptureFingerprint
	if captureFn == nil {
		captureFn = identity.Capture
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
		// (1) TTL gate — universal fallback. Even peers without a watch pid
		// are GC'd here.
		if c.LastSeenTS < cutoff {
			if _, derr := state.DeregisterClient(ctx, db, c.SessionID); derr != nil {
				return alive, fmt.Errorf("daemon: drop ttl client %q: %w", c.SessionID, derr)
			}
			continue
		}

		// (2) PID liveness — fast-path eviction.
		if c.WatchPID.Valid && c.WatchPID.Int64 > 0 {
			pid := int(c.WatchPID.Int64)
			if !aliveFn(pid) {
				if _, derr := state.DeregisterClient(ctx, db, c.SessionID); derr != nil {
					return alive, fmt.Errorf("daemon: drop dead-pid client %q: %w", c.SessionID, derr)
				}
				continue
			}
			// (3) Fingerprint mismatch — PID reuse defense. Only checked
			// when a stored fingerprint is present.
			if c.WatchFP.Valid && c.WatchFP.String != "" {
				live, ferr := captureFn(pid)
				if ferr != nil || live.Empty() {
					// Cannot resolve fingerprint -> treat as dead (legacy
					// verify_process_identity returns False on lookup
					// failure, which the GC consumes as "drop").
					if _, derr := state.DeregisterClient(ctx, db, c.SessionID); derr != nil {
						return alive, fmt.Errorf("daemon: drop unresolved-fp client %q: %w", c.SessionID, derr)
					}
					continue
				}
				if storedHashOf(live) != c.WatchFP.String {
					if _, derr := state.DeregisterClient(ctx, db, c.SessionID); derr != nil {
						return alive, fmt.Errorf("daemon: drop fp-mismatch client %q: %w", c.SessionID, derr)
					}
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
