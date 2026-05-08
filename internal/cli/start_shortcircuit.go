package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

// startCacheVersion is the on-disk schema marker for start-cache.json.
//
// A cache file with version != startCacheVersion is treated as missing and
// the caller falls back to the full registration path. This lets future
// schema changes invalidate stale caches without surgical migration.
//
// v2 (2026-05): adds DaemonStartTS (truncated lstart) + DaemonArgvHash.
// PID-reuse on long-running boxes lets an unrelated process inherit a
// recycled daemon PID; the fingerprint check pins the cached pid to the
// original daemon's process start time + argv hash, so the recycled pid
// fails the equality check and the caller escalates to the cold path.
const startCacheVersion = 2

// startCacheFilename is the per-repo cache file written under
// <gitDir>/acd/. Atomic writes (tmp + rename) keep it safe under concurrent
// readers and writers without taking control.lock.
const startCacheFilename = "start-cache.json"

// startCache is the JSON payload persisted at <gitDir>/acd/start-cache.json
// after a successful runStart. Its sole purpose is to let a subsequent
// runStart with the same session_id confirm — without acquiring control.lock
// or opening SQLite — that the daemon is still healthy and the registration
// is still valid.
type startCache struct {
	Version         int    `json:"version"`
	RepoHash        string `json:"repo_hash"`
	SessionID       string `json:"session_id"`
	Harness         string `json:"harness"`
	DaemonPID       int    `json:"daemon_pid"`
	WatchPID        int    `json:"watch_pid,omitempty"`
	ClientCount     int    `json:"client_count,omitempty"`
	UpdatedAt       int64  `json:"updated_at_unix"`
	DaemonStartTS   string `json:"daemon_start_ts,omitempty"`
	DaemonArgvHash  string `json:"daemon_argv_hash,omitempty"`
}

// captureDaemonFingerprint resolves the running daemon's identity stamp.
// Indirected through a package-level var so unit tests can pin a
// deterministic fingerprint without spawning a real process. Returns the
// captured Fingerprint or an error from the underlying ps call.
var captureDaemonFingerprint = func(ctx context.Context, pid int) (identity.Fingerprint, error) {
	return identity.CaptureContext(ctx, pid)
}

// shortCircuitNow is the clock used by the short-circuit decision matrix.
// Tests override it to pin a deterministic time reference; production
// callers leave it at the default.
var shortCircuitNow = func() time.Time { return time.Now() }

// startCachePath returns the per-repo cache path under gitDir. It does NOT
// create the parent directory — the full runStart path does that under
// control.lock; the short-circuit reader must tolerate a missing directory
// (and treat that as "cold" / no cache).
func startCachePath(gitDir string) string {
	return filepath.Join(gitDir, "acd", startCacheFilename)
}

// readStartCache returns the cache payload or nil if the file is missing,
// empty, malformed, or stamped with an unsupported version. It deliberately
// swallows non-IO errors: a corrupt cache should never block the cold path.
func readStartCache(path string) *startCache {
	b, err := os.ReadFile(path)
	if err != nil || len(b) == 0 {
		return nil
	}
	var sc startCache
	if err := json.Unmarshal(b, &sc); err != nil {
		return nil
	}
	if sc.Version != startCacheVersion {
		return nil
	}
	return &sc
}

// writeStartCache atomically persists sc to <gitDir>/acd/start-cache.json.
// The parent directory is assumed to exist (the full registration path
// creates it). Errors are returned so the caller can log; failure to write
// the cache is non-fatal — the next runStart simply takes the full path.
func writeStartCache(gitDir string, sc startCache) error {
	if sc.Version == 0 {
		sc.Version = startCacheVersion
	}
	dir := filepath.Join(gitDir, "acd")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("start-cache mkdir: %w", err)
	}
	body, err := json.Marshal(sc)
	if err != nil {
		return fmt.Errorf("start-cache marshal: %w", err)
	}
	body = append(body, '\n')
	target := startCachePath(gitDir)
	tmp := target + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("start-cache open tmp: %w", err)
	}
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("start-cache write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("start-cache fsync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("start-cache close tmp: %w", err)
	}
	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("start-cache rename: %w", err)
	}
	return nil
}

// shortCircuitDecision is the output of the registry-read decision matrix.
// `OK` means the caller may skip control.lock acquisition, SQLite open,
// and central registry rewrite. `Reason` records why escalation was forced
// (used in tests and JSON debug output).
type shortCircuitDecision struct {
	OK     bool
	Reason string
	// DaemonPID is the cached daemon pid surfaced to the caller's
	// startResult so a short-circuited reply still reports the right pid.
	DaemonPID int
	// ClientCount is the cached client count snapshot. May lag the live
	// SQLite truth by one tick if a concurrent runStart registered a
	// new session between cache writes; harness consumers do not depend
	// on a strictly-fresh value here.
	ClientCount int
}

// evaluateShortCircuit runs the decision matrix against an explicit cache +
// registry snapshot. Splitting the IO out of the decision lets the unit
// tests cover every branch without filesystem fixtures.
//
// The matrix:
//   - cache == nil          → escalate (no entry / corrupt file)
//   - mismatched repo       → escalate (cache from another repo, paranoid)
//   - mismatched session    → escalate (different session_id)
//   - mismatched harness    → escalate (harness rebinding requires SQLite)
//   - stale heartbeat       → escalate (clientTTL exceeded)
//   - missing registry      → escalate (cold registry / first ever start)
//   - dead daemon pid       → escalate (cache lies about liveness)
//   - fingerprint mismatch  → escalate (PID reuse: pid alive but unrelated process)
//   - all fresh + alive     → short-circuit
//
// fpCapture is the daemon-fingerprint resolver; when nil, the package
// default is used. The captured fingerprint is compared against the cached
// (DaemonStartTS, DaemonArgvHash). If the cache lacks a fingerprint stamp
// (legacy / never written) we treat it as a mismatch and force the cold
// path — schema v2 requires a stamp.
func evaluateShortCircuit(
	cache *startCache,
	repoHash, sessionID, harness string,
	registryRecord *central.RepoRecord,
	now time.Time,
	ttl time.Duration,
	pidAlive func(int) bool,
	fpCapture func(context.Context, int) (identity.Fingerprint, error),
) shortCircuitDecision {
	if cache == nil {
		return shortCircuitDecision{Reason: "no_cache"}
	}
	if cache.RepoHash != repoHash {
		return shortCircuitDecision{Reason: "repo_hash_mismatch"}
	}
	if cache.SessionID != sessionID {
		return shortCircuitDecision{Reason: "session_mismatch"}
	}
	if cache.Harness != harness {
		return shortCircuitDecision{Reason: "harness_mismatch"}
	}
	if cache.DaemonPID <= 0 {
		return shortCircuitDecision{Reason: "no_daemon_pid"}
	}
	updatedAt := time.Unix(cache.UpdatedAt, 0)
	if cache.UpdatedAt <= 0 || now.Sub(updatedAt) >= ttl {
		return shortCircuitDecision{Reason: "stale_heartbeat"}
	}
	if registryRecord == nil {
		return shortCircuitDecision{Reason: "registry_missing_repo"}
	}
	// Defensive: reject cache that claims to belong to a different repo
	// hash than the one currently registered. Should only fire if the
	// .git dir was relocated.
	if registryRecord.RepoHash != "" && registryRecord.RepoHash != repoHash {
		return shortCircuitDecision{Reason: "registry_hash_mismatch"}
	}
	if pidAlive == nil {
		pidAlive = identity.Alive
	}
	if !pidAlive(cache.DaemonPID) {
		return shortCircuitDecision{Reason: "daemon_pid_dead"}
	}
	// Defense against PID reuse: the kill(0) probe above only proves
	// SOMEONE owns this pid; it does not prove the original daemon
	// still owns it. Pin the cached pid to the daemon's lstart + argv
	// fingerprint stamped on cold-path write. Mismatch => recycled pid.
	if cache.DaemonStartTS == "" && cache.DaemonArgvHash == "" {
		return shortCircuitDecision{Reason: "fingerprint_missing"}
	}
	if fpCapture == nil {
		fpCapture = captureDaemonFingerprint
	}
	live, err := fpCapture(context.Background(), cache.DaemonPID)
	if err != nil {
		// A `ps` failure here typically means the pid disappeared
		// between the kill(0) probe and the ps call; treat as a
		// mismatch and force the cold path to re-establish identity.
		return shortCircuitDecision{Reason: "fingerprint_capture_failed"}
	}
	if live.StartTime != cache.DaemonStartTS || live.ArgvHash != cache.DaemonArgvHash {
		return shortCircuitDecision{Reason: "fingerprint_mismatch"}
	}
	return shortCircuitDecision{
		OK:          true,
		DaemonPID:   cache.DaemonPID,
		ClientCount: cache.ClientCount,
	}
}

// tryShortCircuitStart implements the registry-read short-circuit at the
// very top of runStart. It performs only:
//
//   - a single os.ReadFile of <gitDir>/acd/start-cache.json
//   - a single central.Load (no flock; lock-free read of registry.json)
//   - a single identity.Alive(daemonPID) — kill(pid, 0)
//
// It does NOT acquire control.lock, open SQLite, or rewrite the central
// registry. When all preconditions hold, the caller may return the supplied
// startResult immediately and skip the full registration path.
func tryShortCircuitStart(
	ctx context.Context,
	gitDir, repoHash, sessionID, harness, repo string,
) (didShortCircuit bool, daemonPID, clientCount int, reason string) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return false, 0, 0, "ctx_done"
		}
	}
	cache := readStartCache(startCachePath(gitDir))
	roots, err := paths.Resolve()
	if err != nil {
		return false, 0, 0, "paths_resolve_failed"
	}
	reg, err := central.Load(roots)
	if err != nil {
		// A registry that cannot be parsed must escalate to the full
		// path so the writer (under flock) can repair it.
		if errors.Is(err, central.ErrUnsupportedVersion) {
			return false, 0, 0, "registry_unsupported_version"
		}
		return false, 0, 0, "registry_load_failed"
	}
	var rec *central.RepoRecord
	if reg != nil {
		for i := range reg.Repos {
			if central.SameRepoPath(reg.Repos[i].Path, repo) ||
				(reg.Repos[i].RepoHash != "" && reg.Repos[i].RepoHash == repoHash) {
				rec = &reg.Repos[i]
				break
			}
		}
	}
	d := evaluateShortCircuit(cache, repoHash, sessionID, harness, rec,
		shortCircuitNow(), clientTTL(), identity.Alive, captureDaemonFingerprint)
	if !d.OK {
		return false, 0, 0, d.Reason
	}
	return true, d.DaemonPID, d.ClientCount, ""
}
