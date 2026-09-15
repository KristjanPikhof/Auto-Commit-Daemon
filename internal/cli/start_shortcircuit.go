package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
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
//
// Hot-path refcount refresh: the short-circuit branch in runStart calls
// state.TouchClient (via touchClientHotPath in start.go) immediately
// after a successful decision. The single keyed UPDATE on
// daemon_clients.last_seen_ts keeps the daemon's refcount sweeper from
// evicting a session that lives entirely on the hot path. We accept the
// extra ~1ms SQLite open/UPDATE/close because the alternative (halving
// the cache TTL so the cold path runs more often) would defeat the
// 50ms-vs-1s budget that justifies the cache in the first place.
const startCacheVersion = 2

// startCacheFilenamePrefix is the per-repo cache file prefix written under
// <gitDir>/acd/. Atomic writes (tmp + rename) keep each file safe under
// concurrent readers and writers without taking control.lock. The full
// filename is "<prefix><sha256(session_id)[:16]>.json" so two harnesses
// (Claude Code + Codex) or two parallel sessions of the same harness can
// share a repo without evicting each other from the short-circuit cache.
//
// Hashing the session_id (rather than embedding it raw) keeps the path
// fixed-length, avoids os-level filename surprises (slashes, colons in
// some harness session UUIDs), and reduces the disclosure surface — the
// 16-hex prefix is enough to disambiguate hundreds of millions of entries
// before a collision.
const startCacheFilenamePrefix = "start-cache-"

// startCache is the JSON payload persisted at <gitDir>/acd/start-cache.json
// after a successful runStart. Its sole purpose is to let a subsequent
// runStart with the same session_id confirm — without acquiring control.lock
// or opening SQLite — that the daemon is still healthy and the registration
// is still valid.
type startCache struct {
	Version        int    `json:"version"`
	RepoHash       string `json:"repo_hash"`
	SessionID      string `json:"session_id"`
	Harness        string `json:"harness"`
	DaemonPID      int    `json:"daemon_pid"`
	WatchPID       int    `json:"watch_pid,omitempty"`
	ClientCount    int    `json:"client_count,omitempty"`
	UpdatedAt      int64  `json:"updated_at_unix"`
	DaemonStartTS  string `json:"daemon_start_ts,omitempty"`
	DaemonArgvHash string `json:"daemon_argv_hash,omitempty"`
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

type registryBackedStartResult struct {
	gitDir string
	startResult
}

// startCachePath returns the per-session cache path under gitDir. It does
// NOT create the parent directory — the full runStart path does that under
// control.lock; the short-circuit reader must tolerate a missing directory
// (and treat that as "cold" / no cache).
//
// The filename embeds a sha256 prefix of session_id so concurrent sessions
// on the same repo never share a cache file. Empty sessionID is tolerated
// (yields a fixed "empty" suffix) so manual `acd start` from a human can
// also benefit from the cache; the human-session id is itself derived
// from the repo hash so cross-session collisions are not possible.
func startCachePath(gitDir, sessionID string) string {
	return filepath.Join(gitDir, "acd", startCacheFilenamePrefix+sessionCacheSuffix(sessionID)+".json")
}

// sessionCacheSuffix is the 16-hex prefix of sha256(session_id). We only
// take 16 hex chars (64 bits) which is enough to make accidental
// collisions astronomically unlikely while keeping the filename short.
func sessionCacheSuffix(sessionID string) string {
	if sessionID == "" {
		return "empty"
	}
	sum := sha256.Sum256([]byte(sessionID))
	return hex.EncodeToString(sum[:8])
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
