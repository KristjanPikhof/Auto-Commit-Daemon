// Package central owns the cross-repo registry and stats DB.
//
// The registry (~/.local/share/acd/registry.json, §6.2) is a single shared
// JSON document listing every repo any acd daemon has ever touched. It is
// read and mutated by short-lived CLI calls (`acd repo register`,
// `acd repo list`, the daemon at startup) and so must be safe under
// concurrent writers from independent processes.
//
// Concurrency model:
//
//   - All read-modify-write happens under an exclusive POSIX advisory lock
//     (flock LOCK_EX) on ~/.local/share/acd/registry.lock. The lock file is
//     a separate path from the data file so that writers can rename the
//     data file underneath the lock without the lock fd ever pointing at a
//     stale inode.
//   - Writes are atomic at the filesystem level: the new content goes to
//     <path>.tmp, is fsync'd, then renamed over the destination. A reader
//     will only ever observe a fully-formed JSON document.
//   - WithLock is the preferred entry point for callers; Load + Save are
//     exposed for tests and for paths that want explicit control of the
//     critical section.
//
// Versioning:
//
//	{"version": 1, "repos": [...]}
//
// A document with version > 1 is rejected with ErrUnsupportedVersion so an
// older binary cannot silently downgrade-write a newer registry.
package central

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

// RegistryVersion is the current schema version. Future bumps must be paired
// with a forward migration in Load (and a documented compatibility window).
const RegistryVersion = 1

// ErrUnsupportedVersion is returned when a registry on disk reports a
// version newer than this binary understands. Older versions are accepted
// and re-saved at RegistryVersion.
var ErrUnsupportedVersion = errors.New("central: registry version unsupported")

// Registry is the in-memory representation of registry.json (§6.2).
type Registry struct {
	Version int          `json:"version"`
	Repos   []RepoRecord `json:"repos"`
}

// RepoRecord is one entry in Registry.Repos.
type RepoRecord struct {
	Path              string   `json:"path"`
	RepoHash          string   `json:"repo_hash"`
	StateDB           string   `json:"state_db"`
	FirstRegisteredTS int64    `json:"first_registered_ts"`
	LastSeenTS        int64    `json:"last_seen_ts"`
	Harnesses         []string `json:"harnesses"`
}

// NewRegistry returns an empty v1 registry.
func NewRegistry() *Registry {
	return &Registry{Version: RegistryVersion, Repos: []RepoRecord{}}
}

// Load reads the registry from roots without taking the flock. Suitable for
// read-only callers; mutators should use WithLock instead.
//
// A missing file is not an error: an empty registry is returned. A file with
// an unsupported version returns ErrUnsupportedVersion.
func Load(roots paths.Roots) (*Registry, error) {
	path := roots.RegistryPath()
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return NewRegistry(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("central: read registry: %w", err)
	}
	if len(b) == 0 {
		// An empty file (e.g. truncated by a crashed writer that never
		// reached the rename) is treated as missing. Atomic-write callers
		// never produce this state, but be defensive.
		return NewRegistry(), nil
	}
	var reg Registry
	if err := json.Unmarshal(b, &reg); err != nil {
		return nil, fmt.Errorf("central: parse registry: %w", err)
	}
	if reg.Version > RegistryVersion {
		return nil, fmt.Errorf("%w: file=%d binary=%d", ErrUnsupportedVersion, reg.Version, RegistryVersion)
	}
	if reg.Version == 0 {
		// Treat an unstamped file as v1 (greenfield default).
		reg.Version = RegistryVersion
	}
	if reg.Repos == nil {
		reg.Repos = []RepoRecord{}
	}
	reg.Normalize()
	return &reg, nil
}

// Save writes the registry to roots atomically. It does NOT take the flock;
// callers must already hold it (or be the only writer on the system).
func Save(roots paths.Roots, reg *Registry) error {
	if reg == nil {
		return fmt.Errorf("central: Save: nil registry")
	}
	if reg.Version == 0 {
		reg.Version = RegistryVersion
	}
	if reg.Version > RegistryVersion {
		return fmt.Errorf("%w: in-memory=%d binary=%d", ErrUnsupportedVersion, reg.Version, RegistryVersion)
	}
	if reg.Repos == nil {
		reg.Repos = []RepoRecord{}
	}
	reg.Normalize()
	return atomicWriteJSON(roots.RegistryPath(), reg)
}

// WithLock acquires the registry flock, runs fn against the loaded
// registry, and saves any mutations atomically. The lock is held for the
// full read-modify-write cycle so concurrent processes cannot interleave.
//
// The function is the preferred entry point for any code that mutates the
// registry: it guarantees no torn reads, no lost updates, and no partial
// writes even under crash-and-retry from a peer process.
//
// fn is allowed to return a non-nil error; in that case the registry is
// NOT saved (the in-memory mutation is discarded).
func WithLock(roots paths.Roots, fn func(*Registry) error) error {
	if fn == nil {
		return fmt.Errorf("central: WithLock: nil fn")
	}
	lockPath := roots.RegistryLockPath()
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o700); err != nil {
		return fmt.Errorf("central: mkdir share: %w", err)
	}
	// O_CREATE so the lock file is implicitly bootstrapped on first call.
	// 0o600 — share dir is 0700, the lock file does not need to be more
	// permissive than the data file.
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("central: open lock: %w", err)
	}
	defer func() { _ = f.Close() }()

	if err := flockExclusive(int(f.Fd())); err != nil {
		return fmt.Errorf("central: flock: %w", err)
	}
	// flockUnlock is best-effort — closing the fd would also release the
	// lock, but explicit unlock keeps the order obvious in profiling.
	defer func() { _ = flockUnlock(int(f.Fd())) }()

	reg, err := Load(roots)
	if err != nil {
		return err
	}
	if err := fn(reg); err != nil {
		return err
	}
	return Save(roots, reg)
}

// UpsertRepo inserts a new RepoRecord or refreshes an existing one keyed by
// Path. The harness argument is added to Harnesses if absent (set semantics).
// `now` is supplied by the caller so tests can pin the clock.
//
// Idempotency: calling UpsertRepo twice with identical args yields exactly
// one row. Multiple distinct harnesses are accumulated; duplicates are
// deduped.
func (r *Registry) UpsertRepo(path, repoHash, stateDB, harness string, now int64) {
	if r == nil {
		return
	}
	for i := range r.Repos {
		if SameRepoPath(r.Repos[i].Path, path) {
			row := &r.Repos[i]
			// Refresh the metadata that may have changed since the row was
			// first written (state_db can move if .git is relocated; the
			// hash should not, but track it anyway for resilience).
			if path != "" {
				row.Path = path
			}
			if repoHash != "" {
				row.RepoHash = repoHash
			}
			if stateDB != "" {
				row.StateDB = stateDB
			}
			row.LastSeenTS = now
			if harness != "" {
				row.Harnesses = addHarness(row.Harnesses, harness)
			}
			return
		}
	}
	rec := RepoRecord{
		Path:              path,
		RepoHash:          repoHash,
		StateDB:           stateDB,
		FirstRegisteredTS: now,
		LastSeenTS:        now,
	}
	if harness != "" {
		rec.Harnesses = []string{harness}
	} else {
		rec.Harnesses = []string{}
	}
	r.Repos = append(r.Repos, rec)
}

// Normalize merges duplicate repo records that refer to the same repository.
// This repairs older registries where the same macOS/Windows path was saved
// with different casing, or where equivalent paths resolve to the same
// filesystem object.
func (r *Registry) Normalize() {
	if r == nil || len(r.Repos) < 2 {
		return
	}
	out := make([]RepoRecord, 0, len(r.Repos))
	for _, rec := range r.Repos {
		merged := false
		for i := range out {
			if SameRepoPath(out[i].Path, rec.Path) {
				mergeRepoRecord(&out[i], rec)
				merged = true
				break
			}
		}
		if !merged {
			out = append(out, rec)
		}
	}
	r.Repos = out
}

func mergeRepoRecord(dst *RepoRecord, src RepoRecord) {
	if dst == nil {
		return
	}
	if dst.FirstRegisteredTS == 0 ||
		(src.FirstRegisteredTS > 0 && src.FirstRegisteredTS < dst.FirstRegisteredTS) {
		dst.FirstRegisteredTS = src.FirstRegisteredTS
	}
	if src.LastSeenTS >= dst.LastSeenTS {
		if src.Path != "" {
			dst.Path = src.Path
		}
		if src.RepoHash != "" {
			dst.RepoHash = src.RepoHash
		}
		if src.StateDB != "" {
			dst.StateDB = src.StateDB
		}
		dst.LastSeenTS = src.LastSeenTS
	}
	for _, h := range src.Harnesses {
		dst.Harnesses = addHarness(dst.Harnesses, h)
	}
}

// SameRepoPath reports whether two registry paths identify the same repo.
// Existing paths are compared by file identity. As a fallback, platforms
// whose default filesystems are case-insensitive compare cleaned paths with
// case folding so stale records from older binaries collapse predictably.
func SameRepoPath(a, b string) bool {
	if a == "" || b == "" {
		return a == b
	}
	cleanA := filepath.Clean(a)
	cleanB := filepath.Clean(b)
	if cleanA == cleanB {
		return true
	}
	infoA, errA := os.Stat(cleanA)
	infoB, errB := os.Stat(cleanB)
	if errA == nil && errB == nil && os.SameFile(infoA, infoB) {
		return true
	}
	if pathCaseFoldedByDefault() {
		return strings.EqualFold(cleanA, cleanB)
	}
	return false
}

func pathCaseFoldedByDefault() bool {
	return runtime.GOOS == "darwin" || runtime.GOOS == "windows"
}

// addHarness returns the slice with name added if it was not already
// present. The result is always sorted to keep the JSON deterministic.
func addHarness(existing []string, name string) []string {
	for _, h := range existing {
		if h == name {
			return existing
		}
	}
	out := append(existing, name)
	sort.Strings(out)
	return out
}

// atomicWriteJSON marshals v as indented JSON and writes it to path
// atomically: <path>.tmp -> fsync -> rename. The parent directory is
// created with 0700 if missing.
func atomicWriteJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("central: mkdir parent: %w", err)
	}
	body, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("central: marshal: %w", err)
	}
	// Trailing newline so the file is friendly to text tools.
	body = append(body, '\n')

	tmp := path + ".tmp"
	// O_TRUNC because a crashed previous attempt may have left a partial
	// .tmp behind; we are about to overwrite it under the flock.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("central: open tmp: %w", err)
	}
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("central: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("central: fsync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("central: close tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("central: rename tmp: %w", err)
	}
	return nil
}

// flockExclusive acquires an exclusive POSIX advisory lock (LOCK_EX) on fd.
// Blocks until the lock is available. Pure stdlib — no cgo.
func flockExclusive(fd int) error {
	for {
		err := syscall.Flock(fd, syscall.LOCK_EX)
		if err == nil {
			return nil
		}
		if errors.Is(err, syscall.EINTR) {
			// Spurious signal interruption — retry. macOS in particular
			// will surface EINTR here during e.g. SIGCHLD delivery.
			continue
		}
		return err
	}
}

// flockUnlock releases the lock acquired by flockExclusive.
func flockUnlock(fd int) error {
	return syscall.Flock(fd, syscall.LOCK_UN)
}
