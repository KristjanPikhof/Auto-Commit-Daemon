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
//	{"version": 2, "repos": [...]}
//
// A document with version > 2 is rejected with ErrUnsupportedVersion so an
// older binary cannot silently downgrade-write a newer registry.
package central

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// RegistryVersion is the current schema version. Future bumps must be paired
// with an explicit migration plan. Load deliberately preserves older versions
// so read-only commands never perform the one-shot cutover implicitly.
const RegistryVersion = 2

// ErrUnsupportedVersion is returned when a registry on disk reports a
// version newer than this binary understands. Older versions are accepted
// and retain their version until setup migrates them transactionally.
var ErrUnsupportedVersion = errors.New("central: registry version unsupported")

// Registry is the in-memory representation of registry.json (§6.2).
type Registry struct {
	Version int          `json:"version"`
	Repos   []RepoRecord `json:"repos"`
}

// RepoRecord is one entry in Registry.Repos.
type RepoRecord struct {
	Path               string   `json:"path"`
	RepoHash           string   `json:"repo_hash"`
	RepositoryID       string   `json:"repository_id,omitempty"`
	WorktreeID         string   `json:"worktree_id,omitempty"`
	CommonDir          string   `json:"common_dir,omitempty"`
	LegacyRepoHash     string   `json:"legacy_repo_hash,omitempty"`
	StateDB            string   `json:"state_db"`
	FirstRegisteredTS  int64    `json:"first_registered_ts"`
	LastSeenTS         int64    `json:"last_seen_ts"`
	Harnesses          []string `json:"harnesses"`
	LifecycleState     string   `json:"lifecycle_state,omitempty"`
	LifecycleUpdatedTS int64    `json:"lifecycle_updated_ts,omitempty"`
}

const (
	RepoLifecycleEnabled  = "enabled"
	RepoLifecycleDisabled = "disabled"
)

// RepoRegistrationResult describes an explicit registry insert or refresh.
type RepoRegistrationResult struct {
	Record    RepoRecord `json:"record"`
	Inserted  bool       `json:"inserted"`
	Refreshed bool       `json:"refreshed"`
}

// RepoRemovalTarget identifies the registry row to remove. Path should be a
// canonical worktree root when known; StateDB may be supplied to remove legacy
// rows whose path is stale but whose state DB still identifies the repo.
type RepoRemovalTarget struct {
	Path    string `json:"path,omitempty"`
	StateDB string `json:"state_db,omitempty"`
}

// RepoRemovalResult is returned by registry removal helpers. RemovedRecord is
// populated only when Removed is true; Safety is non-destructive metadata for
// the CLI to decide whether to stop daemons, clear caches, or purge state.
type RepoRemovalResult struct {
	Removed       bool              `json:"removed"`
	NotFound      bool              `json:"not_found"`
	RemovedRecord *RepoRecord       `json:"removed_record,omitempty"`
	Safety        RepoRemovalSafety `json:"safety"`
}

// RepoRemovalSafety captures state paths and best-effort daemon liveness for
// a registry row. It never deletes files or opens state.db read-write.
type RepoRemovalSafety struct {
	Path             string `json:"path,omitempty"`
	StateDB          string `json:"state_db,omitempty"`
	GitDir           string `json:"git_dir,omitempty"`
	StateDir         string `json:"state_dir,omitempty"`
	StartCacheDir    string `json:"start_cache_dir,omitempty"`
	StateDBExists    bool   `json:"state_db_exists"`
	StateDirExists   bool   `json:"state_dir_exists"`
	DaemonStateKnown bool   `json:"daemon_state_known"`
	DaemonMode       string `json:"daemon_mode,omitempty"`
	DaemonPID        int    `json:"daemon_pid,omitempty"`
	DaemonAlive      bool   `json:"daemon_alive"`
	DaemonStateError string `json:"daemon_state_error,omitempty"`
}

// RepoLifecycleResult describes a per-repo lifecycle mutation. NotFound is
// true when the target does not match any registry row; Updated is true only
// when the disabled lifecycle fields changed.
type RepoLifecycleResult struct {
	Record   RepoRecord `json:"record,omitempty"`
	Updated  bool       `json:"updated"`
	NotFound bool       `json:"not_found"`
}

// LegacyDuplicateChange describes one registry row removed by
// CleanupLegacyDuplicates. Reason is "same-git-toplevel" or "same-state-db".
type LegacyDuplicateChange struct {
	KeptPath    string `json:"kept_path"`
	DroppedPath string `json:"dropped_path"`
	Reason      string `json:"reason"`
}

// NewRegistry returns an empty v2 registry.
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
		reg.Version = 1
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
	wantStateDB := canonicalStateDB(stateDB)
	for i := range r.Repos {
		rowStateDB := canonicalStateDB(r.Repos[i].StateDB)
		if SameRepoPath(r.Repos[i].Path, path) || (wantStateDB != "" && rowStateDB != "" && sameCleanPath(rowStateDB, wantStateDB)) {
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

// RegisterResolvedRepo inserts or refreshes the registry row for an already
// resolved Git worktree. Call this from inside WithLock when persisting the
// mutation. It preserves the original first_registered_ts on refresh.
func (r *Registry) RegisterResolvedRepo(wt git.Worktree, harness string, now int64) (RepoRegistrationResult, error) {
	if r == nil {
		return RepoRegistrationResult{}, fmt.Errorf("central: RegisterResolvedRepo: nil registry")
	}
	if wt.Root == "" {
		return RepoRegistrationResult{}, fmt.Errorf("central: RegisterResolvedRepo: empty worktree root")
	}
	if wt.GitDir == "" {
		return RepoRegistrationResult{}, fmt.Errorf("central: RegisterResolvedRepo: empty git dir")
	}
	legacyRepoHash, err := paths.RepoHash(wt.Root)
	if err != nil {
		return RepoRegistrationResult{}, fmt.Errorf("central: repo hash: %w", err)
	}
	commonDir := wt.CommonDir
	if commonDir == "" {
		commonDir, err = git.GitCommonDir(context.Background(), wt.Root)
		if err != nil {
			return RepoRegistrationResult{}, fmt.Errorf("central: git common dir: %w", err)
		}
	}
	repositoryID := canonicalID(commonDir)
	worktreeID := canonicalID(wt.Root)
	stateDB := state.DBPathFromGitDir(wt.GitDir)
	_, existed := r.FindRepo(wt.Root, stateDB)
	r.UpsertRepo(wt.Root, repositoryID, stateDB, harness, now)
	for i := range r.Repos {
		if SameRepoPath(r.Repos[i].Path, wt.Root) ||
			sameCleanPath(canonicalStateDB(r.Repos[i].StateDB), canonicalStateDB(stateDB)) {
			r.Repos[i].RepositoryID = repositoryID
			r.Repos[i].WorktreeID = worktreeID
			r.Repos[i].CommonDir = commonDir
			r.Repos[i].LegacyRepoHash = legacyRepoHash
			r.Repos[i].RepoHash = repositoryID
			break
		}
	}
	rec, ok := r.FindRepo(wt.Root, stateDB)
	if !ok {
		return RepoRegistrationResult{}, fmt.Errorf("central: registered repo row not found after upsert")
	}
	return RepoRegistrationResult{
		Record:    rec,
		Inserted:  !existed,
		Refreshed: existed,
	}, nil
}

// CanonicalID returns the 16-hex v2 identity for one canonical filesystem
// path. Repository IDs hash Git common directories; worktree IDs hash roots.
func CanonicalID(path string) string { return canonicalID(path) }

func canonicalID(path string) string {
	digest := sha256.Sum256([]byte(filepath.Clean(path)))
	return hex.EncodeToString(digest[:8])
}

// PlanRegistryV2 resolves every registered worktree and returns a detached v2
// document. The input is never mutated, so setup can include the result in a
// global plan digest and abort before any write when one identity is missing.
func PlanRegistryV2(ctx context.Context, current *Registry) (*Registry, error) {
	if current == nil {
		return nil, errors.New("central: nil registry")
	}
	planned := &Registry{Version: RegistryVersion, Repos: make([]RepoRecord, 0, len(current.Repos))}
	for _, existing := range current.Repos {
		wt, err := git.ResolveWorktree(ctx, existing.Path)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("central: resolve registered worktree %s: %w", existing.Path, err)
			}
			summary, summaryErr := state.ReadLegacyWorkSummary(ctx, existing.StateDB)
			if summaryErr != nil {
				return nil, fmt.Errorf("central: inspect missing registered worktree %s: %w", existing.Path, summaryErr)
			}
			if summary.Unpublished > 0 || summary.Terminal > 0 || summary.OpenPublication > 0 {
				return nil, fmt.Errorf("central: missing registered worktree %s has unresolved captured work", existing.Path)
			}
			// A missing worktree with no surviving unresolved state cannot be
			// supervised, but it also must not brick setup for every live repo.
			// Preserve the row as disabled migration metadata instead of
			// silently deleting it. If the path returns, the user can explicitly
			// enable it again after its identity is resolved.
			existing.LifecycleState = RepoLifecycleDisabled
			commonDir := filepath.Join(existing.Path, ".git")
			if strings.TrimSpace(existing.StateDB) != "" {
				commonDir = filepath.Dir(filepath.Dir(existing.StateDB))
			}
			legacy := existing.RepoHash
			existing.RepositoryID = canonicalID(commonDir)
			existing.WorktreeID = canonicalID(existing.Path)
			existing.CommonDir = commonDir
			existing.LegacyRepoHash = legacy
			existing.RepoHash = existing.RepositoryID
			planned.Repos = append(planned.Repos, existing)
			continue
		}
		legacy := existing.RepoHash
		repositoryID := canonicalID(wt.CommonDir)
		existing.Path = wt.Root
		existing.StateDB = state.DBPathFromGitDir(wt.GitDir)
		existing.RepositoryID = repositoryID
		existing.WorktreeID = canonicalID(wt.Root)
		existing.CommonDir = wt.CommonDir
		existing.LegacyRepoHash = legacy
		existing.RepoHash = repositoryID
		planned.Repos = append(planned.Repos, existing)
	}
	planned.Normalize()
	return planned, nil
}

// FindRepo returns the row matching path or any supplied state DB path. It
// does not mutate the registry; callers that need persistence should use
// RegisterResolvedRepo or UpsertRepo under WithLock.
func (r *Registry) FindRepo(path string, stateDBs ...string) (RepoRecord, bool) {
	idx, ok := r.findRepoIndex(path, stateDBs...)
	if !ok {
		return RepoRecord{}, false
	}
	return r.Repos[idx], true
}

// RemoveRepoByPath removes the row whose Path matches path. Call this from
// inside WithLock when persisting the mutation.
func (r *Registry) RemoveRepoByPath(ctx context.Context, path string) RepoRemovalResult {
	return r.RemoveRepo(ctx, RepoRemovalTarget{Path: path})
}

// RemoveRepoByStateDB removes the row whose StateDB matches stateDB. Call this
// from inside WithLock when persisting the mutation.
func (r *Registry) RemoveRepoByStateDB(ctx context.Context, stateDB string) RepoRemovalResult {
	return r.RemoveRepo(ctx, RepoRemovalTarget{StateDB: stateDB})
}

// RemoveRepo removes exactly one matching registry row by canonical path or
// state DB. It is intentionally non-destructive: it only mutates Registry.Repos
// and returns safety metadata for the removed row. Call this from inside
// WithLock when persisting the mutation.
func (r *Registry) RemoveRepo(ctx context.Context, target RepoRemovalTarget) RepoRemovalResult {
	if r == nil {
		return RepoRemovalResult{NotFound: true}
	}
	idx, ok := r.findRepoIndex(target.Path, target.StateDB)
	if !ok {
		return RepoRemovalResult{
			NotFound: true,
			Safety:   removalSafetyFromTarget(ctx, target),
		}
	}
	removed := r.Repos[idx]
	r.Repos = append(r.Repos[:idx], r.Repos[idx+1:]...)
	return RepoRemovalResult{
		Removed:       true,
		RemovedRecord: &removed,
		Safety:        ProbeRepoRemovalSafety(ctx, removed),
	}
}

// DisableRepo marks a registered repo disabled without touching registration,
// daemon, harness, or timestamp metadata. Call this from inside WithLock when
// persisting the mutation.
func (r *Registry) DisableRepo(target RepoRemovalTarget, now int64) RepoLifecycleResult {
	return r.setRepoDisabled(target, true, now)
}

// EnableRepo clears a registered repo's disabled lifecycle state without
// touching registration, daemon, harness, or timestamp metadata. Call this
// from inside WithLock when persisting the mutation.
func (r *Registry) EnableRepo(target RepoRemovalTarget, now int64) RepoLifecycleResult {
	return r.setRepoDisabled(target, false, now)
}

func (r *Registry) setRepoDisabled(target RepoRemovalTarget, disabled bool, now int64) RepoLifecycleResult {
	if r == nil {
		return RepoLifecycleResult{NotFound: true}
	}
	idx, ok := r.findRepoIndex(target.Path, target.StateDB)
	if !ok {
		return RepoLifecycleResult{NotFound: true}
	}
	row := &r.Repos[idx]
	beforeState := row.LifecycleState
	beforeUpdatedTS := row.LifecycleUpdatedTS
	if disabled {
		if row.LifecycleDisabled() {
			return RepoLifecycleResult{Record: *row}
		}
		row.LifecycleState = RepoLifecycleDisabled
		row.LifecycleUpdatedTS = now
	} else {
		if !row.LifecycleDisabled() && row.LifecycleState == "" {
			return RepoLifecycleResult{Record: *row}
		}
		row.LifecycleState = ""
		row.LifecycleUpdatedTS = now
	}
	return RepoLifecycleResult{
		Record:   *row,
		Updated:  beforeState != row.LifecycleState || beforeUpdatedTS != row.LifecycleUpdatedTS,
		NotFound: false,
	}
}

func (rec RepoRecord) LifecycleStateName() string {
	if rec.LifecycleState == "" {
		return RepoLifecycleEnabled
	}
	return rec.LifecycleState
}

func (rec RepoRecord) LifecycleDisabled() bool {
	return rec.LifecycleState == RepoLifecycleDisabled
}

func (r *Registry) findRepoIndex(path string, stateDBs ...string) (int, bool) {
	if r == nil {
		return -1, false
	}
	for i, rec := range r.Repos {
		if path != "" && SameRepoPath(rec.Path, path) {
			return i, true
		}
		if matchesAnyStateDB(rec.StateDB, stateDBs) {
			return i, true
		}
	}
	return -1, false
}

func matchesAnyStateDB(actual string, expected []string) bool {
	actual = canonicalStateDB(actual)
	if actual == "" {
		return false
	}
	for _, want := range expected {
		want = canonicalStateDB(want)
		if want != "" && sameCleanPath(actual, want) {
			return true
		}
	}
	return false
}

func removalSafetyFromTarget(ctx context.Context, target RepoRemovalTarget) RepoRemovalSafety {
	return ProbeRepoRemovalSafety(ctx, RepoRecord{Path: target.Path, StateDB: target.StateDB})
}

// ProbeRepoRemovalSafety returns non-destructive metadata for repo removal
// previews. It opens state.db read-only when present and records probe errors
// in the result instead of failing the registry mutation.
func ProbeRepoRemovalSafety(ctx context.Context, rec RepoRecord) RepoRemovalSafety {
	if ctx == nil {
		ctx = context.Background()
	}
	s := RepoRemovalSafety{
		Path:    rec.Path,
		StateDB: canonicalStateDB(rec.StateDB),
	}
	if s.StateDB != "" {
		s.StateDir = filepath.Dir(s.StateDB)
		s.GitDir = filepath.Dir(s.StateDir)
		s.StartCacheDir = s.StateDir
		s.StateDBExists = fileExists(s.StateDB)
		s.StateDirExists = fileExists(s.StateDir)
	}
	if !s.StateDBExists {
		return s
	}
	pid, mode, known, err := readDaemonStateReadOnly(ctx, s.StateDB)
	if err != nil {
		s.DaemonStateError = err.Error()
		return s
	}
	s.DaemonStateKnown = known
	s.DaemonPID = pid
	s.DaemonMode = mode
	s.DaemonAlive = identity.AliveContext(ctx, pid)
	return s
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
		if src.RepositoryID != "" {
			dst.RepositoryID = src.RepositoryID
		}
		if src.WorktreeID != "" {
			dst.WorktreeID = src.WorktreeID
		}
		if src.CommonDir != "" {
			dst.CommonDir = src.CommonDir
		}
		if src.LegacyRepoHash != "" {
			dst.LegacyRepoHash = src.LegacyRepoHash
		}
		if shouldReplaceStateDB(dst.StateDB, src.StateDB) {
			dst.StateDB = src.StateDB
		}
		dst.LastSeenTS = src.LastSeenTS
	} else if shouldReplaceStateDB(dst.StateDB, src.StateDB) {
		dst.StateDB = src.StateDB
	}
	for _, h := range src.Harnesses {
		dst.Harnesses = addHarness(dst.Harnesses, h)
	}
	mergeRepoLifecycle(dst, src)
}

func mergeRepoLifecycle(dst *RepoRecord, src RepoRecord) {
	if dst == nil {
		return
	}
	if src.LifecycleState != RepoLifecycleDisabled {
		return
	}
	dst.LifecycleState = RepoLifecycleDisabled
	if src.LifecycleUpdatedTS > dst.LifecycleUpdatedTS {
		dst.LifecycleUpdatedTS = src.LifecycleUpdatedTS
	}
}

func shouldReplaceStateDB(dst, src string) bool {
	if src == "" {
		return false
	}
	if dst == "" {
		return true
	}
	dstExists := fileExists(dst)
	srcExists := fileExists(src)
	if dstExists != srcExists {
		return srcExists
	}
	return true
}

// CleanupLegacyDuplicates merges legacy registry rows that identify the same
// repo by Git toplevel, or that point at the same per-repo state DB. Callers
// should run this under WithLock so cleanup, pruning, and Save are atomic.
func (r *Registry) CleanupLegacyDuplicates(ctx context.Context) ([]LegacyDuplicateChange, error) {
	if r == nil || len(r.Repos) < 2 {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// canonicalGitRoot spawns `git rev-parse --show-toplevel` per row.
	// CleanupLegacyDuplicates runs under registry WithLock so writers
	// block until it completes; parallelize the git probes with a
	// bounded worker pool to keep critical-section time near a single
	// subprocess regardless of registry size.
	infos := make([]legacyRepoInfo, len(r.Repos))
	workers := runtime.GOMAXPROCS(0)
	if workers > 8 {
		workers = 8
	}
	if workers < 1 {
		workers = 1
	}
	if len(r.Repos) < workers {
		workers = len(r.Repos)
	}
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i, rec := range r.Repos {
		i, rec := i, rec
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			infos[i] = legacyRepoInfo{
				Record:  rec,
				Root:    canonicalGitRoot(ctx, rec.Path),
				StateDB: canonicalStateDB(rec.StateDB),
			}
		}()
	}
	wg.Wait()

	out := make([]legacyRepoInfo, 0, len(infos))
	changes := make([]LegacyDuplicateChange, 0)
	for _, info := range infos {
		merged := false
		for i := range out {
			if reason, same := legacyDuplicateReason(out[i], info); same {
				before := out[i].Record.Path
				mergeRepoRecord(&out[i].Record, info.Record)
				if out[i].Root == "" && info.Root != "" {
					out[i].Root = info.Root
				}
				if out[i].StateDB == "" && info.StateDB != "" {
					out[i].StateDB = info.StateDB
				}
				if out[i].Root != "" {
					out[i].Record.Path = out[i].Root
				}
				changes = append(changes, LegacyDuplicateChange{KeptPath: out[i].Record.Path, DroppedPath: info.Record.Path, Reason: reason})
				if before != out[i].Record.Path {
					changes[len(changes)-1].KeptPath = out[i].Record.Path
				}
				merged = true
				break
			}
		}
		if !merged {
			if info.Root != "" {
				info.Record.Path = info.Root
			}
			out = append(out, info)
		}
	}
	if len(changes) == 0 {
		return nil, nil
	}
	r.Repos = make([]RepoRecord, 0, len(out))
	for _, info := range out {
		r.Repos = append(r.Repos, info.Record)
	}
	return changes, nil
}

type legacyRepoInfo struct {
	Record  RepoRecord
	Root    string
	StateDB string
}

func legacyDuplicateReason(a, b legacyRepoInfo) (string, bool) {
	if a.Root != "" && b.Root != "" && SameRepoPath(a.Root, b.Root) {
		return "same-git-toplevel", true
	}
	if a.StateDB != "" && b.StateDB != "" && sameCleanPath(a.StateDB, b.StateDB) {
		return "same-state-db", true
	}
	return "", false
}

func sameCleanPath(a, b string) bool {
	if a == "" || b == "" {
		return a == b
	}
	cleanA := filepath.Clean(a)
	cleanB := filepath.Clean(b)
	if cleanA == cleanB {
		return true
	}
	if pathCaseFoldedByDefault() {
		return strings.EqualFold(cleanA, cleanB)
	}
	return false
}

func canonicalGitRoot(ctx context.Context, path string) string {
	if path == "" {
		return ""
	}
	wt, err := git.ResolveWorktree(ctx, path)
	if err != nil {
		return ""
	}
	return wt.Root
}

func canonicalStateDB(path string) string {
	if path == "" {
		return ""
	}
	abs, err := filepath.Abs(path)
	if err == nil {
		path = abs
	}
	path = filepath.Clean(path)
	if realPath, err := filepath.EvalSymlinks(path); err == nil {
		path = filepath.Clean(realPath)
	}
	return path
}

func readDaemonStateReadOnly(ctx context.Context, dbPath string) (pid int, mode string, known bool, err error) {
	st, known, err := state.LoadDaemonStateReadOnly(ctx, dbPath)
	if err != nil {
		return 0, "", false, err
	}
	return st.PID, st.Mode, known, nil
}

func fileExists(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
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
	if errA == nil && errB == nil {
		return os.SameFile(infoA, infoB)
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
