package central

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// rootsForTest returns a paths.Roots whose Share dir lives under t.TempDir(),
// isolating registry.json across tests so they can run in parallel.
func rootsForTest(t *testing.T) paths.Roots {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", "")
	t.Setenv("XDG_DATA_HOME", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	r, err := paths.Resolve()
	if err != nil {
		t.Fatalf("paths.Resolve: %v", err)
	}
	return r
}

func TestRegistry_RoundTrip(t *testing.T) {
	roots := rootsForTest(t)

	want := NewRegistry()
	want.UpsertRepo("/tmp/repo-A", "aaaa1111", "/tmp/repo-A/.git/acd/state.db", "claude-code", 100)
	want.UpsertRepo("/tmp/repo-B", "bbbb2222", "/tmp/repo-B/.git/acd/state.db", "codex", 200)
	want.DisableRepo(RepoRemovalTarget{Path: "/tmp/repo-B"}, 250)

	if err := Save(roots, want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := Load(roots)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("round-trip mismatch:\n want=%+v\n  got=%+v", want, got)
	}
}

func TestRegistry_LoadMissingReturnsEmpty(t *testing.T) {
	roots := rootsForTest(t)

	reg, err := Load(roots)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if reg.Version != RegistryVersion {
		t.Fatalf("version = %d, want %d", reg.Version, RegistryVersion)
	}
	if len(reg.Repos) != 0 {
		t.Fatalf("repos = %d, want 0", len(reg.Repos))
	}
}

func TestRegistry_RejectFutureVersion(t *testing.T) {
	roots := rootsForTest(t)

	if err := os.MkdirAll(roots.Share, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	body := []byte(`{"version": 99, "repos": []}`)
	if err := os.WriteFile(roots.RegistryPath(), body, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, err := Load(roots); err == nil {
		t.Fatal("Load with version=99 should error")
	}
}

func TestRegistry_UpsertIdempotent(t *testing.T) {
	reg := NewRegistry()
	reg.UpsertRepo("/tmp/x", "h1", "/tmp/x/.git/acd/state.db", "claude-code", 10)
	reg.UpsertRepo("/tmp/x", "h1", "/tmp/x/.git/acd/state.db", "claude-code", 20)
	if len(reg.Repos) != 1 {
		t.Fatalf("repos=%d, want 1", len(reg.Repos))
	}
	if reg.Repos[0].LastSeenTS != 20 {
		t.Fatalf("last_seen_ts=%d, want 20", reg.Repos[0].LastSeenTS)
	}
	if reg.Repos[0].FirstRegisteredTS != 10 {
		t.Fatalf("first_registered_ts=%d, want 10", reg.Repos[0].FirstRegisteredTS)
	}
	if !reflect.DeepEqual(reg.Repos[0].Harnesses, []string{"claude-code"}) {
		t.Fatalf("harnesses=%v, want [claude-code]", reg.Repos[0].Harnesses)
	}
}

func TestRegistry_LoadOldRowsDefaultToEnabled(t *testing.T) {
	roots := rootsForTest(t)

	if err := os.MkdirAll(roots.Share, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	body := []byte(`{"version":1,"repos":[{"path":"/tmp/repo","repo_hash":"h1","state_db":"/tmp/repo/.git/acd/state.db","first_registered_ts":10,"last_seen_ts":20,"harnesses":["codex"]}]}`)
	if err := os.WriteFile(roots.RegistryPath(), body, 0o600); err != nil {
		t.Fatalf("write registry: %v", err)
	}

	reg, err := Load(roots)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(reg.Repos) != 1 {
		t.Fatalf("repos=%d, want 1", len(reg.Repos))
	}
	rec := reg.Repos[0]
	if rec.LifecycleStateName() != RepoLifecycleEnabled || rec.LifecycleUpdatedTS != 0 {
		t.Fatalf("lifecycle=%+v, want enabled old row", rec)
	}
	if reg.Version != RegistryVersion {
		t.Fatalf("version=%d, want %d", reg.Version, RegistryVersion)
	}
}

func TestRegistry_DisableEnableRepoLifecycle(t *testing.T) {
	reg := NewRegistry()
	reg.UpsertRepo("/tmp/repo", "h1", "/tmp/repo/.git/acd/state.db", "codex", 10)

	disabled := reg.DisableRepo(RepoRemovalTarget{Path: "/tmp/repo"}, 30)
	if disabled.NotFound || !disabled.Updated {
		t.Fatalf("disable result=%+v, want updated", disabled)
	}
	if !disabled.Record.LifecycleDisabled() || disabled.Record.LifecycleUpdatedTS != 30 {
		t.Fatalf("disabled record=%+v, want disabled at 30", disabled.Record)
	}
	rec := reg.Repos[0]
	if rec.FirstRegisteredTS != 10 || rec.LastSeenTS != 10 || rec.RepoHash != "h1" || rec.StateDB != "/tmp/repo/.git/acd/state.db" {
		t.Fatalf("non-lifecycle metadata changed: %+v", rec)
	}

	again := reg.DisableRepo(RepoRemovalTarget{StateDB: "/tmp/repo/.git/acd/state.db"}, 30)
	if again.NotFound || again.Updated {
		t.Fatalf("idempotent disable=%+v, want no update", again)
	}

	enabled := reg.EnableRepo(RepoRemovalTarget{Path: "/tmp/repo"}, 40)
	if enabled.NotFound || !enabled.Updated {
		t.Fatalf("enable result=%+v, want updated", enabled)
	}
	if enabled.Record.LifecycleStateName() != RepoLifecycleEnabled || enabled.Record.LifecycleUpdatedTS != 40 {
		t.Fatalf("enabled record=%+v, want lifecycle cleared", enabled.Record)
	}
	enabledAgain := reg.EnableRepo(RepoRemovalTarget{Path: "/tmp/repo"}, 50)
	if enabledAgain.NotFound || enabledAgain.Updated {
		t.Fatalf("idempotent enable=%+v, want no update", enabledAgain)
	}

	missing := reg.DisableRepo(RepoRemovalTarget{Path: "/tmp/missing"}, 50)
	if !missing.NotFound || missing.Updated {
		t.Fatalf("missing result=%+v, want not-found no-op", missing)
	}
}

func TestRegistry_UpsertPreservesDisabledState(t *testing.T) {
	reg := NewRegistry()
	reg.UpsertRepo("/tmp/repo", "old", "/tmp/repo/.git/acd/state.db", "codex", 10)
	reg.DisableRepo(RepoRemovalTarget{Path: "/tmp/repo"}, 25)

	reg.UpsertRepo("/tmp/repo", "new", "/tmp/repo/.git/acd/state.db", "pi", 40)

	if len(reg.Repos) != 1 {
		t.Fatalf("repos=%d, want 1", len(reg.Repos))
	}
	rec := reg.Repos[0]
	if !rec.LifecycleDisabled() || rec.LifecycleUpdatedTS != 25 {
		t.Fatalf("lifecycle=%+v, want disabled preserved", rec)
	}
	if rec.RepoHash != "new" || rec.LastSeenTS != 40 {
		t.Fatalf("refresh metadata=%+v, want updated hash/last_seen", rec)
	}
	wantHarnesses := []string{"codex", "pi"}
	if !reflect.DeepEqual(rec.Harnesses, wantHarnesses) {
		t.Fatalf("harnesses=%v, want %v", rec.Harnesses, wantHarnesses)
	}
}

func TestRegistry_RegisterResolvedRepoPreservesDisabledState(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	if err := git.Init(ctx, repo); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref HEAD: %v", err)
	}
	wt, err := git.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatalf("resolve worktree: %v", err)
	}

	reg := NewRegistry()
	if _, err := reg.RegisterResolvedRepo(wt, "codex", 10); err != nil {
		t.Fatalf("first register: %v", err)
	}
	reg.DisableRepo(RepoRemovalTarget{Path: wt.Root}, 25)

	refreshed, err := reg.RegisterResolvedRepo(wt, "pi", 40)
	if err != nil {
		t.Fatalf("refresh register: %v", err)
	}
	if refreshed.Inserted || !refreshed.Refreshed {
		t.Fatalf("refresh result=%+v, want refreshed", refreshed)
	}
	if !refreshed.Record.LifecycleDisabled() || refreshed.Record.LifecycleUpdatedTS != 25 {
		t.Fatalf("lifecycle=%+v, want disabled preserved", refreshed.Record)
	}
	if refreshed.Record.LastSeenTS != 40 {
		t.Fatalf("last_seen=%d, want 40", refreshed.Record.LastSeenTS)
	}
}

func TestRegistry_NormalizeDisabledWinsWhenMergingDuplicates(t *testing.T) {
	reg := NewRegistry()
	reg.Repos = []RepoRecord{
		{Path: "/tmp/repo", RepoHash: "old", StateDB: "/tmp/repo/.git/acd/state-old.db", FirstRegisteredTS: 10, LastSeenTS: 20, Harnesses: []string{"codex"}, LifecycleState: RepoLifecycleDisabled, LifecycleUpdatedTS: 30},
		{Path: "/tmp/repo", RepoHash: "new", StateDB: "/tmp/repo/.git/acd/state.db", FirstRegisteredTS: 15, LastSeenTS: 40, Harnesses: []string{"pi"}},
	}

	reg.Normalize()

	if len(reg.Repos) != 1 {
		t.Fatalf("repos=%d, want 1", len(reg.Repos))
	}
	rec := reg.Repos[0]
	if !rec.LifecycleDisabled() || rec.LifecycleUpdatedTS != 30 {
		t.Fatalf("lifecycle=%+v, want disabled winner", rec)
	}
	if rec.RepoHash != "new" || rec.LastSeenTS != 40 {
		t.Fatalf("metadata=%+v, want newest non-lifecycle fields", rec)
	}
}

func TestRegistry_UpsertMergesLegacySubdirRowByStateDB(t *testing.T) {
	reg := NewRegistry()
	reg.Repos = []RepoRecord{{
		Path:              "/tmp/repo/subdir",
		RepoHash:          "old",
		StateDB:           "/tmp/repo/.git/acd/state.db",
		FirstRegisteredTS: 10,
		LastSeenTS:        20,
		Harnesses:         []string{"codex"},
		LifecycleState:    RepoLifecycleDisabled,
		LifecycleUpdatedTS: 25,
	}}

	reg.UpsertRepo("/tmp/repo", "new", "/tmp/repo/.git/acd/state.db", "pi", 30)

	if len(reg.Repos) != 1 {
		t.Fatalf("repos=%d, want 1: %+v", len(reg.Repos), reg.Repos)
	}
	rec := reg.Repos[0]
	if rec.Path != "/tmp/repo" || rec.RepoHash != "new" || rec.StateDB != "/tmp/repo/.git/acd/state.db" {
		t.Fatalf("record=%+v, want canonical path/hash/state", rec)
	}
	if rec.FirstRegisteredTS != 10 || rec.LastSeenTS != 30 {
		t.Fatalf("timestamps=%d/%d want 10/30", rec.FirstRegisteredTS, rec.LastSeenTS)
	}
	if !rec.LifecycleDisabled() || rec.LifecycleUpdatedTS != 25 {
		t.Fatalf("lifecycle=%+v, want disabled preserved", rec)
	}
	wantHarnesses := []string{"codex", "pi"}
	if !reflect.DeepEqual(rec.Harnesses, wantHarnesses) {
		t.Fatalf("harnesses=%v, want %v", rec.Harnesses, wantHarnesses)
	}
}

func TestRegistry_NormalizesCaseDuplicateOnCaseFoldedPlatforms(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "windows" {
		t.Skip("case-folded registry path matching is only enabled on darwin/windows")
	}
	reg := NewRegistry()
	reg.UpsertRepo("/Users/me/NotesAssistant", "h1", "/state/one.db", "codex", 10)
	reg.UpsertRepo("/Users/me/notesassistant", "h2", "/state/two.db", "claude-code", 20)

	if len(reg.Repos) != 1 {
		t.Fatalf("repos=%d, want 1", len(reg.Repos))
	}
	rec := reg.Repos[0]
	if rec.Path != "/Users/me/notesassistant" {
		t.Fatalf("path=%q, want latest path spelling", rec.Path)
	}
	if rec.RepoHash != "h2" || rec.StateDB != "/state/two.db" {
		t.Fatalf("metadata not refreshed: %+v", rec)
	}
	wantHarnesses := []string{"claude-code", "codex"}
	if !reflect.DeepEqual(rec.Harnesses, wantHarnesses) {
		t.Fatalf("harnesses=%v, want %v", rec.Harnesses, wantHarnesses)
	}
	if rec.FirstRegisteredTS != 10 || rec.LastSeenTS != 20 {
		t.Fatalf("timestamps not merged: %+v", rec)
	}
}

func TestRegistry_UpsertHarnessesDedupAndSort(t *testing.T) {
	reg := NewRegistry()
	reg.UpsertRepo("/tmp/y", "h2", "sd", "codex", 1)
	reg.UpsertRepo("/tmp/y", "h2", "sd", "claude-code", 2)
	reg.UpsertRepo("/tmp/y", "h2", "sd", "codex", 3) // dup
	reg.UpsertRepo("/tmp/y", "h2", "sd", "pi", 4)

	got := reg.Repos[0].Harnesses
	want := []string{"claude-code", "codex", "pi"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("harnesses=%v, want %v", got, want)
	}
}

func TestRegistry_RegisterResolvedRepoRefreshPreservesFirstTimestamp(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	if err := git.Init(ctx, repo); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref HEAD: %v", err)
	}
	wt, err := git.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatalf("resolve worktree: %v", err)
	}
	wantHash, err := paths.RepoHash(wt.Root)
	if err != nil {
		t.Fatalf("RepoHash: %v", err)
	}
	wantStateDB := state.DBPathFromGitDir(wt.GitDir)

	reg := NewRegistry()
	first, err := reg.RegisterResolvedRepo(wt, "codex", 10)
	if err != nil {
		t.Fatalf("first register: %v", err)
	}
	if !first.Inserted || first.Refreshed {
		t.Fatalf("first result=%+v, want inserted only", first)
	}
	second, err := reg.RegisterResolvedRepo(wt, "pi", 30)
	if err != nil {
		t.Fatalf("second register: %v", err)
	}
	if second.Inserted || !second.Refreshed {
		t.Fatalf("second result=%+v, want refreshed only", second)
	}
	if len(reg.Repos) != 1 {
		t.Fatalf("repos=%d, want 1", len(reg.Repos))
	}
	rec := second.Record
	if rec.Path != wt.Root || rec.RepoHash != wantHash || rec.StateDB != wantStateDB {
		t.Fatalf("record=%+v, want resolved worktree metadata", rec)
	}
	if rec.FirstRegisteredTS != 10 || rec.LastSeenTS != 30 {
		t.Fatalf("timestamps=%d/%d want 10/30", rec.FirstRegisteredTS, rec.LastSeenTS)
	}
	wantHarnesses := []string{"codex", "pi"}
	if !reflect.DeepEqual(rec.Harnesses, wantHarnesses) {
		t.Fatalf("harnesses=%v, want %v", rec.Harnesses, wantHarnesses)
	}
}

func TestRegistry_RemoveRepoByPathReturnsSafetyMetadata(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	if err := git.Init(ctx, repo); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref HEAD: %v", err)
	}
	wt, err := git.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatalf("resolve worktree: %v", err)
	}
	dbPath := state.DBPathFromGitDir(wt.GitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{PID: os.Getpid(), Mode: "running"}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}

	reg := NewRegistry()
	reg.UpsertRepo(wt.Root, "repo-hash", dbPath, "codex", 10)
	reg.UpsertRepo("/tmp/other", "other-hash", "/tmp/other/.git/acd/state.db", "pi", 20)

	res := reg.RemoveRepoByPath(ctx, wt.Root)
	if !res.Removed || res.NotFound {
		t.Fatalf("result=%+v, want removed", res)
	}
	if res.RemovedRecord == nil {
		t.Fatal("removed record is nil")
	}
	if res.RemovedRecord.Path != wt.Root || res.RemovedRecord.StateDB != dbPath {
		t.Fatalf("removed=%+v, want target row", res.RemovedRecord)
	}
	if len(reg.Repos) != 1 || reg.Repos[0].Path != "/tmp/other" {
		t.Fatalf("remaining repos=%+v, want only other row", reg.Repos)
	}
	safety := res.Safety
	if safety.Path != wt.Root || safety.StateDB != dbPath || safety.GitDir != wt.GitDir {
		t.Fatalf("safety paths=%+v, want target metadata", safety)
	}
	if safety.StateDir != filepath.Join(wt.GitDir, "acd") || safety.StartCacheDir != filepath.Join(wt.GitDir, "acd") {
		t.Fatalf("state/cache dirs=%q/%q, want git acd dir", safety.StateDir, safety.StartCacheDir)
	}
	if !safety.StateDBExists || !safety.StateDirExists {
		t.Fatalf("state existence=%+v, want present", safety)
	}
	if !safety.DaemonStateKnown || safety.DaemonMode != "running" || safety.DaemonPID != os.Getpid() || !safety.DaemonAlive {
		t.Fatalf("daemon safety=%+v, want live current process", safety)
	}
	if safety.DaemonStateError != "" {
		t.Fatalf("daemon state error=%q", safety.DaemonStateError)
	}
}

func TestRegistry_RemoveRepoByStateDB(t *testing.T) {
	ctx := context.Background()
	reg := NewRegistry()
	stateDB := filepath.Join(t.TempDir(), "repo", ".git", "acd", "state.db")
	reg.Repos = []RepoRecord{
		{Path: "/tmp/repo/subdir", RepoHash: "legacy", StateDB: stateDB, FirstRegisteredTS: 10, LastSeenTS: 20, Harnesses: []string{"codex"}},
		{Path: "/tmp/other", RepoHash: "other", StateDB: "/tmp/other/.git/acd/state.db", FirstRegisteredTS: 30, LastSeenTS: 40, Harnesses: []string{"pi"}},
	}

	res := reg.RemoveRepoByStateDB(ctx, filepath.Join(filepath.Dir(stateDB), ".", filepath.Base(stateDB)))
	if !res.Removed || res.NotFound {
		t.Fatalf("result=%+v, want removed", res)
	}
	if res.RemovedRecord == nil {
		t.Fatal("removed record is nil")
	}
	if res.RemovedRecord.RepoHash != "legacy" {
		t.Fatalf("removed=%+v, want legacy row", res.RemovedRecord)
	}
	if len(reg.Repos) != 1 || reg.Repos[0].RepoHash != "other" {
		t.Fatalf("remaining repos=%+v, want other row", reg.Repos)
	}
	if res.Safety.StateDB != stateDB || res.Safety.StateDBExists {
		t.Fatalf("safety=%+v, want missing target state DB metadata", res.Safety)
	}
}

func TestRegistry_RemoveUnknownRepoIsStructuredNoOp(t *testing.T) {
	ctx := context.Background()
	reg := NewRegistry()
	reg.UpsertRepo("/tmp/known", "hash", "/tmp/known/.git/acd/state.db", "codex", 10)

	res := reg.RemoveRepoByPath(ctx, "/tmp/missing")
	if res.Removed || !res.NotFound {
		t.Fatalf("result=%+v, want not-found no-op", res)
	}
	if len(reg.Repos) != 1 || reg.Repos[0].Path != "/tmp/known" {
		t.Fatalf("repos=%+v, want unchanged", reg.Repos)
	}
	if res.Safety.Path != "/tmp/missing" {
		t.Fatalf("safety path=%q, want target", res.Safety.Path)
	}
}

func TestRegistry_CleanupLegacyDuplicatesMergesSubdirRowsByGitToplevel(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	if err := git.Init(ctx, repo); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref HEAD: %v", err)
	}
	wt, err := git.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatalf("resolve worktree: %v", err)
	}
	subdir := filepath.Join(wt.Root, "pkg", "sub")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}

	reg := NewRegistry()
	reg.Repos = []RepoRecord{
		{Path: wt.Root, RepoHash: "old", StateDB: filepath.Join(wt.GitDir, "acd", "state.db"), FirstRegisteredTS: 20, LastSeenTS: 30, Harnesses: []string{"codex"}, LifecycleState: RepoLifecycleDisabled, LifecycleUpdatedTS: 35},
		{Path: subdir, RepoHash: "new", StateDB: filepath.Join(wt.GitDir, "acd", "state.db"), FirstRegisteredTS: 10, LastSeenTS: 40, Harnesses: []string{"claude-code"}},
	}

	changes, err := reg.CleanupLegacyDuplicates(ctx)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if len(changes) != 1 || changes[0].Reason != "same-git-toplevel" {
		t.Fatalf("changes=%+v, want one same-git-toplevel merge", changes)
	}
	if len(reg.Repos) != 1 {
		t.Fatalf("repos=%d, want 1", len(reg.Repos))
	}
	rec := reg.Repos[0]
	if rec.Path != wt.Root {
		t.Fatalf("path=%q, want canonical root %q", rec.Path, wt.Root)
	}
	if rec.FirstRegisteredTS != 10 || rec.LastSeenTS != 40 {
		t.Fatalf("timestamps not merged: %+v", rec)
	}
	if !rec.LifecycleDisabled() || rec.LifecycleUpdatedTS != 35 {
		t.Fatalf("lifecycle not merged conservatively: %+v", rec)
	}
	wantHarnesses := []string{"claude-code", "codex"}
	if !reflect.DeepEqual(rec.Harnesses, wantHarnesses) {
		t.Fatalf("harnesses=%v, want %v", rec.Harnesses, wantHarnesses)
	}

	again, err := reg.CleanupLegacyDuplicates(ctx)
	if err != nil {
		t.Fatalf("cleanup again: %v", err)
	}
	if len(again) != 0 || len(reg.Repos) != 1 {
		t.Fatalf("cleanup not idempotent: changes=%+v repos=%+v", again, reg.Repos)
	}
}

// TestRegistry_ConcurrentWriters simulates 10 short-lived processes each
// adding a unique repo entry. The flock-guarded RMW must produce a final
// document with all 10 rows present and the JSON well-formed at every
// observation point.
func TestRegistry_ConcurrentWriters(t *testing.T) {
	roots := rootsForTest(t)

	const N = 10
	var wg sync.WaitGroup
	errs := make(chan error, N)

	// Start a sampling goroutine that tries to read the file mid-flight. It
	// must never see torn JSON: any non-empty file must parse cleanly. (A
	// missing file is fine — the writers may not have committed yet.)
	stopSampler := make(chan struct{})
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		path := roots.RegistryPath()
		for {
			select {
			case <-stopSampler:
				return
			default:
			}
			b, err := os.ReadFile(path)
			if err != nil {
				continue // missing OK
			}
			if len(b) == 0 {
				continue
			}
			var probe Registry
			if err := json.Unmarshal(b, &probe); err != nil {
				errs <- fmt.Errorf("torn JSON observed: %w", err)
				return
			}
		}
	}()

	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			path := fmt.Sprintf("/tmp/repo-%02d", i)
			hash := fmt.Sprintf("hash-%02d", i)
			if err := WithLock(roots, func(reg *Registry) error {
				reg.UpsertRepo(path, hash, path+"/.git/acd/state.db", "claude-code", int64(1000+i))
				return nil
			}); err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(stopSampler)
	<-samplerDone
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent error: %v", err)
	}

	got, err := Load(roots)
	if err != nil {
		t.Fatalf("final Load: %v", err)
	}
	if len(got.Repos) != N {
		t.Fatalf("repos=%d, want %d", len(got.Repos), N)
	}

	// Verify each unique path appeared exactly once.
	seen := make(map[string]int)
	for _, r := range got.Repos {
		seen[r.Path]++
	}
	if len(seen) != N {
		t.Fatalf("unique paths=%d, want %d", len(seen), N)
	}
	for k, v := range seen {
		if v != 1 {
			t.Fatalf("path %q appears %d times, want 1", k, v)
		}
	}
}

func TestRegistry_AtomicWriteSurvivesPartial(t *testing.T) {
	// If a writer crashes between truncating .tmp and renaming, the live
	// registry.json must remain the previous good content. We simulate this
	// by pre-populating registry.json with a known-good document, then
	// dropping a half-written .tmp into the share dir, and verifying that
	// Load still returns the original.
	roots := rootsForTest(t)

	// Seed a good document.
	good := NewRegistry()
	good.UpsertRepo("/tmp/seed", "seed-hash", "/tmp/seed/.git/acd/state.db", "claude-code", 42)
	if err := Save(roots, good); err != nil {
		t.Fatalf("Save seed: %v", err)
	}

	// Drop a torn .tmp next to it (simulating a crashed writer mid-stream).
	tmp := roots.RegistryPath() + ".tmp"
	if err := os.WriteFile(tmp, []byte(`{"version": 1, "repos": [`), 0o600); err != nil {
		t.Fatalf("seed tmp: %v", err)
	}

	// Load must still see the good document — the tmp was never renamed.
	got, err := Load(roots)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got.Repos) != 1 || got.Repos[0].Path != "/tmp/seed" {
		t.Fatalf("Load returned %+v, want seed only", got.Repos)
	}

	// And a fresh WithLock on top of the partial state must still succeed,
	// overwriting the live file (the .tmp gets clobbered when the next
	// writer opens it with O_TRUNC).
	if err := WithLock(roots, func(r *Registry) error {
		r.UpsertRepo("/tmp/another", "another-hash", "sd", "codex", 99)
		return nil
	}); err != nil {
		t.Fatalf("WithLock: %v", err)
	}

	got, err = Load(roots)
	if err != nil {
		t.Fatalf("Load after recover: %v", err)
	}
	gotPaths := make([]string, 0, len(got.Repos))
	for _, r := range got.Repos {
		gotPaths = append(gotPaths, r.Path)
	}
	sort.Strings(gotPaths)
	want := []string{"/tmp/another", "/tmp/seed"}
	if !reflect.DeepEqual(gotPaths, want) {
		t.Fatalf("paths=%v, want %v", gotPaths, want)
	}
}

func TestRegistry_FilePermissionsAndLayout(t *testing.T) {
	roots := rootsForTest(t)

	if err := WithLock(roots, func(r *Registry) error {
		r.UpsertRepo("/tmp/p", "ph", "sd", "claude-code", 1)
		return nil
	}); err != nil {
		t.Fatalf("WithLock: %v", err)
	}

	info, err := os.Stat(roots.Share)
	if err != nil {
		t.Fatalf("stat share: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("share perms=%o, want 0700", info.Mode().Perm())
	}
	if got, err := os.Stat(filepath.Join(roots.Share, "registry.json")); err != nil {
		t.Fatalf("stat registry: %v", err)
	} else if got.Mode().Perm() != 0o600 {
		t.Fatalf("registry perms=%o, want 0600", got.Mode().Perm())
	}
}
