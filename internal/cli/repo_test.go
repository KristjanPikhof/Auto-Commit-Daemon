package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRepoHelpIncludesLifecycleCommands(t *testing.T) {
	help := commandHelp(t, "repo")
	for _, want := range []string{
		"acd repo init",
		"acd repo disable --repo /path/to/repo",
		"acd repo enable --repo /path/to/repo --json",
		"acd repo list --json",
		"acd repo remove --yes --purge-state",
		"Manage explicit acd repository registration",
		"repo_lifecycle.autodiscovery",
		"ACD_REPO_AUTODISCOVERY=disabled",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("repo help missing %q:\n%s", want, help)
		}
	}
}

func TestRepoDisable_StopsClearsCachesPreservesState(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	_ = db.Close()
	registerRepo(t, roots, repo, stateDB, "")
	gitDir := filepath.Dir(filepath.Dir(stateDB))
	cachePath := startCachePath(gitDir, "repo-disable-test")
	if err := os.WriteFile(cachePath, []byte("{}"), 0o600); err != nil {
		t.Fatalf("write cache: %v", err)
	}

	stops := 0
	prev := repoDisableStopOneRepo
	repoDisableStopOneRepo = func(ctx context.Context, repo, sessionID string, force bool) (stopRepoResult, error) {
		stops++
		if !force {
			t.Fatalf("repo disable should force stop live daemon")
		}
		return stopRepoResult{Repo: repo, Stopped: true, Force: true, DaemonPID: os.Getpid()}, nil
	}
	t.Cleanup(func() { repoDisableStopOneRepo = prev })

	var out bytes.Buffer
	if err := runRepoDisable(ctx, &out, repo, true); err != nil {
		t.Fatalf("runRepoDisable: %v\n%s", err, out.String())
	}
	if stops != 1 {
		t.Fatalf("stop calls=%d want 1", stops)
	}
	var got repoLifecycleCommandResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("decode disable json: %v\n%s", err, out.String())
	}
	if got.Action != "disable" || !got.Updated || !got.StatePreserved || !got.StartCachesCleared {
		t.Fatalf("unexpected disable result: %+v", got)
	}
	if got.Stopped == nil || !got.Stopped.Stopped {
		t.Fatalf("missing stopped result: %+v", got)
	}
	if fileExists(cachePath) {
		t.Fatalf("start cache was not cleared: %s", cachePath)
	}
	if !fileExists(stateDB) {
		t.Fatalf("state db should be preserved")
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	rec, ok := reg.FindRepo(repo, stateDB)
	if !ok || !rec.LifecycleDisabled() {
		t.Fatalf("registry row not disabled: ok=%v rec=%+v", ok, rec)
	}
}

func TestRepoDisable_IdempotentAlreadyDisabled(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	_ = db.Close()
	registerRepo(t, roots, repo, stateDB, "")
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		res := reg.DisableRepo(central.RepoRemovalTarget{Path: repo}, 42)
		if res.NotFound {
			t.Fatalf("seed disable not found")
		}
		return nil
	}); err != nil {
		t.Fatalf("seed disable: %v", err)
	}

	var out bytes.Buffer
	if err := runRepoDisable(ctx, &out, repo, false); err != nil {
		t.Fatalf("runRepoDisable: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "already disabled "+repo) || !strings.Contains(out.String(), "state: preserved "+stateDB) {
		t.Fatalf("unexpected human output:\n%s", out.String())
	}
}

func TestRepoEnable_ClearsDisabledWithoutStartingDaemon(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	_ = db.Close()
	registerRepo(t, roots, repo, stateDB, "")
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.DisableRepo(central.RepoRemovalTarget{Path: repo}, 42)
		return nil
	}); err != nil {
		t.Fatalf("seed disable: %v", err)
	}

	var out bytes.Buffer
	if err := runRepoEnable(ctx, &out, repo, true); err != nil {
		t.Fatalf("runRepoEnable: %v\n%s", err, out.String())
	}
	var got repoLifecycleCommandResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("decode enable json: %v\n%s", err, out.String())
	}
	if got.Action != "enable" || !got.Updated || got.Stopped != nil || got.StartCachesCleared {
		t.Fatalf("unexpected enable result: %+v", got)
	}
	if !fileExists(stateDB) {
		t.Fatalf("state db should be preserved")
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	rec, ok := reg.FindRepo(repo, stateDB)
	if !ok || rec.LifecycleDisabled() {
		t.Fatalf("registry row not enabled: ok=%v rec=%+v", ok, rec)
	}
}

func TestRepoEnable_IdempotentAlreadyEnabled(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	_ = db.Close()
	registerRepo(t, roots, repo, stateDB, "")

	var out bytes.Buffer
	if err := runRepoEnable(ctx, &out, repo, false); err != nil {
		t.Fatalf("runRepoEnable: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "already enabled "+repo) || !strings.Contains(out.String(), "state: preserved "+stateDB) {
		t.Fatalf("unexpected human output:\n%s", out.String())
	}
}

func TestRepoLifecycle_UnknownRepoDoesNotCreateState(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo := initRepoForRepoLifecycle(t)
	wt, err := git.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatalf("resolve worktree: %v", err)
	}
	stateDB := state.DBPathFromGitDir(wt.GitDir)
	if fileExists(stateDB) {
		t.Fatalf("test setup unexpectedly created state db: %s", stateDB)
	}

	var out bytes.Buffer
	err = runRepoDisable(ctx, &out, repo, true)
	if err == nil {
		t.Fatalf("runRepoDisable unknown succeeded")
	}
	for _, want := range []string{"not registered", "acd repo init --repo " + repo, "acd repo list"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("unknown repo error missing %q: %v", want, err)
		}
	}
	if fileExists(stateDB) {
		t.Fatalf("unknown disable created state db: %s", stateDB)
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if len(reg.Repos) != 0 {
		t.Fatalf("unknown disable created registry row: %+v", reg.Repos)
	}
}

func TestRepoInit_JSONFromSubdir(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo := initRepoForRepoLifecycle(t)
	subdir := filepath.Join(repo, "nested", "deeper")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}

	var out bytes.Buffer
	if err := runRepoInit(ctx, &out, subdir, true); err != nil {
		t.Fatalf("runRepoInit: %v", err)
	}
	var got repoInitResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("decode init json: %v\n%s", err, out.String())
	}
	if got.Repo != repo {
		t.Fatalf("repo=%q want %q", got.Repo, repo)
	}
	if !got.Inserted || got.Refreshed {
		t.Fatalf("insert/refreshed mismatch: %+v", got)
	}
	if got.BranchRef != "refs/heads/main" {
		t.Fatalf("branch_ref=%q", got.BranchRef)
	}
	if got.ConfigPath != roots.ConfigPath() {
		t.Fatalf("config_path=%q want %q", got.ConfigPath, roots.ConfigPath())
	}
	if !fileExists(got.StateDB) {
		t.Fatalf("state db was not created: %s", got.StateDB)
	}

	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if _, ok := reg.FindRepo(repo, got.StateDB); !ok {
		t.Fatalf("registry missing initialized repo: %+v", reg.Repos)
	}
}

func TestRepoInit_IdempotentAlreadyRegistered(t *testing.T) {
	withIsolatedHome(t)
	ctx := context.Background()
	repo := initRepoForRepoLifecycle(t)

	var first, second bytes.Buffer
	if err := runRepoInit(ctx, &first, repo, true); err != nil {
		t.Fatalf("first init: %v", err)
	}
	if err := runRepoInit(ctx, &second, repo, true); err != nil {
		t.Fatalf("second init: %v", err)
	}
	var got repoInitResult
	if err := json.Unmarshal(second.Bytes(), &got); err != nil {
		t.Fatalf("decode second init: %v\n%s", err, second.String())
	}
	if got.Inserted || !got.Refreshed {
		t.Fatalf("second init should refresh existing row: %+v", got)
	}
}

func TestRepoInit_NonGitDirFails(t *testing.T) {
	withIsolatedHome(t)
	var out bytes.Buffer
	err := runRepoInit(context.Background(), &out, t.TempDir(), true)
	if err == nil {
		t.Fatalf("runRepoInit in non-git dir succeeded")
	}
	if !strings.Contains(err.Error(), "not inside a Git worktree") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRepoRemove_DryRunPreservesRegistryAndState(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	_ = db.Close()
	registerRepo(t, roots, repo, stateDB, "")

	var out bytes.Buffer
	if err := runRepoRemove(ctx, &out, repo, false, false, false, true); err != nil {
		t.Fatalf("runRepoRemove dry-run: %v", err)
	}
	var got repoRemoveResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("decode dry-run: %v\n%s", err, out.String())
	}
	if !got.DryRun || !got.Removed || got.NotFound {
		t.Fatalf("unexpected dry-run result: %+v", got)
	}
	if !fileExists(stateDB) {
		t.Fatalf("dry-run removed state db")
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if _, ok := reg.FindRepo(repo, stateDB); !ok {
		t.Fatalf("dry-run removed registry row")
	}
}

func TestRepoRemove_YesStopsClearsCachesAndRemovesRegistry(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	_ = db.Close()
	registerRepo(t, roots, repo, stateDB, "")
	gitDir := filepath.Dir(filepath.Dir(stateDB))
	cachePath := startCachePath(gitDir, "repo-remove-test")
	if err := os.WriteFile(cachePath, []byte("{}"), 0o600); err != nil {
		t.Fatalf("write cache: %v", err)
	}

	stops := 0
	prev := repoRemoveStopOneRepo
	repoRemoveStopOneRepo = func(ctx context.Context, repo, sessionID string, force bool) (stopRepoResult, error) {
		stops++
		if !force {
			t.Fatalf("repo remove should force stop live daemon")
		}
		return stopRepoResult{Repo: repo, Stopped: true, Force: true, DaemonPID: os.Getpid()}, nil
	}
	t.Cleanup(func() { repoRemoveStopOneRepo = prev })

	var out bytes.Buffer
	if err := runRepoRemove(ctx, &out, repo, false, true, false, true); err != nil {
		t.Fatalf("runRepoRemove --yes: %v\n%s", err, out.String())
	}
	if stops != 1 {
		t.Fatalf("stop calls=%d want 1", stops)
	}
	var got repoRemoveResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("decode remove: %v\n%s", err, out.String())
	}
	if !got.Removed || got.DryRun || got.StatePurged || !got.StatePreserved {
		t.Fatalf("unexpected remove result: %+v", got)
	}
	if fileExists(cachePath) {
		t.Fatalf("start cache was not cleared: %s", cachePath)
	}
	if !fileExists(stateDB) {
		t.Fatalf("state db should be preserved by default")
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if _, ok := reg.FindRepo(repo, stateDB); ok {
		t.Fatalf("registry row still present after remove")
	}
}

func TestRepoRemove_PurgeStateDeletesAcdStateDir(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	_ = db.Close()
	registerRepo(t, roots, repo, stateDB, "")
	stateDir := filepath.Dir(stateDB)

	var out bytes.Buffer
	if err := runRepoRemove(ctx, &out, repo, false, true, true, true); err != nil {
		t.Fatalf("runRepoRemove --purge-state: %v\n%s", err, out.String())
	}
	var got repoRemoveResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("decode purge: %v\n%s", err, out.String())
	}
	if !got.Removed || !got.StatePurged || got.StatePreserved {
		t.Fatalf("unexpected purge result: %+v", got)
	}
	if fileExists(stateDir) {
		t.Fatalf("state dir still exists after purge: %s", stateDir)
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if _, ok := reg.FindRepo(repo, stateDB); ok {
		t.Fatalf("registry row still present after purge")
	}
}

func TestRepoRemove_MissingPathRegistryRow(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	missing := filepath.Join(t.TempDir(), "missing-repo")
	stateDB := filepath.Join(missing, ".git", "acd", "state.db")
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.UpsertRepo(missing, "missinghash", stateDB, "", 42)
		return nil
	}); err != nil {
		t.Fatalf("seed missing row: %v", err)
	}

	var out bytes.Buffer
	if err := runRepoRemove(ctx, &out, missing, false, true, false, true); err != nil {
		t.Fatalf("runRepoRemove missing --yes: %v\n%s", err, out.String())
	}
	var got repoRemoveResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("decode missing remove: %v\n%s", err, out.String())
	}
	if !got.Removed || got.NotFound {
		t.Fatalf("missing row was not removed: %+v", got)
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if _, ok := reg.FindRepo(missing, stateDB); ok {
		t.Fatalf("missing registry row still present")
	}
}

func TestRepoRemoveInteractive_RendersChoicesAndCancelPreservesRegistry(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	if err := state.RegisterClient(ctx, db, state.Client{
		SessionID: "interactive-client", Harness: "codex", LastSeenTS: nowFloat(),
	}); err != nil {
		t.Fatalf("register client: %v", err)
	}
	_ = db.Close()
	registerRepo(t, roots, repo, stateDB, "")

	var out bytes.Buffer
	if err := runRepoRemoveWithInput(ctx, &out, strings.NewReader("c\n"), "", false, false, false, false); err != nil {
		t.Fatalf("interactive cancel: %v\n%s", err, out.String())
	}
	for _, want := range []string{"REPO", "CLIENTS", "PENDING", "BLOCKED", "STATE_DB", repo, "canceled"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("interactive output missing %q:\n%s", want, out.String())
		}
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if _, ok := reg.FindRepo(repo, stateDB); !ok {
		t.Fatalf("cancel removed registry row")
	}
}

func TestRepoRemoveInteractive_ConfirmRemovesSelectedRow(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo1, stateDB1, db1 := makeRepoStateDB(t)
	_ = db1.Close()
	repo2, stateDB2, db2 := makeRepoStateDB(t)
	_ = db2.Close()
	registerRepo(t, roots, repo1, stateDB1, "")
	registerRepo(t, roots, repo2, stateDB2, "")

	var out bytes.Buffer
	if err := runRepoRemoveWithInput(ctx, &out, strings.NewReader("2\nremove\n"), "", false, false, false, false); err != nil {
		t.Fatalf("interactive remove: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "Preview:") || !strings.Contains(out.String(), "removed "+repo2) {
		t.Fatalf("interactive output missing preview/remove:\n%s", out.String())
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if _, ok := reg.FindRepo(repo1, stateDB1); !ok {
		t.Fatalf("unselected registry row was removed")
	}
	if _, ok := reg.FindRepo(repo2, stateDB2); ok {
		t.Fatalf("selected registry row still present")
	}
}

func TestRepoRemoveInteractive_PurgeStateNeedsExplicitConfirmation(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	_ = db.Close()
	registerRepo(t, roots, repo, stateDB, "")
	stateDir := filepath.Dir(stateDB)

	var out bytes.Buffer
	if err := runRepoRemoveWithInput(ctx, &out, strings.NewReader("1\nremove\nno\n"), "", false, false, true, false); err != nil {
		t.Fatalf("interactive purge cancel: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "purge requires explicit confirmation") {
		t.Fatalf("missing purge confirmation message:\n%s", out.String())
	}
	if !fileExists(stateDir) {
		t.Fatalf("state dir was purged without explicit purge confirmation")
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if _, ok := reg.FindRepo(repo, stateDB); !ok {
		t.Fatalf("registry row removed without explicit purge confirmation")
	}
}

func TestRepoRemoveInteractive_PurgeStateConfirmed(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	_ = db.Close()
	registerRepo(t, roots, repo, stateDB, "")
	stateDir := filepath.Dir(stateDB)

	var out bytes.Buffer
	if err := runRepoRemoveWithInput(ctx, &out, strings.NewReader("1\nremove\npurge\n"), "", false, false, true, false); err != nil {
		t.Fatalf("interactive purge confirm: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "state: purged "+stateDir) {
		t.Fatalf("missing purge result:\n%s", out.String())
	}
	if fileExists(stateDir) {
		t.Fatalf("state dir still exists after confirmed purge")
	}
}

func TestRepoRemove_JSONDryRunSkipsInteractiveInput(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	_ = db.Close()
	registerRepo(t, roots, repo, stateDB, "")

	var out bytes.Buffer
	if err := runRepoRemoveWithInput(ctx, &out, strings.NewReader("1\nremove\n"), repo, false, false, false, true); err != nil {
		t.Fatalf("json dry-run: %v\n%s", err, out.String())
	}
	var got repoRemoveResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("decode json dry-run: %v\n%s", err, out.String())
	}
	if !got.DryRun || !got.Removed || got.NotFound {
		t.Fatalf("unexpected json dry-run result: %+v", got)
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if _, ok := reg.FindRepo(repo, stateDB); !ok {
		t.Fatalf("json dry-run removed registry row")
	}
}

func TestRepoList_IncludesStoppedAndMissingRows(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{PID: 0, Mode: "stopped", HeartbeatTS: nowFloat()}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	_ = db.Close()
	registerRepo(t, roots, repo, stateDB, "")

	missing := filepath.Join(t.TempDir(), "missing-repo")
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.UpsertRepo(missing, "missinghash", filepath.Join(missing, ".git", "acd", "state.db"), "", 42)
		return nil
	}); err != nil {
		t.Fatalf("seed missing row: %v", err)
	}

	var out bytes.Buffer
	if err := runRepoList(ctx, &out, true); err != nil {
		t.Fatalf("runRepoList: %v", err)
	}
	var got struct {
		Repos []repoListEntry `json:"repos"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("decode repo list: %v\n%s", err, out.String())
	}
	if len(got.Repos) != 2 {
		t.Fatalf("repo list should include stopped and missing rows, got %d: %+v", len(got.Repos), got.Repos)
	}
	statuses := map[string]bool{}
	for _, entry := range got.Repos {
		statuses[entry.Status] = true
	}
	if !statuses["stopped"] || !statuses["repo-missing"] {
		t.Fatalf("missing expected statuses: %+v", got.Repos)
	}
}

func TestRepoList_HumanOutputOmitsStateDBColumn(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{PID: 0, Mode: "stopped", HeartbeatTS: nowFloat()}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	_ = db.Close()
	registerRepo(t, roots, repo, stateDB, "")

	var out bytes.Buffer
	if err := runRepoList(ctx, &out, false); err != nil {
		t.Fatalf("runRepoList: %v", err)
	}
	text := out.String()
	for _, want := range []string{"REPO", "DAEMON", "CLIENTS", "PENDING", "BLOCKED", "STATUS", repo, "stopped"} {
		if !strings.Contains(text, want) {
			t.Fatalf("human repo list output missing %q:\n%s", want, text)
		}
	}
	for _, unwanted := range []string{"STATE_DB", stateDB} {
		if strings.Contains(text, unwanted) {
			t.Fatalf("human repo list output includes %q:\n%s", unwanted, text)
		}
	}
}

func TestRepoList_ManagementSnapshotIncludesQueueAndMissingRows(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	activeRepo, activeStateDB, activeDB := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, activeDB, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	if err := state.RegisterClient(ctx, activeDB, state.Client{
		SessionID: "active-client", Harness: "codex", LastSeenTS: nowFloat(),
	}); err != nil {
		t.Fatalf("register active client: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, activeDB, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: "base",
		Operation: "modify", Path: "pending.txt", Fidelity: "exact",
		State: state.EventStatePending, CapturedTS: nowFloat(),
	}, nil); err != nil {
		t.Fatalf("append pending event: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, activeDB, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: "base",
		Operation: "modify", Path: "blocked.txt", Fidelity: "exact",
		State: state.EventStateBlockedConflict, CapturedTS: nowFloat(),
	}, nil); err != nil {
		t.Fatalf("append blocked event: %v", err)
	}
	_ = activeDB.Close()
	registerRepo(t, roots, activeRepo, activeStateDB, "")

	stoppedRepo, stoppedStateDB, stoppedDB := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, stoppedDB, state.DaemonState{
		PID: 0, Mode: "stopped", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save stopped daemon state: %v", err)
	}
	_ = stoppedDB.Close()
	registerRepo(t, roots, stoppedRepo, stoppedStateDB, "")

	missingRepo := filepath.Join(t.TempDir(), "missing-repo")
	stateMissingRepo := initRepoForRepoLifecycle(t)
	wt, err := git.ResolveWorktree(ctx, stateMissingRepo)
	if err != nil {
		t.Fatalf("resolve state missing repo: %v", err)
	}
	stateMissingDB := state.DBPathFromGitDir(wt.GitDir)
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.UpsertRepo(missingRepo, "missinghash", filepath.Join(missingRepo, ".git", "acd", "state.db"), "", 42)
		reg.DisableRepo(central.RepoRemovalTarget{Path: stoppedRepo}, 43)
		reg.UpsertRepo(stateMissingRepo, "statemissinghash", stateMissingDB, "", 42)
		return nil
	}); err != nil {
		t.Fatalf("seed missing rows: %v", err)
	}

	var out bytes.Buffer
	if err := runRepoList(ctx, &out, true); err != nil {
		t.Fatalf("runRepoList: %v", err)
	}
	var got struct {
		Repos []repoListEntry `json:"repos"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("decode repo list: %v\n%s", err, out.String())
	}
	if len(got.Repos) != 4 {
		t.Fatalf("repo list should include all registry rows, got %d: %+v", len(got.Repos), got.Repos)
	}
	byPath := map[string]repoListEntry{}
	for _, entry := range got.Repos {
		byPath[entry.Path] = entry
	}
	active := byPath[activeRepo]
	if active.Status != "running" || active.Clients != 1 || active.PendingEvents != 1 || active.BlockedConflicts != 1 {
		t.Fatalf("active summary mismatch: %+v", active)
	}
	if byPath[stoppedRepo].Status != "stopped" {
		t.Fatalf("stopped status mismatch before disabled assertion: %+v", byPath[stoppedRepo])
	}
	if byPath[stoppedRepo].Status != "disabled" {
		t.Fatalf("disabled status mismatch: %+v", byPath[stoppedRepo])
	}
	if byPath[missingRepo].Status != "repo-missing" {
		t.Fatalf("missing repo status mismatch: %+v", byPath[missingRepo])
	}
	if byPath[stateMissingRepo].Status != "state-db-missing" {
		t.Fatalf("state missing status mismatch: %+v", byPath[stateMissingRepo])
	}
}

func TestRepoCommandViaRootAndSetupInitAliasStillWorks(t *testing.T) {
	withIsolatedHome(t)
	repo := initRepoForRepoLifecycle(t)
	root := newRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"repo", "init", "--repo", repo, "--json"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("root repo init: %v\nstderr:%s", err, errOut.String())
	}
	if !json.Valid(out.Bytes()) {
		t.Fatalf("repo init did not emit JSON:\n%s", out.String())
	}

	out.Reset()
	errOut.Reset()
	root = newRootCmd()
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"init", "claude-code", "--raw"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("setup init alias failed: %v\nstderr:%s", err, errOut.String())
	}
	if !strings.Contains(errOut.String(), "deprecated") {
		t.Fatalf("setup init alias warning missing: %q", errOut.String())
	}
}

func initRepoForRepoLifecycle(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	ctx := context.Background()
	if err := git.Init(ctx, dir); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: dir}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref HEAD: %v", err)
	}
	wt, err := git.ResolveWorktree(ctx, dir)
	if err != nil {
		t.Fatalf("resolve worktree: %v", err)
	}
	hash, err := paths.RepoHash(wt.Root)
	if err != nil || hash == "" {
		t.Fatalf("repo hash: %q %v", hash, err)
	}
	return wt.Root
}
