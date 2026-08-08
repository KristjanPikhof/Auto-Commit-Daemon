package installer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type recordingExecutor struct{ calls []string }

func (r *recordingExecutor) Run(_ context.Context, name string, _ ...string) error {
	r.calls = append(r.calls, name)
	return nil
}

func TestBuildPlanIsReadOnlyAndStableForDigest(t *testing.T) {
	ctx := context.Background()
	roots, repo, executable := installerFixture(t, ctx)
	plan, err := BuildPlan(ctx, roots, Options{Repo: repo, Executable: executable, SkipServiceCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Digest == "" || plan.ExistingInstall || !plan.FreshDefaults || len(plan.Repositories) != 1 {
		t.Fatalf("plan=%+v", plan)
	}
	if _, err := os.Stat(roots.RegistryPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("planning wrote registry: %v", err)
	}
	if _, err := os.Stat(plan.Repositories[0].Record.StateDB); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("planning wrote state DB: %v", err)
	}
	if got := digestPlan(plan); got != plan.Digest {
		t.Fatalf("digest=%s want %s", got, plan.Digest)
	}
	second, err := BuildPlan(ctx, roots, Options{Repo: repo, Executable: executable, SkipServiceCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	if second.OperationID == plan.OperationID {
		t.Fatalf("operation ID was reused: %s", plan.OperationID)
	}
	if second.Digest != plan.Digest {
		t.Fatalf("equivalent plan digest changed: first=%s second=%s", plan.Digest, second.Digest)
	}
	for _, repository := range plan.Repositories {
		if !strings.HasPrefix(repository.BackupPath, plan.BackupRoot+string(os.PathSeparator)) {
			t.Fatalf("digest normalization mutated backup path to %q", repository.BackupPath)
		}
	}
	if len(plan.Actions) == 0 || plan.Actions[0].Target != plan.BackupRoot {
		t.Fatalf("digest normalization mutated setup action target: %+v", plan.Actions)
	}
	if plan.Registry.Repos[0].LastSeenTS == 0 {
		t.Fatalf("digest normalization cleared live registry timestamps: %+v", plan.Registry.Repos[0])
	}
}

func TestBuildPlanDisablesStaleMissingRowsAndEnablesCurrentRepo(t *testing.T) {
	ctx := context.Background()
	roots, repo, executable := installerFixture(t, ctx)
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "deleted-repository")
	registry := &central.Registry{Version: 1, Repos: []central.RepoRecord{
		{
			Path: missing, RepoHash: "stale-hash",
			StateDB: filepath.Join(missing, ".git", "acd", "state.db"),
		},
		{
			Path: repo, RepoHash: "current-hash", StateDB: state.DBPathFromGitDir(wt.GitDir),
			LifecycleState: central.RepoLifecycleDisabled,
		},
	}}
	if err := central.Save(roots, registry); err != nil {
		t.Fatal(err)
	}

	plan, err := BuildPlan(ctx, roots, Options{Repo: repo, Executable: executable, SkipServiceCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	current, ok := plan.Registry.FindRepo(repo, state.DBPathFromGitDir(wt.GitDir))
	if !ok || current.LifecycleDisabled() {
		t.Fatalf("current repository was not enabled: %+v ok=%v", current, ok)
	}
	stale, ok := plan.Registry.FindRepo(missing, filepath.Join(missing, ".git", "acd", "state.db"))
	if !ok || !stale.LifecycleDisabled() {
		t.Fatalf("stale repository was not preserved as disabled: %+v ok=%v", stale, ok)
	}
	wantAction := false
	for _, action := range plan.Actions {
		if action.Kind == "disable_missing_repository" && action.Target == missing {
			wantAction = true
		}
	}
	if !wantAction {
		t.Fatalf("plan actions do not disclose stale repository handling: %+v", plan.Actions)
	}
}

func TestApplyFreshSetupPersistsV20AndDefaults(t *testing.T) {
	ctx := context.Background()
	roots, repo, executable := installerFixture(t, ctx)
	plan, err := BuildPlan(ctx, roots, Options{Repo: repo, Executable: executable, SkipServiceCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	executor := &recordingExecutor{}
	var progress []Progress
	result, err := Apply(ctx, roots, plan, ApplyOptions{
		Executor: executor, Ready: readyImmediately, SelfTest: selfTestImmediately,
		Progress: func(update Progress) { progress = append(progress, update) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Changed || len(executor.calls) == 0 {
		t.Fatalf("result=%+v calls=%v", result, executor.calls)
	}
	if got := progressPhases(progress); !containsPhasesInOrder(got,
		"prepare", "backup", "install_binary", "bridge", "quiesce", "migrate", "install",
		"service", "self_test", "workers", "finalize", "completed") {
		t.Fatalf("progress phases=%v", got)
	}
	if body, err := os.ReadFile(plan.ManagedBinary); err != nil || string(body) != "binary" {
		t.Fatalf("managed binary=(%q,%v)", body, err)
	}
	registry, err := central.Load(roots)
	if err != nil {
		t.Fatal(err)
	}
	if registry.Version != central.RegistryVersion || len(registry.Repos) != 1 {
		t.Fatalf("registry=%+v", registry)
	}
	if version, err := state.ReadUserVersion(ctx, registry.Repos[0].StateDB); err != nil || version != state.SchemaVersion {
		t.Fatalf("schema=(%d,%v)", version, err)
	}
	doc, err := config.NewStore(roots).Load()
	if err != nil {
		t.Fatal(err)
	}
	settings := doc.Settings.Repositories[plan.WorktreeID]
	if string(settings.Fields[config.FieldProvider]) != `"deterministic"` || string(settings.Fields[config.FieldCommitStrategy]) != `"intent"` {
		t.Fatalf("settings=%v", settings.Fields)
	}
}

func TestApplyServiceAccessFailureStopsBeforeMigration(t *testing.T) {
	ctx := context.Background()
	roots, repo, executable := installerFixture(t, ctx)
	if err := os.MkdirAll(filepath.Dir(roots.ManagedBinaryPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(roots.ManagedBinaryPath(), []byte("prior-binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlan(ctx, roots, Options{
		Repo: repo, Executable: executable, SkipServiceCheck: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan.ServiceAccessCheck = true
	plan.Digest = digestPlan(plan)
	var progress []Progress
	_, err = Apply(ctx, roots, plan, ApplyOptions{
		Executor: &recordingExecutor{},
		ServiceAccessCheck: func(context.Context, paths.Roots, Plan, Executor) error {
			return &ServiceAccessError{
				Target: repo, ManagedBinary: plan.ManagedBinary,
				Cause: errors.New("injected background denial"),
			}
		},
		Progress: func(update Progress) { progress = append(progress, update) },
	})
	var accessErr *ServiceAccessError
	if !errors.As(err, &accessErr) {
		t.Fatalf("error=%v, want ServiceAccessError", err)
	}
	body, readErr := os.ReadFile(roots.ManagedBinaryPath())
	if readErr != nil || string(body) != "prior-binary" {
		t.Fatalf("managed binary=(%q,%v), want restored prior binary", body, readErr)
	}
	phases := progressPhases(progress)
	if !containsPhasesInOrder(phases, "prepare", "backup", "install_binary", "service_access", "rollback", "rolled_back") {
		t.Fatalf("progress phases=%v", phases)
	}
	for _, phase := range phases {
		if phase == "bridge" || phase == "migrate" {
			t.Fatalf("migration started after access denial: %v", phases)
		}
	}
}

func TestApplyServiceAccessRetryContinuesSameTransaction(t *testing.T) {
	ctx := context.Background()
	roots, repo, executable := installerFixture(t, ctx)
	plan, err := BuildPlan(ctx, roots, Options{
		Repo: repo, Executable: executable, SkipServiceCheck: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan.ServiceAccessCheck = true
	plan.Digest = digestPlan(plan)
	checks := 0
	retries := 0
	var progress []Progress
	_, err = Apply(ctx, roots, plan, ApplyOptions{
		Executor: &recordingExecutor{}, Ready: readyImmediately,
		SelfTest: selfTestImmediately,
		ServiceAccessCheck: func(context.Context, paths.Roots, Plan, Executor) error {
			checks++
			if checks == 1 {
				return &ServiceAccessError{
					Target: repo, ManagedBinary: plan.ManagedBinary,
					Cause: errors.New("injected background denial"),
				}
			}
			return nil
		},
		ServiceAccessRetry: func(context.Context, *ServiceAccessError) error {
			retries++
			return nil
		},
		Progress: func(update Progress) { progress = append(progress, update) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if checks != 2 || retries != 1 {
		t.Fatalf("checks=%d retries=%d", checks, retries)
	}
	if got := progressPhases(progress); !containsPhasesInOrder(got,
		"service_access", "service_access_required", "service_access", "bridge", "migrate", "completed") {
		t.Fatalf("progress phases=%v", got)
	}
}

func TestPurgeTransactionRollbackRestoresStateAndRefs(t *testing.T) {
	ctx := context.Background()
	_, repo, _ := installerFixture(t, ctx)
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	stateDir := state.AcdDirFromGitDir(wt.GitDir)
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(stateDir, "marker")
	if err := os.WriteFile(marker, []byte("protected"), 0o600); err != nil {
		t.Fatal(err)
	}
	oid, err := gitpkg.HashObjectStdin(ctx, repo, []byte("checkpoint"))
	if err != nil {
		t.Fatal(err)
	}
	ref := gitpkg.RefExpectation{Ref: "refs/acd/checkpoints/test/checkpoint", OID: oid}
	if err := gitpkg.CreateRefsCAS(ctx, repo, []gitpkg.RefExpectation{ref}); err != nil {
		t.Fatal(err)
	}
	target := PurgeTarget{RepositoryID: "0123456789abcdef", WorktreeID: "fedcba9876543210",
		RepoRoot: repo, GitDir: wt.GitDir, StateDir: stateDir, Refs: []gitpkg.RefExpectation{ref}}
	txn, err := stagePurge(ctx, "uninstall-test", []PurgeTarget{target})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state remained live during staged purge: %v", err)
	}
	if _, err := gitpkg.RevParse(ctx, repo, ref.Ref); !errors.Is(err, gitpkg.ErrRefNotFound) {
		t.Fatalf("private ref remained during staged purge: %v", err)
	}
	if err := txn.rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(marker); err != nil || string(body) != "protected" {
		t.Fatalf("restored state=(%q,%v)", body, err)
	}
	if got, err := gitpkg.RevParse(ctx, repo, ref.Ref); err != nil || got != oid {
		t.Fatalf("restored ref=(%s,%v), want %s", got, err, oid)
	}
}

func TestApplyRollsBackFilesAndFreshDatabaseOnFailure(t *testing.T) {
	ctx := context.Background()
	roots, repo, executable := installerFixture(t, ctx)
	if err := os.MkdirAll(filepath.Dir(roots.ConfigPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(roots.ConfigPath(), []byte("{\"version\":1}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlan(ctx, roots, Options{Repo: repo, Executable: executable, SkipServiceCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	var progress []Progress
	_, err = Apply(ctx, roots, plan, ApplyOptions{
		Executor: &recordingExecutor{},
		SelfTest: func(context.Context, Plan) error { return errors.New("injected") },
		Ready:    readyImmediately,
		Progress: func(update Progress) { progress = append(progress, update) },
	})
	if err == nil {
		t.Fatal("expected failure")
	}
	body, readErr := os.ReadFile(roots.ConfigPath())
	if readErr != nil || string(body) != "{\"version\":1}\n" {
		t.Fatalf("config=(%q,%v)", body, readErr)
	}
	if _, statErr := os.Stat(plan.Repositories[0].Record.StateDB); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("fresh DB retained: %v", statErr)
	}
	phases := progressPhases(progress)
	if !containsPhasesInOrder(phases, "self_test", "rollback", "rolled_back") {
		t.Fatalf("rollback progress phases=%v", phases)
	}
}

func TestApplyQuiesceFailureDoesNotRestoreUncreatedMigrationBackups(t *testing.T) {
	ctx := context.Background()
	roots, repo, executable := installerFixture(t, ctx)
	plan, err := BuildPlan(ctx, roots, Options{Repo: repo, Executable: executable, SkipServiceCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	plan.Repositories[0].FromVersion = 19
	plan.Digest = digestPlan(plan)
	_, err = Apply(ctx, roots, plan, ApplyOptions{
		Executor: &recordingExecutor{},
		Quiesce:  func(context.Context) error { return errors.New("injected quiesce failure") },
		Ready:    readyImmediately,
		SelfTest: selfTestImmediately,
	})
	if err == nil || !strings.Contains(err.Error(), "injected quiesce failure") {
		t.Fatalf("Apply error=%v", err)
	}
	if strings.Contains(err.Error(), "state-v19.db") || strings.Contains(err.Error(), "no such file") {
		t.Fatalf("pre-migration rollback tried to restore an uncreated database backup: %v", err)
	}
}

func progressPhases(updates []Progress) []string {
	phases := make([]string, 0, len(updates))
	for _, update := range updates {
		phases = append(phases, update.Phase)
	}
	return phases
}

func containsPhasesInOrder(got []string, want ...string) bool {
	next := 0
	for _, phase := range got {
		if next < len(want) && phase == want[next] {
			next++
		}
	}
	return next == len(want)
}

func readyImmediately(context.Context, paths.Roots, *central.Registry) error { return nil }
func selfTestImmediately(context.Context, Plan) error                        { return nil }

func installerFixture(t *testing.T, ctx context.Context) (paths.Roots, string, string) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, "state"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(home, "data"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	roots, err := paths.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	repo := filepath.Join(home, "repo")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := gitpkg.Init(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("content\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(home, "source-acd")
	if err := os.WriteFile(executable, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	return roots, repo, executable
}
