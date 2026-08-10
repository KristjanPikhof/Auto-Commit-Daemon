package installer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/globalops"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

type recordingExecutor struct{ calls []string }

func (r *recordingExecutor) Run(_ context.Context, name string, _ ...string) error {
	r.calls = append(r.calls, name)
	return nil
}

func (r *recordingExecutor) StartSession(_ context.Context, _ paths.Roots, _ supervisor.ServiceDefinition) error {
	r.calls = append(r.calls, "session")
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

func TestBuildPlanUsesMacOSSessionSupervisorWithoutAccessProbe(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS-only setup contract")
	}
	ctx := context.Background()
	roots, repo, executable := installerFixture(t, ctx)
	plan, err := BuildPlan(ctx, roots, Options{Repo: repo, Executable: executable})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Service.Platform != "session" || len(plan.Service.Content) != 0 {
		t.Fatalf("service=%+v, want session-owned supervisor", plan.Service)
	}
	foundSessionStart := false
	for _, action := range plan.Actions {
		if strings.Contains(action.Kind, "access") || strings.Contains(action.Detail, "grant") {
			t.Fatalf("setup plan contains a permission probe: %+v", action)
		}
		if action.Kind == "start_session_supervisor" {
			foundSessionStart = true
		}
	}
	if !foundSessionStart {
		t.Fatalf("setup plan does not disclose session supervisor: %+v", plan.Actions)
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
		"prepare", "quiesce", "backup", "install_binary", "bridge", "migrate", "install",
		"service", "self_test", "workers", "integrations", "finalize", "completed") {
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

func TestSetupRejectsStaleRegistryPreviewAndRecordsNeedsAction(t *testing.T) {
	ctx := context.Background()
	roots, repo, executable := installerFixture(t, ctx)
	plan, err := BuildPlan(ctx, roots, Options{Repo: repo, Executable: executable, SkipServiceCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	changed := central.NewRegistry()
	changed.Repos = append(changed.Repos, central.RepoRecord{
		Path: filepath.Join(t.TempDir(), "other"), StateDB: filepath.Join(t.TempDir(), "state.db"),
		LifecycleState: central.RepoLifecycleDisabled,
	})
	if err := central.Save(roots, changed); err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(ctx, roots, plan, ApplyOptions{Executor: &recordingExecutor{}}); err == nil ||
		!strings.Contains(err.Error(), "state changed after preview") {
		t.Fatalf("Apply stale preview error=%v", err)
	}
	journal, err := globalops.OpenReadOnly(ctx, roots.OperationsDBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	operations, err := journal.OperationsByPhase(ctx, "needs_attention")
	if err != nil || len(operations) != 1 || operations[0].ID != plan.OperationID {
		t.Fatalf("needs_attention=%+v err=%v", operations, err)
	}
	if _, err := os.Stat(plan.ManagedBinary); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale setup mutated managed binary: %v", err)
	}
}

func TestSetupRejectsStaleServiceFilePreview(t *testing.T) {
	ctx := context.Background()
	roots, repo, executable := installerFixture(t, ctx)
	plan, err := BuildPlan(ctx, roots, Options{Repo: repo, Executable: executable, SkipServiceCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(plan.Service.Path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plan.Service.Path, []byte("concurrent service edit"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(ctx, roots, plan, ApplyOptions{Executor: &recordingExecutor{}}); err == nil ||
		!strings.Contains(err.Error(), "state changed after preview") {
		t.Fatalf("Apply stale service preview error=%v", err)
	}
	if body, err := os.ReadFile(plan.Service.Path); err != nil || string(body) != "concurrent service edit" {
		t.Fatalf("service file=(%q,%v)", body, err)
	}
}

func TestRollbackPreservesConcurrentHostEdit(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "config.json")
	if err := os.WriteFile(target, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	backups, err := backupFiles(filepath.Join(root, "backup"), []string{target}, ServiceState{})
	if err != nil {
		t.Fatal(err)
	}
	postDigest := sha256String([]byte("setup output"))
	if err := preparePostMutation(filepath.Join(root, "backup"), backups, ServiceState{}, target, postDigest); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("setup output"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("concurrent edit"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := restoreFiles(backups); err == nil || !strings.Contains(err.Error(), "preserve concurrent edit") {
		t.Fatalf("restoreFiles error=%v", err)
	}
	if body, err := os.ReadFile(target); err != nil || string(body) != "concurrent edit" {
		t.Fatalf("concurrent edit=(%q,%v)", body, err)
	}
}

func TestRollbackPreparedMutationCanProveNoWriteOccurred(t *testing.T) {
	tests := []struct {
		name       string
		before     []byte
		postDigest string
	}{
		{name: "existing file remains", before: []byte("before"), postDigest: sha256String([]byte("setup output"))},
		{name: "prepared deletion does not run", before: []byte("before"), postDigest: absentFileDigest},
		{name: "prepared creation does not run", postDigest: sha256String([]byte("setup output"))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			target := filepath.Join(root, "config.json")
			if test.before != nil {
				if err := os.WriteFile(target, test.before, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			backupRoot := filepath.Join(root, "backup")
			backups, err := backupFiles(backupRoot, []string{target}, ServiceState{})
			if err != nil {
				t.Fatal(err)
			}
			if err := preparePostMutation(backupRoot, backups, ServiceState{}, target, test.postDigest); err != nil {
				t.Fatal(err)
			}
			if err := restoreFiles(backups); err != nil {
				t.Fatal(err)
			}
			body, err := os.ReadFile(target)
			if test.before == nil {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unmodified absent target=(%q,%v)", body, err)
				}
				return
			}
			if err != nil || string(body) != string(test.before) {
				t.Fatalf("unmodified target=(%q,%v)", body, err)
			}
		})
	}
}

func TestRollbackNoReplacePreservesEditAfterClaim(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "config.json")
	backup := filepath.Join(root, "backup")
	if err := os.WriteFile(backup, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("setup output"), 0o600); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimRollbackTarget(target)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(claimed)
	if err := os.WriteFile(target, []byte("concurrent edit"), 0o600); err != nil {
		t.Fatal(err)
	}
	item := fileBackup{Target: target, Exists: true, Backup: backup,
		Digest: sha256String([]byte("before")), Mode: 0o600}
	if err := installBackupNoReplace(item); err == nil || !strings.Contains(err.Error(), "preserve concurrent edit") {
		t.Fatalf("installBackupNoReplace error=%v", err)
	}
	if body, err := os.ReadFile(target); err != nil || string(body) != "concurrent edit" {
		t.Fatalf("concurrent target=(%q,%v)", body, err)
	}
}

func TestSetupRollbackPreservesConcurrentEditAndNeedsAction(t *testing.T) {
	ctx := context.Background()
	roots, repo, executable := installerFixture(t, ctx)
	plan, err := BuildPlan(ctx, roots, Options{Repo: repo, Executable: executable, SkipServiceCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	var progress []Progress
	_, err = Apply(ctx, roots, plan, ApplyOptions{
		Executor: &recordingExecutor{}, Ready: readyImmediately,
		Progress: func(update Progress) { progress = append(progress, update) },
		SelfTest: func(context.Context, Plan) error {
			if err := os.WriteFile(roots.ConfigPath(), []byte("concurrent edit"), 0o600); err != nil {
				return err
			}
			return errors.New("injected self-test failure")
		},
	})
	if err == nil || !strings.Contains(err.Error(), "preserve concurrent edit") {
		t.Fatalf("Apply rollback error=%v", err)
	}
	if body, err := os.ReadFile(roots.ConfigPath()); err != nil || string(body) != "concurrent edit" {
		t.Fatalf("concurrent config=(%q,%v)", body, err)
	}
	phases := progressPhases(progress)
	if !containsPhasesInOrder(phases, "rollback", "needs_attention") || containsPhasesInOrder(phases, "rolled_back") {
		t.Fatalf("rollback progress phases=%v", phases)
	}
	journal, err := globalops.OpenReadOnly(ctx, roots.OperationsDBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	operations, err := journal.OperationsByPhase(ctx, "needs_attention")
	if err != nil || len(operations) != 1 || operations[0].ID != plan.OperationID {
		t.Fatalf("needs_attention=%+v err=%v", operations, err)
	}
}

func TestUninstallRejectsStaleRegistryPreviewAndRecordsNeedsAction(t *testing.T) {
	ctx := context.Background()
	roots, _, _ := installerFixture(t, ctx)
	if err := central.Save(roots, central.NewRegistry()); err != nil {
		t.Fatal(err)
	}
	plan, err := BuildUninstallPlan(ctx, roots, false)
	if err != nil {
		t.Fatal(err)
	}
	changed := central.NewRegistry()
	changed.Repos = append(changed.Repos, central.RepoRecord{
		Path: filepath.Join(t.TempDir(), "other"), StateDB: filepath.Join(t.TempDir(), "state.db"),
		LifecycleState: central.RepoLifecycleDisabled,
	})
	if err := central.Save(roots, changed); err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyUninstall(ctx, roots, plan, &recordingExecutor{}); err == nil ||
		!strings.Contains(err.Error(), "state changed after preview") {
		t.Fatalf("ApplyUninstall stale preview error=%v", err)
	}
	journal, err := globalops.OpenReadOnly(ctx, roots.OperationsDBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	operations, err := journal.OperationsByPhase(ctx, "needs_attention")
	if err != nil || len(operations) != 1 || operations[0].ID != plan.OperationID {
		t.Fatalf("needs_attention=%+v err=%v", operations, err)
	}
}

func TestUninstallPurgeRetainsActiveLifecycleLock(t *testing.T) {
	root := filepath.Join(t.TempDir(), "acd")
	lockPath := filepath.Join(root, "operations.lock")
	if err := os.MkdirAll(filepath.Join(root, "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	for path, body := range map[string]string{
		lockPath: "lock", filepath.Join(root, "operations.db"): "journal",
		filepath.Join(root, "bin", "acd"): "binary",
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := purgeRootExcept(root, lockPath); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(lockPath); err != nil || string(body) != "lock" {
		t.Fatalf("retained lock=(%q,%v)", body, err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 || entries[0].Name() != "operations.lock" {
		t.Fatalf("purged share entries=%v err=%v", entries, err)
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
