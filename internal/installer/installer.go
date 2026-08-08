// Package installer plans and applies the one-command checkpoint-first setup
// transaction. Planning is read-only; Apply is the only mutating entry point.
package installer

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/globalops"
	integrationpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/integration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/migration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version"
)

type Options struct {
	Repo             string
	Integrations     string
	NonInteractive   bool
	ExpectedPlan     string
	Executable       string
	SkipServiceCheck bool
}

type Action struct {
	Kind   string `json:"kind"`
	Target string `json:"target"`
	Detail string `json:"detail"`
}

type Plan struct {
	OperationID        string                       `json:"operation_id"`
	Digest             string                       `json:"digest"`
	ExistingInstall    bool                         `json:"existing_install"`
	RequiresExpected   bool                         `json:"requires_expected_plan"`
	Repo               string                       `json:"repo"`
	RepositoryID       string                       `json:"repository_id"`
	WorktreeID         string                       `json:"worktree_id"`
	ManagedBinary      string                       `json:"managed_binary"`
	SourceExecutable   string                       `json:"source_executable"`
	Service            supervisor.ServiceDefinition `json:"service"`
	PriorService       ServiceState                 `json:"prior_service"`
	Registry           *central.Registry            `json:"registry"`
	Repositories       []migration.RepositoryPlan   `json:"repositories"`
	RecoveryManifests  []string                     `json:"recovery_manifests,omitempty"`
	Actions            []Action                     `json:"actions"`
	FreshDefaults      bool                         `json:"fresh_defaults"`
	Integrations       []string                     `json:"integrations"`
	IntegrationPlans   []integrationpkg.Plan        `json:"integration_plans"`
	BackupRoot         string                       `json:"backup_root"`
	ServiceAccessCheck bool                         `json:"service_access_check"`
}

type ServiceState struct {
	Installed bool `json:"installed"`
	Loaded    bool `json:"loaded"`
	Enabled   bool `json:"enabled"`
}

type ApplyOptions struct {
	Executor           Executor
	Quiesce            func(context.Context) error
	SelfTest           func(context.Context, Plan) error
	Ready              func(context.Context, paths.Roots, *central.Registry) error
	ServiceAccessCheck func(context.Context, paths.Roots, Plan, Executor) error
	ServiceAccessRetry func(context.Context, *ServiceAccessError) error
	Progress           func(Progress)
}

type Progress struct {
	Phase  string
	Detail string
}

type Result struct {
	OperationID string             `json:"operation_id"`
	PlanDigest  string             `json:"plan_digest"`
	Migrations  []migration.Result `json:"migrations"`
	Changed     bool               `json:"changed"`
}

type Executor interface {
	Run(context.Context, string, ...string) error
}
type OSExecutor struct{}

func (OSExecutor) Run(ctx context.Context, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func BuildPlan(ctx context.Context, roots paths.Roots, options Options) (Plan, error) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		return Plan{}, fmt.Errorf("setup: unsupported OS %s", runtime.GOOS)
	}
	wt, err := git.ResolveWorktree(ctx, options.Repo)
	if err != nil {
		return Plan{}, err
	}
	if err := git.DurabilitySupport(ctx, wt.Root); err != nil {
		return Plan{}, fmt.Errorf("setup: Git durability support: %w", err)
	}
	executable := options.Executable
	if executable == "" {
		executable, err = os.Executable()
		if err != nil {
			return Plan{}, err
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return Plan{}, err
	}
	service, err := supervisor.RenderService(home, roots.ManagedBinaryPath(), roots.SupervisorLogPath())
	if err != nil && !options.SkipServiceCheck {
		return Plan{}, err
	}
	if options.SkipServiceCheck && err != nil {
		service = supervisor.ServiceDefinition{Platform: runtime.GOOS, Path: filepath.Join(roots.State, "test.service"), Content: []byte("test")}
	}
	if !options.SkipServiceCheck {
		if err := supervisor.ValidateService(service, roots.ManagedBinaryPath()); err != nil {
			return Plan{}, err
		}
	}
	priorService := inspectServiceState(ctx, service)

	registry, err := central.Load(roots)
	if err != nil {
		return Plan{}, err
	}
	existing := registry.Version < central.RegistryVersion || len(registry.Repos) > 0 || fileExists(roots.ManagedBinaryPath()) || fileExists(service.Path) || fileExists(roots.ConfigPath())
	planned, err := central.PlanRegistryV2(ctx, registry)
	if err != nil && len(registry.Repos) > 0 {
		return Plan{}, err
	}
	if planned == nil {
		planned = central.NewRegistry()
	}
	now := time.Now().Unix()
	registration, err := planned.RegisterResolvedRepo(wt, "", now)
	if err != nil {
		return Plan{}, err
	}
	enabled := planned.EnableRepo(central.RepoRemovalTarget{Path: wt.Root, StateDB: registration.Record.StateDB}, now)
	if enabled.NotFound {
		return Plan{}, errors.New("setup: current repository disappeared from the planned registry")
	}
	registration.Record = enabled.Record
	planned.Version = central.RegistryVersion

	opID, err := newOperationID("setup")
	if err != nil {
		return Plan{}, err
	}
	backupRoot := roots.SetupOperationDir(opID)
	repoPlans := make([]migration.RepositoryPlan, 0, len(planned.Repos))
	for _, record := range planned.Repos {
		backup := filepath.Join(backupRoot, "repositories", record.WorktreeID, "state-v19.db")
		repoPlan, preflightErr := migration.Preflight(ctx, record, backup)
		if preflightErr != nil {
			return Plan{}, fmt.Errorf("setup: preflight %s: %w", record.Path, preflightErr)
		}
		repoPlans = append(repoPlans, repoPlan)
	}
	repoPlans, recoveryManifests, err := migration.AttachBridgeRecoveries(ctx, roots.SetupRoot(), repoPlans)
	if err != nil {
		return Plan{}, fmt.Errorf("setup: retained migration recovery: %w", err)
	}
	sort.Slice(repoPlans, func(i, j int) bool { return repoPlans[i].Record.Path < repoPlans[j].Record.Path })
	integrationPlans, err := integrationpkg.BuildPlans(roots, options.Integrations)
	if err != nil {
		return Plan{}, err
	}
	integrations := make([]string, 0, len(integrationPlans))
	for _, item := range integrationPlans {
		integrations = append(integrations, item.Name)
	}
	plan := Plan{
		OperationID: opID, ExistingInstall: existing, RequiresExpected: existing,
		Repo: wt.Root, RepositoryID: registration.Record.RepositoryID, WorktreeID: registration.Record.WorktreeID,
		ManagedBinary: roots.ManagedBinaryPath(), SourceExecutable: executable, Service: service,
		PriorService: priorService, Registry: planned,
		Repositories: repoPlans, RecoveryManifests: recoveryManifests,
		FreshDefaults: !existing, Integrations: integrations,
		IntegrationPlans: integrationPlans, BackupRoot: backupRoot,
		ServiceAccessCheck: runtime.GOOS == "darwin" && !options.SkipServiceCheck,
	}
	plan.Actions = []Action{
		{Kind: "backup", Target: backupRoot, Detail: "Back up every existing file and repository database"},
		{Kind: "install_binary", Target: plan.ManagedBinary, Detail: "Atomically copy this ACD executable"},
	}
	if plan.ServiceAccessCheck {
		plan.Actions = append(plan.Actions, Action{
			Kind: "verify_service_access", Target: plan.ManagedBinary,
			Detail: "Verify the background service can read every enabled repository",
		})
	}
	plan.Actions = append(plan.Actions,
		Action{Kind: "migrate", Target: "registered repositories", Detail: "Apply the all-or-nothing v19 to v20 checkpoint cutover"},
		Action{Kind: "install_service", Target: plan.Service.Path, Detail: "Install the user supervisor"},
		Action{Kind: "write_registry", Target: roots.RegistryPath(), Detail: "Persist common-directory and worktree identities"},
		Action{Kind: "enable_repository", Target: wt.Root, Detail: "Enable checkpoint protection for the current repository"},
		Action{Kind: "self_test", Target: backupRoot, Detail: "Run isolated checkpoint, publish, and restore verification"},
	)
	if len(recoveryManifests) > 0 {
		recoveryCount := 0
		for _, repository := range repoPlans {
			recoveryCount += len(repository.BridgeSnapshots)
		}
		plan.Actions = append(plan.Actions, Action{
			Kind: "import_recovery_checkpoints", Target: fmt.Sprintf("%d retained checkpoints", recoveryCount),
			Detail: "Import prior migration bridge checkpoints before their old refs are cleaned",
		})
	}
	for _, record := range planned.Repos {
		prior, found := registry.FindRepo(record.Path, record.StateDB)
		if found && !prior.LifecycleDisabled() && record.LifecycleDisabled() {
			plan.Actions = append(plan.Actions, Action{
				Kind:   "disable_missing_repository",
				Target: record.Path,
				Detail: "Preserve the stale registry record as disabled; no unresolved legacy state remains",
			})
		}
	}
	for _, name := range integrations {
		plan.Actions = append(plan.Actions, Action{Kind: "merge_integration", Target: name, Detail: "Merge versioned ACD hint commands while preserving unrelated entries"})
	}
	plan.Digest = digestPlan(plan)
	return plan, nil
}

func Apply(ctx context.Context, roots paths.Roots, plan Plan, options ApplyOptions) (Result, error) {
	if plan.Digest == "" || digestPlan(plan) != plan.Digest {
		return Result{}, errors.New("setup: plan digest mismatch")
	}
	if options.Executor == nil {
		options.Executor = OSExecutor{}
	}
	emitProgress(options, "prepare", "Preparing the setup transaction")
	if err := os.MkdirAll(plan.BackupRoot, 0o700); err != nil {
		return Result{}, err
	}
	journal, err := globalops.Open(ctx, roots.OperationsDBPath())
	if err != nil {
		return Result{}, err
	}
	defer journal.Close()
	steps := make([]globalops.Step, 0, len(plan.Actions))
	for i, action := range plan.Actions {
		steps = append(steps, globalops.Step{Sequence: i + 1, Kind: action.Kind, Target: action.Target, Phase: "planned"})
	}
	if err := journal.Prepare(ctx, globalops.Operation{ID: plan.OperationID, Kind: "setup", Phase: "planned", PlanDigest: plan.Digest}, steps); err != nil {
		return Result{}, err
	}

	emitProgress(options, "backup", "Backing up existing ACD files and repository state")
	targets := []string{roots.RegistryPath(), roots.ConfigPath(), roots.IntegrationsOwnershipPath(), plan.ManagedBinary, plan.Service.Path, roots.SetupPublicationHoldPath()}
	for _, item := range plan.IntegrationPlans {
		targets = append(targets, item.Target)
	}
	backups, err := backupFiles(plan.BackupRoot, targets, plan.PriorService)
	if err != nil {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "backup failed", true)
		return Result{}, err
	}
	if err := journal.Advance(ctx, plan.OperationID, "backed_up", "", false); err != nil {
		return Result{}, err
	}
	rollbackFiles := func(cause error) error {
		emitProgress(options, "rollback", "Setup failed; restoring all backed-up state")
		err := rollbackSetupFiles(journal, plan.OperationID, backups, cause)
		emitProgress(options, "rolled_back", "Rollback completed; the previous ACD state is unchanged")
		return err
	}
	emitProgress(options, "install_binary", "Installing the managed binary for setup preflight")
	if err := copyAtomic(plan.ManagedBinary, plan.SourceExecutable); err != nil {
		return Result{}, rollbackFiles(err)
	}
	if plan.ServiceAccessCheck {
		emitProgress(options, "service_access", "Verifying macOS background access before migration")
		accessCheck := options.ServiceAccessCheck
		if accessCheck == nil {
			accessCheck = verifyMacOSServiceAccess
		}
		for {
			err := accessCheck(ctx, roots, plan, options.Executor)
			if err == nil {
				break
			}
			var accessErr *ServiceAccessError
			if !errors.As(err, &accessErr) || options.ServiceAccessRetry == nil {
				return Result{}, rollbackFiles(err)
			}
			emitProgress(options, "service_access_required", "Waiting for macOS Full Disk Access")
			if retryErr := options.ServiceAccessRetry(ctx, accessErr); retryErr != nil {
				return Result{}, rollbackFiles(errors.Join(err, retryErr))
			}
			emitProgress(options, "service_access", "Rechecking macOS background access")
		}
	}
	holdBody, _ := json.Marshal(map[string]string{"operation_id": plan.OperationID, "plan_digest": plan.Digest})
	if err := writeAtomic(roots.SetupPublicationHoldPath(), append(holdBody, '\n'), 0o600); err != nil {
		return Result{}, rollbackFiles(err)
	}
	activeRepositories := 0
	for _, record := range plan.Registry.Repos {
		if !record.LifecycleDisabled() {
			activeRepositories++
		}
	}
	emitProgress(options, "bridge", fmt.Sprintf("Scanning %d enabled repositories and protecting edits during migration", activeRepositories))
	bridge, err := migration.StartBridgeWithProgress(ctx, plan.OperationID, plan.Registry.Repos, 500*time.Millisecond,
		func(completed, total int, repo string) {
			emitProgress(options, "bridge", fmt.Sprintf("Scanning repository %d of %d: %s", completed, total, repo))
		})
	if err != nil {
		if bridge != nil {
			bridge.Stop()
			_ = bridge.Cleanup(context.Background())
		}
		return Result{}, rollbackFiles(err)
	}
	if err := journal.Advance(ctx, plan.OperationID, "migration_watch_started", "", false); err != nil {
		bridge.Stop()
		_ = bridge.Cleanup(context.Background())
		return Result{}, rollbackFiles(err)
	}
	migrationApplied := false
	supervisorStarted := false
	rollback := func(cause error) error {
		emitProgress(options, "rollback", "Setup failed; restoring all backed-up files, databases, refs, and service state")
		bridge.Stop()
		retainedBridgeCheckpoints := bridge.CreatedCount()
		manifestErr := bridge.WriteRecoveryManifest(filepath.Join(plan.BackupRoot, "migration-bridge-recovery.json"))
		var stopErr error
		if supervisorStarted {
			stopErr = errors.Join(
				shutdownSupervisor(context.Background(), roots),
				unloadService(context.Background(), options.Executor, plan.Service),
			)
		}
		fileErr := restoreFiles(backups)
		var dbErr error
		if migrationApplied {
			dbErr = migration.Rollback(plan.Repositories)
		}
		serviceErr := restoreServiceState(context.Background(), options.Executor, plan.Service, plan.PriorService)
		rollbackErr := errors.Join(stopErr, fileErr, dbErr, serviceErr, manifestErr)
		if rollbackErr != nil {
			_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "setup rollback incomplete", true)
			emitProgress(options, "needs_attention", "Rollback could not be fully verified; run `acd doctor`")
		} else {
			detail := "setup rolled back"
			message := "Rollback completed; the previous ACD state is unchanged"
			if retainedBridgeCheckpoints > 0 {
				detail = fmt.Sprintf("setup rolled back with %d retained migration recovery checkpoints", retainedBridgeCheckpoints)
				message = fmt.Sprintf("Rollback completed; retained %d migration recovery checkpoints", retainedBridgeCheckpoints)
			}
			_ = journal.Advance(context.Background(), plan.OperationID, "rolled_back", detail, true)
			emitProgress(options, "rolled_back", message)
		}
		return errors.Join(cause, rollbackErr)
	}
	emitProgress(options, "quiesce", "Stopping legacy workers at a safe checkpoint boundary")
	if options.Quiesce != nil {
		if err := options.Quiesce(ctx); err != nil {
			return Result{}, rollback(err)
		}
	}
	if err := journal.Advance(ctx, plan.OperationID, "quiesced", "", false); err != nil {
		return Result{}, rollback(err)
	}
	emitProgress(options, "migrate", fmt.Sprintf("Migrating %d repositories to checkpoint storage", len(plan.Repositories)))
	migrations, err := migration.ApplyAllWithProgress(ctx, plan.Repositories, func(update migration.Progress) {
		emitProgress(options, "migrate", fmt.Sprintf("Migrating repository %d of %d: %s", update.Completed, update.Total, update.Repo))
	})
	if err != nil {
		return Result{}, rollback(err)
	}
	migrationApplied = true
	if err := journal.Advance(ctx, plan.OperationID, "legacy_preserved", "", false); err != nil {
		return Result{}, rollback(err)
	}
	if err := journal.Advance(ctx, plan.OperationID, "schema_applied", "", false); err != nil {
		return Result{}, rollback(err)
	}
	emitProgress(options, "install", "Installing the managed binary, supervisor, registry, and integrations")
	if plan.PriorService.Loaded {
		if err := unloadService(ctx, options.Executor, plan.Service); err != nil {
			return Result{}, rollback(err)
		}
	}
	if err := central.Save(roots, plan.Registry); err != nil {
		return Result{}, rollback(err)
	}
	if err := migrateRepositoryConfigKeys(roots, plan.Registry); err != nil {
		return Result{}, rollback(err)
	}
	if plan.FreshDefaults {
		if err := persistFreshDefaults(roots, plan.WorktreeID); err != nil {
			return Result{}, rollback(err)
		}
	}
	if err := writeAtomic(plan.Service.Path, plan.Service.Content, 0o600); err != nil {
		return Result{}, rollback(err)
	}
	for _, item := range plan.IntegrationPlans {
		if item.Changed {
			current, readErr := os.ReadFile(item.Target)
			if errors.Is(readErr, os.ErrNotExist) {
				current = nil
			} else if readErr != nil {
				return Result{}, rollback(readErr)
			}
			if sha256String(current) != item.BeforeDigest {
				return Result{}, rollback(fmt.Errorf("setup: integration changed after preview: %s", item.Target))
			}
			if err := writeAtomic(item.Target, item.Content, 0o600); err != nil {
				return Result{}, rollback(err)
			}
		}
	}
	if len(plan.IntegrationPlans) > 0 {
		if err := integrationpkg.WriteOwnership(roots.IntegrationsOwnershipPath(), plan.IntegrationPlans); err != nil {
			return Result{}, rollback(err)
		}
	}
	if err := journal.Advance(ctx, plan.OperationID, "integrations_merged", "", false); err != nil {
		return Result{}, rollback(err)
	}
	emitProgress(options, "service", "Starting the user supervisor")
	if err := loadService(ctx, options.Executor, plan.Service); err != nil {
		return Result{}, rollback(err)
	}
	supervisorStarted = true
	if err := journal.Advance(ctx, plan.OperationID, "service_installed", "", false); err != nil {
		return Result{}, rollback(err)
	}
	selfTest := options.SelfTest
	if selfTest == nil {
		selfTest = ScratchSelfTest
	}
	emitProgress(options, "self_test", "Running isolated checkpoint, publication, and restore checks")
	if err := selfTest(ctx, plan); err != nil {
		return Result{}, rollback(err)
	}
	if err := journal.Advance(ctx, plan.OperationID, "self_tested", "", false); err != nil {
		return Result{}, rollback(err)
	}
	ready := options.Ready
	if ready == nil {
		ready = func(ctx context.Context, roots paths.Roots, registry *central.Registry) error {
			return waitSetupWorkersWithProgress(ctx, roots, registry, func(ready, total int) {
				emitProgress(options, "workers", fmt.Sprintf(
					"Repository workers ready: %d of %d", ready, total))
			})
		}
	}
	emitProgress(options, "workers", "Waiting for repository workers to report complete protection")
	if err := ready(ctx, roots, plan.Registry); err != nil {
		return Result{}, rollback(err)
	}
	emitProgress(options, "finalize", "Closing the migration observation gap and finalizing setup")
	if _, err := bridge.Finalize(ctx); err != nil {
		return Result{}, rollback(err)
	}
	// A barrier after the bridge's final observation proves every edit that
	// could have arrived during cutover is represented by held v20 workers.
	if err := ready(ctx, roots, plan.Registry); err != nil {
		return Result{}, rollback(err)
	}
	if err := journal.Advance(ctx, plan.OperationID, "workers_ready", "", false); err != nil {
		return Result{}, rollback(err)
	}
	if err := journal.Advance(ctx, plan.OperationID, "committed", "", true); err != nil {
		return Result{}, rollback(err)
	}
	if err := os.Remove(roots.SetupPublicationHoldPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "remove publication hold", true)
		return Result{}, fmt.Errorf("setup committed but publication remains held: %w; run `acd support repair`", err)
	}
	if err := bridge.Cleanup(ctx); err != nil {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "cleanup migration bridge refs", true)
		return Result{}, fmt.Errorf("setup committed but migration bridge cleanup needs attention: %w", err)
	}
	if err := migration.CleanupBridgeRecoveryManifests(ctx, plan.RecoveryManifests); err != nil {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "cleanup retained migration recovery refs", true)
		return Result{}, fmt.Errorf("setup committed but retained migration recovery cleanup needs attention: %w", err)
	}
	emitProgress(options, "completed", "Setup transaction completed")
	return Result{OperationID: plan.OperationID, PlanDigest: plan.Digest, Migrations: migrations, Changed: true}, nil
}

func emitProgress(options ApplyOptions, phase, detail string) {
	if options.Progress != nil {
		options.Progress(Progress{Phase: phase, Detail: detail})
	}
}

func rollbackSetupFiles(journal *globalops.Store, operationID string, backups []fileBackup, cause error) error {
	fileErr := restoreFiles(backups)
	_ = journal.Advance(context.Background(), operationID, "rolled_back", "setup rolled back", true)
	return errors.Join(cause, fileErr)
}

func migrateRepositoryConfigKeys(roots paths.Roots, registry *central.Registry) error {
	if registry == nil {
		return errors.New("setup: nil v2 registry")
	}
	store := config.NewStore(roots)
	document, err := store.Load()
	if err != nil {
		return err
	}
	needsWrite := false
	for _, record := range registry.Repos {
		legacy := strings.TrimSpace(record.LegacyRepoHash)
		if legacy == "" || legacy == record.WorktreeID {
			continue
		}
		if _, exists := document.Settings.Repositories[legacy]; exists {
			needsWrite = true
			break
		}
	}
	if !needsWrite {
		return nil
	}
	return store.Update(func(document *config.Document) error {
		for _, record := range registry.Repos {
			legacy := strings.TrimSpace(record.LegacyRepoHash)
			if legacy == "" || legacy == record.WorktreeID {
				continue
			}
			oldSettings, exists := document.Settings.Repositories[legacy]
			if !exists {
				continue
			}
			if current, collision := document.Settings.Repositories[record.WorktreeID]; collision {
				oldBody, _ := json.Marshal(oldSettings)
				currentBody, _ := json.Marshal(current)
				if string(oldBody) != string(currentBody) {
					return fmt.Errorf("setup: repository config identity collision for %s", record.Path)
				}
			} else {
				document.Settings.Repositories[record.WorktreeID] = oldSettings
			}
			delete(document.Settings.Repositories, legacy)
		}
		return nil
	})
}

type fileBackup struct {
	Target     string      `json:"target"`
	Exists     bool        `json:"exists"`
	Type       string      `json:"type"`
	OwnerKnown bool        `json:"owner_known"`
	UID        uint32      `json:"uid"`
	GID        uint32      `json:"gid"`
	Mode       os.FileMode `json:"mode"`
	Backup     string      `json:"backup"`
	Digest     string      `json:"digest"`
}

type backupManifest struct {
	Files        []fileBackup `json:"files"`
	PriorService ServiceState `json:"prior_service"`
}

func backupFiles(root string, targets []string, priorService ServiceState) ([]fileBackup, error) {
	var backups []fileBackup
	for i, target := range targets {
		item := fileBackup{Target: target}
		info, err := os.Lstat(target)
		if errors.Is(err, os.ErrNotExist) {
			backups = append(backups, item)
			continue
		}
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("setup: target is not a regular file: %s", target)
		}
		body, err := os.ReadFile(target)
		if err != nil {
			return nil, err
		}
		item.Exists, item.Type, item.Mode = true, "regular", info.Mode()
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			item.OwnerKnown = true
			item.UID, item.GID = stat.Uid, stat.Gid
		}
		item.Digest = sha256String(body)
		item.Backup = filepath.Join(root, "files", fmt.Sprintf("%04d", i))
		if err := writeAtomic(item.Backup, body, 0o600); err != nil {
			return nil, err
		}
		backups = append(backups, item)
	}
	body, _ := json.MarshalIndent(backupManifest{Files: backups, PriorService: priorService}, "", "  ")
	if err := writeAtomic(filepath.Join(root, "backup-manifest.json"), append(body, '\n'), 0o600); err != nil {
		return nil, err
	}
	return backups, nil
}

func restoreFiles(backups []fileBackup) error {
	var combined error
	for _, item := range backups {
		if !item.Exists {
			if err := os.Remove(item.Target); err != nil && !errors.Is(err, os.ErrNotExist) {
				combined = errors.Join(combined, err)
			}
			continue
		}
		body, err := os.ReadFile(item.Backup)
		if err != nil {
			combined = errors.Join(combined, err)
			continue
		}
		if sha256String(body) != item.Digest {
			combined = errors.Join(combined, fmt.Errorf("setup: backup digest mismatch for %s", item.Target))
			continue
		}
		if err := writeAtomic(item.Target, body, item.Mode.Perm()); err != nil {
			combined = errors.Join(combined, err)
			continue
		}
		if item.OwnerKnown {
			if err := os.Chown(item.Target, int(item.UID), int(item.GID)); err != nil {
				combined = errors.Join(combined, fmt.Errorf("setup: restore owner for %s: %w", item.Target, err))
			}
		}
	}
	return combined
}

func persistFreshDefaults(roots paths.Roots, repositoryID string) error {
	store := config.NewStore(roots)
	return store.Update(func(document *config.Document) error {
		fields := config.Overrides{}
		values := map[string]any{
			config.FieldProvider: "deterministic", config.FieldCommitStrategy: "intent", config.FieldCommitPreset: "fast",
			config.FieldIntentVerification: "structural", config.FieldIntentRepairEnabled: false, config.FieldDiffEgress: false,
		}
		for key, value := range values {
			body, _ := json.Marshal(value)
			fields[key] = body
		}
		document.Settings.Repositories[repositoryID] = config.RepositorySettings{Fields: fields, Extra: map[string]json.RawMessage{}}
		return nil
	})
}

func loadService(ctx context.Context, executor Executor, service supervisor.ServiceDefinition) error {
	if service.Platform == "launchd" {
		return executor.Run(ctx, "launchctl", "bootstrap", fmt.Sprintf("gui/%d", os.Getuid()), service.Path)
	}
	if err := executor.Run(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		return err
	}
	return executor.Run(ctx, "systemctl", "--user", "enable", "--now", "acd-supervisor.service")
}
func unloadService(ctx context.Context, executor Executor, service supervisor.ServiceDefinition) error {
	if service.Platform == "launchd" {
		return executor.Run(ctx, "launchctl", "bootout", fmt.Sprintf("gui/%d", os.Getuid()), service.Path)
	}
	return executor.Run(ctx, "systemctl", "--user", "disable", "--now", "acd-supervisor.service")
}

func shutdownSupervisor(ctx context.Context, roots paths.Roots) error {
	if _, err := os.Lstat(roots.SupervisorSocketPath()); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	requestCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	response, err := (&supervisor.Client{
		SocketPath: roots.SupervisorSocketPath(), Timeout: 60 * time.Second,
	}).Do(requestCtx, supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: "setup-rollback-shutdown",
		Method: "shutdown", DeadlineMS: time.Now().Add(60 * time.Second).UnixMilli(),
	})
	if err != nil {
		return err
	}
	if response.Error != nil {
		return errors.New(response.Error.Message)
	}
	return nil
}

func inspectServiceState(ctx context.Context, service supervisor.ServiceDefinition) ServiceState {
	result := ServiceState{Installed: fileExists(service.Path)}
	if service.Platform == "launchd" {
		command := exec.CommandContext(ctx, "launchctl", "print", fmt.Sprintf("gui/%d/%s", os.Getuid(), supervisor.ServiceLabel))
		result.Loaded = command.Run() == nil
		result.Enabled = result.Loaded
		return result
	}
	result.Loaded = exec.CommandContext(ctx, "systemctl", "--user", "is-active", "--quiet", "acd-supervisor.service").Run() == nil
	result.Enabled = exec.CommandContext(ctx, "systemctl", "--user", "is-enabled", "--quiet", "acd-supervisor.service").Run() == nil
	return result
}

func restoreServiceState(ctx context.Context, executor Executor, service supervisor.ServiceDefinition, prior ServiceState) error {
	if service.Platform == "launchd" {
		if prior.Loaded {
			return loadService(ctx, executor, service)
		}
		return nil
	}
	if err := executor.Run(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		return err
	}
	if prior.Enabled {
		if err := executor.Run(ctx, "systemctl", "--user", "enable", "acd-supervisor.service"); err != nil {
			return err
		}
	}
	if prior.Loaded {
		return executor.Run(ctx, "systemctl", "--user", "start", "acd-supervisor.service")
	}
	return nil
}

func waitSupervisor(ctx context.Context, roots paths.Roots, repositoryID string) error {
	return waitSupervisorReady(ctx, roots, repositoryID, nil)
}

func waitSupervisorReady(ctx context.Context, roots paths.Roots, repositoryID string, processDone <-chan struct{}) error {
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: time.Second}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-processDone:
			return errors.New("setup: supervisor exited before becoming ready")
		case <-deadline.C:
			return errors.New("setup: supervisor did not become ready")
		case <-ticker.C:
			response, err := client.Do(ctx, supervisor.Request{Version: supervisor.ProtocolVersion, ID: "setup-ready", Method: "status"})
			if err != nil || !response.OK {
				continue
			}
			status, err := decode[supervisor.Status](response.Data)
			if err != nil {
				continue
			}
			for _, worker := range status.Workers {
				if worker.RepositoryID == repositoryID && worker.State == "running" {
					workerResponse, workerErr := supervisor.DoWorker(ctx,
						supervisor.WorkerSocketPath(roots, repositoryID),
						supervisor.Request{Version: supervisor.ProtocolVersion,
							ID: "setup-worker-ready", Method: "status",
							RepositoryID: repositoryID}, time.Second)
					if workerErr == nil && workerResponse.Version == supervisor.ProtocolVersion {
						return nil
					}
				}
			}
		}
	}
}

func waitSetupWorkers(ctx context.Context, roots paths.Roots, registry *central.Registry) error {
	return waitSetupWorkersWithProgress(ctx, roots, registry, nil)
}

func waitSetupWorkersWithProgress(
	ctx context.Context,
	roots paths.Roots,
	registry *central.Registry,
	progress func(ready, total int),
) error {
	if registry == nil {
		return errors.New("setup: v2 registry is unavailable")
	}
	repositories := make(map[string]bool)
	for _, record := range registry.Repos {
		if !record.LifecycleDisabled() {
			repositories[record.RepositoryID] = true
		}
	}
	if err := waitSupervisorWorkersReady(ctx, roots, repositories, 5*time.Minute, progress); err != nil {
		return err
	}
	client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 60 * time.Second}
	statusResponse, err := client.Do(ctx, supervisor.Request{Version: supervisor.ProtocolVersion, ID: "setup-version", Method: "status", DeadlineMS: time.Now().Add(10 * time.Second).UnixMilli()})
	if err != nil {
		return err
	}
	status, err := decode[supervisor.Status](statusResponse.Data)
	if err != nil {
		return err
	}
	if status.Version != version.String() {
		return fmt.Errorf("setup: supervisor version %q does not match managed binary %q", status.Version, version.String())
	}
	for _, record := range registry.Repos {
		if record.LifecycleDisabled() {
			continue
		}
		response, callErr := client.Do(ctx, supervisor.Request{
			Version: supervisor.ProtocolVersion, ID: "setup-barrier-" + record.WorktreeID,
			Method: "checkpoint_barrier", RepositoryID: record.RepositoryID,
			WorktreeID: record.WorktreeID, DeadlineMS: time.Now().Add(60 * time.Second).UnixMilli(),
		})
		if callErr != nil {
			return callErr
		}
		if response.Error != nil {
			return errors.New(response.Error.Message)
		}
		var barrier struct {
			Protected bool `json:"protected"`
		}
		if err := decodeInto(response.Data, &barrier); err != nil || !barrier.Protected {
			return fmt.Errorf("setup: worktree %s did not confirm checkpoint coverage", record.Path)
		}
		if schema, readErr := state.ReadUserVersion(ctx, record.StateDB); readErr != nil || schema != state.SchemaVersion {
			return fmt.Errorf("setup: worktree %s schema is not v%d", record.Path, state.SchemaVersion)
		}
	}
	return nil
}

func waitSupervisorWorkersReady(
	ctx context.Context,
	roots paths.Roots,
	repositories map[string]bool,
	timeout time.Duration,
	progress func(ready, total int),
) error {
	pending := make(map[string]bool, len(repositories))
	for repositoryID := range repositories {
		pending[repositoryID] = true
	}
	total := len(pending)
	if progress != nil {
		progress(0, total)
	}
	if total == 0 {
		return nil
	}
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 2 * time.Second}
	lastStatus := make(map[string]supervisor.WorkerStatus, total)
	lastProbeError := make(map[string]string, total)
	lastReported := 0
	check := func() {
		response, err := client.Do(ctx, supervisor.Request{
			Version: supervisor.ProtocolVersion, ID: "setup-workers-ready", Method: "status",
		})
		if err != nil || !response.OK {
			return
		}
		status, err := decode[supervisor.Status](response.Data)
		if err != nil {
			return
		}
		for _, worker := range status.Workers {
			lastStatus[worker.RepositoryID] = worker
			if !pending[worker.RepositoryID] || worker.State != "running" {
				continue
			}
			workerResponse, workerErr := supervisor.DoWorker(ctx,
				supervisor.WorkerSocketPath(roots, worker.RepositoryID),
				supervisor.Request{Version: supervisor.ProtocolVersion,
					ID: "setup-worker-ready", Method: "status",
					RepositoryID: worker.RepositoryID}, 500*time.Millisecond)
			if workerErr != nil {
				lastProbeError[worker.RepositoryID] = workerErr.Error()
				continue
			}
			if workerResponse.Version != supervisor.ProtocolVersion {
				lastProbeError[worker.RepositoryID] = "worker protocol version mismatch"
				continue
			}
			delete(pending, worker.RepositoryID)
			delete(lastProbeError, worker.RepositoryID)
		}
		ready := total - len(pending)
		if progress != nil && ready != lastReported {
			progress(ready, total)
			lastReported = ready
		}
	}
	check()
	for len(pending) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("setup: repository workers ready %d of %d; pending: %s",
				total-len(pending), total, summarizePendingWorkers(pending, lastStatus, lastProbeError))
		case <-ticker.C:
			check()
		}
	}
	return nil
}

func summarizePendingWorkers(
	pending map[string]bool,
	statuses map[string]supervisor.WorkerStatus,
	probeErrors map[string]string,
) string {
	ids := make([]string, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	const limit = 8
	parts := make([]string, 0, min(len(ids), limit))
	for _, id := range ids[:min(len(ids), limit)] {
		stateName := "not_started"
		detail := probeErrors[id]
		if status, ok := statuses[id]; ok {
			stateName = status.State
			if status.LastError != "" {
				detail = status.LastError
			}
		}
		if detail == "" {
			parts = append(parts, fmt.Sprintf("%s(%s)", id, stateName))
		} else {
			parts = append(parts, fmt.Sprintf("%s(%s: %s)", id, stateName, detail))
		}
	}
	if len(ids) > limit {
		parts = append(parts, fmt.Sprintf("and %d more", len(ids)-limit))
	}
	return strings.Join(parts, ", ")
}

func parseIntegrations(value string) ([]string, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "auto" || value == "none" {
		return []string{}, nil
	}
	seen := map[string]bool{}
	var result []string
	for _, name := range strings.Split(value, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}
func digestPlan(plan Plan) string {
	copy := plan
	if plan.Registry != nil {
		registryCopy := *plan.Registry
		registryCopy.Repos = append([]central.RepoRecord(nil), plan.Registry.Repos...)
		copy.Registry = &registryCopy
	}
	copy.Repositories = append([]migration.RepositoryPlan(nil), plan.Repositories...)
	copy.Actions = append([]Action(nil), plan.Actions...)
	copy.Digest = ""
	copy.OperationID = ""
	copy.BackupRoot = "<setup-operation>"
	if copy.Registry != nil {
		for i := range copy.Registry.Repos {
			clearPlanRecordTimestamps(&copy.Registry.Repos[i])
		}
	}
	for i := range copy.Repositories {
		clearPlanRecordTimestamps(&copy.Repositories[i].Record)
		copy.Repositories[i].BackupPath = filepath.Join("<setup-operation>", "repositories", copy.Repositories[i].Record.WorktreeID, "state-v19.db")
	}
	for i := range copy.Actions {
		if copy.Actions[i].Kind == "backup" || copy.Actions[i].Kind == "self_test" {
			copy.Actions[i].Target = "<setup-operation>"
		}
	}
	body, _ := json.Marshal(copy)
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func clearPlanRecordTimestamps(record *central.RepoRecord) {
	record.FirstRegisteredTS = 0
	record.LastSeenTS = 0
	record.LifecycleUpdatedTS = 0
}
func sha256String(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func newOperationID(prefix string) (string, error) {
	body := make([]byte, 8)
	if _, err := rand.Read(body); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%d-%s", prefix, time.Now().UnixMilli(), hex.EncodeToString(body)), nil
}
func fileExists(path string) bool { _, err := os.Stat(path); return err == nil }
func decode[T any](value any) (T, error) {
	var target T
	body, err := json.Marshal(value)
	if err != nil {
		return target, err
	}
	err = json.Unmarshal(body, &target)
	return target, err
}

func copyAtomic(target, source string) error {
	body, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	return writeAtomic(target, body, 0o755)
}
func writeAtomic(path string, body []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".acd-install-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
