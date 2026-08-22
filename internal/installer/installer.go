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
	"sync"
	"syscall"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/globalops"
	integrationpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/integration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/migration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
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
	Configuration    *SetupConfiguration
}

const setupBarrierConcurrency = 4

type Action struct {
	Kind   string `json:"kind"`
	Target string `json:"target"`
	Detail string `json:"detail"`
}

// SetupConfiguration contains only reviewed, non-secret setup values. The
// bearer token is passed separately at apply time and is never serialized.
type SetupConfiguration struct {
	Values             map[string]string            `json:"values"`
	Fingerprint        string                       `json:"tested_fingerprint"`
	Confirmations      []ai.ConfirmationRequirement `json:"confirmations"`
	SourceGeneration   uint64                       `json:"source_generation"`
	CredentialSource   credentials.Source           `json:"credential_source"`
	StoreCredential    bool                         `json:"store_credential"`
	ProviderTestStatus string                       `json:"provider_test_status"`
}

type Plan struct {
	Scope                string                       `json:"scope"`
	Warnings             []string                     `json:"warnings"`
	Mode                 string                       `json:"mode"`
	OperationID          string                       `json:"operation_id"`
	Digest               string                       `json:"digest"`
	ExistingInstall      bool                         `json:"existing_install"`
	RequiresExpected     bool                         `json:"requires_expected_plan"`
	Repo                 string                       `json:"repo"`
	RepositoryID         string                       `json:"repository_id"`
	WorktreeID           string                       `json:"worktree_id"`
	ManagedBinary        string                       `json:"managed_binary"`
	SourceExecutable     string                       `json:"source_executable"`
	Service              supervisor.ServiceDefinition `json:"service"`
	PriorService         ServiceState                 `json:"prior_service"`
	Registry             *central.Registry            `json:"registry"`
	Repositories         []migration.RepositoryPlan   `json:"repositories"`
	DeferredRepositories int                          `json:"deferred_repositories"`
	RecoveryManifests    []string                     `json:"recovery_manifests,omitempty"`
	Actions              []Action                     `json:"actions"`
	FreshDefaults        bool                         `json:"fresh_defaults"`
	Configuration        *SetupConfiguration          `json:"configuration,omitempty"`
	Integrations         []string                     `json:"integrations"`
	IntegrationPlans     []integrationpkg.Plan        `json:"integration_plans"`
	OwnershipDigest      string                       `json:"integration_ownership_digest"`
	BackupRoot           string                       `json:"backup_root"`
	ServiceCheckSkipped  bool                         `json:"-"`
}

type ServiceState struct {
	Installed     bool   `json:"installed"`
	FileDigest    string `json:"file_digest"`
	Loaded        bool   `json:"loaded"`
	Enabled       bool   `json:"enabled"`
	LegacyLoaded  bool   `json:"legacy_loaded,omitempty"`
	SessionLoaded bool   `json:"session_loaded,omitempty"`
}

type ApplyOptions struct {
	Executor   Executor
	Quiesce    func(context.Context) error
	SelfTest   func(context.Context, Plan) error
	Ready      func(context.Context, paths.Roots, *central.Registry) error
	Progress   func(Progress)
	Credential string
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

func (OSExecutor) StartSession(ctx context.Context, roots paths.Roots, service supervisor.ServiceDefinition) error {
	return supervisor.EnsureSession(ctx, roots, service.Binary, service.LogPath)
}

func BuildPlan(ctx context.Context, roots paths.Roots, options Options) (Plan, error) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		return Plan{}, fmt.Errorf("setup: unsupported OS %s", runtime.GOOS)
	}
	var warnings []string
	if repo := strings.TrimSpace(options.Repo); repo != "" {
		warnings = append(warnings,
			fmt.Sprintf("--repo no longer enables a repository during setup; run `acd on --repo %s` after setup", repo))
	}
	executable := options.Executable
	if executable == "" {
		var err error
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
	priorService := inspectServiceState(ctx, roots, service)

	registry, err := central.Load(roots)
	if err != nil {
		return Plan{}, err
	}
	if plan, ok, compatibleErr := buildCompatibleSetupPlan(ctx, roots, options, executable, service, priorService, registry); compatibleErr != nil {
		return Plan{}, compatibleErr
	} else if ok {
		plan.Warnings = warnings
		plan.Digest = digestPlan(plan)
		return plan, nil
	}
	existing := registry.Version < central.RegistryVersion || len(registry.Repos) > 0 || fileExists(roots.ManagedBinaryPath()) || fileExists(service.Path) || fileExists(roots.ConfigPath())
	planned, err := central.PlanRegistryV2(ctx, registry)
	if err != nil && len(registry.Repos) > 0 {
		return Plan{}, err
	}
	if planned == nil {
		planned = central.NewRegistry()
	}
	planned.Version = central.RegistryVersion

	opID, err := newOperationID("setup")
	if err != nil {
		return Plan{}, err
	}
	backupRoot := roots.SetupOperationDir(opID)
	repoPlans := make([]migration.RepositoryPlan, 0, len(planned.Repos))
	deferredRepositories := 0
	for _, record := range planned.Repos {
		if record.LifecycleDisabled() {
			deferredRepositories++
			continue
		}
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
	ownershipDigest, err := currentFileDigest(roots.IntegrationsOwnershipPath())
	if err != nil {
		return Plan{}, err
	}
	configuration := options.Configuration
	if existing {
		configuration = nil
	}
	plan := Plan{
		Scope:       "global",
		Warnings:    warnings,
		Mode:        "full",
		OperationID: opID, ExistingInstall: existing, RequiresExpected: existing,
		ManagedBinary: roots.ManagedBinaryPath(), SourceExecutable: executable, Service: service,
		PriorService: priorService, Registry: planned,
		Repositories: repoPlans, DeferredRepositories: deferredRepositories,
		RecoveryManifests: recoveryManifests,
		FreshDefaults:     !existing, Integrations: integrations,
		Configuration:    configuration,
		IntegrationPlans: integrationPlans, OwnershipDigest: ownershipDigest, BackupRoot: backupRoot,
		ServiceCheckSkipped: options.SkipServiceCheck,
	}
	plan.Actions = []Action{
		{Kind: "backup", Target: backupRoot, Detail: "Back up every existing file and repository database"},
		{Kind: "install_binary", Target: plan.ManagedBinary, Detail: "Atomically copy this ACD executable"},
	}
	if len(repoPlans) > 0 {
		plan.Actions = append(plan.Actions,
			Action{Kind: "migrate", Target: fmt.Sprintf("%d enabled repositories", len(repoPlans)), Detail: "Apply the all-or-nothing checkpoint cutover"})
	}
	if deferredRepositories > 0 {
		plan.Actions = append(plan.Actions, Action{
			Kind: "defer_disabled_migrations", Target: fmt.Sprintf("%d disabled repositories", deferredRepositories),
			Detail: "Keep disabled repository state unchanged until its next acd on",
		})
	}
	plan.Actions = append(plan.Actions,
		Action{Kind: supervisorActionKind(plan.Service), Target: supervisorActionTarget(plan), Detail: supervisorActionDetail(plan.Service)},
		Action{Kind: "write_registry", Target: roots.RegistryPath(), Detail: "Persist common-directory and worktree identities"},
		Action{Kind: "self_test", Target: backupRoot, Detail: "Run isolated checkpoint, publish, and restore verification"},
	)
	if plan.FreshDefaults && plan.Configuration != nil {
		plan.Actions = append(plan.Actions, Action{
			Kind: "save_preferences", Target: roots.ConfigPath(),
			Detail: "Save reviewed user defaults and provider approval",
		})
	}
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
	if plan.Mode == "compatible_upgrade" {
		forceReplacement := false
		for _, integrationPlan := range plan.IntegrationPlans {
			if integrationPlan.Changed {
				forceReplacement = true
				break
			}
		}
		changed, err := ApplyCompatibleRuntime(ctx, roots, RuntimeUpgradeOptions{
			SourceExecutable:             plan.SourceExecutable,
			SourceVersion:                version.String(),
			Compatibility:                RuntimeCompatibility(),
			Integrations:                 strings.Join(plan.Integrations, ","),
			Force:                        forceReplacement,
			AllowUnadvertised:            true,
			AllowSameDistanceReplacement: true,
		})
		return Result{OperationID: plan.OperationID, PlanDigest: plan.Digest, Changed: changed}, err
	}
	lifecycleLock, err := globalops.AcquireUserLock(ctx, roots.OperationsDBPath())
	if err != nil {
		return Result{}, fmt.Errorf("setup: acquire user lifecycle lock: %w", err)
	}
	defer lifecycleLock.Release()
	if options.Executor == nil {
		options.Executor = OSExecutor{}
	}
	emitProgress(options, "prepare", "Preparing the setup transaction")
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
	if err := revalidateSetupPlan(ctx, roots, plan, false); err != nil {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "setup preview became stale before quiescence", true)
		return Result{}, err
	}
	emitProgress(options, "quiesce", "Saving current changes and stopping old background services")
	serviceQuiesced, err := quiesceSetup(ctx, roots, options.Executor, plan.Service, plan.PriorService, options.Quiesce)
	if err != nil {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "setup quiescence failed", true)
		return Result{}, err
	}
	sessionLock, err := acquireSetupSessionFence(ctx, roots, plan.Service, options.Quiesce)
	if err != nil {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "setup session quiescence failed", true)
		if serviceQuiesced {
			err = errors.Join(err, restoreServiceState(context.Background(), roots, options.Executor, plan.Service, plan.PriorService))
		}
		return Result{}, err
	}
	releaseSessionLock := func() {
		if sessionLock != nil {
			sessionLock.Release()
			sessionLock = nil
		}
	}
	restoreQuiescedService := func(cause error) error {
		releaseSessionLock()
		if !serviceQuiesced {
			return cause
		}
		return errors.Join(cause, restoreServiceState(context.Background(), roots, options.Executor, plan.Service, plan.PriorService))
	}
	quiesced := options.Quiesce != nil || serviceQuiesced
	if err := revalidateSetupPlan(ctx, roots, plan, quiesced); err != nil {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "setup preview became stale after quiescence", true)
		return Result{}, restoreQuiescedService(err)
	}
	if err := journal.Advance(ctx, plan.OperationID, "quiesced", "", false); err != nil {
		return Result{}, restoreQuiescedService(err)
	}
	if err := os.MkdirAll(plan.BackupRoot, 0o700); err != nil {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "setup backup directory failed", true)
		return Result{}, restoreQuiescedService(err)
	}

	emitProgress(options, "backup", "Backing up existing ACD files and repository state")
	targets := []string{roots.RegistryPath(), roots.ConfigPath(), roots.IntegrationsOwnershipPath(), plan.ManagedBinary, plan.Service.Path, roots.SetupPublicationHoldPath()}
	for _, item := range plan.IntegrationPlans {
		targets = append(targets, item.Target)
	}
	backups, err := backupFiles(plan.BackupRoot, targets, plan.PriorService)
	if err != nil {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "backup failed", true)
		return Result{}, restoreQuiescedService(err)
	}
	if err := journal.Advance(ctx, plan.OperationID, "backed_up", "", false); err != nil {
		return Result{}, restoreQuiescedService(err)
	}
	var credentialReplacement *credentials.Replacement
	rollbackCredential := func() error {
		if credentialReplacement == nil {
			return nil
		}
		return credentialReplacement.Rollback()
	}
	rollbackFiles := func(cause error) error {
		emitProgress(options, "rollback", "Setup failed; restoring all backed-up state")
		credentialErr := rollbackCredential()
		fileErr := restoreFiles(backups)
		serviceErr := restoreQuiescedService(nil)
		if fileErr != nil || serviceErr != nil || errors.Is(cause, errPostMutationProof) {
			_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "setup rollback incomplete", true)
			emitProgress(options, "needs_attention", "Rollback could not be fully verified; run `acd doctor`")
		} else {
			_ = journal.Advance(context.Background(), plan.OperationID, "rolled_back", "setup rolled back", true)
			emitProgress(options, "rolled_back", "Rollback completed; the previous ACD state is unchanged")
		}
		return errors.Join(cause, credentialErr, fileErr, serviceErr)
	}
	if plan.Configuration != nil && plan.Configuration.StoreCredential {
		if strings.TrimSpace(options.Credential) == "" {
			return Result{}, rollbackFiles(errors.New("setup: reviewed provider credential is missing"))
		}
		credentialReplacement, err = credentials.NewStore(roots).BeginReplacement(options.Credential)
		if err != nil {
			return Result{}, rollbackFiles(err)
		}
	}
	emitProgress(options, "install_binary", "Installing ACD for setup checks")
	managedBody, err := os.ReadFile(plan.SourceExecutable)
	if err != nil {
		return Result{}, rollbackFiles(err)
	}
	if err := preparePostMutation(plan.BackupRoot, backups, plan.PriorService, plan.ManagedBinary, fileDigest(managedBody, 0o755)); err != nil {
		return Result{}, rollbackFiles(err)
	}
	if err := writeAtomic(plan.ManagedBinary, managedBody, 0o755); err != nil {
		return Result{}, rollbackFiles(err)
	}
	holdBody, _ := json.Marshal(map[string]string{"operation_id": plan.OperationID, "plan_digest": plan.Digest})
	holdBody = append(holdBody, '\n')
	if err := preparePostMutation(plan.BackupRoot, backups, plan.PriorService, roots.SetupPublicationHoldPath(), fileDigest(holdBody, 0o600)); err != nil {
		return Result{}, rollbackFiles(err)
	}
	if err := writeAtomic(roots.SetupPublicationHoldPath(), holdBody, 0o600); err != nil {
		return Result{}, rollbackFiles(err)
	}
	activeRepositories := 0
	for _, record := range plan.Registry.Repos {
		if !record.LifecycleDisabled() {
			activeRepositories++
		}
	}
	emitProgress(options, "bridge", fmt.Sprintf("Checking %d enabled repositories and protecting changes during the upgrade", activeRepositories))
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
				unloadService(context.Background(), roots, options.Executor, plan.Service),
			)
		}
		credentialErr := rollbackCredential()
		fileErr := restoreFiles(backups)
		var dbErr error
		if migrationApplied {
			dbErr = migration.Rollback(plan.Repositories)
		}
		releaseSessionLock()
		serviceErr := restoreServiceState(context.Background(), roots, options.Executor, plan.Service, plan.PriorService)
		rollbackErr := errors.Join(credentialErr, stopErr, fileErr, dbErr, serviceErr, manifestErr)
		if rollbackErr != nil || errors.Is(cause, errPostMutationProof) {
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
	emitProgress(options, "migrate", fmt.Sprintf("Upgrading protection data for %d repositories", len(plan.Repositories)))
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
	emitProgress(options, "install", "Installing ACD, its background service, and the repository list")
	plan.Registry.Normalize()
	registryDigest, err := jsonFileDigest(plan.Registry)
	if err != nil {
		return Result{}, rollback(err)
	}
	if err := preparePostMutation(plan.BackupRoot, backups, plan.PriorService, roots.RegistryPath(), registryDigest); err != nil {
		return Result{}, rollback(err)
	}
	if err := central.Save(roots, plan.Registry); err != nil {
		return Result{}, rollback(err)
	}
	prepareConfig := func(body []byte) error {
		return preparePostMutation(plan.BackupRoot, backups, plan.PriorService, roots.ConfigPath(), fileDigest(body, 0o600))
	}
	if err := migrateRepositoryConfigKeys(roots, plan.Registry, prepareConfig); err != nil {
		return Result{}, rollback(err)
	}
	if plan.FreshDefaults {
		if plan.Configuration == nil {
			if err := persistFreshGlobalDefaults(roots, prepareConfig); err != nil {
				return Result{}, rollback(err)
			}
		} else {
			service, serviceErr := settings.NewGlobalService(ctx, settings.Options{Roots: roots})
			if serviceErr != nil {
				return Result{}, rollback(serviceErr)
			}
			_, saveErr := service.SaveGlobalSetup(ctx, settings.SaveGlobalSetupRequest{
				Values: plan.Configuration.Values, TestedFingerprint: plan.Configuration.Fingerprint,
				Confirmations:      plan.Configuration.Confirmations,
				ExpectedGeneration: plan.Configuration.SourceGeneration,
				Prepare:            prepareConfig,
			})
			if saveErr != nil {
				return Result{}, rollback(saveErr)
			}
		}
	}
	serviceDigest := absentFileDigest
	if plan.Service.Platform != "session" {
		serviceDigest = fileDigest(plan.Service.Content, 0o600)
	}
	if err := preparePostMutation(plan.BackupRoot, backups, plan.PriorService, plan.Service.Path, serviceDigest); err != nil {
		return Result{}, rollback(err)
	}
	if plan.Service.Platform == "session" {
		if err := os.Remove(plan.Service.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return Result{}, rollback(err)
		}
	} else if err := writeAtomic(plan.Service.Path, plan.Service.Content, 0o600); err != nil {
		return Result{}, rollback(err)
	}
	emitProgress(options, "service", supervisorStartProgress(plan.Service))
	releaseSessionLock()
	if err := loadService(ctx, roots, options.Executor, plan.Service); err != nil {
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
	readyPhase := "workers"
	if ready == nil {
		ready = func(ctx context.Context, roots paths.Roots, registry *central.Registry) error {
			return waitSetupWorkersWithProgress(ctx, roots, registry, func(ready, total int) {
				emitProgress(options, readyPhase, fmt.Sprintf(
					"Repository workers ready: %d of %d", ready, total))
			}, func(completed, total int, path string) {
				emitProgress(options, readyPhase, fmt.Sprintf(
					"Confirming checkpoint coverage %d of %d: %s", completed, total, path))
			})
		}
	}
	emitProgress(options, "workers", "Waiting for repository workers to report complete protection")
	if err := ready(ctx, roots, plan.Registry); err != nil {
		return Result{}, rollback(err)
	}
	emitProgress(options, "integrations", "Updating coding-tool integrations without changing unrelated settings")
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
			if err := preparePostMutation(plan.BackupRoot, backups, plan.PriorService, item.Target, fileDigest(item.Content, 0o600)); err != nil {
				return Result{}, rollback(err)
			}
			if err := writeAtomic(item.Target, item.Content, 0o600); err != nil {
				return Result{}, rollback(err)
			}
		}
	}
	if len(plan.IntegrationPlans) > 0 {
		if current, err := currentFileDigest(roots.IntegrationsOwnershipPath()); err != nil || current != plan.OwnershipDigest {
			return Result{}, rollback(fmt.Errorf("setup: integration ownership changed after preview"))
		}
		ownershipBody, err := integrationOwnershipContent(roots.IntegrationsOwnershipPath(), plan.IntegrationPlans)
		if err != nil {
			return Result{}, rollback(err)
		}
		if err := preparePostMutation(plan.BackupRoot, backups, plan.PriorService, roots.IntegrationsOwnershipPath(), fileDigest(ownershipBody, 0o600)); err != nil {
			return Result{}, rollback(err)
		}
		if err := integrationpkg.WriteOwnership(roots.IntegrationsOwnershipPath(), plan.IntegrationPlans); err != nil {
			return Result{}, rollback(err)
		}
	}
	if err := journal.Advance(ctx, plan.OperationID, "integrations_merged", "", false); err != nil {
		return Result{}, rollback(err)
	}
	emitProgress(options, "finalize", "Checking for changes made during setup and finishing safely")
	if _, err := bridge.Finalize(ctx); err != nil {
		return Result{}, rollback(err)
	}
	// A barrier after the bridge's final observation proves every edit that
	// could have arrived during cutover is represented by held v20 workers.
	readyPhase = "final_workers"
	emitProgress(options, readyPhase, "Running final checkpoint coverage verification")
	if err := ready(ctx, roots, plan.Registry); err != nil {
		return Result{}, rollback(err)
	}
	if err := journal.Advance(ctx, plan.OperationID, "workers_ready", "", false); err != nil {
		return Result{}, rollback(err)
	}
	if err := journal.Advance(ctx, plan.OperationID, "needs_attention", "setup final cleanup pending", false); err != nil {
		return Result{}, rollback(err)
	}
	if credentialReplacement != nil {
		if err := credentialReplacement.Commit(); err != nil {
			_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "finish protected credential transaction", true)
			return Result{}, fmt.Errorf("setup applied but credential cleanup needs attention: %w", err)
		}
	}
	if err := preparePostMutation(plan.BackupRoot, backups, plan.PriorService, roots.SetupPublicationHoldPath(), absentFileDigest); err != nil {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "record publication hold cleanup", true)
		return Result{}, fmt.Errorf("setup applied but publication hold cleanup proof needs attention: %w", err)
	}
	if err := os.Remove(roots.SetupPublicationHoldPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "remove publication hold", true)
		return Result{}, fmt.Errorf("setup applied but publication remains held: %w; run `acd support repair`", err)
	}
	if err := bridge.Cleanup(ctx); err != nil {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "cleanup migration bridge refs", true)
		return Result{}, fmt.Errorf("setup applied but migration bridge cleanup needs attention: %w", err)
	}
	if err := migration.CleanupBridgeRecoveryManifests(ctx, plan.RecoveryManifests); err != nil {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "cleanup retained migration recovery refs", true)
		return Result{}, fmt.Errorf("setup applied but retained migration recovery cleanup needs attention: %w", err)
	}
	if err := journal.Advance(ctx, plan.OperationID, "committed", "", true); err != nil {
		return Result{}, fmt.Errorf("setup cleanup completed but final journal commit needs attention: %w", err)
	}
	emitProgress(options, "completed", "Setup completed")
	return Result{OperationID: plan.OperationID, PlanDigest: plan.Digest, Migrations: migrations, Changed: true}, nil
}

func emitProgress(options ApplyOptions, phase, detail string) {
	if options.Progress != nil {
		options.Progress(Progress{Phase: phase, Detail: detail})
	}
}

func migrateRepositoryConfigKeys(roots paths.Roots, registry *central.Registry, prepare func([]byte) error) error {
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
	mutate := func(document *config.Document) error {
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
	}
	return updateConfigPrepared(store, document, mutate, prepare)
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
	PostDigest string      `json:"post_digest,omitempty"`
}

type backupManifest struct {
	Files        []fileBackup `json:"files"`
	PriorService ServiceState `json:"prior_service"`
}

const absentFileDigest = "absent"

var errPostMutationProof = errors.New("setup: post-mutation state was not durably recorded")

func backupFiles(root string, targets []string, priorService ServiceState) ([]fileBackup, error) {
	var backups []fileBackup
	for i, target := range targets {
		item := fileBackup{Target: target}
		info, err := os.Lstat(target)
		if errors.Is(err, os.ErrNotExist) {
			item.Digest = absentFileDigest
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
		item.Digest = fileDigest(body, info.Mode())
		item.Backup = filepath.Join(root, "files", fmt.Sprintf("%04d", i))
		if err := writeAtomic(item.Backup, body, 0o600); err != nil {
			return nil, err
		}
		backups = append(backups, item)
	}
	if err := persistBackupManifest(root, backups, priorService); err != nil {
		return nil, err
	}
	return backups, nil
}

func restoreFiles(backups []fileBackup) error {
	var combined error
	for _, item := range backups {
		if item.PostDigest == "" {
			continue
		}
		if err := restoreFileCAS(item); err != nil {
			combined = errors.Join(combined, err)
		}
	}
	return combined
}

func restoreFileCAS(item fileBackup) error {
	claimed, err := claimRollbackTarget(item.Target)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if !item.Exists {
				return nil
			}
			if item.PostDigest == absentFileDigest {
				return installBackupNoReplace(item)
			}
			return fmt.Errorf("setup: preserve concurrent edit at %s; operation output disappeared", item.Target)
		}
		return err
	}
	claimedDigest, digestErr := currentFileDigest(claimed)
	if digestErr != nil || (claimedDigest != item.PostDigest && claimedDigest != item.Digest) {
		restoreErr := restoreClaimNoReplace(claimed, item.Target)
		return errors.Join(fmt.Errorf("setup: preserve concurrent edit at %s; current state no longer matches operation output", item.Target), digestErr, restoreErr)
	}
	if claimedDigest == item.Digest {
		return restoreClaimNoReplace(claimed, item.Target)
	}
	if !item.Exists {
		if err := os.Remove(claimed); err != nil {
			return err
		}
		if _, err := os.Lstat(item.Target); err == nil {
			return fmt.Errorf("setup: preserve concurrent edit at %s created during rollback", item.Target)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if err := installBackupNoReplace(item); err != nil {
		_ = os.Remove(claimed)
		return err
	}
	return os.Remove(claimed)
}

func claimRollbackTarget(target string) (string, error) {
	tmp, err := os.CreateTemp(filepath.Dir(target), ".acd-rollback-claim-")
	if err != nil {
		return "", err
	}
	claimed := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(claimed)
		return "", err
	}
	if err := os.Remove(claimed); err != nil {
		return "", err
	}
	if err := os.Rename(target, claimed); err != nil {
		return "", err
	}
	return claimed, nil
}

func restoreClaimNoReplace(claimed, target string) error {
	if err := os.Link(claimed, target); err != nil {
		return fmt.Errorf("setup: concurrent edit occupied %s; retained displaced content at %s: %w", target, claimed, err)
	}
	return os.Remove(claimed)
}

func installBackupNoReplace(item fileBackup) error {
	body, err := os.ReadFile(item.Backup)
	if err != nil {
		return err
	}
	if fileDigest(body, item.Mode) != item.Digest {
		return fmt.Errorf("setup: backup digest mismatch for %s", item.Target)
	}
	tmp, err := os.CreateTemp(filepath.Dir(item.Target), ".acd-rollback-restore-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return err
	}
	if item.OwnerKnown {
		if err := tmp.Chown(int(item.UID), int(item.GID)); err != nil {
			tmp.Close()
			return fmt.Errorf("setup: restore owner for %s: %w", item.Target, err)
		}
	}
	if err := tmp.Chmod(fileModeBits(item.Mode)); err != nil {
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
	if err := os.Link(name, item.Target); err != nil {
		return fmt.Errorf("setup: preserve concurrent edit at %s created during rollback: %w", item.Target, err)
	}
	dir, err := os.Open(filepath.Dir(item.Target))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func preparePostMutation(root string, backups []fileBackup, priorService ServiceState, target, postDigest string) error {
	if postDigest == "" {
		return fmt.Errorf("%w: empty post-mutation digest for %s", errPostMutationProof, target)
	}
	found := false
	for index := range backups {
		if filepath.Clean(backups[index].Target) == filepath.Clean(target) {
			backups[index].PostDigest = postDigest
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("%w: mutation target was not backed up: %s", errPostMutationProof, target)
	}
	if err := persistBackupManifest(root, backups, priorService); err != nil {
		return fmt.Errorf("%w: persist manifest: %v", errPostMutationProof, err)
	}
	return nil
}

func currentFileDigest(path string) (string, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return absentFileDigest, nil
	}
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("setup: mutation target is not a regular file: %s", path)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return fileDigest(body, info.Mode()), nil
}

func jsonFileDigest(value any) (string, error) {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "", err
	}
	return fileDigest(append(body, '\n'), 0o600), nil
}

func persistBackupManifest(root string, backups []fileBackup, priorService ServiceState) error {
	body, _ := json.MarshalIndent(backupManifest{Files: backups, PriorService: priorService}, "", "  ")
	return writeAtomic(filepath.Join(root, "backup-manifest.json"), append(body, '\n'), 0o600)
}

func persistFreshDefaults(roots paths.Roots, repositoryID string, prepareCallbacks ...func([]byte) error) error {
	store := config.NewStore(roots)
	document, err := store.Load()
	if err != nil {
		return err
	}
	mutate := func(document *config.Document) error {
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
	}
	var prepare func([]byte) error
	if len(prepareCallbacks) > 0 {
		prepare = prepareCallbacks[0]
	}
	return updateConfigPrepared(store, document, mutate, prepare)
}

func persistFreshGlobalDefaults(roots paths.Roots, prepareCallbacks ...func([]byte) error) error {
	store := config.NewStore(roots)
	document, err := store.Load()
	if err != nil {
		return err
	}
	mutate := func(document *config.Document) error {
		values := map[string]any{
			config.FieldProvider: "deterministic", config.FieldCommitStrategy: "intent", config.FieldCommitPreset: "fast",
			config.FieldIntentVerification: "structural", config.FieldIntentRepairEnabled: false, config.FieldDiffEgress: false,
		}
		for key, value := range values {
			body, _ := json.Marshal(value)
			document.Settings.Global[key] = body
		}
		return nil
	}
	var prepare func([]byte) error
	if len(prepareCallbacks) > 0 {
		prepare = prepareCallbacks[0]
	}
	return updateConfigPrepared(store, document, mutate, prepare)
}

func updateConfigPrepared(store *config.Store, snapshot *config.Document, mutate func(*config.Document) error, prepare func([]byte) error) error {
	snapshotBody, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	planned, err := config.ParseDocument(snapshotBody)
	if err != nil {
		return err
	}
	if err := mutate(planned); err != nil {
		return err
	}
	planned.Generation++
	if err := config.ValidateDocument(planned); err != nil {
		return err
	}
	body, err := json.MarshalIndent(planned, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	if prepare != nil {
		if err := prepare(body); err != nil {
			return err
		}
	}
	return store.UpdateExpected(snapshot.Generation, mutate)
}

func integrationOwnershipContent(path string, plans []integrationpkg.Plan) ([]byte, error) {
	document := integrationpkg.Ownership{Version: 1, Entries: map[string]integrationpkg.OwnedEntry{}}
	if body, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(body, &document); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if document.Entries == nil {
		document.Entries = map[string]integrationpkg.OwnedEntry{}
	}
	for _, plan := range plans {
		document.Entries[plan.Target] = integrationpkg.OwnedEntry{
			Name: plan.Name, Digest: plan.AfterDigest, TemplateVersion: integrationpkg.TemplateVersion,
			Signatures: []string{"acd internal "}, Format: plan.Format,
			Elements: append([]integrationpkg.OwnedElement(nil), plan.Owned...),
		}
	}
	body, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(body, '\n'), nil
}

func supervisorActionKind(service supervisor.ServiceDefinition) string {
	if service.Platform == "session" {
		return "start_session_supervisor"
	}
	return "install_service"
}

func supervisorActionTarget(plan Plan) string {
	if plan.Service.Platform == "session" {
		return plan.ManagedBinary
	}
	return plan.Service.Path
}

func supervisorActionDetail(service supervisor.ServiceDefinition) string {
	if service.Platform == "session" {
		return "Start the supervisor from this authorized macOS session without Full Disk Access"
	}
	return "Install the user supervisor"
}

func supervisorStartProgress(service supervisor.ServiceDefinition) string {
	if service.Platform == "session" {
		return "Starting the repository-scoped macOS session supervisor"
	}
	return "Starting the user supervisor"
}

func loadService(ctx context.Context, roots paths.Roots, executor Executor, service supervisor.ServiceDefinition) error {
	if service.Platform == "session" {
		starter, ok := executor.(interface {
			StartSession(context.Context, paths.Roots, supervisor.ServiceDefinition) error
		})
		if !ok {
			return errors.New("setup: executor cannot start a macOS session supervisor")
		}
		return starter.StartSession(ctx, roots, service)
	}
	if service.Platform == "launchd" {
		return executor.Run(ctx, "launchctl", "bootstrap", fmt.Sprintf("gui/%d", os.Getuid()), service.Path)
	}
	if err := executor.Run(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		return err
	}
	return executor.Run(ctx, "systemctl", "--user", "enable", "--now", "acd-supervisor.service")
}
func unloadService(ctx context.Context, roots paths.Roots, executor Executor, service supervisor.ServiceDefinition) error {
	if service.Platform == "session" {
		shutdownErr := shutdownSupervisor(ctx, roots)
		legacyErr := unloadLegacyLaunchd(ctx, executor)
		return errors.Join(shutdownErr, legacyErr)
	}
	if service.Platform == "launchd" {
		return executor.Run(ctx, "launchctl", "bootout", fmt.Sprintf("gui/%d", os.Getuid()), service.Path)
	}
	return executor.Run(ctx, "systemctl", "--user", "disable", "--now", "acd-supervisor.service")
}

func unloadLegacyLaunchd(ctx context.Context, executor Executor) error {
	target := fmt.Sprintf("gui/%d/%s", os.Getuid(), supervisor.ServiceLabel)
	if exec.CommandContext(ctx, "launchctl", "print", target).Run() != nil {
		return nil
	}
	return executor.Run(ctx, "launchctl", "bootout", target)
}

func quiesceSetup(
	ctx context.Context,
	roots paths.Roots,
	executor Executor,
	service supervisor.ServiceDefinition,
	prior ServiceState,
	stopWorkers func(context.Context) error,
) (bool, error) {
	if prior.Loaded {
		if err := unloadService(ctx, roots, executor, service); err != nil {
			return false, errors.Join(err,
				restoreServiceState(context.Background(), roots, executor, service, prior))
		}
	}
	if stopWorkers != nil {
		if err := stopWorkers(ctx); err != nil {
			if !prior.Loaded {
				return false, err
			}
			return false, errors.Join(err,
				restoreServiceState(context.Background(), roots, executor, service, prior))
		}
	}
	return prior.Loaded, nil
}

func acquireSetupSessionFence(
	ctx context.Context,
	roots paths.Roots,
	service supervisor.ServiceDefinition,
	stopWorkers func(context.Context) error,
) (*supervisor.SessionLifecycleLock, error) {
	if service.Platform != "session" {
		return nil, nil
	}
	lock, err := supervisor.AcquireSessionLifecycleLock(ctx, roots)
	if err != nil {
		return nil, err
	}
	// A hook may have started a supervisor between the initial shutdown and
	// lock acquisition. Recheck shutdown while starts are fenced.
	if err := shutdownSupervisor(ctx, roots); err != nil {
		lock.Release()
		return nil, err
	}
	if stopWorkers != nil {
		if err := stopWorkers(ctx); err != nil {
			lock.Release()
			return nil, err
		}
	}
	return lock, nil
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

func inspectServiceState(ctx context.Context, roots paths.Roots, service supervisor.ServiceDefinition) ServiceState {
	digest, err := currentFileDigest(service.Path)
	if err != nil {
		digest = "invalid"
	}
	result := ServiceState{Installed: fileExists(service.Path), FileDigest: digest}
	if service.Platform == "session" {
		result.SessionLoaded = supervisorSessionRunning(ctx, roots)
		result.LegacyLoaded = exec.CommandContext(ctx, "launchctl", "print", fmt.Sprintf("gui/%d/%s", os.Getuid(), supervisor.ServiceLabel)).Run() == nil
		result.Loaded = result.SessionLoaded || result.LegacyLoaded
		result.Enabled = result.SessionLoaded
		return result
	}
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

func restoreServiceState(ctx context.Context, roots paths.Roots, executor Executor, service supervisor.ServiceDefinition, prior ServiceState) error {
	if service.Platform == "session" {
		var combined error
		if prior.LegacyLoaded {
			combined = errors.Join(combined, executor.Run(ctx, "launchctl", "bootstrap", fmt.Sprintf("gui/%d", os.Getuid()), service.Path))
		}
		if prior.SessionLoaded {
			combined = errors.Join(combined, loadService(ctx, roots, executor, service))
		}
		return combined
	}
	if service.Platform == "launchd" {
		if prior.Loaded {
			return loadService(ctx, roots, executor, service)
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

func supervisorSessionRunning(ctx context.Context, roots paths.Roots) bool {
	requestCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	response, err := (&supervisor.Client{
		SocketPath: roots.SupervisorSocketPath(), Timeout: 300 * time.Millisecond,
	}).Do(requestCtx, supervisor.Request{Version: supervisor.ProtocolVersion, ID: "setup-session-state", Method: "status"})
	return err == nil && response.Error == nil
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
					if workerErr == nil && workerReadinessMatches(workerResponse, worker) {
						return nil
					}
				}
			}
		}
	}
}

func waitSetupWorkers(ctx context.Context, roots paths.Roots, registry *central.Registry) error {
	return waitSetupWorkersWithProgress(ctx, roots, registry, nil, nil)
}

func waitSetupWorkersWithProgress(
	ctx context.Context,
	roots paths.Roots,
	registry *central.Registry,
	progress func(ready, total int),
	barrierProgress func(completed, total int, path string),
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
	if err := waitSupervisorWorkersReady(ctx, roots, repositories, 15*time.Minute, progress); err != nil {
		return err
	}
	client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: supervisor.CheckpointBarrierTimeout}
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
	managedDigest, err := version.FileDigest(roots.ManagedBinaryPath())
	if err != nil {
		return fmt.Errorf("setup: digest managed binary: %w", err)
	}
	if status.BinaryDigest != managedDigest || !status.Compatibility.Equal(RuntimeCompatibility()) {
		return errors.New("setup: supervisor did not advertise the installed runtime compatibility contract")
	}
	enabledRecords := make([]central.RepoRecord, 0, len(registry.Repos))
	for _, record := range registry.Repos {
		if !record.LifecycleDisabled() {
			enabledRecords = append(enabledRecords, record)
		}
	}
	groups := groupSetupBarrierRecords(enabledRecords)
	barrierCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan []central.RepoRecord, len(groups))
	for _, group := range groups {
		jobs <- group
	}
	close(jobs)
	var (
		workers    sync.WaitGroup
		firstErr   error
		errOnce    sync.Once
		progressMu sync.Mutex
		started    int
	)
	workerCount := min(setupBarrierConcurrency, len(groups))
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for group := range jobs {
				for _, record := range group {
					if barrierCtx.Err() != nil {
						return
					}
					progressMu.Lock()
					started++
					position := started
					progressMu.Unlock()
					if barrierProgress != nil {
						barrierProgress(position, len(enabledRecords), record.Path)
					}
					if err := confirmSetupCheckpointCoverage(barrierCtx, client, record); err != nil {
						errOnce.Do(func() {
							firstErr = err
							cancel()
						})
						return
					}
				}
			}
		}()
	}
	workers.Wait()
	return firstErr
}

func confirmSetupCheckpointCoverage(ctx context.Context, client supervisor.Client, record central.RepoRecord) error {
	response, callErr := client.Do(ctx, supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: "setup-barrier-" + record.WorktreeID,
		Method: "checkpoint_barrier", RepositoryID: record.RepositoryID,
		WorktreeID: record.WorktreeID, DeadlineMS: time.Now().Add(supervisor.CheckpointBarrierTimeout).UnixMilli(),
	})
	if callErr != nil {
		return fmt.Errorf("setup: worktree %s checkpoint barrier: %w", record.Path, callErr)
	}
	if response.Error != nil {
		return fmt.Errorf("setup: worktree %s checkpoint barrier: %s", record.Path, response.Error.Message)
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
	return nil
}

func groupSetupBarrierRecords(records []central.RepoRecord) [][]central.RepoRecord {
	byRepository := make(map[string]int)
	groups := make([][]central.RepoRecord, 0, len(records))
	for _, record := range records {
		if index, ok := byRepository[record.RepositoryID]; ok {
			groups[index] = append(groups[index], record)
			continue
		}
		byRepository[record.RepositoryID] = len(groups)
		groups = append(groups, []central.RepoRecord{record})
	}
	return groups
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
			if !pending[worker.RepositoryID] || worker.State != "running" || worker.PID <= 0 {
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
			if !workerReadinessMatches(workerResponse, worker) {
				lastProbeError[worker.RepositoryID] = "worker readiness identity mismatch"
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

func workerReadinessMatches(response supervisor.Response, worker supervisor.WorkerStatus) bool {
	if !response.OK || response.Version != supervisor.ProtocolVersion || response.Error != nil {
		return false
	}
	readiness, err := decode[supervisor.WorkerReadiness](response.Data)
	return err == nil && readiness.Ready && readiness.PID == worker.PID &&
		readiness.RepositoryID == worker.RepositoryID
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
	if plan.Configuration != nil {
		configurationCopy := *plan.Configuration
		configurationCopy.CredentialSource = "reviewed_credential"
		configurationCopy.StoreCredential = false
		copy.Configuration = &configurationCopy
	}
	if plan.Registry != nil {
		registryCopy := *plan.Registry
		registryCopy.Repos = append([]central.RepoRecord(nil), plan.Registry.Repos...)
		copy.Registry = &registryCopy
	}
	copy.Repositories = append([]migration.RepositoryPlan(nil), plan.Repositories...)
	copy.Actions = append([]Action(nil), plan.Actions...)
	copy.Warnings = nil
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

func fileDigest(body []byte, mode os.FileMode) string {
	return sha256String([]byte(fmt.Sprintf("mode:%#o\x00%s", fileModeBits(mode), body)))
}

func fileModeBits(mode os.FileMode) os.FileMode {
	return mode.Perm() | mode&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky)
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
