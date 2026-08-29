package installer

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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/globalops"
	integrationpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/integration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/migration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version"
)

// RuntimeUpgradeOptions describes a compatible managed-runtime replacement.
// Additive state migrations are backed up and rolled back with the binary.
type RuntimeUpgradeOptions struct {
	SourceExecutable string
	SourceVersion    string
	Compatibility    supervisor.Compatibility
	Integrations     string
	Force            bool
	// AllowSameDistanceReplacement is reserved for explicit setup. It lets a
	// user replace one development build with another after local history was
	// amended, while automatic lifecycle upgrades remain order-safe.
	AllowSameDistanceReplacement bool
	// AllowUnadvertised permits a strictly older runtime to prove it supports
	// the checkpoint barrier before any binary replacement. A failed probe
	// leaves the old runtime and every repository untouched.
	AllowUnadvertised bool
}

func RuntimeCompatibility() supervisor.Compatibility {
	return supervisor.Compatibility{
		ProtocolVersion:    supervisor.ProtocolVersion,
		RegistryVersion:    central.RegistryVersion,
		StateSchemaVersion: state.SchemaVersion,
		IntegrationVersion: integrationpkg.TemplateVersion,
	}
}

// CanUpgradeRuntimeCompatibility permits a checkpoint-first managed-runtime
// handoff when the wire and registry contracts are unchanged and the source
// only moves durable schemas or managed integrations forward. The old worker
// checkpoints with its own schema before replacement; only the new worker may
// open and migrate repository databases afterward.
func CanUpgradeRuntimeCompatibility(
	running supervisor.Compatibility,
	target supervisor.Compatibility,
) bool {
	return running != (supervisor.Compatibility{}) &&
		target != (supervisor.Compatibility{}) &&
		running.ProtocolVersion == target.ProtocolVersion &&
		running.RegistryVersion == target.RegistryVersion &&
		state.CanRuntimeMigrate(
			running.StateSchemaVersion, target.StateSchemaVersion) &&
		running.IntegrationVersion > 0 &&
		running.IntegrationVersion <= target.IntegrationVersion
}

func CanProbeUnadvertisedRuntime(
	status supervisor.Status,
	sourceVersion string,
) bool {
	if status.Compatibility != (supervisor.Compatibility{}) {
		return false
	}
	order, comparable := version.Compare(sourceVersion, status.Version)
	return comparable && order > 0
}

func buildCompatibleSetupPlan(
	ctx context.Context,
	roots paths.Roots,
	options Options,
	executable string,
	service supervisor.ServiceDefinition,
	priorService ServiceState,
	registry *central.Registry,
) (Plan, bool, error) {
	if runtime.GOOS != "darwin" || service.Platform != "session" || registry.Version != central.RegistryVersion {
		return Plan{}, false, nil
	}
	status, err := runtimeStatus(ctx, roots)
	if err != nil || (!status.Compatibility.Equal(RuntimeCompatibility()) &&
		!CanUpgradeRuntimeCompatibility(status.Compatibility, RuntimeCompatibility()) &&
		!CanProbeUnadvertisedRuntime(status, version.String())) {
		return Plan{}, false, nil
	}
	sourceDigest, err := version.FileDigest(executable)
	if err != nil {
		return Plan{}, false, err
	}
	if _, err := shouldUpgradeRuntime(status, sourceDigest, RuntimeUpgradeOptions{
		SourceExecutable:             executable,
		SourceVersion:                version.String(),
		Compatibility:                RuntimeCompatibility(),
		Force:                        true,
		AllowUnadvertised:            true,
		AllowSameDistanceReplacement: true,
	}); err != nil {
		return Plan{}, false, err
	}
	integrationPlans, err := integrationpkg.BuildPlans(roots, options.Integrations)
	if err != nil {
		return Plan{}, false, err
	}
	integrations := make([]string, 0, len(integrationPlans))
	integrationChanged := false
	for _, item := range integrationPlans {
		integrations = append(integrations, item.Name)
		if item.Changed {
			integrationChanged = true
		}
	}
	opID, err := newOperationID("setup-compatible")
	if err != nil {
		return Plan{}, false, err
	}
	ownershipDigest, err := currentFileDigest(roots.IntegrationsOwnershipPath())
	if err != nil {
		return Plan{}, false, err
	}
	plan := Plan{
		Scope: "global", Mode: "compatible_upgrade", OperationID: opID, ExistingInstall: true,
		RequiresExpected: true, ManagedBinary: roots.ManagedBinaryPath(),
		SourceExecutable: executable, Service: service, PriorService: priorService,
		Registry: registry, Integrations: integrations, IntegrationPlans: integrationPlans,
		OwnershipDigest: ownershipDigest, BackupRoot: roots.SetupOperationDir(opID),
		ServiceCheckSkipped: options.SkipServiceCheck,
	}
	runtimeChanged := status.Version != version.String() ||
		status.BinaryDigest != sourceDigest ||
		!status.Compatibility.Equal(RuntimeCompatibility())
	if !runtimeChanged && !integrationChanged {
		plan.RequiresExpected = false
		plan.Actions = []Action{{Kind: "verify_compatible_runtime", Target: plan.ManagedBinary, Detail: "The managed runtime and hooks are already current"}}
	} else {
		plan.Actions = []Action{
			{Kind: "backup", Target: plan.BackupRoot, Detail: "Back up the managed binary and integration files"},
			{Kind: "checkpoint", Target: "enabled repositories", Detail: "Protect repository changes at a safe checkpoint boundary"},
			{Kind: "install_binary", Target: plan.ManagedBinary, Detail: "Atomically replace the compatible ACD runtime"},
		}
		for _, name := range integrations {
			plan.Actions = append(plan.Actions, Action{Kind: "merge_integration", Target: name, Detail: "Merge compatible ACD hook updates"})
		}
		plan.Actions = append(plan.Actions, Action{Kind: "restart_session_supervisor", Target: plan.ManagedBinary, Detail: "Restart the shared per-user macOS supervisor"})
	}
	plan.Digest = digestPlan(plan)
	return plan, true, nil
}

// ApplyCompatibleRuntime replaces a compatible macOS session runtime and its
// managed hook templates. It returns changed=false when the running runtime is
// already current or newer than the caller.
func ApplyCompatibleRuntime(ctx context.Context, roots paths.Roots, options RuntimeUpgradeOptions) (bool, error) {
	if runtime.GOOS != "darwin" {
		return false, errors.New("compatible runtime replacement is currently available only for the macOS session supervisor")
	}
	if options.SourceExecutable == "" || options.SourceVersion == "" || options.Compatibility == (supervisor.Compatibility{}) {
		return false, errors.New("runtime upgrade: incomplete source compatibility")
	}
	sourceDigest, err := version.FileDigest(options.SourceExecutable)
	if err != nil {
		return false, fmt.Errorf("runtime upgrade: digest source executable: %w", err)
	}
	status, err := runtimeStatus(ctx, roots)
	if err != nil {
		return false, err
	}
	upgrade, err := shouldUpgradeRuntime(status, sourceDigest, options)
	if err != nil || !upgrade {
		return false, err
	}

	userLock, err := globalops.AcquireUserLock(ctx, roots.OperationsDBPath())
	if err != nil {
		return false, fmt.Errorf("runtime upgrade: acquire user lifecycle lock: %w", err)
	}
	defer userLock.Release()
	sessionLock, err := supervisor.AcquireSessionLifecycleLock(ctx, roots)
	if err != nil {
		return false, err
	}
	locked := true
	defer func() {
		if locked {
			sessionLock.Release()
		}
	}()

	status, err = runtimeStatus(ctx, roots)
	if err != nil {
		return false, err
	}
	upgrade, err = shouldUpgradeRuntime(status, sourceDigest, options)
	if err != nil || !upgrade {
		return false, err
	}
	registry, err := central.Load(roots)
	if err != nil {
		return false, err
	}
	if registry.Version != options.Compatibility.RegistryVersion {
		return false, fmt.Errorf("runtime upgrade: registry v%d requires full `acd setup`", registry.Version)
	}
	integrationSelection := options.Integrations
	if integrationSelection == "" {
		integrationSelection = "auto"
	}
	integrationPlans, err := integrationpkg.BuildPlans(roots, integrationSelection)
	if err != nil {
		return false, err
	}
	opID, err := newOperationID("runtime-upgrade")
	if err != nil {
		return false, err
	}
	backupRoot := roots.SetupOperationDir(opID)
	targets := []string{roots.ManagedBinaryPath(), roots.IntegrationsOwnershipPath()}
	for _, plan := range integrationPlans {
		targets = append(targets, plan.Target)
	}
	if err := os.MkdirAll(backupRoot, 0o700); err != nil {
		return false, err
	}
	backups, err := backupFiles(backupRoot, targets, ServiceState{SessionLoaded: true, Loaded: true, Enabled: true})
	if err != nil {
		return false, err
	}
	planDigest := runtimeUpgradeDigest(status, sourceDigest, integrationPlans)
	journal, err := globalops.Open(ctx, roots.OperationsDBPath())
	if err != nil {
		return false, err
	}
	defer journal.Close()
	steps := []globalops.Step{
		{Sequence: 1, Kind: "checkpoint", Target: "enabled repositories", Phase: "planned"},
		{Sequence: 2, Kind: "backup_state", Target: "enabled repositories", Phase: "planned"},
		{Sequence: 3, Kind: "install_binary", Target: roots.ManagedBinaryPath(), Phase: "planned"},
		{Sequence: 4, Kind: "merge_integrations", Target: roots.IntegrationsOwnershipPath(), Phase: "planned"},
		{Sequence: 5, Kind: "restart_supervisor", Target: roots.SupervisorSocketPath(), Phase: "planned"},
	}
	if err := journal.Prepare(ctx, globalops.Operation{ID: opID, Kind: "runtime_upgrade", Phase: "planned", PlanDigest: planDigest}, steps); err != nil {
		return false, err
	}
	var stateBackups []migration.RepositoryPlan
	rollback := func(cause error) error {
		if !locked {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			var lockErr error
			sessionLock, lockErr = supervisor.AcquireSessionLifecycleLock(rollbackCtx, roots)
			cancel()
			if lockErr != nil {
				_ = journal.Advance(context.Background(), opID, "needs_attention", "compatible runtime rollback could not fence session startup", true)
				return errors.Join(cause, lockErr)
			}
			locked = true
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		shutdownErr := shutdownSupervisor(shutdownCtx, roots)
		cancel()
		stateErr := migration.Rollback(stateBackups)
		fileErr := restoreFiles(backups)
		if locked {
			sessionLock.Release()
			locked = false
		}
		restartErr := supervisor.EnsureSession(context.Background(), roots, roots.ManagedBinaryPath(), roots.SupervisorLogPath())
		if shutdownErr != nil || stateErr != nil || fileErr != nil || restartErr != nil {
			_ = journal.Advance(context.Background(), opID, "needs_attention", "compatible runtime rollback incomplete", true)
		} else {
			_ = journal.Advance(context.Background(), opID, "rolled_back", cause.Error(), true)
		}
		return errors.Join(cause, shutdownErr, stateErr, fileErr, restartErr)
	}

	if err := checkpointUpgradeRepositories(ctx, roots, registry); err != nil {
		_ = journal.Advance(context.Background(), opID, "rolled_back", "checkpoint barrier failed before runtime mutation", true)
		return false, err
	}
	if err := journal.Advance(ctx, opID, "checkpointed", "", false); err != nil {
		return false, err
	}
	if err := shutdownSupervisor(ctx, roots); err != nil {
		return false, rollback(err)
	}
	if err := waitForSupervisorStopped(ctx, roots.SupervisorSocketPath()); err != nil {
		return false, rollback(err)
	}
	stateBackups, err = backupCompatibleRuntimeState(ctx, registry, backupRoot,
		options.Compatibility.StateSchemaVersion)
	if err != nil {
		return false, rollback(err)
	}
	if err := journal.Advance(ctx, opID, "state_backed_up", "", false); err != nil {
		return false, rollback(err)
	}
	sourceBody, err := os.ReadFile(options.SourceExecutable)
	if err != nil {
		return false, rollback(err)
	}
	if err := writeAtomic(roots.ManagedBinaryPath(), sourceBody, 0o755); err != nil {
		return false, rollback(err)
	}
	for _, plan := range integrationPlans {
		if !plan.Changed {
			continue
		}
		if err := writeAtomic(plan.Target, plan.Content, 0o600); err != nil {
			return false, rollback(err)
		}
	}
	if len(integrationPlans) > 0 {
		if err := integrationpkg.WriteOwnership(roots.IntegrationsOwnershipPath(), integrationPlans); err != nil {
			return false, rollback(err)
		}
	}
	if err := journal.Advance(ctx, opID, "installed", "", false); err != nil {
		return false, rollback(err)
	}
	sessionLock.Release()
	locked = false
	if err := supervisor.EnsureSession(ctx, roots, roots.ManagedBinaryPath(), roots.SupervisorLogPath()); err != nil {
		return false, rollback(err)
	}
	ready, err := runtimeStatus(ctx, roots)
	if err != nil || ready.Version != options.SourceVersion || ready.BinaryDigest != sourceDigest || !ready.Compatibility.Equal(options.Compatibility) {
		if err == nil {
			err = fmt.Errorf("runtime upgrade: restarted supervisor did not report the installed compatibility contract")
		}
		return false, rollback(err)
	}
	// Worker startup is admission-limited because each repository performs a
	// protection scan. Match full setup's bounded readiness window so a user
	// with dozens of repositories does not get a false rollback after one
	// minute while healthy workers are still starting in turn.
	readyCtx, readyCancel := context.WithTimeout(ctx, supervisor.CheckpointBarrierTimeout)
	err = waitForCompatibleRuntimeWorkers(readyCtx, roots, registry)
	readyCancel()
	if err != nil {
		return false, rollback(err)
	}
	if err := journal.Advance(ctx, opID, "committed", "", true); err != nil {
		return false, err
	}
	return true, nil
}

func backupCompatibleRuntimeState(
	ctx context.Context,
	registry *central.Registry,
	backupRoot string,
	targetVersion int,
) ([]migration.RepositoryPlan, error) {
	if targetVersion != state.SchemaVersion {
		return nil, fmt.Errorf("runtime upgrade: target state schema v%d does not match this binary's v%d",
			targetVersion, state.SchemaVersion)
	}
	records := append([]central.RepoRecord(nil), registry.Repos...)
	sort.Slice(records, func(i, j int) bool { return records[i].StateDB < records[j].StateDB })
	seen := make(map[string]bool)
	plans := make([]migration.RepositoryPlan, 0)
	for _, record := range records {
		if record.LifecycleDisabled() || record.StateDB == "" || seen[record.StateDB] {
			continue
		}
		seen[record.StateDB] = true
		version, err := state.ReadUserVersion(ctx, record.StateDB)
		if err != nil {
			return plans, fmt.Errorf("runtime upgrade: inspect state for %s: %w", record.Path, err)
		}
		if version == targetVersion {
			continue
		}
		if !state.CanRuntimeMigrate(version, targetVersion) {
			return plans, fmt.Errorf("runtime upgrade: %s uses state schema v%d; run `acd setup` to upgrade it safely",
				record.Path, version)
		}
		digest := sha256.Sum256([]byte(record.StateDB))
		backupPath := filepath.Join(backupRoot,
			"state-"+hex.EncodeToString(digest[:8])+".db")
		if err := state.BackupDatabase(ctx, record.StateDB, backupPath); err != nil {
			return plans, fmt.Errorf("runtime upgrade: back up state for %s: %w", record.Path, err)
		}
		plans = append(plans, migration.RepositoryPlan{
			Record: record, FromVersion: version, BackupPath: backupPath,
		})
	}
	return plans, nil
}

func waitForCompatibleRuntimeWorkers(
	ctx context.Context,
	roots paths.Roots,
	registry *central.Registry,
) error {
	pending := make(map[string]central.RepoRecord)
	for _, record := range registry.Repos {
		if record.LifecycleDisabled() || record.RepositoryID == "" || record.WorktreeID == "" {
			continue
		}
		pending[record.RepositoryID+"\x00"+record.WorktreeID] = record
	}
	var lastErr error
	for len(pending) > 0 {
		if status, statusErr := runtimeStatus(ctx, roots); statusErr == nil {
			for _, worker := range status.Workers {
				if worker.State != "needs_action" {
					continue
				}
				for _, record := range pending {
					if record.RepositoryID == worker.RepositoryID {
						return fmt.Errorf("runtime upgrade: worker for %s could not start: %s",
							record.Path, worker.LastError)
					}
				}
			}
		}
		for key, record := range pending {
			request := supervisor.Request{
				Version: supervisor.ProtocolVersion,
				ID:      "runtime-upgrade-ready-" + record.WorktreeID,
				Method:  "status", RepositoryID: record.RepositoryID,
				WorktreeID: record.WorktreeID,
				DeadlineMS: time.Now().Add(time.Second).UnixMilli(),
			}
			response, err := supervisor.DoWorker(ctx,
				supervisor.WorkerSocketPath(roots, record.RepositoryID),
				request, time.Second)
			if err == nil && response.Error == nil && response.OK {
				delete(pending, key)
				continue
			}
			if err != nil {
				lastErr = err
			} else if response.Error != nil {
				lastErr = errors.New(response.Error.Message)
			}
		}
		if len(pending) == 0 {
			break
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("runtime upgrade: %d worker(s) did not become ready: %w",
				len(pending), errors.Join(ctx.Err(), lastErr))
		case <-timer.C:
		}
	}
	if err := checkpointUpgradeRepositories(ctx, roots, registry); err != nil {
		return fmt.Errorf("runtime upgrade: verify restarted workers: %w", err)
	}
	return nil
}

func shouldUpgradeRuntime(status supervisor.Status, sourceDigest string, options RuntimeUpgradeOptions) (bool, error) {
	compatibilityUpgrade := !status.Compatibility.Equal(options.Compatibility)
	if compatibilityUpgrade &&
		!CanUpgradeRuntimeCompatibility(status.Compatibility, options.Compatibility) &&
		!(options.AllowUnadvertised &&
			CanProbeUnadvertisedRuntime(status, options.SourceVersion)) {
		return false, errors.New("running ACD does not advertise the current compatibility contract; run `acd setup` once to complete the compatibility cutover")
	}
	if compatibilityUpgrade {
		return true, nil
	}
	if status.Version == options.SourceVersion {
		if status.BinaryDigest == sourceDigest {
			return options.Force, nil
		}
		return true, nil
	}
	order, comparable := version.Compare(options.SourceVersion, status.Version)
	if !comparable {
		return false, fmt.Errorf("CLI version %s cannot be ordered against supervisor version %s; run `acd setup`", options.SourceVersion, status.Version)
	}
	if order < 0 {
		// git describe orders a dirty build after the clean build from the same
		// commit. Explicit setup may replace that temporary runtime with the
		// reproducible clean binary; this is not a source-code downgrade.
		if options.AllowSameDistanceReplacement &&
			sameRuntimeBuildIdentity(options.SourceVersion, status.Version) {
			return status.BinaryDigest != sourceDigest || options.Force, nil
		}
		return false, nil
	}
	if order == 0 && status.Version != options.SourceVersion {
		if options.AllowSameDistanceReplacement {
			return true, nil
		}
		return false, fmt.Errorf("CLI version %s diverges from supervisor version %s at the same release distance; run `acd setup`", options.SourceVersion, status.Version)
	}
	return true, nil
}

func sameRuntimeBuildIdentity(left, right string) bool {
	return strings.Replace(left, "-dirty", "", 1) ==
		strings.Replace(right, "-dirty", "", 1)
}

func runtimeStatus(ctx context.Context, roots paths.Roots) (supervisor.Status, error) {
	response, err := (&supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 3 * time.Second}).Do(ctx,
		supervisor.Request{Version: supervisor.ProtocolVersion, ID: "runtime-upgrade-status", Method: "status", DeadlineMS: time.Now().Add(3 * time.Second).UnixMilli()})
	if err != nil {
		return supervisor.Status{}, err
	}
	if response.Error != nil {
		return supervisor.Status{}, errors.New(response.Error.Message)
	}
	return decode[supervisor.Status](response.Data)
}

func checkpointUpgradeRepositories(ctx context.Context, roots paths.Roots, registry *central.Registry) error {
	groups := groupSetupBarrierRecords(registry.Repos)
	jobs := make(chan []central.RepoRecord, len(groups))
	for _, group := range groups {
		jobs <- group
	}
	close(jobs)
	barrierCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		workers sync.WaitGroup
		once    sync.Once
		first   error
	)
	workerCount := min(setupBarrierConcurrency, len(groups))
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for group := range jobs {
				for _, record := range group {
					if record.LifecycleDisabled() || barrierCtx.Err() != nil {
						continue
					}
					// Runtime replacement needs a durable checkpoint, not a Git
					// publication. Detached worktrees remain protected and resume
					// normal publication after the supervisor restarts.
					params, _ := json.Marshal(map[string]any{"kind": "checkpoint", "drain_publication": false})
					request := supervisor.Request{Version: supervisor.ProtocolVersion, ID: "runtime-upgrade-checkpoint-" + record.WorktreeID,
						Method: "checkpoint_barrier", RepositoryID: record.RepositoryID, WorktreeID: record.WorktreeID,
						DeadlineMS: time.Now().Add(supervisor.CheckpointBarrierTimeout).UnixMilli(), Params: params}
					response, err := (&supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: supervisor.CheckpointBarrierTimeout}).Do(barrierCtx, request)
					if err == nil && response.Error != nil {
						err = errors.New(response.Error.Message)
					}
					if err != nil {
						// A schema-forward repository can leave the old worker
						// unable to open state. A clean Git worktree has no
						// uncheckpointed content, so it is already durable and
						// safe to hand to the compatible replacement runtime.
						if cleanErr := proveRuntimeUpgradeGitDurable(
							barrierCtx, record); cleanErr == nil {
							continue
						} else {
							err = errors.Join(err, cleanErr)
						}
						once.Do(func() { first = fmt.Errorf("runtime upgrade: checkpoint %s: %w", record.Path, err); cancel() })
						return
					}
				}
			}
		}()
	}
	workers.Wait()
	return first
}

func proveRuntimeUpgradeGitDurable(
	ctx context.Context,
	record central.RepoRecord,
) error {
	if record.Path == "" {
		return errors.New("runtime upgrade: repository path is missing")
	}
	worktree, err := git.ResolveWorktree(ctx, record.Path)
	if err != nil {
		return fmt.Errorf("runtime upgrade: resolve worktree: %w", err)
	}
	status, err := git.Run(ctx, git.RunOpts{Dir: worktree.Root},
		"status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return fmt.Errorf("runtime upgrade: verify Git durability: %w", err)
	}
	if len(status) != 0 {
		if err := proveRuntimeUpgradeCheckpoint(ctx, record); err != nil {
			return errors.Join(errors.New(
				"runtime upgrade: worker is unavailable and the worktree has uncheckpointed changes"), err)
		}
	}
	return nil
}

func proveRuntimeUpgradeCheckpoint(
	ctx context.Context,
	record central.RepoRecord,
) error {
	if record.StateDB == "" {
		return errors.New("runtime upgrade: repository state path is missing")
	}
	version, err := state.ReadUserVersion(ctx, record.StateDB)
	if err != nil {
		return fmt.Errorf("runtime upgrade: inspect protected state: %w", err)
	}
	if version != state.SchemaVersion &&
		!state.CanRuntimeMigrate(version, state.SchemaVersion) {
		return fmt.Errorf(
			"runtime upgrade: protected state schema v%d is not runtime-compatible",
			version)
	}
	db, err := state.OpenReadOnly(ctx, record.StateDB)
	if err != nil {
		return fmt.Errorf("runtime upgrade: open protected state: %w", err)
	}
	defer db.Close()
	values := make(map[string]string)
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT key,value FROM daemon_meta
WHERE key IN (
 'protection.complete','protection.observation_epoch',
 'protection.covered_epoch','protection.checkpoint_id'
)`)
	if err != nil {
		return fmt.Errorf("runtime upgrade: read protection proof: %w", err)
	}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			rows.Close()
			return err
		}
		values[key] = value
	}
	if err := rows.Close(); err != nil {
		return err
	}
	observation, observationErr := strconv.ParseInt(
		values["protection.observation_epoch"], 10, 64)
	covered, coveredErr := strconv.ParseInt(
		values["protection.covered_epoch"], 10, 64)
	checkpointID := values["protection.checkpoint_id"]
	if values["protection.complete"] != "true" || observationErr != nil ||
		coveredErr != nil || observation <= 0 || covered < observation ||
		checkpointID == "" {
		return errors.New("runtime upgrade: latest observation lacks a completed checkpoint")
	}
	var phase string
	if err := db.ReadSQL().QueryRowContext(ctx,
		`SELECT phase FROM checkpoints WHERE id=?`, checkpointID).Scan(&phase); err != nil {
		return fmt.Errorf("runtime upgrade: verify completed checkpoint: %w", err)
	}
	if phase != state.CheckpointCompleted {
		return fmt.Errorf("runtime upgrade: checkpoint %s is %s", checkpointID, phase)
	}
	return nil
}

func waitForSupervisorStopped(ctx context.Context, path string) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func runtimeUpgradeDigest(status supervisor.Status, sourceDigest string, plans []integrationpkg.Plan) string {
	type digestPlan struct {
		From          string
		To            string
		Compatibility supervisor.Compatibility
		Integrations  []string
	}
	value := digestPlan{From: status.BinaryDigest, To: sourceDigest, Compatibility: status.Compatibility}
	for _, plan := range plans {
		value.Integrations = append(value.Integrations, plan.Name+":"+plan.BeforeDigest+":"+plan.AfterDigest)
	}
	body, _ := json.Marshal(value)
	digest := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(digest[:])
}
