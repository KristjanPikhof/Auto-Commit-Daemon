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
	"sort"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/globalops"
	integrationpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/integration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

type UninstallPlan struct {
	OperationID   string                       `json:"operation_id"`
	Digest        string                       `json:"digest"`
	PurgeData     bool                         `json:"purge_data"`
	Service       supervisor.ServiceDefinition `json:"service"`
	PriorService  ServiceState                 `json:"prior_service"`
	ManagedBinary string                       `json:"managed_binary"`
	Registry      *central.Registry            `json:"registry"`
	Integrations  []integrationpkg.RemovalPlan `json:"integrations"`
	Actions       []Action                     `json:"actions"`
	BackupRoot    string                       `json:"backup_root"`
	PurgeTargets  []PurgeTarget                `json:"purge_targets,omitempty"`
}

type PurgeTarget struct {
	RepositoryID string                  `json:"repository_id"`
	WorktreeID   string                  `json:"worktree_id"`
	RepoRoot     string                  `json:"repo_root"`
	GitDir       string                  `json:"git_dir"`
	StateDir     string                  `json:"state_dir"`
	Refs         []gitpkg.RefExpectation `json:"refs,omitempty"`
}

func BuildUninstallPlan(ctx context.Context, roots paths.Roots, purge bool) (UninstallPlan, error) {
	registry, err := central.Load(roots)
	if err != nil {
		return UninstallPlan{}, err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return UninstallPlan{}, err
	}
	service, err := supervisor.RenderService(home, roots.ManagedBinaryPath(), roots.SupervisorLogPath())
	if err != nil {
		return UninstallPlan{}, err
	}
	removals, err := integrationpkg.BuildRemovalPlans(roots.IntegrationsOwnershipPath())
	if err != nil {
		return UninstallPlan{}, err
	}
	opID, err := newOperationID("uninstall")
	if err != nil {
		return UninstallPlan{}, err
	}
	plan := UninstallPlan{OperationID: opID, PurgeData: purge, Service: service,
		PriorService: inspectServiceState(ctx, roots, service), ManagedBinary: roots.ManagedBinaryPath(),
		Registry: registry, Integrations: removals, BackupRoot: roots.SetupOperationDir(opID)}
	for _, record := range registry.Repos {
		if !record.LifecycleDisabled() {
			plan.Actions = append(plan.Actions, Action{Kind: "checkpoint_barrier", Target: record.Path, Detail: "Protect current changes before disabling"})
		}
	}
	supervisorTarget := service.Path
	supervisorDetail := "Stop workers and unload the user supervisor"
	if service.Platform == "session" {
		supervisorTarget = service.Binary
		supervisorDetail = "Stop the repository-scoped macOS session supervisor"
	}
	plan.Actions = append(plan.Actions, Action{Kind: "stop_supervisor", Target: supervisorTarget, Detail: supervisorDetail})
	for _, item := range removals {
		plan.Actions = append(plan.Actions, Action{Kind: "remove_integration", Target: item.Target, Detail: "Remove only verified ACD-owned entries"})
	}
	plan.Actions = append(plan.Actions, Action{Kind: "remove_managed_files", Target: plan.ManagedBinary, Detail: "Remove the managed binary and service file"}, Action{Kind: "disable_repositories", Target: roots.RegistryPath(), Detail: "Preserve repository databases and checkpoint refs"})
	if purge {
		seenRefs := make(map[string]bool)
		for _, record := range registry.Repos {
			target, targetErr := buildPurgeTarget(ctx, record, !seenRefs[record.RepositoryID])
			if targetErr != nil {
				return UninstallPlan{}, targetErr
			}
			seenRefs[record.RepositoryID] = true
			plan.PurgeTargets = append(plan.PurgeTargets, target)
			plan.Actions = append(plan.Actions, Action{Kind: "purge_repository_data", Target: record.StateDB, Detail: "Delete verified ACD state and private refs"})
		}
	}
	plan.Digest = digestUninstallPlan(plan)
	_ = ctx
	return plan, nil
}

func ApplyUninstall(ctx context.Context, roots paths.Roots, plan UninstallPlan, executor Executor) (Result, error) {
	if digestUninstallPlan(plan) != plan.Digest {
		return Result{}, errors.New("uninstall: plan digest mismatch")
	}
	if executor == nil {
		executor = OSExecutor{}
	}
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
	if err := journal.Prepare(ctx, globalops.Operation{ID: plan.OperationID, Kind: "uninstall", Phase: "planned", PlanDigest: plan.Digest}, steps); err != nil {
		return Result{}, err
	}
	targets := []string{roots.RegistryPath(), roots.IntegrationsOwnershipPath(), plan.ManagedBinary, plan.Service.Path}
	for _, item := range plan.Integrations {
		targets = append(targets, item.Target)
	}
	backups, err := backupFiles(plan.BackupRoot, targets, plan.PriorService)
	if err != nil {
		return Result{}, err
	}
	sessionStarted := false
	var purgeTxn *purgeTransaction
	rollback := func(cause error) error {
		var purgeErr error
		if purgeTxn != nil {
			purgeErr = purgeTxn.rollback(context.Background())
		}
		var sessionErr error
		if sessionStarted && !plan.PriorService.SessionLoaded {
			sessionErr = shutdownSupervisor(context.Background(), roots)
		}
		fileErr := restoreFiles(backups)
		serviceErr := restoreServiceState(context.Background(), roots, executor, plan.Service, plan.PriorService)
		_ = journal.Advance(context.Background(), plan.OperationID, "rolled_back", "uninstall rolled back", true)
		return errors.Join(cause, purgeErr, sessionErr, fileErr, serviceErr)
	}
	client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 60 * time.Second}
	if plan.Service.Platform == "session" {
		if err := supervisor.EnsureSession(ctx, roots, plan.Service.Binary, plan.Service.LogPath); err != nil {
			return Result{}, rollback(err)
		}
		sessionStarted = true
	}
	for _, record := range plan.Registry.Repos {
		if record.LifecycleDisabled() {
			continue
		}
		request := supervisor.Request{Version: supervisor.ProtocolVersion, ID: fmt.Sprintf("uninstall-barrier-%d", time.Now().UnixNano()), Method: "checkpoint_barrier", RepositoryID: record.RepositoryID, WorktreeID: record.WorktreeID, DeadlineMS: time.Now().Add(60 * time.Second).UnixMilli()}
		response, callErr := client.Do(ctx, request)
		if callErr != nil {
			return Result{}, rollback(callErr)
		}
		if response.Error != nil {
			return Result{}, rollback(errors.New(response.Error.Message))
		}
	}
	if err := stopSupervisorForUninstall(ctx, roots, client, 10*time.Second); err != nil {
		return Result{}, rollback(err)
	}
	if plan.PriorService.Loaded {
		if err := unloadService(ctx, roots, executor, plan.Service); err != nil {
			return Result{}, rollback(err)
		}
	}
	for _, item := range plan.Integrations {
		if item.Changed {
			current, readErr := os.ReadFile(item.Target)
			if readErr != nil || sha256String(current) != item.BeforeDigest {
				return Result{}, rollback(fmt.Errorf("uninstall: integration changed after preview: %s", item.Target))
			}
			if err := writeAtomic(item.Target, item.Content, 0o600); err != nil {
				return Result{}, rollback(err)
			}
		}
	}
	for _, target := range []string{roots.IntegrationsOwnershipPath(), plan.Service.Path, plan.ManagedBinary} {
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return Result{}, rollback(err)
		}
	}
	for i := range plan.Registry.Repos {
		plan.Registry.Repos[i].LifecycleState = central.RepoLifecycleDisabled
		plan.Registry.Repos[i].LifecycleUpdatedTS = time.Now().Unix()
	}
	if err := central.Save(roots, plan.Registry); err != nil {
		return Result{}, rollback(err)
	}
	if plan.PurgeData {
		purgeTxn, err = stagePurge(ctx, plan.OperationID, plan.PurgeTargets)
		if err != nil {
			return Result{}, rollback(err)
		}
	}
	if err := journal.Advance(ctx, plan.OperationID, "committed", "", true); err != nil {
		return Result{}, rollback(err)
	}
	result := Result{OperationID: plan.OperationID, PlanDigest: plan.Digest, Changed: true}
	if plan.PurgeData {
		if purgeTxn != nil {
			if err := purgeTxn.commit(); err != nil {
				return Result{}, fmt.Errorf("uninstall committed; protected data cleanup needs attention: %w", err)
			}
		}
		if err := journal.Close(); err != nil {
			return Result{}, err
		}
		for _, root := range []string{roots.State, roots.Config, roots.Share} {
			if err := validateACDRoot(root); err != nil {
				return Result{}, err
			}
			if err := os.RemoveAll(root); err != nil {
				return Result{}, err
			}
		}
	}
	return result, nil
}

func buildPurgeTarget(ctx context.Context, record central.RepoRecord, includeRefs bool) (PurgeTarget, error) {
	wt, err := gitpkg.ResolveWorktree(ctx, record.Path)
	if err != nil {
		return PurgeTarget{}, err
	}
	wantDB := state.DBPathFromGitDir(wt.GitDir)
	if filepath.Clean(record.StateDB) != filepath.Clean(wantDB) {
		return PurgeTarget{}, fmt.Errorf("uninstall: refuse unverified state DB path %s", record.StateDB)
	}
	stateDir := state.AcdDirFromGitDir(wt.GitDir)
	rel, err := filepath.Rel(wt.GitDir, stateDir)
	if err != nil || rel != "acd" {
		return PurgeTarget{}, fmt.Errorf("uninstall: refuse unverified state directory %s", stateDir)
	}
	target := PurgeTarget{RepositoryID: record.RepositoryID, WorktreeID: record.WorktreeID,
		RepoRoot: wt.Root, GitDir: wt.GitDir, StateDir: stateDir}
	if !includeRefs {
		return target, nil
	}
	out, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: wt.Root}, "for-each-ref", "--format=%(refname) %(objectname)", "refs/acd/")
	if err != nil {
		return PurgeTarget{}, err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 || !strings.HasPrefix(fields[0], "refs/acd/") {
			return PurgeTarget{}, fmt.Errorf("uninstall: invalid private ref inventory")
		}
		target.Refs = append(target.Refs, gitpkg.RefExpectation{Ref: fields[0], OID: fields[1]})
	}
	return target, nil
}

type stagedPurge struct {
	target     PurgeTarget
	quarantine string
	moved      bool
	refsGone   bool
}

type purgeTransaction struct {
	items []stagedPurge
	locks []*daemon.DaemonLock
}

func stagePurge(ctx context.Context, operationID string, targets []PurgeTarget) (*purgeTransaction, error) {
	txn := &purgeTransaction{}
	ordered := append([]PurgeTarget(nil), targets...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].RepositoryID == ordered[j].RepositoryID {
			return ordered[i].WorktreeID < ordered[j].WorktreeID
		}
		return ordered[i].RepositoryID < ordered[j].RepositoryID
	})
	seenLocks := make(map[string]bool)
	for _, target := range ordered {
		if !seenLocks[target.RepositoryID] {
			lock, err := daemon.AcquireDaemonLock(target.GitDir)
			if err != nil {
				_ = txn.rollback(context.Background())
				return txn, fmt.Errorf("uninstall: acquire repository ownership: %w", err)
			}
			txn.locks = append(txn.locks, lock)
			seenLocks[target.RepositoryID] = true
		}
		txn.items = append(txn.items, stagedPurge{target: target})
		item := &txn.items[len(txn.items)-1]
		if len(target.Refs) > 0 {
			if err := gitpkg.DeleteRefsCAS(ctx, target.RepoRoot, target.Refs); err != nil {
				_ = txn.rollback(context.Background())
				return txn, err
			}
			item.refsGone = true
		}
		if _, err := os.Lstat(target.StateDir); err == nil {
			suffix := strings.NewReplacer("/", "-", "\\", "-", ":", "-").Replace(operationID)
			item.quarantine = target.StateDir + ".uninstall-" + suffix
			if _, existsErr := os.Lstat(item.quarantine); !errors.Is(existsErr, os.ErrNotExist) {
				_ = txn.rollback(context.Background())
				return txn, fmt.Errorf("uninstall: quarantine already exists: %s", item.quarantine)
			}
			if err := os.Rename(target.StateDir, item.quarantine); err != nil {
				_ = txn.rollback(context.Background())
				return txn, err
			}
			item.moved = true
		} else if !errors.Is(err, os.ErrNotExist) {
			_ = txn.rollback(context.Background())
			return txn, err
		}
	}
	return txn, nil
}

func (txn *purgeTransaction) rollback(ctx context.Context) error {
	if txn == nil {
		return nil
	}
	var combined error
	for i := len(txn.items) - 1; i >= 0; i-- {
		item := &txn.items[i]
		if item.moved {
			if err := os.Rename(item.quarantine, item.target.StateDir); err != nil {
				combined = errors.Join(combined, err)
			} else {
				item.moved = false
			}
		}
		if item.refsGone {
			if err := gitpkg.CreateRefsCAS(ctx, item.target.RepoRoot, item.target.Refs); err != nil {
				combined = errors.Join(combined, err)
			} else {
				item.refsGone = false
			}
		}
	}
	combined = errors.Join(combined, txn.release())
	return combined
}

func (txn *purgeTransaction) commit() error {
	if txn == nil {
		return nil
	}
	var combined error
	for i := range txn.items {
		if txn.items[i].moved {
			combined = errors.Join(combined, os.RemoveAll(txn.items[i].quarantine))
			txn.items[i].moved = false
		}
	}
	return errors.Join(combined, txn.release())
}

func (txn *purgeTransaction) release() error {
	var combined error
	for i := len(txn.locks) - 1; i >= 0; i-- {
		combined = errors.Join(combined, txn.locks[i].Release())
	}
	txn.locks = nil
	return combined
}

func validateACDRoot(path string) error {
	if !filepath.IsAbs(path) || filepath.Base(filepath.Clean(path)) != "acd" {
		return fmt.Errorf("uninstall: refuse broad data root %s", path)
	}
	return nil
}

func digestUninstallPlan(plan UninstallPlan) string {
	copy := plan
	copy.Digest = ""
	copy.OperationID = ""
	copy.BackupRoot = "<uninstall-operation>"
	body, _ := json.Marshal(copy)
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}
