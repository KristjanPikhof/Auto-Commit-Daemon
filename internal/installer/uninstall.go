package installer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
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
	OperationID           string                       `json:"operation_id"`
	Digest                string                       `json:"digest"`
	PurgeData             bool                         `json:"purge_data"`
	Service               supervisor.ServiceDefinition `json:"service"`
	PriorService          ServiceState                 `json:"prior_service"`
	Launcher              string                       `json:"launcher,omitempty"`
	LauncherDigest        string                       `json:"launcher_digest,omitempty"`
	ManagedBinaryDigest   string                       `json:"managed_binary_digest,omitempty"`
	PublicationHoldDigest string                       `json:"publication_hold_digest"`
	Warnings              []string                     `json:"warnings,omitempty"`
	ManagedBinary         string                       `json:"managed_binary"`
	Registry              *central.Registry            `json:"registry"`
	Integrations          []integrationpkg.RemovalPlan `json:"integrations"`
	Actions               []Action                     `json:"actions"`
	BackupRoot            string                       `json:"backup_root"`
	PurgeTargets          []PurgeTarget                `json:"purge_targets,omitempty"`
	checkpointRefs        map[string]string
}

type PurgeTarget struct {
	RepositoryID string                  `json:"repository_id"`
	WorktreeID   string                  `json:"worktree_id"`
	PurgeRefs    bool                    `json:"purge_refs"`
	Unavailable  string                  `json:"unavailable,omitempty"`
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
	if body, readErr := os.ReadFile(plan.ManagedBinary); readErr == nil {
		plan.ManagedBinaryDigest = sha256String(body)
		launcher := filepath.Join(home, ".local", "bin", "acd")
		if info, statErr := os.Lstat(launcher); statErr == nil {
			if info.Mode().IsRegular() {
				launcherBody, readErr := os.ReadFile(launcher)
				if readErr != nil {
					return UninstallPlan{}, readErr
				}
				if sha256String(launcherBody) == plan.ManagedBinaryDigest {
					plan.Launcher, plan.LauncherDigest = launcher, sha256String(launcherBody)
				} else {
					plan.Warnings = append(plan.Warnings, "ACD cannot verify ownership; keep this launcher: "+launcher)
				}
			} else {
				plan.Warnings = append(plan.Warnings, "ACD cannot verify ownership; keep this launcher: "+launcher)
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return UninstallPlan{}, statErr
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return UninstallPlan{}, readErr
	}
	plan.PublicationHoldDigest, err = uninstallHoldDigest(roots.SetupPublicationHoldPath())
	if err != nil {
		return UninstallPlan{}, err
	}
	missing := map[string]bool{}
	if purge {
		seenRefs := make(map[string]bool)
		for _, record := range registry.Repos {
			target, targetErr := buildPurgeTarget(ctx, record, !seenRefs[record.RepositoryID])
			if targetErr != nil {
				if !errors.Is(targetErr, os.ErrNotExist) {
					return UninstallPlan{}, targetErr
				}
				target = PurgeTarget{RepositoryID: record.RepositoryID, WorktreeID: record.WorktreeID, RepoRoot: record.Path, Unavailable: "Repository folder missing. Its saved ACD data was kept."}
				missing[record.Path] = true
				plan.Warnings = append(plan.Warnings, target.Unavailable+": "+record.Path)
			} else {
				if seenRefs[target.RepositoryID] {
					target.PurgeRefs = false
					target.Refs = nil
				}
				seenRefs[target.RepositoryID] = true
			}
			plan.PurgeTargets = append(plan.PurgeTargets, target)
		}
	}
	for _, record := range registry.Repos {
		if !record.LifecycleDisabled() && !missing[record.Path] {
			plan.Actions = append(plan.Actions, Action{Kind: "checkpoint_barrier", Target: record.Path, Detail: "Protect current changes before disabling"})
		}
	}
	supervisorTarget := service.Path
	supervisorDetail := "Stop ACD background protection"
	if service.Platform == "session" {
		supervisorTarget = service.Binary
		supervisorDetail = "Stop ACD background protection for this session"
	}
	plan.Actions = append(plan.Actions, Action{Kind: "stop_supervisor", Target: supervisorTarget, Detail: supervisorDetail})
	for _, item := range removals {
		plan.Actions = append(plan.Actions, Action{Kind: "remove_integration", Target: item.Target, Detail: "Remove only verified ACD-owned entries"})
	}
	plan.Actions = append(plan.Actions, Action{Kind: "remove_managed_files", Target: plan.ManagedBinary, Detail: "Remove the managed binary and service file"}, Action{Kind: "disable_repositories", Target: roots.RegistryPath(), Detail: "Disable all registered repositories"})
	if plan.Launcher != "" {
		plan.Actions = append(plan.Actions, Action{Kind: "remove_launcher", Target: plan.Launcher, Detail: "Remove the launcher matching the managed ACD binary"})
	}
	if purge {
		for _, target := range plan.PurgeTargets {
			if target.Unavailable != "" {
				plan.Actions = append(plan.Actions, Action{Kind: "skip_unavailable_repository", Target: target.RepoRoot, Detail: target.Unavailable})
				continue
			}
			plan.Actions = append(plan.Actions, Action{Kind: "purge_repository_data", Target: target.StateDir, Detail: "Permanently delete saved ACD history and recovery data. Keep project files and Git history."})
		}
	}

	plan.checkpointRefs, err = qualifyUninstallCheckpoints(ctx, plan.PurgeTargets)
	if err != nil {
		return UninstallPlan{}, err
	}
	plan.Digest = digestUninstallPlan(plan)
	_ = ctx
	return plan, nil
}

func ApplyUninstall(ctx context.Context, roots paths.Roots, plan UninstallPlan, executor Executor) (Result, error) {
	if plan.PurgeData {
		for _, root := range []string{roots.State, roots.Config, roots.Share} {
			if err := validateACDRoot(root); err != nil {
				return Result{}, err
			}
		}
	}
	if digestUninstallPlan(plan) != plan.Digest {
		return Result{}, errors.New("uninstall: plan digest mismatch")
	}
	lifecycleLock, err := globalops.AcquireUserLock(ctx, roots.OperationsDBPath())
	if err != nil {
		return Result{}, fmt.Errorf("uninstall: acquire user lifecycle lock: %w", err)
	}
	defer lifecycleLock.Release()
	if executor == nil {
		executor = OSExecutor{}
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
	preflight := plan
	preflight.PurgeTargets = slices.Clone(plan.PurgeTargets)
	if err := refreshUninstallPurgeRefs(ctx, roots, &preflight, true); err != nil {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "uninstall preview became stale before shutdown", true)
		return Result{}, err
	}
	if err := os.MkdirAll(plan.BackupRoot, 0o700); err != nil {
		_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "uninstall backup directory failed", true)
		return Result{}, err
	}
	targets := []string{roots.RegistryPath(), roots.IntegrationsOwnershipPath(), plan.ManagedBinary, plan.Service.Path, roots.SetupPublicationHoldPath()}
	if plan.Launcher != "" {
		targets = append(targets, plan.Launcher)
	}
	for _, item := range plan.Integrations {
		targets = append(targets, item.Target)
	}
	backups, err := backupFiles(plan.BackupRoot, targets, plan.PriorService)
	if err != nil {
		return Result{}, err
	}
	forceNeedsAction := false
	var purgeTxn *purgeTransaction
	rollback := func(cause error) error {
		var purgeErr error
		if purgeTxn != nil {
			purgeErr = purgeTxn.rollback(context.Background())
		}
		fileErr := restoreFiles(backups)
		serviceErr := restoreServiceState(context.Background(), roots, executor, plan.Service, plan.PriorService)
		rollbackErr := errors.Join(purgeErr, fileErr, serviceErr)
		if forceNeedsAction || rollbackErr != nil || errors.Is(cause, errPostMutationProof) || errors.Is(cause, gitpkg.ErrPrivateRefDeleteOutcomeUnknown) {
			_ = journal.Advance(context.Background(), plan.OperationID, "needs_attention", "uninstall rollback or shutdown needs attention", true)
		} else {
			_ = journal.Advance(context.Background(), plan.OperationID, "rolled_back", "uninstall rolled back", true)
		}
		return errors.Join(cause, rollbackErr)
	}
	if actual, err := uninstallHoldDigest(roots.SetupPublicationHoldPath()); err != nil || actual != plan.PublicationHoldDigest {
		return Result{}, rollback(errors.Join(errors.New("uninstall: publication hold changed after preview"), err))
	}
	holdBody := uninstallHoldBody(plan)
	if err := preparePostMutation(plan.BackupRoot, backups, plan.PriorService, roots.SetupPublicationHoldPath(), fileDigest(holdBody, 0600)); err != nil {
		return Result{}, rollback(err)
	}
	if err := writeAtomic(roots.SetupPublicationHoldPath(), holdBody, 0600); err != nil {
		return Result{}, rollback(err)
	}
	if err := refuseLiveUninstallPublication(ctx, plan.Registry); err != nil {
		return Result{}, rollback(err)
	}
	client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: supervisor.CheckpointBarrierTimeout}
	for _, record := range plan.Registry.Repos {
		if record.LifecycleDisabled() || uninstallTargetUnavailable(plan, record.Path) {
			continue
		}
		wt, err := gitpkg.ResolveWorktree(ctx, record.Path)
		if err != nil {
			return Result{}, rollback(err)
		}
		// A stopped repository must stay stopped, especially when it has a
		// prepared finish. The canonical lock proves no worker needs a barrier.
		lock, err := daemon.AcquireDaemonLock(wt.GitDir)
		if err == nil {
			if err := lock.Release(); err != nil {
				return Result{}, rollback(err)
			}
			continue
		}
		if !errors.Is(err, daemon.ErrDaemonLockHeld) {
			return Result{}, rollback(err)
		}
		if err := waitSupervisorWorkersReady(ctx, roots, map[string]bool{record.RepositoryID: true}, 15*time.Minute, nil); err != nil {
			return Result{}, rollback(fmt.Errorf("uninstall: wait for protected repository worker: %w", err))
		}
		request := supervisor.Request{Version: supervisor.ProtocolVersion, ID: fmt.Sprintf("uninstall-barrier-%d", time.Now().UnixNano()), Method: "checkpoint_barrier", RepositoryID: record.RepositoryID, WorktreeID: record.WorktreeID, DeadlineMS: time.Now().Add(supervisor.CheckpointBarrierTimeout).UnixMilli()}
		response, callErr := client.Do(ctx, request)
		if callErr != nil {
			return Result{}, rollback(callErr)
		}
		if response.Error != nil {
			return Result{}, rollback(errors.New(response.Error.Message))
		}
	}
	if err := stopSupervisorForUninstall(ctx, roots, client, 10*time.Second); err != nil {
		forceNeedsAction = true
		return Result{}, rollback(err)
	}
	if err := refreshUninstallPurgeRefs(ctx, roots, &plan, false); err != nil {
		forceNeedsAction = true
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
			if err := preparePostMutation(plan.BackupRoot, backups, plan.PriorService, item.Target, fileDigest(item.Content, 0o600)); err != nil {
				return Result{}, rollback(err)
			}
			if err := writeAtomic(item.Target, item.Content, 0o600); err != nil {
				return Result{}, rollback(err)
			}
		}
	}
	removeTargets := []string{roots.IntegrationsOwnershipPath(), plan.Service.Path, plan.ManagedBinary}
	if plan.Launcher != "" {
		removeTargets = append(removeTargets, plan.Launcher)
	}
	for _, target := range removeTargets {
		want := ""
		if target == plan.ManagedBinary {
			want = plan.ManagedBinaryDigest
		}
		if target == plan.Launcher {
			want = plan.LauncherDigest
		}
		if want != "" {
			info, err := os.Lstat(target)
			if err != nil || !info.Mode().IsRegular() {
				return Result{}, rollback(errors.Join(errors.New("uninstall: executable identity changed after preview"), err))
			}
			body, err := os.ReadFile(target)
			if err != nil || sha256String(body) != want {
				return Result{}, rollback(errors.Join(errors.New("uninstall: executable changed after preview"), err))
			}
		}
		if err := preparePostMutation(plan.BackupRoot, backups, plan.PriorService, target, absentFileDigest); err != nil {
			return Result{}, rollback(err)
		}
		if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
			return Result{}, rollback(err)
		}
	}
	for i := range plan.Registry.Repos {
		plan.Registry.Repos[i].LifecycleState = central.RepoLifecycleDisabled
		plan.Registry.Repos[i].LifecycleUpdatedTS = time.Now().Unix()
	}
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
	if plan.PurgeData {
		purgeTxn, err = stagePurge(ctx, plan.OperationID, plan.PurgeTargets)
		if err != nil {
			return Result{}, rollback(err)
		}
	}
	if actual, err := uninstallHoldDigest(roots.SetupPublicationHoldPath()); err != nil || actual != fileDigest(uninstallHoldBody(plan), 0600) {
		return Result{}, rollback(errors.Join(errors.New("uninstall: owned publication hold changed before cleanup"), err))
	}
	if err := preparePostMutation(plan.BackupRoot, backups, plan.PriorService, roots.SetupPublicationHoldPath(), absentFileDigest); err != nil {
		return Result{}, rollback(err)
	}
	if err := os.Remove(roots.SetupPublicationHoldPath()); err != nil {
		return Result{}, rollback(err)
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
		for _, root := range []string{roots.State, roots.Config} {
			if err := validateACDRoot(root); err != nil {
				return Result{}, err
			}
			if err := os.RemoveAll(root); err != nil {
				return Result{}, err
			}
		}
		if err := purgeRootExcept(roots.Share, globalops.UserLockPath(roots.OperationsDBPath())); err != nil {
			return Result{}, err
		}
	}
	return result, nil
}

func buildPurgeTarget(ctx context.Context, record central.RepoRecord, includeRefs bool) (PurgeTarget, error) {
	wt, err := gitpkg.ResolveWorktree(ctx, record.Path)
	if err != nil {
		return PurgeTarget{}, err
	}
	if (record.RepositoryID != "" && record.RepositoryID != central.CanonicalID(wt.CommonDir)) || (record.WorktreeID != "" && record.WorktreeID != central.CanonicalID(wt.Root)) || (record.CommonDir != "" && filepath.Clean(record.CommonDir) != wt.CommonDir) {
		return PurgeTarget{}, errors.New("uninstall: registered repository identity changed; preview again")
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
	if info, err := os.Lstat(stateDir); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return PurgeTarget{}, fmt.Errorf("uninstall: refuse unverified state directory %s", stateDir)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return PurgeTarget{}, err
	}
	target := PurgeTarget{RepositoryID: central.CanonicalID(wt.CommonDir), WorktreeID: central.CanonicalID(wt.Root),
		RepoRoot: wt.Root, GitDir: wt.GitDir, StateDir: stateDir, PurgeRefs: includeRefs}
	if !includeRefs {
		return target, nil
	}
	out, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: wt.Root}, "for-each-ref", "--format=%(refname) %(objectname) %(symref)", "refs/acd/")
	if err != nil {
		return PurgeTarget{}, err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 || !strings.HasPrefix(fields[0], "refs/acd/") {
			return PurgeTarget{}, fmt.Errorf("Ambiguous private ref inventory; inspect symbolic Git references before purge.")
		}
		target.Refs = append(target.Refs, gitpkg.RefExpectation{Ref: fields[0], OID: fields[1]})
	}
	if err := validateLoosePurgeRefs(wt.CommonDir, target.Refs); err != nil {
		return PurgeTarget{}, err
	}
	return target, nil
}

func uninstallTargetUnavailable(plan UninstallPlan, path string) bool {
	for _, target := range plan.PurgeTargets {
		if target.RepoRoot == path && target.Unavailable != "" {
			return true
		}
	}
	return false
}

// Git omits dangling refs from for-each-ref. Never silently skip or follow an
// unproved loose ref, including aliases to public branches.
func validateLoosePurgeRefs(common string, refs []gitpkg.RefExpectation) error {
	known := make(map[string]string, len(refs))
	for _, ref := range refs {
		known[ref.Ref] = ref.OID
	}
	root := filepath.Join(common, "refs", "acd")
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) && path == root {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("uninstall: private ref is not a regular direct ref: %s; inspect before purge", path)
		}
		// Git lock files are metadata, never valid ref names. Leave collisions to
		// the native deletion transaction; preview must not read or remove locks.
		if strings.HasSuffix(entry.Name(), ".lock") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(common, path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if known[name] == "" || strings.TrimSpace(string(body)) != known[name] {
			return fmt.Errorf("uninstall: ambiguous or dangling private ref %s; inspect before purge", name)
		}
		return nil
	})
}

type stagedPurge struct {
	target      PurgeTarget
	quarantine  string
	moved       bool
	removedRefs []gitpkg.RefExpectation
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
		if target.Unavailable != "" {
			continue
		}
		if !seenLocks[target.RepositoryID] {
			lock, err := daemon.AcquireDaemonLock(target.GitDir)
			if err != nil {
				return txn, errors.Join(fmt.Errorf("uninstall: acquire repository ownership: %w", err), txn.rollback(context.Background()))
			}
			txn.locks = append(txn.locks, lock)
			seenLocks[target.RepositoryID] = true
		}
		if target.PurgeRefs {
			current, err := buildPurgeTarget(ctx, central.RepoRecord{Path: target.RepoRoot, RepositoryID: target.RepositoryID, WorktreeID: target.WorktreeID, StateDB: state.DBPathFromGitDir(target.GitDir)}, true)
			if err != nil || current.GitDir != target.GitDir || !slices.Equal(current.Refs, target.Refs) {
				return txn, errors.Join(errors.New("uninstall: private refs changed before purge; preview again"), err, txn.rollback(context.Background()))
			}
		}
		txn.items = append(txn.items, stagedPurge{target: target})
		item := &txn.items[len(txn.items)-1]
		if len(target.Refs) > 0 {
			var err error
			item.removedRefs, err = gitpkg.DeletePrivateRefsCAS(ctx, target.RepoRoot, target.Refs)
			if err != nil {
				return txn, errors.Join(err, txn.rollback(context.Background()))
			}
		}
		if _, err := os.Lstat(target.StateDir); err == nil {
			suffix := strings.NewReplacer("/", "-", "\\", "-", ":", "-").Replace(operationID)
			item.quarantine = target.StateDir + ".uninstall-" + suffix
			if _, existsErr := os.Lstat(item.quarantine); !errors.Is(existsErr, os.ErrNotExist) {
				return txn, errors.Join(fmt.Errorf("uninstall: quarantine already exists: %s", item.quarantine), txn.rollback(context.Background()))
			}
			if err := os.Rename(target.StateDir, item.quarantine); err != nil {
				return txn, errors.Join(err, txn.rollback(context.Background()))
			}
			item.moved = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return txn, errors.Join(err, txn.rollback(context.Background()))
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
		var remaining []gitpkg.RefExpectation
		for start := 0; start < len(item.removedRefs); start += 64 {
			batch := item.removedRefs[start:min(start+64, len(item.removedRefs))]
			if err := gitpkg.CreateRefsCAS(ctx, item.target.RepoRoot, batch); err != nil {
				combined = errors.Join(combined, err)
				remaining = append(remaining, batch...)
			}
		}
		item.removedRefs = remaining
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
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("uninstall: refuse unverified ACD data root %s", path)
	}
	return nil
}

func purgeRootExcept(root, retainedPath string) error {
	if err := validateACDRoot(root); err != nil {
		return err
	}
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	retainedPath = filepath.Clean(retainedPath)
	for _, entry := range entries {
		path := filepath.Join(root, entry.Name())
		if filepath.Clean(path) == retainedPath {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	return nil
}

func digestUninstallPlan(plan UninstallPlan) string {
	copy := plan
	copy.Digest = ""
	copy.OperationID = ""
	copy.BackupRoot = "<uninstall-operation>"
	copy.PurgeTargets = slices.Clone(plan.PurgeTargets)
	for i, target := range plan.PurgeTargets {
		copy.PurgeTargets[i].Refs = nil
		for _, ref := range target.Refs {
			if plan.checkpointRefs[target.RepositoryID+"\x00"+ref.Ref] != ref.OID {
				copy.PurgeTargets[i].Refs = append(copy.PurgeTargets[i].Refs, ref)
			}
		}
	}
	body, _ := json.Marshal(copy)
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func uninstallHoldBody(plan UninstallPlan) []byte {
	body, _ := json.Marshal(map[string]string{"operation_id": plan.OperationID, "plan_digest": plan.Digest})
	return append(body, '\n')
}

func uninstallHoldDigest(path string) (string, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return absentFileDigest, nil
	}
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("uninstall: publication hold is not an owned regular file")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return fileDigest(body, info.Mode().Perm()), nil
}

// A stopped prepared request is safe under the owned hold. Existing live jobs
// must finish or be stopped explicitly before uninstall can proceed.
func refuseLiveUninstallPublication(ctx context.Context, registry *central.Registry) error {
	for _, record := range registry.Repos {
		wt, err := gitpkg.ResolveWorktree(ctx, record.Path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		dbPath := state.DBPathFromGitDir(wt.GitDir)
		if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		db, err := state.OpenReadOnly(ctx, dbPath)
		if err != nil {
			return err
		}
		busy := false
		for _, query := range []struct{ table, sql string }{
			{"finish_requests", `SELECT EXISTS(SELECT 1 FROM finish_requests WHERE state IN ('running','prepared'))`},
			{"finish_history_requests", `SELECT EXISTS(SELECT 1 FROM finish_history_requests WHERE status IN ('running','prepared'))`},
		} {
			exists, tableErr := state.LegacyTableExists(ctx, db.ReadSQL(), query.table)
			if tableErr != nil {
				db.Close()
				return tableErr
			}
			if exists {
				var active bool
				if err := db.ReadSQL().QueryRowContext(ctx, query.sql).Scan(&active); err != nil {
					db.Close()
					return err
				}
				busy = busy || active
			}
		}
		if err := db.Close(); err != nil {
			return err
		}
		if !busy {
			continue
		}
		lock, err := daemon.AcquireDaemonLock(wt.GitDir)
		if errors.Is(err, daemon.ErrDaemonLockHeld) {
			return fmt.Errorf("uninstall: finish or history publication is active in %s; wait for it or stop that repository before uninstalling", record.Path)
		}
		if err != nil {
			return err
		}
		if err := lock.Release(); err != nil {
			return err
		}
	}
	return nil
}
