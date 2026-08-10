package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/globalops"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/migration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type globalRepairPlan struct {
	OperationID        string   `json:"operation_id"`
	ProvingOperationID string   `json:"proving_operation_id,omitempty"`
	RepairKind         string   `json:"repair_kind"`
	CanRepair          bool     `json:"can_repair"`
	Refusal            string   `json:"refusal,omitempty"`
	Checks             []string `json:"checks"`
	setupHoldBody      []byte
	bridgeRefs         []migration.RefProof
	recoveryManifests  []string
}

const (
	globalRepairSuperseded    = "superseded"
	globalRepairFinalizeSetup = "finalize_setup_cleanup"
	setupMigrationRefPrefix   = "refs/acd/migration/"
)

func previewGlobalRepairs(ctx context.Context, roots paths.Roots) ([]globalRepairPlan, error) {
	info, err := os.Lstat(roots.OperationsDBPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("global operations journal is not a regular file")
	}
	store, err := globalops.OpenReadOnly(ctx, roots.OperationsDBPath())
	if err != nil {
		return nil, err
	}
	defer store.Close()
	unresolved, err := store.OperationsByPhase(ctx, "needs_attention")
	if err != nil {
		return nil, err
	}
	if len(unresolved) == 0 {
		return nil, nil
	}
	committed, hasCommitted, err := store.LatestCommittedSetup(ctx)
	if err != nil {
		return nil, err
	}
	latest, hasLatest, err := store.LatestSetup(ctx)
	if err != nil {
		return nil, err
	}
	plans := make([]globalRepairPlan, 0, len(unresolved))
	for _, operation := range unresolved {
		plan, err := previewGlobalRepairOperation(ctx, roots, store, operation,
			committed, hasCommitted, latest, hasLatest)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

func previewGlobalRepairOperation(
	ctx context.Context,
	roots paths.Roots,
	store *globalops.Store,
	operation, committed globalops.Operation, hasCommitted bool,
	latest globalops.Operation, hasLatest bool,
) (globalRepairPlan, error) {
	plan := globalRepairPlan{OperationID: operation.ID, Checks: []string{}}
	if operation.Kind != "setup" {
		plan.Refusal = "only interrupted setup operations have a forward proof in this release"
		return plan, nil
	}
	finalizeCurrent := hasLatest && latest.ID == operation.ID && setupCleanupCanResume(operation.Error)
	if finalizeCurrent {
		plan.RepairKind = globalRepairFinalizeSetup
		plan.ProvingOperationID = operation.ID
		plan.Checks = append(plan.Checks, "latest setup reached its durable final-cleanup boundary")
	} else {
		plan.RepairKind = globalRepairSuperseded
		if !hasCommitted || committed.CreatedTS <= operation.CreatedTS {
			plan.Refusal = "no newer committed setup operation supersedes this operation"
			return plan, nil
		}
		plan.ProvingOperationID = committed.ID
	}
	holdBody, holdPresent, err := inspectSetupPublicationHold(roots.SetupPublicationHoldPath(), operation)
	if err != nil {
		return globalRepairPlan{}, err
	}
	if holdPresent && !finalizeCurrent {
		plan.Refusal = "a setup publication hold is still present"
		return plan, nil
	}
	plan.setupHoldBody = holdBody
	if holdPresent {
		plan.Checks = append(plan.Checks, "setup publication hold matches the interrupted operation")
	} else {
		plan.Checks = append(plan.Checks, "no setup publication hold remains")
	}
	registry, err := central.Load(roots)
	if err != nil || registry.Version != central.RegistryVersion {
		plan.Refusal = "the current registry is not a valid v2 registry"
		return plan, nil
	}
	plan.Checks = append(plan.Checks, "current registry is v2")
	if info, err := os.Lstat(roots.ManagedBinaryPath()); err != nil || !info.Mode().IsRegular() {
		plan.Refusal = "the managed binary is missing or not a regular file"
		return plan, nil
	}
	plan.Checks = append(plan.Checks, "managed binary is present")
	owned, err := runtimeReferencesOperation(roots, operation.ID, roots.SetupPublicationHoldPath())
	if err != nil {
		plan.Refusal = "runtime ownership cannot be inspected: " + err.Error()
		return plan, nil
	}
	if owned {
		plan.Refusal = "a worker or service runtime record still references the interrupted operation"
		return plan, nil
	}
	plan.Checks = append(plan.Checks, "no worker or service runtime record references the operation")
	if !finalizeCurrent {
		oldSteps, err := store.Steps(ctx, operation.ID)
		if err != nil {
			return globalRepairPlan{}, err
		}
		newSteps, err := store.Steps(ctx, committed.ID)
		if err != nil {
			return globalRepairPlan{}, err
		}
		if !stepsSuperseded(oldSteps, newSteps) {
			plan.Refusal = "the newer committed setup does not supersede every target in the interrupted plan"
			return plan, nil
		}
		plan.Checks = append(plan.Checks, "every interrupted target is superseded by the newer committed setup")
	}

	operationRoot := roots.SetupOperationDir(operation.ID)
	operationInfo, statErr := os.Lstat(operationRoot)
	if statErr != nil || !operationInfo.IsDir() {
		plan.Refusal = "the interrupted setup operation directory is missing or invalid"
		return plan, nil
	}
	var proofFiles, backups []string
	walkErr := filepath.WalkDir(operationRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		switch {
		case strings.HasSuffix(path, ".created-refs.json"):
			proofFiles = append(proofFiles, path)
		case !finalizeCurrent && strings.HasSuffix(path, "migration-bridge-recovery.json"):
			proofFiles = append(proofFiles, path)
		case strings.HasSuffix(path, "state-v19.db"):
			backups = append(backups, path)
		}
		return nil
	})
	if walkErr != nil {
		plan.Refusal = "the interrupted setup operation directory cannot be fully inspected"
		return plan, nil
	}
	if err := verifyOperationProofManifests(ctx, operation.ID, proofFiles); err != nil {
		plan.Refusal = "migration ref proof failed: " + err.Error()
		return plan, nil
	}
	for _, backup := range backups {
		if err := state.QuickCheck(ctx, backup); err != nil {
			plan.Refusal = "database backup proof failed for " + backup
			return plan, nil
		}
	}
	plan.Checks = append(plan.Checks, "migration refs match retained proof manifests", "repository backups pass quick_check")
	if finalizeCurrent {
		bridgeRefs, err := setupBridgeRefInventory(ctx, operation.ID, registry)
		if err != nil {
			plan.Refusal = "temporary migration bridge refs cannot be proved: " + err.Error()
			return plan, nil
		}
		manifests, err := setupRecoveryManifestInventory(ctx, roots.SetupRoot())
		if err != nil {
			plan.Refusal = "retained migration recovery refs cannot be proved: " + err.Error()
			return plan, nil
		}
		plan.bridgeRefs = bridgeRefs
		plan.recoveryManifests = manifests
		plan.Checks = append(plan.Checks,
			fmt.Sprintf("%d temporary migration bridge refs have exact targets", len(bridgeRefs)),
			fmt.Sprintf("%d retained migration recovery manifests are valid", len(manifests)))
	}
	plan.CanRepair = true
	return plan, nil
}

func previewGlobalRepair(ctx context.Context, roots paths.Roots) (globalRepairPlan, bool, error) {
	plans, err := previewGlobalRepairs(ctx, roots)
	if err != nil || len(plans) == 0 {
		return globalRepairPlan{}, false, err
	}
	return plans[0], true, nil
}

func applyGlobalRepair(ctx context.Context, roots paths.Roots, reviewed globalRepairPlan) error {
	lock, err := globalops.AcquireUserLock(ctx, roots.OperationsDBPath())
	if err != nil {
		return fmt.Errorf("acd support repair: acquire user lifecycle lock: %w", err)
	}
	defer lock.Release()
	current, found, err := previewGlobalRepair(ctx, roots)
	if err != nil {
		return err
	}
	if !found || !reviewed.CanRepair || current.OperationID != reviewed.OperationID ||
		current.ProvingOperationID != reviewed.ProvingOperationID ||
		current.RepairKind != reviewed.RepairKind || !current.CanRepair {
		return errors.New("acd support repair: global repair proof changed; preview again")
	}
	store, err := globalops.Open(ctx, roots.OperationsDBPath())
	if err != nil {
		return err
	}
	defer store.Close()
	if current.RepairKind == globalRepairFinalizeSetup {
		if err := finishSetupCleanup(ctx, roots, current); err != nil {
			return err
		}
		return store.AdvanceIfPhase(ctx, reviewed.OperationID, "needs_attention", "committed", "", true)
	}
	return store.AdvanceIfPhase(ctx, reviewed.OperationID, "needs_attention", "superseded",
		"proved by committed setup "+reviewed.ProvingOperationID, true)
}

func setupCleanupCanResume(operationError string) bool {
	switch operationError {
	case "setup final cleanup pending",
		"record publication hold cleanup",
		"remove publication hold",
		"cleanup migration bridge refs",
		"cleanup retained migration recovery refs":
		return true
	default:
		return false
	}
}

func inspectSetupPublicationHold(path string, operation globalops.Operation) ([]byte, bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return nil, false, errors.New("setup publication hold is not a valid regular file")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}
	var hold struct {
		OperationID string `json:"operation_id"`
		PlanDigest  string `json:"plan_digest"`
	}
	if err := json.Unmarshal(body, &hold); err != nil ||
		hold.OperationID != operation.ID || hold.PlanDigest != operation.PlanDigest {
		return nil, false, errors.New("setup publication hold does not match the interrupted operation")
	}
	return body, true, nil
}

func setupBridgeRefInventory(
	ctx context.Context,
	operationID string,
	registry *central.Registry,
) ([]migration.RefProof, error) {
	prefix := setupMigrationRefPrefix + operationID + "/"
	seenRepositories := make(map[string]bool)
	var proofs []migration.RefProof
	for _, record := range registry.Repos {
		if record.LifecycleDisabled() || seenRepositories[record.RepositoryID] {
			continue
		}
		seenRepositories[record.RepositoryID] = true
		out, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: record.Path},
			"for-each-ref", "--format=%(refname) %(objectname)", prefix)
		if err != nil {
			return nil, err
		}
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if line == "" {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) != 2 || !strings.HasPrefix(fields[0], prefix) {
				return nil, fmt.Errorf("invalid temporary bridge ref inventory in %s", record.Path)
			}
			proofs = append(proofs, migration.RefProof{
				Repo: record.Path, Ref: fields[0], CommitOID: fields[1],
			})
		}
	}
	sort.Slice(proofs, func(i, j int) bool {
		if proofs[i].Repo == proofs[j].Repo {
			return proofs[i].Ref < proofs[j].Ref
		}
		return proofs[i].Repo < proofs[j].Repo
	})
	return proofs, nil
}

func setupRecoveryManifestInventory(ctx context.Context, setupRoot string) ([]string, error) {
	entries, err := os.ReadDir(setupRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(setupRoot, entry.Name(), "migration-bridge-recovery.json")
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Size() > 16<<20 {
			return nil, fmt.Errorf("invalid bridge recovery manifest %s", path)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var manifest struct {
			OperationID string                     `json:"operation_id"`
			Retained    []migration.BridgeSnapshot `json:"retained_for_recovery"`
		}
		if err := json.Unmarshal(body, &manifest); err != nil ||
			manifest.OperationID != entry.Name() || len(manifest.Retained) == 0 {
			return nil, fmt.Errorf("invalid bridge recovery manifest %s", path)
		}
		for _, snapshot := range manifest.Retained {
			if err := verifyOperationRefAllowMissing(ctx, snapshot.Repo, snapshot.Ref, snapshot.CommitOID); err != nil {
				return nil, err
			}
		}
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths, nil
}

func verifyOperationRefAllowMissing(ctx context.Context, repo, ref, expected string) error {
	if repo == "" || ref == "" || expected == "" || !strings.HasPrefix(ref, setupMigrationRefPrefix) {
		return fmt.Errorf("invalid retained ref proof %q", ref)
	}
	actual, err := gitpkg.RevParse(ctx, repo, ref)
	if errors.Is(err, gitpkg.ErrRefNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("retained ref %s points to %s, expected %s", ref, actual, expected)
	}
	return nil
}

func finishSetupCleanup(ctx context.Context, roots paths.Roots, plan globalRepairPlan) error {
	if len(plan.setupHoldBody) > 0 {
		body, err := os.ReadFile(roots.SetupPublicationHoldPath())
		if err != nil {
			return fmt.Errorf("acd support repair: re-read setup publication hold: %w", err)
		}
		if string(body) != string(plan.setupHoldBody) {
			return errors.New("acd support repair: setup publication hold changed; preview again")
		}
		if err := os.Remove(roots.SetupPublicationHoldPath()); err != nil {
			return fmt.Errorf("acd support repair: remove setup publication hold: %w", err)
		}
		if err := syncGlobalRepairDirectory(filepath.Dir(roots.SetupPublicationHoldPath())); err != nil {
			return fmt.Errorf("acd support repair: sync setup publication hold removal: %w", err)
		}
	}
	for _, proof := range plan.bridgeRefs {
		if err := gitpkg.DeletePrivateRefDurable(ctx, proof.Repo, setupMigrationRefPrefix,
			proof.Ref, proof.CommitOID); err != nil {
			return fmt.Errorf("acd support repair: clean temporary bridge ref %s: %w", proof.Ref, err)
		}
	}
	if err := migration.CleanupBridgeRecoveryManifests(ctx, plan.recoveryManifests); err != nil {
		return fmt.Errorf("acd support repair: clean retained migration recovery refs: %w", err)
	}
	return nil
}

func syncGlobalRepairDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func runGlobalOnlyRepair(
	ctx context.Context,
	out io.Writer,
	roots paths.Roots,
	plans []globalRepairPlan,
	apply, jsonOut bool,
) error {
	repairable := true
	for _, plan := range plans {
		if !plan.CanRepair {
			repairable = false
			break
		}
	}
	preview := struct {
		Global []globalRepairPlan `json:"global"`
	}{Global: plans}
	if !apply {
		if jsonOut {
			var next *string
			if repairable {
				value := "acd support repair --yes"
				next = &value
			}
			return renderAnyProductEnvelope(out, productEnvelope{OK: true, State: productStateNeedsAction,
				Actions: []productAction{}, NextAction: next, Data: preview}, true)
		}
		for _, plan := range plans {
			fmt.Fprintf(out, "Global setup operation %s needs repair.\n", plan.OperationID)
			if plan.CanRepair {
				fmt.Fprintf(out, "Proved safe through committed setup %s.\n", plan.ProvingOperationID)
			} else {
				fmt.Fprintf(out, "Cannot repair automatically: %s\n", plan.Refusal)
			}
		}
		if repairable {
			fmt.Fprintln(out, "Apply: acd support repair --yes")
		}
		return nil
	}
	if !repairable {
		for _, plan := range plans {
			if !plan.CanRepair {
				return actionRequiredError("repair_unavailable",
					fmt.Sprintf("global setup operation %s: %s", plan.OperationID, plan.Refusal))
			}
		}
	}
	actions := make([]productAction, 0, len(plans))
	for _, plan := range plans {
		if err := applyGlobalRepair(ctx, roots, plan); err != nil {
			return actionRequiredError("repair_failed", err.Error())
		}
		actions = append(actions, productAction{Kind: "global_repair", Status: "completed", Target: plan.OperationID})
		if !jsonOut {
			fmt.Fprintf(out, "Resolved global setup operation %s.\n", plan.OperationID)
		}
	}
	if jsonOut {
		return renderAnyProductEnvelope(out, productEnvelope{OK: true, State: productStateProtected,
			Changed: true, Actions: actions, Data: map[string]any{"preview": preview}}, true)
	}
	return nil
}

func stepsSuperseded(interrupted, committed []globalops.Step) bool {
	if len(interrupted) == 0 {
		return false
	}
	available := make(map[string]bool, len(committed))
	for _, step := range committed {
		available[step.Kind+"\x00"+step.Target] = true
	}
	for _, step := range interrupted {
		if !available[step.Kind+"\x00"+step.Target] {
			return false
		}
	}
	return true
}

func runtimeReferencesOperation(roots paths.Roots, operationID string, ignoredPaths ...string) (bool, error) {
	runRoot := filepath.Dir(roots.SupervisorSocketPath())
	ignored := make(map[string]bool, len(ignoredPaths))
	for _, path := range ignoredPaths {
		ignored[filepath.Clean(path)] = true
	}
	entries, err := os.ReadDir(runRoot)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		path := filepath.Join(runRoot, entry.Name())
		if ignored[filepath.Clean(path)] {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return false, err
		}
		if !info.Mode().IsRegular() || info.Size() > 1<<20 {
			continue
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return false, err
		}
		if strings.Contains(string(body), operationID) {
			return true, nil
		}
	}
	return false, nil
}

func verifyOperationProofManifests(ctx context.Context, operationID string, paths []string) error {
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() > 16<<20 {
			return fmt.Errorf("invalid proof manifest %s", path)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.HasSuffix(path, ".created-refs.json") {
			var proofs []migration.RefProof
			if err := json.Unmarshal(body, &proofs); err != nil || len(proofs) == 0 {
				return fmt.Errorf("invalid created-ref manifest %s", path)
			}
			for _, proof := range proofs {
				if err := verifyOperationRef(ctx, proof.Repo, proof.Ref, proof.CommitOID); err != nil {
					return err
				}
			}
			continue
		}
		var manifest struct {
			OperationID string                     `json:"operation_id"`
			Retained    []migration.BridgeSnapshot `json:"retained_for_recovery"`
		}
		if err := json.Unmarshal(body, &manifest); err != nil ||
			manifest.OperationID != operationID || len(manifest.Retained) == 0 {
			return fmt.Errorf("invalid bridge recovery manifest %s", path)
		}
		for _, snapshot := range manifest.Retained {
			if err := verifyOperationRef(ctx, snapshot.Repo, snapshot.Ref, snapshot.CommitOID); err != nil {
				return err
			}
		}
	}
	return nil
}

func verifyOperationRef(ctx context.Context, repo, ref, expected string) error {
	if repo == "" || ref == "" || expected == "" || !strings.HasPrefix(ref, "refs/acd/") {
		return fmt.Errorf("invalid retained ref proof %q", ref)
	}
	actual, err := gitpkg.RevParse(ctx, repo, ref)
	if err != nil {
		return fmt.Errorf("resolve retained ref %s: %w", ref, err)
	}
	if actual != expected {
		return fmt.Errorf("retained ref %s points to %s, expected %s", ref, actual, expected)
	}
	return nil
}
