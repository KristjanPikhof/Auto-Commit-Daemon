package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/globalops"
)

func TestGlobalRepairRequiresAndAppliesCompleteProof(t *testing.T) {
	ctx := context.Background()
	roots := withIsolatedHome(t)
	if err := central.Save(roots, &central.Registry{Version: central.RegistryVersion}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(roots.ManagedBinaryPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(roots.ManagedBinaryPath(), []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal, err := globalops.Open(ctx, roots.OperationsDBPath())
	if err != nil {
		t.Fatal(err)
	}
	steps := []globalops.Step{{Sequence: 1, Kind: "write_registry", Target: roots.RegistryPath(), Phase: "planned"}}
	if err := journal.Prepare(ctx, globalops.Operation{ID: "interrupted", Kind: "setup", Phase: "planned", PlanDigest: "old"}, steps); err != nil {
		t.Fatal(err)
	}
	if err := journal.Advance(ctx, "interrupted", "needs_attention", "failed", true); err != nil {
		t.Fatal(err)
	}
	if err := journal.Prepare(ctx, globalops.Operation{ID: "current", Kind: "setup", Phase: "planned", PlanDigest: "new"}, steps); err != nil {
		t.Fatal(err)
	}
	if err := journal.Advance(ctx, "current", "committed", "", true); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(roots.SetupOperationDir("interrupted"), 0o700); err != nil {
		t.Fatal(err)
	}

	plan, found, err := previewGlobalRepair(ctx, roots)
	if err != nil || !found || !plan.CanRepair || plan.ProvingOperationID != "current" {
		t.Fatalf("plan=%+v found=%t err=%v", plan, found, err)
	}
	if err := applyGlobalRepair(ctx, roots, plan); err != nil {
		t.Fatal(err)
	}
	store, err := globalops.OpenReadOnly(ctx, roots.OperationsDBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	resolved, err := store.OperationsByPhase(ctx, "superseded")
	if err != nil || len(resolved) != 1 || resolved[0].ID != "interrupted" ||
		!strings.Contains(resolved[0].Error, "current") {
		t.Fatalf("resolved=%+v err=%v", resolved, err)
	}
}

func TestGlobalRepairFinalizesLatestInterruptedSetupCleanup(t *testing.T) {
	ctx := context.Background()
	roots := withIsolatedHome(t)
	repo, stateDB, db := makeSeededRepoStateDB(t)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	registerRepo(t, roots, repo, stateDB, "codex")
	if err := os.MkdirAll(filepath.Dir(roots.ManagedBinaryPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(roots.ManagedBinaryPath(), []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}

	const operationID = "cleanup"
	const planDigest = "cleanup-digest"
	journal, err := globalops.Open(ctx, roots.OperationsDBPath())
	if err != nil {
		t.Fatal(err)
	}
	steps := []globalops.Step{{Sequence: 1, Kind: "write_registry", Target: roots.RegistryPath(), Phase: "planned"}}
	if err := journal.Prepare(ctx, globalops.Operation{
		ID: operationID, Kind: "setup", Phase: "planned", PlanDigest: planDigest,
	}, steps); err != nil {
		t.Fatal(err)
	}
	if err := journal.Advance(ctx, operationID, "needs_attention", "setup final cleanup pending", false); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(roots.SetupOperationDir(operationID), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(roots.SetupPublicationHoldPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	holdBody := []byte(`{"operation_id":"cleanup","plan_digest":"cleanup-digest"}` + "\n")
	if err := os.WriteFile(roots.SetupPublicationHoldPath(), holdBody, 0o600); err != nil {
		t.Fatal(err)
	}
	head, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	bridgeRef := setupMigrationRefPrefix + operationID + "/worktree/1"
	if _, err := gitpkg.EnsurePrivateRefDurable(ctx, repo,
		setupMigrationRefPrefix, bridgeRef, head); err != nil {
		t.Fatal(err)
	}

	plan, found, err := previewGlobalRepair(ctx, roots)
	if err != nil || !found || !plan.CanRepair || plan.RepairKind != globalRepairFinalizeSetup ||
		len(plan.bridgeRefs) != 1 {
		t.Fatalf("plan=%+v found=%t err=%v", plan, found, err)
	}
	if err := applyGlobalRepair(ctx, roots, plan); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(roots.SetupPublicationHoldPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("publication hold remains: %v", err)
	}
	if _, err := gitpkg.RevParse(ctx, repo, bridgeRef); !errors.Is(err, gitpkg.ErrRefNotFound) {
		t.Fatalf("temporary bridge ref remains: %v", err)
	}
	store, err := globalops.OpenReadOnly(ctx, roots.OperationsDBPath())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	committed, ok, err := store.LatestCommittedSetup(ctx)
	if err != nil || !ok || committed.ID != operationID {
		t.Fatalf("committed=%+v ok=%t err=%v", committed, ok, err)
	}
}

func TestGlobalRepairRefusesMissingOperationDirectory(t *testing.T) {
	ctx := context.Background()
	roots := withIsolatedHome(t)
	if err := central.Save(roots, &central.Registry{Version: central.RegistryVersion}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(roots.ManagedBinaryPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(roots.ManagedBinaryPath(), []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal, err := globalops.Open(ctx, roots.OperationsDBPath())
	if err != nil {
		t.Fatal(err)
	}
	steps := []globalops.Step{{Sequence: 1, Kind: "write_registry", Target: roots.RegistryPath(), Phase: "planned"}}
	if err := journal.Prepare(ctx, globalops.Operation{ID: "missing", Kind: "setup", Phase: "planned", PlanDigest: "old"}, steps); err != nil {
		t.Fatal(err)
	}
	if err := journal.Advance(ctx, "missing", "needs_attention", "failed", true); err != nil {
		t.Fatal(err)
	}
	if err := journal.Prepare(ctx, globalops.Operation{ID: "newer", Kind: "setup", Phase: "planned", PlanDigest: "new"}, steps); err != nil {
		t.Fatal(err)
	}
	if err := journal.Advance(ctx, "newer", "committed", "", true); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	plan, found, err := previewGlobalRepair(ctx, roots)
	if err != nil || !found || plan.CanRepair || !strings.Contains(plan.Refusal, "directory") {
		t.Fatalf("plan=%+v found=%t err=%v", plan, found, err)
	}
}

func TestGlobalRepairAggregatesAndAppliesAllProvedOperations(t *testing.T) {
	ctx := context.Background()
	roots := withIsolatedHome(t)
	if err := central.Save(roots, &central.Registry{Version: central.RegistryVersion}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(roots.ManagedBinaryPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(roots.ManagedBinaryPath(), []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal, err := globalops.Open(ctx, roots.OperationsDBPath())
	if err != nil {
		t.Fatal(err)
	}
	steps := []globalops.Step{{Sequence: 1, Kind: "write_registry", Target: roots.RegistryPath(), Phase: "planned"}}
	for _, id := range []string{"interrupted-a", "interrupted-b"} {
		if err := journal.Prepare(ctx, globalops.Operation{ID: id, Kind: "setup", Phase: "planned", PlanDigest: id}, steps); err != nil {
			t.Fatal(err)
		}
		if err := journal.Advance(ctx, id, "needs_attention", "failed", true); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(roots.SetupOperationDir(id), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := journal.Prepare(ctx, globalops.Operation{ID: "current", Kind: "setup", Phase: "planned", PlanDigest: "current"}, steps); err != nil {
		t.Fatal(err)
	}
	if err := journal.Advance(ctx, "current", "committed", "", true); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	plans, err := previewGlobalRepairs(ctx, roots)
	if err != nil || len(plans) != 2 {
		t.Fatalf("plans=%+v err=%v", plans, err)
	}
	for _, plan := range plans {
		if !plan.CanRepair || plan.ProvingOperationID != "current" {
			t.Fatalf("plan=%+v", plan)
		}
		if err := applyGlobalRepair(ctx, roots, plan); err != nil {
			t.Fatal(err)
		}
	}
	remaining, err := previewGlobalRepairs(ctx, roots)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("remaining=%+v err=%v", remaining, err)
	}
}

func TestGlobalRepairPreviewWorksOutsideRepository(t *testing.T) {
	ctx := context.Background()
	roots := withIsolatedHome(t)
	if err := central.Save(roots, central.NewRegistry()); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(roots.ManagedBinaryPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(roots.ManagedBinaryPath(), []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	journal, err := globalops.Open(ctx, roots.OperationsDBPath())
	if err != nil {
		t.Fatal(err)
	}
	steps := []globalops.Step{{Sequence: 1, Kind: "write_registry", Target: roots.RegistryPath(), Phase: "planned"}}
	if err := journal.Prepare(ctx, globalops.Operation{ID: "outside-repo", Kind: "setup", Phase: "planned", PlanDigest: "old"}, steps); err != nil {
		t.Fatal(err)
	}
	if err := journal.Advance(ctx, "outside-repo", "needs_attention", "failed", true); err != nil {
		t.Fatal(err)
	}
	if err := journal.Prepare(ctx, globalops.Operation{ID: "current", Kind: "setup", Phase: "planned", PlanDigest: "new"}, steps); err != nil {
		t.Fatal(err)
	}
	if err := journal.Advance(ctx, "current", "committed", "", true); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(roots.SetupOperationDir("outside-repo"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Chdir(t.TempDir())
	var out bytes.Buffer
	if err := runProductRepair(ctx, &out, "", false, false); err != nil {
		t.Fatal(err)
	}
	if text := out.String(); !strings.Contains(text, "outside-repo") || !strings.Contains(text, "acd support repair --yes") {
		t.Fatalf("global-only preview output=%q", text)
	}
}
