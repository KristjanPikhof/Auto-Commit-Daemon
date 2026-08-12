package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestRestorePreviewReadsDirectlyWithoutSupervisor(t *testing.T) {
	roots, repo, stateDB := registeredProductMutationRepo(t)
	before := fileDigest(t, stateDB)

	err := runRestore(context.Background(), &bytes.Buffer{}, repo, "missing-checkpoint", false, false)
	if err == nil || !strings.Contains(err.Error(), "preview") {
		t.Fatalf("restore preview error=%v, want direct checkpoint preview error", err)
	}
	if strings.Contains(err.Error(), "supervisor") || strings.Contains(err.Error(), "managed ACD binary") {
		t.Fatalf("restore preview attempted to start runtime: %v", err)
	}
	if _, err := os.Stat(roots.SupervisorSocketPath()); !os.IsNotExist(err) {
		t.Fatalf("restore preview created supervisor socket: %v", err)
	}
	if after := fileDigest(t, stateDB); after != before {
		t.Fatalf("restore preview changed state DB: before=%s after=%s", before, after)
	}
}

func TestRepositoryRepairPreviewReadsDirectlyWithoutSupervisor(t *testing.T) {
	roots, repo, stateDB := registeredProductMutationRepo(t)
	before := fileDigest(t, stateDB)

	var out bytes.Buffer
	if err := runProductRepair(context.Background(), &out, repo, false, false); err != nil {
		t.Fatalf("repair preview: %v", err)
	}
	if !strings.Contains(out.String(), "No interrupted operation needs repair") {
		t.Fatalf("repair preview output=%q", out.String())
	}
	if _, err := os.Stat(roots.SupervisorSocketPath()); !os.IsNotExist(err) {
		t.Fatalf("repair preview created supervisor socket: %v", err)
	}
	if after := fileDigest(t, stateDB); after != before {
		t.Fatalf("repair preview changed state DB: before=%s after=%s", before, after)
	}
}

func registeredProductMutationRepo(t *testing.T) (paths.Roots, string, string) {
	t.Helper()
	roots := withIsolatedHome(t)
	repo, stateDB, db := makeRepoStateDB(t)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	registerRepo(t, roots, repo, stateDB, "codex")
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		for i := range registry.Repos {
			if registry.Repos[i].Path == repo {
				registry.Repos[i].RepositoryID = "repository-id"
				registry.Repos[i].WorktreeID = "worktree-id"
				return nil
			}
		}
		return fmt.Errorf("registered repository %s not found", repo)
	}); err != nil {
		t.Fatal(err)
	}
	return roots, repo, stateDB
}

func fileDigest(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(body))
}
