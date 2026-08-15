package central

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestReconcileMissingReposRemovesSafeRows(t *testing.T) {
	present := t.TempDir()
	registry := &Registry{Version: RegistryVersion, Repos: []RepoRecord{
		{Path: present, StateDB: filepath.Join(present, "state.db"),
			FirstRegisteredTS: 1},
		{Path: filepath.Join(t.TempDir(), "deleted"),
			StateDB:           filepath.Join(t.TempDir(), "deleted-state.db"),
			FirstRegisteredTS: 1},
	}}

	removed, disabled, err := registry.ReconcileMissingRepos(
		context.Background(), time.Unix(20, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || len(disabled) != 0 || len(registry.Repos) != 1 ||
		registry.Repos[0].Path != present {
		t.Fatalf("removed=%+v disabled=%+v registry=%+v",
			removed, disabled, registry.Repos)
	}
}

func TestReconcileMissingReposDisablesUnresolvedRows(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "base", Operation: "create", Path: "protected.txt",
		Fidelity: "exact",
	}, []state.CaptureOp{{
		Op: "create", Path: "protected.txt", Fidelity: "exact",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "deleted")
	registry := &Registry{Version: RegistryVersion, Repos: []RepoRecord{{
		Path: missing, StateDB: dbPath,
		RepositoryID: "0123456789abcdef", WorktreeID: "fedcba9876543210",
		FirstRegisteredTS: 1,
	}}}

	removed, disabled, err := registry.ReconcileMissingRepos(
		ctx, time.Unix(20, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 || len(disabled) != 1 || len(registry.Repos) != 1 ||
		!registry.Repos[0].LifecycleDisabled() ||
		registry.Repos[0].LifecycleUpdatedTS != 20 {
		t.Fatalf("removed=%+v disabled=%+v registry=%+v",
			removed, disabled, registry.Repos)
	}
}
