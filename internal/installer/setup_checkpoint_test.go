package installer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestSetupPreviewSurvivesCurrentCheckpointGrowth(t *testing.T) {
	ctx := t.Context()
	roots, repo, executable := installerFixture(t, ctx)
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, state.DBPathFromGitDir(wt.GitDir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	registry := central.NewRegistry()
	if _, err := registry.RegisterResolvedRepo(wt, "test", 1); err != nil {
		t.Fatal(err)
	}
	if err := central.Save(roots, registry); err != nil {
		t.Fatal(err)
	}
	uninstallCheckpoint(t, repo, db)
	reviewed, err := BuildPlan(ctx, roots, Options{Executable: executable, Integrations: "none", SkipServiceCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	if reviewed.Mode != "full" {
		t.Fatalf("expected full setup path, got %s", reviewed.Mode)
	}
	// Continuous protection saves another edit between preview and apply.
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("later protected work\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	latest := uninstallCheckpoint(t, repo, db)
	if err := revalidateSetupPlan(ctx, roots, reviewed, false); err != nil {
		t.Fatalf("ordinary protection invalidated setup: %v", err)
	}
	if got, err := gitpkg.RevParse(ctx, repo, latest.Checkpoint.Ref); err != nil || got != latest.Checkpoint.CommitOID {
		t.Fatalf("latest checkpoint changed: %s %v", got, err)
	}
}
