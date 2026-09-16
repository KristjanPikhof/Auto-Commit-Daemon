package cli

import (
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func registerDisabledRepo(t *testing.T, roots paths.Roots, repoDir string) {
	t.Helper()
	stateDB := state.DBPathFromGitDir(filepath.Join(repoDir, ".git"))
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.UpsertRepo(repoDir, "disabled-hash", stateDB, "codex", 10)
		reg.DisableRepo(central.RepoRemovalTarget{Path: repoDir}, 20)
		return nil
	}); err != nil {
		t.Fatalf("register disabled repo: %v", err)
	}
}
