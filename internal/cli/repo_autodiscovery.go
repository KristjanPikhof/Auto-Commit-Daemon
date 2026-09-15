package cli

import (
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

func registryRecordActivatedForWorktree(rec central.RepoRecord, wt git.Worktree) bool {
	return registryRecordHasCanonicalIdentity(rec) &&
		central.SameRepoPath(rec.Path, wt.Root) &&
		rec.RepositoryID == central.CanonicalID(wt.CommonDir) &&
		rec.WorktreeID == central.CanonicalID(wt.Root)
}

func registryRecordHasCanonicalIdentity(rec central.RepoRecord) bool {
	return rec.Path != "" && rec.CommonDir != "" &&
		rec.RepositoryID != "" && rec.WorktreeID != "" &&
		rec.RepositoryID == central.CanonicalID(rec.CommonDir) &&
		rec.WorktreeID == central.CanonicalID(rec.Path)
}
