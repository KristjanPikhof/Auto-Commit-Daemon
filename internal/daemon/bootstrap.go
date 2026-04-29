// bootstrap.go seeds shadow_paths from the active branch's HEAD tree before
// the first capture pass.
//
// Without this, the very first capture would see every file already on disk
// as "unknown" → emit a 'create' event per path → drive the daemon to commit
// files that already exist at HEAD. The legacy daemon's bootstrap_shadow
// solves the same problem; this helper is its Go port.
//
// Idempotent: calling BootstrapShadow on an already-seeded (branch,
// generation) is a no-op when the existing rows already match HEAD's tree.
// We use UpsertShadowPath so re-running across daemon restarts is safe.
package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// EnvShadowRetentionGenerations controls how many prior shadow generations are
// retained after a successful reseed.
const EnvShadowRetentionGenerations = "ACD_SHADOW_RETENTION_GENERATIONS"

// DefaultShadowRetentionGenerations keeps one previous generation for local
// inspection while bounding shadow_paths growth across repeated rebases.
const DefaultShadowRetentionGenerations int64 = 1

func resolveShadowRetentionGenerations() int64 {
	if env := os.Getenv(EnvShadowRetentionGenerations); env != "" {
		if n, err := strconv.ParseInt(env, 10, 64); err == nil && n >= 0 {
			return n
		}
	}
	return DefaultShadowRetentionGenerations
}

func pruneShadowGenerations(ctx context.Context, db *state.DB, cctx CaptureContext) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("daemon: pruneShadowGenerations: nil db")
	}
	if cctx.BranchRef == "" || cctx.BranchGeneration <= 0 {
		return 0, nil
	}
	return state.PruneShadowGenerations(ctx, db, cctx.BranchRef, cctx.BranchGeneration, resolveShadowRetentionGenerations())
}

// BootstrapShadow seeds shadow_paths for (cctx.BranchRef,
// cctx.BranchGeneration) from HEAD's tree at cctx.BaseHead. Returns the
// number of rows seeded. A missing/empty BaseHead is a no-op (orphan repo
// case — the next capture pass will see every file as a create against an
// empty shadow, which is the correct behaviour on a brand new branch).
//
// Submodule entries (mode 160000) are skipped — submodules live outside
// the worktree the daemon owns.
func BootstrapShadow(ctx context.Context, repoDir string, db *state.DB, cctx CaptureContext) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("daemon: BootstrapShadow: nil db")
	}
	if cctx.BranchRef == "" {
		return 0, fmt.Errorf("daemon: BootstrapShadow: empty branch_ref")
	}
	if cctx.BaseHead == "" {
		return 0, nil
	}

	// Skip if shadow already populated for this (branch, generation). The
	// presence check uses a single COUNT(*) query so we don't redundantly
	// re-walk HEAD on every daemon restart.
	var existing int
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM shadow_paths WHERE branch_ref = ? AND branch_generation = ?`,
		cctx.BranchRef, cctx.BranchGeneration,
	).Scan(&existing); err != nil {
		return 0, fmt.Errorf("daemon: count shadow: %w", err)
	}
	if existing > 0 {
		return 0, nil
	}

	entries, err := git.LsTree(ctx, repoDir, cctx.BaseHead, true)
	if err != nil {
		return 0, fmt.Errorf("daemon: ls-tree HEAD: %w", err)
	}

	seeded := 0
	for _, e := range entries {
		// Submodules (gitlinks) are not part of our worktree.
		if e.Mode == "160000" {
			continue
		}
		// We only seed blob entries (regular files + symlinks). Trees
		// don't appear with -r anyway; defensive guard for future use.
		if e.Type != "blob" {
			continue
		}
		sp := state.ShadowPath{
			BranchRef:        cctx.BranchRef,
			BranchGeneration: cctx.BranchGeneration,
			Path:             e.Path,
			Operation:        "bootstrap",
			Mode:             sql.NullString{String: e.Mode, Valid: true},
			OID:              sql.NullString{String: e.OID, Valid: true},
			BaseHead:         cctx.BaseHead,
			Fidelity:         "full",
		}
		if err := state.UpsertShadowPath(ctx, db, sp); err != nil {
			return seeded, fmt.Errorf("daemon: upsert shadow %q: %w", e.Path, err)
		}
		seeded++
	}
	return seeded, nil
}
