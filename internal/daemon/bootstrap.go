// bootstrap.go seeds shadow_paths from the active branch's HEAD tree before
// the first capture pass.
//
// Without this, the very first capture would see every file already on disk
// as "unknown" → emit a 'create' event per path → drive the daemon to commit
// files that already exist at HEAD. The legacy daemon's bootstrap_shadow
// solves the same problem; this helper is its Go port.
//
// # Atomicity
//
// Seeding writes shadow_paths in fixed-size chunks (`shadowBootstrapChunkSize`)
// using state.AppendShadowBatch — each chunk is its own transaction with a
// reused prepared statement, avoiding the per-row begin/commit fsync overhead
// that previously wedged 30k+ file repos at startup. Chunking trades
// "all-or-nothing across the whole reseed" for "each chunk is atomic", but
// completion is still all-or-nothing at the daemon-meta level: the
// MetaKeyShadowBootstrapped marker is set ONLY after every chunk succeeds.
// On any chunk failure we delete the partial rows for the active
// (branch_ref, branch_generation) before returning the error — so a retry
// starts from an empty shadow set instead of resuming half-seeded state.
//
// # Idempotency
//
// Idempotency is keyed on a daemon_meta marker
// (`shadow.bootstrapped:<branch_ref>:<branch_generation>`) rather than a
// COUNT(*) probe. The COUNT-based check could not distinguish "fully seeded"
// from "crashed mid-seed" and would skip reseed after a partial failure,
// leaving the next capture pass to classify every tracked file as a phantom
// `create`. The marker is the explicit completion signal.
//
// Capture/replay must refuse to operate on a generation without this marker.
// BootstrapShadow itself surfaces the gate: it returns early if the marker is
// already present, otherwise it does the work and writes the marker as the
// last step.
package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sort"
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

// shadowBootstrapChunkSize bounds the number of rows written per
// AppendShadowBatch transaction. 5000 keeps each tx small enough to commit
// well under SQLite's default busy_timeout while still amortizing the
// begin/commit fsync over thousands of rows. Tuning rationale: a 30k-file
// repo seeds in 6 chunks instead of 30k independent commits.
const shadowBootstrapChunkSize = 5000

// A pending event normally owns one path and a rename owns two. Keep the
// partial-recovery overlay bounded even when the normal capture cap is
// disabled by configuration.
const maxPreservedShadowPaths = DefaultMaxPendingEvents * 2

// MetaKeyShadowBootstrappedPrefix is the daemon_meta key prefix used to mark
// a (branch_ref, branch_generation) pair as fully seeded. The full key is
// formatted by ShadowBootstrappedKey.
const MetaKeyShadowBootstrappedPrefix = "shadow.bootstrapped:"

// ShadowBootstrappedKey returns the daemon_meta key under which the
// completion marker is stored for a given (branch_ref, branch_generation)
// pair. Format: `shadow.bootstrapped:<branch_ref>:<branch_generation>`.
func ShadowBootstrappedKey(branchRef string, generation int64) string {
	return fmt.Sprintf("%s%s:%d", MetaKeyShadowBootstrappedPrefix, branchRef, generation)
}

// appendShadowBatchFn is a test seam. Production code calls
// state.AppendShadowBatch directly; tests can swap this var to inject errors
// mid-bootstrap and exercise the partial-row cleanup path.
var appendShadowBatchFn = state.AppendShadowBatch

func resolveShadowRetentionGenerations() int64 {
	if env := os.Getenv(EnvShadowRetentionGenerations); env != "" {
		if n, err := strconv.ParseInt(env, 10, 64); err == nil && n >= 0 {
			return n
		}
	}
	return DefaultShadowRetentionGenerations
}

func pruneShadowGenerations(ctx context.Context, db *state.DB, cctx CaptureContext) (int, error) {
	return pruneShadowGenerationsWithRetention(ctx, db, cctx, resolveShadowRetentionGenerations())
}

func pruneShadowGenerationsWithRetention(ctx context.Context, db *state.DB, cctx CaptureContext, retention int64) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("daemon: pruneShadowGenerations: nil db")
	}
	if cctx.BranchRef == "" || cctx.BranchGeneration <= 0 {
		return 0, nil
	}
	return state.PruneShadowGenerations(ctx, db, cctx.BranchRef, cctx.BranchGeneration, retention)
}

// IsShadowBootstrapped reports whether the (branch_ref, branch_generation)
// pair has a completion marker in daemon_meta. Capture/replay should refuse
// to operate on a generation that returns false here — the shadow set is
// either unseeded or known-partial from a crashed reseed.
func IsShadowBootstrapped(ctx context.Context, db *state.DB, branchRef string, generation int64) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("daemon: IsShadowBootstrapped: nil db")
	}
	if branchRef == "" {
		return false, fmt.Errorf("daemon: IsShadowBootstrapped: empty branch_ref")
	}
	_, ok, err := state.MetaGet(ctx, db, ShadowBootstrappedKey(branchRef, generation))
	if err != nil {
		return false, fmt.Errorf("daemon: read shadow bootstrap marker: %w", err)
	}
	return ok, nil
}

// BootstrapShadow seeds shadow_paths for (cctx.BranchRef,
// cctx.BranchGeneration) from HEAD's tree at cctx.BaseHead. Returns the
// number of rows seeded (0 when the marker was already present and no work
// was needed). A missing/empty BaseHead is a no-op (orphan repo case — the
// next capture pass will see every file as a create against an empty shadow,
// which is the correct behaviour on a brand new branch). The completion
// marker is still set in the orphan case so capture/replay can proceed.
//
// Submodule entries (mode 160000) are skipped — submodules live outside
// the worktree the daemon owns.
//
// On any chunk failure mid-seed, partial rows for the active
// (branch_ref, branch_generation) are deleted before the error is returned.
// The completion marker is NOT set in that case — a retry starts from an
// empty shadow set.
func BootstrapShadow(ctx context.Context, repoDir string, db *state.DB, cctx CaptureContext) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("daemon: BootstrapShadow: nil db")
	}
	if cctx.BranchRef == "" {
		return 0, fmt.Errorf("daemon: BootstrapShadow: empty branch_ref")
	}

	// Marker-based idempotency: if a completion marker already exists for
	// this (branch, generation), the shadow set is known-good and we skip
	// the ls-tree walk + reseed entirely.
	already, err := IsShadowBootstrapped(ctx, db, cctx.BranchRef, cctx.BranchGeneration)
	if err != nil {
		return 0, err
	}
	if already {
		return 0, nil
	}

	// Orphan-branch case: no HEAD to walk. Set the marker so capture/replay
	// can proceed against an empty shadow set; the first capture pass will
	// observe every on-disk file as a create against the empty shadow,
	// which is the correct behaviour for a brand-new branch.
	if cctx.BaseHead == "" {
		if err := setShadowBootstrappedMarker(ctx, db, cctx); err != nil {
			return 0, err
		}
		return 0, nil
	}

	entries, err := git.LsTree(ctx, repoDir, cctx.BaseHead, true)
	if err != nil {
		return 0, fmt.Errorf("daemon: ls-tree HEAD: %w", err)
	}

	// Build the slice of rows to upsert. We materialize the full slice
	// because git.LsTree already returns the complete entry list — the
	// memory cost is dominated by the ls-tree call itself, not the slice.
	rows := make([]state.ShadowPath, 0, len(entries))
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
		rows = append(rows, state.ShadowPath{
			BranchRef:        cctx.BranchRef,
			BranchGeneration: cctx.BranchGeneration,
			Path:             e.Path,
			Operation:        "bootstrap",
			Mode:             sql.NullString{String: e.Mode, Valid: true},
			OID:              sql.NullString{String: e.OID, Valid: true},
			BaseHead:         cctx.BaseHead,
			Fidelity:         "full",
		})
	}

	seeded := 0
	for start := 0; start < len(rows); start += shadowBootstrapChunkSize {
		end := start + shadowBootstrapChunkSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[start:end]
		if err := appendShadowBatchFn(ctx, db, chunk); err != nil {
			// Cleanup partial rows so a retry does not see a half-seeded
			// shadow set. We use a background context so the cleanup
			// runs even when the inbound ctx was canceled mid-seed.
			cleanupErr := cleanupPartialShadow(db, cctx)
			if cleanupErr != nil {
				return seeded, fmt.Errorf("daemon: append shadow batch [%d:%d]: %w (cleanup: %v)", start, end, err, cleanupErr)
			}
			return seeded, fmt.Errorf("daemon: append shadow batch [%d:%d]: %w", start, end, err)
		}
		seeded += len(chunk)
	}

	if err := setShadowBootstrappedMarker(ctx, db, cctx); err != nil {
		// We seeded rows but failed to stamp the marker. Clean up so the
		// next call retries from scratch instead of being blocked by the
		// gate forever.
		if cleanupErr := cleanupPartialShadow(db, cctx); cleanupErr != nil {
			return seeded, fmt.Errorf("%w (cleanup: %v)", err, cleanupErr)
		}
		return seeded, err
	}
	return seeded, nil
}

// ReseedShadowFromHead replaces the shadow rows for the active
// (branch_ref, branch_generation) with cctx.BaseHead's tree even when the
// normal bootstrap marker is already present. External same-branch
// fast-forwards use this to absorb upstream changes into the shadow baseline
// instead of capturing them as local worktree edits.
func ReseedShadowFromHead(ctx context.Context, repoDir string, db *state.DB, cctx CaptureContext) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("daemon: ReseedShadowFromHead: nil db")
	}
	if cctx.BranchRef == "" {
		return 0, fmt.Errorf("daemon: ReseedShadowFromHead: empty branch_ref")
	}
	if err := state.InvalidateShadowGeneration(ctx, db, cctx.BranchRef, cctx.BranchGeneration,
		ShadowBootstrappedKey(cctx.BranchRef, cctx.BranchGeneration)); err != nil {
		return 0, err
	}
	return BootstrapShadow(ctx, repoDir, db, cctx)
}

// ReseedShadowFromHeadPreservingUnpublished builds a fresh shadow from HEAD,
// then overlays only paths represented by the remaining unpublished capture
// chain. This absorbs unrelated external commits without forgetting dirty work
// that ACD already captured behind a frozen publication target.
func ReseedShadowFromHeadPreservingUnpublished(
	ctx context.Context,
	repoDir string,
	db *state.DB,
	cctx CaptureContext,
	chain []state.RecoveryChainEvent,
) (int, error) {
	if db == nil || cctx.BranchRef == "" || cctx.BranchGeneration < 1 ||
		cctx.BaseHead == "" || len(chain) == 0 {
		return 0, fmt.Errorf(
			"daemon: preserve unpublished shadow: invalid recovery context")
	}
	touched := make(map[string]struct{})
	addPath := func(path string) error {
		if path == "" {
			return nil
		}
		touched[path] = struct{}{}
		if len(touched) > maxPreservedShadowPaths {
			return fmt.Errorf(
				"daemon: preserve unpublished shadow: path limit exceeds %d",
				maxPreservedShadowPaths)
		}
		return nil
	}
	for _, item := range chain {
		if item.Event.BranchRef != cctx.BranchRef ||
			item.Event.BranchGeneration != cctx.BranchGeneration {
			return 0, fmt.Errorf(
				"daemon: preserve unpublished shadow: capture %d changed branch identity",
				item.Event.Seq)
		}
		switch item.Event.State {
		case state.EventStatePending, state.EventStateBlockedConflict,
			state.EventStateFailed:
		default:
			return 0, fmt.Errorf(
				"daemon: preserve unpublished shadow: capture %d is %s",
				item.Event.Seq, item.Event.State)
		}
		if err := addPath(item.Event.Path); err != nil {
			return 0, err
		}
		if item.Event.OldPath.Valid {
			if err := addPath(item.Event.OldPath.String); err != nil {
				return 0, err
			}
		}
		for _, op := range item.Ops {
			if err := addPath(op.Path); err != nil {
				return 0, err
			}
			if op.OldPath.Valid {
				if err := addPath(op.OldPath.String); err != nil {
					return 0, err
				}
			}
		}
	}

	retained := make(map[string]state.ShadowPath, len(touched))
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT branch_ref, branch_generation, path, operation, mode, oid,
       old_path, base_head, fidelity, updated_ts
FROM shadow_paths
WHERE branch_ref = ? AND branch_generation = ?`,
		cctx.BranchRef, cctx.BranchGeneration)
	if err != nil {
		return 0, fmt.Errorf("daemon: preserve unpublished shadow: query current rows: %w", err)
	}
	for rows.Next() {
		var row state.ShadowPath
		if err := rows.Scan(
			&row.BranchRef, &row.BranchGeneration, &row.Path, &row.Operation,
			&row.Mode, &row.OID, &row.OldPath, &row.BaseHead, &row.Fidelity,
			&row.UpdatedTS); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf(
				"daemon: preserve unpublished shadow: scan current row: %w", err)
		}
		if _, keep := touched[row.Path]; keep {
			row.BaseHead = cctx.BaseHead
			retained[row.Path] = row
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf(
			"daemon: preserve unpublished shadow: iterate current rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf(
			"daemon: preserve unpublished shadow: close current rows: %w", err)
	}

	entries, err := git.LsTree(ctx, repoDir, cctx.BaseHead, true)
	if err != nil {
		return 0, fmt.Errorf("daemon: preserve unpublished shadow: ls-tree HEAD: %w", err)
	}
	merged := make(map[string]state.ShadowPath, len(entries)+len(retained))
	for _, entry := range entries {
		if entry.Mode == "160000" || entry.Type != "blob" {
			continue
		}
		merged[entry.Path] = state.ShadowPath{
			BranchRef:        cctx.BranchRef,
			BranchGeneration: cctx.BranchGeneration,
			Path:             entry.Path,
			Operation:        "bootstrap",
			Mode:             sql.NullString{String: entry.Mode, Valid: true},
			OID:              sql.NullString{String: entry.OID, Valid: true},
			BaseHead:         cctx.BaseHead,
			Fidelity:         "full",
		}
	}
	for path := range touched {
		delete(merged, path)
		if row, ok := retained[path]; ok {
			merged[path] = row
		}
	}
	paths := make([]string, 0, len(merged))
	for path := range merged {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	mergedRows := make([]state.ShadowPath, 0, len(paths))
	for _, path := range paths {
		mergedRows = append(mergedRows, merged[path])
	}
	return state.ReplaceShadowGeneration(
		ctx, db, cctx.BranchRef, cctx.BranchGeneration,
		ShadowBootstrappedKey(cctx.BranchRef, cctx.BranchGeneration), mergedRows)
}

func setShadowBootstrappedMarker(ctx context.Context, db *state.DB, cctx CaptureContext) error {
	key := ShadowBootstrappedKey(cctx.BranchRef, cctx.BranchGeneration)
	if err := state.MetaSet(ctx, db, key, "1"); err != nil {
		return fmt.Errorf("daemon: set shadow bootstrap marker: %w", err)
	}
	return nil
}

// cleanupPartialShadow removes any shadow_paths rows for the active
// (branch_ref, branch_generation). Uses a fresh background context with no
// deadline because callers reach this path on ctx-cancel mid-seed; we still
// want the cleanup to run to completion so a retry sees a clean slate.
func cleanupPartialShadow(db *state.DB, cctx CaptureContext) error {
	if cctx.BranchRef == "" {
		return nil
	}
	bg := context.Background()
	if _, err := state.DeleteShadowGeneration(bg, db, cctx.BranchRef, cctx.BranchGeneration); err != nil {
		// Best-effort cleanup; surface the error so the caller can log.
		return err
	}
	return nil
}

// errShadowMissing is exposed for test inspection of the gate behaviour.
var errShadowMissing = errors.New("daemon: shadow not bootstrapped for active generation")
