package migration

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

var ErrFinishLegacyAttention = errors.New("migration: legacy publication requires attention")
var errFinishLegacyObjectsMissing = errors.New("migration: legacy objects are missing")

// ActivateFinish runs only under the canonical worker owner and exclusive
// operation gate during explicit activation. It never opens/upgrades the DB,
// publishes old work, checks out files, or changes the native index.
func ActivateFinish(ctx context.Context, repo string, db *state.DB) (state.FinishLegacyCutover, error) {
	var empty state.FinishLegacyCutover
	if db == nil {
		return empty, errors.New("migration: finish activation requires a database")
	}
	version, err := db.UserVersion(ctx)
	if err != nil {
		return empty, err
	}
	if version != state.SchemaVersion {
		return empty, state.ErrSetupRequired
	}
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		return empty, err
	}
	saved, err := state.LoadFinishLegacyCutover(ctx, db)
	if err != nil {
		return saved, err
	}
	if saved.WorktreeID == "" {
		manifest, err := state.InventoryFinishLegacy(ctx, db)
		if err != nil {
			return saved, err
		}
		ref, oid, pins, err := pinFinishLegacy(ctx, wt.Root, checkpoint.WorktreeID(wt.Root), manifest)
		if err != nil {
			if ctx.Err() != nil {
				return saved, ctx.Err()
			}
			code := "legacy_proof_ambiguous"
			if errors.Is(err, errFinishLegacyObjectsMissing) {
				code = "legacy_object_missing"
			}
			saved, writeErr := state.RecordFinishLegacyActivationFailure(ctx, db, code, "Legacy available versions are retained. Restore the old object or ref proof, then activate protection again: "+err.Error(), ref, oid)
			return saved, errors.Join(ErrFinishLegacyAttention, err, writeErr)
		}
		saved, err = state.PrepareFinishLegacyCutover(ctx, db, checkpoint.WorktreeID(wt.Root), manifest, ref, oid, pins)
		if err != nil {
			return saved, err
		}
	}
	attention := func(code string, cause error) (state.FinishLegacyCutover, error) {
		reason := fmt.Sprintf("Legacy evidence is retained. Resolve %s before finish: %v", code, cause)
		r, writeErr := state.CompleteFinishLegacyCutover(ctx, db, code, reason)
		return r, errors.Join(ErrFinishLegacyAttention, cause, writeErr)
	}
	if saved.WorktreeID != checkpoint.WorktreeID(wt.Root) {
		return attention("legacy_worktree_mismatch", errors.New("canonical worktree identity changed"))
	}
	pins, err := state.FinishLegacyObjectPins(ctx, db)
	if err != nil {
		return saved, err
	}
	for _, p := range pins {
		if err = verifyFinishLegacyPin(ctx, wt.Root, p.PinRef, p.PinOID); err != nil {
			return attention("legacy_pin_moved", err)
		}
		if p.ObjectType == "blob" {
			actual, readErr := gitpkg.LsTreeBlobOID(ctx, wt.Root, p.PinOID, p.ObjectOID)
			if readErr != nil || actual != p.ObjectOID {
				return attention("legacy_object_missing", fmt.Errorf("captured object %s is no longer proved: %w", p.ObjectOID, readErr))
			}
		}
	}
	if err = verifyFinishLegacyPin(ctx, wt.Root, saved.SnapshotRef, saved.SnapshotOID); err != nil {
		return attention("legacy_pin_moved", err)
	}
	if saved.State == "completed" {
		return saved, nil
	}
	if err = daemon.RecoverSelfPublicationsBeforePlanning(ctx, wt.Root, db); err != nil {
		return attention("legacy_publication_ambiguous", err)
	}
	// Old repair engines rewrite capture OIDs. Preserve that provenance instead.
	// A prepared row with no replacement and the exact old branch is inert;
	// all other interrupted repairs retain their backup and require review.
	rows, err := db.ReadSQL().QueryContext(ctx, `SELECT id,branch_ref,status,expected_head,COALESCE(new_head,''),COALESCE(backup_ref,'') FROM intent_repairs WHERE status IN ('prepared','git_applied') ORDER BY id`)
	if err != nil {
		return saved, err
	}
	type repair struct{ id, branch, status, expected, next, backup string }
	var repairs []repair
	for rows.Next() {
		var r repair
		if err = rows.Scan(&r.id, &r.branch, &r.status, &r.expected, &r.next, &r.backup); err != nil {
			rows.Close()
			return saved, err
		}
		repairs = append(repairs, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return saved, err
	}
	for _, r := range repairs {
		if err = directFinishLegacyRef(ctx, wt.Root, r.branch); err != nil {
			return attention("legacy_repair_ambiguous", err)
		}
		head, headErr := gitpkg.RevParse(ctx, wt.Root, r.branch)
		backupRef, backupErr := gitpkg.IntentRepairBackupRef(r.branch, r.id)
		if backupErr != nil {
			return attention("legacy_repair_ambiguous", backupErr)
		}
		_, backupErr = gitpkg.RevParse(ctx, wt.Root, backupRef)
		if !errors.Is(backupErr, gitpkg.ErrRefNotFound) {
			return attention("legacy_repair_ambiguous", fmt.Errorf("repair %s retains a Git CAS backup; immutable index settlement proof is unavailable", r.id))
		}
		if r.backup != "" {
			_, declaredErr := gitpkg.RevParse(ctx, wt.Root, r.backup)
			if !errors.Is(declaredErr, gitpkg.ErrRefNotFound) {
				return attention("legacy_repair_ambiguous", fmt.Errorf("repair %s retains declared backup %s; immutable index settlement proof is unavailable", r.id, r.backup))
			}
		}
		if r.status != "prepared" || r.next != "" || headErr != nil || head != r.expected {
			return attention("legacy_repair_ambiguous", fmt.Errorf("repair %s has no proven unapplied outcome", r.id))
		}
	}
	return state.CompleteFinishLegacyCutover(ctx, db, "", "")
}

func pinFinishLegacy(ctx context.Context, repo, worktree string, m state.FinishLegacyManifest) (string, string, []state.FinishLegacyPin, error) {
	digest := strings.TrimPrefix(state.FinishLegacyDigest(m), "sha256:")
	prefix := "refs/acd/migration/finish-v1/" + worktree + "/" + digest + "/"
	var entries []gitpkg.MktreeEntry
	var available []state.FinishLegacyObject
	var missing error
	for _, o := range m.Objects {
		_, decodeErr := hex.DecodeString(o.OID)
		if decodeErr != nil || (len(o.OID) != 40 && len(o.OID) != 64) || (o.Type != "blob" && o.Type != "commit") {
			return "", "", nil, state.ErrFinishIdentity
		}
		if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo, Timeout: gitpkg.DefaultReadTimeout}, "cat-file", "-e", o.OID+"^{"+o.Type+"}"); err != nil {
			if ctx.Err() != nil {
				return "", "", nil, ctx.Err()
			}
			missing = errors.Join(errFinishLegacyObjectsMissing, missing, fmt.Errorf("legacy %s %s is unavailable: %w", o.Type, o.OID, err))
			continue
		}
		available = append(available, o)
		if o.Type == "blob" {
			entries = append(entries, gitpkg.MktreeEntry{Mode: "100644", Type: "blob", OID: o.OID, Path: o.OID})
		}
	}
	var treeInput bytes.Buffer
	for _, entry := range entries {
		fmt.Fprintf(&treeInput, "%s %s %s\t%s\x00", entry.Mode, entry.Type, entry.OID, entry.Path)
	}
	treeOutput, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo, Stdin: &treeInput}, "-c", "core.fsync=loose-object", "-c", "core.fsyncMethod=fsync", "mktree", "-z")
	if err != nil {
		return "", "", nil, err
	}
	tree := strings.TrimSpace(string(treeOutput))
	snapshotRef := prefix + "snapshot/" + tree
	oid, err := gitpkg.RevParse(ctx, repo, snapshotRef)
	if errors.Is(err, gitpkg.ErrRefNotFound) {
		oid, err = gitpkg.CommitTreeDurable(ctx, repo, tree, "Preserve legacy captured versions before explicit finish\n", "ACD Recovery", "recovery@localhost")
	}
	if err != nil {
		return "", "", nil, err
	}
	actualTree, err := gitpkg.RevParse(ctx, repo, oid+"^{tree}")
	if err != nil || actualTree != tree {
		return "", "", nil, fmt.Errorf("legacy snapshot tree does not match frozen inventory: %w", err)
	}
	if _, err = gitpkg.EnsurePrivateRefDurable(ctx, repo, "refs/acd/migration/", snapshotRef, oid); err != nil {
		return "", "", nil, err
	}
	if err = verifyFinishLegacyPin(ctx, repo, snapshotRef, oid); err != nil {
		return "", "", nil, err
	}
	var pins []state.FinishLegacyPin
	for _, o := range available {
		p := state.FinishLegacyPin{ObjectOID: o.OID, ObjectType: o.Type, PinRef: snapshotRef, PinOID: oid}
		if o.Type == "commit" {
			p.PinRef = prefix + "commits/" + o.OID
			p.PinOID = o.OID
			if _, err = gitpkg.EnsurePrivateRefDurable(ctx, repo, "refs/acd/migration/", p.PinRef, p.PinOID); err != nil {
				return "", "", nil, err
			}
			if err = verifyFinishLegacyPin(ctx, repo, p.PinRef, p.PinOID); err != nil {
				return "", "", nil, err
			}
		}
		pins = append(pins, p)
	}
	return snapshotRef, oid, pins, missing
}

func directFinishLegacyRef(ctx context.Context, repo, ref string) error {
	_, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo, Timeout: gitpkg.DefaultReadTimeout}, "symbolic-ref", "--no-recurse", "-q", ref)
	if err == nil {
		return fmt.Errorf("legacy ref %s is symbolic", ref)
	}
	var exit *gitpkg.Error
	if errors.As(err, &exit) && exit.ExitCode == 1 {
		return nil
	}
	return err
}
func verifyFinishLegacyPin(ctx context.Context, repo, ref, oid string) error {
	if err := directFinishLegacyRef(ctx, repo, ref); err != nil {
		return err
	}
	actual, err := gitpkg.RevParse(ctx, repo, ref)
	if err != nil {
		return err
	}
	if actual != oid {
		return fmt.Errorf("legacy pin %s moved from %s to %s", ref, oid, actual)
	}
	return nil
}
