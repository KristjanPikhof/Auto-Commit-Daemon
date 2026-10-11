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
			if ctx.Err() != nil {
				return saved, ctx.Err()
			}
			ref, oid, _, pinErr := pinFinishLegacy(ctx, wt.Root, checkpoint.WorktreeID(wt.Root), manifest)
			failed, writeErr := state.RecordFinishLegacyActivationFailure(ctx, db, "legacy_inventory_invalid", "Legacy selections could not be proved. Available versions are retained; repair the saved selection before finish: "+err.Error(), ref, oid)
			return failed, errors.Join(ErrFinishLegacyAttention, err, pinErr, writeErr)
		}
		ref, oid, pins, err := pinFinishLegacy(ctx, wt.Root, checkpoint.WorktreeID(wt.Root), manifest)
		if err != nil {
			if ctx.Err() != nil {
				return saved, ctx.Err()
			}
			code := "legacy_proof_ambiguous"
			if errors.Is(err, state.ErrFinishIdentity) {
				code = "legacy_inventory_invalid"
			} else if errors.Is(err, errFinishLegacyObjectsMissing) {
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
	complete := func() (state.FinishLegacyCutover, error) {
		var completed state.FinishLegacyCutover
		err := gitpkg.WithLockedPrivateRef(ctx, wt.Root, saved.SnapshotRef, saved.SnapshotOID, func(lockedCtx context.Context) error {
			var err error
			completed, err = state.CompleteFinishLegacyCutover(lockedCtx, db, "", "")
			return err
		})
		if err != nil {
			return attention("legacy_pin_moved", err)
		}
		return completed, nil
	}
	if saved.WorktreeID != checkpoint.WorktreeID(wt.Root) {
		return attention("legacy_worktree_mismatch", errors.New("canonical worktree identity changed"))
	}
	pins, err := state.FinishLegacyObjectPins(ctx, db)
	if err != nil {
		return saved, err
	}
	refs := []gitpkg.RefExpectation{{Ref: saved.SnapshotRef, OID: saved.SnapshotOID}}
	for _, p := range pins {
		refs = append(refs, gitpkg.RefExpectation{Ref: p.PinRef, OID: p.PinOID})
	}
	if err = gitpkg.VerifyPrivateRefs(ctx, wt.Root, refs); err != nil {
		return attention("legacy_pin_moved", err)
	}
	var blobOIDs []string
	for _, p := range pins {
		if p.ObjectType == "blob" {
			blobOIDs = append(blobOIDs, p.ObjectOID)
		}
	}
	objectTypes, err := gitpkg.ObjectTypes(ctx, wt.Root, blobOIDs)
	if err != nil {
		return attention("legacy_object_missing", err)
	}
	// Blob pins share one flat snapshot tree; read it once rather than
	// starting a Git process for every archived path version.
	entries, err := gitpkg.LsTree(ctx, wt.Root, saved.SnapshotOID, false)
	if err != nil {
		return attention("legacy_object_missing", err)
	}
	blobs := make(map[string]string, len(entries))
	for _, entry := range entries {
		if entry.Type == "blob" {
			blobs[entry.Path] = entry.OID
		}
	}
	for _, p := range pins {
		if p.ObjectType == "blob" && (objectTypes[p.ObjectOID] != "blob" || p.PinOID != saved.SnapshotOID || blobs[p.ObjectOID] != p.ObjectOID) {
			return attention("legacy_object_missing", fmt.Errorf("captured object %s is no longer proved", p.ObjectOID))
		}
	}
	if saved.State == "completed" {
		return saved, nil
	}
	hasJournal, err := state.LegacyTableExists(ctx, db.ReadSQL(), "self_publications")
	if err != nil {
		return saved, err
	}
	if hasJournal {
		if err = daemon.RecoverSelfPublicationsBeforePlanning(ctx, wt.Root, db); err != nil {
			return attention("legacy_publication_ambiguous", err)
		}
	}
	// Old repair engines rewrite capture OIDs. Preserve that provenance instead.
	// A prepared row with no replacement and the exact old branch is inert;
	// all other interrupted repairs retain their backup and require review.
	hasRepairs, err := state.LegacyTableExists(ctx, db.ReadSQL(), "intent_repairs")
	if err != nil {
		return saved, err
	}
	if !hasRepairs {
		return complete()
	}
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
	return complete()
}

func pinFinishLegacy(ctx context.Context, repo, worktree string, m state.FinishLegacyManifest) (string, string, []state.FinishLegacyPin, error) {
	digest := strings.TrimPrefix(state.FinishLegacyDigest(m), "sha256:")
	worktreePrefix := "refs/acd/migration/finish-v1/" + worktree + "/"
	prefix := worktreePrefix + digest + "/"
	var treeInput bytes.Buffer
	var available []state.FinishLegacyObject
	var inventoryErr error
	var valid []state.FinishLegacyObject
	var oids []string
	for _, o := range m.Objects {
		_, decodeErr := hex.DecodeString(o.OID)
		if decodeErr != nil || strings.ToLower(o.OID) != o.OID || (len(o.OID) != 40 && len(o.OID) != 64) || (o.Type != "blob" && o.Type != "commit") {
			inventoryErr = errors.Join(inventoryErr, state.ErrFinishIdentity)
			continue
		}
		valid = append(valid, o)
		oids = append(oids, o.OID)
	}
	types, err := gitpkg.ObjectTypes(ctx, repo, oids)
	if err != nil {
		return "", "", nil, err
	}
	for _, o := range valid {
		if types[o.OID] != o.Type {
			inventoryErr = errors.Join(inventoryErr, errFinishLegacyObjectsMissing, fmt.Errorf("legacy %s %s is unavailable (observed %s)", o.Type, o.OID, types[o.OID]))
			continue
		}
		available = append(available, o)
		if o.Type == "blob" {
			fmt.Fprintf(&treeInput, "100644 blob %s\t%s\x00", o.OID, o.OID)
		}
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
	if err != nil {
		return "", "", nil, err
	}
	if actualTree != tree {
		return "", "", nil, errors.New("legacy snapshot tree does not match frozen inventory")
	}
	refs := []gitpkg.RefExpectation{{Ref: snapshotRef, OID: oid}}
	var pins []state.FinishLegacyPin
	for _, o := range available {
		p := state.FinishLegacyPin{ObjectOID: o.OID, ObjectType: o.Type, PinRef: snapshotRef, PinOID: oid}
		if o.Type == "commit" {
			// Commit content never changes across manifests. New checkpoints
			// must not recreate every retained legacy commit pin.
			p.PinRef = worktreePrefix + "commits/" + o.OID
			p.PinOID = o.OID
			refs = append(refs, gitpkg.RefExpectation{Ref: p.PinRef, OID: p.PinOID})
		}
		pins = append(pins, p)
	}
	if err = gitpkg.EnsurePrivateRefsDurable(ctx, repo, "refs/acd/migration/", refs); err != nil {
		return "", "", nil, errors.Join(inventoryErr, err)
	}
	return snapshotRef, oid, pins, inventoryErr
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
