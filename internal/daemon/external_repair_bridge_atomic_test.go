package daemon

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestExternalRepairBridgeRejectsPostProofCaptureAtomically(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	keys := seedExternalBridgeAtomicBaseline(t, ctx, f)
	before := readExternalBridgeAtomicState(t, ctx, f, keys)

	laterBlob := externalBridgeTestBlob(t, ctx, f.capture, "captured during reconciliation\n")
	var laterSeq int64
	opts := externalBridgeTestRecoveryOptions(f)
	opts.beforeStateTransition = func() {
		laterSeq = appendRecoveryEvent(t, ctx, f.capture, f.live, state.CaptureOp{
			Op: "create", Path: "captured-during-reconciliation.txt",
			AfterOID:  oidValue(laterBlob),
			AfterMode: oidValue(git.RegularFileMode),
		})
	}

	result, err := ReconcileUnpublishedChain(
		ctx, f.capture.dir, f.capture.db, opts)
	if !errors.Is(err, state.ErrRecoveryChainChanged) {
		t.Fatalf("ReconcileUnpublishedChain result=%+v err=%v want ErrRecoveryChainChanged",
			result, err)
	}
	if laterSeq == 0 {
		t.Fatal("post-proof capture hook did not run")
	}
	allPending := append([]int64(nil), f.pendingSeqs...)
	allPending = append(allPending, laterSeq)
	assertExternalBridgeEventsPending(t, ctx, f.capture.db,
		allPending)
	after := readExternalBridgeAtomicState(t, ctx, f, keys)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected post-proof capture changed atomic state:\nbefore=%+v\nafter=%+v",
			before, after)
	}
}

func TestExternalRepairBridgeRejectsBranchMoveBeforeCombinedLock(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	keys := seedExternalBridgeAtomicBaseline(t, ctx, f)
	before := readExternalBridgeAtomicState(t, ctx, f, keys)

	moved := commitTreeWithIndexUpdates(
		t, ctx, f.capture, f.live, "branch moved during reconciliation")
	ref := recoveryProofRefName(
		f.capture.cctx.BranchRef, f.capture.cctx.BranchGeneration,
		f.pendingSeqs[0], f.pendingSeqs[len(f.pendingSeqs)-1], f.target)
	var callbackRan bool
	opts := externalBridgeTestRecoveryOptions(f)
	opts.beforeStateTransition = func() {
		if protected, err := git.RevParse(ctx, f.capture.dir, ref); err != nil || protected != f.target {
			t.Fatalf("recovery ref before branch race=%q err=%v want %s",
				protected, err, f.target)
		}
		if err := git.UpdateRef(
			ctx, f.capture.dir, f.capture.cctx.BranchRef, moved, f.live); err != nil {
			t.Fatalf("move branch before combined lock: %v", err)
		}
	}
	opts.afterRecoveryRefLocked = func() { callbackRan = true }

	result, err := ReconcileUnpublishedChain(
		ctx, f.capture.dir, f.capture.db, opts)
	if err == nil {
		t.Fatalf("ReconcileUnpublishedChain result=%+v err=nil after branch move", result)
	}
	if callbackRan {
		t.Fatal("state transition callback ran after the literal branch ref moved")
	}
	if head, parseErr := git.RevParse(
		ctx, f.capture.dir, f.capture.cctx.BranchRef); parseErr != nil || head != moved {
		t.Fatalf("branch ref after race=%q err=%v want moved head %s",
			head, parseErr, moved)
	}
	assertExternalBridgeEventsPending(
		t, ctx, f.capture.db, f.pendingSeqs)
	after := readExternalBridgeAtomicState(t, ctx, f, keys)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected branch move changed atomic state:\nbefore=%+v\nafter=%+v",
			before, after)
	}
}

type externalBridgeAtomicMeta struct {
	Value string
	OK    bool
}

type externalBridgeAtomicState struct {
	ShadowRows      []state.ShadowPath
	Meta            map[string]externalBridgeAtomicMeta
	Snapshots       int
	SnapshotMembers int
}

func seedExternalBridgeAtomicBaseline(
	t *testing.T,
	ctx context.Context,
	f externalRepairBridgeFixture,
) []string {
	t.Helper()
	shadowBlob := externalBridgeTestBlob(t, ctx, f.capture, "old shadow evidence\n")
	if err := state.UpsertShadowPath(ctx, f.capture.db, state.ShadowPath{
		BranchRef:        f.capture.cctx.BranchRef,
		BranchGeneration: f.capture.cctx.BranchGeneration,
		Path:             "old-shadow-evidence.txt",
		Operation:        "create",
		Mode:             oidValue(git.RegularFileMode),
		OID:              oidValue(shadowBlob),
		BaseHead:         f.source,
		Fidelity:         "rescan",
		UpdatedTS:        11,
	}); err != nil {
		t.Fatalf("seed atomic bridge shadow: %v", err)
	}
	marker := ShadowBootstrappedKey(
		f.capture.cctx.BranchRef, f.capture.cctx.BranchGeneration)
	meta := map[string]string{
		marker:                                "old-marker",
		MetaKeyBranchGeneration:               "1",
		MetaKeyBranchHead:                     f.source,
		MetaKeyBranchToken:                    branchTokenRev(f.source, f.capture.cctx.BranchRef),
		MetaKeyBranchTokenChangedAt:           "7",
		MetaKeyBranchTransitionNeedsAttention: "old transition warning",
		"manual_pause.resumed_at":            "6",
	}
	keys := make([]string, 0, len(meta))
	for key, value := range meta {
		if err := state.MetaSet(ctx, f.capture.db, key, value); err != nil {
			t.Fatalf("seed atomic bridge meta %s: %v", key, err)
		}
		keys = append(keys, key)
	}
	return keys
}

func readExternalBridgeAtomicState(
	t *testing.T,
	ctx context.Context,
	f externalRepairBridgeFixture,
	metaKeys []string,
) externalBridgeAtomicState {
	t.Helper()
	result := externalBridgeAtomicState{
		Meta: make(map[string]externalBridgeAtomicMeta, len(metaKeys)),
	}
	rows, err := f.capture.db.ReadSQL().QueryContext(ctx, `
SELECT branch_ref,branch_generation,path,operation,mode,oid,old_path,
       base_head,fidelity,updated_ts
FROM shadow_paths
WHERE branch_ref=? AND branch_generation=?
ORDER BY path`, f.capture.cctx.BranchRef, f.capture.cctx.BranchGeneration)
	if err != nil {
		t.Fatalf("query atomic bridge shadow: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row state.ShadowPath
		if err := rows.Scan(
			&row.BranchRef, &row.BranchGeneration, &row.Path, &row.Operation,
			&row.Mode, &row.OID, &row.OldPath, &row.BaseHead, &row.Fidelity,
			&row.UpdatedTS); err != nil {
			t.Fatalf("scan atomic bridge shadow: %v", err)
		}
		result.ShadowRows = append(result.ShadowRows, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate atomic bridge shadow: %v", err)
	}
	for _, key := range metaKeys {
		value, ok, err := state.MetaGet(ctx, f.capture.db, key)
		if err != nil {
			t.Fatalf("read atomic bridge meta %s: %v", key, err)
		}
		result.Meta[key] = externalBridgeAtomicMeta{Value: value, OK: ok}
	}
	for query, target := range map[string]*int{
		`SELECT COUNT(*) FROM recovery_snapshots`:       &result.Snapshots,
		`SELECT COUNT(*) FROM recovery_snapshot_events`: &result.SnapshotMembers,
	} {
		if err := f.capture.db.ReadSQL().QueryRowContext(ctx, query).Scan(target); err != nil {
			t.Fatalf("count atomic bridge rows for %q: %v", query, err)
		}
	}
	return result
}

func assertExternalBridgeEventsPending(
	t *testing.T,
	ctx context.Context,
	db *state.DB,
	seqs []int64,
) {
	t.Helper()
	for _, seq := range seqs {
		eventState, commit := readEventState(t, ctx, db, seq)
		if eventState != state.EventStatePending || commit != (sql.NullString{}) {
			t.Fatalf("seq=%d lifecycle=(%q,%+v) want pending,NULL",
				seq, eventState, commit)
		}
	}
}
