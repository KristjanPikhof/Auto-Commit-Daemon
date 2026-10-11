package checkpoint

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishRetentionBoundsOrdinaryMetadata(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	store := Store{DB: db}
	now := time.Now()
	blob, err := gitpkg.HashObjectStdinDurable(ctx, repo, []byte("protected ordinary bytes\n"))
	if err != nil {
		t.Fatal(err)
	}
	dates := make([]time.Time, 301)
	for i := range dates {
		dates[i] = now.Add(-60*24*time.Hour + time.Duration(i)*time.Second)
	}
	cps := seedPublishedRetentionCheckpoints(t, ctx, store, repo, WorktreeID(repo), blob, dates)
	// Production checkpoint-only protection creates no capture-event members.
	if _, err = db.SQL().ExecContext(ctx, "DELETE FROM checkpoint_events; DELETE FROM capture_events"); err != nil {
		t.Fatal(err)
	}
	for pass, want := range []int{100, 100, 1, 0} {
		summary, err := store.ApplyRetention(ctx, repo, WorktreeID(repo), now)
		if err != nil || summary.Pruned != want {
			t.Fatalf("pass %d: %+v %v", pass, summary, err)
		}
	}
	var rows, ops int
	if err = db.ReadSQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM checkpoints").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err = db.ReadSQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM operations").Scan(&ops); err != nil {
		t.Fatal(err)
	}
	if rows != 100 || ops != 100 {
		t.Fatalf("ordinary growth retained rows=%d operations=%d", rows, ops)
	}
	if _, err = gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "gc", "--prune=now"); err != nil {
		t.Fatal(err)
	}
	if oid, err := gitpkg.RevParse(ctx, repo, cps[300].Ref); err != nil || oid != cps[300].CommitOID {
		t.Fatal("latest complete checkpoint lost", err)
	}
}

func TestFinishRetentionSavedPinsAndQuietBudget(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	store := Store{DB: db}
	now := time.Now()
	cp, err := store.Create(ctx, Request{RepoRoot: repo, WorktreeID: WorktreeID(repo), Reason: state.CheckpointReasonManualBarrier, ObservationEpoch: 1, CoverageEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	// An orphan private chain left before its SQLite pointer is still protected.
	ref := "refs/acd/finish/v1/" + WorktreeID(repo) + "/orphan/chain"
	if _, err = gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "update-ref", ref, cp.Checkpoint.CommitOID); err != nil {
		t.Fatal(err)
	}
	calls := 0
	store.retentionInventory = func(_ context.Context, _ string, refs []string) (map[string]int64, error) {
		calls++
		found := false
		for _, r := range refs {
			if r == ref {
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("private chain missing from measurement: %v", refs)
		}
		return map[string]int64{"proof": DefaultContentBudget + 1}, nil
	}
	outcome, err := store.Maintain(ctx, repo, WorktreeID(repo), now, MaintenanceStatus{})
	if err != nil || !outcome.OverBudget || outcome.ProtectedBytes != DefaultContentBudget+1 {
		t.Fatalf("budget %+v %v", outcome, err)
	}
	before := calls
	for i := 0; i < 10; i++ {
		if _, err = store.Maintain(ctx, repo, WorktreeID(repo), now.Add(time.Minute), outcome); err != nil {
			t.Fatal(err)
		}
	}
	if calls != before {
		t.Fatal("quiet maintenance repeatedly measured unchanged evidence")
	}
	if _, err = gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "gc", "--prune=now"); err != nil {
		t.Fatal(err)
	}
	if oid, err := gitpkg.RevParse(ctx, repo, ref); err != nil || oid != cp.Checkpoint.CommitOID {
		t.Fatal("budget discarded proof")
	}
}

func TestFinishRetentionRechecksInterruptedPrune(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	store := Store{DB: db}
	now := time.Now()
	blob, err := gitpkg.HashObjectStdinDurable(ctx, repo, []byte("old\n"))
	if err != nil {
		t.Fatal(err)
	}
	seedPublishedRetentionCheckpoints(t, ctx, store, repo, WorktreeID(repo), blob, []time.Time{now.Add(-60 * 24 * time.Hour), now})
	items, err := state.RetentionCheckpoints(ctx, db, WorktreeID(repo))
	if err != nil {
		t.Fatal(err)
	}
	old := items[1]
	if _, err = state.PrepareCheckpointPrune(ctx, db, old, "sha256:"+strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	// Cutover can subsequently seal the checkpoint as immutable old evidence.
	if _, err = db.SQL().ExecContext(ctx, "INSERT INTO finish_legacy_object_pins(object_oid,object_type,pin_ref,pin_oid) VALUES(?,'commit',?,?)", old.CommitOID, old.Ref, old.CommitOID); err != nil {
		t.Fatal(err)
	}
	if err = store.RecoverRetention(ctx, repo); err == nil {
		t.Fatal("interrupted prune ignored new legacy evidence owner")
	}
	if oid, err := gitpkg.RevParse(ctx, repo, old.Ref); err != nil || oid != old.CommitOID {
		t.Fatal("legacy evidence ref deleted")
	}
}

func TestFinishRetentionMissingLegacyInventoryBlocksInterruptedGC(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	store := Store{DB: db}
	now := time.Now()
	blob, err := gitpkg.HashObjectStdinDurable(ctx, repo, []byte("old captured-only version\n"))
	if err != nil {
		t.Fatal(err)
	}
	dates := make([]time.Time, DefaultMinimumRetained+1)
	for i := range dates {
		dates[i] = now.Add(-60*24*time.Hour + time.Duration(i)*time.Second)
	}
	cps := seedRetentionCheckpoints(t, ctx, store, repo, WorktreeID(repo), blob, dates, false)
	items, err := state.RetentionCheckpoints(ctx, db, WorktreeID(repo))
	if err != nil {
		t.Fatal(err)
	}
	old := items[len(items)-1]
	if _, err = state.PrepareCheckpointPrune(ctx, db, old, "sha256:"+strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err = db.SQL().ExecContext(ctx, "UPDATE capture_ops SET after_oid=?,after_mode='100644' WHERE event_seq=?", blob, cps[0].EventSeqs[0]); err != nil {
		t.Fatal(err)
	}
	outcome, err := store.Maintain(ctx, repo, WorktreeID(repo), now, MaintenanceStatus{})
	if err != nil || outcome.State != "needs_action" {
		t.Fatalf("missing inventory %+v %v", outcome, err)
	}
	if oid, err := gitpkg.RevParse(ctx, repo, old.Ref); err != nil || oid != cps[0].CommitOID {
		t.Fatal("recovery pruned before inventory proof")
	}
	// Protection remains independent from maintenance attention.
	if _, err = store.Create(ctx, Request{RepoRoot: repo, WorktreeID: WorktreeID(repo), Reason: state.CheckpointReasonPoll, ObservationEpoch: 102, CoverageEpoch: 102}); err != nil {
		t.Fatal("maintenance stopped checkpoint protection", err)
	}
}
