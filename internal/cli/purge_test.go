package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestPurgeEvents_RequiresSafeSelector(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	var out bytes.Buffer
	err := runPurgeEvents(context.Background(), &out, repo, false, false, false, false, false, true, true)
	if err == nil || !strings.Contains(err.Error(), "pass --all") {
		t.Fatalf("runPurgeEvents err=%v want safe-selector refusal", err)
	}
}

func TestPurgeEvents_RequiresYesWhenApplying(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	var out bytes.Buffer
	err := runPurgeEvents(context.Background(), &out, repo, false, false, false, true, false, false, true)
	if err == nil || !strings.Contains(err.Error(), "without --yes") {
		t.Fatalf("runPurgeEvents err=%v want --yes refusal", err)
	}
}

func TestPurgeEvents_RefusesAllSelectiveRecovery(t *testing.T) {
	repo, stateDB, _ := makeRegisteredGitRepoStateDB(t)
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}
	for _, tc := range []struct {
		blocked bool
		pending bool
		failed  bool
	}{
		{blocked: true},
		{pending: true},
		{failed: true},
	} {
		var out bytes.Buffer
		err := runPurgeEvents(context.Background(), &out, repo, tc.blocked, tc.pending, tc.failed, false, true, false, true)
		if err == nil || !strings.Contains(err.Error(), "selective --blocked/--pending/--failed recovery is no longer supported") {
			t.Fatalf("runPurgeEvents blocked=%v pending=%v failed=%v err=%v", tc.blocked, tc.pending, tc.failed, err)
		}
	}
	after, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if before != after {
		t.Fatalf("refused selective purge mutated state.db: before=%s after=%s", before, after)
	}
}

func TestPurgeEvents_DryRunDelegatesArchiveOnlyFix(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	stageRecoverableBarrierPair(t, context.Background(), repo, db, "refs/heads/main", 1)
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}

	var out bytes.Buffer
	if err := runPurgeEvents(context.Background(), &out, repo, false, false, false, true, false, true, true); err != nil {
		t.Fatalf("runPurgeEvents dry-run: %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal delegated fix plan: %v\n%s", err, out.String())
	}
	action := findFixAction(plan, fixActionReconcileUnpublishedChain)
	if !plan.DryRun || action == nil || !action.ArchiveOnly || !action.RequiresForce {
		t.Fatalf("purge alias did not plan archive-only recovery: %+v", plan)
	}
	after, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if before != after {
		t.Fatalf("purge dry-run mutated state.db: before=%s after=%s", before, after)
	}
}

func TestPurgeEvents_AllArchivesWholePairWithoutDeletingRows(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)

	var out bytes.Buffer
	if err := runPurgeEvents(ctx, &out, repo, false, false, false, true, true, false, true); err != nil {
		t.Fatalf("runPurgeEvents apply: %v\n%s", err, out.String())
	}
	assertFixEventState(t, ctx, db, first, state.EventStateRecovered)
	assertFixEventState(t, ctx, db, second, state.EventStateRecovered)
	if got := countRowsWhere(t, db, "capture_events", "seq IN (?, ?)", first, second); got != 2 {
		t.Fatalf("purge alias deleted protected capture rows: %d", got)
	}
	var snapshots int
	if err := db.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM recovery_snapshots WHERE event_count = 2`).Scan(&snapshots); err != nil {
		t.Fatalf("count snapshots: %v", err)
	}
	if snapshots != 1 {
		t.Fatalf("whole-pair recovery snapshots=%d want 1", snapshots)
	}
}

// seedPurgeFixtureRows keeps the compact read-only fixtures used by fix and
// diagnose tests. Apply tests use complete capture ops instead.
func seedPurgeFixtureRows(t *testing.T, db *state.DB) {
	t.Helper()
	ctx := context.Background()
	for _, row := range []struct {
		state string
		path  string
	}{
		{state.EventStateBlockedConflict, "blocked.txt"},
		{state.EventStateFailed, "failed.txt"},
		{state.EventStatePending, "pending-a.txt"},
		{state.EventStatePending, "pending-b.txt"},
	} {
		if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: "fixture-head", Operation: "modify", Path: row.path,
			Fidelity: "exact", State: row.state,
		}, nil); err != nil {
			t.Fatalf("AppendCaptureEvent(%s): %v", row.path, err)
		}
	}
}
