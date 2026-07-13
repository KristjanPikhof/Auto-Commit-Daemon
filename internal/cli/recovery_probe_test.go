package cli

import (
	"context"
	"database/sql"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRecoveryBlockerCountsDistinguishActiveAndNonActiveRows(t *testing.T) {
	ctx := context.Background()
	_, _, d := makeRepoStateDB(t)

	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID:              1,
		Mode:             "running",
		HeartbeatTS:      nowFloat(),
		BranchRef:        sqlNullStr("refs/heads/main"),
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}

	appendBlocked := func(branch string, gen int64, path string) {
		t.Helper()
		seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
			BranchRef: branch, BranchGeneration: gen,
			BaseHead: "deadbeef", Operation: "modify", Path: path, Fidelity: "exact",
		}, []state.CaptureOp{{Op: "modify", Path: path, Fidelity: "exact"}})
		if err != nil {
			t.Fatalf("append blocked %s: %v", path, err)
		}
		if err := state.MarkEventBlocked(ctx, d, seq, "before-state mismatch", nowFloat(),
			sqlNullStr(branch), sql.NullInt64{Int64: gen, Valid: true}, sqlNullStr("deadbeef")); err != nil {
			t.Fatalf("mark blocked %s: %v", path, err)
		}
	}
	appendPending := func(branch string, gen int64, path string) {
		t.Helper()
		if _, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
			BranchRef: branch, BranchGeneration: gen,
			BaseHead: "deadbeef", Operation: "modify", Path: path, Fidelity: "exact",
		}, []state.CaptureOp{{Op: "modify", Path: path, Fidelity: "exact"}}); err != nil {
			t.Fatalf("append pending %s: %v", path, err)
		}
	}
	appendFailed := func(branch string, gen int64, path string) {
		t.Helper()
		seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
			BranchRef: branch, BranchGeneration: gen,
			BaseHead: "deadbeef", Operation: "modify", Path: path, Fidelity: "exact",
		}, []state.CaptureOp{{Op: "modify", Path: path, Fidelity: "exact"}})
		if err != nil {
			t.Fatalf("append failed %s: %v", path, err)
		}
		if err := state.MarkEventPublished(ctx, d, seq, state.EventStateFailed,
			sql.NullString{}, sqlNullStr("commit failed"), sql.NullString{}, nowFloat()); err != nil {
			t.Fatalf("mark failed %s: %v", path, err)
		}
	}

	appendBlocked("refs/heads/main", 1, "active-blocked.go")
	appendPending("refs/heads/main", 1, "active-hidden.go")
	appendBlocked("refs/heads/topic", 7, "other-blocked.go")
	appendPending("refs/heads/topic", 7, "other-hidden.go")
	appendFailed("refs/heads/main", 2, "failed.go")
	appendPending("refs/heads/main", 2, "failed-hidden.go")
	appendPending("refs/heads/pending-only", 1, "visible.go")
	appendFailed("refs/heads/main", 1, "active-tail-failed.go")

	counts, err := loadRecoveryBlockerCounts(ctx, d.SQL(), "refs/heads/main", 1)
	if err != nil {
		t.Fatalf("load counts: %v", err)
	}
	if counts.TotalBlockedConflicts != 2 {
		t.Fatalf("TotalBlockedConflicts=%d want 2", counts.TotalBlockedConflicts)
	}
	if counts.ActiveBlockedBarriersWithSuccessors != 1 {
		t.Fatalf("ActiveBlockedBarriersWithSuccessors=%d want 1", counts.ActiveBlockedBarriersWithSuccessors)
	}
	if counts.ActiveTerminalEvents != 2 {
		t.Fatalf("ActiveTerminalEvents=%d want 2", counts.ActiveTerminalEvents)
	}
	if counts.FailedBarriersWithSuccessors != 1 {
		t.Fatalf("FailedBarriersWithSuccessors=%d want 1", counts.FailedBarriersWithSuccessors)
	}
	if counts.PendingOnlyIntentDepth != 1 {
		t.Fatalf("PendingOnlyIntentDepth=%d want 1", counts.PendingOnlyIntentDepth)
	}
}

func TestRecoveryBlockerCountsPendingOnlyRepo(t *testing.T) {
	ctx := context.Background()
	_, _, d := makeRepoStateDB(t)
	for _, path := range []string{"one.go", "two.go"} {
		if _, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: "deadbeef", Operation: "modify", Path: path, Fidelity: "exact",
		}, []state.CaptureOp{{Op: "modify", Path: path, Fidelity: "exact"}}); err != nil {
			t.Fatalf("append pending %s: %v", path, err)
		}
	}

	counts, err := loadRecoveryBlockerCounts(ctx, d.SQL(), "refs/heads/main", 1)
	if err != nil {
		t.Fatalf("load counts: %v", err)
	}
	if counts.TotalBlockedConflicts != 0 || counts.ActiveBlockedBarriersWithSuccessors != 0 || counts.ActiveTerminalEvents != 0 || counts.FailedBarriersWithSuccessors != 0 {
		t.Fatalf("blocker counts = %+v, want no blockers", counts)
	}
	if counts.PendingOnlyIntentDepth != 2 {
		t.Fatalf("PendingOnlyIntentDepth=%d want 2", counts.PendingOnlyIntentDepth)
	}
}
