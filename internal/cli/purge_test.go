package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// seedPurgeFixtureRows inserts a known mix of capture_events rows and a
// blocked_conflict publish_state row so the purge tests have something
// real to delete. Returns the *state.DB for additional assertions.
func seedPurgeFixtureRows(t *testing.T, db *state.DB) {
	t.Helper()
	ctx := context.Background()
	mk := func(op, path, st string) {
		ev := state.CaptureEvent{
			BranchRef:        "refs/heads/main",
			BranchGeneration: 1,
			BaseHead:         "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
			Operation:        op,
			Path:             path,
			Fidelity:         "exact",
			State:            st,
		}
		if _, err := state.AppendCaptureEvent(ctx, db, ev, nil); err != nil {
			t.Fatalf("AppendCaptureEvent(%s,%s,%s): %v", op, path, st, err)
		}
	}
	// 1 published, 2 pending, 1 blocked_conflict, 1 failed.
	mk("create", "kept.txt", state.EventStatePublished)
	mk("modify", "p1.txt", state.EventStatePending)
	mk("modify", "p2.txt", state.EventStatePending)
	mk("modify", "blocked.txt", state.EventStateBlockedConflict)
	mk("delete", "broken.txt", state.EventStateFailed)

	// Mirror what the daemon would set when it terminally settles a
	// blocked event: publish_state singleton + breadcrumb meta keys.
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO publish_state(id, event_seq, branch_ref, branch_generation, source_head, status, error, updated_ts)
VALUES (1, 4, 'refs/heads/main', 1, 'deadbeef', 'blocked_conflict', 'modify before-state mismatch', 1.0)
ON CONFLICT(id) DO UPDATE SET status=excluded.status, error=excluded.error`); err != nil {
		t.Fatalf("seed publish_state: %v", err)
	}
	if err := state.MetaSet(ctx, db, "last_replay_conflict",
		`{"seq":4,"error_class":"before_state_mismatch","message":"x"}`); err != nil {
		t.Fatalf("seed meta: %v", err)
	}
}

func countCaptureRowsByState(t *testing.T, db *state.DB) map[string]int {
	t.Helper()
	rows, err := db.SQL().QueryContext(context.Background(),
		`SELECT state, COUNT(*) FROM capture_events GROUP BY state`)
	if err != nil {
		t.Fatalf("count rows: %v", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[s] = n
	}
	return out
}

// TestPurgeEvents_RequiresAtLeastOneState ensures the CLI rejects a
// flag-less invocation. Without this guard a user typing `acd purge-events`
// could expect the command to "do nothing" but instead would still
// execute (deleting zero rows is harmless, but accepting the call masks
// the operator's likely intent).
func TestPurgeEvents_RequiresAtLeastOneState(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	var out bytes.Buffer
	err := runPurgeEvents(context.Background(), &out, repo,
		false, false, false, false, false, true, true)
	if err == nil {
		t.Fatalf("expected error when no state flag is passed; got out=%s", out.String())
	}
	if !strings.Contains(err.Error(), "at least one") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestPurgeEvents_RequiresYesWhenNotDryRun guards against silent
// destructive runs. --yes must be explicit.
func TestPurgeEvents_RequiresYesWhenNotDryRun(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	var out bytes.Buffer
	// blocked=true, dryRun=false, yes=false ⇒ refused.
	err := runPurgeEvents(context.Background(), &out, repo,
		true, false, false, false, false, false, true)
	if err == nil {
		t.Fatalf("expected error without --yes; got out=%s", out.String())
	}
	if !strings.Contains(err.Error(), "--yes") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestPurgeEvents_DryRunCountsButDoesNotMutate validates the inspection
// path: counts surface, no rows disappear, no backup file created.
func TestPurgeEvents_DryRunCountsButDoesNotMutate(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	seedPurgeFixtureRows(t, db)
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}

	var out bytes.Buffer
	err = runPurgeEvents(context.Background(), &out, repo,
		true /*blocked*/, true /*pending*/, true /*failed*/, false, false /*yes*/, true /*dryRun*/, true)
	if err != nil {
		t.Fatalf("runPurgeEvents dry-run: %v\n%s", err, out.String())
	}

	var plan purgePlan
	if jerr := json.Unmarshal(out.Bytes(), &plan); jerr != nil {
		t.Fatalf("unmarshal: %v\n%s", jerr, out.String())
	}
	if !plan.DryRun {
		t.Fatalf("plan.DryRun=false; want true")
	}
	if plan.RowsDeleted != 0 {
		t.Fatalf("plan.RowsDeleted=%d; dry run must not delete", plan.RowsDeleted)
	}
	if got := plan.StateCounts[state.EventStatePending]; got != 2 {
		t.Fatalf("pending count = %d, want 2", got)
	}
	if got := plan.StateCounts[state.EventStateBlockedConflict]; got != 1 {
		t.Fatalf("blocked count = %d, want 1", got)
	}
	if got := plan.StateCounts[state.EventStateFailed]; got != 1 {
		t.Fatalf("failed count = %d, want 1", got)
	}
	if !plan.BarrierLift {
		t.Fatalf("BarrierLift=false; expected true since blocked count > 0")
	}
	if plan.BackupPath != "" {
		t.Fatalf("dry-run wrote backup %q; should be empty", plan.BackupPath)
	}

	after, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if before != after {
		t.Fatalf("dry-run mutated state.db: before=%s after=%s", before, after)
	}
}

// TestPurgeEvents_ApplyDeletesRowsAndLiftsBarrier is the happy-path
// regression. After --all --yes, blocked + pending + failed rows must
// be gone, the published row must remain, publish_state must transition
// out of blocked_conflict, and the breadcrumb meta key must be gone so
// `acd status` reads clean.
func TestPurgeEvents_ApplyDeletesRowsAndLiftsBarrier(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	seedPurgeFixtureRows(t, db)

	var out bytes.Buffer
	err := runPurgeEvents(context.Background(), &out, repo,
		false, false, false, true /*all*/, true /*yes*/, false /*dryRun*/, true)
	if err != nil {
		t.Fatalf("runPurgeEvents: %v\n%s", err, out.String())
	}

	var plan purgePlan
	if jerr := json.Unmarshal(out.Bytes(), &plan); jerr != nil {
		t.Fatalf("unmarshal: %v\n%s", jerr, out.String())
	}
	if plan.RowsDeleted != 4 {
		t.Fatalf("RowsDeleted=%d, want 4 (2 pending + 1 blocked + 1 failed)", plan.RowsDeleted)
	}
	if plan.BackupPath == "" {
		t.Fatalf("apply path must produce a backup file")
	}

	got := countCaptureRowsByState(t, db)
	if got[state.EventStatePublished] != 1 {
		t.Fatalf("published row deleted: counts=%v", got)
	}
	if got[state.EventStatePending] != 0 || got[state.EventStateBlockedConflict] != 0 || got[state.EventStateFailed] != 0 {
		t.Fatalf("non-published rows remain: counts=%v", got)
	}

	// publish_state lifted.
	var status string
	if err := db.SQL().QueryRowContext(context.Background(),
		`SELECT status FROM publish_state WHERE id=1`).Scan(&status); err != nil {
		t.Fatalf("read publish_state: %v", err)
	}
	if status == "blocked_conflict" {
		t.Fatalf("publish_state.status still blocked_conflict")
	}

	// Breadcrumb meta gone.
	if _, ok, err := state.MetaGet(context.Background(), db, "last_replay_conflict"); err != nil {
		t.Fatalf("MetaGet: %v", err)
	} else if ok {
		t.Fatalf("last_replay_conflict meta still present after purge")
	}
}

// TestPurgeEvents_OnlyPendingLeavesBlockedAlone verifies the state
// selector is precise. Asking only for --pending must NOT touch the
// blocked row (operators sometimes want to keep the diagnostic and
// just clear the tail).
func TestPurgeEvents_OnlyPendingLeavesBlockedAlone(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	seedPurgeFixtureRows(t, db)

	var out bytes.Buffer
	err := runPurgeEvents(context.Background(), &out, repo,
		false /*blocked*/, true /*pending*/, false /*failed*/, false, true, false, true)
	if err != nil {
		t.Fatalf("runPurgeEvents: %v\n%s", err, out.String())
	}

	got := countCaptureRowsByState(t, db)
	if got[state.EventStatePending] != 0 {
		t.Fatalf("pending rows remain: %v", got)
	}
	if got[state.EventStateBlockedConflict] != 1 {
		t.Fatalf("blocked row removed unexpectedly: %v", got)
	}
	if got[state.EventStateFailed] != 1 {
		t.Fatalf("failed row removed unexpectedly: %v", got)
	}

	// Barrier NOT lifted because we did not include blocked.
	var status string
	if err := db.SQL().QueryRowContext(context.Background(),
		`SELECT status FROM publish_state WHERE id=1`).Scan(&status); err != nil {
		t.Fatalf("read publish_state: %v", err)
	}
	if status != "blocked_conflict" {
		t.Fatalf("publish_state.status=%q, want blocked_conflict (untouched)", status)
	}
}

// TestSelectPurgeStates_AllExpandsAndDedupes pins the flag-resolution
// helper directly so a future flag refactor doesn't quietly drop a
// state from the --all set or break alphabetical determinism (the SQL
// IN(...) generation depends on it).
func TestSelectPurgeStates_AllExpandsAndDedupes(t *testing.T) {
	got := selectPurgeStates(false, false, false, true)
	want := []string{state.EventStateBlockedConflict, state.EventStateFailed, state.EventStatePending}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("--all → %v, want %v", got, want)
	}
	// Redundant flags + --all must not duplicate.
	got = selectPurgeStates(true, true, true, true)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("redundant flags + --all → %v, want %v", got, want)
	}
}
