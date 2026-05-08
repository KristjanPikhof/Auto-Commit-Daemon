package state

import (
	"context"
	"database/sql"
	"testing"
)

// TestPurgeUnpublishedForDeadBranch pins the behaviour of the helper that
// cleans up both pending and terminal capture_events rows for a (branch_ref,
// branch_generation) pair after the branch has been merged and deleted.
//
// Invariants:
//   - pending + blocked_conflict + failed rows for the exact (branchRef,
//     generation) pair are removed.
//   - published rows for that same pair are untouched.
//   - All rows for a different branch_ref are untouched.
//   - All rows for a different branch_generation are untouched.
//   - An empty branchRef returns an error and deletes nothing.
//   - When publish_state.status='blocked_conflict' AT id=1, the helper lifts
//     the singleton barrier (status->'ok', error->NULL) and clears the
//     last_replay_conflict / last_replay_conflict_legacy / last_replay_error
//     breadcrumbs in the same transaction.
//   - When publish_state status is NOT 'blocked_conflict', the breadcrumbs
//     are left intact (they may belong to a different live barrier).
func TestPurgeUnpublishedForDeadBranch(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	appendEvent := func(branch string, generation int64, stateName, path string) int64 {
		t.Helper()
		seq, err := AppendCaptureEvent(ctx, d, CaptureEvent{
			BranchRef:        branch,
			BranchGeneration: generation,
			BaseHead:         "deadbeef",
			Operation:        "modify",
			Path:             path,
			Fidelity:         "exact",
			State:            stateName,
		}, []CaptureOp{{Op: "modify", Path: path, Fidelity: "exact"}})
		if err != nil {
			t.Fatalf("append %s: %v", path, err)
		}
		return seq
	}

	// Target (refs/heads/dead, gen 3) — pending + terminal must be deleted.
	targetBlocked := appendEvent("refs/heads/dead", 3, EventStateBlockedConflict, "dead-blocked.txt")
	targetFailed := appendEvent("refs/heads/dead", 3, EventStateFailed, "dead-failed.txt")
	targetPending := appendEvent("refs/heads/dead", 3, EventStatePending, "dead-pending.txt")

	// Same (branch, gen) but published — must survive.
	keepPublished := appendEvent("refs/heads/dead", 3, EventStatePublished, "dead-published.txt")

	// Different branch_ref (same generation number) — must survive.
	otherBranchBlocked := appendEvent("refs/heads/other", 3, EventStateBlockedConflict, "other-blocked.txt")
	otherBranchFailed := appendEvent("refs/heads/other", 3, EventStateFailed, "other-failed.txt")
	otherBranchPending := appendEvent("refs/heads/other", 3, EventStatePending, "other-pending.txt")

	// Same branch but different generation — must survive.
	otherGenBlocked := appendEvent("refs/heads/dead", 4, EventStateBlockedConflict, "dead-gen4-blocked.txt")
	otherGenFailed := appendEvent("refs/heads/dead", 4, EventStateFailed, "dead-gen4-failed.txt")
	otherGenPending := appendEvent("refs/heads/dead", 4, EventStatePending, "dead-gen4-pending.txt")

	// Stamp publish_state.status='blocked_conflict' so the helper lifts the
	// singleton barrier in the same tx. Also seed the breadcrumb meta keys.
	if err := MarkEventBlocked(ctx, d, targetBlocked, "seeded blocker", nowSeconds(),
		sql.NullString{String: "refs/heads/dead", Valid: true},
		sql.NullInt64{Int64: 3, Valid: true},
		sql.NullString{String: "deadbeef", Valid: true}); err != nil {
		t.Fatalf("MarkEventBlocked: %v", err)
	}
	for _, key := range []string{
		"last_replay_conflict", "last_replay_conflict_legacy", "last_replay_error",
	} {
		if err := MetaSet(ctx, d, key, "seeded"); err != nil {
			t.Fatalf("seed meta %q: %v", key, err)
		}
	}

	n, err := PurgeUnpublishedForDeadBranch(ctx, d, "refs/heads/dead", 3)
	if err != nil {
		t.Fatalf("PurgeUnpublishedForDeadBranch: %v", err)
	}
	// pending + blocked_conflict + failed = 3 rows.
	if n != 3 {
		t.Fatalf("deleted=%d want 3 (pending + blocked_conflict + failed for target pair)", n)
	}

	// Collect surviving seq values.
	rows, err := d.SQL().QueryContext(ctx, `SELECT seq FROM capture_events ORDER BY seq ASC`)
	if err != nil {
		t.Fatalf("query remaining: %v", err)
	}
	defer rows.Close()
	remaining := map[int64]bool{}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatalf("scan: %v", err)
		}
		remaining[seq] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iter: %v", err)
	}

	// Target pending + terminal rows must be gone.
	for _, seq := range []int64{targetBlocked, targetFailed, targetPending} {
		if remaining[seq] {
			t.Fatalf("target row seq %d survived; remaining=%v", seq, remaining)
		}
	}
	// Everything else must still be present.
	for _, seq := range []int64{
		keepPublished,
		otherBranchBlocked, otherBranchFailed, otherBranchPending,
		otherGenBlocked, otherGenFailed, otherGenPending,
	} {
		if !remaining[seq] {
			t.Fatalf("seq %d wrongly deleted; remaining=%v", seq, remaining)
		}
	}

	// publish_state.status must have been lifted to 'ok'.
	var status string
	var errMsg sql.NullString
	if err := d.SQL().QueryRowContext(ctx,
		`SELECT status, error FROM publish_state WHERE id = 1`,
	).Scan(&status, &errMsg); err != nil {
		t.Fatalf("read publish_state: %v", err)
	}
	if status != "ok" {
		t.Fatalf("publish_state.status=%q want 'ok' (barrier should be lifted)", status)
	}
	if errMsg.Valid {
		t.Fatalf("publish_state.error=%q want NULL (barrier should be lifted)", errMsg.String)
	}

	// Breadcrumb meta keys must be gone (we lifted the publish_state singleton).
	for _, key := range []string{
		"last_replay_conflict", "last_replay_conflict_legacy", "last_replay_error",
	} {
		if _, ok, err := MetaGet(ctx, d, key); err != nil {
			t.Fatalf("MetaGet %q: %v", key, err)
		} else if ok {
			t.Fatalf("breadcrumb %q still present after purge; expected cleared", key)
		}
	}

	// Idempotent: a second call for the same pair deletes zero rows.
	n2, err := PurgeUnpublishedForDeadBranch(ctx, d, "refs/heads/dead", 3)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if n2 != 0 {
		t.Fatalf("second call deleted=%d want 0 (idempotent)", n2)
	}

	// Empty branchRef must return an error and must not delete anything.
	t.Run("empty_branch_ref", func(t *testing.T) {
		d2, _ := openTestDB(t)

		seq2, err := AppendCaptureEvent(ctx, d2, CaptureEvent{
			BranchRef:        "refs/heads/x",
			BranchGeneration: 1,
			BaseHead:         "abc",
			Operation:        "modify",
			Path:             "x.txt",
			Fidelity:         "exact",
			State:            EventStateBlockedConflict,
		}, []CaptureOp{{Op: "modify", Path: "x.txt", Fidelity: "exact"}})
		if err != nil {
			t.Fatalf("seed d2: %v", err)
		}

		if _, err := PurgeUnpublishedForDeadBranch(ctx, d2, "", 1); err == nil {
			t.Fatalf("empty branch_ref must error")
		}

		var count int
		if err := d2.SQL().QueryRowContext(ctx,
			`SELECT COUNT(*) FROM capture_events WHERE seq = ?`, seq2,
		).Scan(&count); err != nil {
			t.Fatalf("count d2 row: %v", err)
		}
		if count != 1 {
			t.Fatalf("d2 row deleted despite empty-branchRef error; count=%d want 1", count)
		}
	})

	// Breadcrumbs must NOT be cleared when publish_state status is not blocked.
	t.Run("breadcrumbs_preserved_when_publish_state_clean", func(t *testing.T) {
		d3, _ := openTestDB(t)
		seqA, err := AppendCaptureEvent(ctx, d3, CaptureEvent{
			BranchRef:        "refs/heads/dead2",
			BranchGeneration: 1,
			BaseHead:         "abc",
			Operation:        "modify",
			Path:             "p.txt",
			Fidelity:         "exact",
			State:            EventStateBlockedConflict,
		}, []CaptureOp{{Op: "modify", Path: "p.txt", Fidelity: "exact"}})
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		_ = seqA
		// Seed a "live" publish_state with status != blocked_conflict, plus
		// breadcrumbs that belong to that live barrier.
		if err := MetaSet(ctx, d3, "last_replay_conflict", "live"); err != nil {
			t.Fatalf("seed live breadcrumb: %v", err)
		}

		if _, err := PurgeUnpublishedForDeadBranch(ctx, d3, "refs/heads/dead2", 1); err != nil {
			t.Fatalf("purge: %v", err)
		}

		v, ok, err := MetaGet(ctx, d3, "last_replay_conflict")
		if err != nil {
			t.Fatalf("MetaGet: %v", err)
		}
		if !ok || v != "live" {
			t.Fatalf("breadcrumb was cleared but publish_state was not in blocked_conflict; got ok=%v v=%q", ok, v)
		}
	})
}
