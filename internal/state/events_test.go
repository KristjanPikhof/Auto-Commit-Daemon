package state

import (
	"context"
	"testing"
)

// TestDeleteTerminalForDeadBranch pins the behaviour of the helper that cleans
// up terminal capture_events rows for a (branch_ref, branch_generation) pair
// after the branch has been merged and deleted.
//
// Invariants:
//   - Only blocked_conflict + failed rows for the exact (branchRef, generation)
//     are removed.
//   - pending rows for that same pair are untouched.
//   - published rows for that same pair are untouched.
//   - All rows for a different branch_ref are untouched.
//   - All rows for a different branch_generation are untouched.
//   - An empty branchRef returns an error and deletes nothing.
func TestDeleteTerminalForDeadBranch(t *testing.T) {
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

	// Target (refs/heads/dead, gen 3) — terminal rows must be deleted.
	targetBlocked := appendEvent("refs/heads/dead", 3, EventStateBlockedConflict, "dead-blocked.txt")
	targetFailed := appendEvent("refs/heads/dead", 3, EventStateFailed, "dead-failed.txt")

	// Same (branch, gen) but pending or published — must survive.
	keepPending := appendEvent("refs/heads/dead", 3, EventStatePending, "dead-pending.txt")
	keepPublished := appendEvent("refs/heads/dead", 3, EventStatePublished, "dead-published.txt")

	// Different branch_ref (same generation number) — must survive.
	otherBranchBlocked := appendEvent("refs/heads/other", 3, EventStateBlockedConflict, "other-blocked.txt")
	otherBranchFailed := appendEvent("refs/heads/other", 3, EventStateFailed, "other-failed.txt")

	// Same branch but different generation — must survive.
	otherGenBlocked := appendEvent("refs/heads/dead", 4, EventStateBlockedConflict, "dead-gen4-blocked.txt")
	otherGenFailed := appendEvent("refs/heads/dead", 4, EventStateFailed, "dead-gen4-failed.txt")

	n, err := DeleteTerminalForDeadBranch(ctx, d, "refs/heads/dead", 3)
	if err != nil {
		t.Fatalf("DeleteTerminalForDeadBranch: %v", err)
	}
	if n != 2 {
		t.Fatalf("deleted=%d want 2 (blocked_conflict + failed for target pair)", n)
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

	// Target rows must be gone.
	for _, seq := range []int64{targetBlocked, targetFailed} {
		if remaining[seq] {
			t.Fatalf("target terminal seq %d survived; remaining=%v", seq, remaining)
		}
	}

	// Everything else must still be present.
	for _, seq := range []int64{
		keepPending, keepPublished,
		otherBranchBlocked, otherBranchFailed,
		otherGenBlocked, otherGenFailed,
	} {
		if !remaining[seq] {
			t.Fatalf("seq %d wrongly deleted; remaining=%v", seq, remaining)
		}
	}

	// Idempotent: a second call for the same pair deletes zero rows.
	n2, err := DeleteTerminalForDeadBranch(ctx, d, "refs/heads/dead", 3)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if n2 != 0 {
		t.Fatalf("second call deleted=%d want 0 (idempotent)", n2)
	}

	// Empty branchRef must return an error and must not delete anything.
	t.Run("empty_branch_ref", func(t *testing.T) {
		d2, _ := openTestDB(t)

		// Seed a terminal row in d2 so we can assert nothing was deleted.
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

		if _, err := DeleteTerminalForDeadBranch(ctx, d2, "", 1); err == nil {
			t.Fatalf("empty branch_ref must error")
		}

		// The seeded row must still be present.
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
}
