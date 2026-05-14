package daemon

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// seedBlockedModify lands a blocked_conflict row whose ops describe a
// before->after modify. The shared fixture uses HEAD=base; tests advance
// HEAD via an external commit after seeding so the self-heal probe sees
// HEAD's blob match the captured AfterOID.
func seedBlockedModify(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	path, beforeBlob, afterBlob, base string,
) (int64, state.CaptureEvent, state.CaptureOp) {
	t.Helper()
	ev := state.CaptureEvent{
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		BaseHead:         base,
		Operation:        "modify",
		Path:             path,
		Fidelity:         "rescan",
	}
	op := state.CaptureOp{
		Op:         "modify",
		Path:       path,
		BeforeOID:  sql.NullString{String: beforeBlob, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: afterBlob, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
		Fidelity:   "rescan",
	}
	seq, err := state.AppendCaptureEvent(ctx, f.db, ev, []state.CaptureOp{op})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	// Settle as blocked_conflict the same way recordConflict would.
	if err := state.MarkEventBlocked(ctx, f.db, seq,
		"modify before-state mismatch for "+path, 1.0,
		sql.NullString{String: f.cctx.BranchRef, Valid: true},
		sql.NullInt64{Int64: f.cctx.BranchGeneration, Valid: true},
		sql.NullString{String: base, Valid: true},
	); err != nil {
		t.Fatalf("MarkEventBlocked: %v", err)
	}
	return seq, ev, op
}

// readEventState returns the (state, commit_oid) for a capture_events row.
func readEventState(t *testing.T, ctx context.Context, db *state.DB, seq int64) (string, sql.NullString) {
	t.Helper()
	var s string
	var oid sql.NullString
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT state, commit_oid FROM capture_events WHERE seq = ?`, seq).Scan(&s, &oid); err != nil {
		t.Fatalf("query capture_events seq=%d: %v", seq, err)
	}
	return s, oid
}

// TestReplay_SelfHealsBlockedWhenHeadAlreadyMatches covers the happy path:
// a blocked_conflict row whose AfterOID matches HEAD's blob is promoted to
// published with the handled_external_after_block decision, without minting
// a new commit.
func TestReplay_SelfHealsBlockedWhenHeadAlreadyMatches(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	beforeBlob, err := git.HashObjectStdin(ctx, f.dir, []byte("before\n"))
	if err != nil {
		t.Fatalf("hash before: %v", err)
	}
	afterBlob, err := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
	if err != nil {
		t.Fatalf("hash after: %v", err)
	}

	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", beforeBlob, "seed before")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update-ref base: %v", err)
	}
	f.cctx.BaseHead = base

	seq, _, _ := seedBlockedModify(t, ctx, f, "doc.md", beforeBlob, afterBlob, base)

	// External committer lands the same after-state on top of base.
	external := commitSingleFileTree(t, ctx, f.dir, "doc.md", afterBlob, "external lands after", base)
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, external, base); err != nil {
		t.Fatalf("update-ref external: %v", err)
	}
	cctx := f.cctx
	cctx.BaseHead = external

	beforeCount := revListCount(t, ctx, f.dir, "HEAD")
	trace := &memoryTraceLogger{}
	sum, err := Replay(ctx, f.dir, f.db, cctx, ReplayOpts{
		GitDir: f.gitDir,
		Trace:  trace,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if sum.Conflicts != 0 || sum.Failed != 0 {
		t.Fatalf("unexpected summary: %+v", sum)
	}
	if got := revListCount(t, ctx, f.dir, "HEAD"); got != beforeCount {
		t.Fatalf("commit count changed from %d to %d; self-heal must not mint a commit", beforeCount, got)
	}

	st, commitOID := readEventState(t, ctx, f.db, seq)
	if st != state.EventStatePublished {
		t.Fatalf("state=%q want published", st)
	}
	if !commitOID.Valid || commitOID.String != external {
		t.Fatalf("commit_oid=%v want %s", commitOID, external)
	}
	assertReplayDecision(t, ctx, f.db, seq,
		state.DecisionKindHandledExternalAfterBlock,
		"handled_external_after_block")

	// publish_state singleton should be 'published' with the external OID.
	var status string
	var pubCommit sql.NullString
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT status, target_commit_oid FROM publish_state WHERE id = 1`).Scan(&status, &pubCommit); err != nil {
		t.Fatalf("query publish_state: %v", err)
	}
	if status != "published" {
		t.Fatalf("publish_state status=%q want published", status)
	}
	if !pubCommit.Valid || pubCommit.String != external {
		t.Fatalf("publish_state.target_commit_oid=%v want %s", pubCommit, external)
	}

	// Trace event class replay.self_heal should be emitted.
	found := 0
	for _, ev := range trace.Events() {
		if ev.EventClass == "replay.self_heal" && ev.Decision == state.DecisionKindHandledExternalAfterBlock {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("replay.self_heal trace count=%d want 1; events=%+v", found, trace.Events())
	}

	// Breadcrumb meta keys cleared (no remaining blocked rows on this anchor).
	for _, key := range []string{
		"last_replay_conflict",
		"last_replay_conflict_legacy",
		"last_replay_error",
	} {
		v, ok, err := state.MetaGet(ctx, f.db, key)
		if err != nil {
			t.Fatalf("MetaGet %s: %v", key, err)
		}
		if ok {
			t.Fatalf("meta %s still present after self-heal: %q", key, v)
		}
	}
}

// TestReplay_SelfHealRefusesCreateOps guards the op-type narrowing: a
// blocked_conflict row whose ops include a create stays blocked even when
// HEAD's tree happens to match the after-state. Self-heal is intentionally
// scoped to modify/mode/rename-with-BeforeOID at first cut.
func TestReplay_SelfHealRefusesCreateOps(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	afterBlob, err := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
	if err != nil {
		t.Fatalf("hash after: %v", err)
	}

	// Base commit has no path "newfile.txt". External commit creates it
	// with the captured AfterOID. A real-life create barrier could match
	// HEAD's blob, but self-heal must not promote create rows.
	emptyTree, err := git.Mktree(ctx, f.dir, nil)
	if err != nil {
		t.Fatalf("mktree empty: %v", err)
	}
	base, err := git.CommitTree(ctx, f.dir, emptyTree, "seed empty")
	if err != nil {
		t.Fatalf("commit empty: %v", err)
	}
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update-ref base: %v", err)
	}
	f.cctx.BaseHead = base

	ev := state.CaptureEvent{
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		BaseHead:         base,
		Operation:        "create",
		Path:             "newfile.txt",
		Fidelity:         "rescan",
	}
	op := state.CaptureOp{
		Op:        "create",
		Path:      "newfile.txt",
		AfterOID:  sql.NullString{String: afterBlob, Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		Fidelity:  "rescan",
	}
	seq, err := state.AppendCaptureEvent(ctx, f.db, ev, []state.CaptureOp{op})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	if err := state.MarkEventBlocked(ctx, f.db, seq,
		"create conflict for newfile.txt", 1.0,
		sql.NullString{String: f.cctx.BranchRef, Valid: true},
		sql.NullInt64{Int64: f.cctx.BranchGeneration, Valid: true},
		sql.NullString{String: base, Valid: true},
	); err != nil {
		t.Fatalf("MarkEventBlocked: %v", err)
	}

	// External commit creates the file at the captured AfterOID.
	external := commitSingleFileTree(t, ctx, f.dir, "newfile.txt", afterBlob, "external create", base)
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, external, base); err != nil {
		t.Fatalf("update-ref external: %v", err)
	}
	cctx := f.cctx
	cctx.BaseHead = external

	if _, err := Replay(ctx, f.dir, f.db, cctx, ReplayOpts{GitDir: f.gitDir}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	st, _ := readEventState(t, ctx, f.db, seq)
	if st != state.EventStateBlockedConflict {
		t.Fatalf("state=%q want blocked_conflict (self-heal must skip create ops)", st)
	}
}

// TestReplay_SelfHealRefusesWhenHeadBlobDiffers guards alreadyPublishedAtHEAD's
// per-op blob check: a blocked_conflict row whose captured AfterOID does NOT
// match HEAD's blob stays blocked.
func TestReplay_SelfHealRefusesWhenHeadBlobDiffers(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	beforeBlob, err := git.HashObjectStdin(ctx, f.dir, []byte("before\n"))
	if err != nil {
		t.Fatalf("hash before: %v", err)
	}
	afterBlob, err := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
	if err != nil {
		t.Fatalf("hash after: %v", err)
	}
	otherBlob, err := git.HashObjectStdin(ctx, f.dir, []byte("other-after\n"))
	if err != nil {
		t.Fatalf("hash other: %v", err)
	}

	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", beforeBlob, "seed before")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update-ref base: %v", err)
	}
	f.cctx.BaseHead = base

	// Capture expects afterBlob, but HEAD will end up holding otherBlob.
	seq, _, _ := seedBlockedModify(t, ctx, f, "doc.md", beforeBlob, afterBlob, base)

	external := commitSingleFileTree(t, ctx, f.dir, "doc.md", otherBlob, "external lands different content", base)
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, external, base); err != nil {
		t.Fatalf("update-ref external: %v", err)
	}
	cctx := f.cctx
	cctx.BaseHead = external

	if _, err := Replay(ctx, f.dir, f.db, cctx, ReplayOpts{GitDir: f.gitDir}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	st, _ := readEventState(t, ctx, f.db, seq)
	if st != state.EventStateBlockedConflict {
		t.Fatalf("state=%q want blocked_conflict (self-heal must skip when HEAD blob differs)", st)
	}
}

// TestReplay_SelfHealRefusesWhenAncestryFails guards alreadyPublishedAtHEAD's
// ancestry probe: if the captured base_head is not an ancestor of HEAD, the
// matching tree state is coincidence (operator hard-reset to an unrelated
// branch), not a successful parallel publish. Stay blocked.
func TestReplay_SelfHealRefusesWhenAncestryFails(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	beforeBlob, err := git.HashObjectStdin(ctx, f.dir, []byte("before\n"))
	if err != nil {
		t.Fatalf("hash before: %v", err)
	}
	afterBlob, err := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
	if err != nil {
		t.Fatalf("hash after: %v", err)
	}

	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", beforeBlob, "seed before")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update-ref base: %v", err)
	}
	f.cctx.BaseHead = base

	seq, _, _ := seedBlockedModify(t, ctx, f, "doc.md", beforeBlob, afterBlob, base)

	// Diverged HEAD: a parentless commit that holds afterBlob at doc.md.
	// base is NOT an ancestor of this commit, so the ancestry guard must
	// refuse settling on it. We use commitSingleFileTree without parents to
	// produce an orphan commit.
	divergent := commitSingleFileTree(t, ctx, f.dir, "doc.md", afterBlob, "orphan divergent")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, divergent, base); err != nil {
		t.Fatalf("update-ref divergent: %v", err)
	}
	cctx := f.cctx
	cctx.BaseHead = divergent

	if _, err := Replay(ctx, f.dir, f.db, cctx, ReplayOpts{GitDir: f.gitDir}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	st, _ := readEventState(t, ctx, f.db, seq)
	if st != state.EventStateBlockedConflict {
		t.Fatalf("state=%q want blocked_conflict (ancestry guard must skip diverged HEAD)", st)
	}
}

// TestTransitionBlockedToPublished_RefusesAfterRace exercises the
// race-safety of the state helper: when a concurrent recovery action moves
// the row out of blocked_conflict between the daemon's load and update, the
// helper must refuse rather than silently overwrite.
func TestTransitionBlockedToPublished_RefusesAfterRace(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	beforeBlob, err := git.HashObjectStdin(ctx, f.dir, []byte("before\n"))
	if err != nil {
		t.Fatalf("hash before: %v", err)
	}
	afterBlob, err := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
	if err != nil {
		t.Fatalf("hash after: %v", err)
	}
	seq, _, _ := seedBlockedModify(t, ctx, f, "doc.md", beforeBlob, afterBlob, f.cctx.BaseHead)

	// Simulate a race: another writer marks the row failed before the
	// daemon calls TransitionBlockedToPublished. The state predicate in
	// the helper's UPDATE should refuse to overwrite the row.
	if _, err := f.db.SQL().ExecContext(ctx,
		`UPDATE capture_events SET state = ? WHERE seq = ?`, state.EventStateFailed, seq); err != nil {
		t.Fatalf("race-simulate update: %v", err)
	}

	err = state.TransitionBlockedToPublished(ctx, f.db, seq,
		"deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", 2.0,
		f.cctx.BranchRef, f.cctx.BranchGeneration, f.cctx.BaseHead)
	if !errors.Is(err, state.ErrBlockedRowNotEligible) {
		t.Fatalf("TransitionBlockedToPublished err=%v want ErrBlockedRowNotEligible", err)
	}

	// Row must still be 'failed', commit_oid must NOT have been written.
	st, oid := readEventState(t, ctx, f.db, seq)
	if st != state.EventStateFailed {
		t.Fatalf("state=%q want failed (helper must not overwrite a moved row)", st)
	}
	if oid.Valid {
		t.Fatalf("commit_oid=%q want NULL (helper must not write to a moved row)", oid.String)
	}
}
