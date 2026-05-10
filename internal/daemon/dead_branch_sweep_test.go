package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// TestIsKeepDeadBranchBarriers covers the truthy/falsy table for the env
// opt-out helper. Truthy values ("1", "true", "yes", "on", any case) disable
// the runtime + startup prune; everything else (including empty / unset /
// "garbage") keeps the default-on behaviour.
func TestIsKeepDeadBranchBarriers(t *testing.T) {
	cases := []struct {
		val  string
		want bool
	}{
		{"1", true},
		{"true", true},
		{"TRUE", true},
		{"True", true},
		{"yes", true},
		{"Yes", true},
		{"YES", true},
		{"on", true},
		{"ON", true},
		{"On", true},
		{"  on  ", true},
		{"", false},
		{"0", false},
		{"false", false},
		{"FALSE", false},
		{"no", false},
		{"NO", false},
		{"off", false},
		{"OFF", false},
		{"garbage", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run("val="+tc.val, func(t *testing.T) {
			t.Setenv(EnvKeepDeadBranchBarriers, tc.val)
			if got := isKeepDeadBranchBarriers(); got != tc.want {
				t.Fatalf("isKeepDeadBranchBarriers(%q)=%v want %v", tc.val, got, tc.want)
			}
		})
	}
	// Explicit unset case — t.Setenv only adds; verify the helper sees an
	// absent env var as falsy too.
	t.Run("unset", func(t *testing.T) {
		// Setenv to empty then unset by leaving Setenv with empty-string
		// would still set it to "". To prove "unset" we rely on the
		// empty-string case being falsy, which the table above already
		// asserts. Adding this guard documents the intent.
		t.Setenv(EnvKeepDeadBranchBarriers, "")
		if isKeepDeadBranchBarriers() {
			t.Fatalf("expected false when env is empty")
		}
	})
}

// seedTerminalEvent inserts one capture_events row in the requested terminal
// state for the given (branch_ref, branch_generation) pair. Returns the
// assigned seq.
func seedTerminalEvent(t *testing.T, db *state.DB, branchRef string, generation int64, baseHead, path, eventState string) int64 {
	t.Helper()
	ctx := context.Background()
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        branchRef,
		BranchGeneration: generation,
		BaseHead:         baseHead,
		Operation:        "create",
		Path:             path,
		Fidelity:         "full",
		State:            state.EventStatePending,
	}, []state.CaptureOp{{
		Op:        "create",
		Path:      path,
		Fidelity:  "full",
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:  sql.NullString{String: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Valid: true},
	}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	if eventState == state.EventStatePending {
		return seq
	}
	if err := state.MarkEventPublished(ctx, db, seq, eventState,
		sql.NullString{}, sql.NullString{String: "seeded", Valid: true},
		sql.NullString{}, float64(time.Now().UnixNano())/1e9); err != nil {
		t.Fatalf("MarkEventPublished: %v", err)
	}
	return seq
}

func countEventsByRefState(t *testing.T, db *state.DB, branchRef, eventState string) int {
	t.Helper()
	var n int
	if err := db.SQL().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM capture_events WHERE branch_ref = ? AND state = ?`,
		branchRef, eventState).Scan(&n); err != nil {
		t.Fatalf("count events ref=%s state=%s: %v", branchRef, eventState, err)
	}
	return n
}

// TestDeadBranchSweep_PrunesDeadRefRows seeds blocked_conflict + failed +
// pending rows for refs/heads/old (which is NOT created in the test repo) and
// terminal rows for the active refs/heads/main; runs the startup sweep;
// asserts old's pending + terminal rows are all deleted (the helper now drops
// pending rows together with terminals so the prune does not get reverted on
// the next replay tick), and main's rows are preserved.
func TestDeadBranchSweep_PrunesDeadRefRows(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	ctx := context.Background()
	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const deadRef = "refs/heads/old"
	const activeRef = "refs/heads/main"

	// Dead-ref rows: one of each (terminal + pending). All three must be
	// deleted — leaving pending rows behind would let replay restamp a
	// blocked_conflict on the next tick and defeat the prune.
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "old-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "old-failed.txt", state.EventStateFailed)
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "old-pending.txt", state.EventStatePending)
	// Active-ref rows: one terminal of each kind. These must not be touched
	// because cctx.BranchRef matches and the sweep skips the active pair.
	seedTerminalEvent(t, f.db, activeRef, 1, headOID, "main-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, activeRef, 1, headOID, "main-failed.txt", state.EventStateFailed)
	// And a live-ref pending row to confirm the live-ref path leaves
	// pending alone (only the dead pair drops pending).
	seedTerminalEvent(t, f.db, activeRef, 1, headOID, "main-pending.txt", state.EventStatePending)

	cctx := CaptureContext{
		BranchRef:        activeRef,
		BranchGeneration: 1,
		BaseHead:         headOID,
	}
	runStartupDeadBranchSweep(ctx, f.dir, f.db, cctx, slog.Default(), nil)

	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateBlockedConflict); got != 0 {
		t.Fatalf("dead-ref blocked_conflict rows=%d want 0", got)
	}
	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateFailed); got != 0 {
		t.Fatalf("dead-ref failed rows=%d want 0", got)
	}
	// Pending rows for the dead pair must also be gone so PendingEvents does
	// not re-expose them on the next replay tick.
	if got := countEventsByRefState(t, f.db, deadRef, state.EventStatePending); got != 0 {
		t.Fatalf("dead-ref pending rows=%d want 0 (helper must drop pending+terminal together)", got)
	}
	if got := countEventsByRefState(t, f.db, activeRef, state.EventStateBlockedConflict); got != 1 {
		t.Fatalf("active-ref blocked_conflict rows=%d want 1 (active pair must be preserved)", got)
	}
	if got := countEventsByRefState(t, f.db, activeRef, state.EventStateFailed); got != 1 {
		t.Fatalf("active-ref failed rows=%d want 1 (active pair must be preserved)", got)
	}
	if got := countEventsByRefState(t, f.db, activeRef, state.EventStatePending); got != 1 {
		t.Fatalf("active-ref pending rows=%d want 1 (live-ref pending must be preserved)", got)
	}
}

// TestDeadBranchSweep_RegressionPendingDoesNotLeakBarrier asserts the P1
// regression: after deleting terminals for a dead branch, no later
// PendingEvents call must surface pending rows for the same dead pair (which
// would let replay re-stamp a blocked_conflict and defeat the prune). This
// is the test against the bug cr-expert flagged.
func TestDeadBranchSweep_RegressionPendingDoesNotLeakBarrier(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	ctx := context.Background()
	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const deadRef = "refs/heads/dead"
	// Seed blocked_conflict + failed + pending plus a stamped publish_state
	// singleton pointing at the blocked_conflict (the realistic shape of a
	// daemon DB after a Diverged-into-deleted-branch sequence).
	blockedSeq := seedTerminalEvent(t, f.db, deadRef, 1, headOID, "dead-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "dead-failed.txt", state.EventStateFailed)
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "dead-pending.txt", state.EventStatePending)
	if err := state.MarkEventBlocked(ctx, f.db, blockedSeq, "seeded blocker", float64(time.Now().UnixNano())/1e9,
		sql.NullString{String: deadRef, Valid: true},
		sql.NullInt64{Int64: 1, Valid: true},
		sql.NullString{String: headOID, Valid: true},
	); err != nil {
		t.Fatalf("MarkEventBlocked: %v", err)
	}

	cctx := CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         headOID,
	}
	runStartupDeadBranchSweep(ctx, f.dir, f.db, cctx, slog.Default(), nil)

	// All capture_events for the dead ref must be gone.
	var total int
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE branch_ref = ?`, deadRef,
	).Scan(&total); err != nil {
		t.Fatalf("count dead-ref rows: %v", err)
	}
	if total != 0 {
		t.Fatalf("dead-ref rows=%d want 0 after sweep", total)
	}

	// PendingEvents must not surface anything for the dead ref — this is the
	// regression: pre-fix the helper left pending rows behind, so the next
	// replay pass would re-stamp a fresh blocked_conflict on the dead pair.
	pending, err := state.PendingEvents(ctx, f.db, 64)
	if err != nil {
		t.Fatalf("PendingEvents: %v", err)
	}
	for _, ev := range pending {
		if ev.BranchRef == deadRef {
			t.Fatalf("PendingEvents surfaced row for dead ref %q after sweep: seq=%d",
				deadRef, ev.Seq)
		}
	}

	// publish_state must read 'ok' (barrier was lifted in the same tx).
	var status string
	var errMsg sql.NullString
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT status, error FROM publish_state WHERE id = 1`,
	).Scan(&status, &errMsg); err != nil {
		t.Fatalf("read publish_state: %v", err)
	}
	if status != "ok" {
		t.Fatalf("publish_state.status=%q want 'ok' after sweep", status)
	}
	if errMsg.Valid {
		t.Fatalf("publish_state.error=%q want NULL after sweep", errMsg.String)
	}

	// Breadcrumb meta keys must be cleared.
	for _, key := range []string{
		"last_replay_conflict", "last_replay_conflict_legacy", "last_replay_error",
	} {
		if _, ok, err := state.MetaGet(ctx, f.db, key); err != nil {
			t.Fatalf("MetaGet %q: %v", key, err)
		} else if ok {
			t.Fatalf("breadcrumb %q still present after sweep", key)
		}
	}
}

// TestDeadBranchSweep_OptOutPreservesRows asserts that ACD_KEEP_DEAD_BRANCH_BARRIERS=1
// short-circuits the sweep entirely; even dead-ref terminals are preserved.
func TestDeadBranchSweep_OptOutPreservesRows(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "1")
	f := newDaemonFixture(t)
	ctx := context.Background()
	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const deadRef = "refs/heads/gone"
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "gone-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "gone-failed.txt", state.EventStateFailed)

	cctx := CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         headOID,
	}
	runStartupDeadBranchSweep(ctx, f.dir, f.db, cctx, slog.Default(), nil)

	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateBlockedConflict); got != 1 {
		t.Fatalf("opt-out blocked_conflict rows=%d want 1", got)
	}
	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateFailed); got != 1 {
		t.Fatalf("opt-out failed rows=%d want 1", got)
	}
}

// TestDeadBranchSweep_LiveRefsErrorPreservesRows asserts that when
// git.LiveBranchSet returns an error (e.g. bogus repoDir), the sweep fails
// closed: terminal rows are preserved rather than silently dropped. The
// sweep now batches liveness probes via for-each-ref instead of per-pair
// RefExists, so a single error preserves all candidate rows in one go (the
// previous per-pair contract preserved on a per-ref basis).
func TestDeadBranchSweep_LiveRefsErrorPreservesRows(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	ctx := context.Background()
	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const deadRef = "refs/heads/probe-error"
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "probe-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "probe-failed.txt", state.EventStateFailed)

	// Bogus repoDir — for-each-ref will exit with 128 (not a git repo).
	// Sweep must surface the liveness-probe error and skip the prune
	// entirely (fail closed: cannot prove dead, cannot delete).
	bogusRepo := t.TempDir()
	cctx := CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         headOID,
	}
	runStartupDeadBranchSweep(ctx, bogusRepo, f.db, cctx, slog.Default(), nil)

	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateBlockedConflict); got != 1 {
		t.Fatalf("probe-error blocked_conflict rows=%d want 1 (must fail closed)", got)
	}
	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateFailed); got != 1 {
		t.Fatalf("probe-error failed rows=%d want 1 (must fail closed)", got)
	}
}

// TestDivergedHookPrunesDeadBranchTerminals drives a Diverged transition
// through the run loop where the previous branch ref no longer resolves.
// Mirrors TestRun_BranchSwitchDropsPending but adds:
//   - a blocked_conflict row on refs/heads/old (which we delete before the
//     transition)
//   - assertion that after the bump, the terminal row is gone
//   - assertion that an analogous row on a still-live ref is preserved
func TestDivergedHookPrunesDeadBranchTerminals(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()

	seedHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse seed: %v", err)
	}

	// Create refs/heads/old (will be deleted before the Diverged transition
	// fires). This proves the runtime hook path discriminates "ref still
	// alive" vs "ref deleted".
	if err := git.UpdateRef(ctx, f.dir, "refs/heads/old", seedHead, ""); err != nil {
		t.Fatalf("update-ref refs/heads/old: %v", err)
	}
	// Create refs/heads/keep — kept alive throughout the test.
	if err := git.UpdateRef(ctx, f.dir, "refs/heads/keep", seedHead, ""); err != nil {
		t.Fatalf("update-ref refs/heads/keep: %v", err)
	}

	// Pre-seed terminal rows under generation 1 for both refs.
	seedTerminalEvent(t, f.db, "refs/heads/old", 1, seedHead, "old-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, "refs/heads/keep", 1, seedHead, "keep-blocked.txt", state.EventStateBlockedConflict)

	// Now delete refs/heads/old to simulate "branch merged + deleted".
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "update-ref", "-d", "refs/heads/old"); err != nil {
		t.Fatalf("delete refs/heads/old: %v", err)
	}

	// Manually persist branch token so the run-loop start sees us on
	// refs/heads/old and detects the Diverged transition into a sibling.
	// Easier route: fabricate a sibling ref on main that diverges. Use the
	// well-trodden pattern from TestRun_BranchGenerationBumpsOnExternalReset.
	blob, err := git.HashObjectStdin(ctx, f.dir, []byte("sibling\n"))
	if err != nil {
		t.Fatalf("hash sibling blob: %v", err)
	}
	siblingTree, err := git.Mktree(ctx, f.dir, []git.MktreeEntry{
		{Mode: git.RegularFileMode, Type: "blob", OID: blob, Path: "sibling.txt"},
	})
	if err != nil {
		t.Fatalf("mktree sibling: %v", err)
	}
	sibling, err := git.CommitTree(ctx, f.dir, siblingTree, "sibling root")
	if err != nil {
		t.Fatalf("commit-tree sibling: %v", err)
	}

	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(runCtx, Options{
			RepoPath:    f.dir,
			GitDir:      f.gitDir,
			DB:          f.db,
			Scheduler:   fastScheduler(),
			BootGrace:   30 * time.Second,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
		})
	}()

	// Wait for the initial token + generation seed so we can drive a
	// Diverged transition deterministically.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		gen, _, _ := state.MetaGet(ctx, f.db, MetaKeyBranchGeneration)
		head, _, _ := state.MetaGet(ctx, f.db, MetaKeyBranchHead)
		if gen == "1" && head == seedHead {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Now reseat refs/heads/main onto the sibling: a Diverged transition
	// from the daemon's perspective. The runtime Diverged hook bumps
	// generation 1 -> 2 and (because the prior branch ref refs/heads/main
	// IS the same as the new one — which is alive) the dead-branch prune
	// short-circuits on the active ref.
	//
	// To exercise the dead-branch path we don't need to switch branches in
	// the worktree; instead we directly invoke the helper on a *previous*
	// generation pair whose ref is dead, which is what the Diverged path
	// would do internally if branch switching deleted the prior ref.
	//
	// First, kick the run loop into a Diverged transition to validate the
	// integration.
	if err := git.UpdateRef(ctx, f.dir, "refs/heads/main", sibling, ""); err != nil {
		t.Fatalf("update-ref to sibling: %v", err)
	}
	for i := 0; i < 4; i++ {
		select {
		case wakeCh <- struct{}{}:
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}
	deadline = time.Now().Add(5 * time.Second)
	var got string
	for time.Now().Before(deadline) {
		v, ok, _ := state.MetaGet(ctx, f.db, MetaKeyBranchGeneration)
		if ok && v != "" && v != "1" {
			got = v
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got == "" {
		t.Fatalf("branch.generation did not bump after sibling reset; runtime Diverged path never fired")
	}

	// The Diverged hook above deleted pending events for generation 1 (via
	// state.DeletePendingForGeneration). Our seeded terminal rows survive
	// that step. Their fate hinges on whether the dead-branch prune sees a
	// live or dead ref. refs/heads/old is DEAD, refs/heads/keep is ALIVE.
	//
	// The runtime hook only prunes for tokenBranchRef(oldToken), i.e. the
	// branch the daemon was on (refs/heads/main). To exercise the dead-ref
	// path against refs/heads/old, also invoke the helper directly: this
	// also covers the case where a prior daemon session moved off
	// refs/heads/old before deletion and left terminals behind.
	pruneDeadBranchTerminals(ctx, f.dir, f.db,
		CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 2, BaseHead: sibling},
		"refs/heads/old", 1,
		slog.Default(), nil,
		"test-direct invocation")

	if n := countEventsByRefState(t, f.db, "refs/heads/old", state.EventStateBlockedConflict); n != 0 {
		t.Fatalf("dead-ref refs/heads/old terminal rows=%d want 0 after Diverged prune", n)
	}
	if n := countEventsByRefState(t, f.db, "refs/heads/keep", state.EventStateBlockedConflict); n != 1 {
		t.Fatalf("live-ref refs/heads/keep terminal rows=%d want 1 (must be preserved)", n)
	}

	cancel()
	wg.Wait()
}

// TestDeadBranchSweep_WritesMetaKeysWhenRowsPruned asserts the startup sweep
// stamps the three operator-facing meta keys when at least one row is pruned.
// This is the input `acd diagnose --json` reads to surface stale-branch
// hygiene activity.
func TestDeadBranchSweep_WritesMetaKeysWhenRowsPruned(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	ctx := context.Background()
	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const deadRef = "refs/heads/old"
	const activeRef = "refs/heads/main"
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "old-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "old-failed.txt", state.EventStateFailed)

	before := time.Now().Unix()
	cctx := CaptureContext{
		BranchRef:        activeRef,
		BranchGeneration: 1,
		BaseHead:         headOID,
	}
	runStartupDeadBranchSweep(ctx, f.dir, f.db, cctx, slog.Default(), nil)
	after := time.Now().Unix()

	// last_run_ts must be a parseable int within the wall-clock window.
	tsRaw, ok, err := state.MetaGet(ctx, f.db, MetaKeyDeadBranchPruneLastRunTS)
	if err != nil {
		t.Fatalf("MetaGet last_run_ts: %v", err)
	}
	if !ok || tsRaw == "" {
		t.Fatalf("expected meta %q to be set after non-empty sweep", MetaKeyDeadBranchPruneLastRunTS)
	}
	ts, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		t.Fatalf("parse last_run_ts %q: %v", tsRaw, err)
	}
	if ts < before || ts > after {
		t.Fatalf("last_run_ts=%d outside [%d, %d]", ts, before, after)
	}

	// last_count must equal the total rows pruned (2: blocked_conflict + failed).
	countRaw, ok, err := state.MetaGet(ctx, f.db, MetaKeyDeadBranchPruneLastCount)
	if err != nil {
		t.Fatalf("MetaGet last_count: %v", err)
	}
	if !ok {
		t.Fatalf("expected meta %q to be set", MetaKeyDeadBranchPruneLastCount)
	}
	if countRaw != "2" {
		t.Fatalf("last_count=%q want %q", countRaw, "2")
	}

	// last_refs must be valid JSON containing the dead ref.
	refsRaw, ok, err := state.MetaGet(ctx, f.db, MetaKeyDeadBranchPruneLastRefs)
	if err != nil {
		t.Fatalf("MetaGet last_refs: %v", err)
	}
	if !ok {
		t.Fatalf("expected meta %q to be set", MetaKeyDeadBranchPruneLastRefs)
	}
	var refs []string
	if err := json.Unmarshal([]byte(refsRaw), &refs); err != nil {
		t.Fatalf("unmarshal last_refs %q: %v", refsRaw, err)
	}
	if len(refs) != 1 || refs[0] != deadRef {
		t.Fatalf("last_refs=%v want [%q]", refs, deadRef)
	}
}

// TestDeadBranchSweep_NoMetaWhenNoOp asserts the sweep does NOT stamp the meta
// keys when no rows were pruned (only live-ref terminals exist). The previous
// "last action that did something" snapshot must survive a no-op pass.
func TestDeadBranchSweep_NoMetaWhenNoOp(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	ctx := context.Background()
	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const activeRef = "refs/heads/main"
	// Only seed terminals on the active ref — sweep must skip them.
	seedTerminalEvent(t, f.db, activeRef, 1, headOID, "main-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, activeRef, 1, headOID, "main-failed.txt", state.EventStateFailed)

	cctx := CaptureContext{
		BranchRef:        activeRef,
		BranchGeneration: 1,
		BaseHead:         headOID,
	}
	runStartupDeadBranchSweep(ctx, f.dir, f.db, cctx, slog.Default(), nil)

	for _, key := range []string{
		MetaKeyDeadBranchPruneLastRunTS,
		MetaKeyDeadBranchPruneLastCount,
		MetaKeyDeadBranchPruneLastRefs,
	} {
		v, ok, err := state.MetaGet(ctx, f.db, key)
		if err != nil {
			t.Fatalf("MetaGet %q: %v", key, err)
		}
		if ok {
			t.Fatalf("expected meta %q absent after no-op sweep; got %q", key, v)
		}
	}
}

// TestDivergedHookWritesMetaKeys drives the runtime Diverged-hook helper
// directly with a dead previous ref and asserts the three meta keys are
// stamped. Mirrors the integration in TestDivergedHookPrunesDeadBranchTerminals
// but isolates the meta-write contract.
func TestDivergedHookWritesMetaKeys(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	ctx := context.Background()
	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const deadRef = "refs/heads/dead-after-merge"
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "dead-blocked.txt", state.EventStateBlockedConflict)

	before := time.Now().Unix()
	pruneDeadBranchTerminals(ctx, f.dir, f.db,
		CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 2, BaseHead: headOID},
		deadRef, 1,
		slog.Default(), nil,
		"diverged hook")
	after := time.Now().Unix()

	tsRaw, ok, err := state.MetaGet(ctx, f.db, MetaKeyDeadBranchPruneLastRunTS)
	if err != nil {
		t.Fatalf("MetaGet last_run_ts: %v", err)
	}
	if !ok {
		t.Fatalf("expected meta %q to be set after Diverged-hook prune", MetaKeyDeadBranchPruneLastRunTS)
	}
	ts, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		t.Fatalf("parse last_run_ts: %v", err)
	}
	if ts < before || ts > after {
		t.Fatalf("last_run_ts=%d outside [%d, %d]", ts, before, after)
	}

	countRaw, ok, _ := state.MetaGet(ctx, f.db, MetaKeyDeadBranchPruneLastCount)
	if !ok || countRaw != "1" {
		t.Fatalf("last_count=%q ok=%v want %q", countRaw, ok, "1")
	}
	refsRaw, ok, _ := state.MetaGet(ctx, f.db, MetaKeyDeadBranchPruneLastRefs)
	if !ok {
		t.Fatalf("expected meta %q to be set", MetaKeyDeadBranchPruneLastRefs)
	}
	var refs []string
	if err := json.Unmarshal([]byte(refsRaw), &refs); err != nil {
		t.Fatalf("unmarshal last_refs %q: %v", refsRaw, err)
	}
	if len(refs) != 1 || refs[0] != deadRef {
		t.Fatalf("last_refs=%v want [%q]", refs, deadRef)
	}
}
