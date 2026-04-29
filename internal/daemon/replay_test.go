package daemon

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	acdtrace "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/trace"
)

// TestReplay_Lifecycle: capture creates events for a new file; replay
// produces one commit per event with the correct tree+message+parent.
func TestReplay_Lifecycle(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	// Add two files: foo.txt and bar.txt. Each becomes its own capture event
	// (one classify op per event), and replay must produce two commits.
	if err := os.WriteFile(filepath.Join(f.dir, "foo.txt"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("write foo: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "bar.txt"), []byte("world\n"), 0o644); err != nil {
		t.Fatalf("write bar: %v", err)
	}

	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("Capture: %v", err)
	}

	// Verify capture wrote events as `pending`.
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil {
		t.Fatalf("PendingEvents: %v", err)
	}
	if len(pending) < 2 {
		t.Fatalf("want >=2 pending events for foo+bar, got %d", len(pending))
	}

	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		MessageFn: DeterministicMessage,
		GitDir:    f.gitDir,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if sum.Published != len(pending) {
		t.Fatalf("Published=%d want %d", sum.Published, len(pending))
	}
	if sum.Conflicts != 0 || sum.Failed != 0 {
		t.Fatalf("unexpected conflicts/failed: %+v", sum)
	}

	// `git log --oneline` on main must show one commit per event on top of
	// the seed commit.
	out, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "log", "--oneline", "-n", "10", f.cctx.BranchRef)
	if err != nil {
		t.Fatalf("git log: %v", err)
	}
	logLines := strings.Split(strings.TrimSpace(string(out)), "\n")
	// seed + N capture commits.
	wantCommits := len(pending) + 1
	if len(logLines) != wantCommits {
		t.Fatalf("git log lines=%d, want %d:\n%s", len(logLines), wantCommits, out)
	}

	// Each pending event's commit_oid should now be set on the published row.
	rows, err := f.db.SQL().QueryContext(ctx,
		`SELECT operation, path, state, commit_oid FROM capture_events ORDER BY seq ASC`)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var op, path, st string
		var oid sql.NullString
		if err := rows.Scan(&op, &path, &st, &oid); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if st != "published" {
			t.Fatalf("event op=%s path=%s state=%s want=published", op, path, st)
		}
		if !oid.Valid || oid.String == "" {
			t.Fatalf("event op=%s path=%s missing commit_oid", op, path)
		}
	}
}

// TestReplay_Conflict: when the scratch index diverges from the event's
// before-state, replay must terminally settle the event in
// state.EventStateBlockedConflict and upsert publish_state.status to match.
// The row must drop out of PendingEvents so a stuck blocker no longer
// re-runs on every poll tick.
func TestReplay_Conflict(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	// Stage an event by hand: a `modify` op on a path that does NOT exist
	// in the live index. Conflict detection should catch this.
	const branch = "refs/heads/main"
	ev := state.CaptureEvent{
		BranchRef:        branch,
		BranchGeneration: 1,
		BaseHead:         f.cctx.BaseHead,
		Operation:        "modify",
		Path:             "nonexistent.txt",
		Fidelity:         "rescan",
	}
	op := state.CaptureOp{
		Op:         "modify",
		Path:       "nonexistent.txt",
		BeforeOID:  sql.NullString{String: "1111111111111111111111111111111111111111", Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: "2222222222222222222222222222222222222222", Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
		Fidelity:   "rescan",
	}
	seq, err := state.AppendCaptureEvent(ctx, f.db, ev, []state.CaptureOp{op})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}

	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if sum.Conflicts != 1 {
		t.Fatalf("Conflicts=%d want 1 (sum=%+v)", sum.Conflicts, sum)
	}
	if sum.Published != 0 {
		t.Fatalf("Published=%d want 0", sum.Published)
	}

	// Event must NOT remain pending — terminal blocker drops out of FIFO.
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil {
		t.Fatalf("PendingEvents: %v", err)
	}
	for _, p := range pending {
		if p.Seq == seq {
			t.Fatalf("blocked event seq=%d should NOT be pending; pending=%+v", seq, pending)
		}
	}

	// And it must show up under blocked_conflict.
	blocked, err := state.CountEventsByState(ctx, f.db, state.EventStateBlockedConflict)
	if err != nil {
		t.Fatalf("CountEventsByState: %v", err)
	}
	if blocked != 1 {
		t.Fatalf("blocked_conflict count = %d, want 1", blocked)
	}

	pub, ok, err := state.LoadPublishState(ctx, f.db)
	if err != nil {
		t.Fatalf("LoadPublishState: %v", err)
	}
	if !ok {
		t.Fatalf("publish_state row not written")
	}
	if pub.Status != state.EventStateBlockedConflict {
		t.Fatalf("publish_state.status=%q want blocked_conflict", pub.Status)
	}
	if !pub.EventSeq.Valid || pub.EventSeq.Int64 != seq {
		t.Fatalf("publish_state.event_seq=%v want %d", pub.EventSeq, seq)
	}
}

func TestReplay_CASRetryRecoversFromLock(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	if err := os.WriteFile(filepath.Join(f.dir, "cas-retry.txt"), []byte("ok\n"), 0o644); err != nil {
		t.Fatalf("write cas-retry.txt: %v", err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("Capture: %v", err)
	}

	restoreReplayRefSeams(t)
	var attempts int
	replayUpdateRef = func(ctx context.Context, repoRoot, ref, newOID, oldOID string) error {
		attempts++
		if attempts < 3 {
			return errors.New("cannot lock ref 'refs/heads/main': File exists")
		}
		return git.UpdateRef(ctx, repoRoot, ref, newOID, oldOID)
	}
	var sleeps []time.Duration
	replayUpdateRefSleep = func(ctx context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	}
	trace := &memoryTraceLogger{}

	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		MessageFn: DeterministicMessage,
		GitDir:    f.gitDir,
		Trace:     trace,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("UpdateRef attempts=%d want 3", attempts)
	}
	if sum.Published != 1 || sum.Conflicts != 0 || sum.Failed != 0 {
		t.Fatalf("unexpected summary: %+v", sum)
	}
	wantSleeps := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond}
	if len(sleeps) != len(wantSleeps) {
		t.Fatalf("sleeps=%v want %v", sleeps, wantSleeps)
	}
	for i := range wantSleeps {
		if sleeps[i] != wantSleeps[i] {
			t.Fatalf("sleeps=%v want %v", sleeps, wantSleeps)
		}
	}

	events := trace.Events()
	updateRefEvents := traceEventsByClass(events, "replay.update_ref")
	if len(updateRefEvents) != 3 {
		t.Fatalf("update_ref trace events=%d want 3; events=%+v", len(updateRefEvents), events)
	}
	if updateRefEvents[0].Decision != "retry" || updateRefEvents[1].Decision != "retry" {
		t.Fatalf("first two update_ref decisions=%q,%q want retry,retry", updateRefEvents[0].Decision, updateRefEvents[1].Decision)
	}
	if updateRefEvents[2].Decision != state.EventStatePublished {
		t.Fatalf("final update_ref decision=%q want %q", updateRefEvents[2].Decision, state.EventStatePublished)
	}
	if !traceHasClass(events, "replay.commit") {
		t.Fatalf("missing final replay.commit trace; events=%+v", events)
	}
}

func TestReplay_CASMismatchNoRetry(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	if err := os.WriteFile(filepath.Join(f.dir, "cas-mismatch.txt"), []byte("ok\n"), 0o644); err != nil {
		t.Fatalf("write cas-mismatch.txt: %v", err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("Capture: %v", err)
	}

	restoreReplayRefSeams(t)
	var attempts int
	replayUpdateRef = func(ctx context.Context, repoRoot, ref, newOID, oldOID string) error {
		attempts++
		return errors.New("cannot lock ref 'refs/heads/main': is at 1111111111111111111111111111111111111111 but expected " + oldOID)
	}
	replayUpdateRefSleep = func(ctx context.Context, d time.Duration) error {
		t.Fatalf("sleep called for true CAS mismatch: %s", d)
		return nil
	}
	trace := &memoryTraceLogger{}

	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		MessageFn: DeterministicMessage,
		GitDir:    f.gitDir,
		Trace:     trace,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("UpdateRef attempts=%d want 1", attempts)
	}
	if sum.Published != 0 || sum.Conflicts != 1 || sum.Failed != 0 {
		t.Fatalf("unexpected summary: %+v", sum)
	}

	blocked, err := state.CountEventsByState(ctx, f.db, state.EventStateBlockedConflict)
	if err != nil {
		t.Fatalf("CountEventsByState: %v", err)
	}
	if blocked != 1 {
		t.Fatalf("blocked_conflict count=%d want 1", blocked)
	}
	events := trace.Events()
	updateRefEvents := traceEventsByClass(events, "replay.update_ref")
	if len(updateRefEvents) != 1 {
		t.Fatalf("update_ref trace events=%d want 1; events=%+v", len(updateRefEvents), events)
	}
	if updateRefEvents[0].Decision != state.EventStateBlockedConflict {
		t.Fatalf("update_ref decision=%q want %q", updateRefEvents[0].Decision, state.EventStateBlockedConflict)
	}
	if traceHasDecision(events, "retry") {
		t.Fatalf("unexpected retry trace for true CAS mismatch; events=%+v", events)
	}
	if !traceHasClass(events, "replay.conflict") {
		t.Fatalf("missing final replay.conflict trace; events=%+v", events)
	}
}

func TestRecordConflict_StructuredMetadata(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	ev := state.CaptureEvent{
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		BaseHead:         f.cctx.BaseHead,
		Operation:        "modify",
		Path:             "nonexistent.txt",
		Fidelity:         "rescan",
	}
	op := state.CaptureOp{
		Op:         "modify",
		Path:       "nonexistent.txt",
		BeforeOID:  sql.NullString{String: "1111111111111111111111111111111111111111", Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: "2222222222222222222222222222222222222222", Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
		Fidelity:   "rescan",
	}
	seq, err := state.AppendCaptureEvent(ctx, f.db, ev, []state.CaptureOp{op})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}

	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if sum.Conflicts != 1 {
		t.Fatalf("Conflicts=%d want 1", sum.Conflicts)
	}

	var meta replayConflictMetadata
	ok, err := state.MetaGetJSON(ctx, f.db, metaKeyLastReplayConflict, &meta)
	if err != nil {
		t.Fatalf("MetaGetJSON: %v", err)
	}
	if !ok {
		t.Fatalf("%s not written", metaKeyLastReplayConflict)
	}
	if meta.Seq != seq {
		t.Fatalf("meta.Seq=%d want %d", meta.Seq, seq)
	}
	if meta.ErrorClass != replayErrorBeforeStateMismatch {
		t.Fatalf("meta.ErrorClass=%q want %q", meta.ErrorClass, replayErrorBeforeStateMismatch)
	}
	if meta.Ref != f.cctx.BranchRef {
		t.Fatalf("meta.Ref=%q want %q", meta.Ref, f.cctx.BranchRef)
	}
	if meta.Path != "nonexistent.txt" {
		t.Fatalf("meta.Path=%q want nonexistent.txt", meta.Path)
	}
	if meta.Message == "" || !strings.Contains(meta.Message, "missing-in-index") {
		t.Fatalf("meta.Message=%q want missing-in-index", meta.Message)
	}
	if meta.TS == "" {
		t.Fatalf("meta.TS empty")
	}

	legacy, ok, err := state.MetaGet(ctx, f.db, metaKeyLastReplayConflictLegacy)
	if err != nil {
		t.Fatalf("MetaGet legacy: %v", err)
	}
	if !ok || !strings.Contains(legacy, "seq=") || !strings.Contains(legacy, meta.Message) {
		t.Fatalf("legacy mirror=%q", legacy)
	}
}

func TestClassifyReplayIssue(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want string
	}{
		{
			name: "cas",
			msg:  "update-ref CAS failed: cannot lock ref",
			want: replayErrorCASFail,
		},
		{
			name: "before-state",
			msg:  "modify before-state mismatch for file.txt",
			want: replayErrorBeforeStateMismatch,
		},
		{
			name: "commit build",
			msg:  "commit-tree: missing tree",
			want: replayErrorCommitBuildFailure,
		},
		{
			name: "ref missing",
			msg:  "branch ref mismatch: event captured on refs/heads/a but daemon is on refs/heads/b",
			want: replayErrorRefMissing,
		},
		{
			name: "validation",
			msg:  "missing after_oid for create file.txt",
			want: replayErrorValidation,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyReplayIssue(tt.msg); got != tt.want {
				t.Fatalf("classifyReplayIssue(%q)=%q want %q", tt.msg, got, tt.want)
			}
		})
	}
}

// TestReplay_BatchHaltsOnBlockedConflict: a blocker in the middle of the
// queue must terminally settle that event and stop the batch — every event
// behind it stays pending so the next poll tick can re-attempt them once
// the operator has reconciled the broken predecessor. Without this, the
// daemon would replay later events on top of a stale parent and produce a
// tree that diverges from the operator's intent.
func TestReplay_BatchHaltsOnBlockedConflict(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	// Event 1: a clean create that will publish.
	if err := os.WriteFile(filepath.Join(f.dir, "ok.txt"), []byte("ok\n"), 0o644); err != nil {
		t.Fatalf("write ok: %v", err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("Capture ok: %v", err)
	}

	// Event 2: a hand-crafted blocker — modify a non-existent path.
	blockerEv := state.CaptureEvent{
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		BaseHead:         f.cctx.BaseHead,
		Operation:        "modify",
		Path:             "ghost.txt",
		Fidelity:         "rescan",
	}
	blockerOp := state.CaptureOp{
		Op:         "modify",
		Path:       "ghost.txt",
		BeforeOID:  sql.NullString{String: "1111111111111111111111111111111111111111", Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: "2222222222222222222222222222222222222222", Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
		Fidelity:   "rescan",
	}
	blockerSeq, err := state.AppendCaptureEvent(ctx, f.db, blockerEv, []state.CaptureOp{blockerOp})
	if err != nil {
		t.Fatalf("AppendCaptureEvent blocker: %v", err)
	}

	// Event 3: another clean create captured AFTER the blocker. It should
	// remain pending — the batch halts on event 2.
	if err := os.WriteFile(filepath.Join(f.dir, "after.txt"), []byte("after\n"), 0o644); err != nil {
		t.Fatalf("write after: %v", err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("Capture after: %v", err)
	}

	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		MessageFn: DeterministicMessage,
		GitDir:    f.gitDir,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if sum.Conflicts != 1 {
		t.Fatalf("Conflicts=%d want 1 (sum=%+v)", sum.Conflicts, sum)
	}
	if sum.Published < 1 {
		t.Fatalf("Published=%d want >=1 (event 1 should land before the blocker)", sum.Published)
	}
	var publishedHead sql.NullString
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT commit_oid FROM capture_events WHERE path = 'ok.txt' AND state = ?`,
		state.EventStatePublished).Scan(&publishedHead); err != nil {
		t.Fatalf("query ok.txt commit: %v", err)
	}
	if !publishedHead.Valid || publishedHead.String == "" {
		t.Fatalf("ok.txt published without commit oid")
	}
	pub, ok, err := state.LoadPublishState(ctx, f.db)
	if err != nil {
		t.Fatalf("LoadPublishState: %v", err)
	}
	if !ok {
		t.Fatalf("publish_state row not written")
	}
	if !pub.SourceHead.Valid || pub.SourceHead.String != publishedHead.String {
		t.Fatalf("publish_state.source_head=%v want first published head %s",
			pub.SourceHead, publishedHead.String)
	}

	// `after.txt`'s event must be held behind the terminal blocker; the blocker
	// itself must not re-enter the pending queue.
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil {
		t.Fatalf("PendingEvents: %v", err)
	}
	var sawBlocker, sawAfter bool
	for _, p := range pending {
		if p.Seq == blockerSeq {
			sawBlocker = true
		}
		if p.Path == "after.txt" {
			sawAfter = true
		}
	}
	if sawBlocker {
		t.Fatalf("blocker seq=%d should NOT be pending after settle", blockerSeq)
	}
	if sawAfter {
		t.Fatalf("after.txt should be held behind blocked predecessor; pending=%+v", pending)
	}

	// A second pass with the blocker still in place must NOT drain `after.txt`.
	sum2, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		MessageFn: DeterministicMessage,
		GitDir:    f.gitDir,
	})
	if err != nil {
		t.Fatalf("Replay second pass: %v", err)
	}
	if sum2.Published != 0 || sum2.Conflicts != 0 || sum2.Failed != 0 {
		t.Fatalf("second pass should be held by seq barrier; sum=%+v", sum2)
	}
	pending2, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil {
		t.Fatalf("PendingEvents 2: %v", err)
	}
	for _, p := range pending2 {
		if p.Seq == blockerSeq {
			t.Fatalf("blocker re-entered pending on second pass: %+v", p)
		}
		if p.Path == "after.txt" {
			t.Fatalf("after.txt re-entered pending while blocker remains: %+v", p)
		}
	}
}

// TestReplay_ModifyChain_OrderedReplay regression-tests the scratch-index
// refactor: when four captured blob states A→B→C→D are queued for the same
// path as three sequential `modify` events, replay must commit them in
// order even when the live worktree (and live index) have moved past A.
//
// Pre-fix this failed with "modify before-state mismatch" because the
// conflict probe consulted the live repo index, which was empty for
// chain.txt — the daemon never `git add`s captured blobs. The fix seeds an
// isolated GIT_INDEX_FILE from BaseHead and advances it per event, so each
// event's before-state matches the prior event's after-state.
func TestReplay_ModifyChain_OrderedReplay(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	// Hash four blob states A, B, C, D.
	a, err := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	if err != nil {
		t.Fatalf("hash A: %v", err)
	}
	b, err := git.HashObjectStdin(ctx, f.dir, []byte("B\n"))
	if err != nil {
		t.Fatalf("hash B: %v", err)
	}
	c, err := git.HashObjectStdin(ctx, f.dir, []byte("C\n"))
	if err != nil {
		t.Fatalf("hash C: %v", err)
	}
	d, err := git.HashObjectStdin(ctx, f.dir, []byte("D\n"))
	if err != nil {
		t.Fatalf("hash D: %v", err)
	}

	// Seed BaseHead with chain.txt=A. The fixture's seed commit only
	// carried .gitignore; rewrite HEAD to a tree that also pins chain.txt
	// to blob A so the scratch index sees it as the chain's prior state.
	gitignoreBlob, err := git.HashObjectStdin(ctx, f.dir, []byte("ignored.txt\n"))
	if err != nil {
		t.Fatalf("hash gitignore: %v", err)
	}
	tree, err := git.Mktree(ctx, f.dir, []git.MktreeEntry{
		{Mode: git.RegularFileMode, Type: "blob", OID: gitignoreBlob, Path: ".gitignore"},
		{Mode: git.RegularFileMode, Type: "blob", OID: a, Path: "chain.txt"},
	})
	if err != nil {
		t.Fatalf("mktree: %v", err)
	}
	commit, err := git.CommitTree(ctx, f.dir, tree, "seed: chain.txt=A")
	if err != nil {
		t.Fatalf("commit-tree: %v", err)
	}
	// Move main onto the new commit. Use empty-old to bypass CAS — the
	// fixture is single-threaded and the prior tip is irrelevant for the
	// regression.
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, commit, ""); err != nil {
		t.Fatalf("update-ref: %v", err)
	}
	f.cctx.BaseHead = commit

	// Live worktree drifts ahead to D — this is the situation the old
	// live-index probe could not handle.
	if err := os.WriteFile(filepath.Join(f.dir, "chain.txt"), []byte("D\n"), 0o644); err != nil {
		t.Fatalf("write chain.txt: %v", err)
	}

	// Queue three modify events forming the chain A→B, B→C, C→D.
	chain := []struct{ before, after string }{
		{a, b},
		{b, c},
		{c, d},
	}
	for _, step := range chain {
		ev := state.CaptureEvent{
			BranchRef:        f.cctx.BranchRef,
			BranchGeneration: f.cctx.BranchGeneration,
			BaseHead:         f.cctx.BaseHead,
			Operation:        "modify",
			Path:             "chain.txt",
			Fidelity:         "rescan",
		}
		op := state.CaptureOp{
			Op:         "modify",
			Path:       "chain.txt",
			BeforeOID:  sql.NullString{String: step.before, Valid: true},
			BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
			AfterOID:   sql.NullString{String: step.after, Valid: true},
			AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
			Fidelity:   "rescan",
		}
		if _, err := state.AppendCaptureEvent(ctx, f.db, ev, []state.CaptureOp{op}); err != nil {
			t.Fatalf("AppendCaptureEvent: %v", err)
		}
	}

	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		MessageFn: DeterministicMessage,
		GitDir:    f.gitDir,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if sum.Published != 3 {
		t.Fatalf("Published=%d want 3 (sum=%+v)", sum.Published, sum)
	}
	if sum.Conflicts != 0 || sum.Failed != 0 {
		t.Fatalf("unexpected conflicts/failed: %+v", sum)
	}

	// Walk the resulting log and assert chain.txt's blob progresses
	// A → B → C → D commit-by-commit. log --reverse so [0] is the seed.
	out, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "log", "--reverse", "--format=%H", f.cctx.BranchRef)
	if err != nil {
		t.Fatalf("git log: %v", err)
	}
	hashes := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(hashes) != 4 {
		t.Fatalf("expected 4 commits (seed+3), got %d:\n%s", len(hashes), out)
	}
	wantBlobs := []string{a, b, c, d}
	for i, h := range hashes {
		entries, err := git.LsTree(ctx, f.dir, h, false, "chain.txt")
		if err != nil {
			t.Fatalf("ls-tree %s: %v", h, err)
		}
		if len(entries) != 1 {
			t.Fatalf("commit %d (%s) chain.txt missing: %+v", i, h, entries)
		}
		if entries[0].OID != wantBlobs[i] {
			t.Fatalf("commit %d (%s) chain.txt blob=%s want %s", i, h, entries[0].OID, wantBlobs[i])
		}
	}
}

func TestReplay_DefaultIndexIsPerPassTempfile(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	if err := os.WriteFile(filepath.Join(f.dir, "temp-index.txt"), []byte("ok\n"), 0o644); err != nil {
		t.Fatalf("write temp-index.txt: %v", err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("Capture: %v", err)
	}

	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		MessageFn: DeterministicMessage,
		GitDir:    f.gitDir,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if sum.Published == 0 {
		t.Fatalf("Published=0 want >0")
	}
	if _, err := os.Stat(filepath.Join(f.gitDir, "acd", "replay.index")); !os.IsNotExist(err) {
		t.Fatalf("fixed replay.index exists or stat failed unexpectedly: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(f.gitDir, "acd", "replay-*.index"))
	if err != nil {
		t.Fatalf("glob temp indexes: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("temp replay indexes were not cleaned up: %v", matches)
	}
}

// TestReplay_StaleGenerationBlocked: an event captured under a prior
// branch_generation must be terminally blocked when the daemon's active
// generation has bumped. The error message must mention the generation
// mismatch so operators can spot the cause.
func TestReplay_StaleGenerationBlocked(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	// Stage one well-formed event captured at generation 1 against the
	// fixture's seed BaseHead. The event itself is otherwise valid — the
	// guard fires before validation even runs.
	stale := state.CaptureEvent{
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: 1,
		BaseHead:         f.cctx.BaseHead,
		Operation:        "create",
		Path:             "stale.txt",
		Fidelity:         "rescan",
	}
	staleOp := state.CaptureOp{
		Op:        "create",
		Path:      "stale.txt",
		AfterOID:  sql.NullString{String: "3333333333333333333333333333333333333333", Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		Fidelity:  "rescan",
	}
	seq, err := state.AppendCaptureEvent(ctx, f.db, stale, []state.CaptureOp{staleOp})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}

	// Daemon now operates at generation 2 — i.e. the branch was
	// rebased/reset since the event was captured.
	cctx := f.cctx
	cctx.BranchGeneration = 2

	sum, err := Replay(ctx, f.dir, f.db, cctx, ReplayOpts{
		MessageFn: DeterministicMessage,
		GitDir:    f.gitDir,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if sum.Conflicts != 1 {
		t.Fatalf("Conflicts=%d want 1 (sum=%+v)", sum.Conflicts, sum)
	}
	if sum.Published != 0 {
		t.Fatalf("Published=%d want 0", sum.Published)
	}

	// The blocked event must drop out of pending and land in
	// blocked_conflict.
	blocked, err := state.CountEventsByState(ctx, f.db, state.EventStateBlockedConflict)
	if err != nil {
		t.Fatalf("CountEventsByState: %v", err)
	}
	if blocked != 1 {
		t.Fatalf("blocked_conflict count = %d, want 1", blocked)
	}

	// publish_state.error must mention "generation" so operators can spot
	// the cause without parsing daemon_meta.
	pub, ok, err := state.LoadPublishState(ctx, f.db)
	if err != nil {
		t.Fatalf("LoadPublishState: %v", err)
	}
	if !ok {
		t.Fatalf("publish_state row not written")
	}
	if !pub.EventSeq.Valid || pub.EventSeq.Int64 != seq {
		t.Fatalf("publish_state.event_seq=%v want %d", pub.EventSeq, seq)
	}
	if !pub.Error.Valid || !strings.Contains(pub.Error.String, "generation") {
		t.Fatalf("publish_state.error=%q want contains 'generation'", pub.Error.String)
	}
}

// TestReplay_StaleAncestryBlocked: even when generations agree (e.g. a
// daemon restart missed the bump), a queued event whose BaseHead is no
// longer reachable from the replay parent must be terminally blocked.
// Replaying it would chain a commit off a stale parent and produce a tree
// that diverges from the operator's intent.
func TestReplay_StaleAncestryBlocked(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	// Build a branch from seed: seed -> alt (a sibling that is NOT an
	// ancestor of f.cctx.BaseHead). We rewrite BranchRef onto a fresh
	// commit from a different tree, so the prior BaseHead is no longer
	// reachable from HEAD.
	altTree, err := git.Mktree(ctx, f.dir, []git.MktreeEntry{
		{Mode: git.RegularFileMode, Type: "blob",
			OID: "0000000000000000000000000000000000000000", Path: "_skip"},
	})
	// Mktree may reject a zero blob OID; the test only needs a sibling
	// commit, so fall back to a real blob if so.
	if err != nil {
		blob, hErr := git.HashObjectStdin(ctx, f.dir, []byte("alt\n"))
		if hErr != nil {
			t.Fatalf("hash alt blob: %v", hErr)
		}
		altTree, err = git.Mktree(ctx, f.dir, []git.MktreeEntry{
			{Mode: git.RegularFileMode, Type: "blob", OID: blob, Path: "alt.txt"},
		})
		if err != nil {
			t.Fatalf("mktree alt: %v", err)
		}
	}
	altCommit, err := git.CommitTree(ctx, f.dir, altTree, "alt root")
	if err != nil {
		t.Fatalf("commit-tree alt: %v", err)
	}

	// Stage an event captured against the original BaseHead.
	stale := state.CaptureEvent{
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: 7, // both sides agree → exercises ancestry leg
		BaseHead:         f.cctx.BaseHead,
		Operation:        "create",
		Path:             "ancestry.txt",
		Fidelity:         "rescan",
	}
	op := state.CaptureOp{
		Op:        "create",
		Path:      "ancestry.txt",
		AfterOID:  sql.NullString{String: "4444444444444444444444444444444444444444", Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		Fidelity:  "rescan",
	}
	if _, err := state.AppendCaptureEvent(ctx, f.db, stale, []state.CaptureOp{op}); err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}

	// Daemon now sits on altCommit (an unrelated history) at the same
	// generation 7.
	cctx := f.cctx
	cctx.BaseHead = altCommit
	cctx.BranchGeneration = 7

	sum, err := Replay(ctx, f.dir, f.db, cctx, ReplayOpts{
		MessageFn: DeterministicMessage,
		GitDir:    f.gitDir,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if sum.Conflicts != 1 {
		t.Fatalf("Conflicts=%d want 1 (sum=%+v)", sum.Conflicts, sum)
	}
	if sum.Published != 0 {
		t.Fatalf("Published=%d want 0", sum.Published)
	}

	// Reason should mention "ancestor" or "ancestry" so operators can
	// distinguish this from a generation mismatch.
	pub, ok, err := state.LoadPublishState(ctx, f.db)
	if err != nil {
		t.Fatalf("LoadPublishState: %v", err)
	}
	if !ok {
		t.Fatalf("publish_state row not written")
	}
	if !pub.Error.Valid {
		t.Fatalf("publish_state.error empty; want ancestry message")
	}
	got := pub.Error.String
	if !strings.Contains(got, "ancestor") && !strings.Contains(got, "ancestry") {
		t.Fatalf("publish_state.error=%q want contains 'ancestor' or 'ancestry'", got)
	}
}

// TestReplay_MatchingGeneration_Publishes: a sanity-check counterpart to
// the stale tests above — when generation + ancestry agree, the guard is
// transparent and the event publishes normally.
func TestReplay_MatchingGeneration_Publishes(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	if err := os.WriteFile(filepath.Join(f.dir, "ok.txt"), []byte("ok\n"), 0o644); err != nil {
		t.Fatalf("write ok: %v", err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("Capture: %v", err)
	}

	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		MessageFn: DeterministicMessage,
		GitDir:    f.gitDir,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if sum.Published == 0 || sum.Conflicts != 0 {
		t.Fatalf("expected clean publish, got %+v", sum)
	}
}

// TestDeterministicMessage_Format: subject lines for each op kind plus a
// multi-op event match the legacy format.
func TestDeterministicMessage_Format(t *testing.T) {
	cases := []struct {
		name string
		ops  []state.CaptureOp
		want string
	}{
		{
			name: "add",
			ops:  []state.CaptureOp{{Op: "create", Path: "src/foo.go"}},
			want: "Add foo.go",
		},
		{
			name: "update",
			ops:  []state.CaptureOp{{Op: "modify", Path: "src/foo.go"}},
			want: "Update foo.go",
		},
		{
			name: "delete",
			ops:  []state.CaptureOp{{Op: "delete", Path: "src/foo.go"}},
			want: "Remove foo.go",
		},
		{
			name: "rename",
			ops: []state.CaptureOp{{
				Op:      "rename",
				Path:    "src/bar.go",
				OldPath: sql.NullString{String: "src/foo.go", Valid: true},
			}},
			want: "Rename foo.go to bar.go",
		},
		{
			name: "multi-shared-dir",
			ops: []state.CaptureOp{
				{Op: "modify", Path: "src/a.go"},
				{Op: "modify", Path: "src/b.go"},
			},
			want: "Update 2 files in src",
		},
		{
			name: "multi-disjoint",
			ops: []state.CaptureOp{
				{Op: "create", Path: "a/foo.go"},
				{Op: "create", Path: "b/bar.go"},
			},
			want: "Update 2 files",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg, err := DeterministicMessage(context.Background(), EventContext{
				Event: state.CaptureEvent{Seq: 1, BranchRef: "refs/heads/main"},
				Ops:   tc.ops,
			})
			if err != nil {
				t.Fatalf("DeterministicMessage: %v", err)
			}
			subject := strings.SplitN(msg, "\n", 2)[0]
			if subject != tc.want {
				t.Fatalf("subject=%q want %q (full=%q)", subject, tc.want, msg)
			}
		})
	}
}

func restoreReplayRefSeams(t *testing.T) {
	t.Helper()
	origUpdateRef := replayUpdateRef
	origSleep := replayUpdateRefSleep
	t.Cleanup(func() {
		replayUpdateRef = origUpdateRef
		replayUpdateRefSleep = origSleep
	})
}

type memoryTraceLogger struct {
	mu     sync.Mutex
	events []acdtrace.Event
}

func (l *memoryTraceLogger) Record(ev acdtrace.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, ev)
}

func (l *memoryTraceLogger) Close() error { return nil }

func (l *memoryTraceLogger) Dropped() uint64 { return 0 }

func (l *memoryTraceLogger) Events() []acdtrace.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]acdtrace.Event, len(l.events))
	copy(out, l.events)
	return out
}

func traceEventsByClass(events []acdtrace.Event, class string) []acdtrace.Event {
	var matches []acdtrace.Event
	for _, ev := range events {
		if ev.EventClass == class {
			matches = append(matches, ev)
		}
	}
	return matches
}

func traceHasClass(events []acdtrace.Event, class string) bool {
	for _, ev := range events {
		if ev.EventClass == class {
			return true
		}
	}
	return false
}

func traceHasDecision(events []acdtrace.Event, decision string) bool {
	for _, ev := range events {
		if ev.Decision == decision {
			return true
		}
	}
	return false
}
