package daemon

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
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

// TestReplay_Conflict: when the live index diverges from the event's
// before-state, replay must mark publish_state=conflict and leave the event
// pending.
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

	// Event must remain pending; publish_state.status must be conflict.
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil {
		t.Fatalf("PendingEvents: %v", err)
	}
	var stillPending bool
	for _, p := range pending {
		if p.Seq == seq {
			stillPending = true
		}
	}
	if !stillPending {
		t.Fatalf("conflicted event seq=%d should still be pending", seq)
	}

	pub, ok, err := state.LoadPublishState(ctx, f.db)
	if err != nil {
		t.Fatalf("LoadPublishState: %v", err)
	}
	if !ok {
		t.Fatalf("publish_state row not written")
	}
	if pub.Status != "conflict" {
		t.Fatalf("publish_state.status=%q want conflict", pub.Status)
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
