package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestApplyRecoveryOpsInMemoryAcceptsIdempotentDuplicateTransitions(t *testing.T) {
	entry := func(path, oid string) git.IndexEntry {
		return git.IndexEntry{Path: path, Mode: git.RegularFileMode, OID: oid}
	}
	null := func(value string) sql.NullString { return sql.NullString{String: value, Valid: true} }

	index := map[string]git.IndexEntry{
		"modified.txt": entry("modified.txt", "after-modify"),
		"created.txt":  entry("created.txt", "after-create"),
		"renamed.txt":  entry("renamed.txt", "after-rename"),
	}
	ops := []state.CaptureOp{
		{Op: "modify", Path: "modified.txt", BeforeOID: null("before-modify"), BeforeMode: null(git.RegularFileMode), AfterOID: null("after-modify"), AfterMode: null(git.RegularFileMode)},
		{Op: "create", Path: "created.txt", AfterOID: null("after-create"), AfterMode: null(git.RegularFileMode)},
		{Op: "delete", Path: "deleted.txt", BeforeOID: null("before-delete"), BeforeMode: null(git.RegularFileMode)},
		{Op: "rename", Path: "renamed.txt", OldPath: null("old.txt"), BeforeOID: null("before-rename"), BeforeMode: null(git.RegularFileMode), AfterOID: null("after-rename"), AfterMode: null(git.RegularFileMode)},
	}
	if conflict := applyRecoveryOpsInMemory(index, ops); conflict != "" {
		t.Fatalf("idempotent duplicate transitions conflicted: %s", conflict)
	}

	conflicting := []state.CaptureOp{{
		Op: "modify", Path: "modified.txt",
		BeforeOID: null("different-before"), BeforeMode: null(git.RegularFileMode),
		AfterOID: null("different-after"), AfterMode: null(git.RegularFileMode),
	}}
	if conflict := applyRecoveryOpsInMemory(index, conflicting); !strings.Contains(conflict, "before-state mismatch") {
		t.Fatalf("conflicting transition=%q want before-state mismatch", conflict)
	}
}

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

func assertRecoverySnapshot(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	seq int64,
	commitOID string,
	baseHead string,
	wantBlob string,
) string {
	t.Helper()
	var ref string
	if err := f.db.SQL().QueryRowContext(ctx, `
SELECT recovery_ref
FROM recovery_snapshots
WHERE first_event_seq = ? AND last_event_seq = ?`, seq, seq).Scan(&ref); err != nil {
		t.Fatalf("query recovery snapshot: %v", err)
	}
	if !strings.HasPrefix(ref, git.RecoveryRefPrefix) {
		t.Fatalf("recovery ref=%q want prefix %q", ref, git.RecoveryRefPrefix)
	}
	resolved, err := git.RevParse(ctx, f.dir, ref)
	if err != nil {
		t.Fatalf("resolve recovery ref: %v", err)
	}
	if resolved != commitOID {
		t.Fatalf("recovery ref commit=%s want %s", resolved, commitOID)
	}
	parent, err := git.RevParse(ctx, f.dir, ref+"^")
	if err != nil {
		t.Fatalf("resolve recovery parent: %v", err)
	}
	if parent != baseHead {
		t.Fatalf("recovery parent=%s want immutable base %s", parent, baseHead)
	}
	gotBlob, err := git.LsTreeBlobOID(ctx, f.dir, ref, "doc.md")
	if err != nil {
		t.Fatalf("read recovery tree: %v", err)
	}
	if gotBlob != wantBlob {
		t.Fatalf("recovery doc.md blob=%s want %s", gotBlob, wantBlob)
	}
	assertReplayDecision(t, ctx, f.db, seq,
		state.DecisionKindRecoveryArchived, "replay_terminal_barrier")
	return ref
}

func appendRecoveryEvent(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	baseHead string,
	op state.CaptureOp,
) int64 {
	t.Helper()
	if op.Fidelity == "" {
		op.Fidelity = "rescan"
	}
	ev := state.CaptureEvent{
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		BaseHead:         baseHead,
		Operation:        op.Op,
		Path:             op.Path,
		OldPath:          op.OldPath,
		Fidelity:         op.Fidelity,
	}
	seq, err := state.AppendCaptureEvent(ctx, f.db, ev, []state.CaptureOp{op})
	if err != nil {
		t.Fatalf("AppendCaptureEvent %s %s: %v", op.Op, op.Path, err)
	}
	return seq
}

func markRecoveryBarrier(t *testing.T, ctx context.Context, f *captureFixture, seq int64, baseHead, message string) {
	t.Helper()
	if err := state.MarkEventBlocked(ctx, f.db, seq, message, 1.0,
		sql.NullString{String: f.cctx.BranchRef, Valid: true},
		sql.NullInt64{Int64: f.cctx.BranchGeneration, Valid: true},
		sql.NullString{String: baseHead, Valid: true},
	); err != nil {
		t.Fatalf("MarkEventBlocked seq=%d: %v", seq, err)
	}
}

func commitTreeWithIndexUpdates(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	baseHead string,
	message string,
	lines ...string,
) string {
	t.Helper()
	indexFile := filepath.Join(t.TempDir(), "idx")
	if err := git.ReadTree(ctx, f.dir, indexFile, baseHead); err != nil {
		t.Fatalf("ReadTree %s: %v", baseHead, err)
	}
	if err := git.UpdateIndexInfo(ctx, f.dir, indexFile, lines); err != nil {
		t.Fatalf("UpdateIndexInfo: %v", err)
	}
	tree, err := git.WriteTree(ctx, f.dir, indexFile)
	if err != nil {
		t.Fatalf("WriteTree: %v", err)
	}
	commit, err := git.CommitTree(ctx, f.dir, tree, message, baseHead)
	if err != nil {
		t.Fatalf("CommitTree: %v", err)
	}
	return commit
}

func countRecoverySnapshots(t *testing.T, ctx context.Context, db *state.DB) int {
	t.Helper()
	var count int
	if err := db.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM recovery_snapshots`).Scan(&count); err != nil {
		t.Fatalf("count recovery snapshots: %v", err)
	}
	return count
}

func recoveryRefs(t *testing.T, ctx context.Context, repoDir string) []string {
	t.Helper()
	out, err := git.Run(ctx, git.RunOpts{Dir: repoDir},
		"for-each-ref", "--format=%(refname)", git.RecoveryRefPrefix)
	if err != nil {
		t.Fatalf("list recovery refs: %v", err)
	}
	return strings.Fields(string(out))
}

func gitRawOutput(t *testing.T, ctx context.Context, repoDir string, args ...string) string {
	t.Helper()
	out, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, args...)
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return string(out)
}

func TestReconcile_LargeChainHonorsCancellation(t *testing.T) {
	chain := make([]state.RecoveryChainEvent, 20_000)
	for i := range chain {
		chain[i].Event.Seq = int64(i + 1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	err := validateRecoveryObjects(ctx, t.TempDir(), chain)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("validateRecoveryObjects err=%v want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("canceled large chain took %s", elapsed)
	}
}

func TestProveUnpublishedChainIsReadOnly(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	before, err := git.HashObjectStdin(ctx, f.dir, []byte("before\n"))
	if err != nil {
		t.Fatalf("hash before: %v", err)
	}
	after, err := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
	if err != nil {
		t.Fatalf("hash after: %v", err)
	}
	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", before, "base")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}
	seq, _, _ := seedBlockedModify(t, ctx, f, "doc.md", before, after, base)

	readOnlyDB, err := state.OpenReadOnly(ctx, f.db.Path())
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer readOnlyDB.Close()
	proof, err := ProveUnpublishedChain(ctx, f.dir, readOnlyDB, RecoveryReconcileOptions{
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         seq,
	})
	if err != nil {
		t.Fatalf("ProveUnpublishedChain: %v", err)
	}
	if !proof.Handled || proof.Outcome != state.EventStateRecovered ||
		proof.FirstSeq != seq || proof.LastSeq != seq || proof.EventCount != 1 {
		t.Fatalf("proof=%+v want one recovered event", proof)
	}
	gotState, oid := readEventState(t, ctx, f.db, seq)
	if gotState != state.EventStateBlockedConflict || oid.Valid {
		t.Fatalf("proof changed event state=%q oid=%v", gotState, oid)
	}
	if count := countRecoverySnapshots(t, ctx, f.db); count != 0 {
		t.Fatalf("proof created %d recovery snapshots", count)
	}
	if refs := recoveryRefs(t, ctx, f.dir); len(refs) != 0 {
		t.Fatalf("proof created recovery refs: %v", refs)
	}
	if _, err := readOnlyDB.SQL().ExecContext(ctx,
		`UPDATE capture_events SET state = 'published' WHERE seq = ?`, seq); err == nil {
		t.Fatal("read-only state handle accepted a write")
	}
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
	for _, key := range replayErrorMetaTestKeys() {
		if err := state.MetaSet(ctx, f.db, key, "stale"); err != nil {
			t.Fatalf("seed replay metadata %s: %v", key, err)
		}
	}

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
	for _, key := range replayErrorMetaTestKeys() {
		v, ok, err := state.MetaGet(ctx, f.db, key)
		if err != nil {
			t.Fatalf("MetaGet %s: %v", key, err)
		}
		if ok {
			t.Fatalf("meta %s still present after self-heal: %q", key, v)
		}
	}
}

// TestReplay_ReconcilesExactCreateChain keeps the legacy self-heal predicate
// narrow while proving the whole-chain reconciler can safely settle an exact
// create at HEAD.
func TestReplay_ReconcilesExactCreateChain(t *testing.T) {
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
	if st != state.EventStatePublished {
		t.Fatalf("state=%q want published", st)
	}
	assertReplayDecision(t, ctx, f.db, seq,
		state.DecisionKindRecoveryPublished, "replay_terminal_barrier")
}

// TestReplay_ArchivesWhenHeadBlobDiffers proves a non-matching but completely
// materializable chain is protected by a hidden ref before its barrier clears.
func TestReplay_ArchivesWhenHeadBlobDiffers(t *testing.T) {
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

	st, commitOID := readEventState(t, ctx, f.db, seq)
	if st != state.EventStateRecovered {
		t.Fatalf("state=%q want recovered", st)
	}
	assertRecoverySnapshot(t, ctx, f, seq, commitOID.String, base, afterBlob)
}

// TestReplay_ArchivesWhenAncestryFails guards external-publish proof: matching
// path content on an unrelated HEAD is coincidence, so the chain is archived
// rather than marked externally published.
func TestReplay_ArchivesWhenAncestryFails(t *testing.T) {
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

	st, commitOID := readEventState(t, ctx, f.db, seq)
	if st != state.EventStateRecovered {
		t.Fatalf("state=%q want recovered", st)
	}
	assertRecoverySnapshot(t, ctx, f, seq, commitOID.String, base, afterBlob)
}

func TestReconcile_PublishesLiteralFilenames(t *testing.T) {
	runBoundedParallel(t)
	for _, tc := range []struct {
		name           string
		capturedPath   string
		distractorPath string
	}{
		{name: "pathspec magic", capturedPath: ":(top)colon.txt", distractorPath: "colon.txt"},
		{name: "surrounding whitespace", capturedPath: " spaced.txt ", distractorPath: "spaced.txt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertReconcilePublishesLiteralFilename(t, tc.capturedPath, tc.distractorPath)
		})
	}
}

func assertReconcilePublishesLiteralFilename(t *testing.T, capturedPath, distractorPath string) {
	t.Helper()
	f := newCaptureFixture(t)
	ctx := context.Background()
	before, _ := git.HashObjectStdin(ctx, f.dir, []byte("before\n"))
	after, _ := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
	distractor, _ := git.HashObjectStdin(ctx, f.dir, []byte("distractor\n"))

	baseTree, err := git.Mktree(ctx, f.dir, []git.MktreeEntry{
		{Mode: git.RegularFileMode, Type: "blob", OID: before, Path: capturedPath},
		{Mode: git.RegularFileMode, Type: "blob", OID: distractor, Path: distractorPath},
	})
	if err != nil {
		t.Fatalf("Mktree base: %v", err)
	}
	base, err := git.CommitTree(ctx, f.dir, baseTree, "base with pathspec-magic filename", f.cctx.BaseHead)
	if err != nil {
		t.Fatalf("CommitTree base: %v", err)
	}
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}
	f.cctx.BaseHead = base

	seq := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: capturedPath,
		BeforeOID: sql.NullString{String: before, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: after, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	markRecoveryBarrier(t, ctx, f, seq, base, "modify before-state mismatch for "+capturedPath)

	externalTree, err := git.Mktree(ctx, f.dir, []git.MktreeEntry{
		{Mode: git.RegularFileMode, Type: "blob", OID: after, Path: capturedPath},
		{Mode: git.RegularFileMode, Type: "blob", OID: distractor, Path: distractorPath},
	})
	if err != nil {
		t.Fatalf("Mktree external: %v", err)
	}
	external, err := git.CommitTree(ctx, f.dir, externalTree, "publish pathspec-magic filename", base)
	if err != nil {
		t.Fatalf("CommitTree external: %v", err)
	}
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, external, base); err != nil {
		t.Fatalf("update external: %v", err)
	}

	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         seq,
		Trigger:          "test_literal_pathspec_recovery",
	})
	if err != nil {
		t.Fatalf("ReconcileUnpublishedChain: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStatePublished || result.CommitOID != external {
		t.Fatalf("result=%+v want published at %s", result, external)
	}
	gotState, commitOID := readEventState(t, ctx, f.db, seq)
	if gotState != state.EventStatePublished || !commitOID.Valid || commitOID.String != external {
		t.Fatalf("event state=%q commit=%v want published at %s", gotState, commitOID, external)
	}
	if resolved, err := git.RevParse(ctx, f.dir, result.RecoveryRef); err != nil || resolved != external {
		t.Fatalf("proof ref=%s err=%v want %s", resolved, err, external)
	}
}

func TestReplay_ReconcilesWholeSquashedChain(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	before, _ := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	middle, _ := git.HashObjectStdin(ctx, f.dir, []byte("B\n"))
	final, _ := git.HashObjectStdin(ctx, f.dir, []byte("C\n"))
	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", before, "seed A")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}

	seq1 := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID:  sql.NullString{String: before, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: middle, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	seq2 := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID:  sql.NullString{String: middle, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: final, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	markRecoveryBarrier(t, ctx, f, seq2, base, "modify before-state mismatch for doc.md")

	external := commitSingleFileTree(t, ctx, f.dir, "doc.md", final, "squash A to C", base)
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, external, base); err != nil {
		t.Fatalf("update external: %v", err)
	}
	cctx := f.cctx
	cctx.BaseHead = external
	if _, err := Replay(ctx, f.dir, f.db, cctx, ReplayOpts{GitDir: f.gitDir}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	for _, seq := range []int64{seq1, seq2} {
		gotState, oid := readEventState(t, ctx, f.db, seq)
		if gotState != state.EventStatePublished || !oid.Valid || oid.String != external {
			t.Fatalf("seq=%d state=%q oid=%v want published at %s", seq, gotState, oid, external)
		}
		assertReplayDecision(t, ctx, f.db, seq,
			state.DecisionKindRecoveryPublished, "replay_terminal_barrier")
	}
	var outcome string
	var eventCount int
	var recoveryRef sql.NullString
	if err := f.db.SQL().QueryRowContext(ctx, `
SELECT outcome, event_count, recovery_ref FROM recovery_snapshots`).Scan(
		&outcome, &eventCount, &recoveryRef); err != nil {
		t.Fatalf("query recovery snapshot: %v", err)
	}
	if outcome != state.EventStatePublished || eventCount != 2 || !recoveryRef.Valid {
		t.Fatalf("snapshot outcome=%q count=%d ref=%v", outcome, eventCount, recoveryRef)
	}
	if resolved, err := git.RevParse(ctx, f.dir, recoveryRef.String); err != nil || resolved != external {
		t.Fatalf("published proof ref target=%s err=%v want %s", resolved, err, external)
	}
	if refs := recoveryRefs(t, ctx, f.dir); len(refs) != 1 {
		t.Fatalf("published proof refs=%v want one", refs)
	}
}

func TestReplay_ReconcilesBarrierAfterAgedPublishedSameBasePrefix(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	a, _ := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	b, _ := git.HashObjectStdin(ctx, f.dir, []byte("B\n"))
	c, _ := git.HashObjectStdin(ctx, f.dir, []byte("C\n"))
	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", a, "base A")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}
	seq1 := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: a, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: b, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	commitB := commitSingleFileTree(t, ctx, f.dir, "doc.md", b, "publish B", base)
	if err := state.MarkEventPublished(ctx, f.db, seq1, state.EventStatePublished,
		sql.NullString{String: commitB, Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
		t.Fatalf("MarkEventPublished prefix: %v", err)
	}
	if _, err := state.AppendDecision(ctx, f.db, state.DecisionRecord{
		DecisionTS: 2, Kind: state.DecisionKindCommitted,
		EventSeq: sql.NullInt64{Int64: seq1, Valid: true},
	}); err != nil {
		t.Fatalf("AppendDecision prefix: %v", err)
	}
	seq2 := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: b, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: c, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	markRecoveryBarrier(t, ctx, f, seq2, base, "modify before-state mismatch for doc.md")
	if _, err := f.db.SQL().ExecContext(ctx,
		`UPDATE capture_events SET captured_ts = 1 WHERE seq = ?`, seq1); err != nil {
		t.Fatalf("age published prefix: %v", err)
	}
	if pruned, err := state.PrunePublishedEventsBefore(ctx, f.db, 100); err != nil {
		t.Fatalf("PrunePublishedEventsBefore: %v", err)
	} else if pruned != 0 {
		t.Fatalf("pruned=%d want aged materialization prefix retained", pruned)
	}
	commitC := commitSingleFileTree(t, ctx, f.dir, "doc.md", c, "publish C", commitB)
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, commitC, base); err != nil {
		t.Fatalf("update HEAD to C: %v", err)
	}
	cctx := f.cctx
	cctx.BaseHead = commitC
	if _, err := Replay(ctx, f.dir, f.db, cctx, ReplayOpts{GitDir: f.gitDir}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	gotState, oid := readEventState(t, ctx, f.db, seq2)
	if gotState != state.EventStatePublished || oid.String != commitC {
		t.Fatalf("suffix state=%q oid=%v want published at %s", gotState, oid, commitC)
	}
}

func TestReconcile_UsesInterleavedPublishedContext(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	anchorBefore, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor before\n"))
	anchorAfter, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor after\n"))
	a, _ := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	b, _ := git.HashObjectStdin(ctx, f.dir, []byte("B\n"))
	c, _ := git.HashObjectStdin(ctx, f.dir, []byte("C\n"))
	base := commitTreeWithIndexUpdates(t, ctx, f, f.cctx.BaseHead, "base",
		git.RegularFileMode+" "+anchorBefore+"\tanchor.txt",
		git.RegularFileMode+" "+a+"\tdoc.md")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}
	first := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "anchor.txt",
		BeforeOID: sql.NullString{String: anchorBefore, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: anchorAfter, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	published := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: a, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: b, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	commitB := commitTreeWithIndexUpdates(t, ctx, f, base, "publish B",
		git.RegularFileMode+" "+b+"\tdoc.md")
	if err := state.MarkEventPublished(ctx, f.db, published, state.EventStatePublished,
		sql.NullString{String: commitB, Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
		t.Fatalf("MarkEventPublished interleaved context: %v", err)
	}
	if _, err := state.AppendDecision(ctx, f.db, state.DecisionRecord{
		DecisionTS: 2, Kind: state.DecisionKindCommitted,
		EventSeq: sql.NullInt64{Int64: published, Valid: true},
	}); err != nil {
		t.Fatalf("AppendDecision interleaved context: %v", err)
	}
	last := appendRecoveryEvent(t, ctx, f, commitB, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: b, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: c, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, commitB, base); err != nil {
		t.Fatalf("update HEAD to published context: %v", err)
	}

	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         first,
		Trigger:          "test_interleaved_published_context",
		ArchiveOnly:      true,
	})
	if err != nil {
		t.Fatalf("ReconcileUnpublishedChain: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered || result.EventCount != 2 ||
		result.FirstSeq != first || result.LastSeq != last {
		t.Fatalf("result=%+v want two-event recovered suffix", result)
	}
	if gotState, oid := readEventState(t, ctx, f.db, published); gotState != state.EventStatePublished ||
		!oid.Valid || oid.String != commitB {
		t.Fatalf("published context state=%q oid=%v want unchanged at %s", gotState, oid, commitB)
	}
	for path, want := range map[string]string{"anchor.txt": anchorAfter, "doc.md": c} {
		got, err := git.LsTreeBlobOID(ctx, f.dir, result.RecoveryRef, path)
		if err != nil || got != want {
			t.Fatalf("recovery %s blob=%s err=%v want %s", path, got, err, want)
		}
	}
}

func TestReconcile_UsesPublishedPrefixAcrossAdvancedBase(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	anchorBefore, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor before\n"))
	anchorAfter, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor after\n"))
	z, _ := git.HashObjectStdin(ctx, f.dir, []byte("Z\n"))
	a, _ := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	b, _ := git.HashObjectStdin(ctx, f.dir, []byte("B\n"))
	c, _ := git.HashObjectStdin(ctx, f.dir, []byte("C\n"))
	unrelated, _ := git.HashObjectStdin(ctx, f.dir, []byte("unrelated\n"))
	initialBase := commitTreeWithIndexUpdates(t, ctx, f, f.cctx.BaseHead, "initial base",
		git.RegularFileMode+" "+anchorBefore+"\tanchor.txt",
		git.RegularFileMode+" "+z+"\tdoc.md")
	represented := appendRecoveryEvent(t, ctx, f, initialBase, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: z, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: a, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	oldBase := commitTreeWithIndexUpdates(t, ctx, f, initialBase, "old base",
		git.RegularFileMode+" "+a+"\tdoc.md")
	if err := state.MarkEventPublished(ctx, f.db, represented, state.EventStatePublished,
		sql.NullString{String: oldBase, Valid: true}, sql.NullString{}, sql.NullString{}, 1); err != nil {
		t.Fatalf("MarkEventPublished represented prefix: %v", err)
	}
	published := appendRecoveryEvent(t, ctx, f, oldBase, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: a, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: b, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	firstBase := commitTreeWithIndexUpdates(t, ctx, f, oldBase, "advance unrelated path",
		git.RegularFileMode+" "+unrelated+"\tunrelated.txt")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, firstBase, ""); err != nil {
		t.Fatalf("update first base: %v", err)
	}
	first := appendRecoveryEvent(t, ctx, f, firstBase, state.CaptureOp{
		Op: "modify", Path: "anchor.txt",
		BeforeOID: sql.NullString{String: anchorBefore, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: anchorAfter, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	commitB := commitTreeWithIndexUpdates(t, ctx, f, firstBase, "publish B",
		git.RegularFileMode+" "+b+"\tdoc.md")
	if err := state.MarkEventPublished(ctx, f.db, published, state.EventStatePublished,
		sql.NullString{String: commitB, Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
		t.Fatalf("MarkEventPublished advanced-base prefix: %v", err)
	}
	if _, err := state.AppendDecision(ctx, f.db, state.DecisionRecord{
		DecisionTS: 2, Kind: state.DecisionKindCommitted,
		EventSeq: sql.NullInt64{Int64: published, Valid: true},
	}); err != nil {
		t.Fatalf("AppendDecision advanced-base prefix: %v", err)
	}
	last := appendRecoveryEvent(t, ctx, f, commitB, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: b, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: c, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, commitB, firstBase); err != nil {
		t.Fatalf("update HEAD to published prefix: %v", err)
	}

	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         first,
		Trigger:          "test_advanced_base_published_prefix",
		ArchiveOnly:      true,
	})
	if err != nil {
		t.Fatalf("ReconcileUnpublishedChain: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered || result.EventCount != 2 ||
		result.FirstSeq != first || result.LastSeq != last {
		t.Fatalf("result=%+v want two-event recovered suffix", result)
	}
	for seq, wantOID := range map[int64]string{represented: oldBase, published: commitB} {
		if gotState, oid := readEventState(t, ctx, f.db, seq); gotState != state.EventStatePublished ||
			!oid.Valid || oid.String != wantOID {
			t.Fatalf("published prefix seq=%d state=%q oid=%v want unchanged at %s",
				seq, gotState, oid, wantOID)
		}
	}
	for path, want := range map[string]string{"anchor.txt": anchorAfter, "doc.md": c} {
		got, err := git.LsTreeBlobOID(ctx, f.dir, result.RecoveryRef, path)
		if err != nil || got != want {
			t.Fatalf("recovery %s blob=%s err=%v want %s", path, got, err, want)
		}
	}
}

func TestReconcile_UsesTransitiveRenameContext(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	anchorBefore, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor before\n"))
	anchorAfter, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor after\n"))
	a, _ := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	d, _ := git.HashObjectStdin(ctx, f.dir, []byte("D\n"))
	unrelated, _ := git.HashObjectStdin(ctx, f.dir, []byte("unrelated\n"))
	initialBase := commitTreeWithIndexUpdates(t, ctx, f, f.cctx.BaseHead, "initial base",
		git.RegularFileMode+" "+anchorBefore+"\tanchor.txt",
		git.RegularFileMode+" "+a+"\ta.txt")
	firstRename := appendRecoveryEvent(t, ctx, f, initialBase, state.CaptureOp{
		Op: "rename", Path: "b.txt", OldPath: sql.NullString{String: "a.txt", Valid: true},
		BeforeOID: sql.NullString{String: a, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: a, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	secondRename := appendRecoveryEvent(t, ctx, f, initialBase, state.CaptureOp{
		Op: "rename", Path: "c.txt", OldPath: sql.NullString{String: "b.txt", Valid: true},
		BeforeOID: sql.NullString{String: a, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: a, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	firstBase := commitTreeWithIndexUpdates(t, ctx, f, initialBase, "advance unrelated path",
		git.RegularFileMode+" "+unrelated+"\tunrelated.txt")
	first := appendRecoveryEvent(t, ctx, f, firstBase, state.CaptureOp{
		Op: "modify", Path: "anchor.txt",
		BeforeOID: sql.NullString{String: anchorBefore, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: anchorAfter, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	renameCommit := commitTreeWithIndexUpdates(t, ctx, f, firstBase, "publish rename chain",
		"0 0000000000000000000000000000000000000000\ta.txt",
		git.RegularFileMode+" "+a+"\tc.txt")
	for _, seq := range []int64{firstRename, secondRename} {
		if err := state.MarkEventPublished(ctx, f.db, seq, state.EventStatePublished,
			sql.NullString{String: renameCommit, Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
			t.Fatalf("MarkEventPublished rename context seq=%d: %v", seq, err)
		}
		if _, err := state.AppendDecision(ctx, f.db, state.DecisionRecord{
			DecisionTS: 2, Kind: state.DecisionKindCommitted,
			EventSeq: sql.NullInt64{Int64: seq, Valid: true},
		}); err != nil {
			t.Fatalf("AppendDecision rename context seq=%d: %v", seq, err)
		}
	}
	last := appendRecoveryEvent(t, ctx, f, renameCommit, state.CaptureOp{
		Op: "modify", Path: "c.txt",
		BeforeOID: sql.NullString{String: a, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: d, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, renameCommit, ""); err != nil {
		t.Fatalf("update HEAD to rename context: %v", err)
	}

	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         first,
		Trigger:          "test_transitive_rename_context",
		ArchiveOnly:      true,
	})
	if err != nil {
		t.Fatalf("ReconcileUnpublishedChain: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered || result.EventCount != 2 ||
		result.FirstSeq != first || result.LastSeq != last {
		t.Fatalf("result=%+v want two-event recovered suffix", result)
	}
	if got, err := git.LsTreeBlobOID(ctx, f.dir, result.RecoveryRef, "c.txt"); err != nil || got != d {
		t.Fatalf("recovery c.txt blob=%s err=%v want %s", got, err, d)
	}
	for _, absent := range []string{"a.txt", "b.txt"} {
		if got, err := git.LsTreeBlobOID(ctx, f.dir, result.RecoveryRef, absent); err != nil || got != "" {
			t.Fatalf("recovery %s blob=%s err=%v want absent", absent, got, err)
		}
	}
}

func TestReconcile_ExcludesAdvancedContextAlreadyInSeed(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	anchorBefore, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor before\n"))
	anchorAfter, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor after\n"))
	a, _ := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	b, _ := git.HashObjectStdin(ctx, f.dir, []byte("B\n"))
	c, _ := git.HashObjectStdin(ctx, f.dir, []byte("C\n"))
	unrelated, _ := git.HashObjectStdin(ctx, f.dir, []byte("unrelated\n"))
	initialBase := commitTreeWithIndexUpdates(t, ctx, f, f.cctx.BaseHead, "initial base",
		git.RegularFileMode+" "+anchorBefore+"\tanchor.txt",
		git.RegularFileMode+" "+a+"\tdoc.md")
	published := appendRecoveryEvent(t, ctx, f, initialBase, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: a, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: b, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	firstBase := commitTreeWithIndexUpdates(t, ctx, f, initialBase, "external B",
		git.RegularFileMode+" "+b+"\tdoc.md")
	first := appendRecoveryEvent(t, ctx, f, firstBase, state.CaptureOp{
		Op: "modify", Path: "anchor.txt",
		BeforeOID: sql.NullString{String: anchorBefore, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: anchorAfter, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	settledHead := commitTreeWithIndexUpdates(t, ctx, f, firstBase, "advance after external B",
		git.RegularFileMode+" "+unrelated+"\tunrelated.txt")
	if err := state.MarkEventPublished(ctx, f.db, published, state.EventStatePublished,
		sql.NullString{String: settledHead, Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
		t.Fatalf("MarkEventPublished handled-external prefix: %v", err)
	}
	if _, err := state.AppendDecision(ctx, f.db, state.DecisionRecord{
		DecisionTS: 2, Kind: state.DecisionKindHandledExternal,
		EventSeq: sql.NullInt64{Int64: published, Valid: true},
	}); err != nil {
		t.Fatalf("AppendDecision handled-external prefix: %v", err)
	}
	last := appendRecoveryEvent(t, ctx, f, settledHead, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: b, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: c, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, settledHead, ""); err != nil {
		t.Fatalf("update HEAD to settled context: %v", err)
	}

	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         first,
		Trigger:          "test_advanced_context_already_in_seed",
		ArchiveOnly:      true,
	})
	if err != nil {
		t.Fatalf("ReconcileUnpublishedChain: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered || result.EventCount != 2 ||
		result.FirstSeq != first || result.LastSeq != last {
		t.Fatalf("result=%+v want two-event recovered suffix", result)
	}
	for path, want := range map[string]string{"anchor.txt": anchorAfter, "doc.md": c} {
		got, err := git.LsTreeBlobOID(ctx, f.dir, result.RecoveryRef, path)
		if err != nil || got != want {
			t.Fatalf("recovery %s blob=%s err=%v want %s", path, got, err, want)
		}
	}
}

func TestReconcile_ExcludesOnlySeedRepresentedCommitPaths(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	anchorBefore, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor before\n"))
	anchorAfter, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor after\n"))
	a, _ := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	b, _ := git.HashObjectStdin(ctx, f.dir, []byte("B\n"))
	c, _ := git.HashObjectStdin(ctx, f.dir, []byte("C\n"))
	x, _ := git.HashObjectStdin(ctx, f.dir, []byte("X\n"))
	y, _ := git.HashObjectStdin(ctx, f.dir, []byte("Y\n"))
	z, _ := git.HashObjectStdin(ctx, f.dir, []byte("Z\n"))
	initialBase := commitTreeWithIndexUpdates(t, ctx, f, f.cctx.BaseHead, "initial base",
		git.RegularFileMode+" "+anchorBefore+"\tanchor.txt",
		git.RegularFileMode+" "+a+"\tdoc.md",
		git.RegularFileMode+" "+x+"\tother.md")
	docPublished := appendRecoveryEvent(t, ctx, f, initialBase, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: a, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: b, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	otherPublished := appendRecoveryEvent(t, ctx, f, initialBase, state.CaptureOp{
		Op: "modify", Path: "other.md",
		BeforeOID: sql.NullString{String: x, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: y, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	firstBase := commitTreeWithIndexUpdates(t, ctx, f, initialBase, "external doc B",
		git.RegularFileMode+" "+b+"\tdoc.md")
	first := appendRecoveryEvent(t, ctx, f, firstBase, state.CaptureOp{
		Op: "modify", Path: "anchor.txt",
		BeforeOID: sql.NullString{String: anchorBefore, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: anchorAfter, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	settledHead := commitTreeWithIndexUpdates(t, ctx, f, firstBase, "publish grouped paths",
		git.RegularFileMode+" "+y+"\tother.md")
	for _, seq := range []int64{docPublished, otherPublished} {
		if err := state.MarkEventPublished(ctx, f.db, seq, state.EventStatePublished,
			sql.NullString{String: settledHead, Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
			t.Fatalf("MarkEventPublished grouped context seq=%d: %v", seq, err)
		}
		if _, err := state.AppendDecision(ctx, f.db, state.DecisionRecord{
			DecisionTS: 2, Kind: state.DecisionKindCommitted,
			EventSeq: sql.NullInt64{Int64: seq, Valid: true},
		}); err != nil {
			t.Fatalf("AppendDecision grouped context seq=%d: %v", seq, err)
		}
	}
	_ = appendRecoveryEvent(t, ctx, f, settledHead, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: b, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: c, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	last := appendRecoveryEvent(t, ctx, f, settledHead, state.CaptureOp{
		Op: "modify", Path: "other.md",
		BeforeOID: sql.NullString{String: y, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: z, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, settledHead, ""); err != nil {
		t.Fatalf("update HEAD to grouped context: %v", err)
	}

	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         first,
		Trigger:          "test_partial_seed_context",
		ArchiveOnly:      true,
	})
	if err != nil {
		t.Fatalf("ReconcileUnpublishedChain: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered || result.EventCount != 3 ||
		result.FirstSeq != first || result.LastSeq != last {
		t.Fatalf("result=%+v want three-event recovered suffix", result)
	}
	for path, want := range map[string]string{
		"anchor.txt": anchorAfter,
		"doc.md":     c,
		"other.md":   z,
	} {
		got, err := git.LsTreeBlobOID(ctx, f.dir, result.RecoveryRef, path)
		if err != nil || got != want {
			t.Fatalf("recovery %s blob=%s err=%v want %s", path, got, err, want)
		}
	}
}

func TestGroupPublishedContextRejectsTooManyCommits(t *testing.T) {
	t.Parallel()
	contextEvents := make([]state.RecoveryChainEvent, 0, maxPublishedRecoveryContextCommits+1)
	for i := 0; i <= maxPublishedRecoveryContextCommits; i++ {
		contextEvents = append(contextEvents, state.RecoveryChainEvent{
			Event: state.CaptureEvent{
				Seq:       int64(i + 1),
				CommitOID: sql.NullString{String: fmt.Sprintf("%040x", i+1), Valid: true},
			},
		})
	}
	if _, err := groupPublishedContextByCommit(contextEvents); err == nil ||
		!strings.Contains(err.Error(), "bounded limit") {
		t.Fatalf("groupPublishedContextByCommit err=%v want bounded limit", err)
	}
}

func TestDescendantPublishedContextAllowsInterveningCommits(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	blob, _ := git.HashObjectStdin(ctx, f.dir, []byte("same tree\n"))
	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", blob, "base")
	firstContextCommit := commitSingleFileTree(t, ctx, f.dir, "doc.md", blob,
		"first context", base)
	interveningCommit := commitSingleFileTree(t, ctx, f.dir, "doc.md", blob,
		"intervening side commit", base)
	secondContextCommit := commitSingleFileTree(t, ctx, f.dir, "doc.md", blob,
		"second context merge", firstContextCommit, interveningCommit)
	contextEvents := []state.RecoveryChainEvent{
		{Event: state.CaptureEvent{
			Seq:       1,
			CommitOID: sql.NullString{String: firstContextCommit, Valid: true},
		}},
		{Event: state.CaptureEvent{
			Seq:       2,
			CommitOID: sql.NullString{String: secondContextCommit, Valid: true},
		}},
	}

	got, err := descendantPublishedContext(ctx, f.dir, base, contextEvents)
	if err != nil {
		t.Fatalf("descendantPublishedContext: %v", err)
	}
	if len(got) != len(contextEvents) {
		t.Fatalf("descendantPublishedContext len=%d want %d", len(got), len(contextEvents))
	}
	for i := range got {
		if got[i].Event.Seq != contextEvents[i].Event.Seq {
			t.Fatalf("descendantPublishedContext[%d].Seq=%d want %d",
				i, got[i].Event.Seq, contextEvents[i].Event.Seq)
		}
	}
}

func TestReconcile_DropsSeedRepresentedContextBeforeCommitLimit(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	currentBlob, _ := git.HashObjectStdin(ctx, f.dir, []byte("version 0\n"))
	head := commitSingleFileTree(t, ctx, f.dir, "doc.md", currentBlob, "base")
	for i := 1; i <= maxPublishedRecoveryContextCommits+1; i++ {
		nextBlob, _ := git.HashObjectStdin(ctx, f.dir, []byte(fmt.Sprintf("version %d\n", i)))
		seq := appendRecoveryEvent(t, ctx, f, head, state.CaptureOp{
			Op: "modify", Path: "doc.md",
			BeforeOID: sql.NullString{String: currentBlob, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
			AfterOID: sql.NullString{String: nextBlob, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		})
		nextHead := commitSingleFileTree(t, ctx, f.dir, "doc.md", nextBlob,
			fmt.Sprintf("publish version %d", i), head)
		if err := state.MarkEventPublished(ctx, f.db, seq, state.EventStatePublished,
			sql.NullString{String: nextHead, Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
			t.Fatalf("MarkEventPublished seq=%d: %v", seq, err)
		}
		if _, err := state.AppendDecision(ctx, f.db, state.DecisionRecord{
			DecisionTS: 2, Kind: state.DecisionKindCommitted,
			EventSeq: sql.NullInt64{Int64: seq, Valid: true},
		}); err != nil {
			t.Fatalf("AppendDecision seq=%d: %v", seq, err)
		}
		currentBlob = nextBlob
		head = nextHead
	}
	finalBlob, _ := git.HashObjectStdin(ctx, f.dir, []byte("unpublished final\n"))
	first := appendRecoveryEvent(t, ctx, f, head, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: currentBlob, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: finalBlob, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, head, ""); err != nil {
		t.Fatalf("update HEAD to represented context: %v", err)
	}

	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         first,
		Trigger:          "test_seed_filter_before_commit_limit",
		ArchiveOnly:      true,
	})
	if err != nil {
		t.Fatalf("ReconcileUnpublishedChain: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered || result.EventCount != 1 {
		t.Fatalf("result=%+v want one-event recovered suffix", result)
	}
	if got, err := git.LsTreeBlobOID(ctx, f.dir, result.RecoveryRef, "doc.md"); err != nil || got != finalBlob {
		t.Fatalf("recovery doc blob=%s err=%v want %s", got, err, finalBlob)
	}
}

func TestReconcile_DropsAncestorContextBeforeCommitLimit(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	currentBlob, _ := git.HashObjectStdin(ctx, f.dir, []byte("version 0\n"))
	head := commitSingleFileTree(t, ctx, f.dir, "doc.md", currentBlob, "base")
	for i := 1; i <= maxPublishedRecoveryContextCommits+1; i++ {
		nextBlob, _ := git.HashObjectStdin(ctx, f.dir, []byte(fmt.Sprintf("version %d\n", i)))
		seq := appendRecoveryEvent(t, ctx, f, head, state.CaptureOp{
			Op: "modify", Path: "doc.md",
			BeforeOID: sql.NullString{String: currentBlob, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
			AfterOID: sql.NullString{String: nextBlob, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		})
		nextHead := commitSingleFileTree(t, ctx, f.dir, "doc.md", nextBlob,
			fmt.Sprintf("publish version %d", i), head)
		if err := state.MarkEventPublished(ctx, f.db, seq, state.EventStatePublished,
			sql.NullString{String: nextHead, Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
			t.Fatalf("MarkEventPublished seq=%d: %v", seq, err)
		}
		if _, err := state.AppendDecision(ctx, f.db, state.DecisionRecord{
			DecisionTS: 2, Kind: state.DecisionKindCommitted,
			EventSeq: sql.NullInt64{Int64: seq, Valid: true},
		}); err != nil {
			t.Fatalf("AppendDecision seq=%d: %v", seq, err)
		}
		currentBlob = nextBlob
		head = nextHead
	}

	externalBlob, _ := git.HashObjectStdin(ctx, f.dir, []byte("external seed\n"))
	externalHead := commitSingleFileTree(t, ctx, f.dir, "doc.md", externalBlob,
		"external seed change", head)
	finalBlob, _ := git.HashObjectStdin(ctx, f.dir, []byte("unpublished final\n"))
	first := appendRecoveryEvent(t, ctx, f, externalHead, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: externalBlob, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: finalBlob, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, externalHead, ""); err != nil {
		t.Fatalf("update HEAD to external seed: %v", err)
	}

	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         first,
		Trigger:          "test_ancestor_filter_before_commit_limit",
		ArchiveOnly:      true,
	})
	if err != nil {
		t.Fatalf("ReconcileUnpublishedChain: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered || result.EventCount != 1 {
		t.Fatalf("result=%+v want one-event recovered suffix", result)
	}
	if got, err := git.LsTreeBlobOID(ctx, f.dir, result.RecoveryRef, "doc.md"); err != nil || got != finalBlob {
		t.Fatalf("recovery doc blob=%s err=%v want %s", got, err, finalBlob)
	}
}

func TestReconcile_IgnoresUnrelatedPublishedContext(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	anchorBefore, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor before\n"))
	anchorAfter, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor after\n"))
	a, _ := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	b, _ := git.HashObjectStdin(ctx, f.dir, []byte("B\n"))
	c, _ := git.HashObjectStdin(ctx, f.dir, []byte("C\n"))
	base := commitTreeWithIndexUpdates(t, ctx, f, f.cctx.BaseHead, "base",
		git.RegularFileMode+" "+anchorBefore+"\tanchor.txt",
		git.RegularFileMode+" "+a+"\tunrelated.txt")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}
	published := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "unrelated.txt",
		BeforeOID: sql.NullString{String: a, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: b, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	commitC := commitTreeWithIndexUpdates(t, ctx, f, base, "publish unrelated C",
		git.RegularFileMode+" "+c+"\tunrelated.txt")
	if err := state.MarkEventPublished(ctx, f.db, published, state.EventStatePublished,
		sql.NullString{String: commitC, Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
		t.Fatalf("MarkEventPublished unrelated context: %v", err)
	}
	first := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "anchor.txt",
		BeforeOID: sql.NullString{String: anchorBefore, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: anchorAfter, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})

	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         first,
		Trigger:          "test_unrelated_published_context",
		ArchiveOnly:      true,
	})
	if err != nil {
		t.Fatalf("ReconcileUnpublishedChain: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered || result.EventCount != 1 {
		t.Fatalf("result=%+v want one-event recovered suffix", result)
	}
	got, err := git.LsTreeBlobOID(ctx, f.dir, result.RecoveryRef, "anchor.txt")
	if err != nil || got != anchorAfter {
		t.Fatalf("recovery anchor blob=%s err=%v want %s", got, err, anchorAfter)
	}
}

func TestReconcile_GroupsNoncontiguousPublishedContextByCommit(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	anchorBefore, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor before\n"))
	anchorAfter, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor after\n"))
	a, _ := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	b, _ := git.HashObjectStdin(ctx, f.dir, []byte("B\n"))
	x, _ := git.HashObjectStdin(ctx, f.dir, []byte("X\n"))
	c, _ := git.HashObjectStdin(ctx, f.dir, []byte("C\n"))
	d, _ := git.HashObjectStdin(ctx, f.dir, []byte("D\n"))
	base := commitTreeWithIndexUpdates(t, ctx, f, f.cctx.BaseHead, "base",
		git.RegularFileMode+" "+anchorBefore+"\tanchor.txt",
		git.RegularFileMode+" "+a+"\tdoc.md")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}
	first := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "anchor.txt",
		BeforeOID: sql.NullString{String: anchorBefore, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: anchorAfter, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	commitX := commitTreeWithIndexUpdates(t, ctx, f, base, "publish X",
		git.RegularFileMode+" "+x+"\tdoc.md")
	commitC := commitTreeWithIndexUpdates(t, ctx, f, base, "publish C",
		git.RegularFileMode+" "+c+"\tdoc.md")
	markPublished := func(before, after, commit string) {
		t.Helper()
		seq := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
			Op: "modify", Path: "doc.md",
			BeforeOID: sql.NullString{String: before, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
			AfterOID: sql.NullString{String: after, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		})
		if err := state.MarkEventPublished(ctx, f.db, seq, state.EventStatePublished,
			sql.NullString{String: commit, Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
			t.Fatalf("MarkEventPublished seq=%d: %v", seq, err)
		}
	}
	markPublished(a, b, commitC)
	markPublished(b, x, commitX)
	markPublished(x, c, commitC)
	last := appendRecoveryEvent(t, ctx, f, commitC, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: c, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: d, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})

	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         first,
		Trigger:          "test_noncontiguous_published_context",
		ArchiveOnly:      true,
	})
	if err != nil {
		t.Fatalf("ReconcileUnpublishedChain: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered || result.EventCount != 2 || result.LastSeq != last {
		t.Fatalf("result=%+v want two-event recovered suffix", result)
	}
	got, err := git.LsTreeBlobOID(ctx, f.dir, result.RecoveryRef, "doc.md")
	if err != nil || got != d {
		t.Fatalf("recovery doc blob=%s err=%v want %s", got, err, d)
	}
}

func TestReplay_ReconcilesBarrierAfterGroupedSamePathPrefix(t *testing.T) {
	// Keep this exact-outcome assertion in the serial phase: Replay deliberately
	// leaves the barrier unchanged when a best-effort reconciliation command
	// fails, so Git process starvation must not masquerade as a semantic failure.
	f := newCaptureFixture(t)
	ctx := context.Background()
	a, _ := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	b, _ := git.HashObjectStdin(ctx, f.dir, []byte("B\n"))
	c, _ := git.HashObjectStdin(ctx, f.dir, []byte("C\n"))
	d, _ := git.HashObjectStdin(ctx, f.dir, []byte("D\n"))
	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", a, "base A")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}
	seq1 := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: a, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: b, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	seq2 := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: b, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: c, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	groupedCommit := commitSingleFileTree(t, ctx, f.dir, "doc.md", c, "publish grouped C", base)
	for _, seq := range []int64{seq1, seq2} {
		if err := state.MarkEventPublished(ctx, f.db, seq, state.EventStatePublished,
			sql.NullString{String: groupedCommit, Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
			t.Fatalf("MarkEventPublished grouped prefix seq=%d: %v", seq, err)
		}
		if _, err := state.AppendDecision(ctx, f.db, state.DecisionRecord{
			DecisionTS: 2, Kind: state.DecisionKindCommitted,
			EventSeq: sql.NullInt64{Int64: seq, Valid: true},
		}); err != nil {
			t.Fatalf("AppendDecision grouped prefix seq=%d: %v", seq, err)
		}
	}
	seq3 := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: c, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: d, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	markRecoveryBarrier(t, ctx, f, seq3, base, "modify before-state mismatch for doc.md")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, groupedCommit, base); err != nil {
		t.Fatalf("update HEAD to grouped commit: %v", err)
	}
	cctx := f.cctx
	cctx.BaseHead = groupedCommit
	if _, err := Replay(ctx, f.dir, f.db, cctx, ReplayOpts{GitDir: f.gitDir}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	gotState, oid := readEventState(t, ctx, f.db, seq3)
	if gotState != state.EventStateRecovered || !oid.Valid {
		t.Fatalf("suffix state=%q oid=%v want recovered", gotState, oid)
	}
	var recoveryRef string
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT recovery_ref FROM recovery_snapshots WHERE first_event_seq = ?`, seq3).Scan(&recoveryRef); err != nil {
		t.Fatalf("query recovery ref: %v", err)
	}
	gotBlob, err := git.LsTreeBlobOID(ctx, f.dir, recoveryRef, "doc.md")
	if err != nil || gotBlob != d {
		t.Fatalf("recovery blob=%s err=%v want final D %s", gotBlob, err, d)
	}
}

func TestReplay_ExcludesSupersededPublishedPrefix(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	a, _ := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	b, _ := git.HashObjectStdin(ctx, f.dir, []byte("superseded B\n"))
	c, _ := git.HashObjectStdin(ctx, f.dir, []byte("captured C\n"))
	unrelated, _ := git.HashObjectStdin(ctx, f.dir, []byte("external unrelated\n"))
	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", a, "base A")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}
	seq1 := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: a, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: b, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	external := commitTreeWithIndexUpdates(t, ctx, f, base, "external supersedes B",
		git.RegularFileMode+" "+unrelated+"\tunrelated.txt")
	if err := state.MarkEventPublished(ctx, f.db, seq1, state.EventStatePublished,
		sql.NullString{String: external, Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
		t.Fatalf("MarkEventPublished superseded prefix: %v", err)
	}
	if _, err := state.AppendDecision(ctx, f.db, state.DecisionRecord{
		DecisionTS: 2, Kind: state.DecisionKindSupersededExternal,
		EventSeq: sql.NullInt64{Int64: seq1, Valid: true},
	}); err != nil {
		t.Fatalf("AppendDecision superseded prefix: %v", err)
	}
	seq2 := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID: sql.NullString{String: a, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: c, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	markRecoveryBarrier(t, ctx, f, seq2, base, "modify before-state mismatch for doc.md")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, external, base); err != nil {
		t.Fatalf("update external HEAD: %v", err)
	}
	cctx := f.cctx
	cctx.BaseHead = external
	if _, err := Replay(ctx, f.dir, f.db, cctx, ReplayOpts{GitDir: f.gitDir}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	gotState, oid := readEventState(t, ctx, f.db, seq2)
	if gotState != state.EventStateRecovered {
		t.Fatalf("suffix state=%q oid=%v want recovered", gotState, oid)
	}
	var ref string
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT recovery_ref FROM recovery_snapshots WHERE first_event_seq = ?`, seq2).Scan(&ref); err != nil {
		t.Fatalf("query recovery ref: %v", err)
	}
	gotBlob, err := git.LsTreeBlobOID(ctx, f.dir, ref, "doc.md")
	if err != nil || gotBlob != c {
		t.Fatalf("recovery blob=%s err=%v want captured C %s", gotBlob, err, c)
	}
}

func TestReplay_ArchivesCompositeChainWithoutGitMutation(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	modifyBefore, _ := git.HashObjectStdin(ctx, f.dir, []byte("modify before\n"))
	modifyAfter, _ := git.HashObjectStdin(ctx, f.dir, []byte("modify after\n"))
	deleteBlob, _ := git.HashObjectStdin(ctx, f.dir, []byte("delete me\n"))
	renameBlob, _ := git.HashObjectStdin(ctx, f.dir, []byte("rename me\n"))
	modeBlob, _ := git.HashObjectStdin(ctx, f.dir, []byte("#!/bin/sh\n"))
	symlinkBlob, _ := git.HashObjectStdin(ctx, f.dir, []byte("modify.txt"))

	baseTree, err := git.Mktree(ctx, f.dir, []git.MktreeEntry{
		{Mode: git.RegularFileMode, Type: "blob", OID: deleteBlob, Path: "delete.txt"},
		{Mode: git.RegularFileMode, Type: "blob", OID: modeBlob, Path: "mode.sh"},
		{Mode: git.RegularFileMode, Type: "blob", OID: modifyBefore, Path: "modify.txt"},
		{Mode: git.RegularFileMode, Type: "blob", OID: renameBlob, Path: "old.txt"},
	})
	if err != nil {
		t.Fatalf("Mktree base: %v", err)
	}
	base, err := git.CommitTree(ctx, f.dir, baseTree, "composite base", f.cctx.BaseHead)
	if err != nil {
		t.Fatalf("CommitTree base: %v", err)
	}
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, f.cctx.BaseHead); err != nil {
		t.Fatalf("update base: %v", err)
	}
	f.cctx.BaseHead = base

	ops := []state.CaptureOp{
		{Op: "modify", Path: "modify.txt", BeforeOID: sql.NullString{String: modifyBefore, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true}, AfterOID: sql.NullString{String: modifyAfter, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true}},
		{Op: "delete", Path: "delete.txt", BeforeOID: sql.NullString{String: deleteBlob, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true}},
		{Op: "rename", Path: "new.txt", OldPath: sql.NullString{String: "old.txt", Valid: true}, BeforeOID: sql.NullString{String: renameBlob, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true}, AfterOID: sql.NullString{String: renameBlob, Valid: true}, AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true}},
		{Op: "mode", Path: "mode.sh", BeforeOID: sql.NullString{String: modeBlob, Valid: true}, BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true}, AfterOID: sql.NullString{String: modeBlob, Valid: true}, AfterMode: sql.NullString{String: "100755", Valid: true}},
		{Op: "create", Path: "link", AfterOID: sql.NullString{String: symlinkBlob, Valid: true}, AfterMode: sql.NullString{String: git.SymlinkMode, Valid: true}},
	}
	seqs := make([]int64, 0, len(ops))
	for _, op := range ops {
		seqs = append(seqs, appendRecoveryEvent(t, ctx, f, base, op))
	}
	markRecoveryBarrier(t, ctx, f, seqs[0], base, "modify before-state mismatch for modify.txt")

	// HEAD matches only the first final path. Proof must inspect every path
	// and archive the whole chain, never partially publish it.
	external := commitTreeWithIndexUpdates(t, ctx, f, base, "partial external match",
		git.RegularFileMode+" "+modifyAfter+"\tmodify.txt")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, external, base); err != nil {
		t.Fatalf("update external: %v", err)
	}
	cctx := f.cctx
	cctx.BaseHead = external
	headBefore, _ := git.RevParse(ctx, f.dir, "HEAD")
	branchBefore, _ := git.RunBranchRef(ctx, f.dir)
	indexBefore := gitRawOutput(t, ctx, f.dir, "ls-files", "-s", "-z")
	worktreeBefore := gitRawOutput(t, ctx, f.dir, "status", "--porcelain=v1", "-z")

	if _, err := Replay(ctx, f.dir, f.db, cctx, ReplayOpts{GitDir: f.gitDir}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	var ref string
	var commitOID string
	var eventCount int
	if err := f.db.SQL().QueryRowContext(ctx, `
SELECT recovery_ref, commit_oid, event_count FROM recovery_snapshots`).Scan(
		&ref, &commitOID, &eventCount); err != nil {
		t.Fatalf("query recovery snapshot: %v", err)
	}
	if eventCount != len(seqs) {
		t.Fatalf("snapshot event_count=%d want %d", eventCount, len(seqs))
	}
	for _, seq := range seqs {
		gotState, oid := readEventState(t, ctx, f.db, seq)
		if gotState != state.EventStateRecovered || oid.String != commitOID {
			t.Fatalf("seq=%d state=%q oid=%v want recovered at %s", seq, gotState, oid, commitOID)
		}
	}
	parent, err := git.RevParse(ctx, f.dir, ref+"^")
	if err != nil || parent != base {
		t.Fatalf("recovery parent=%s err=%v want %s", parent, err, base)
	}
	entries, err := git.LsTree(ctx, f.dir, ref, false,
		"modify.txt", "delete.txt", "old.txt", "new.txt", "mode.sh", "link")
	if err != nil {
		t.Fatalf("LsTree recovery ref: %v", err)
	}
	wantEntries := map[string]git.TreeEntry{
		"modify.txt": {Mode: git.RegularFileMode, Type: "blob", OID: modifyAfter},
		"new.txt":    {Mode: git.RegularFileMode, Type: "blob", OID: renameBlob},
		"mode.sh":    {Mode: "100755", Type: "blob", OID: modeBlob},
		"link":       {Mode: git.SymlinkMode, Type: "blob", OID: symlinkBlob},
	}
	for _, entry := range entries {
		want, ok := wantEntries[entry.Path]
		if !ok {
			t.Fatalf("unexpected recovery path: %+v", entry)
		}
		if entry.Mode != want.Mode || entry.Type != want.Type || entry.OID != want.OID {
			t.Fatalf("recovery entry=%+v want=%+v", entry, want)
		}
		delete(wantEntries, entry.Path)
	}
	if len(wantEntries) != 0 {
		t.Fatalf("missing recovery entries: %v", wantEntries)
	}
	headAfter, _ := git.RevParse(ctx, f.dir, "HEAD")
	branchAfter, _ := git.RunBranchRef(ctx, f.dir)
	if headAfter != headBefore || branchAfter != branchBefore {
		t.Fatalf("recovery mutated HEAD/ref: head %s->%s branch %s->%s", headBefore, headAfter, branchBefore, branchAfter)
	}
	if got := gitRawOutput(t, ctx, f.dir, "ls-files", "-s", "-z"); got != indexBefore {
		t.Fatal("recovery mutated live index")
	}
	if got := gitRawOutput(t, ctx, f.dir, "status", "--porcelain=v1", "-z"); got != worktreeBefore {
		t.Fatal("recovery mutated worktree status")
	}
}

func TestReplay_RecoveryOrdersDirectoryReplacementDeletesFirst(t *testing.T) {
	runBoundedParallel(t)

	for _, tc := range []struct {
		name       string
		basePath   string
		targetPath string
	}{
		{name: "directory to file", basePath: "dir/file.txt", targetPath: "dir"},
		{name: "file to directory", basePath: "dir", targetPath: "dir/file.txt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCaptureFixture(t)
			ctx := context.Background()
			before, _ := git.HashObjectStdin(ctx, f.dir, []byte("before\n"))
			after, _ := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
			base := commitTreeWithIndexUpdates(t, ctx, f, f.cctx.BaseHead, "replacement base",
				git.RegularFileMode+" "+before+"\t"+tc.basePath)
			if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, f.cctx.BaseHead); err != nil {
				t.Fatalf("update base: %v", err)
			}
			seq1 := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
				Op: "delete", Path: tc.basePath,
				BeforeOID:  sql.NullString{String: before, Valid: true},
				BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
			})
			markRecoveryBarrier(t, ctx, f, seq1, base, "delete before-state mismatch for "+tc.basePath)
			seq2 := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
				Op: "create", Path: tc.targetPath,
				AfterOID:  sql.NullString{String: after, Valid: true},
				AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
			})
			cctx := f.cctx
			cctx.BaseHead = base
			if _, err := Replay(ctx, f.dir, f.db, cctx, ReplayOpts{GitDir: f.gitDir}); err != nil {
				t.Fatalf("Replay: %v", err)
			}
			for _, seq := range []int64{seq1, seq2} {
				if gotState, _ := readEventState(t, ctx, f.db, seq); gotState != state.EventStateRecovered {
					t.Fatalf("seq=%d state=%q want recovered", seq, gotState)
				}
			}
			var ref string
			if err := f.db.SQL().QueryRowContext(ctx,
				`SELECT recovery_ref FROM recovery_snapshots`).Scan(&ref); err != nil {
				t.Fatalf("query recovery ref: %v", err)
			}
			got, err := git.LsTreeBlobOID(ctx, f.dir, ref, tc.targetPath)
			if err != nil || got != after {
				t.Fatalf("target %s blob=%s err=%v want %s", tc.targetPath, got, err, after)
			}
			if got, err := git.LsTreeBlobOID(ctx, f.dir, ref, tc.basePath); tc.basePath != tc.targetPath && (err != nil || got != "") {
				t.Fatalf("base path %s still present blob=%s err=%v", tc.basePath, got, err)
			}
		})
	}
}

func TestReplay_ReconcileMissingObjectLeavesChainUntouched(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	after, err := git.HashObjectStdin(ctx, f.dir, []byte("unreachable payload\n"))
	if err != nil {
		t.Fatalf("HashObjectStdin: %v", err)
	}
	seq := appendRecoveryEvent(t, ctx, f, f.cctx.BaseHead, state.CaptureOp{
		Op: "create", Path: "missing.txt",
		AfterOID:  sql.NullString{String: after, Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	markRecoveryBarrier(t, ctx, f, seq, f.cctx.BaseHead, "create conflict for missing.txt")
	objectPath := filepath.Join(f.gitDir, "objects", after[:2], after[2:])
	if err := os.Remove(objectPath); err != nil {
		t.Fatalf("remove captured object %s: %v", objectPath, err)
	}

	if _, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	gotState, oid := readEventState(t, ctx, f.db, seq)
	if gotState != state.EventStateBlockedConflict || oid.Valid {
		t.Fatalf("state=%q oid=%v want unchanged blocked row", gotState, oid)
	}
	if count := countRecoverySnapshots(t, ctx, f.db); count != 0 {
		t.Fatalf("recovery snapshots=%d want 0", count)
	}
	if refs := recoveryRefs(t, ctx, f.dir); len(refs) != 0 {
		t.Fatalf("recovery refs=%v want none", refs)
	}
}

func TestReconcile_StableTokenRaceRetriesSameTreeRef(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	before, _ := git.HashObjectStdin(ctx, f.dir, []byte("before\n"))
	after, _ := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", before, "base")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}
	seq, _, _ := seedBlockedModify(t, ctx, f, "doc.md", before, after, base)
	otherRef := "refs/heads/recovery-race"
	if err := git.UpdateRef(ctx, f.dir, otherRef, base,
		"0000000000000000000000000000000000000000"); err != nil {
		t.Fatalf("create race ref: %v", err)
	}

	var switchErr error
	_, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         seq,
		Trigger:          "test_token_race",
		beforeFinalHeadCheck: func() {
			_, switchErr = git.Run(ctx, git.RunOpts{Dir: f.dir}, "symbolic-ref", "HEAD", otherRef)
		},
	})
	if switchErr != nil {
		t.Fatalf("switch symbolic HEAD: %v", switchErr)
	}
	if err == nil || !strings.Contains(err.Error(), "branch token moved") {
		t.Fatalf("Reconcile error=%v want stable-token refusal", err)
	}
	gotState, oid := readEventState(t, ctx, f.db, seq)
	if gotState != state.EventStateBlockedConflict || oid.Valid || countRecoverySnapshots(t, ctx, f.db) != 0 {
		t.Fatalf("race changed DB state=%q oid=%v snapshots=%d", gotState, oid, countRecoverySnapshots(t, ctx, f.db))
	}
	refs := recoveryRefs(t, ctx, f.dir)
	if len(refs) != 1 {
		t.Fatalf("safe evidence refs=%v want one", refs)
	}
	protectedCommit, err := git.RevParse(ctx, f.dir, refs[0])
	if err != nil {
		t.Fatalf("resolve protected commit: %v", err)
	}

	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "symbolic-ref", "HEAD", f.cctx.BranchRef); err != nil {
		t.Fatalf("restore symbolic HEAD: %v", err)
	}
	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         seq,
		Trigger:          "test_retry",
	})
	if err != nil {
		t.Fatalf("retry Reconcile: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered || result.CommitOID != protectedCommit {
		t.Fatalf("retry result=%+v want reused recovered commit %s", result, protectedCommit)
	}
	if refs := recoveryRefs(t, ctx, f.dir); len(refs) != 1 {
		t.Fatalf("retry created duplicate refs: %v", refs)
	}
}

func TestReconcile_ABAHeadSampleCannotFalsePublish(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	before, _ := git.HashObjectStdin(ctx, f.dir, []byte("before\n"))
	after, _ := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", before, "base")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}
	seq, _, _ := seedBlockedModify(t, ctx, f, "doc.md", before, after, base)
	external := commitSingleFileTree(t, ctx, f.dir, "doc.md", after, "transient external", base)
	otherRef := "refs/heads/recovery-aba"
	if err := git.UpdateRef(ctx, f.dir, otherRef, external,
		"0000000000000000000000000000000000000000"); err != nil {
		t.Fatalf("create transient ref: %v", err)
	}

	var switchErr, restoreErr error
	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         seq,
		Trigger:          "test_aba_head_sample",
		afterInitialLiveToken: func() {
			_, switchErr = git.Run(ctx, git.RunOpts{Dir: f.dir}, "symbolic-ref", "HEAD", otherRef)
		},
		beforeLiveTokenRecheck: func() {
			_, restoreErr = git.Run(ctx, git.RunOpts{Dir: f.dir}, "symbolic-ref", "HEAD", f.cctx.BranchRef)
		},
	})
	if switchErr != nil || restoreErr != nil {
		t.Fatalf("ABA hooks: switch=%v restore=%v", switchErr, restoreErr)
	}
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered {
		t.Fatalf("ABA result=%+v want recovered, not transient published proof", result)
	}
	if !strings.HasSuffix(result.RecoveryRef, "/archive") {
		t.Fatalf("recovery ref=%q want archive", result.RecoveryRef)
	}
	if head, err := git.RevParse(ctx, f.dir, "HEAD"); err != nil || head != base {
		t.Fatalf("HEAD=%s err=%v want stable base %s", head, err, base)
	}
}

func TestReconcile_PublishedProofSurvivesTokenRaceAndRetries(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	before, _ := git.HashObjectStdin(ctx, f.dir, []byte("before\n"))
	after, _ := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", before, "base")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}
	seq, _, _ := seedBlockedModify(t, ctx, f, "doc.md", before, after, base)
	external := commitSingleFileTree(t, ctx, f.dir, "doc.md", after, "external", base)
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, external, base); err != nil {
		t.Fatalf("update external: %v", err)
	}
	otherRef := "refs/heads/published-proof-race"
	if err := git.UpdateRef(ctx, f.dir, otherRef, external,
		"0000000000000000000000000000000000000000"); err != nil {
		t.Fatalf("create other ref: %v", err)
	}

	var switchErr error
	_, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         seq,
		Trigger:          "test_published_race",
		beforeStateTransition: func() {
			_, switchErr = git.Run(ctx, git.RunOpts{Dir: f.dir}, "symbolic-ref", "HEAD", otherRef)
		},
	})
	if switchErr != nil {
		t.Fatalf("switch symbolic HEAD: %v", switchErr)
	}
	if err == nil || !strings.Contains(err.Error(), "branch token moved") {
		t.Fatalf("Reconcile error=%v want stable-token refusal", err)
	}
	gotState, oid := readEventState(t, ctx, f.db, seq)
	if gotState != state.EventStateBlockedConflict || oid.Valid || countRecoverySnapshots(t, ctx, f.db) != 0 {
		t.Fatalf("race changed DB state=%q oid=%v snapshots=%d", gotState, oid, countRecoverySnapshots(t, ctx, f.db))
	}
	proofRef := recoveryProofRefName(f.cctx.BranchRef, f.cctx.BranchGeneration, seq, seq, external)
	protected, err := git.RevParse(ctx, f.dir, proofRef)
	if err != nil || protected != external {
		t.Fatalf("published proof target=%s err=%v want %s", protected, err, external)
	}

	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "symbolic-ref", "HEAD", f.cctx.BranchRef); err != nil {
		t.Fatalf("restore symbolic HEAD: %v", err)
	}
	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         seq,
		Trigger:          "test_published_retry",
	})
	if err != nil {
		t.Fatalf("retry Reconcile: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStatePublished ||
		result.CommitOID != external || result.RecoveryRef != proofRef {
		t.Fatalf("retry result=%+v want protected published commit", result)
	}
}

func TestReconcile_ProtectsRecoveryRefThroughStateTransition(t *testing.T) {
	runBoundedParallel(t)

	for _, mutation := range []string{"delete", "move"} {
		t.Run(mutation, func(t *testing.T) {
			f := newCaptureFixture(t)
			ctx := context.Background()
			before, _ := git.HashObjectStdin(ctx, f.dir, []byte("before\n"))
			after, _ := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
			base := commitSingleFileTree(t, ctx, f.dir, "doc.md", before, "base")
			if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
				t.Fatalf("update base: %v", err)
			}
			seq, _, _ := seedBlockedModify(t, ctx, f, "doc.md", before, after, base)
			external := commitSingleFileTree(t, ctx, f.dir, "doc.md", after, "external", base)
			if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, external, base); err != nil {
				t.Fatalf("update external: %v", err)
			}
			proofRef := recoveryProofRefName(f.cctx.BranchRef, f.cctx.BranchGeneration, seq, seq, external)

			var mutationErr error
			result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
				GitDir:           f.gitDir,
				BranchRef:        f.cctx.BranchRef,
				BranchGeneration: f.cctx.BranchGeneration,
				FirstSeq:         seq,
				Trigger:          "test_protected_transition_" + mutation,
				afterRecoveryRefLocked: func() {
					mutationCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
					defer cancel()
					if mutation == "delete" {
						_, mutationErr = git.Run(mutationCtx, git.RunOpts{Dir: f.dir},
							"update-ref", "-d", proofRef, external)
						return
					}
					mutationErr = git.UpdateRef(mutationCtx, f.dir, proofRef, base, external)
				},
			})
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if mutationErr == nil {
				t.Fatalf("concurrent recovery ref %s succeeded while transition lock was held", mutation)
			}
			if !result.Handled || result.Outcome != state.EventStatePublished || result.CommitOID != external {
				t.Fatalf("result=%+v want published at %s", result, external)
			}
			gotState, commitOID := readEventState(t, ctx, f.db, seq)
			if gotState != state.EventStatePublished || !commitOID.Valid || commitOID.String != external {
				t.Fatalf("DB state=%q commit=%v want published at %s", gotState, commitOID, external)
			}
			resolved, err := git.RevParse(ctx, f.dir, proofRef)
			if err != nil || resolved != external {
				t.Fatalf("protected proof ref=%s err=%v want reachable %s", resolved, err, external)
			}
		})
	}
}

func TestReconcile_RecoveryRefCollisionLeavesChainUntouched(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	before, _ := git.HashObjectStdin(ctx, f.dir, []byte("before\n"))
	after, _ := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
	wrong, _ := git.HashObjectStdin(ctx, f.dir, []byte("wrong\n"))
	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", before, "base")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}
	seq, _, _ := seedBlockedModify(t, ctx, f, "doc.md", before, after, base)
	expectedCommit := commitSingleFileTree(t, ctx, f.dir, "doc.md", after, "expected recovery", base)
	expectedTree, err := git.RevParse(ctx, f.dir, expectedCommit+"^{tree}")
	if err != nil {
		t.Fatalf("resolve expected tree: %v", err)
	}
	wrongCommit := commitSingleFileTree(t, ctx, f.dir, "doc.md", wrong, "wrong recovery", base)
	ref := recoveryRefName(f.cctx.BranchRef, f.cctx.BranchGeneration, seq, seq, base, expectedTree)
	if _, err := git.EnsureRecoveryRef(ctx, f.dir, ref, wrongCommit); err != nil {
		t.Fatalf("seed conflicting recovery ref: %v", err)
	}

	_, err = ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         seq,
		Trigger:          "test_collision",
	})
	if !errors.Is(err, git.ErrRecoveryRefCollision) {
		t.Fatalf("Reconcile err=%v want ErrRecoveryRefCollision", err)
	}
	gotState, oid := readEventState(t, ctx, f.db, seq)
	if gotState != state.EventStateBlockedConflict || oid.Valid || countRecoverySnapshots(t, ctx, f.db) != 0 {
		t.Fatalf("collision changed DB state=%q oid=%v snapshots=%d", gotState, oid, countRecoverySnapshots(t, ctx, f.db))
	}
	resolved, err := git.RevParse(ctx, f.dir, ref)
	if err != nil || resolved != wrongCommit {
		t.Fatalf("collision overwrote ref: got=%s err=%v want=%s", resolved, err, wrongCommit)
	}
}

func TestReconcile_RecoveryRefTargetDigestSurvivesStateReset(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	before, _ := git.HashObjectStdin(ctx, f.dir, []byte("before\n"))
	afterFirst, _ := git.HashObjectStdin(ctx, f.dir, []byte("after first\n"))
	afterSecond, _ := git.HashObjectStdin(ctx, f.dir, []byte("after second\n"))
	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", before, "base")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}

	firstSeq, _, _ := seedBlockedModify(t, ctx, f, "doc.md", before, afterFirst, base)
	first, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         firstSeq,
		Trigger:          "test_state_reset_first",
		ArchiveOnly:      true,
	})
	if err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}

	// Simulate replacement of a worktree-local state DB while the shared Git
	// recovery namespace remains. The next capture deliberately reuses the
	// same branch, generation, and sequence selector with different content.
	for _, statement := range []string{
		"DELETE FROM recovery_snapshots",
		"DELETE FROM capture_events",
		"DELETE FROM sqlite_sequence WHERE name IN ('capture_events', 'recovery_snapshots')",
	} {
		if _, err := f.db.SQL().ExecContext(ctx, statement); err != nil {
			t.Fatalf("reset state with %q: %v", statement, err)
		}
	}
	secondSeq, _, _ := seedBlockedModify(t, ctx, f, "doc.md", before, afterSecond, base)
	if secondSeq != firstSeq {
		t.Fatalf("reset sequence=%d want reused selector %d", secondSeq, firstSeq)
	}
	second, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         secondSeq,
		Trigger:          "test_state_reset_second",
		ArchiveOnly:      true,
	})
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if !first.Handled || !second.Handled || first.RecoveryRef == second.RecoveryRef {
		t.Fatalf("recovery refs first=%q second=%q want distinct handled snapshots",
			first.RecoveryRef, second.RecoveryRef)
	}
	for _, result := range []RecoveryChainResult{first, second} {
		if !strings.Contains(result.RecoveryRef, fmt.Sprintf("/%d-%d-", firstSeq, firstSeq)) ||
			!strings.HasSuffix(result.RecoveryRef, "/archive") {
			t.Fatalf("recovery ref %q lost inspectable selector", result.RecoveryRef)
		}
		resolved, err := git.RevParse(ctx, f.dir, result.RecoveryRef)
		if err != nil || resolved != result.CommitOID {
			t.Fatalf("recovery ref %q=(%q,%v) want %q",
				result.RecoveryRef, resolved, err, result.CommitOID)
		}
	}
}

func TestReconcile_DBRaceRollsBackShadowInvalidation(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	before, _ := git.HashObjectStdin(ctx, f.dir, []byte("before\n"))
	after, _ := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
	base := commitSingleFileTree(t, ctx, f.dir, "doc.md", before, "base")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, base, ""); err != nil {
		t.Fatalf("update base: %v", err)
	}
	seq, _, _ := seedBlockedModify(t, ctx, f, "doc.md", before, after, base)
	if err := state.UpsertShadowPath(ctx, f.db, state.ShadowPath{
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		Path: "doc.md", Operation: "modify",
		Mode:     sql.NullString{String: git.RegularFileMode, Valid: true},
		OID:      sql.NullString{String: after, Valid: true},
		BaseHead: base, Fidelity: "rescan",
	}); err != nil {
		t.Fatalf("UpsertShadowPath: %v", err)
	}
	marker := ShadowBootstrappedKey(f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err := state.MetaSet(ctx, f.db, marker, "1"); err != nil {
		t.Fatalf("MetaSet shadow marker: %v", err)
	}

	var raceErr error
	_, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         seq,
		Trigger:          "test_db_race",
		InvalidateShadow: true,
		beforeStateTransition: func() {
			_, raceErr = f.db.SQL().ExecContext(ctx,
				`UPDATE capture_events SET state = ? WHERE seq = ?`, state.EventStateFailed, seq)
		},
	})
	if raceErr != nil {
		t.Fatalf("inject DB race: %v", raceErr)
	}
	if !errors.Is(err, state.ErrRecoveryChainChanged) {
		t.Fatalf("Reconcile err=%v want ErrRecoveryChainChanged", err)
	}
	gotState, oid := readEventState(t, ctx, f.db, seq)
	if gotState != state.EventStateFailed || oid.Valid || countRecoverySnapshots(t, ctx, f.db) != 0 {
		t.Fatalf("reconciler clobbered race state=%q oid=%v snapshots=%d", gotState, oid, countRecoverySnapshots(t, ctx, f.db))
	}
	if _, ok, err := state.GetShadowPath(ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration, "doc.md"); err != nil || !ok {
		t.Fatalf("shadow invalidation escaped failed tx: ok=%v err=%v", ok, err)
	}
	if _, ok, err := state.MetaGet(ctx, f.db, marker); err != nil || !ok {
		t.Fatalf("shadow marker invalidation escaped failed tx: ok=%v err=%v", ok, err)
	}
}

func TestReconcile_ArchiveOnlySupportsMissingLiveHEAD(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	after, _ := git.HashObjectStdin(ctx, f.dir, []byte("captured on old base\n"))
	base := f.cctx.BaseHead
	seq := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "create", Path: "orphaned.txt",
		AfterOID:  sql.NullString{String: after, Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	markRecoveryBarrier(t, ctx, f, seq, base, "create conflict for orphaned.txt")
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir},
		"update-ref", "-d", f.cctx.BranchRef, base); err != nil {
		t.Fatalf("delete live branch ref: %v", err)
	}

	selector := RecoveryReconcileOptions{
		GitDir:           f.gitDir,
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq:         seq,
		Trigger:          "test_missing_head",
	}
	if _, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, selector); err == nil ||
		!strings.Contains(err.Error(), "archive-only mode required") {
		t.Fatalf("normal reconcile err=%v want explicit archive-only refusal", err)
	}
	if refs := recoveryRefs(t, ctx, f.dir); len(refs) != 0 {
		t.Fatalf("normal missing-HEAD attempt created refs: %v", refs)
	}
	selector.ArchiveOnly = true
	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, selector)
	if err != nil {
		t.Fatalf("archive-only reconcile: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered || result.RecoveryRef == "" {
		t.Fatalf("archive-only result=%+v", result)
	}
	parent, err := git.RevParse(ctx, f.dir, result.RecoveryRef+"^")
	if err != nil || parent != base {
		t.Fatalf("recovery parent=%s err=%v want %s", parent, err, base)
	}
}

func TestReplay_ArchivedWorkIsRecapturedAndPublished(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	seedTrackedFileCommit(t, ctx, f, "doc.md", "before\n")
	base := f.cctx.BaseHead
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatalf("BootstrapShadow base: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "doc.md"), []byte("after\n"), 0o644); err != nil {
		t.Fatalf("write dirty doc: %v", err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("Capture dirty doc: %v", err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending after capture=%v err=%v want one", pending, err)
	}
	oldSeq := pending[0].Seq
	markRecoveryBarrier(t, ctx, f, oldSeq, base, "modify before-state mismatch for doc.md")
	otherBlob, _ := git.HashObjectStdin(ctx, f.dir, []byte("external other\n"))
	external := commitTreeWithIndexUpdates(t, ctx, f, base, "external different doc",
		git.RegularFileMode+" "+otherBlob+"\tdoc.md")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, external, base); err != nil {
		t.Fatalf("update external: %v", err)
	}
	cctx := f.cctx
	cctx.BaseHead = external
	if _, err := Replay(ctx, f.dir, f.db, cctx, ReplayOpts{GitDir: f.gitDir}); err != nil {
		t.Fatalf("Replay archive: %v", err)
	}
	if gotState, _ := readEventState(t, ctx, f.db, oldSeq); gotState != state.EventStateRecovered {
		t.Fatalf("old event state=%q want recovered", gotState)
	}
	if bootstrapped, err := IsShadowBootstrapped(ctx, f.db, cctx.BranchRef, cctx.BranchGeneration); err != nil || bootstrapped {
		t.Fatalf("shadow marker after archive=%v err=%v want invalidated", bootstrapped, err)
	}
	var shadowRows int
	if err := f.db.SQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM shadow_paths WHERE branch_ref = ? AND branch_generation = ?`,
		cctx.BranchRef, cctx.BranchGeneration).Scan(&shadowRows); err != nil {
		t.Fatalf("count shadow rows: %v", err)
	}
	if shadowRows != 0 {
		t.Fatalf("shadow rows=%d want 0 after atomic invalidation", shadowRows)
	}

	// This mirrors the run loop's missing-marker pass: seed the stable HEAD,
	// then the following capture observes the still-dirty worktree again.
	if _, err := BootstrapShadow(ctx, f.dir, f.db, cctx); err != nil {
		t.Fatalf("BootstrapShadow external HEAD: %v", err)
	}
	if _, err := Capture(ctx, f.dir, f.db, cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("recapture dirty doc: %v", err)
	}
	pending, err = state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Seq == oldSeq {
		t.Fatalf("recaptured pending=%v err=%v", pending, err)
	}
	newSeq := pending[0].Seq
	if _, err := Replay(ctx, f.dir, f.db, cctx, ReplayOpts{GitDir: f.gitDir}); err != nil {
		t.Fatalf("Replay recaptured event: %v", err)
	}
	gotState, _ := readEventState(t, ctx, f.db, newSeq)
	if gotState != state.EventStatePublished {
		t.Fatalf("recaptured event state=%q want published", gotState)
	}
	wantBlob, _ := git.HashObjectStdin(ctx, f.dir, []byte("after\n"))
	gotBlob, err := git.LsTreeBlobOID(ctx, f.dir, "HEAD", "doc.md")
	if err != nil || gotBlob != wantBlob {
		t.Fatalf("published HEAD doc blob=%s err=%v want %s", gotBlob, err, wantBlob)
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
