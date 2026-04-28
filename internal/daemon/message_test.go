package daemon

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// hashContent writes content into the test repo's object store and
// returns its OID. Tests use this to fabricate before/after blobs that
// resemble what the capture pass would persist.
func hashContent(t *testing.T, repo, content string) string {
	t.Helper()
	oid, err := git.HashObjectStdin(context.Background(), repo, []byte(content))
	if err != nil {
		t.Fatalf("HashObjectStdin: %v", err)
	}
	return oid
}

// TestBuildOpsDiff_ModifyFromCapturedBlobs verifies that the diff is
// reconstructed from the captured before/after OIDs even after the live
// worktree has moved on. This is the regression the task targets:
// providers must see the actual delta the user authored, not whatever
// the file currently happens to look like.
func TestBuildOpsDiff_ModifyFromCapturedBlobs(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	beforeOID := hashContent(t, f.dir, "alpha\n")
	afterOID := hashContent(t, f.dir, "beta\n")

	op := state.CaptureOp{
		Op:         "modify",
		Path:       "src/foo.go",
		BeforeOID:  sql.NullString{String: beforeOID, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: afterOID, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
		Fidelity:   "rescan",
	}

	diff, err := BuildOpsDiff(ctx, f.dir, []state.CaptureOp{op})
	if err != nil {
		t.Fatalf("BuildOpsDiff: %v", err)
	}
	if !strings.Contains(diff, "diff --git a/src/foo.go b/src/foo.go") {
		t.Fatalf("missing rewritten diff header:\n%s", diff)
	}
	if !strings.Contains(diff, "--- a/src/foo.go") || !strings.Contains(diff, "+++ b/src/foo.go") {
		t.Fatalf("missing path-anchored header:\n%s", diff)
	}
	if !strings.Contains(diff, "-alpha") || !strings.Contains(diff, "+beta") {
		t.Fatalf("diff body did not show captured contents:\n%s", diff)
	}
}

// TestBuildOpsDiff_CreateUsesEmptyBlob exercises the create path: the
// before side is the synthesised empty blob, after side is a real OID.
func TestBuildOpsDiff_CreateUsesEmptyBlob(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	afterOID := hashContent(t, f.dir, "fresh content\n")

	op := state.CaptureOp{
		Op:        "create",
		Path:      "new.txt",
		AfterOID:  sql.NullString{String: afterOID, Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		Fidelity:  "rescan",
	}
	diff, err := BuildOpsDiff(ctx, f.dir, []state.CaptureOp{op})
	if err != nil {
		t.Fatalf("BuildOpsDiff: %v", err)
	}
	if !strings.Contains(diff, "diff --git a/new.txt b/new.txt") {
		t.Fatalf("missing diff header:\n%s", diff)
	}
	if !strings.Contains(diff, "new file mode 100644") {
		t.Fatalf("missing new-file-mode header:\n%s", diff)
	}
	if !strings.Contains(diff, "+fresh content") {
		t.Fatalf("missing added line:\n%s", diff)
	}
}

// TestBuildOpsDiff_DeleteUsesEmptyBlob mirrors create: after side is
// empty blob, before is the captured OID.
func TestBuildOpsDiff_DeleteUsesEmptyBlob(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	beforeOID := hashContent(t, f.dir, "doomed\n")

	op := state.CaptureOp{
		Op:         "delete",
		Path:       "gone.txt",
		BeforeOID:  sql.NullString{String: beforeOID, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		Fidelity:   "rescan",
	}
	diff, err := BuildOpsDiff(ctx, f.dir, []state.CaptureOp{op})
	if err != nil {
		t.Fatalf("BuildOpsDiff: %v", err)
	}
	if !strings.Contains(diff, "diff --git a/gone.txt b/gone.txt") {
		t.Fatalf("missing diff header:\n%s", diff)
	}
	if !strings.Contains(diff, "deleted file mode 100644") {
		t.Fatalf("missing deleted-file-mode header:\n%s", diff)
	}
	if !strings.Contains(diff, "-doomed") {
		t.Fatalf("missing deleted line:\n%s", diff)
	}
}

// TestBuildOpsDiff_RenameAttachesHeader: the rename header is prepended
// even when the content is unchanged (then-the body is empty, but the
// section still informs the model that a rename happened).
func TestBuildOpsDiff_RenameAttachesHeader(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	oid := hashContent(t, f.dir, "same content\n")

	op := state.CaptureOp{
		Op:         "rename",
		Path:       "new/name.txt",
		OldPath:    sql.NullString{String: "old/name.txt", Valid: true},
		BeforeOID:  sql.NullString{String: oid, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: oid, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
		Fidelity:   "rescan",
	}
	diff, err := BuildOpsDiff(ctx, f.dir, []state.CaptureOp{op})
	if err != nil {
		t.Fatalf("BuildOpsDiff: %v", err)
	}
	if !strings.Contains(diff, "rename from old/name.txt") {
		t.Fatalf("missing rename-from header:\n%s", diff)
	}
	if !strings.Contains(diff, "rename to new/name.txt") {
		t.Fatalf("missing rename-to header:\n%s", diff)
	}
}

// TestBuildOpsDiff_ModeOnly emits header-only output: before==after
// content, mode flips.
func TestBuildOpsDiff_ModeOnly(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	oid := hashContent(t, f.dir, "exec me\n")

	op := state.CaptureOp{
		Op:         "mode",
		Path:       "script.sh",
		BeforeOID:  sql.NullString{String: oid, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: oid, Valid: true},
		AfterMode:  sql.NullString{String: git.ExecutableFileMode, Valid: true},
		Fidelity:   "rescan",
	}
	diff, err := BuildOpsDiff(ctx, f.dir, []state.CaptureOp{op})
	if err != nil {
		t.Fatalf("BuildOpsDiff: %v", err)
	}
	if !strings.Contains(diff, "old mode 100644") || !strings.Contains(diff, "new mode 100755") {
		t.Fatalf("missing mode headers:\n%s", diff)
	}
	if strings.Contains(diff, "@@") {
		t.Fatalf("mode-only diff should not carry hunks:\n%s", diff)
	}
}

// TestBuildOpsDiff_MultiOpConcatenates: multiple ops produce sections
// joined with a newline; each section keeps its own diff header.
func TestBuildOpsDiff_MultiOpConcatenates(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	beforeA := hashContent(t, f.dir, "old A\n")
	afterA := hashContent(t, f.dir, "new A\n")
	afterB := hashContent(t, f.dir, "fresh B\n")

	ops := []state.CaptureOp{
		{
			Op:         "modify",
			Path:       "a.txt",
			BeforeOID:  sql.NullString{String: beforeA, Valid: true},
			BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
			AfterOID:   sql.NullString{String: afterA, Valid: true},
			AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
			Fidelity:   "rescan",
		},
		{
			Op:        "create",
			Path:      "b.txt",
			AfterOID:  sql.NullString{String: afterB, Valid: true},
			AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
			Fidelity:  "rescan",
		},
	}
	diff, err := BuildOpsDiff(ctx, f.dir, ops)
	if err != nil {
		t.Fatalf("BuildOpsDiff: %v", err)
	}
	if !strings.Contains(diff, "diff --git a/a.txt b/a.txt") {
		t.Fatalf("missing first section:\n%s", diff)
	}
	if !strings.Contains(diff, "diff --git a/b.txt b/b.txt") {
		t.Fatalf("missing second section:\n%s", diff)
	}
	// Both deltas survive.
	if !strings.Contains(diff, "+new A") || !strings.Contains(diff, "+fresh B") {
		t.Fatalf("missing per-op deltas:\n%s", diff)
	}
}

// TestBuildOpsDiff_SurvivesLiveWorktreeChange — the canonical
// regression. The blobs are persisted at capture time; the live file
// then changes again. The reconstructed diff still describes the
// captured delta, not the new live state.
func TestBuildOpsDiff_SurvivesLiveWorktreeChange(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	// Captured blobs reflect "alpha -> beta".
	beforeOID := hashContent(t, f.dir, "alpha\n")
	afterOID := hashContent(t, f.dir, "beta\n")

	// Live file moved on to "gamma" since capture.
	if err := writeFileForTest(f.dir, "src/foo.go", "gamma\n"); err != nil {
		t.Fatalf("write live: %v", err)
	}

	op := state.CaptureOp{
		Op:         "modify",
		Path:       "src/foo.go",
		BeforeOID:  sql.NullString{String: beforeOID, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: afterOID, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
		Fidelity:   "rescan",
	}
	diff, err := BuildOpsDiff(ctx, f.dir, []state.CaptureOp{op})
	if err != nil {
		t.Fatalf("BuildOpsDiff: %v", err)
	}
	if strings.Contains(diff, "gamma") {
		t.Fatalf("diff leaked live-worktree state:\n%s", diff)
	}
	if !strings.Contains(diff, "-alpha") || !strings.Contains(diff, "+beta") {
		t.Fatalf("captured delta missing:\n%s", diff)
	}
}

// TestCommitContextFromEvent_PopulatesAIFields asserts the wiring:
// Branch, RepoRoot, MultiOp, Now, and a non-empty truncated DiffText.
func TestCommitContextFromEvent_PopulatesAIFields(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	beforeOID := hashContent(t, f.dir, "old\n")
	afterOID := hashContent(t, f.dir, "new\n")

	ev := state.CaptureEvent{
		Seq:              1,
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         f.cctx.BaseHead,
		Operation:        "modify",
		Path:             "a.txt",
		Fidelity:         "rescan",
	}
	ops := []state.CaptureOp{
		{
			Op:         "modify",
			Path:       "a.txt",
			BeforeOID:  sql.NullString{String: beforeOID, Valid: true},
			BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
			AfterOID:   sql.NullString{String: afterOID, Valid: true},
			AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
			Fidelity:   "rescan",
		},
		{
			Op:         "modify",
			Path:       "b.txt",
			BeforeOID:  sql.NullString{String: beforeOID, Valid: true},
			BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
			AfterOID:   sql.NullString{String: afterOID, Valid: true},
			AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
			Fidelity:   "rescan",
		},
	}

	cc := commitContextFromEvent(ctx, EventContext{Event: ev, Ops: ops}, f.dir)

	if cc.Branch != "refs/heads/main" {
		t.Fatalf("Branch=%q", cc.Branch)
	}
	if cc.RepoRoot != f.dir {
		t.Fatalf("RepoRoot=%q want %q", cc.RepoRoot, f.dir)
	}
	if cc.Now.IsZero() {
		t.Fatalf("Now is zero")
	}
	if len(cc.MultiOp) != 2 {
		t.Fatalf("MultiOp len=%d want 2", len(cc.MultiOp))
	}
	if cc.DiffText == "" {
		t.Fatalf("DiffText empty")
	}
	if !strings.Contains(cc.DiffText, "diff --git a/a.txt b/a.txt") {
		t.Fatalf("DiffText missing first op:\n%s", cc.DiffText)
	}
	if !strings.Contains(cc.DiffText, "diff --git a/b.txt b/b.txt") {
		t.Fatalf("DiffText missing second op:\n%s", cc.DiffText)
	}
}

// writeFileForTest is a lightweight helper used only by message_test.go;
// keeps the test suite from importing os/path/filepath redundantly.
func writeFileForTest(dir, rel, body string) error {
	full := dir + "/" + rel
	if err := mkdirAllForTest(full); err != nil {
		return err
	}
	return writeBytesForTest(full, []byte(body))
}
