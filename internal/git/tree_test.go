package git

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestHashObjectStdinWritesBlob(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	oid, err := HashObjectStdin(ctx, dir, []byte("hello\n"))
	if err != nil {
		t.Fatalf("hash-object: %v", err)
	}
	if len(oid) != 40 {
		t.Fatalf("expected 40-char sha, got %q", oid)
	}
	// Re-hash; OID must be deterministic.
	oid2, err := HashObjectStdin(ctx, dir, []byte("hello\n"))
	if err != nil {
		t.Fatalf("hash-object 2: %v", err)
	}
	if oid != oid2 {
		t.Fatalf("non-deterministic: %s vs %s", oid, oid2)
	}
	// Read it back via cat-file.
	got, err := CatFileBlob(ctx, dir, oid)
	if err != nil {
		t.Fatalf("cat-file blob: %v", err)
	}
	if string(got) != "hello\n" {
		t.Fatalf("blob roundtrip mismatch: %q", string(got))
	}
}

func TestHashSymlinkBlobUsesMode120000(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	oid, mode, err := HashSymlinkBlob(ctx, dir, "../target/file")
	if err != nil {
		t.Fatalf("HashSymlinkBlob: %v", err)
	}
	if mode != "120000" {
		t.Fatalf("expected symlink mode 120000, got %q", mode)
	}
	// Wire it into a tree and ls-tree it back: the entry must come out
	// as mode 120000 with type=blob.
	treeOID, err := Mktree(ctx, dir, []MktreeEntry{
		{Mode: mode, Type: "blob", OID: oid, Path: "link"},
	})
	if err != nil {
		t.Fatalf("Mktree: %v", err)
	}
	entries, err := LsTree(ctx, dir, treeOID, false)
	if err != nil {
		t.Fatalf("LsTree: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	got := entries[0]
	if got.Mode != "120000" || got.Type != "blob" || got.Path != "link" {
		t.Fatalf("unexpected entry: %+v", got)
	}
	if got.OID != oid {
		t.Fatalf("oid mismatch: %s vs %s", got.OID, oid)
	}
}

func TestMktreeAndLsTreeRoundTrip(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	a, err := HashObjectStdin(ctx, dir, []byte("a"))
	if err != nil {
		t.Fatalf("hash a: %v", err)
	}
	b, err := HashObjectStdin(ctx, dir, []byte("b"))
	if err != nil {
		t.Fatalf("hash b: %v", err)
	}
	tree, err := Mktree(ctx, dir, []MktreeEntry{
		{Mode: "100644", Type: "blob", OID: a, Path: "a.txt"},
		{Mode: "100644", Type: "blob", OID: b, Path: "b.txt"},
	})
	if err != nil {
		t.Fatalf("Mktree: %v", err)
	}
	entries, err := LsTree(ctx, dir, tree, false)
	if err != nil {
		t.Fatalf("LsTree: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	if entries[0].Path != "a.txt" || entries[0].OID != a {
		t.Fatalf("entry 0 mismatch: %+v want a.txt/%s", entries[0], a)
	}
	if entries[1].Path != "b.txt" || entries[1].OID != b {
		t.Fatalf("entry 1 mismatch: %+v want b.txt/%s", entries[1], b)
	}
}

func TestLsTreeBlobOID(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	blob, err := HashObjectStdin(ctx, dir, []byte("hello\n"))
	if err != nil {
		t.Fatalf("hash blob: %v", err)
	}
	tree, err := Mktree(ctx, dir, []MktreeEntry{
		{Mode: RegularFileMode, Type: "blob", OID: blob, Path: "hello.txt"},
	})
	if err != nil {
		t.Fatalf("mktree: %v", err)
	}

	got, err := LsTreeBlobOID(ctx, dir, tree, "hello.txt")
	if err != nil {
		t.Fatalf("LsTreeBlobOID existing: %v", err)
	}
	if got != blob {
		t.Fatalf("LsTreeBlobOID existing=%s want %s", got, blob)
	}

	missing, err := LsTreeBlobOID(ctx, dir, tree, "missing.txt")
	if err != nil {
		t.Fatalf("LsTreeBlobOID missing: %v", err)
	}
	if missing != "" {
		t.Fatalf("LsTreeBlobOID missing=%q want empty", missing)
	}
}

func TestCommitTreeAndUpdateRefProduceValidCommit(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	blob, err := HashObjectStdin(ctx, dir, []byte("hello world\n"))
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	tree, err := Mktree(ctx, dir, []MktreeEntry{
		{Mode: "100644", Type: "blob", OID: blob, Path: "hello.txt"},
	})
	if err != nil {
		t.Fatalf("mktree: %v", err)
	}
	commit, err := CommitTree(ctx, dir, tree, "init: hello world")
	if err != nil {
		t.Fatalf("commit-tree: %v", err)
	}
	if len(commit) != 40 {
		t.Fatalf("bad commit oid: %q", commit)
	}
	if err := UpdateRef(ctx, dir, "refs/heads/main", commit, ""); err != nil {
		t.Fatalf("update-ref: %v", err)
	}
	got, err := RevParse(ctx, dir, "refs/heads/main")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if got != commit {
		t.Fatalf("rev-parse mismatch: %s vs %s", got, commit)
	}
}

func TestUpdateIndexInfoWithIsolatedIndex(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	blob, err := HashObjectStdin(ctx, dir, []byte("indexed!"))
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	idx := filepath.Join(t.TempDir(), "isolated.index")
	line := "100644 " + blob + "\tone.txt"
	if err := UpdateIndexInfo(ctx, dir, idx, []string{line}); err != nil {
		t.Fatalf("UpdateIndexInfo: %v", err)
	}
	tree, err := WriteTree(ctx, dir, idx)
	if err != nil {
		t.Fatalf("WriteTree: %v", err)
	}
	entries, err := LsTree(ctx, dir, tree, false)
	if err != nil {
		t.Fatalf("LsTree: %v", err)
	}
	if len(entries) != 1 || entries[0].Path != "one.txt" || entries[0].OID != blob {
		t.Fatalf("unexpected tree contents: %+v", entries)
	}

	// Sanity: the repo's main index is untouched.
	main, err := LsFilesStaged(ctx, dir)
	if err != nil {
		t.Fatalf("ls-files (main): %v", err)
	}
	if len(main) != 0 {
		t.Fatalf("expected main index empty, got %+v", main)
	}
}

// TestLsFilesIndex_IsolatedIndex stages entries on a per-call scratch index
// via UpdateIndexInfo + GIT_INDEX_FILE, then reads them back through
// LsFilesIndex. The repo's default index must remain empty — replay relies
// on this isolation so a busy worktree never poisons a queued event's
// before-state check.
func TestLsFilesIndex_IsolatedIndex(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()

	a, err := HashObjectStdin(ctx, dir, []byte("alpha"))
	if err != nil {
		t.Fatalf("hash a: %v", err)
	}
	b, err := HashObjectStdin(ctx, dir, []byte("beta"))
	if err != nil {
		t.Fatalf("hash b: %v", err)
	}

	idx := filepath.Join(t.TempDir(), "scratch.index")
	lines := []string{
		"100644 " + a + "\tdir/a.txt",
		"100755 " + b + "\tdir/b.sh",
	}
	if err := UpdateIndexInfo(ctx, dir, idx, lines); err != nil {
		t.Fatalf("UpdateIndexInfo: %v", err)
	}

	got, err := LsFilesIndex(ctx, dir, idx)
	if err != nil {
		t.Fatalf("LsFilesIndex: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d (%+v)", len(got), got)
	}
	sort.Slice(got, func(i, j int) bool { return got[i].Path < got[j].Path })
	if got[0].Path != "dir/a.txt" || got[0].OID != a || got[0].Mode != "100644" || got[0].Stage != 0 {
		t.Fatalf("entry 0 mismatch: %+v", got[0])
	}
	if got[1].Path != "dir/b.sh" || got[1].OID != b || got[1].Mode != "100755" || got[1].Stage != 0 {
		t.Fatalf("entry 1 mismatch: %+v", got[1])
	}

	// Default (live) index must stay empty.
	live, err := LsFilesStaged(ctx, dir)
	if err != nil {
		t.Fatalf("LsFilesStaged: %v", err)
	}
	if len(live) != 0 {
		t.Fatalf("expected live index empty, got %+v", live)
	}
}

// TestLsFilesIndex_PathFiltering scopes the read with a `paths` filter and
// ensures (a) only matching entries come back and (b) paths with embedded
// whitespace round-trip via the NUL delimiter.
func TestLsFilesIndex_PathFiltering(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()

	a, err := HashObjectStdin(ctx, dir, []byte("a"))
	if err != nil {
		t.Fatalf("hash a: %v", err)
	}
	b, err := HashObjectStdin(ctx, dir, []byte("b"))
	if err != nil {
		t.Fatalf("hash b: %v", err)
	}
	c, err := HashObjectStdin(ctx, dir, []byte("c"))
	if err != nil {
		t.Fatalf("hash c: %v", err)
	}

	idx := filepath.Join(t.TempDir(), "scratch.index")
	// Path "weird name.txt" exercises the NUL-delimited parse — a tab- or
	// newline-split parser would mis-segment the record.
	lines := []string{
		"100644 " + a + "\tweird name.txt",
		"100644 " + b + "\tsrc/keep.go",
		"100644 " + c + "\tsrc/skip.go",
	}
	if err := UpdateIndexInfo(ctx, dir, idx, lines); err != nil {
		t.Fatalf("UpdateIndexInfo: %v", err)
	}

	// Filter to a single explicit path including the whitespace one.
	got, err := LsFilesIndex(ctx, dir, idx, "weird name.txt", "src/keep.go")
	if err != nil {
		t.Fatalf("LsFilesIndex (filter): %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 filtered entries, got %d (%+v)", len(got), got)
	}
	sort.Slice(got, func(i, j int) bool { return got[i].Path < got[j].Path })
	if got[0].Path != "src/keep.go" {
		t.Fatalf("filter entry 0 mismatch: %+v", got[0])
	}
	if got[1].Path != "weird name.txt" {
		t.Fatalf("filter entry 1 mismatch (NUL parse?): %+v", got[1])
	}

	// Filter that matches nothing returns an empty slice without error.
	none, err := LsFilesIndex(ctx, dir, idx, "does/not/exist.txt")
	if err != nil {
		t.Fatalf("LsFilesIndex (empty filter): %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("expected no entries, got %+v", none)
	}
}

func TestReconcileLiveIndexAppliesSafePathScopedUpdates(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()

	before, err := HashObjectStdin(ctx, dir, []byte("before\n"))
	if err != nil {
		t.Fatalf("hash before: %v", err)
	}
	after, err := HashObjectStdin(ctx, dir, []byte("after\n"))
	if err != nil {
		t.Fatalf("hash after: %v", err)
	}
	fresh, err := HashObjectStdin(ctx, dir, []byte("fresh\n"))
	if err != nil {
		t.Fatalf("hash fresh: %v", err)
	}
	unrelated, err := HashObjectStdin(ctx, dir, []byte("unrelated\n"))
	if err != nil {
		t.Fatalf("hash unrelated: %v", err)
	}

	if err := UpdateIndexInfo(ctx, dir, "", []string{
		RegularFileMode + " " + before + "\tmodify.txt",
		RegularFileMode + " " + before + "\tdelete.txt",
		RegularFileMode + " " + unrelated + "\tunrelated.txt",
	}); err != nil {
		t.Fatalf("seed live index: %v", err)
	}

	res, err := ReconcileLiveIndex(ctx, dir, []LiveIndexOp{
		{Path: "create.txt", AfterMode: RegularFileMode, AfterOID: fresh},
		{Path: "modify.txt", BeforeMode: RegularFileMode, BeforeOID: before, AfterMode: RegularFileMode, AfterOID: after},
		{Path: "delete.txt", BeforeMode: RegularFileMode, BeforeOID: before, Delete: true},
	})
	if err != nil {
		t.Fatalf("ReconcileLiveIndex: %v", err)
	}
	if len(res.Skipped) != 0 {
		t.Fatalf("unexpected skips: %+v", res.Skipped)
	}
	if got := strings.Join(res.Applied, ","); got != "create.txt,modify.txt,delete.txt" {
		t.Fatalf("applied=%q", got)
	}

	entries, err := LsFilesStaged(ctx, dir, "create.txt", "modify.txt", "delete.txt", "unrelated.txt")
	if err != nil {
		t.Fatalf("LsFilesStaged: %v", err)
	}
	index := indexEntriesByPath(entries)
	assertIndexEntry(t, index, "create.txt", RegularFileMode, fresh)
	assertIndexEntry(t, index, "modify.txt", RegularFileMode, after)
	if _, ok := index["delete.txt"]; ok {
		t.Fatalf("delete.txt still present in live index: %+v", index["delete.txt"])
	}
	assertIndexEntry(t, index, "unrelated.txt", RegularFileMode, unrelated)
}

func TestReconcileLiveIndexSkipsProtectedLiveIndexStates(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()

	before, err := HashObjectStdin(ctx, dir, []byte("before\n"))
	if err != nil {
		t.Fatalf("hash before: %v", err)
	}
	other, err := HashObjectStdin(ctx, dir, []byte("other\n"))
	if err != nil {
		t.Fatalf("hash other: %v", err)
	}
	after, err := HashObjectStdin(ctx, dir, []byte("after\n"))
	if err != nil {
		t.Fatalf("hash after: %v", err)
	}
	if err := UpdateIndexInfo(ctx, dir, "", []string{
		RegularFileMode + " " + other + "\tmismatch.txt",
		RegularFileMode + " " + before + " 1\tconflict.txt",
		RegularFileMode + " " + other + "\texisting-create.txt",
	}); err != nil {
		t.Fatalf("seed live index: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "intent.txt"), []byte("intent\n"), 0o644); err != nil {
		t.Fatalf("write intent: %v", err)
	}
	if _, err := Run(ctx, RunOpts{Dir: dir}, "add", "-N", "--", "intent.txt"); err != nil {
		t.Fatalf("git add -N: %v", err)
	}

	res, err := ReconcileLiveIndex(ctx, dir, []LiveIndexOp{
		{Path: "mismatch.txt", BeforeMode: RegularFileMode, BeforeOID: before, AfterMode: RegularFileMode, AfterOID: after},
		{Path: "conflict.txt", BeforeMode: RegularFileMode, BeforeOID: before, AfterMode: RegularFileMode, AfterOID: after},
		{Path: "existing-create.txt", AfterMode: RegularFileMode, AfterOID: after},
		{Path: "intent.txt", BeforeMode: RegularFileMode, BeforeOID: emptyBlobOID, AfterMode: RegularFileMode, AfterOID: after},
	})
	if err != nil {
		t.Fatalf("ReconcileLiveIndex: %v", err)
	}
	if len(res.Applied) != 0 {
		t.Fatalf("applied unsafe paths: %+v", res.Applied)
	}
	gotReasons := map[string]string{}
	for _, skip := range res.Skipped {
		gotReasons[skip.Path] = skip.Reason
	}
	wantReasons := map[string]string{
		"mismatch.txt":        "mismatched_entry",
		"conflict.txt":        "unmerged_entry",
		"existing-create.txt": "unexpected_existing_entry",
		"intent.txt":          "protected_index_flags",
	}
	for path, want := range wantReasons {
		if gotReasons[path] != want {
			t.Fatalf("skip reason for %s=%q want %q; all skips=%+v", path, gotReasons[path], want, res.Skipped)
		}
	}
}

func TestReconcileLiveIndexHandlesWhitespacePaths(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()

	blob, err := HashObjectStdin(ctx, dir, []byte("weird\n"))
	if err != nil {
		t.Fatalf("hash blob: %v", err)
	}
	path := "dir/weird name\nfile.txt"
	res, err := ReconcileLiveIndex(ctx, dir, []LiveIndexOp{
		{Path: path, AfterMode: RegularFileMode, AfterOID: blob},
	})
	if err != nil {
		t.Fatalf("ReconcileLiveIndex: %v", err)
	}
	if len(res.Applied) != 1 || res.Applied[0] != path || len(res.Skipped) != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	entries, err := LsFilesStaged(ctx, dir, path)
	if err != nil {
		t.Fatalf("LsFilesStaged: %v", err)
	}
	index := indexEntriesByPath(entries)
	assertIndexEntry(t, index, path, RegularFileMode, blob)
}

func indexEntriesByPath(entries []IndexEntry) map[string]IndexEntry {
	out := make(map[string]IndexEntry, len(entries))
	for _, entry := range entries {
		out[entry.Path] = entry
	}
	return out
}

func assertIndexEntry(t *testing.T, entries map[string]IndexEntry, path, mode, oid string) {
	t.Helper()
	entry, ok := entries[path]
	if !ok {
		t.Fatalf("missing index entry %q; entries=%+v", path, entries)
	}
	if entry.Mode != mode || entry.OID != oid || entry.Stage != 0 {
		t.Fatalf("index entry %q=%+v want mode=%s oid=%s stage=0", path, entry, mode, oid)
	}
}

func TestLsFilesStagedReturnsIndexedEntries(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	// Stage a file via update-index on the default index.
	blob, err := HashObjectStdin(ctx, dir, []byte("staged"))
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if err := UpdateIndexInfo(ctx, dir, "", []string{"100644 " + blob + "\tfile.txt"}); err != nil {
		t.Fatalf("UpdateIndexInfo: %v", err)
	}
	entries, err := LsFilesStaged(ctx, dir)
	if err != nil {
		t.Fatalf("LsFilesStaged: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d (%+v)", len(entries), entries)
	}
	got := entries[0]
	if got.Path != "file.txt" || got.OID != blob || got.Mode != "100644" || got.Stage != 0 {
		t.Fatalf("unexpected entry: %+v", got)
	}
}
