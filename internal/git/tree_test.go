package git

import (
	"context"
	"path/filepath"
	"sort"
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
