package git

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestBlobHasherMatchesGitAndDurablyWritesMissingObjects(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	hasher, err := NewBlobHasher(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	contents := [][]byte{[]byte("already present\n"), []byte("new protected blob\n")}
	firstOID, err := HashObjectStdin(ctx, repo, contents[0])
	if err != nil {
		t.Fatal(err)
	}
	secondOID, err := hasher.BlobOID(contents[1])
	if err != nil {
		t.Fatal(err)
	}
	if computed, err := hasher.BlobOID(contents[0]); err != nil || computed != firstOID {
		t.Fatalf("computed existing blob=(%q,%v), want %q", computed, err, firstOID)
	}
	if err := EnsureBlobObjectsDurable(ctx, repo, []DurableBlob{
		{OID: firstOID, Content: contents[0]},
		{OID: secondOID, Content: contents[1]},
		{OID: secondOID, Content: contents[1]},
	}); err != nil {
		t.Fatal(err)
	}
	if out, err := Run(ctx, RunOpts{Dir: repo}, "cat-file", "-p", secondOID); err != nil || string(out) != string(contents[1]) {
		t.Fatalf("new durable blob=(%q,%v)", out, err)
	}
}

func TestEnsureBlobObjectsDurableRejectsOIDContentMismatch(t *testing.T) {
	repo := initRepo(t)
	hasher, err := NewBlobHasher(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	oid, err := hasher.BlobOID([]byte("expected\n"))
	if err != nil {
		t.Fatal(err)
	}
	err = EnsureBlobObjectsDurable(context.Background(), repo, []DurableBlob{{
		OID: oid, Content: []byte("different\n"),
	}})
	if err == nil || !strings.Contains(err.Error(), "want "+oid) {
		t.Fatalf("mismatch error=%v", err)
	}
}

func TestDurableCheckpointObjectsAndRefSurvivePrune(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	if err := DurabilitySupport(ctx, repo); err != nil {
		t.Fatalf("durability support: %v", err)
	}
	blob, err := HashObjectStdinDurable(ctx, repo, []byte("protected\n"))
	if err != nil {
		t.Fatal(err)
	}
	tree, err := WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "checkpoint.index"), []IndexEntry{
		{Mode: RegularFileMode, OID: blob, Path: "nested/protected.txt"},
	})
	if err != nil {
		t.Fatal(err)
	}
	commit, err := CommitTreeDurable(ctx, repo, tree, "acd checkpoint cp-test\n",
		"ACD Checkpoint", "checkpoint@localhost")
	if err != nil {
		t.Fatal(err)
	}
	ref := CheckpointRefPrefix + "0123456789abcdef/cp-1-0123456789abcdef"
	created, err := EnsureCheckpointRef(ctx, repo, ref, commit)
	if err != nil || !created {
		t.Fatalf("ensure ref=(%t,%v), want (true,nil)", created, err)
	}
	created, err = EnsureCheckpointRef(ctx, repo, ref, commit)
	if err != nil || created {
		t.Fatalf("idempotent ensure ref=(%t,%v), want (false,nil)", created, err)
	}

	parents, err := Run(ctx, RunOpts{Dir: repo}, "show", "-s", "--format=%P", commit)
	if err != nil {
		t.Fatal(err)
	}
	if len(parents) != 1 || parents[0] != '\n' {
		t.Fatalf("checkpoint commit has parents: %q", parents)
	}
	if _, err := Run(ctx, RunOpts{Dir: repo}, "gc", "--prune=now"); err != nil {
		t.Fatal(err)
	}
	if got, err := RevParse(ctx, repo, ref); err != nil || got != commit {
		t.Fatalf("ref after gc=(%q,%v), want %q", got, err, commit)
	}
	entries, err := LsTree(ctx, repo, commit, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Path != "nested/protected.txt" || entries[0].OID != blob {
		t.Fatalf("checkpoint tree after gc=%+v", entries)
	}
	mainIndex, err := LsFilesStaged(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	if len(mainIndex) != 0 {
		t.Fatalf("durable tree changed live index: %+v", mainIndex)
	}
}

func TestEnsureCheckpointRefRejectsCollision(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	tree, err := WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "checkpoint.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := CommitTreeDurable(ctx, repo, tree, "first", "ACD Checkpoint", "checkpoint@localhost")
	if err != nil {
		t.Fatal(err)
	}
	second, err := CommitTreeDurable(ctx, repo, tree, "second", "ACD Checkpoint", "checkpoint@localhost")
	if err != nil {
		t.Fatal(err)
	}
	ref := CheckpointRefPrefix + "0123456789abcdef/cp-2-0123456789abcdef"
	if _, err := EnsureCheckpointRef(ctx, repo, ref, first); err != nil {
		t.Fatal(err)
	}
	if _, err := EnsureCheckpointRef(ctx, repo, ref, second); !errors.Is(err, ErrCheckpointRefCollision) {
		t.Fatalf("collision error=%v", err)
	}
	got, err := RevParse(ctx, repo, ref)
	if err != nil || got != first {
		t.Fatalf("collision moved ref to (%q,%v), want %q", got, err, first)
	}
}

func TestEnsureCheckpointRefSupportsSHA256Repository(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	if _, err := Run(ctx, RunOpts{Dir: repo}, "init", "--object-format=sha256", "."); err != nil {
		t.Skipf("Git SHA-256 repositories unavailable: %v", err)
	}
	tree, err := WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "sha256.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := CommitTreeDurable(ctx, repo, tree, "sha256 checkpoint",
		"ACD Checkpoint", "checkpoint@localhost")
	if err != nil {
		t.Fatal(err)
	}
	if len(commit) != 64 {
		t.Fatalf("commit oid length=%d want=64", len(commit))
	}
	ref := CheckpointRefPrefix + "0123456789abcdef/cp-1-0123456789abcdef"
	if created, err := EnsureCheckpointRef(ctx, repo, ref, commit); err != nil || !created {
		t.Fatalf("EnsureCheckpointRef=(%t,%v)", created, err)
	}
}
