package git

import (
	"context"
	"errors"
	"os"
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

func TestFinishWorkerPrivateTargetPin(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	tree, err := WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "target.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := CommitTreeDurable(ctx, repo, tree, "finish target", "ACD Checkpoint", "checkpoint@localhost")
	if err != nil {
		t.Fatal(err)
	}
	ref := FinishRefPrefix + "worktree/request/target"
	created, err := EnsurePrivateRefDurable(ctx, repo, FinishRefPrefix, ref, commit)
	if err != nil || !created {
		t.Fatalf("create private target=%t error=%v", created, err)
	}
	created, err = EnsurePrivateRefDurable(ctx, repo, FinishRefPrefix, ref, commit)
	if err != nil || created {
		t.Fatalf("identical pin=%t error=%v", created, err)
	}
	if _, err := EnsurePrivateRefDurable(ctx, repo, FinishRefPrefix, "refs/heads/unsafe", commit); err == nil {
		t.Fatal("target pin escaped its private namespace")
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

func TestFinishPrivatePinsRejectSymbolicAliases(t *testing.T) {
	for _, prefix := range []string{CheckpointRefPrefix, "refs/acd/migration/", FinishRefPrefix} {
		for _, dangling := range []bool{false, true} {
			t.Run(prefix+map[bool]string{false: "same-OID", true: "dangling"}[dangling], func(t *testing.T) {
				repo := initRepo(t)
				ctx := context.Background()
				tree, err := WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "pin.index"), nil)
				if err != nil {
					t.Fatal(err)
				}
				commit, err := CommitTreeDurable(ctx, repo, tree, "protected pin", "ACD Checkpoint", "checkpoint@localhost")
				if err != nil {
					t.Fatal(err)
				}
				target := "refs/acd/alias-target"
				if !dangling {
					if err := UpdateRef(ctx, repo, target, commit, ""); err != nil {
						t.Fatal(err)
					}
				}
				ref := prefix + "test/pin"
				if _, err := Run(ctx, RunOpts{Dir: repo}, "symbolic-ref", ref, target); err != nil {
					t.Fatal(err)
				}
				if created, err := EnsurePrivateRefDurable(ctx, repo, prefix, ref, commit); created || !errors.Is(err, ErrCheckpointRefCollision) {
					t.Fatalf("symbolic pin accepted: created=%t error=%v", created, err)
				}
				called := false
				if err := WithLockedPrivateRef(ctx, repo, ref, commit, func(context.Context) error { called = true; return nil }); !errors.Is(err, ErrCheckpointRefCollision) || called {
					t.Fatalf("symbolic pin callback: called=%t error=%v", called, err)
				}
				if out, err := Run(ctx, RunOpts{Dir: repo}, "symbolic-ref", "--no-recurse", ref); err != nil || strings.TrimSpace(string(out)) != target {
					t.Fatalf("alias changed: %q error=%v", out, err)
				}
				if got, err := RevParse(ctx, repo, target); dangling && !errors.Is(err, ErrRefNotFound) || !dangling && (err != nil || got != commit) {
					t.Fatalf("alias target changed: %q error=%v", got, err)
				}
			})
		}
	}
}

func TestFinishPrivatePinRejectsPostCreateSymbolicReplacement(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	tree, err := WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "pin.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := CommitTreeDurable(ctx, repo, tree, "protected pin", "ACD Checkpoint", "checkpoint@localhost")
	if err != nil {
		t.Fatal(err)
	}
	const target = "refs/acd/alias-target"
	if err := UpdateRef(ctx, repo, target, commit, ""); err != nil {
		t.Fatal(err)
	}
	ref := CheckpointRefPrefix + "test/post-create"
	// The committed hook runs after Git releases the creation lock, before
	// Ensure returns. Replace the pin using ordinary Git, without direct file
	// mutation, to exercise its required post-create proof.
	hook := "#!/bin/sh\nif [ \"$1\" = committed ] && [ ! -e .git/alias-replaced ]; then\n  : > .git/alias-replaced\n  git -c core.hooksPath=/dev/null symbolic-ref " + ref + " " + target + " || exit 1\nfi\n"
	if err := os.WriteFile(filepath.Join(repo, ".git", "hooks", "reference-transaction"), []byte(hook), 0o755); err != nil {
		t.Fatal(err)
	}
	if created, err := EnsureCheckpointRef(ctx, repo, ref, commit); created || !errors.Is(err, ErrCheckpointRefCollision) {
		t.Fatalf("post-create alias accepted: created=%t error=%v", created, err)
	}
	if out, err := Run(ctx, RunOpts{Dir: repo}, "symbolic-ref", "--no-recurse", ref); err != nil || strings.TrimSpace(string(out)) != target {
		t.Fatalf("post-create alias changed: %q error=%v", out, err)
	}
}
