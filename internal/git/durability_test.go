package git

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

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
