package git

import (
	"context"
	"errors"
	"testing"
)

func TestRevParseReturnsErrRefNotFoundForMissingRef(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	_, err := RevParse(ctx, dir, "refs/heads/does-not-exist")
	if !errors.Is(err, ErrRefNotFound) {
		t.Fatalf("expected ErrRefNotFound, got %v", err)
	}
}

func TestRevParseResolvesExistingRef(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	blob, err := HashObjectStdin(ctx, dir, []byte("x"))
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	tree, err := Mktree(ctx, dir, []MktreeEntry{
		{Mode: "100644", Type: "blob", OID: blob, Path: "x"},
	})
	if err != nil {
		t.Fatalf("mktree: %v", err)
	}
	commit, err := CommitTree(ctx, dir, tree, "x")
	if err != nil {
		t.Fatalf("commit-tree: %v", err)
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

func TestUpdateRefCompareAndSwapRejectsStaleOld(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	blob, _ := HashObjectStdin(ctx, dir, []byte("y"))
	tree, _ := Mktree(ctx, dir, []MktreeEntry{
		{Mode: "100644", Type: "blob", OID: blob, Path: "y"},
	})
	c1, err := CommitTree(ctx, dir, tree, "y1")
	if err != nil {
		t.Fatalf("c1: %v", err)
	}
	if err := UpdateRef(ctx, dir, "refs/heads/main", c1, ""); err != nil {
		t.Fatalf("set c1: %v", err)
	}
	c2, err := CommitTree(ctx, dir, tree, "y2", c1)
	if err != nil {
		t.Fatalf("c2: %v", err)
	}
	// Bogus expected-old triggers a CAS failure.
	bogus := "0000000000000000000000000000000000000000"
	err = UpdateRef(ctx, dir, "refs/heads/main", c2, bogus)
	if err == nil {
		t.Fatalf("expected CAS failure with stale old oid")
	}
	// With the correct expected-old, the swap succeeds.
	if err := UpdateRef(ctx, dir, "refs/heads/main", c2, c1); err != nil {
		t.Fatalf("CAS with correct old: %v", err)
	}
}

func TestShowToplevelAndAbsoluteGitDir(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	top, err := ShowToplevel(ctx, dir)
	if err != nil {
		t.Fatalf("show-toplevel: %v", err)
	}
	if top == "" {
		t.Fatal("expected non-empty toplevel")
	}
	gd, err := AbsoluteGitDir(ctx, dir)
	if err != nil {
		t.Fatalf("absolute-git-dir: %v", err)
	}
	if gd == "" {
		t.Fatal("expected non-empty git dir")
	}
}
