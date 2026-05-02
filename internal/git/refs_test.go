package git

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
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

func TestIsAncestor_True(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	base := commitFile(t, ctx, dir, "base.txt", "base", "base")
	descendant := commitFile(t, ctx, dir, "descendant.txt", "descendant", "descendant", base)

	ok, err := IsAncestor(ctx, dir, base, descendant)
	if err != nil {
		t.Fatalf("is ancestor: %v", err)
	}
	if !ok {
		t.Fatal("expected base commit to be ancestor of descendant")
	}
}

func TestIsAncestor_False(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	base := commitFile(t, ctx, dir, "base.txt", "base", "base")
	main := commitFile(t, ctx, dir, "main.txt", "main", "main", base)
	divergent := commitFile(t, ctx, dir, "branch.txt", "branch", "branch", base)

	ok, err := IsAncestor(ctx, dir, divergent, main)
	if err != nil {
		t.Fatalf("is ancestor: %v", err)
	}
	if ok {
		t.Fatal("expected divergent commit not to be ancestor of main commit")
	}
}

func TestIsAncestor_BadOID(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	base := commitFile(t, ctx, dir, "base.txt", "base", "base")

	ok, err := IsAncestor(ctx, dir, "not-an-oid", base)
	if err == nil {
		t.Fatal("expected malformed oid to return an error")
	}
	if ok {
		t.Fatal("expected malformed oid not to be reported as ancestor")
	}
	var gerr *Error
	if !errors.As(err, &gerr) {
		t.Fatalf("expected *git.Error, got %T: %v", err, err)
	}
	if gerr.ExitCode != 128 {
		t.Fatalf("expected exit 128 for bad oid, got %d", gerr.ExitCode)
	}
}

func TestIsAncestor_RepoMissing(t *testing.T) {
	requireGit(t)
	ctx := context.Background()
	missingRepo := filepath.Join(t.TempDir(), "missing")

	ok, err := IsAncestor(ctx, missingRepo, "HEAD", "HEAD")
	if err == nil {
		t.Fatal("expected missing repo to return an error")
	}
	if ok {
		t.Fatal("expected missing repo not to be reported as ancestor")
	}
	var gerr *Error
	if !errors.As(err, &gerr) {
		t.Fatalf("expected *git.Error, got %T: %v", err, err)
	}
}

// TestRevParse_DisambiguatesAmbiguousRef proves the exit-1 disambiguation
// contract: when git emits an "ambiguous" warning to stderr at exit 1, we
// must surface it as a real error rather than coercing it to ErrRefNotFound.
//
// Pre-fix `--quiet` swallowed the ambiguity warning and ExitCode==1 mapped
// unconditionally to ErrRefNotFound, hiding misconfiguration. With --quiet
// dropped and the empty-stderr predicate added, the ambiguous case surfaces
// as a *git.Error whose stderr contains the warning.
//
// Setup: create a branch named "foo" AND a tag named "foo" pointing at the
// same commit. `git rev-parse --verify foo` exits 1 with an
// "ambiguous"/"refers to multiple objects" warning to stderr.
func TestRevParse_DisambiguatesAmbiguousRef(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()

	// Build a commit so we have something to reference.
	blob, err := HashObjectStdin(ctx, dir, []byte("hello"))
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	tree, err := Mktree(ctx, dir, []MktreeEntry{
		{Mode: "100644", Type: "blob", OID: blob, Path: "hello"},
	})
	if err != nil {
		t.Fatalf("mktree: %v", err)
	}
	commit, err := CommitTree(ctx, dir, tree, "seed")
	if err != nil {
		t.Fatalf("commit-tree: %v", err)
	}
	if err := UpdateRef(ctx, dir, "refs/heads/main", commit, ""); err != nil {
		t.Fatalf("update-ref main: %v", err)
	}
	// Create a branch and a tag with the same short name.
	if err := UpdateRef(ctx, dir, "refs/heads/foo", commit, ""); err != nil {
		t.Fatalf("update-ref refs/heads/foo: %v", err)
	}
	if err := UpdateRef(ctx, dir, "refs/tags/foo", commit, ""); err != nil {
		t.Fatalf("update-ref refs/tags/foo: %v", err)
	}

	_, err = RevParse(ctx, dir, "foo")
	if err == nil {
		t.Fatal("expected ambiguous-ref error, got nil")
	}
	if errors.Is(err, ErrRefNotFound) {
		t.Fatalf("ambiguous ref incorrectly mapped to ErrRefNotFound: %v", err)
	}
	// The error string must surface git's ambiguity warning so callers can
	// diagnose the misconfiguration.
	if !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("expected error to mention 'ambiguous', got: %v", err)
	}
	var gerr *Error
	if !errors.As(err, &gerr) {
		t.Fatalf("expected *git.Error, got %T: %v", err, err)
	}
	if gerr.ExitCode == 0 {
		t.Fatalf("expected non-zero exit on ambiguous ref, got %d", gerr.ExitCode)
	}
}

func commitFile(t *testing.T, ctx context.Context, dir, path, content, message string, parents ...string) string {
	t.Helper()
	blob, err := HashObjectStdin(ctx, dir, []byte(content))
	if err != nil {
		t.Fatalf("hash %s: %v", path, err)
	}
	tree, err := Mktree(ctx, dir, []MktreeEntry{
		{Mode: RegularFileMode, Type: "blob", OID: blob, Path: path},
	})
	if err != nil {
		t.Fatalf("mktree %s: %v", path, err)
	}
	commit, err := CommitTree(ctx, dir, tree, message, parents...)
	if err != nil {
		t.Fatalf("commit-tree %s: %v", path, err)
	}
	return commit
}
