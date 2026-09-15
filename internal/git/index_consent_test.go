package git

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConsumeApprovedIndexPreservesChangedStagingAndRecovers(t *testing.T) {
	ctx := context.Background()
	repo := initRepo(t)
	run := func(args ...string) string {
		t.Helper()
		out, err := Run(ctx, RunOpts{Dir: repo}, args...)
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(out))
	}
	run("commit", "--allow-empty", "-m", "seed")
	head := run("rev-parse", "HEAD")
	path := filepath.Join(repo, "staged.txt")
	if err := os.WriteFile(path, []byte("approved"), 0600); err != nil {
		t.Fatal(err)
	}
	run("add", "staged.txt")
	approved, err := IndexContentDigest(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("new selection"), 0600); err != nil {
		t.Fatal(err)
	}
	run("add", "staged.txt")
	changed, err := IndexContentDigest(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	if err := ConsumeApprovedIndex(ctx, repo, approved, head); !errors.Is(err, ErrStagingChanged) {
		t.Fatalf("err=%v", err)
	}
	after, err := IndexContentDigest(ctx, repo)
	if err != nil || after != changed {
		t.Fatalf("index changed: %s %v", after, err)
	}
	if err := ConsumeApprovedIndex(ctx, repo, changed, head); err != nil {
		t.Fatal(err)
	}
	if err := ConsumeApprovedIndex(ctx, repo, changed, head); err != nil {
		t.Fatalf("recover completed replacement: %v", err)
	}
	if body, err := os.ReadFile(path); err != nil || string(body) != "new selection" {
		t.Fatalf("worktree=%q err=%v", body, err)
	}
	if got := run("diff", "--cached", "--name-only"); got != "" {
		t.Fatalf("still staged: %s", got)
	}
}

func TestConsumeApprovedIndexNeverRemovesAnotherGitLock(t *testing.T) {
	ctx := context.Background()
	repo := initRepo(t)
	if _, err := Run(ctx, RunOpts{Dir: repo}, "commit", "--allow-empty", "-m", "seed"); err != nil {
		t.Fatal(err)
	}
	head, err := Run(ctx, RunOpts{Dir: repo}, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	digest, err := IndexContentDigest(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(repo, ".git", "index.lock")
	if err := os.WriteFile(lock, []byte("other Git writer"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ConsumeApprovedIndex(ctx, repo, digest, strings.TrimSpace(string(head))); err == nil {
		t.Fatal("consumed while another Git writer held the lock")
	}
	if body, err := os.ReadFile(lock); err != nil || string(body) != "other Git writer" {
		t.Fatalf("other lock=%q err=%v", body, err)
	}
}

func TestConsumeApprovedIndexPreservesFlagsAndDetectsIntentToAdd(t *testing.T) {
	ctx := context.Background()
	repo := initRepo(t)
	run := func(args ...string) string {
		t.Helper()
		out, err := Run(ctx, RunOpts{Dir: repo}, args...)
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(out))
	}
	for _, name := range []string{"sparse", "assumed", "both"} {
		if err := os.WriteFile(filepath.Join(repo, name), []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	run("add", ".")
	run("commit", "-m", "seed")
	head := run("rev-parse", "HEAD")
	run("update-index", "--skip-worktree", "sparse", "both")
	run("update-index", "--assume-unchanged", "assumed", "both")
	if err := os.WriteFile(filepath.Join(repo, "empty"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	run("add", "-N", "empty")
	intent, err := IndexContentDigest(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	run("add", "empty")
	staged, err := IndexContentDigest(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	if intent == staged {
		t.Fatal("intent-to-add equals staged empty file")
	}
	if err := ConsumeApprovedIndex(ctx, repo, intent, head); !errors.Is(err, ErrStagingChanged) {
		t.Fatalf("err=%v", err)
	}
	if err := ConsumeApprovedIndex(ctx, repo, staged, head); err != nil {
		t.Fatal(err)
	}
	if got := run("ls-files", "-v"); got != "h assumed\ns both\nS sparse" {
		t.Fatalf("flags changed: %q", got)
	}
	if err := ConsumeApprovedIndex(ctx, repo, staged, head); err != nil {
		t.Fatalf("restart: %v", err)
	}
}
