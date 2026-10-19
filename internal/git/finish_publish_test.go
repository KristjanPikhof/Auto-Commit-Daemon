package git

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func finishGitRun(t *testing.T, repo string, args ...string) string {
	t.Helper()
	out, err := Run(context.Background(), RunOpts{Dir: repo}, args...)
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(out))
}

func finishTree(t *testing.T, repo string, files map[string]string) string {
	t.Helper()
	entries := make([]IndexEntry, 0, len(files))
	for path, body := range files {
		oid, err := HashObjectStdinDurable(context.Background(), repo, []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, IndexEntry{Mode: "100644", OID: oid, Path: path})
	}
	tree, err := WriteTreeDurable(context.Background(), repo, filepath.Join(t.TempDir(), "index"), entries)
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

func finishGitFixture(t *testing.T, staged bool) (string, FinishBuildOptions) {
	t.Helper()
	repo := initRepo(t)
	finishGitRun(t, repo, "commit", "--allow-empty", "-m", "Initial")
	head := finishGitRun(t, repo, "rev-parse", "HEAD")
	first := finishTree(t, repo, map[string]string{"a.txt": "approved\n"})
	last := finishTree(t, repo, map[string]string{"a.txt": "approved\n", "b.txt": "second\n"})
	if staged {
		if err := os.WriteFile(filepath.Join(repo, "a.txt"), []byte("approved\n"), 0600); err != nil {
			t.Fatal(err)
		}
		finishGitRun(t, repo, "add", "a.txt")
	}
	digest, err := IndexContentDigest(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	return repo, FinishBuildOptions{RequestID: "request", Attempt: 1, WorktreeID: "0123456789abcdef", BranchRef: "refs/heads/main", ExpectedHead: head, TargetTree: last, IndexDigest: digest, IncludeStaged: staged, ObjectRef: FinishRefPrefix + "request/1", Commits: []FinishCommit{{TreeOID: first, Message: "Add first goal"}, {TreeOID: last, Message: "Add second goal"}}}
}

func buildFinishFixture(t *testing.T, repo string, opts FinishBuildOptions) FinishPublicationProof {
	t.Helper()
	p, err := BuildFinishChain(context.Background(), repo, opts)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFinishPublicationOneCASPreservesWorktree(t *testing.T) {
	ctx := context.Background()
	for _, staged := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "approved_staging"}[staged], func(t *testing.T) {
			repo, opts := finishGitFixture(t, staged)
			before := finishGitRun(t, repo, "reflog", "show", "--format=%H", "refs/heads/main")
			verified := 0
			opts.Verify = func(_ context.Context, _ int, _, _ string) error {
				verified++
				if got := finishGitRun(t, repo, "rev-parse", "HEAD"); got != opts.ExpectedHead {
					t.Fatal("candidate escaped private chain")
				}
				return nil
			}
			p := buildFinishFixture(t, repo, opts)
			if verified != 2 {
				t.Fatalf("verified %d candidates", verified)
			}
			if err := os.WriteFile(filepath.Join(repo, "a.txt"), []byte("later edit\n"), 0600); err != nil {
				t.Fatal(err)
			}
			called := false
			result, err := ApplyFinishPublication(ctx, repo, p, false, func(result FinishPublicationResult) error {
				called = true
				if !result.Published || !result.IndexReconciled {
					t.Fatalf("journal result %+v", result)
				}
				for _, name := range []string{"index.lock", "HEAD.lock"} {
					if _, err := os.Stat(filepath.Join(p.GitDir, name)); err != nil {
						t.Fatalf("journal completed without %s: %v", name, err)
					}
				}
				return nil
			})
			if !called {
				t.Fatal("journal completion callback not called")
			}
			if err != nil || !result.Published || !result.IndexReconciled || result.StagedConsumed != staged || result.CommitsCreated != 2 {
				t.Fatalf("result %+v: %v", result, err)
			}
			after := finishGitRun(t, repo, "reflog", "show", "--format=%H", "refs/heads/main")
			if after != p.NewHead+"\n"+before {
				t.Fatalf("branch moved more than once: %q", after)
			}
			if body, _ := os.ReadFile(filepath.Join(repo, "a.txt")); string(body) != "later edit\n" {
				t.Fatalf("live file overwritten: %q", body)
			}
			if stagedNow := finishGitRun(t, repo, "diff", "--cached", "--name-only"); stagedNow != "" {
				t.Fatalf("unreconciled index: %s", stagedNow)
			}
		})
	}
}

func TestFinishPublicationVerificationFailureIsPrivate(t *testing.T) {
	repo, opts := finishGitFixture(t, false)
	rejected := errors.New("candidate verification failed")
	opts.Verify = func(_ context.Context, n int, _, _ string) error {
		if n == 1 {
			return rejected
		}
		return nil
	}
	if _, err := BuildFinishChain(context.Background(), repo, opts); !errors.Is(err, rejected) {
		t.Fatalf("verification: %v", err)
	}
	if got := finishGitRun(t, repo, "rev-parse", "HEAD"); got != opts.ExpectedHead {
		t.Fatal("partial publication")
	}
	if _, err := RevParse(context.Background(), repo, opts.ObjectRef); !errors.Is(err, ErrRefNotFound) {
		t.Fatalf("failed chain pinned: %v", err)
	}
	if digest, _ := IndexContentDigest(context.Background(), repo); digest != opts.IndexDigest {
		t.Fatal("live index changed")
	}
}

func TestFinishPublicationCompletionFailureCanRecover(t *testing.T) {
	repo, opts := finishGitFixture(t, false)
	p := buildFinishFixture(t, repo, opts)
	journalErr := errors.New("journal unavailable")
	result, err := ApplyFinishPublication(context.Background(), repo, p, false, func(FinishPublicationResult) error {
		return journalErr
	})
	if !errors.Is(err, journalErr) || !result.Published || !result.IndexReconciled {
		t.Fatalf("completion failure %+v: %v", result, err)
	}
	before := finishGitRun(t, repo, "reflog", "show", "--format=%H", "refs/heads/main")
	called := false
	result, err = ApplyFinishPublication(context.Background(), repo, p, true, func(FinishPublicationResult) error {
		called = true
		return nil
	})
	if err != nil || !called || !result.Published || !result.IndexReconciled {
		t.Fatalf("recovery %+v called=%t: %v", result, called, err)
	}
	if after := finishGitRun(t, repo, "reflog", "show", "--format=%H", "refs/heads/main"); after != before {
		t.Fatal("journal recovery published again")
	}
	for _, name := range []string{"index.lock", "HEAD.lock"} {
		if _, err := os.Stat(filepath.Join(p.GitDir, name)); !os.IsNotExist(err) {
			t.Fatalf("recovery left %s: %v", name, err)
		}
	}
}

func TestFinishLockPreservesFilesystemError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.lock")
	if err := os.WriteFile(path, []byte("another Git writer"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := acquireFinishLock(path, strings.Repeat("a", 64), false)
	if !errors.Is(err, ErrFinishLock) || !errors.Is(err, os.ErrExist) {
		t.Fatalf("missing lock or filesystem cause: %v", err)
	}
	if body, err := os.ReadFile(path); err != nil || string(body) != "another Git writer" {
		t.Fatalf("foreign lock changed: %q: %v", body, err)
	}
}

func TestFinishPublicationCrashRecovery(t *testing.T) {
	for _, boundary := range []string{"locked", "before_cas", "after_cas", "before_index", "after_index"} {
		t.Run(boundary, func(t *testing.T) {
			repo, opts := finishGitFixture(t, true)
			p := buildFinishFixture(t, repo, opts)
			before := finishGitRun(t, repo, "reflog", "show", "--format=%H", "refs/heads/main")
			crash := errors.New("worker stopped")
			_, err := applyFinishPublication(context.Background(), repo, p, false, nil, func(at string) error {
				if at == boundary {
					return crash
				}
				return nil
			})
			if !errors.Is(err, crash) {
				t.Fatalf("crash boundary: %v", err)
			}
			for _, name := range []string{"index.lock", "HEAD.lock"} {
				if name == "HEAD.lock" && (boundary == "before_index" || boundary == "after_index") {
					continue // Git's prepared transaction owns HEAD.lock at these boundaries.
				}
				body, err := os.ReadFile(filepath.Join(p.GitDir, name))
				if err != nil || string(body) != "acd finish "+p.LockToken+"\n" {
					t.Fatalf("owned lock %s: %q %v", name, body, err)
				}
			}
			result, err := ApplyFinishPublication(context.Background(), repo, p, true, nil)
			if err != nil || !result.Published || !result.IndexReconciled {
				t.Fatalf("resume %+v: %v", result, err)
			}
			result, err = ApplyFinishPublication(context.Background(), repo, p, true, nil)
			if err != nil || !result.IndexReconciled {
				t.Fatalf("idempotent resume %+v: %v", result, err)
			}
			if got := finishGitRun(t, repo, "reflog", "show", "--format=%H", "refs/heads/main"); got != p.NewHead+"\n"+before {
				t.Fatalf("replayed branch CAS: %q", got)
			}
		})
	}
}

func TestFinishPublicationPreservesLaterStaging(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_cas", true: "after_cas"}[published], func(t *testing.T) {
			repo, opts := finishGitFixture(t, true)
			p := buildFinishFixture(t, repo, opts)
			if published {
				_, _ = applyFinishPublication(context.Background(), repo, p, false, nil, func(at string) error {
					if at == "after_cas" {
						return errors.New("stop")
					}
					return nil
				})
				for _, name := range []string{"index.lock", "HEAD.lock"} {
					release, err := acquireFinishLock(filepath.Join(p.GitDir, name), p.LockToken, true)
					if err != nil {
						t.Fatal(err)
					}
					release()
				}
			}
			if err := os.WriteFile(filepath.Join(repo, "a.txt"), []byte("new staged work\n"), 0600); err != nil {
				t.Fatal(err)
			}
			finishGitRun(t, repo, "add", "a.txt")
			later, _ := IndexContentDigest(context.Background(), repo)
			result, err := ApplyFinishPublication(context.Background(), repo, p, true, nil)
			if !errors.Is(err, ErrStagingChanged) || result.Published != published || result.IndexReconciled {
				t.Fatalf("later staging %+v: %v", result, err)
			}
			if digest, _ := IndexContentDigest(context.Background(), repo); digest != later {
				t.Fatal("later staging cleared")
			}
			want := opts.ExpectedHead
			if published {
				want = p.NewHead
			}
			if head := finishGitRun(t, repo, "rev-parse", "HEAD"); head != want {
				t.Fatal("unexpected branch movement")
			}
		})
	}
}

func TestFinishPublicationUnsafeOwnership(t *testing.T) {
	for _, scenario := range []string{"staged", "intent_to_add", "unknown_lock", "unknown_head_lock", "symbolic_branch", "symbolic_branch_after_cas", "linked_worktree", "operation", "conflict", "detached", "symlink_head", "moved_branch"} {
		t.Run(scenario, func(t *testing.T) {
			repo, opts := finishGitFixture(t, false)
			p := buildFinishFixture(t, repo, opts)
			want := ErrFinishUnsafe
			switch scenario {
			case "staged", "intent_to_add":
				if err := os.WriteFile(filepath.Join(repo, "later.txt"), []byte("later"), 0600); err != nil {
					t.Fatal(err)
				}
				args := []string{"add", "later.txt"}
				if scenario == "intent_to_add" {
					args = []string{"add", "-N", "later.txt"}
				}
				finishGitRun(t, repo, args...)
				opts.IndexDigest, _ = IndexContentDigest(context.Background(), repo)
				if err := CheckFinishPublicationSafety(context.Background(), repo, opts.BranchRef, opts.ExpectedHead, opts.IndexDigest, false); !errors.Is(err, ErrFinishStaged) {
					t.Fatalf("staged refusal: %v", err)
				}
				want = ErrStagingChanged
			case "unknown_lock", "unknown_head_lock":
				name := "index.lock"
				if scenario == "unknown_head_lock" {
					name = "HEAD.lock"
				}
				if err := os.WriteFile(filepath.Join(p.GitDir, name), []byte("another owner"), 0600); err != nil {
					t.Fatal(err)
				}
				want = ErrFinishLock
			case "symbolic_branch", "symbolic_branch_after_cas":
				if scenario == "symbolic_branch_after_cas" {
					if _, err := ApplyFinishPublication(context.Background(), repo, p, false, nil); err != nil {
						t.Fatal(err)
					}
				}
				finishGitRun(t, repo, "update-ref", "refs/heads/real", finishGitRun(t, repo, "rev-parse", "HEAD"))
				finishGitRun(t, repo, "symbolic-ref", "refs/heads/main", "refs/heads/real")
			case "linked_worktree":
				finishGitRun(t, repo, "worktree", "add", "--force", filepath.Join(t.TempDir(), "linked"), "main")
			case "operation":
				if err := os.WriteFile(filepath.Join(p.GitDir, "MERGE_HEAD"), []byte(opts.ExpectedHead+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "conflict":
				oid, err := HashObjectStdinDurable(context.Background(), repo, []byte("conflict"))
				if err != nil {
					t.Fatal(err)
				}
				_, err = Run(context.Background(), RunOpts{Dir: repo, Stdin: strings.NewReader("100644 " + oid + " 1\tconflict.txt\n")}, "update-index", "--index-info")
				if err != nil {
					t.Fatal(err)
				}
				// Either the changed staging identity or unmerged entries rejects it.
				conflicted, err := IndexContentDigest(context.Background(), repo)
				if err != nil {
					t.Fatal(err)
				}
				if err := CheckFinishPublicationSafety(context.Background(), repo, opts.BranchRef, opts.ExpectedHead, conflicted, true); !errors.Is(err, ErrFinishUnsafe) {
					t.Fatalf("unmerged index accepted: %v", err)
				}
				want = ErrStagingChanged
			case "detached":
				if err := os.WriteFile(filepath.Join(p.GitDir, "HEAD"), []byte(opts.ExpectedHead+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink_head":
				if err := os.Rename(filepath.Join(p.GitDir, "HEAD"), filepath.Join(p.GitDir, "head-source")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("head-source", filepath.Join(p.GitDir, "HEAD")); err != nil {
					t.Fatal(err)
				}
				want = ErrNotWorktree
			case "moved_branch":
				finishGitRun(t, repo, "commit", "--allow-empty", "-m", "Other work")
			}
			_, err := ApplyFinishPublication(context.Background(), repo, p, true, nil)
			if !errors.Is(err, want) {
				t.Fatalf("unsafe %s: %v", scenario, err)
			}
			if scenario == "unknown_lock" || scenario == "unknown_head_lock" {
				name := "index.lock"
				if scenario == "unknown_head_lock" {
					name = "HEAD.lock"
				}
				body, _ := os.ReadFile(filepath.Join(p.GitDir, name))
				if string(body) != "another owner" {
					t.Fatal("unknown lock removed")
				}
			}
			if strings.HasPrefix(scenario, "symbolic_branch") {
				if got := finishGitRun(t, repo, "symbolic-ref", "refs/heads/main"); got != "refs/heads/real" {
					t.Fatal("alias replaced")
				}
			}
		})
	}
}

func TestFinishPublicationRejectsHEADReplacementAtPrepare(t *testing.T) {
	repo, opts := finishGitFixture(t, false)
	p := buildFinishFixture(t, repo, opts)
	// A prepared hook replaces HEAD with the same text, hiding an away-and-back
	// branch change from a literal branch-name check. Exact file identity rejects it.
	hook := filepath.Join(p.GitDir, "hooks", "reference-transaction")
	body := "#!/bin/sh\nif [ \"$1\" = prepared ]; then\n printf 'ref: refs/heads/main\\n' > .git/finish-head-replacement\n mv .git/finish-head-replacement .git/HEAD\nfi\n"
	if err := os.WriteFile(hook, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	result, err := ApplyFinishPublication(context.Background(), repo, p, false, nil)
	if !errors.Is(err, ErrFinishUnsafe) || result.Published {
		t.Fatalf("changed HEAD identity %+v: %v", result, err)
	}
	if head := finishGitRun(t, repo, "rev-parse", "HEAD"); head != opts.ExpectedHead {
		t.Fatal("branch moved after HEAD replacement")
	}
}

func TestFinishPublicationRejectsSecondWorktreeAtPrepare(t *testing.T) {
	repo, opts := finishGitFixture(t, false)
	p := buildFinishFixture(t, repo, opts)
	linked := filepath.Join(t.TempDir(), "linked")
	quoted := "'" + strings.ReplaceAll(linked, "'", "'\\''") + "'"
	hook := filepath.Join(p.GitDir, "hooks", "reference-transaction")
	body := "#!/bin/sh\nset -e\nif [ \"$1\" = prepared ]; then\n unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE\n git -c core.hooksPath=/dev/null worktree add --force --detach " + quoted + " HEAD\n git -C " + quoted + " -c core.hooksPath=/dev/null symbolic-ref HEAD refs/heads/main\nfi\n"
	if err := os.WriteFile(hook, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	result, err := ApplyFinishPublication(context.Background(), repo, p, false, nil)
	if !errors.Is(err, ErrFinishUnsafe) || result.Published {
		t.Fatalf("second owner during prepare %+v: %v", result, err)
	}
	if head := finishGitRun(t, repo, "rev-parse", "HEAD"); head != opts.ExpectedHead {
		t.Fatal("published despite second worktree owner")
	}
}

func TestFinishPublicationUnbornBranch(t *testing.T) {
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			repo := t.TempDir()
			finishGitRun(t, repo, "init", "--object-format="+format)
			finishGitRun(t, repo, "symbolic-ref", "HEAD", "refs/heads/main")
			finishGitRun(t, repo, "config", "user.name", "ACD Test")
			finishGitRun(t, repo, "config", "user.email", "test@example.com")
			tree := finishTree(t, repo, map[string]string{"root.txt": "root"})
			digest, err := IndexContentDigest(context.Background(), repo)
			if err != nil {
				t.Fatal(err)
			}
			opts := FinishBuildOptions{RequestID: "root", Attempt: 1, WorktreeID: "0123456789abcdef", BranchRef: "refs/heads/main", TargetTree: tree, IndexDigest: digest, ObjectRef: FinishRefPrefix + "root/1", Commits: []FinishCommit{{TreeOID: tree, Message: "Add root", AuthorName: "Original Author", AuthorEmail: "original@example.com", AuthorDate: "2025-01-02T03:04:05Z"}}}
			p := buildFinishFixture(t, repo, opts)
			result, err := ApplyFinishPublication(context.Background(), repo, p, false, nil)
			if err != nil || !result.IndexReconciled {
				t.Fatalf("root %+v: %v", result, err)
			}
			if parents, err := parentsOf(context.Background(), repo, p.NewHead); err != nil || len(parents) != 0 {
				t.Fatalf("root parents %v: %v", parents, err)
			}
			if author := finishGitRun(t, repo, "show", "-s", "--format=%an <%ae>", "HEAD"); author != "Original Author <original@example.com>" {
				t.Fatalf("author %q", author)
			}
		})
	}
}
