package git

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLatestBranchCommitSummaries_ReturnsNewestSubjects(t *testing.T) {
	dir := initRepoWithMain(t)
	ctx := context.Background()

	first := commitWorktreeFile(t, ctx, dir, "a.txt", "a\n", "first change")
	second := commitWorktreeFile(t, ctx, dir, "b.txt", "b\n", "second change")
	third := commitWorktreeFile(t, ctx, dir, "c.txt", "c\n", "third change")

	got, err := LatestBranchCommitSummaries(ctx, dir, "HEAD", 2)
	if err != nil {
		t.Fatalf("latest branch commits: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("commits=%d want 2: %#v", len(got), got)
	}
	if got[0].ShortOID != third[:12] || got[0].Subject != "third change" {
		t.Fatalf("newest commit mismatch: %#v", got[0])
	}
	if got[1].ShortOID != second[:12] || got[1].Subject != "second change" {
		t.Fatalf("second commit mismatch: %#v", got[1])
	}
	if strings.Contains(strings.Join(FormatCommitSummaries(got), "\n"), first[:12]) {
		t.Fatalf("limit included oldest commit: %#v", got)
	}
}

func TestLatestBranchCommitSummaries_EmptyHistoryReturnsWarningError(t *testing.T) {
	dir := initRepoWithMain(t)
	ctx := context.Background()

	got, err := LatestBranchCommitSummaries(ctx, dir, "HEAD", 4)
	if len(got) != 0 {
		t.Fatalf("commits=%#v want empty", got)
	}
	if !errors.Is(err, ErrRefNotFound) {
		t.Fatalf("expected ErrRefNotFound, got %v", err)
	}
}

func TestLatestBranchCommitSummaries_AmbiguousRefReturnsWarningError(t *testing.T) {
	dir := initRepoWithMain(t)
	ctx := context.Background()
	commit := commitWorktreeFile(t, ctx, dir, "seed.txt", "seed\n", "seed")
	if err := UpdateRef(ctx, dir, "refs/heads/foo", commit, ""); err != nil {
		t.Fatalf("update branch foo: %v", err)
	}
	if err := UpdateRef(ctx, dir, "refs/tags/foo", commit, ""); err != nil {
		t.Fatalf("update tag foo: %v", err)
	}

	got, err := LatestBranchCommitSummaries(ctx, dir, "foo", 1)
	if len(got) != 0 {
		t.Fatalf("commits=%#v want empty", got)
	}
	if !errors.Is(err, ErrRefAmbiguous) {
		t.Fatalf("expected ErrRefAmbiguous, got %v", err)
	}
	if !strings.Contains(err.Error(), "foo") {
		t.Fatalf("warning error should mention ref, got %v", err)
	}
}

func TestLatestPathCommitSummaries_ReturnsTouchedRelevance(t *testing.T) {
	dir := initRepoWithMain(t)
	ctx := context.Background()
	commitWorktreeFile(t, ctx, dir, "README.md", "readme\n", "docs seed")
	daemonCommit := commitWorktreeFile(t, ctx, dir, "internal/daemon/message.go", "package daemon\n", "daemon context")
	gitCommit := commitWorktreeFile(t, ctx, dir, "internal/git/history.go", "package git\n", "git context")
	commitWorktreeFile(t, ctx, dir, "cmd/acd/main.go", "package main\n", "cli unrelated")

	got, err := LatestPathCommitSummaries(ctx, dir, "HEAD", []string{"internal/git", "README.md", "internal/daemon"}, 5)
	if err != nil {
		t.Fatalf("latest path commits: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("commits=%d want 3: %#v", len(got), got)
	}
	if got[0].ShortOID != gitCommit[:12] || got[0].Subject != "git context" {
		t.Fatalf("git context commit mismatch: %#v", got[0])
	}
	assertStringSliceEqual(t, got[0].TouchedPaths, []string{"internal/git/history.go"})
	if got[1].ShortOID != daemonCommit[:12] || got[1].Subject != "daemon context" {
		t.Fatalf("daemon context commit mismatch: %#v", got[1])
	}
	assertStringSliceEqual(t, got[1].TouchedPaths, []string{"internal/daemon/message.go"})
	if got[2].Subject != "docs seed" {
		t.Fatalf("readme commit mismatch: %#v", got[2])
	}
	assertStringSliceEqual(t, got[2].TouchedPaths, []string{"README.md"})
}

func TestLatestPathCommitSummaries_TreatsPathspecMagicAsLiteral(t *testing.T) {
	for _, tc := range []struct {
		name       string
		literal    string
		distractor string
	}{
		{
			name:       "star",
			literal:    "literal*.txt",
			distractor: "literal-any.txt",
		},
		{
			name:       "bracket",
			literal:    "bracket[abc].txt",
			distractor: "bracketa.txt",
		},
		{
			name:       "colon_magic",
			literal:    ":(top)colon.txt",
			distractor: "colon.txt",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := initRepoWithMain(t)
			ctx := context.Background()
			literalCommit := commitWorktreeFileLiteral(t, ctx, dir, tc.literal, "literal\n", "literal path")
			commitWorktreeFileLiteral(t, ctx, dir, tc.distractor, "distractor\n", "distractor path")

			got, err := LatestPathCommitSummaries(ctx, dir, "HEAD", []string{tc.literal}, 5)
			if err != nil {
				t.Fatalf("latest path commits: %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("commits=%d want 1 literal match: %#v", len(got), got)
			}
			if got[0].ShortOID != literalCommit[:12] || got[0].Subject != "literal path" {
				t.Fatalf("literal commit mismatch: %#v want oid prefix %s subject %q", got[0], literalCommit[:12], "literal path")
			}
			assertStringSliceEqual(t, got[0].TouchedPaths, []string{tc.literal})
		})
	}
}

func TestLatestPathCommitSummaries_MissingPathReturnsEmpty(t *testing.T) {
	dir := initRepoWithMain(t)
	ctx := context.Background()
	commitWorktreeFile(t, ctx, dir, "present.txt", "present\n", "present")

	got, err := LatestPathCommitSummaries(ctx, dir, "HEAD", []string{"missing"}, 4)
	if err != nil {
		t.Fatalf("latest path commits: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("commits=%#v want empty", got)
	}
}

func TestLatestCommitSummaries_KeepContextCompact(t *testing.T) {
	dir := initRepoWithMain(t)
	ctx := context.Background()
	longSubject := strings.Repeat("subject ", 80)
	commitWorktreeFile(t, ctx, dir, "long.txt", "long\n", longSubject)

	branch, err := LatestBranchCommitSummaries(ctx, dir, "HEAD", 1)
	if err != nil {
		t.Fatalf("latest branch commits: %v", err)
	}
	if len(branch) != 1 {
		t.Fatalf("commits=%d want 1", len(branch))
	}
	if len(branch[0].Subject) > HistorySubjectByteCap {
		t.Fatalf("subject length=%d want <= %d", len(branch[0].Subject), HistorySubjectByteCap)
	}

	for i := 0; i < MaxHistoryTouchedPaths+8; i++ {
		path := fmt.Sprintf("bulk/file-%02d.txt", i)
		writeFile(t, filepath.Join(dir, path), "bulk\n")
	}
	if _, err := Run(ctx, RunOpts{Dir: dir, Timeout: DefaultWriteTimeout}, "add", "bulk"); err != nil {
		t.Fatalf("git add bulk: %v", err)
	}
	if _, err := Run(ctx, RunOpts{Dir: dir, Timeout: DefaultWriteTimeout}, "commit", "-q", "-m", "bulk paths"); err != nil {
		t.Fatalf("git commit bulk: %v", err)
	}

	pathCommits, err := LatestPathCommitSummaries(ctx, dir, "HEAD", []string{"bulk"}, 1)
	if err != nil {
		t.Fatalf("latest path commits: %v", err)
	}
	if len(pathCommits) != 1 {
		t.Fatalf("commits=%d want 1", len(pathCommits))
	}
	if len(pathCommits[0].TouchedPaths) != MaxHistoryTouchedPaths {
		t.Fatalf("touched paths=%d want %d: %#v", len(pathCommits[0].TouchedPaths), MaxHistoryTouchedPaths, pathCommits[0].TouchedPaths)
	}
	for _, path := range pathCommits[0].TouchedPaths {
		if len(path) > HistoryPathByteCap {
			t.Fatalf("path %q length=%d want <= %d", path, len(path), HistoryPathByteCap)
		}
	}
}

func initRepoWithMain(t *testing.T) string {
	t.Helper()
	dir := initRepo(t)
	if _, err := Run(context.Background(), RunOpts{Dir: dir, Timeout: DefaultWriteTimeout}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref main: %v", err)
	}
	return dir
}

func commitWorktreeFile(t *testing.T, ctx context.Context, dir, path, content, message string) string {
	t.Helper()
	writeFile(t, filepath.Join(dir, path), content)
	if _, err := Run(ctx, RunOpts{Dir: dir, Timeout: DefaultWriteTimeout}, "add", path); err != nil {
		t.Fatalf("git add %s: %v", path, err)
	}
	if _, err := Run(ctx, RunOpts{Dir: dir, Timeout: DefaultWriteTimeout}, "commit", "-q", "-m", message); err != nil {
		t.Fatalf("git commit %s: %v", path, err)
	}
	head, err := RevParse(ctx, dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	return head
}

func commitWorktreeFileLiteral(t *testing.T, ctx context.Context, dir, path, content, message string) string {
	t.Helper()
	writeFile(t, filepath.Join(dir, path), content)
	if _, err := Run(ctx, RunOpts{Dir: dir, Timeout: DefaultWriteTimeout}, "--literal-pathspecs", "add", "--", path); err != nil {
		t.Fatalf("git add literal %s: %v", path, err)
	}
	if _, err := Run(ctx, RunOpts{Dir: dir, Timeout: DefaultWriteTimeout}, "commit", "-q", "-m", message); err != nil {
		t.Fatalf("git commit literal %s: %v", path, err)
	}
	head, err := RevParse(ctx, dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	return head
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func assertStringSliceEqual(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("slice length=%d want %d: got %#v want %#v", len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("slice[%d]=%q want %q (got %#v want %#v)", i, got[i], want[i], got, want)
		}
	}
}
