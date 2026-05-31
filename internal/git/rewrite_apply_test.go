package git

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestApplyRewritePlanCreatesBackupAndRecreatesDescendants(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	old1 := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")
	old2 := commitWorktreePath(t, ctx, repo, "two.txt", "two\n", "two")
	old3 := commitWorktreePath(t, ctx, repo, "three.txt", "three\n", "three")

	res, err := ApplyRewritePlan(ctx, repo, RewriteApplyOptions{
		BranchRef:    "refs/heads/main",
		ExpectedHead: old3,
		PlanID:       "plan-1",
		Commits:      []RewriteApplyCommit{{OldOID: old1, ProposedMessage: "one rewritten"}, {OldOID: old2, ProposedMessage: "two rewritten"}},
		Now:          time.Date(2026, 5, 20, 1, 2, 3, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("ApplyRewritePlan: %v", err)
	}
	if res.BackupBranchRef != "refs/heads/acd-backup/rewrite-20260520-010203" || res.InternalBackupRef != "refs/acd/rewrite-backups/plan-1" {
		t.Fatalf("backup refs mismatch: %+v", res)
	}
	backupOID, err := RevParse(ctx, repo, res.BackupBranchRef)
	if err != nil || backupOID != old3 {
		t.Fatalf("backup ref oid=%s err=%v want %s", backupOID, err, old3)
	}
	newHead, err := RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	if newHead == old3 || newHead != res.NewHead || res.CommitMap[old3] != newHead {
		t.Fatalf("new head/mapping mismatch: head=%s res=%+v", newHead, res)
	}
	msgs := commitSubjectsNewestFirst(t, ctx, repo, "HEAD", 3)
	if strings.Join(msgs, ",") != "three,two rewritten,one rewritten" {
		t.Fatalf("subjects=%v", msgs)
	}
}

func TestApplyRewritePlanProgressCallbackOrder(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	old1 := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")
	old2 := commitWorktreePath(t, ctx, repo, "two.txt", "two\n", "two")
	old3 := commitWorktreePath(t, ctx, repo, "three.txt", "three\n", "three")

	var events []RewriteApplyProgress
	res, err := ApplyRewritePlan(ctx, repo, RewriteApplyOptions{
		BranchRef:    "refs/heads/main",
		ExpectedHead: old3,
		PlanID:       "plan-1",
		Commits:      []RewriteApplyCommit{{OldOID: old1, ProposedMessage: "one rewritten"}, {OldOID: old2, ProposedMessage: "two rewritten"}},
		Now:          time.Date(2026, 5, 20, 1, 2, 3, 0, time.UTC),
		Progress: func(event RewriteApplyProgress) error {
			events = append(events, event)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("ApplyRewritePlan: %v", err)
	}
	wantPhases := []string{"validate", "validate", "backup", "recreate_selected", "recreate_selected", "recreate_unchanged", "update_ref"}
	if len(events) != len(wantPhases) {
		t.Fatalf("events=%+v want phases %v", events, wantPhases)
	}
	for i, want := range wantPhases {
		if events[i].Phase != want {
			t.Fatalf("event %d phase=%q want %q; events=%+v", i, events[i].Phase, want, events)
		}
	}
	if events[2].BackupRef != res.BackupBranchRef {
		t.Fatalf("backup event=%+v result=%+v", events[2], res)
	}
	if events[3].OldOID != old1 || events[3].NewOID == "" || events[3].Current != 1 || events[3].Total != 2 {
		t.Fatalf("selected recreate event missing fields: %+v", events[3])
	}
	if events[5].OldOID != old3 || events[5].NewOID != res.NewHead {
		t.Fatalf("unchanged recreate event=%+v result=%+v", events[5], res)
	}
}

func TestApplyRewritePlanDryRunProgressIsValidationOnly(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	old1 := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")

	var events []RewriteApplyProgress
	res, err := ApplyRewritePlan(ctx, repo, RewriteApplyOptions{
		BranchRef:    "refs/heads/main",
		ExpectedHead: old1,
		Commits:      []RewriteApplyCommit{{OldOID: old1, ProposedMessage: "one rewritten"}},
		DryRun:       true,
		Progress: func(event RewriteApplyProgress) error {
			events = append(events, event)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("ApplyRewritePlan dry-run: %v", err)
	}
	if res.NewHead != "" || res.RecreatedCount != 0 {
		t.Fatalf("dry-run result mutated rewrite state: %+v", res)
	}
	if got := progressPhases(events); strings.Join(got, ",") != "validate,validate" {
		t.Fatalf("dry-run phases=%v want validation only; events=%+v", got, events)
	}
	head, err := RevParse(ctx, repo, "HEAD")
	if err != nil || head != old1 {
		t.Fatalf("dry-run changed HEAD=%s err=%v want %s", head, err, old1)
	}
}

func TestApplyRewritePlanPreservesOriginalAuthorMetadata(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	old1 := commitWorktreePathWithEnv(t, ctx, repo, "one.txt", "one\n", "one", map[string]string{
		"GIT_AUTHOR_NAME":  "Original Author",
		"GIT_AUTHOR_EMAIL": "author@example.com",
		"GIT_AUTHOR_DATE":  "2020-01-02T03:04:05+00:00",
	})

	_, err := ApplyRewritePlan(ctx, repo, RewriteApplyOptions{
		BranchRef:    "refs/heads/main",
		ExpectedHead: old1,
		Commits:      []RewriteApplyCommit{{OldOID: old1, ProposedMessage: "one rewritten"}},
	})
	if err != nil {
		t.Fatalf("ApplyRewritePlan: %v", err)
	}
	out, err := Run(ctx, RunOpts{Dir: repo, Timeout: DefaultReadTimeout}, "show", "-s", "--format=%an%x00%ae%x00%aI", "HEAD")
	if err != nil {
		t.Fatalf("git show author: %v", err)
	}
	got := strings.Split(strings.TrimSpace(string(out)), "\x00")
	want := []string{"Original Author", "author@example.com", "2020-01-02T03:04:05Z"}
	if len(got) != len(want) {
		t.Fatalf("author fields=%q", out)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("author field %d = %q, want %q (all=%v)", i, got[i], want[i], got)
		}
	}
}

func TestApplyRewritePlanRefusesMovedHeadWithoutBackup(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	old1 := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")
	old2 := commitWorktreePath(t, ctx, repo, "two.txt", "two\n", "two")
	_ = commitWorktreePath(t, ctx, repo, "three.txt", "three\n", "three")

	_, err := ApplyRewritePlan(ctx, repo, RewriteApplyOptions{
		BranchRef:    "refs/heads/main",
		ExpectedHead: old2,
		Commits:      []RewriteApplyCommit{{OldOID: old1, ProposedMessage: "one rewritten"}},
	})
	if err == nil || !strings.Contains(err.Error(), "HEAD moved") {
		t.Fatalf("error=%v, want HEAD moved refusal", err)
	}
	exists, err := RefExists(ctx, repo, "refs/heads/acd-backup/rewrite-20260520-010203")
	if err != nil || exists {
		t.Fatalf("backup exists after refusal=%v err=%v", exists, err)
	}
}

func progressPhases(events []RewriteApplyProgress) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		out = append(out, event.Phase)
	}
	return out
}

func commitWorktreePath(t *testing.T, ctx context.Context, repo, path, body, msg string) string {
	t.Helper()
	return commitWorktreePathWithEnv(t, ctx, repo, path, body, msg, nil)
}

func commitWorktreePathWithEnv(t *testing.T, ctx context.Context, repo, path, body, msg string, env map[string]string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(repo, path), []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if _, err := Run(ctx, RunOpts{Dir: repo, Timeout: DefaultWriteTimeout}, "add", path); err != nil {
		t.Fatalf("git add %s: %v", path, err)
	}
	if _, err := Run(ctx, RunOpts{Dir: repo, Timeout: DefaultWriteTimeout, ExtraEnv: env}, "commit", "-q", "-m", msg); err != nil {
		t.Fatalf("git commit %s: %v", path, err)
	}
	oid, err := RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	return oid
}

func commitSubjectsNewestFirst(t *testing.T, ctx context.Context, repo, rev string, n int) []string {
	t.Helper()
	out, err := Run(ctx, RunOpts{Dir: repo, Timeout: DefaultReadTimeout}, "log", "--format=%s", "-n", strconv.Itoa(n), rev)
	if err != nil {
		t.Fatalf("git log: %v", err)
	}
	var subjects []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line != "" {
			subjects = append(subjects, line)
		}
	}
	return subjects
}
