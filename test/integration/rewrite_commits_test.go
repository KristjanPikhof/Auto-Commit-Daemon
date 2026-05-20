//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRewriteCommitsSelectionCLI(t *testing.T) {
	repo := tempRepo(t)
	for _, spec := range []struct{ path, body, msg string }{
		{"one.txt", "one\n", "one"},
		{"two.txt", "two\n", "two"},
		{"three.txt", "three\n", "three"},
	} {
		if err := os.WriteFile(filepath.Join(repo, spec.path), []byte(spec.body), 0o644); err != nil {
			t.Fatalf("write %s: %v", spec.path, err)
		}
		runGitOK(t, repo, "add", spec.path)
		runGitOK(t, repo, "commit", "-q", "-m", spec.msg)
	}

	res := runAcd(t, context.Background(), withIsolatedHome(t), "--repo", repo, "--json", "rewrite-commits", "--range", "2-3")
	if res.ExitCode != 0 {
		t.Fatalf("acd rewrite-commits failed exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	var got struct {
		OK       bool `json:"ok"`
		Selected []struct {
			Subject string `json:"subject"`
		} `json:"selected"`
		RecreateUnchanged []struct {
			Subject string `json:"subject"`
		} `json:"recreate_unchanged"`
		SelectedPositions string `json:"selected_positions"`
	}
	if err := json.Unmarshal([]byte(res.Stdout), &got); err != nil {
		t.Fatalf("decode json: %v\n%s", err, res.Stdout)
	}
	if !got.OK || got.SelectedPositions != "2-3" {
		t.Fatalf("unexpected report: %+v", got)
	}
	if len(got.Selected) != 2 || got.Selected[0].Subject != "one" || got.Selected[1].Subject != "two" {
		t.Fatalf("selected subjects mismatch: %+v", got.Selected)
	}
	if len(got.RecreateUnchanged) != 1 || got.RecreateUnchanged[0].Subject != "three" {
		t.Fatalf("recreated subjects mismatch: %+v", got.RecreateUnchanged)
	}
}

func TestRewriteCommitsApplyPlanCLI(t *testing.T) {
	repo := tempRepo(t)
	one := rewriteIntegrationCommit(t, repo, "one.txt", "one\n", "one")
	two := rewriteIntegrationCommit(t, repo, "two.txt", "two\n", "two")
	three := rewriteIntegrationCommit(t, repo, "three.txt", "three\n", "three")
	planPath := filepath.Join(t.TempDir(), "rewrite.json")
	writeIntegrationRewritePlan(t, planPath, state.RewritePlan{
		ID:               "rp_integration",
		BranchRef:        "refs/heads/main",
		ExpectedHead:     three,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits: []state.RewritePlanCommit{
			{OldOID: one, ProposedMessage: "one rewritten", OriginalMessage: "one"},
			{OldOID: two, ProposedMessage: "two rewritten", OriginalMessage: "two"},
		},
	})

	res := runAcd(t, context.Background(), envWith(withIsolatedHome(t), "ACD_AI_PROVIDER=openai-compat"), "--repo", repo, "rewrite-commits", "--apply-plan", planPath, "--yes")
	if res.ExitCode != 0 {
		t.Fatalf("acd apply failed exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "no second AI call") || !strings.Contains(res.Stdout, "Backup branch:") || !strings.Contains(res.Stdout, "Recovery: git reset --hard") {
		t.Fatalf("apply output missing safety/recovery details:\n%s", res.Stdout)
	}
	if got := strings.TrimSpace(runGitOK(t, repo, "log", "--format=%s", "-n", "3")); got != "three\ntwo rewritten\none rewritten" {
		t.Fatalf("rewritten log subjects:\n%s", got)
	}
	backupRef := firstOutputLineWithPrefix(res.Stdout, "Backup branch: ")
	backupOID := strings.TrimSpace(runGitOK(t, repo, "rev-parse", strings.TrimPrefix(backupRef, "Backup branch: ")))
	if backupOID != three {
		t.Fatalf("backup oid=%s want original head %s", backupOID, three)
	}
}

func TestRewriteCommitsApplyPlanCLIRefusesMovedHead(t *testing.T) {
	repo := tempRepo(t)
	one := rewriteIntegrationCommit(t, repo, "one.txt", "one\n", "one")
	two := rewriteIntegrationCommit(t, repo, "two.txt", "two\n", "two")
	_ = rewriteIntegrationCommit(t, repo, "three.txt", "three\n", "three")
	planPath := filepath.Join(t.TempDir(), "rewrite.json")
	writeIntegrationRewritePlan(t, planPath, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     two,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: one, ProposedMessage: "one rewritten", OriginalMessage: "one"}},
	})

	res := runAcd(t, context.Background(), withIsolatedHome(t), "--repo", repo, "rewrite-commits", "--apply-plan", planPath, "--yes")
	if res.ExitCode == 0 || !strings.Contains(res.Stderr, "HEAD moved") {
		t.Fatalf("expected HEAD moved refusal exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	if refs := runGitOK(t, repo, "for-each-ref", "--format=%(refname)", "refs/heads/acd-backup"); strings.TrimSpace(refs) != "" {
		t.Fatalf("backup refs created despite refusal:\n%s", refs)
	}
}

func TestRewriteCommitsSelectionCLIRefusesDirtyWorktree(t *testing.T) {
	repo := tempRepo(t)
	if err := os.WriteFile(filepath.Join(repo, "dirty.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty: %v", err)
	}
	res := runAcd(t, context.Background(), withIsolatedHome(t), "--repo", repo, "rewrite-commits", "--last", "1")
	if res.ExitCode == 0 || !strings.Contains(res.Stderr, "dirty index or worktree") {
		t.Fatalf("expected dirty refusal, exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
}

func rewriteIntegrationCommit(t *testing.T, repo, path, body, msg string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(repo, path), []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	runGitOK(t, repo, "add", path)
	runGitOK(t, repo, "commit", "-q", "-m", msg)
	return strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
}

func writeIntegrationRewritePlan(t *testing.T, path string, plan state.RewritePlan) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(plan); err != nil {
		t.Fatalf("encode plan: %v", err)
	}
}

func firstOutputLineWithPrefix(out, prefix string) string {
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, prefix) {
			return line
		}
	}
	return ""
}
