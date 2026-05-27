package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRewriteCommitsHelpIncludesContract(t *testing.T) {
	help := commandHelp(t, "rewrite-commits")
	for _, want := range []string{
		"Aliases:",
		"edit-commits",
		"edit-commit",
		"ACD_COMMIT_STRATEGY",
		"ACD_AI_PROVIDER",
		"--from 8f4c2a1",
		"--from 5",
		"--range 5-12",
		"--last 4",
		"--git-range",
		"--base",
		"--head",
		"--plan-out",
		"--show-plan",
		"--edit",
		"$EDITOR",
		"new plan id is printed",
		"standalone JSON plan file",
		"--apply-plan",
		"--apply",
		"--review",
		"--no-review",
		"--plan-only",
		"--format",
		"backup recovery",
		"current branch linear ranges only",
		"merge commit rewrites are refused",
		"no daemon automation",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("rewrite-commits help missing %q:\n%s", want, help)
		}
	}
}

func TestRewriteCommitsAliasHelpIncludesEditContract(t *testing.T) {
	for _, command := range []string{"edit-commits", "edit-commit"} {
		t.Run(command, func(t *testing.T) {
			help := commandHelp(t, command)
			for _, want := range []string{
				"--edit <plan-id-or-file>",
				"$EDITOR",
				"saved plan id",
				"standalone JSON plan file",
				"--plan-only",
				"--dry-run",
				"no new plan is generated",
			} {
				if !strings.Contains(help, want) {
					t.Fatalf("%s help missing %q:\n%s", command, want, help)
				}
			}
		})
	}
}

func TestRewriteCommitsPlanGenerationRequiresIntentStrategy(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "openai-compat")
	t.Setenv(ai.EnvAPIKey, "test-key")
	repo := rewriteSelectionTestRepo(t)

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}}, false)
	if !errors.Is(err, ai.ErrRewriteRequiresIntentStrategy) {
		t.Fatalf("runRewriteCommits error = %v, want intent-strategy gate", err)
	}
}

func TestRewriteCommitsPlanGenerationRequiresUsableAIProvider(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, "intent")
	t.Setenv(ai.EnvProvider, "openai-compat")
	t.Setenv(ai.EnvAPIKey, "")
	repo := rewriteSelectionTestRepo(t)

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}}, false)
	if !errors.Is(err, ai.ErrRewriteRequiresAIProvider) {
		t.Fatalf("runRewriteCommits error = %v, want provider gate", err)
	}
}

func TestRewriteCommitsSavedPlanBypassesProviderGate(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "")
	planPath := filepath.Join(t.TempDir(), "rewrite.json")
	writeRewritePlanTestFile(t, planPath, state.RewritePlan{
		ID:               "plan-show",
		BranchRef:        "refs/heads/main",
		ExpectedHead:     "abc123456789",
		ValidationStatus: state.RewritePlanValidationValid,
		ApplyStatus:      state.RewritePlanApplyPending,
		Commits:          []state.RewritePlanCommit{{OldOID: "abc123456789", ProposedMessage: "shown subject", OriginalMessage: "old subject"}},
	})

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, "", rewriteCommitsOptions{showPlan: planPath}, false)
	if err != nil {
		t.Fatalf("runRewriteCommits show-plan returned error: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "without AI provider check") || !strings.Contains(got, "shown subject") || !strings.Contains(got, "Validation status: valid") {
		t.Fatalf("show-plan output missing loaded plan details/bypass note: %q", got)
	}
}

func TestRewritePlanRefArg(t *testing.T) {
	tests := []struct {
		ref  string
		want string
	}{
		{"", ""},
		{"rp_abc123", "rp_abc123"},
		{"/tmp/rewrite.json", "/tmp/rewrite.json"},
		{"has space", strconv.Quote("has space")},
		{"tab\there", strconv.Quote("tab\there")},
		{"quote'path", strconv.Quote("quote'path")},
		{`dollar$path`, strconv.Quote(`dollar$path`)},
	}
	for _, tc := range tests {
		if got := rewritePlanRefArg(tc.ref); got != tc.want {
			t.Errorf("rewritePlanRefArg(%q) = %q, want %q", tc.ref, got, tc.want)
		}
	}
}

func TestRewriteCommitsPlanOnlyQuotesPlanOutPathWithSpaces(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	providerDir := t.TempDir()
	provider := filepath.Join(providerDir, "acd-provider-rewrite-test")
	if err := os.WriteFile(provider, []byte("#!/usr/bin/env python3\nimport json, sys\nfor line in sys.stdin:\n    req = json.loads(line)\n    print(json.dumps({'version': 1, 'subject': 'plugin subject', 'body': ''}), flush=True)\n"), 0o755); err != nil {
		t.Fatalf("write provider: %v", err)
	}
	t.Setenv("PATH", providerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(ai.EnvCommitStrategy, "intent")
	t.Setenv(ai.EnvProvider, "subprocess:rewrite-test")

	planDir := filepath.Join(t.TempDir(), "acd plans")
	if err := os.Mkdir(planDir, 0o755); err != nil {
		t.Fatalf("mkdir plan dir: %v", err)
	}
	planPath := filepath.Join(planDir, "rewrite plan.json")
	quoted := strconv.Quote(planPath)

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{
		selection: git.RewriteSelectionOptions{Last: 1},
		planOnly:  true,
		planOut:   planPath,
	}, false)
	if err != nil {
		t.Fatalf("plan-only with spaced plan-out: %v\noutput:\n%s", err, out.String())
	}
	got := out.String()
	assertRewritePlanNextFooter(t, got)
	for _, prefix := range []string{
		"  acd rewrite-commits --show-plan ",
		"  acd rewrite-commits --apply-plan ",
	} {
		var found bool
		for _, line := range strings.Split(got, "\n") {
			if !strings.HasPrefix(line, prefix) {
				continue
			}
			if !strings.Contains(line, quoted) {
				t.Fatalf("next step line missing quoted plan path %q:\n%s", quoted, line)
			}
			found = true
		}
		if !found {
			t.Fatalf("missing next step line with prefix %q in:\n%s", prefix, got)
		}
	}
}

func TestRewriteCommitsPlanOnlyGeneratePrintsNextFooter(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	providerDir := t.TempDir()
	provider := filepath.Join(providerDir, "acd-provider-rewrite-test")
	if err := os.WriteFile(provider, []byte("#!/usr/bin/env python3\nimport json, sys\nfor line in sys.stdin:\n    req = json.loads(line)\n    print(json.dumps({'version': 1, 'subject': 'plugin subject', 'body': ''}), flush=True)\n"), 0o755); err != nil {
		t.Fatalf("write provider: %v", err)
	}
	t.Setenv("PATH", providerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(ai.EnvCommitStrategy, "intent")
	t.Setenv(ai.EnvProvider, "subprocess:rewrite-test")

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}, planOnly: true}, false)
	if err != nil {
		t.Fatalf("plan-only generate: %v\noutput:\n%s", err, out.String())
	}
	assertRewritePlanNextFooter(t, out.String())
}

func TestRewriteCommitsEditSavedPlanByIDPlanOnlyBypassesProviderGate(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	planID := saveRewritePlanForRepo(t, ctx, repo, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     mustRevParse(t, ctx, repo, "HEAD"),
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: mustRevParse(t, ctx, repo, "HEAD"), ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("EDITOR", editor)
	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "")

	var out bytes.Buffer
	err := runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{editPlan: planID, planOnly: true, editFormat: rewriteEditFormatText}, false)
	if err != nil {
		t.Fatalf("edit saved plan by id: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "unchanged") || !strings.Contains(got, "no AI call") {
		t.Fatalf("edit output missing unchanged/no-AI status: %q", got)
	}
	if strings.Contains(got, "No commits were rewritten.") && !strings.Contains(got, "Plan saved") {
		t.Fatalf("plan-only edit must not end with only no-rewrite message: %q", got)
	}
	assertRewritePlanNextFooter(t, got)
}

func TestRewriteCommitsDeclinedApplyOmitsNextFooter(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	providerDir := t.TempDir()
	provider := filepath.Join(providerDir, "acd-provider-rewrite-test")
	if err := os.WriteFile(provider, []byte("#!/usr/bin/env python3\nimport json, sys\nfor line in sys.stdin:\n    req = json.loads(line)\n    print(json.dumps({'version': 1, 'subject': 'plugin subject', 'body': ''}), flush=True)\n"), 0o755); err != nil {
		t.Fatalf("write provider: %v", err)
	}
	t.Setenv("PATH", providerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(ai.EnvCommitStrategy, "intent")
	t.Setenv(ai.EnvProvider, "subprocess:rewrite-test")

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{
		selection: git.RewriteSelectionOptions{Last: 1},
		noReview:  true,
		in:        strings.NewReader("n\n"),
	}, false)
	if err != nil {
		t.Fatalf("declined apply generate: %v\noutput:\n%s", err, out.String())
	}
	got := out.String()
	if !strings.Contains(got, "No rewrite performed.") {
		t.Fatalf("declined apply output missing no-rewrite message: %q", got)
	}
	if strings.Contains(got, "Next:") {
		t.Fatalf("declined apply output must not include Next footer: %q", got)
	}
}

func TestRewriteCommitsEditStandaloneFilePersistsBackToFile(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	head := mustRevParse(t, ctx, repo, "HEAD")
	planPath := filepath.Join(t.TempDir(), "rewrite.json")
	writeRewritePlanTestFile(t, planPath, state.RewritePlan{
		ID:               "file-plan",
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		ApplyStatus:      state.RewritePlanApplyPending,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\npython3 - <<'PY' \"$1\"\nimport pathlib, sys\np = pathlib.Path(sys.argv[1])\np.write_text(p.read_text().replace('seed rewritten', 'file edited subject'))\nPY\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("EDITOR", editor)
	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "")

	var out bytes.Buffer
	err := runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{editPlan: planPath, planOnly: true, editFormat: rewriteEditFormatJSON}, false)
	if err != nil {
		t.Fatalf("edit standalone file: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "Edited rewrite plan file saved") || !strings.Contains(got, "no AI call") {
		t.Fatalf("edit file output missing saved/no-AI status: %q", got)
	}
	updated, err := readRewritePlanFile(planPath)
	if err != nil {
		t.Fatalf("read updated plan file: %v", err)
	}
	if updated.Commits[0].ProposedMessage != "file edited subject" || !updated.Edited || updated.ValidationStatus != state.RewritePlanValidationValid {
		t.Fatalf("standalone file not persisted as edited valid plan: %#v", updated)
	}
}

func TestRewriteCommitsGenerationReviewPrintsEditedRevisionID(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	providerDir := t.TempDir()
	provider := filepath.Join(providerDir, "acd-provider-rewrite-test")
	if err := os.WriteFile(provider, []byte("#!/usr/bin/env python3\nimport json, sys\nfor line in sys.stdin:\n    req = json.loads(line)\n    print(json.dumps({'version': 1, 'subject': 'plugin subject', 'body': ''}), flush=True)\n"), 0o755); err != nil {
		t.Fatalf("write provider: %v", err)
	}
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\npython3 - <<'PY' \"$1\"\nimport pathlib, sys\np = pathlib.Path(sys.argv[1])\np.write_text(p.read_text().replace('plugin subject', 'review edited subject'))\nPY\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("PATH", providerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(ai.EnvCommitStrategy, "intent")
	t.Setenv(ai.EnvProvider, "subprocess:rewrite-test")
	t.Setenv("EDITOR", editor)

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}, review: true, editFormat: rewriteEditFormatText, in: strings.NewReader("n\n")}, false)
	if err != nil {
		t.Fatalf("generation review edit: %v\noutput:\n%s", err, out.String())
	}
	got := out.String()
	if !strings.Contains(got, "Edited rewrite plan saved as ") || !strings.Contains(got, "No rewrite performed.") {
		t.Fatalf("generation review output missing edited revision id/no-rewrite: %q", got)
	}
	editedID := parseRewritePlanIDAfter(t, got, "Edited rewrite plan saved as")
	db := openRewritePlanTestDB(t, context.Background(), repo)
	defer db.Close()
	edited, ok, err := state.LoadRewritePlan(context.Background(), db, editedID)
	if err != nil || !ok {
		t.Fatalf("load edited revision: ok=%v err=%v", ok, err)
	}
	if !edited.Edited || edited.Commits[0].ProposedMessage != "review edited subject" {
		t.Fatalf("edited review revision not saved: %#v", edited)
	}
}

func TestRewriteCommitsEditSavedPlanPersistsRevisionWithoutAICall(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	head := mustRevParse(t, ctx, repo, "HEAD")
	planID := saveRewritePlanForRepo(t, ctx, repo, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\npython3 - <<'PY' \"$1\"\nimport pathlib, sys\np = pathlib.Path(sys.argv[1])\np.write_text(p.read_text().replace('seed rewritten', 'edited seed subject'))\nPY\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("EDITOR", editor)
	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "")

	var out bytes.Buffer
	err := runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{editPlan: planID, planOnly: true, editFormat: rewriteEditFormatText}, false)
	if err != nil {
		t.Fatalf("edit saved plan persisted revision: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "Edited rewrite plan saved as ") || !strings.Contains(got, "no AI call") {
		t.Fatalf("edit output missing saved revision/no-AI status: %q", got)
	}
	db := openRewritePlanTestDB(t, ctx, repo)
	defer db.Close()
	base, ok, err := state.LoadRewritePlan(ctx, db, planID)
	if err != nil || !ok {
		t.Fatalf("load base plan: ok=%v err=%v", ok, err)
	}
	if base.Commits[0].ProposedMessage != "seed rewritten" {
		t.Fatalf("base plan mutated: %#v", base.Commits[0])
	}
	editedID := parseEditedRewritePlanID(t, got)
	edited, ok, err := state.LoadRewritePlan(ctx, db, editedID)
	if err != nil || !ok {
		t.Fatalf("load edited plan: ok=%v err=%v", ok, err)
	}
	if !edited.BasePlanID.Valid || edited.BasePlanID.String != planID || edited.Revision != base.Revision+1 || !edited.Edited || edited.Commits[0].ProposedMessage != "edited seed subject" {
		t.Fatalf("edited revision not saved as expected: %#v", edited)
	}
}

func TestRewriteCommitsEditSavedPlanValidationFailure(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	head := mustRevParse(t, ctx, repo, "HEAD")
	planID := saveRewritePlanForRepo(t, ctx, repo, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\npython3 - <<'PY' \"$1\"\nimport pathlib, sys\np = pathlib.Path(sys.argv[1])\ns = p.read_text()\nstart = s.index('message <<ACD_COMMIT_MESSAGE') + len('message <<ACD_COMMIT_MESSAGE')\nend = s.index('ACD_COMMIT_MESSAGE', start)\np.write_text(s[:start] + '\\n   \\n' + s[end:])\nPY\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("EDITOR", editor)

	var out bytes.Buffer
	err := runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{editPlan: planID, planOnly: true, editFormat: rewriteEditFormatText}, false)
	if err == nil || !strings.Contains(err.Error(), "empty message") {
		t.Fatalf("edit validation err = %v, want empty message", err)
	}
}

func TestRewriteCommitsEditSavedPlanPromptsBeforeApply(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	head := mustRevParse(t, ctx, repo, "HEAD")
	planID := saveRewritePlanForRepo(t, ctx, repo, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("EDITOR", editor)

	var out bytes.Buffer
	err := runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{editPlan: planID, editFormat: rewriteEditFormatText, in: strings.NewReader("n\n")}, false)
	if err != nil {
		t.Fatalf("edit prompt flow: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "Apply this edited rewrite plan now?") || !strings.Contains(got, "No rewrite performed.") {
		t.Fatalf("edit output missing apply prompt/no-rewrite: %q", got)
	}
}

func TestRewriteCommitsApplyPlanRequiresValidValidationStatus(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	head, err := git.RevParse(context.Background(), repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	for _, status := range []string{"", state.RewritePlanValidationDraft, state.RewritePlanValidationInvalid} {
		planPath := filepath.Join(t.TempDir(), "rewrite-"+status+".json")
		writeRewritePlanTestFile(t, planPath, state.RewritePlan{
			BranchRef:        "refs/heads/main",
			ExpectedHead:     head,
			ValidationStatus: status,
			Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
		})
		var out bytes.Buffer
		err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{applyPlan: planPath, dryRun: true}, false)
		if err == nil || !strings.Contains(err.Error(), "validation status") {
			t.Fatalf("status %q apply err = %v, want validation-status refusal", status, err)
		}
	}
}

func TestRewriteCommitsApplyPlanRequiresConfirmationButBypassesProviderGate(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	head, err := git.RevParse(context.Background(), repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	planPath := filepath.Join(t.TempDir(), "rewrite.json")
	writeRewritePlanTestFile(t, planPath, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})

	var out bytes.Buffer
	err = runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{applyPlan: planPath}, false)
	if err == nil || !strings.Contains(err.Error(), "--yes or --dry-run") {
		t.Fatalf("apply-plan without confirmation error = %v, want confirmation refusal", err)
	}

	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "")
	out.Reset()
	err = runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{applyPlan: planPath, dryRun: true}, false)
	if err != nil {
		t.Fatalf("apply-plan dry-run returned error: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "no second AI call") {
		t.Fatalf("apply-plan output missing no-AI-call note: %q", got)
	}
}

func assertRewritePlanNextFooter(t *testing.T, got string) {
	t.Helper()
	for _, want := range []string{
		"Plan saved",
		"Next:",
		"--show-plan",
		"--apply-plan",
		"--dry-run",
		"--yes",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("rewrite plan-only next footer missing %q in:\n%s", want, got)
		}
	}
}

func rewriteSelectionTestRepo(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	repo := initRepoForRepoLifecycle(t)
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo, Timeout: git.DefaultWriteTimeout}, "config", "user.email", "test@example.com"); err != nil {
		t.Fatalf("git config user.email: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo, Timeout: git.DefaultWriteTimeout}, "config", "user.name", "Test User"); err != nil {
		t.Fatalf("git config user.name: %v", err)
	}
	writeRewriteTestFile(t, repo, "seed.txt", "seed\n")
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo, Timeout: git.DefaultWriteTimeout}, "add", "seed.txt"); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo, Timeout: git.DefaultWriteTimeout}, "commit", "-q", "-m", "seed"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	return repo
}

func writeRewriteTestFile(t *testing.T, repo, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(repo, name), []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func parseEditedRewritePlanID(t *testing.T, output string) string {
	t.Helper()
	return parseRewritePlanIDAfter(t, output, "")
}

func parseRewritePlanIDAfter(t *testing.T, output, marker string) string {
	t.Helper()
	search := output
	if marker != "" {
		idx := strings.Index(output, marker)
		if idx < 0 {
			t.Fatalf("marker %q not found in output: %q", marker, output)
		}
		search = output[idx:]
	}
	for _, field := range strings.Fields(search) {
		field = strings.TrimSuffix(field, ".")
		if strings.HasPrefix(field, "rp_") || strings.HasPrefix(field, "rewrite-plan-") {
			return field
		}
	}
	t.Fatalf("no rewrite plan id found in output: %q", search)
	return ""
}

func mustRevParse(t *testing.T, ctx context.Context, repo, rev string) string {
	t.Helper()
	oid, err := git.RevParse(ctx, repo, rev)
	if err != nil {
		t.Fatalf("rev-parse %s: %v", rev, err)
	}
	return oid
}

func openRewritePlanTestDB(t *testing.T, ctx context.Context, repo string) *state.DB {
	t.Helper()
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		t.Fatalf("state db path: %v", err)
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open state db: %v", err)
	}
	return db
}

func saveRewritePlanForRepo(t *testing.T, ctx context.Context, repo string, plan state.RewritePlan) string {
	t.Helper()
	db := openRewritePlanTestDB(t, ctx, repo)
	defer db.Close()
	id, err := state.SaveRewritePlan(ctx, db, plan)
	if err != nil {
		t.Fatalf("save rewrite plan: %v", err)
	}
	return id
}

func writeRewritePlanTestFile(t *testing.T, path string, plan state.RewritePlan) {
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

func TestRewriteCommitsApplyRefusesPendingCaptureQueue(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		t.Fatalf("state db path: %v", err)
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open state db: %v", err)
	}
	defer db.Close()
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head, Operation: "modify", Path: "queued.txt", Fidelity: "full"}, nil)
	if err != nil {
		t.Fatalf("append pending event: %v", err)
	}
	planPath := filepath.Join(t.TempDir(), "rewrite.json")
	writeRewritePlanTestFile(t, planPath, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})

	var out bytes.Buffer
	err = runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{applyPlan: planPath, yes: true}, false)
	if err == nil || !strings.Contains(err.Error(), "pending event") || !strings.Contains(err.Error(), strconv.FormatInt(seq, 10)) {
		t.Fatalf("apply with pending queue err = %v, want pending refusal with seq %d", err, seq)
	}
}

func TestRewriteCommitsApplyRefusesPendingCaptureQueueHiddenBehindBarrier(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		t.Fatalf("state db path: %v", err)
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open state db: %v", err)
	}
	defer db.Close()
	if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head, Operation: "modify", Path: "blocked.txt", Fidelity: "full", State: state.EventStateBlockedConflict}, nil); err != nil {
		t.Fatalf("append blocked barrier: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head, Operation: "modify", Path: "hidden-pending.txt", Fidelity: "full"}, nil)
	if err != nil {
		t.Fatalf("append hidden pending event: %v", err)
	}
	visible, err := state.PendingEvents(ctx, db, 1)
	if err != nil {
		t.Fatalf("PendingEvents: %v", err)
	}
	if len(visible) != 0 {
		t.Fatalf("test setup expected barrier-hidden pending event, got visible=%+v", visible)
	}
	planPath := filepath.Join(t.TempDir(), "rewrite.json")
	writeRewritePlanTestFile(t, planPath, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})

	var out bytes.Buffer
	err = runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{applyPlan: planPath, yes: true}, false)
	if err == nil || !strings.Contains(err.Error(), "pending event") || !strings.Contains(err.Error(), strconv.FormatInt(seq, 10)) {
		t.Fatalf("apply with barrier-hidden pending queue err = %v, want pending refusal with seq %d", err, seq)
	}
}

func TestRewriteCommitsParserFlags(t *testing.T) {
	cmd := newRewriteCommitsCmd()
	cmd.SetArgs([]string{"--last", "2", "--review", "--format", "json", "--plan-only"})
	if err := cmd.ParseFlags(cmd.Flags().Args()); err == nil {
		// ParseFlags does not parse SetArgs; execute through commandHelp coverage below instead.
	}

	var opts rewriteCommitsOptions
	cmd = newRewriteCommitsCmd()
	cmd.SetArgs([]string{"--last", "2", "--review", "--format", "json", "--plan-only"})
	cmd.RunE = func(cmd *cobra.Command, args []string) error { return nil }
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute parser flags: %v", err)
	}
	_ = opts

	err := normalizeAndValidateRewriteOptions(&rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}, review: true, noReview: true, editFormat: "text"})
	if err == nil || !strings.Contains(err.Error(), "--review") {
		t.Fatalf("review/no-review validation err = %v", err)
	}
	err = normalizeAndValidateRewriteOptions(&rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}, editFormat: "yaml"})
	if err == nil || !strings.Contains(err.Error(), "--format") {
		t.Fatalf("format validation err = %v", err)
	}
	err = normalizeAndValidateRewriteOptions(&rewriteCommitsOptions{applyPlan: "plan-id", planOnly: true, editFormat: "text"})
	if err == nil || !strings.Contains(err.Error(), "--plan-only") {
		t.Fatalf("plan-only/apply validation err = %v", err)
	}
}

func TestRewritePlanTextEditRoundTripAndValidation(t *testing.T) {
	plan := rewritePlanEditTestPlan()
	rendered, err := renderRewritePlanEdit(plan, rewriteEditFormatText)
	if err != nil {
		t.Fatalf("render text: %v", err)
	}
	parsed, err := parseRewritePlanEdit(rendered, rewriteEditFormatText, plan)
	if err != nil {
		t.Fatalf("parse rendered text: %v\n%s", err, rendered)
	}
	if !rewritePlanMessagesEqual(plan.Commits, parsed) {
		t.Fatalf("text round trip changed commits: %#v", parsed)
	}

	edited := strings.Replace(string(rendered), "better subject", "edited subject", 1)
	parsed, err = parseRewritePlanEdit([]byte(edited), rewriteEditFormatText, plan)
	if err != nil {
		t.Fatalf("parse edited text: %v", err)
	}
	if got := parsed[0].ProposedMessage; !strings.Contains(got, "edited subject") {
		t.Fatalf("edited message = %q", got)
	}

	invalid := strings.Replace(string(rendered), "commit abc123", "commit different", 1)
	if _, err := parseRewritePlanEdit([]byte(invalid), rewriteEditFormatText, plan); err == nil || !strings.Contains(err.Error(), "oid") {
		t.Fatalf("parse invalid oid err = %v, want oid validation", err)
	}
}

func TestRewritePlanJSONEditRoundTripAndValidation(t *testing.T) {
	plan := rewritePlanEditTestPlan()
	rendered, err := renderRewritePlanEdit(plan, rewriteEditFormatJSON)
	if err != nil {
		t.Fatalf("render json: %v", err)
	}
	parsed, err := parseRewritePlanEdit(rendered, rewriteEditFormatJSON, plan)
	if err != nil {
		t.Fatalf("parse rendered json: %v\n%s", err, rendered)
	}
	if !rewritePlanMessagesEqual(plan.Commits, parsed) {
		t.Fatalf("json round trip changed commits: %#v", parsed)
	}

	unknown := strings.Replace(string(rendered), `"commits":`, `"unknown": true, "commits":`, 1)
	if _, err := parseRewritePlanEdit([]byte(unknown), rewriteEditFormatJSON, plan); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("parse unknown field err = %v, want unknown-field validation", err)
	}

	var doc rewritePlanEditJSON
	if err := json.Unmarshal(rendered, &doc); err != nil {
		t.Fatalf("unmarshal rendered json: %v", err)
	}
	doc.Commits[0].Message = "   "
	empty, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal empty-message json: %v", err)
	}
	if _, err := parseRewritePlanEdit(empty, rewriteEditFormatJSON, plan); err == nil {
		t.Fatalf("parse empty message succeeded, want validation error")
	}

	trailing := append(append([]byte{}, rendered...), []byte("\n{}")...)
	if _, err := parseRewritePlanEdit(trailing, rewriteEditFormatJSON, plan); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("parse trailing json err = %v, want trailing-data validation", err)
	}
}

func TestRewritePlanFakeEditorAcceptsUnchangedContent(t *testing.T) {
	plan := rewritePlanEditTestPlan()
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("EDITOR", editor)
	commits, changed, err := editRewritePlanWithEditor(plan, rewriteEditFormatText)
	if err != nil {
		t.Fatalf("edit unchanged: %v", err)
	}
	if changed {
		t.Fatalf("changed=true for unchanged editor content")
	}
	if !rewritePlanMessagesEqual(plan.Commits, commits) {
		t.Fatalf("unchanged editor altered commits: %#v", commits)
	}
}

func TestRewritePlanFakeEditorCanEditJSON(t *testing.T) {
	plan := rewritePlanEditTestPlan()
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\npython3 - <<'PY' \"$1\"\nimport pathlib, sys\np = pathlib.Path(sys.argv[1])\ns = p.read_text()\np.write_text(s.replace('better subject', 'json edited subject'))\nPY\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("EDITOR", editor)
	commits, changed, err := editRewritePlanWithEditor(plan, rewriteEditFormatJSON)
	if err != nil {
		t.Fatalf("edit json: %v", err)
	}
	if !changed {
		t.Fatalf("changed=false after fake editor edit")
	}
	if got := commits[0].ProposedMessage; !strings.Contains(got, "json edited subject") {
		t.Fatalf("edited message = %q", got)
	}
}

func TestRewritePromptBehavior(t *testing.T) {
	var out bytes.Buffer
	yes, err := promptRewriteYesNo(strings.NewReader("y\n"), &out, "Review or edit proposed messages before applying?", false)
	if err != nil || !yes {
		t.Fatalf("yes prompt got yes=%v err=%v", yes, err)
	}
	if !strings.Contains(out.String(), "Review or edit proposed messages before applying?") {
		t.Fatalf("prompt output = %q", out.String())
	}

	out.Reset()
	no, err := promptRewriteYesNo(strings.NewReader("\n"), &out, "Apply this rewrite plan now?", false)
	if err != nil || no {
		t.Fatalf("default-no prompt got yes=%v err=%v", no, err)
	}
}

func rewritePlanEditTestPlan() state.RewritePlan {
	return state.RewritePlan{
		ID:               "plan-test",
		BranchRef:        "refs/heads/main",
		ExpectedHead:     "feedface",
		ValidationStatus: state.RewritePlanValidationValid,
		ApplyStatus:      state.RewritePlanApplyPending,
		Commits: []state.RewritePlanCommit{
			{OldOID: "abc123", OriginalMessage: "old subject", ProposedMessage: "better subject\n\nbody"},
			{OldOID: "def456", OriginalMessage: "second old", ProposedMessage: "second better"},
		},
	}
}
