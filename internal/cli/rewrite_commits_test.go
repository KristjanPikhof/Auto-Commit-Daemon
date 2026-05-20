package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

func TestRewriteCommitsHelpIncludesContract(t *testing.T) {
	help := commandHelp(t, "rewrite-commits")
	for _, want := range []string{
		"ACD_COMMIT_STRATEGY",
		"ACD_AI_PROVIDER",
		"--range",
		"--base",
		"--head",
		"--plan-out",
		"--show-plan",
		"--apply-plan",
		"current branch linear ranges only",
		"merge commit rewrites are refused",
		"no daemon automation",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("rewrite-commits help missing %q:\n%s", want, help)
		}
	}
}

func TestRewriteCommitsPlanGenerationRequiresIntentStrategy(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "openai-compat")
	t.Setenv(ai.EnvAPIKey, "test-key")

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, "", rewriteCommitsOptions{selection: gitRewriteRange("HEAD~1..HEAD")}, false)
	if !errors.Is(err, ai.ErrRewriteRequiresIntentStrategy) {
		t.Fatalf("runRewriteCommits error = %v, want intent-strategy gate", err)
	}
}

func TestRewriteCommitsPlanGenerationRequiresUsableAIProvider(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, "intent")
	t.Setenv(ai.EnvProvider, "openai-compat")
	t.Setenv(ai.EnvAPIKey, "")

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, "", rewriteCommitsOptions{base: "HEAD~1"}, false)
	if !errors.Is(err, ai.ErrRewriteRequiresAIProvider) {
		t.Fatalf("runRewriteCommits error = %v, want provider gate", err)
	}
}

func TestRewriteCommitsSavedPlanBypassesProviderGate(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "")

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, "", rewriteCommitsOptions{showPlan: "rewrite.json"}, false)
	if err != nil {
		t.Fatalf("runRewriteCommits show-plan returned error: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "no AI provider check required") {
		t.Fatalf("show-plan output missing bypass note: %q", got)
	}
}

func TestRewriteCommitsApplyPlanRequiresConfirmationButBypassesProviderGate(t *testing.T) {
	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, "", rewriteCommitsOptions{applyPlan: "rewrite.json"}, false)
	if err == nil || !strings.Contains(err.Error(), "--yes or --dry-run") {
		t.Fatalf("apply-plan without confirmation error = %v, want confirmation refusal", err)
	}

	out.Reset()
	err = runRewriteCommits(context.Background(), &out, "", rewriteCommitsOptions{applyPlan: "rewrite.json", dryRun: true}, false)
	if err != nil {
		t.Fatalf("apply-plan dry-run returned error: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "no AI provider check required") {
		t.Fatalf("apply-plan output missing bypass note: %q", got)
	}
}

func gitRewriteRange(spec string) git.RewriteSelectionOptions {
	return git.RewriteSelectionOptions{GitRange: spec}
}
