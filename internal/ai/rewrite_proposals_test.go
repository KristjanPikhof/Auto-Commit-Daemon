package ai

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCommitRewritePromptContextIncludesEvidenceAndRespectsDiffPrivacy(t *testing.T) {
	req := CommitRewriteRequest{
		OldOID:          "abc123",
		OriginalMessage: "update files",
		ChangedPaths:    []string{"internal/ai/prompt.go"},
		DiffStat:        " internal/ai/prompt.go | 2 ++",
		DiffIncluded:    false,
		NeighborCommits: []CommitSummary{{OID: "parent", Subject: "Add prompt helpers"}},
		DecisionContext: []RewriteDecisionContext{{Kind: "committed", Path: "internal/ai/prompt.go", EventSeq: 42, Reason: "intent selected"}},
	}
	prompt, err := BuildCommitRewriteUserPrompt(req)
	if err != nil {
		t.Fatalf("BuildCommitRewriteUserPrompt: %v", err)
	}
	for _, want := range []string{"old message", "changed paths", "diff stat", "neighboring commits", "ACD decision context", "abc123", "update files", "internal/ai/prompt.go", "intent selected"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
	if strings.Contains(prompt, "redacted_diff") || strings.Contains(prompt, "SECRET") {
		t.Fatalf("prompt leaked diff when DiffIncluded=false: %s", prompt)
	}
}

func TestOpenAICommitRewriteRequestUsesCommitMessageTool(t *testing.T) {
	body, err := buildOpenAICommitRewriteRequest("test-model", CommitRewriteRequest{
		OldOID:          "abc123",
		OriginalMessage: "Update file",
		ChangedPaths:    []string{"x.go"},
		RedactedDiff:    "diff --git a/x.go b/x.go\n+[REDACTED]",
		DiffIncluded:    true,
	})
	if err != nil {
		t.Fatalf("buildOpenAICommitRewriteRequest: %v", err)
	}
	var got struct {
		Tools []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
		ToolChoice struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tool_choice"`
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("request JSON: %v", err)
	}
	if got.Tools[0].Function.Name != "commit_message" || got.ToolChoice.Function.Name != "commit_message" {
		t.Fatalf("unexpected tool wiring: %s / %s", got.Tools[0].Function.Name, got.ToolChoice.Function.Name)
	}
	user := got.Messages[len(got.Messages)-1].Content
	if !strings.Contains(user, "redacted_diff") || !strings.Contains(user, "[REDACTED]") {
		t.Fatalf("allowed redacted diff missing from user prompt: %s", user)
	}
}

func TestValidateCommitRewriteProposalRejectsGenericAndMalformedResponses(t *testing.T) {
	req := CommitRewriteRequest{OldOID: "abc123", ChangedPaths: []string{"internal/cli/rewrite_commits.go"}, RedactedDiff: strings.Repeat("x", 1300)}
	if _, err := ValidateCommitRewriteProposal(req, Result{Subject: "Update file"}); err == nil {
		t.Fatalf("expected generic proposal rejection")
	}
	if _, err := ValidateCommitRewriteProposal(req, Result{Subject: "Explain rewrite proposal", Body: "plain prose body"}); err == nil {
		t.Fatalf("expected malformed body rejection")
	}
	got, err := ValidateCommitRewriteProposal(req, Result{Subject: "Improve rewrite prompt context", Body: "- Explain the commit rewrite evidence used by the provider"})
	if err != nil {
		t.Fatalf("valid proposal rejected: %v", err)
	}
	if got.Subject != "Improve rewrite prompt context" {
		t.Fatalf("subject=%q", got.Subject)
	}
}
