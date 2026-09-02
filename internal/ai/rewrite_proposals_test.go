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

func TestValidateCommitRewriteProposalConventional(t *testing.T) {
	req := CommitRewriteRequest{
		OldOID:       "abc123",
		ChangedPaths: []string{"internal/ai/rewrite_proposals.go"},
		CommitFormat: CommitFormatConventional,
	}
	if _, err := ValidateCommitRewriteProposal(req, Result{Subject: "Fix rewrite proposal validation"}); err == nil {
		t.Fatalf("expected imperative proposal rejection in conventional mode")
	}
	got, err := ValidateCommitRewriteProposal(req, Result{Subject: "fix: validate rewrite proposals"})
	if err != nil {
		t.Fatalf("valid conventional proposal rejected: %v", err)
	}
	if got.Subject != "fix: validate rewrite proposals" {
		t.Fatalf("subject=%q", got.Subject)
	}
}

func TestValidateHistoryRewritePlanAcceptsOrderedSameAuthorGroups(t *testing.T) {
	req := HistoryRewritePlanRequest{
		CommitFormat: CommitFormatImperative,
		Commits: []HistoryRewriteCommit{
			{OldOID: "one", AuthorName: "Dev", AuthorEmail: "dev@example.com", ChangedPaths: []string{"a.go"}},
			{OldOID: "two", AuthorName: "Dev", AuthorEmail: "dev@example.com", ChangedPaths: []string{"a_test.go"}},
			{OldOID: "three", AuthorName: "Dev", AuthorEmail: "dev@example.com", ChangedPaths: []string{"notes.md"}},
		},
	}
	got, err := ValidateHistoryRewritePlan(req, HistoryRewritePlan{Groups: []HistoryRewriteGroup{
		{OldOIDs: []string{"one", "two"}, Subject: "Add grouped rewrite behavior", GroupingReason: "implementation and test"},
		{OldOIDs: []string{"three"}, Subject: "Document grouped rewrites", GroupingReason: "separate documentation"},
	}})
	if err != nil {
		t.Fatalf("ValidateHistoryRewritePlan: %v", err)
	}
	if len(got.Groups) != 2 || len(got.Groups[0].OldOIDs) != 2 || got.Groups[1].OldOIDs[0] != "three" {
		t.Fatalf("groups=%+v", got.Groups)
	}
}

func TestValidateHistoryRewritePlanRejectsInvalidPartitions(t *testing.T) {
	req := HistoryRewritePlanRequest{Commits: []HistoryRewriteCommit{
		{OldOID: "one", AuthorName: "Dev", AuthorEmail: "dev@example.com"},
		{OldOID: "two", AuthorName: "Other", AuthorEmail: "other@example.com"},
		{OldOID: "three", AuthorName: "Dev", AuthorEmail: "dev@example.com"},
	}}
	tests := []struct {
		name   string
		groups []HistoryRewriteGroup
		want   string
	}{
		{"missing", []HistoryRewriteGroup{{OldOIDs: []string{"one"}, Subject: "Keep ordered commits", GroupingReason: "test"}}, "included 1 of 3"},
		{"reordered", []HistoryRewriteGroup{{OldOIDs: []string{"two"}, Subject: "Keep ordered commits", GroupingReason: "test"}}, "exactly once in chronological order"},
		{"duplicate", []HistoryRewriteGroup{{OldOIDs: []string{"one", "one"}, Subject: "Keep ordered commits", GroupingReason: "test"}}, "exactly once in chronological order"},
		{"unknown", []HistoryRewriteGroup{{OldOIDs: []string{"unknown"}, Subject: "Keep ordered commits", GroupingReason: "test"}}, "unknown oid"},
		{"mixed author", []HistoryRewriteGroup{{OldOIDs: []string{"one", "two"}, Subject: "Keep ordered commits", GroupingReason: "test"}, {OldOIDs: []string{"three"}, Subject: "Keep last commit", GroupingReason: "test"}}, "author boundary"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateHistoryRewritePlan(req, HistoryRewritePlan{Groups: tt.groups})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error=%v want %q", err, tt.want)
			}
		})
	}
}

func TestBuildHistoryRewriteUserPromptStatesGroupingBoundaries(t *testing.T) {
	prompt, err := BuildHistoryRewriteUserPrompt(HistoryRewritePlanRequest{Commits: []HistoryRewriteCommit{{OldOID: "one"}}})
	if err != nil {
		t.Fatalf("BuildHistoryRewriteUserPrompt: %v", err)
	}
	for _, want := range []string{"exactly once", "chronological order", "contiguous groups", "same file", "different author_name or author_email"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing %q:\n%s", want, prompt)
		}
	}
}

func TestBuildHistoryRewriteUserPromptRejectsOversizedRequest(t *testing.T) {
	req := HistoryRewritePlanRequest{Commits: []HistoryRewriteCommit{{
		OldOID:          "one",
		OriginalMessage: strings.Repeat("x", HistoryRewriteRequestByteCap),
	}}}
	if _, err := BuildHistoryRewriteUserPrompt(req); err == nil || !strings.Contains(err.Error(), "select a smaller commit range") {
		t.Fatalf("error=%v", err)
	}
}

func TestOpenAIHistoryRewriteRequestUsesStructuredTool(t *testing.T) {
	body, err := buildOpenAIHistoryRewritePlanRequest("test-model", HistoryRewritePlanRequest{Commits: []HistoryRewriteCommit{{OldOID: "one", AuthorName: "Dev", AuthorEmail: "dev@example.com"}}})
	if err != nil {
		t.Fatalf("buildOpenAIHistoryRewritePlanRequest: %v", err)
	}
	var request struct {
		Tools []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if len(request.Tools) != 1 || request.Tools[0].Function.Name != "history_rewrite_plan" {
		t.Fatalf("tools=%+v", request.Tools)
	}
}
