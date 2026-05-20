package ai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// CommitRewriteProposer is the optional provider capability used by
// rewrite-commits plan generation. It receives historical commit context and
// returns only a replacement commit message proposal.
type CommitRewriteProposer interface {
	Name() string
	ProposeCommitRewrite(ctx context.Context, req CommitRewriteRequest) (Result, error)
}

// RewriteDecisionContext is ACD ledger context attached to the commit OID
// being rewritten.
type RewriteDecisionContext struct {
	Kind        string `json:"kind"`
	Path        string `json:"path,omitempty"`
	Reason      string `json:"reason,omitempty"`
	EventSeq    int64  `json:"event_seq,omitempty"`
	ActionTaken string `json:"action_taken,omitempty"`
	UserMessage string `json:"user_message,omitempty"`
}

// CommitRewriteRequest is the deterministic prompt payload for one historical
// commit-message rewrite proposal.
type CommitRewriteRequest struct {
	OldOID          string                   `json:"old_oid"`
	OriginalMessage string                   `json:"original_message"`
	ChangedPaths    []string                 `json:"changed_paths"`
	DiffStat        string                   `json:"diff_stat,omitempty"`
	RedactedDiff    string                   `json:"redacted_diff,omitempty"`
	DiffIncluded    bool                     `json:"diff_included"`
	NeighborCommits []CommitSummary          `json:"neighbor_commits,omitempty"`
	DecisionContext []RewriteDecisionContext `json:"acd_decision_context,omitempty"`
	Now             time.Time                `json:"now,omitempty"`
}

// BuildCommitRewriteUserPrompt serializes a stable rewrite prompt. The output
// structure is fixed so provider snapshot tests can assert privacy behavior.
func BuildCommitRewriteUserPrompt(req CommitRewriteRequest) (string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("commit rewrite: marshal request: %w", err)
	}
	return "Rewrite the git commit subject and body for this existing commit.\n" +
		"Use the old message, changed paths, diff stat, allowed redacted diff, neighboring commits, and ACD decision context as evidence.\n" +
		"Return only the commit_message tool output. Do not invent behavior not supported by the evidence.\n" +
		commitMessageFormatInstructions + "\n" + string(body), nil
}

// ValidateCommitRewriteProposal sanitizes and quality-checks a proposal before
// it can be stored as a valid rewrite plan row.
func ValidateCommitRewriteProposal(req CommitRewriteRequest, result Result) (Result, error) {
	if strings.TrimSpace(result.Body) != "" && !wellFormedBulletBody(strings.TrimSpace(result.Body)) {
		return Result{}, errors.New("commit rewrite: proposal body must use commit-message bullets")
	}
	cleaned := SanitizeMessage(result.Subject + "\n\n" + result.Body)
	parts := strings.SplitN(cleaned, "\n\n", 2)
	out := Result{Subject: parts[0], Source: result.Source}
	if len(parts) == 2 {
		out.Body = parts[1]
	}
	if strings.TrimSpace(out.Subject) == "" {
		return Result{}, errors.New("commit rewrite: empty subject after sanitize")
	}
	planReq := IntentPlanRequest{OfferedCaptures: []OfferedCapture{{Seq: 1, Path: firstRewritePath(req.ChangedPaths), CapturedDiff: req.RedactedDiff}}}
	plan := IntentPlan{SelectedSeqs: []int64{1}, Subject: out.Subject, Body: out.Body, GroupingReason: "commit rewrite proposal"}
	report := EvaluateIntentPlanMessageQuality(planReq, plan)
	if report.Action != MessageQualityClean && report.Action != MessageQualitySanitizeAccept {
		return Result{}, fmt.Errorf("commit rewrite: proposal failed quality validation: %s", messageQualitySummary(report))
	}
	if report.Action == MessageQualitySanitizeAccept {
		out.Subject = report.SanitizedSubject
		out.Body = report.SanitizedBody
	}
	return out, nil
}

func firstRewritePath(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	return paths[0]
}

// ProposeCommitRewrite on composed providers uses only the primary provider;
// rewrite planning is AI-gated and must not silently fall back to deterministic.
func (c *composed) ProposeCommitRewrite(ctx context.Context, req CommitRewriteRequest) (Result, error) {
	p, ok := c.primary.(CommitRewriteProposer)
	if !ok {
		return Result{}, fmt.Errorf("ai: primary provider %s cannot propose commit rewrites", c.primary.Name())
	}
	return p.ProposeCommitRewrite(ctx, req)
}
