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

const (
	HistoryRewritePerCommitDiffCap = IntentStageDiffCap
	HistoryRewriteTotalDiffCap     = 128 * 1024
	HistoryRewriteRequestByteCap   = 512 * 1024
)

// HistoryRewritePlanner groups a selected linear history and proposes one
// message for each resulting commit.
type HistoryRewritePlanner interface {
	Name() string
	ProposeHistoryRewritePlan(context.Context, HistoryRewritePlanRequest) (HistoryRewritePlan, error)
}

// HistoryRewriteCommit is bounded evidence for one selected historical commit.
type HistoryRewriteCommit struct {
	OldOID          string                   `json:"old_oid"`
	Position        int                      `json:"position"`
	OriginalMessage string                   `json:"original_message"`
	AuthorName      string                   `json:"author_name"`
	AuthorEmail     string                   `json:"author_email"`
	ChangedPaths    []string                 `json:"changed_paths"`
	DiffStat        string                   `json:"diff_stat,omitempty"`
	RedactedDiff    string                   `json:"redacted_diff,omitempty"`
	DiffIncluded    bool                     `json:"diff_included"`
	DecisionContext []RewriteDecisionContext `json:"acd_decision_context,omitempty"`
}

// HistoryRewritePlanRequest is the complete ordered selection presented to a
// history rewrite planner.
type HistoryRewritePlanRequest struct {
	Commits      []HistoryRewriteCommit `json:"commits"`
	CommitFormat CommitFormat           `json:"commit_format,omitempty"`
	Now          time.Time              `json:"now,omitempty"`
}

// HistoryRewriteGroup is one proposed output commit.
type HistoryRewriteGroup struct {
	OldOIDs        []string `json:"old_oids"`
	Subject        string   `json:"subject"`
	Body           string   `json:"body,omitempty"`
	GroupingReason string   `json:"grouping_reason"`
}

// HistoryRewritePlan is the structured result of historical grouping.
type HistoryRewritePlan struct {
	Groups []HistoryRewriteGroup `json:"groups"`
	Source string                `json:"-"`
}

// BuildHistoryRewriteUserPrompt serializes the bounded historical grouping
// request sent to providers.
func BuildHistoryRewriteUserPrompt(req HistoryRewritePlanRequest) (string, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("history rewrite plan: marshal request: %w", err)
	}
	if len(body) > HistoryRewriteRequestByteCap {
		return "", fmt.Errorf("history rewrite plan: request is %d bytes; maximum is %d bytes; select a smaller commit range", len(body), HistoryRewriteRequestByteCap)
	}
	return "Group these existing commits into a smaller semantic history.\n" +
		"Return every old_oid exactly once, in the same chronological order, using contiguous groups only. Separate unrelated work even when it touches the same file. Never group commits with different author_name or author_email values.\n" +
		"Return one evidence-grounded subject, body, and grouping_reason per group. Do not invent behavior not supported by the evidence.\n" +
		CommitMessageFormatInstructions(req.CommitFormat) + "\n" + string(body), nil
}

// ValidateHistoryRewritePlan validates and sanitizes a provider partition.
func ValidateHistoryRewritePlan(req HistoryRewritePlanRequest, plan HistoryRewritePlan) (HistoryRewritePlan, error) {
	if len(req.Commits) == 0 {
		return HistoryRewritePlan{}, errors.New("history rewrite plan: no commits were offered")
	}
	if len(plan.Groups) == 0 {
		return HistoryRewritePlan{}, errors.New("history rewrite plan: groups must be non-empty")
	}
	byOID := make(map[string]HistoryRewriteCommit, len(req.Commits))
	for _, commit := range req.Commits {
		if commit.OldOID == "" {
			return HistoryRewritePlan{}, errors.New("history rewrite plan: offered commit has an empty oid")
		}
		if _, ok := byOID[commit.OldOID]; ok {
			return HistoryRewritePlan{}, fmt.Errorf("history rewrite plan: duplicate offered oid %s", commit.OldOID)
		}
		byOID[commit.OldOID] = commit
	}
	result := HistoryRewritePlan{Source: plan.Source}
	flatIndex := 0
	for groupIndex, group := range plan.Groups {
		if len(group.OldOIDs) == 0 {
			return HistoryRewritePlan{}, fmt.Errorf("history rewrite plan: groups[%d].old_oids must be non-empty", groupIndex)
		}
		reason := NormalizeIntentReason(group.GroupingReason)
		if reason == "" {
			return HistoryRewritePlan{}, fmt.Errorf("history rewrite plan: groups[%d].grouping_reason must be non-empty", groupIndex)
		}
		first, ok := byOID[group.OldOIDs[0]]
		if !ok {
			return HistoryRewritePlan{}, fmt.Errorf("history rewrite plan: unknown oid %s", group.OldOIDs[0])
		}
		var paths []string
		var diffs []string
		for _, oid := range group.OldOIDs {
			if flatIndex >= len(req.Commits) || req.Commits[flatIndex].OldOID != oid {
				return HistoryRewritePlan{}, fmt.Errorf("history rewrite plan: groups must contain every offered oid exactly once in chronological order")
			}
			commit, ok := byOID[oid]
			if !ok {
				return HistoryRewritePlan{}, fmt.Errorf("history rewrite plan: unknown oid %s", oid)
			}
			if commit.AuthorName != first.AuthorName || commit.AuthorEmail != first.AuthorEmail {
				return HistoryRewritePlan{}, fmt.Errorf("history rewrite plan: group %d crosses an author boundary at %s", groupIndex+1, oid)
			}
			paths = append(paths, commit.ChangedPaths...)
			if commit.DiffIncluded {
				diffs = append(diffs, commit.RedactedDiff)
			}
			flatIndex++
		}
		validated, err := ValidateCommitRewriteProposal(CommitRewriteRequest{
			OldOID:       group.OldOIDs[len(group.OldOIDs)-1],
			ChangedPaths: paths,
			RedactedDiff: strings.Join(diffs, "\n"),
			DiffIncluded: len(diffs) > 0,
			CommitFormat: req.CommitFormat,
		}, Result{Subject: group.Subject, Body: group.Body, Source: plan.Source})
		if err != nil {
			return HistoryRewritePlan{}, fmt.Errorf("history rewrite plan: group %d message: %w", groupIndex+1, err)
		}
		result.Groups = append(result.Groups, HistoryRewriteGroup{
			OldOIDs:        append([]string(nil), group.OldOIDs...),
			Subject:        validated.Subject,
			Body:           validated.Body,
			GroupingReason: reason,
		})
	}
	if flatIndex != len(req.Commits) {
		return HistoryRewritePlan{}, fmt.Errorf("history rewrite plan: groups included %d of %d offered commits", flatIndex, len(req.Commits))
	}
	return result, nil
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
	CommitFormat    CommitFormat             `json:"commit_format,omitempty"`
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
		CommitMessageFormatInstructions(req.CommitFormat) + "\n" + string(body), nil
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
	planReq := IntentPlanRequest{CommitFormat: effectiveCommitFormat(req.CommitFormat), OfferedCaptures: []OfferedCapture{{Seq: 1, Path: firstRewritePath(req.ChangedPaths), CapturedDiff: req.RedactedDiff}}}
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

// ProposeHistoryRewritePlan on composed providers uses only the primary
// provider. Historical grouping never falls back to local rules.
func (c *composed) ProposeHistoryRewritePlan(ctx context.Context, req HistoryRewritePlanRequest) (HistoryRewritePlan, error) {
	p, ok := c.primary.(HistoryRewritePlanner)
	if !ok {
		return HistoryRewritePlan{}, fmt.Errorf("ai: primary provider %s cannot plan grouped history rewrites", c.primary.Name())
	}
	return p.ProposeHistoryRewritePlan(ctx, req)
}
