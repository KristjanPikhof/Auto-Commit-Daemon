package ai

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
)

// IntentPlanner chooses which captured events belong in the next commit.
// Implementations must validate plans before returning them so replay never
// sees an incomplete or malformed grouping decision.
type IntentPlanner interface {
	Name() string
	PlanIntent(ctx context.Context, req IntentPlanRequest) (IntentPlan, error)
}

// CommitSummary provides recent commit context to the planner.
type CommitSummary struct {
	OID       string    `json:"oid,omitempty"`
	Subject   string    `json:"subject"`
	Body      string    `json:"body,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
	Paths     []string  `json:"paths,omitempty"`
}

// PathCommitContext scopes recent history to a path touched by an offered
// capture. It is optional and may be omitted when the caller cannot compute it.
type PathCommitContext struct {
	Path    string          `json:"path"`
	Commits []CommitSummary `json:"commits"`
}

// OfferedCapture is one capture the planner may either select or defer.
type OfferedCapture struct {
	Seq          int64     `json:"seq"`
	Path         string    `json:"path"`
	Op           string    `json:"op"`
	Timestamp    time.Time `json:"timestamp"`
	Fidelity     string    `json:"fidelity"`
	DeferCount   int       `json:"defer_count"`
	CapturedDiff string    `json:"captured_diff,omitempty"`
}

// IntentPlanRequest is the structured planner input shared by OpenAI-compatible
// providers and subprocess plugins.
type IntentPlanRequest struct {
	LatestCommit          *CommitSummary                `json:"latest_commit,omitempty"`
	PathCommitContext     []PathCommitContext           `json:"path_commit_context,omitempty"`
	OfferedCaptures       []OfferedCapture              `json:"offered_captures"`
	ForcedAging           bool                          `json:"forced_aging,omitempty"`
	CapturedDiffTransform prompttrace.TransformMetadata `json:"-"`
}

// IntentPlanRequestOptions carries raw captured diffs before the request is
// serialized. IncludeCapturedDiffs must be true only after the daemon's current
// diff-egress gates allow it: provider NeedsDiff=true and operator opt-in.
type IntentPlanRequestOptions struct {
	LatestCommit         *CommitSummary
	PathCommitContext    []PathCommitContext
	OfferedCaptures      []OfferedCapture
	ForcedAging          bool
	IncludeCapturedDiffs bool
}

// NewIntentPlanRequest builds a planner request and applies the egress policy
// for captured diffs. Forced-aging requests must contain exactly one offered
// capture: the overdue capture being forced through the planner window.
func NewIntentPlanRequest(opts IntentPlanRequestOptions) (IntentPlanRequest, error) {
	if opts.ForcedAging && len(opts.OfferedCaptures) != 1 {
		return IntentPlanRequest{}, fmt.Errorf("intent planner: forced-aging request offered %d captures, want 1", len(opts.OfferedCaptures))
	}
	req := IntentPlanRequest{
		LatestCommit:      cloneCommitSummary(opts.LatestCommit),
		PathCommitContext: clonePathCommitContext(opts.PathCommitContext),
		OfferedCaptures:   make([]OfferedCapture, 0, len(opts.OfferedCaptures)),
		ForcedAging:       opts.ForcedAging,
	}
	for _, offered := range opts.OfferedCaptures {
		cp := offered
		if opts.IncludeCapturedDiffs {
			input := cp.CapturedDiff
			redacted := RedactDiffSecrets(input)
			cp.CapturedDiff = Truncate(redacted, DiffCap)
			req.CapturedDiffTransform = mergePromptTransformMetadata(req.CapturedDiffTransform, promptTransformMetadata(input, redacted, cp.CapturedDiff))
		} else {
			cp.CapturedDiff = ""
		}
		req.OfferedCaptures = append(req.OfferedCaptures, cp)
	}
	return req, nil
}

func mergePromptTransformMetadata(a, b prompttrace.TransformMetadata) prompttrace.TransformMetadata {
	return prompttrace.TransformMetadata{
		RedactionApplied: a.RedactionApplied || b.RedactionApplied,
		Truncated:        a.Truncated || b.Truncated,
		InputBytes:       a.InputBytes + b.InputBytes,
		RedactedBytes:    a.RedactedBytes + b.RedactedBytes,
		OutputBytes:      a.OutputBytes + b.OutputBytes,
	}
}

func cloneCommitSummary(in *CommitSummary) *CommitSummary {
	if in == nil {
		return nil
	}
	out := *in
	out.Paths = append([]string(nil), in.Paths...)
	return &out
}

func clonePathCommitContext(in []PathCommitContext) []PathCommitContext {
	if len(in) == 0 {
		return nil
	}
	out := make([]PathCommitContext, 0, len(in))
	for _, item := range in {
		cp := PathCommitContext{
			Path:    item.Path,
			Commits: make([]CommitSummary, 0, len(item.Commits)),
		}
		for i := range item.Commits {
			commit := item.Commits[i]
			commit.Paths = append([]string(nil), item.Commits[i].Paths...)
			cp.Commits = append(cp.Commits, commit)
		}
		out = append(out, cp)
	}
	return out
}

// DeferredReason explains why an offered capture was not selected.
type DeferredReason struct {
	Seq    int64  `json:"seq"`
	Reason string `json:"reason"`
}

// IntentPlan is the structured planner response. Every offered capture seq must
// appear in exactly one of SelectedSeqs or DeferredSeqs.
type IntentPlan struct {
	SelectedSeqs    []int64          `json:"selected_seqs"`
	DeferredSeqs    []int64          `json:"deferred_seqs"`
	Subject         string           `json:"subject"`
	Body            string           `json:"body,omitempty"`
	GroupingReason  string           `json:"grouping_reason"`
	DeferredReasons []DeferredReason `json:"deferred_reasons,omitempty"`
	Source          string           `json:"-"`
}

// IntentReasonCap bounds planner explanation fields before they are persisted
// into the decision ledger.
const IntentReasonCap = 512

// NormalizeIntentPlanReasons trims planner explanation fields, removes ASCII
// control characters, and caps them to a bounded size for diagnostics.
func NormalizeIntentPlanReasons(plan IntentPlan) IntentPlan {
	plan.GroupingReason = NormalizeIntentReason(plan.GroupingReason)
	for i := range plan.DeferredReasons {
		plan.DeferredReasons[i].Reason = NormalizeIntentReason(plan.DeferredReasons[i].Reason)
	}
	return plan
}

// NormalizeIntentReason applies the shared normalization for planner reason
// strings.
func NormalizeIntentReason(reason string) string {
	reason = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, reason)
	reason = strings.TrimSpace(reason)
	runes := []rune(reason)
	if len(runes) <= IntentReasonCap {
		return reason
	}
	return string(runes[:IntentReasonCap])
}

// ValidateIntentPlan rejects malformed or incomplete planner output before it
// can influence replay.
func ValidateIntentPlan(req IntentPlanRequest, plan IntentPlan) error {
	if len(plan.SelectedSeqs) == 0 {
		return errors.New("intent planner: selected_seqs must be non-empty")
	}
	if strings.TrimSpace(plan.Subject) == "" {
		return errors.New("intent planner: subject must be non-empty")
	}
	if strings.TrimSpace(plan.GroupingReason) == "" {
		return errors.New("intent planner: grouping_reason must be non-empty")
	}

	offered := make(map[int64]struct{}, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		if capture.Seq == 0 {
			return errors.New("intent planner: offered capture seq must be non-zero")
		}
		if _, exists := offered[capture.Seq]; exists {
			return fmt.Errorf("intent planner: duplicate offered seq %d", capture.Seq)
		}
		offered[capture.Seq] = struct{}{}
	}

	selected := make(map[int64]struct{}, len(plan.SelectedSeqs))
	for _, seq := range plan.SelectedSeqs {
		if _, ok := offered[seq]; !ok {
			return fmt.Errorf("intent planner: selected seq %d outside offered window", seq)
		}
		if _, exists := selected[seq]; exists {
			return fmt.Errorf("intent planner: duplicate selected seq %d", seq)
		}
		selected[seq] = struct{}{}
	}

	deferred := make(map[int64]struct{}, len(plan.DeferredSeqs))
	for _, seq := range plan.DeferredSeqs {
		if _, ok := offered[seq]; !ok {
			return fmt.Errorf("intent planner: deferred seq %d is unknown", seq)
		}
		if _, exists := deferred[seq]; exists {
			return fmt.Errorf("intent planner: duplicate deferred seq %d", seq)
		}
		if _, overlap := selected[seq]; overlap {
			return fmt.Errorf("intent planner: seq %d appears in selected and deferred", seq)
		}
		deferred[seq] = struct{}{}
	}

	if len(selected)+len(deferred) != len(offered) {
		for seq := range offered {
			if _, ok := selected[seq]; ok {
				continue
			}
			if _, ok := deferred[seq]; ok {
				continue
			}
			return fmt.Errorf("intent planner: offered seq %d omitted from selected/deferred", seq)
		}
		return errors.New("intent planner: selected/deferred coverage mismatch")
	}

	reasons := make(map[int64]struct{}, len(plan.DeferredReasons))
	for _, reason := range plan.DeferredReasons {
		if _, ok := deferred[reason.Seq]; !ok {
			return fmt.Errorf("intent planner: deferred reason references non-deferred seq %d", reason.Seq)
		}
		if _, exists := reasons[reason.Seq]; exists {
			return fmt.Errorf("intent planner: duplicate deferred reason for seq %d", reason.Seq)
		}
		if strings.TrimSpace(reason.Reason) == "" {
			return fmt.Errorf("intent planner: deferred seq %d reason must be non-empty", reason.Seq)
		}
		reasons[reason.Seq] = struct{}{}
	}
	for seq := range deferred {
		if _, ok := reasons[seq]; !ok {
			return fmt.Errorf("intent planner: deferred seq %d missing reason", seq)
		}
	}

	return nil
}

// PlanIntent provides the deterministic fallback planner. It selects exactly
// one capture and defers every other offered capture with explicit reasons.
func (p DeterministicProvider) PlanIntent(ctx context.Context, req IntentPlanRequest) (IntentPlan, error) {
	if err := ctx.Err(); err != nil {
		return IntentPlan{}, err
	}
	if len(req.OfferedCaptures) == 0 {
		return IntentPlan{}, errors.New("deterministic: no captures offered")
	}
	selected := req.OfferedCaptures[0]
	result, err := p.Generate(ctx, CommitContext{Path: selected.Path, Op: selected.Op})
	if err != nil {
		return IntentPlan{}, err
	}
	plan := IntentPlan{
		SelectedSeqs:   []int64{selected.Seq},
		Subject:        result.Subject,
		Body:           result.Body,
		GroupingReason: "deterministic fallback selected the earliest offered capture",
		Source:         p.Name(),
	}
	for _, capture := range req.OfferedCaptures[1:] {
		plan.DeferredSeqs = append(plan.DeferredSeqs, capture.Seq)
		plan.DeferredReasons = append(plan.DeferredReasons, DeferredReason{
			Seq:    capture.Seq,
			Reason: "deferred by deterministic fallback",
		})
	}
	plan = NormalizeIntentPlanReasons(plan)
	if err := ValidateIntentPlan(req, plan); err != nil {
		return IntentPlan{}, err
	}
	return plan, nil
}
