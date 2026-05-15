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

// PathRecentCommitSuggestionExtendOrWait is the only suggested-action value
// surfaced today. v1 limits the planner hint to "extend the recent commit or
// wait for more captures"; future actions (e.g. an explicit amend trigger)
// will arrive alongside actual amend support.
const PathRecentCommitSuggestionExtendOrWait = "extend or wait"

// PathRecentCommit is a hint added to IntentPlanRequest when the captured
// path matches a HEAD commit landed within the affinity window
// (ACD_RECENT_COMMIT_AFFINITY_SECONDS). The hint is informational only —
// v1 does NOT amend on the planner's behalf; SuggestedAction is fixed at
// PathRecentCommitSuggestionExtendOrWait so the planner can lean toward
// extending the existing commit's intent rather than splitting a related
// change across two commits. Empty slice when no path matches.
type PathRecentCommit struct {
	Path            string `json:"path"`
	OID             string `json:"oid"`
	AgeSeconds      int64  `json:"age_seconds"`
	SuggestedAction string `json:"suggested_action"`
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
	LatestCommit      *CommitSummary      `json:"latest_commit,omitempty"`
	PathCommitContext []PathCommitContext `json:"path_commit_context,omitempty"`
	OfferedCaptures   []OfferedCapture    `json:"offered_captures"`
	ForcedAging       bool                `json:"forced_aging,omitempty"`
	// PathRecentCommits surfaces the prior-commit affinity hint computed by
	// the daemon (see ACD_RECENT_COMMIT_AFFINITY_SECONDS). The planner is
	// expected to read it as guidance only — v1 does not amend; the hint
	// merely informs the planner that an offered path matches a recent
	// HEAD commit so it can lean toward extending the existing commit
	// rather than splitting related work.
	PathRecentCommits     []PathRecentCommit            `json:"path_recent_commits,omitempty"`
	CapturedDiffTransform prompttrace.TransformMetadata `json:"-"`
	// RetryCorrection carries a verbatim validation error from the previous
	// attempt when the composed planner is asking the provider to correct an
	// invalid plan. Providers append it to the planner user prompt as a
	// follow-up correction request so the planner can fix its mistake without
	// blowing away the captured context. Empty on first attempts; set only by
	// composed.PlanIntent's retry loop. The field is not serialized into the
	// shared wire payload — it is surfaced separately by the user-prompt
	// builder.
	RetryCorrection string `json:"-"`
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
	// PathRecentCommits forwards the daemon's prior-commit affinity hint
	// into IntentPlanRequest. The slice is shallow-cloned so callers can
	// reuse the input without observing in-place mutation.
	PathRecentCommits []PathRecentCommit
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
		PathRecentCommits: clonePathRecentCommits(opts.PathRecentCommits),
	}
	for _, offered := range opts.OfferedCaptures {
		cp := offered
		if opts.IncludeCapturedDiffs {
			input := cp.CapturedDiff
			redacted := RedactDiffSecrets(input)
			// Intent planner stage uses IntentStageDiffCap (16 KiB) rather
			// than the per-event DiffCap (4 KiB) so the planner sees enough
			// of each captured diff to reason about multi-file grouping.
			cp.CapturedDiff = Truncate(redacted, IntentStageDiffCap)
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

func clonePathRecentCommits(in []PathRecentCommit) []PathRecentCommit {
	if len(in) == 0 {
		return nil
	}
	out := make([]PathRecentCommit, len(in))
	copy(out, in)
	return out
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

// IntentPlanValidationCode classifies planner-output validation failures so
// providers can decide whether to normalize the plan locally or hard-fail.
// New codes must be appended to keep the wire-style ordering stable for any
// future telemetry mapping.
type IntentPlanValidationCode int

const (
	// IntentPlanValidationUnknown is the zero-value catch-all. Returned for
	// validation failures that do not fit a more specific code; the
	// composed retry loop still treats it as a validation error and may
	// retry.
	IntentPlanValidationUnknown IntentPlanValidationCode = iota
	// IntentPlanValidationDeferredReasonNotDeferred fires when a
	// DeferredReason carries a seq that is not present in DeferredSeqs
	// (the seq is selected, or absent from the offered window). Providers
	// can normalize by dropping the spurious entry; see
	// NormalizeIntentPlanDeferredReasons.
	IntentPlanValidationDeferredReasonNotDeferred
	// IntentPlanValidationDeferredReasonMissing fires when a deferred seq
	// has no entry in DeferredReasons. The composed retry loop quotes the
	// validator message back to the provider so it can synthesize a
	// reason on the second attempt.
	IntentPlanValidationDeferredReasonMissing
	// IntentPlanValidationShape covers structural mismatches that the
	// planner could correct on retry — empty selected_seqs, omitted
	// offered seqs, duplicate selected/deferred entries, overlap between
	// selected and deferred, or an empty subject/grouping_reason.
	IntentPlanValidationShape
	// IntentPlanValidationOfferedWindow covers planner output that
	// references a seq outside the offered window (selected or deferred).
	// Often a planner hallucination; retry quotes the validator message
	// back so the planner can drop the spurious seq.
	IntentPlanValidationOfferedWindow
)

// IntentPlanValidationError is the typed error returned by ValidateIntentPlan
// when the failure is one providers may normalize. Untyped errors keep using
// fmt.Errorf so existing string matches stay green.
type IntentPlanValidationError struct {
	Code    IntentPlanValidationCode
	Seq     int64
	Message string
}

func (e *IntentPlanValidationError) Error() string { return e.Message }

// ValidateIntentPlan rejects malformed or incomplete planner output before it
// can influence replay. All failures return *IntentPlanValidationError so the
// composed retry loop can quote the message back to the planner; the Code
// field classifies the failure for telemetry and future provider-side
// normalization.
func ValidateIntentPlan(req IntentPlanRequest, plan IntentPlan) error {
	if len(plan.SelectedSeqs) == 0 {
		return &IntentPlanValidationError{
			Code:    IntentPlanValidationShape,
			Message: "intent planner: selected_seqs must be non-empty",
		}
	}
	if strings.TrimSpace(plan.Subject) == "" {
		return &IntentPlanValidationError{
			Code:    IntentPlanValidationShape,
			Message: "intent planner: subject must be non-empty",
		}
	}
	if strings.TrimSpace(plan.GroupingReason) == "" {
		return &IntentPlanValidationError{
			Code:    IntentPlanValidationShape,
			Message: "intent planner: grouping_reason must be non-empty",
		}
	}

	offered := make(map[int64]struct{}, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		if capture.Seq == 0 {
			return &IntentPlanValidationError{
				Code:    IntentPlanValidationShape,
				Message: "intent planner: offered capture seq must be non-zero",
			}
		}
		if _, exists := offered[capture.Seq]; exists {
			return &IntentPlanValidationError{
				Code:    IntentPlanValidationShape,
				Seq:     capture.Seq,
				Message: fmt.Sprintf("intent planner: duplicate offered seq %d", capture.Seq),
			}
		}
		offered[capture.Seq] = struct{}{}
	}

	selected := make(map[int64]struct{}, len(plan.SelectedSeqs))
	for _, seq := range plan.SelectedSeqs {
		if _, ok := offered[seq]; !ok {
			return &IntentPlanValidationError{
				Code:    IntentPlanValidationOfferedWindow,
				Seq:     seq,
				Message: fmt.Sprintf("intent planner: selected seq %d outside offered window", seq),
			}
		}
		if _, exists := selected[seq]; exists {
			return &IntentPlanValidationError{
				Code:    IntentPlanValidationShape,
				Seq:     seq,
				Message: fmt.Sprintf("intent planner: duplicate selected seq %d", seq),
			}
		}
		selected[seq] = struct{}{}
	}

	deferred := make(map[int64]struct{}, len(plan.DeferredSeqs))
	for _, seq := range plan.DeferredSeqs {
		if _, ok := offered[seq]; !ok {
			return &IntentPlanValidationError{
				Code:    IntentPlanValidationOfferedWindow,
				Seq:     seq,
				Message: fmt.Sprintf("intent planner: deferred seq %d is unknown", seq),
			}
		}
		if _, exists := deferred[seq]; exists {
			return &IntentPlanValidationError{
				Code:    IntentPlanValidationShape,
				Seq:     seq,
				Message: fmt.Sprintf("intent planner: duplicate deferred seq %d", seq),
			}
		}
		if _, overlap := selected[seq]; overlap {
			return &IntentPlanValidationError{
				Code:    IntentPlanValidationShape,
				Seq:     seq,
				Message: fmt.Sprintf("intent planner: seq %d appears in selected and deferred", seq),
			}
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
			return &IntentPlanValidationError{
				Code:    IntentPlanValidationShape,
				Seq:     seq,
				Message: fmt.Sprintf("intent planner: offered seq %d omitted from selected/deferred", seq),
			}
		}
		return &IntentPlanValidationError{
			Code:    IntentPlanValidationShape,
			Message: "intent planner: selected/deferred coverage mismatch",
		}
	}

	reasons := make(map[int64]struct{}, len(plan.DeferredReasons))
	for _, reason := range plan.DeferredReasons {
		// Cross-check: every DeferredReason.Seq must appear in DeferredSeqs.
		// Planners that emit a reason for a selected seq, or for a seq not in
		// the offered window, get rejected here. Providers can pre-normalize
		// via NormalizeIntentPlanDeferredReasons to drop the spurious entry
		// and keep the rest of the plan.
		if _, ok := deferred[reason.Seq]; !ok {
			return &IntentPlanValidationError{
				Code:    IntentPlanValidationDeferredReasonNotDeferred,
				Seq:     reason.Seq,
				Message: fmt.Sprintf("intent planner: deferred reason references non-deferred seq %d", reason.Seq),
			}
		}
		if _, exists := reasons[reason.Seq]; exists {
			return &IntentPlanValidationError{
				Code:    IntentPlanValidationShape,
				Seq:     reason.Seq,
				Message: fmt.Sprintf("intent planner: duplicate deferred reason for seq %d", reason.Seq),
			}
		}
		if strings.TrimSpace(reason.Reason) == "" {
			return &IntentPlanValidationError{
				Code:    IntentPlanValidationDeferredReasonMissing,
				Seq:     reason.Seq,
				Message: fmt.Sprintf("intent planner: deferred seq %d reason must be non-empty", reason.Seq),
			}
		}
		reasons[reason.Seq] = struct{}{}
	}
	for seq := range deferred {
		if _, ok := reasons[seq]; !ok {
			return &IntentPlanValidationError{
				Code:    IntentPlanValidationDeferredReasonMissing,
				Seq:     seq,
				Message: fmt.Sprintf("intent planner: deferred seq %d missing reason", seq),
			}
		}
	}

	return nil
}

// IntentPlanReasonMarker is the synthesized reason text used by
// NormalizeIntentPlanDeferredReasons when a deferred seq has no planner-
// supplied reason. Persisting a fixed marker into decision_records.reason
// keeps the user-facing column non-blank so operators inspecting deferred
// captures see "planner omitted reason" rather than an empty string. The
// constant is exported so daemon-side ledger writers can reference it from
// tests and downstream renderers can recognize the marker.
const IntentPlanReasonMarker = "planner omitted reason"

// NormalizeIntentPlanDeferredReasons normalizes plan.DeferredReasons against
// plan.DeferredSeqs in two ways:
//
//  1. Drops DeferredReason entries whose Seq is not present in DeferredSeqs
//     (planner emitted a reason for a selected or non-offered seq). Providers
//     call this before ValidateIntentPlan so the spurious entry does not
//     collapse the entire plan to deterministic fallback.
//  2. Synthesizes DeferredReason entries for deferred seqs that have no
//     matching reason from the planner. The synthesized Reason carries the
//     IntentPlanReasonMarker so the marker round-trips into the decision
//     ledger and operators see "planner omitted reason" instead of blank.
//
// Returns the cleaned plan, the dropped seqs in input order (over-emitted by
// the planner), and the synthesized seqs in DeferredSeqs order (planner
// omitted them). Callers emit a single slog.Warn naming both lists when
// either is non-empty so double-normalize defense-in-depth runs (e.g., the
// daemon's planIntentWithFallback) stay silent on the second pass.
//
// Aliasing: on the no-op path the returned plan.DeferredReasons still aliases
// the caller's backing array; callers must not mutate the slice in place. On
// the drop or synth path the slice is freshly allocated.
func NormalizeIntentPlanDeferredReasons(plan IntentPlan) (IntentPlan, []int64, []int64) {
	if len(plan.DeferredReasons) == 0 && len(plan.DeferredSeqs) == 0 {
		return plan, nil, nil
	}
	deferred := make(map[int64]struct{}, len(plan.DeferredSeqs))
	for _, seq := range plan.DeferredSeqs {
		deferred[seq] = struct{}{}
	}
	cleaned := make([]DeferredReason, 0, len(plan.DeferredReasons))
	have := make(map[int64]struct{}, len(plan.DeferredReasons))
	var dropped []int64
	for _, r := range plan.DeferredReasons {
		if _, ok := deferred[r.Seq]; !ok {
			dropped = append(dropped, r.Seq)
			continue
		}
		// First reason wins; subsequent duplicates fall through to the
		// validator which rejects them. Synthesis only runs for seqs
		// that have no entry at all.
		if _, exists := have[r.Seq]; !exists {
			have[r.Seq] = struct{}{}
		}
		cleaned = append(cleaned, r)
	}
	var synthesized []int64
	for _, seq := range plan.DeferredSeqs {
		if _, ok := have[seq]; ok {
			continue
		}
		synthesized = append(synthesized, seq)
		cleaned = append(cleaned, DeferredReason{
			Seq:    seq,
			Reason: IntentPlanReasonMarker,
		})
		have[seq] = struct{}{}
	}
	if len(dropped) == 0 && len(synthesized) == 0 {
		return plan, nil, nil
	}
	plan.DeferredReasons = cleaned
	return plan, dropped, synthesized
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
	// DiffText is fed into Generate so the deterministic single-op path can
	// extract a symbol via DiffAwareSubject. When CapturedDiff is empty
	// (most fallback paths) this is a no-op and the subject stays at the
	// legacy basename verb form.
	result, err := p.Generate(ctx, CommitContext{
		Path:     selected.Path,
		Op:       selected.Op,
		DiffText: selected.CapturedDiff,
	})
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
