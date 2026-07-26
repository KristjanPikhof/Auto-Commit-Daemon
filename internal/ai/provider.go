// Package ai exposes commit-message providers used by the replay loop.
//
// Three implementations live here:
//   - DeterministicProvider — pure rule-based subject + bullet body, used
//     as the always-available fallback (and the v1 default).
//   - OpenAIProvider — chat-completions call with a structured tool-call
//     constraint, sanitized via SanitizeMessage.
//   - (subprocess plugin lives in plugin_subprocess.go; not implemented
//     in this task.)
//
// Compose chains a primary and fallback Provider so callers never need to
// open-code the "try AI then deterministic" pattern.
package ai

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
)

// Provider abstracts commit-message generation. Implementations must be
// concurrency-safe; the run loop may invoke Generate from multiple
// goroutines (currently it does not, but the interface is shaped to allow
// a future async pipeline).
type Provider interface {
	Name() string
	Generate(ctx context.Context, cc CommitContext) (Result, error)
}

// DiffProvider is an optional capability for providers that can declare
// whether they need CommitContext.DiffText populated.
type DiffProvider interface {
	NeedsDiff() bool
}

// ProviderNeedsDiff reports whether the daemon should reconstruct captured
// diffs before calling p. Providers that do not implement the optional
// capability default to true because network/plugin providers are the paths
// that benefit from diff context.
func ProviderNeedsDiff(p Provider) bool {
	if p == nil {
		return false
	}
	if dp, ok := p.(DiffProvider); ok {
		return dp.NeedsDiff()
	}
	return true
}

// Compose returns a Provider that calls `primary`, and on error or zero
// result, falls back to `fallback`. The Result.Source field reports which
// provider actually satisfied the request — useful for telemetry and for
// pinpointing which path the message came from in commit history.
//
// A nil primary degenerates to the fallback alone (so callers can build
// "deterministic only" without conditional wiring). A nil fallback is a
// programming error; v1 always pairs the AI provider with deterministic.
func Compose(primary, fallback Provider) Provider {
	if fallback == nil {
		panic("ai: Compose requires a non-nil fallback provider")
	}
	if primary == nil {
		return fallback
	}
	return &composed{primary: primary, fallback: fallback}
}

type composed struct {
	primary  Provider
	fallback Provider
}

func (c *composed) Name() string {
	return c.primary.Name() + "+" + c.fallback.Name()
}

// PrimaryProviderName returns the provider that receives the first request in a
// composed chain. It is useful for diagnostics that need to associate fallback
// records with the original provider request.
func PrimaryProviderName(p interface{ Name() string }) string {
	if p == nil {
		return ""
	}
	if c, ok := p.(*composed); ok && c.primary != nil {
		return c.primary.Name()
	}
	return p.Name()
}

// NeedsDiff is true when either side of the fallback chain can consume diff
// text. A primary AI provider still needs the diff even when the fallback is
// deterministic.
func (c *composed) NeedsDiff() bool {
	return ProviderNeedsDiff(c.primary) || ProviderNeedsDiff(c.fallback)
}

// Generate tries the primary provider; on error or empty subject we fall
// through to the fallback. Source is rewritten to reflect whichever
// provider produced the final Result so downstream telemetry sees the
// actual source rather than the composed alias.
func (c *composed) Generate(ctx context.Context, cc CommitContext) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	r, err := c.primary.Generate(ctx, cc)
	if err == nil && r.Subject != "" {
		if r.Source == "" {
			r.Source = c.primary.Name()
		}
		return r, nil
	}
	reason := "empty subject"
	if err != nil {
		reason = err.Error()
	}
	recordPromptFallback(ctx, "event", c.primary.Name(), c.fallback.Name(), reason)
	r, ferr := c.fallback.Generate(ctx, cc)
	if ferr != nil {
		// Surface the fallback error; the primary error becomes
		// secondary context (the run loop logs both).
		return Result{}, ferr
	}
	if r.Source == "" {
		r.Source = c.fallback.Name()
	}
	return r, nil
}

func recordPromptFallback(ctx context.Context, strategy, primary, fallback, reason string) {
	logger, meta, ok := prompttrace.From(ctx)
	if !ok {
		return
	}
	if meta.Strategy == "" {
		meta.Strategy = strategy
	}
	reason = SanitizePlannerError(reason)
	logger.Record(prompttrace.Record{
		Stage:        "fallback",
		Strategy:     meta.Strategy,
		Provider:     primary,
		Model:        meta.Model,
		Seq:          meta.Seq,
		OfferedSeqs:  append([]int64(nil), meta.OfferedSeqs...),
		BranchRef:    meta.BranchRef,
		Generation:   meta.Generation,
		DiffIncluded: meta.DiffIncluded,
		DiffCap:      meta.DiffCap,
		Response: &prompttrace.Response{
			FallbackProvider: fallback,
			FallbackReason:   reason,
		},
	})
}

// PlanIntent uses the primary planner when it supports intent planning. Unlike
// Generate, primary planner errors and invalid plans are returned directly so
// replay can record intent_planner_error diagnostics before invoking its own
// deterministic fallback path.
//
// On a typed *IntentPlanValidationError from the primary planner, PlanIntent
// retries the primary with the validator message quoted verbatim in the planner
// request's RetryCorrection field. The retry cap defaults to
// DefaultIntentRetryOnInvalid and can be overridden with
// ACD_INTENT_RETRY_ON_INVALID. Transport errors (timeouts, HTTP errors, context
// cancellation, network failures) and untyped validation errors do not trigger
// a retry. The retry path fires whether the typed error originates from the
// primary's own internal ValidateIntentPlan call (returned through PlanIntent)
// or from the composed-layer re-validation that runs after normalization.
func (c *composed) PlanIntent(ctx context.Context, req IntentPlanRequest) (IntentPlan, error) {
	if err := ctx.Err(); err != nil {
		return IntentPlan{}, err
	}
	if primary, ok := c.primary.(IntentPlanner); ok {
		plan, err := c.runPrimaryWithRetry(ctx, primary, req)
		if err != nil {
			return IntentPlan{}, err
		}
		return plan, nil
	}
	fallback, ok := c.fallback.(IntentPlanner)
	if !ok {
		return IntentPlan{}, errors.New("ai: composed fallback does not implement intent planning")
	}
	plan, err := fallback.PlanIntent(ctx, req)
	if err != nil {
		return IntentPlan{}, err
	}
	plan = NormalizeIntentPlanReasons(plan)
	plan, dropped, synthesized, overlapRemoved := NormalizeIntentPlanDeferredReasons(plan)
	logIntentPlanNormalization(c.fallback.Name(), dropped, synthesized, overlapRemoved)
	if err := ValidateIntentPlan(req, plan); err != nil {
		return IntentPlan{}, err
	}
	if plan.Source == "" {
		plan.Source = c.fallback.Name()
	}
	return plan, nil
}

// PlanIntentV2 keeps candidate fallback policy outside the provider layer. A
// primary transport or validation failure is returned to the candidate engine;
// it is never converted into a deterministic success here (notably for the
// Quality preset). Legacy primary planners are adapted and explicitly labeled
// v1_compat.
func (c *composed) PlanIntentV2(ctx context.Context, req IntentPlanRequestV2) (IntentPlanV2, error) {
	if err := ctx.Err(); err != nil {
		return IntentPlanV2{}, err
	}
	if err := ValidateIntentPlanRequestV2(req); err != nil {
		return IntentPlanV2{}, err
	}
	recordIntentAttempt(ctx)
	plan, err := PlanIntentV2WithCompatibility(ctx, c.primary, req)
	if err != nil {
		return IntentPlanV2{}, err
	}
	if err := ValidateIntentPlanV2(req, plan); err != nil {
		return IntentPlanV2{}, err
	}
	return applyIntentV2MessageQuality(ctx, c.primary, req, plan)
}

func applyIntentV2MessageQuality(ctx context.Context, provider Provider, req IntentPlanRequestV2, plan IntentPlanV2) (IntentPlanV2, error) {
	legacyReq := LegacyIntentPlanRequest(req)
	out := plan
	for i, candidate := range plan.Candidates {
		if candidate.Readiness != IntentCandidateReady {
			continue
		}
		locked := IntentPlan{
			SelectedSeqs:   append([]int64(nil), candidate.SelectedSeqs...),
			Subject:        candidate.Subject,
			Body:           candidate.Body,
			GroupingReason: candidate.GroupingReason,
			Source:         provider.Name(),
		}
		checked, err := applyIntentMessageQuality(ctx, provider, legacyReq, locked)
		if err != nil {
			return IntentPlanV2{}, err
		}
		out.Candidates[i].Subject = checked.Subject
		out.Candidates[i].Body = checked.Body
	}
	if err := ValidateIntentPlanV2(req, out); err != nil {
		return IntentPlanV2{}, err
	}
	return out, nil
}

// logIntentPlanNormalization emits a single deterministic slog.Warn naming
// both dropped and synthesized seqs from a NormalizeIntentPlanDeferredReasons
// call. No-op when both lists are empty so defense-in-depth re-normalization
// stays silent on the second pass.
func logIntentPlanNormalization(provider string, dropped, synthesized, overlapRemoved []int64) {
	if len(dropped) == 0 && len(synthesized) == 0 && len(overlapRemoved) == 0 {
		return
	}
	attrs := []any{slog.String("provider", provider)}
	if len(dropped) > 0 {
		attrs = append(attrs, slog.Any("dropped_seqs", dropped))
	}
	if len(synthesized) > 0 {
		attrs = append(attrs, slog.Any("synthesized_seqs", synthesized))
	}
	if len(overlapRemoved) > 0 {
		attrs = append(attrs, slog.Any("overlap_removed_seqs", overlapRemoved))
	}
	slog.Warn("intent planner: normalized deferred_reasons", attrs...)
}

// runPrimaryWithRetry runs the primary planner once, then up to the configured
// retry limit. On a typed validation error it appends the validator message to
// the request via RetryCorrection and calls the planner again. Once retries are
// exhausted, any error is returned to the composed caller, which records the
// intent_planner_error decision and falls back to deterministic.
func (c *composed) runPrimaryWithRetry(ctx context.Context, primary IntentPlanner, req IntentPlanRequest) (IntentPlan, error) {
	maxRetries := intentRetryOnInvalidLimit(ctx)
	maxAttempts := 1 + maxRetries
	currentReq := req
	var (
		lastErr        error
		dropped        []int64
		synthesized    []int64
		overlapRemoved []int64
	)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		recordIntentAttempt(ctx)
		plan, err := primary.PlanIntent(ctx, currentReq)
		if err == nil {
			plan = NormalizeIntentPlanReasons(plan)
			// Pre-declared `dropped` / `synthesized` so the assignment uses
			// `=` (not `:=`) and does NOT shadow the outer `plan` declared
			// by the for-loop's `plan, err := primary.PlanIntent(...)`.
			// The earlier `:=` form silently created a fresh inner binding
			// that the surrounding return paths could never observe.
			plan, dropped, synthesized, overlapRemoved = NormalizeIntentPlanDeferredReasons(plan)
			logIntentPlanNormalization(c.primary.Name(), dropped, synthesized, overlapRemoved)
			err = ValidateIntentPlan(req, plan)
			if err == nil {
				if plan.Source == "" {
					plan.Source = c.primary.Name()
				}
				plan, err = applyIntentMessageQuality(ctx, c.primary, req, plan)
				if err != nil {
					lastErr = err
					break
				}
				return plan, nil
			}
		}
		lastErr = err
		// Decide whether to retry. Only typed validation errors qualify,
		// and the configured limit counts retries after the initial call.
		var typed *IntentPlanValidationError
		if attempt >= maxAttempts || !errors.As(err, &typed) {
			break
		}
		// Skip retry for codes the provider-side normalizer is supposed to
		// heal. If validation still fails after NormalizeIntentPlanDeferredReasons
		// ran on these codes, the planner output is in a worse state than
		// the error code suggests; a second round-trip would burn budget
		// without changing the outcome.
		if typed.Code == IntentPlanValidationDeferredReasonNotDeferred ||
			typed.Code == IntentPlanValidationDeferredReasonMissing {
			slog.Info("intent planner: skipping retry for healed code",
				slog.String("provider", c.primary.Name()),
				slog.Int("code", int(typed.Code)),
				slog.Int64("seq", typed.Seq),
			)
			break
		}
		slog.Info("intent planner: retry attempted",
			slog.String("provider", c.primary.Name()),
			slog.Int("retry", attempt),
			slog.Int("max_retries", maxRetries),
			slog.Int("code", int(typed.Code)),
			slog.Int64("seq", typed.Seq),
			slog.String("error", SanitizePlannerError(typed.Message)),
		)
		currentReq = req
		currentReq.RetryCorrection = typed.Message
	}
	return IntentPlan{}, lastErr
}

func applyIntentMessageQuality(ctx context.Context, provider Provider, req IntentPlanRequest, plan IntentPlan) (IntentPlan, error) {
	if len(plan.CommitGroups) > 0 {
		out := plan
		for i, group := range plan.CommitGroups {
			groupPlan := IntentPlanForCommitGroup(plan, group)
			checked, err := applyIntentMessageQuality(ctx, provider, req, groupPlan)
			if err != nil {
				return IntentPlan{}, err
			}
			out.CommitGroups[i].Subject = checked.Subject
			out.CommitGroups[i].Body = checked.Body
			if checked.MessageQuality != "" {
				out.MessageQuality = checked.MessageQuality
				out.MessageQualityReason = checked.MessageQualityReason
			}
		}
		return out, nil
	}
	report := EvaluateIntentPlanMessageQuality(req, plan)
	switch report.Action {
	case MessageQualityClean:
		return plan, nil
	case MessageQualitySanitizeAccept:
		plan.Subject = report.SanitizedSubject
		plan.Body = report.SanitizedBody
		plan.MessageQuality = MessageQualitySanitizeAccept
		plan.MessageQualityReason = messageQualitySummary(report)
		return plan, nil
	case MessageQualityRewrite:
		rewriter, ok := provider.(IntentMessageRewriter)
		if !ok {
			return IntentPlan{}, &MessageQualityError{Provider: provider.Name(), Report: report}
		}
		rewriteReq := NewIntentMessageRewriteRequest(req, plan, report)
		result, err := rewriter.RewriteIntentMessage(ctx, rewriteReq)
		if err != nil {
			return IntentPlan{}, &MessageQualityError{Provider: provider.Name(), Report: report, Cause: err}
		}
		candidate := plan
		candidate.Subject = result.Subject
		candidate.Body = result.Body
		next := EvaluateIntentPlanMessageQuality(req, candidate)
		switch next.Action {
		case MessageQualityClean:
			candidate.MessageQuality = MessageQualityRewrite
			candidate.MessageQualityReason = messageQualitySummary(report)
			return candidate, nil
		case MessageQualitySanitizeAccept:
			candidate.Subject = next.SanitizedSubject
			candidate.Body = next.SanitizedBody
			candidate.MessageQuality = MessageQualityRewrite
			candidate.MessageQualityReason = messageQualitySummary(report)
			return candidate, nil
		default:
			return IntentPlan{}, &MessageQualityError{Provider: provider.Name(), Report: next}
		}
	default:
		return IntentPlan{}, &MessageQualityError{Provider: provider.Name(), Report: report}
	}
}

func messageQualitySummary(report MessageQualityReport) string {
	if len(report.Reasons) == 0 {
		return string(report.Action)
	}
	codes := make([]string, 0, len(report.Reasons))
	for _, reason := range report.Reasons {
		codes = append(codes, string(reason.Code))
	}
	return strings.Join(codes, ",")
}

func withPromptTraceStrategy(ctx context.Context, strategy string, offeredSeqs []int64, diffIncluded bool, diffCap int) context.Context {
	logger, meta, ok := prompttrace.From(ctx)
	if !ok {
		return ctx
	}
	meta.Strategy = strategy
	meta.OfferedSeqs = append([]int64(nil), offeredSeqs...)
	meta.DiffIncluded = diffIncluded
	meta.DiffCap = diffCap
	return prompttrace.With(ctx, logger, meta)
}

// intentRetryOnInvalidLimit reports how many correction retries the composed
// planner may make after typed validation errors. The initial planner call is
// not counted. Empty or invalid values use DefaultIntentRetryOnInvalid.
// False-like values disable retries for cost-sensitive environments.
type intentRetryLimitContextKey struct{}
type intentAttemptCounterContextKey struct{}

// IntentAttemptCounter observes composed planner attempts without changing
// request payloads, prompts, logs, or provider output. RetryCount excludes the
// initial attempt.
type IntentAttemptCounter struct{ attempts atomic.Int64 }

func WithIntentAttemptCounter(ctx context.Context) (context.Context, *IntentAttemptCounter) {
	if ctx == nil {
		ctx = context.Background()
	}
	counter := &IntentAttemptCounter{}
	return context.WithValue(ctx, intentAttemptCounterContextKey{}, counter), counter
}

func (c *IntentAttemptCounter) RetryCount() int {
	if c == nil {
		return 0
	}
	attempts := c.attempts.Load()
	if attempts <= 1 {
		return 0
	}
	return int(attempts - 1)
}

func recordIntentAttempt(ctx context.Context) {
	if ctx == nil {
		return
	}
	if counter, ok := ctx.Value(intentAttemptCounterContextKey{}).(*IntentAttemptCounter); ok && counter != nil {
		counter.attempts.Add(1)
	}
}

// WithIntentRetryLimit pins correction retries to one immutable runtime
// bundle for the lifetime of ctx. Negative limits are clamped to zero. When
// absent, the established environment/default behavior is unchanged.
func WithIntentRetryLimit(ctx context.Context, limit int) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if limit < 0 {
		limit = 0
	}
	return context.WithValue(ctx, intentRetryLimitContextKey{}, limit)
}

func intentRetryOnInvalidLimit(ctx context.Context) int {
	if ctx != nil {
		if limit, ok := ctx.Value(intentRetryLimitContextKey{}).(int); ok {
			if limit < 0 {
				return 0
			}
			return limit
		}
	}
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(EnvIntentRetryOnInvalid)))
	switch raw {
	case "":
		return DefaultIntentRetryOnInvalid
	case "false", "no", "off":
		return 0
	case "true", "yes", "on":
		return DefaultIntentRetryOnInvalid
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return DefaultIntentRetryOnInvalid
	}
	return n
}
