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
func (c *composed) PlanIntent(ctx context.Context, req IntentPlanRequest) (IntentPlan, error) {
	if err := ctx.Err(); err != nil {
		return IntentPlan{}, err
	}
	if primary, ok := c.primary.(IntentPlanner); ok {
		plan, err := primary.PlanIntent(ctx, req)
		if err != nil {
			return IntentPlan{}, err
		}
		plan = NormalizeIntentPlanReasons(plan)
		plan, _ = NormalizeIntentPlanDeferredReasons(plan)
		if err := ValidateIntentPlan(req, plan); err != nil {
			return IntentPlan{}, err
		}
		if plan.Source == "" {
			plan.Source = c.primary.Name()
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
	if err := ValidateIntentPlan(req, plan); err != nil {
		return IntentPlan{}, err
	}
	if plan.Source == "" {
		plan.Source = c.fallback.Name()
	}
	return plan, nil
}
