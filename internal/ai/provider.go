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

import "context"

// Provider abstracts commit-message generation. Implementations must be
// concurrency-safe; the run loop may invoke Generate from multiple
// goroutines (currently it does not, but the interface is shaped to allow
// a future async pipeline).
type Provider interface {
	Name() string
	Generate(ctx context.Context, cc CommitContext) (Result, error)
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
