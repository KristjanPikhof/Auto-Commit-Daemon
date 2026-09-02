package ai

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrRewriteRequiresIntentStrategy is returned when rewrite plan generation is
	// requested while the effective commit strategy is not intent.
	ErrRewriteRequiresIntentStrategy = errors.New("history rewrite needs Intent mode")

	// ErrRewriteRequiresAIProvider is returned when rewrite plan generation is
	// requested without an explicitly configured non-deterministic AI provider.
	ErrRewriteRequiresAIProvider = errors.New("history rewrite needs an OpenAI-compatible or local subprocess provider")

	// ErrRewriteProviderCannotPlan is returned when the configured provider cannot
	// produce intent plans.
	ErrRewriteProviderCannotPlan = errors.New("the configured provider cannot plan a history rewrite")
)

// CheckRewritePlanGenerationGate verifies that the v1 rewrite-commits command
// may ask AI to generate a rewrite plan. Displaying or applying a saved plan is
// intentionally outside this gate: those paths do not need provider access.
func CheckRewritePlanGenerationGate(cfg ProviderConfig, provider Provider) error {
	if cfg.CommitStrategy != CommitStrategyIntent {
		return ErrRewriteRequiresIntentStrategy
	}
	mode := strings.TrimSpace(strings.ToLower(cfg.Mode))
	if mode == "" || mode == "deterministic" || PrimaryProviderName(provider) == "deterministic" {
		return ErrRewriteRequiresAIProvider
	}
	if _, ok := provider.(IntentPlanner); !ok {
		return fmt.Errorf("%w: %s", ErrRewriteProviderCannotPlan, PrimaryProviderName(provider))
	}
	return nil
}

// CheckHistoryRewritePlanGenerationGate verifies the provider capability for
// the requested history rewrite mode after the common Intent/provider gate.
func CheckHistoryRewritePlanGenerationGate(cfg ProviderConfig, provider Provider, messagesOnly bool) error {
	if err := CheckRewritePlanGenerationGate(cfg, provider); err != nil {
		return err
	}
	if messagesOnly {
		if _, ok := provider.(CommitRewriteProposer); !ok {
			return fmt.Errorf("%w: %s cannot rewrite commit messages", ErrRewriteProviderCannotPlan, PrimaryProviderName(provider))
		}
		return nil
	}
	if _, ok := provider.(HistoryRewritePlanner); !ok {
		return fmt.Errorf("%w: %s cannot group historical commits", ErrRewriteProviderCannotPlan, PrimaryProviderName(provider))
	}
	return nil
}
