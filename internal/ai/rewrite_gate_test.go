package ai

import (
	"context"
	"errors"
	"testing"
)

func TestCheckRewritePlanGenerationGate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ProviderConfig
		p       Provider
		wantErr error
	}{
		{
			name:    "event strategy disabled",
			cfg:     ProviderConfig{CommitStrategy: CommitStrategyEvent, Mode: "openai-compat"},
			p:       Compose(staticRewritePlannerProvider{name: "openai-compat"}, DeterministicProvider{}),
			wantErr: ErrRewriteRequiresIntentStrategy,
		},
		{
			name:    "deterministic default refused",
			cfg:     ProviderConfig{CommitStrategy: CommitStrategyIntent},
			p:       DeterministicProvider{},
			wantErr: ErrRewriteRequiresAIProvider,
		},
		{
			name:    "configured provider degraded to deterministic refused",
			cfg:     ProviderConfig{CommitStrategy: CommitStrategyIntent, Mode: "openai-compat"},
			p:       DeterministicProvider{},
			wantErr: ErrRewriteRequiresAIProvider,
		},
		{
			name:    "configured provider without planner refused",
			cfg:     ProviderConfig{CommitStrategy: CommitStrategyIntent, Mode: "openai-compat"},
			p:       rewriteGenerateOnlyProvider{name: "openai-compat"},
			wantErr: ErrRewriteProviderCannotPlan,
		},
		{
			name: "configured planner accepted",
			cfg:  ProviderConfig{CommitStrategy: CommitStrategyIntent, Mode: "openai-compat"},
			p:    Compose(staticRewritePlannerProvider{name: "openai-compat"}, DeterministicProvider{}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckRewritePlanGenerationGate(tt.cfg, tt.p)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("CheckRewritePlanGenerationGate returned error: %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("CheckRewritePlanGenerationGate error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

type rewriteGenerateOnlyProvider struct{ name string }

func (p rewriteGenerateOnlyProvider) Name() string { return p.name }

func (p rewriteGenerateOnlyProvider) Generate(_ context.Context, _ CommitContext) (Result, error) {
	return Result{Subject: "test", Source: p.name}, nil
}

type staticRewritePlannerProvider struct{ name string }

func (p staticRewritePlannerProvider) Name() string { return p.name }

func (p staticRewritePlannerProvider) Generate(_ context.Context, _ CommitContext) (Result, error) {
	return Result{Subject: "test", Source: p.name}, nil
}

func (p staticRewritePlannerProvider) PlanIntent(_ context.Context, req IntentPlanRequest) (IntentPlan, error) {
	return IntentPlan{SelectedSeqs: []int64{req.OfferedCaptures[0].Seq}, Subject: "test", Source: p.name}, nil
}
