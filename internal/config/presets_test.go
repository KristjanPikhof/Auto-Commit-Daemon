package config

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestPresetCatalogV2Contract(t *testing.T) {
	t.Parallel()
	catalog := PresetCatalog()
	if len(catalog) != 6 {
		t.Fatalf("preset count = %d, want 6", len(catalog))
	}
	tests := []struct {
		strategy CommitStrategy
		name     PresetName
		values   map[string]string
		fallback string
		hard     bool
	}{
		{StrategyEvent, PresetFast, map[string]string{
			FieldProvider: "deterministic", FieldDiffEgress: "false",
			FieldIntentVerification: "none", FieldIntentRepairEnabled: "false",
		}, "", false},
		{StrategyEvent, PresetBalanced, map[string]string{
			FieldDiffEgress: "true", FieldIntentRecentCommits: "5",
			FieldIntentRetryOnInvalid: "1",
		}, "", false},
		{StrategyEvent, PresetQuality, map[string]string{
			FieldDiffEgress: "true", FieldIntentRecentCommits: "10",
			FieldIntentRetryOnInvalid: "2",
		}, "", false},
		{StrategyIntent, PresetFast, map[string]string{
			FieldIntentWindow: "10", FieldIntentSettleWindow: "10s",
			FieldIntentMaxPendingAge: "1m30s", FieldIntentDeferLimit: "1",
			FieldIntentVerification: "none", FieldIntentRepairEnabled: "false",
		}, "hard_dependency_component", false},
		{StrategyIntent, PresetBalanced, map[string]string{
			FieldIntentWindow: "20", FieldIntentSettleWindow: "30s",
			FieldIntentMaxPendingAge: "3m0s", FieldIntentDeferLimit: "2",
			FieldIntentVerification: "fast", FieldIntentRepairEnabled: "true",
			FieldIntentRepairHorizon: "10m0s", FieldIntentRepairMaxCommits: "3",
			FieldVerificationFastTimeout: "2m0s",
		}, "verified_dependency_partition", false},
		{StrategyIntent, PresetQuality, map[string]string{
			FieldIntentWindow: "30", FieldIntentSettleWindow: "1m0s",
			FieldIntentMaxPendingAge: "10m0s", FieldIntentDeferLimit: "3",
			FieldIntentVerification: "full", FieldIntentRepairEnabled: "true",
			FieldIntentRepairHorizon: "30m0s", FieldIntentRepairMaxCommits: "5",
			FieldVerificationFullTimeout: "10m0s",
		}, "needs_attention", true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(string(tt.strategy)+"/"+string(tt.name), func(t *testing.T) {
			t.Parallel()
			preset, ok := LookupPreset(tt.strategy, tt.name)
			if !ok {
				t.Fatal("preset missing")
			}
			if preset.Version != 2 || preset.Reference() != string(tt.strategy)+"."+string(tt.name)+"@2" {
				t.Fatalf("identity = %s v%d", preset.Reference(), preset.Version)
			}
			for name, want := range tt.values {
				field, ok := LookupField(name)
				if !ok {
					t.Fatalf("owned field %q missing from catalog", name)
				}
				raw, ok := preset.Values[name]
				if !ok {
					t.Fatalf("preset does not own %q", name)
				}
				got, err := normalizeValue(field, raw)
				if err != nil || got != want {
					t.Fatalf("%s = %q, want %q (err=%v)", name, got, want, err)
				}
			}
			if preset.PlannerFallback != tt.fallback || preset.HardBoundaryPreferred != tt.hard {
				t.Fatalf("policy = fallback %q hard=%v", preset.PlannerFallback, preset.HardBoundaryPreferred)
			}
			if tt.strategy == StrategyIntent && (!preset.ProviderTestRequired || !preset.DiffContextRequired) {
				t.Fatal("regular Intent preset does not require tested provider and diff context")
			}
			if tt.name != PresetFast && tt.strategy == StrategyEvent &&
				(!preset.ProviderTestRequired || !preset.DiffContextRequired) {
				t.Fatal("non-fast Event preset does not require provider/diff context")
			}
		})
	}
	catalog[0].Values[FieldProvider] = "changed"
	again, _ := LookupPreset(StrategyEvent, PresetFast)
	if again.Values[FieldProvider] != "deterministic" {
		t.Fatal("caller mutated the immutable preset catalog")
	}
}

func TestResolveAllPresetPrecedenceAndCustomization(t *testing.T) {
	t.Parallel()
	input := ResolveInput{
		Repository: Overrides{
			FieldCommitStrategy: rawString("intent"),
			FieldIntentWindow:   rawString("19"),
		},
		Profile: Overrides{FieldIntentWindow: rawString("18")},
		Global:  Overrides{FieldIntentWindow: rawString("17")},
		LookupEnv: func(name string) (string, bool) {
			switch name {
			case "ACD_COMMIT_PRESET":
				return "quality", true
			case "ACD_INTENT_WINDOW":
				return "16", true
			default:
				return "", false
			}
		},
	}
	resolved, preset, err := ResolveAll(input, input.Repository)
	if err != nil {
		t.Fatal(err)
	}
	if preset.Reference() != "intent.quality@2" || !preset.Customized {
		t.Fatalf("preset = %+v", preset)
	}
	if got := resolved[FieldIntentWindow]; got.EffectiveValue() != "19" || got.Source != SourceRepository {
		t.Fatalf("repository precedence = %#v", got)
	}
	delete(input.Repository, FieldIntentWindow)
	resolved, _, err = ResolveAll(input, input.Repository)
	if err != nil {
		t.Fatal(err)
	}
	if got := resolved[FieldIntentWindow]; got.EffectiveValue() != "18" || got.Source != SourceProfile {
		t.Fatalf("profile precedence = %#v", got)
	}
	delete(input.Profile, FieldIntentWindow)
	delete(input.Global, FieldIntentWindow)
	input.LookupEnv = func(name string) (string, bool) {
		if name == "ACD_COMMIT_PRESET" {
			return "quality", true
		}
		return "", false
	}
	resolved, preset, err = ResolveAll(input, input.Repository)
	if err != nil {
		t.Fatal(err)
	}
	if got := resolved[FieldIntentWindow]; got.EffectiveValue() != "30" || got.Source != SourcePreset {
		t.Fatalf("preset precedence = %#v", got)
	}
	if preset.Customized {
		t.Fatal("matching preset without advanced overrides marked customized")
	}
}

func TestIntentDefaultsBalancedAndResetOwnsOnlyPresetFields(t *testing.T) {
	t.Parallel()
	input := ResolveInput{Repository: Overrides{FieldCommitStrategy: rawString("intent")}}
	resolved, preset, err := ResolveAll(input, input.Repository)
	if err != nil {
		t.Fatal(err)
	}
	if preset.Reference() != "intent.balanced@2" ||
		resolved[FieldCommitPreset].EffectiveValue() != "balanced" {
		t.Fatalf("implicit Intent preset = %+v / %#v", preset, resolved[FieldCommitPreset])
	}
	values := Overrides{
		FieldCommitStrategy: rawString("intent"),
		FieldCommitPreset:   rawString("balanced"),
		FieldIntentWindow:   rawString("22"),
		FieldModel:          rawString("local-model"),
	}
	cleaned := ResetPresetOverrides(values, preset.Definition)
	want := Overrides{
		FieldCommitStrategy: rawString("intent"),
		FieldCommitPreset:   rawString("balanced"),
		FieldModel:          rawString("local-model"),
	}
	if !reflect.DeepEqual(cleaned, want) {
		t.Fatalf("reset = %v, want %v", cleaned, want)
	}
}

func TestPresetFieldValidationCapsRepairChain(t *testing.T) {
	t.Parallel()
	doc := NewDocument()
	doc.Settings.Global[FieldIntentRepairMaxCommits] = json.RawMessage(`"6"`)
	if err := ValidateDocument(doc); err == nil {
		t.Fatal("repair chain above five commits accepted")
	}
}
