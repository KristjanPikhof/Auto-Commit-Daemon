package config

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const PresetCatalogVersion = 2

type CommitStrategy string

const (
	StrategyEvent  CommitStrategy = "event"
	StrategyIntent CommitStrategy = "intent"
)

type PresetName string

const (
	PresetFast     PresetName = "fast"
	PresetBalanced PresetName = "balanced"
	PresetQuality  PresetName = "quality"
)

// PresetDefinition is an immutable built-in configuration layer. Values lists
// only fields owned by the preset, which also defines reset/customization
// behavior.
type PresetDefinition struct {
	Strategy              CommitStrategy
	Name                  PresetName
	Version               int
	Values                map[string]string
	ProviderTestRequired  bool
	DiffContextRequired   bool
	HardBoundaryPreferred bool
	PlannerFallback       string
}

func (p PresetDefinition) ID() string {
	return fmt.Sprintf("%s.%s", p.Strategy, p.Name)
}

func (p PresetDefinition) Reference() string {
	return fmt.Sprintf("%s@%d", p.ID(), p.Version)
}

type PresetResolution struct {
	Definition PresetDefinition
	Customized bool
}

func (p PresetResolution) ID() string        { return p.Definition.ID() }
func (p PresetResolution) Reference() string { return p.Definition.Reference() }
func (p PresetResolution) Version() int      { return p.Definition.Version }

var presetCatalog = []PresetDefinition{
	{
		Strategy: StrategyEvent, Name: PresetFast, Version: PresetCatalogVersion,
		Values: map[string]string{
			FieldProvider: "deterministic", FieldDiffEgress: "false",
			FieldIntentRecentCommits: "5", FieldIntentRetryOnInvalid: "0",
			FieldIntentPathCoalescing: "false", FieldIntentVerification: "none",
			FieldIntentRepairEnabled: "false", FieldIntentRepairHorizon: "10m",
			FieldIntentRepairMaxCommits: "3",
		},
	},
	{
		Strategy: StrategyEvent, Name: PresetBalanced, Version: PresetCatalogVersion,
		ProviderTestRequired: true, DiffContextRequired: true,
		Values: map[string]string{
			FieldDiffEgress: "true", FieldIntentRecentCommits: "5",
			FieldIntentRetryOnInvalid: "1", FieldIntentPathCoalescing: "false",
			FieldIntentVerification: "none", FieldIntentRepairEnabled: "false",
			FieldIntentRepairHorizon: "10m", FieldIntentRepairMaxCommits: "3",
		},
	},
	{
		Strategy: StrategyEvent, Name: PresetQuality, Version: PresetCatalogVersion,
		ProviderTestRequired: true, DiffContextRequired: true,
		Values: map[string]string{
			FieldDiffEgress: "true", FieldIntentRecentCommits: "10",
			FieldIntentRetryOnInvalid: "2", FieldIntentPathCoalescing: "false",
			FieldIntentVerification: "none", FieldIntentRepairEnabled: "false",
			FieldIntentRepairHorizon: "30m", FieldIntentRepairMaxCommits: "5",
		},
	},
	{
		Strategy: StrategyIntent, Name: PresetFast, Version: PresetCatalogVersion,
		ProviderTestRequired: true, DiffContextRequired: true, PlannerFallback: "hard_dependency_component",
		Values: map[string]string{
			FieldDiffEgress: "true", FieldIntentWindow: "10",
			FieldIntentMinPending: "10", FieldIntentSettleWindow: "10s",
			FieldIntentMaxPendingAge: "90s", FieldIntentDeferLimit: "1",
			FieldIntentRetryOnInvalid: "2", FieldIntentPathCoalescing: "false",
			FieldIntentVerification: "none", FieldIntentRepairEnabled: "false",
			FieldIntentRepairHorizon: "10m", FieldIntentRepairMaxCommits: "3",
		},
	},
	{
		Strategy: StrategyIntent, Name: PresetBalanced, Version: PresetCatalogVersion,
		ProviderTestRequired: true, DiffContextRequired: true, PlannerFallback: "verified_dependency_partition",
		Values: map[string]string{
			FieldDiffEgress: "true", FieldIntentWindow: "20",
			FieldIntentMinPending: "20", FieldIntentSettleWindow: "30s",
			FieldIntentMaxPendingAge: "3m", FieldIntentDeferLimit: "2",
			FieldIntentRetryOnInvalid: "2", FieldIntentPathCoalescing: "false",
			FieldIntentVerification: "fast", FieldIntentRepairEnabled: "true",
			FieldIntentRepairHorizon: "10m", FieldIntentRepairMaxCommits: "3",
			FieldVerificationFastTimeout: "2m",
		},
	},
	{
		Strategy: StrategyIntent, Name: PresetQuality, Version: PresetCatalogVersion,
		ProviderTestRequired: true, DiffContextRequired: true, HardBoundaryPreferred: true,
		PlannerFallback: "needs_attention",
		Values: map[string]string{
			FieldDiffEgress: "true", FieldIntentWindow: "30",
			FieldIntentMinPending: "30", FieldIntentSettleWindow: "60s",
			FieldIntentMaxPendingAge: "10m", FieldIntentDeferLimit: "3",
			FieldIntentRetryOnInvalid: "2", FieldIntentPathCoalescing: "false",
			FieldIntentVerification: "full", FieldIntentRepairEnabled: "true",
			FieldIntentRepairHorizon: "30m", FieldIntentRepairMaxCommits: "5",
			FieldVerificationFullTimeout: "10m",
		},
	},
}

func PresetCatalog() []PresetDefinition {
	out := make([]PresetDefinition, len(presetCatalog))
	for i := range presetCatalog {
		out[i] = clonePreset(presetCatalog[i])
	}
	return out
}

func LookupPreset(strategy CommitStrategy, name PresetName) (PresetDefinition, bool) {
	for _, preset := range presetCatalog {
		if preset.Strategy == strategy && preset.Name == name {
			return clonePreset(preset), true
		}
	}
	return PresetDefinition{}, false
}

func clonePreset(p PresetDefinition) PresetDefinition {
	out := p
	out.Values = make(map[string]string, len(p.Values))
	for name, value := range p.Values {
		out.Values[name] = value
	}
	return out
}

func defaultPreset(strategy CommitStrategy) PresetName {
	if strategy == StrategyIntent {
		return PresetBalanced
	}
	return PresetFast
}

// ResolvePreset selects the catalog entry using authored/environment values,
// then reports whether the selected authoring scope overrides preset-owned
// values. Experiment values remain the highest-priority authored layer.
func ResolvePreset(input ResolveInput, selectedScope Overrides) (PresetResolution, error) {
	strategyField, err := ResolveField(FieldCommitStrategy, withoutPreset(input))
	if err != nil {
		return PresetResolution{}, err
	}
	strategy := CommitStrategy(strategyField.EffectiveValue())
	name, set, err := explicitValue(FieldCommitPreset, withoutPreset(input))
	if err != nil {
		return PresetResolution{}, err
	}
	presetName := defaultPreset(strategy)
	if set {
		presetName = PresetName(name)
	}
	definition, ok := LookupPreset(strategy, presetName)
	if !ok {
		return PresetResolution{}, fmt.Errorf("acd config: unsupported preset %q for strategy %q", presetName, strategy)
	}
	return PresetResolution{
		Definition: definition,
		Customized: PresetCustomized(definition, selectedScope),
	}, nil
}

func withoutPreset(input ResolveInput) ResolveInput {
	input.Preset = nil
	return input
}

func explicitValue(name string, input ResolveInput) (string, bool, error) {
	field, ok := LookupField(name)
	if !ok {
		return "", false, fmt.Errorf("acd config: unknown field %q", name)
	}
	lookup := input.LookupEnv
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}
	for _, values := range []Overrides{input.Experiment, input.Repository, input.Profile, input.Global} {
		if raw, exists := values[name]; exists {
			value, err := validateRaw(field, raw, true)
			return value, true, err
		}
	}
	if value, set := lookup(field.Environment); set {
		normalized, err := normalizeValue(field, value)
		return normalized, err == nil, err
	}
	return "", false, nil
}

func presetOverrides(definition PresetDefinition) Overrides {
	out := make(Overrides, len(definition.Values)+1)
	for name, value := range definition.Values {
		out[name], _ = json.Marshal(value)
	}
	out[FieldCommitPreset], _ = json.Marshal(string(definition.Name))
	return out
}

// ResolveAll applies one selected preset below the environment and returns the
// normalized effective catalog.
func ResolveAll(input ResolveInput, selectedScope Overrides) (map[string]ResolvedField, PresetResolution, error) {
	preset, err := ResolvePreset(input, selectedScope)
	if err != nil {
		return nil, PresetResolution{}, err
	}
	input.Preset = presetOverrides(preset.Definition)
	out := make(map[string]ResolvedField, len(fieldCatalog))
	for _, field := range fieldCatalog {
		value, err := ResolveField(field.Name, input)
		if err != nil {
			return nil, PresetResolution{}, err
		}
		out[field.Name] = value
	}
	for name, expected := range preset.Definition.Values {
		field, exists := LookupField(name)
		normalized := expected
		if exists {
			normalized, err = normalizeValue(field, expected)
			if err != nil {
				return nil, PresetResolution{}, fmt.Errorf("acd config: preset %s field %q: %w",
					preset.Reference(), name, err)
			}
		}
		if value, ok := out[name]; ok && value.EffectiveValue() != normalized {
			preset.Customized = true
			break
		}
	}
	return out, preset, nil
}

func PresetCustomized(preset PresetDefinition, values Overrides) bool {
	for name, expected := range preset.Values {
		raw, ok := values[name]
		if !ok {
			continue
		}
		field, exists := LookupField(name)
		if !exists {
			return true
		}
		actual, err := validateRaw(field, raw, true)
		normalized, expectedErr := normalizeValue(field, expected)
		if err != nil || expectedErr != nil || actual != normalized {
			return true
		}
	}
	return false
}

// ResetPresetOverrides returns a copy with only preset-owned keys removed.
func ResetPresetOverrides(values Overrides, preset PresetDefinition) Overrides {
	out := make(Overrides, len(values))
	for name, value := range values {
		if _, owned := preset.Values[name]; !owned {
			out[name] = append(json.RawMessage(nil), value...)
		}
	}
	return out
}

func PresetOwnedFields(preset PresetDefinition) []string {
	out := make([]string, 0, len(preset.Values))
	for name := range preset.Values {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func ParsePresetReference(value string) (CommitStrategy, PresetName, int, error) {
	id, versionText, ok := strings.Cut(value, "@")
	if !ok {
		return "", "", 0, fmt.Errorf("acd config: invalid preset reference %q", value)
	}
	strategyText, nameText, ok := strings.Cut(id, ".")
	if !ok {
		return "", "", 0, fmt.Errorf("acd config: invalid preset reference %q", value)
	}
	var version int
	if _, err := fmt.Sscanf(versionText, "%d", &version); err != nil || version <= 0 {
		return "", "", 0, fmt.Errorf("acd config: invalid preset reference %q", value)
	}
	return CommitStrategy(strategyText), PresetName(nameText), version, nil
}
