package config

import "errors"

type CommitStrategy string

const (
	StrategyLocal  CommitStrategy = "local"
	StrategyIntent CommitStrategy = "intent"
	// Deprecated: legacy persisted strategy; finish translation maps it to local.
	StrategyEvent CommitStrategy = "event"
)

// Deprecated: compatibility types for callers being removed with the old
// publication engine. No preset participates in finish configuration.
type PresetName string

const (
	PresetFast           PresetName = "fast"
	PresetBalanced       PresetName = "balanced"
	PresetQuality        PresetName = "quality"
	PresetCatalogVersion            = 3
)

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

type PresetResolution struct {
	Definition PresetDefinition
	Customized bool
}

func (p PresetDefinition) ID() string        { return "" }
func (p PresetDefinition) Reference() string { return "" }
func (p PresetResolution) ID() string        { return "" }
func (p PresetResolution) Reference() string { return "" }
func (p PresetResolution) Version() int      { return 0 }

// ResolveAll resolves only supported finish settings. The second result and
// selectedScope argument are deprecated while existing callers migrate.
func ResolveAll(input ResolveInput, _ Overrides) (map[string]ResolvedField, PresetResolution, error) {
	out := make(map[string]ResolvedField, len(fieldCatalog))
	for _, field := range fieldCatalog {
		value, err := ResolveField(field.Name, input)
		if err != nil {
			return nil, PresetResolution{}, err
		}
		out[field.Name] = value
	}
	return out, PresetResolution{}, nil
}

// Deprecated: preset authoring is no longer available. Keep these small
// adapters only until CLI/runtime surface removal deletes their callers.
func PresetCatalog() []PresetDefinition { return nil }
func LookupPreset(CommitStrategy, PresetName) (PresetDefinition, bool) {
	return PresetDefinition{}, false
}
func ResolvePreset(ResolveInput, Overrides) (PresetResolution, error) {
	return PresetResolution{}, errors.New("acd config: presets were removed; choose commit.strategy=local or intent")
}
func PresetCustomized(PresetDefinition, Overrides) bool { return false }
func PresetOwnedFields(PresetDefinition) []string       { return nil }
func ResetPresetOverrides(values Overrides, _ PresetDefinition) Overrides {
	return cloneRawMap(values)
}
func ParsePresetReference(string) (CommitStrategy, PresetName, int, error) {
	return "", "", 0, errors.New("acd config: legacy preset reference; run acd setup to upgrade")
}
