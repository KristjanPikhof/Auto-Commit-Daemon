package settingsui

import (
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
)

// applyPresetSwitch replaces only values that came from the previous preset.
// Repository/profile/global/environment authored values remain untouched.
func applyPresetSwitch(draft map[string]string, fields []FieldValue, strategy, preset string) map[string]bool {
	definition, ok := config.LookupPreset(config.CommitStrategy(strategy), config.PresetName(preset))
	if !ok {
		return nil
	}
	changed := map[string]bool{}
	for _, field := range fields {
		if field.Source != string(config.SourcePreset) {
			continue
		}
		value, owned := definition.Values[field.Key]
		if !owned {
			if catalogField, exists := config.LookupField(field.Key); exists {
				value = catalogField.Default
			}
		}
		if draft[field.Key] != value {
			draft[field.Key] = value
			changed[field.Key] = true
		}
	}
	if draft[config.FieldCommitStrategy] != strategy {
		draft[config.FieldCommitStrategy] = strategy
	}
	changed[config.FieldCommitStrategy] = true
	if draft[config.FieldCommitPreset] != preset {
		draft[config.FieldCommitPreset] = preset
	}
	changed[config.FieldCommitPreset] = true
	return changed
}
