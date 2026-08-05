package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// SettingsFingerprint binds the effective hot, persistable, non-secret
// settings to their selected immutable preset identity. Runtime consumers can
// use it to verify a stored global approval without importing settings.
func SettingsFingerprint(values map[string]string, preset PresetResolution) (string, error) {
	payload := make(map[string]any)
	for _, field := range Catalog() {
		if field.Boundary == ApplyHot && field.Persistable && !field.Sensitive {
			payload[field.Name] = values[field.Name]
		}
	}
	payload["preset_id"] = preset.ID()
	payload["preset_version"] = preset.Version()
	payload["customized"] = preset.Customized
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("acd config: fingerprint: %w", err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}
