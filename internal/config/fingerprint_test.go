package config

import (
	"strings"
	"testing"
)

func TestSettingsFingerprintBindsHotValuesAndPresetIdentity(t *testing.T) {
	preset, ok := LookupPreset(StrategyIntent, PresetBalanced)
	if !ok {
		t.Fatal("intent balanced preset missing")
	}
	values := map[string]string{
		FieldModel:                   "model-a",
		FieldCommitStrategy:          "intent",
		FieldCommitPreset:            "balanced",
		FieldIntentVerification:      "structural",
		"capture.max_pending_events": "10",
	}
	first, err := SettingsFingerprint(values, PresetResolution{Definition: preset})
	if err != nil || len(first) != 64 {
		t.Fatalf("SettingsFingerprint = %q err=%v", first, err)
	}
	values["capture.max_pending_events"] = "20"
	restartOnly, err := SettingsFingerprint(values, PresetResolution{Definition: preset})
	if err != nil || restartOnly != first {
		t.Fatalf("restart value changed fingerprint: %q -> %q err=%v", first, restartOnly, err)
	}
	values[FieldModel] = "model-b"
	changed, err := SettingsFingerprint(values, PresetResolution{Definition: preset})
	if err != nil || changed == first {
		t.Fatalf("hot value did not change fingerprint: %q -> %q err=%v", first, changed, err)
	}
	customized, err := SettingsFingerprint(values, PresetResolution{
		Definition: preset,
		Customized: true,
	})
	if err != nil || customized == changed || strings.Trim(customized, "0123456789abcdef") != "" {
		t.Fatalf("preset identity fingerprint = %q err=%v", customized, err)
	}
}
