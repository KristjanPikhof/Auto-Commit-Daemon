package config

import "testing"

func TestFinishConfigFingerprintBindsSupportedHotValues(t *testing.T) {
	values := map[string]string{FieldModel: "model-a", FieldCommitStrategy: "intent", FieldIntentVerification: "structural", "capture.max_file_bytes": "10"}
	first, err := SettingsFingerprint(values)
	if err != nil || len(first) != 64 {
		t.Fatalf("fingerprint = %q %v", first, err)
	}
	values["capture.max_file_bytes"] = "20"
	values[FieldIntentWindow] = "10"
	second, err := SettingsFingerprint(values)
	if err != nil || second != first {
		t.Fatalf("retired/restart fields affected fingerprint: %q %q %v", first, second, err)
	}
	values[FieldModel] = "model-b"
	third, err := SettingsFingerprint(values)
	if err != nil || third == first {
		t.Fatalf("hot value did not affect fingerprint: %q %v", third, err)
	}
}
