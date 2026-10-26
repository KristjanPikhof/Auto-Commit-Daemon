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

func TestInputBudgetDefaultPreservesExistingApproval(t *testing.T) {
	values := map[string]string{FieldModel: "approved-model", FieldCommitStrategy: "intent"}
	before, _ := SettingsFingerprint(values)
	field, _ := LookupField(FieldMaxInputBytes)
	values[FieldMaxInputBytes] = field.Default
	after, _ := SettingsFingerprint(values)
	if before != after {
		t.Fatal("new default revoked existing approval")
	}
	values[FieldMaxInputBytes] = "16777216"
	custom, _ := SettingsFingerprint(values)
	if custom == before {
		t.Fatal("custom budget escaped approved configuration identity")
	}
}

func TestInputBudgetSettingsBounds(t *testing.T) {
	for _, value := range []string{"131072", "1048576", "16777216", "67108864", "0", "-1", "67108865"} {
		_, err := ResolveField(FieldMaxInputBytes, ResolveInput{LookupEnv: func(string) (string, bool) { return "", false }, Draft: Overrides{FieldMaxInputBytes: []byte(value)}})
		valid := value != "0" && value != "-1" && value != "67108865"
		if (err == nil) != valid {
			t.Fatalf("budget %s: %v", value, err)
		}
	}
}
