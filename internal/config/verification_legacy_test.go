package config

import (
	"encoding/json"
	"testing"
)

func TestProjectCheckResolvesSelectedLegacySlotAcrossScopes(t *testing.T) {
	input := ResolveInput{
		Repository: Overrides{
			FieldIntentVerification:     json.RawMessage(`"full"`),
			"verification.full.command": json.RawMessage(`"repo check"`),
		},
		Profile: Overrides{
			"verification.full.timeout": json.RawMessage(`"3m"`),
		},
		Global: Overrides{
			FieldVerificationCommand:    json.RawMessage(`"global check"`),
			"verification.fast.command": json.RawMessage(`"old quick check"`),
		},
		LookupEnv: func(name string) (string, bool) {
			if name == "ACD_VERIFICATION_TIMEOUT" {
				return "15s", true
			}
			return "", false
		},
	}
	assertField := func(name, want string, source Source) {
		t.Helper()
		got, err := ResolveField(name, input)
		if err != nil || got.Value != want || got.Source != source {
			t.Fatalf("%s = %q from %s, want %q from %s: %v", name, got.Value, got.Source, want, source, err)
		}
	}
	assertField(FieldIntentVerification, VerificationProject, SourceRepository)
	assertField(FieldVerificationCommand, "repo check", SourceRepository)
	assertField(FieldVerificationTimeout, "3m0s", SourceProfile)

	ResetOverride(input.Repository, FieldVerificationCommand)
	assertField(FieldVerificationCommand, "global check", SourceGlobal)
	ResetOverride(input.Profile, FieldVerificationTimeout)
	assertField(FieldVerificationTimeout, "15s", SourceEnvironment)

	input.Repository[FieldIntentVerification] = json.RawMessage(`"none"`)
	assertField(FieldIntentVerification, VerificationNone, SourceRepository)
	ResetOverride(input.Global, FieldVerificationCommand)
	assertField(FieldVerificationCommand, "", SourceDefault)
}
