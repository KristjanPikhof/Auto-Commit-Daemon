package config

import "strings"

// Legacy verification fields remain readable in saved config and approved
// snapshots. Only the selected slot contributes to the current project check.
var legacyVerificationFields = map[string]FieldDefinition{
	FieldIntentVerification:     {Name: FieldIntentVerification, Environment: "ACD_INTENT_VERIFICATION", Default: VerificationNone, Kind: KindString, Choices: []string{VerificationNone, VerificationProject, "structural", "fast", "full"}, Persistable: true},
	"verification.fast.command": {Name: "verification.fast.command", Environment: "ACD_VERIFICATION_FAST_COMMAND", Kind: KindString, Persistable: true},
	"verification.fast.timeout": {Name: "verification.fast.timeout", Environment: "ACD_VERIFICATION_FAST_TIMEOUT", Default: "2m", Kind: KindDuration, Persistable: true},
	"verification.full.command": {Name: "verification.full.command", Environment: "ACD_VERIFICATION_FULL_COMMAND", Kind: KindString, Persistable: true},
	"verification.full.timeout": {Name: "verification.full.timeout", Environment: "ACD_VERIFICATION_FULL_TIMEOUT", Default: "10m", Kind: KindDuration, Persistable: true},
}

func LegacyVerificationMode(input ResolveInput) (string, error) {
	mode, err := resolveField(legacyVerificationFields[FieldIntentVerification], input)
	return mode.EffectiveValue(), err
}

// LegacyUncheckedFingerprint keeps provider consent and frozen request
// identities for settings that predate the single project check.
func LegacyUncheckedFingerprint(values map[string]string, input ResolveInput) (string, error) {
	payload := settingsFingerprintPayload(values)
	delete(payload, FieldVerificationCommand)
	delete(payload, FieldVerificationTimeout)
	for name, field := range legacyVerificationFields {
		resolved, err := resolveField(field, input)
		if err != nil {
			return "", err
		}
		payload[name] = resolved.EffectiveValue()
	}
	return fingerprintPayload(payload)
}

func resolveVerificationField(field FieldDefinition, input ResolveInput) (ResolvedField, error) {
	policy, err := resolveField(legacyVerificationFields[FieldIntentVerification], input)
	if err != nil {
		return ResolvedField{}, err
	}
	mode := policy.EffectiveValue()
	if field.Name == FieldIntentVerification {
		policy.Definition = field
		policy.Value = currentVerificationMode(mode)
		policy.effectiveValue = policy.Value
		if policy.ShadowedEnvironment != nil {
			policy.ShadowedEnvironment.Value = currentVerificationMode(policy.ShadowedEnvironment.Value)
		}
		return policy, nil
	}
	current, err := resolveField(field, input)
	if err != nil || (mode != "fast" && mode != "full") {
		return current, err
	}
	suffix := strings.TrimPrefix(field.Name, "verification.")
	legacy, err := resolveField(legacyVerificationFields["verification."+mode+"."+suffix], input)
	if err != nil {
		return ResolvedField{}, err
	}
	for _, source := range []Source{SourceDraft, SourceRepository, SourceProfile, SourceGlobal, SourceEnvironment, SourceLegacyTranslation} {
		if current.Source == source {
			return current, nil
		}
		if legacy.Source == source {
			break
		}
	}
	legacy.Definition = field
	return legacy, nil
}

func currentVerificationMode(mode string) string {
	switch mode {
	case "fast", "full":
		return VerificationProject
	case "structural":
		return VerificationNone
	default:
		return mode
	}
}

// ResetOverride also clears old aliases in the same scope so an inherited
// project setting cannot immediately fall back to the value just removed.
func ResetOverride(values Overrides, name string) {
	delete(values, name)
	switch name {
	case FieldVerificationCommand:
		delete(values, "verification.fast.command")
		delete(values, "verification.full.command")
	case FieldVerificationTimeout:
		delete(values, "verification.fast.timeout")
		delete(values, "verification.full.timeout")
	}
}
