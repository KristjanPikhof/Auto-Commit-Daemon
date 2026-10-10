package config

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Retired settings stay recognizable only for migration and actionable errors.
var retiredFinishFields = map[string]bool{
	FieldCommitPreset: true,
	FieldIntentWindow: true, FieldIntentMinPending: true,
	FieldIntentSettleWindow: true, FieldIntentMaxPendingAge: true,
	FieldIntentRecentCommits: true, FieldIntentDeferLimit: true,
	FieldIntentRetryOnInvalid: true, FieldIntentPathCoalescing: true,
	FieldIntentRepairEnabled: true, FieldIntentRepairHorizon: true,
	FieldIntentRepairMaxCommits:  true,
	"capture.max_pending_events": true, "retention.event_days": true,
	"recovery.rewind_grace": true, "recovery.shadow_generations": true,
}

// UnsupportedFieldError distinguishes removed publication controls from typos.
func UnsupportedFieldError(name string) error {
	if retiredFinishFields[name] {
		return fmt.Errorf("acd config: setting %q was removed; use acd finish for completed work and acd setup to upgrade legacy configuration", name)
	}
	return fmt.Errorf("acd config: unsupported field %q", name)
}

// TranslateLegacyDocument upgrades a decoded v1 document without writing it.
// Retired values and obsolete repair approval remain archived as provenance;
// current global/profile/repository scopes keep their explicit supported values.
func TranslateLegacyDocument(doc *Document) {
	if doc == nil || doc.Version != 1 {
		return
	}
	archived := map[string]Overrides{}
	translate := func(scope string, values Overrides) {
		old := Overrides{}
		for name, value := range values {
			if retiredFinishFields[name] {
				old[name] = cloneRaw(value)
				delete(values, name)
			}
		}
		var strategy string
		if value, ok := values[FieldCommitStrategy]; ok && json.Unmarshal(value, &strategy) == nil && strategy == "event" {
			old[FieldCommitStrategy] = cloneRaw(value)
			values[FieldCommitStrategy] = json.RawMessage(`"local"`)
		}
		if len(old) > 0 {
			archived[scope] = old
		}
	}
	translate("global", doc.Settings.Global)
	for name, profile := range doc.Settings.Profiles {
		translate("profile:"+name, profile.Fields)
	}
	for hash, repo := range doc.Settings.Repositories {
		translate("repository:"+hash, repo.Fields)
	}
	if approval := doc.Settings.GlobalSetupApproval; approval != nil {
		kept := make([]string, 0, len(approval.Confirmations))
		for _, confirmation := range approval.Confirmations {
			if confirmation != "intent_repair" {
				kept = append(kept, confirmation)
			}
		}
		if len(kept) != len(approval.Confirmations) {
			original, _ := json.Marshal(approval)
			archived["global_setup_approval"] = Overrides{"original": original}
			approval.Confirmations = kept
		}
	}
	if len(archived) > 0 {
		if doc.Extra == nil {
			doc.Extra = map[string]json.RawMessage{}
		}
		archive := map[string]any{"scopes": archived}
		if previous, exists := doc.Extra["legacy_finish_settings"]; exists {
			archive["previous"] = cloneRaw(previous)
		}
		body, _ := json.Marshal(archive)
		doc.Extra["legacy_finish_settings"] = body
	}
	doc.Version = SettingsSchemaVersion
}

// TranslateLegacyEnvironment is the only legacy environment-value boundary.
// Removed variables never enter Catalog; explicit Event becomes local batch.
func TranslateLegacyEnvironment(lookup func(string) (string, bool)) func(string) (string, bool) {
	return func(name string) (string, bool) {
		if lookup == nil {
			return "", false
		}
		value, set := lookup(name)
		if name == "ACD_COMMIT_STRATEGY" && strings.ToLower(strings.TrimSpace(value)) == "event" {
			return "local", set
		}
		return value, set
	}
}
