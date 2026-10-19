package config

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Identifiers used only for legacy translation and removed-setting guidance.
const (
	FieldCommitPreset           = "commit.preset"
	FieldIntentWindow           = "intent.window"
	FieldIntentMinPending       = "intent.min_pending"
	FieldIntentSettleWindow     = "intent.settle_window"
	FieldIntentMaxPendingAge    = "intent.max_pending_age"
	FieldIntentRecentCommits    = "intent.recent_commits"
	FieldIntentDeferLimit       = "intent.defer_limit"
	FieldIntentRetryOnInvalid   = "intent.retry_on_invalid"
	FieldIntentPathCoalescing   = "intent.path_coalescing"
	FieldIntentRepairEnabled    = "intent.repair.enabled"
	FieldIntentRepairHorizon    = "intent.repair.horizon"
	FieldIntentRepairMaxCommits = "intent.repair.max_commits"
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
	"client.ttl": true, "recovery.rewind_grace": true, "recovery.shadow_generations": true,
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
		if value, ok := values[FieldCommitStrategy]; ok && json.Unmarshal(value, &strategy) == nil {
			old[FieldCommitStrategy] = cloneRaw(value)
			if strings.EqualFold(strings.TrimSpace(strategy), "event") {
				values[FieldCommitStrategy] = json.RawMessage(`"local"`)
			}
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
	if doc.Extra == nil {
		doc.Extra = map[string]json.RawMessage{}
	}
	archive := map[string]any{"version": 1, "scopes": archived}
	if previous, exists := doc.Extra["legacy_finish_settings"]; exists {
		archive["previous"] = cloneRaw(previous)
	}
	body, _ := json.Marshal(archive)
	doc.Extra["legacy_finish_settings"] = body
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

// LegacyFinishDefaults preserves only supported values from the former preset
// layer. Scopes are ordered from repository to profile to global, excluding
// scopes omitted when inspecting inherited settings. Values remain below all
// authored and environment overrides; they never grant provider consent.
func LegacyFinishDefaults(doc *Document, scopes []string, lookup func(string) (string, bool)) Overrides {
	if doc == nil {
		return nil
	}
	var archive struct {
		Version int                  `json:"version"`
		Retired bool                 `json:"retired"`
		Scopes  map[string]Overrides `json:"scopes"`
	}
	if json.Unmarshal(doc.Extra["legacy_finish_settings"], &archive) != nil || archive.Version != 1 || archive.Retired {
		return nil
	}
	selector := func(field, env, fallback string) string {
		for _, scope := range scopes {
			var value string
			if raw, ok := archive.Scopes[scope][field]; ok && json.Unmarshal(raw, &value) == nil {
				return strings.ToLower(strings.TrimSpace(value))
			}
		}
		if lookup != nil {
			if value, ok := lookup(env); ok {
				return strings.ToLower(strings.TrimSpace(value))
			}
		}
		return fallback
	}
	strategy := selector(FieldCommitStrategy, "ACD_COMMIT_STRATEGY", "event")
	presetDefault := "fast"
	if strategy == "intent" {
		presetDefault = "balanced"
	}
	preset := selector(FieldCommitPreset, "ACD_COMMIT_PRESET", presetDefault)
	if (strategy != "event" && strategy != "intent") || (preset != "fast" && preset != "balanced" && preset != "quality") {
		return nil
	}
	values := Overrides{FieldIntentVerification: json.RawMessage(`"none"`), FieldDiffEgress: json.RawMessage(`"true"`)}
	if strategy == "event" && preset == "fast" {
		values[FieldProvider] = json.RawMessage(`"deterministic"`)
		values[FieldDiffEgress] = json.RawMessage(`"false"`)
	}
	if strategy == "intent" {
		switch preset {
		case "balanced":
			values[FieldIntentVerification] = json.RawMessage(`"structural"`)
		case "quality":
			values[FieldIntentVerification] = json.RawMessage(`"full"`)
		}
	}
	return values
}
