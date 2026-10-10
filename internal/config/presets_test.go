package config

import (
	"strings"
	"testing"
)

func TestFinishConfigCatalogAndDefaults(t *testing.T) {
	resolved, preset, err := ResolveAll(ResolveInput{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resolved[FieldCommitStrategy].EffectiveValue() != "local" || resolved[FieldCommitFormat].EffectiveValue() != "imperative" || preset.ID() != "" {
		t.Fatalf("defaults = %v, preset = %+v", resolved, preset)
	}
	for name := range retiredFinishFields {
		if _, exists := LookupField(name); exists {
			t.Fatalf("retired field %q remains public", name)
		}
		if _, err := ResolveField(name, ResolveInput{}); err == nil || !strings.Contains(err.Error(), "acd finish") {
			t.Fatalf("removed field error = %v", err)
		}
	}
	for _, mode := range []string{"local", "intent"} {
		for _, style := range []string{"imperative", "conventional"} {
			doc := NewDocument()
			doc.Settings.Global = Overrides{FieldCommitStrategy: rawString(mode), FieldCommitFormat: rawString(style)}
			if err := ValidateDocument(doc); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(PresetCatalog()) != 0 {
		t.Fatal("presets still exposed")
	}
	doc := NewDocument()
	doc.Settings.Global[FieldCommitStrategy] = rawString("event")
	if err := ValidateDocument(doc); err == nil || !strings.Contains(err.Error(), "choose local") {
		t.Fatalf("event authoring = %v", err)
	}
}

func TestFinishConfigResolutionScopes(t *testing.T) {
	input := ResolveInput{
		Draft:      Overrides{FieldModel: rawString("draft")},
		Repository: Overrides{FieldModel: rawString("repository")},
		Profile:    Overrides{FieldModel: rawString("profile")},
		Global:     Overrides{FieldModel: rawString("global")},
		Experiment: Overrides{FieldModel: rawString("obsolete experiment")},
		Preset:     Overrides{FieldModel: rawString("obsolete preset")},
		LookupEnv:  func(string) (string, bool) { return "environment", true },
	}
	for _, want := range []struct {
		value  string
		source Source
	}{{"draft", SourceDraft}, {"repository", SourceRepository}, {"profile", SourceProfile}, {"global", SourceGlobal}, {"environment", SourceEnvironment}} {
		got, err := ResolveField(FieldModel, input)
		if err != nil || got.EffectiveValue() != want.value || got.Source != want.source {
			t.Fatalf("resolution = %+v, %v; want %+v", got, err, want)
		}
		switch want.source {
		case SourceDraft:
			input.Draft = nil
		case SourceRepository:
			input.Repository = nil
		case SourceProfile:
			input.Profile = nil
		case SourceGlobal:
			input.Global = nil
		}
	}
	input.LookupEnv = nil
	got, err := ResolveField(FieldModel, input)
	if err != nil || got.Source != SourceDefault {
		t.Fatalf("retired layer affected resolution: %+v %v", got, err)
	}
}
