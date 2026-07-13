package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// SettingsSchemaVersion is the on-disk operator configuration version.
const SettingsSchemaVersion = 1

// Overrides contains only explicit persisted values. Removing a key clears
// the override and restores inheritance; JSON null is rejected.
type Overrides map[string]json.RawMessage

// Document is the lossless top-level operator configuration document.
// Unknown fields are retained in Extra when a known section is updated.
type Document struct {
	Version       int
	Generation    uint64
	RepoLifecycle json.RawMessage
	Settings      SettingsDocument
	Extra         map[string]json.RawMessage
}

// SettingsDocument contains global defaults, named profiles, and per-repo
// profile selections/overrides.
type SettingsDocument struct {
	Global       Overrides
	Profiles     map[string]Profile
	Repositories map[string]RepositorySettings
	Extra        map[string]json.RawMessage
}

// Profile is a named set of explicit field overrides.
type Profile struct {
	Fields Overrides
	Extra  map[string]json.RawMessage
}

// RepositorySettings selects an optional profile and adds repo-local
// overrides. Repository keys are canonical privacy-safe repo hashes.
type RepositorySettings struct {
	Profile string
	Fields  Overrides
	Extra   map[string]json.RawMessage
}

// NewDocument returns an empty document at the current schema version.
func NewDocument() *Document {
	return &Document{
		Version: SettingsSchemaVersion,
		Settings: SettingsDocument{
			Global:       Overrides{},
			Profiles:     map[string]Profile{},
			Repositories: map[string]RepositorySettings{},
			Extra:        map[string]json.RawMessage{},
		},
		Extra: map[string]json.RawMessage{},
	}
}

// ParseDocument decodes one JSON value while retaining unknown fields.
// Documents written before settings existed (no version) are treated as v1.
func ParseDocument(body []byte) (*Document, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, errors.New("acd config: empty document")
	}
	var raw map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("acd config: parse: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("acd config: parse: multiple JSON values")
	}
	doc := NewDocument()
	if value, ok := takeRaw(raw, "version"); ok {
		if err := json.Unmarshal(value, &doc.Version); err != nil {
			return nil, fmt.Errorf("acd config: version: %w", err)
		}
		if doc.Version < 1 {
			return nil, fmt.Errorf("acd config: unsupported version %d", doc.Version)
		}
		if doc.Version > SettingsSchemaVersion {
			return nil, fmt.Errorf("acd config: version %d is newer than supported version %d", doc.Version, SettingsSchemaVersion)
		}
	}
	if value, ok := takeRaw(raw, "generation"); ok {
		if err := json.Unmarshal(value, &doc.Generation); err != nil {
			return nil, fmt.Errorf("acd config: generation: %w", err)
		}
	}
	if value, ok := takeRaw(raw, "repo_lifecycle"); ok {
		doc.RepoLifecycle = cloneRaw(value)
	}
	if value, ok := takeRaw(raw, "settings"); ok {
		settings, err := parseSettings(value)
		if err != nil {
			return nil, err
		}
		doc.Settings = settings
	}
	doc.Extra = cloneRawMap(raw)
	return doc, nil
}

func parseSettings(body json.RawMessage) (SettingsDocument, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return SettingsDocument{}, fmt.Errorf("acd config: settings: %w", err)
	}
	out := NewDocument().Settings
	if value, ok := takeRaw(raw, "global"); ok {
		if err := json.Unmarshal(value, &out.Global); err != nil {
			return SettingsDocument{}, fmt.Errorf("acd config: settings.global: %w", err)
		}
	}
	if value, ok := takeRaw(raw, "profiles"); ok {
		var profiles map[string]json.RawMessage
		if err := json.Unmarshal(value, &profiles); err != nil {
			return SettingsDocument{}, fmt.Errorf("acd config: settings.profiles: %w", err)
		}
		for name, value := range profiles {
			profile, err := parseProfile(value)
			if err != nil {
				return SettingsDocument{}, fmt.Errorf("acd config: profile %q: %w", name, err)
			}
			out.Profiles[name] = profile
		}
	}
	if value, ok := takeRaw(raw, "repositories"); ok {
		var repos map[string]json.RawMessage
		if err := json.Unmarshal(value, &repos); err != nil {
			return SettingsDocument{}, fmt.Errorf("acd config: settings.repositories: %w", err)
		}
		for hash, value := range repos {
			repo, err := parseRepository(value)
			if err != nil {
				return SettingsDocument{}, fmt.Errorf("acd config: repository %q: %w", hash, err)
			}
			out.Repositories[hash] = repo
		}
	}
	out.Extra = cloneRawMap(raw)
	return out, nil
}

func parseProfile(body json.RawMessage) (Profile, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return Profile{}, err
	}
	out := Profile{Fields: Overrides{}, Extra: map[string]json.RawMessage{}}
	if value, ok := takeRaw(raw, "fields"); ok {
		if err := json.Unmarshal(value, &out.Fields); err != nil {
			return Profile{}, fmt.Errorf("fields: %w", err)
		}
	}
	out.Extra = cloneRawMap(raw)
	return out, nil
}

func parseRepository(body json.RawMessage) (RepositorySettings, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return RepositorySettings{}, err
	}
	out := RepositorySettings{Fields: Overrides{}, Extra: map[string]json.RawMessage{}}
	if value, ok := takeRaw(raw, "profile"); ok {
		if err := json.Unmarshal(value, &out.Profile); err != nil {
			return RepositorySettings{}, fmt.Errorf("profile: %w", err)
		}
	}
	if value, ok := takeRaw(raw, "fields"); ok {
		if err := json.Unmarshal(value, &out.Fields); err != nil {
			return RepositorySettings{}, fmt.Errorf("fields: %w", err)
		}
	}
	out.Extra = cloneRawMap(raw)
	return out, nil
}

// MarshalJSON merges known sections back into their retained unknown fields.
func (d Document) MarshalJSON() ([]byte, error) {
	raw := cloneRawMap(d.Extra)
	version, err := json.Marshal(d.Version)
	if err != nil {
		return nil, err
	}
	raw["version"] = version
	generation, err := json.Marshal(d.Generation)
	if err != nil {
		return nil, err
	}
	raw["generation"] = generation
	if len(d.RepoLifecycle) > 0 {
		raw["repo_lifecycle"] = cloneRaw(d.RepoLifecycle)
	}
	settings, err := json.Marshal(d.Settings)
	if err != nil {
		return nil, err
	}
	raw["settings"] = settings
	return json.Marshal(raw)
}

func (s SettingsDocument) MarshalJSON() ([]byte, error) {
	raw := cloneRawMap(s.Extra)
	global, err := json.Marshal(nonNilOverrides(s.Global))
	if err != nil {
		return nil, err
	}
	profiles, err := json.Marshal(nonNilProfiles(s.Profiles))
	if err != nil {
		return nil, err
	}
	repositories, err := json.Marshal(nonNilRepositories(s.Repositories))
	if err != nil {
		return nil, err
	}
	raw["global"] = global
	raw["profiles"] = profiles
	raw["repositories"] = repositories
	return json.Marshal(raw)
}

func (p Profile) MarshalJSON() ([]byte, error) {
	raw := cloneRawMap(p.Extra)
	fields, err := json.Marshal(nonNilOverrides(p.Fields))
	if err != nil {
		return nil, err
	}
	raw["fields"] = fields
	return json.Marshal(raw)
}

func (r RepositorySettings) MarshalJSON() ([]byte, error) {
	raw := cloneRawMap(r.Extra)
	if r.Profile != "" {
		profile, err := json.Marshal(r.Profile)
		if err != nil {
			return nil, err
		}
		raw["profile"] = profile
	} else {
		delete(raw, "profile")
	}
	fields, err := json.Marshal(nonNilOverrides(r.Fields))
	if err != nil {
		return nil, err
	}
	raw["fields"] = fields
	return json.Marshal(raw)
}

func takeRaw(raw map[string]json.RawMessage, key string) (json.RawMessage, bool) {
	value, ok := raw[key]
	delete(raw, key)
	return value, ok
}

func cloneRaw(value json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), value...) }

func cloneRawMap(in map[string]json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(in))
	for key, value := range in {
		out[key] = cloneRaw(value)
	}
	return out
}

func nonNilOverrides(value Overrides) Overrides {
	if value == nil {
		return Overrides{}
	}
	return value
}
func nonNilProfiles(value map[string]Profile) map[string]Profile {
	if value == nil {
		return map[string]Profile{}
	}
	return value
}
func nonNilRepositories(value map[string]RepositorySettings) map[string]RepositorySettings {
	if value == nil {
		return map[string]RepositorySettings{}
	}
	return value
}
