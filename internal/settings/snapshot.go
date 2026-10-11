package settings

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type FieldSnapshot struct {
	Name                string
	DraftValue          string
	ActiveValue         string
	Source              config.Source
	ShadowedEnvironment string
	EnvironmentSet      bool
	Boundary            config.ApplyBoundary
	Sensitive           bool
	Persistable         bool
	InheritedValue      string
}

type Snapshot struct {
	Scope                   Scope
	Profile                 string
	RepoHash                string
	SavedGeneration         uint64
	Fields                  []FieldSnapshot
	DesiredRevisionID       int64
	AppliedRevisionID       int64
	LastKnownGoodRevisionID int64
	DesiredRequestID        int64
	PendingSince            time.Time
	PendingAge              time.Duration
	PendingError            string
	PendingStatus           string
	DaemonMode              string
	DaemonRunning           bool
	Profiles                []string
}

// Snapshot projects authoring and runtime state without writing config or DB.
func (s *Service) Snapshot(ctx context.Context, scope Scope, profileName string) (Snapshot, error) {
	if err := s.requireRepository("snapshot runtime state"); err != nil {
		return Snapshot{}, err
	}
	if err := validateScope(scope, profileName); err != nil {
		return Snapshot{}, err
	}
	doc, err := s.store.Load()
	if err != nil {
		return Snapshot{}, sanitizeError(err)
	}
	repo := doc.Settings.Repositories[s.repoHash]
	activeProfile := ""
	currentInput := config.ResolveInput{Global: doc.Settings.Global, LookupEnv: s.lookupEnv, LegacyDefaults: config.LegacyFinishDefaults(doc, []string{"global"}, s.lookupEnv)}
	inheritedInput := currentInput
	switch scope {
	case ScopeRepository:
		activeProfile = repo.Profile
		currentInput.Repository = repo.Fields
		currentInput.Profile = doc.Settings.Profiles[activeProfile].Fields
		currentInput.LegacyDefaults = config.LegacyFinishDefaults(doc, []string{"repository:" + s.repoHash, "profile:" + activeProfile, "global"}, s.lookupEnv)
		inheritedInput = currentInput
		inheritedInput.Repository = nil
		inheritedInput.LegacyDefaults = config.LegacyFinishDefaults(doc, []string{"profile:" + activeProfile, "global"}, s.lookupEnv)
	case ScopeProfile:
		activeProfile = profileName
		currentInput.Profile = doc.Settings.Profiles[activeProfile].Fields
		currentInput.LegacyDefaults = config.LegacyFinishDefaults(doc, []string{"profile:" + activeProfile, "global"}, s.lookupEnv)
		inheritedInput = currentInput
		inheritedInput.Profile = nil
		inheritedInput.LegacyDefaults = config.LegacyFinishDefaults(doc, []string{"global"}, s.lookupEnv)
	case ScopeGlobal:
		inheritedInput.Global = nil
		inheritedInput.LegacyDefaults = config.LegacyFinishDefaults(doc, nil, s.lookupEnv)
	}
	runtimeState, err := state.RuntimeConfigActivationState(ctx, s.db)
	if err != nil {
		return Snapshot{}, sanitizeError(err)
	}
	activeValues := map[string]string{}
	if runtimeState.AppliedRevisionID.Valid {
		rev, err := state.ConfigRevisionByID(ctx, s.db, runtimeState.AppliedRevisionID.Int64)
		if err != nil {
			return Snapshot{}, sanitizeError(err)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal([]byte(rev.SnapshotJSON), &raw); err != nil {
			return Snapshot{}, errors.New("acd settings: applied revision is unreadable")
		}
		for name, value := range raw {
			if name == "confirmations" || name == "preset_id" ||
				name == "preset_version" || name == "customized" {
				continue
			}
			var text string
			if json.Unmarshal(value, &text) == nil {
				activeValues[name] = cleanText(text)
			}
		}
	}
	resolvedFields, err := config.ResolveAll(currentInput)
	if err != nil {
		return Snapshot{}, sanitizeError(err)
	}
	inheritedFields, err := config.ResolveAll(inheritedInput)
	if err != nil {
		return Snapshot{}, sanitizeError(err)
	}
	fields := make([]FieldSnapshot, 0, len(config.Catalog()))
	for _, field := range config.Catalog() {
		resolved := resolvedFields[field.Name]
		inherited := inheritedFields[field.Name]
		item := FieldSnapshot{Name: field.Name, DraftValue: cleanText(resolved.Value),
			ActiveValue: cleanText(activeValues[field.Name]), Source: resolved.Source,
			Boundary: field.Boundary, Sensitive: field.Sensitive, Persistable: field.Persistable,
			InheritedValue: inherited.EffectiveValue()}
		if resolved.ShadowedEnvironment != nil {
			item.EnvironmentSet = resolved.ShadowedEnvironment.Set
			item.ShadowedEnvironment = cleanText(resolved.ShadowedEnvironment.Value)
		}
		if field.Sensitive {
			item.ActiveValue = ""
			item.InheritedValue = ""
			if item.DraftValue != "set" {
				item.DraftValue = "unset"
			}
			if item.EnvironmentSet {
				item.ShadowedEnvironment = "set"
			}
		}
		fields = append(fields, item)
	}
	ds, _, err := state.LoadDaemonState(ctx, s.db)
	if err != nil {
		return Snapshot{}, sanitizeError(err)
	}
	running := ds.PID > 0 && ds.Mode != "stopped" && identity.Alive(ds.PID)
	profiles := make([]string, 0, len(doc.Settings.Profiles))
	for name := range doc.Settings.Profiles {
		profiles = append(profiles, cleanText(name))
	}
	sort.Strings(profiles)
	out := Snapshot{Scope: scope, Profile: activeProfile, RepoHash: s.repoHash, Profiles: profiles,
		SavedGeneration: doc.Generation, Fields: fields,
		DesiredRevisionID:       nullableValue(runtimeState.DesiredRevisionID),
		AppliedRevisionID:       nullableValue(runtimeState.AppliedRevisionID),
		LastKnownGoodRevisionID: nullableValue(runtimeState.LastKnownGoodRevisionID),
		DesiredRequestID:        nullableValue(runtimeState.DesiredRequestID),
		PendingError:            cleanText(runtimeState.LastError.String), DaemonMode: cleanText(ds.Mode),
		DaemonRunning: running}
	if runtimeState.DesiredRequestID.Valid {
		request, requestErr := state.ActivationRequestByID(ctx, s.db, runtimeState.DesiredRequestID.Int64)
		if requestErr == nil {
			out.PendingStatus = cleanText(request.Status)
		}
	}
	if runtimeState.DesiredTS.Valid && out.DesiredRevisionID != out.AppliedRevisionID {
		out.PendingSince = time.Unix(0, int64(runtimeState.DesiredTS.Float64*float64(time.Second)))
		out.PendingAge = s.now().Sub(out.PendingSince)
		if out.PendingAge < 0 {
			out.PendingAge = 0
		}
	}
	return out, nil
}

func nullableValue(value sql.NullInt64) int64 {
	if !value.Valid {
		return 0
	}
	return value.Int64
}
