package settings

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

type ExperimentSnapshot struct {
	ID                  int64
	BaselineRevisionID  int64
	CandidateRevisionID int64
	WindowBudget        int
	CompletedWindows    int
	ExpiresAt           time.Time
	FailurePolicy       string
	Status              string
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
	Experiment              *ExperimentSnapshot
	Profiles                []string
	PresetID                string
	PresetVersion           int
	PresetCustomized        bool
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
	currentInput := config.ResolveInput{Global: doc.Settings.Global, LookupEnv: s.lookupEnv}
	inheritedInput := currentInput
	var selectedScope config.Overrides
	switch scope {
	case ScopeRepository:
		activeProfile = repo.Profile
		currentInput.Repository = repo.Fields
		currentInput.Profile = doc.Settings.Profiles[activeProfile].Fields
		selectedScope = currentInput.Repository
		inheritedInput = currentInput
		inheritedInput.Repository = nil
	case ScopeProfile:
		activeProfile = profileName
		currentInput.Profile = doc.Settings.Profiles[activeProfile].Fields
		selectedScope = currentInput.Profile
		inheritedInput = currentInput
		inheritedInput.Profile = nil
	case ScopeGlobal:
		selectedScope = currentInput.Global
		inheritedInput.Global = nil
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
	resolvedFields, preset, err := config.ResolveAll(currentInput, selectedScope)
	if err != nil {
		return Snapshot{}, sanitizeError(err)
	}
	inheritedFields, _, err := config.ResolveAll(inheritedInput, nil)
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
	experiment, err := latestExperimentSnapshot(ctx, s.db)
	if err != nil {
		return Snapshot{}, err
	}
	profiles := make([]string, 0, len(doc.Settings.Profiles))
	for name := range doc.Settings.Profiles {
		profiles = append(profiles, cleanText(name))
	}
	sort.Strings(profiles)
	out := Snapshot{Scope: scope, Profile: activeProfile, RepoHash: s.repoHash, Profiles: profiles,
		PresetID: preset.ID(), PresetVersion: preset.Version(), PresetCustomized: preset.Customized,
		SavedGeneration: doc.Generation, Fields: fields,
		DesiredRevisionID:       nullableValue(runtimeState.DesiredRevisionID),
		AppliedRevisionID:       nullableValue(runtimeState.AppliedRevisionID),
		LastKnownGoodRevisionID: nullableValue(runtimeState.LastKnownGoodRevisionID),
		DesiredRequestID:        nullableValue(runtimeState.DesiredRequestID),
		PendingError:            cleanText(runtimeState.LastError.String), DaemonMode: cleanText(ds.Mode),
		DaemonRunning: running, Experiment: experiment}
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

func latestExperimentSnapshot(ctx context.Context, d *state.DB) (*ExperimentSnapshot, error) {
	var out ExperimentSnapshot
	var expires sql.NullFloat64
	err := d.ReadSQL().QueryRowContext(ctx, `
SELECT id, baseline_revision_id, candidate_revision_id, window_budget,
       completed_windows, expires_ts, failure_policy, status
FROM config_experiments ORDER BY id DESC LIMIT 1`).Scan(
		&out.ID, &out.BaselineRevisionID, &out.CandidateRevisionID,
		&out.WindowBudget, &out.CompletedWindows, &expires,
		&out.FailurePolicy, &out.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("acd settings: read experiment state: %w", sanitizeError(err))
	}
	if expires.Valid {
		out.ExpiresAt = time.Unix(0, int64(expires.Float64*float64(time.Second)))
	}
	out.FailurePolicy = cleanText(out.FailurePolicy)
	out.Status = cleanText(out.Status)
	return &out, nil
}

func nullableValue(value sql.NullInt64) int64 {
	if !value.Valid {
		return 0
	}
	return value.Int64
}
