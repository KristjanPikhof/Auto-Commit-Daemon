package settings

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type ApplyRequest struct {
	Values                  map[string]string
	TestedFingerprint       string
	Confirmations           []ai.ConfirmationRequirement
	ExpectedGeneration      uint64
	ExpectedDesiredRevision int64
}

type ApplyResult struct {
	RevisionID   int64
	RequestID    int64
	Queued       bool
	Signaled     bool
	DaemonMode   string
	SnapshotHash string
}

// Apply records one immutable resolved hot snapshot and atomically requests
// it as desired. It never saves XDG config or enqueues a normal wake request.
func (s *Service) Apply(ctx context.Context, req ApplyRequest) (ApplyResult, error) {
	if err := s.rejectWhileExperimentActive(ctx); err != nil {
		return ApplyResult{}, err
	}
	return s.apply(ctx, req, true)
}

func (s *Service) apply(ctx context.Context, req ApplyRequest, signal bool) (ApplyResult, error) {
	revision, err := s.prepareRevision(ctx, req)
	if err != nil {
		return ApplyResult{}, err
	}
	activation, ok, err := state.RequestConfigActivation(ctx, s.db, revision.ID,
		nullableID(req.ExpectedDesiredRevision))
	if err != nil {
		return ApplyResult{}, sanitizeError(err)
	}
	if !ok {
		return ApplyResult{}, errors.New("acd settings: desired revision changed; refresh before applying")
	}
	return s.finishApply(ctx, revision, activation, signal)
}

func (s *Service) prepareRevision(ctx context.Context, req ApplyRequest) (state.ConfigRevision, error) {
	validation, err := s.Validate(ctx, req.Values, req.Confirmations)
	if err != nil {
		return state.ConfigRevision{}, err
	}
	if validation.SourceGeneration != req.ExpectedGeneration {
		return state.ConfigRevision{}, errors.New("acd settings: stale saved generation; refresh before applying")
	}
	if len(validation.Missing) > 0 {
		return state.ConfigRevision{}, &ConfirmationRequiredError{Missing: validation.Missing}
	}
	if len(validation.RestartChanged) > 0 {
		return state.ConfigRevision{}, fmt.Errorf("acd settings: restart required for changed fields: %s",
			strings.Join(validation.RestartChanged, ", "))
	}
	if req.TestedFingerprint == "" || req.TestedFingerprint != validation.Fingerprint {
		return state.ConfigRevision{}, errors.New("acd settings: tested settings are stale; test the current draft again")
	}
	if req.ExpectedGeneration > math.MaxInt64 {
		return state.ConfigRevision{}, errors.New("acd settings: saved generation is out of range")
	}
	body, err := revisionSnapshotJSON(validation.ResolvedHot, req.Confirmations, validation.Preset)
	if err != nil {
		return state.ConfigRevision{}, err
	}
	doc, err := s.store.Load()
	if err != nil {
		return state.ConfigRevision{}, sanitizeError(err)
	}
	if doc.Generation != req.ExpectedGeneration {
		return state.ConfigRevision{}, errors.New("acd settings: stale saved generation; refresh before applying")
	}
	profile := doc.Settings.Repositories[s.repoHash].Profile
	if profile == "" {
		profile = "default"
	}
	revision, err := state.InsertConfigRevision(ctx, s.db, state.ConfigRevisionInput{
		Snapshot: body, Profile: profile, Scope: string(ScopeRepository),
		SourceGeneration: int64(req.ExpectedGeneration), Reason: "settings apply",
	})
	if err != nil {
		return state.ConfigRevision{}, sanitizeError(err)
	}
	return revision, nil
}

func (s *Service) finishApply(ctx context.Context, revision state.ConfigRevision, activation state.ConfigActivationRequest, signal bool) (ApplyResult, error) {
	daemonState, _, err := state.LoadDaemonState(ctx, s.db)
	if err != nil {
		return ApplyResult{}, sanitizeError(err)
	}
	result := ApplyResult{RevisionID: revision.ID, RequestID: activation.ID,
		Queued: true, DaemonMode: cleanText(daemonState.Mode), SnapshotHash: revision.SnapshotHash}
	if signal && daemonState.PID > 0 && daemonState.Mode != "stopped" {
		if err := s.nudge(ctx, daemonState); err != nil {
			return result, sanitizeError(err)
		}
		result.Signaled = true
	}
	return result, nil
}

type RevertRequest struct {
	ExpectedGeneration      uint64
	ExpectedDesiredRevision int64
}

func (s *Service) Revert(ctx context.Context, req RevertRequest) (ApplyResult, error) {
	if err := s.rejectWhileExperimentActive(ctx); err != nil {
		return ApplyResult{}, err
	}
	doc, err := s.store.Load()
	if err != nil {
		return ApplyResult{}, sanitizeError(err)
	}
	if doc.Generation != req.ExpectedGeneration {
		return ApplyResult{}, errors.New("acd settings: stale saved generation; refresh before reverting")
	}
	if req.ExpectedGeneration > math.MaxInt64 {
		return ApplyResult{}, errors.New("acd settings: saved generation is out of range")
	}
	runtimeState, err := state.RuntimeConfigActivationState(ctx, s.db)
	if err != nil {
		return ApplyResult{}, sanitizeError(err)
	}
	if !runtimeState.LastKnownGoodRevisionID.Valid {
		return ApplyResult{}, errors.New("acd settings: no last-known-good revision is available")
	}
	revision, activation, ok, err := state.RevertConfigActivation(ctx, s.db,
		runtimeState.LastKnownGoodRevisionID.Int64,
		nullableID(req.ExpectedDesiredRevision), int64(req.ExpectedGeneration))
	if err != nil {
		return ApplyResult{}, sanitizeError(err)
	}
	if !ok {
		return ApplyResult{}, errors.New("acd settings: desired revision changed; refresh before reverting")
	}
	daemonState, _, err := state.LoadDaemonState(ctx, s.db)
	if err != nil {
		return ApplyResult{}, sanitizeError(err)
	}
	result := ApplyResult{RevisionID: revision.ID, RequestID: activation.ID,
		Queued: true, DaemonMode: cleanText(daemonState.Mode), SnapshotHash: revision.SnapshotHash}
	if daemonState.PID > 0 && daemonState.Mode != "stopped" {
		if err := s.nudge(ctx, daemonState); err != nil {
			return result, sanitizeError(err)
		}
		result.Signaled = true
	}
	return result, nil
}

func (s *Service) rejectWhileExperimentActive(ctx context.Context) error {
	if _, active, err := state.ActiveConfigExperiment(ctx, s.db); err != nil {
		return sanitizeError(err)
	} else if active {
		return errors.New("acd settings: cancel or finish the active experiment before applying another revision")
	}
	return nil
}

func revisionSnapshotJSON(values map[string]string, confirmations []ai.ConfirmationRequirement, presets ...config.PresetResolution) ([]byte, error) {
	payload := make(map[string]any, len(values))
	for key, value := range values {
		payload[key] = value
	}
	consents := make([]string, len(confirmations))
	for i := range confirmations {
		consents[i] = string(confirmations[i])
	}
	sort.Strings(consents)
	payload["confirmations"] = consents
	if len(presets) > 0 {
		payload["preset_id"] = presets[0].ID()
		payload["preset_version"] = presets[0].Version()
		payload["customized"] = presets[0].Customized
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("acd settings: encode runtime snapshot: %w", err)
	}
	return body, nil
}
