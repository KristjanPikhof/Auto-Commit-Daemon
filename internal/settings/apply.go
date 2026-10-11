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
	Applied      bool
	SnapshotHash string
}

// Apply approves one immutable hot snapshot. It does not run a provider,
// project check, wake the worker, or publish branch commits.
func (s *Service) Apply(ctx context.Context, req ApplyRequest) (ApplyResult, error) {
	if err := s.requireRepository("apply runtime revision"); err != nil {
		return ApplyResult{}, err
	}
	revision, err := s.prepareRevision(ctx, req)
	if err != nil {
		return ApplyResult{}, err
	}
	activation, ok, err := state.RequestConfigActivation(ctx, s.db, revision.ID, nullableID(req.ExpectedDesiredRevision))
	if err != nil {
		return ApplyResult{}, sanitizeError(err)
	}
	if !ok {
		return ApplyResult{}, errors.New("acd settings: desired revision changed; refresh before applying")
	}
	return s.finishApply(ctx, revision, activation)
}

func (s *Service) prepareRevision(ctx context.Context, req ApplyRequest) (state.ConfigRevision, error) {
	input, err := s.prepareRevisionInput(ctx, req)
	if err != nil {
		return state.ConfigRevision{}, err
	}
	revision, err := state.InsertConfigRevision(ctx, s.db, input)
	if err != nil {
		return state.ConfigRevision{}, sanitizeError(err)
	}
	return revision, nil
}

func (s *Service) prepareRevisionInput(
	ctx context.Context,
	req ApplyRequest,
) (state.ConfigRevisionInput, error) {
	validation, err := s.Validate(ctx, req.Values, req.Confirmations)
	if err != nil {
		return state.ConfigRevisionInput{}, err
	}
	if validation.SourceGeneration != req.ExpectedGeneration {
		return state.ConfigRevisionInput{}, errors.New("acd settings: stale saved generation; refresh before applying")
	}
	if len(validation.Missing) > 0 {
		return state.ConfigRevisionInput{}, &ConfirmationRequiredError{Missing: validation.Missing}
	}
	if len(validation.RestartChanged) > 0 {
		return state.ConfigRevisionInput{}, fmt.Errorf("acd settings: restart required for changed fields: %s",
			strings.Join(validation.RestartChanged, ", "))
	}
	if req.TestedFingerprint != "" && req.TestedFingerprint != validation.Fingerprint {
		return state.ConfigRevisionInput{}, errors.New("acd settings: reviewed settings are stale; review the current draft again")
	}
	if req.ExpectedGeneration > math.MaxInt64 {
		return state.ConfigRevisionInput{}, errors.New("acd settings: saved generation is out of range")
	}
	body, err := revisionSnapshotJSON(validation.ResolvedHot, req.Confirmations)
	if err != nil {
		return state.ConfigRevisionInput{}, err
	}
	doc, err := s.store.Load()
	if err != nil {
		return state.ConfigRevisionInput{}, sanitizeError(err)
	}
	if doc.Generation != req.ExpectedGeneration {
		return state.ConfigRevisionInput{}, errors.New("acd settings: stale saved generation; refresh before applying")
	}
	profile := doc.Settings.Repositories[s.repoHash].Profile
	if profile == "" {
		profile = "default"
	}
	return state.ConfigRevisionInput{
		Snapshot: body, Profile: profile, Scope: string(ScopeRepository),
		SourceGeneration: int64(req.ExpectedGeneration), Reason: "settings apply",
	}, nil
}

func (s *Service) finishApply(ctx context.Context, revision state.ConfigRevision, activation state.ConfigActivationRequest) (ApplyResult, error) {
	result := ApplyResult{RevisionID: revision.ID, RequestID: activation.ID, SnapshotHash: revision.SnapshotHash}
	ok, err := state.AcknowledgeConfigActivation(ctx, s.db, activation.ID, revision.ID)
	if err != nil {
		return result, sanitizeError(err)
	}
	if !ok {
		return result, errors.New("acd settings: desired revision changed; refresh before applying")
	}
	ok, err = state.ApplyConfigActivation(ctx, s.db, activation.ID, revision.ID)
	if err != nil {
		return result, sanitizeError(err)
	}
	if !ok {
		return result, errors.New("acd settings: desired revision changed; refresh before applying")
	}
	result.Applied = true
	return result, nil
}

type RevertRequest struct {
	ExpectedGeneration      uint64
	ExpectedDesiredRevision int64
}

func (s *Service) Revert(ctx context.Context, req RevertRequest) (ApplyResult, error) {
	if err := s.requireRepository("revert runtime revision"); err != nil {
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
	return s.finishApply(ctx, revision, activation)
}

func revisionSnapshotJSON(values map[string]string, confirmations []ai.ConfirmationRequirement) ([]byte, error) {
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
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("acd settings: encode runtime snapshot: %w", err)
	}
	return body, nil
}
