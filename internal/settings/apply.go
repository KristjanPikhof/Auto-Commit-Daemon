package settings

import (
	"context"
	"crypto/sha256"
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
	SetupValidation         *SetupValidation
}

type SetupValidation struct {
	BranchRef        string
	BranchGeneration int64
	ExpectedHead     string
	Mode             string
	CommandSource    string
	CommandDigest    string
	ApprovalID       string
}

type ApplyResult struct {
	RevisionID       int64
	RequestID        int64
	ValidationRunID  int64
	ValidationStatus string
	Queued           bool
	Signaled         bool
	DaemonMode       string
	SnapshotHash     string
}

// Apply records one immutable resolved hot snapshot and atomically requests
// it as desired. It never saves XDG config or enqueues a normal wake request.
func (s *Service) Apply(ctx context.Context, req ApplyRequest) (ApplyResult, error) {
	if err := s.requireRepository("apply runtime revision"); err != nil {
		return ApplyResult{}, err
	}
	if err := s.rejectWhileExperimentActive(ctx); err != nil {
		return ApplyResult{}, err
	}
	return s.apply(ctx, req, true)
}

func (s *Service) apply(ctx context.Context, req ApplyRequest, signal bool) (ApplyResult, error) {
	if req.SetupValidation != nil {
		input, err := s.prepareRevisionInput(ctx, req)
		if err != nil {
			return ApplyResult{}, err
		}
		if err := validateSetupValidation(req, input); err != nil {
			return ApplyResult{}, err
		}
		spec := state.ConfigValidationSpec{
			BranchRef:        req.SetupValidation.BranchRef,
			BranchGeneration: req.SetupValidation.BranchGeneration,
			ExpectedHead:     req.SetupValidation.ExpectedHead,
			Mode:             req.SetupValidation.Mode,
			CommandSource:    req.SetupValidation.CommandSource,
			CommandDigest:    req.SetupValidation.CommandDigest,
			ApprovalID:       req.SetupValidation.ApprovalID,
		}
		revision, activation, validation, ok, err :=
			state.CreateConfigActivationWithValidation(
				ctx, s.db, input, nullableID(req.ExpectedDesiredRevision), spec,
			)
		if err != nil {
			return ApplyResult{}, sanitizeError(err)
		}
		if !ok {
			return ApplyResult{}, errors.New("acd settings: desired revision changed; refresh before applying")
		}
		result, err := s.finishApply(ctx, revision, activation, signal)
		if err != nil {
			return result, err
		}
		result.ValidationRunID = validation.ID
		result.ValidationStatus = validation.Status
		return result, nil
	}
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

func validateSetupValidation(
	req ApplyRequest,
	input state.ConfigRevisionInput,
) error {
	setup := req.SetupValidation
	if setup == nil {
		return nil
	}
	if setup.ApprovalID == "" || setup.ApprovalID != req.TestedFingerprint {
		return errors.New("acd settings: setup approval no longer matches the reviewed fingerprint")
	}
	var snapshot map[string]any
	if err := json.Unmarshal(input.Snapshot, &snapshot); err != nil {
		return errors.New("acd settings: invalid setup validation snapshot")
	}
	mode, _ := snapshot[config.FieldIntentVerification].(string)
	if mode != setup.Mode {
		return errors.New("acd settings: setup validation mode no longer matches the reviewed settings")
	}
	commandField := config.FieldVerificationFastCommand
	if setup.Mode == "structural" {
		commandField = ""
	} else if setup.Mode == "full" {
		commandField = config.FieldVerificationFullCommand
	}
	command := ""
	if commandField != "" {
		command, _ = snapshot[commandField].(string)
	}
	sum := sha256.Sum256([]byte(command))
	if (setup.Mode != "structural" && command == "") ||
		setup.CommandDigest != fmt.Sprintf("%x", sum[:]) {
		return errors.New("acd settings: setup validation command no longer matches the reviewed settings")
	}
	return nil
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
	if req.TestedFingerprint == "" || req.TestedFingerprint != validation.Fingerprint {
		return state.ConfigRevisionInput{}, errors.New("acd settings: tested settings are stale; test the current draft again")
	}
	if req.ExpectedGeneration > math.MaxInt64 {
		return state.ConfigRevisionInput{}, errors.New("acd settings: saved generation is out of range")
	}
	body, err := revisionSnapshotJSON(validation.ResolvedHot, req.Confirmations, validation.Preset)
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
	if err := s.requireRepository("revert runtime revision"); err != nil {
		return ApplyResult{}, err
	}
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
	return s.finishApply(ctx, revision, activation, true)
}

func (s *Service) rejectWhileExperimentActive(ctx context.Context) error {
	if err := s.requireRepository("inspect runtime experiments"); err != nil {
		return err
	}
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
