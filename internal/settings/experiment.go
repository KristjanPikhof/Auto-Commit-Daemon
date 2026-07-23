package settings

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	MaxExperimentWindows     = 1000
	ExperimentPolicyContinue = "continue"
	ExperimentPolicyRevert   = "revert"
)

type ExperimentRequest struct {
	Values                  map[string]string
	TestedFingerprint       string
	Confirmations           []ai.ConfirmationRequirement
	ExpectedGeneration      uint64
	ExpectedDesiredRevision int64
	WindowBudget            int
	ExpiresAt               time.Time
	FailurePolicy           string
}

type ExperimentResult struct {
	Experiment ExperimentSnapshot
	Candidate  ApplyResult
	Revert     ApplyResult
	Comparison Comparison
}

// StartExperiment queues one immutable candidate revision against the
// currently applied baseline. The daemon is nudged only after the bounded
// experiment row exists, and no existing commit is changed or removed.
func (s *Service) StartExperiment(ctx context.Context, req ExperimentRequest) (ExperimentResult, error) {
	if req.WindowBudget <= 0 || req.WindowBudget > MaxExperimentWindows {
		return ExperimentResult{}, errors.New("acd settings: experiment window budget must be between 1 and 1000")
	}
	policy := req.FailurePolicy
	if policy == "" {
		policy = ExperimentPolicyContinue
	}
	if policy != ExperimentPolicyContinue && policy != ExperimentPolicyRevert {
		return ExperimentResult{}, errors.New("acd settings: experiment failure policy must be continue or revert")
	}
	if !req.ExpiresAt.IsZero() && !req.ExpiresAt.After(s.now()) {
		return ExperimentResult{}, errors.New("acd settings: experiment expiry must be in the future")
	}
	var active int
	if err := s.db.ReadSQL().QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM config_experiments WHERE status='active')`).Scan(&active); err != nil {
		return ExperimentResult{}, sanitizeError(err)
	} else if active != 0 {
		return ExperimentResult{}, errors.New("acd settings: an experiment is already active")
	}
	runtimeState, err := state.RuntimeConfigActivationState(ctx, s.db)
	if err != nil {
		return ExperimentResult{}, sanitizeError(err)
	}
	if !runtimeState.AppliedRevisionID.Valid {
		return ExperimentResult{}, errors.New("acd settings: apply a baseline revision before starting an experiment")
	}
	baselineID := runtimeState.AppliedRevisionID.Int64
	if !runtimeState.DesiredRevisionID.Valid || runtimeState.DesiredRevisionID.Int64 != baselineID ||
		req.ExpectedDesiredRevision != baselineID {
		return ExperimentResult{}, errors.New("acd settings: desired revision changed; refresh before starting experiment")
	}
	validation, err := s.Validate(ctx, req.Values, req.Confirmations)
	if err != nil {
		return ExperimentResult{}, err
	}
	if validation.ResolvedHot[config.FieldCommitStrategy] != string(ai.CommitStrategyIntent) {
		return ExperimentResult{}, errors.New("acd settings: experiments require commit.strategy=intent because budgets count planner windows")
	}
	applyReq := ApplyRequest{
		Values: req.Values, TestedFingerprint: req.TestedFingerprint,
		Confirmations: req.Confirmations, ExpectedGeneration: req.ExpectedGeneration,
		ExpectedDesiredRevision: req.ExpectedDesiredRevision,
	}
	revision, err := s.prepareRevision(ctx, applyReq)
	if err != nil {
		return ExperimentResult{}, err
	}
	expires := sql.NullFloat64{}
	if !req.ExpiresAt.IsZero() {
		expires = sql.NullFloat64{Float64: float64(req.ExpiresAt.UnixNano()) / float64(time.Second), Valid: true}
	}
	experiment, activation, ok, err := state.RequestConfigExperimentActivation(ctx, s.db, state.ConfigExperimentInput{
		BaselineRevisionID: baselineID, CandidateRevisionID: revision.ID,
		WindowBudget: req.WindowBudget, ExpiresTS: expires, FailurePolicy: policy,
	}, nullableID(req.ExpectedDesiredRevision))
	if err != nil {
		return ExperimentResult{}, sanitizeError(err)
	}
	if !ok {
		return ExperimentResult{}, errors.New("acd settings: desired revision changed; refresh before starting experiment")
	}
	candidate, err := s.finishApply(ctx, revision, activation, false)
	if err != nil {
		return ExperimentResult{}, err
	}
	result := ExperimentResult{Experiment: experimentSnapshot(experiment), Candidate: candidate}
	if candidate.DaemonMode != "stopped" {
		daemonState, _, loadErr := state.LoadDaemonState(ctx, s.db)
		if loadErr != nil {
			return result, sanitizeError(loadErr)
		}
		if daemonState.PID > 0 {
			if err := s.nudge(ctx, daemonState); err != nil {
				return result, sanitizeError(err)
			}
			result.Candidate.Signaled = true
		}
	}
	return result, nil
}

func (s *Service) ExperimentProgress(ctx context.Context, id int64) (ExperimentSnapshot, error) {
	if id <= 0 {
		return ExperimentSnapshot{}, errors.New("acd settings: invalid experiment id")
	}
	experiment, err := state.ConfigExperimentByID(ctx, s.db, id)
	if err != nil {
		return ExperimentSnapshot{}, sanitizeError(err)
	}
	return experimentSnapshot(experiment), nil
}

func (s *Service) CancelExperiment(ctx context.Context, id int64) (ExperimentResult, error) {
	return s.finishExperiment(ctx, id, "operator cancelled experiment")
}

func (s *Service) RevertExperiment(ctx context.Context, id int64) (ExperimentResult, error) {
	return s.finishExperiment(ctx, id, "operator requested baseline revert")
}

func (s *Service) finishExperiment(ctx context.Context, id int64, reason string) (ExperimentResult, error) {
	if id <= 0 {
		return ExperimentResult{}, errors.New("acd settings: invalid experiment id")
	}
	experiment, err := state.ConfigExperimentByID(ctx, s.db, id)
	if err != nil {
		return ExperimentResult{}, sanitizeError(err)
	}
	if experiment.Status == state.ExperimentActive {
		if _, err := state.FinishConfigExperiment(ctx, s.db, id, state.ExperimentCancelled, reason); err != nil {
			return ExperimentResult{}, sanitizeError(err)
		}
	}
	revision, request, queued, err := state.QueueExperimentBaselineRevert(ctx, s.db, id,
		float64(s.now().UnixNano())/float64(time.Second))
	if err != nil {
		return ExperimentResult{}, sanitizeError(err)
	}
	updated, err := state.ConfigExperimentByID(ctx, s.db, id)
	if err != nil {
		return ExperimentResult{}, sanitizeError(err)
	}
	comparison, err := s.Compare(ctx, updated.BaselineRevisionID, updated.CandidateRevisionID)
	if err != nil {
		return ExperimentResult{}, err
	}
	result := ExperimentResult{Experiment: experimentSnapshot(updated), Comparison: comparison}
	if queued {
		result.Revert = ApplyResult{RevisionID: revision.ID, RequestID: request.ID,
			Queued: true, SnapshotHash: revision.SnapshotHash}
		daemonState, _, loadErr := state.LoadDaemonState(ctx, s.db)
		if loadErr != nil {
			return result, sanitizeError(loadErr)
		}
		result.Revert.DaemonMode = cleanText(daemonState.Mode)
		if daemonState.PID > 0 && daemonState.Mode != "stopped" {
			if err := s.nudge(ctx, daemonState); err != nil {
				return result, sanitizeError(err)
			}
			result.Revert.Signaled = true
		}
	}
	return result, nil
}

// Compare returns descriptive operational evidence for immutable revisions.
func (s *Service) Compare(ctx context.Context, revisionIDs ...int64) (Comparison, error) {
	comparison, err := CompareRevisions(ctx, s.db.ReadSQL(), revisionIDs...)
	if err != nil {
		return Comparison{}, sanitizeError(err)
	}
	return comparison, nil
}

func experimentSnapshot(experiment state.ConfigExperiment) ExperimentSnapshot {
	out := ExperimentSnapshot{ID: experiment.ID,
		BaselineRevisionID:  experiment.BaselineRevisionID,
		CandidateRevisionID: experiment.CandidateRevisionID,
		WindowBudget:        experiment.WindowBudget, CompletedWindows: experiment.CompletedWindows,
		FailurePolicy: cleanText(experiment.FailurePolicy), Status: cleanText(experiment.Status)}
	if experiment.ExpiresTS.Valid {
		out.ExpiresAt = time.Unix(0, int64(experiment.ExpiresTS.Float64*float64(time.Second)))
	}
	return out
}
