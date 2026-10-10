package settings

import (
	"context"
	"errors"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
)

// Deprecated: these shapes keep old CLI callers compilable until surface
// removal. Experiments cannot write or activate any configuration.
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

var errExperimentsRemoved = errors.New("acd settings: experiments were removed; choose local or Intent mode and run acd finish explicitly")

func (s *Service) StartExperiment(context.Context, ExperimentRequest) (ExperimentResult, error) {
	return ExperimentResult{}, errExperimentsRemoved
}
func (s *Service) ExperimentProgress(context.Context, int64) (ExperimentSnapshot, error) {
	return ExperimentSnapshot{}, errExperimentsRemoved
}
func (s *Service) CancelExperiment(context.Context, int64) (ExperimentResult, error) {
	return ExperimentResult{}, errExperimentsRemoved
}
func (s *Service) RevertExperiment(context.Context, int64) (ExperimentResult, error) {
	return ExperimentResult{}, errExperimentsRemoved
}
func (s *Service) Compare(context.Context, ...int64) (Comparison, error) {
	return Comparison{}, errors.New("acd settings: planner-window comparisons were removed; inspect explicit finish outcomes")
}
