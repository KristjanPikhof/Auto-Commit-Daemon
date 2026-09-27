package daemon

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	publicationReasonHeadChanged           = "head_changed"
	publicationReasonRecoveredTarget       = "target_recovered"
	publicationReasonProviderConfiguration = "provider_configuration_required"
	publicationReasonSemanticUnavailable   = "semantic_message_unavailable"
	publicationReasonPreflight             = "preflight_invalid"
	publicationReasonSemanticExhausted     = "semantic_correction_exhausted"
	publicationReasonSupersededCandidate   = "candidate_superseded"
	publicationReasonSuccessorExhausted    = "candidate_successors_exhausted"
	publicationReasonProviderWait          = "provider_wait"
)

type publicationReasonError struct {
	code string
	err  error
}

func (e *publicationReasonError) Error() string { return e.err.Error() }
func (e *publicationReasonError) Unwrap() error { return e.err }
func publicationErrorReason(err error) string {
	var reason *publicationReasonError
	var preflight *IntentPlanPreflightError
	var exhausted *IntentSemanticFallbackRequiredError
	var terminal *state.IntentCandidateTerminalError
	switch {
	case errors.As(err, &reason):
		return reason.code
	case ai.ProviderNeedsConfiguration(err):
		return publicationReasonProviderConfiguration
	case isIntentPlannerCircuitWait(err):
		return publicationReasonProviderWait
	case errors.Is(err, gitpkg.ErrStagingApprovalMissing):
		return "staging_approval_missing"
	case errors.Is(err, gitpkg.ErrStagingChanged):
		return "staging_changed"
	case errors.As(err, &preflight):
		return publicationReasonPreflight
	case errors.As(err, &exhausted):
		return publicationReasonSemanticExhausted
	case errors.As(err, &terminal) && terminal.Status == state.IntentCandidateSuperseded:
		return publicationReasonSupersededCandidate
	default:
		return "publication_failed"
	}
}

// Old rows have no reason code. Decode their former display strings only at
// this compatibility boundary; current writers persist typed reason codes.
func publicationDrainReason(drain state.PublicationDrain) string {
	if drain.ReasonCode != "" {
		return drain.ReasonCode
	}
	switch {
	case strings.HasPrefix(drain.LastError, publicationDrainHeadChangedPrefix):
		return publicationReasonHeadChanged
	case drain.LastError == publicationDrainRecoveredTargetError:
		return publicationReasonRecoveredTarget
	case drain.LastError == PublicationDrainSemanticMessageUnavailableReason:
		return publicationReasonSemanticUnavailable
	case drain.LastError == publicationReasonProviderConfiguration:
		return publicationReasonProviderConfiguration
	case strings.HasPrefix(drain.LastError, "daemon: intent candidates: preflight blocked"):
		return publicationReasonPreflight
	}
	if failure, ok := recoverableTerminalCandidateDrain(drain.LastError); ok {
		if failure.exhaustedSuccessors {
			return publicationReasonSuccessorExhausted
		}
		return publicationReasonSupersededCandidate
	}
	return ""
}

func publicationDrainPreflightEvidence(drain state.PublicationDrain) string {
	if drain.ReasonEvidence != "" {
		return drain.ReasonEvidence
	}
	// Legacy rows stored only the rendered preflight failure.
	if drain.ReasonCode == "" {
		if detail, ok := strings.CutPrefix(drain.LastError, "daemon: intent candidates: preflight blocked: "); ok {
			return fmt.Sprintf("%x", sha256.Sum256([]byte(detail)))
		}
	}
	return ""
}
