package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type productState string

const (
	productStateOff         productState = "off"
	productStateReady       productState = "ready"
	productStateProtected   productState = "protected"
	productStateWaiting     productState = "waiting"
	productStatePublishing  productState = "publishing"
	productStateNeedsAction productState = "needs_action"
)

type productAction struct {
	Kind   string `json:"kind"`
	Status string `json:"status"`
	Target string `json:"target,omitempty"`
	Detail string `json:"detail,omitempty"`
}

type productError struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Details   map[string]any `json:"details,omitempty"`
}

type productEnvelope struct {
	OK         bool            `json:"ok"`
	State      productState    `json:"state"`
	Changed    bool            `json:"changed"`
	Actions    []productAction `json:"actions"`
	NextAction *string         `json:"next_action"`
	Data       any             `json:"data"`
	Error      *productError   `json:"error"`
}

type productStatusData struct {
	CaptureHealth            state.CaptureHealth `json:"capture_health"`
	PublicationOutcome       publicationOutcome  `json:"publication_outcome"`
	Repo                     string              `json:"repo"`
	Command                  string              `json:"command"`
	Registered               bool                `json:"registered"`
	Enabled                  bool                `json:"enabled"`
	Worker                   string              `json:"worker"`
	Protected                bool                `json:"protected"`
	Published                bool                `json:"published"`
	Busy                     bool                `json:"busy"`
	OperationalState         string              `json:"operational_state"`
	WorktreeClean            bool                `json:"worktree_clean"`
	AllChangesCommittedInGit bool                `json:"all_changes_committed_in_git"`
	CheckpointPublishedByACD bool                `json:"checkpoint_published_by_acd"`
	ActionRequired           bool                `json:"action_required"`
	CheckpointID             string              `json:"checkpoint_id,omitempty"`

	CheckpointMaintenance    checkpoint.MaintenanceStatus `json:"checkpoint_maintenance"`
	PublicationProgress      publicationProgressReport    `json:"publication_progress"`
	PendingEvents            int                          `json:"pending_events"`
	BlockedEvents            int                          `json:"blocked_events"`
	Summary                  string                       `json:"summary"`
	StatePreserved           bool                         `json:"state_preserved"`
	CLIVersion               string                       `json:"cli_version,omitempty"`
	SupervisorVersion        string                       `json:"supervisor_version,omitempty"`
	SupervisorWorkerState    string                       `json:"supervisor_worker_state,omitempty"`
	SupervisorWorkerRestarts int                          `json:"supervisor_worker_restarts,omitempty"`
	SupervisorWorkerError    string                       `json:"supervisor_worker_error,omitempty"`
}

func envelopeFromControl(result controlResult) productEnvelope {
	stateName := productStateProtected
	switch result.Health {
	case controlHealthOff:
		stateName = productStateOff
	case controlHealthNeedsAttention, controlHealthNotRepo:
		stateName = productStateNeedsAction
	case controlHealthPublishing:
		stateName = productStatePublishing
	case controlHealthWaiting, controlHealthDegraded:
		stateName = productStateWaiting
	}
	actions := make([]productAction, 0, len(result.Actions))
	for _, action := range result.Actions {
		actions = append(actions, productAction{
			Kind: action, Status: "completed", Target: result.Repo,
		})
	}
	var next *string
	if result.NextAction != "" && result.NextAction != "No action needed." &&
		!strings.HasPrefix(result.NextAction, "No action needed;") &&
		!strings.HasPrefix(result.NextAction, "No immediate action needed;") {
		value := result.NextAction
		next = &value
	}
	actionRequired := stateName == productStateNeedsAction
	return productEnvelope{
		OK:         true,
		State:      stateName,
		Changed:    result.Changed,
		Actions:    actions,
		NextAction: next,
		Data: productStatusData{
			PublicationOutcome:       result.PublicationOutcome,
			Repo:                     result.Repo,
			Command:                  result.Command,
			Registered:               result.Registered,
			Enabled:                  result.Enabled,
			Worker:                   result.Daemon,
			Protected:                result.Protected,
			Published:                result.Published,
			Busy:                     result.Busy,
			OperationalState:         result.OperationalState,
			WorktreeClean:            result.WorktreeClean,
			AllChangesCommittedInGit: result.AllChangesCommittedInGit,
			CheckpointPublishedByACD: result.CheckpointPublishedByACD,
			ActionRequired:           actionRequired,
			CheckpointID:             result.CheckpointID,
			CaptureHealth:            result.CaptureHealth,
			PublicationProgress:      result.PublicationProgress,
			CheckpointMaintenance:    result.CheckpointMaintenance,
			PendingEvents:            result.PendingEvents,
			BlockedEvents:            result.BlockedEvents,
			Summary:                  result.Summary,
			StatePreserved:           result.StatePreserved,
			CLIVersion:               result.CLIVersion,
			SupervisorVersion:        result.SupervisorVersion,
			SupervisorWorkerState:    result.SupervisorWorkerState,
			SupervisorWorkerRestarts: result.SupervisorWorkerRestarts,
			SupervisorWorkerError:    result.SupervisorWorkerError,
		},
	}
}

func renderProductEnvelope(out io.Writer, envelope productEnvelope, jsonOut bool) error {
	if envelope.Actions == nil {
		envelope.Actions = []productAction{}
	}
	if jsonOut {
		return renderJSONEnvelope(out, envelope)
	}
	data, ok := envelope.Data.(productStatusData)
	if !ok {
		return fmt.Errorf("acd: unsupported human result %T", envelope.Data)
	}
	fmt.Fprintf(out, "Protection: %s\n", onOff(data.Enabled))
	fmt.Fprintf(out, "Last observed work saved: %s\n", yesNo(data.Protected))
	fmt.Fprintf(out, "Branch commits: %s\n", publicationOutcomeLabel(data.PublicationOutcome, data.PublicationProgress))
	if data.PublicationOutcome.CheckpointAvailable {
		renderFinishUncommitted(out, data.PublicationOutcome)
		if f := data.PublicationOutcome.Finish; f != nil {
			fmt.Fprintf(out, "Finish %s: %s\n", f.RequestID, finishStatusSummary(f))
			renderFinishStatusChecks(out, f)
		}
		if data.PublicationOutcome.LaterChanges > 0 {
			fmt.Fprintf(out, "Later work: %d changed path(s), separate from the finish target\n", data.PublicationOutcome.LaterChanges)
		}
		worker := "not responsive"
		if data.PublicationProgress.WorkerResponsive {
			worker = "responsive"
		}
		fmt.Fprintf(out, "Worker: %s; heartbeat %ds ago\n", worker, data.PublicationProgress.HeartbeatAgeSeconds)
	}
	if h := data.PublicationOutcome.History; h != nil {
		fmt.Fprintf(out, "History plan %s: %s\n", h.PlanID, h.State)
	}
	if legacy := data.PublicationOutcome.Legacy; legacy.State == "needs_attention" {
		renderLegacyAttention(out, legacy)
	}
	if data.PublicationOutcome.Legacy.Unpublished > 0 {
		fmt.Fprintf(out, "Legacy archive: %d captured version(s) retained separately\n", data.PublicationOutcome.Legacy.Unpublished)
	}
	if data.PublicationOutcome.RecoveredChanges > 0 {
		fmt.Fprintf(out, "Recovery: %d changes saved separately\n", data.PublicationOutcome.RecoveredChanges)
	}
	if data.ActionRequired && data.Summary != "" {
		fmt.Fprintf(out, "Status: %s\n", data.Summary)

	}
	if envelope.NextAction == nil {
		fmt.Fprintln(out, "Next: No action needed.")
	} else {
		fmt.Fprintf(out, "Next: %s\n", *envelope.NextAction)
	}
	return nil
}

func publicationOutcomeLabel(outcome publicationOutcome, progress publicationProgressReport) string {
	if outcome.CheckpointAvailable {
		if outcome.BranchCommitted == nil {
			return "unknown"
		}
		if *outcome.BranchCommitted {
			return "saved work is on the current branch"
		}
		return fmt.Sprintf("%d saved path(s) await finish", outcome.PendingChanges)
	}
	if outcome.BranchCommitted == nil {
		return "unknown"
	}
	if outcome.PendingClassification && outcome.WaitingChanges == 0 {
		return "saved changes waiting for grouping"
	}
	if outcome.WaitingChanges > 0 {
		if outcome.ReasonCode == "provider_wait" {
			label := fmt.Sprintf("%d changes waiting for AI", outcome.WaitingChanges)
			if outcome.RetryAt > 0 {
				label += "; retry at " + time.Unix(int64(outcome.RetryAt), 0).Format(time.RFC3339)
			}
			return label
		}
		return fmt.Sprintf("%d changes waiting (%s)", outcome.WaitingChanges, publicationProgressPhaseLabel(progress))
	}
	if *outcome.BranchCommitted {
		return "up to date"
	}
	if outcome.RecoveredChanges > 0 {
		return "some changes saved in recovery"
	}
	return "waiting for current changes to be saved"
}

func renderProductPublicationProgress(out io.Writer, progress publicationProgressReport) {
	if progress.Strategy != "" {
		fmt.Fprintf(out, "Finish mode: %s\n", progress.Strategy)
	}
	fmt.Fprintf(out, "Observed activity: %s\n", publicationProgressPhaseLabel(progress))
	if progress.LastProgressTS > 0 {
		fmt.Fprintf(out, "Last protection progress: %s ago\n", formatDurationCompact(time.Duration(progress.LastProgressAgeSeconds)*time.Second))
	}
	worker := "not responsive"
	if progress.WorkerResponsive {
		worker = "responsive"
	}
	fmt.Fprintf(out, "Worker: %s; heartbeat %ds ago\n", worker, progress.HeartbeatAgeSeconds)
}

func publicationProgressPhaseLabel(progress publicationProgressReport) string {
	switch progress.Phase {
	case "capture_retry", "capture_blocked", "protection_incomplete":
		return "incomplete capture; checkpoint protection needs attention"
	case "protection_refresh":
		return "saving observed work"
	case "saved":
		return "saved; ready for finish"
	default:
		return strings.ReplaceAll(progress.Phase, "_", " ")
	}
}

func renderJSONEnvelope(out io.Writer, envelope productEnvelope) error {
	if envelope.Actions == nil {
		envelope.Actions = []productAction{}
	}
	if envelope.Data == nil {
		envelope.Data = map[string]any{}
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(envelope)
}

func yesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func onOff(value bool) string {
	if value {
		return "on"
	}
	return "off"
}
