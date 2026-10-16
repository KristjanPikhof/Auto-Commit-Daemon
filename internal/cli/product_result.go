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
	for _, action := range envelope.Actions {
		if action.Kind == "checkpoint_unconfirmed" {
			fmt.Fprintln(out, "Protection stopped. ACD could not confirm that the latest eligible changes are saved.")
		}
	}
	fmt.Fprintf(out, "Protection: %s\n", onOff(data.Enabled))
	if data.AllChangesCommittedInGit && data.PublicationOutcome.CountsKnown && data.PublicationOutcome.PendingChanges == 0 {
		fmt.Fprintln(out, "Git commits: all current changes are committed.")
	} else {
		fmt.Fprintf(out, "Git commits: %s\n", publicationOutcomeLabel(data.PublicationOutcome, data.PublicationProgress))
	}
	if data.Enabled && !data.ActionRequired {
		switch {
		case data.CaptureHealth.State == "retrying":
			fmt.Fprintln(out, "ACD is retrying automatically. Other readable changes remain saved.")
		case !data.Protected && data.Worker == "running":
			fmt.Fprintln(out, "ACD is checking and saving the latest changes automatically.")
		}
	}
	if data.PublicationOutcome.CheckpointAvailable {
		if !data.PublicationOutcome.CountsKnown || data.PublicationOutcome.WorktreeChanges != data.PublicationOutcome.PendingChanges {
			renderFinishUncommitted(out, data.PublicationOutcome)
		}
		if f := data.PublicationOutcome.Finish; finishStatusTakesPriority(f) {
			switch f.State {
			case state.FinishNeedsAttention:
				fmt.Fprintln(out, "Finish needs attention. The requested changes remain saved.")
			default:
				fmt.Fprintln(out, finishStatusSummary(f))
			}
		}
		if data.Busy && data.PublicationOutcome.LaterChanges > 0 {
			fmt.Fprintf(out, "Later changes: %d changed items for the next finish\n", data.PublicationOutcome.LaterChanges)
		}

	}
	if h := data.PublicationOutcome.History; h != nil {
		fmt.Fprintf(out, "History rewrite: %s\n", safePreviewText(strings.ReplaceAll(h.State, "_", " "), 0))
	}
	if legacy := data.PublicationOutcome.Legacy; legacy.State == "needs_attention" {
		renderLegacyAttention(out, legacy)
	}
	if data.PublicationOutcome.Legacy.Unpublished > 0 {
		fmt.Fprintf(out, "Older saved versions: %d retained\n", data.PublicationOutcome.Legacy.Unpublished)
	}
	if data.PublicationOutcome.RecoveredChanges > 0 {
		fmt.Fprintf(out, "Recovery: %d changes saved separately from the branch\n", data.PublicationOutcome.RecoveredChanges)
	}
	if data.ActionRequired && data.Summary != "" && (!finishStatusTakesPriority(data.PublicationOutcome.Finish) || data.CaptureHealth.Error != "") {
		summary := data.Summary
		if maintenance := data.CheckpointMaintenance.Summary(); maintenance != "" {
			if before, _, found := strings.Cut(summary, maintenance); found {
				summary = before + maintenance
			}
		}
		fmt.Fprintf(out, "Status: %s\n", consoleProse(summary))

	}
	if envelope.NextAction == nil {
		if data.ActionRequired {
			fmt.Fprintln(out, "\nAction required: Run `acd doctor` to inspect the problem.")
		} else {
			fmt.Fprintln(out, "No action needed.")
		}
	} else if data.ActionRequired {
		fmt.Fprintf(out, "\nAction required: %s\n", consoleProse(*envelope.NextAction))
	} else {
		fmt.Fprintf(out, "\nOptional: %s\n", consoleProse(*envelope.NextAction))
	}
	return nil
}

func publicationOutcomeLabel(outcome publicationOutcome, progress publicationProgressReport) string {
	if outcome.CheckpointAvailable {
		if outcome.BranchCommitted == nil {
			return "unknown"
		}
		if *outcome.BranchCommitted {
			return "saved work is committed on this branch"
		}
		if outcome.PendingChanges == 1 {
			return "1 saved item waiting for acd finish"
		}
		return fmt.Sprintf("%d saved items waiting for acd finish", outcome.PendingChanges)
	}
	if outcome.BranchCommitted == nil {
		return "unknown"
	}
	if outcome.PendingClassification && outcome.WaitingChanges == 0 {
		return "older saved changes awaiting review"
	}
	if outcome.WaitingChanges > 0 {
		return fmt.Sprintf("%d older changes awaiting review (%s)", outcome.WaitingChanges, publicationProgressPhaseLabel(progress))
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
	fmt.Fprintf(out, "Current activity: %s\n", publicationProgressPhaseLabel(progress))
	if progress.LastProgressTS > 0 {
		fmt.Fprintf(out, "Last save progress: %s ago\n", formatDurationCompact(time.Duration(progress.LastProgressAgeSeconds)*time.Second))
	}

}

func publicationProgressPhaseLabel(progress publicationProgressReport) string {
	switch progress.Phase {
	case "capture_retry", "capture_blocked", "protection_incomplete":
		return "some changes could not be saved"
	case "protection_refresh":
		return "saving observed work"
	case "saved":
		return "saved; ready for finish"
	default:
		return "activity unavailable"
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
