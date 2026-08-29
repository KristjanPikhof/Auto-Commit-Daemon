package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
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
	Repo                     string                    `json:"repo"`
	Command                  string                    `json:"command"`
	Registered               bool                      `json:"registered"`
	Enabled                  bool                      `json:"enabled"`
	Worker                   string                    `json:"worker"`
	Protected                bool                      `json:"protected"`
	Published                bool                      `json:"published"`
	Busy                     bool                      `json:"busy"`
	OperationalState         string                    `json:"operational_state"`
	WorktreeClean            bool                      `json:"worktree_clean"`
	AllChangesCommittedInGit bool                      `json:"all_changes_committed_in_git"`
	CheckpointPublishedByACD bool                      `json:"checkpoint_published_by_acd"`
	ActionRequired           bool                      `json:"action_required"`
	CheckpointID             string                    `json:"checkpoint_id,omitempty"`
	PublicationDrain         publicationDrainReport    `json:"publication_drain"`
	PublicationProgress      publicationProgressReport `json:"publication_progress"`
	PendingEvents            int                       `json:"pending_events"`
	BlockedEvents            int                       `json:"blocked_events"`
	Summary                  string                    `json:"summary"`
	StatePreserved           bool                      `json:"state_preserved"`
	CLIVersion               string                    `json:"cli_version,omitempty"`
	SupervisorVersion        string                    `json:"supervisor_version,omitempty"`
	SupervisorWorkerState    string                    `json:"supervisor_worker_state,omitempty"`
	SupervisorWorkerRestarts int                       `json:"supervisor_worker_restarts,omitempty"`
	SupervisorWorkerError    string                    `json:"supervisor_worker_error,omitempty"`
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
			PublicationDrain:         result.PublicationDrain,
			PublicationProgress:      result.PublicationProgress,
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
	fmt.Fprintf(out, "State: %s\n", envelope.State)
	fmt.Fprintf(out, "ACD protection: %s\n", onOff(data.Enabled))
	fmt.Fprintf(out, "Current changes protected: %s\n", yesNo(data.Protected))
	fmt.Fprintf(out, "Published to Git: %s\n", yesNo(data.Published))
	renderProductPublicationProgress(out, data.PublicationProgress)
	fmt.Fprintf(out, "Action needed: %s\n", yesNo(data.ActionRequired))
	if data.Summary != "" {
		fmt.Fprintf(out, "Status: %s\n", data.Summary)
	}
	if envelope.NextAction == nil {
		fmt.Fprintln(out, "Next: No action needed.")
	} else {
		fmt.Fprintf(out, "Next: %s\n", *envelope.NextAction)
	}
	return nil
}

func renderProductPublicationProgress(
	out io.Writer,
	progress publicationProgressReport,
) {
	if progress.Strategy == "" {
		return
	}
	strategy := strings.ToUpper(progress.Strategy[:1]) + progress.Strategy[1:]
	if progress.TemporaryLocalFallback {
		if progress.Origin == "intent_recovery" {
			strategy += " (verified Intent-group widening active)"
		} else {
			strategy += " (temporary local fallback active)"
		}
	}
	fmt.Fprintf(out, "Commit mode: %s\n", strategy)
	if progress.PlannerProvider != "" {
		planner := progress.PlannerProvider
		if progress.PlannerModel != "" {
			planner += " / " + progress.PlannerModel
		}
		fmt.Fprintf(out, "Intent provider: %s\n",
			planner)
	}
	fmt.Fprintf(out, "Publication queue: %d protected change(s)\n",
		progress.QueuePending)
	if progress.Origin == "commit_all" && progress.TargetTotal > 0 {
		fmt.Fprintf(out, "Active target: earlier commit-all request, %d of %d left\n",
			progress.TargetRemaining, progress.TargetTotal)
	} else if progress.Origin == "intent_recovery" && progress.TargetTotal > 0 {
		fmt.Fprintf(out, "Active target: automatic Intent recovery, %d of %d left\n",
			progress.TargetRemaining, progress.TargetTotal)
	}
	fmt.Fprintf(out, "Publication phase: %s\n",
		publicationProgressPhaseLabel(progress))
	if progress.NeedsAttention && progress.AttentionReason != "" {
		fmt.Fprintf(out, "Recovery reason: %s\n", progress.AttentionReason)
	}
	if progress.LastProgressTS > 0 {
		fmt.Fprintf(out, "Last queue movement: %s ago\n",
			formatDurationCompact(time.Duration(progress.LastProgressAgeSeconds)*time.Second))
	}
	worker := "not responsive"
	if progress.WorkerResponsive {
		worker = "responsive"
		if progress.HeartbeatAgeSeconds >= 0 {
			worker += ", heartbeat " + formatDurationCompact(
				time.Duration(progress.HeartbeatAgeSeconds)*time.Second) + " ago"
		}
	}
	fmt.Fprintf(out, "Worker liveness: %s\n", worker)
}

func publicationProgressPhaseLabel(progress publicationProgressReport) string {
	switch progress.Phase {
	case "idle":
		return "idle"
	case "checkpointing":
		return "saving the protected checkpoint"
	case "paused":
		return "paused by the user"
	case "rewind_wait":
		if progress.WaitRemainingSeconds > 0 {
			return fmt.Sprintf("waiting after a Git history change (%s remaining)",
				formatDurationCompact(time.Duration(progress.WaitRemainingSeconds)*time.Second))
		}
		return "waiting after a Git history change"
	case "config_wait":
		return "waiting for configuration validation"
	case "intent_wait":
		if progress.WaitRemainingSeconds > 0 {
			return fmt.Sprintf("waiting normally for the Intent batch (%s remaining)",
				formatDurationCompact(time.Duration(progress.WaitRemainingSeconds)*time.Second))
		}
		return "waiting normally for the Intent batch"
	case "intent_planning":
		return "planning commit groups by Intent"
	case "intent_replanning":
		return "recovering by replanning commit groups by Intent"
	case "intent_processing":
		return "grouping and publishing by Intent"
	case "intent_verification_recovery":
		return "verification failed; automatic checkpoint replan pending"
	case "recovering":
		return "recovering the publication plan"
	case "local_fallback":
		if progress.Origin == "intent_recovery" {
			return "widening a verified Intent group to recover the protected target"
		}
		return "publishing one safe local group, then returning to Intent"
	case "provider_wait":
		if progress.WaitRemainingSeconds > 0 {
			return fmt.Sprintf("waiting for the Intent provider retry (%s remaining)",
				formatDurationCompact(time.Duration(progress.WaitRemainingSeconds)*time.Second))
		}
		if progress.Origin == "intent_recovery" {
			return "waiting for the Intent provider before continuing automatic recovery"
		}
		return "waiting for the Intent provider to write a semantic commit message"
	case "provider_call":
		return "waiting for the current Intent provider response"
	case "stalled":
		if progress.Origin == "intent_recovery" {
			return "automatic Intent recovery active; no target movement yet"
		}
		return "stalled; bounded automatic recovery is expected to start"
	case "retrying":
		return "retrying publication automatically"
	case "needs_action":
		if progress.Origin == "intent_recovery" {
			return intentRecoveryVerificationAttentionSummary + " " +
				intentRecoveryVerificationAttentionNext
		}
		return "stopped at a safety check"
	case "event_publishing":
		return "publishing captured events"
	default:
		return strings.ReplaceAll(progress.Phase, "_", " ")
	}
}

func publicationPhaseLabel(phase string, fallbackMode string) string {
	switch phase {
	case "checkpointing":
		return "saving the checkpoint"
	case "normalizing":
		return "preparing a safe publication plan"
	case "semantic":
		return "planning commit groups"
	case "event_fallback":
		if fallbackMode == "semantic_replan" {
			return "replanning commit groups by intent"
		}
		return "publishing safe commit groups"
	case "needs_action":
		return "blocked and waiting for action"
	default:
		return "working"
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
