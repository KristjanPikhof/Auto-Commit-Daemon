package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

type productState string

const (
	productStateOff         productState = "off"
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
	Repo                     string                 `json:"repo"`
	Command                  string                 `json:"command"`
	Registered               bool                   `json:"registered"`
	Enabled                  bool                   `json:"enabled"`
	Worker                   string                 `json:"worker"`
	Protected                bool                   `json:"protected"`
	Published                bool                   `json:"published"`
	Busy                     bool                   `json:"busy"`
	OperationalState         string                 `json:"operational_state"`
	WorktreeClean            bool                   `json:"worktree_clean"`
	AllChangesCommittedInGit bool                   `json:"all_changes_committed_in_git"`
	CheckpointPublishedByACD bool                   `json:"checkpoint_published_by_acd"`
	ActionRequired           bool                   `json:"action_required"`
	CheckpointID             string                 `json:"checkpoint_id,omitempty"`
	PublicationDrain         publicationDrainReport `json:"publication_drain"`
	PendingEvents            int                    `json:"pending_events"`
	BlockedEvents            int                    `json:"blocked_events"`
	Summary                  string                 `json:"summary"`
	StatePreserved           bool                   `json:"state_preserved"`
	CLIVersion               string                 `json:"cli_version,omitempty"`
	SupervisorVersion        string                 `json:"supervisor_version,omitempty"`
	SupervisorWorkerState    string                 `json:"supervisor_worker_state,omitempty"`
	SupervisorWorkerRestarts int                    `json:"supervisor_worker_restarts,omitempty"`
	SupervisorWorkerError    string                 `json:"supervisor_worker_error,omitempty"`
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
	fmt.Fprintf(out, "Enabled: %s\n", yesNo(data.Enabled))
	fmt.Fprintf(out, "Protected: %s\n", yesNo(data.Protected))
	fmt.Fprintf(out, "Published to Git: %s\n", yesNo(data.Published))
	fmt.Fprintf(out, "Worktree clean: %s\n", yesNo(data.WorktreeClean))
	fmt.Fprintf(out, "All changes committed in Git: %s\n", yesNo(data.AllChangesCommittedInGit))
	fmt.Fprintf(out, "Latest protection checkpoint published by ACD: %s\n", yesNo(data.CheckpointPublishedByACD))
	if data.PublicationDrain.ID != "" {
		fmt.Fprintf(out, "Publication drain: %s phase=%s remaining=%d/%d\n",
			data.PublicationDrain.ID, data.PublicationDrain.Phase,
			data.PublicationDrain.RemainingEvents,
			data.PublicationDrain.TargetEvents)
	}
	fmt.Fprintf(out, "Action required: %s\n", yesNo(data.ActionRequired))
	if envelope.NextAction == nil {
		fmt.Fprintln(out, "Next: No action needed.")
	} else {
		fmt.Fprintf(out, "Next: %s\n", *envelope.NextAction)
	}
	if data.Summary != "" {
		fmt.Fprintf(out, "Status: %s\n", data.Summary)
	}
	return nil
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
