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
	Repo              string `json:"repo"`
	Command           string `json:"command"`
	Registered        bool   `json:"registered"`
	Enabled           bool   `json:"enabled"`
	Protected         bool   `json:"protected"`
	Published         bool   `json:"published"`
	ActionRequired    bool   `json:"action_required"`
	Worker            string `json:"worker"`
	WorkerPID         int    `json:"worker_pid,omitempty"`
	CheckpointID      string `json:"checkpoint_id,omitempty"`
	PendingEvents     int    `json:"pending_events"`
	BlockedEvents     int    `json:"blocked_events"`
	Summary           string `json:"summary"`
	StatePreserved    bool   `json:"state_preserved"`
	CLIVersion        string `json:"cli_version,omitempty"`
	SupervisorVersion string `json:"supervisor_version,omitempty"`
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
			Repo:              result.Repo,
			Command:           result.Command,
			Registered:        result.Registered,
			Enabled:           result.Enabled,
			Protected:         result.Protected,
			Published:         result.Published,
			ActionRequired:    actionRequired,
			Worker:            result.Daemon,
			WorkerPID:         result.DaemonPID,
			CheckpointID:      result.CheckpointID,
			PendingEvents:     result.PendingEvents,
			BlockedEvents:     result.BlockedEvents,
			Summary:           result.Summary,
			StatePreserved:    result.StatePreserved,
			CLIVersion:        result.CLIVersion,
			SupervisorVersion: result.SupervisorVersion,
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
