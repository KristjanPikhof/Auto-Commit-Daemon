package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mattn/go-isatty"
)

type rewriteProgressMode string

const (
	rewriteProgressModeAuto  rewriteProgressMode = "auto"
	rewriteProgressModePlain rewriteProgressMode = "plain"
	rewriteProgressModeJSON  rewriteProgressMode = "json"
	rewriteProgressModeOff   rewriteProgressMode = "off"
)

type rewriteProgressEvent struct {
	Event         string `json:"event"`
	Phase         string `json:"phase"`
	Message       string `json:"message,omitempty"`
	Current       int    `json:"current,omitempty"`
	Total         int    `json:"total,omitempty"`
	CommitOID     string `json:"commit_oid,omitempty"`
	NewCommitOID  string `json:"new_commit_oid,omitempty"`
	CommitSubject string `json:"commit_subject,omitempty"`
	PlanID        string `json:"plan_id,omitempty"`
	BackupRef     string `json:"backup_ref,omitempty"`
}

type rewriteProgressSink struct {
	mode rewriteProgressMode
	out  io.Writer
}

func validRewriteProgressMode(mode string) bool {
	switch rewriteProgressMode(strings.ToLower(strings.TrimSpace(mode))) {
	case rewriteProgressModeAuto, rewriteProgressModePlain, rewriteProgressModeJSON, rewriteProgressModeOff:
		return true
	default:
		return false
	}
}

func newRewriteProgressSink(mode string, quiet bool, out io.Writer) (rewriteProgressSink, error) {
	if quiet {
		return rewriteProgressSink{mode: rewriteProgressModeOff, out: io.Discard}, nil
	}
	progressMode := rewriteProgressMode(strings.ToLower(strings.TrimSpace(mode)))
	if progressMode == "" {
		progressMode = rewriteProgressModeAuto
	}
	if !validRewriteProgressMode(string(progressMode)) {
		return rewriteProgressSink{}, fmt.Errorf("acd history rewrite: --progress must be auto, plain, json, or off")
	}
	if out == nil {
		out = io.Discard
	}
	if progressMode == rewriteProgressModeAuto {
		if rewriteProgressIsTerminal(out) {
			progressMode = rewriteProgressModePlain
		} else {
			progressMode = rewriteProgressModeOff
		}
	}
	return rewriteProgressSink{mode: progressMode, out: out}, nil
}

func rewriteProgressIsTerminal(out io.Writer) bool {
	f, ok := out.(*os.File)
	if !ok {
		return false
	}
	return isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd())
}

func (s rewriteProgressSink) Emit(event rewriteProgressEvent) error {
	switch s.mode {
	case rewriteProgressModeOff:
		return nil
	case rewriteProgressModeJSON:
		if event.Event == "" {
			event.Event = "rewrite_progress"
		}
		enc := json.NewEncoder(s.out)
		return enc.Encode(event)
	case rewriteProgressModePlain:
		phase := rewriteProgressPhaseLabel(event.Phase)
		message := rewriteProgressMessage(event.Message)
		position := ""
		if event.Current > 0 && event.Total > 0 {
			position = fmt.Sprintf(" [%d/%d]", event.Current, event.Total)
		}
		if phase == "" && message == "" {
			return nil
		}
		if message == "" {
			_, err := fmt.Fprintf(s.out, "History rewrite: %s%s\n", phase, position)
			return err
		}
		if phase == "" {
			_, err := fmt.Fprintf(s.out, "History rewrite:%s %s\n", position, message)
			return err
		}
		_, err := fmt.Fprintf(s.out, "History rewrite: %s%s: %s\n", phase, position, message)
		return err
	default:
		return nil
	}
}

func rewriteProgressPhaseLabel(phase string) string {
	switch phase {
	case "selection":
		return "Selected commits"
	case "provider":
		return "AI provider"
	case "proposal":
		return "Commit messages"
	case "save":
		return "Saved plan"
	case "validation", "apply_validate":
		return "Plan check"
	case "next":
		return "Next"
	case "apply_backup":
		return "Recovery backup"
	case "apply_recreate_selected":
		return "Applying messages"
	case "apply_recreate_unchanged":
		return "Keeping later commits"
	case "apply_update_ref":
		return "Finishing"
	case "apply_reconcile":
		return "ACD records"
	default:
		return phase
	}
}

func rewriteProgressMessage(message string) string {
	switch message {
	case "requesting proposal":
		return "generating a message"
	case "proposal accepted":
		return "message ready"
	case "proposal failed":
		return "could not generate a message"
	case "checking repository":
		return "checking the repository"
	case "validated plan":
		return "plan is safe to apply"
	case "created backup refs":
		return "created a recovery backup"
	case "recreated selected commit":
		return "applied the new message"
	case "recreated unchanged descendant":
		return "kept a later commit unchanged"
	case "updating branch ref":
		return "updating the branch"
	case "reconciled state OIDs":
		return "updated ACD records"
	case "status valid":
		return "plan is valid"
	default:
		return message
	}
}
