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
		return rewriteProgressSink{}, fmt.Errorf("acd rewrite-commits: --progress must be auto, plain, json, or off")
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
		if event.Phase == "" && event.Message == "" {
			return nil
		}
		if event.Message == "" {
			_, err := fmt.Fprintf(s.out, "rewrite-commits: %s\n", event.Phase)
			return err
		}
		if event.Phase == "" {
			_, err := fmt.Fprintf(s.out, "rewrite-commits: %s\n", event.Message)
			return err
		}
		_, err := fmt.Fprintf(s.out, "rewrite-commits: %s: %s\n", event.Phase, event.Message)
		return err
	default:
		return nil
	}
}
