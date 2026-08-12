package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	rewriteEditFormatText = "text"
	rewriteEditFormatJSON = "json"
)

func renderRewritePlanEdit(plan state.RewritePlan, format string) ([]byte, error) {
	switch format {
	case rewriteEditFormatText:
		var b strings.Builder
		fmt.Fprintf(&b, "# ACD rewrite plan %s\n", plan.ID)
		b.WriteString("# Edit only the commit message text between message markers.\n")
		b.WriteString("# Comment lines outside message blocks are ignored.\n\n")
		for _, c := range plan.Commits {
			fmt.Fprintf(&b, "commit %s\n", c.OldOID)
			b.WriteString("message <<ACD_COMMIT_MESSAGE\n")
			b.WriteString(strings.TrimRight(c.ProposedMessage, "\n"))
			b.WriteString("\nACD_COMMIT_MESSAGE\n\n")
		}
		return []byte(b.String()), nil
	case rewriteEditFormatJSON:
		doc := rewritePlanEditJSON{PlanID: plan.ID}
		for _, c := range plan.Commits {
			doc.Commits = append(doc.Commits, rewritePlanEditJSONCommit{OldOID: c.OldOID, Message: c.ProposedMessage})
		}
		return json.MarshalIndent(doc, "", "  ")
	default:
		return nil, fmt.Errorf("acd history rewrite: unsupported --format %q", format)
	}
}

func parseRewritePlanEdit(data []byte, format string, base state.RewritePlan) ([]state.RewritePlanCommit, error) {
	switch format {
	case rewriteEditFormatText:
		return parseRewritePlanEditText(data, base)
	case rewriteEditFormatJSON:
		return parseRewritePlanEditJSON(data, base)
	default:
		return nil, fmt.Errorf("acd history rewrite: unsupported --format %q", format)
	}
}

func parseRewritePlanEditText(data []byte, base state.RewritePlan) ([]state.RewritePlanCommit, error) {
	type block struct{ oid, message string }
	var blocks []block
	s := bufio.NewScanner(bytes.NewReader(data))
	s.Buffer(make([]byte, 1024), 1024*1024)
	for s.Scan() {
		line := s.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if !strings.HasPrefix(trimmed, "commit ") {
			return nil, fmt.Errorf("acd history rewrite: invalid text plan: expected commit line, got %q", line)
		}
		oid := strings.TrimSpace(strings.TrimPrefix(trimmed, "commit "))
		if oid == "" {
			return nil, errors.New("acd history rewrite: invalid text plan: empty commit oid")
		}
		if !s.Scan() {
			return nil, fmt.Errorf("acd history rewrite: invalid text plan for %s: missing message marker", oid)
		}
		if strings.TrimSpace(s.Text()) != "message <<ACD_COMMIT_MESSAGE" {
			return nil, fmt.Errorf("acd history rewrite: invalid text plan for %s: expected message marker", oid)
		}
		var msg []string
		closed := false
		for s.Scan() {
			line = s.Text()
			if strings.TrimSpace(line) == "ACD_COMMIT_MESSAGE" {
				closed = true
				break
			}
			msg = append(msg, line)
		}
		if !closed {
			return nil, fmt.Errorf("acd history rewrite: invalid text plan for %s: missing end marker", oid)
		}
		blocks = append(blocks, block{oid: oid, message: strings.TrimRight(strings.Join(msg, "\n"), "\n")})
	}
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("acd history rewrite: read text plan: %w", err)
	}
	if len(blocks) != len(base.Commits) {
		return nil, fmt.Errorf("acd history rewrite: invalid text plan: got %d commit block(s), want %d", len(blocks), len(base.Commits))
	}
	out := make([]state.RewritePlanCommit, len(base.Commits))
	for i, baseCommit := range base.Commits {
		if blocks[i].oid != baseCommit.OldOID {
			return nil, fmt.Errorf("acd history rewrite: invalid text plan: commit %d oid %q, want %q", i+1, blocks[i].oid, baseCommit.OldOID)
		}
		if strings.TrimSpace(blocks[i].message) == "" {
			return nil, fmt.Errorf("acd history rewrite: invalid text plan: commit %s has empty message", blocks[i].oid)
		}
		if err := validateEditedRewriteMessage(base, baseCommit, blocks[i].message); err != nil {
			return nil, err
		}
		out[i] = baseCommit
		out[i].ProposedMessage = blocks[i].message
	}
	return out, nil
}

type rewritePlanEditJSON struct {
	PlanID  string                      `json:"plan_id"`
	Commits []rewritePlanEditJSONCommit `json:"commits"`
}

type rewritePlanEditJSONCommit struct {
	OldOID  string `json:"old_oid"`
	Message string `json:"message"`
}

func parseRewritePlanEditJSON(data []byte, base state.RewritePlan) ([]state.RewritePlanCommit, error) {
	var doc rewritePlanEditJSON
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("acd history rewrite: invalid JSON plan: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, errors.New("acd history rewrite: invalid JSON plan: trailing JSON value")
		}
		return nil, fmt.Errorf("acd history rewrite: invalid JSON plan: trailing data: %w", err)
	}
	if strings.TrimSpace(doc.PlanID) != "" && doc.PlanID != base.ID {
		return nil, fmt.Errorf("acd history rewrite: invalid JSON plan: plan_id %q, want %q", doc.PlanID, base.ID)
	}
	if len(doc.Commits) != len(base.Commits) {
		return nil, fmt.Errorf("acd history rewrite: invalid JSON plan: got %d commit(s), want %d", len(doc.Commits), len(base.Commits))
	}
	out := make([]state.RewritePlanCommit, len(base.Commits))
	for i, baseCommit := range base.Commits {
		got := doc.Commits[i]
		if got.OldOID != baseCommit.OldOID {
			return nil, fmt.Errorf("acd history rewrite: invalid JSON plan: commit %d oid %q, want %q", i+1, got.OldOID, baseCommit.OldOID)
		}
		if strings.TrimSpace(got.Message) == "" {
			return nil, fmt.Errorf("acd history rewrite: invalid JSON plan: commit %s has empty message", got.OldOID)
		}
		if err := validateEditedRewriteMessage(base, baseCommit, got.Message); err != nil {
			return nil, err
		}
		out[i] = baseCommit
		out[i].ProposedMessage = got.Message
	}
	return out, nil
}

func validateEditedRewriteMessage(plan state.RewritePlan, commit state.RewritePlanCommit, message string) error {
	if normalizeRewritePlanCommitFormat(plan.CommitFormat) != string(ai.CommitFormatConventional) {
		return nil
	}
	parts := strings.SplitN(strings.TrimSpace(message), "\n\n", 2)
	result := ai.Result{Subject: parts[0]}
	if len(parts) == 2 {
		result.Body = parts[1]
	}
	_, err := ai.ValidateCommitRewriteProposal(ai.CommitRewriteRequest{
		OldOID:          commit.OldOID,
		OriginalMessage: commit.OriginalMessage,
		CommitFormat:    ai.CommitFormatConventional,
	}, result)
	if err != nil {
		return fmt.Errorf("acd history rewrite: invalid conventional message for %s: %w", commit.OldOID, err)
	}
	return nil
}

func rewritePlanMessagesEqual(a, b []state.RewritePlanCommit) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].OldOID != b[i].OldOID || a[i].ProposedMessage != b[i].ProposedMessage {
			return false
		}
	}
	return true
}

func promptRewriteYesNo(in io.Reader, out io.Writer, question string, defaultYes bool) (bool, error) {
	if !isInteractiveInput(in) {
		return false, errors.New("acd history rewrite: interactive prompt required; use --yes, --plan-only, --review/--no-review, or --apply-plan/--apply with --yes/--dry-run for noninteractive use")
	}
	suffix := "[y/N]"
	if defaultYes {
		suffix = "[Y/n]"
	}
	fmt.Fprintf(out, "%s %s ", question, suffix)
	line, err := bufio.NewReader(in).ReadString('\n')
	if err != nil && err != io.EOF {
		return false, err
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	if answer == "" {
		return defaultYes, nil
	}
	return answer == "y" || answer == "yes", nil
}

func inputOrStdin(in io.Reader) io.Reader {
	if in != nil {
		return in
	}
	return os.Stdin
}

func isInteractiveInput(in io.Reader) bool {
	f, ok := in.(*os.File)
	if !ok {
		return true
	}
	st, err := f.Stat()
	return err == nil && (st.Mode()&os.ModeCharDevice) != 0
}

func editRewritePlanWithEditor(plan state.RewritePlan, format string) ([]state.RewritePlanCommit, bool, error) {
	initial, err := renderRewritePlanEdit(plan, format)
	if err != nil {
		return nil, false, err
	}
	editor := strings.TrimSpace(os.Getenv("EDITOR"))
	if editor == "" {
		return nil, false, errors.New("acd history rewrite: EDITOR is not set")
	}
	dir, err := os.MkdirTemp("", "acd-rewrite-plan-*")
	if err != nil {
		return nil, false, err
	}
	defer os.RemoveAll(dir)
	ext := ".txt"
	if format == rewriteEditFormatJSON {
		ext = ".json"
	}
	path := filepath.Join(dir, "rewrite-plan"+ext)
	if err := os.WriteFile(path, initial, 0o600); err != nil {
		return nil, false, err
	}
	cmd := exec.Command("sh", "-c", editor+" \"$1\"", "acd-editor", path)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, false, fmt.Errorf("acd history rewrite: editor failed: %w", err)
	}
	edited, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}
	commits, err := parseRewritePlanEdit(edited, format, plan)
	if err != nil {
		return nil, false, err
	}
	return commits, !bytes.Equal(initial, edited), nil
}

func persistEditedRewritePlan(ctx context.Context, repo string, plan state.RewritePlan, commits []state.RewritePlanCommit) (state.RewritePlan, error) {
	if rewritePlanMessagesEqual(plan.Commits, commits) {
		return plan, nil
	}
	if strings.TrimSpace(plan.ID) == "" {
		return state.RewritePlan{}, errors.New("acd history rewrite: cannot save edited plan revision without a saved plan id")
	}
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		return state.RewritePlan{}, err
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		return state.RewritePlan{}, fmt.Errorf("acd history rewrite: open state db: %w", err)
	}
	defer db.Close()
	id, err := state.CreateEditedRewritePlanRevision(ctx, db, plan.ID, commits, state.RewritePlanValidationValid)
	if err != nil {
		return state.RewritePlan{}, fmt.Errorf("acd history rewrite: save edited plan: %w", err)
	}
	updated, ok, err := state.LoadRewritePlan(ctx, db, id)
	if err != nil {
		return state.RewritePlan{}, fmt.Errorf("acd history rewrite: load edited plan: %w", err)
	}
	if !ok {
		return state.RewritePlan{}, fmt.Errorf("acd history rewrite: edited plan %q not found after save", id)
	}
	return updated, nil
}
