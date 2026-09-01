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
		b.WriteString("# Move commit lines only to change adjacent group boundaries.\n")
		b.WriteString("# Keep every commit exactly once and in the same order.\n")
		b.WriteString("# Comment lines outside message blocks are ignored.\n\n")
		for i, group := range plan.Groups {
			fmt.Fprintf(&b, "group %d\n", i+1)
			for _, member := range group.Members {
				fmt.Fprintf(&b, "commit %s\n", member.OldOID)
			}
			b.WriteString("reason <<ACD_GROUP_REASON\n")
			b.WriteString(strings.TrimRight(group.GroupingReason, "\n"))
			b.WriteString("\nACD_GROUP_REASON\n")
			b.WriteString("message <<ACD_COMMIT_MESSAGE\n")
			b.WriteString(strings.TrimRight(group.ProposedMessage, "\n"))
			b.WriteString("\nACD_COMMIT_MESSAGE\n\n")
		}
		return []byte(b.String()), nil
	case rewriteEditFormatJSON:
		doc := rewritePlanEditJSON{PlanID: plan.ID}
		for _, group := range plan.Groups {
			item := rewritePlanEditJSONGroup{GroupingReason: group.GroupingReason, Message: group.ProposedMessage}
			for _, member := range group.Members {
				item.OldOIDs = append(item.OldOIDs, member.OldOID)
			}
			doc.Groups = append(doc.Groups, item)
		}
		return json.MarshalIndent(doc, "", "  ")
	default:
		return nil, fmt.Errorf("acd history rewrite: unsupported --format %q", format)
	}
}

func parseRewritePlanEdit(data []byte, format string, base state.RewritePlan) ([]state.RewritePlanGroup, error) {
	switch format {
	case rewriteEditFormatText:
		return parseRewritePlanEditText(data, base)
	case rewriteEditFormatJSON:
		return parseRewritePlanEditJSON(data, base)
	default:
		return nil, fmt.Errorf("acd history rewrite: unsupported --format %q", format)
	}
}

func parseRewritePlanEditText(data []byte, base state.RewritePlan) ([]state.RewritePlanGroup, error) {
	s := bufio.NewScanner(bytes.NewReader(data))
	s.Buffer(make([]byte, 1024), 1024*1024)
	var groups []state.RewritePlanGroup
	membersByOID := rewritePlanMembersByOID(base.Groups)
	for s.Scan() {
		line := s.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		expectedGroupLine := fmt.Sprintf("group %d", len(groups)+1)
		if trimmed != expectedGroupLine {
			return nil, fmt.Errorf("acd history rewrite: invalid text plan: expected %q, got %q", expectedGroupLine, line)
		}
		var group state.RewritePlanGroup
		for s.Scan() {
			trimmed = strings.TrimSpace(s.Text())
			if trimmed == "reason <<ACD_GROUP_REASON" {
				break
			}
			if !strings.HasPrefix(trimmed, "commit ") {
				return nil, fmt.Errorf("acd history rewrite: invalid text plan: expected commit or reason line, got %q", s.Text())
			}
			oid := strings.TrimSpace(strings.TrimPrefix(trimmed, "commit "))
			member, ok := membersByOID[oid]
			if !ok {
				return nil, fmt.Errorf("acd history rewrite: invalid text plan: unknown commit oid %q", oid)
			}
			group.Members = append(group.Members, member)
		}
		if len(group.Members) == 0 {
			return nil, fmt.Errorf("acd history rewrite: invalid text plan: group %d has no commits", len(groups)+1)
		}
		reason, err := scanRewriteEditBlock(s, "ACD_GROUP_REASON")
		if err != nil {
			return nil, fmt.Errorf("acd history rewrite: invalid text plan group %d reason: %w", len(groups)+1, err)
		}
		if !s.Scan() || strings.TrimSpace(s.Text()) != "message <<ACD_COMMIT_MESSAGE" {
			return nil, fmt.Errorf("acd history rewrite: invalid text plan group %d: expected message marker", len(groups)+1)
		}
		message, err := scanRewriteEditBlock(s, "ACD_COMMIT_MESSAGE")
		if err != nil {
			return nil, fmt.Errorf("acd history rewrite: invalid text plan group %d message: %w", len(groups)+1, err)
		}
		group.GroupingReason = reason
		group.ProposedMessage = message
		groups = append(groups, group)
	}
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("acd history rewrite: read text plan: %w", err)
	}
	return validateEditedRewriteGroups(base, groups, "text")
}

func scanRewriteEditBlock(s *bufio.Scanner, marker string) (string, error) {
	var lines []string
	for s.Scan() {
		if strings.TrimSpace(s.Text()) == marker {
			value := strings.TrimRight(strings.Join(lines, "\n"), "\n")
			if strings.TrimSpace(value) == "" {
				return "", errors.New("block is empty")
			}
			return value, nil
		}
		lines = append(lines, s.Text())
	}
	return "", fmt.Errorf("missing end marker %s", marker)
}

type rewritePlanEditJSON struct {
	PlanID string                       `json:"plan_id"`
	Groups []rewritePlanEditJSONGroup `json:"groups"`
}

type rewritePlanEditJSONGroup struct {
	OldOIDs        []string `json:"old_oids"`
	GroupingReason string   `json:"grouping_reason"`
	Message        string   `json:"message"`
}

func parseRewritePlanEditJSON(data []byte, base state.RewritePlan) ([]state.RewritePlanGroup, error) {
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
	membersByOID := rewritePlanMembersByOID(base.Groups)
	groups := make([]state.RewritePlanGroup, 0, len(doc.Groups))
	for i, item := range doc.Groups {
		group := state.RewritePlanGroup{GroupingReason: item.GroupingReason, ProposedMessage: item.Message}
		for _, oid := range item.OldOIDs {
			member, ok := membersByOID[oid]
			if !ok {
				return nil, fmt.Errorf("acd history rewrite: invalid JSON plan: group %d has unknown oid %q", i+1, oid)
			}
			group.Members = append(group.Members, member)
		}
		groups = append(groups, group)
	}
	return validateEditedRewriteGroups(base, groups, "JSON")
}

func validateEditedRewriteGroups(base state.RewritePlan, groups []state.RewritePlanGroup, format string) ([]state.RewritePlanGroup, error) {
	if len(groups) == 0 {
		return nil, fmt.Errorf("acd history rewrite: invalid %s plan: groups must be non-empty", format)
	}
	want := flattenRewritePlanMemberOIDs(base.Groups)
	got := flattenRewritePlanMemberOIDs(groups)
	if len(got) != len(want) {
		return nil, fmt.Errorf("acd history rewrite: invalid %s plan: got %d commit(s), want %d", format, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			return nil, fmt.Errorf("acd history rewrite: invalid %s plan: commit %d oid %q, want %q", format, i+1, got[i], want[i])
		}
	}
	for i, group := range groups {
		if len(group.Members) == 0 || strings.TrimSpace(group.GroupingReason) == "" || strings.TrimSpace(group.ProposedMessage) == "" {
			return nil, fmt.Errorf("acd history rewrite: invalid %s plan: group %d is missing commits, reason, or message", format, i+1)
		}
		if err := validateEditedRewriteMessage(base, group); err != nil {
			return nil, err
		}
	}
	return groups, nil
}

func validateEditedRewriteMessage(plan state.RewritePlan, group state.RewritePlanGroup) error {
	if normalizeRewritePlanCommitFormat(plan.CommitFormat) != string(ai.CommitFormatConventional) {
		return nil
	}
	parts := strings.SplitN(strings.TrimSpace(group.ProposedMessage), "\n\n", 2)
	result := ai.Result{Subject: parts[0]}
	if len(parts) == 2 {
		result.Body = parts[1]
	}
	last := group.Members[len(group.Members)-1]
	_, err := ai.ValidateCommitRewriteProposal(ai.CommitRewriteRequest{OldOID: last.OldOID, OriginalMessage: last.OriginalMessage, CommitFormat: ai.CommitFormatConventional}, result)
	if err != nil {
		return fmt.Errorf("acd history rewrite: invalid conventional message for group ending at %s: %w", last.OldOID, err)
	}
	return nil
}

func rewritePlanGroupsEqual(a, b []state.RewritePlanGroup) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ProposedMessage != b[i].ProposedMessage || a[i].GroupingReason != b[i].GroupingReason {
			return false
		}
		if len(a[i].Members) != len(b[i].Members) {
			return false
		}
		for j := range a[i].Members {
			if a[i].Members[j].OldOID != b[i].Members[j].OldOID {
				return false
			}
		}
	}
	return true
}

func rewritePlanMembersByOID(groups []state.RewritePlanGroup) map[string]state.RewritePlanMember {
	out := make(map[string]state.RewritePlanMember)
	for _, group := range groups {
		for _, member := range group.Members {
			out[member.OldOID] = member
		}
	}
	return out
}

func flattenRewritePlanMemberOIDs(groups []state.RewritePlanGroup) []string {
	var out []string
	for _, group := range groups {
		for _, member := range group.Members {
			out = append(out, member.OldOID)
		}
	}
	return out
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

func editRewritePlanWithEditor(plan state.RewritePlan, format string) ([]state.RewritePlanGroup, bool, error) {
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
	groups, err := parseRewritePlanEdit(edited, format, plan)
	if err != nil {
		return nil, false, err
	}
	return groups, !bytes.Equal(initial, edited), nil
}

func persistEditedRewritePlan(ctx context.Context, repo string, plan state.RewritePlan, groups []state.RewritePlanGroup) (state.RewritePlan, error) {
	if rewritePlanGroupsEqual(plan.Groups, groups) {
		return plan, nil
	}
	if err := validateRewritePlanGroupsInRepo(ctx, repo, groups); err != nil {
		return state.RewritePlan{}, err
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
	id, err := state.CreateEditedRewritePlanRevision(ctx, db, plan.ID, groups, state.RewritePlanValidationValid)
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
