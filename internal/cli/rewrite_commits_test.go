package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRewriteCommitsHelpIncludesContract(t *testing.T) {
	help := commandHelp(t, "history rewrite")
	for _, want := range []string{
		"--from-sha 8f4c2a1",
		"--range-sha",
		"--last 5",
		"--plan-out",
		"--show-plan",
		"--edit",
		"$EDITOR",
		"standalone plan file",
		"--apply",
		"--plan-only",
		"--progress",
		"private backup ref",
		"Merge commits are",
		"Normal ACD protection never runs this command",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("rewrite-commits help missing %q:\n%s", want, help)
		}
	}
}

func TestHistoryRewriteHelpIncludesProviderFreeEditContract(t *testing.T) {
	for _, command := range []string{"history rewrite"} {
		t.Run(command, func(t *testing.T) {
			help := commandHelp(t, command)
			for _, want := range []string{
				"--edit",
				"$EDITOR",
				"saved plan ID",
				"standalone plan file",
				"--plan-only",
				"--dry-run",
				"make no AI request",
			} {
				if !strings.Contains(help, want) {
					t.Fatalf("%s help missing %q:\n%s", command, want, help)
				}
			}
		})
	}
}

func TestRewritePlanRefArg(t *testing.T) {
	tests := []struct {
		ref  string
		want string
	}{
		{"", ""},
		{"rp_abc123", "rp_abc123"},
		{"/tmp/rewrite.json", "/tmp/rewrite.json"},
		{"has space", strconv.Quote("has space")},
		{"tab\there", strconv.Quote("tab\there")},
		{"quote'path", strconv.Quote("quote'path")},
		{`dollar$path`, strconv.Quote(`dollar$path`)},
	}
	for _, tc := range tests {
		if got := rewritePlanRefArg(tc.ref); got != tc.want {
			t.Errorf("rewritePlanRefArg(%q) = %q, want %q", tc.ref, got, tc.want)
		}
	}
}

func writeRewriteTestFile(t *testing.T, repo, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(repo, name), []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func TestRewriteCommitsParserFlags(t *testing.T) {
	cmd := newRewriteCommitsCmd()
	cmd.SetArgs([]string{"--last", "2", "--review", "--format", "json", "--plan-only"})
	if err := cmd.ParseFlags(cmd.Flags().Args()); err == nil {
		// ParseFlags does not parse SetArgs; execute through commandHelp coverage below instead.
	}

	var opts rewriteCommitsOptions
	cmd = newRewriteCommitsCmd()
	cmd.SetArgs([]string{"--last", "2", "--review", "--format", "json", "--plan-only"})
	cmd.RunE = func(cmd *cobra.Command, args []string) error { return nil }
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute parser flags: %v", err)
	}
	_ = opts

	for _, args := range [][]string{
		{"--from-sha", "8f4c2a1", "--plan-only"},
		{"--from-nr", "5", "--plan-only"},
		{"--range-nr", "5-12", "--plan-only"},
		{"--range-sha", "main~12..main~4", "--plan-only"},
	} {
		cmd = newRewriteCommitsCmd()
		cmd.SetArgs(args)
		cmd.RunE = func(cmd *cobra.Command, args []string) error { return nil }
		if err := cmd.Execute(); err != nil {
			t.Fatalf("execute parser flags %v: %v", args, err)
		}
	}

	err := normalizeAndValidateRewriteOptions(&rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}, review: true, noReview: true, editFormat: "text"})
	if err == nil || !strings.Contains(err.Error(), "--review") {
		t.Fatalf("review/no-review validation err = %v", err)
	}
	err = normalizeAndValidateRewriteOptions(&rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}, editFormat: "yaml"})
	if err == nil || !strings.Contains(err.Error(), "--format") {
		t.Fatalf("format validation err = %v", err)
	}
	err = normalizeAndValidateRewriteOptions(&rewriteCommitsOptions{applyPlan: "plan-id", planOnly: true, editFormat: "text"})
	if err == nil || !strings.Contains(err.Error(), "--plan-only") {
		t.Fatalf("plan-only/apply validation err = %v", err)
	}
	err = normalizeAndValidateRewriteOptions(&rewriteCommitsOptions{showPlan: "plan-id", messagesOnly: true, editFormat: "text"})
	if err == nil || !strings.Contains(err.Error(), "only valid when generating") {
		t.Fatalf("messages-only saved-plan validation err = %v", err)
	}
}

func TestRewriteCommitsSelectorAliasesNormalize(t *testing.T) {
	tests := []struct {
		name string
		opts rewriteCommitsOptions
		want git.RewriteSelectionOptions
	}{
		{
			name: "from-sha",
			opts: rewriteCommitsOptions{fromSHA: "1234abcd", editFormat: "text"},
			want: git.RewriteSelectionOptions{FromSHA: "1234abcd"},
		},
		{
			name: "from-nr",
			opts: rewriteCommitsOptions{fromNR: 5, editFormat: "text"},
			want: git.RewriteSelectionOptions{FromPosition: 5},
		},
		{
			name: "range-nr",
			opts: rewriteCommitsOptions{rangeNR: "5-12", editFormat: "text"},
			want: git.RewriteSelectionOptions{Range: "5-12"},
		},
		{
			name: "range-sha",
			opts: rewriteCommitsOptions{rangeSHA: "main~12..main~4", editFormat: "text"},
			want: git.RewriteSelectionOptions{GitRange: "main~12..main~4"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := normalizeAndValidateRewriteOptions(&tc.opts); err != nil {
				t.Fatalf("normalize: %v", err)
			}
			if tc.opts.selection != tc.want {
				t.Fatalf("selection=%+v want %+v", tc.opts.selection, tc.want)
			}
		})
	}

	err := normalizeAndValidateRewriteOptions(&rewriteCommitsOptions{fromSHA: "abc123", fromNR: 2, editFormat: "text"})
	if err == nil || !strings.Contains(err.Error(), "choose only one") {
		t.Fatalf("mixed new selectors err = %v", err)
	}
	err = normalizeAndValidateRewriteOptions(&rewriteCommitsOptions{fromSHA: "abc123", selection: git.RewriteSelectionOptions{From: "2"}, editFormat: "text"})
	if err == nil || !strings.Contains(err.Error(), "one selector family") {
		t.Fatalf("mixed selector families err = %v", err)
	}
	err = normalizeAndValidateRewriteOptions(&rewriteCommitsOptions{rangeSHA: "--no-walk abc", editFormat: "text"})
	if err == nil || !strings.Contains(err.Error(), "--range-sha") {
		t.Fatalf("range-sha validation err = %v", err)
	}
}

func TestRewriteProgressPlainIncludesBoundedCounts(t *testing.T) {
	var out bytes.Buffer
	sink, err := newRewriteProgressSink("plain", false, &out)
	if err != nil {
		t.Fatalf("create plain progress sink: %v", err)
	}
	events := []rewriteProgressEvent{
		{Phase: "proposal", Message: "requesting proposal", Current: 42, Total: 169},
		{Phase: "proposal", Message: "proposal accepted", Current: 42, Total: 169},
		{Phase: "apply_recreate_selected", Message: "recreated selected commit", Current: 42, Total: 169},
		{Phase: "apply_recreate_unchanged", Message: "recreated unchanged descendant", Current: 2, Total: 3},
		{Phase: "validation", Message: "status valid", Current: 1},
	}
	for _, event := range events {
		if err := sink.Emit(event); err != nil {
			t.Fatalf("emit %+v: %v", event, err)
		}
	}

	want := strings.Join([]string{
		"History rewrite: Commit messages [42/169]: message ready",
		"History rewrite: Applying groups [42/169]: applied the new message",
		"History rewrite: Keeping later commits [2/3]: kept a later commit unchanged",
		"History rewrite: Plan check: plan is valid",
		"",
	}, "\n")
	if got := out.String(); got != want {
		t.Fatalf("plain progress:\n%s\nwant:\n%s", got, want)
	}
}

func TestRewritePlanTextEditRoundTripAndValidation(t *testing.T) {
	plan := rewritePlanEditTestPlan()
	rendered, err := renderRewritePlanEdit(plan, rewriteEditFormatText)
	if err != nil {
		t.Fatalf("render text: %v", err)
	}
	parsed, err := parseRewritePlanEdit(rendered, rewriteEditFormatText, plan)
	if err != nil {
		t.Fatalf("parse rendered text: %v\n%s", err, rendered)
	}
	if !rewritePlanGroupsEqual(plan.Groups, parsed) {
		t.Fatalf("text round trip changed commits: %#v", parsed)
	}

	edited := strings.Replace(string(rendered), "better subject", "edited subject", 1)
	parsed, err = parseRewritePlanEdit([]byte(edited), rewriteEditFormatText, plan)
	if err != nil {
		t.Fatalf("parse edited text: %v", err)
	}
	if got := parsed[0].ProposedMessage; !strings.Contains(got, "edited subject") {
		t.Fatalf("edited message = %q", got)
	}

	invalid := strings.Replace(string(rendered), "commit abc123", "commit different", 1)
	if _, err := parseRewritePlanEdit([]byte(invalid), rewriteEditFormatText, plan); err == nil || !strings.Contains(err.Error(), "oid") {
		t.Fatalf("parse invalid oid err = %v, want oid validation", err)
	}
}

func TestRewritePlanTextEditCanMergeAdjacentGroups(t *testing.T) {
	plan := rewritePlanEditTestPlan()
	edited := `group 1
commit abc123
commit def456
reason <<ACD_GROUP_REASON
implementation and follow-up
ACD_GROUP_REASON
message <<ACD_COMMIT_MESSAGE
Combine related history changes
ACD_COMMIT_MESSAGE
`
	groups, err := parseRewritePlanEdit([]byte(edited), rewriteEditFormatText, plan)
	if err != nil {
		t.Fatalf("parse merged text groups: %v", err)
	}
	if len(groups) != 1 || len(groups[0].Members) != 2 {
		t.Fatalf("groups=%+v", groups)
	}
}

func TestRewritePlanJSONEditRoundTripAndValidation(t *testing.T) {
	plan := rewritePlanEditTestPlan()
	rendered, err := renderRewritePlanEdit(plan, rewriteEditFormatJSON)
	if err != nil {
		t.Fatalf("render json: %v", err)
	}
	parsed, err := parseRewritePlanEdit(rendered, rewriteEditFormatJSON, plan)
	if err != nil {
		t.Fatalf("parse rendered json: %v\n%s", err, rendered)
	}
	if !rewritePlanGroupsEqual(plan.Groups, parsed) {
		t.Fatalf("json round trip changed commits: %#v", parsed)
	}

	unknown := strings.Replace(string(rendered), `"groups":`, `"unknown": true, "groups":`, 1)
	if _, err := parseRewritePlanEdit([]byte(unknown), rewriteEditFormatJSON, plan); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("parse unknown field err = %v, want unknown-field validation", err)
	}

	var doc rewritePlanEditJSON
	if err := json.Unmarshal(rendered, &doc); err != nil {
		t.Fatalf("unmarshal rendered json: %v", err)
	}
	doc.Groups[0].Message = "   "
	empty, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal empty-message json: %v", err)
	}
	if _, err := parseRewritePlanEdit(empty, rewriteEditFormatJSON, plan); err == nil {
		t.Fatalf("parse empty message succeeded, want validation error")
	}

	trailing := append(append([]byte{}, rendered...), []byte("\n{}")...)
	if _, err := parseRewritePlanEdit(trailing, rewriteEditFormatJSON, plan); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("parse trailing json err = %v, want trailing-data validation", err)
	}
}

func TestRewritePlanJSONEditCanMergeAdjacentGroups(t *testing.T) {
	plan := rewritePlanEditTestPlan()
	doc := rewritePlanEditJSON{
		PlanID: plan.ID,
		Groups: []rewritePlanEditJSONGroup{{
			OldOIDs:        []string{"abc123", "def456"},
			GroupingReason: "implementation and follow-up",
			Message:        "Combine related history changes",
		}},
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	groups, err := parseRewritePlanEdit(raw, rewriteEditFormatJSON, plan)
	if err != nil {
		t.Fatalf("parse merged groups: %v", err)
	}
	if len(groups) != 1 || len(groups[0].Members) != 2 {
		t.Fatalf("groups=%+v", groups)
	}

	doc.Groups[0].OldOIDs = []string{"def456", "abc123"}
	raw, _ = json.Marshal(doc)
	if _, err := parseRewritePlanEdit(raw, rewriteEditFormatJSON, plan); err == nil || !strings.Contains(err.Error(), "want") {
		t.Fatalf("reordered membership error=%v", err)
	}
}

func TestRewritePlanEditRejectsWrongConventionalFormat(t *testing.T) {
	plan := rewritePlanEditTestPlan()
	plan.CommitFormat = string(ai.CommitFormatConventional)
	plan.Groups = []state.RewritePlanGroup{{
		Members:         []state.RewritePlanMember{{OldOID: "abc123", OriginalMessage: "old subject"}},
		GroupingReason:  "test conventional message",
		ProposedMessage: "fix: improve rewrite edit validation",
	}}
	rendered, err := renderRewritePlanEdit(plan, rewriteEditFormatText)
	if err != nil {
		t.Fatalf("renderRewritePlanEdit: %v", err)
	}
	invalid := strings.Replace(string(rendered), "fix: improve rewrite edit validation", "Improve rewrite edit validation", 1)
	if _, err := parseRewritePlanEdit([]byte(invalid), rewriteEditFormatText, plan); err == nil {
		t.Fatalf("expected conventional format validation error")
	}
	valid, err := parseRewritePlanEdit(rendered, rewriteEditFormatText, plan)
	if err != nil {
		t.Fatalf("valid conventional edit rejected: %v", err)
	}
	if valid[0].ProposedMessage != "fix: improve rewrite edit validation" {
		t.Fatalf("ProposedMessage=%q", valid[0].ProposedMessage)
	}
}

func TestRewritePlanFakeEditorAcceptsUnchangedContent(t *testing.T) {
	plan := rewritePlanEditTestPlan()
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("EDITOR", editor)
	commits, changed, err := editRewritePlanWithEditor(plan, rewriteEditFormatText)
	if err != nil {
		t.Fatalf("edit unchanged: %v", err)
	}
	if changed {
		t.Fatalf("changed=true for unchanged editor content")
	}
	if !rewritePlanGroupsEqual(plan.Groups, commits) {
		t.Fatalf("unchanged editor altered commits: %#v", commits)
	}
}

func TestRewritePlanFakeEditorCanEditJSON(t *testing.T) {
	plan := rewritePlanEditTestPlan()
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\npython3 - <<'PY' \"$1\"\nimport pathlib, sys\np = pathlib.Path(sys.argv[1])\ns = p.read_text()\np.write_text(s.replace('better subject', 'json edited subject'))\nPY\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("EDITOR", editor)
	commits, changed, err := editRewritePlanWithEditor(plan, rewriteEditFormatJSON)
	if err != nil {
		t.Fatalf("edit json: %v", err)
	}
	if !changed {
		t.Fatalf("changed=false after fake editor edit")
	}
	if got := commits[0].ProposedMessage; !strings.Contains(got, "json edited subject") {
		t.Fatalf("edited message = %q", got)
	}
}

func rewritePlanEditTestPlan() state.RewritePlan {
	return state.RewritePlan{
		ID:               "plan-test",
		BranchRef:        "refs/heads/main",
		ExpectedHead:     "feedface",
		ValidationStatus: state.RewritePlanValidationValid,
		ApplyStatus:      state.RewritePlanApplyPending,
		Groups: []state.RewritePlanGroup{
			{Members: []state.RewritePlanMember{{OldOID: "abc123", OriginalMessage: "old subject"}}, GroupingReason: "first test group", ProposedMessage: "better subject\n\n- body"},
			{Members: []state.RewritePlanMember{{OldOID: "def456", OriginalMessage: "second old"}}, GroupingReason: "second test group", ProposedMessage: "second better"},
		},
	}
}
