package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	help := commandHelp(t, "rewrite-commits")
	for _, want := range []string{
		"ACD_COMMIT_STRATEGY",
		"ACD_AI_PROVIDER",
		"--from 8f4c2a1",
		"--from 5",
		"--range 5-12",
		"--last 4",
		"--git-range",
		"--base",
		"--head",
		"--plan-out",
		"--show-plan",
		"--apply-plan",
		"--apply",
		"--review",
		"--no-review",
		"--plan-only",
		"--format",
		"backup recovery",
		"current branch linear ranges only",
		"merge commit rewrites are refused",
		"no daemon automation",
	} {
		if !strings.Contains(help, want) {
			t.Fatalf("rewrite-commits help missing %q:\n%s", want, help)
		}
	}
}

func TestRewriteCommitsPlanGenerationRequiresIntentStrategy(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "openai-compat")
	t.Setenv(ai.EnvAPIKey, "test-key")
	repo := rewriteSelectionTestRepo(t)

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}}, false)
	if !errors.Is(err, ai.ErrRewriteRequiresIntentStrategy) {
		t.Fatalf("runRewriteCommits error = %v, want intent-strategy gate", err)
	}
}

func TestRewriteCommitsPlanGenerationRequiresUsableAIProvider(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, "intent")
	t.Setenv(ai.EnvProvider, "openai-compat")
	t.Setenv(ai.EnvAPIKey, "")
	repo := rewriteSelectionTestRepo(t)

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}}, false)
	if !errors.Is(err, ai.ErrRewriteRequiresAIProvider) {
		t.Fatalf("runRewriteCommits error = %v, want provider gate", err)
	}
}

func TestRewriteCommitsSavedPlanBypassesProviderGate(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "")
	planPath := filepath.Join(t.TempDir(), "rewrite.json")
	writeRewritePlanTestFile(t, planPath, state.RewritePlan{
		ID:               "plan-show",
		BranchRef:        "refs/heads/main",
		ExpectedHead:     "abc123456789",
		ValidationStatus: state.RewritePlanValidationValid,
		ApplyStatus:      state.RewritePlanApplyPending,
		Commits:          []state.RewritePlanCommit{{OldOID: "abc123456789", ProposedMessage: "shown subject", OriginalMessage: "old subject"}},
	})

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, "", rewriteCommitsOptions{showPlan: planPath}, false)
	if err != nil {
		t.Fatalf("runRewriteCommits show-plan returned error: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "without AI provider check") || !strings.Contains(got, "shown subject") || !strings.Contains(got, "Validation status: valid") {
		t.Fatalf("show-plan output missing loaded plan details/bypass note: %q", got)
	}
}

func TestRewriteCommitsApplyPlanRequiresValidValidationStatus(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	head, err := git.RevParse(context.Background(), repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	for _, status := range []string{"", state.RewritePlanValidationDraft, state.RewritePlanValidationInvalid} {
		planPath := filepath.Join(t.TempDir(), "rewrite-"+status+".json")
		writeRewritePlanTestFile(t, planPath, state.RewritePlan{
			BranchRef:        "refs/heads/main",
			ExpectedHead:     head,
			ValidationStatus: status,
			Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
		})
		var out bytes.Buffer
		err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{applyPlan: planPath, dryRun: true}, false)
		if err == nil || !strings.Contains(err.Error(), "validation status") {
			t.Fatalf("status %q apply err = %v, want validation-status refusal", status, err)
		}
	}
}

func TestRewriteCommitsApplyPlanRequiresConfirmationButBypassesProviderGate(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	head, err := git.RevParse(context.Background(), repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	planPath := filepath.Join(t.TempDir(), "rewrite.json")
	writeRewritePlanTestFile(t, planPath, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})

	var out bytes.Buffer
	err = runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{applyPlan: planPath}, false)
	if err == nil || !strings.Contains(err.Error(), "--yes or --dry-run") {
		t.Fatalf("apply-plan without confirmation error = %v, want confirmation refusal", err)
	}

	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "")
	out.Reset()
	err = runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{applyPlan: planPath, dryRun: true}, false)
	if err != nil {
		t.Fatalf("apply-plan dry-run returned error: %v", err)
	}
	if got := out.String(); !strings.Contains(got, "no second AI call") {
		t.Fatalf("apply-plan output missing no-AI-call note: %q", got)
	}
}

func rewriteSelectionTestRepo(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	repo := initRepoForRepoLifecycle(t)
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo, Timeout: git.DefaultWriteTimeout}, "config", "user.email", "test@example.com"); err != nil {
		t.Fatalf("git config user.email: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo, Timeout: git.DefaultWriteTimeout}, "config", "user.name", "Test User"); err != nil {
		t.Fatalf("git config user.name: %v", err)
	}
	writeRewriteTestFile(t, repo, "seed.txt", "seed\n")
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo, Timeout: git.DefaultWriteTimeout}, "add", "seed.txt"); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo, Timeout: git.DefaultWriteTimeout}, "commit", "-q", "-m", "seed"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	return repo
}

func writeRewriteTestFile(t *testing.T, repo, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(repo, name), []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func writeRewritePlanTestFile(t *testing.T, path string, plan state.RewritePlan) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create plan: %v", err)
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(plan); err != nil {
		t.Fatalf("encode plan: %v", err)
	}
}

func TestRewriteCommitsApplyRefusesPendingCaptureQueue(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		t.Fatalf("state db path: %v", err)
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open state db: %v", err)
	}
	defer db.Close()
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head, Operation: "modify", Path: "queued.txt", Fidelity: "full"}, nil)
	if err != nil {
		t.Fatalf("append pending event: %v", err)
	}
	planPath := filepath.Join(t.TempDir(), "rewrite.json")
	writeRewritePlanTestFile(t, planPath, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})

	var out bytes.Buffer
	err = runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{applyPlan: planPath, yes: true}, false)
	if err == nil || !strings.Contains(err.Error(), "pending event") || !strings.Contains(err.Error(), strconv.FormatInt(seq, 10)) {
		t.Fatalf("apply with pending queue err = %v, want pending refusal with seq %d", err, seq)
	}
}

func TestRewriteCommitsApplyRefusesPendingCaptureQueueHiddenBehindBarrier(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		t.Fatalf("state db path: %v", err)
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open state db: %v", err)
	}
	defer db.Close()
	if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head, Operation: "modify", Path: "blocked.txt", Fidelity: "full", State: state.EventStateBlockedConflict}, nil); err != nil {
		t.Fatalf("append blocked barrier: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head, Operation: "modify", Path: "hidden-pending.txt", Fidelity: "full"}, nil)
	if err != nil {
		t.Fatalf("append hidden pending event: %v", err)
	}
	visible, err := state.PendingEvents(ctx, db, 1)
	if err != nil {
		t.Fatalf("PendingEvents: %v", err)
	}
	if len(visible) != 0 {
		t.Fatalf("test setup expected barrier-hidden pending event, got visible=%+v", visible)
	}
	planPath := filepath.Join(t.TempDir(), "rewrite.json")
	writeRewritePlanTestFile(t, planPath, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})

	var out bytes.Buffer
	err = runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{applyPlan: planPath, yes: true}, false)
	if err == nil || !strings.Contains(err.Error(), "pending event") || !strings.Contains(err.Error(), strconv.FormatInt(seq, 10)) {
		t.Fatalf("apply with barrier-hidden pending queue err = %v, want pending refusal with seq %d", err, seq)
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
	if !rewritePlanMessagesEqual(plan.Commits, parsed) {
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
	if !rewritePlanMessagesEqual(plan.Commits, parsed) {
		t.Fatalf("json round trip changed commits: %#v", parsed)
	}

	unknown := strings.Replace(string(rendered), `"commits":`, `"unknown": true, "commits":`, 1)
	if _, err := parseRewritePlanEdit([]byte(unknown), rewriteEditFormatJSON, plan); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("parse unknown field err = %v, want unknown-field validation", err)
	}

	var doc rewritePlanEditJSON
	if err := json.Unmarshal(rendered, &doc); err != nil {
		t.Fatalf("unmarshal rendered json: %v", err)
	}
	doc.Commits[0].Message = "   "
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
	if !rewritePlanMessagesEqual(plan.Commits, commits) {
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

func TestRewritePromptBehavior(t *testing.T) {
	var out bytes.Buffer
	yes, err := promptRewriteYesNo(strings.NewReader("y\n"), &out, "Review or edit proposed messages before applying?", false)
	if err != nil || !yes {
		t.Fatalf("yes prompt got yes=%v err=%v", yes, err)
	}
	if !strings.Contains(out.String(), "Review or edit proposed messages before applying?") {
		t.Fatalf("prompt output = %q", out.String())
	}

	out.Reset()
	no, err := promptRewriteYesNo(strings.NewReader("\n"), &out, "Apply this rewrite plan now?", false)
	if err != nil || no {
		t.Fatalf("default-no prompt got yes=%v err=%v", no, err)
	}
}

func rewritePlanEditTestPlan() state.RewritePlan {
	return state.RewritePlan{
		ID:               "plan-test",
		BranchRef:        "refs/heads/main",
		ExpectedHead:     "feedface",
		ValidationStatus: state.RewritePlanValidationValid,
		ApplyStatus:      state.RewritePlanApplyPending,
		Commits: []state.RewritePlanCommit{
			{OldOID: "abc123", OriginalMessage: "old subject", ProposedMessage: "better subject\n\nbody"},
			{OldOID: "def456", OriginalMessage: "second old", ProposedMessage: "second better"},
		},
	}
}
