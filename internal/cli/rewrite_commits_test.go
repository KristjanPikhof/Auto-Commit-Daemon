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
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRewriteCommitsHelpIncludesContract(t *testing.T) {
	help := commandHelp(t, "rewrite-commits")
	for _, want := range []string{
		"Aliases:",
		"edit-commits",
		"edit-commit",
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

func TestRewriteCommitsAliasHelpIncludesEditContract(t *testing.T) {
	for _, command := range []string{"edit-commits", "edit-commit"} {
		t.Run(command, func(t *testing.T) {
			help := commandHelp(t, command)
			for _, want := range []string{
				"--edit",
				"$EDITOR",
				"saved plan ID",
				"standalone plan file",
				"--plan-only",
				"--dry-run",
				"does not create a new AI request",
			} {
				if !strings.Contains(help, want) {
					t.Fatalf("%s help missing %q:\n%s", command, want, help)
				}
			}
		})
	}
}

func TestRewriteCommitsPlanGenerationRequiresIntentStrategy(t *testing.T) {
	withIsolatedHome(t)
	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "openai-compat")
	t.Setenv(ai.EnvAPIKey, "test-key")
	repo := rewriteSelectionTestRepo(t)

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}}, false)
	if !errors.Is(err, ai.ErrRewriteRequiresIntentStrategy) {
		t.Fatalf("runRewriteCommits error = %v, want intent-strategy gate", err)
	}
	if out.Len() != 0 || !strings.Contains(err.Error(), "Current mode: Event (environment)") ||
		!strings.Contains(err.Error(), "Git history is unchanged") {
		t.Fatalf("intent guidance is incomplete: output=%q error=%v", out.String(), err)
	}
}

func TestRewriteCommitsPlanGenerationRequiresUsableAIProvider(t *testing.T) {
	withIsolatedHome(t)
	t.Setenv(ai.EnvCommitStrategy, "intent")
	t.Setenv(ai.EnvProvider, "openai-compat")
	t.Setenv(ai.EnvAPIKey, "")
	repo := rewriteSelectionTestRepo(t)

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}}, false)
	if !errors.Is(err, ai.ErrRewriteRequiresAIProvider) {
		t.Fatalf("runRewriteCommits error = %v, want provider gate", err)
	}
	if out.Len() != 0 || !strings.Contains(err.Error(), "acd auth set") ||
		!strings.Contains(err.Error(), "Git history is unchanged") {
		t.Fatalf("missing credential guidance is incomplete: output=%q error=%v", out.String(), err)
	}
}

func TestRewriteCommitsProviderRejectionExplainsRepositoryOverride(t *testing.T) {
	roots := withIsolatedHome(t)
	t.Setenv(ai.EnvAPIKey, "test-key")
	repo := rewriteSelectionTestRepo(t)
	repoHash := central.CanonicalID(repo)
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Global[config.FieldCommitStrategy] = json.RawMessage(`"intent"`)
		doc.Settings.Global[config.FieldProvider] = json.RawMessage(`"openai-compat"`)
		doc.Settings.Global[config.FieldModel] = json.RawMessage(`"global-model"`)
		doc.Settings.Repositories[repoHash] = config.RepositorySettings{
			Fields: config.Overrides{
				config.FieldCommitStrategy: json.RawMessage(`"intent"`),
				config.FieldProvider:       json.RawMessage(`"deterministic"`),
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("save settings: %v", err)
	}

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{
		selection: git.RewriteSelectionOptions{Last: 1},
	}, false)
	if !errors.Is(err, ai.ErrRewriteRequiresAIProvider) {
		t.Fatalf("error = %v, want provider gate", err)
	}
	if out.Len() != 0 {
		t.Fatalf("rejection printed success output before provider gate:\n%s", out.String())
	}
	message := err.Error()
	for _, want := range []string{
		"Current provider: Local rules (repository override)",
		"Global default: OpenAI-compatible (global-model)",
		"acd config edit --repo",
		"--inherit",
		"No plan was generated. Git history is unchanged.",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("error missing %q:\n%s", want, message)
		}
	}
}

func TestRewriteCommitsPlanGenerationUsesPersistedSettings(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := rewriteSelectionTestRepo(t)
	providerDir := t.TempDir()
	provider := filepath.Join(providerDir, "acd-provider-rewrite-test")
	if err := os.WriteFile(provider, []byte("#!/usr/bin/env python3\nimport json, sys\nfor line in sys.stdin:\n    json.loads(line)\n    print(json.dumps({'version': 1, 'subject': 'configured subject', 'body': ''}), flush=True)\n"), 0o755); err != nil {
		t.Fatalf("write provider: %v", err)
	}
	t.Setenv("PATH", providerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "deterministic")

	repoHash := central.CanonicalID(repo)
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Repositories[repoHash] = config.RepositorySettings{
			Fields: config.Overrides{
				config.FieldCommitStrategy: json.RawMessage(`"intent"`),
				config.FieldProvider:       json.RawMessage(`"subprocess:rewrite-test"`),
			},
		}
		return nil
	}); err != nil {
		t.Fatalf("save repository settings: %v", err)
	}

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{
		selection: git.RewriteSelectionOptions{Last: 1},
		planOnly:  true,
	}, false)
	if err != nil {
		t.Fatalf("plan generation with persisted settings: %v\noutput:\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "Generated valid rewrite plan") {
		t.Fatalf("persisted settings did not generate a plan:\n%s", out.String())
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

func TestRewriteCommitsPlanOnlyQuotesPlanOutPathWithSpaces(t *testing.T) {
	withIsolatedHome(t)
	repo := rewriteSelectionTestRepo(t)
	providerDir := t.TempDir()
	provider := filepath.Join(providerDir, "acd-provider-rewrite-test")
	if err := os.WriteFile(provider, []byte("#!/usr/bin/env python3\nimport json, sys\nfor line in sys.stdin:\n    req = json.loads(line)\n    print(json.dumps({'version': 1, 'subject': 'plugin subject', 'body': ''}), flush=True)\n"), 0o755); err != nil {
		t.Fatalf("write provider: %v", err)
	}
	t.Setenv("PATH", providerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(ai.EnvCommitStrategy, "intent")
	t.Setenv(ai.EnvProvider, "subprocess:rewrite-test")

	planDir := filepath.Join(t.TempDir(), "acd plans")
	if err := os.Mkdir(planDir, 0o755); err != nil {
		t.Fatalf("mkdir plan dir: %v", err)
	}
	planPath := filepath.Join(planDir, "rewrite plan.json")
	quoted := strconv.Quote(planPath)

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{
		selection: git.RewriteSelectionOptions{Last: 1},
		planOnly:  true,
		planOut:   planPath,
	}, false)
	if err != nil {
		t.Fatalf("plan-only with spaced plan-out: %v\noutput:\n%s", err, out.String())
	}
	got := out.String()
	assertRewritePlanNextFooter(t, got)
	for _, prefix := range []string{
		"  acd history rewrite --show-plan ",
		"  acd history rewrite --apply-plan ",
	} {
		var found bool
		for _, line := range strings.Split(got, "\n") {
			if !strings.HasPrefix(line, prefix) {
				continue
			}
			if !strings.Contains(line, quoted) {
				t.Fatalf("next step line missing quoted plan path %q:\n%s", quoted, line)
			}
			found = true
		}
		if !found {
			t.Fatalf("missing next step line with prefix %q in:\n%s", prefix, got)
		}
	}
}

func TestRewriteCommitsPlanOnlyGeneratePrintsNextFooter(t *testing.T) {
	withIsolatedHome(t)
	repo := rewriteSelectionTestRepo(t)
	providerDir := t.TempDir()
	provider := filepath.Join(providerDir, "acd-provider-rewrite-test")
	if err := os.WriteFile(provider, []byte("#!/usr/bin/env python3\nimport json, sys\nfor line in sys.stdin:\n    req = json.loads(line)\n    print(json.dumps({'version': 1, 'subject': 'plugin subject', 'body': ''}), flush=True)\n"), 0o755); err != nil {
		t.Fatalf("write provider: %v", err)
	}
	t.Setenv("PATH", providerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(ai.EnvCommitStrategy, "intent")
	t.Setenv(ai.EnvProvider, "subprocess:rewrite-test")

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}, planOnly: true}, false)
	if err != nil {
		t.Fatalf("plan-only generate: %v\noutput:\n%s", err, out.String())
	}
	assertRewritePlanNextFooter(t, out.String())
}

func TestRewriteCommitsGenerationProgressEvents(t *testing.T) {
	withIsolatedHome(t)
	repo := rewriteSelectionTestRepo(t)
	providerDir := t.TempDir()
	provider := filepath.Join(providerDir, "acd-provider-rewrite-test")
	if err := os.WriteFile(provider, []byte("#!/usr/bin/env python3\nimport json, sys\nfor line in sys.stdin:\n    json.loads(line)\n    print(json.dumps({'version': 1, 'subject': 'plugin subject', 'body': ''}), flush=True)\n"), 0o755); err != nil {
		t.Fatalf("write provider: %v", err)
	}
	t.Setenv("PATH", providerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(ai.EnvCommitStrategy, "intent")
	t.Setenv(ai.EnvProvider, "subprocess:rewrite-test")

	var stdout, stderr bytes.Buffer
	err := runRewriteCommits(context.Background(), &stdout, repo, rewriteCommitsOptions{
		selection:  git.RewriteSelectionOptions{Last: 1},
		planOnly:   true,
		progress:   "json",
		progressTo: &stderr,
	}, false)
	if err != nil {
		t.Fatalf("plan-only generate: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	got := stdout.String()
	selectionIdx := strings.Index(got, "Selected commits")
	generatedIdx := strings.Index(got, "Generated valid rewrite plan")
	if selectionIdx < 0 || generatedIdx < 0 || selectionIdx > generatedIdx {
		t.Fatalf("selection summary must appear before generated status:\n%s", got)
	}
	events := parseRewriteProgressEvents(t, stderr.String())
	wantPhases := []string{"selection", "provider", "proposal", "proposal", "save", "validation", "next"}
	if len(events) != len(wantPhases) {
		t.Fatalf("events=%+v want phases %v", events, wantPhases)
	}
	for i, want := range wantPhases {
		if events[i].Phase != want {
			t.Fatalf("event %d phase=%q want %q; events=%+v", i, events[i].Phase, want, events)
		}
	}
	if events[2].CommitOID == "" || events[2].CommitSubject == "" || events[2].Current != 1 || events[2].Total != 1 {
		t.Fatalf("proposal event missing commit progress fields: %+v", events[2])
	}
}

func parseRewriteProgressEvents(t *testing.T, body string) []rewriteProgressEvent {
	t.Helper()
	var events []rewriteProgressEvent
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if line == "" {
			continue
		}
		var event rewriteProgressEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("parse progress line %q: %v", line, err)
		}
		events = append(events, event)
	}
	return events
}

func progressEventPhases(events []rewriteProgressEvent) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		out = append(out, event.Phase)
	}
	return out
}

func TestRewriteCommitsEditSavedPlanByIDPlanOnlyBypassesProviderGate(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	planID := saveRewritePlanForRepo(t, ctx, repo, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     mustRevParse(t, ctx, repo, "HEAD"),
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: mustRevParse(t, ctx, repo, "HEAD"), ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("EDITOR", editor)
	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "")

	var out bytes.Buffer
	err := runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{editPlan: planID, planOnly: true, editFormat: rewriteEditFormatText}, false)
	if err != nil {
		t.Fatalf("edit saved plan by id: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "unchanged") || !strings.Contains(got, "no AI call") {
		t.Fatalf("edit output missing unchanged/no-AI status: %q", got)
	}
	if strings.Contains(got, "No commits were rewritten.") && !strings.Contains(got, "Plan saved") {
		t.Fatalf("plan-only edit must not end with only no-rewrite message: %q", got)
	}
	assertRewritePlanNextFooter(t, got)
}

func TestRewriteCommitsDeclinedApplyOmitsNextFooter(t *testing.T) {
	withIsolatedHome(t)
	repo := rewriteSelectionTestRepo(t)
	providerDir := t.TempDir()
	provider := filepath.Join(providerDir, "acd-provider-rewrite-test")
	if err := os.WriteFile(provider, []byte("#!/usr/bin/env python3\nimport json, sys\nfor line in sys.stdin:\n    req = json.loads(line)\n    print(json.dumps({'version': 1, 'subject': 'plugin subject', 'body': ''}), flush=True)\n"), 0o755); err != nil {
		t.Fatalf("write provider: %v", err)
	}
	t.Setenv("PATH", providerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(ai.EnvCommitStrategy, "intent")
	t.Setenv(ai.EnvProvider, "subprocess:rewrite-test")

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{
		selection: git.RewriteSelectionOptions{Last: 1},
		noReview:  true,
		in:        strings.NewReader("n\n"),
	}, false)
	if err != nil {
		t.Fatalf("declined apply generate: %v\noutput:\n%s", err, out.String())
	}
	got := out.String()
	if !strings.Contains(got, "No rewrite performed.") {
		t.Fatalf("declined apply output missing no-rewrite message: %q", got)
	}
	if strings.Contains(got, "Next:") {
		t.Fatalf("declined apply output must not include Next footer: %q", got)
	}
}

func TestRewriteCommitsEditStandaloneFilePersistsBackToFile(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	head := mustRevParse(t, ctx, repo, "HEAD")
	planPath := filepath.Join(t.TempDir(), "rewrite.json")
	writeRewritePlanTestFile(t, planPath, state.RewritePlan{
		ID:               "file-plan",
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		ApplyStatus:      state.RewritePlanApplyPending,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\npython3 - <<'PY' \"$1\"\nimport pathlib, sys\np = pathlib.Path(sys.argv[1])\np.write_text(p.read_text().replace('seed rewritten', 'file edited subject'))\nPY\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("EDITOR", editor)
	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "")

	var out bytes.Buffer
	err := runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{editPlan: planPath, planOnly: true, editFormat: rewriteEditFormatJSON}, false)
	if err != nil {
		t.Fatalf("edit standalone file: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "Edited rewrite plan file saved") || !strings.Contains(got, "no AI call") {
		t.Fatalf("edit file output missing saved/no-AI status: %q", got)
	}
	updated, err := readRewritePlanFile(planPath)
	if err != nil {
		t.Fatalf("read updated plan file: %v", err)
	}
	if updated.Commits[0].ProposedMessage != "file edited subject" || !updated.Edited || updated.ValidationStatus != state.RewritePlanValidationValid {
		t.Fatalf("standalone file not persisted as edited valid plan: %#v", updated)
	}
}

func TestRewriteCommitsGenerationReviewPrintsEditedRevisionID(t *testing.T) {
	withIsolatedHome(t)
	repo := rewriteSelectionTestRepo(t)
	providerDir := t.TempDir()
	provider := filepath.Join(providerDir, "acd-provider-rewrite-test")
	if err := os.WriteFile(provider, []byte("#!/usr/bin/env python3\nimport json, sys\nfor line in sys.stdin:\n    req = json.loads(line)\n    print(json.dumps({'version': 1, 'subject': 'plugin subject', 'body': ''}), flush=True)\n"), 0o755); err != nil {
		t.Fatalf("write provider: %v", err)
	}
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\npython3 - <<'PY' \"$1\"\nimport pathlib, sys\np = pathlib.Path(sys.argv[1])\np.write_text(p.read_text().replace('plugin subject', 'review edited subject'))\nPY\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("PATH", providerDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(ai.EnvCommitStrategy, "intent")
	t.Setenv(ai.EnvProvider, "subprocess:rewrite-test")
	t.Setenv("EDITOR", editor)

	var out bytes.Buffer
	err := runRewriteCommits(context.Background(), &out, repo, rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}, review: true, editFormat: rewriteEditFormatText, in: strings.NewReader("n\n")}, false)
	if err != nil {
		t.Fatalf("generation review edit: %v\noutput:\n%s", err, out.String())
	}
	got := out.String()
	if !strings.Contains(got, "Edited rewrite plan saved as ") || !strings.Contains(got, "No rewrite performed.") {
		t.Fatalf("generation review output missing edited revision id/no-rewrite: %q", got)
	}
	editedID := parseRewritePlanIDAfter(t, got, "Edited rewrite plan saved as")
	db := openRewritePlanTestDB(t, context.Background(), repo)
	defer db.Close()
	edited, ok, err := state.LoadRewritePlan(context.Background(), db, editedID)
	if err != nil || !ok {
		t.Fatalf("load edited revision: ok=%v err=%v", ok, err)
	}
	if !edited.Edited || edited.Commits[0].ProposedMessage != "review edited subject" {
		t.Fatalf("edited review revision not saved: %#v", edited)
	}
}

func TestRewriteCommitsEditSavedPlanPersistsRevisionWithoutAICall(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	head := mustRevParse(t, ctx, repo, "HEAD")
	planID := saveRewritePlanForRepo(t, ctx, repo, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\npython3 - <<'PY' \"$1\"\nimport pathlib, sys\np = pathlib.Path(sys.argv[1])\np.write_text(p.read_text().replace('seed rewritten', 'edited seed subject'))\nPY\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("EDITOR", editor)
	t.Setenv(ai.EnvCommitStrategy, "event")
	t.Setenv(ai.EnvProvider, "")

	var out bytes.Buffer
	err := runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{editPlan: planID, planOnly: true, editFormat: rewriteEditFormatText}, false)
	if err != nil {
		t.Fatalf("edit saved plan persisted revision: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "Edited rewrite plan saved as ") || !strings.Contains(got, "no AI call") {
		t.Fatalf("edit output missing saved revision/no-AI status: %q", got)
	}
	db := openRewritePlanTestDB(t, ctx, repo)
	defer db.Close()
	base, ok, err := state.LoadRewritePlan(ctx, db, planID)
	if err != nil || !ok {
		t.Fatalf("load base plan: ok=%v err=%v", ok, err)
	}
	if base.Commits[0].ProposedMessage != "seed rewritten" {
		t.Fatalf("base plan mutated: %#v", base.Commits[0])
	}
	editedID := parseEditedRewritePlanID(t, got)
	edited, ok, err := state.LoadRewritePlan(ctx, db, editedID)
	if err != nil || !ok {
		t.Fatalf("load edited plan: ok=%v err=%v", ok, err)
	}
	if !edited.BasePlanID.Valid || edited.BasePlanID.String != planID || edited.Revision != base.Revision+1 || !edited.Edited || edited.Commits[0].ProposedMessage != "edited seed subject" {
		t.Fatalf("edited revision not saved as expected: %#v", edited)
	}
}

func TestRewriteCommitsEditSavedPlanValidationFailure(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	head := mustRevParse(t, ctx, repo, "HEAD")
	planID := saveRewritePlanForRepo(t, ctx, repo, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\npython3 - <<'PY' \"$1\"\nimport pathlib, sys\np = pathlib.Path(sys.argv[1])\ns = p.read_text()\nstart = s.index('message <<ACD_COMMIT_MESSAGE') + len('message <<ACD_COMMIT_MESSAGE')\nend = s.index('ACD_COMMIT_MESSAGE', start)\np.write_text(s[:start] + '\\n   \\n' + s[end:])\nPY\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("EDITOR", editor)

	var out bytes.Buffer
	err := runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{editPlan: planID, planOnly: true, editFormat: rewriteEditFormatText}, false)
	if err == nil || !strings.Contains(err.Error(), "empty message") {
		t.Fatalf("edit validation err = %v, want empty message", err)
	}
}

func TestRewriteCommitsEditSavedPlanPromptsBeforeApply(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	head := mustRevParse(t, ctx, repo, "HEAD")
	planID := saveRewritePlanForRepo(t, ctx, repo, state.RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})
	editor := filepath.Join(t.TempDir(), "editor.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write editor: %v", err)
	}
	t.Setenv("EDITOR", editor)

	var out bytes.Buffer
	err := runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{editPlan: planID, editFormat: rewriteEditFormatText, in: strings.NewReader("n\n")}, false)
	if err != nil {
		t.Fatalf("edit prompt flow: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "Apply this edited rewrite plan now?") || !strings.Contains(got, "No rewrite performed.") {
		t.Fatalf("edit output missing apply prompt/no-rewrite: %q", got)
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
	if got := out.String(); !strings.Contains(got, "No new AI request was made") {
		t.Fatalf("apply-plan output missing no-AI-call note: %q", got)
	}
}

func TestRewriteApplyHealsStaleRunningStateWhenOff(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	head := mustRevParse(t, ctx, repo, "HEAD")
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		result := registry.DisableRepo(central.RepoRemovalTarget{Path: repo}, 10)
		if result.NotFound || !result.Record.LifecycleDisabled() {
			t.Fatalf("disable result=%+v", result)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	planPath := filepath.Join(t.TempDir(), "rewrite.json")
	writeRewritePlanTestFile(t, planPath, state.RewritePlan{
		BranchRef: "refs/heads/main", ExpectedHead: head,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits: []state.RewritePlanCommit{{
			OldOID: head, OriginalMessage: "seed",
			ProposedMessage: "Rewrite stale state safely",
		}},
	})

	var out bytes.Buffer
	if err := runRewriteCommits(ctx, &out, repo, rewriteCommitsOptions{
		applyPlan: planPath, yes: true,
	}, false); err != nil {
		t.Fatalf("apply with stale running row: %v\n%s", err, out.String())
	}
	db, err = state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	daemonState, _, err := state.LoadDaemonState(ctx, db)
	if err != nil || daemonState.Mode != "stopped" || daemonState.PID != 0 {
		t.Fatalf("daemon state=%+v err=%v", daemonState, err)
	}
}

func TestRewriteApplyRefusesEnabledLifecycleWithoutWriter(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := rewriteSelectionTestRepo(t)
	dbPath, err := rewriteStateDBPath(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	registerRepo(t, roots, repo, dbPath, "codex")

	lock, err := acquireRewriteApplyOwnership(context.Background(), repo, false)
	if lock != nil {
		_ = lock.Release()
	}
	if err == nil || !strings.Contains(err.Error(), "current state: enabled") {
		t.Fatalf("enabled lifecycle error=%v", err)
	}
}

func TestRewriteApplyRefusesCanonicalWriterLock(t *testing.T) {
	withIsolatedHome(t)
	repo := rewriteSelectionTestRepo(t)
	worktree, err := git.ResolveWorktree(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := daemon.AcquireDaemonLock(worktree.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Release()

	lock, err := acquireRewriteApplyOwnership(context.Background(), repo, false)
	if lock != nil {
		_ = lock.Release()
	}
	if err == nil || !strings.Contains(err.Error(), "writer lock") {
		t.Fatalf("writer ownership error=%v", err)
	}
}

func TestRewriteCommitsApplyProgressJSON(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)
	ctx := context.Background()
	head := mustRevParse(t, ctx, repo, "HEAD")
	planPath := filepath.Join(t.TempDir(), "rewrite.json")
	writeRewritePlanTestFile(t, planPath, state.RewritePlan{
		ID:               "file-plan",
		BranchRef:        "refs/heads/main",
		ExpectedHead:     head,
		ValidationStatus: state.RewritePlanValidationValid,
		Commits:          []state.RewritePlanCommit{{OldOID: head, ProposedMessage: "seed rewritten", OriginalMessage: "seed"}},
	})

	var stdout, stderr bytes.Buffer
	err := runRewriteCommits(ctx, &stdout, repo, rewriteCommitsOptions{
		applyPlan:  planPath,
		dryRun:     true,
		progress:   "json",
		progressTo: &stderr,
	}, false)
	if err != nil {
		t.Fatalf("apply dry-run: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	events := parseRewriteProgressEvents(t, stderr.String())
	if got := progressEventPhases(events); strings.Join(got, ",") != "apply_validate,apply_validate" {
		t.Fatalf("apply phases=%v want validation only; events=%+v", got, events)
	}
	if !strings.Contains(stdout.String(), "Status: This plan can be applied") {
		t.Fatalf("stdout missing dry-run result:\n%s", stdout.String())
	}
}

func assertRewritePlanNextFooter(t *testing.T, got string) {
	t.Helper()
	for _, want := range []string{
		"Plan saved",
		"Next:",
		"--show-plan",
		"--apply-plan",
		"--dry-run",
		"--yes",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("rewrite plan-only next footer missing %q in:\n%s", want, got)
		}
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

func parseEditedRewritePlanID(t *testing.T, output string) string {
	t.Helper()
	return parseRewritePlanIDAfter(t, output, "")
}

func parseRewritePlanIDAfter(t *testing.T, output, marker string) string {
	t.Helper()
	search := output
	if marker != "" {
		idx := strings.Index(output, marker)
		if idx < 0 {
			t.Fatalf("marker %q not found in output: %q", marker, output)
		}
		search = output[idx:]
	}
	for _, field := range strings.Fields(search) {
		field = strings.TrimSuffix(field, ".")
		if strings.HasPrefix(field, "rp_") || strings.HasPrefix(field, "rewrite-plan-") {
			return field
		}
	}
	t.Fatalf("no rewrite plan id found in output: %q", search)
	return ""
}

func mustRevParse(t *testing.T, ctx context.Context, repo, rev string) string {
	t.Helper()
	oid, err := git.RevParse(ctx, repo, rev)
	if err != nil {
		t.Fatalf("rev-parse %s: %v", rev, err)
	}
	return oid
}

func openRewritePlanTestDB(t *testing.T, ctx context.Context, repo string) *state.DB {
	t.Helper()
	dbPath, err := rewriteStateDBPath(ctx, repo)
	if err != nil {
		t.Fatalf("state db path: %v", err)
	}
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open state db: %v", err)
	}
	return db
}

func saveRewritePlanForRepo(t *testing.T, ctx context.Context, repo string, plan state.RewritePlan) string {
	t.Helper()
	db := openRewritePlanTestDB(t, ctx, repo)
	defer db.Close()
	id, err := state.SaveRewritePlan(ctx, db, plan)
	if err != nil {
		t.Fatalf("save rewrite plan: %v", err)
	}
	return id
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
	if err == nil || !strings.Contains(err.Error(), "protected change") || !strings.Contains(err.Error(), "acd commit-all") {
		t.Fatalf("apply with pending queue err = %v, want clear next step for seq %d", err, seq)
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
	if err == nil || !strings.Contains(err.Error(), "protected change") || !strings.Contains(err.Error(), "acd commit-all") {
		t.Fatalf("apply with barrier-hidden pending queue err = %v, want clear next step for seq %d", err, seq)
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

func TestRewriteCommitsProgressModesKeepJSONStdoutClean(t *testing.T) {
	repo := rewriteSelectionTestRepo(t)

	t.Run("json progress writes jsonl stderr", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		err := runRewriteCommits(context.Background(), &stdout, repo, rewriteCommitsOptions{
			selection:  git.RewriteSelectionOptions{Last: 1},
			progress:   "json",
			progressTo: &stderr,
		}, true)
		if err != nil {
			t.Fatalf("runRewriteCommits: %v", err)
		}
		var report rewriteSelectionReport
		if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
			t.Fatalf("stdout is not clean JSON: %v\n%s", err, stdout.String())
		}
		var event rewriteProgressEvent
		if err := json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &event); err != nil {
			t.Fatalf("stderr is not progress JSONL: %v\n%s", err, stderr.String())
		}
		if event.Event != "rewrite_progress" || event.Phase != "selection" {
			t.Fatalf("event=%+v, want rewrite_progress selection", event)
		}
	})

	t.Run("plain progress writes stderr", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		err := runRewriteCommits(context.Background(), &stdout, repo, rewriteCommitsOptions{
			selection:  git.RewriteSelectionOptions{Last: 1},
			progress:   "plain",
			progressTo: &stderr,
		}, true)
		if err != nil {
			t.Fatalf("runRewriteCommits: %v", err)
		}
		if !strings.Contains(stderr.String(), "History rewrite: Selected commits [1/1]: selected 1 commit") {
			t.Fatalf("stderr missing plain progress:\n%s", stderr.String())
		}
		if err := json.Unmarshal(stdout.Bytes(), &rewriteSelectionReport{}); err != nil {
			t.Fatalf("stdout is not clean JSON: %v\n%s", err, stdout.String())
		}
	})

	t.Run("auto non-tty and quiet are silent", func(t *testing.T) {
		for _, opts := range []rewriteCommitsOptions{
			{selection: git.RewriteSelectionOptions{Last: 1}, progress: "auto"},
			{selection: git.RewriteSelectionOptions{Last: 1}, progress: "json", quiet: true},
		} {
			var stdout, stderr bytes.Buffer
			opts.progressTo = &stderr
			if err := runRewriteCommits(context.Background(), &stdout, repo, opts, true); err != nil {
				t.Fatalf("runRewriteCommits: %v", err)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr=%q, want silent", stderr.String())
			}
		}
	})

	err := normalizeAndValidateRewriteOptions(&rewriteCommitsOptions{selection: git.RewriteSelectionOptions{Last: 1}, progress: "loud"})
	if err == nil || !strings.Contains(err.Error(), "--progress") {
		t.Fatalf("progress validation err = %v", err)
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
		"History rewrite: Applying messages [42/169]: applied the new message",
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

func TestRewritePlanEditRejectsWrongConventionalFormat(t *testing.T) {
	plan := rewritePlanEditTestPlan()
	plan.CommitFormat = string(ai.CommitFormatConventional)
	plan.Commits = []state.RewritePlanCommit{{
		OldOID:          "abc123",
		OriginalMessage: "old subject",
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
