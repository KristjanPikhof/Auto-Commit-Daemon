package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestExplainPathUsesDecisionAndPendingState(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, _, db := makeExplainRepo(t, roots)
	ctx := context.Background()

	if _, err := state.AppendDecision(ctx, db, state.DecisionRecord{
		DecisionTS:  10,
		Kind:        state.DecisionKindProtected,
		Path:        sqlNullStr("secret.env"),
		Reason:      sqlNullStr("sensitive"),
		ActionTaken: sqlNullStr("no_delete_generated"),
		UserMessage: sqlNullStr("Skipped protected path secret.env without generating a delete."),
	}); err != nil {
		t.Fatalf("AppendDecision: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         "deadbeef",
		Operation:        "modify",
		Path:             "secret.env",
		Fidelity:         "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "secret.env", Fidelity: "exact"}}); err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}

	var out bytes.Buffer
	if err := runExplain(ctx, &out, repo, "secret.env", "", false, 0, 10, false); err != nil {
		t.Fatalf("runExplain path: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Path: secret.env",
		"Skipped protected path secret.env",
		"Replay is still pending for this path",
		"protected",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("explain path missing %q:\n%s", want, got)
		}
	}
}

func TestExplainCommitAndDefaultJSON(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, _, db := makeExplainRepo(t, roots)
	ctx := context.Background()

	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("RevParse HEAD: %v", err)
	}
	if _, err := state.AppendDecision(ctx, db, state.DecisionRecord{
		DecisionTS:  11,
		Kind:        state.DecisionKindHandledExternal,
		Path:        sqlNullStr("seed.txt"),
		CommitOID:   sqlNullStr(head),
		ActionTaken: sqlNullStr("handled externally"),
		UserMessage: sqlNullStr("External commit already contains seed.txt."),
	}); err != nil {
		t.Fatalf("AppendDecision: %v", err)
	}

	var out bytes.Buffer
	if err := runExplain(ctx, &out, repo, "", "HEAD", false, 0, 10, true); err != nil {
		t.Fatalf("runExplain commit: %v", err)
	}
	var rep explainReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("decode explain commit: %v\n%s", err, out.String())
	}
	if rep.Mode != "commit" || rep.Commit != head || len(rep.Decisions) != 1 {
		t.Fatalf("commit report = %+v, want HEAD decision", rep)
	}
	if !strings.Contains(rep.Explanation, "External commit already contains") {
		t.Fatalf("explanation=%q", rep.Explanation)
	}

	out.Reset()
	if err := runExplain(ctx, &out, repo, "", "", false, 0, 10, true); err != nil {
		t.Fatalf("runExplain default: %v", err)
	}
	var summary explainReport
	if err := json.Unmarshal(out.Bytes(), &summary); err != nil {
		t.Fatalf("decode explain default: %v\n%s", err, out.String())
	}
	if summary.Mode != "summary" || len(summary.Decisions) != 1 {
		t.Fatalf("summary report = %+v, want one recent decision", summary)
	}
}

func TestExplainSinceSummarizesNewestPostCursorDecision(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, _, db := makeExplainRepo(t, roots)
	ctx := context.Background()

	cursor, err := state.AppendDecision(ctx, db, state.DecisionRecord{
		DecisionTS: 10,
		Kind:       state.DecisionKindCaptured,
		Path:       sqlNullStr("old.go"),
	})
	if err != nil {
		t.Fatalf("AppendDecision cursor: %v", err)
	}
	if _, err := state.AppendDecision(ctx, db, state.DecisionRecord{
		DecisionTS:  11,
		Kind:        state.DecisionKindCaptured,
		Path:        sqlNullStr("first.go"),
		UserMessage: sqlNullStr("first post-cursor decision"),
	}); err != nil {
		t.Fatalf("AppendDecision first post-cursor: %v", err)
	}
	newestID, err := state.AppendDecision(ctx, db, state.DecisionRecord{
		DecisionTS:  12,
		Kind:        state.DecisionKindCommitted,
		Path:        sqlNullStr("newest.go"),
		UserMessage: sqlNullStr("newest post-cursor decision"),
	})
	if err != nil {
		t.Fatalf("AppendDecision newest post-cursor: %v", err)
	}

	var out bytes.Buffer
	if err := runExplain(ctx, &out, repo, "", "", false, cursor, 10, true); err != nil {
		t.Fatalf("runExplain since: %v", err)
	}
	var rep explainReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("decode explain since: %v\n%s", err, out.String())
	}
	if rep.DecisionCursor != newestID {
		t.Fatalf("DecisionCursor=%d want %d", rep.DecisionCursor, newestID)
	}
	if rep.Explanation != "newest post-cursor decision" {
		t.Fatalf("Explanation=%q want newest post-cursor decision; decisions=%+v", rep.Explanation, rep.Decisions)
	}
}

func TestExplainMissingDecisionLedgerIsReadOnly(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, dbPath, db := makeExplainRepo(t, roots)
	preparePreDecisionLedgerDB(t, db, dbPath)
	before := mustSHA256(t, dbPath)
	versionBefore := readUserVersionReadOnly(t, dbPath)

	var out bytes.Buffer
	if err := runExplain(context.Background(), &out, repo, "", "", false, 0, 10, true); err != nil {
		t.Fatalf("runExplain missing ledger: %v\n%s", err, out.String())
	}
	var rep explainReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("decode explain: %v\n%s", err, out.String())
	}
	if rep.DecisionLedgerAvailable || !strings.Contains(rep.Explanation, "Decision ledger is not available") {
		t.Fatalf("unexpected explain report: %+v", rep)
	}
	if after := mustSHA256(t, dbPath); after != before {
		t.Fatalf("state.db checksum changed: before=%s after=%s", before, after)
	}
	if got := readUserVersionReadOnly(t, dbPath); got != versionBefore {
		t.Fatalf("user_version changed: before=%d after=%d", versionBefore, got)
	}
}

func TestExplainValidationAndHelp(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, _, _ := makeExplainRepo(t, roots)

	if err := runExplain(context.Background(), &bytes.Buffer{}, repo, "x", "HEAD", false, 0, 10, false); err == nil {
		t.Fatalf("path+commit validation returned nil")
	}
	if err := runExplain(context.Background(), &bytes.Buffer{}, repo, "", "", false, -1, 10, false); err == nil {
		t.Fatalf("negative since validation returned nil")
	}

	root := newRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"explain", "--help"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("explain help: %v\nstderr:\n%s", err, errOut.String())
	}
	help := out.String()
	for _, want := range []string{"Explain recent ACD decisions", "--path", "--commit", "--last", "--since"} {
		if !strings.Contains(help, want) {
			t.Fatalf("explain help missing %q:\n%s", want, help)
		}
	}
}

func makeExplainRepo(t *testing.T, roots paths.Roots) (repoDir, dbPath string, d *state.DB) {
	t.Helper()
	repoDir, dbPath, d = makeDiagnoseRepo(t, roots)
	ctx := context.Background()
	for _, kv := range [][]string{
		{"user.email", "acd-test@example.com"},
		{"user.name", "ACD Test"},
		{"commit.gpgsign", "false"},
	} {
		if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "config", kv[0], kv[1]); err != nil {
			t.Fatalf("git config %s: %v", kv[0], err)
		}
	}
	if err := writeAndCommitSeed(t, ctx, repoDir); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	return repoDir, dbPath, d
}

func writeAndCommitSeed(t *testing.T, ctx context.Context, repoDir string) error {
	t.Helper()
	path := filepath.Join(repoDir, "seed.txt")
	if err := os.WriteFile(path, []byte("seed\n"), 0o644); err != nil {
		return err
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "add", "seed.txt"); err != nil {
		return err
	}
	_, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "commit", "-q", "-m", "seed")
	return err
}
