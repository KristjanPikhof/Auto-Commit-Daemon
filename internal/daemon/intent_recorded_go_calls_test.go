package daemon

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentRecordedGoCallsRetainOnlyMissingChangedFunctionCalls(t *testing.T) {
	t.Parallel()
	const contents = `package app
func TestReferences(t *testing.T) {
	cases := []string{"example", "new case"}
	for _, value := range cases {
		t.Run(value, func(t *testing.T) {
			if CapturedReference(value) {
				t.Fatal("unexpected reference")
			}
		})
	}
}
func Unrelated() { OtherHelper() }
`
	const diff = "@@ -2,1 +2,2 @@ func TestReferences(t *testing.T) {\n+\tcases := []string{\"example\", \"new case\"}\n"
	calls := intentGoRecordedCallContext("table_test.go", contents, diff, 4096)
	if calls.packageName != "app" || len(calls.names) != 1 || calls.names[0] != "CapturedReference" || calls.context != " \t\t\tif CapturedReference(value) {\n" {
		t.Fatalf("missing call was not retained exactly: %+v", calls)
	}
	if existing := intentGoRecordedCallContext("table_test.go", contents, diff+" \t\t\tif CapturedReference(value) {\n", 4096); existing.context != "" {
		t.Fatalf("already supplied call gained redundant context: %q", existing.context)
	}
	if bounded := intentGoRecordedCallContext("table_test.go", contents, diff, 8); bounded.context != "" {
		t.Fatal("partial call line escaped the reference budget")
	}
	// Git can label an early-function hunk with the package declaration.
	// Its new-line coordinates still identify the one edited function.
	contextHunk := "@@ -1,3 +1,4 @@ package app\n package app\n func TestReferences(t *testing.T) {\n+\tcases := []string{\"example\", \"new case\"}\n"
	if actual := intentGoRecordedCallContext("table_test.go", contents, contextHunk, 4096); len(actual.names) != 1 || actual.names[0] != "CapturedReference" {
		t.Fatalf("context-labelled hunk lost its actual enclosing function: %+v", actual)
	}
}

func TestIntentRecordedGoCallsExcludeQuotedSelectorsAndCallbacks(t *testing.T) {
	t.Parallel()
	for _, body := range []string{
		`label := "CapturedReference(value)"; _ = label`,
		`// CapturedReference(value)`,
		`foreign.CapturedReference(value)`,
		`CapturedReference := func(string) {}; CapturedReference(value)`,
		`callback(value)`,
		`_ = len(value)`,
	} {
		contents := "package app\nfunc TestReferences(value string, callback func(string)) {\n" + body + "\n}\n"
		calls := intentGoRecordedCallContext("table_test.go", contents, "@@ -2,1 +2,2 @@ func TestReferences(value string, callback func(string)) {\n+value = \"new case\"\n", 4096)
		if calls.context != "" {
			t.Fatalf("ambiguous call supplied context for %q: %q", body, calls.context)
		}
	}
	if calls := intentGoRecordedCallContext("table_test.go", "package app\nfunc TestReferences() { CapturedReference(\"x\") }\n", "@@ -2,1 +2,2 @@ func TestReferences() {\n+// fixture explanation\n", 4096); calls.context != "" {
		t.Fatal("comment-only hunk became changed function ownership")
	}
	if calls := intentGoRecordedCallContext("table_test.go", "package app\nimport . \"other\"\nfunc TestReferences() { CapturedReference(\"x\") }\n", "@@ -3,1 +3,2 @@ func TestReferences() {\n+value := 2\n", 4096); calls.context != "" {
		t.Fatal("dot-imported call became local package ownership")
	}
	if calls := intentGoRecordedCallContext("table_test.go", "package app\nfunc TestReferences() {\n/*\nvalue := 2\n*/\nCapturedReference(\"x\")\n}\n", "@@ -4,1 +4,1 @@ func TestReferences() {\n+value := 2\n", 4096); calls.context != "" {
		t.Fatal("edit inside an existing block comment gained call ownership")
	}
}

func TestIntentRecordedGoCallsRequireSamePackageAndDirectory(t *testing.T) {
	t.Parallel()
	calls := intentGoRecordedCallContext("app/table_test.go", "package app\nfunc TestReferences() { CapturedReference(\"x\") }\n", "@@ -2,1 +2,2 @@ func TestReferences() {\n+value := 2\n", 4096)
	names := make(intentReferenceNames)
	addIntentRecordedGoCallNames(names, "app/table_test.go", calls)
	for _, tc := range []struct{ path, contents string }{
		{"other/references.go", "package app\nfunc CapturedReference(value string) {}\n"},
		{"app/references.go", "package other\nfunc CapturedReference(value string) {}\n"},
	} {
		if got := intentRecordedDeclarationContext(tc.path, tc.contents, names); got != "" {
			t.Fatalf("foreign owner supplied declaration: %q", got)
		}
	}
	if got := intentRecordedDeclarationContext("app/references.go", "package app\nfunc CapturedReference(value string) {}\n", names); got == "" {
		t.Fatal("same-package direct call lost its recorded owner")
	}
}

func TestIntentLateTableCorrectionUsesPublishedRecordedHelper(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newCaptureFixture(t)
	blob := func(contents string) string {
		oid, err := git.HashObjectStdin(ctx, f.dir, []byte(contents))
		if err != nil {
			t.Fatal(err)
		}
		return oid
	}
	before := "package app\nfunc CapturedReference(value string) bool { return len(value) < 256 }\n"
	after := "package app\nfunc CapturedReference(value string) bool { return len(value) < 128 }\n"
	implementation := appendIntentCandidateCapture(t, f.db, "references.go", "modify", blob(before), blob(after))
	rows := strings.Repeat("\t\t\"ordinary example\",\n", 20)
	testBefore := "package app\nimport \"testing\"\nfunc TestReferences(t *testing.T) {\n\tcases := []string{\n" + rows + "\t}\n\tfor _, value := range cases {\n\t\tif CapturedReference(value) {\n\t\t\tt.Fatal(\"unexpected reference\")\n\t\t}\n\t}\n}\n"
	testAfter := strings.Replace(testBefore, rows, "\t\t\"quoted span cap\",\n"+rows, 1)
	correction := appendIntentCandidateCapture(t, f.db, "table_test.go", "modify", blob(testBefore), blob(testAfter))
	saveWaitingIntentCandidate(t, f.db, "quoted-reference-regression", 1, implementation, correction)
	commit := mustCommitPath(t, f.dir, "references.go", after, "Bound quoted reference examples")
	commitOID := sql.NullString{String: commit, Valid: true}
	if err := state.MarkEventPublished(ctx, f.db, implementation.Event.Seq, state.EventStatePublished, commitOID, sql.NullString{}, sql.NullString{}, float64(time.Now().Unix())); err != nil {
		t.Fatal(err)
	}
	implementation.Event.State, implementation.Event.CommitOID = state.EventStatePublished, commitOID
	candidate := state.IntentCandidate{ID: "quoted-references", BranchRef: implementation.Event.BranchRef,
		BranchGeneration: implementation.Event.BranchGeneration, Status: state.IntentCandidatePublished,
		Readiness: state.IntentReadinessReady, Purpose: "bound quoted reference examples", PublishedCommitOID: commitOID,
		Events: []state.IntentCandidateEvent{{EventSeq: implementation.Event.Seq, EventRole: "code"}}}
	if err := state.SaveIntentCandidate(ctx, f.db, candidate); err != nil {
		t.Fatal(err)
	}
	raw, err := BuildOpsDiff(ctx, f.dir, correction.Ops)
	if err != nil || strings.Contains(raw, "CapturedReference") {
		t.Fatalf("table-only fixture unexpectedly supplies the helper call: %q err=%v", raw, err)
	}
	input := IntentCandidateEvaluation{RepoPath: f.dir, BranchRef: implementation.Event.BranchRef,
		BranchGeneration: implementation.Event.BranchGeneration, Captures: []IntentCandidateCapture{correction}, IncludeDiffs: true, Now: time.Now()}
	evidence, err := loadFocusedIntentGoalEvidence(ctx, input, []state.IntentCandidate{candidate}, []IntentCandidateCapture{implementation, correction})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(evidence[1].CapturedDiff, " if CapturedReference") && !strings.Contains(evidence[1].CapturedDiff, "\tif CapturedReference") {
		t.Fatalf("late correction lost its actual recorded call: %q", evidence[1].CapturedDiff)
	}
	if !strings.Contains(evidence[0].CapturedDiff, "func CapturedReference") {
		t.Fatal("published recorded implementation was omitted")
	}
	input.Captures = []IntentCandidateCapture{evidence[1]}
	req, err := buildIntentCandidateRequest(input, []state.IntentCandidate{candidate}, nil, nil, evidence)
	if err != nil {
		t.Fatal(err)
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: "quoted-reference-regression", SelectedSeqs: []int64{correction.Event.Seq},
		Purpose: "cover bounded quoted reference examples", Readiness: ai.IntentCandidateReady,
		Subject: "Cover bounded quoted reference examples", Body: "- Exercise the published reference cap with a late table case",
		GroupingReason: "The recorded enclosing test calls the already-published reference helper",
	}}}
	if err := ValidateIntentGoalPlan(req, plan); err != nil {
		t.Fatalf("late test still waited for its published implementation: %v", err)
	}
	if len(req.OfferedCaptures) != 1 || req.OfferedCaptures[0].Seq != correction.Event.Seq {
		t.Fatal("published implementation became newly offered work")
	}
	planner := &intentCandidatePlannerStub{plan: plan}
	input.Captures = []IntentCandidateCapture{correction}
	input.LatestCommit = aiCommitSummary(git.CommitSummary{ShortOID: commit[:12]})
	input.Planner, input.Preset, input.VerificationMode = planner, config.PresetBalanced, "structural"
	input.Materialize = func(context.Context, []IntentCandidateCapture) error { return nil }
	result, err := EvaluateIntentCandidates(ctx, f.db, input)
	if err != nil || len(result.Decisions) != 1 || !result.Decisions[0].Publishable {
		t.Fatalf("normal evaluation did not recover the late test: %+v err=%v", result, err)
	}
	visible := false
	for _, id := range result.VisibleCandidateIDs {
		visible = visible || id == candidate.ID
	}
	if !visible || len(planner.req.OfferedCaptures) != 1 || planner.req.OfferedCaptures[0].Seq != correction.Event.Seq {
		t.Fatal("normal evaluation omitted the published companion or reoffered it")
	}
	input.IncludeDiffs = false
	private, err := loadFocusedIntentGoalEvidence(ctx, input, []state.IntentCandidate{candidate}, []IntentCandidateCapture{implementation, correction})
	if err != nil || private[0].CapturedDiff != "" || private[1].CapturedDiff != "" {
		t.Fatal("immutable call loading bypassed diff privacy")
	}
}
