package daemon

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentGoalOverviewRetrievesOnlyRelatedRecordedEvidence(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	blob := func(content string) string {
		oid, err := git.HashObjectStdin(ctx, f.dir, []byte(content))
		if err != nil {
			t.Fatal(err)
		}
		return oid
	}
	source := appendIntentCandidateCapture(t, f.db, "export.go", "create", "", blob("package example\nfunc ExportArchive() {}\nvar api_key = \"abcdef1234567890\"\n"))
	unrelated := appendIntentCandidateCapture(t, f.db, "welcome.go", "create", "", blob("package example\nfunc WelcomeVisitor() {}\n"))
	companion := appendIntentCandidateCapture(t, f.db, "export_test.go", "create", "", blob("package example\nfunc TestExportArchive() { ExportArchive() }\n"))
	companion.CapturedDiff = "+func TestExportArchive() { ExportArchive() }\n"
	existing := []state.IntentCandidate{
		{ID: "archive", Status: state.IntentCandidateWaiting, Purpose: "complete archive exports", Events: []state.IntentCandidateEvent{{EventSeq: source.Event.Seq}}},
		{ID: "welcome", Status: state.IntentCandidateWaiting, Purpose: "complete visitor welcome", Events: []state.IntentCandidateEvent{{EventSeq: unrelated.Event.Seq}}},
	}
	input := IntentCandidateEvaluation{RepoPath: f.dir, BranchRef: source.Event.BranchRef, BranchGeneration: source.Event.BranchGeneration, Captures: []IntentCandidateCapture{companion}, IncludeDiffs: true, Now: time.Now()}
	contextCaptures, err := loadFocusedIntentGoalEvidence(ctx, input, existing, []IntentCandidateCapture{source, unrelated, companion})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(contextCaptures[0].CapturedDiff, "ExportArchive") || strings.Contains(contextCaptures[0].CapturedDiff, "abcdef1234567890") {
		t.Fatal("related recorded source was absent or unredacted")
	}
	if contextCaptures[1].CapturedDiff != "" {
		t.Fatal("unrelated goal fetched full source")
	}
	req, err := buildIntentCandidateRequest(input, existing, nil, nil, contextCaptures)
	if err != nil {
		t.Fatal(err)
	}
	if len(req.Candidates[0].Paths) != 1 || req.Candidates[0].Paths[0] != "export.go" || len(req.Candidates[0].CapturedEvidence) != 1 {
		t.Fatalf("related goal=%+v", req.Candidates[0])
	}
	if len(req.Candidates[1].Paths) != 1 || len(req.Candidates[1].CapturedEvidence) != 0 {
		t.Fatalf("unrelated overview=%+v", req.Candidates[1])
	}
	if len(req.OfferedCaptures) != 1 || req.OfferedCaptures[0].Seq != companion.Event.Seq {
		t.Fatal("historical context became a new unowned offer")
	}
	input.IncludeDiffs = false
	private, err := buildIntentCandidateRequest(input, existing, nil, nil, contextCaptures)
	if err != nil {
		t.Fatal(err)
	}
	if private.Candidates[0].CapturedEvidence[0].CapturedDiff != "" {
		t.Fatal("context bypassed diff privacy permission")
	}
}
