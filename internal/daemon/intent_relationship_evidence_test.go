package daemon

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

func TestIntentRelationshipEvidenceRetainsRecordedMiddleWitnesses(t *testing.T) {
	t.Parallel()
	padding := strings.Repeat("+// ordinary unrelated explanation\n", 700)
	producer := intentCandidateCaptureFixture(1, "recording.swift", "create", "", "recording")
	producer.CapturedDiff = padding + "+func CompleteRecording() {}\n" + padding
	consumer := intentCandidateCaptureFixture(2, "upload.swift", "create", "", "upload")
	consumer.CapturedDiff = padding + "+CompleteRecording()\n" + padding
	captures := []IntentCandidateCapture{producer, consumer}
	if hints := runtimeIntentDependencyHints([]IntentCandidateCapture{{Event: producer.Event, CapturedDiff: truncateIntentEvidenceDiff(producer.CapturedDiff, 512)}, {Event: consumer.Event, CapturedDiff: truncateIntentEvidenceDiff(consumer.CapturedDiff, 512)}}); len(hints) != 0 {
		t.Fatalf("clipped fixture unexpectedly retains a relationship: %+v", hints)
	}
	diffs := prioritizeIntentRelationshipEvidence(captures)
	for i := range captures {
		if len(diffs[i]) > ai.IntentStageDiffCap {
			t.Fatal("per-capture evidence cap exceeded")
		}
		captures[i].CapturedDiff = truncateIntentEvidenceDiff(diffs[i], 512)
	}
	if hints := runtimeIntentDependencyHints(captures); len(hints) != 1 || hints[0].Kind != "symbol_hash" {
		t.Fatalf("recorded declaration/call lost after total allocation: %+v diffs=%q", hints, diffs)
	}
	if !strings.HasPrefix(diffs[0], intentRelationshipEvidencePrefix+"+func CompleteRecording() {}\n") || !strings.HasPrefix(diffs[1], intentRelationshipEvidencePrefix+"+CompleteRecording()\n") {
		t.Fatal("priority witness changed the original signed line")
	}
	if again := prioritizeIntentRelationshipEvidence([]IntentCandidateCapture{producer, consumer}); !reflect.DeepEqual(diffs, again) {
		t.Fatal("same recorded evidence produced unstable priority")
	}
	producer.CapturedDiff, consumer.CapturedDiff = diffs[0], diffs[1]
	if again := prioritizeIntentRelationshipEvidence([]IntentCandidateCapture{producer, consumer}); !reflect.DeepEqual(diffs, again) {
		t.Fatal("already prioritized evidence was duplicated")
	}
}

func TestIntentLiveEvidenceReloadsImmutableRawMiddleWitnesses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newCaptureFixture(t)
	padding := strings.Repeat("// ordinary unrelated explanation\n", 1500)
	var captures []IntentCandidateCapture
	for i, tc := range []struct{ path, code string }{{"recording.swift", "func CompleteRecording() {}\n"}, {"upload.swift", "CompleteRecording()\n"}} {
		oid, err := git.HashObjectStdin(ctx, f.dir, []byte(padding+tc.code+padding))
		if err != nil {
			t.Fatal(err)
		}
		capture := appendIntentCandidateCapture(t, f.db, tc.path, "create", "", oid)
		capture.CapturedDiff = "+// prior clipped input contains no source relationship\n"
		captures = append(captures, capture)
		if i == 1 && len(captures[0].Ops) == 0 {
			t.Fatal("fixture lost immutable captured operations")
		}
	}
	input := IntentCandidateEvaluation{RepoPath: f.dir, BranchRef: captures[0].Event.BranchRef,
		BranchGeneration: captures[0].Event.BranchGeneration, Captures: captures, IncludeDiffs: true, Now: time.Now()}
	loaded, err := loadFocusedIntentGoalEvidence(ctx, input, nil, append([]IntentCandidateCapture(nil), captures...))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(loaded[0].CapturedDiff, intentRelationshipEvidencePrefix+"+func CompleteRecording() {}\n") || !strings.HasPrefix(loaded[1].CapturedDiff, intentRelationshipEvidencePrefix+"+CompleteRecording()\n") {
		t.Fatalf("live planner reused clipped inputs instead of recorded raw witnesses: %q / %q", loaded[0].CapturedDiff, loaded[1].CapturedDiff)
	}
	if hints := runtimeIntentDependencyHints(loaded); len(hints) != 1 || hints[0].Kind != "symbol_hash" {
		t.Fatalf("immutable raw relationship did not survive live caps: %+v", hints)
	}
	input.IncludeDiffs = false
	private, err := loadFocusedIntentGoalEvidence(ctx, input, nil, append([]IntentCandidateCapture(nil), captures...))
	if err != nil || !reflect.DeepEqual(private, captures) {
		t.Fatal("raw source evidence bypassed disabled diff egress")
	}
}

func TestIntentRelationshipEvidencePreservesDocumentedPublicFences(t *testing.T) {
	t.Parallel()
	for _, fence := range []string{"```bash", "~~~bash"} {
		producer := intentCandidateCaptureFixture(1, "command.go", "create", "", "command")
		producer.CapturedDiff = "+cmd.Flags().StringVar(&branch, \"new-branch\", \"\", \"Create branch\")\n"
		doc := intentCandidateCaptureFixture(2, "docs/commands.md", "create", "", "docs")
		doc.CapturedDiff = strings.Repeat("+Ordinary prose\n", 1500) + " " + fence + "\n+acd history rewrite --new-branch goals\n " + fence[:3] + "\n" + strings.Repeat("+Ordinary prose\n", 1500)
		captures := []IntentCandidateCapture{producer, doc}
		diffs := prioritizeIntentRelationshipEvidence(captures)
		for i := range captures {
			captures[i].CapturedDiff = truncateIntentEvidenceDiff(diffs[i], 512)
		}
		hints := runtimeIntentDependencyHints(captures)
		if len(hints) != 1 || hints[0].Kind != "documented_public_reference" {
			t.Fatalf("recorded fenced public reference lost: %+v", hints)
		}
	}
}

func TestIntentRelationshipEvidenceRejectsUnrelatedLiteralAndCommentLines(t *testing.T) {
	t.Parallel()
	producer := intentCandidateCaptureFixture(1, "first.go", "create", "", "first")
	producer.CapturedDiff = "+func Value() {}\n"
	for _, unrelated := range []string{"+func Value() {}\n", "+// Value()\n", "+const label = \"Value()\"\n", " /* existing comment\n+func Value() {}\n */\n"} {
		consumer := intentCandidateCaptureFixture(2, "second.go", "create", "", "second")
		consumer.CapturedDiff = unrelated
		captures := []IntentCandidateCapture{producer, consumer}
		diffs := prioritizeIntentRelationshipEvidence(captures)
		for i := range captures {
			captures[i].CapturedDiff = diffs[i]
		}
		if hints := runtimeIntentDependencyHints(captures); len(hints) != 0 {
			t.Fatalf("priority invented a relationship for %q: %+v", unrelated, hints)
		}
	}
}

func TestIntentEnclosingChangedFunctionRequiresActualCode(t *testing.T) {
	t.Parallel()
	producer := intentCandidateCaptureFixture(1, "recording.go", "modify", "before", "after")
	consumer := intentCandidateCaptureFixture(2, "recording_test.go", "create", "", "test")
	consumer.CapturedDiff = "+func TestProtectedRecording() { CompleteRecording() }\n"
	for _, tc := range []struct {
		diff      string
		connected bool
	}{
		{"@@ -10,3 +10,3 @@ func CompleteRecording() {\n-limit := 1\n+limit := 2\n", true},
		{"@@ -10,3 +10,3 @@ func CompleteRecording() {\n-// old explanation\n+// new explanation\n", false},
		{"@@ -10,3 +10,3 @@ func CompleteRecording() {\n /* existing comment\n+fake implementation\n */\n", false},
	} {
		producer.CapturedDiff = tc.diff
		diffs := prioritizeIntentRelationshipEvidence([]IntentCandidateCapture{producer, consumer})
		producer.CapturedDiff, consumer.CapturedDiff = diffs[0], diffs[1]
		connected := false
		for _, hint := range runtimeIntentDependencyHints([]IntentCandidateCapture{producer, consumer}) {
			connected = connected || hint.Kind == "symbol_hash"
		}
		if connected != tc.connected {
			t.Fatalf("heading connected=%t want=%t: %q", connected, tc.connected, tc.diff)
		}
	}
}

func TestIntentModuleImportDoesNotImportTestFiles(t *testing.T) {
	t.Parallel()
	imports := map[string]struct{}{"example.org/project/internal/daemon": {}}
	if !intentSourceImports(imports, "internal/cli/status.go", "internal/daemon/worker.go") {
		t.Fatal("real production package import was lost")
	}
	if intentSourceImports(imports, "internal/cli/status.go", "internal/daemon/worker_test.go") {
		t.Fatal("ordinary package import falsely imports a test file")
	}
}

func TestIntentSwiftMacroReferencesRemainPathAware(t *testing.T) {
	t.Parallel()
	producer := intentCandidateCaptureFixture(1, "MeetingPolicy.swift", "create", "", "policy")
	producer.CapturedDiff = "+enum MeetingPolicy {}\n"
	for _, tc := range []struct {
		path, line string
		connected  bool
	}{
		{"MeetingTests.swift", "+#expect(MeetingPolicy.accepts())\n", true},
		{"MeetingTests.swift", "+try #require(MeetingPolicy.accepts())\n", true},
		{"meeting_test.py", "+#expect(MeetingPolicy.accepts())\n", false},
		{"MeetingTests.swift", "+// #expect(MeetingPolicy.accepts())\n", false},
		{"MeetingTests.swift", "+let label = \"#expect(MeetingPolicy.accepts())\"\n", false},
	} {
		consumer := intentCandidateCaptureFixture(2, tc.path, "create", "", "test")
		consumer.CapturedDiff = tc.line
		captures := []IntentCandidateCapture{producer, consumer}
		diffs := prioritizeIntentRelationshipEvidence(captures)
		for i := range captures {
			captures[i].CapturedDiff = diffs[i]
		}
		connected := false
		for _, hint := range runtimeIntentDependencyHints(captures) {
			connected = connected || hint.Kind == "symbol_hash"
		}
		if connected != tc.connected {
			t.Fatalf("%s %q connected=%t", tc.path, tc.line, connected)
		}
	}
}

func TestIntentRecordedDeclarationsAndContextUsesStayGrounded(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		producer, consumer string
		connected          bool
	}{
		{prependIntentRecordedReferenceContext("+// update diagnostics explanation\n", " class SourceDiagnostics {}\n"), "+SourceDiagnostics.report()\n", true},
		{" class SourceDiagnostics {}\n+// update diagnostics explanation\n", "+SourceDiagnostics.report()\n", false},
		{"+class SourceDiagnostics {}\n", " SourceDiagnostics.report()\n+let limit = 2\n", true},
		{"+class SourceDiagnostics {}\n", " // SourceDiagnostics.report()\n+let limit = 2\n", false},
		{prependIntentRecordedReferenceContext("+// explain state\n", " // class SourceDiagnostics {}\n"), "+SourceDiagnostics.report()\n", false},
	} {
		producer := intentCandidateCaptureFixture(1, "SourceDiagnostics.swift", "modify", "before", "after")
		consumer := intentCandidateCaptureFixture(2, "router.swift", "modify", "before2", "after2")
		producer.CapturedDiff, consumer.CapturedDiff = tc.producer, tc.consumer
		captures := []IntentCandidateCapture{producer, consumer}
		diffs := prioritizeIntentRelationshipEvidence(captures)
		for i := range captures {
			captures[i].CapturedDiff = diffs[i]
		}
		connected := false
		for _, hint := range runtimeIntentDependencyHints(captures) {
			connected = connected || hint.Kind == "symbol_hash"
		}
		if connected != tc.connected {
			t.Fatalf("producer=%q consumer=%q connected=%t", tc.producer, tc.consumer, connected)
		}
	}
}

func TestIntentQuotedSpacePathsAndTypePrioritySurviveAllocation(t *testing.T) {
	t.Parallel()
	producer := intentCandidateCaptureFixture(1, "Assistant iOS/RecordingManager.swift", "create", "", "manager")
	producer.CapturedDiff = "+let localState = 1\n+class RecordingManager {}\n"
	consumer := intentCandidateCaptureFixture(2, "router.swift", "create", "", "router")
	consumer.CapturedDiff = "+localState = 2\n+RecordingManager.start()\n"
	diffs := prioritizeIntentRelationshipEvidence([]IntentCandidateCapture{producer, consumer})
	if !strings.HasPrefix(diffs[0], intentRelationshipEvidencePrefix+"+class RecordingManager") {
		t.Fatalf("local bindings displaced the real type declaration: %q", diffs[0])
	}
	for _, quoted := range []string{`"Assistant iOS/RecordingManager.swift"`, "`Assistant iOS/RecordingManager.swift`"} {
		consumer.CapturedDiff = "+register(" + quoted + ")\n"
		files, _ := intentSourcePathReferences(consumer.CapturedDiff)
		if !intentSourceReferencesFile(files, consumer.Event.Path, strings.ToLower(producer.Event.Path), false) {
			t.Fatalf("exact space-bearing recorded path lost: %q %+v", quoted, files)
		}
	}
}
