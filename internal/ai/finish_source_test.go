package ai

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

func finishSourceFixture() (finish.Evidence, FinishOptions, []finish.Goal) {
	before := "package camera\ntype Status struct { Running bool }\nfunc selected(s Status) string { return \"\" }\n// PRIVATE_UNCHANGED_VALUE\n"
	after := "package camera\ntype Status struct { Running bool; CameraID string }\nfunc selected(s Status) string { return s.CameraID }\n// PRIVATE_UNCHANGED_VALUE\n"
	entry := func(text string) finish.Entry {
		return finish.Entry{Mode: "100644", OID: fmt.Sprintf("%x", sha1.Sum([]byte(fmt.Sprintf("blob %d\x00%s", len(text), text))))}
	}
	base, target := finish.Tree{"camera.go": entry(before)}, finish.Tree{"camera.go": entry(after)}
	evidence := finish.Evidence{Base: base, Target: target, Changes: finish.NetChanges(base, target, "author")}
	options := finishTestOptions(evidence)
	options.SourceFiles = []finish.SourceFile{{ChangeID: "camera.go", Before: []byte(before), After: []byte(after), Edits: []finish.SourceEdit{
		{ID: "edit-000001", Kind: "delete", BeforeLine: 1}, {ID: "edit-000002", Kind: "insert", BeforeLine: 1, AfterLine: 1},
		{ID: "edit-000003", Kind: "delete", BeforeLine: 2}, {ID: "edit-000004", Kind: "insert", BeforeLine: 2, AfterLine: 2},
	}}}
	options.Diffs[0].Text = "diff --acd-source camera.go\n[edit-000001] -type Status struct { Running bool }\n[edit-000002] +type Status struct { Running bool; CameraID string }\n[edit-000003] -func selected(s Status) string { return \"\" }\n[edit-000004] +func selected(s Status) string { return s.CameraID }\n"
	goals := []finish.Goal{
		{Purpose: "Expose camera identity", Boundary: "Supply the field before its consumer", Reason: "Controls need to identify the active camera.", Message: "Expose camera identity\n\n- Controls need to identify the active camera.", ChangeIDs: []string{"edit-000001..edit-000002"}},
		{Purpose: "Synchronize camera selection", Boundary: "Read the available identity", Reason: "Reading status keeps controls synchronized with the running camera.", Message: "Synchronize camera selection\n\n- Reading status keeps controls synchronized with the running camera.", ChangeIDs: []string{"edit-000003..edit-000004"}},
	}
	return evidence, options, goals
}

func TestFinishSourcePlanUsesOnlyFrozenSelections(t *testing.T) {
	evidence, options, goals := finishSourceFixture()
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if len(request.SourceEdits) != 4 || len(request.Changes) != 1 || request.Correction != nil {
			t.Fatalf("source catalogue or budget changed: %+v", request)
		}
		encoded, _ := json.Marshal(finishWireRequest(request))
		if strings.Contains(string(encoded), "PRIVATE_UNCHANGED_VALUE") || strings.Contains(string(encoded), "SourceFiles") {
			t.Fatal("local frozen source escaped into the provider request")
		}
		if !strings.Contains(finishPrompt(request), "source_edits") {
			t.Fatal("source membership rules missing")
		}
		return json.Marshal(finishPlanOutput{Goals: goals})
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || result.Calls != 1 || result.Source == nil || !maps.Equal(result.Plan.Target, evidence.Target) || len(result.Plan.Steps) != 2 {
		t.Fatalf("source plan: %+v %v", result, err)
	}
	if err := finish.Validate(result.Source.Evidence, result.Plan); err != nil {
		t.Fatal(err)
	}
	if result.Source.Selections[0].ChangeIDs[1] != "edit-000002" || result.Plan.Steps[0].Reason != goals[0].Reason {
		t.Fatal("range expansion changed decisions")
	}
}

func TestFinishSourceCorrectionIncludesMessagesAndMembership(t *testing.T) {
	evidence, options, goals := finishSourceFixture()
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		bad := append([]finish.Goal(nil), goals...)
		if request.Correction == nil {
			bad[0].ChangeIDs = []string{"edit-000099..edit-000100"}
			bad[0].Reason = ""
		} else if !strings.Contains(request.Correction.ValidationError, "reason") || !strings.Contains(request.Correction.ValidationError, "source selection") {
			t.Fatalf("combined correction lost errors: %+v", request.Correction)
		}
		return json.Marshal(finishPlanOutput{Goals: bad})
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || result.Calls != 2 || result.Source == nil {
		t.Fatalf("bounded source correction: %+v %v", result, err)
	}
}

func TestFinishSourceRejectsInvalidInputBeforeProvider(t *testing.T) {
	evidence, options, _ := finishSourceFixture()
	options.SourceFiles[0].After = []byte("unrecorded replacement\n")
	provider := &finishFakeTransport{}
	if _, err := PlanFinish(t.Context(), provider, evidence, options); err == nil || len(provider.requests) != 0 {
		t.Fatalf("invalid source evidence reached provider: %v", err)
	}
}

func TestFinishSourceAnnotationsDoNotDiscloseUnreferencedFiles(t *testing.T) {
	evidence, options, _ := finishSourceFixture()
	for _, path := range []string{"edit-000001", "diff", "base", "target"} {
		evidence.Base[path] = finishTestEntry("c")
		evidence.Target[path] = finishTestEntry("c")
	}
	request, _, err := prepareFinishRequest(evidence, options)
	if err != nil || len(request.ReferencedUnchangedPaths) != 0 {
		t.Fatalf("synthetic annotation became a file reference: %+v %v", request.ReferencedUnchangedPaths, err)
	}
}

func TestFinishSourceCorrectsFailedCheckWithoutSendingCommandOutput(t *testing.T) {
	evidence, options, goals := finishSourceFixture()
	checks := 0
	options.VerifySource = func(ctx context.Context, plan finish.SourcePlan) error {
		checks++
		if deadline, ok := ctx.Deadline(); !ok || !deadline.Equal(options.Deadline) || !maps.Equal(plan.Plan.Target, evidence.Target) {
			t.Fatal("source checks changed the accepted deadline or target")
		}
		if checks == 1 {
			return &SourceCheckFailure{Step: 2, Output: "PRIVATE_COMMAND_OUTPUT /private/path: cannot find 'CameraID' in scope; undefined: SecretOutsideApprovedDiff"}
		}
		return nil
	}
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if checks == 1 {
			if request.Correction == nil || !strings.Contains(request.Correction.ValidationError, "step 2") || !strings.Contains(request.Correction.ValidationError, "CameraID") {
				t.Fatalf("source correction lost approved diagnostic: %+v", request.Correction)
			}
			wire, _ := json.Marshal(request)
			for _, secret := range []string{"PRIVATE_COMMAND_OUTPUT", "/private/path", "SecretOutsideApprovedDiff"} {
				if strings.Contains(string(wire), secret) {
					t.Fatalf("command output escaped in provider correction: %s", secret)
				}
			}
		}
		return json.Marshal(finishPlanOutput{Goals: goals})
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || result.Calls != 2 || checks != 2 || result.Source == nil {
		t.Fatalf("source check correction: %+v checks=%d %v", result, checks, err)
	}
}

func TestFinishSourceVerifierFailureDoesNotRequestSemanticCorrection(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint(canceled), func(t *testing.T) {
			evidence, options, goals := finishSourceFixture()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			options.VerifySource = func(context.Context, finish.SourcePlan) error {
				if canceled {
					cancel()
				}
				return errors.New("local verifier unavailable")
			}
			provider := &finishFakeTransport{exchange: func(context.Context, FinishPlanRequest) ([]byte, error) {
				return json.Marshal(finishPlanOutput{Goals: goals})
			}}
			result, err := PlanFinish(ctx, provider, evidence, options)
			var failed *FinishPlanningError
			want := "source_verification_failed"
			if canceled {
				want = "provider_deadline"
			}
			if !errors.As(err, &failed) || failed.Code != want || result.Calls != 1 || result.Source != nil {
				t.Fatalf("verifier failure spent a correction: %+v %v", result, err)
			}
		})
	}
}

func TestFinishSourceCodeFitsGlobalBudgetWithoutReviews(t *testing.T) {
	evidence, options, goals := finishSourceFixture()
	options.Purpose = ""
	options.Diffs[0].Text += " unchanged UTF-8 evidence: õun 界\n"
	provider := &finishFakeTransport{padding: 600 << 10, exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if request.Review != nil || request.Purpose != "" || request.Diffs[0].Text != options.Diffs[0].Text {
			t.Fatal("complete source code was summarized, clipped or given mandatory context")
		}
		return json.Marshal(finishPlanOutput{Goals: goals})
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || result.Calls != 1 || result.Disclosure.EvidenceCalls != 0 || result.Source == nil {
		t.Fatalf("direct source plan: calls=%d reviews=%d %v", result.Calls, result.Disclosure.EvidenceCalls, err)
	}
}

func TestFinishSourceCodeSurvivesReferenceReviewCorrectionAndRetry(t *testing.T) {
	evidence, options, goals := finishSourceFixture()
	evidence.Target["reference.txt"] = finishTestEntry("c")
	evidence.Changes = finish.NetChanges(evidence.Base, evidence.Target, "author")
	reference := strings.Repeat("+ imported reference entry\n", 90000)
	options.Diffs = append(options.Diffs, FinishDiff{ChangeID: "reference.txt", Text: reference})
	goals = append(goals, finish.Goal{Purpose: "Supply reference data", Boundary: "Keep imported reference data independently reviewable", Reason: "Make the imported reference available to its consumers.", Message: "Supply reference data\n\n- Make the imported reference available to its consumers.", ChangeIDs: []string{"reference.txt"}})
	var saved finish.ReviewProgress
	options.SaveReviewProgress = func(_ context.Context, p finish.ReviewProgress) error { saved = p; return nil }
	retry, invalidReview, invalidPlan := false, false, false
	reviewed := ""
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if request.Review != nil {
			if retry {
				t.Fatal("matching whole-file reviews were not reused")
			}
			var observations []map[string]string
			for _, part := range request.Review.Parts {
				if part.ChangeID != "reference.txt" {
					t.Fatal("selectable source was redundantly summarized")
				}
				if request.Correction == nil {
					reviewed += part.Text
				}
				observations = append(observations, map[string]string{"id": part.ID, "summary": "Retain the complete imported reference and its role."})
			}
			if !invalidReview {
				invalidReview = true
				observations[0]["summary"] = "All tests passed"
			}
			return json.Marshal(map[string]any{"observations": observations})
		}
		if reviewed != reference || request.Diffs[0].Text != options.Diffs[0].Text || request.Diffs[1].Text != "" || len(request.Reviews) == 0 {
			t.Fatal("source code or complete reference review was lost")
		}
		bad := slices.Clone(goals)
		if !invalidPlan {
			invalidPlan = true
			bad[0].Reason = ""
		}
		return json.Marshal(finishPlanOutput{Goals: bad})
	}}
	first, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || first.Calls != 2 || first.Source == nil || len(saved.Observations) == 0 {
		t.Fatalf("review and correction: calls=%d %v", first.Calls, err)
	}
	retry = true
	options.ReviewProgress = saved
	second, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || second.Calls != 1 || second.Disclosure.EvidenceCalls != 0 || second.Disclosure.ReusedFragments != len(saved.Observations) || second.Source == nil {
		t.Fatalf("reference retry lost exact source: calls=%d reused=%d %v", second.Calls, second.Disclosure.ReusedFragments, err)
	}
}

func TestFinishSourceReviewRejectsOversizedGlobalCodeBeforeCalls(t *testing.T) {
	evidence, options, _ := finishSourceFixture()
	options.MaxInputBytes = 128 << 10
	options.Diffs[0].Text += strings.Repeat(" unchanged approved context\n", 2800)
	provider := &finishFakeTransport{padding: 64 << 10}
	_, err := PlanFinish(t.Context(), provider, evidence, options)
	finishAssertError(t, err, "provider_input_too_large")
	if len(provider.requests) != 0 {
		t.Fatal("oversized selectable code spent provider calls or lost code")
	}
}

func TestFinishSourceOwnershipPartitionsTheFrozenCatalogue(t *testing.T) {
	evidence, options, goals := finishSourceFixture()
	for i := range goals {
		goals[i].ChangeIDs = nil
	}
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if !slices.Equal(request.SelectionOrder, []string{"edit-000001", "edit-000002", "edit-000003", "edit-000004"}) || request.Purpose == "" {
			t.Fatal("request lost the exact ownership order")
		}
		return json.Marshal(finishPlanOutput{Goals: goals, Ownership: []finishOwnershipRun{{"edit-000002", 1}, {"edit-000004", 2}}})
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || result.Source == nil || len(result.Plan.Steps) != 2 || !slices.Equal(result.Source.Selections[1].ChangeIDs, []string{"edit-000003", "edit-000004"}) {
		t.Fatalf("ownership changed source membership: %+v %v", result, err)
	}
	_, catalogue, _ := finishSourceCatalogue(evidence, options.SourceFiles)
	for _, runs := range [][]finishOwnershipRun{
		{{"edit-000004", 0}}, {{"edit-000004", 3}}, {{"unseen", 1}}, {{"edit-000002", 1}},
		{{"edit-000002", 1}, {"edit-000001", 2}}, {{"edit-000002", 1}, {"edit-000002", 2}},
	} {
		if _, err := allocateFinishSourceGoals(goals, catalogue, runs); err == nil {
			t.Fatalf("invalid ownership was accepted: %+v", runs)
		}
	}
	goals[0].ChangeIDs = []string{"edit-000001"}
	if _, err := allocateFinishSourceGoals(goals, catalogue, []finishOwnershipRun{{"edit-000004", 1}}); err == nil {
		t.Fatal("mixed run and legacy ownership was accepted")
	}
}

func TestFinishSourceCheckFeedbackRetainsApprovedReceiver(t *testing.T) {
	failure := &SourceCheckFailure{Step: 4, Output: "/private/path.swift: value of type 'MetalCompositor' has no member 'texture'; value of type 'UnapprovedType' has no member 'secret'"}
	feedback := sourceCheckFeedback(failure, []FinishDiff{{Text: "-let texture = MetalCompositor()\n"}}).Error()
	if !strings.Contains(feedback, "MetalCompositor, texture") || strings.Contains(feedback, "UnapprovedType") || strings.Contains(feedback, "secret") || strings.Contains(feedback, "/private/") {
		t.Fatalf("receiver or privacy proof lost: %s", feedback)
	}
}

func TestFinishSourceCorrectionIncludesEveryFailedCandidate(t *testing.T) {
	evidence, options, goals := finishSourceFixture()
	options.VerifySource = func(_ context.Context, _ finish.SourcePlan) error {
		return errors.Join(
			&SourceCheckFailure{Step: 1, Output: "/private/work/camera.go: cannot find 'CameraID' in scope"},
			&SourceCheckFailure{Step: 2, Output: "/private/work/camera.go: value of type 'Status' has no member 'CameraID'"},
		)
	}
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if request.Correction != nil {
			feedback := request.Correction.ValidationError
			for _, needed := range []string{"step 1", "step 2", "CameraID", "Status", "camera.go"} {
				if !strings.Contains(feedback, needed) {
					t.Fatalf("combined correction lost %s: %s", needed, feedback)
				}
			}
			if strings.Contains(feedback, "/private/") {
				t.Fatal("combined correction disclosed private command paths")
			}
		}
		return json.Marshal(finishPlanOutput{Goals: goals})
	}}
	// Keep checks failing to prove that the one correction is shared and bounded.
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err == nil || result.Calls != 2 {
		t.Fatalf("combined failure bypassed verification or the correction limit: %+v %v", result, err)
	}
	feedback := sourceChecksFeedback(errors.Join(&SourceCheckFailure{Step: 1}, errors.New("PRIVATE_OUTPUT")), nil).Error()
	if strings.Contains(feedback, "PRIVATE_OUTPUT") {
		t.Fatal("unrecognized joined output escaped diagnostic filtering")
	}
}
