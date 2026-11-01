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

func TestFinishSourceReviewRetainsMappingsThroughCorrectionAndRetry(t *testing.T) {
	evidence, options, goals := finishSourceFixture()
	options.MaxInputBytes = 1 << 20
	options.Diffs[0].Text += strings.Repeat(" unchanged approved context\n", 6000)
	var saved finish.ReviewProgress
	options.SaveReviewProgress = func(_ context.Context, progress finish.ReviewProgress) error { saved = progress; return nil }
	retry, invalidReview, invalidPlan := false, false, false
	provider := &finishFakeTransport{padding: 400 << 10, exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if request.Review != nil {
			if retry {
				t.Fatal("matching mapped reviews were not reused")
			}
			var observations []map[string]string
			for _, part := range request.Review.Parts {
				summary := "Retain the supplied unchanged context."
				if len(part.SourceEditIDs) > 0 {
					if !strings.Contains(finishPrompt(request), "Cover every supplied source_edit_ids entry exactly once") || len(part.SourceEditIDs) != 4 {
						t.Fatal("review lost its scoped IDs or source instructions")
					}
					summary = "edit-000001..edit-000002 expose Status.CameraID; edit-000003..edit-000004 consume CameraID in selected, after its definition."
					if !invalidReview {
						invalidReview = true
						summary = "Expose camera identity and synchronize the consumer."
					} else if request.Correction == nil || !strings.Contains(request.Correction.ValidationError, "every supplied edit ID") {
						t.Fatal("unmapped source evidence was not corrected with specific feedback")
					}
				}
				observations = append(observations, map[string]string{"id": part.ID, "summary": summary})
			}
			return json.Marshal(map[string]any{"observations": observations})
		}
		mapped := false
		for _, observation := range request.Reviews {
			if strings.Contains(observation.Summary, "edit-000003..edit-000004 consume CameraID") {
				mapped = true
			}
		}
		if !mapped || len(request.SourceEdits) != 4 || request.Diffs[0].Text != options.Diffs[0].Text {
			t.Fatal("global planner lost the mapped observations or exact selectable code")
		}
		bad := append([]finish.Goal(nil), goals...)
		if !invalidPlan {
			invalidPlan = true
			bad[0].Reason = ""
		} else if !retry && request.Correction == nil {
			t.Fatal("global correction did not retain the mapped evidence")
		}
		return json.Marshal(finishPlanOutput{Goals: bad})
	}}
	first, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || first.Calls != 2 || first.Source == nil || !invalidReview || len(saved.Observations) == 0 {
		t.Fatalf("mapped review did not reach a corrected source plan: calls=%d %v", first.Calls, err)
	}
	retry = true
	options.ReviewProgress = saved
	second, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || second.Calls != 1 || second.Disclosure.EvidenceCalls != 0 || second.Disclosure.ReusedFragments != len(saved.Observations) || second.Source == nil {
		t.Fatalf("retry lost mapped evidence: calls=%d reused=%d %v", second.Calls, second.Disclosure.ReusedFragments, err)
	}
	// Stored provenance cannot make an unmapped source summary reusable.
	options.ReviewProgress.Observations[0].Summary = "Expose identity and synchronize the consumer."
	request, _, err := prepareFinishRequest(evidence, options)
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := finishReviewChunks(provider, request, FinishContextByteCap-(4<<10))
	if err != nil || cachedFinishReview(chunks[0].Review.Parts, options.ReviewProgress, saved.Fingerprint) != nil {
		t.Fatalf("cache accepted an unmapped source summary: %v", err)
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

func TestFinishSourceReviewMappingValidation(t *testing.T) {
	ids := []string{"_edit-000001", "_edit-000002", "_edit-000004"}
	for _, test := range []struct{ name, summary string }{
		{"missing", "_edit-000001 defines status; _edit-000004 consumes status."},
		{"unknown", "_edit-000001.._edit-000004 changes status and its consumer."},
		{"duplicate", "_edit-000001.._edit-000002 defines status; _edit-000002 and _edit-000004 consume status."},
		{"backwards", "_edit-000002.._edit-000001 defines status; _edit-000004 consumes status."},
		{"unmapped", "Define status and consume the available identity."},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateFinishReviewSourceIDs(test.summary, ids); err == nil {
				t.Fatal("incomplete or invented source mapping was accepted")
			}
		})
	}
	if err := validateFinishReviewSourceIDs("_edit-000001.._edit-000002 define status; _edit-000004 consumes the available identity.", ids); err != nil {
		t.Fatal(err)
	}
}

func TestFinishSourceReviewMapsSplitUTF8Lines(t *testing.T) {
	text := "[edit-000001] +" + strings.Repeat("界", 200) + "\n[edit-000002] -previous value\n"
	texts := finishDiffParts(text, 32)
	mapped, err := finishReviewSourceIDs(texts, []string{"edit-000001", "edit-000002"})
	if err != nil || strings.Join(texts, "") != text || len(mapped) != len(texts) {
		t.Fatalf("mapping changed the fragment evidence: %v", err)
	}
	offset := 0
	for i, fragment := range texts {
		for _, id := range mapped[i] {
			lineStart := strings.Index(text, "["+id+"] ")
			lineEnd := lineStart + strings.IndexByte(text[lineStart:], '\n') + 1
			if offset >= lineEnd || offset+len(fragment) <= lineStart {
				t.Fatalf("%s was associated with unrelated fragment %d", id, i)
			}
		}
		if len(mapped[i]) == 0 {
			t.Fatalf("fragment %d lost its annotated line identity", i)
		}
		offset += len(fragment)
	}
	if _, err := finishReviewSourceIDs(texts, []string{"edit-000003"}); err == nil {
		t.Fatal("missing source annotation was accepted")
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
