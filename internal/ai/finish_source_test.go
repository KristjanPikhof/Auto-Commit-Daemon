package ai

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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
