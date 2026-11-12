package ai

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

func TestFinishWrapsGeneratedReasonWithoutCorrection(t *testing.T) {
	for _, history := range []bool{false, true} {
		name := "finish"
		if history {
			name = "history messages"
		}
		t.Run(name, func(t *testing.T) {
			evidence := finishTestEvidence()
			goal := finishTestGoal(evidence)
			goal.Reason = "JPEG conversion blocked capture, and old results could replace a newer room. Moving conversion to a utility queue and checking background versions keeps capture responsive and rejects stale images."
			goal.Message = "Generate thumbnails off the capture queue\n\n- " + goal.Reason
			if history {
				goal.ChangeIDs = []string{"original"}
			}
			provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
				if request.Correction != nil {
					t.Fatal("line wrapping consumed the semantic correction")
				}
				return json.Marshal(finishPlanOutput{Goals: []finish.Goal{goal}})
			}}
			var published finish.Goal
			if history {
				commits := []HistoryMessageCommit{{ID: "original", ChangeIDs: []string{evidence.Changes[0].ID}}}
				goals, result, err := PlanHistoryMessages(t.Context(), provider, evidence, commits, finishTestOptions(evidence))
				if err != nil || result.Calls != 1 {
					t.Fatalf("wrap history reason: %+v, %v", result, err)
				}
				published = goals[0]
			} else {
				result, err := PlanFinish(t.Context(), provider, evidence, finishTestOptions(evidence))
				if err != nil || result.Calls != 1 {
					t.Fatalf("wrap finish reason: %+v, %v", result, err)
				}
				published = result.Plan.Steps[0].Goal
			}
			if err := finish.ValidateMessage(finish.StyleImperative, published.Message); err != nil {
				t.Fatal(err)
			}
			if published.Reason != goal.Reason || validateGeneratedReason(published) != nil || !strings.Contains(published.Message, "\n  ") {
				t.Fatalf("reason wording lost: %+v", published)
			}
		})
	}
}

func TestFinishExactMessageIsNotRewrapped(t *testing.T) {
	evidence := finishTestEvidence()
	options := finishTestOptions(evidence)
	options.Message = "Preserve explicit settings\n\n- " + strings.Repeat("Keep authored settings. ", 5)
	provider := &finishFakeTransport{}
	if _, err := PlanFinish(t.Context(), provider, evidence, options); err == nil || len(provider.requests) != 0 {
		t.Fatalf("exact invalid message was repaired or sent: %v", err)
	}
}

func TestFinishReflowKeepsInvalidMessagesInvalid(t *testing.T) {
	for _, message := range []string{
		"Fix " + strings.Repeat("long ", 14) + "subject\n\n- A concrete reason belongs here.",
		"Fix startup errors\n\n- " + strings.Repeat("x", 73),
		"Fix startup errors\n\n- Keep\tunsafe text unchanged.",
		"Fix startup errors\n\n  A continuation without a bullet.",
		"Fix startup errors\n\n- First reason.\n\n- Second reason.",
		"Fix startup errors\n\n- First reason.\n  - A malformed nested bullet.",
		"Fix startup errors\n\nA paragraph without a bullet.",
	} {
		if got := reflowGeneratedMessage(finish.StyleImperative, message); finish.ValidateMessage(finish.StyleImperative, got) == nil {
			t.Errorf("repaired invalid structure, subject or words: %q", got)
		}
	}
}
