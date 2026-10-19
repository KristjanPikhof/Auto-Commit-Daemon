package ai

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

func TestFinishHistoryAcceptsCompleteModerateRange(t *testing.T) {
	evidence := finish.Evidence{Base: finish.Tree{}, Target: finish.Tree{}}
	options := finishTestOptions()
	options.DiffEgress = true
	var ids []string
	for i := 0; i < 6; i++ {
		path := fmt.Sprintf("file-%d.go", i)
		evidence.Target[path] = finishTestEntry("b")
		ids = append(ids, path)
		size := 44 << 10
		if i == 0 {
			size = 96 << 10
		}
		options.Diffs = append(options.Diffs, FinishDiff{ChangeID: path, Text: strings.Repeat("x", size)})
	}
	evidence.Changes = finish.NetChanges(evidence.Base, evidence.Target, "author-a")
	commits := []HistoryMessageCommit{{ID: "original", Author: "author-a", TreeOID: strings.Repeat("b", 40), OriginalMessage: "Update implementation", ChangeIDs: ids}}
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if len(request.Diffs) != len(options.Diffs) {
			t.Fatal("history input omitted a recorded transition")
		}
		for i, diff := range request.Diffs {
			if diff != options.Diffs[i] {
				t.Fatal("history input truncated or changed a recorded transition")
			}
		}
		return finishTestOutput(finish.Goal{Purpose: "Complete implementation", Boundary: "Keep the original authored scope", Message: "Complete implementation", ChangeIDs: []string{"original"}}), nil
	}}
	_, result, err := PlanHistoryMessages(t.Context(), provider, evidence, commits, options)
	if err != nil || result.Calls != 1 {
		t.Fatalf("complete history range: %+v %v", result, err)
	}
}

func TestFinishHistoryRewriteMessageTransportRedactsAndStopsCancelledBudget(t *testing.T) {
	evidence := finishTestEvidence()
	commits := []HistoryMessageCommit{{ID: "original", Author: "author-a", TreeOID: strings.Repeat("a", 40), OriginalMessage: "Bearer sk-secret-token", ChangeIDs: []string{evidence.Changes[0].ID}}}
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		if strings.Contains(request.HistoryCommits[0].OriginalMessage, "sk-secret-token") {
			t.Fatal("secret sent")
		}
		return finishTestOutput(finish.Goal{Purpose: "Preserve authored settings", Boundary: "Keep the original authored scope", Message: "Preserve authored settings", ChangeIDs: []string{"original"}}), nil
	}}
	_, result, err := PlanHistoryMessages(t.Context(), provider, evidence, commits, finishTestOptions())
	if err != nil || !result.Disclosure.Redacted || result.Calls != 1 {
		t.Fatalf("redaction %+v %v", result, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	options := finishTestOptions()
	options.BeforeCall = func(context.Context) error { cancel(); return nil }
	_, _, err = PlanHistoryMessages(ctx, provider, evidence, commits, options)
	if err == nil || len(provider.requests) != 1 {
		t.Fatal("cancelled budget performed exchange")
	}
}
