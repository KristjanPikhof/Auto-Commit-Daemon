package ai

import (
	"strings"
	"testing"
	"time"
)

func TestEvaluateIntentPlanMessageQuality(t *testing.T) {
	now := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	baseReq := func(captures ...OfferedCapture) IntentPlanRequest {
		t.Helper()
		req, err := NewIntentPlanRequest(IntentPlanRequestOptions{OfferedCaptures: captures})
		if err != nil {
			t.Fatalf("NewIntentPlanRequest: %v", err)
		}
		return req
	}
	capture := func(seq int64, path, op string) OfferedCapture {
		return OfferedCapture{
			Seq:       seq,
			Path:      path,
			Op:        op,
			Timestamp: now,
			Fidelity:  "full",
		}
	}

	tests := []struct {
		name              string
		req               IntentPlanRequest
		plan              IntentPlan
		wantAction        MessageQualityAction
		wantReasons       []MessageQualityReasonCode
		wantBodyRequired  bool
		wantSanitizedBody string
	}{
		{
			name: "small single file semantic subject accepted without body",
			req:  baseReq(capture(1, "internal/ai/message_quality_test.go", "modify")),
			plan: IntentPlan{
				SelectedSeqs:   []int64{1},
				Subject:        "Add message quality tests",
				GroupingReason: "single test change",
			},
			wantAction: MessageQualityClean,
		},
		{
			name: "sanitized but otherwise semantic subject accepted",
			req:  baseReq(capture(1, "internal/ai/prompt.go", "modify")),
			plan: IntentPlan{
				SelectedSeqs:   []int64{1},
				Subject:        "- Refine prompt validation.",
				GroupingReason: "single prompt change",
			},
			wantAction:        MessageQualitySanitizeAccept,
			wantReasons:       []MessageQualityReasonCode{MessageQualityReasonSanitizedSubject},
			wantSanitizedBody: "",
		},
		{
			name: "generic update parsed subject requests rewrite",
			req:  baseReq(capture(1, "src/parser.ts", "modify")),
			plan: IntentPlan{
				SelectedSeqs:   []int64{1},
				Subject:        "Update parsed",
				GroupingReason: "single parser change",
			},
			wantAction:  MessageQualityRewrite,
			wantReasons: []MessageQualityReasonCode{MessageQualityReasonTokenOnly},
		},
		{
			name: "filename only subject requests rewrite",
			req:  baseReq(capture(1, "src/effort.ts", "modify")),
			plan: IntentPlan{
				SelectedSeqs:   []int64{1},
				Subject:        "Update effort.ts",
				GroupingReason: "single effort change",
			},
			wantAction:  MessageQualityRewrite,
			wantReasons: []MessageQualityReasonCode{MessageQualityReasonFilenameOnly},
		},
		{
			name: "generic file subject requests rewrite",
			req:  baseReq(capture(1, "internal/ai/provider.go", "modify")),
			plan: IntentPlan{
				SelectedSeqs:   []int64{1},
				Subject:        "Update file",
				GroupingReason: "single provider change",
			},
			wantAction:  MessageQualityRewrite,
			wantReasons: []MessageQualityReasonCode{MessageQualityReasonGenericSubject},
		},
		{
			name: "multi file missing body requests rewrite",
			req: baseReq(
				capture(1, "internal/ai/message_quality.go", "modify"),
				capture(2, "internal/ai/message_quality_test.go", "modify"),
			),
			plan: IntentPlan{
				SelectedSeqs:   []int64{1, 2},
				Subject:        "Add message quality classifier",
				GroupingReason: "classifier and tests share one policy",
			},
			wantAction:       MessageQualityRewrite,
			wantReasons:      []MessageQualityReasonCode{MessageQualityReasonBodyRequired, MessageQualityReasonMixedChangeClasses},
			wantBodyRequired: true,
		},
		{
			name: "high impact cli path requires body",
			req:  baseReq(capture(1, "internal/cli/status.go", "modify")),
			plan: IntentPlan{
				SelectedSeqs:   []int64{1},
				Subject:        "Surface rewrite diagnostics",
				GroupingReason: "single CLI surface change",
			},
			wantAction:       MessageQualityRewrite,
			wantReasons:      []MessageQualityReasonCode{MessageQualityReasonBodyRequired, MessageQualityReasonHighImpactChange},
			wantBodyRequired: true,
		},
		{
			name: "high impact change with bullets is clean",
			req:  baseReq(capture(1, "internal/cli/status.go", "modify")),
			plan: IntentPlan{
				SelectedSeqs:   []int64{1},
				Subject:        "Surface rewrite diagnostics",
				Body:           "- Show latest rewrite reason in status output",
				GroupingReason: "single CLI surface change",
			},
			wantAction:       MessageQualityClean,
			wantReasons:      []MessageQualityReasonCode{MessageQualityReasonHighImpactChange},
			wantBodyRequired: true,
		},
		{
			name: "malformed body requests rewrite",
			req:  baseReq(capture(1, "internal/ai/message_quality.go", "modify")),
			plan: IntentPlan{
				SelectedSeqs:   []int64{1},
				Subject:        "Add message quality classifier",
				Body:           "explains the change without a bullet",
				GroupingReason: "single classifier change",
			},
			wantAction:  MessageQualityRewrite,
			wantReasons: []MessageQualityReasonCode{MessageQualityReasonMalformedBody},
		},
		{
			name: "unknown selected capture falls back",
			req:  baseReq(capture(1, "internal/ai/message_quality.go", "modify")),
			plan: IntentPlan{
				SelectedSeqs:   []int64{2},
				Subject:        "Add message quality classifier",
				GroupingReason: "invalid selected seq",
			},
			wantAction:  MessageQualityFallback,
			wantReasons: []MessageQualityReasonCode{MessageQualityReasonUnknownSelection},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := EvaluateIntentPlanMessageQuality(tc.req, tc.plan)
			if got.Action != tc.wantAction {
				t.Fatalf("action=%s want %s; reasons=%+v", got.Action, tc.wantAction, got.Reasons)
			}
			if got.BodyRequired != tc.wantBodyRequired {
				t.Fatalf("bodyRequired=%v want %v", got.BodyRequired, tc.wantBodyRequired)
			}
			for _, code := range tc.wantReasons {
				if !got.HasReason(code) {
					t.Fatalf("missing reason %s in %+v", code, got.Reasons)
				}
			}
			if tc.wantSanitizedBody != "" && got.SanitizedBody != tc.wantSanitizedBody {
				t.Fatalf("sanitizedBody=%q want %q", got.SanitizedBody, tc.wantSanitizedBody)
			}
		})
	}
}

func TestEvaluateIntentPlanMessageQuality_DiffSizeRequiresBody(t *testing.T) {
	now := time.Date(2026, 5, 19, 12, 0, 0, 0, time.UTC)
	req, err := NewIntentPlanRequest(IntentPlanRequestOptions{
		OfferedCaptures: []OfferedCapture{{
			Seq:          1,
			Path:         "internal/ai/provider.go",
			Op:           "modify",
			Timestamp:    now,
			Fidelity:     "full",
			CapturedDiff: strings.Repeat("+context\n", 200),
		}},
		IncludeCapturedDiffs: true,
	})
	if err != nil {
		t.Fatalf("NewIntentPlanRequest: %v", err)
	}

	got := EvaluateIntentPlanMessageQuality(req, IntentPlan{
		SelectedSeqs:   []int64{1},
		Subject:        "Refine planner provider retry",
		GroupingReason: "large provider change",
	})
	if got.Action != MessageQualityRewrite {
		t.Fatalf("action=%s want %s; reasons=%+v", got.Action, MessageQualityRewrite, got.Reasons)
	}
	if !got.BodyRequired {
		t.Fatalf("bodyRequired=false want true")
	}
	if !got.HasReason(MessageQualityReasonBodyRequired) {
		t.Fatalf("missing body_required reason: %+v", got.Reasons)
	}
}
