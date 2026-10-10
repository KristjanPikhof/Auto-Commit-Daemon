package ai

import "testing"

func TestIntentMessageQualityRejectsCapturedTestAndHeadingLabels(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ path, label, diff string }{
		{"CONTRIBUTING.md", "Local setup", "@@ -1,2 +1,2 @@\n # Local setup\n-Pi0.85\n+Pi1.1\n"},
		{"docs/architecture.md", "Key decisions", "@@ -1,2 +1,2 @@\n # Key decisions\n-old\n+new\n"},
		{"docs/operations.md", "Pi 1.1.0 upgrade", "+# Pi 1.1.0 upgrade\n+Require a newer Pi runtime\n"},
		{"tests/control-plane/extension-wiring.test.ts", "event", " const event = { type: 'settled' };\n+expect(event.type).toBe('settled');\n"},
		{"tests/control-plane/toasts.test.ts", "message", " const message = { type: 'info' };\n+expect(message.type).toBe('info');\n"},
		{"tests/package-publish.test.ts", "manifest", " const manifest = { version: '1.1' };\n+expect(manifest.version).toBe('1.1');\n"},
		{"tests/safety/launch-policy.test.ts", "profile", " const profile = { tools: [] };\n+expect(profile.tools).toHaveLength(0);\n"},
		{"tests/control-plane/team-manager.test.ts", "manager", " const manager = createManager();\n+expect(manager).toBeDefined();\n"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			capture := OfferedCapture{Seq: 1, Path: tc.path, Op: "modify", CapturedDiff: tc.diff}
			for _, subject := range []string{"Update " + tc.label, "fix: Update " + tc.label} {
				report := EvaluateIntentPlanMessageQuality(IntentPlanRequest{OfferedCaptures: []OfferedCapture{capture}},
					IntentPlan{SelectedSeqs: []int64{1}, Subject: subject, Body: "- Preserve the captured modify of " + tc.path})
				if report.Action != MessageQualityRewrite || !report.HasReason(MessageQualityReasonTokenOnly) {
					t.Fatalf("captured label became a goal: subject=%q report=%+v", subject, report)
				}
			}
		})
	}
}

func TestIntentMessageQualityKeepsNamedNewGuideAndRealOutcome(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ op, subject string }{
		{"create", "Add Protected publication retries"},
		{"modify", "Require Pi1.1 for local development"},
	} {
		report := EvaluateIntentPlanMessageQuality(IntentPlanRequest{OfferedCaptures: []OfferedCapture{{
			Seq: 1, Path: "docs/setup.md", Op: tc.op, CapturedDiff: "+# Protected publication retries\n+Keep work protected across provider outages\n",
		}}}, IntentPlan{SelectedSeqs: []int64{1}, Subject: tc.subject, Body: "- Explain the supported runtime and protection behavior"})
		if report.Action != MessageQualityClean {
			t.Fatalf("meaningful goal was rejected: %+v", report)
		}
	}
}
