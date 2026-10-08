package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentLocalFallbackRecoversRejectedSymbolSubjects(t *testing.T) {
	for _, tc := range []struct {
		name, path, diff string
	}{
		{"Python snake case", "settings.py", "+def save_settings(value):\n"},
		{"Go lower camel case", "settings.go", "+func saveSettings() {}\n"},
		{"long semantic stem", strings.Repeat("settings", 12) + ".py", "+def save_settings(value):\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
				OfferedCaptures: []ai.OfferedCapture{{Seq: 1, Path: tc.path, Op: "modify", CapturedDiff: tc.diff}}}
			plan := deterministicIntentCandidatePlan(req, true, false)
			out, err := applyIntentFallbackMessageQuality(req, plan)
			if err != nil {
				t.Fatalf("local fallback cannot recover an ordinary edit: %v", err)
			}
			if len(out.Candidates) != 1 || !reflect.DeepEqual(out.Candidates[0].SelectedSeqs, []int64{1}) ||
				out.Candidates[0].Readiness != ai.IntentCandidateWait || out.Candidates[0].Subject != "" ||
				len(out.Candidates[0].MissingCompanions) == 0 {
				t.Fatalf("unknown meaning was not retained as a protected goal: %+v", out)
			}
			if err := ai.ValidateIntentPlanV2(req, out); err != nil {
				t.Fatalf("waiting plan lost capture ownership: %v", err)
			}
			// Later captured implementation provides the outcome that the bare
			// declaration could not explain. The original capture stays owned.
			followup := "+def save_settings(value):\n+    with open(\"settings.json\", \"w\") as output:\n+        json.dump(value, output)\n"
			if strings.HasSuffix(tc.path, ".go") {
				followup = "+func saveSettings(value any) error {\n+    data, err := json.Marshal(value)\n+    if err != nil { return err }\n+    return os.WriteFile(\"settings.json\", data, 0600)\n+}\n"
			}
			req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: 2, Path: tc.path, Op: "modify", CapturedDiff: followup})
			req.Dependencies = []ai.IntentCaptureDependency{{FromSeq: 1, ToSeq: 2, Strength: ai.IntentDependencyHard,
				Kind: "same_path_order", EvidenceHash: "recorded follow-up on the same settings path"}}
			recovered := cloneIntentPlanV2(out)
			goal := &recovered.Candidates[0]
			goal.SelectedSeqs = []int64{1, 2}
			goal.Purpose = "persist settings for the next application run"
			goal.Readiness, goal.MissingCompanions = ai.IntentCandidateReady, nil
			goal.Subject = "Preserve settings across restarts"
			goal.Body = "- Store the supplied settings in a JSON file for the next run"
			goal.GroupingReason = "the follow-up completes the original settings persistence edit"
			if err := ValidateIntentGoalPlan(req, recovered); err != nil {
				t.Fatalf("meaningful later evidence could not recover the owned goal: %v", err)
			}
			report := ai.EvaluateIntentPlanMessageQuality(ai.LegacyIntentPlanRequest(req), ai.IntentPlan{
				SelectedSeqs: goal.SelectedSeqs, Subject: goal.Subject, Body: goal.Body,
			})
			if report.Action != ai.MessageQualityClean {
				t.Fatalf("local message failed quality: %+v", report)
			}
		})
	}
}

func TestIntentLocalFallbackPreservesValidLockedAssignments(t *testing.T) {
	req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		OfferedCaptures: []ai.OfferedCapture{{Seq: 1, Path: "settings.py", Op: "modify", CapturedDiff: "+def save_settings(value):\n"}}}
	plan := deterministicIntentCandidatePlan(req, true, false)
	plan.Candidates[0].CandidateID = "locked-settings"
	plan.Candidates[0].Purpose = "keep saved settings available"
	plan.Candidates[0].Subject = "Keep saved settings available"
	plan.Candidates[0].Body = "- Preserve settings after the application restarts"
	out, err := applyIntentFallbackMessageQuality(req, plan)
	if err != nil || !reflect.DeepEqual(out, plan) {
		t.Fatalf("valid locked assignment changed: before=%+v after=%+v err=%v", plan, out, err)
	}
}

type recordingUnavailableMetadataPlanner struct {
	requests    []ai.IntentPlanRequestV2
	unavailable bool
}

func (*recordingUnavailableMetadataPlanner) Name() string { return "metadata-review" }

func (*recordingUnavailableMetadataPlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("native planning must not use the legacy protocol")
}

func (p *recordingUnavailableMetadataPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.requests = append(p.requests, req)
	if p.unavailable {
		return ai.IntentPlanV2{}, &IntentPlannerTransportFailure{Err: errors.New("provider unavailable")}
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for _, capture := range req.OfferedCaptures {
		purpose, subject := "document capture notes", "Document capture notes"
		switch capture.Path {
		case "large.md":
			purpose, subject = "explain bounded capture details", "Document bounded capture details"
		case "redacted.md":
			purpose, subject = "explain redacted capture content", "Document redacted capture content"
		}
		plan.Candidates = append(plan.Candidates, ai.IntentCandidateAssignment{
			CandidateID: capture.Path, SelectedSeqs: []int64{capture.Seq},
			Purpose: purpose, Readiness: ai.IntentCandidateReady, Subject: subject,
			Body:           "- Explain the captured documentation behavior",
			GroupingReason: "the independent documentation change is complete",
		})
	}
	return plan, nil
}

func TestReplayIntentMetadataPreservesPerCaptureTruncation(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	contents := map[string]string{
		"large.md":    strings.Repeat("Changed documentation text.\n", ai.IntentStageDiffCap),
		"redacted.md": "token = \"" + strings.Repeat("a", ai.IntentStageDiffCap*2) + "\"\nRemaining documentation.\n",
		"small.md":    "Small documentation update.\n",
	}
	for path, content := range contents {
		if err := os.WriteFile(filepath.Join(f.dir, path), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	protected := capturePublicationFiles(t, f)
	if !protected.Protected {
		t.Fatalf("metadata captures were not protected: %+v", protected)
	}
	planner := &recordingUnavailableMetadataPlanner{unavailable: true}
	now := time.Now().UTC().Truncate(time.Second)
	health := NewIntentPlannerHealth(ctx, f.db, IntentPlannerHealthOptions{
		Provider: IntentPlannerProviderIdentity{Provider: planner.Name()}, Now: func() time.Time { return now },
	})
	opts := ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetFast, IntentHealth: health,
		IntentIncludeDiffs: true, IntentBypassBatchWait: true, IntentWindow: 10,
		IntentVerificationMode: "structural",
	}
	sum, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || sum.Published != 0 || sum.Disposition != ReplayDispositionTransientWait || len(planner.requests) != 1 {
		t.Fatalf("offline capture wait=%+v calls=%d err=%v", sum, len(planner.requests), err)
	}
	for _, capture := range planner.requests[0].OfferedCaptures {
		wantReason := ""
		switch capture.Path {
		case "large.md":
			wantReason = "truncated"
			if !strings.Contains(capture.CapturedDiff, "<truncated>") || len(capture.CapturedDiff) > ai.IntentStageDiffCap {
				t.Fatalf("large diff was not bounded: %d bytes", len(capture.CapturedDiff))
			}
		case "redacted.md":
			wantReason = "truncated"
			if len(capture.CapturedDiff) >= ai.IntentStageDiffCap || !strings.Contains(capture.CapturedDiff, "[REDACTED_SECRET]") || strings.Contains(capture.CapturedDiff, strings.Repeat("a", 32)) {
				t.Fatal("redaction must remove the generated secret and shorten the clipped source")
			}
		}
		if capture.FileMetadata == nil || capture.FileMetadata.Kind != "text" || capture.FileMetadata.DiffOmittedReason != wantReason {
			t.Fatalf("%s metadata=%+v want reason=%q", capture.Path, capture.FileMetadata, wantReason)
		}
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != len(contents) {
		t.Fatalf("outage lost protected captures: %+v err=%v", pending, err)
	}
	if _, err := Replay(ctx, f.dir, f.db, f.cctx, opts); err != nil || len(planner.requests) != 1 {
		t.Fatalf("cooldown consumed another call: calls=%d err=%v", len(planner.requests), err)
	}
	planner.unavailable = false
	now = now.Add(5*time.Minute + time.Second)
	online, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || online.Published != len(contents) || len(planner.requests) != 2 {
		t.Fatalf("reconnection did not publish protected captures: %+v calls=%d err=%v", online, len(planner.requests), err)
	}
	if !reflect.DeepEqual(planner.requests[0].OfferedCaptures, planner.requests[1].OfferedCaptures) {
		t.Fatal("reconnection lost per-capture clipping or redaction metadata")
	}
	pending, err = state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 0 {
		t.Fatalf("online publication left captures pending: %+v err=%v", pending, err)
	}
}
