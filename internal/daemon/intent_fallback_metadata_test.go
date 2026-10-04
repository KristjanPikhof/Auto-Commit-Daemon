package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

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
			if len(out.Candidates) != 1 || !reflect.DeepEqual(out.Candidates[0].SelectedSeqs, []int64{1}) || out.Candidates[0].Body == "" {
				t.Fatalf("local fallback changed membership or omitted evidence: %+v", out)
			}
			report := ai.EvaluateIntentPlanMessageQuality(ai.LegacyIntentPlanRequest(req), ai.IntentPlan{
				SelectedSeqs: out.Candidates[0].SelectedSeqs, Subject: out.Candidates[0].Subject, Body: out.Candidates[0].Body,
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
	requests []ai.IntentPlanRequestV2
}

func (*recordingUnavailableMetadataPlanner) Name() string { return "metadata-review" }

func (*recordingUnavailableMetadataPlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("native planning must not use the legacy protocol")
}

func (p *recordingUnavailableMetadataPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.requests = append(p.requests, req)
	return ai.IntentPlanV2{}, &IntentPlannerTransportFailure{Err: errors.New("provider unavailable")}
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
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{IgnoreChecker: f.ig, SensitiveMatcher: f.matcher}); err != nil {
		t.Fatal(err)
	}
	planner := &recordingUnavailableMetadataPlanner{}
	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetFast,
		IntentIncludeDiffs: true, IntentBypassBatchWait: true, IntentWindow: 10,
	})
	if err != nil || sum.Published != len(contents) || len(planner.requests) != 1 {
		t.Fatalf("offline publication=%+v calls=%d err=%v", sum, len(planner.requests), err)
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
	if err != nil || len(pending) != 0 {
		t.Fatalf("offline publication left pending captures: %+v err=%v", pending, err)
	}
}
