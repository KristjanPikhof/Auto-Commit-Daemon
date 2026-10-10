package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentSemanticRetryCadenceSurvivesRestartAndNewEvidence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	req, input, planner := semanticRetryRequest(t)
	var previous IntentSemanticRetrySnapshot
	for i, delay := range []time.Duration{5 * time.Minute, 10 * time.Minute, time.Hour, time.Hour} {
		if i > 0 {
			input.Now = secondsTime(previous.RetryAtTS)
		}
		_, _, _, _, attention, _, run, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
		if err != nil || attention || run.ProgressState.String != "waiting_semantic_retry" || planner.calls != i+1 {
			t.Fatalf("review %d did not run once: run=%+v calls=%d err=%v", i+1, run, planner.calls, err)
		}
		retry, found, err := loadIntentSemanticRetry(ctx, db)
		if err != nil || !found || retry.ReviewCount != min(i+1, 3) || retry.ScheduledAtTS != intentPlannerHealthTimestamp(input.Now) || retry.RetryAtTS != intentPlannerHealthTimestamp(input.Now.Add(delay)) {
			t.Fatalf("review %d cadence=%+v found=%t err=%v", i+1, retry, found, err)
		}
		path := db.Path()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = state.Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		input.Now = secondsTime(retry.RetryAtTS).Add(-time.Second)
		for poll := 0; poll < 3; poll++ {
			_, _, _, _, _, _, _, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
			var wait *IntentSemanticRetryWaitError
			if !errors.As(err, &wait) || !wait.RetryAt.Equal(secondsTime(retry.RetryAtTS)) || planner.calls != i+1 {
				t.Fatalf("unchanged review escaped cooldown: calls=%d wait=%+v err=%v", planner.calls, wait, err)
			}
			if saved, found, err := loadIntentSemanticRetry(ctx, db); err != nil || !found || saved != retry {
				t.Fatalf("poll moved deadline/count: before=%+v after=%+v err=%v", retry, saved, err)
			}
		}
		previous = retry
	}
	// New captured evidence starts its own five-minute sequence, even while
	// the old unresolved goal is on its hourly cooldown.
	req.OfferedCaptures = []ai.OfferedCapture{{Seq: 2, Path: "CameraController.swift", Op: "modify"}}
	if _, _, _, _, _, _, _, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input); err != nil {
		t.Fatal(err)
	}
	fresh, found, err := loadIntentSemanticRetry(ctx, db)
	if err != nil || !found || planner.calls != 5 || fresh.ReviewCount != 1 || fresh.RetryAtTS != intentPlannerHealthTimestamp(input.Now.Add(5*time.Minute)) || fresh.EvidenceFingerprint == previous.EvidenceFingerprint {
		t.Fatalf("new evidence inherited old cooldown: fresh=%+v calls=%d err=%v", fresh, planner.calls, err)
	}
	if old, found, err := loadIntentSemanticRetryForEvidence(ctx, db, previous.EvidenceFingerprint); err != nil || !found || old != previous {
		t.Fatalf("new evidence changed old deadline: old=%+v err=%v", old, err)
	}
}

func TestIntentSemanticRetryLegacyDeadlineShortensOnceInWorker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	req, input, planner := semanticRetryRequest(t)
	_, _, _, _, _, _, _, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
	if err != nil {
		t.Fatal(err)
	}
	legacy, _, err := loadIntentSemanticRetry(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	originalSchedule := input.Now.Add(-2 * time.Minute)
	legacy.ReviewCount, legacy.ScheduledAtTS = 0, 0
	legacy.RetryAtTS = intentPlannerHealthTimestamp(originalSchedule.Add(time.Hour))
	if err := saveIntentSemanticRetry(ctx, db, legacy); err != nil {
		t.Fatal(err)
	}
	raw, _, err := state.MetaGet(ctx, db, MetaKeyIntentSemanticRetry)
	decoded, decodeErr := DecodeIntentSemanticRetrySnapshot(raw)
	if err != nil || decodeErr != nil || decoded != legacy {
		t.Fatalf("read-only decoding rewrote old deadline: %+v err=%v/%v", decoded, err, decodeErr)
	}
	var migrated IntentSemanticRetrySnapshot
	for i := 0; i < 3; i++ {
		_, _, _, _, _, _, _, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input)
		var wait *IntentSemanticRetryWaitError
		if !errors.As(err, &wait) || planner.calls != 1 || !wait.RetryAt.Equal(originalSchedule.Add(5*time.Minute)) {
			t.Fatalf("legacy shortening reset its clock: calls=%d wait=%+v err=%v", planner.calls, wait, err)
		}
		retry, found, err := loadIntentSemanticRetry(ctx, db)
		if err != nil || !found || retry.ReviewCount != 1 || retry.ScheduledAtTS != intentPlannerHealthTimestamp(originalSchedule) || i > 0 && retry != migrated {
			t.Fatalf("worker shortening changed again: retry=%+v before=%+v err=%v", retry, migrated, err)
		}
		migrated = retry
		input.Now = input.Now.Add(30 * time.Second)
	}
	input.Now = secondsTime(migrated.RetryAtTS)
	if _, _, _, _, _, _, _, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 0, input.Preset, nil, db, input); err != nil || planner.calls != 2 {
		t.Fatalf("legacy due review did not run: calls=%d err=%v", planner.calls, err)
	}
	next, _, err := loadIntentSemanticRetry(ctx, db)
	if err != nil || next.ReviewCount != 2 || next.RetryAtTS != intentPlannerHealthTimestamp(input.Now.Add(10*time.Minute)) {
		t.Fatalf("legacy review did not progress to ten minutes: %+v err=%v", next, err)
	}
}

func TestIntentSemanticRetryLegacyDueSelectionRemainsReadOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	held := appendIntentCandidateCapture(t, db, "SpeechEngine.swift", "modify", "before", "after")
	fresh := appendIntentCandidateCapture(t, db, "release.md", "create", "", "release")
	saveWaitingIntentCandidate(t, db, "held-recognition", float64(now.Unix()), held)
	fingerprint := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	run, err := state.EnsureIntentPlanRun(ctx, db, state.IntentPlanRun{Fingerprint: fingerprint, BranchRef: held.Event.BranchRef, BranchGeneration: held.Event.BranchGeneration, AttemptLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	run.Completed = true
	run.UnresolvedSeqs = []int64{held.Event.Seq}
	run.ProgressState = sql.NullString{String: "waiting_semantic_retry", Valid: true}
	run.ResolutionMode = run.ProgressState
	if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
		t.Fatal(err)
	}
	legacy := IntentSemanticRetrySnapshot{Version: 1, BranchRef: run.BranchRef, BranchGeneration: run.BranchGeneration, EvidenceFingerprint: fingerprint, PlanFingerprint: fingerprint, RetryAtTS: intentPlannerHealthTimestamp(now.Add(50 * time.Minute))}
	if err := saveIntentSemanticRetry(ctx, db, legacy); err != nil {
		t.Fatal(err)
	}
	pending, err := state.PendingEvents(ctx, db, 0)
	if err != nil {
		t.Fatal(err)
	}
	window, err := dueIntentSemanticReviewWindow(ctx, db, pending, 1, now)
	if err != nil || len(window) != 1 || window[0].Seq != held.Event.Seq || window[0].Seq == fresh.Event.Seq {
		t.Fatalf("old hour deadline starved due review: window=%+v err=%v", window, err)
	}
	if record, found, err := loadIntentSemanticRetry(ctx, db); err != nil || !found || record != legacy {
		t.Fatalf("read-only selection rewrote derived state: %+v err=%v", record, err)
	}
}

func TestIntentSemanticRetryDecodeBoundsCadenceFields(t *testing.T) {
	t.Parallel()
	base := IntentSemanticRetrySnapshot{Version: 1, BranchRef: "refs/heads/main", EvidenceFingerprint: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", PlanFingerprint: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", RetryAtTS: 7200}
	for _, tc := range []struct {
		name      string
		count     int
		scheduled float64
		valid     bool
	}{{"legacy", 0, 0, true}, {"first", 1, 6900, true}, {"hourly", 3, 3600, true}, {"negative_count", -1, 0, false}, {"unbounded_count", 4, 3600, false}, {"missing_clock", 1, 0, false}, {"negative_clock", 1, -1, false}, {"future_clock", 1, 7201, false}, {"partial_legacy", 0, 1, false}} {
		t.Run(tc.name, func(t *testing.T) {
			record := base
			record.ReviewCount, record.ScheduledAtTS = tc.count, tc.scheduled
			raw, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			_, err = DecodeIntentSemanticRetrySnapshot(string(raw))
			if (err == nil) != tc.valid {
				t.Fatalf("cadence record validity=%t want=%t err=%v", err == nil, tc.valid, err)
			}
		})
	}
}
