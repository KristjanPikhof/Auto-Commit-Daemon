package state

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
)

func TestIntentPlannerWindowRoundTrip(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	win := IntentPlannerWindow{
		PlannedTS:        123,
		Provider:         sql.NullString{String: "openai-compat", Valid: true},
		Model:            sql.NullString{String: "gpt-test", Valid: true},
		BranchRef:        "refs/heads/main",
		BranchGeneration: 7,
		Source:           sql.NullString{String: "openai-compat", Valid: true},
		CommitFormat:     sql.NullString{String: "imperative", Valid: true},
		Forced:           true,
		ForcedReason:     sql.NullString{String: "defer_limit", Valid: true},
		OfferedSeqs:      []int64{10, 12},
		VisibleOriginalSeqs: []int64{
			10, 11, 12,
		},
		HiddenSeqs: []int64{11},
		SelectedGroups: []IntentPlannerWindowGroup{{
			SelectedSeqs:   []int64{10},
			OriginalSeqs:   []int64{10, 11},
			Subject:        "Update parser",
			GroupingReason: "related parser edits",
		}},
		DeferredSeqs: []int64{12},
		DeferredReasons: []IntentPlannerWindowDeferredReason{{
			Seq:    12,
			Reason: "separate docs change",
		}},
		Events: []IntentPlannerWindowEvent{
			{EventSeq: 10, Offered: true, Selected: true, GroupOrd: sql.NullInt64{Int64: 0, Valid: true}},
			{EventSeq: 11, Hidden: true, Selected: true, GroupOrd: sql.NullInt64{Int64: 0, Valid: true}},
			{EventSeq: 12, Offered: true, Deferred: true},
		},
	}
	id, err := AppendIntentPlannerWindow(ctx, d, win)
	if err != nil {
		t.Fatalf("AppendIntentPlannerWindow: %v", err)
	}
	if id == 0 {
		t.Fatalf("AppendIntentPlannerWindow id = 0")
	}

	recent, err := RecentIntentPlannerWindows(ctx, d, 1)
	if err != nil {
		t.Fatalf("RecentIntentPlannerWindows: %v", err)
	}
	if len(recent) != 1 {
		t.Fatalf("recent windows = %d, want 1", len(recent))
	}
	got := recent[0]
	if got.ID != id || got.Provider.String != "openai-compat" || got.Model.String != "gpt-test" ||
		!got.Forced || got.ForcedReason.String != "defer_limit" {
		t.Fatalf("window identity fields = %+v", got)
	}
	if !reflect.DeepEqual(got.OfferedSeqs, []int64{10, 12}) ||
		!reflect.DeepEqual(got.VisibleOriginalSeqs, []int64{10, 11, 12}) ||
		!reflect.DeepEqual(got.HiddenSeqs, []int64{11}) ||
		!reflect.DeepEqual(got.DeferredSeqs, []int64{12}) {
		t.Fatalf("window seq fields = %+v", got)
	}
	if len(got.SelectedGroups) != 1 ||
		!reflect.DeepEqual(got.SelectedGroups[0].OriginalSeqs, []int64{10, 11}) ||
		got.SelectedGroups[0].GroupingReason != "related parser edits" {
		t.Fatalf("selected groups = %+v", got.SelectedGroups)
	}
	if len(got.Events) != 3 || !got.Events[1].Hidden || !got.Events[1].Selected ||
		!got.Events[1].GroupOrd.Valid || got.Events[1].GroupOrd.Int64 != 0 {
		t.Fatalf("window events = %+v", got.Events)
	}

	forEvent, ok, err := IntentPlannerWindowForEvent(ctx, d, 11)
	if err != nil {
		t.Fatalf("IntentPlannerWindowForEvent: %v", err)
	}
	if !ok || forEvent.ID != id || len(forEvent.Events) != 3 {
		t.Fatalf("window for event = %+v ok=%v", forEvent, ok)
	}
}

func TestExperimentTerminalBaselineRevertIsAtomicAndIdempotent(t *testing.T) {
	for _, terminal := range []string{"completed", "expired", "cancelled", "provider_error"} {
		t.Run(terminal, func(t *testing.T) {
			d, _ := openTestDB(t)
			ctx := context.Background()
			baseline := testConfigRevision(t, d, "baseline", 1)
			candidate := testConfigRevision(t, d, "candidate", 2)
			request, ok, err := RequestConfigActivation(ctx, d, candidate.ID, sql.NullInt64{})
			if err != nil || !ok {
				t.Fatalf("request=%+v ok=%v err=%v", request, ok, err)
			}
			if ok, err := AcknowledgeConfigActivation(ctx, d, request.ID, candidate.ID); err != nil || !ok {
				t.Fatal(err)
			}
			if ok, err := ApplyConfigActivation(ctx, d, request.ID, candidate.ID); err != nil || !ok {
				t.Fatal(err)
			}
			expires := sql.NullFloat64{}
			if terminal == "expired" {
				expires = sql.NullFloat64{Float64: 50, Valid: true}
			}
			experiment, err := CreateConfigExperiment(ctx, d, ConfigExperimentInput{
				BaselineRevisionID: baseline.ID, CandidateRevisionID: candidate.ID,
				WindowBudget: 1, ExpiresTS: expires, FailurePolicy: "revert",
			})
			if err != nil {
				t.Fatal(err)
			}
			if terminal == "cancelled" {
				if ok, err := FinishConfigExperiment(ctx, d, experiment.ID, ExperimentCancelled, "operator cancelled"); err != nil || !ok {
					t.Fatal(err)
				}
			} else if terminal != "expired" {
				win := IntentPlannerWindow{
					PlannedTS: 40, BranchRef: "refs/heads/main", BranchGeneration: 1,
					ConfigRevisionID: sql.NullInt64{Int64: candidate.ID, Valid: true},
					ConfigProfile:    sql.NullString{String: "candidate", Valid: true},
					ExperimentID:     sql.NullInt64{Int64: experiment.ID, Valid: true},
					Outcome:          sql.NullString{String: "selected", Valid: true},
				}
				if terminal == "provider_error" {
					win.FallbackUsed = true
					win.ValidationFailure = sql.NullString{String: "sanitized provider failure", Valid: true}
				}
				if _, err := AppendIntentPlannerWindow(ctx, d, win); err != nil {
					t.Fatal(err)
				}
			}
			_, err = AppendDecision(ctx, d, DecisionRecord{Kind: DecisionKindCommitted, CommitOID: sql.NullString{String: "deadbeef", Valid: true}})
			if err != nil {
				t.Fatal(err)
			}
			now := float64(41)
			if terminal == "expired" {
				now = 51
			}
			revert, revertRequest, queued, err := QueueExperimentBaselineRevert(ctx, d, experiment.ID, now)
			if err != nil || !queued {
				t.Fatalf("revert=%+v request=%+v queued=%v err=%v", revert, revertRequest, queued, err)
			}
			if revert.SnapshotHash != baseline.SnapshotHash || revert.ID == baseline.ID || revertRequest.RevisionID != revert.ID {
				t.Fatalf("revert=%+v request=%+v", revert, revertRequest)
			}
			projection, _ := RuntimeConfigActivationState(ctx, d)
			if projection.DesiredRevisionID.Int64 != revert.ID || projection.AppliedRevisionID.Int64 != candidate.ID {
				t.Fatalf("projection=%+v", projection)
			}
			if _, _, queued, err := QueueExperimentBaselineRevert(ctx, d, experiment.ID, now+1); err != nil || queued {
				t.Fatalf("second queued=%v err=%v", queued, err)
			}
			var revisionCount int
			if err := d.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM config_revisions`).Scan(&revisionCount); err != nil || revisionCount != 3 {
				t.Fatalf("revision count=%d err=%v", revisionCount, err)
			}
			decisions, err := RecentDecisions(ctx, d, 1)
			if err != nil || len(decisions) != 1 || decisions[0].CommitOID.String != "deadbeef" {
				t.Fatalf("commit decision changed: %+v err=%v", decisions, err)
			}
			gotExperiment, _ := ConfigExperimentByID(ctx, d, experiment.ID)
			if terminal == "provider_error" && gotExperiment.Status != ExperimentFailed {
				t.Fatalf("provider failure status=%s", gotExperiment.Status)
			}
			if terminal == "expired" && (gotExperiment.Status != ExperimentExpired || !strings.Contains(gotExperiment.TerminalReason.String, "expiry")) {
				t.Fatalf("expiry=%+v", gotExperiment)
			}
		})
	}
}
