package state

import (
	"context"
	"database/sql"
	"reflect"
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
