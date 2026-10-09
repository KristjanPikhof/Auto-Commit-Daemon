package daemon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRecoverExpandedForcedIntentWindowKeepsFrozenTarget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		mutate    string
		reason    string
		recovered bool
	}{
		{name: "protected partial publication", recovered: true},
		{name: "missing checkpoint", mutate: "DELETE FROM checkpoint_events WHERE event_seq=(SELECT MIN(seq) FROM capture_events WHERE state='pending')"},
		{name: "foreign generation", mutate: "UPDATE capture_events SET branch_generation=99 WHERE seq=(SELECT MIN(seq) FROM capture_events WHERE state='pending')"},
		{name: "unsupported count", reason: "intent planner: forced-aging request offered 1 captures, want 1"},
		{name: "ambiguous failure", reason: "intent planner: forced-aging request offered 2 captures, want 1; changed branch"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, events, drain := openPublicationDrainTestState(t, 4, 3)
			if _, err := db.SQL().ExecContext(ctx, "UPDATE publication_drains SET commit_strategy='intent' WHERE id=?", drain.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := db.SQL().ExecContext(ctx, "UPDATE capture_events SET state='published',published_ts=11,commit_oid='published' WHERE seq=?", events[0].Seq); err != nil {
				t.Fatal(err)
			}
			update := PublicationDrainUpdateFrom(drain, 11, 11)
			update.Phase = state.PublicationDrainSemantic
			update.PublishedEventCount = 1
			current, err := state.AdvancePublicationDrain(ctx, db, drain.ID, update)
			if err != nil {
				t.Fatal(err)
			}
			update = PublicationDrainUpdateFrom(current, 12, 11)
			update.Phase = state.PublicationDrainNeedsAction
			update.LastError = "intent planner: forced-aging request offered 2 captures, want 1"
			if tc.reason != "" {
				update.LastError = tc.reason
			}
			update.ReasonCode = "publication_failed"
			if _, err := state.AdvancePublicationDrain(ctx, db, drain.ID, update); err != nil {
				t.Fatal(err)
			}
			if tc.mutate != "" {
				if _, err := db.SQL().ExecContext(ctx, tc.mutate); err != nil {
					t.Fatal(err)
				}
			}
			got, err := RecoverSoftDependencyCapPublicationDrain(ctx, db, drain.BranchRef, drain.BranchGeneration, time.Unix(13, 0).UTC())
			if err != nil {
				t.Fatal(err)
			}
			if (got != nil) != tc.recovered {
				t.Fatalf("recovered=%+v want=%v", got, tc.recovered)
			}
			if got != nil {
				if got.ID != drain.ID || got.CheckpointID != drain.CheckpointID || got.Phase != state.PublicationDrainCheckpointing || got.LastError != "" || got.TargetEventCount != 3 || got.PublishedEventCount != 1 || fmt.Sprint(got.EventSeqs) != fmt.Sprint(drain.EventSeqs) {
					t.Fatalf("target changed: %+v", got)
				}
				var laterState string
				if err := db.ReadSQL().QueryRowContext(ctx, "SELECT state FROM capture_events WHERE seq=?", events[3].Seq).Scan(&laterState); err != nil || laterState != state.EventStatePending {
					t.Fatalf("later capture=%s err=%v", laterState, err)
				}
				again, err := RecoverSoftDependencyCapPublicationDrain(ctx, db, drain.BranchRef, drain.BranchGeneration, time.Unix(14, 0).UTC())
				if err != nil || again != nil {
					t.Fatalf("repeated recovery=%+v err=%v", again, err)
				}
			}
		})
	}
}
