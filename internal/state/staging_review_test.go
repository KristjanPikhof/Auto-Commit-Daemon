package state

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestReplacedStagingReviewRequiresPreservedMembership(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	cp := seedPublicationDrainCheckpoint(t, db, []string{"approved.txt"})
	drain := PublicationDrain{ID: "review", CheckpointID: cp.ID, WorktreeID: cp.WorktreeID,
		BranchRef: cp.ObservedRef, BranchGeneration: 7, Phase: PublicationDrainNeedsAction,
		TargetEventCount: 1, StagedConsent: true, ExpectedIndexDigest: strings.Repeat("a", 64),
		ReasonCode: "staging_changed", CreatedTS: 10, UpdatedTS: 10, LastProgressTS: 10}
	if _, err := PreparePublicationDrain(ctx, db, drain); err != nil {
		t.Fatal(err)
	}
	if err := CompleteReplacedStagingDrain(ctx, db, drain.ID, 11); !errors.Is(err, ErrPublicationDrainProgress) {
		t.Fatalf("unprotected membership accepted: %v", err)
	}
	if _, err := db.SQL().ExecContext(ctx, "UPDATE capture_events SET state='recovered'"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, "UPDATE publication_drains SET reason_code='ownership_unknown'"); err != nil {
		t.Fatal(err)
	}
	if err := CompleteReplacedStagingDrain(ctx, db, drain.ID, 11); !errors.Is(err, ErrPublicationDrainProgress) {
		t.Fatalf("unrelated block bypassed: %v", err)
	}
	if _, err := db.SQL().ExecContext(ctx, "UPDATE publication_drains SET reason_code='staging_changed'"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := CompleteReplacedStagingDrain(ctx, db, drain.ID, 11); err != nil {
			t.Fatal(err)
		}
	}
	got, err := PublicationDrainByID(ctx, db, drain.ID)
	if err != nil || got.Phase != PublicationDrainCompleted || got.StagedConsumed || got.ExpectedIndexDigest != drain.ExpectedIndexDigest || got.ReasonCode != "staging_review_replaced" {
		t.Fatalf("old approval changed: %+v %v", got, err)
	}
}
