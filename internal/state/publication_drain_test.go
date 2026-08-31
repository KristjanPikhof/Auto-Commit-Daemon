package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestPublicationDrainPersistsFrozenMembershipAndTransitions(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	checkpoint := seedPublicationDrainCheckpoint(t, db, []string{"a", "b"})
	drain := PublicationDrain{
		ID: "drain-1", CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 7, Phase: PublicationDrainCheckpointing,
		TargetEventCount: int64(len(checkpoint.EventSeqs)),
		CreatedTS:        10, UpdatedTS: 10, LastProgressTS: 10,
	}
	created, err := PreparePublicationDrain(ctx, db, drain)
	if err != nil || !created {
		t.Fatalf("prepare=(%t,%v)", created, err)
	}
	if created, err = PreparePublicationDrain(ctx, db, drain); err != nil || created {
		t.Fatalf("idempotent prepare=(%t,%v)", created, err)
	}

	semantic, err := AdvancePublicationDrain(ctx, db, drain.ID,
		PublicationDrainUpdate{
			ExpectedPhase: PublicationDrainCheckpointing,
			Phase:         PublicationDrainSemantic, PublishedEventCount: 0,
			UpdatedTS: 11, LastProgressTS: 10,
		})
	if err != nil || semantic.Phase != PublicationDrainSemantic {
		t.Fatalf("semantic=(%+v,%v)", semantic, err)
	}
	normalizing, err := AdvancePublicationDrain(ctx, db, drain.ID,
		PublicationDrainUpdate{
			ExpectedPhase: PublicationDrainSemantic,
			Phase:         PublicationDrainNormalizing, SemanticRebuildAttempts: 1,
			FallbackMode: "deterministic_semantic", LastError: "invalid\x00 graph",
			UpdatedTS: 12, LastProgressTS: 12,
		})
	if err != nil || normalizing.LastError != "invalid graph" {
		t.Fatalf("normalizing=(%+v,%v)", normalizing, err)
	}
	fallbackUpdate := PublicationDrainUpdate{
		ExpectedPhase: PublicationDrainNormalizing,
		Phase:         PublicationDrainEventFallback, PublishedEventCount: 1,
		SemanticRebuildAttempts: 1, EventFallbackCount: 1, CommitCount: 1,
		FallbackMode: "dependency_ordered_events", LastError: normalizing.LastError,
		StagedConsumed: true, UpdatedTS: 13, LastProgressTS: 13,
	}
	fallback, err := AdvancePublicationDrain(ctx, db, drain.ID, fallbackUpdate)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := AdvancePublicationDrain(ctx, db, drain.ID, fallbackUpdate); err != nil || !reflect.DeepEqual(replay, fallback) {
		t.Fatalf("idempotent fallback=(%+v,%v) want=%+v", replay, err, fallback)
	}
	completed, err := AdvancePublicationDrain(ctx, db, drain.ID,
		PublicationDrainUpdate{
			ExpectedPhase: PublicationDrainEventFallback,
			Phase:         PublicationDrainCompleted, PublishedEventCount: 2,
			SemanticRebuildAttempts: 1, EventFallbackCount: 2, CommitCount: 2,
			FallbackMode: "dependency_ordered_events", LastError: normalizing.LastError,
			StagedConsumed: true, UpdatedTS: 14, LastProgressTS: 14,
			CompletedTS: sql.NullFloat64{Float64: 14, Valid: true},
		})
	if err != nil || completed.Phase != PublicationDrainCompleted ||
		!reflect.DeepEqual(completed.EventSeqs, checkpoint.EventSeqs) {
		t.Fatalf("completed=(%+v,%v)", completed, err)
	}
	if active, err := ActivePublicationDrains(ctx, db); err != nil || len(active) != 0 {
		t.Fatalf("active=%+v err=%v", active, err)
	}
}

func TestActivePublicationDrainsForPairIsExactAndBounded(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	type drainSpec struct {
		id         string
		branchRef  string
		generation int64
		phase      string
		createdTS  float64
	}
	specs := []drainSpec{
		{id: "drain-pair-1", branchRef: "refs/heads/main", generation: 7,
			phase: PublicationDrainCheckpointing, createdTS: 1},
		{id: "drain-pair-2", branchRef: "refs/heads/main", generation: 7,
			phase: PublicationDrainSemantic, createdTS: 2},
		{id: "drain-pair-3", branchRef: "refs/heads/main", generation: 7,
			phase: PublicationDrainEventFallback, createdTS: 3},
		{id: "drain-other-generation", branchRef: "refs/heads/main", generation: 8,
			phase: PublicationDrainCheckpointing, createdTS: 4},
		{id: "drain-needs-action", branchRef: "refs/heads/main", generation: 7,
			phase: PublicationDrainNeedsAction, createdTS: 5},
	}
	for i, spec := range specs {
		suffix := strings.Repeat("0", 15) + strconv.Itoa(i+1)
		checkpoint := Checkpoint{
			ID:               "cp-" + strconv.Itoa(1000+i) + "-" + suffix,
			OperationID:      "op-drain-pair-" + strconv.Itoa(i+1),
			WorktreeID:       "0123456789abcdef",
			Reason:           CheckpointReasonManualBarrier,
			ObservationEpoch: 1, CoverageEpoch: 1,
			ObservedHead: "head", ObservedRef: spec.branchRef,
			TreeOID: "tree", CommitOID: "commit", CreatedTS: spec.createdTS,
		}
		checkpoint.Ref = "refs/acd/checkpoints/v1/" +
			checkpoint.WorktreeID + "/" + checkpoint.ID
		if created, err := PrepareCheckpoint(
			ctx, db, checkpoint, checkpointTestDigest); err != nil || !created {
			t.Fatalf("prepare checkpoint %s=(%t,%v)", checkpoint.ID, created, err)
		}
		if err := CompleteCheckpoint(
			ctx, db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID,
			spec.createdTS); err != nil {
			t.Fatalf("complete checkpoint %s: %v", checkpoint.ID, err)
		}
		drain := PublicationDrain{
			ID: spec.id, CheckpointID: checkpoint.ID,
			WorktreeID: checkpoint.WorktreeID, BranchRef: spec.branchRef,
			BranchGeneration: spec.generation, Phase: PublicationDrainCompleted,
			TargetEventCount: 0, PublishedEventCount: 0,
			CreatedTS: spec.createdTS, UpdatedTS: spec.createdTS,
			LastProgressTS: spec.createdTS,
		}
		if created, err := PreparePublicationDrain(
			ctx, db, drain); err != nil || !created {
			t.Fatalf("prepare drain %s=(%t,%v)", spec.id, created, err)
		}
	}
	for _, spec := range specs {
		if _, err := db.SQL().ExecContext(ctx,
			"UPDATE publication_drains SET phase=? WHERE id=?",
			spec.phase, spec.id); err != nil {
			t.Fatalf("activate drain %s: %v", spec.id, err)
		}
	}

	drains, err := ActivePublicationDrainsForPair(
		ctx, db, "refs/heads/main", 7)
	if err != nil {
		t.Fatalf("ActivePublicationDrainsForPair: %v", err)
	}
	if len(drains) != 2 || drains[0].ID != "drain-pair-1" ||
		drains[1].ID != "drain-pair-2" {
		t.Fatalf("drains=%+v want first two exact active rows", drains)
	}
}

func TestPublicationDrainFreezesEnvironmentIntentRuntime(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	fingerprint := "sha256:" + strings.Repeat("a", 64)
	for key, value := range map[string]string{
		"publication.runtime.ready":                "true",
		"publication.runtime.commit_strategy":      "intent",
		"publication.runtime.commit_format":        "conventional",
		"publication.runtime.config_revision_id":   "0",
		"publication.runtime.provider":             "openai-compat",
		"publication.runtime.provider_model":       "semantic-model",
		"publication.runtime.provider_fingerprint": fingerprint,
	} {
		if err := MetaSet(ctx, db, key, value); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint := seedPublicationDrainCheckpoint(t, db, []string{"intent.go"})
	drain := PublicationDrain{
		ID: "drain-env-intent", CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 7, Phase: PublicationDrainCheckpointing,
		TargetEventCount: 1, CreatedTS: 10, UpdatedTS: 10,
		LastProgressTS: 10,
	}
	if created, err := PreparePublicationDrain(ctx, db, drain); err != nil || !created {
		t.Fatalf("prepare=(%t,%v)", created, err)
	}
	loaded, err := PublicationDrainByID(ctx, db, drain.ID)
	if err != nil || loaded.CommitStrategy != "intent" ||
		loaded.CommitFormat != "conventional" || loaded.ConfigRevisionID != 0 ||
		loaded.Provider != "openai-compat" ||
		loaded.ProviderModel != "semantic-model" ||
		loaded.ProviderFingerprint != fingerprint {
		t.Fatalf("frozen runtime=%+v err=%v", loaded, err)
	}
	if err := MetaSet(ctx, db, "publication.runtime.commit_strategy", "event"); err != nil {
		t.Fatal(err)
	}
	if err := MetaSet(ctx, db, "publication.runtime.ready", "false"); err != nil {
		t.Fatal(err)
	}
	if created, err := PreparePublicationDrain(ctx, db, drain); err != nil || created {
		t.Fatalf("idempotent prepare after runtime change=(%t,%v)", created, err)
	}
}

func TestPreparePublicationDrainRejectsBlockedRuntimeProjection(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	for key, value := range map[string]string{
		"publication.runtime.ready":                "false",
		"publication.runtime.commit_strategy":      "intent",
		"publication.runtime.commit_format":        "imperative",
		"publication.runtime.config_revision_id":   "1",
		"publication.runtime.provider":             "openai-compat",
		"publication.runtime.provider_model":       "stale-model",
		"publication.runtime.provider_fingerprint": "sha256:stale",
	} {
		if err := MetaSet(ctx, db, key, value); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint := seedPublicationDrainCheckpoint(t, db, []string{"blocked.go"})
	drain := PublicationDrain{
		ID: "drain-blocked-runtime", CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 7, Phase: PublicationDrainCheckpointing,
		TargetEventCount: 1, CreatedTS: 10, UpdatedTS: 10,
		LastProgressTS: 10,
	}
	if created, err := PreparePublicationDrain(ctx, db, drain); created || !errors.Is(err, ErrPublicationDrainRuntime) {
		t.Fatalf("prepare blocked runtime=(%t,%v)", created, err)
	}
	var count int
	if err := db.ReadSQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM publication_drains WHERE id=?`, drain.ID,
	).Scan(&count); err != nil || count != 0 {
		t.Fatalf("blocked drain rows=%d err=%v", count, err)
	}
}

func TestPreparePublicationDrainRejectsStaleAppliedRevision(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	revisionA, err := InsertConfigRevision(ctx, db, ConfigRevisionInput{
		Snapshot: []byte(`{"ai.provider":"openai-compat","commit.strategy":"intent"}`),
		Profile:  "a", Scope: "repository", CreatedTS: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	revisionB, err := InsertConfigRevision(ctx, db, ConfigRevisionInput{
		Snapshot: []byte(`{"ai.provider":"deterministic","commit.strategy":"event"}`),
		Profile:  "b", Scope: "repository", CreatedTS: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO runtime_config_state(id,applied_revision_id,updated_ts)
VALUES(1,?,2)
ON CONFLICT(id) DO UPDATE SET applied_revision_id=excluded.applied_revision_id,
 updated_ts=excluded.updated_ts`, revisionB.ID); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{
		"publication.runtime.ready":                "true",
		"publication.runtime.commit_strategy":      "intent",
		"publication.runtime.commit_format":        "imperative",
		"publication.runtime.config_revision_id":   strconv.FormatInt(revisionA.ID, 10),
		"publication.runtime.provider":             "openai-compat",
		"publication.runtime.provider_model":       "semantic-model",
		"publication.runtime.provider_fingerprint": "sha256:stale-a",
	} {
		if err := MetaSet(ctx, db, key, value); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint := seedPublicationDrainCheckpoint(t, db, []string{"race.go"})
	drain := PublicationDrain{
		ID: "drain-stale-applied", CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 7, Phase: PublicationDrainCheckpointing,
		TargetEventCount: 1, CreatedTS: 10, UpdatedTS: 10,
		LastProgressTS: 10,
	}
	if created, err := PreparePublicationDrain(ctx, db, drain); created || !errors.Is(err, ErrPublicationDrainRuntime) {
		t.Fatalf("prepare stale applied runtime=(%t,%v)", created, err)
	}
}

func TestPreparePublicationDrainKeepsRevisionIdentityAfterRuntimeChange(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	revisionA, err := InsertConfigRevision(ctx, db, ConfigRevisionInput{
		Snapshot: []byte(`{"ai.provider":"openai-compat","commit.strategy":"intent"}`),
		Profile:  "a", Scope: "repository", CreatedTS: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	revisionB, err := InsertConfigRevision(ctx, db, ConfigRevisionInput{
		Snapshot: []byte(`{"ai.provider":"deterministic","commit.strategy":"event"}`),
		Profile:  "b", Scope: "repository", CreatedTS: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	setApplied := func(revisionID int64) {
		t.Helper()
		if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO runtime_config_state(id,applied_revision_id,updated_ts)
VALUES(1,?,2)
ON CONFLICT(id) DO UPDATE SET applied_revision_id=excluded.applied_revision_id,
 updated_ts=excluded.updated_ts`, revisionID); err != nil {
			t.Fatal(err)
		}
	}
	setApplied(revisionA.ID)
	for key, value := range map[string]string{
		"publication.runtime.ready":                "true",
		"publication.runtime.commit_strategy":      "intent",
		"publication.runtime.commit_format":        "imperative",
		"publication.runtime.config_revision_id":   strconv.FormatInt(revisionA.ID, 10),
		"publication.runtime.provider":             "openai-compat",
		"publication.runtime.provider_model":       "semantic-model",
		"publication.runtime.provider_fingerprint": "sha256:revision-a",
	} {
		if err := MetaSet(ctx, db, key, value); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint := seedPublicationDrainCheckpoint(t, db, []string{"idempotent.go"})
	drain := PublicationDrain{
		ID: "drain-revision-idempotent", CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 7, Phase: PublicationDrainCheckpointing,
		TargetEventCount: 1, CreatedTS: 10, UpdatedTS: 10,
		LastProgressTS: 10,
	}
	if created, err := PreparePublicationDrain(ctx, db, drain); err != nil || !created {
		t.Fatalf("prepare revision drain=(%t,%v)", created, err)
	}
	setApplied(revisionB.ID)
	if err := MetaSet(ctx, db, "publication.runtime.ready", "false"); err != nil {
		t.Fatal(err)
	}
	if created, err := PreparePublicationDrain(ctx, db, drain); err != nil || created {
		t.Fatalf("idempotent prepare after revision change=(%t,%v)", created, err)
	}
}

func TestMigrationAdoptsLegacyIntentProofInsteadOfLaterEventMeta(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	checkpoint := seedPublicationDrainCheckpoint(t, db, []string{"legacy.go"})
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO publication_drains(
 id,checkpoint_id,worktree_id,branch_ref,branch_generation,phase,
 target_event_count,created_ts,updated_ts,last_progress_ts
) VALUES('drain-legacy-intent',? ,? ,? ,7,'semantic',1,10,10,10)`,
		checkpoint.ID, checkpoint.WorktreeID, checkpoint.ObservedRef); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO publication_drain_events(drain_id,ord,event_seq)
VALUES('drain-legacy-intent',0,?)`, checkpoint.EventSeqs[0]); err != nil {
		t.Fatal(err)
	}
	revision, err := InsertConfigRevision(ctx, db, ConfigRevisionInput{
		Snapshot: []byte(`{"ai.provider":"openai-compat","ai.model":"semantic-model","commit.strategy":"intent","commit.format":"imperative"}`),
		Profile:  "legacy", Scope: "repository", CreatedTS: 11,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AppendIntentPlannerWindow(ctx, db, IntentPlannerWindow{
		PlannedTS: 12,
		Provider:  sql.NullString{String: "openai-compat", Valid: true},
		Model:     sql.NullString{String: "semantic-model", Valid: true},
		BranchRef: checkpoint.ObservedRef, BranchGeneration: 7,
		CommitFormat:        sql.NullString{String: "imperative", Valid: true},
		OfferedSeqs:         []int64{checkpoint.EventSeqs[0]},
		VisibleOriginalSeqs: []int64{checkpoint.EventSeqs[0]},
		SelectedGroups: []IntentPlannerWindowGroup{{
			SelectedSeqs: []int64{checkpoint.EventSeqs[0]},
		}},
		Events: []IntentPlannerWindowEvent{{
			EventSeq: checkpoint.EventSeqs[0], Offered: true, Selected: true,
		}},
		ConfigRevisionID: sql.NullInt64{Int64: revision.ID, Valid: true},
		Outcome:          sql.NullString{String: "selected", Valid: true},
		ResolutionMode:   sql.NullString{String: "provider", Valid: true},
	}); err != nil {
		t.Fatal(err)
	}
	if err := MetaSet(ctx, db, "commit.strategy", "event"); err != nil {
		t.Fatal(err)
	}
	if err := MetaSet(ctx, db, "ai.provider", "deterministic"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.SQL().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := applyVersionedMigrations(ctx, tx, 24); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	loaded, err := PublicationDrainByID(ctx, db, "drain-legacy-intent")
	if err != nil || loaded.CommitStrategy != "intent" ||
		loaded.CommitFormat != "imperative" ||
		loaded.ConfigRevisionID != revision.ID ||
		loaded.Provider != "openai-compat" ||
		loaded.ProviderModel != "semantic-model" {
		t.Fatalf("migrated contract=%+v err=%v", loaded, err)
	}
}

func TestPublicationDrainRejectsIdentityPhaseAndProgressRegression(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	checkpoint := seedPublicationDrainCheckpoint(t, db, []string{"a"})
	drain := PublicationDrain{
		ID: "drain-guarded", CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 7, Phase: PublicationDrainSemantic,
		TargetEventCount: 1, PublishedEventCount: 1,
		CreatedTS: 20, UpdatedTS: 20, LastProgressTS: 20,
	}
	if _, err := PreparePublicationDrain(ctx, db, drain); err != nil {
		t.Fatal(err)
	}
	changed := drain
	changed.BranchGeneration = 2
	if _, err := PreparePublicationDrain(ctx, db, changed); !errors.Is(err, ErrPublicationDrainIdentity) {
		t.Fatalf("identity error=%v", err)
	}
	if _, err := AdvancePublicationDrain(ctx, db, drain.ID,
		PublicationDrainUpdate{
			ExpectedPhase: PublicationDrainCheckpointing,
			Phase:         PublicationDrainEventFallback, PublishedEventCount: 1,
			UpdatedTS: 21, LastProgressTS: 21,
		}); !errors.Is(err, ErrPublicationDrainPhase) {
		t.Fatalf("CAS error=%v", err)
	}
	if _, err := AdvancePublicationDrain(ctx, db, drain.ID,
		PublicationDrainUpdate{
			ExpectedPhase: PublicationDrainSemantic,
			Phase:         PublicationDrainNormalizing, PublishedEventCount: 0,
			UpdatedTS: 21, LastProgressTS: 21,
		}); !errors.Is(err, ErrPublicationDrainProgress) {
		t.Fatalf("progress error=%v", err)
	}
	loaded, err := PublicationDrainByID(ctx, db, drain.ID)
	if err != nil || loaded.Phase != PublicationDrainSemantic ||
		loaded.PublishedEventCount != 1 {
		t.Fatalf("loaded after rejection=%+v err=%v", loaded, err)
	}
}

func TestReconcileResolvedPublicationDrainsCompletesProvenTargets(t *testing.T) {
	for _, tc := range []struct {
		name       string
		paths      []string
		states     []string
		wantCommit int64
	}{
		{name: "all published", paths: []string{"a", "b"},
			states: []string{EventStatePublished, EventStatePublished}, wantCommit: 2},
		{name: "published and recovered", paths: []string{"a", "b"},
			states: []string{EventStatePublished, EventStateRecovered}, wantCommit: 1},
		{name: "empty target"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, _ := openTestDB(t)
			checkpoint := seedPublicationDrainCheckpoint(t, db, tc.paths)
			drain := PublicationDrain{
				ID: "drain-resolved", CheckpointID: checkpoint.ID,
				WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
				BranchGeneration: 7, Phase: PublicationDrainNeedsAction,
				TargetEventCount: int64(len(checkpoint.EventSeqs)),
				LastError:        "forced_capture_deferred", StagedConsent: true,
				StagedConsumed: true, CreatedTS: 10, UpdatedTS: 10,
				LastProgressTS: 10,
			}
			if _, err := PreparePublicationDrain(ctx, db, drain); err != nil {
				t.Fatal(err)
			}
			for i, eventState := range tc.states {
				commitOID := any(nil)
				if eventState == EventStatePublished {
					commitOID = "commit-" + tc.paths[i]
				}
				if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state=?,commit_oid=? WHERE seq=?`,
					eventState, commitOID, checkpoint.EventSeqs[i]); err != nil {
					t.Fatal(err)
				}
			}

			candidates, err := ResolvedPublicationDrainCandidates(ctx, db.ReadSQL())
			if err != nil || len(candidates) != 1 ||
				candidates[0].ResolvedEvents != int64(len(tc.paths)) {
				t.Fatalf("candidates=%+v err=%v", candidates, err)
			}
			reconciled, err := ReconcileResolvedPublicationDrains(ctx, db, 20)
			if err != nil || len(reconciled) != 1 ||
				reconciled[0].PreviousPhase != PublicationDrainNeedsAction {
				t.Fatalf("reconciled=%+v err=%v", reconciled, err)
			}
			loaded, err := PublicationDrainByID(ctx, db, drain.ID)
			if err != nil || loaded.Phase != PublicationDrainCompleted ||
				loaded.PublishedEventCount != int64(len(tc.paths)) ||
				loaded.CommitCount != tc.wantCommit || loaded.LastError != "" ||
				!loaded.CompletedTS.Valid || loaded.CompletedTS.Float64 != 20 {
				t.Fatalf("loaded=%+v err=%v", loaded, err)
			}
			if again, err := ReconcileResolvedPublicationDrains(ctx, db, 21); err != nil || len(again) != 0 {
				t.Fatalf("idempotent reconcile=%+v err=%v", again, err)
			}
		})
	}
}

func TestReconcileResolvedPublicationDrainsRequiresCompleteProof(t *testing.T) {
	for _, tc := range []struct {
		name   string
		state  string
		mutate func(context.Context, *DB, PublicationDrain, Checkpoint)
	}{
		{name: "pending", state: EventStatePending},
		{name: "failed", state: EventStateFailed},
		{name: "blocked", state: EventStateBlockedConflict},
		{name: "unconsumed staging", state: EventStatePublished,
			mutate: func(ctx context.Context, db *DB, drain PublicationDrain, _ Checkpoint) {
				if _, err := db.SQL().ExecContext(ctx,
					`UPDATE publication_drains SET staged_consumed=0 WHERE id=?`, drain.ID); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "incomplete checkpoint", state: EventStatePublished,
			mutate: func(ctx context.Context, db *DB, _ PublicationDrain, checkpoint Checkpoint) {
				if _, err := db.SQL().ExecContext(ctx,
					`UPDATE checkpoints SET phase='needs_action' WHERE id=?`, checkpoint.ID); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "membership count mismatch", state: EventStatePublished,
			mutate: func(ctx context.Context, db *DB, drain PublicationDrain, _ Checkpoint) {
				if _, err := db.SQL().ExecContext(ctx,
					`UPDATE publication_drains SET target_event_count=2 WHERE id=?`, drain.ID); err != nil {
					t.Fatal(err)
				}
			}},
		{name: "missing member", state: EventStatePublished,
			mutate: func(ctx context.Context, db *DB, drain PublicationDrain, _ Checkpoint) {
				conn, err := db.SQL().Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys=OFF`); err != nil {
					t.Fatal(err)
				}
				if _, err := conn.ExecContext(ctx, `
UPDATE publication_drain_events SET event_seq=999999 WHERE drain_id=?`, drain.ID); err != nil {
					t.Fatal(err)
				}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, _ := openTestDB(t)
			checkpoint := seedPublicationDrainCheckpoint(t, db, []string{"a"})
			drain := PublicationDrain{
				ID: "drain-unresolved", CheckpointID: checkpoint.ID,
				WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
				BranchGeneration: 7, Phase: PublicationDrainNeedsAction,
				TargetEventCount: 1, LastError: "blocked", StagedConsent: true,
				StagedConsumed: true, CreatedTS: 10, UpdatedTS: 10,
				LastProgressTS: 10,
			}
			if _, err := PreparePublicationDrain(ctx, db, drain); err != nil {
				t.Fatal(err)
			}
			commitOID := any(nil)
			if tc.state == EventStatePublished {
				commitOID = "commit-a"
			}
			if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state=?,commit_oid=? WHERE seq=?`,
				tc.state, commitOID, checkpoint.EventSeqs[0]); err != nil {
				t.Fatal(err)
			}
			if tc.mutate != nil {
				tc.mutate(ctx, db, drain, checkpoint)
			}
			if reconciled, err := ReconcileResolvedPublicationDrains(ctx, db, 20); err != nil || len(reconciled) != 0 {
				t.Fatalf("reconciled=%+v err=%v", reconciled, err)
			}
			loaded, err := PublicationDrainByID(ctx, db, drain.ID)
			if err != nil || loaded.Phase != PublicationDrainNeedsAction {
				t.Fatalf("loaded=%+v err=%v", loaded, err)
			}
		})
	}
}

func TestReconcileResolvedPublicationDrainsIsConcurrentAndIdempotent(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	checkpoint := seedPublicationDrainCheckpoint(t, db, []string{"a"})
	drain := PublicationDrain{
		ID: "drain-concurrent", CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 7, Phase: PublicationDrainNeedsAction,
		TargetEventCount: 1, LastError: "blocked",
		CreatedTS: 10, UpdatedTS: 10, LastProgressTS: 10,
	}
	if _, err := PreparePublicationDrain(ctx, db, drain); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='published',commit_oid='commit-a'
WHERE seq=?`, checkpoint.EventSeqs[0]); err != nil {
		t.Fatal(err)
	}

	const callers = 8
	var wg sync.WaitGroup
	results := make(chan []PublicationDrainReconciliation, callers)
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reconciled, err := ReconcileResolvedPublicationDrains(ctx, db, 20)
			results <- reconciled
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent reconcile: %v", err)
		}
	}
	changed := 0
	for reconciled := range results {
		changed += len(reconciled)
	}
	if changed != 1 {
		t.Fatalf("concurrent reconciliations changed=%d want 1", changed)
	}
}

func TestPublicationDrainRequiresCompleteCheckpointMembership(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	checkpoint := seedPublicationDrainCheckpoint(t, db, []string{"a", "b"})
	drain := PublicationDrain{
		ID: "drain-incomplete", CheckpointID: checkpoint.ID,
		WorktreeID: checkpoint.WorktreeID, BranchRef: checkpoint.ObservedRef,
		BranchGeneration: 7, Phase: PublicationDrainCheckpointing,
		TargetEventCount: 1, EventSeqs: checkpoint.EventSeqs[:1],
		CreatedTS: 20, UpdatedTS: 20, LastProgressTS: 20,
	}
	if _, err := PreparePublicationDrain(ctx, db, drain); !errors.Is(err, ErrPublicationDrainIdentity) {
		t.Fatalf("incomplete checkpoint membership error=%v", err)
	}
}

func TestReadOnlyDrainProjectionDoesNotMigrateV20(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, "PRAGMA user_version=20"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	beforeBody, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	before := sha256.Sum256(beforeBody)
	projection, err := ReadPublicationDrainProjection(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	afterBody, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	after := sha256.Sum256(afterBody)
	if projection.Available || projection.SchemaVersion != 20 || before != after {
		t.Fatalf("projection=%+v sha before=%x after=%x",
			projection, before, after)
	}
}

func TestMigrateSchema20ToCurrentAddsPublicationDrains(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx,
		"DROP INDEX idx_publication_drains_active"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx,
		"DROP TABLE publication_drains"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, "PRAGMA user_version=20"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if version, err := db.UserVersion(ctx); err != nil || version != SchemaVersion {
		t.Fatalf("version=(%d,%v)", version, err)
	}
	var table string
	if err := db.ReadSQL().QueryRowContext(ctx, `
SELECT name FROM sqlite_master WHERE type='table' AND name='publication_drains'`).
		Scan(&table); err != nil || table != "publication_drains" {
		t.Fatalf("publication_drains=(%q,%v)", table, err)
	}
}

func seedPublicationDrainCheckpoint(
	t *testing.T,
	db *DB,
	paths []string,
) Checkpoint {
	t.Helper()
	ctx := context.Background()
	checkpoint := Checkpoint{
		ID:          "cp-1786486000000-0123456789abcdef",
		OperationID: "op-publication-drain",
		WorktreeID:  "0123456789abcdef", Reason: CheckpointReasonManualBarrier,
		ObservationEpoch: 1, CoverageEpoch: 1, ObservedHead: "head",
		ObservedRef: "refs/heads/main", TreeOID: "tree", CommitOID: "commit",
		Ref:       "refs/acd/checkpoints/v1/0123456789abcdef/cp-1786486000000-0123456789abcdef",
		CreatedTS: 1,
	}
	for i, path := range paths {
		result, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state
) VALUES('refs/heads/main',7,'head','modify',?,'exact',?,'pending')`, path, i+1)
		if err != nil {
			t.Fatal(err)
		}
		seq, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		checkpoint.EventSeqs = append(checkpoint.EventSeqs, seq)
	}
	if created, err := PrepareCheckpoint(
		ctx, db, checkpoint, checkpointTestDigest); err != nil || !created {
		t.Fatalf("checkpoint prepare=(%t,%v)", created, err)
	}
	if err := CompleteCheckpoint(
		ctx, db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 2); err != nil {
		t.Fatal(err)
	}
	return checkpoint
}
