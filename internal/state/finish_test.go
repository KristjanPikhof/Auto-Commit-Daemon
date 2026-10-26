package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func finishFixture(t *testing.T, db *DB, n int) FinishRequest {
	t.Helper()
	ctx := context.Background()
	cp := Checkpoint{ID: fmt.Sprintf("cp-%d-0123456789abcdef", n), OperationID: fmt.Sprintf("finish-cp-%d", n), WorktreeID: "0123456789abcdef", Reason: CheckpointReasonManualBarrier, ObservedRef: "refs/heads/main", ObservedHead: "head", TreeOID: "tree", CommitOID: fmt.Sprintf("commit-%d", n), CreatedTS: float64(n)}
	cp.Ref = "refs/acd/checkpoints/v1/" + cp.WorktreeID + "/" + cp.ID
	if _, err := PrepareCheckpoint(ctx, db, cp, checkpointTestDigest); err != nil {
		t.Fatal(err)
	}
	if err := CompleteCheckpoint(ctx, db, cp.ID, cp.Ref, cp.CommitOID, float64(n)); err != nil {
		t.Fatal(err)
	}
	return FinishRequest{ID: fmt.Sprintf("finish-%d", n), DeadlineTS: nowSeconds() + 300, Target: FinishTarget{Version: 1, RepositoryID: "repository", WorktreeID: cp.WorktreeID, BranchRef: cp.ObservedRef, ExpectedHead: cp.ObservedHead, CheckpointID: cp.ID, CheckpointTree: cp.TreeOID, TargetTree: cp.TreeOID, IndexDigest: "index", Mode: "local", Style: "imperative", ConfigDigest: "config"}}
}

func startFinishFixture(t *testing.T, db *DB, r FinishRequest) FinishRequest {
	t.Helper()
	saved, created, err := StartFinishRequest(context.Background(), db, r)
	if err != nil || !created {
		t.Fatalf("start created=%t error=%v", created, err)
	}
	return saved
}

func failFinishFixture(t *testing.T, db *DB, r FinishRequest) FinishRequest {
	t.Helper()
	saved, err := UpdateFinishRequest(context.Background(), db, r.ID, r.Attempt, FinishRunning, FinishUpdate{State: FinishFailed, ReasonCode: "provider_rejected", Reason: "Protected target was not published"})
	if err != nil {
		t.Fatal(err)
	}
	return saved
}

func TestFinishStateOwnershipAndImmutableTarget(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	r := finishFixture(t, db, 1)
	malformed := r
	malformed.Target.TargetCommitOID = "unpaired"
	if _, _, err := StartFinishRequest(ctx, db, malformed); !errors.Is(err, ErrFinishIdentity) {
		t.Fatalf("unpaired pin: %v", err)
	}
	saved := startFinishFixture(t, db, r)
	if _, err := RetryFinishRequest(ctx, db, r.ID, nowSeconds()+300, true); !errors.Is(err, ErrFinishBusy) {
		t.Fatalf("preview attached to active apply: %v", err)
	}
	repeated := r
	repeated.ID = "reconnect"
	repeated.DeadlineTS += 100
	got, created, err := StartFinishRequest(ctx, db, repeated)
	if err != nil || created || got.ID != saved.ID || got.DeadlineTS != saved.DeadlineTS {
		t.Fatalf("reconnect %+v created=%t err=%v", got, created, err)
	}
	if _, err := UpdateFinishRequest(ctx, db, r.ID, 1, FinishRunning, FinishUpdate{State: FinishCompleted, Result: json.RawMessage(`{"published":true}`)}); !errors.Is(err, ErrFinishState) {
		t.Fatalf("published without prepared proof: %v", err)
	}
	changed := r
	changed.ID = "different"
	changed.Target.Message = "Different goal"
	if _, _, err := StartFinishRequest(ctx, db, changed); !errors.Is(err, ErrFinishBusy) {
		t.Fatalf("changed target: %v", err)
	}
	if _, err := db.SQL().ExecContext(ctx, `UPDATE finish_requests SET target_json='{}' WHERE request_id=?`, r.ID); err == nil {
		t.Fatal("target mutated")
	}
	if _, err := db.SQL().ExecContext(ctx, `UPDATE finish_requests SET deadline_ts=deadline_ts+1 WHERE request_id=?`, r.ID); err == nil {
		t.Fatal("deadline renewed")
	}
	if _, err := db.SQL().ExecContext(ctx, `DELETE FROM finish_requests WHERE request_id=?`, r.ID); err == nil {
		t.Fatal("evidence deleted")
	}
	if _, err := UpdateFinishRequest(ctx, db, r.ID, 99, FinishRunning, FinishUpdate{State: FinishCompleted}); !errors.Is(err, ErrFinishState) {
		t.Fatalf("stale attempt: %v", err)
	}
}

func TestFinishStateConcurrentReconnect(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	r := finishFixture(t, db, 1)
	const count = 8
	var wg sync.WaitGroup
	errs := make(chan error, count)
	created := make(chan bool, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, newRequest, err := StartFinishRequest(ctx, db, r)
			if err == nil && got.ID != r.ID {
				err = ErrFinishIdentity
			}
			errs <- err
			created <- newRequest
		}()
	}
	wg.Wait()
	close(errs)
	close(created)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	owners := 0
	for newRequest := range created {
		if newRequest {
			owners++
		}
	}
	if owners != 1 {
		t.Fatalf("owners=%d", owners)
	}
}

func TestFinishStatePreparedJournalAndAttention(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	r := startFinishFixture(t, db, finishFixture(t, db, 1))
	plan := json.RawMessage(`{"goals":["Preserve work"]}`)
	proof := json.RawMessage(`{"object_ref":"refs/acd/finish/chain","object_oid":"tip"}`)
	prepared, err := UpdateFinishRequest(ctx, db, r.ID, 1, FinishRunning, FinishUpdate{State: FinishPrepared, Plan: plan, Publication: proof})
	if err != nil || prepared.State != FinishPrepared {
		t.Fatalf("prepare %+v %v", prepared, err)
	}
	if n, err := InterruptFinishRequests(ctx, db, r.Target.WorktreeID); err != nil || n != 0 {
		t.Fatalf("restart prepared n=%d err=%v", n, err)
	}
	if _, err := UpdateFinishRequest(ctx, db, r.ID, 1, FinishPrepared, FinishUpdate{State: FinishFailed, ReasonCode: "unsafe"}); !errors.Is(err, ErrFinishState) {
		t.Fatalf("prepared became retryable: %v", err)
	}
	if _, err := UpdateFinishRequest(ctx, db, r.ID, 1, FinishPrepared, FinishUpdate{State: FinishPrepared, Publication: json.RawMessage(`{"object_oid":"other"}`)}); !errors.Is(err, ErrFinishIdentity) {
		t.Fatalf("journal mutated: %v", err)
	}
	if prepared.PlanID == "" {
		t.Fatal("saved plan has no content identity")
	}
	attention, err := UpdateFinishRequest(ctx, db, r.ID, 1, FinishPrepared, FinishUpdate{State: FinishNeedsAttention, ReasonCode: "ambiguous_ref"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RetryFinishRequest(ctx, db, r.ID, nowSeconds()+300, false); !errors.Is(err, ErrFinishState) {
		t.Fatalf("ambiguous retry: %v", err)
	}
	next := finishFixture(t, db, 2)
	if _, _, err := StartFinishRequest(ctx, db, next); !errors.Is(err, ErrFinishBusy) {
		t.Fatalf("ambiguous supersede: %v", err)
	}
	pins, err := FinishObjectPins(ctx, db, r.Target.WorktreeID)
	if err != nil || len(pins) != 1 || pins[0].CommitOID != "tip" {
		t.Fatalf("pins %+v err=%v", pins, err)
	}
	if _, err := UpdateFinishRequest(ctx, db, r.ID, attention.Attempt, FinishNeedsAttention, FinishUpdate{State: FinishCompleted, Result: json.RawMessage(`{"published":true}`)}); err != nil {
		t.Fatal(err)
	}
}

func TestFinishRetryRestartAndDurableBudget(t *testing.T) {
	ctx := context.Background()
	db, path := openTestDB(t)
	r := startFinishFixture(t, db, finishFixture(t, db, 1))
	for expected := 1; expected <= 2; expected++ {
		if calls, err := ConsumeFinishPlanningCall(ctx, db, r.ID, 1); err != nil || calls != expected {
			t.Fatalf("call %d err=%v", calls, err)
		}
	}
	if _, err := ConsumeFinishPlanningCall(ctx, db, r.ID, 1); !errors.Is(err, ErrFinishBudget) {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := LoadFinishRequest(ctx, reopened, r.ID)
	if err != nil || got.PlanningCalls != 2 || got.DeadlineTS != r.DeadlineTS {
		t.Fatalf("reopen %+v %v", got, err)
	}
	if n, err := InterruptFinishRequests(ctx, reopened, r.Target.WorktreeID); err != nil || n != 1 {
		t.Fatalf("interrupt %d %v", n, err)
	}
	failed, err := LatestFinishRequest(ctx, reopened, r.Target.WorktreeID)
	if err != nil || failed.State != FinishFailed {
		t.Fatalf("failure %+v %v", failed, err)
	}
	retried, err := RetryFinishRequest(ctx, reopened, r.ID, nowSeconds()+600, true)
	if err != nil || retried.Attempt != 2 || retried.PlanningCalls != 0 || !retried.DryRun || !reflect.DeepEqual(retried.Target, r.Target) {
		t.Fatalf("retry %+v %v", retried, err)
	}
	if _, err := ConsumeFinishPlanningCall(ctx, reopened, r.ID, 1); !errors.Is(err, ErrFinishBudget) {
		t.Fatalf("old attempt called: %v", err)
	}
	if _, err := UpdateFinishRequest(ctx, reopened, r.ID, 2, FinishRunning, FinishUpdate{State: FinishCompleted, Result: json.RawMessage(`{"published":false}`)}); err != nil {
		t.Fatal(err)
	}
	applied, err := RetryFinishRequest(ctx, reopened, r.ID, nowSeconds()+600, false)
	if err != nil || applied.Attempt != 3 || applied.DryRun || !reflect.DeepEqual(applied.Target, r.Target) {
		t.Fatalf("after preview %+v %v", applied, err)
	}
	var originalCalls int
	if err := reopened.ReadSQL().QueryRowContext(ctx, `SELECT planning_calls FROM finish_requests WHERE request_id=? AND attempt=1`, r.ID).Scan(&originalCalls); err != nil || originalCalls != 2 {
		t.Fatalf("old evidence %d %v", originalCalls, err)
	}
}

func TestFinishRetrySupersedeAndDeadline(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	first := startFinishFixture(t, db, finishFixture(t, db, 1))
	failFinishFixture(t, db, first)
	if _, err := RetryFinishRequest(ctx, db, first.ID, nowSeconds()-1, false); !errors.Is(err, ErrFinishBudget) {
		t.Fatalf("expired retry: %v", err)
	}
	next := startFinishFixture(t, db, finishFixture(t, db, 2))
	old, err := LoadFinishRequest(ctx, db, first.ID)
	if err != nil || old.SupersededBy != next.ID || !reflect.DeepEqual(old.Target, first.Target) {
		t.Fatalf("supersede %+v %v", old, err)
	}
	if _, err := RetryFinishRequest(ctx, db, first.ID, nowSeconds()+300, false); !errors.Is(err, ErrFinishState) {
		t.Fatalf("superseded retry: %v", err)
	}
	expired := finishFixture(t, db, 3)
	expired.Target.WorktreeID = "other"
	expired.DeadlineTS = nowSeconds() - 1
	if _, _, err := StartFinishRequest(ctx, db, expired); !errors.Is(err, ErrFinishBudget) {
		t.Fatalf("expired start: %v", err)
	}
}

func TestFinishRetentionPinsFailedOverlayAndPruneRace(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	r := finishFixture(t, db, 1)
	r.Target.IncludeStaged = true
	r.Target.TargetTree = "overlay"
	r.Target.TargetRef = "refs/acd/finish/target"
	r.Target.TargetCommitOID = "overlay-commit"
	r = startFinishFixture(t, db, r)
	failFinishFixture(t, db, r)
	cps, err := RetentionCheckpoints(ctx, db, r.Target.WorktreeID)
	if err != nil || len(cps) != 1 || !cps[0].Unresolved {
		t.Fatalf("retention %+v %v", cps, err)
	}
	if _, err := db.SQL().ExecContext(ctx, `UPDATE checkpoints SET retained=0 WHERE id=?`, r.Target.CheckpointID); err == nil {
		t.Fatal("pinned checkpoint marked pruned")
	}
	if _, err := PrepareCheckpointPrune(ctx, db, cps[0], checkpointTestDigest); err == nil {
		t.Fatal("pinned checkpoint pruning prepared")
	}
	pins, err := FinishObjectPins(ctx, db, r.Target.WorktreeID)
	if err != nil || len(pins) != 1 || pins[0].Ref != r.Target.TargetRef {
		t.Fatalf("overlay pins %+v %v", pins, err)
	}
	next := finishFixture(t, db, 2)
	cps, err = RetentionCheckpoints(ctx, db, next.Target.WorktreeID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareCheckpointPrune(ctx, db, cps[0], checkpointTestDigest); err != nil {
		t.Fatal(err)
	}
	if _, _, err := StartFinishRequest(ctx, db, next); !errors.Is(err, ErrFinishIdentity) {
		t.Fatalf("finish stole pruning checkpoint: %v", err)
	}
}

func TestFinishSchemaUpgradeAndReadOnly(t *testing.T) {
	ctx := context.Background()
	db, path := openTestDB(t)
	original := finishFixture(t, db, 1)
	if _, err := db.SQL().ExecContext(ctx, `DROP TRIGGER finish_checkpoint_pin_prune; DROP TRIGGER finish_checkpoint_pin_retained; DROP TABLE finish_requests; PRAGMA user_version=30`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	readonly, err := OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFinishRequest(ctx, readonly, "absent"); !errors.Is(err, ErrSetupRequired) {
		t.Fatalf("old readonly load %v", err)
	}
	if _, err := FinishObjectPins(ctx, readonly, original.Target.WorktreeID); !errors.Is(err, ErrSetupRequired) {
		t.Fatalf("old readonly pins: %v", err)
	}
	if version, err := readonly.UserVersion(ctx); err != nil || version != 30 {
		t.Fatalf("readonly migrated %d %v", version, err)
	}
	if _, err := RetentionCheckpoints(ctx, readonly, original.Target.WorktreeID); err != nil {
		t.Fatalf("legacy readonly retention: %v", err)
	}
	readonly.Close()
	upgraded, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer upgraded.Close()
	if version, err := upgraded.UserVersion(ctx); err != nil || version != SchemaVersion {
		t.Fatalf("upgrade %d %v", version, err)
	}
	startFinishFixture(t, upgraded, original)
	ro, err := OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if got, err := LoadFinishRequest(ctx, ro, original.ID); err != nil || got.Target.CheckpointID != original.Target.CheckpointID {
		t.Fatalf("read target %+v %v", got, err)
	}
	if _, err := ConsumeFinishPlanningCall(ctx, ro, original.ID, 1); err == nil {
		t.Fatal("readonly wrote call counter")
	}
}

func TestFinishEvidenceBudgetSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	db, path := openTestDB(t)
	r := startFinishFixture(t, db, finishFixture(t, db, 1))
	for i := 1; i <= 32; i++ {
		if n, err := ConsumeFinishEvidenceCall(ctx, db, r.ID, r.Attempt); err != nil || n != i {
			t.Fatalf("review reservation %d: %d %v", i, n, err)
		}
	}
	if _, err := ConsumeFinishEvidenceCall(ctx, db, r.ID, r.Attempt); !errors.Is(err, ErrFinishBudget) {
		t.Fatal("review limit bypassed", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got, err := LoadFinishRequest(ctx, db, r.ID)
	if err != nil || got.EvidenceCalls != 32 || got.PlanningCalls != 0 || got.Stage != "reviewing_evidence" || len(got.Plan) != 0 {
		t.Fatalf("durable review progress: %+v %v", got, err)
	}
	if _, err := ConsumeFinishEvidenceCall(ctx, db, r.ID, r.Attempt); !errors.Is(err, ErrFinishBudget) {
		t.Fatal(err)
	}
	if _, err := ConsumeFinishPlanningCall(ctx, db, r.ID, r.Attempt); err != nil {
		t.Fatal("review consumed planning allowance", err)
	}
	if _, err := InterruptFinishRequests(ctx, db, r.Target.WorktreeID); err != nil {
		t.Fatal(err)
	}
	interrupted, _ := LoadFinishRequest(ctx, db, r.ID)
	if interrupted.EvidenceCalls != 32 || interrupted.State != FinishFailed {
		t.Fatalf("restart discarded progress: %+v", interrupted)
	}
	retry, err := RetryFinishRequest(ctx, db, r.ID, nowSeconds()+300, false)
	if err != nil || retry.EvidenceCalls != 0 || retry.PlanningCalls != 0 || !reflect.DeepEqual(retry.Target, r.Target) {
		t.Fatalf("retry changed target or retained spent budget: %+v %v", retry, err)
	}
	if _, err := ConsumeFinishEvidenceCall(ctx, db, r.ID, r.Attempt); !errors.Is(err, ErrFinishBudget) {
		t.Fatal("stale attempt reserved a call", err)
	}
}
