package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	restorepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/restore"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestWorkerPublicationHoldIncludesInterruptedRestore(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "setup-publication-hold")
	restoreHeld := &atomic.Bool{}
	if workerPublicationHeld(marker, restoreHeld) {
		t.Fatal("worker started held without a setup marker or interrupted restore")
	}
	restoreHeld.Store(true)
	if !workerPublicationHeld(marker, restoreHeld) {
		t.Fatal("interrupted restore did not hold worker publication")
	}
	restoreHeld.Store(false)
	if err := os.WriteFile(marker, []byte("setup\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !workerPublicationHeld(marker, restoreHeld) {
		t.Fatal("setup marker did not hold worker publication")
	}
}

func TestWorkerRepairPreviewRemainsAvailableDuringRestoreHold(t *testing.T) {
	db, err := state.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	held := &atomic.Bool{}
	held.Store(true)
	handler := repositoryWorkerHandler{runtimes: map[string]*workerRuntime{
		"worktree": {db: db, gate: &sync.RWMutex{}, restoreHeld: held},
	}}
	result, protocolErr := handler.HandleWorkerRequest(context.Background(), supervisor.Request{
		Method: "repair", WorktreeID: "worktree",
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if plan, ok := result.(restorepkg.RepairPlan); !ok || plan.OperationID != "" {
		t.Fatalf("repair preview=%+v", result)
	}
	if held.Load() {
		t.Fatal("repair preview did not clear a stale restore hold")
	}
}

func TestSupervisorWorkerEnvironmentRequiresEnabledRepository(t *testing.T) {
	repo := materializeTestRepo(t, false)
	wt, err := gitpkg.ResolveWorktree(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	roots := paths.Roots{
		State: filepath.Join(root, "state"), Share: filepath.Join(root, "share"),
		Config: filepath.Join(root, "config"),
	}
	var repositoryID string
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		registry.Version = central.RegistryVersion
		registration, err := registry.RegisterResolvedRepo(wt, "test", time.Now().Unix())
		repositoryID = registration.Record.RepositoryID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	handler := cliSupervisorHandler{
		roots: roots,
		environment: map[string]string{
			"ACD_AI_API_KEY": "secret-test-value", "PATH": "/test/bin",
		},
	}
	data, protocolErr := handler.HandleSupervisorRequest(context.Background(), supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: "environment", Method: "worker_environment",
		RepositoryID: repositoryID,
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	values, ok := data.(map[string]string)
	if !ok || values["ACD_AI_API_KEY"] != "secret-test-value" || values["PATH"] != "/test/bin" {
		t.Fatalf("environment=%T %v", data, data)
	}
	values["PATH"] = "mutated"
	if handler.environment["PATH"] != "/test/bin" {
		t.Fatal("handler returned its mutable environment map")
	}
	_, protocolErr = handler.HandleSupervisorRequest(context.Background(), supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: "environment-missing", Method: "worker_environment",
		RepositoryID: "ffffffffffffffff",
	})
	if protocolErr == nil || protocolErr.Code != "repository_not_enabled" {
		t.Fatalf("missing repository error=%+v", protocolErr)
	}
}

func TestCheckpointBarrierWaitHonorsRequestDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	wait := checkpointBarrierWait(ctx)
	if wait < 119*time.Second || wait > 2*time.Minute {
		t.Fatalf("barrier wait=%s, want request deadline near 2m", wait)
	}
	if got := checkpointBarrierWait(context.Background()); got != supervisor.CheckpointBarrierTimeout {
		t.Fatalf("default barrier wait=%s want %s", got, supervisor.CheckpointBarrierTimeout)
	}
}

func TestPublicationDrainStatusIgnoresLaterEditsAndPriorTerminals(t *testing.T) {
	ctx := context.Background()
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	insert := func(eventState, commit string, published float64) int64 {
		result, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state,commit_oid,published_ts
) VALUES('refs/heads/main',1,'base','modify',?, 'exact',1,?,?,?)`,
			eventState+commit, eventState, commit, published)
		if err != nil {
			t.Fatal(err)
		}
		seq, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		return seq
	}
	insert("failed", "", 0)
	target := insert("published", "commit-1", 100)
	insert("pending", "", 0)
	progress, err := publicationDrainStatus(ctx, db, publicationDrainTarget{
		BranchRef: "refs/heads/main", Generation: 1, EventSeqs: []int64{target}, MaxSeq: target,
	})
	if err != nil {
		t.Fatal(err)
	}
	if progress.Remaining != 0 || progress.Commits != 1 || progress.Published != 1 || progress.Terminal != 0 {
		t.Fatalf("progress=%+v", progress)
	}
}

func TestPublicationDrainStatusDistinguishesRecoveredTarget(t *testing.T) {
	ctx := context.Background()
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	result, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state
) VALUES('refs/heads/main',3,'base','modify','recovered.txt','exact',1,'recovered')`)
	if err != nil {
		t.Fatal(err)
	}
	seq, _ := result.LastInsertId()
	progress, err := publicationDrainStatus(ctx, db, publicationDrainTarget{
		BranchRef: "refs/heads/main", Generation: 3, EventSeqs: []int64{seq}, MaxSeq: seq,
	})
	if err != nil {
		t.Fatal(err)
	}
	if progress.Recovered != 1 || progress.Published != 0 || progress.Remaining != 0 {
		t.Fatalf("progress=%+v", progress)
	}
}

func TestFreezePublicationDrainTargetUsesCheckpointPair(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, true)
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const checkpointID = "cp-freeze"
	result, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state
) VALUES('refs/heads/main',7,'head','modify','old-backlog.txt','exact',1,'pending')`)
	if err != nil {
		t.Fatal(err)
	}
	backlogSeq, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := snapshotPublicationDrainTarget(ctx, db, "refs/heads/main", 7)
	if err != nil {
		t.Fatal(err)
	}
	worktreeID := checkpointpkg.WorktreeID(repo)
	seqs := insertCompletedCheckpoint(t, db, checkpointID, worktreeID, []checkpointMemberFixture{
		{State: state.EventStatePublished, CommitOID: "commit-1"},
		{State: state.EventStateRecovered},
		{State: state.EventStateFailed},
	})
	alignCheckpointHead(t, db, repo, checkpointID)
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "7"); err != nil {
		t.Fatal(err)
	}

	target, err := freezePublicationDrainTarget(
		ctx, db, repo, checkpointID, worktreeID, 1, anchor)
	if err != nil {
		t.Fatal(err)
	}
	wantSeqs := append([]int64{backlogSeq}, seqs...)
	if target.BranchRef != "refs/heads/main" || target.Generation != 7 ||
		len(target.EventSeqs) != len(wantSeqs) {
		t.Fatalf("target=%+v", target)
	}
	for index, seq := range wantSeqs {
		if target.EventSeqs[index] != seq {
			t.Fatalf("target members=%v, want %v", target.EventSeqs, wantSeqs)
		}
	}
	progress, err := publicationDrainStatus(ctx, db, target)
	if err != nil {
		t.Fatal(err)
	}
	if progress.Published != 1 || progress.Recovered != 1 || progress.Terminal != 1 ||
		progress.Remaining != 1 || progress.Commits != 1 {
		t.Fatalf("progress=%+v", progress)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo},
		"symbolic-ref", "HEAD", "refs/heads/switched"); err != nil {
		t.Fatal(err)
	}
	if _, err := freezePublicationDrainTarget(
		ctx, db, repo, checkpointID, worktreeID, 1, anchor); err == nil ||
		!strings.Contains(err.Error(), "branch changed") {
		t.Fatalf("branch switch error=%v", err)
	}
}

func TestFreezeEmptyPublicationDrainTargetRequiresCurrentGeneration(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, true)
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	worktreeID := checkpointpkg.WorktreeID(repo)
	insertCompletedCheckpoint(t, db, "cp-empty", worktreeID, nil)
	alignCheckpointHead(t, db, repo, "cp-empty")
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "99"); err != nil {
		t.Fatal(err)
	}
	anchor := publicationDrainTarget{BranchRef: "refs/heads/main", Generation: 7}
	if _, err := freezePublicationDrainTarget(
		ctx, db, repo, "cp-empty", worktreeID, 1, anchor); err == nil ||
		!strings.Contains(err.Error(), "generation changed") {
		t.Fatalf("generation mismatch error=%v", err)
	}
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "7"); err != nil {
		t.Fatal(err)
	}
	target, err := freezePublicationDrainTarget(
		ctx, db, repo, "cp-empty", worktreeID, 1, anchor)
	if err != nil {
		t.Fatal(err)
	}
	if target.BranchRef != "refs/heads/main" || target.Generation != 7 ||
		len(target.EventSeqs) != 0 || target.EventSeqs == nil {
		t.Fatalf("empty target=%+v", target)
	}
}

func TestFreezePublicationDrainTargetRequiresCheckpointHead(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, true)
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	worktreeID := checkpointpkg.WorktreeID(repo)
	insertCompletedCheckpoint(t, db, "cp-head", worktreeID, nil)
	alignCheckpointHead(t, db, repo, "cp-head")
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "7"); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo},
		"commit", "--allow-empty", "-q", "-m", "external commit"); err != nil {
		t.Fatal(err)
	}
	anchor := publicationDrainTarget{BranchRef: "refs/heads/main", Generation: 7}
	if _, err := freezePublicationDrainTarget(
		ctx, db, repo, "cp-head", worktreeID, 1, anchor); err == nil ||
		!strings.Contains(err.Error(), "without a completed ACD publication chain") {
		t.Fatalf("HEAD mismatch error=%v", err)
	}
}

func TestCheckpointBarrierDrainsPreBarrierBacklogWithEmptyCheckpoint(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, true)
	worktree, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	result, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state
) VALUES('refs/heads/main',7,'head','modify','backlog.txt','exact',1,'pending')`)
	if err != nil {
		t.Fatal(err)
	}
	backlogSeq, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	worktreeID := checkpointpkg.WorktreeID(repo)
	insertCompletedCheckpoint(t, db, "cp-empty-backlog", worktreeID, nil)
	alignCheckpointHead(t, db, repo, "cp-empty-backlog")
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "7"); err != nil {
		t.Fatal(err)
	}
	var wakeErr error
	var freshCheckpointOnce sync.Once
	handler := repositoryWorkerHandler{
		runtimes: map[string]*workerRuntime{"worktree": {
			worktree: worktree, db: db, gate: &sync.RWMutex{},
		}},
		wake: func(string) {
			freshCheckpointOnce.Do(func() {
				_, publishErr := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='published',commit_oid='backlog-commit' WHERE seq=?`, backlogSeq)
				wakeErr = errors.Join(
					publishErr,
					insertFreshBarrierCheckpoint(
						ctx, db, repo, "cp-empty-fresh", worktreeID, 2),
				)
			})
		},
	}
	params, err := json.Marshal(map[string]bool{"drain_publication": true})
	if err != nil {
		t.Fatal(err)
	}
	response, protocolErr := handler.HandleWorkerRequest(ctx, supervisor.Request{
		Method: "checkpoint_barrier", WorktreeID: "worktree", Params: params,
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if wakeErr != nil {
		t.Fatal(wakeErr)
	}
	data, ok := response.(map[string]any)
	if !ok || data["target_events"] != 1 || data["published_events"] != int64(1) ||
		data["remaining_events"] != int64(0) || data["commits_created"] != int64(1) {
		t.Fatalf("backlog drain result=%T %v", response, response)
	}
}

func TestCheckpointBarrierWaitsForMatchingBranchCheckpoint(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, true)
	worktree, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	worktreeID := checkpointpkg.WorktreeID(repo)
	insertCompletedCheckpoint(t, db, "cp-stale-feature", worktreeID, nil)
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE checkpoints SET observed_ref='refs/heads/feature'
WHERE id='cp-stale-feature'`); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "7"); err != nil {
		t.Fatal(err)
	}
	head, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	tree, err := gitpkg.RevParse(ctx, repo, "HEAD^{tree}")
	if err != nil {
		t.Fatal(err)
	}

	wakeCalls := 0
	handler := repositoryWorkerHandler{
		runtimes: map[string]*workerRuntime{"worktree": {
			worktree: worktree, db: db, gate: &sync.RWMutex{},
		}},
		wake: func(string) {
			wakeCalls++
			accepted, _, metaErr := state.MetaGet(
				ctx, db, daemon.MetaKeyProtectionObservationEpoch)
			if metaErr != nil {
				t.Fatal(metaErr)
			}
			if err := errors.Join(
				state.MetaSet(ctx, db, daemon.MetaKeyProtectionCoveredEpoch, accepted),
				state.MetaSet(ctx, db, daemon.MetaKeyProtectionComplete, "true"),
				state.MetaSet(ctx, db, daemon.MetaKeyProtectionCheckpointID,
					"cp-stale-feature"),
			); err != nil {
				t.Fatal(err)
			}
			if wakeCalls != 2 {
				return
			}
			coverage, err := strconv.ParseInt(accepted, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			const checkpointID = "cp-1787439200000-0123456789abcdef"
			checkpoint := state.Checkpoint{
				ID: checkpointID, OperationID: "op-matching-main-checkpoint",
				WorktreeID: worktreeID, Reason: state.CheckpointReasonPoll,
				ObservationEpoch: coverage, CoverageEpoch: coverage,
				ObservedHead: head, ObservedRef: "refs/heads/main",
				TreeOID: tree, CommitOID: head,
				Ref:       "refs/acd/checkpoints/v1/" + worktreeID + "/" + checkpointID,
				CreatedTS: 2,
			}
			if created, err := state.PrepareCheckpoint(
				ctx, db, checkpoint, fixCheckpointTestDigest); err != nil || !created {
				t.Fatalf("prepare matching checkpoint=(%t,%v)", created, err)
			}
			if err := state.CompleteCheckpoint(
				ctx, db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 3); err != nil {
				t.Fatal(err)
			}
		},
	}
	params, err := json.Marshal(map[string]bool{"drain_publication": true})
	if err != nil {
		t.Fatal(err)
	}
	result, protocolErr := handler.HandleWorkerRequest(ctx, supervisor.Request{
		Method: "publication_drain_start", WorktreeID: "worktree", Params: params,
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	data, ok := result.(map[string]any)
	if !ok || data["checkpoint_id"] != "cp-1787439200000-0123456789abcdef" {
		t.Fatalf("publication result=%T %v", result, result)
	}
	if wakeCalls < 2 {
		t.Fatalf("worker wakes=%d want at least 2", wakeCalls)
	}
	if _, ok, err := state.PublicationDrainByCheckpoint(
		ctx, db, "cp-stale-feature"); err != nil || ok {
		t.Fatalf("stale checkpoint drain=(%t,%v)", ok, err)
	}
}

func TestCheckpointBarrierTimeoutReportsRejectedCheckpoint(t *testing.T) {
	repo := materializeTestRepo(t, true)
	worktree, err := gitpkg.ResolveWorktree(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	worktreeID := checkpointpkg.WorktreeID(repo)
	insertCompletedCheckpoint(t, db, "cp-stale-feature", worktreeID, nil)
	if _, err := db.SQL().ExecContext(context.Background(), `
UPDATE checkpoints SET observed_ref='refs/heads/feature'
WHERE id='cp-stale-feature'`); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSet(
		context.Background(), db, daemon.MetaKeyBranchGeneration, "7"); err != nil {
		t.Fatal(err)
	}

	handler := repositoryWorkerHandler{
		runtimes: map[string]*workerRuntime{"worktree": {
			worktree: worktree, db: db, gate: &sync.RWMutex{},
		}},
		wake: func(string) {
			ctx := context.Background()
			accepted, _, metaErr := state.MetaGet(
				ctx, db, daemon.MetaKeyProtectionObservationEpoch)
			if metaErr != nil {
				t.Fatal(metaErr)
			}
			if err := errors.Join(
				state.MetaSet(ctx, db, daemon.MetaKeyProtectionCoveredEpoch, accepted),
				state.MetaSet(ctx, db, daemon.MetaKeyProtectionComplete, "true"),
				state.MetaSet(ctx, db, daemon.MetaKeyProtectionCheckpointID,
					"cp-stale-feature"),
			); err != nil {
				t.Fatal(err)
			}
		},
	}
	params, err := json.Marshal(map[string]bool{"drain_publication": true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, protocolErr := handler.HandleWorkerRequest(ctx, supervisor.Request{
		Method: "publication_drain_start", WorktreeID: "worktree", Params: params,
	})
	if protocolErr == nil || protocolErr.Code != "checkpoint_timeout" ||
		!strings.Contains(protocolErr.Message, `rejected_checkpoint="cp-stale-feature"`) {
		t.Fatalf("checkpoint timeout=%+v", protocolErr)
	}
}

func TestCheckpointBarrierRefusesBranchChangeWhileWaiting(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, true)
	worktree, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "7"); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	handler := repositoryWorkerHandler{
		runtimes: map[string]*workerRuntime{"worktree": {
			worktree: worktree, db: db, gate: &sync.RWMutex{},
		}},
		wake: func(string) {
			once.Do(func() {
				if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo},
					"switch", "-q", "-c", "feature"); err != nil {
					t.Fatal(err)
				}
			})
		},
	}
	params, err := json.Marshal(map[string]bool{"drain_publication": true})
	if err != nil {
		t.Fatal(err)
	}
	_, protocolErr := handler.HandleWorkerRequest(ctx, supervisor.Request{
		Method: "publication_drain_start", WorktreeID: "worktree", Params: params,
	})
	if protocolErr == nil || protocolErr.Code != "publication_needs_action" ||
		!strings.Contains(protocolErr.Message, "branch changed") {
		t.Fatalf("branch-change result=%+v", protocolErr)
	}
	var drains int
	if err := db.ReadSQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM publication_drains`).Scan(&drains); err != nil {
		t.Fatal(err)
	}
	if drains != 0 {
		t.Fatalf("branch change created %d publication drain(s)", drains)
	}
}

func TestCommitAllCheckpointBarrierRejectsStagedIndexBeforeWake(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, true)
	if err := os.WriteFile(filepath.Join(repo, "staged.txt"), []byte("staged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "add", "staged.txt"); err != nil {
		t.Fatal(err)
	}
	worktree, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	headBefore, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	wakeCalls := 0
	handler := repositoryWorkerHandler{
		runtimes: map[string]*workerRuntime{"worktree": {
			worktree: worktree, db: db, gate: &sync.RWMutex{},
		}},
		wake: func(string) {
			wakeCalls++
			if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "commit", "-m", "unexpected wake"); err != nil {
				t.Fatal(err)
			}
		},
	}
	params, err := json.Marshal(map[string]bool{"drain_publication": true})
	if err != nil {
		t.Fatal(err)
	}
	_, protocolErr := handler.HandleWorkerRequest(ctx, supervisor.Request{
		Method: "checkpoint_barrier", WorktreeID: "worktree", Params: params,
	})
	if protocolErr == nil || protocolErr.Code != "publication_needs_action" ||
		!strings.Contains(protocolErr.Message, "staged") {
		t.Fatalf("staged barrier error=%+v", protocolErr)
	}
	if wakeCalls != 0 {
		t.Fatalf("staged barrier woke publication %d time(s)", wakeCalls)
	}
	headAfter, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if string(headAfter) != string(headBefore) {
		t.Fatalf("HEAD changed on staged refusal: before=%q after=%q", headBefore, headAfter)
	}
}

func TestCommitAllCheckpointBarrierConsumesStagedAfterCheckpoint(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, true)
	stagedPath := filepath.Join(repo, "staged.txt")
	wantBody := []byte("staged content remains in the worktree\n")
	if err := os.WriteFile(stagedPath, wantBody, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo},
		"add", "staged.txt"); err != nil {
		t.Fatal(err)
	}
	worktree, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	worktreeID := checkpointpkg.WorktreeID(repo)
	insertCompletedCheckpoint(t, db, "cp-staged-consent", worktreeID, nil)
	alignCheckpointHead(t, db, repo, "cp-staged-consent")
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "7"); err != nil {
		t.Fatal(err)
	}
	var wakeErr error
	var freshCheckpointOnce sync.Once
	handler := repositoryWorkerHandler{
		runtimes: map[string]*workerRuntime{"worktree": {
			worktree: worktree, db: db, gate: &sync.RWMutex{},
		}},
		wake: func(string) {
			freshCheckpointOnce.Do(func() {
				wakeErr = insertFreshBarrierCheckpoint(
					ctx, db, repo, "cp-staged-consent-fresh", worktreeID, 2)
			})
		},
	}
	params, err := json.Marshal(map[string]bool{
		"drain_publication": true, "consume_staged": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, protocolErr := handler.HandleWorkerRequest(ctx, supervisor.Request{
		Method: "publication_drain_start", WorktreeID: "worktree", Params: params,
	}); protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if wakeErr != nil {
		t.Fatal(wakeErr)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo},
		"diff", "--cached", "--quiet"); err != nil {
		t.Fatalf("index remains staged: %v", err)
	}
	if body, err := os.ReadFile(stagedPath); err != nil ||
		!reflect.DeepEqual(body, wantBody) {
		t.Fatalf("worktree body=%q err=%v", body, err)
	}
	drain, ok, err := state.PublicationDrainByCheckpoint(
		ctx, db, "cp-staged-consent-fresh")
	if err != nil || !ok || !drain.StagedConsent || !drain.StagedConsumed {
		t.Fatalf("drain=(%+v,%t,%v)", drain, ok, err)
	}
}

func TestCommitAllDrainDetachAndReattachKeepsSameOperation(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, false)
	worktree, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	worktreeID := checkpointpkg.WorktreeID(repo)
	seqs := insertCompletedCheckpoint(t, db, "cp-detach", worktreeID, []checkpointMemberFixture{{
		State: state.EventStatePending,
	}})
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "7"); err != nil {
		t.Fatal(err)
	}
	drain := state.PublicationDrain{
		ID: "drain-cp-detach", CheckpointID: "cp-detach",
		WorktreeID: worktreeID, BranchRef: "refs/heads/main",
		BranchGeneration: 7, Phase: state.PublicationDrainSemantic,
		TargetEventCount: 1, CreatedTS: 1, UpdatedTS: 1, LastProgressTS: 1,
		EventSeqs: seqs,
	}
	if created, err := state.PreparePublicationDrain(
		ctx, db, drain); err != nil || !created {
		t.Fatalf("prepare drain=(%t,%v)", created, err)
	}
	handler := repositoryWorkerHandler{
		runtimes: map[string]*workerRuntime{"worktree": {
			worktree: worktree, db: db, gate: &sync.RWMutex{},
		}},
		wake: func(string) {},
	}
	detachCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	started, protocolErr := handler.HandleWorkerRequest(detachCtx, supervisor.Request{
		Method: "publication_drain_start", WorktreeID: "worktree",
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	startedData, ok := started.(map[string]any)
	if !ok || startedData["drain_id"] != drain.ID ||
		startedData["publication_drained"] != false {
		t.Fatalf("started result=%T %v", started, started)
	}
	stillActive, err := state.PublicationDrainByID(ctx, db, drain.ID)
	if err != nil || stillActive.Phase != state.PublicationDrainSemantic {
		t.Fatalf("durable after detach=(%+v,%v)", stillActive, err)
	}

	reattachCtx, stopReattach := context.WithTimeout(ctx, 10*time.Second)
	defer stopReattach()
	result, protocolErr := handler.HandleWorkerRequest(reattachCtx, supervisor.Request{
		Method: "publication_drain_start", WorktreeID: "worktree",
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	data, ok := result.(map[string]any)
	if !ok || data["drain_id"] != drain.ID ||
		data["publication_drained"] != false {
		t.Fatalf("reattach result=%T %v", result, result)
	}

	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='published',commit_oid='detach-commit',published_ts=2
WHERE seq=?`, seqs[0]); err != nil {
		t.Fatal(err)
	}
	current, err := state.PublicationDrainByID(ctx, db, drain.ID)
	if err == nil {
		_, err = daemon.UpdatePublicationDrainAfterReplay(
			ctx, db, current, daemon.ReplaySummary{Published: 1}, nil,
			time.Unix(2, 0).UTC())
	}
	if err != nil {
		t.Fatal(err)
	}
	projection, err := state.ReadPublicationDrainProjection(ctx, db.Path())
	if err != nil || projection.Latest == nil ||
		projection.Latest.ID != drain.ID ||
		projection.Latest.Phase != state.PublicationDrainCompleted {
		t.Fatalf("completed projection=(%+v,%v)", projection.Latest, err)
	}
}

func TestCheckpointBarrierReturnsMeasuredFinalDrainProgress(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, true)
	worktree, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	worktreeID := checkpointpkg.WorktreeID(repo)
	seqs := insertCompletedCheckpoint(t, db, "cp-drained", worktreeID, []checkpointMemberFixture{
		{State: state.EventStatePublished, CommitOID: "commit-1"},
		{State: state.EventStatePublished, CommitOID: "commit-2"},
	})
	alignCheckpointHead(t, db, repo, "cp-drained")
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "7"); err != nil {
		t.Fatal(err)
	}
	var wakeErr error
	handler := repositoryWorkerHandler{
		runtimes: map[string]*workerRuntime{"worktree": {
			worktree: worktree, db: db, gate: &sync.RWMutex{},
		}},
		wake: func(string) {
			accepted, _, err := state.MetaGet(ctx, db, daemon.MetaKeyProtectionObservationEpoch)
			if err != nil {
				wakeErr = err
				return
			}
			wakeErr = errors.Join(
				state.MetaSet(ctx, db, daemon.MetaKeyProtectionCoveredEpoch, accepted),
				state.MetaSet(ctx, db, daemon.MetaKeyProtectionComplete, "true"),
				state.MetaSet(ctx, db, daemon.MetaKeyProtectionCheckpointID, "cp-drained"),
			)
		},
	}
	params, err := json.Marshal(map[string]bool{"drain_publication": true})
	if err != nil {
		t.Fatal(err)
	}
	result, protocolErr := handler.HandleWorkerRequest(ctx, supervisor.Request{
		Method: "checkpoint_barrier", WorktreeID: "worktree", Params: params,
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if wakeErr != nil {
		t.Fatal(wakeErr)
	}
	data, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result=%T %v", result, result)
	}
	if data["publication_drained"] != true || data["target_events"] != len(seqs) ||
		data["published_events"] != int64(len(seqs)) || data["remaining_events"] != int64(0) ||
		data["commits_created"] != int64(2) {
		t.Fatalf("drain result=%v", data)
	}
}

type checkpointMemberFixture struct {
	State     string
	CommitOID string
}

func insertCompletedCheckpoint(
	t *testing.T,
	db *state.DB,
	checkpointID string,
	worktreeID string,
	members []checkpointMemberFixture,
) []int64 {
	t.Helper()
	ctx := context.Background()
	operationID := "op-" + checkpointID
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO operations(id,kind,worktree_id,phase,status,created_ts,updated_ts)
VALUES(?, 'checkpoint', ?, 'completed', 'completed', 1, 1)`, operationID, worktreeID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO checkpoints(
 id,seq,operation_id,worktree_id,reason,observation_epoch,coverage_epoch,
 observed_head,observed_ref,tree_oid,commit_oid,checkpoint_ref,phase,created_ts,completed_ts
) VALUES(?,1,?,?,'manual_barrier',1,1,'head','refs/heads/main','tree','commit',?,'completed',1,1)`,
		checkpointID, operationID, worktreeID, "refs/acd/checkpoints/"+checkpointID); err != nil {
		t.Fatal(err)
	}
	seqs := make([]int64, 0, len(members))
	for index, member := range members {
		result, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state,commit_oid
) VALUES('refs/heads/main',7,'head','modify',?,'exact',1,?,NULLIF(?,''))`,
			fmt.Sprintf("member-%d.txt", index), member.State, member.CommitOID)
		if err != nil {
			t.Fatal(err)
		}
		seq, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.SQL().ExecContext(ctx,
			`INSERT INTO checkpoint_events(checkpoint_id,ord,event_seq) VALUES(?,?,?)`, checkpointID, index, seq); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
	}
	return seqs
}

func insertFreshBarrierCheckpoint(
	ctx context.Context,
	db *state.DB,
	repo string,
	checkpointID string,
	worktreeID string,
	seq int64,
) error {
	accepted, _, err := state.MetaGet(
		ctx, db, daemon.MetaKeyProtectionObservationEpoch)
	if err != nil {
		return err
	}
	head, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo},
		"rev-parse", "--verify", "HEAD")
	if err != nil {
		return err
	}
	operationID := "op-" + checkpointID
	_, operationErr := db.SQL().ExecContext(ctx, `
INSERT INTO operations(id,kind,worktree_id,phase,status,created_ts,updated_ts)
VALUES(?, 'checkpoint', ?, 'completed', 'completed', ?, ?)`,
		operationID, worktreeID, seq, seq)
	_, checkpointErr := db.SQL().ExecContext(ctx, `
INSERT INTO checkpoints(
 id,seq,operation_id,worktree_id,reason,observation_epoch,coverage_epoch,
 observed_head,observed_ref,tree_oid,commit_oid,checkpoint_ref,phase,
 created_ts,completed_ts
) VALUES(
 ?,?,?,?,'manual_barrier',?,?,?,'refs/heads/main','tree-fresh','commit-fresh',
 ?,'completed',?,?
)`, checkpointID, seq, operationID, worktreeID, accepted, accepted,
		strings.TrimSpace(string(head)), "refs/acd/checkpoints/"+checkpointID,
		seq, seq)
	return errors.Join(
		operationErr,
		checkpointErr,
		state.MetaSet(ctx, db, daemon.MetaKeyProtectionCoveredEpoch, accepted),
		state.MetaSet(ctx, db, daemon.MetaKeyProtectionComplete, "true"),
		state.MetaSet(ctx, db, daemon.MetaKeyProtectionCheckpointID, checkpointID),
	)
}

func alignCheckpointHead(t *testing.T, db *state.DB, repo, checkpointID string) {
	t.Helper()
	ctx := context.Background()
	head, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo},
		"rev-parse", "--verify", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx,
		`UPDATE checkpoints SET observed_head=? WHERE id=?`,
		strings.TrimSpace(string(head)), checkpointID); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerHintWakesOnlyAddressedWorktree(t *testing.T) {
	ctx := context.Background()
	open := func(name string) *state.DB {
		db, err := state.Open(ctx, filepath.Join(t.TempDir(), name+".db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	first, second := open("first"), open("second")
	woken := make(chan string, 1)
	handler := repositoryWorkerHandler{
		runtimes: map[string]*workerRuntime{
			"worktree-a": {db: first},
			"worktree-b": {db: second},
		},
		wake: func(worktreeID string) { woken <- worktreeID },
	}
	_, protocolErr := handler.HandleWorkerRequest(ctx, supervisor.Request{
		Method: "hint", WorktreeID: "worktree-a",
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if got := <-woken; got != "worktree-a" {
		t.Fatalf("wake target=%q", got)
	}
	if complete, ok, err := state.MetaGet(ctx, first, daemon.MetaKeyProtectionComplete); err != nil || !ok || complete != "false" {
		t.Fatalf("addressed protection complete=%q ok=%t err=%v", complete, ok, err)
	}
	if _, ok, err := state.MetaGet(ctx, second, daemon.MetaKeyProtectionComplete); err != nil || ok {
		t.Fatalf("sibling observation changed: ok=%t err=%v", ok, err)
	}
}

func TestSupervisorWorkerStatusRequiresCanonicalRepositoryIdentity(t *testing.T) {
	handler := repositoryWorkerHandler{repositoryID: "0123456789abcdef"}
	result, protocolErr := handler.HandleWorkerRequest(context.Background(), supervisor.Request{
		Method: "status", RepositoryID: "0123456789abcdef",
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	readiness, ok := result.(supervisor.WorkerReadiness)
	if !ok || !readiness.Ready || readiness.PID != os.Getpid() || readiness.RepositoryID != "0123456789abcdef" {
		t.Fatalf("readiness=%+v", result)
	}
	_, protocolErr = handler.HandleWorkerRequest(context.Background(), supervisor.Request{
		Method: "status", RepositoryID: "fedcba9876543210",
	})
	if protocolErr == nil || protocolErr.Code != "worker_identity_mismatch" {
		t.Fatalf("identity error=%+v", protocolErr)
	}
}

func TestPublicationUnsafeReasonRejectsStagedIndex(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, false)
	if err := os.WriteFile(filepath.Join(repo, "staged.txt"), []byte("staged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "add", "staged.txt"); err != nil {
		t.Fatal(err)
	}
	worktree, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	reason, err := publicationUnsafeReason(ctx, worktree, false)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(reason, "staged") || !strings.Contains(reason, "acd commit-all --yes") {
		t.Fatalf("reason=%q", reason)
	}
}
