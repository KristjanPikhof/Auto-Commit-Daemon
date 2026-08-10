package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestWorkerAccessCheckReportsCompletedRepositoryRead(t *testing.T) {
	repo := materializeTestRepo(t, false)
	result := filepath.Join(t.TempDir(), "access.json")
	if err := runWorkerAccessCheck(context.Background(), []string{repo}, result); err != nil {
		t.Fatal(err)
	}
	status, err := supervisor.ReadServiceAccessStatus(result)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != "completed" || status.Target != "" {
		t.Fatalf("status=%+v", status)
	}
}

func TestWorkerAccessCheckIdentifiesUnreadableRepository(t *testing.T) {
	target := t.TempDir()
	result := filepath.Join(t.TempDir(), "access.json")
	if err := runWorkerAccessCheck(context.Background(), []string{target}, result); err == nil {
		t.Fatal("non-repository access check unexpectedly succeeded")
	}
	status, err := supervisor.ReadServiceAccessStatus(result)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != "failed" || status.Target != target || status.Error == "" {
		t.Fatalf("status=%+v", status)
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
	repo := materializeTestRepo(t, false)
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
	seqs := insertCompletedCheckpoint(t, db, checkpointID, []checkpointMemberFixture{
		{State: state.EventStatePublished, CommitOID: "commit-1"},
		{State: state.EventStateRecovered},
		{State: state.EventStateFailed},
	})
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "99"); err != nil {
		t.Fatal(err)
	}

	target, err := freezePublicationDrainTarget(ctx, db, repo, checkpointID, anchor)
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
	if _, err := freezePublicationDrainTarget(ctx, db, repo, checkpointID, anchor); err == nil ||
		!strings.Contains(err.Error(), "branch changed") {
		t.Fatalf("branch switch error=%v", err)
	}
}

func TestFreezeEmptyPublicationDrainTargetUsesPreBarrierGeneration(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, false)
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	insertCompletedCheckpoint(t, db, "cp-empty", nil)
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "99"); err != nil {
		t.Fatal(err)
	}
	target, err := freezePublicationDrainTarget(ctx, db, repo, "cp-empty",
		publicationDrainTarget{BranchRef: "refs/heads/main", Generation: 7})
	if err != nil {
		t.Fatal(err)
	}
	if target.BranchRef != "refs/heads/main" || target.Generation != 7 ||
		len(target.EventSeqs) != 0 || target.EventSeqs == nil {
		t.Fatalf("empty target=%+v", target)
	}
}

func TestCheckpointBarrierDrainsPreBarrierBacklogWithEmptyCheckpoint(t *testing.T) {
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
	insertCompletedCheckpoint(t, db, "cp-empty-backlog", nil)
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
			_, publishErr := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='published',commit_oid='backlog-commit' WHERE seq=?`, backlogSeq)
			wakeErr = errors.Join(
				publishErr,
				state.MetaSet(ctx, db, daemon.MetaKeyProtectionCoveredEpoch, accepted),
				state.MetaSet(ctx, db, daemon.MetaKeyProtectionComplete, "true"),
				state.MetaSet(ctx, db, daemon.MetaKeyProtectionCheckpointID, "cp-empty-backlog"),
			)
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

func TestCheckpointBarrierReturnsMeasuredFinalDrainProgress(t *testing.T) {
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
	seqs := insertCompletedCheckpoint(t, db, "cp-drained", []checkpointMemberFixture{
		{State: state.EventStatePublished, CommitOID: "commit-1"},
		{State: state.EventStatePublished, CommitOID: "commit-2"},
	})
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
	members []checkpointMemberFixture,
) []int64 {
	t.Helper()
	ctx := context.Background()
	operationID := "op-" + checkpointID
	const worktreeID = "0123456789abcdef"
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
	reason, err := publicationUnsafeReason(ctx, worktree)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(reason, "staged") || !strings.Contains(reason, "acd commit-all --yes") {
		t.Fatalf("reason=%q", reason)
	}
}
