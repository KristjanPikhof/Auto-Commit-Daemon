package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func writeFinishTestHold(t *testing.T, runtime *workerRuntime) {
	t.Helper()
	marker := runtime.roots.SetupPublicationHoldPath()
	if err := os.MkdirAll(filepath.Dir(marker), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, []byte("held\n"), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestFinishHeldBarrierRefusesActiveJobBeforeOperationGate(t *testing.T) {
	lookup, runtime, handler := controlLifecycleFixture(t)
	writeFinishTestHold(t, runtime)
	runtime.activeFinishJobs.Store(1)
	before, _, err := state.MetaGet(t.Context(), runtime.db, daemon.MetaKeyProtectionObservationEpoch)
	if err != nil {
		t.Fatal(err)
	}
	runtime.gate.RLock()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	done := make(chan *supervisor.ProtocolError, 1)
	go func() {
		_, failure := handler.HandleWorkerRequest(ctx, supervisor.Request{Method: "checkpoint_barrier", RepositoryID: lookup.Record.RepositoryID, WorktreeID: lookup.Record.WorktreeID})
		done <- failure
	}()
	var failure *supervisor.ProtocolError
	select {
	case failure = <-done:
		runtime.gate.RUnlock()
	case <-ctx.Done():
		runtime.gate.RUnlock()
		<-done
		t.Fatal("held barrier waited on the active job's operation gate")
	}
	if failure == nil || failure.Code != "finish_busy" {
		t.Fatalf("active job accepted during hold: %+v", failure)
	}
	after, _, err := state.MetaGet(t.Context(), runtime.db, daemon.MetaKeyProtectionObservationEpoch)
	if err != nil || after != before {
		t.Fatalf("refused barrier changed observation: %q => %q: %v", before, after, err)
	}
}

func TestFinishHeldBarrierAllowsPreparedWithoutActiveJob(t *testing.T) {
	lookup, runtime, handler := controlLifecycleFixture(t)
	ctx := t.Context()
	if err := os.WriteFile(filepath.Join(lookup.Worktree.Root, "current-work.txt"), []byte("protected target\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := resolveFinishWorkerConfig(ctx, runtime.roots, runtime)
	if err != nil {
		t.Fatal(err)
	}
	worker := daemon.FinishWorker{RepoPath: lookup.Worktree.Root, RepositoryID: lookup.Record.RepositoryID, DB: runtime.db, Config: cfg}
	r, _, err := worker.Start(ctx, daemon.FinishOptions{})
	if err != nil {
		t.Fatal(err)
	}
	proof, err := git.BuildFinishChain(ctx, lookup.Worktree.Root, git.FinishBuildOptions{
		RequestID: r.ID, Attempt: r.Attempt, WorktreeID: r.Target.WorktreeID,
		BranchRef: r.Target.BranchRef, ExpectedHead: r.Target.ExpectedHead,
		TargetTree: r.Target.TargetTree, IndexDigest: r.Target.IndexDigest,
		ObjectRef: git.FinishRefPrefix + r.ID + "/chain",
		Commits:   []git.FinishCommit{{TreeOID: r.Target.TargetTree, Message: "Protect current work"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.PrepareFinishPublication(ctx, runtime.db, r.ID, r.Attempt, proof, json.RawMessage(`{"steps":1}`)); err != nil {
		t.Fatal(err)
	}
	writeFinishTestHold(t, runtime)
	serialized := false
	handler.wake = func(string) {
		if runtime.finishMu.TryLock() {
			runtime.finishMu.Unlock()
		} else {
			serialized = true
		}
		protectBarrierFixture(t, ctx, lookup, runtime)
	}
	requestProtectionBarrier(t, ctx, lookup, handler)
	if !serialized || runtime.activeFinishJobs.Load() != 0 {
		t.Fatal("held barrier did not serialize starts while protecting the worktree")
	}
	saved, err := state.LoadFinishRequest(ctx, runtime.db, r.ID)
	if err != nil || saved.State != state.FinishPrepared {
		t.Fatalf("barrier changed prepared publication: %s: %v", saved.State, err)
	}
	head, err := git.RevParse(ctx, lookup.Worktree.Root, "HEAD")
	index, indexErr := git.IndexContentDigest(ctx, lookup.Worktree.Root)
	if err != nil || indexErr != nil || head != r.Target.ExpectedHead || index != r.Target.IndexDigest {
		t.Fatal("held barrier published prepared work or changed the index")
	}
}
