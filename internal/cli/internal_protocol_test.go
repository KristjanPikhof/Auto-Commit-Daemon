package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

func TestSupervisorRejectsRetiredPublicationRequests(t *testing.T) {
	handler := cliSupervisorHandler{roots: withIsolatedHome(t)}
	for _, method := range []string{"publication_drain_start", "publication_drain_status"} {
		data, failure := handler.HandleSupervisorRequest(context.Background(), supervisor.Request{
			Version: supervisor.ProtocolVersion, ID: "retired", Method: method,
		})
		if data != nil || failure == nil || failure.Code != "invalid_request" || failure.Retryable {
			t.Fatalf("retired method %s forwarded: data=%v error=%+v", method, data, failure)
		}
	}
}

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

func freshProtectionBarrierFixture(t *testing.T) (controlRepoLookup, *workerRuntime, *repositoryWorkerHandler) {
	t.Helper()
	repo := materializeTestRepo(t, true)
	worktree, err := gitpkg.ResolveWorktree(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(t.Context(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	record := central.RepoRecord{RepositoryID: "barrier-repository", WorktreeID: checkpointpkg.WorktreeID(worktree.Root)}
	lookup := controlRepoLookup{Record: record, Worktree: worktree}
	runtime := &workerRuntime{record: record, worktree: worktree, db: db, gate: &sync.RWMutex{}}
	handler := &repositoryWorkerHandler{repositoryID: record.RepositoryID, runtimes: map[string]*workerRuntime{record.WorktreeID: runtime}}
	return lookup, runtime, handler
}

func protectBarrierFixture(t *testing.T, ctx context.Context, lookup controlRepoLookup, runtime *workerRuntime) daemon.CaptureSummary {
	t.Helper()
	branch, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: lookup.Worktree.Root}, "symbolic-ref", "-q", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	head, err := gitpkg.RevParse(ctx, lookup.Worktree.Root, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	summary, err := daemon.ProtectWorktree(ctx, lookup.Worktree.Root, runtime.db, daemon.CaptureContext{BranchRef: strings.TrimSpace(string(branch)), BaseHead: head}, daemon.CaptureOpts{CheckpointStore: &checkpointpkg.Store{DB: runtime.db}, WorktreeID: lookup.Record.WorktreeID})
	if err != nil || !summary.Protected || summary.Partial {
		t.Fatalf("protect %+v: %v", summary, err)
	}
	return summary
}

func requestProtectionBarrier(t *testing.T, ctx context.Context, lookup controlRepoLookup, handler *repositoryWorkerHandler) map[string]any {
	t.Helper()
	result, protocolErr := handler.HandleWorkerRequest(ctx, supervisor.Request{Method: "checkpoint_barrier", WorktreeID: lookup.Record.WorktreeID, RepositoryID: lookup.Record.RepositoryID})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	data, ok := result.(map[string]any)
	if !ok || data["protected"] != true || data["checkpoint_id"] == "" || len(data) != 2 {
		t.Fatalf("protection result=%T %v", result, result)
	}
	return data
}

func assertBarrierNeverPublishes(t *testing.T, lookup controlRepoLookup, runtime *workerRuntime, head, index string) {
	t.Helper()
	current, err := gitpkg.RevParse(t.Context(), lookup.Worktree.Root, "HEAD")
	if err != nil || current != head {
		t.Fatalf("branch changed %s: %v", current, err)
	}
	currentIndex, err := gitpkg.IndexContentDigest(t.Context(), lookup.Worktree.Root)
	if err != nil || currentIndex != index {
		t.Fatalf("index changed %s: %v", currentIndex, err)
	}
	var requests int
	if err := runtime.db.ReadSQL().QueryRow(`SELECT COUNT(*) FROM finish_requests`).Scan(&requests); err != nil || requests != 0 {
		t.Fatalf("finish requests=%d: %v", requests, err)
	}
	for _, table := range []string{"capture_events", "publication_drains"} {
		exists, err := state.LegacyTableExists(t.Context(), runtime.db.ReadSQL(), table)
		if err != nil || exists {
			t.Fatalf("retired %s exists=%t: %v", table, exists, err)
		}
	}
}

func TestCheckpointBarrierProtectsPreBarrierSavesWithoutPublishing(t *testing.T) {
	lookup, runtime, handler := freshProtectionBarrierFixture(t)
	head, _ := gitpkg.RevParse(t.Context(), lookup.Worktree.Root, "HEAD")
	index, _ := gitpkg.IndexContentDigest(t.Context(), lookup.Worktree.Root)
	for _, contents := range []string{"first save", "completed save"} {
		if err := os.WriteFile(filepath.Join(lookup.Worktree.Root, "backlog.txt"), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		protectBarrierFixture(t, t.Context(), lookup, runtime)
	}
	handler.wake = func(string) { protectBarrierFixture(t, t.Context(), lookup, runtime) }
	data := requestProtectionBarrier(t, t.Context(), lookup, handler)
	projection, err := state.ReadCheckpointProjection(t.Context(), runtime.db.Path(), 1)
	if err != nil || projection.Latest == nil || projection.Latest.ID != data["checkpoint_id"] {
		t.Fatalf("projection %+v: %v", projection, err)
	}
	contents, err := gitpkg.Run(t.Context(), gitpkg.RunOpts{Dir: lookup.Worktree.Root}, "show", projection.Latest.CommitOID+":backlog.txt")
	if err != nil || string(contents) != "completed save" {
		t.Fatalf("protected contents=%q: %v", contents, err)
	}
	assertBarrierNeverPublishes(t, lookup, runtime, head, index)
}

func TestCheckpointBarrierWaitsForFreshMatchingCoverage(t *testing.T) {
	lookup, runtime, handler := freshProtectionBarrierFixture(t)
	prior := protectBarrierFixture(t, t.Context(), lookup, runtime)
	head, _ := gitpkg.RevParse(t.Context(), lookup.Worktree.Root, "HEAD")
	index, _ := gitpkg.IndexContentDigest(t.Context(), lookup.Worktree.Root)
	var fresh daemon.CaptureSummary
	handler.wake = func(string) { fresh = protectBarrierFixture(t, t.Context(), lookup, runtime) }
	data := requestProtectionBarrier(t, t.Context(), lookup, handler)
	if data["checkpoint_id"] != fresh.CheckpointID {
		t.Fatalf("stale checkpoint returned: %v prior=%s fresh=%s", data, prior.CheckpointID, fresh.CheckpointID)
	}
	accepted, _, err := state.MetaGet(t.Context(), runtime.db, daemon.MetaKeyProtectionObservationEpoch)
	if err != nil {
		t.Fatal(err)
	}
	covered, _, err := state.MetaGet(t.Context(), runtime.db, daemon.MetaKeyProtectionCoveredEpoch)
	if err != nil || accepted != covered {
		t.Fatalf("accepted=%s covered=%s: %v", accepted, covered, err)
	}
	if _, protocolErr := handler.HandleWorkerRequest(t.Context(), supervisor.Request{Method: "checkpoint_barrier", WorktreeID: "foreign-worktree"}); protocolErr == nil {
		t.Fatal("foreign worktree accepted")
	}
	assertBarrierNeverPublishes(t, lookup, runtime, head, index)
}

func TestCheckpointBarrierTimeoutReportsIncompleteCoverage(t *testing.T) {
	lookup, runtime, handler := freshProtectionBarrierFixture(t)
	prior := protectBarrierFixture(t, t.Context(), lookup, runtime)
	handler.wake = func(string) {} // A stalled worker cannot satisfy the newly accepted observation.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, protocolErr := handler.HandleWorkerRequest(ctx, supervisor.Request{Method: "checkpoint_barrier", WorktreeID: lookup.Record.WorktreeID})
	if protocolErr == nil || protocolErr.Code != "checkpoint_timeout" || !protocolErr.Retryable || !strings.Contains(protocolErr.Message, "accepted_epoch=") {
		t.Fatalf("checkpoint timeout=%+v", protocolErr)
	}
	projection, err := state.ReadCheckpointProjection(t.Context(), runtime.db.Path(), 1)
	if err != nil || projection.Latest == nil || projection.Latest.ID != prior.CheckpointID {
		t.Fatalf("lost prior protection: %+v %v", projection, err)
	}
}

func TestCheckpointBarrierGitFailurePreservesTimeoutEvidence(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%t", deadline), func(t *testing.T) {
			lookup, runtime, handler := freshProtectionBarrierFixture(t)
			realGit, err := exec.LookPath("git")
			if err != nil {
				t.Fatal(err)
			}
			binDir := t.TempDir()
			marker := filepath.Join(binDir, "observed")
			quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }
			body := fmt.Sprintf(`#!/bin/sh
if [ "$1" = symbolic-ref ]; then
 echo entered > %s
 if [ %t = true ]; then exec sleep 10; fi
 echo branch-probe-failed >&2
 exit 42
fi
exec %s "$@"
`, quote(marker), deadline, quote(realGit))
			if err := os.WriteFile(filepath.Join(binDir, "git"), []byte(body), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			handler.wake = func(string) {
				_, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: lookup.Worktree.Root}, "symbolic-ref", "-q", "HEAD")
				if err == nil {
					t.Fatal("Git rejection did not run")
				}
				if healthErr := state.RecordCaptureHealth(context.Background(), runtime.db, err.Error(), "", time.Now()); healthErr != nil {
					t.Fatal(healthErr)
				}
			}
			_, protocolErr := handler.HandleWorkerRequest(ctx, supervisor.Request{Method: "checkpoint_barrier", WorktreeID: lookup.Record.WorktreeID})
			if _, err := os.Stat(marker); err != nil {
				t.Fatalf("actual Git probe not reached: %v", err)
			}
			if protocolErr == nil || protocolErr.Code != "checkpoint_timeout" || !protocolErr.Retryable || !strings.Contains(protocolErr.Message, "accepted_epoch=") {
				t.Fatalf("lost barrier evidence: %+v", protocolErr)
			}
			health, err := state.ReadCaptureHealth(t.Context(), runtime.db.ReadSQL())
			if err != nil || health.State != "blocked" || health.Error == "" {
				t.Fatalf("lost Git health %+v: %v", health, err)
			}
			if !deadline && !strings.Contains(health.Error, "branch-probe-failed") {
				t.Fatalf("Git rejection hidden: %+v", health)
			}
			if deadline && ctx.Err() != context.DeadlineExceeded {
				t.Fatalf("Git did not honor deadline: %v", ctx.Err())
			}
		})
	}
}

func TestCheckpointBarrierProtectsBranchChangeWithoutPublishing(t *testing.T) {
	lookup, runtime, handler := freshProtectionBarrierFixture(t)
	protectBarrierFixture(t, t.Context(), lookup, runtime)
	head, _ := gitpkg.RevParse(t.Context(), lookup.Worktree.Root, "HEAD")
	index, _ := gitpkg.IndexContentDigest(t.Context(), lookup.Worktree.Root)
	handler.wake = func(string) {
		if _, err := gitpkg.Run(t.Context(), gitpkg.RunOpts{Dir: lookup.Worktree.Root}, "switch", "-q", "-c", "feature"); err != nil {
			t.Fatal(err)
		}
		protectBarrierFixture(t, t.Context(), lookup, runtime)
	}
	data := requestProtectionBarrier(t, t.Context(), lookup, handler)
	projection, err := state.ReadCheckpointProjection(t.Context(), runtime.db.Path(), 1)
	if err != nil || projection.Latest == nil || projection.Latest.ID != data["checkpoint_id"] || projection.Latest.ObservedRef != "refs/heads/feature" {
		t.Fatalf("branch protection %+v: %v", projection, err)
	}
	assertBarrierNeverPublishes(t, lookup, runtime, head, index)
}

func TestFinishStagedRefusalPrecedesWakeAndDurableRequest(t *testing.T) {
	lookup, runtime, handler := finishCLIFixture(t)
	path := filepath.Join(lookup.Worktree.Root, "staged.txt")
	if err := os.WriteFile(path, []byte("staged\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(t.Context(), gitpkg.RunOpts{Dir: lookup.Worktree.Root}, "add", "staged.txt"); err != nil {
		t.Fatal(err)
	}
	head, _ := gitpkg.RevParse(t.Context(), lookup.Worktree.Root, "HEAD")
	index, _ := gitpkg.IndexContentDigest(t.Context(), lookup.Worktree.Root)
	wakes := 0
	handler.wake = func(string) { wakes++ }
	params, err := json.Marshal(finishStartParams{Purpose: "Complete staged goal"})
	if err != nil {
		t.Fatal(err)
	}
	_, protocolErr := handler.HandleWorkerRequest(t.Context(), supervisor.Request{Method: "finish_start", RepositoryID: lookup.Record.RepositoryID, WorktreeID: lookup.Record.WorktreeID, Params: params})
	if protocolErr == nil || !strings.Contains(protocolErr.Message, "staged") {
		t.Fatalf("staged refusal=%+v", protocolErr)
	}
	if wakes != 0 {
		t.Fatalf("staged refusal woke worker %d times", wakes)
	}
	after, err := gitpkg.RevParse(t.Context(), lookup.Worktree.Root, "HEAD")
	if err != nil || after != head {
		t.Fatalf("refusal changed HEAD: %v", err)
	}
	afterIndex, err := gitpkg.IndexContentDigest(t.Context(), lookup.Worktree.Root)
	if err != nil || afterIndex != index {
		t.Fatalf("refusal changed index: %v", err)
	}
	var requests int
	if err := runtime.db.ReadSQL().QueryRow(`SELECT COUNT(*) FROM finish_requests`).Scan(&requests); err != nil || requests != 0 {
		t.Fatalf("refusal created requests=%d: %v", requests, err)
	}
	body, err := os.ReadFile(path)
	if err != nil || string(body) != "staged\n" {
		t.Fatalf("refusal changed bytes %q: %v", body, err)
	}
}

func TestFinishStagedConsentPublishesExactBytesAndPreservesIndexBytes(t *testing.T) {
	lookup, runtime, handler := finishCLIFixture(t)
	serveIntentHistoryWorker(t, lookup, handler)
	path := filepath.Join(lookup.Worktree.Root, "staged.txt")
	body := "staged content remains in the worktree\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(t.Context(), gitpkg.RunOpts{Dir: lookup.Worktree.Root}, "add", "staged.txt"); err != nil {
		t.Fatal(err)
	}
	index, _ := gitpkg.IndexContentDigest(t.Context(), lookup.Worktree.Root)
	result, _, err := finishCLIExecute(t, t.Context(), lookup.Worktree.Root, "finish", "Complete staged goal", "--include-staged")
	if err != nil || result.State != state.FinishCompleted || !result.Protected || !result.Published || !result.StagedConsumed {
		t.Fatalf("staged finish %+v: %v", result, err)
	}
	afterIndex, err := gitpkg.IndexContentDigest(t.Context(), lookup.Worktree.Root)
	if err != nil || afterIndex != index {
		t.Fatalf("staged consent consumed index: %v", err)
	}
	if _, err := gitpkg.Run(t.Context(), gitpkg.RunOpts{Dir: lookup.Worktree.Root}, "diff", "--cached", "--quiet"); err != nil {
		t.Fatalf("consented staging remains unresolved: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != body {
		t.Fatalf("staged consent changed bytes %q: %v", after, err)
	}
	published, err := gitpkg.Run(t.Context(), gitpkg.RunOpts{Dir: lookup.Worktree.Root}, "show", "HEAD:staged.txt")
	if err != nil || string(published) != body {
		t.Fatalf("published bytes %q: %v", published, err)
	}
	saved, err := state.LatestFinishRequest(t.Context(), runtime.db, lookup.Record.WorktreeID)
	if err != nil || !saved.Target.IncludeStaged || saved.Target.CheckpointID == "" {
		t.Fatalf("missing approved frozen checkpoint %+v: %v", saved, err)
	}
}

func TestCheckpointBarrierReportsProtectedCheckpointWithoutPublicationClaims(t *testing.T) {
	lookup, runtime, handler := freshProtectionBarrierFixture(t)
	prior := protectBarrierFixture(t, t.Context(), lookup, runtime)
	head, _ := gitpkg.RevParse(t.Context(), lookup.Worktree.Root, "HEAD")
	index, _ := gitpkg.IndexContentDigest(t.Context(), lookup.Worktree.Root)
	handler.wake = func(string) { protectBarrierFixture(t, t.Context(), lookup, runtime) }
	data := requestProtectionBarrier(t, t.Context(), lookup, handler)
	projection, err := state.ReadCheckpointProjection(t.Context(), runtime.db.Path(), 1)
	if err != nil || projection.Latest == nil || projection.Latest.ID != data["checkpoint_id"] || projection.Latest.TreeOID == "" {
		t.Fatalf("protection %+v: %v", projection, err)
	}
	tree, err := gitpkg.RevParse(t.Context(), lookup.Worktree.Root, "HEAD^{tree}")
	if err != nil || projection.Latest.TreeOID != tree {
		t.Fatalf("unchanged checkpoint tree differs: %v", err)
	}

	if prior.CheckpointID == "" {
		t.Fatal("missing prior checkpoint")
	}
	assertBarrierNeverPublishes(t, lookup, runtime, head, index)
}

func insertFreshBarrierCheckpoint(
	ctx context.Context,
	db *state.DB,
	repo string,
	checkpointID string,
	worktreeID string,
	seq int64,
	members ...int64,
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
	errs := []error{
		operationErr,
		checkpointErr,
		state.MetaSet(ctx, db, daemon.MetaKeyProtectionCoveredEpoch, accepted),
		state.MetaSet(ctx, db, daemon.MetaKeyProtectionComplete, "true"),
		state.MetaSet(ctx, db, daemon.MetaKeyProtectionCheckpointID, checkpointID),
	}
	for index, eventSeq := range members {
		_, memberErr := db.SQL().ExecContext(ctx,
			`INSERT INTO checkpoint_events(checkpoint_id,ord,event_seq) VALUES(?,?,?)`,
			checkpointID, index, eventSeq)
		errs = append(errs, memberErr)
	}
	return errors.Join(errs...)
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
