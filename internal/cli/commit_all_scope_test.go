package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestCommitAllReviewsPathsAddedDuringCheckpoint(t *testing.T) {
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
	head, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	cctx := daemon.CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 7, BaseHead: head}
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "7"); err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.BootstrapShadow(ctx, repo, db, cctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "approved.txt"), []byte("approved\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "add", "approved.txt"); err != nil {
		t.Fatal(err)
	}
	scope, err := inspectCommitAllScope(ctx, repo, db.Path())
	if err != nil {
		t.Fatal(err)
	}
	checker := gitpkg.NewIgnoreChecker(repo)
	defer checker.Close()
	store := checkpointpkg.Store{DB: db}
	var captureErr error
	captures := 0
	checkpointID := ""
	handler := repositoryWorkerHandler{runtimes: map[string]*workerRuntime{"worktree": {worktree: worktree, db: db, gate: &sync.RWMutex{}}}}
	handler.wake = func(string) {
		// The worker has accepted the preview. Only the first request introduces
		// new scope; every requested observation completes through real capture.
		accepted, _, err := state.MetaGet(ctx, db, daemon.MetaKeyProtectionObservationEpoch)
		if err != nil {
			captureErr = err
			return
		}
		covered, _, err := state.MetaGet(ctx, db, daemon.MetaKeyProtectionCoveredEpoch)
		if err != nil {
			captureErr = err
			return
		}
		if accepted == covered {
			return
		}
		if captures == 0 {
			if err := os.WriteFile(filepath.Join(repo, "later.txt"), []byte("later work\n"), 0600); err != nil {
				captureErr = err
				return
			}
		}
		captures++
		epoch, err := strconv.ParseInt(accepted, 10, 64)
		if err != nil {
			captureErr = err
			return
		}
		_, captureErr = daemon.Capture(ctx, repo, db, cctx, daemon.CaptureOpts{
			IgnoreChecker: checker, SensitiveMatcher: state.NewSensitiveMatcher(), GitDir: worktree.GitDir,
			CheckpointStore: &store, WorktreeID: checkpointpkg.WorktreeID(repo),
			CheckpointReason: state.CheckpointReasonPoll, ObservationEpoch: epoch,
		})
		checkpointID, _, _ = state.MetaGet(ctx, db, daemon.MetaKeyProtectionCheckpointID)
	}
	request := func(scope commitAllScope) *supervisor.ProtocolError {
		params, _ := json.Marshal(map[string]any{"consume_staged": true, "preview_digest": scope.Digest})
		_, err := handler.HandleWorkerRequest(ctx, supervisor.Request{Method: "publication_drain_start", WorktreeID: "worktree", Params: params})
		return err
	}
	protocolErr := request(scope)
	if captureErr != nil {
		t.Fatal(captureErr)
	}
	if protocolErr == nil || protocolErr.Code != "plan_changed" {
		t.Fatalf("new path accepted: %v", protocolErr)
	}
	var drains int
	if err := db.ReadSQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM publication_drains").Scan(&drains); err != nil || drains != 0 {
		t.Fatalf("drains=%d err=%v", drains, err)
	}
	index, err := gitpkg.IndexContentDigest(ctx, repo)
	if err != nil || index != scope.IndexDigest {
		t.Fatalf("staging consumed: %s %v", index, err)
	}
	var commit string
	if err := db.ReadSQL().QueryRowContext(ctx, "SELECT commit_oid FROM checkpoints WHERE id=? AND phase='completed'", checkpointID).Scan(&commit); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{"approved.txt": "approved\n", "later.txt": "later work\n"} {
		body, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "show", commit+":"+path)
		if err != nil || string(body) != want {
			t.Fatalf("protected %s=%q err=%v", path, body, err)
		}
	}
	refreshed, err := inspectCommitAllScope(ctx, repo, db.Path())
	if err != nil {
		t.Fatal(err)
	}
	if protocolErr := request(refreshed); protocolErr != nil || captureErr != nil {
		t.Fatalf("refreshed approval: %v capture=%v", protocolErr, captureErr)
	}
	drain, found, err := state.PublicationDrainByCheckpoint(ctx, db, checkpointID)
	if err != nil || !found || !drain.StagedConsumed || drain.TargetEventCount != 2 {
		t.Fatalf("refreshed drain=%+v found=%v err=%v", drain, found, err)
	}
}

func TestCommitAllTargetScopePreservesQueuedRenamesAndChecksNewEndpoints(t *testing.T) {
	ctx := context.Background()
	_, _, db := makeRepoStateDB(t)
	seq := appendFixEvent(t, ctx, db, state.CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: "head", Operation: "rename", Path: "new.txt", OldPath: sql.NullString{String: "old.txt", Valid: true}, Fidelity: "exact", State: state.EventStatePending}, nil)
	target := publicationDrainTarget{EventSeqs: []int64{seq}}
	for _, test := range []struct {
		name  string
		scope commitAllScope
		want  bool
	}{
		{"new destination unapproved", commitAllScope{ChangedPaths: []commitAllPath{{Path: "old.txt"}}}, false},
		{"new source unapproved", commitAllScope{ChangedPaths: []commitAllPath{{Path: "new.txt"}}}, false},
		{"reviewed rename", commitAllScope{ChangedPaths: []commitAllPath{{Path: "new.txt", OldPath: "old.txt"}}}, true},
		{"already queued rename", commitAllScope{QueuedPaths: []string{"new.txt"}, queuedSeqs: []int64{seq}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := commitAllTargetMatchesScope(ctx, db, target, test.scope)
			if err != nil || got != test.want {
				t.Fatalf("matches=%v err=%v want=%v", got, err, test.want)
			}
		})
	}
}
