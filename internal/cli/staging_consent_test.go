package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestAdmittedCommitAllPreservesStagingChangedDuringCheckpoint(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, true)
	path := filepath.Join(repo, "staged.txt")
	stage := func(body string) error {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			return err
		}
		_, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "add", "staged.txt")
		return err
	}
	if err := stage("approved"); err != nil {
		t.Fatal(err)
	}
	approved, err := gitpkg.IndexContentDigest(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	worktreeID := checkpointpkg.WorktreeID(repo)
	insertCompletedCheckpoint(t, db, "cp-before", worktreeID, nil)
	alignCheckpointHead(t, db, repo, "cp-before")
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "7"); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	var wakeErr error
	var changed string
	handler := repositoryWorkerHandler{runtimes: map[string]*workerRuntime{"worktree": {worktree: worktree, db: db, gate: &sync.RWMutex{}}}, wake: func(string) {
		once.Do(func() {
			// The request has passed its approval check; its checkpoint cannot
			// complete until this intervening user staging change has happened.
			wakeErr = stage("new unapproved staging")
			if wakeErr == nil {
				changed, wakeErr = gitpkg.IndexContentDigest(ctx, repo)
			}
			if wakeErr == nil {
				wakeErr = insertFreshBarrierCheckpoint(ctx, db, repo, "cp-after", worktreeID, 2)
			}
		})
	}}
	params, _ := json.Marshal(map[string]bool{"drain_publication": true, "consume_staged": true})
	_, protocolErr := handler.HandleWorkerRequest(ctx, supervisor.Request{Method: "publication_drain_start", WorktreeID: "worktree", Params: params})
	if wakeErr != nil {
		t.Fatal(wakeErr)
	}
	if protocolErr == nil || !strings.Contains(protocolErr.Message, "staging changed") {
		t.Fatalf("protocol error=%v", protocolErr)
	}
	drain, ok, err := state.PublicationDrainByCheckpoint(ctx, db, "cp-after")
	if err != nil || !ok || drain.ExpectedIndexDigest != approved || drain.StagedConsumed || drain.Phase != state.PublicationDrainNeedsAction {
		t.Fatalf("drain=%+v exists=%v err=%v", drain, ok, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	persisted, err := state.PublicationDrainByID(ctx, reopened, drain.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.ResumePublicationDrainCheckpointing(ctx, repo, reopened, persisted, time.Now()); err != nil {
		t.Fatal(err)
	}
	after, err := gitpkg.IndexContentDigest(ctx, repo)
	if err != nil || after != changed {
		t.Fatalf("restart consumed new staging: digest=%s err=%v", after, err)
	}
}
