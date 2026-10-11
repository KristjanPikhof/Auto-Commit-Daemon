package cli

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestFinishRemovalCheckpointHintPreservesArchivedActivity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params string
	}{
		{"setup readiness", ""},
		{"compatible upgrade", `{"kind":"checkpoint","drain_publication":false}`},
		{"logical hook", `{"kind":"logical_boundary"}`},
		{"session checkpoint", `{"kind":"checkpoint","session_id":"editor","harness":"codex"}`},
		{"explicit publication", `{"drain_publication":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			repo, _, db := makeSeededRepoStateDB(t)
			wt, err := gitpkg.ResolveWorktree(ctx, repo)
			if err != nil {
				t.Fatal(err)
			}
			old := time.Now().Add(-2 * time.Hour).Unix()
			if err := state.MetaSet(ctx, db, "activity.last_event_ts", strconv.FormatInt(old, 10)); err != nil {
				t.Fatal(err)
			}
			if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "1"); err != nil {
				t.Fatal(err)
			}
			var wakeErr error
			var once sync.Once
			handler := repositoryWorkerHandler{
				runtimes: map[string]*workerRuntime{"worktree": {worktree: wt, db: db, gate: &sync.RWMutex{}}},
				wake: func(string) {
					once.Do(func() {
						wakeErr = insertFreshBarrierCheckpoint(ctx, db, repo, "cp-readiness", checkpointpkg.WorktreeID(repo), 1)
					})
				},
			}
			response, requestErr := handler.HandleWorkerRequest(ctx, supervisor.Request{Method: "checkpoint_barrier", WorktreeID: "worktree", Params: json.RawMessage(tc.params)})
			if requestErr != nil || wakeErr != nil {
				t.Fatalf("barrier: %v wake: %v", requestErr, wakeErr)
			}
			data, ok := response.(map[string]any)
			if !ok || data["protected"] != true {
				t.Fatalf("checkpoint was not protected: %#v", response)
			}
			value, _, err := state.MetaGet(ctx, db, "activity.last_event_ts")
			if err != nil {
				t.Fatal(err)
			}
			got, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			if got != old {
				t.Fatalf("checkpoint hint rewrote archived activity: got=%d old=%d", got, old)
			}
		})
	}
}
