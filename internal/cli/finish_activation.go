package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// The canonical owner calls this before serving requests. Recovery failures
// require finish attention; they never prevent the checkpoint loop starting.
func activateWorkerFinish(ctx context.Context, out io.Writer, repo, repositoryID, worktreeID string, db *state.DB, publicationHeld bool) {
	attention := func(err error) {
		if err != nil {
			fmt.Fprintf(out, "An interrupted finish needs review for %s. Protection continues.\nDetails: %v\n", repo, err)
		}
	}
	attention(state.InterruptFinishHistoryRequests(ctx, db, worktreeID))
	_, interruptErr := state.InterruptFinishRequests(ctx, db, worktreeID)
	attention(interruptErr)
	if publicationHeld {
		return
	}
	worker := daemon.FinishWorker{RepoPath: repo, RepositoryID: repositoryID, DB: db}
	_, err := worker.RecoverPrepared(ctx)
	attention(err)
}
