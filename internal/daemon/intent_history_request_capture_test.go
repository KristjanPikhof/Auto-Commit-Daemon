package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

func TestFinishHistoryRewriteProtectsLaterWorkDuringFrozenVerification(t *testing.T) {
	t.Parallel()
	f, w := finishWorkerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	oldA := mustCommitPath(t, f.dir, "a.txt", "alpha\n", "Add alpha")
	oldB := mustCommitPath(t, f.dir, "b.txt", "beta\n", "Add beta")
	f.cctx.BaseHead = oldB
	plan, err := PlanFinishHistory(ctx, f.dir, f.cctx.BranchRef, []string{oldA, oldB}, nil, nil, FinishHistoryOptions{Mode: "local", Style: "imperative", VerificationConfigDigest: w.Config.ConfigDigest})
	if err != nil {
		t.Fatal(err)
	}
	plan.TargetBranchRef = "refs/heads/semantic-history"
	plan, err = state.SaveIntentHistoryPlan(ctx, f.db, plan)
	if err != nil {
		t.Fatal(err)
	}
	control := t.TempDir()
	started, release := filepath.Join(control, "started"), filepath.Join(control, "release")
	command, err := verification.NewApprovedCommand(f.dir, w.Config.ConfigDigest, verification.ModeFast, "touch '"+started+"'; while [ ! -f '"+release+"' ]; do sleep 0.05; done", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	wake := make(chan struct{}, 1)
	w.Config.VerificationMode = "fast"
	w.Config.Command = command
	w.Wake = wake
	w.EvaluationInterval = 25 * time.Millisecond
	request := historyStartTest(t, w, plan, false)
	jobCtx, stopJob := context.WithCancel(ctx)
	defer stopJob()
	done := make(chan error, 1)
	go func() {
		_, err := w.ExecuteFinishHistoryRequest(jobCtx, plan, request)
		done <- err
		close(done)
	}()
	t.Cleanup(func() {
		stopJob()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
	})
	waitFor(t, 5*time.Second, "historical goal verification started", func() bool { _, err := os.Stat(started); return err == nil })
	if err := os.WriteFile(filepath.Join(f.dir, "later.txt"), []byte("later protected work\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	wake <- struct{}{}
	waitFor(t, 5*time.Second, "later edit protected during historical verification", func() bool {
		projection, err := state.ReadCheckpointProjection(ctx, f.db.Path(), 1)
		if err != nil || projection.Latest == nil {
			return false
		}
		_, err = git.RevParse(ctx, f.dir, projection.Latest.CommitOID+":later.txt")
		return err == nil
	})
	if _, err := git.RevParse(ctx, f.dir, plan.TargetBranchRef); err == nil {
		t.Fatal("reconstructed branch published before verification completed")
	}
	if err := os.WriteFile(release, []byte("continue"), 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	request, ok, err := state.LoadFinishHistoryRequest(ctx, f.db, plan.ID)
	if err != nil || !ok || request.Status != "completed" {
		t.Fatalf("request=%+v err=%v", request, err)
	}
	if head, _ := git.RevParse(ctx, f.dir, "HEAD"); head != oldB {
		t.Fatal("explicit reconstruction moved the source branch")
	}
	if _, err := git.RevParse(ctx, f.dir, plan.TargetBranchRef+":later.txt"); err == nil {
		t.Fatal("later protected edit entered the frozen history")
	}
	later, err := os.ReadFile(filepath.Join(f.dir, "later.txt"))
	if err != nil || string(later) != "later protected work\n" {
		t.Fatal("later live work was replaced")
	}
}
