package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type productRecoveryTestResult struct {
	OK      bool    `json:"ok"`
	Changed bool    `json:"changed"`
	Data    fixPlan `json:"data"`
}

func runProductRecoveryJSON(t *testing.T, repo string, flags ...string) productRecoveryTestResult {
	t.Helper()
	var out, stderr bytes.Buffer
	command := newRootCmd()
	command.SetOut(&out)
	command.SetErr(&stderr)
	command.SetArgs(append([]string{"support", "recover", "--repo", repo, "--json"}, flags...))
	if err := command.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("support recover: %v\n%s\n%s", err, &out, &stderr)
	}
	var result productRecoveryTestResult
	decoder := json.NewDecoder(&out)
	if err := decoder.Decode(&result); err != nil {
		t.Fatalf("decode recovery: %v\n%s", err, &out)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		t.Fatalf("extra recovery output: %v", err)
	}
	if !result.OK {
		t.Fatalf("recovery failed: %+v", result)
	}
	return result
}

func TestProductRecoveryJSONReportsArchiveChange(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	if result := runProductRecoveryJSON(t, repo, "--yes"); result.Changed || len(result.Data.Actions) != 0 {
		t.Fatalf("no-op reported a change: %+v", result)
	}
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	if _, err := db.SQL().ExecContext(ctx, "UPDATE capture_events SET state='pending',error=NULL WHERE seq=?", first); err != nil {
		t.Fatal(err)
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatal(err)
	}
	preview := runProductRecoveryJSON(t, repo, "--force", "--yes", "--dry-run")
	if preview.Changed || !preview.Data.DryRun || findFixAction(preview.Data, fixActionReconcileUnpublishedChain) == nil {
		t.Fatalf("preview reported a change: %+v", preview)
	}
	if after, err := fileSHA256(stateDB); err != nil || after != before {
		t.Fatalf("preview changed state: before=%s after=%s err=%v", before, after, err)
	}
	applied := runProductRecoveryJSON(t, repo, "--force", "--yes")
	action := findFixAction(applied.Data, fixActionReconcileUnpublishedChain)
	if !applied.Changed || applied.Data.RowsChanged != 2 || action == nil || !action.Applied || action.RecoveryRef == "" {
		t.Fatalf("archive change not reported: %+v", applied)
	}
	for _, seq := range []int64{first, second} {
		assertFixEventState(t, ctx, db, seq, state.EventStateRecovered)
	}
	if got, err := git.Run(ctx, git.RunOpts{Dir: repo}, "show", action.RecoveryRef+":second-recovery.txt"); err != nil || string(got) != "second captured\n" {
		t.Fatalf("archive lost captured contents: %q err=%v", got, err)
	}
	if after, err := git.RevParse(ctx, repo, "HEAD"); err != nil || after != head {
		t.Fatalf("archive changed HEAD: before=%s after=%s err=%v", head, after, err)
	}
	if result := runProductRecoveryJSON(t, repo, "--force", "--yes"); result.Changed || result.Data.RowsChanged != 0 {
		t.Fatalf("repeated recovery reported a change: %+v", result)
	}
}

func TestProductRecoveryJSONReportsPauseRemoval(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	markerPath := filepath.Join(repo, ".git", "acd", "paused")
	if _, err := pausepkg.Write(markerPath, pausepkg.Marker{
		Reason: "maintenance", SetAt: time.Now().UTC().Format(time.RFC3339), SetBy: "test",
	}, true); err != nil {
		t.Fatal(err)
	}
	preview := runProductRecoveryJSON(t, repo, "--clear-pause", "--dry-run")
	if preview.Changed || preview.Data.ManualPauseRemoved {
		t.Fatalf("pause preview reported a change: %+v", preview)
	}
	applied := runProductRecoveryJSON(t, repo, "--clear-pause", "--yes")
	if !applied.Changed || !applied.Data.ManualPauseRemoved || applied.Data.RowsChanged != 0 {
		t.Fatalf("pause-only change not reported: %+v", applied)
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("pause marker remains: %v", err)
	}
}
