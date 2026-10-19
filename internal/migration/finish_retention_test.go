package migration

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishRetentionLegacySavedSelectionSurvivesGCAndRetry(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	if err := gitpkg.Init(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, state.DBPathFromGitDir(wt.GitDir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tree, err := gitpkg.WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	var commits []string
	for _, message := range []string{"saved source", "old source unit", "unchanged author provenance"} {
		oid, err := gitpkg.CommitTreeDurable(ctx, repo, tree, message, "Original Author", "original@example.com")
		if err != nil {
			t.Fatal(err)
		}
		commits = append(commits, oid)
	}
	// Version-one previews predate SourceRef; no database capture refers to them.
	plan := state.IntentHistoryPlan{Version: 1, ID: "a-old", ExpectedHead: commits[0], SourceChain: []string{commits[1]}, RecreateUnchanged: []string{commits[2]}}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if err = state.MetaSetMany(ctx, db, map[string]string{"intent.history.plan.a-old": string(raw), "intent.history.plan.0broken": "{"}); err != nil {
		t.Fatal(err)
	}
	failed, err := ActivateFinish(ctx, repo, db)
	if !errors.Is(err, ErrFinishLegacyAttention) || failed.State != "needs_attention" || failed.ReasonCode != "legacy_inventory_invalid" {
		t.Fatalf("invalid inventory %+v %v", failed, err)
	}
	if _, err = gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "gc", "--prune=now"); err != nil {
		t.Fatal(err)
	}
	for _, oid := range commits {
		if _, err = gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "cat-file", "-e", oid+"^{commit}"); err != nil {
			t.Fatalf("saved original commit lost: %s %v", oid, err)
		}
	}
	if _, err = db.SQL().ExecContext(ctx, "DELETE FROM daemon_meta WHERE key='intent.history.plan.0broken'"); err != nil {
		t.Fatal(err)
	}
	completed, err := ActivateFinish(ctx, repo, db)
	if err != nil || completed.State != "completed" {
		t.Fatalf("same-db retry %+v %v", completed, err)
	}
	pins, err := state.RetentionObjectPins(ctx, db, completed.WorktreeID)
	if err != nil || len(pins) != 4 {
		t.Fatalf("history provenance %+v %v", pins, err)
	}
	if _, err = gitpkg.RevParse(ctx, repo, "HEAD"); !errors.Is(err, gitpkg.ErrRefNotFound) {
		t.Fatal("legacy activation created a branch")
	}
}
