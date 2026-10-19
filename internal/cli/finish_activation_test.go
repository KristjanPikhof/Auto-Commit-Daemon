package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishActivationHonorsPublicationHold(t *testing.T) {
	for _, scenario := range []string{"held", "resume", "running-held"} {
		t.Run(scenario, func(t *testing.T) {
			lookup, runtime, _ := finishCLIFixture(t)
			ctx, repo := t.Context(), lookup.Worktree.Root
			archive := finishArchiveAttention(t, runtime)
			if err := os.WriteFile(filepath.Join(repo, "goal.txt"), []byte("protected\n"), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := resolveFinishWorkerConfig(ctx, lookup.Roots, runtime)
			if err != nil {
				t.Fatal(err)
			}
			worker := daemon.FinishWorker{RepoPath: repo, RepositoryID: lookup.Record.RepositoryID, DB: runtime.db, Config: cfg}
			r, _, err := worker.Start(ctx, daemon.FinishOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "running-held" {
				proof, err := git.BuildFinishChain(ctx, repo, git.FinishBuildOptions{
					RequestID: r.ID, Attempt: r.Attempt, WorktreeID: r.Target.WorktreeID,
					BranchRef: r.Target.BranchRef, ExpectedHead: r.Target.ExpectedHead,
					TargetTree: r.Target.TargetTree, IndexDigest: r.Target.IndexDigest,
					ObjectRef: git.FinishRefPrefix + r.ID + "/chain",
					Commits:   []git.FinishCommit{{TreeOID: r.Target.TargetTree, Message: "Complete protected goal"}},
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := state.PrepareFinishPublication(ctx, runtime.db, r.ID, r.Attempt, proof, json.RawMessage(`{"steps":1}`)); err != nil {
					t.Fatal(err)
				}
			}
			beforeIndex, _ := git.IndexContentDigest(ctx, repo)
			var diagnostics bytes.Buffer
			activateWorkerFinish(ctx, &diagnostics, repo, lookup.Record.RepositoryID, lookup.Record.WorktreeID, runtime.db, scenario != "resume")
			if diagnostics.Len() != 0 {
				t.Fatal(diagnostics.String())
			}
			saved, err := state.LoadFinishRequest(ctx, runtime.db, r.ID)
			if err != nil {
				t.Fatal(err)
			}
			want := state.FinishPrepared
			if scenario == "resume" {
				want = state.FinishCompleted
			} else if scenario == "running-held" {
				want = state.FinishFailed
			}
			if saved.State != want {
				t.Fatalf("request state %s, want %s", saved.State, want)
			}
			retained, err := state.LoadFinishLegacyCutover(ctx, runtime.db)
			if err != nil || retained.Reason != archive.Reason || retained.State != archive.State {
				t.Fatalf("current recovery changed archived proof: %+v error=%v", retained, err)
			}
			if scenario != "resume" {
				head, _ := git.RevParse(ctx, repo, "HEAD")
				index, _ := git.IndexContentDigest(ctx, repo)
				if head != r.Target.ExpectedHead || index != beforeIndex {
					t.Fatal("startup hold changed HEAD or staging")
				}
			}
		})
	}
}
