package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestSavedCheckpointAwaitsClassificationWithoutClaimingPublication(t *testing.T) {
	withIsolatedHome(t)
	_, _, db := makeRepoStateDB(t)
	ctx := context.Background()
	insertCompletedCheckpoint(t, db, "cp-unclassified", "0123456789abcdef", nil)
	if err := state.MetaSet(ctx, db, daemon.MetaKeyProtectionClassificationPending, "true"); err != nil {
		t.Fatal(err)
	}
	outcome, err := readPublicationOutcome(ctx, db.SQL(), true)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.BranchCommitted == nil || *outcome.BranchCommitted || !outcome.PendingClassification {
		t.Fatalf("unclassified checkpoint reported published: %+v", outcome)
	}
	if got := publicationOutcomeLabel(outcome, publicationProgressReport{}); got != "saved changes waiting for grouping" {
		t.Fatal(got)
	}
	if got := checkpointOutcome(state.CheckpointCompleted, 0, 0, 0); got != "saved" {
		t.Fatalf("unclassified history: %s", got)
	}
	if err := state.MetaSet(ctx, db, daemon.MetaKeyProtectionClassificationPending, "false"); err != nil {
		t.Fatal(err)
	}
	outcome, err = readPublicationOutcome(ctx, db.SQL(), true)
	if err != nil || outcome.BranchCommitted == nil || !*outcome.BranchCommitted {
		t.Fatalf("resolved checkpoint: %+v, %v", outcome, err)
	}
}

func TestCommitAllPreviewShowsPartialStagingAndDetectsChangedIndex(t *testing.T) {
	_, repo, dbPath := registeredProductMutationRepo(t)
	ctx := context.Background()
	path := filepath.Join(repo, "partial.txt")
	if err := os.WriteFile(path, []byte("staged\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "add", "partial.txt"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("later\n"), 0600); err != nil {
		t.Fatal(err)
	}
	before := fileDigest(t, dbPath)
	scope, err := inspectCommitAllScope(ctx, repo, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, path := range scope.ChangedPaths {
		if path.Path == "partial.txt" {
			found = path.Staged && path.Unstaged
		}
	}
	if !found {
		t.Fatalf("partial staging missing: %+v", scope)
	}
	var out bytes.Buffer
	renderCommitAllScope(&out, scope)
	for _, want := range []string{"partial.txt", "staged and unstaged", "may overlap", "consumed after checkpoint protection"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("preview missing %q: %s", want, &out)
		}
	}
	if fileDigest(t, dbPath) != before {
		t.Fatal("preview wrote state")
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "add", "partial.txt"); err != nil {
		t.Fatal(err)
	}
	updated, err := inspectCommitAllScope(ctx, repo, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Digest == scope.Digest {
		t.Fatal("staged content changed without invalidating approval")
	}
}

func TestRestoreWithoutIDDoesNotApplyOutsideTerminal(t *testing.T) {
	_, repo, dbPath := registeredProductMutationRepo(t)
	before := fileDigest(t, dbPath)
	cmd := newRootCmd()
	cmd.SetIn(strings.NewReader("1\ny\n"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"restore", "--repo", repo})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "acd history") {
		t.Fatalf("bare nonterminal restore: %v", err)
	}
	if fileDigest(t, dbPath) != before {
		t.Fatal("nonterminal restore changed state")
	}
}

func TestCompactStatusSeparatesAIWaitAndRecovery(t *testing.T) {
	committed := false
	var out bytes.Buffer
	err := renderProductEnvelope(&out, productEnvelope{State: productStateWaiting, Data: productStatusData{
		Enabled: true, Protected: true,
		PublicationOutcome: publicationOutcome{BranchCommitted: &committed, WaitingChanges: 3, RecoveredChanges: 2, ReasonCode: "provider_wait"},
	}}, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"Protection: on", "Current changes saved: yes", "Branch commits: 3 changes waiting for AI", "Recovery: 2 changes saved separately", "Next: No action needed."} {
		if !strings.Contains(out.String(), line) {
			t.Fatalf("missing %q: %s", line, &out)
		}
	}
	if strings.Contains(out.String(), "Worker liveness") || strings.Contains(out.String(), "Published to Git") {
		t.Fatalf("default status contains obsolete/detail output: %s", &out)
	}
}
