package checkpoint

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestCreatePublishesCompletedRootlessCheckpoint(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	blob, err := gitpkg.HashObjectStdinDurable(ctx, repo, []byte("protected\n"))
	if err != nil {
		t.Fatal(err)
	}
	request := Request{
		RepoRoot:         repo,
		WorktreeID:       WorktreeID(repo),
		Reason:           state.CheckpointReasonPoll,
		ObservationEpoch: 4,
		CoverageEpoch:    4,
		ObservedRef:      "refs/heads/main",
		Entries: []Entry{
			{Path: "safe.txt", Mode: gitpkg.RegularFileMode, OID: blob},
		},
		Exclusions: []state.CheckpointExclusion{{Category: "ignored", Count: 2}},
		Now:        time.UnixMilli(1786061000000),
	}
	result, err := (Store{DB: db}).Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Checkpoint.Phase != state.CheckpointCompleted || !result.RefCreated {
		t.Fatalf("result=%+v", result)
	}
	if !strings.HasPrefix(result.Checkpoint.ID, "cp-1786061000000-") {
		t.Fatalf("checkpoint id=%q", result.Checkpoint.ID)
	}
	if got, err := gitpkg.RevParse(ctx, repo, result.Checkpoint.Ref); err != nil || got != result.Checkpoint.CommitOID {
		t.Fatalf("private ref=(%q,%v), want %q", got, err, result.Checkpoint.CommitOID)
	}
	parents, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "show", "-s", "--format=%P", result.Checkpoint.CommitOID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(parents)) != "" {
		t.Fatalf("checkpoint has parents %q", parents)
	}
	identity, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "show", "-s",
		"--format=%an%x00%ae%x00%cn%x00%ce", result.Checkpoint.CommitOID)
	if err != nil {
		t.Fatal(err)
	}
	wantIdentity := IdentityName + "\x00" + IdentityEmail + "\x00" + IdentityName + "\x00" + IdentityEmail
	if strings.TrimSpace(string(identity)) != wantIdentity {
		t.Fatalf("identity=%q want=%q", strings.TrimSpace(string(identity)), wantIdentity)
	}
	projection, err := state.ReadCheckpointProjection(ctx, db.Path(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Completed != 1 || projection.Prepared != 0 || projection.Latest == nil {
		t.Fatalf("projection=%+v", projection)
	}
}

func TestRecoverPreparedCreatesMissingRefAndCompletes(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	checkpoint := preparedCheckpointFixture(t, ctx, repo, db,
		"cp-1786061000001-0123456789abcdef", "op-recover-missing")
	if _, err := gitpkg.RevParse(ctx, repo, checkpoint.Ref); !errors.Is(err, gitpkg.ErrRefNotFound) {
		t.Fatalf("prepared ref exists before recovery: %v", err)
	}
	if err := (Store{DB: db}).RecoverPrepared(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if got, err := gitpkg.RevParse(ctx, repo, checkpoint.Ref); err != nil || got != checkpoint.CommitOID {
		t.Fatalf("recovered ref=(%q,%v), want %q", got, err, checkpoint.CommitOID)
	}
	projection, err := state.ReadCheckpointProjection(ctx, db.Path(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Completed != 1 || projection.Prepared != 0 {
		t.Fatalf("projection=%+v", projection)
	}
}

func TestRecoverPreparedMarksUnexpectedRefNeedsAction(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	checkpoint := preparedCheckpointFixture(t, ctx, repo, db,
		"cp-1786061000002-fedcba9876543210", "op-recover-collision")
	wrong, err := gitpkg.CommitTreeDurable(ctx, repo, checkpoint.TreeOID,
		"wrong target", IdentityName, IdentityEmail)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.EnsureCheckpointRef(ctx, repo, checkpoint.Ref, wrong); err != nil {
		t.Fatal(err)
	}
	err = (Store{DB: db}).RecoverPrepared(ctx, repo)
	if !errors.Is(err, gitpkg.ErrCheckpointRefCollision) {
		t.Fatalf("recovery error=%v", err)
	}
	projection, readErr := state.ReadCheckpointProjection(ctx, db.Path(), 10)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if projection.NeedsAction != 1 || projection.Prepared != 0 || len(projection.Recoverable) != 1 {
		t.Fatalf("projection=%+v", projection)
	}
	if got, resolveErr := gitpkg.RevParse(ctx, repo, checkpoint.Ref); resolveErr != nil || got != wrong {
		t.Fatalf("ambiguous ref changed to (%q,%v), want retained %q", got, resolveErr, wrong)
	}
}

func checkpointFixture(t *testing.T, ctx context.Context) (string, *state.DB) {
	t.Helper()
	repo := t.TempDir()
	if err := gitpkg.Init(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo},
		"symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	gitDir, err := gitpkg.AbsoluteGitDir(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, state.DBPathFromGitDir(gitDir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return repo, db
}

func preparedCheckpointFixture(t *testing.T, ctx context.Context, repo string, db *state.DB, id, operationID string) state.Checkpoint {
	t.Helper()
	tree, err := gitpkg.WriteTreeDurable(ctx, repo,
		filepath.Join(t.TempDir(), "prepared.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := gitpkg.CommitTreeDurable(ctx, repo, tree,
		"acd checkpoint "+id+"\n", IdentityName, IdentityEmail)
	if err != nil {
		t.Fatal(err)
	}
	worktreeID := WorktreeID(repo)
	checkpoint := state.Checkpoint{
		ID:               id,
		OperationID:      operationID,
		WorktreeID:       worktreeID,
		Reason:           state.CheckpointReasonManualBarrier,
		ObservationEpoch: 1,
		CoverageEpoch:    1,
		TreeOID:          tree,
		CommitOID:        commit,
		Ref:              gitpkg.CheckpointRefPrefix + worktreeID + "/" + id,
	}
	const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if _, err := state.PrepareCheckpoint(ctx, db, checkpoint, digest); err != nil {
		t.Fatal(err)
	}
	return checkpoint
}
