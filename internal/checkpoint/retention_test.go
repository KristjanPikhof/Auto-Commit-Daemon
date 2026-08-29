package checkpoint

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRetentionKeepsNewestHundredAndRefsSurviveGC(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	store := Store{DB: db}
	worktreeID := WorktreeID(repo)
	blob, err := gitpkg.HashObjectStdinDurable(ctx, repo, []byte("shared protected content\n"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	var oldestRef, newestRef, newestCommit string
	for i := 0; i < DefaultMinimumRetained+1; i++ {
		seq, appendErr := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: "seed", Operation: "modify", Path: fmt.Sprintf("file-%03d", i), Fidelity: "exact",
		}, []state.CaptureOp{{Op: "modify", Path: fmt.Sprintf("file-%03d", i), Fidelity: "exact"}})
		if appendErr != nil {
			t.Fatal(appendErr)
		}
		created, createErr := store.Create(ctx, Request{
			RepoRoot: repo, WorktreeID: worktreeID, Reason: state.CheckpointReasonPoll,
			ObservationEpoch: int64(i + 1), CoverageEpoch: int64(i + 1),
			Entries:   []Entry{{Path: "protected.txt", Mode: gitpkg.RegularFileMode, OID: blob}},
			EventSeqs: []int64{seq}, Now: now.Add(-60*24*time.Hour + time.Duration(i)*time.Second),
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		if err := state.MarkEventPublished(ctx, db, seq, state.EventStatePublished,
			sql.NullString{String: "normal-commit", Valid: true}, sql.NullString{}, sql.NullString{}, float64(now.Unix())); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			oldestRef = created.Checkpoint.Ref
		}
		newestRef, newestCommit = created.Checkpoint.Ref, created.Checkpoint.CommitOID
	}
	summary, err := store.ApplyRetention(ctx, repo, worktreeID, now)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Pruned != 1 || summary.Retained != DefaultMinimumRetained {
		t.Fatalf("retention summary=%+v", summary)
	}
	if _, err := gitpkg.RevParse(ctx, repo, oldestRef); err == nil {
		t.Fatalf("expired checkpoint ref %s still exists", oldestRef)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "gc", "--prune=now"); err != nil {
		t.Fatal(err)
	}
	if got, err := gitpkg.RevParse(ctx, repo, newestRef); err != nil || got != newestCommit {
		t.Fatalf("retained checkpoint after gc=(%q,%v), want %q", got, err, newestCommit)
	}
}

func TestRetentionTreatsZeroMemberCheckpointAsProtectionOnly(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	result, err := (Store{DB: db}).Create(ctx, Request{
		RepoRoot: repo, WorktreeID: WorktreeID(repo),
		Reason:           state.CheckpointReasonManualBarrier,
		ObservationEpoch: 1, CoverageEpoch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	items, err := state.RetentionCheckpoints(ctx, db, WorktreeID(repo))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ID != result.Checkpoint.ID || items[0].Published {
		t.Fatalf("retention checkpoints=%+v", items)
	}
}

func TestRetentionDeduplicatesProtectedObjectBytes(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	store := Store{DB: db}
	for epoch := int64(1); epoch <= 2; epoch++ {
		if _, err := store.Create(ctx, Request{
			RepoRoot: repo, WorktreeID: WorktreeID(repo),
			Reason:           state.CheckpointReasonManualBarrier,
			ObservationEpoch: epoch, CoverageEpoch: epoch,
		}); err != nil {
			t.Fatal(err)
		}
	}
	summary, err := store.ApplyRetention(ctx, repo, WorktreeID(repo), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if summary.ProtectedBytes > summary.ContentBytes {
		t.Fatalf("protected bytes=%d exceed retained content=%d", summary.ProtectedBytes, summary.ContentBytes)
	}
}

func TestRetentionYoungCheckpointsUseOneUnionInventory(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	worktreeID := WorktreeID(repo)
	store := Store{DB: db}
	blob, err := gitpkg.HashObjectStdinDurable(ctx, repo, []byte("shared young content\n"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for i := 0; i < DefaultMinimumRetained+1; i++ {
		createPublishedRetentionCheckpoint(
			t, ctx, store, repo, worktreeID, blob, i, now.Add(-time.Hour))
	}
	inventoryCalls := 0
	store.retentionInventory = func(
		context.Context, string, []string,
	) (map[string]int64, error) {
		inventoryCalls++
		return map[string]int64{"shared": 1}, nil
	}
	summary, err := store.ApplyRetention(ctx, repo, worktreeID, now)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Pruned != 0 || summary.Retained != DefaultMinimumRetained+1 {
		t.Fatalf("retention summary=%+v", summary)
	}
	if inventoryCalls != 1 {
		t.Fatalf("inventory calls=%d want 1 for %d young checkpoints",
			inventoryCalls, DefaultMinimumRetained+1)
	}
}

func TestRetentionBudgetPrunesExactOldestPrefix(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	worktreeID := WorktreeID(repo)
	store := Store{DB: db}
	blob, err := gitpkg.HashObjectStdinDurable(ctx, repo, []byte("shared budget content\n"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	uniqueSizes := make(map[string]int64)
	for i := 0; i < DefaultMinimumRetained+3; i++ {
		createdAt := now.Add(-24 * time.Hour)
		if i < 3 {
			createdAt = now.Add(-8*24*time.Hour + time.Duration(i)*time.Second)
		}
		ref := createPublishedRetentionCheckpoint(
			t, ctx, store, repo, worktreeID, blob, i, createdAt)
		switch i {
		case 0, 1:
			uniqueSizes[ref] = 3 << 30
		case 2:
			uniqueSizes[ref] = 1 << 30
		}
	}
	inventoryCalls := 0
	store.retentionInventory = func(
		_ context.Context, _ string, refs []string,
	) (map[string]int64, error) {
		inventoryCalls++
		objects := map[string]int64{"shared": 1 << 30}
		for _, ref := range refs {
			if size := uniqueSizes[ref]; size > 0 {
				objects[ref] = size
			}
		}
		return objects, nil
	}
	summary, err := store.ApplyRetention(ctx, repo, worktreeID, now)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Pruned != 1 || summary.Retained != DefaultMinimumRetained+2 ||
		summary.ContentBytes != DefaultContentBudget || summary.OverBudget {
		t.Fatalf("retention summary=%+v", summary)
	}
	if inventoryCalls > 5 {
		t.Fatalf("inventory calls=%d want logarithmic budget evaluation", inventoryCalls)
	}
}

func createPublishedRetentionCheckpoint(
	t *testing.T,
	ctx context.Context,
	store Store,
	repo, worktreeID, blob string,
	index int,
	createdAt time.Time,
) string {
	t.Helper()
	path := fmt.Sprintf("retention-%03d.txt", index)
	seq, err := state.AppendCaptureEvent(ctx, store.DB, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "seed", Operation: "modify", Path: path, Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: path, Fidelity: "exact"}})
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.Create(ctx, Request{
		RepoRoot: repo, WorktreeID: worktreeID, Reason: state.CheckpointReasonPoll,
		ObservationEpoch: int64(index + 1), CoverageEpoch: int64(index + 1),
		Entries:   []Entry{{Path: "protected.txt", Mode: gitpkg.RegularFileMode, OID: blob}},
		EventSeqs: []int64{seq}, Now: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.MarkEventPublished(ctx, store.DB, seq, state.EventStatePublished,
		sql.NullString{String: "normal-commit", Valid: true}, sql.NullString{},
		sql.NullString{}, float64(createdAt.Unix())); err != nil {
		t.Fatal(err)
	}
	return created.Checkpoint.Ref
}
