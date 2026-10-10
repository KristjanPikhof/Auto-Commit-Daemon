package checkpoint

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	createdAt := make([]time.Time, DefaultMinimumRetained+1)
	for i := 0; i < DefaultMinimumRetained+1; i++ {
		createdAt[i] = now.Add(-60*24*time.Hour + time.Duration(i)*time.Second)
	}
	checkpoints := seedPublishedRetentionCheckpoints(t, ctx, store, repo, worktreeID, blob, createdAt)
	oldestRef := checkpoints[0].Ref
	newest := checkpoints[len(checkpoints)-1]
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
	if got, err := gitpkg.RevParse(ctx, repo, newest.Ref); err != nil || got != newest.CommitOID {
		t.Fatalf("retained checkpoint after gc=(%q,%v), want %q", got, err, newest.CommitOID)
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
	createdAt := make([]time.Time, DefaultMinimumRetained+1)
	for i := range createdAt {
		createdAt[i] = now.Add(-time.Hour)
	}
	seedPublishedRetentionCheckpoints(t, ctx, store, repo, worktreeID, blob, createdAt)
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
	createdAt := make([]time.Time, DefaultMinimumRetained+3)
	for i := range createdAt {
		createdAt[i] = now.Add(-24 * time.Hour)
		if i < 3 {
			createdAt[i] = now.Add(-8*24*time.Hour + time.Duration(i)*time.Second)
		}
	}
	checkpoints := seedPublishedRetentionCheckpoints(t, ctx, store, repo, worktreeID, blob, createdAt)
	for i, checkpoint := range checkpoints {
		switch i {
		case 0, 1:
			uniqueSizes[checkpoint.Ref] = 3 << 30
		case 2:
			uniqueSizes[checkpoint.Ref] = 1 << 30
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

// Retention tests need many completed snapshots, not repeated coverage of the
// checkpoint writer. Keep distinct real commits and refs while sharing their
// identical tree and batching Git setup. State still prepares and completes
// every checkpoint with its published capture membership.
func seedPublishedRetentionCheckpoints(
	t *testing.T,
	ctx context.Context,
	store Store,
	repo, worktreeID, blob string,
	createdAt []time.Time,
) []state.Checkpoint {
	t.Helper()
	if len(createdAt) == 0 || len(createdAt) > DefaultMinimumRetained+3 {
		t.Fatal("retention fixture requires a bounded checkpoint batch")
	}
	scratch := t.TempDir()
	entries := []Entry{{Path: "protected.txt", Mode: gitpkg.RegularFileMode, OID: blob}}
	tree, err := gitpkg.WriteTreeDurable(ctx, repo, filepath.Join(scratch, "checkpoint.index"), []gitpkg.IndexEntry{
		{Path: entries[0].Path, Mode: entries[0].Mode, OID: entries[0].OID},
	})
	if err != nil {
		t.Fatal(err)
	}
	checkpoints := make([]state.Checkpoint, len(createdAt))
	var commitPaths strings.Builder
	for i, now := range createdAt {
		id, err := NewID(now)
		if err != nil {
			t.Fatal(err)
		}
		checkpoints[i] = state.Checkpoint{
			ID: id, OperationID: "op-" + id, WorktreeID: worktreeID,
			Reason: state.CheckpointReasonPoll, ObservationEpoch: int64(i + 1), CoverageEpoch: int64(i + 1),
			TreeOID: tree, Ref: gitpkg.CheckpointRefPrefix + worktreeID + "/" + id,
			CreatedTS: float64(now.UnixNano()) / float64(time.Second),
		}
		commitPath := filepath.Join(scratch, fmt.Sprintf("commit-%03d", i))
		commit := fmt.Sprintf("tree %s\nauthor %s <%s> %d +0000\ncommitter %s <%s> %d +0000\n\nacd checkpoint %s\n",
			tree, IdentityName, IdentityEmail, now.Unix(), IdentityName, IdentityEmail, now.Unix(), id)
		if err := os.WriteFile(commitPath, []byte(commit), 0o600); err != nil {
			t.Fatal(err)
		}
		commitPaths.WriteString(commitPath + "\n")
	}
	out, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo, Stdin: strings.NewReader(commitPaths.String())},
		"hash-object", "-w", "-t", "commit", "--stdin-paths", "--no-filters")
	if err != nil {
		t.Fatal(err)
	}
	commits := strings.Fields(string(out))
	if len(commits) != len(checkpoints) {
		t.Fatalf("created %d commit objects for %d checkpoints", len(commits), len(checkpoints))
	}
	seenCommits := make(map[string]bool)
	var refs strings.Builder
	refs.WriteString("start\n")
	for i := range checkpoints {
		checkpoint := &checkpoints[i]
		checkpoint.CommitOID = commits[i]
		if seenCommits[checkpoint.CommitOID] {
			t.Fatal("checkpoint fixture reused a commit identity")
		}
		seenCommits[checkpoint.CommitOID] = true
		capturePath := fmt.Sprintf("retention-%03d.txt", i)
		seq, err := state.AppendCaptureEvent(ctx, store.DB, state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: "seed", Operation: "modify", Path: capturePath, Fidelity: "exact",
		}, []state.CaptureOp{{Op: "modify", Path: capturePath, Fidelity: "exact"}})
		if err != nil {
			t.Fatal(err)
		}
		checkpoint.EventSeqs = []int64{seq}
		digest := requestDigest(Request{RepoRoot: repo, WorktreeID: worktreeID, Reason: checkpoint.Reason,
			ObservationEpoch: checkpoint.ObservationEpoch, CoverageEpoch: checkpoint.CoverageEpoch,
			EventSeqs: checkpoint.EventSeqs}, entries, tree, checkpoint.CommitOID, checkpoint.Ref)
		if _, err := state.PrepareCheckpoint(ctx, store.DB, *checkpoint, digest); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&refs, "create %s %s\n", checkpoint.Ref, checkpoint.CommitOID)
	}
	refs.WriteString("prepare\ncommit\n")
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo, Stdin: strings.NewReader(refs.String())},
		"update-ref", "--no-deref", "--stdin"); err != nil {
		t.Fatal(err)
	}
	out, err = gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "for-each-ref", "--format=%(refname) %(objectname)",
		gitpkg.CheckpointRefPrefix+worktreeID+"/")
	if err != nil {
		t.Fatal(err)
	}
	observed := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("invalid checkpoint ref inventory: %q", line)
		}
		observed[fields[0]] = fields[1]
	}
	if len(observed) != len(checkpoints) {
		t.Fatalf("created %d refs for %d checkpoints", len(observed), len(checkpoints))
	}
	for _, checkpoint := range checkpoints {
		if observed[checkpoint.Ref] != checkpoint.CommitOID {
			t.Fatalf("checkpoint ref %s lost its exact commit", checkpoint.Ref)
		}
		if err := state.CompleteCheckpoint(ctx, store.DB, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, checkpoint.CreatedTS); err != nil {
			t.Fatal(err)
		}
		if err := state.MarkEventPublished(ctx, store.DB, checkpoint.EventSeqs[0], state.EventStatePublished,
			sql.NullString{String: "normal-commit", Valid: true}, sql.NullString{}, sql.NullString{}, checkpoint.CreatedTS); err != nil {
			t.Fatal(err)
		}
	}
	return checkpoints
}
