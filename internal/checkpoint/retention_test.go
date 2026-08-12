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
