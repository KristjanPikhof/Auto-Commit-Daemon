package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestProductListRetainsActivityAndWorkWhenGitProofTimesOut(t *testing.T) {
	for _, test := range []struct {
		name                         string
		pairTimeout, pending, recent bool
	}{
		{"pair timeout with old pending work", true, true, false},
		{"pair timeout with recent activity", true, false, true},
		{"outcome timeout with old pending work", false, true, false},
		{"outcome timeout with recent activity", false, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			repo, dbPath, db := makeSeededRepoStateDB(t)
			now := time.Now().Truncate(time.Second)
			activity := now.Add(-2 * productListActiveWindow)
			if test.recent {
				activity = now.Add(-time.Minute)
			}
			if err := state.MetaSet(ctx, db, state.ActivityMetaKey, fmt.Sprint(activity.Unix())); err != nil {
				t.Fatal(err)
			}
			if err := state.SaveDaemonState(ctx, db, state.DaemonState{PID: os.Getpid(), Mode: "running", HeartbeatTS: float64(now.Unix()), UpdatedTS: float64(now.Unix())}); err != nil {
				t.Fatal(err)
			}
			if test.pending {
				appendFixEvent(t, ctx, db, state.CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: "head", Operation: "create", Path: "waiting.txt", Fidelity: "exact", State: state.EventStatePending, CapturedTS: float64(activity.Unix())}, nil)
			}
			originalPair, originalOutcome := productListCurrentPair, productListReadOutcome
			t.Cleanup(func() { productListCurrentPair, productListReadOutcome = originalPair, originalOutcome })
			pairCalls, outcomeCalls := 0, 0
			productListCurrentPair = func(context.Context, *sql.DB, string) (string, int64, bool, error) {
				pairCalls++
				if test.pairTimeout {
					return "", 0, false, context.DeadlineExceeded
				}
				return "refs/heads/main", 1, true, nil
			}
			productListReadOutcome = func(ctx context.Context, conn *sql.DB, protected bool, repo, branch string, generation int64, known bool) (publicationOutcome, error) {
				outcomeCalls++
				if branch != "refs/heads/main" || generation != 1 || !known {
					t.Fatalf("lost resolved pair: %s %d %v", branch, generation, known)
				}
				// A proof exhausting the row budget must leave its truth unknown,
				// while already read activity and pending work remain usable.
				return publicationOutcome{}, context.DeadlineExceeded
			}
			before := fileDigest(t, dbPath)
			record := central.RepoRecord{Path: repo, StateDB: dbPath, RepositoryID: "repo", WorktreeID: "worktree"}
			overview, err := readProductListRepo(ctx, record, now)
			if test.pairTimeout != errors.Is(err, context.DeadlineExceeded) || !test.pairTimeout && err != nil {
				t.Fatalf("error=%v", err)
			}
			if pairCalls != 1 || outcomeCalls != map[bool]int{true: 0, false: 1}[test.pairTimeout] {
				t.Fatalf("duplicate proof: pair=%d outcome=%d", pairCalls, outcomeCalls)
			}
			if !overview.lastActivity.Equal(activity) || overview.unfinished != test.pending {
				t.Fatalf("lost durable overview: activity=%s unfinished=%v", overview.lastActivity, overview.unfinished)
			}
			entry := productListEntryFromOverview(record, supervisor.WorkerStatus{State: "running"}, overview, err)
			visible, _ := selectProductListEntriesAt([]productListEntry{entry}, false, now)
			if len(visible) != 1 || entry.UnfinishedWork != test.pending || entry.PublicationOutcome.BranchCommitted != nil {
				t.Fatalf("slow proof hid work or invented commitment: %+v", entry)
			}
			if fileDigest(t, dbPath) != before {
				t.Fatal("dashboard wrote state")
			}
		})
	}
}
