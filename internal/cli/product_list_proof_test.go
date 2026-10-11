package cli

import (
	"context"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestProductListRetainsActivityAndWorkWhenGitProofTimesOut(t *testing.T) {
	for _, pending := range []bool{true, false} {
		t.Run(map[bool]string{true: "saved work", false: "recent activity"}[pending], func(t *testing.T) {
			now := time.Now()
			overview := productListRepoOverview{lastActivity: now.Add(-time.Minute), unfinished: pending}
			entry := productListEntryFromOverview(central.RepoRecord{Path: "/slow"}, supervisor.WorkerStatus{State: "running"}, overview, context.DeadlineExceeded)
			visible, _ := selectProductListEntriesAt([]productListEntry{entry}, false, now)
			if len(visible) != 1 || entry.UnfinishedWork != pending || entry.PublicationOutcome.BranchCommitted != nil {
				t.Fatalf("timeout hid known work or invented commitment: %+v", entry)
			}
		})
	}
}
