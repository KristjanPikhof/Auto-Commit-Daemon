package central

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// MissingRepoAssessment describes whether a stale registry row can be
// removed without discarding the only record of protected work.
type MissingRepoAssessment struct {
	Missing      bool
	SafeToRemove bool
	Reason       string
}

// AssessMissingRepo distinguishes a deleted worktree from a temporarily
// inaccessible path. A missing state database is safe to forget because the
// deleted worktree no longer has recoverable ACD state. An accessible database
// with unresolved work is retained fail-closed.
func AssessMissingRepo(
	ctx context.Context,
	record RepoRecord,
) (MissingRepoAssessment, error) {
	if strings.TrimSpace(record.Path) == "" {
		return MissingRepoAssessment{}, errors.New("central: empty repository path")
	}
	if _, err := os.Stat(record.Path); err == nil {
		return MissingRepoAssessment{}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return MissingRepoAssessment{}, fmt.Errorf(
			"central: inspect registered worktree %s: %w", record.Path, err)
	}

	summary, err := state.ReadLegacyWorkSummary(ctx, record.StateDB)
	if err != nil {
		return MissingRepoAssessment{Missing: true}, fmt.Errorf(
			"central: inspect missing worktree state %s: %w", record.Path, err)
	}
	if summary.Unpublished > 0 || summary.Terminal > 0 ||
		summary.OpenPublication > 0 {
		return MissingRepoAssessment{
			Missing: true,
			Reason:  "missing worktree has unresolved protected work",
		}, nil
	}
	return MissingRepoAssessment{
		Missing: true, SafeToRemove: true, Reason: "repo-missing",
	}, nil
}

// ReconcileMissingRepos removes deleted worktrees that have no surviving
// unresolved state. Unsafe missing rows are disabled so they cannot keep a
// repository worker in a restart loop.
func (r *Registry) ReconcileMissingRepos(
	ctx context.Context,
	now time.Time,
) (removed, disabled []RepoRecord, err error) {
	if r == nil {
		return nil, nil, errors.New("central: nil registry")
	}
	kept := make([]RepoRecord, 0, len(r.Repos))
	for _, record := range r.Repos {
		if record.FirstRegisteredTS <= 0 && record.LastSeenTS <= 0 {
			kept = append(kept, record)
			continue
		}
		assessment, assessErr := AssessMissingRepo(ctx, record)
		if !assessment.Missing {
			kept = append(kept, record)
			continue
		}
		if assessErr == nil && assessment.SafeToRemove {
			removed = append(removed, record)
			continue
		}
		if !record.LifecycleDisabled() {
			record.LifecycleState = RepoLifecycleDisabled
			record.LifecycleUpdatedTS = now.Unix()
			disabled = append(disabled, record)
		}
		kept = append(kept, record)
	}
	r.Repos = kept
	return removed, disabled, nil
}
