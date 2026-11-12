package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

// FinishReviewSize describes the actual change introduced by one candidate.
// Binary entries count as files but have no textual line count.
type FinishReviewSize struct {
	ChangedLines int `json:"changed_lines"`
	ChangedFiles int `json:"changed_files"`
}

func finishReviewTarget(target state.FinishTarget) int {
	if target.Version >= 5 && target.Mode == "intent" && target.Message == "" {
		return ai.FinishReviewLineTarget
	}
	return 0
}

func finishRequiresPartition(target state.FinishTarget) bool {
	return target.Version >= state.FinishPartitionVersion && target.Mode == "intent" &&
		target.Message == "" && !target.SourceSelection && !target.RecordedSourceSelection
}

func measureFinishPlan(ctx context.Context, repo string, plan finish.Plan) ([]ai.FinishStepSize, error) {
	before, err := finishHistoryTree(ctx, repo, plan.Base)
	if err != nil {
		return nil, err
	}
	sizes := make([]ai.FinishStepSize, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		tree, err := finishHistoryTree(ctx, repo, step.Tree)
		if err != nil {
			return nil, err
		}
		size, err := measureFinishReviewSize(ctx, repo, before, tree)
		if err != nil {
			return nil, err
		}
		sizes = append(sizes, ai.FinishStepSize{ChangedLines: size.ChangedLines, ChangedFiles: size.ChangedFiles})
		before = tree
	}
	return sizes, nil
}

func (w *FinishWorker) retrySizeReview(ctx context.Context, current state.FinishRequest, evidence finish.Evidence) (*ai.FinishRetrySizeReview, error) {
	prior, err := state.PriorFinishCandidate(ctx, w.DB, current.ID, current.Attempt)
	if errors.Is(err, state.ErrFinishNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var saved FinishSavedPlan
	if !reflect.DeepEqual(prior.Target, current.Target) || json.Unmarshal(prior.Plan, &saved) != nil || saved.Source != nil {
		return nil, nil
	}
	review := &ai.FinishRetrySizeReview{}
	limit := finishReviewTarget(current.Target)
	for i, step := range saved.Steps {
		if step.ReviewSize == nil || step.Check.TreeOID != step.TreeOID ||
			step.Check.Result.Status != verification.StatusPassed || step.Check.Executed || step.Check.Result.NeedsAttention {
			return nil, nil
		}
		review.Goals = append(review.Goals, step.Goal)
		if step.ReviewSize.ChangedLines > limit && strings.TrimSpace(step.LargeChangeReason) == "" {
			review.Failures = append(review.Failures, ai.CommitSizeFailure{
				Step: i + 1, ChangedLines: step.ReviewSize.ChangedLines, ChangedFiles: step.ReviewSize.ChangedFiles, Limit: limit,
			})
		}
	}
	if len(review.Failures) == 0 {
		return nil, nil
	}
	if _, err := finish.Build(evidence, review.Goals); err != nil {
		return nil, nil
	}
	return review, nil
}

func measureFinishReviewSize(ctx context.Context, repo, before, after string) (FinishReviewSize, error) {
	out, err := git.RunWithLimit(ctx, git.RunOpts{Dir: repo}, state.FinishJSONByteCap,
		"diff", "--numstat", "-z", "--no-renames", "--no-ext-diff", "--no-textconv", before, after, "--")
	if err != nil {
		return FinishReviewSize{}, err
	}
	var size FinishReviewSize
	for _, entry := range strings.Split(string(out), "\x00") {
		if entry == "" {
			continue
		}
		fields := strings.SplitN(entry, "\t", 3)
		if len(fields) != 3 || fields[2] == "" {
			return FinishReviewSize{}, errors.New("finish: malformed candidate size evidence")
		}
		size.ChangedFiles++
		if fields[0] == "-" && fields[1] == "-" {
			continue
		}
		for _, value := range fields[:2] {
			lines, err := strconv.Atoi(value)
			if err != nil || lines < 0 || lines > math.MaxInt-size.ChangedLines {
				return FinishReviewSize{}, errors.New("finish: invalid candidate line count")
			}
			size.ChangedLines += lines
		}
	}
	return size, nil
}
