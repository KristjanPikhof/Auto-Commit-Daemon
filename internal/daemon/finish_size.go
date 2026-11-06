package daemon

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
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
