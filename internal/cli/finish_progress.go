package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const finishProgressInterval = 30 * time.Second

type finishProgress struct {
	out         io.Writer
	label       string
	lastPrinted time.Time
}

func (p *finishProgress) checkpointScan(raw json.RawMessage) {
	var scan daemon.FinishCheckpointProgress
	if json.Unmarshal(raw, &scan) != nil {
		return
	}
	switch {
	case scan.Complete:
		fmt.Fprintf(p.out, "Scanned %s and kept %s for planning.\n",
			finishProgressCount(scan.Scanned, "checkpoint"), finishProgressCount(scan.Kept, "intermediate version"))
	case scan.Scanned > 0:
		fmt.Fprintf(p.out, "Scanned %s.\n", finishProgressCount(scan.Scanned, "checkpoint"))
	default:
		fmt.Fprintln(p.out, "Scanning saved checkpoints.")
	}
}

func (p *finishProgress) update(r finishWorkerResponse, now time.Time) {
	if p.out == nil {
		return
	}
	label := finishProgressLabel(r)
	if label == p.label && now.Sub(p.lastPrinted) < finishProgressInterval {
		return
	}
	p.label, p.lastPrinted = label, now
	switch r.Stage {
	case "reviewing_evidence", "planning", "assigning_paths", "splitting_commits":
		if r.DeadlineTS > 0 {
			label += fmt.Sprintf(" (%s remaining)", formatDurationCompact(finishProgressTime(r.DeadlineTS).Sub(now)))
		}
	default:
		if elapsed := now.Sub(finishProgressTime(r.UpdatedTS)); r.UpdatedTS > 0 && elapsed >= finishProgressInterval {
			label += fmt.Sprintf(" (%s elapsed)", formatDurationCompact(elapsed))
		}
	}
	fmt.Fprintln(p.out, label+".")
}

func finishProgressTime(timestamp float64) time.Time {
	return time.Unix(0, int64(timestamp*float64(time.Second)))
}

func finishProgressLabel(r finishWorkerResponse) string {
	switch r.Stage {
	case "preparing":
		if r.Target.Mode == "intent" {
			return "Preparing saved changes for Intent"
		}
	case "reviewing_evidence":
		if r.ReviewFragments > 0 {
			return finishReviewProgressLabel(r.ReviewedFragments, r.ReviewFragments, r.ReusedFragments)
		}
	case "planning":
		if r.PlanningCalls > 1 {
			return "Waiting for Intent to revise the proposed commits"
		}
	case "verifying":
		if check := r.Checking; check != nil && check.Commit > 0 && check.Commit <= check.Total {
			return fmt.Sprintf("Checking commit %d of %d", check.Commit, check.Total)
		}
	case "publishing", "prepared":
		var proof state.FinishPublicationProof
		if json.Unmarshal(r.Publication, &proof) == nil && len(proof.Commits) > 0 {
			return fmt.Sprintf("Publishing %s to your branch", finishProgressCount(len(proof.Commits), "verified commit"))
		}
	}
	return strings.TrimSuffix(finishStageLabel(r.Stage), ".")
}

func finishReviewProgressLabel(reviewed, total, reused int) string {
	return fmt.Sprintf("Reviewing saved changes (%d of %d fragments, %d reused)", reviewed, total, reused)
}

func finishProgressCount(count int, singular string) string {
	if count != 1 {
		singular += "s"
	}
	return fmt.Sprintf("%d %s", count, singular)
}
