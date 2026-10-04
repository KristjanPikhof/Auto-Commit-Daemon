package daemon

import (
	"context"
	"path"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// A read failure leaves the missing content's dependencies unknown. Only
// independent documentation can be proved from path/reference evidence alone;
// code, configuration and generated assets wait for complete observation.
func capturePublicationHold(ctx context.Context, db *state.DB, paths []string, evidence string) (string, error) {
	issues, err := state.CurrentCaptureIssues(ctx, db)
	if err != nil || len(issues) == 0 {
		return "", err
	}
	for _, issue := range issues {
		if issue.Path == "" {
			return "capture scope is unknown", nil
		}
		for _, candidatePath := range paths {
			if candidatePath == issue.Path || issue.Subtree && strings.HasPrefix(candidatePath, issue.Path+"/") {
				return "candidate includes an unreadable or unstable path", nil
			}
			if intentCaptureRole(IntentCandidateCapture{Event: state.CaptureEvent{Path: candidatePath}}) != "documentation" {
				return "candidate independence from incomplete capture is unproven", nil
			}
		}
		if strings.Contains(evidence, issue.Path) || strings.Contains(evidence, path.Base(issue.Path)) {
			return "candidate references an incompletely captured path", nil
		}
	}
	return "", nil
}

func capturePublicationHoldOps(ctx context.Context, repo string, db *state.DB, ops []state.CaptureOp, paths []string) (string, error) {
	issues, err := state.CurrentCaptureIssues(ctx, db)
	if err != nil || len(issues) == 0 {
		return "", err
	}
	evidence, err := BuildOpsDiff(ctx, repo, ops)
	if err != nil {
		return "", err
	}
	return capturePublicationHold(ctx, db, paths, evidence)
}
