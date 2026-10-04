package daemon

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// A read failure leaves the missing content's dependencies unknown. Only
// independent documentation can be proved from path/reference evidence alone;
// code, configuration and generated assets wait for complete observation.
func capturePublicationHold(ctx context.Context, db *state.DB, paths []string, evidence string) (string, error) {
	issues, err := state.CurrentCaptureIssues(ctx, db)
	if err != nil {
		return "", err
	}
	return capturePublicationHoldForIssues(issues, paths, evidence), nil
}

func capturePublicationHoldForIssues(issues []state.CheckpointCaptureIssue, paths []string, evidence string) string {
	for _, issue := range issues {
		if issue.Path == "" {
			return "capture scope is unknown"
		}
		for _, candidatePath := range paths {
			if candidatePath == issue.Path || issue.Subtree && strings.HasPrefix(candidatePath, issue.Path+"/") {
				return "candidate includes an unreadable or unstable path"
			}
			if intentCaptureRole(IntentCandidateCapture{Event: state.CaptureEvent{Path: candidatePath}}) != "documentation" {
				return "candidate independence from incomplete capture is unproven"
			}
		}
		if strings.Contains(evidence, issue.Path) || strings.Contains(evidence, path.Base(issue.Path)) {
			return "candidate references an incompletely captured path"
		}
	}
	return ""
}

func capturePublicationHoldOps(ctx context.Context, repo string, db *state.DB, ops []state.CaptureOp, paths []string) (string, error) {
	issues, err := state.CurrentCaptureIssues(ctx, db)
	if err != nil {
		return "", err
	}
	return capturePublicationHoldOpsForIssues(ctx, repo, ops, paths, issues)
}

func capturePublicationHoldOpsForIssues(ctx context.Context, repo string, ops []state.CaptureOp, paths []string, issues []state.CheckpointCaptureIssue) (string, error) {
	if len(issues) == 0 {
		return "", nil
	}
	for _, op := range ops {
		if op.Op == "delete" || op.Op == "rename" {
			return "capture may be missing a rename or deletion companion", nil
		}
	}
	if hold := capturePublicationHoldForIssues(issues, paths, ""); hold != "" {
		return hold, nil
	}
	evidence, err := BuildOpsDiff(ctx, repo, ops)
	if err != nil {
		return "", err
	}
	return capturePublicationHoldForIssues(issues, paths, evidence), nil
}

// A cursor only rotates the bounded scan. Captures retain their FIFO identity;
// a pending predecessor on the same path must still publish first.
type captureEventScanCursor struct {
	BranchRef        string
	BranchGeneration int64
	TargetID         string
	Seq              int64
}

func loadCaptureEventScanCursor(ctx context.Context, db *state.DB, repo string, cctx CaptureContext, targetID string) (string, captureEventScanCursor, error) {
	key := "capture.event_scan." + checkpoint.WorktreeID(repo)
	cursor := captureEventScanCursor{}
	if _, err := state.MetaGetJSON(ctx, db, key, &cursor); err != nil {
		return "", cursor, err
	}
	if cursor.BranchRef != cctx.BranchRef || cursor.BranchGeneration != cctx.BranchGeneration || cursor.TargetID != targetID || cursor.Seq < 0 {
		cursor = captureEventScanCursor{BranchRef: cctx.BranchRef, BranchGeneration: cctx.BranchGeneration, TargetID: targetID}
	}
	return key, cursor, nil
}

func captureEventHasPendingPredecessor(ctx context.Context, db *state.DB, event state.CaptureEvent) (bool, error) {
	var pending bool
	err := db.ReadSQL().QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM capture_events
    WHERE state='pending' AND branch_ref=? AND branch_generation=? AND seq<?
      AND (path=? OR old_path=?)
)`, event.BranchRef, event.BranchGeneration, event.Seq, event.Path, event.Path).Scan(&pending)
	if err != nil {
		return false, fmt.Errorf("daemon: check held capture predecessor: %w", err)
	}
	return pending, nil
}
