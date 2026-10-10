package daemon

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const intentSwiftMaintenanceReadCap = 2 << 20

// Scheduling picks the fairness anchor, not a commit boundary. Repeated
// per-file review records must not split one proved normalization operation.
func expandIntentSwiftMaintenanceWindow(ctx context.Context, repo string, db *state.DB, pending, window []state.CaptureEvent, cfg intentReplayConfig, now time.Time) ([]state.CaptureEvent, error) {
	if len(window) == 0 || len(window) >= state.IntentCandidateMaxCaptures {
		return window, nil
	}
	target := make(map[int64]bool, len(cfg.targetEventSeqs))
	for _, seq := range cfg.targetEventSeqs {
		target[seq] = true
	}
	selected := make(map[int64]bool, len(window))
	captures := make([]IntentCandidateCapture, 0, state.IntentCandidateMaxCaptures)
	load := func(event state.CaptureEvent) error {
		ops, err := state.LoadCaptureOps(ctx, db, event.Seq)
		if err == nil {
			captures = append(captures, IntentCandidateCapture{Event: event, Ops: ops})
		}
		return err
	}
	for _, event := range window {
		if event.State != state.EventStatePending || event.Operation != "modify" ||
			path.Ext(event.Path) != ".swift" || event.OldPath.Valid {
			return window, nil
		}
		if len(target) > 0 && !target[event.Seq] {
			return window, nil
		}
		selected[event.Seq] = true
		if err := load(event); err != nil {
			return nil, err
		}
	}
	for _, event := range pending {
		if selected[event.Seq] || (len(target) > 0 && !target[event.Seq]) {
			continue
		}
		if len(captures) == state.IntentCandidateMaxCaptures {
			break
		}
		if event.Operation != "modify" || path.Ext(event.Path) != ".swift" || event.OldPath.Valid {
			continue
		}
		if err := load(event); err != nil {
			return nil, err
		}
	}
	proved, err := proveIntentSwiftBlankLineMaintenance(ctx, repo, captures)
	if err != nil {
		return nil, err
	}
	bySeq := make(map[int64]IntentCandidateCapture, len(proved))
	for _, capture := range proved {
		bySeq[capture.Event.Seq] = capture
		if selected[capture.Event.Seq] && (!capture.FileMetadata.ProvesSwiftBlankLineMaintenance(capture.Event.Seq, capture.Event.Path) ||
			!pathQuiescentForEvent(capture.Event, capture.Ops, cfg.pathQuiescence, now)) {
			// A mixed initial window belongs to the ordinary goal planner.
			return window, nil
		}
	}
	paths := make(map[string]int64)
	var expanded []state.CaptureEvent
	for _, event := range pending {
		capture, ok := bySeq[event.Seq]
		if !ok || !capture.FileMetadata.ProvesSwiftBlankLineMaintenance(event.Seq, event.Path) ||
			!pathQuiescentForEvent(event, capture.Ops, cfg.pathQuiescence, now) {
			continue
		}
		paths[event.Path] = event.Seq
		expanded = append(expanded, event)
	}
	if len(expanded) <= len(window) {
		return window, nil
	}
	for _, event := range pending {
		if len(target) > 0 && !target[event.Seq] {
			continue
		}
		for name := range captureEventPathSet(event) {
			if seq, selectedPath := paths[name]; selectedPath && seq != event.Seq {
				// Let the ordinary dependency closure handle any same-path chain,
				// including a substantive predecessor or successor outside context.
				return window, nil
			}
		}
	}
	return expanded, nil
}

// Inspect complete immutable blobs. Diff clipping, provider text, and the live
// worktree cannot establish a maintenance proof. The optional analysis remains
// bounded even when the ordinary planning window contains large source files.
func proveIntentSwiftBlankLineMaintenance(ctx context.Context, repo string, captures []IntentCandidateCapture) ([]IntentCandidateCapture, error) {
	if repo == "" {
		return captures, nil
	}
	result := append([]IntentCandidateCapture(nil), captures...)
	blobs := make(map[string][]byte)
	readBytes := 0
	load := func(oid string) ([]byte, bool, error) {
		if contents, ok := blobs[oid]; ok {
			return contents, true, nil
		}
		contents, err := git.CatFileBlobLimited(ctx, repo, oid, ai.IntentMaintenanceBlobCap)
		readBytes += len(contents)
		if errors.Is(err, git.ErrStdoutOverflow) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		if readBytes > intentSwiftMaintenanceReadCap {
			return nil, false, nil
		}
		blobs[oid] = contents
		return contents, true, nil
	}
	for i, capture := range result {
		if readBytes >= intentSwiftMaintenanceReadCap {
			break
		}
		if capture.Event.State != state.EventStatePending || capture.Event.Operation != "modify" ||
			capture.Event.OldPath.Valid || path.Ext(capture.Event.Path) != ".swift" || len(capture.Ops) != 1 {
			continue
		}
		op := capture.Ops[0]
		if op.Op != "modify" || op.Path != capture.Event.Path || op.OldPath.Valid ||
			!op.BeforeOID.Valid || !op.AfterOID.Valid || op.BeforeOID.String == "" || op.AfterOID.String == "" ||
			!op.BeforeMode.Valid || !op.AfterMode.Valid || op.BeforeMode.String != "100644" || op.AfterMode.String != op.BeforeMode.String {
			continue
		}
		if capture.FileMetadata != nil && (capture.FileMetadata.Kind == "binary" ||
			capture.FileMetadata.BeforeBytes > ai.IntentMaintenanceBlobCap || capture.FileMetadata.AfterBytes > ai.IntentMaintenanceBlobCap ||
			capture.FileMetadata.BeforeBytes+capture.FileMetadata.AfterBytes > int64(intentSwiftMaintenanceReadCap-readBytes)) {
			continue
		}
		before, beforeOK, err := load(op.BeforeOID.String)
		if err != nil {
			return nil, err
		}
		if !beforeOK {
			continue
		}
		after, afterOK, err := load(op.AfterOID.String)
		if err != nil {
			return nil, err
		}
		if !afterOK {
			continue
		}
		metadata := ai.ProveIntentSwiftBlankLineMaintenance(capture.Event.Seq, capture.Event.Path,
			op.BeforeMode.String, op.AfterMode.String, before, after)
		if metadata != nil {
			if capture.FileMetadata != nil {
				metadata.DiffOmittedReason = capture.FileMetadata.DiffOmittedReason
			}
			result[i].FileMetadata = metadata
		}
	}
	return result, nil
}

// This is one actual normalization operation, rather than an invented source
// dependency. Provider JSON cannot set the private proof in file metadata.
func intentSwiftBlankLineMaintenanceSelection(req ai.IntentPlanRequestV2, seqs []int64) bool {
	if len(seqs) == 0 {
		return false
	}
	bySeq := make(map[int64]ai.OfferedCapture, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		bySeq[capture.Seq] = capture
	}
	for _, seq := range seqs {
		capture, ok := bySeq[seq]
		if !ok || !capture.FileMetadata.ProvesSwiftBlankLineMaintenance(seq, capture.Path) {
			return false
		}
	}
	return true
}

func swiftBlankLineMaintenancePlan(req ai.IntentPlanRequestV2) (ai.IntentPlanV2, bool) {
	if len(req.OfferedCaptures) == 0 || intentRequestTouchesRepairableSuffix(req) {
		return ai.IntentPlanV2{}, false
	}
	seqs := offeredIntentSeqs(req)
	if !intentSwiftBlankLineMaintenanceSelection(req, seqs) {
		return ai.IntentPlanV2{}, false
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: fmt.Sprintf("swift-blank-lines-%d", seqs[0]), SelectedSeqs: seqs,
			Purpose: "normalize Swift source blank lines without changing code", Readiness: ai.IntentCandidateReady,
			Subject:        "Normalize Swift source blank lines",
			Body:           "- Remove blank-line spaces and excess EOF lines without changing code",
			GroupingReason: "complete immutable blobs prove the same blank-line normalization in every selected source file",
		}},
	}, true
}
