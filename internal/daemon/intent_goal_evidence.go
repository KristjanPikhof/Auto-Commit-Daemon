package daemon

import (
	"context"
	"path"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// All open goals keep a cheap path/purpose overview. Detailed captured evidence
// is fetched only for goals connected to the current work by real relationships.
func loadFocusedIntentGoalEvidence(ctx context.Context, input IntentCandidateEvaluation, existing []state.IntentCandidate, captures []IntentCandidateCapture) ([]IntentCandidateCapture, error) {
	if input.RepoPath == "" || !input.IncludeDiffs {
		return captures, nil
	}
	var offeredPaths []string
	var freshPaths []string
	for _, capture := range captures {
		offeredPaths = append(offeredPaths, capture.Event.Path)
	}
	newCaptures := make(map[int64]bool)
	for _, capture := range input.Captures {
		newCaptures[capture.Event.Seq] = true
		freshPaths = append(freshPaths, capture.Event.Path)
	}
	withReferences := make(map[int64]bool)
	attachReferences := func(capture *IntentCandidateCapture) error {
		if withReferences[capture.Event.Seq] {
			return nil
		}
		withReferences[capture.Event.Seq] = true
		for i := len(capture.Ops) - 1; i >= 0; i-- {
			op := capture.Ops[i]
			if op.Path != capture.Event.Path {
				continue
			}
			references, err := loadIntentRecordedReferenceContext(ctx, input.RepoPath, op.Path, op.AfterOID.String, op.AfterMode.String, offeredPaths)
			if err != nil {
				return err
			}
			if references != "" && capture.CapturedDiff == "" {
				capture.CapturedDiff, err = BuildOpsDiffWithCap(ctx, input.RepoPath, capture.Ops, ai.IntentStageDiffCap)
				if err != nil {
					return err
				}
			}
			capture.CapturedDiff = includeIntentRecordedReferenceContext(capture.CapturedDiff, references)
			break
		}
		return nil
	}
	for i := range captures {
		if newCaptures[captures[i].Event.Seq] && len(withReferences) < ai.IntentCandidateCaptureCap {
			if err := attachReferences(&captures[i]); err != nil {
				return nil, err
			}
		}
	}
	// A late configuration/helper capture may name no symbols itself. Prove
	// that an existing script goal calls or reads it before deciding which
	// detailed old diffs are relevant. The scan stays on bounded captured blobs.
	scanned := len(withReferences)
	for i := range captures {
		capture := &captures[i]
		ext := path.Ext(capture.Event.Path)
		if newCaptures[capture.Event.Seq] || (ext != ".sh" && ext != ".py") || scanned >= ai.IntentCandidateCaptureCap {
			continue
		}
		scanned++
		for j := len(capture.Ops) - 1; j >= 0; j-- {
			op := capture.Ops[j]
			if op.Path != capture.Event.Path {
				continue
			}
			references, err := loadIntentRecordedReferenceContext(ctx, input.RepoPath, op.Path, op.AfterOID.String, op.AfterMode.String, freshPaths)
			if err != nil {
				return nil, err
			}
			if references != "" {
				if capture.CapturedDiff == "" {
					capture.CapturedDiff, err = BuildOpsDiffWithCap(ctx, input.RepoPath, capture.Ops, ai.IntentStageDiffCap)
					if err != nil {
						return nil, err
					}
				}
				capture.CapturedDiff = includeIntentRecordedReferenceContext(capture.CapturedDiff, references)
			}
			break
		}
	}
	edges, err := BuildIntentCandidateDependencies(input.BranchRef, input.BranchGeneration, captures, append(append([]IntentDependencyHint(nil), input.Hints...), runtimeIntentDependencyHints(captures)...), input.Now)
	if err != nil {
		return nil, err
	}
	related := make(map[int64]bool)
	for _, capture := range input.Captures {
		related[capture.Event.Seq] = true
	}
	for changed := true; changed; {
		changed = false
		for _, edge := range edges {
			if !strongIntentSemanticDependency(edge.Kind) {
				continue
			}
			if related[edge.PrerequisiteSeq] == related[edge.DependentSeq] {
				continue
			}
			related[edge.PrerequisiteSeq], related[edge.DependentSeq] = true, true
			changed = true
		}
	}
	for _, candidate := range existing {
		connected := false
		for _, member := range candidate.Events {
			connected = connected || related[member.EventSeq]
		}
		if connected {
			for _, member := range candidate.Events {
				related[member.EventSeq] = true
			}
		}
	}
	diffs := make([]string, len(captures))
	count := 0
	for i, capture := range captures {
		if !related[capture.Event.Seq] || count >= ai.IntentCandidateCaptureCap {
			continue
		}
		count++
		if capture.CapturedDiff == "" {
			captures[i].CapturedDiff, err = BuildOpsDiffWithCap(ctx, input.RepoPath, capture.Ops, ai.IntentStageDiffCap)
			if err != nil {
				return nil, err
			}
		}
		if err := attachReferences(&captures[i]); err != nil {
			return nil, err
		}
		diffs[i] = ai.RedactDiffSecrets(captures[i].CapturedDiff)
	}
	diffs = allocateIntentEvidenceDiffs(diffs, ai.HistoryRewriteTotalDiffCap)
	for i := range captures {
		if related[captures[i].Event.Seq] {
			captures[i].CapturedDiff = diffs[i]
		}
	}
	return captures, nil
}
