package daemon

import (
	"context"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// All open goals keep a cheap path/purpose overview. Detailed captured evidence
// is fetched only for goals connected to the current work by real relationships.
func loadFocusedIntentGoalEvidence(ctx context.Context, input IntentCandidateEvaluation, existing []state.IntentCandidate, captures []IntentCandidateCapture) ([]IntentCandidateCapture, error) {
	if input.RepoPath == "" || !input.IncludeDiffs {
		return captures, nil
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
		diffs[i] = capture.CapturedDiff
		if diffs[i] == "" {
			diffs[i], err = BuildOpsDiffWithCap(ctx, input.RepoPath, capture.Ops, ai.IntentStageDiffCap)
			if err != nil {
				return nil, err
			}
		}
		diffs[i] = ai.RedactDiffSecrets(diffs[i])
	}
	diffs = allocateIntentEvidenceDiffs(diffs, ai.HistoryRewriteTotalDiffCap)
	for i := range captures {
		if related[captures[i].Event.Seq] {
			captures[i].CapturedDiff = diffs[i]
		}
	}
	return captures, nil
}
