package daemon

import (
	"context"
	"path"
	"strings"

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
	withRawDiff := make(map[int64]bool)
	rawDiffs := make(map[int64]string)
	goCalls := make(map[int64]intentRecordedGoCalls)
	var referenceNames intentReferenceNames
	loadRawDiff := func(capture *IntentCandidateCapture) error {
		if withRawDiff[capture.Event.Seq] {
			return nil
		}
		withRawDiff[capture.Event.Seq] = true
		raw, err := BuildOpsDiffWithCap(ctx, input.RepoPath, capture.Ops, intentSourceReferenceScanCap)
		if err != nil {
			return err
		}
		if raw != "" {
			capture.CapturedDiff = intentCompleteRawDiff(raw)
		}
		rawDiffs[capture.Event.Seq] = capture.CapturedDiff
		for i := len(capture.Ops) - 1; i >= 0; i-- {
			op := capture.Ops[i]
			if op.Path != capture.Event.Path {
				continue
			}
			calls, err := loadIntentRecordedGoCalls(ctx, input.RepoPath, op.Path, op.AfterOID.String, op.AfterMode.String, capture.CapturedDiff, intentSourceReferenceContextCap)
			if err != nil {
				return err
			}
			goCalls[capture.Event.Seq] = calls
			break
		}
		return nil
	}
	attachReferences := func(capture *IntentCandidateCapture) error {
		if withReferences[capture.Event.Seq] {
			return nil
		}
		withReferences[capture.Event.Seq] = true
		if newCaptures[capture.Event.Seq] {
			if err := loadRawDiff(capture); err != nil {
				return err
			}
		}
		for i := len(capture.Ops) - 1; i >= 0; i-- {
			op := capture.Ops[i]
			if op.Path != capture.Event.Path {
				continue
			}
			references, err := loadIntentRecordedReferenceContext(ctx, input.RepoPath, op.Path, op.AfterOID.String, op.AfterMode.String, offeredPaths, referenceNames)
			if err != nil {
				return err
			}
			if references != "" {
				if err := loadRawDiff(capture); err != nil {
					return err
				}
			}
			references = mergeIntentRecordedGoCallContext(goCalls[capture.Event.Seq].context, references)
			capture.CapturedDiff = prependIntentRecordedReferenceContext(capture.CapturedDiff, references)
			break
		}
		return nil
	}
	var freshCaptures []IntentCandidateCapture
	for i := range captures {
		if newCaptures[captures[i].Event.Seq] && len(freshCaptures) < ai.IntentCandidateCaptureCap {
			if err := loadRawDiff(&captures[i]); err != nil {
				return nil, err
			}
			freshCaptures = append(freshCaptures, captures[i])
		}
	}
	referenceNames = intentOtherCaptureReferenceNames(freshCaptures)
	for _, capture := range freshCaptures {
		addIntentRecordedGoCallNames(referenceNames, capture.Event.Path, goCalls[capture.Event.Seq])
	}
	for i := range captures {
		if newCaptures[captures[i].Event.Seq] && len(withReferences) < ai.IntentCandidateCaptureCap {
			if err := attachReferences(&captures[i]); err != nil {
				return nil, err
			}
		}
	}
	// A late consumer may refer to an existing goal's unchanged helper or type.
	// A late configuration may instead be read by an unchanged script caller.
	// Fetch old diffs only after the recorded blob proves a fresh relationship.
	scanned := len(withReferences)
	for i := range captures {
		capture := &captures[i]
		ext := path.Ext(capture.Event.Path)
		if newCaptures[capture.Event.Seq] || (ext != ".sh" && ext != ".py" && ext != ".go" && ext != ".swift") || scanned >= ai.IntentCandidateCaptureCap {
			continue
		}
		scanned++
		for j := len(capture.Ops) - 1; j >= 0; j-- {
			op := capture.Ops[j]
			if op.Path != capture.Event.Path {
				continue
			}
			references, err := loadIntentRecordedReferenceContext(ctx, input.RepoPath, op.Path, op.AfterOID.String, op.AfterMode.String, freshPaths, referenceNames)
			if err != nil {
				return nil, err
			}
			if references != "" {
				if err := loadRawDiff(capture); err != nil {
					return nil, err
				}
				capture.CapturedDiff = prependIntentRecordedReferenceContext(capture.CapturedDiff, references)
			}
			break
		}
	}
	typeScriptReferences, err := loadIntentRecordedTypeScriptReferences(ctx, input.RepoPath, captures)
	if err != nil {
		return nil, err
	}
	attachIntentTypeScriptReferences(captures, typeScriptReferences)
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
	var relatedCaptures []IntentCandidateCapture
	detailed := make(map[int64]bool)
	count := 0
	for i, capture := range captures {
		if !related[capture.Event.Seq] || count >= ai.IntentCandidateCaptureCap {
			continue
		}
		count++
		detailed[capture.Event.Seq] = true
		if err := loadRawDiff(&captures[i]); err != nil {
			return nil, err
		}
		rawCapture := captures[i]
		rawCapture.CapturedDiff = rawDiffs[capture.Event.Seq]
		relatedCaptures = append(relatedCaptures, rawCapture)
	}
	referenceNames = intentOtherCaptureReferenceNames(relatedCaptures)
	for _, capture := range relatedCaptures {
		addIntentRecordedGoCallNames(referenceNames, capture.Event.Path, goCalls[capture.Event.Seq])
	}
	for i := range captures {
		if !detailed[captures[i].Event.Seq] {
			continue
		}
		delete(withReferences, captures[i].Event.Seq)
		if err := attachReferences(&captures[i]); err != nil {
			return nil, err
		}
		attachIntentTypeScriptReferences(captures[i:i+1], typeScriptReferences)
		diffs[i] = captures[i].CapturedDiff
	}
	priorityCaptures := append([]IntentCandidateCapture(nil), captures...)
	for i := range priorityCaptures {
		priorityCaptures[i].CapturedDiff = diffs[i]
	}
	diffs = prioritizeIntentRelationshipEvidence(priorityCaptures)
	diffs = allocateIntentEvidenceDiffs(diffs, ai.HistoryRewriteTotalDiffCap)
	for i := range captures {
		if related[captures[i].Event.Seq] {
			captures[i].CapturedDiff = diffs[i]
		}
	}
	return captures, nil
}

func intentCompleteRawDiff(diff string) string {
	if len(diff) < intentSourceReferenceScanCap {
		return diff
	}
	diff = diff[:intentSourceReferenceScanCap]
	if end := strings.LastIndexByte(diff, '\n'); end >= 0 {
		return diff[:end+1]
	}
	return ""
}
