package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
)

// Correct ownership metadata without replaying untrusted response text or
// choosing which goal should own a capture. The provider must replan, and every
// ordinary history validator still runs against its next response.
func intentHistoryPlanCorrection(req ai.IntentPlanRequestV2, plan ai.IntentPlanV2, err error) string {
	correction := ai.SanitizePlannerError(err.Error())
	var validation *ai.IntentPlanV2ValidationError
	if !errors.As(err, &validation) {
		return correction
	}
	duplicate := false
	for _, finding := range validation.Findings[:min(len(validation.Findings), ai.IntentPriorFindingCap)] {
		duplicate = duplicate || finding.Code == "capture_assigned_twice"
	}
	if !duplicate {
		return correction
	}
	if rejected, ok := ai.RejectedIntentPlanV2(err); ok {
		plan = rejected
	}
	offered := make(map[int64]bool)
	for _, capture := range req.OfferedCaptures[:min(len(req.OfferedCaptures), ai.IntentCandidateCaptureCap)] {
		offered[capture.Seq] = true
	}
	owners := make(map[int64][]string)
	for _, candidate := range plan.Candidates[:min(len(plan.Candidates), ai.IntentOpenCandidateCap)] {
		id := candidate.CandidateID
		if len(id) > 4*ai.IntentCandidateIDCap {
			id = id[:4*ai.IntentCandidateIDCap]
		}
		id = ai.SanitizePlannerError(strings.ToValidUTF8(id, ""))
		seen := make(map[int64]bool)
		for _, seq := range candidate.SelectedSeqs[:min(len(candidate.SelectedSeqs), ai.IntentCandidateCaptureCap)] {
			if offered[seq] && !seen[seq] {
				owners[seq] = append(owners[seq], id)
				seen[seq] = true
			}
		}
	}
	var conflicts []int64
	for seq, ids := range owners {
		if len(ids) > 1 {
			conflicts = append(conflicts, seq)
		}
	}
	sort.Slice(conflicts, func(i, j int) bool { return conflicts[i] < conflicts[j] })
	correction += "\nEach offered history capture is indivisible. A coalesced recorded path chain cannot be split between goals. If multiple goals genuinely require the same capture, merge those inseparable goals with all their required implementation, callers, tests, and documentation; rewrite their purpose and commit message around the combined net behavior. Resolve the entire connected overlap set together. If goals are independently complete and ownership was merely mistaken, correct the assignment instead. Do not drop captures or invent hunk splits. Return every offered seq exactly once; revised goals must still satisfy cohesion, dependencies, completeness, and materialization.\nRejected ownership overlaps:"
	shown := 0
	// Reserve room for an explicit omission count, and append complete rows only.
	limit := ai.IntentAtomicityCorrectionCap - 128
	for _, seq := range conflicts {
		sort.Strings(owners[seq])
		ids, _ := json.Marshal(owners[seq])
		line := fmt.Sprintf("\nseq=%d owners=%s", seq, ids)
		if utf8.RuneCountInString(correction)+utf8.RuneCountInString(line) > limit {
			continue
		}
		correction += line
		shown++
	}
	if shown < len(conflicts) {
		correction += fmt.Sprintf("\n%d additional overlap rows omitted by the correction limit; recheck the entire offered partition.", len(conflicts)-shown)
	}
	return ai.NormalizeIntentAtomicityCorrection(correction)
}
