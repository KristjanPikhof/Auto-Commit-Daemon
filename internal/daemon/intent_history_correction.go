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
	type priorGroup struct {
		id   string
		seqs []int64
	}
	var groups []priorGroup
	for _, candidate := range plan.Candidates[:min(len(plan.Candidates), ai.IntentOpenCandidateCap)] {
		id := candidate.CandidateID
		if len(id) > 4*ai.IntentCandidateIDCap {
			id = id[:4*ai.IntentCandidateIDCap]
		}
		id = ai.SanitizePlannerError(strings.ToValidUTF8(id, ""))
		seen := make(map[int64]bool)
		var seqs []int64
		for _, seq := range candidate.SelectedSeqs[:min(len(candidate.SelectedSeqs), ai.IntentCandidateCaptureCap)] {
			if offered[seq] && !seen[seq] {
				owners[seq] = append(owners[seq], id)
				seen[seq] = true
				seqs = append(seqs, seq)
			}
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		groups = append(groups, priorGroup{id: id, seqs: seqs})
	}
	var conflicts []int64
	conflictingGroups := make(map[string]bool)
	for seq, ids := range owners {
		if len(ids) > 1 {
			conflicts = append(conflicts, seq)
			for _, id := range ids {
				conflictingGroups[id] = true
			}
		}
	}
	sort.Slice(conflicts, func(i, j int) bool { return conflicts[i] < conflicts[j] })
	correction += "\nEach offered history capture is indivisible. A coalesced recorded path chain cannot be split between goals. Repair the listed prior partition rather than rebuilding every assignment from scratch; preserve other groups only if they remain valid. If multiple goals genuinely require the same capture, merge those inseparable goals with all their required implementation, callers, tests, and documentation; rewrite their purpose and commit message around the combined net behavior. Resolve the entire connected overlap set together. If goals are independently complete and ownership was merely mistaken, correct the assignment instead. Do not drop captures or invent hunk splits. Return every offered seq exactly once; revised goals must still satisfy cohesion, dependencies, completeness, and materialization.\nPrior conflicting groups (offered membership only):"
	sort.SliceStable(groups, func(i, j int) bool { return groups[i].id < groups[j].id })
	shownGroups := 0
	// Append only complete membership/ownership rows; reserve both omission
	// counts and section labels. These are rejected metadata, not accepted goals.
	limit := ai.IntentAtomicityCorrectionCap - 384
	appendGroup := func(group priorGroup) {
		id, _ := json.Marshal(group.id)
		seqs, _ := json.Marshal(group.seqs)
		line := fmt.Sprintf("\ncandidate=%s selected_seqs=%s", id, seqs)
		if utf8.RuneCountInString(correction)+utf8.RuneCountInString(line) <= limit {
			correction += line
			shownGroups++
		}
	}
	for _, group := range groups {
		if conflictingGroups[group.id] {
			appendGroup(group)
		}
	}
	correction += "\nRejected ownership overlaps:"
	shown := 0
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
	correction += "\nOther prior groups (revalidate before preserving):"
	for _, group := range groups {
		if !conflictingGroups[group.id] {
			appendGroup(group)
		}
	}
	if shownGroups < len(plan.Candidates) {
		correction += fmt.Sprintf("\n%d prior group memberships omitted by the correction limit; use the original request to recheck the entire partition.", len(plan.Candidates)-shownGroups)
	}
	if shown < len(conflicts) {
		correction += fmt.Sprintf("\n%d additional overlap rows omitted by the correction limit; recheck the entire offered partition.", len(conflicts)-shown)
	}
	return ai.NormalizeIntentAtomicityCorrection(correction)
}
