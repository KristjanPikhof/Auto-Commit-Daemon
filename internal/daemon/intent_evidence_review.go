package daemon

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func intentPlanRunEvidenceReviewed(run state.IntentPlanRun) bool {
	if !run.ResolvedPlanJSON.Valid || len(run.ResolvedPlanJSON.String) > state.IntentResolvedPlanJSONCap {
		return false
	}
	var stored struct {
		EvidenceReviewed bool `json:"evidence_reviewed"`
	}
	return json.Unmarshal([]byte(run.ResolvedPlanJSON.String), &stored) == nil && stored.EvidenceReviewed
}

// This inventory describes supplied evidence, not goal completeness. It never
// infers what a free-text missing-companion reason meant or authorizes READY.
func intentRecordedEvidenceCorrection(req ai.IntentPlanRequestV2, selected []int64) string {
	if len(selected) == 0 {
		return ""
	}
	waiting := make(map[int64]bool, len(selected))
	for _, seq := range selected {
		waiting[seq] = true
	}
	var facts strings.Builder
	eligible := make(map[int64]bool)
	for _, capture := range req.OfferedCaptures {
		if !waiting[capture.Seq] || capture.FileMetadata == nil || capture.FileMetadata.Kind != "text" ||
			capture.CapturedDiff == "" || capture.CapturedDiffTruncated || capture.FileMetadata.DiffOmittedReason != "" ||
			strings.Contains(capture.CapturedDiff, "... <truncated> ...") {
			continue
		}
		eligible[capture.Seq] = true
		fmt.Fprintf(&facts, "- Offered capture %d at %q includes %d bytes of supplied text diff with no renderer clipping or omission.\n", capture.Seq, capture.Path, len(capture.CapturedDiff))
	}
	if len(eligible) == 0 {
		return ""
	}
	documentationOnly := true
	for _, capture := range req.OfferedCaptures {
		if !eligible[capture.Seq] {
			continue
		}
		documentationOnly = documentationOnly && intentCaptureRole(IntentCandidateCapture{
			Event: state.CaptureEvent{Path: capture.Path},
		}) == "documentation"
	}
	// Published members have no assignment authority. List only evidence related
	// to unresolved offered members by the same grounded relationship inventory
	// used by the goal gates, rather than every prior commit in the request.
	related := make(map[int64]bool)
	for _, edge := range groundedIntentRequestDependencies(req) {
		if eligible[edge.FromSeq] {
			related[edge.ToSeq] = true
		}
		if eligible[edge.ToSeq] {
			related[edge.FromSeq] = true
		}
	}
	for _, candidate := range req.Candidates {
		if candidate.Status != state.IntentCandidatePublished {
			continue
		}
		for _, capture := range candidate.CapturedEvidence {
			if (related[capture.Seq] || documentationOnly) && capture.CapturedDiff != "" && containsIntentSeq(candidate.SelectedSeqs, capture.Seq) {
				fmt.Fprintf(&facts, "- Published baseline %q supplies recorded evidence for related path %q. It is read-only context, not unoffered pending work.\n", candidate.CandidateID, capture.Path)
			}
		}
	}
	if documentationOnly {
		facts.WriteString("- Review this documentation goal against the supplied published baseline. Documentation can complete a useful goal without new implementation or tests when it describes behavior already present there; do not require those captures to be offered again. Keep WAIT if the described behavior is actually absent or unproven.\n")
	}
	if facts.Len() == 0 {
		return ""
	}
	return ai.NormalizeIntentAtomicityCorrection("Review the waiting goals against these facts from the current request. An unclipped diff does not itself prove a complete goal. Keep WAIT when the goal remains incomplete or unclear; every READY goal must still satisfy all ownership, dependency, completeness, materialization, verification, and message checks. Preserve validated locked READY groups. Read-only published context has no assignment authority.\n" + facts.String())
}
