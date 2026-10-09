package ai

// Keep the host's full ownership and recorded evidence for validation. Only
// offered captures have numeric assignment handles on the provider wire.
type intentCandidateWireV2 struct {
	IntentCandidateSummary
	SelectedSeqs     []int64                  `json:"selected_seqs,omitempty"`
	CapturedEvidence []OfferedCapture         `json:"captured_evidence,omitempty"`
	ReadonlyEvidence []intentReadonlyEvidence `json:"readonly_evidence,omitempty"`
}

type intentReadonlyEvidence struct {
	Path         string              `json:"path"`
	Op           string              `json:"op,omitempty"`
	Fidelity     string              `json:"fidelity,omitempty"`
	CapturedDiff string              `json:"captured_diff,omitempty"`
	FileMetadata *IntentFileMetadata `json:"file_metadata,omitempty"`
}

type intentReadonlyDependency struct {
	FromSeq         int64                    `json:"from_seq,omitempty"`
	ToSeq           int64                    `json:"to_seq,omitempty"`
	FromCandidateID string                   `json:"from_candidate_id,omitempty"`
	ToCandidateID   string                   `json:"to_candidate_id,omitempty"`
	Strength        IntentDependencyStrength `json:"strength"`
	Kind            string                   `json:"kind"`
	EvidenceHash    string                   `json:"evidence_hash,omitempty"`
}

type intentPlanWireV2 struct {
	IntentPlanRequestV2
	Candidates           []intentCandidateWireV2     `json:"candidates,omitempty"`
	Dependencies         []IntentCaptureDependency   `json:"dependencies,omitempty"`
	ReadonlyDependencies []intentReadonlyDependency  `json:"readonly_dependencies,omitempty"`
	BaselineCandidates   []IntentCandidateAssignment `json:"baseline_candidates,omitempty"`
}

func intentPlanV2WireRequest(req IntentPlanRequestV2) intentPlanWireV2 {
	wire := intentPlanWireV2{IntentPlanRequestV2: req}
	offered := make(map[int64]bool, len(req.OfferedCaptures))
	owners := make(map[int64]string)
	for _, capture := range req.OfferedCaptures {
		offered[capture.Seq] = true
	}
	for _, candidate := range req.Candidates {
		item := intentCandidateWireV2{IntentCandidateSummary: candidate}
		for _, seq := range candidate.SelectedSeqs {
			owners[seq] = candidate.CandidateID
			if offered[seq] {
				item.SelectedSeqs = append(item.SelectedSeqs, seq)
			}
		}
		for _, capture := range candidate.CapturedEvidence {
			if offered[capture.Seq] {
				item.CapturedEvidence = append(item.CapturedEvidence, capture)
			} else {
				item.ReadonlyEvidence = append(item.ReadonlyEvidence, intentReadonlyEvidence{
					Path: capture.Path, Op: capture.Op, Fidelity: capture.Fidelity,
					CapturedDiff: capture.CapturedDiff, FileMetadata: capture.FileMetadata,
				})
			}
		}
		wire.Candidates = append(wire.Candidates, item)
	}
	for _, candidate := range cloneIntentCandidateAssignments(req.BaselineCandidates) {
		var selected []int64
		for _, seq := range candidate.SelectedSeqs {
			if offered[seq] {
				selected = append(selected, seq)
			}
		}
		if len(selected) > 0 {
			candidate.SelectedSeqs = selected
			wire.BaselineCandidates = append(wire.BaselineCandidates, candidate)
		}
	}
	for _, edge := range req.Dependencies {
		if offered[edge.FromSeq] && offered[edge.ToSeq] {
			wire.Dependencies = append(wire.Dependencies, edge)
			continue
		}
		item := intentReadonlyDependency{Strength: edge.Strength, Kind: edge.Kind, EvidenceHash: edge.EvidenceHash}
		if offered[edge.FromSeq] {
			item.FromSeq = edge.FromSeq
		} else {
			item.FromCandidateID = owners[edge.FromSeq]
		}
		if offered[edge.ToSeq] {
			item.ToSeq = edge.ToSeq
		} else {
			item.ToCandidateID = owners[edge.ToSeq]
		}
		wire.ReadonlyDependencies = append(wire.ReadonlyDependencies, item)
	}
	return wire
}
