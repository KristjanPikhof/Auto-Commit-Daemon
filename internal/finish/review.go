package finish

// EvidenceReviewCallLimit bounds one explicit attempt independently of its
// single global plan and optional correction. It never authorizes retries.
const EvidenceReviewCallLimit = 128

// ReviewObservation retains the exact redacted fragment behind a factual review.
type ReviewObservation struct {
	ID       string `json:"id"`
	ChangeID string `json:"change_id"`
	Part     int    `json:"part"`
	Parts    int    `json:"parts"`
	Digest   string `json:"digest,omitempty"`
	Summary  string `json:"summary"`
}

// ReviewProgress belongs to one frozen request. Its fingerprint binds the
// evidence, provider configuration and instructions, independently of deadlines.
type ReviewProgress struct {
	Fingerprint     string              `json:"fingerprint"`
	TotalFragments  int                 `json:"total_fragments"`
	ReusedFragments int                 `json:"reused_fragments"`
	Observations    []ReviewObservation `json:"observations"`
}
