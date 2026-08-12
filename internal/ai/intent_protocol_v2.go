package ai

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
)

// Intent planner protocol identifiers are persisted with candidate decisions.
// A v1-compatible response is safe to consume, but does not carry enough
// information to claim native v2 readiness quality.
const (
	IntentPlannerProtocolV2       = "v2"
	IntentPlannerProtocolV1Compat = "v1_compat"
)

// Intent v2 protocol bounds mirror the durable state limits. Request builders
// reject over-limit metadata rather than silently dropping dependency evidence
// that could change an atomicity decision.
const (
	IntentCandidateCaptureCap    = 256
	IntentOpenCandidateCap       = 128
	IntentDependencyEdgeCap      = 4096
	IntentCandidatePurposeCap    = 512
	IntentAtomicitySummaryCap    = 2048
	IntentCandidateIDCap         = 128
	IntentDependencyKindCap      = 64
	IntentEvidenceHashCap        = 128
	IntentCandidateStatusCap     = 32
	IntentBoundaryEpochCap       = 128
	IntentAtomicityCorrectionCap = 4096
	IntentActivityBoundaryCap    = 64
	IntentRecentSoftCommitCap    = 5
	IntentPriorFindingCap        = 256
	IntentContextPathCap         = 512
	IntentContextPathsCap        = 256
)

// IntentDependencyStrength distinguishes publication constraints from
// evidence that only helps the planner assess semantic cohesion.
type IntentDependencyStrength string

const (
	IntentDependencyHard IntentDependencyStrength = "hard"
	IntentDependencySoft IntentDependencyStrength = "soft"
)

// IntentCaptureDependency is a privacy-safe dependency edge. EvidenceHash is
// a hash of symbol/import/hunk evidence; raw source and diffs never belong
// here. Direction is prerequisite -> dependent.
type IntentCaptureDependency struct {
	FromSeq      int64                    `json:"from_seq"`
	ToSeq        int64                    `json:"to_seq"`
	Strength     IntentDependencyStrength `json:"strength"`
	Kind         string                   `json:"kind"`
	EvidenceHash string                   `json:"evidence_hash,omitempty"`
}

// IntentCandidateSummary carries durable candidate context without raw source.
type IntentCandidateSummary struct {
	CandidateID       string    `json:"candidate_id"`
	Status            string    `json:"status"`
	Purpose           string    `json:"purpose,omitempty"`
	SelectedSeqs      []int64   `json:"selected_seqs,omitempty"`
	Paths             []string  `json:"paths,omitempty"`
	MissingCompanions []string  `json:"missing_companions,omitempty"`
	Ready             bool      `json:"ready"`
	CreatedAt         time.Time `json:"created_at,omitempty"`
	UpdatedAt         time.Time `json:"updated_at,omitempty"`
}

// IntentActivityBoundary contains no prompt text. Epoch is an opaque,
// repository-generated identifier and Kind is soft or hard.
type IntentActivityBoundary struct {
	Epoch     string    `json:"epoch"`
	Kind      string    `json:"kind"`
	CreatedAt time.Time `json:"created_at,omitempty"`
}

// IntentSoftCommitSummary is bounded context for recent ACD-owned commits that
// remain eligible for automatic candidate repair.
type IntentSoftCommitSummary struct {
	CandidateID string    `json:"candidate_id"`
	OID         string    `json:"oid"`
	Subject     string    `json:"subject"`
	Paths       []string  `json:"paths,omitempty"`
	PublishedAt time.Time `json:"published_at,omitempty"`
	Deadline    time.Time `json:"soft_publication_deadline,omitempty"`
}

// IntentAtomicityGate names the seven independent gates applied after planner
// shape validation.
type IntentAtomicityGate string

const (
	IntentAtomicityCohesion        IntentAtomicityGate = "cohesion"
	IntentAtomicityCompleteness    IntentAtomicityGate = "completeness"
	IntentAtomicitySeparation      IntentAtomicityGate = "separation"
	IntentAtomicityDependency      IntentAtomicityGate = "dependency"
	IntentAtomicityMaterialization IntentAtomicityGate = "materialization"
	IntentAtomicityVerification    IntentAtomicityGate = "verification"
	IntentAtomicityRevertibility   IntentAtomicityGate = "revertibility"
)

var intentAtomicityGates = [...]IntentAtomicityGate{
	IntentAtomicityCohesion,
	IntentAtomicityCompleteness,
	IntentAtomicitySeparation,
	IntentAtomicityDependency,
	IntentAtomicityMaterialization,
	IntentAtomicityVerification,
	IntentAtomicityRevertibility,
}

// IntentAtomicityStatus keeps pending external gates distinct from failed
// gates. Only passed or explicitly not-required gates can authorize publish.
type IntentAtomicityStatus string

const (
	IntentAtomicityPassed      IntentAtomicityStatus = "passed"
	IntentAtomicityFailed      IntentAtomicityStatus = "failed"
	IntentAtomicityPending     IntentAtomicityStatus = "pending"
	IntentAtomicityNotRequired IntentAtomicityStatus = "not_required"
)

// IntentAtomicityFinding is safe to persist and return in a correction prompt.
// Summary is normalized and capped before it crosses either boundary.
type IntentAtomicityFinding struct {
	CandidateID string              `json:"candidate_id"`
	Gate        IntentAtomicityGate `json:"gate"`
	Code        string              `json:"code"`
	Summary     string              `json:"summary"`
}

// IntentAtomicityGateResult is the result of one required atomicity gate.
type IntentAtomicityGateResult struct {
	Gate    IntentAtomicityGate     `json:"gate"`
	Status  IntentAtomicityStatus   `json:"status"`
	Finding *IntentAtomicityFinding `json:"finding,omitempty"`
}

// IntentAtomicityReport always contains all seven gates in stable order.
type IntentAtomicityReport struct {
	CandidateID string                      `json:"candidate_id"`
	Valid       bool                        `json:"valid"`
	Gates       []IntentAtomicityGateResult `json:"gates"`
}

// IntentPlanRequestV2 extends the planner context with durable candidates,
// capture dependencies, boundaries, recent soft commits, and exact prior
// atomicity findings. RetryCorrection is kept off the wire and is rendered by
// BuildIntentPlanV2UserPrompt as a bounded follow-up.
type IntentPlanRequestV2 struct {
	ProtocolVersion        string                        `json:"protocol_version"`
	LatestCommit           *CommitSummary                `json:"latest_commit,omitempty"`
	PathCommitContext      []PathCommitContext           `json:"path_commit_context,omitempty"`
	OfferedCaptures        []OfferedCapture              `json:"offered_captures"`
	Candidates             []IntentCandidateSummary      `json:"candidates,omitempty"`
	Dependencies           []IntentCaptureDependency     `json:"dependencies,omitempty"`
	ActivityBoundaries     []IntentActivityBoundary      `json:"activity_boundaries,omitempty"`
	RecentSoftCommits      []IntentSoftCommitSummary     `json:"recent_soft_commits,omitempty"`
	PriorAtomicityFindings []IntentAtomicityFinding      `json:"prior_atomicity_findings,omitempty"`
	ForcedAging            bool                          `json:"forced_aging,omitempty"`
	CommitFormat           CommitFormat                  `json:"commit_format,omitempty"`
	CapturedDiffTransform  prompttrace.TransformMetadata `json:"-"`
	RetryCorrection        string                        `json:"-"`
}

// IntentPlanRequestV2Options carries raw diff-bearing captures into the v2
// builder. Candidate and dependency metadata must already be privacy-safe.
type IntentPlanRequestV2Options struct {
	LatestCommit           *CommitSummary
	PathCommitContext      []PathCommitContext
	OfferedCaptures        []OfferedCapture
	Candidates             []IntentCandidateSummary
	Dependencies           []IntentCaptureDependency
	ActivityBoundaries     []IntentActivityBoundary
	RecentSoftCommits      []IntentSoftCommitSummary
	PriorAtomicityFindings []IntentAtomicityFinding
	ForcedAging            bool
	IncludeCapturedDiffs   bool
	CommitFormat           CommitFormat
}

// IntentCandidateReadiness is explicit so a waiting candidate cannot be
// mistaken for a publishable one merely because it already has a message.
type IntentCandidateReadiness string

const (
	IntentCandidateReady IntentCandidateReadiness = "ready"
	IntentCandidateWait  IntentCandidateReadiness = "wait"
)

// IntentCandidateAssignment is one durable candidate update. The response
// assigns every visible capture to exactly one candidate.
type IntentCandidateAssignment struct {
	CandidateID         string                   `json:"candidate_id"`
	SelectedSeqs        []int64                  `json:"selected_seqs"`
	Purpose             string                   `json:"purpose"`
	Readiness           IntentCandidateReadiness `json:"readiness"`
	MissingCompanions   []string                 `json:"missing_companions,omitempty"`
	DependsOnCandidates []string                 `json:"depends_on_candidates,omitempty"`
	Subject             string                   `json:"subject,omitempty"`
	Body                string                   `json:"body,omitempty"`
	GroupingReason      string                   `json:"grouping_reason"`
}

// IntentPlanV2 is the native candidate protocol response. Candidate order is
// publication order and must be a valid topological ordering.
type IntentPlanV2 struct {
	ProtocolVersion string                      `json:"protocol_version"`
	Candidates      []IntentCandidateAssignment `json:"candidates"`
}

// IntentPlannerV2 is implemented by providers that natively understand
// durable candidates. Providers that only implement IntentPlanner are routed
// through PlanIntentV2WithCompatibility.
type IntentPlannerV2 interface {
	Name() string
	PlanIntentV2(ctx context.Context, req IntentPlanRequestV2) (IntentPlanV2, error)
}

// IntentPlannerV2UnsupportedError lets a provider probe a backward-compatible
// wire protocol without losing a valid legacy response. The outer adapter
// performs the v1 -> v2 conversion so callers can reliably distinguish native
// v2 planning from planner_protocol=v1_compat.
type IntentPlannerV2UnsupportedError struct {
	LegacyPlan IntentPlan
}

func (e *IntentPlannerV2UnsupportedError) Error() string {
	return "intent planner v2: provider returned a legacy v1 response"
}

// IntentPlanV2ValidationError carries bounded structural and atomicity
// findings for a correction retry.
type IntentPlanV2ValidationError struct {
	Message  string
	Findings []IntentAtomicityFinding
	rejected *IntentPlanV2
}

func rejectedIntentPlanV2(err error, plan IntentPlanV2) error {
	var validation *IntentPlanV2ValidationError
	if !errors.As(err, &validation) {
		return err
	}
	copy := cloneIntentPlanV2Value(plan)
	validation.rejected = &copy
	return err
}

// RejectedIntentPlanV2 returns the decoded native plan that failed structural
// validation. Callers may use this transient copy only for deterministic local
// repair; it is never persisted or logged as raw planner output.
func RejectedIntentPlanV2(err error) (IntentPlanV2, bool) {
	var validation *IntentPlanV2ValidationError
	if !errors.As(err, &validation) || validation.rejected == nil {
		return IntentPlanV2{}, false
	}
	return cloneIntentPlanV2Value(*validation.rejected), true
}

func (e *IntentPlanV2ValidationError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

// DecodeIntentPlanV2 decodes exactly one strict JSON value and validates it
// against the request. Unknown fields and trailing JSON are rejected so a
// provider cannot smuggle an unreviewed alternate grouping into the response.
func DecodeIntentPlanV2(raw []byte, req IntentPlanRequestV2) (IntentPlanV2, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var plan IntentPlanV2
	if err := decoder.Decode(&plan); err != nil {
		return IntentPlanV2{}, v2ValidationError("", IntentAtomicityCohesion, "response_decode_failed", err.Error())
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err == nil {
		return IntentPlanV2{}, v2ValidationError("", IntentAtomicityCohesion, "multiple_json_values",
			"response must contain exactly one JSON value")
	} else if !errors.Is(err, io.EOF) {
		return IntentPlanV2{}, v2ValidationError("", IntentAtomicityCohesion, "response_trailing_data", err.Error())
	}
	if err := ValidateIntentPlanV2(req, plan); err != nil {
		return plan, rejectedIntentPlanV2(err, plan)
	}
	return plan, nil
}

// NewIntentPlanRequestV2 builds and validates the bounded wire request.
func NewIntentPlanRequestV2(opts IntentPlanRequestV2Options) (IntentPlanRequestV2, error) {
	legacy, err := NewIntentPlanRequest(IntentPlanRequestOptions{
		LatestCommit:         opts.LatestCommit,
		PathCommitContext:    opts.PathCommitContext,
		OfferedCaptures:      opts.OfferedCaptures,
		ForcedAging:          opts.ForcedAging,
		IncludeCapturedDiffs: opts.IncludeCapturedDiffs,
		CommitFormat:         opts.CommitFormat,
	})
	if err != nil {
		return IntentPlanRequestV2{}, err
	}
	req := IntentPlanRequestV2{
		ProtocolVersion:        IntentPlannerProtocolV2,
		LatestCommit:           legacy.LatestCommit,
		PathCommitContext:      legacy.PathCommitContext,
		OfferedCaptures:        legacy.OfferedCaptures,
		Candidates:             cloneCandidateSummaries(opts.Candidates),
		Dependencies:           append([]IntentCaptureDependency(nil), opts.Dependencies...),
		ActivityBoundaries:     append([]IntentActivityBoundary(nil), opts.ActivityBoundaries...),
		RecentSoftCommits:      cloneSoftCommitSummaries(opts.RecentSoftCommits),
		PriorAtomicityFindings: normalizeAtomicityFindings(opts.PriorAtomicityFindings),
		ForcedAging:            legacy.ForcedAging,
		CommitFormat:           legacy.CommitFormat,
		CapturedDiffTransform:  legacy.CapturedDiffTransform,
	}
	if err := ValidateIntentPlanRequestV2(req); err != nil {
		return IntentPlanRequestV2{}, err
	}
	return req, nil
}

// PlanIntentV2WithCompatibility prefers a native v2 provider. Existing
// subprocess and OpenAI-compatible v1 planners are translated into one-pass
// candidate updates explicitly labeled planner_protocol=v1_compat.
func PlanIntentV2WithCompatibility(ctx context.Context, planner interface{ Name() string }, req IntentPlanRequestV2) (IntentPlanV2, error) {
	if err := ctx.Err(); err != nil {
		return IntentPlanV2{}, err
	}
	if planner == nil {
		return IntentPlanV2{}, errors.New("intent planner v2: planner is nil")
	}
	if native, ok := planner.(IntentPlannerV2); ok {
		plan, err := native.PlanIntentV2(ctx, req)
		if err != nil {
			var unsupported *IntentPlannerV2UnsupportedError
			if !errors.As(err, &unsupported) {
				return IntentPlanV2{}, err
			}
			return AdaptIntentPlanV1(req, unsupported.LegacyPlan)
		}
		if err := validateNativeIntentPlanV2(req, plan); err != nil {
			return plan, rejectedIntentPlanV2(err, plan)
		}
		return plan, nil
	}
	legacyPlanner, ok := planner.(IntentPlanner)
	if !ok {
		return IntentPlanV2{}, fmt.Errorf("intent planner v2: %s implements neither v2 nor v1 planning", planner.Name())
	}
	legacy, err := legacyPlanner.PlanIntent(ctx, LegacyIntentPlanRequest(req))
	if err != nil {
		return IntentPlanV2{}, err
	}
	return AdaptIntentPlanV1(req, legacy)
}

func cloneIntentPlanV2Value(plan IntentPlanV2) IntentPlanV2 {
	clone := plan
	clone.Candidates = append([]IntentCandidateAssignment(nil), plan.Candidates...)
	for i := range clone.Candidates {
		clone.Candidates[i].SelectedSeqs = append(
			[]int64(nil), plan.Candidates[i].SelectedSeqs...)
		clone.Candidates[i].MissingCompanions = append(
			[]string(nil), plan.Candidates[i].MissingCompanions...)
		clone.Candidates[i].DependsOnCandidates = append(
			[]string(nil), plan.Candidates[i].DependsOnCandidates...)
	}
	return clone
}

func validateNativeIntentPlanV2(req IntentPlanRequestV2, plan IntentPlanV2) error {
	if plan.ProtocolVersion != IntentPlannerProtocolV2 {
		return v2ValidationError("", IntentAtomicityDependency, "native_protocol_invalid",
			fmt.Sprintf("native provider must return protocol_version %q", IntentPlannerProtocolV2))
	}
	return ValidateIntentPlanV2(req, plan)
}

// LegacyIntentPlanRequest projects common fields for v1 provider compatibility.
func LegacyIntentPlanRequest(req IntentPlanRequestV2) IntentPlanRequest {
	return IntentPlanRequest{
		LatestCommit:          cloneCommitSummary(req.LatestCommit),
		PathCommitContext:     clonePathCommitContext(req.PathCommitContext),
		OfferedCaptures:       append([]OfferedCapture(nil), req.OfferedCaptures...),
		ForcedAging:           req.ForcedAging,
		CommitFormat:          req.CommitFormat,
		CapturedDiffTransform: req.CapturedDiffTransform,
	}
}

// ValidateIntentPlanRequestV2 rejects request metadata that would exceed the
// durable/privacy contract or make dependency reasoning ambiguous.
func ValidateIntentPlanRequestV2(req IntentPlanRequestV2) error {
	if req.ProtocolVersion != IntentPlannerProtocolV2 {
		return fmt.Errorf("intent planner v2: protocol_version must be %q", IntentPlannerProtocolV2)
	}
	if len(req.OfferedCaptures) == 0 {
		return errors.New("intent planner v2: offered_captures must be non-empty")
	}
	if len(req.OfferedCaptures) > IntentCandidateCaptureCap {
		return fmt.Errorf("intent planner v2: offered captures exceed cap %d", IntentCandidateCaptureCap)
	}
	if len(req.Candidates) > IntentOpenCandidateCap {
		return fmt.Errorf("intent planner v2: candidate summaries exceed cap %d", IntentOpenCandidateCap)
	}
	if len(req.Dependencies) > IntentDependencyEdgeCap {
		return fmt.Errorf("intent planner v2: dependency edges exceed cap %d", IntentDependencyEdgeCap)
	}
	if len(req.ActivityBoundaries) > IntentActivityBoundaryCap {
		return fmt.Errorf("intent planner v2: activity boundaries exceed cap %d", IntentActivityBoundaryCap)
	}
	if len(req.RecentSoftCommits) > IntentRecentSoftCommitCap {
		return fmt.Errorf("intent planner v2: recent soft commits exceed cap %d", IntentRecentSoftCommitCap)
	}
	if len(req.PriorAtomicityFindings) > IntentPriorFindingCap {
		return fmt.Errorf("intent planner v2: prior atomicity findings exceed cap %d", IntentPriorFindingCap)
	}
	offered := make(map[int64]struct{}, len(req.OfferedCaptures))
	knownSeqs := make(map[int64]struct{}, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		if capture.Seq == 0 {
			return errors.New("intent planner v2: offered capture seq must be non-zero")
		}
		if _, ok := offered[capture.Seq]; ok {
			return fmt.Errorf("intent planner v2: duplicate offered seq %d", capture.Seq)
		}
		offered[capture.Seq] = struct{}{}
		knownSeqs[capture.Seq] = struct{}{}
	}
	candidateIDs := make(map[string]struct{}, len(req.Candidates))
	candidateSeqOwners := make(map[int64]string)
	for i, candidate := range req.Candidates {
		if err := validateBoundedText("candidate_id", candidate.CandidateID, IntentCandidateIDCap, true); err != nil {
			return fmt.Errorf("intent planner v2: candidates[%d]: %w", i, err)
		}
		if _, exists := candidateIDs[candidate.CandidateID]; exists {
			return fmt.Errorf("intent planner v2: duplicate candidate_id %q", candidate.CandidateID)
		}
		candidateIDs[candidate.CandidateID] = struct{}{}
		if err := validateBoundedText("status", candidate.Status, IntentCandidateStatusCap, true); err != nil {
			return fmt.Errorf("intent planner v2: candidates[%d]: %w", i, err)
		}
		if err := validateBoundedText("purpose", candidate.Purpose, IntentCandidatePurposeCap, false); err != nil {
			return fmt.Errorf("intent planner v2: candidates[%d]: %w", i, err)
		}
		if err := validateStringList("missing_companions", candidate.MissingCompanions, IntentAtomicitySummaryCap); err != nil {
			return fmt.Errorf("intent planner v2: candidates[%d]: %w", i, err)
		}
		if len(candidate.SelectedSeqs) > IntentCandidateCaptureCap {
			return fmt.Errorf("intent planner v2: candidates[%d] selected seqs exceed cap %d", i, IntentCandidateCaptureCap)
		}
		for _, seq := range candidate.SelectedSeqs {
			if seq == 0 {
				return fmt.Errorf("intent planner v2: candidates[%d] has zero selected seq", i)
			}
			if owner, duplicate := candidateSeqOwners[seq]; duplicate {
				return fmt.Errorf("intent planner v2: candidate seq %d belongs to %q and %q", seq, owner, candidate.CandidateID)
			}
			candidateSeqOwners[seq] = candidate.CandidateID
			knownSeqs[seq] = struct{}{}
		}
		if err := validateContextPaths(candidate.Paths); err != nil {
			return fmt.Errorf("intent planner v2: candidates[%d]: %w", i, err)
		}
	}
	edges := make(map[string]struct{}, len(req.Dependencies))
	for i, edge := range req.Dependencies {
		if edge.FromSeq == 0 || edge.ToSeq == 0 || edge.FromSeq == edge.ToSeq {
			return fmt.Errorf("intent planner v2: dependencies[%d] has invalid endpoints", i)
		}
		if _, ok := knownSeqs[edge.FromSeq]; !ok {
			return fmt.Errorf("intent planner v2: dependencies[%d] from_seq %d has no visible or candidate capture", i, edge.FromSeq)
		}
		if _, ok := knownSeqs[edge.ToSeq]; !ok {
			return fmt.Errorf("intent planner v2: dependencies[%d] to_seq %d has no visible or candidate capture", i, edge.ToSeq)
		}
		if edge.Strength != IntentDependencyHard && edge.Strength != IntentDependencySoft {
			return fmt.Errorf("intent planner v2: dependencies[%d] has invalid strength %q", i, edge.Strength)
		}
		if err := validateBoundedText("kind", edge.Kind, IntentDependencyKindCap, true); err != nil {
			return fmt.Errorf("intent planner v2: dependencies[%d]: %w", i, err)
		}
		if err := validateBoundedText("evidence_hash", edge.EvidenceHash, IntentEvidenceHashCap, false); err != nil {
			return fmt.Errorf("intent planner v2: dependencies[%d]: %w", i, err)
		}
		key := fmt.Sprintf("%d\x00%d\x00%s\x00%s", edge.FromSeq, edge.ToSeq, edge.Strength, edge.Kind)
		if _, exists := edges[key]; exists {
			return fmt.Errorf("intent planner v2: duplicate dependency %d -> %d", edge.FromSeq, edge.ToSeq)
		}
		edges[key] = struct{}{}
	}
	for i, boundary := range req.ActivityBoundaries {
		if boundary.Kind != "soft" && boundary.Kind != "hard" {
			return fmt.Errorf("intent planner v2: activity_boundaries[%d] has invalid kind %q", i, boundary.Kind)
		}
		if err := validateBoundedText("epoch", boundary.Epoch, IntentBoundaryEpochCap, true); err != nil {
			return fmt.Errorf("intent planner v2: activity_boundaries[%d]: %w", i, err)
		}
	}
	for i, finding := range req.PriorAtomicityFindings {
		if err := validateAtomicityFinding(finding); err != nil {
			return fmt.Errorf("intent planner v2: prior_atomicity_findings[%d]: %w", i, err)
		}
	}
	for i, commit := range req.RecentSoftCommits {
		if err := validateBoundedText("candidate_id", commit.CandidateID, IntentCandidateIDCap, true); err != nil {
			return fmt.Errorf("intent planner v2: recent_soft_commits[%d]: %w", i, err)
		}
		if err := validateBoundedText("oid", commit.OID, IntentEvidenceHashCap, true); err != nil {
			return fmt.Errorf("intent planner v2: recent_soft_commits[%d]: %w", i, err)
		}
		if err := validateBoundedText("subject", commit.Subject, SubjectCap, true); err != nil {
			return fmt.Errorf("intent planner v2: recent_soft_commits[%d]: %w", i, err)
		}
		if err := validateContextPaths(commit.Paths); err != nil {
			return fmt.Errorf("intent planner v2: recent_soft_commits[%d]: %w", i, err)
		}
	}
	return nil
}

// ValidateIntentPlanV2 enforces exact assignment, candidate DAG safety, hard
// capture dependencies, completeness, and evidence-backed cohesion.
func ValidateIntentPlanV2(req IntentPlanRequestV2, plan IntentPlanV2) error {
	if err := ValidateIntentPlanRequestV2(req); err != nil {
		return err
	}
	if plan.ProtocolVersion != IntentPlannerProtocolV2 && plan.ProtocolVersion != IntentPlannerProtocolV1Compat {
		return v2ValidationError("", IntentAtomicityDependency, "protocol_invalid",
			fmt.Sprintf("protocol_version must be %q or %q", IntentPlannerProtocolV2, IntentPlannerProtocolV1Compat))
	}
	if len(plan.Candidates) == 0 || len(plan.Candidates) > IntentOpenCandidateCap {
		return v2ValidationError("", IntentAtomicityCohesion, "candidate_count_invalid",
			fmt.Sprintf("candidate count must be between 1 and %d", IntentOpenCandidateCap))
	}
	offered := make(map[int64]struct{}, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		offered[capture.Seq] = struct{}{}
	}
	persistedByID := make(map[string]IntentCandidateSummary, len(req.Candidates))
	persistedOwnerBySeq := make(map[int64]string)
	for _, candidate := range req.Candidates {
		persistedByID[candidate.CandidateID] = candidate
		for _, seq := range candidate.SelectedSeqs {
			persistedOwnerBySeq[seq] = candidate.CandidateID
		}
	}
	assignments := make(map[int64]int, len(offered))
	candidateByID := make(map[string]int, len(plan.Candidates))
	for i, candidate := range plan.Candidates {
		if err := validateCandidateAssignment(candidate, i); err != nil {
			return err
		}
		if _, exists := candidateByID[candidate.CandidateID]; exists {
			return v2ValidationError(candidate.CandidateID, IntentAtomicitySeparation, "candidate_id_duplicate",
				fmt.Sprintf("duplicate candidate_id %q", candidate.CandidateID))
		}
		candidateByID[candidate.CandidateID] = i
		for _, seq := range candidate.SelectedSeqs {
			if _, exists := offered[seq]; !exists {
				return v2ValidationError(candidate.CandidateID, IntentAtomicityDependency, "capture_outside_window",
					fmt.Sprintf("selected seq %d is outside the offered window", seq))
			}
			if owner, exists := assignments[seq]; exists {
				return v2ValidationError(candidate.CandidateID, IntentAtomicitySeparation, "capture_assigned_twice",
					fmt.Sprintf("seq %d is assigned to candidates %q and %q", seq, plan.Candidates[owner].CandidateID, candidate.CandidateID))
			}
			assignments[seq] = i
		}
	}
	unionCandidateIDs := make(map[string]struct{}, len(req.Candidates)+len(plan.Candidates))
	for id := range persistedByID {
		unionCandidateIDs[id] = struct{}{}
	}
	for id := range candidateByID {
		unionCandidateIDs[id] = struct{}{}
	}
	if len(unionCandidateIDs) > IntentOpenCandidateCap {
		return v2ValidationError("", IntentAtomicityCohesion, "open_candidate_cap_exceeded",
			fmt.Sprintf("candidate updates would exceed open candidate cap %d", IntentOpenCandidateCap))
	}
	for _, capture := range req.OfferedCaptures {
		if _, ok := assignments[capture.Seq]; !ok {
			return v2ValidationError("", IntentAtomicityCompleteness, "capture_unassigned",
				fmt.Sprintf("offered seq %d is not assigned to a candidate", capture.Seq))
		}
	}
	if req.ForcedAging {
		for _, capture := range req.OfferedCaptures {
			candidate := plan.Candidates[assignments[capture.Seq]]
			if candidate.Readiness != IntentCandidateReady ||
				len(candidate.MissingCompanions) > 0 {
				return v2ValidationError(candidate.CandidateID,
					IntentAtomicityCompleteness, "forced_capture_deferred",
					fmt.Sprintf(
						"forced-aging seq %d must be ready with no missing companions",
						capture.Seq))
			}
		}
	}

	dependencies := make([]map[int]struct{}, len(plan.Candidates))
	declaredDependencyIDs := make([]map[string]struct{}, len(plan.Candidates))
	for i, candidate := range plan.Candidates {
		dependencies[i] = make(map[int]struct{}, len(candidate.DependsOnCandidates))
		declaredDependencyIDs[i] = make(map[string]struct{}, len(candidate.DependsOnCandidates))
		for _, dependencyID := range candidate.DependsOnCandidates {
			declaredDependencyIDs[i][dependencyID] = struct{}{}
			dependencyIndex, ok := candidateByID[dependencyID]
			if !ok {
				persisted, persistedOK := persistedByID[dependencyID]
				if !persistedOK {
					return v2ValidationError(candidate.CandidateID, IntentAtomicityDependency, "candidate_dependency_unknown",
						fmt.Sprintf("depends_on candidate %q is unknown", dependencyID))
				}
				if candidate.Readiness == IntentCandidateReady && !persisted.Ready {
					return v2ValidationError(candidate.CandidateID, IntentAtomicityDependency, "dependency_not_ready",
						fmt.Sprintf("ready candidate depends on waiting persisted candidate %q", dependencyID))
				}
				continue
			}
			if dependencyIndex == i {
				return v2ValidationError(candidate.CandidateID, IntentAtomicityDependency, "candidate_dependency_self",
					"candidate cannot depend on itself")
			}
			if _, duplicate := dependencies[i][dependencyIndex]; duplicate {
				return v2ValidationError(candidate.CandidateID, IntentAtomicityDependency, "candidate_dependency_duplicate",
					fmt.Sprintf("candidate dependency %q is duplicated", dependencyID))
			}
			dependencies[i][dependencyIndex] = struct{}{}
			if dependencyIndex > i {
				return v2ValidationError(candidate.CandidateID, IntentAtomicityDependency, "publish_order_not_topological",
					fmt.Sprintf("candidate depends on later candidate %q", dependencyID))
			}
			if candidate.Readiness == IntentCandidateReady && plan.Candidates[dependencyIndex].Readiness != IntentCandidateReady {
				return v2ValidationError(candidate.CandidateID, IntentAtomicityDependency, "dependency_not_ready",
					fmt.Sprintf("ready candidate depends on waiting candidate %q", dependencyID))
			}
		}
	}
	if hasCandidateCycle(dependencies) {
		return v2ValidationError("", IntentAtomicityDependency, "candidate_dependency_cycle",
			"candidate dependency graph contains a cycle")
	}

	knownSeqs := make(map[int64]struct{}, len(offered)+len(persistedOwnerBySeq))
	for seq := range offered {
		knownSeqs[seq] = struct{}{}
	}
	for seq := range persistedOwnerBySeq {
		knownSeqs[seq] = struct{}{}
	}
	adjacent := make(map[int64]map[int64]struct{}, len(knownSeqs))
	for _, edge := range req.Dependencies {
		toCandidate := assignments[edge.ToSeq]
		_, fromKnown := knownSeqs[edge.FromSeq]
		_, toKnown := knownSeqs[edge.ToSeq]
		if fromKnown && toKnown && edgeSupportsCohesion(edge) {
			addUndirectedEdge(adjacent, edge.FromSeq, edge.ToSeq)
		}
		if edge.Strength == IntentDependencyHard {
			fromID, fromOutput, fromKnown := intentDependencyOwner(edge.FromSeq, assignments, plan, persistedOwnerBySeq)
			toID, toOutput, toKnown := intentDependencyOwner(edge.ToSeq, assignments, plan, persistedOwnerBySeq)
			if fromKnown && toKnown && fromID != toID {
				if !toOutput {
					if fromOutput {
						return v2ValidationError(toID, IntentAtomicityDependency, "persisted_dependent_not_updated",
							fmt.Sprintf("persisted candidate %q depends on newly assigned seq %d and must be updated", toID, edge.FromSeq))
					}
					continue
				}
				if _, declared := declaredDependencyIDs[toCandidate][fromID]; !declared {
					return v2ValidationError(plan.Candidates[toCandidate].CandidateID, IntentAtomicityDependency, "hard_dependency_undeclared",
						fmt.Sprintf("hard dependency %d -> %d crosses candidates without depends_on_candidates", edge.FromSeq, edge.ToSeq))
				}
			}
		}
	}
	// A model may identify semantic cohesion that the bounded local graph does
	// not already contain. The daemon still subjects that exact membership to
	// hard-boundary, dependency, materialization, verification, and Git gates.
	// Evidence fallbacks remain graph-derived; this validator must not turn a
	// missing soft edge into a provider outage.
	return nil
}

func validateCandidateAssignment(candidate IntentCandidateAssignment, index int) error {
	if err := validateBoundedText("candidate_id", candidate.CandidateID, IntentCandidateIDCap, true); err != nil {
		return v2ValidationError(candidate.CandidateID, IntentAtomicityCohesion, "candidate_id_invalid",
			fmt.Sprintf("candidates[%d]: %v", index, err))
	}
	if len(candidate.SelectedSeqs) == 0 || len(candidate.SelectedSeqs) > IntentCandidateCaptureCap {
		return v2ValidationError(candidate.CandidateID, IntentAtomicityCohesion, "candidate_capture_count_invalid",
			fmt.Sprintf("selected_seqs must contain between 1 and %d captures", IntentCandidateCaptureCap))
	}
	if err := validateBoundedText("purpose", candidate.Purpose, IntentCandidatePurposeCap, true); err != nil {
		return v2ValidationError(candidate.CandidateID, IntentAtomicityCohesion, "purpose_invalid", err.Error())
	}
	if err := validateBoundedText("grouping_reason", candidate.GroupingReason, IntentReasonCap, true); err != nil {
		return v2ValidationError(candidate.CandidateID, IntentAtomicityCohesion, "grouping_reason_invalid", err.Error())
	}
	if err := validateStringList("missing_companions", candidate.MissingCompanions, IntentAtomicitySummaryCap); err != nil {
		return v2ValidationError(candidate.CandidateID, IntentAtomicityCompleteness, "missing_companions_invalid", err.Error())
	}
	switch candidate.Readiness {
	case IntentCandidateReady:
		if len(candidate.MissingCompanions) > 0 {
			return v2ValidationError(candidate.CandidateID, IntentAtomicityCompleteness, "ready_with_missing_companions",
				"ready candidate still lists missing companions")
		}
		if strings.TrimSpace(candidate.Subject) == "" {
			return v2ValidationError(candidate.CandidateID, IntentAtomicityCohesion, "ready_subject_empty",
				"ready candidate subject must be non-empty")
		}
	case IntentCandidateWait:
	default:
		return v2ValidationError(candidate.CandidateID, IntentAtomicityCompleteness, "readiness_invalid",
			fmt.Sprintf("readiness must be %q or %q", IntentCandidateReady, IntentCandidateWait))
	}
	seen := make(map[int64]struct{}, len(candidate.SelectedSeqs))
	for _, seq := range candidate.SelectedSeqs {
		if seq == 0 {
			return v2ValidationError(candidate.CandidateID, IntentAtomicityCohesion, "capture_seq_invalid",
				"selected seq must be non-zero")
		}
		if _, duplicate := seen[seq]; duplicate {
			return v2ValidationError(candidate.CandidateID, IntentAtomicitySeparation, "capture_seq_duplicate",
				fmt.Sprintf("selected seq %d is duplicated", seq))
		}
		seen[seq] = struct{}{}
	}
	dependencyIDs := make(map[string]struct{}, len(candidate.DependsOnCandidates))
	for _, id := range candidate.DependsOnCandidates {
		if err := validateBoundedText("depends_on_candidates", id, IntentCandidateIDCap, true); err != nil {
			return v2ValidationError(candidate.CandidateID, IntentAtomicityDependency, "candidate_dependency_invalid", err.Error())
		}
		if _, duplicate := dependencyIDs[id]; duplicate {
			return v2ValidationError(candidate.CandidateID, IntentAtomicityDependency, "candidate_dependency_duplicate",
				fmt.Sprintf("candidate dependency %q is duplicated", id))
		}
		dependencyIDs[id] = struct{}{}
	}
	return nil
}

// AdaptIntentPlanV1 converts one safe legacy response into durable one-pass
// candidate updates and labels the result v1_compat. Deferred captures become
// singleton waiting candidates so the adapter never invents semantic cohesion.
func AdaptIntentPlanV1(req IntentPlanRequestV2, legacy IntentPlan) (IntentPlanV2, error) {
	legacyReq := LegacyIntentPlanRequest(req)
	if err := ValidateIntentPlan(legacyReq, legacy); err != nil {
		return IntentPlanV2{}, err
	}
	groups, err := IntentPlanCommitGroups(legacy)
	if err != nil {
		return IntentPlanV2{}, err
	}
	plan := IntentPlanV2{ProtocolVersion: IntentPlannerProtocolV1Compat}
	for _, group := range groups {
		plan.Candidates = append(plan.Candidates, IntentCandidateAssignment{
			CandidateID:    legacyCompatCandidateID(group.SelectedSeqs),
			SelectedSeqs:   append([]int64(nil), group.SelectedSeqs...),
			Purpose:        NormalizeIntentReason(group.GroupingReason),
			Readiness:      IntentCandidateReady,
			Subject:        group.Subject,
			Body:           group.Body,
			GroupingReason: NormalizeIntentReason(group.GroupingReason),
		})
	}
	reasons := make(map[int64]string, len(legacy.DeferredReasons))
	for _, reason := range legacy.DeferredReasons {
		reasons[reason.Seq] = NormalizeIntentAtomicitySummary(reason.Reason)
	}
	for _, seq := range legacy.DeferredSeqs {
		reason := reasons[seq]
		if reason == "" {
			reason = IntentPlanReasonMarker
		}
		plan.Candidates = append(plan.Candidates, IntentCandidateAssignment{
			CandidateID:       fmt.Sprintf("v1-compat-wait-%d", seq),
			SelectedSeqs:      []int64{seq},
			Purpose:           "legacy planner deferred this capture",
			Readiness:         IntentCandidateWait,
			MissingCompanions: []string{reason},
			GroupingReason:    "translated from a legacy deferred decision",
		})
	}
	addCompatCandidateDependencies(req, &plan)
	if err := ValidateIntentPlanV2(req, plan); err != nil {
		return IntentPlanV2{}, fmt.Errorf("intent planner v1 compatibility: %w", err)
	}
	return plan, nil
}

func legacyCompatCandidateID(seqs []int64) string {
	ordered := append([]int64(nil), seqs...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	var input strings.Builder
	for i, seq := range ordered {
		if i > 0 {
			input.WriteByte(',')
		}
		fmt.Fprintf(&input, "%d", seq)
	}
	sum := sha256.Sum256([]byte(input.String()))
	return "v1-compat-ready-" + hex.EncodeToString(sum[:12])
}

func addCompatCandidateDependencies(req IntentPlanRequestV2, plan *IntentPlanV2) {
	assigned := make(map[int64]int)
	for i := range plan.Candidates {
		for _, seq := range plan.Candidates[i].SelectedSeqs {
			assigned[seq] = i
		}
	}
	persistedOwners := make(map[int64]string)
	for _, candidate := range req.Candidates {
		for _, seq := range candidate.SelectedSeqs {
			persistedOwners[seq] = candidate.CandidateID
		}
	}
	for _, edge := range req.Dependencies {
		if edge.Strength != IntentDependencyHard {
			continue
		}
		from, fromOK := assigned[edge.FromSeq]
		to, toOK := assigned[edge.ToSeq]
		if !toOK {
			continue
		}
		dependencyID := ""
		switch {
		case fromOK && from != to:
			dependencyID = plan.Candidates[from].CandidateID
		case !fromOK:
			dependencyID = persistedOwners[edge.FromSeq]
		}
		if dependencyID == "" || dependencyID == plan.Candidates[to].CandidateID {
			continue
		}
		if !containsString(plan.Candidates[to].DependsOnCandidates, dependencyID) {
			plan.Candidates[to].DependsOnCandidates = append(plan.Candidates[to].DependsOnCandidates, dependencyID)
		}
	}
}

func intentDependencyOwner(
	seq int64,
	assignments map[int64]int,
	plan IntentPlanV2,
	persistedOwners map[int64]string,
) (id string, output bool, known bool) {
	if index, ok := assignments[seq]; ok {
		return plan.Candidates[index].CandidateID, true, true
	}
	id, ok := persistedOwners[seq]
	return id, false, ok
}

// BuildIntentPlanV2UserPrompt serializes the bounded request and includes
// correction findings without exposing any raw source beyond the already
// redacted/capped offered capture diffs.
func BuildIntentPlanV2UserPrompt(req IntentPlanRequestV2) (string, error) {
	if err := ValidateIntentPlanRequestV2(req); err != nil {
		return "", err
	}
	body, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("intent planner v2: marshal request: %w", err)
	}
	out := "Plan durable semantic commit candidates for these offered captures:\n" + string(body)
	if correction := NormalizeIntentAtomicityCorrection(req.RetryCorrection); correction != "" {
		out += "\n\nThe previous candidate plan failed atomicity validation:\n" + correction +
			"\n\nReturn a corrected v2 candidate plan. Assign every offered seq exactly once, split disconnected components, preserve every hard dependency, and do not mark a candidate ready while it has missing companions."
	}
	return out, nil
}

// IntentPlannerV2SystemPrompt returns the stable candidate protocol
// instructions. Activity and time are explicitly weak evidence: they may
// trigger evaluation but cannot alone justify a multi-capture candidate.
func IntentPlannerV2SystemPrompt(format ...CommitFormat) string {
	selected := CommitFormatImperative
	if len(format) > 0 {
		selected = format[0]
	}
	return "You are a semantic intent planner for atomic git commits. " +
		"Return only a v2 candidate plan. Assign every offered seq to exactly one candidate. " +
		"Candidate order must be topological. Non-contiguous capture groups are allowed when dependency evidence proves independence and ordering. " +
		"A group may add semantic cohesion not present in the dependency graph when the exact diffs prove one intent. Never group by time or directory alone. " +
		"Activity epochs and temporal proximity may trigger evaluation but cannot alone prove cohesion. " +
		"Mark readiness=wait when any required companion is missing. A ready candidate must not depend on a waiting candidate. " +
		"Keep raw-source reasoning out of purpose, grouping_reason, and missing_companions. " +
		CommitMessageFormatInstructions(selected) + " " +
		"Keep grouping rationale in grouping_reason, not in the commit body."
}

// BuildIntentAtomicityCorrection returns a deterministic bounded correction
// block suitable for RetryCorrection.
func BuildIntentAtomicityCorrection(findings []IntentAtomicityFinding) string {
	findings = normalizeAtomicityFindings(findings)
	var lines []string
	for _, finding := range findings {
		lines = append(lines, fmt.Sprintf(
			"candidate=%s gate=%s code=%s: %s",
			finding.CandidateID,
			finding.Gate,
			finding.Code,
			finding.Summary,
		))
	}
	return NormalizeIntentAtomicityCorrection(strings.Join(lines, "\n"))
}

// IntentPlanV2CorrectionEligible reports whether one remote correction can
// safely improve planner metadata without asking the model to reconsider
// capture membership or dependency evidence. Structural findings fall through
// to the deterministic preset fallback instead of consuming another model
// call.
func IntentPlanV2CorrectionEligible(findings []IntentAtomicityFinding) bool {
	if len(findings) == 0 {
		return false
	}
	for _, finding := range findings {
		switch finding.Code {
		case "protocol_invalid",
			"candidate_id_invalid",
			"candidate_id_duplicate",
			"purpose_invalid",
			"grouping_reason_invalid",
			"missing_companions_invalid",
			"readiness_invalid",
			"ready_subject_empty",
			"ready_with_missing_companions",
			"candidate_dependency_unknown",
			"candidate_dependency_self",
			"candidate_dependency_duplicate",
			"publish_order_not_topological",
			"dependency_not_ready":
			continue
		default:
			return false
		}
	}
	return true
}

// NewIntentAtomicityReport combines planner-structural results with
// materialization, verification, and revertibility results supplied by the
// candidate engine. Missing results remain pending and therefore not valid.
func NewIntentAtomicityReport(candidateID string, results ...IntentAtomicityGateResult) IntentAtomicityReport {
	byGate := make(map[IntentAtomicityGate]IntentAtomicityGateResult, len(results))
	for _, result := range results {
		if !validAtomicityGate(result.Gate) || !validAtomicityStatus(result.Status) {
			continue
		}
		if result.Finding != nil {
			findings := normalizeAtomicityFindings([]IntentAtomicityFinding{*result.Finding})
			if len(findings) == 1 {
				result.Finding = &findings[0]
			} else {
				result.Finding = nil
			}
		}
		byGate[result.Gate] = result
	}
	report := IntentAtomicityReport{
		CandidateID: candidateID,
		Gates:       make([]IntentAtomicityGateResult, 0, len(intentAtomicityGates)),
		Valid:       true,
	}
	for _, gate := range intentAtomicityGates {
		result, ok := byGate[gate]
		if !ok {
			result = IntentAtomicityGateResult{Gate: gate, Status: IntentAtomicityPending}
		}
		report.Gates = append(report.Gates, result)
		if !atomicityGateAllowsPublish(result.Gate, result.Status) {
			report.Valid = false
		}
	}
	return report
}

// ValidateIntentAtomicityReport prevents callers from publishing a partial or
// failed seven-gate report.
func ValidateIntentAtomicityReport(report IntentAtomicityReport) error {
	if err := validateBoundedText("candidate_id", report.CandidateID, IntentCandidateIDCap, true); err != nil {
		return fmt.Errorf("intent atomicity: %w", err)
	}
	if len(report.Gates) != len(intentAtomicityGates) {
		return fmt.Errorf("intent atomicity: report must contain exactly %d gates", len(intentAtomicityGates))
	}
	seen := make(map[IntentAtomicityGate]struct{}, len(report.Gates))
	for _, result := range report.Gates {
		if !validAtomicityGate(result.Gate) {
			return fmt.Errorf("intent atomicity: unknown gate %q", result.Gate)
		}
		if _, duplicate := seen[result.Gate]; duplicate {
			return fmt.Errorf("intent atomicity: duplicate gate %q", result.Gate)
		}
		seen[result.Gate] = struct{}{}
		if !validAtomicityStatus(result.Status) {
			return fmt.Errorf("intent atomicity: gate %q has invalid status %q", result.Gate, result.Status)
		}
		if result.Status == IntentAtomicityNotRequired && result.Gate != IntentAtomicityVerification {
			return fmt.Errorf("intent atomicity: gate %q cannot be not_required", result.Gate)
		}
		if result.Status == IntentAtomicityFailed && result.Finding == nil {
			return fmt.Errorf("intent atomicity: failed gate %q requires a finding", result.Gate)
		}
		if result.Finding != nil {
			if err := validateAtomicityFinding(*result.Finding); err != nil {
				return fmt.Errorf("intent atomicity: gate %q: %w", result.Gate, err)
			}
			if result.Finding.Gate != result.Gate {
				return fmt.Errorf("intent atomicity: finding gate %q does not match result gate %q", result.Finding.Gate, result.Gate)
			}
			if result.Finding.CandidateID != "" && result.Finding.CandidateID != report.CandidateID {
				return fmt.Errorf("intent atomicity: finding candidate %q does not match report candidate %q", result.Finding.CandidateID, report.CandidateID)
			}
		}
	}
	computedValid := true
	for _, result := range report.Gates {
		if !atomicityGateAllowsPublish(result.Gate, result.Status) {
			computedValid = false
		}
	}
	if report.Valid != computedValid {
		return fmt.Errorf("intent atomicity: valid=%t does not match gate results", report.Valid)
	}
	return nil
}

// NormalizeIntentAtomicitySummary removes control characters and caps
// persisted/correction summaries to the state contract.
func NormalizeIntentAtomicitySummary(summary string) string {
	return normalizeBoundedText(summary, IntentAtomicitySummaryCap)
}

// NormalizeIntentAtomicityCorrection bounds the entire retry block.
func NormalizeIntentAtomicityCorrection(correction string) string {
	correction = strings.Map(func(r rune) rune {
		if r == '\n' {
			return r
		}
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, correction)
	correction = strings.TrimSpace(correction)
	runes := []rune(correction)
	if len(runes) <= IntentAtomicityCorrectionCap {
		return correction
	}
	return string(runes[:IntentAtomicityCorrectionCap])
}

func v2ValidationError(candidateID string, gate IntentAtomicityGate, code, summary string) error {
	finding := IntentAtomicityFinding{
		CandidateID: normalizeBoundedText(candidateID, IntentCandidateIDCap),
		Gate:        gate,
		Code:        normalizeBoundedText(code, IntentDependencyKindCap),
		Summary:     NormalizeIntentAtomicitySummary(summary),
	}
	return &IntentPlanV2ValidationError{
		Message:  fmt.Sprintf("intent planner v2: %s: %s", code, finding.Summary),
		Findings: []IntentAtomicityFinding{finding},
	}
}

func validateBoundedText(field, value string, cap int, required bool) error {
	if required && strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s must be non-empty", field)
	}
	if utf8.RuneCountInString(value) > cap {
		return fmt.Errorf("%s exceeds %d characters", field, cap)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%s contains control characters", field)
		}
	}
	return nil
}

func validateStringList(field string, values []string, totalCap int) error {
	total := 0
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s contains an empty value", field)
		}
		for _, r := range value {
			if r < 0x20 || r == 0x7f {
				return fmt.Errorf("%s contains control characters", field)
			}
		}
		total += utf8.RuneCountInString(value)
		if total > totalCap {
			return fmt.Errorf("%s exceeds %d characters", field, totalCap)
		}
	}
	return nil
}

func validateContextPaths(paths []string) error {
	if len(paths) > IntentContextPathsCap {
		return fmt.Errorf("paths exceed item cap %d", IntentContextPathsCap)
	}
	for _, path := range paths {
		if err := validateBoundedText("path", path, IntentContextPathCap, true); err != nil {
			return err
		}
	}
	return nil
}

func validateAtomicityFinding(finding IntentAtomicityFinding) error {
	if !validAtomicityGate(finding.Gate) {
		return fmt.Errorf("unknown gate %q", finding.Gate)
	}
	if err := validateBoundedText("candidate_id", finding.CandidateID, IntentCandidateIDCap, false); err != nil {
		return err
	}
	if err := validateBoundedText("code", finding.Code, IntentDependencyKindCap, true); err != nil {
		return err
	}
	return validateBoundedText("summary", finding.Summary, IntentAtomicitySummaryCap, true)
}

func normalizeAtomicityFindings(in []IntentAtomicityFinding) []IntentAtomicityFinding {
	out := make([]IntentAtomicityFinding, 0, len(in))
	for _, finding := range in {
		if !validAtomicityGate(finding.Gate) {
			continue
		}
		finding.CandidateID = normalizeBoundedText(finding.CandidateID, IntentCandidateIDCap)
		finding.Code = normalizeBoundedText(finding.Code, IntentDependencyKindCap)
		finding.Summary = NormalizeIntentAtomicitySummary(finding.Summary)
		if finding.Code == "" || finding.Summary == "" {
			continue
		}
		out = append(out, finding)
	}
	return out
}

func normalizeBoundedText(value string, cap int) string {
	value = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, value)
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) <= cap {
		return value
	}
	return string(runes[:cap])
}

func cloneCandidateSummaries(in []IntentCandidateSummary) []IntentCandidateSummary {
	out := make([]IntentCandidateSummary, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].SelectedSeqs = append([]int64(nil), in[i].SelectedSeqs...)
		out[i].Paths = append([]string(nil), in[i].Paths...)
		out[i].MissingCompanions = append([]string(nil), in[i].MissingCompanions...)
	}
	return out
}

func cloneSoftCommitSummaries(in []IntentSoftCommitSummary) []IntentSoftCommitSummary {
	out := make([]IntentSoftCommitSummary, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].Paths = append([]string(nil), in[i].Paths...)
	}
	return out
}

func hasCandidateCycle(dependencies []map[int]struct{}) bool {
	state := make([]uint8, len(dependencies))
	var visit func(int) bool
	visit = func(node int) bool {
		switch state[node] {
		case 1:
			return true
		case 2:
			return false
		}
		state[node] = 1
		for dependency := range dependencies[node] {
			if visit(dependency) {
				return true
			}
		}
		state[node] = 2
		return false
	}
	for i := range dependencies {
		if visit(i) {
			return true
		}
	}
	return false
}

func addUndirectedEdge(adjacency map[int64]map[int64]struct{}, a, b int64) {
	if adjacency[a] == nil {
		adjacency[a] = make(map[int64]struct{})
	}
	if adjacency[b] == nil {
		adjacency[b] = make(map[int64]struct{})
	}
	adjacency[a][b] = struct{}{}
	adjacency[b][a] = struct{}{}
}

func edgeSupportsCohesion(edge IntentCaptureDependency) bool {
	if edge.Strength == IntentDependencyHard {
		return true
	}
	switch edge.Kind {
	case "activity_epoch", "temporal_proximity", "module_proximity":
		return false
	default:
		return true
	}
}

func seqSetConnected(seqs []int64, adjacency map[int64]map[int64]struct{}) bool {
	allowed := make(map[int64]struct{}, len(seqs))
	for _, seq := range seqs {
		allowed[seq] = struct{}{}
	}
	seen := map[int64]struct{}{seqs[0]: {}}
	queue := []int64{seqs[0]}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for next := range adjacency[current] {
			if _, ok := allowed[next]; !ok {
				continue
			}
			if _, ok := seen[next]; ok {
				continue
			}
			seen[next] = struct{}{}
			queue = append(queue, next)
		}
	}
	return len(seen) == len(allowed)
}

func validAtomicityGate(gate IntentAtomicityGate) bool {
	for _, candidate := range intentAtomicityGates {
		if gate == candidate {
			return true
		}
	}
	return false
}

func validAtomicityStatus(status IntentAtomicityStatus) bool {
	switch status {
	case IntentAtomicityPassed, IntentAtomicityFailed, IntentAtomicityPending, IntentAtomicityNotRequired:
		return true
	default:
		return false
	}
}

func atomicityGateAllowsPublish(gate IntentAtomicityGate, status IntentAtomicityStatus) bool {
	return status == IntentAtomicityPassed ||
		(gate == IntentAtomicityVerification && status == IntentAtomicityNotRequired)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// SortedIntentCandidateIDs is useful for deterministic diagnostics.
func SortedIntentCandidateIDs(plan IntentPlanV2) []string {
	ids := make([]string, 0, len(plan.Candidates))
	for _, candidate := range plan.Candidates {
		ids = append(ids, candidate.CandidateID)
	}
	sort.Strings(ids)
	return ids
}
