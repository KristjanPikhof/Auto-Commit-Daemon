package state

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	IntentCandidateMaxCaptures           = 256
	IntentCandidateMaxOpenPerPair        = 128
	IntentDependencyMaxPerPair           = 4096
	IntentCandidatePurposeMaxChars       = 512
	IntentCandidateSummaryMaxChars       = 2048
	IntentCandidateLineageReasonMaxChars = 512
	IntentCandidateLineageMaxPerPair     = 4096
	IntentVerificationOutputMaxBytes     = 64 * 1024
	IntentRepairMaxCommits               = 5
	IntentRepairMaxMembers               = IntentCandidateMaxCaptures * IntentRepairMaxCommits

	IntentCandidateOpen          = "open"
	IntentCandidateWaiting       = "waiting"
	IntentCandidateReady         = "ready"
	IntentCandidateSoftPublished = "soft_published"
	IntentCandidatePublished     = "published"
	IntentCandidateSuperseded    = "superseded"
	IntentCandidateBlocked       = "blocked"
	IntentCandidateFailed        = "failed"

	IntentReadinessReady = "ready"
	IntentReadinessWait  = "wait"

	IntentMembershipActive     = "active"
	IntentMembershipSuperseded = "superseded"

	IntentDependencyHard = "hard"
	IntentDependencySoft = "soft"

	IntentBoundarySoft = "soft"
	IntentBoundaryHard = "hard"

	IntentRepairPrepared   = "prepared"
	IntentRepairGitApplied = "git_applied"
	IntentRepairCompleted  = "completed"
	IntentRepairSkipped    = "skipped"
	IntentRepairFailed     = "failed"

	IntentRepairMembershipLegacy = "legacy"
	IntentRepairMembershipFrozen = "frozen"
	IntentRepairMembershipNone   = "none"
)

// IntentCandidate is one durable semantic commit candidate. It contains only
// bounded summaries and capture identities; raw diffs remain in capture_ops.
type IntentCandidate struct {
	ID                      string
	BranchRef               string
	BranchGeneration        int64
	Status                  string
	Purpose                 string
	CreatedTS               float64
	UpdatedTS               float64
	ReadyTS                 sql.NullFloat64
	Readiness               string
	MissingCompanions       string
	AtomicityStatus         sql.NullString
	AtomicitySummary        string
	AtomicityCheckedTS      sql.NullFloat64
	Provider                sql.NullString
	Model                   sql.NullString
	PlannerProtocol         sql.NullString
	ConfigRevisionID        sql.NullInt64
	ConfigProfile           sql.NullString
	PresetID                sql.NullString
	PresetVersion           sql.NullInt64
	SoftPublicationDeadline sql.NullFloat64
	VerificationStatus      sql.NullString
	VerificationOutput      string
	VerificationTS          sql.NullFloat64
	PublishedCommitOID      sql.NullString
	Events                  []IntentCandidateEvent
}

// IntentCandidateEvent records ordered candidate membership. Reassignment
// preserves the previous candidate's row as superseded.
type IntentCandidateEvent struct {
	CandidateID     string
	Ord             int
	EventSeq        int64
	EventRole       string
	MembershipState string
}

// IntentCandidateLineage records one direct canonical merge without source
// content. SourceStatus and SourcePublishedCommitOID preserve repair-relevant
// provenance from immediately before the source was superseded. A canonical
// target may later become another merge's source, so consumers that need full
// ancestry must traverse these direct edges transitively.
type IntentCandidateLineage struct {
	BranchRef                string
	BranchGeneration         int64
	TargetCandidateID        string
	SourceCandidateID        string
	SourceStatus             string
	SourcePublishedCommitOID sql.NullString
	Reason                   string
	CreatedTS                float64
}

type IntentCandidateMergeRequest struct {
	Target             IntentCandidate
	SourceCandidateIDs []string
	Reason             string
	MergedTS           float64
}

type IntentCandidateMergeResult struct {
	Candidate IntentCandidate
	Lineage   []IntentCandidateLineage
}

// IntentCaptureDependency is one hard ordering edge or soft semantic edge.
// Evidence must be a bounded, privacy-safe explanation or evidence hash.
type IntentCaptureDependency struct {
	ID               int64
	BranchRef        string
	BranchGeneration int64
	PrerequisiteSeq  int64
	DependentSeq     int64
	Strength         string
	Kind             string
	Evidence         string
	CreatedTS        float64
}

// IntentActivityBoundary is a repo-wide monotonically increasing evaluation
// epoch. It intentionally stores no prompt or source text.
type IntentActivityBoundary struct {
	ID               int64
	Epoch            int64
	Kind             string
	Source           string
	BranchRef        sql.NullString
	BranchGeneration sql.NullInt64
	CreatedTS        float64
	ConsumedTS       sql.NullFloat64
}

// IntentRepair is one crash-recoverable automatic history repair transaction.
type IntentRepair struct {
	ID               string
	BranchRef        string
	BranchGeneration int64
	Status           string
	ExpectedHead     string
	PlanDigest       string
	BackupRef        sql.NullString
	OldHead          sql.NullString
	NewHead          sql.NullString
	CreatedTS        float64
	UpdatedTS        float64
	GitAppliedTS     sql.NullFloat64
	CompletedTS      sql.NullFloat64
	Error            string
	MembershipMode   string
	Commits          []IntentRepairCommit
	Members          []IntentRepairMember
}

type IntentRepairCommit struct {
	RepairID    string
	Ord         int
	CandidateID sql.NullString
	OldOID      string
	NewOID      sql.NullString
}

// IntentRepairMember is one immutable active candidate membership captured
// before the repair is allowed to change Git. Legacy repairs have no members.
type IntentRepairMember struct {
	RepairID    string
	Ord         int
	CandidateID string
	EventSeq    int64
	PriorState  string
}

// IntentRepairTransition applies one compare-and-swap state transition and,
// when supplied, replaces the old-to-new commit mapping in the same
// transaction.
type IntentRepairTransition struct {
	ExpectedStatus string
	Status         string
	BackupRef      sql.NullString
	OldHead        sql.NullString
	NewHead        sql.NullString
	Error          string
	Commits        []IntentRepairCommit
	TransitionTS   float64
}

// IntentV2ReadOnlyProjection is the compatibility-safe summary exposed to
// status paths. Available is false for pre-v15 databases and no migration is
// attempted.
type IntentV2ReadOnlyProjection struct {
	Available                 bool
	SchemaVersion             int
	CandidateLineageAvailable bool
	CandidateLineageRecords   int
	OpenCandidates            int
	VerificationAttention     int
	RecoverableRepairs        int
	LastBoundaryEpoch         int64
}

// SaveIntentCandidate inserts or revises one candidate and its active
// membership atomically. Captures assigned elsewhere are superseded, never
// deleted, and all captures must belong to the candidate's exact branch pair.
func SaveIntentCandidate(ctx context.Context, d *DB, candidate IntentCandidate) error {
	if d == nil {
		return errors.New("state: SaveIntentCandidate: nil db")
	}
	if candidate.Readiness == "" {
		candidate.Readiness = IntentReadinessWait
	}
	if err := validateIntentCandidate(candidate); err != nil {
		return err
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("state: begin intent candidate save: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var existingStatus string
	err = tx.QueryRowContext(ctx,
		`SELECT status FROM intent_candidates WHERE id=?`, candidate.ID,
	).Scan(&existingStatus)
	switch {
	case err == nil && isTerminalIntentCandidateStatus(existingStatus):
		return fmt.Errorf("state: candidate %s is terminal in status %s", candidate.ID, existingStatus)
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("state: load existing candidate: %w", err)
	case errors.Is(err, sql.ErrNoRows):
		var open int
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_candidates
WHERE branch_ref=? AND branch_generation=?
  AND status IN ('open','waiting','ready','soft_published','blocked')
  AND EXISTS (
      SELECT 1 FROM intent_candidate_events active_membership
      WHERE active_membership.candidate_id=intent_candidates.id
        AND active_membership.membership_state='active'
  )`,
			candidate.BranchRef, candidate.BranchGeneration).Scan(&open); err != nil {
			return fmt.Errorf("state: count open intent candidates: %w", err)
		}
		if open >= IntentCandidateMaxOpenPerPair {
			return fmt.Errorf("state: open intent candidate cap %d exceeded for exact branch pair",
				IntentCandidateMaxOpenPerPair)
		}
	}

	for _, event := range candidate.Events {
		var exists int
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM capture_events
    WHERE seq=? AND branch_ref=? AND branch_generation=?
)`, event.EventSeq, candidate.BranchRef, candidate.BranchGeneration).Scan(&exists); err != nil {
			return fmt.Errorf("state: validate candidate event %d: %w", event.EventSeq, err)
		}
		if exists == 0 {
			return fmt.Errorf("state: candidate event %d does not belong to exact branch pair", event.EventSeq)
		}
	}

	now := candidate.UpdatedTS
	if now <= 0 {
		now = nowSeconds()
	}
	if err := upsertIntentCandidateRow(ctx, tx, candidate, now); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
UPDATE intent_candidate_events
SET membership_state='superseded'
WHERE candidate_id=? AND membership_state='active'`, candidate.ID); err != nil {
		return fmt.Errorf("state: supersede prior candidate membership: %w", err)
	}
	for ord, event := range candidate.Events {
		if _, err := tx.ExecContext(ctx, `
UPDATE intent_candidate_events
SET membership_state='superseded'
WHERE event_seq=? AND candidate_id<>? AND membership_state='active'`,
			event.EventSeq, candidate.ID); err != nil {
			return fmt.Errorf("state: supersede reassigned event %d: %w", event.EventSeq, err)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO intent_candidate_events(
    candidate_id, ord, event_seq, event_role, membership_state
) VALUES (?, ?, ?, ?, 'active')
ON CONFLICT(candidate_id, event_seq) DO UPDATE SET
    ord=excluded.ord,
    event_role=excluded.event_role,
    membership_state='active'`,
			candidate.ID, ord, event.EventSeq, event.EventRole); err != nil {
			return fmt.Errorf("state: save candidate event %d: %w", event.EventSeq, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE intent_candidates
SET status='superseded', readiness='wait', updated_ts=?
WHERE branch_ref=? AND branch_generation=? AND id<>?
  AND status IN ('open','waiting','ready','soft_published','blocked')
  AND NOT EXISTS (
      SELECT 1 FROM intent_candidate_events active_membership
      WHERE active_membership.candidate_id=intent_candidates.id
        AND active_membership.membership_state='active'
  )`, now, candidate.BranchRef, candidate.BranchGeneration, candidate.ID); err != nil {
		return fmt.Errorf("state: retire empty reassigned candidates: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("state: commit intent candidate save: %w", err)
	}
	return nil
}

func upsertIntentCandidateRow(
	ctx context.Context,
	tx *sql.Tx,
	candidate IntentCandidate,
	now float64,
) error {
	created := candidate.CreatedTS
	if created <= 0 {
		created = now
	}
	verifyOutput := sanitizedOutputTail(candidate.VerificationOutput)
	res, err := tx.ExecContext(ctx, `
INSERT INTO intent_candidates(
    id, branch_ref, branch_generation, status, purpose, created_ts, updated_ts,
    ready_ts, readiness, missing_companions, atomicity_status,
    atomicity_summary, atomicity_checked_ts, provider, model, planner_protocol,
    config_revision_id, config_profile, preset_id, preset_version,
    soft_publication_deadline, verification_status, verification_output,
    verification_ts, published_commit_oid
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    status=excluded.status,
    purpose=excluded.purpose,
    updated_ts=excluded.updated_ts,
    ready_ts=excluded.ready_ts,
    readiness=excluded.readiness,
    missing_companions=excluded.missing_companions,
    atomicity_status=excluded.atomicity_status,
    atomicity_summary=excluded.atomicity_summary,
    atomicity_checked_ts=excluded.atomicity_checked_ts,
    provider=excluded.provider,
    model=excluded.model,
    planner_protocol=excluded.planner_protocol,
    config_revision_id=excluded.config_revision_id,
    config_profile=excluded.config_profile,
    preset_id=excluded.preset_id,
    preset_version=excluded.preset_version,
    soft_publication_deadline=excluded.soft_publication_deadline,
    verification_status=excluded.verification_status,
    verification_output=excluded.verification_output,
    verification_ts=excluded.verification_ts,
    published_commit_oid=excluded.published_commit_oid
WHERE intent_candidates.branch_ref=excluded.branch_ref
  AND intent_candidates.branch_generation=excluded.branch_generation`,
		candidate.ID, candidate.BranchRef, candidate.BranchGeneration,
		candidate.Status, candidate.Purpose, created, now, candidate.ReadyTS,
		candidate.Readiness, candidate.MissingCompanions,
		candidate.AtomicityStatus, candidate.AtomicitySummary,
		candidate.AtomicityCheckedTS, candidate.Provider, candidate.Model,
		candidate.PlannerProtocol, candidate.ConfigRevisionID,
		candidate.ConfigProfile, candidate.PresetID, candidate.PresetVersion,
		candidate.SoftPublicationDeadline, candidate.VerificationStatus,
		verifyOutput, candidate.VerificationTS, candidate.PublishedCommitOID)
	if err != nil {
		return fmt.Errorf("state: save intent candidate: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		if err != nil {
			return fmt.Errorf("state: candidate rows affected: %w", err)
		}
		return fmt.Errorf("state: candidate %s exact branch pair changed", candidate.ID)
	}
	return nil
}

// MergeIntentCandidates canonically combines persisted nonterminal candidates
// in one transaction. Every active source membership is retained on Target,
// emptied sources become superseded, and repair-relevant lineage is recorded
// without storing raw capture content.
func MergeIntentCandidates(
	ctx context.Context,
	d *DB,
	req IntentCandidateMergeRequest,
) (IntentCandidateMergeResult, error) {
	if d == nil {
		return IntentCandidateMergeResult{},
			errors.New("state: MergeIntentCandidates: nil db")
	}
	if req.Target.Readiness == "" {
		req.Target.Readiness = IntentReadinessWait
	}
	if err := validateIntentCandidate(req.Target); err != nil {
		return IntentCandidateMergeResult{}, err
	}
	if isTerminalIntentCandidateStatus(req.Target.Status) {
		return IntentCandidateMergeResult{},
			errors.New("state: merged target must remain nonterminal")
	}
	if len(req.SourceCandidateIDs) == 0 ||
		len(req.SourceCandidateIDs) >= IntentCandidateMaxOpenPerPair {
		return IntentCandidateMergeResult{}, fmt.Errorf(
			"state: candidate merge requires 1..%d sources",
			IntentCandidateMaxOpenPerPair-1)
	}
	if strings.TrimSpace(req.Reason) == "" {
		return IntentCandidateMergeResult{},
			errors.New("state: candidate lineage reason is required")
	}
	if err := boundedIntentSummary("candidate lineage reason", req.Reason,
		IntentCandidateLineageReasonMaxChars); err != nil {
		return IntentCandidateMergeResult{}, err
	}
	sourceIDs := append([]string(nil), req.SourceCandidateIDs...)
	sort.Strings(sourceIDs)
	unique := sourceIDs[:0]
	for _, sourceID := range sourceIDs {
		if err := boundedIntentLabel("source candidate id", sourceID, 128, true); err != nil {
			return IntentCandidateMergeResult{}, err
		}
		if sourceID == req.Target.ID {
			return IntentCandidateMergeResult{},
				errors.New("state: merge target cannot also be a source")
		}
		if len(unique) == 0 || unique[len(unique)-1] != sourceID {
			unique = append(unique, sourceID)
		}
	}
	sourceIDs = unique

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return IntentCandidateMergeResult{},
			fmt.Errorf("state: begin intent candidate merge: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var targetStatus string
	var targetCreated float64
	if err := tx.QueryRowContext(ctx, `
SELECT status, created_ts FROM intent_candidates
WHERE id=? AND branch_ref=? AND branch_generation=?`,
		req.Target.ID, req.Target.BranchRef, req.Target.BranchGeneration,
	).Scan(&targetStatus, &targetCreated); errors.Is(err, sql.ErrNoRows) {
		return IntentCandidateMergeResult{},
			errors.New("state: merge target does not exist on exact branch pair")
	} else if err != nil {
		return IntentCandidateMergeResult{},
			fmt.Errorf("state: load merge target: %w", err)
	}
	if isTerminalIntentCandidateStatus(targetStatus) {
		return IntentCandidateMergeResult{},
			fmt.Errorf("state: merge target %s is terminal in status %s",
				req.Target.ID, targetStatus)
	}
	if req.Target.CreatedTS <= 0 {
		req.Target.CreatedTS = targetCreated
	}

	type sourceState struct {
		id        string
		status    string
		published sql.NullString
		existing  *IntentCandidateLineage
	}
	sources := make([]sourceState, 0, len(sourceIDs))
	newLineage := 0
	for _, sourceID := range sourceIDs {
		var source sourceState
		source.id = sourceID
		var branchRef string
		var generation int64
		if err := tx.QueryRowContext(ctx, `
SELECT branch_ref, branch_generation, status, published_commit_oid
FROM intent_candidates WHERE id=?`, sourceID).Scan(
			&branchRef, &generation, &source.status, &source.published,
		); errors.Is(err, sql.ErrNoRows) {
			return IntentCandidateMergeResult{},
				fmt.Errorf("state: merge source %s does not exist", sourceID)
		} else if err != nil {
			return IntentCandidateMergeResult{},
				fmt.Errorf("state: load merge source %s: %w", sourceID, err)
		}
		if branchRef != req.Target.BranchRef ||
			generation != req.Target.BranchGeneration {
			return IntentCandidateMergeResult{}, fmt.Errorf(
				"state: merge source %s does not belong to exact branch pair",
				sourceID)
		}
		lineage, ok, err := intentCandidateLineageBySourceTx(
			ctx, tx, branchRef, generation, sourceID)
		if err != nil {
			return IntentCandidateMergeResult{}, err
		}
		if ok {
			if lineage.TargetCandidateID != req.Target.ID ||
				lineage.Reason != req.Reason {
				return IntentCandidateMergeResult{}, fmt.Errorf(
					"state: source candidate %s already has different lineage",
					sourceID)
			}
			source.existing = &lineage
		} else {
			if isTerminalIntentCandidateStatus(source.status) {
				return IntentCandidateMergeResult{}, fmt.Errorf(
					"state: merge source %s is terminal in status %s",
					sourceID, source.status)
			}
			if source.published.Valid {
				if err := boundedIntentLabel("source published commit oid",
					source.published.String, 128, true); err != nil {
					return IntentCandidateMergeResult{}, err
				}
			}
			newLineage++
		}
		sources = append(sources, source)
	}
	var lineageCount int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_candidate_lineage
WHERE branch_ref=? AND branch_generation=?`,
		req.Target.BranchRef, req.Target.BranchGeneration,
	).Scan(&lineageCount); err != nil {
		return IntentCandidateMergeResult{},
			fmt.Errorf("state: count candidate lineage: %w", err)
	}
	if lineageCount+newLineage > IntentCandidateLineageMaxPerPair {
		return IntentCandidateMergeResult{}, fmt.Errorf(
			"state: candidate lineage cap %d exceeded for exact branch pair",
			IntentCandidateLineageMaxPerPair)
	}

	members := make(map[int64]IntentCandidateEvent)
	loadMembers := func(candidateID string) ([]IntentCandidateEvent, error) {
		events, err := loadIntentCandidateEvents(ctx, tx, candidateID, true)
		if err != nil {
			return nil, err
		}
		for _, event := range events {
			members[event.EventSeq] = event
		}
		return events, nil
	}
	if _, err := loadMembers(req.Target.ID); err != nil {
		return IntentCandidateMergeResult{}, err
	}
	for _, source := range sources {
		events, err := loadMembers(source.id)
		if err != nil {
			return IntentCandidateMergeResult{}, err
		}
		if source.existing == nil && len(events) == 0 {
			return IntentCandidateMergeResult{}, fmt.Errorf(
				"state: merge source %s has no active membership", source.id)
		}
	}
	allowedOwners := make(map[string]struct{}, len(sources)+1)
	allowedOwners[req.Target.ID] = struct{}{}
	for _, source := range sources {
		allowedOwners[source.id] = struct{}{}
	}
	for _, event := range req.Target.Events {
		var exists int
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM capture_events
    WHERE seq=? AND branch_ref=? AND branch_generation=?
)`, event.EventSeq, req.Target.BranchRef,
			req.Target.BranchGeneration).Scan(&exists); err != nil {
			return IntentCandidateMergeResult{}, fmt.Errorf(
				"state: validate merged candidate event %d: %w",
				event.EventSeq, err)
		}
		if exists == 0 {
			return IntentCandidateMergeResult{}, fmt.Errorf(
				"state: merged candidate event %d does not belong to exact branch pair",
				event.EventSeq)
		}
		var owner string
		err := tx.QueryRowContext(ctx, `
SELECT candidate_id FROM intent_candidate_events
WHERE event_seq=? AND membership_state='active'`, event.EventSeq).Scan(&owner)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return IntentCandidateMergeResult{}, fmt.Errorf(
				"state: load merged event %d owner: %w", event.EventSeq, err)
		}
		if err == nil {
			if _, ok := allowedOwners[owner]; !ok {
				return IntentCandidateMergeResult{}, fmt.Errorf(
					"state: merged event %d belongs to unlisted candidate %s",
					event.EventSeq, owner)
			}
		}
		members[event.EventSeq] = event
	}
	if len(members) == 0 || len(members) > IntentCandidateMaxCaptures {
		return IntentCandidateMergeResult{}, fmt.Errorf(
			"state: merged candidate requires 1..%d captures",
			IntentCandidateMaxCaptures)
	}
	seqs := make([]int64, 0, len(members))
	for seq := range members {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	req.Target.Events = make([]IntentCandidateEvent, 0, len(seqs))
	for ord, seq := range seqs {
		event := members[seq]
		event.CandidateID = req.Target.ID
		event.Ord = ord
		event.MembershipState = IntentMembershipActive
		req.Target.Events = append(req.Target.Events, event)
	}
	if err := validateIntentCandidate(req.Target); err != nil {
		return IntentCandidateMergeResult{}, err
	}

	lineage := make([]IntentCandidateLineage, 0, len(sources))
	for _, source := range sources {
		if source.existing != nil {
			lineage = append(lineage, *source.existing)
		}
	}

	mergedTS := req.MergedTS
	if mergedTS <= 0 {
		mergedTS = nowSeconds()
	}
	req.Target.UpdatedTS = mergedTS
	if err := upsertIntentCandidateRow(
		ctx, tx, req.Target, mergedTS); err != nil {
		return IntentCandidateMergeResult{}, err
	}
	candidateIDs := append([]string{req.Target.ID}, sourceIDs...)
	for _, candidateID := range candidateIDs {
		if _, err := tx.ExecContext(ctx, `
UPDATE intent_candidate_events
SET membership_state='superseded'
WHERE candidate_id=? AND membership_state='active'`, candidateID); err != nil {
			return IntentCandidateMergeResult{}, fmt.Errorf(
				"state: supersede merged membership for %s: %w",
				candidateID, err)
		}
	}
	for ord, event := range req.Target.Events {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO intent_candidate_events(
    candidate_id, ord, event_seq, event_role, membership_state
) VALUES (?, ?, ?, ?, 'active')
ON CONFLICT(candidate_id, event_seq) DO UPDATE SET
    ord=excluded.ord,
    event_role=excluded.event_role,
    membership_state='active'`,
			req.Target.ID, ord, event.EventSeq, event.EventRole); err != nil {
			return IntentCandidateMergeResult{}, fmt.Errorf(
				"state: save merged candidate event %d: %w",
				event.EventSeq, err)
		}
	}
	for _, source := range sources {
		if source.existing != nil {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE intent_candidates
SET status='superseded', readiness='wait', updated_ts=?
WHERE id=? AND branch_ref=? AND branch_generation=?
  AND status IN ('open','waiting','ready','soft_published','blocked')`,
			mergedTS, source.id, req.Target.BranchRef,
			req.Target.BranchGeneration); err != nil {
			return IntentCandidateMergeResult{}, fmt.Errorf(
				"state: supersede merged source %s: %w", source.id, err)
		}
		record := IntentCandidateLineage{
			BranchRef:                req.Target.BranchRef,
			BranchGeneration:         req.Target.BranchGeneration,
			TargetCandidateID:        req.Target.ID,
			SourceCandidateID:        source.id,
			SourceStatus:             source.status,
			SourcePublishedCommitOID: source.published,
			Reason:                   req.Reason,
			CreatedTS:                mergedTS,
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO intent_candidate_lineage(
    branch_ref, branch_generation, target_candidate_id, source_candidate_id,
    source_status, source_published_commit_oid, reason, created_ts
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			record.BranchRef, record.BranchGeneration,
			record.TargetCandidateID, record.SourceCandidateID,
			record.SourceStatus, record.SourcePublishedCommitOID,
			record.Reason, record.CreatedTS); err != nil {
			return IntentCandidateMergeResult{}, fmt.Errorf(
				"state: record candidate lineage for %s: %w",
				source.id, err)
		}
		lineage = append(lineage, record)
	}
	sort.Slice(lineage, func(i, j int) bool {
		return lineage[i].SourceCandidateID <
			lineage[j].SourceCandidateID
	})
	if err := tx.Commit(); err != nil {
		return IntentCandidateMergeResult{},
			fmt.Errorf("state: commit intent candidate merge: %w", err)
	}
	req.Target.VerificationOutput =
		sanitizedOutputTail(req.Target.VerificationOutput)
	return IntentCandidateMergeResult{
		Candidate: req.Target,
		Lineage:   lineage,
	}, nil
}

type intentCandidateLineageScanner interface {
	Scan(dest ...any) error
}

func scanIntentCandidateLineage(
	row intentCandidateLineageScanner,
) (IntentCandidateLineage, error) {
	var lineage IntentCandidateLineage
	if err := row.Scan(
		&lineage.BranchRef, &lineage.BranchGeneration,
		&lineage.TargetCandidateID, &lineage.SourceCandidateID,
		&lineage.SourceStatus, &lineage.SourcePublishedCommitOID,
		&lineage.Reason, &lineage.CreatedTS,
	); err != nil {
		return IntentCandidateLineage{},
			fmt.Errorf("state: scan intent candidate lineage: %w", err)
	}
	return lineage, nil
}

func intentCandidateLineageBySourceTx(
	ctx context.Context,
	tx *sql.Tx,
	branchRef string,
	generation int64,
	sourceCandidateID string,
) (IntentCandidateLineage, bool, error) {
	lineage, err := scanIntentCandidateLineage(tx.QueryRowContext(ctx, `
SELECT branch_ref, branch_generation, target_candidate_id,
       source_candidate_id, source_status, source_published_commit_oid,
       reason, created_ts
FROM intent_candidate_lineage
WHERE branch_ref=? AND branch_generation=? AND source_candidate_id=?`,
		branchRef, generation, sourceCandidateID))
	if errors.Is(err, sql.ErrNoRows) {
		return IntentCandidateLineage{}, false, nil
	}
	if err != nil {
		return IntentCandidateLineage{}, false, err
	}
	return lineage, true, nil
}

// IntentCandidateLineageBySource returns one direct canonical lineage edge.
func IntentCandidateLineageBySource(
	ctx context.Context,
	d *DB,
	branchRef string,
	generation int64,
	sourceCandidateID string,
) (IntentCandidateLineage, bool, error) {
	if d == nil || branchRef == "" || generation < 0 ||
		strings.TrimSpace(sourceCandidateID) == "" {
		return IntentCandidateLineage{}, false,
			errors.New("state: IntentCandidateLineageBySource: invalid input")
	}
	lineage, err := scanIntentCandidateLineage(
		d.readSQL().QueryRowContext(ctx, `
SELECT branch_ref, branch_generation, target_candidate_id,
       source_candidate_id, source_status, source_published_commit_oid,
       reason, created_ts
FROM intent_candidate_lineage
WHERE branch_ref=? AND branch_generation=? AND source_candidate_id=?`,
			branchRef, generation, sourceCandidateID))
	if errors.Is(err, sql.ErrNoRows) {
		return IntentCandidateLineage{}, false, nil
	}
	if err != nil {
		return IntentCandidateLineage{}, false, err
	}
	return lineage, true, nil
}

// IntentCandidateLineageForTarget returns bounded direct source edges in
// deterministic creation/source order.
func IntentCandidateLineageForTarget(
	ctx context.Context,
	d *DB,
	branchRef string,
	generation int64,
	targetCandidateID string,
	limit int,
) ([]IntentCandidateLineage, error) {
	if d == nil || branchRef == "" || generation < 0 ||
		strings.TrimSpace(targetCandidateID) == "" {
		return nil,
			errors.New("state: IntentCandidateLineageForTarget: invalid input")
	}
	if limit <= 0 || limit > IntentCandidateLineageMaxPerPair {
		limit = IntentCandidateLineageMaxPerPair
	}
	rows, err := d.readSQL().QueryContext(ctx, `
SELECT branch_ref, branch_generation, target_candidate_id,
       source_candidate_id, source_status, source_published_commit_oid,
       reason, created_ts
FROM intent_candidate_lineage
WHERE branch_ref=? AND branch_generation=? AND target_candidate_id=?
ORDER BY created_ts, source_candidate_id
LIMIT ?`, branchRef, generation, targetCandidateID, limit)
	if err != nil {
		return nil, fmt.Errorf("state: query intent candidate lineage: %w", err)
	}
	defer rows.Close()
	var out []IntentCandidateLineage
	for rows.Next() {
		lineage, err := scanIntentCandidateLineage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, lineage)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate intent candidate lineage: %w", err)
	}
	return out, nil
}

// IntentCandidateByID returns a candidate and active memberships in order.
func IntentCandidateByID(ctx context.Context, d *DB, id string) (IntentCandidate, bool, error) {
	if d == nil || strings.TrimSpace(id) == "" {
		return IntentCandidate{}, false, errors.New("state: IntentCandidateByID: invalid input")
	}
	candidate, err := scanIntentCandidate(d.readSQL().QueryRowContext(ctx,
		intentCandidateSelect+` WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return IntentCandidate{}, false, nil
	}
	if err != nil {
		return IntentCandidate{}, false, err
	}
	events, err := loadIntentCandidateEvents(ctx, d.readSQL(), id, true)
	if err != nil {
		return IntentCandidate{}, false, err
	}
	candidate.Events = events
	return candidate, true, nil
}

// IntentEventHeldByCandidate reports whether one capture belongs to an active
// candidate that still needs more evidence or a corrected plan before it can
// publish. Replay uses this narrow probe to keep such a candidate from
// starving later, unoffered companion captures.
func IntentEventHeldByCandidate(
	ctx context.Context,
	d *DB,
	branchRef string,
	branchGeneration int64,
	eventSeq int64,
) (bool, error) {
	if d == nil || branchRef == "" || branchGeneration < 0 || eventSeq <= 0 {
		return false, errors.New("state: IntentEventHeldByCandidate: invalid input")
	}
	var held int
	if err := d.readSQL().QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM intent_candidate_events membership
    JOIN intent_candidates candidate ON candidate.id=membership.candidate_id
    WHERE membership.event_seq=?
      AND membership.membership_state='active'
      AND candidate.branch_ref=?
      AND candidate.branch_generation=?
      AND candidate.status IN ('open','waiting','blocked')
)`, eventSeq, branchRef, branchGeneration).Scan(&held); err != nil {
		return false, fmt.Errorf("state: probe held intent event: %w", err)
	}
	return held != 0, nil
}

// IntentCandidateByPublishedCommit resolves the semantic candidate that owns
// one ACD-published commit on an exact branch pair.
func IntentCandidateByPublishedCommit(
	ctx context.Context,
	d *DB,
	branchRef string,
	generation int64,
	commitOID string,
) (IntentCandidate, bool, error) {
	if d == nil || branchRef == "" || generation < 0 || commitOID == "" {
		return IntentCandidate{}, false,
			errors.New("state: IntentCandidateByPublishedCommit: invalid input")
	}
	candidate, err := scanIntentCandidate(d.readSQL().QueryRowContext(ctx,
		intentCandidateSelect+`
 WHERE branch_ref=? AND branch_generation=? AND published_commit_oid=?
   AND status IN ('soft_published','published')
 ORDER BY updated_ts DESC, id DESC LIMIT 1`,
		branchRef, generation, commitOID))
	if errors.Is(err, sql.ErrNoRows) {
		return IntentCandidate{}, false, nil
	}
	if err != nil {
		return IntentCandidate{}, false, err
	}
	events, err := loadIntentCandidateEvents(ctx, d.readSQL(), candidate.ID, true)
	if err != nil {
		return IntentCandidate{}, false, err
	}
	candidate.Events = events
	return candidate, true, nil
}

// IntentCandidateEventHistory returns both active and superseded memberships
// for diagnostics and planner reconciliation.
func IntentCandidateEventHistory(ctx context.Context, d *DB, id string) ([]IntentCandidateEvent, error) {
	if d == nil || strings.TrimSpace(id) == "" {
		return nil, errors.New("state: IntentCandidateEventHistory: invalid input")
	}
	return loadIntentCandidateEvents(ctx, d.readSQL(), id, false)
}

// IntentCandidatesForPair returns the oldest-updated nonterminal candidates
// with active membership for one exact branch pair. A non-positive limit uses
// the open-candidate cap.
func IntentCandidatesForPair(ctx context.Context, d *DB, branchRef string, generation int64, limit int) ([]IntentCandidate, error) {
	if d == nil || branchRef == "" || generation < 0 {
		return nil, errors.New("state: IntentCandidatesForPair: invalid input")
	}
	if limit <= 0 || limit > IntentCandidateMaxOpenPerPair {
		limit = IntentCandidateMaxOpenPerPair
	}
	rows, err := d.readSQL().QueryContext(ctx, intentCandidateSelect+`
 WHERE branch_ref=? AND branch_generation=?
   AND status IN ('open','waiting','ready','soft_published','blocked')
   AND EXISTS (
       SELECT 1 FROM intent_candidate_events active_membership
       WHERE active_membership.candidate_id=intent_candidates.id
         AND active_membership.membership_state='active'
   )
 ORDER BY updated_ts, id LIMIT ?`, branchRef, generation, limit)
	if err != nil {
		return nil, fmt.Errorf("state: query intent candidates: %w", err)
	}
	defer rows.Close()
	var out []IntentCandidate
	for rows.Next() {
		candidate, err := scanIntentCandidate(rows)
		if err != nil {
			return nil, err
		}
		events, err := loadIntentCandidateEvents(ctx, d.readSQL(), candidate.ID, true)
		if err != nil {
			return nil, err
		}
		candidate.Events = events
		out = append(out, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate intent candidates: %w", err)
	}
	return out, nil
}

// FinalizeExpiredIntentCandidates makes soft publication terminal once its
// repair horizon has elapsed. Terminal candidates remain available by ID for
// diagnostics but no longer consume planner capacity.
func FinalizeExpiredIntentCandidates(
	ctx context.Context,
	d *DB,
	branchRef string,
	generation int64,
	now float64,
) (int64, error) {
	if d == nil || branchRef == "" || generation < 0 {
		return 0, errors.New("state: FinalizeExpiredIntentCandidates: invalid input")
	}
	if now <= 0 {
		now = nowSeconds()
	}
	res, err := d.conn.ExecContext(ctx, `
UPDATE intent_candidates
SET status='published', updated_ts=?
WHERE branch_ref=? AND branch_generation=?
  AND status='soft_published'
  AND soft_publication_deadline IS NOT NULL
  AND soft_publication_deadline<=?`,
		now, branchRef, generation, now)
	if err != nil {
		return 0, fmt.Errorf("state: finalize expired intent candidates: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("state: expired intent candidate count: %w", err)
	}
	return n, nil
}

const intentCandidateSelect = `
SELECT id, branch_ref, branch_generation, status, purpose, created_ts,
       updated_ts, ready_ts, readiness, missing_companions, atomicity_status,
       atomicity_summary, atomicity_checked_ts, provider, model,
       planner_protocol, config_revision_id, config_profile, preset_id,
       preset_version, soft_publication_deadline, verification_status,
       verification_output, verification_ts, published_commit_oid
FROM intent_candidates`

type intentCandidateScanner interface {
	Scan(dest ...any) error
}

func scanIntentCandidate(row intentCandidateScanner) (IntentCandidate, error) {
	var candidate IntentCandidate
	if err := row.Scan(
		&candidate.ID, &candidate.BranchRef, &candidate.BranchGeneration,
		&candidate.Status, &candidate.Purpose, &candidate.CreatedTS,
		&candidate.UpdatedTS, &candidate.ReadyTS, &candidate.Readiness,
		&candidate.MissingCompanions, &candidate.AtomicityStatus,
		&candidate.AtomicitySummary, &candidate.AtomicityCheckedTS,
		&candidate.Provider, &candidate.Model, &candidate.PlannerProtocol,
		&candidate.ConfigRevisionID, &candidate.ConfigProfile,
		&candidate.PresetID, &candidate.PresetVersion,
		&candidate.SoftPublicationDeadline, &candidate.VerificationStatus,
		&candidate.VerificationOutput, &candidate.VerificationTS,
		&candidate.PublishedCommitOID,
	); err != nil {
		return IntentCandidate{}, fmt.Errorf("state: scan intent candidate: %w", err)
	}
	return candidate, nil
}

type intentV2Queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadIntentCandidateEvents(ctx context.Context, q intentV2Queryer, candidateID string, activeOnly bool) ([]IntentCandidateEvent, error) {
	query := `
SELECT candidate_id, ord, event_seq, event_role, membership_state
FROM intent_candidate_events WHERE candidate_id=?`
	if activeOnly {
		query += ` AND membership_state='active'`
	}
	query += ` ORDER BY ord, event_seq`
	rows, err := q.QueryContext(ctx, query, candidateID)
	if err != nil {
		return nil, fmt.Errorf("state: query candidate events: %w", err)
	}
	defer rows.Close()
	var events []IntentCandidateEvent
	for rows.Next() {
		var event IntentCandidateEvent
		if err := rows.Scan(&event.CandidateID, &event.Ord, &event.EventSeq,
			&event.EventRole, &event.MembershipState); err != nil {
			return nil, fmt.Errorf("state: scan candidate event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate candidate events: %w", err)
	}
	return events, nil
}

// ReplaceIntentCaptureDependencies replaces all dependency edges for an exact
// branch pair in one transaction. Invalid or over-cap input leaves the prior
// graph unchanged.
func ReplaceIntentCaptureDependencies(ctx context.Context, d *DB, branchRef string, generation int64, dependencies []IntentCaptureDependency) error {
	if d == nil || branchRef == "" || generation < 0 {
		return errors.New("state: ReplaceIntentCaptureDependencies: invalid input")
	}
	if len(dependencies) > IntentDependencyMaxPerPair {
		return fmt.Errorf("state: intent dependency cap %d exceeded for exact branch pair",
			IntentDependencyMaxPerPair)
	}
	for i := range dependencies {
		dependencies[i].BranchRef = branchRef
		dependencies[i].BranchGeneration = generation
		if err := validateIntentDependency(dependencies[i]); err != nil {
			return fmt.Errorf("state: dependency %d: %w", i, err)
		}
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("state: begin dependency replacement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
DELETE FROM intent_capture_dependencies
WHERE branch_ref=? AND branch_generation=?`, branchRef, generation); err != nil {
		return fmt.Errorf("state: clear intent dependencies: %w", err)
	}
	for _, dependency := range dependencies {
		for _, seq := range []int64{dependency.PrerequisiteSeq, dependency.DependentSeq} {
			var exists int
			if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM capture_events
    WHERE seq=? AND branch_ref=? AND branch_generation=?
)`, seq, branchRef, generation).Scan(&exists); err != nil {
				return fmt.Errorf("state: validate dependency event %d: %w", seq, err)
			}
			if exists == 0 {
				return fmt.Errorf("state: dependency event %d does not belong to exact branch pair", seq)
			}
		}
		created := dependency.CreatedTS
		if created <= 0 {
			created = nowSeconds()
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO intent_capture_dependencies(
    branch_ref, branch_generation, prerequisite_seq, dependent_seq,
    strength, kind, evidence, created_ts
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			branchRef, generation, dependency.PrerequisiteSeq,
			dependency.DependentSeq, dependency.Strength, dependency.Kind,
			dependency.Evidence, created); err != nil {
			return fmt.Errorf("state: insert intent dependency: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("state: commit dependency replacement: %w", err)
	}
	return nil
}

func IntentCaptureDependenciesForPair(ctx context.Context, d *DB, branchRef string, generation int64) ([]IntentCaptureDependency, error) {
	if d == nil || branchRef == "" || generation < 0 {
		return nil, errors.New("state: IntentCaptureDependenciesForPair: invalid input")
	}
	rows, err := d.readSQL().QueryContext(ctx, `
SELECT id, branch_ref, branch_generation, prerequisite_seq, dependent_seq,
       strength, kind, evidence, created_ts
FROM intent_capture_dependencies
WHERE branch_ref=? AND branch_generation=?
ORDER BY prerequisite_seq, dependent_seq, strength, kind`,
		branchRef, generation)
	if err != nil {
		return nil, fmt.Errorf("state: query intent dependencies: %w", err)
	}
	defer rows.Close()
	var out []IntentCaptureDependency
	for rows.Next() {
		var dependency IntentCaptureDependency
		if err := rows.Scan(
			&dependency.ID, &dependency.BranchRef,
			&dependency.BranchGeneration, &dependency.PrerequisiteSeq,
			&dependency.DependentSeq, &dependency.Strength, &dependency.Kind,
			&dependency.Evidence, &dependency.CreatedTS,
		); err != nil {
			return nil, fmt.Errorf("state: scan intent dependency: %w", err)
		}
		out = append(out, dependency)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate intent dependencies: %w", err)
	}
	return out, nil
}

// AppendIntentActivityBoundary creates the next repo-wide epoch when Epoch is
// zero. The increment and insert share the writer transaction.
func AppendIntentActivityBoundary(ctx context.Context, d *DB, boundary IntentActivityBoundary) (IntentActivityBoundary, error) {
	if d == nil {
		return IntentActivityBoundary{}, errors.New("state: AppendIntentActivityBoundary: nil db")
	}
	if err := validateIntentBoundary(boundary); err != nil {
		return IntentActivityBoundary{}, err
	}
	const attempts = 8
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		current := boundary
		result, err := appendIntentActivityBoundaryOnce(ctx, d, current)
		if err == nil || !isSQLiteLocked(err) {
			return result, err
		}
		lastErr = err
		timer := time.NewTimer(time.Duration(attempt+1) * 25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return IntentActivityBoundary{}, ctx.Err()
		case <-timer.C:
		}
	}
	return IntentActivityBoundary{}, lastErr
}

func appendIntentActivityBoundaryOnce(
	ctx context.Context,
	d *DB,
	boundary IntentActivityBoundary,
) (IntentActivityBoundary, error) {
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return IntentActivityBoundary{}, fmt.Errorf("state: begin activity boundary: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if boundary.Epoch == 0 {
		if err := tx.QueryRowContext(ctx,
			`SELECT COALESCE(MAX(epoch), 0) + 1 FROM intent_activity_boundaries`,
		).Scan(&boundary.Epoch); err != nil {
			return IntentActivityBoundary{}, fmt.Errorf("state: allocate boundary epoch: %w", err)
		}
	}
	if boundary.CreatedTS <= 0 {
		boundary.CreatedTS = nowSeconds()
	}
	res, err := tx.ExecContext(ctx, `
INSERT INTO intent_activity_boundaries(
    epoch, kind, source, branch_ref, branch_generation, created_ts, consumed_ts
) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		boundary.Epoch, boundary.Kind, boundary.Source, boundary.BranchRef,
		boundary.BranchGeneration, boundary.CreatedTS, boundary.ConsumedTS)
	if err != nil {
		return IntentActivityBoundary{}, fmt.Errorf("state: insert activity boundary: %w", err)
	}
	boundary.ID, err = res.LastInsertId()
	if err != nil {
		return IntentActivityBoundary{}, fmt.Errorf("state: activity boundary id: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return IntentActivityBoundary{}, fmt.Errorf("state: commit activity boundary: %w", err)
	}
	return boundary, nil
}

func PendingIntentActivityBoundaries(ctx context.Context, d *DB, afterEpoch int64, limit int) ([]IntentActivityBoundary, error) {
	if d == nil || afterEpoch < 0 {
		return nil, errors.New("state: PendingIntentActivityBoundaries: invalid input")
	}
	if limit <= 0 || limit > IntentDependencyMaxPerPair {
		limit = IntentDependencyMaxPerPair
	}
	rows, err := d.readSQL().QueryContext(ctx, `
SELECT id, epoch, kind, source, branch_ref, branch_generation, created_ts,
       consumed_ts
FROM intent_activity_boundaries
WHERE consumed_ts IS NULL AND epoch>?
ORDER BY epoch LIMIT ?`, afterEpoch, limit)
	if err != nil {
		return nil, fmt.Errorf("state: query activity boundaries: %w", err)
	}
	defer rows.Close()
	var out []IntentActivityBoundary
	for rows.Next() {
		var boundary IntentActivityBoundary
		if err := rows.Scan(
			&boundary.ID, &boundary.Epoch, &boundary.Kind, &boundary.Source,
			&boundary.BranchRef, &boundary.BranchGeneration,
			&boundary.CreatedTS, &boundary.ConsumedTS,
		); err != nil {
			return nil, fmt.Errorf("state: scan activity boundary: %w", err)
		}
		out = append(out, boundary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate activity boundaries: %w", err)
	}
	return out, nil
}

func ConsumeIntentActivityBoundaries(ctx context.Context, d *DB, throughEpoch int64, consumedTS float64) (int64, error) {
	if d == nil || throughEpoch <= 0 {
		return 0, errors.New("state: ConsumeIntentActivityBoundaries: invalid input")
	}
	if consumedTS <= 0 {
		consumedTS = nowSeconds()
	}
	res, err := d.conn.ExecContext(ctx, `
UPDATE intent_activity_boundaries
SET consumed_ts=?
WHERE consumed_ts IS NULL AND epoch<=?`, consumedTS, throughEpoch)
	if err != nil {
		return 0, fmt.Errorf("state: consume activity boundaries: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("state: consumed boundary count: %w", err)
	}
	return n, nil
}

// SnapshotIntentRepairMembers reads the exact active membership that a new
// repair intends to settle. SaveIntentRepair revalidates the snapshot in its
// prepared transaction, so a concurrent membership or capture-state change
// makes preparation fail closed.
func SnapshotIntentRepairMembers(
	ctx context.Context,
	d *DB,
	repairID, branchRef string,
	branchGeneration int64,
	candidateIDs []string,
) ([]IntentRepairMember, error) {
	if d == nil || strings.TrimSpace(repairID) == "" || branchRef == "" ||
		branchGeneration < 0 || len(candidateIDs) == 0 ||
		len(candidateIDs) > IntentRepairMaxCommits {
		return nil, errors.New("state: SnapshotIntentRepairMembers: invalid input")
	}
	seenCandidates := make(map[string]struct{}, len(candidateIDs))
	seenEvents := make(map[int64]struct{})
	members := make([]IntentRepairMember, 0)
	for _, candidateID := range candidateIDs {
		if err := boundedIntentLabel("intent repair candidate id", candidateID,
			128, true); err != nil {
			return nil, err
		}
		if _, duplicate := seenCandidates[candidateID]; duplicate {
			continue
		}
		seenCandidates[candidateID] = struct{}{}

		var status string
		if err := d.readSQL().QueryRowContext(ctx, `
SELECT status FROM intent_candidates
WHERE id=? AND branch_ref=? AND branch_generation=?`,
			candidateID, branchRef, branchGeneration).Scan(&status); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf(
					"state: intent repair candidate %s is outside the exact branch pair",
					candidateID)
			}
			return nil, fmt.Errorf("state: load intent repair candidate %s: %w",
				candidateID, err)
		}
		if status != IntentCandidateReady &&
			status != IntentCandidateSoftPublished &&
			status != IntentCandidatePublished {
			return nil, fmt.Errorf("state: intent repair candidate %s is %s",
				candidateID, status)
		}

		rows, err := d.readSQL().QueryContext(ctx, `
SELECT event.seq, event.state, event.commit_oid
FROM intent_candidate_events membership
JOIN capture_events event ON event.seq=membership.event_seq
WHERE membership.candidate_id=?
  AND membership.membership_state='active'
  AND event.branch_ref=? AND event.branch_generation=?
ORDER BY membership.ord, event.seq`, candidateID, branchRef, branchGeneration)
		if err != nil {
			return nil, fmt.Errorf("state: snapshot intent repair candidate %s: %w",
				candidateID, err)
		}
		candidateMembers := 0
		for rows.Next() {
			var eventSeq int64
			var priorState string
			var commitOID sql.NullString
			if err := rows.Scan(&eventSeq, &priorState, &commitOID); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("state: scan intent repair member: %w", err)
			}
			if priorState != EventStatePending && priorState != EventStatePublished {
				_ = rows.Close()
				return nil, fmt.Errorf(
					"state: intent repair event %d is %s", eventSeq, priorState)
			}
			if (priorState == EventStatePending && commitOID.Valid) ||
				(priorState == EventStatePublished &&
					(!commitOID.Valid || commitOID.String == "")) {
				_ = rows.Close()
				return nil, fmt.Errorf(
					"state: intent repair event %d has inconsistent commit identity",
					eventSeq)
			}
			if _, duplicate := seenEvents[eventSeq]; duplicate {
				_ = rows.Close()
				return nil, fmt.Errorf(
					"state: intent repair event %d has multiple active owners",
					eventSeq)
			}
			seenEvents[eventSeq] = struct{}{}
			members = append(members, IntentRepairMember{
				RepairID: repairID, Ord: len(members), CandidateID: candidateID,
				EventSeq: eventSeq, PriorState: priorState,
			})
			candidateMembers++
			if len(members) > IntentRepairMaxMembers {
				_ = rows.Close()
				return nil, fmt.Errorf("state: intent repair member cap %d exceeded",
					IntentRepairMaxMembers)
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("state: iterate intent repair candidate %s: %w",
				candidateID, err)
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("state: close intent repair members: %w", err)
		}
		if candidateMembers == 0 {
			return nil, fmt.Errorf(
				"state: intent repair candidate %s has no active membership",
				candidateID)
		}
	}
	return members, nil
}

// SaveIntentRepair atomically stores a prepared row, its old-to-new mapping,
// and its exact membership. Repairs that may move Git require frozen members.
// Memberless rows are reserved for skipped or failed plans that never move
// Git. Only migration can create legacy membership.
func SaveIntentRepair(ctx context.Context, d *DB, repair IntentRepair) error {
	if d == nil {
		return errors.New("state: SaveIntentRepair: nil db")
	}
	if repair.Status == "" {
		repair.Status = IntentRepairPrepared
	}
	if repair.Status != IntentRepairPrepared {
		return fmt.Errorf("state: new intent repair must be %s", IntentRepairPrepared)
	}
	if repair.MembershipMode == "" {
		if len(repair.Members) > 0 {
			repair.MembershipMode = IntentRepairMembershipFrozen
		} else {
			repair.MembershipMode = IntentRepairMembershipNone
		}
	}
	if repair.MembershipMode == IntentRepairMembershipLegacy {
		return errors.New(
			"state: legacy intent repair membership is migration-only")
	}
	if err := validateIntentRepair(repair); err != nil {
		return err
	}
	now := repair.CreatedTS
	if now <= 0 {
		now = nowSeconds()
	}
	if repair.UpdatedTS <= 0 {
		repair.UpdatedTS = now
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("state: begin intent repair save: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO intent_repairs(
    id, branch_ref, branch_generation, status, expected_head, plan_digest, backup_ref,
    old_head, new_head, created_ts, updated_ts, git_applied_ts, completed_ts,
    error
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		repair.ID, repair.BranchRef, repair.BranchGeneration, repair.Status,
		repair.ExpectedHead, repair.PlanDigest, repair.BackupRef, repair.OldHead, repair.NewHead,
		now, repair.UpdatedTS, repair.GitAppliedTS, repair.CompletedTS,
		repair.Error); err != nil {
		return fmt.Errorf("state: insert intent repair: %w", err)
	}
	if err := replaceIntentRepairCommits(ctx, tx, repair.ID, repair.Commits); err != nil {
		return err
	}
	if err := insertIntentRepairMembers(ctx, tx, repair); err != nil {
		return err
	}
	if len(repair.Members) > 0 {
		if err := validateStoredIntentRepairMembers(ctx, tx, repair); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO intent_repair_member_seals(
    repair_id, membership_mode, member_count
) VALUES (?, ?, ?)`, repair.ID, repair.MembershipMode,
		len(repair.Members)); err != nil {
		return fmt.Errorf("state: seal intent repair membership: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("state: commit intent repair save: %w", err)
	}
	return nil
}

// TransitionIntentRepair advances a repair with compare-and-swap semantics.
// A false result means another actor changed the repair first.
func TransitionIntentRepair(ctx context.Context, d *DB, id string, transition IntentRepairTransition) (bool, error) {
	if d == nil || strings.TrimSpace(id) == "" {
		return false, errors.New("state: TransitionIntentRepair: invalid input")
	}
	if !validIntentRepairTransition(transition.ExpectedStatus, transition.Status) {
		return false, fmt.Errorf("state: invalid intent repair transition %s -> %s",
			transition.ExpectedStatus, transition.Status)
	}
	if err := boundedIntentSummary("intent repair error", transition.Error,
		IntentCandidateSummaryMaxChars); err != nil {
		return false, err
	}
	if len(transition.Commits) > IntentRepairMaxCommits {
		return false, fmt.Errorf("state: intent repair commit cap %d exceeded",
			IntentRepairMaxCommits)
	}
	if transition.Status == IntentRepairGitApplied && transition.Commits == nil {
		return false, errors.New(
			"state: Git-applied intent repair requires exact commit mappings")
	}
	ts := transition.TransitionTS
	if ts <= 0 {
		ts = nowSeconds()
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("state: begin intent repair transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var appliedTS, completedTS sql.NullFloat64
	if transition.Status == IntentRepairGitApplied {
		appliedTS = sql.NullFloat64{Float64: ts, Valid: true}
	}
	if transition.Status == IntentRepairCompleted ||
		transition.Status == IntentRepairSkipped ||
		transition.Status == IntentRepairFailed {
		completedTS = sql.NullFloat64{Float64: ts, Valid: true}
	}
	res, err := tx.ExecContext(ctx, `
UPDATE intent_repairs
SET status=?, backup_ref=COALESCE(?, backup_ref),
    old_head=COALESCE(?, old_head), new_head=COALESCE(?, new_head),
    updated_ts=?, git_applied_ts=COALESCE(?, git_applied_ts),
    completed_ts=COALESCE(?, completed_ts), error=?
WHERE id=? AND status=?`,
		transition.Status, transition.BackupRef, transition.OldHead,
		transition.NewHead, ts, appliedTS, completedTS, transition.Error,
		id, transition.ExpectedStatus)
	if err != nil {
		return false, fmt.Errorf("state: transition intent repair: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("state: intent repair transition count: %w", err)
	}
	if n == 0 {
		return false, nil
	}
	if transition.Commits != nil {
		if err := replaceIntentRepairCommits(ctx, tx, id, transition.Commits); err != nil {
			return false, err
		}
	}
	if transition.Status == IntentRepairGitApplied {
		if err := validateTransitionedIntentRepairMembers(ctx, tx, id); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("state: commit intent repair transition: %w", err)
	}
	return true, nil
}

func IntentRepairByID(ctx context.Context, d *DB, id string) (IntentRepair, bool, error) {
	if d == nil || strings.TrimSpace(id) == "" {
		return IntentRepair{}, false, errors.New("state: IntentRepairByID: invalid input")
	}
	repair, err := scanIntentRepair(d.readSQL().QueryRowContext(ctx,
		intentRepairSelect+` WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return IntentRepair{}, false, nil
	}
	if err != nil {
		return IntentRepair{}, false, err
	}
	commits, err := loadIntentRepairCommits(ctx, d.readSQL(), id)
	if err != nil {
		return IntentRepair{}, false, err
	}
	repair.Commits = commits
	members, err := loadIntentRepairMembers(ctx, d.readSQL(), id)
	if err != nil {
		return IntentRepair{}, false, err
	}
	repair.Members = members
	mode, sealedCount, err := loadIntentRepairMembershipSeal(
		ctx, d.readSQL(), id)
	if err != nil {
		return IntentRepair{}, false, err
	}
	if sealedCount != len(members) {
		return IntentRepair{}, false, fmt.Errorf(
			"state: intent repair membership seal=%d members=%d",
			sealedCount, len(members))
	}
	repair.MembershipMode = mode
	return repair, true, nil
}

func RecoverableIntentRepairs(ctx context.Context, d *DB, limit int) ([]IntentRepair, error) {
	if d == nil {
		return nil, errors.New("state: RecoverableIntentRepairs: nil db")
	}
	if limit <= 0 || limit > 50 {
		limit = 50
	}
	rows, err := d.readSQL().QueryContext(ctx, intentRepairSelect+`
 WHERE status IN ('prepared','git_applied')
 ORDER BY created_ts, id LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("state: query recoverable intent repairs: %w", err)
	}
	defer rows.Close()
	var out []IntentRepair
	for rows.Next() {
		repair, err := scanIntentRepair(rows)
		if err != nil {
			return nil, err
		}
		commits, err := loadIntentRepairCommits(ctx, d.readSQL(), repair.ID)
		if err != nil {
			return nil, err
		}
		repair.Commits = commits
		members, err := loadIntentRepairMembers(ctx, d.readSQL(), repair.ID)
		if err != nil {
			return nil, err
		}
		repair.Members = members
		mode, sealedCount, err := loadIntentRepairMembershipSeal(
			ctx, d.readSQL(), repair.ID)
		if err != nil {
			return nil, err
		}
		if sealedCount != len(members) {
			return nil, fmt.Errorf(
				"state: intent repair membership seal=%d members=%d",
				sealedCount, len(members))
		}
		repair.MembershipMode = mode
		out = append(out, repair)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate recoverable intent repairs: %w", err)
	}
	return out, nil
}

const intentRepairSelect = `
SELECT id, branch_ref, branch_generation, status, expected_head, plan_digest, backup_ref,
       old_head, new_head, created_ts, updated_ts, git_applied_ts, completed_ts,
       error
FROM intent_repairs`

func scanIntentRepair(row intentCandidateScanner) (IntentRepair, error) {
	var repair IntentRepair
	if err := row.Scan(
		&repair.ID, &repair.BranchRef, &repair.BranchGeneration,
		&repair.Status, &repair.ExpectedHead, &repair.PlanDigest, &repair.BackupRef,
		&repair.OldHead, &repair.NewHead, &repair.CreatedTS,
		&repair.UpdatedTS, &repair.GitAppliedTS, &repair.CompletedTS,
		&repair.Error,
	); err != nil {
		return IntentRepair{}, fmt.Errorf("state: scan intent repair: %w", err)
	}
	return repair, nil
}

func replaceIntentRepairCommits(ctx context.Context, tx *sql.Tx, repairID string, commits []IntentRepairCommit) error {
	if len(commits) == 0 || len(commits) > IntentRepairMaxCommits {
		return fmt.Errorf("state: intent repair requires 1..%d commits", IntentRepairMaxCommits)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM intent_repair_commits WHERE repair_id=?`, repairID); err != nil {
		return fmt.Errorf("state: clear intent repair commits: %w", err)
	}
	seen := make(map[string]struct{}, len(commits))
	for ord, commit := range commits {
		if strings.TrimSpace(commit.OldOID) == "" {
			return errors.New("state: intent repair commit has empty old oid")
		}
		if _, exists := seen[commit.OldOID]; exists {
			return fmt.Errorf("state: duplicate intent repair old oid %s", commit.OldOID)
		}
		seen[commit.OldOID] = struct{}{}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO intent_repair_commits(
    repair_id, ord, candidate_id, old_oid, new_oid
) VALUES (?, ?, ?, ?, ?)`,
			repairID, ord, commit.CandidateID, commit.OldOID,
			commit.NewOID); err != nil {
			return fmt.Errorf("state: insert intent repair commit %d: %w", ord, err)
		}
	}
	return nil
}

func insertIntentRepairMembers(
	ctx context.Context,
	tx *sql.Tx,
	repair IntentRepair,
) error {
	for _, member := range repair.Members {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO intent_repair_members(
    repair_id, ord, candidate_id, event_seq, prior_state
) VALUES (?, ?, ?, ?, ?)`,
			member.RepairID, member.Ord, member.CandidateID,
			member.EventSeq, member.PriorState); err != nil {
			return fmt.Errorf("state: insert intent repair member %d: %w",
				member.Ord, err)
		}
	}
	return nil
}

// validateStoredIntentRepairMembers proves that the immutable rows are the
// complete active membership for every mapped candidate and still describe
// the exact pre-repair capture state. It runs inside the prepared transaction.
func validateStoredIntentRepairMembers(
	ctx context.Context,
	q intentV2Queryer,
	repair IntentRepair,
) error {
	var stored int
	if err := q.QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_repair_members WHERE repair_id=?`, repair.ID).
		Scan(&stored); err != nil {
		return fmt.Errorf("state: count intent repair members: %w", err)
	}
	if stored != len(repair.Members) {
		return fmt.Errorf("state: intent repair member count=%d want=%d",
			stored, len(repair.Members))
	}

	var exact int
	if err := q.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM intent_repair_members owned
JOIN intent_candidates candidate
  ON candidate.id=owned.candidate_id
 AND candidate.branch_ref=? AND candidate.branch_generation=?
 AND candidate.status IN ('ready','soft_published','published')
JOIN intent_candidate_events membership
  ON membership.candidate_id=owned.candidate_id
 AND membership.event_seq=owned.event_seq
 AND membership.membership_state='active'
JOIN capture_events event
  ON event.seq=owned.event_seq
 AND event.branch_ref=? AND event.branch_generation=?
 AND event.state=owned.prior_state
WHERE owned.repair_id=?
  AND (
      (owned.prior_state='pending' AND event.commit_oid IS NULL)
      OR
      (owned.prior_state='published' AND event.commit_oid IS NOT NULL
       AND EXISTS (
           SELECT 1 FROM intent_repair_commits mapped
           WHERE mapped.repair_id=owned.repair_id
             AND mapped.candidate_id=owned.candidate_id
             AND mapped.old_oid=event.commit_oid
       ))
  )`, repair.BranchRef, repair.BranchGeneration,
		repair.BranchRef, repair.BranchGeneration, repair.ID).Scan(&exact); err != nil {
		return fmt.Errorf("state: inspect exact intent repair membership: %w", err)
	}
	if exact != len(repair.Members) {
		return fmt.Errorf(
			"state: intent repair ownership changed: exact members=%d want=%d",
			exact, len(repair.Members))
	}

	var incomplete int
	if err := q.QueryRowContext(ctx, `
WITH expected AS (
    SELECT candidate_id, COUNT(*) AS member_count
    FROM intent_repair_members
    WHERE repair_id=?
    GROUP BY candidate_id
), active AS (
    SELECT membership.candidate_id, COUNT(*) AS member_count
    FROM intent_candidate_events membership
    JOIN expected ON expected.candidate_id=membership.candidate_id
    WHERE membership.membership_state='active'
    GROUP BY membership.candidate_id
)
SELECT COUNT(*)
FROM expected
LEFT JOIN active USING(candidate_id)
WHERE COALESCE(active.member_count, 0)<>expected.member_count`, repair.ID).
		Scan(&incomplete); err != nil {
		return fmt.Errorf("state: inspect complete intent repair membership: %w", err)
	}
	if incomplete != 0 {
		return fmt.Errorf(
			"state: intent repair ownership changed: %d candidates have incomplete membership",
			incomplete)
	}

	var overlapping int
	if err := q.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM intent_repair_members owned
JOIN intent_repair_members other
  ON other.event_seq=owned.event_seq AND other.repair_id<>owned.repair_id
JOIN intent_repairs repair
  ON repair.id=other.repair_id AND repair.status IN ('prepared','git_applied')
WHERE owned.repair_id=?`, repair.ID).Scan(&overlapping); err != nil {
		return fmt.Errorf("state: inspect overlapping intent repairs: %w", err)
	}
	if overlapping != 0 {
		return fmt.Errorf(
			"state: intent repair ownership changed: %d events have overlapping live repairs",
			overlapping)
	}
	return nil
}

func validateTransitionedIntentRepairMembers(
	ctx context.Context,
	tx *sql.Tx,
	repairID string,
) error {
	var repair IntentRepair
	var memberCount, sealedCount int
	if err := tx.QueryRowContext(ctx, `
SELECT repair.branch_ref, repair.branch_generation,
       COUNT(member.event_seq), seal.membership_mode, seal.member_count
FROM intent_repairs repair
JOIN intent_repair_member_seals seal ON seal.repair_id=repair.id
LEFT JOIN intent_repair_members member ON member.repair_id=repair.id
WHERE repair.id=?
GROUP BY repair.id`, repairID).Scan(
		&repair.BranchRef, &repair.BranchGeneration,
		&memberCount, &repair.MembershipMode, &sealedCount); err != nil {
		return fmt.Errorf("state: load transitioned intent repair membership: %w", err)
	}
	if memberCount != sealedCount {
		return fmt.Errorf(
			"state: intent repair membership seal=%d members=%d",
			sealedCount, memberCount)
	}
	switch repair.MembershipMode {
	case IntentRepairMembershipLegacy:
		if memberCount != 0 {
			return errors.New(
				"state: legacy intent repair has frozen membership")
		}
		return nil
	case IntentRepairMembershipFrozen:
		if memberCount == 0 {
			return errors.New("state: frozen intent repair membership is empty")
		}
	case IntentRepairMembershipNone:
		return errors.New(
			"state: memberless intent repair cannot transition to Git-applied")
	default:
		return fmt.Errorf("state: invalid intent repair membership mode %q",
			repair.MembershipMode)
	}
	repair.ID = repairID
	repair.Members = make([]IntentRepairMember, memberCount)
	if err := validateStoredIntentRepairMembers(ctx, tx, repair); err != nil {
		return err
	}
	var unmapped int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM intent_repair_commits mapped
WHERE mapped.repair_id=?
  AND (
      mapped.candidate_id IS NULL
      OR NOT EXISTS (
          SELECT 1 FROM intent_repair_members member
          WHERE member.repair_id=mapped.repair_id
            AND member.candidate_id=mapped.candidate_id
      )
  )`, repairID).Scan(&unmapped); err != nil {
		return fmt.Errorf("state: inspect transitioned intent repair mapping: %w", err)
	}
	if unmapped != 0 {
		return fmt.Errorf(
			"state: intent repair mapping changed: %d commits have no immutable membership",
			unmapped)
	}
	return nil
}

func loadIntentRepairCommits(ctx context.Context, q intentV2Queryer, repairID string) ([]IntentRepairCommit, error) {
	rows, err := q.QueryContext(ctx, `
SELECT repair_id, ord, candidate_id, old_oid, new_oid
FROM intent_repair_commits
WHERE repair_id=? ORDER BY ord`, repairID)
	if err != nil {
		return nil, fmt.Errorf("state: query intent repair commits: %w", err)
	}
	defer rows.Close()
	var out []IntentRepairCommit
	for rows.Next() {
		var commit IntentRepairCommit
		if err := rows.Scan(&commit.RepairID, &commit.Ord,
			&commit.CandidateID, &commit.OldOID, &commit.NewOID); err != nil {
			return nil, fmt.Errorf("state: scan intent repair commit: %w", err)
		}
		out = append(out, commit)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate intent repair commits: %w", err)
	}
	return out, nil
}

func loadIntentRepairMembers(
	ctx context.Context,
	q intentV2Queryer,
	repairID string,
) ([]IntentRepairMember, error) {
	rows, err := q.QueryContext(ctx, `
SELECT repair_id, ord, candidate_id, event_seq, prior_state
FROM intent_repair_members
WHERE repair_id=? ORDER BY ord`, repairID)
	if err != nil {
		return nil, fmt.Errorf("state: query intent repair members: %w", err)
	}
	defer rows.Close()
	var out []IntentRepairMember
	for rows.Next() {
		var member IntentRepairMember
		if err := rows.Scan(&member.RepairID, &member.Ord,
			&member.CandidateID, &member.EventSeq, &member.PriorState); err != nil {
			return nil, fmt.Errorf("state: scan intent repair member: %w", err)
		}
		out = append(out, member)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate intent repair members: %w", err)
	}
	return out, nil
}

func loadIntentRepairMembershipSeal(
	ctx context.Context,
	q intentV2Queryer,
	repairID string,
) (string, int, error) {
	var mode string
	var memberCount int
	if err := q.QueryRowContext(ctx, `
SELECT membership_mode, member_count
FROM intent_repair_member_seals
WHERE repair_id=?`, repairID).Scan(&mode, &memberCount); err != nil {
		return "", 0, fmt.Errorf(
			"state: load intent repair membership seal: %w", err)
	}
	return mode, memberCount, nil
}

// LoadIntentV2StateReadOnly projects v15+ state without running Open or any
// migration. Pre-v15 and missing databases return Available=false. v15/v16
// projections remain available but report candidate lineage as unavailable.
func LoadIntentV2StateReadOnly(ctx context.Context, dbPath string) (IntentV2ReadOnlyProjection, error) {
	var projection IntentV2ReadOnlyProjection
	if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
		return projection, nil
	} else if err != nil {
		return projection, fmt.Errorf("state: stat intent v2 state.db: %w", err)
	}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("mode", "ro")
	conn, err := sql.Open(driverName, "file:"+dbPath+"?"+q.Encode())
	if err != nil {
		return projection, fmt.Errorf("state: open intent v2 state.db read-only: %w", err)
	}
	defer func() { _ = conn.Close() }()
	conn.SetMaxOpenConns(1)
	if err := conn.PingContext(ctx); err != nil {
		return projection, fmt.Errorf("state: ping intent v2 state.db read-only: %w", err)
	}
	if err := conn.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&projection.SchemaVersion); err != nil {
		return projection, fmt.Errorf("state: read intent v2 schema version: %w", err)
	}
	if projection.SchemaVersion < 15 {
		return projection, nil
	}
	var tables int
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sqlite_master
WHERE type='table' AND name IN (
    'intent_candidates','intent_candidate_events',
    'intent_capture_dependencies','intent_activity_boundaries',
    'intent_repairs','intent_repair_commits'
)`).Scan(&tables); err != nil {
		return projection, fmt.Errorf("state: inspect intent v2 tables: %w", err)
	}
	if tables != 6 {
		return projection, nil
	}
	projection.Available = true
	if projection.SchemaVersion >= 17 {
		var lineageTables int
		if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sqlite_master
WHERE type='table' AND name='intent_candidate_lineage'`,
		).Scan(&lineageTables); err != nil {
			return IntentV2ReadOnlyProjection{},
				fmt.Errorf("state: inspect intent candidate lineage: %w", err)
		}
		if lineageTables == 1 {
			projection.CandidateLineageAvailable = true
			if err := conn.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM intent_candidate_lineage`,
			).Scan(&projection.CandidateLineageRecords); err != nil {
				return IntentV2ReadOnlyProjection{},
					fmt.Errorf("state: project intent candidate lineage: %w", err)
			}
		}
	}
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_candidates
WHERE status IN ('open','waiting','ready','soft_published','blocked')
  AND EXISTS (
      SELECT 1 FROM intent_candidate_events active_membership
      WHERE active_membership.candidate_id=intent_candidates.id
        AND active_membership.membership_state='active'
  )`,
	).Scan(&projection.OpenCandidates); err != nil {
		return IntentV2ReadOnlyProjection{}, fmt.Errorf("state: project open intent candidates: %w", err)
	}
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM intent_candidates c
WHERE (c.status='blocked'
       OR c.verification_status IN ('failed','timed_out','needs_attention'))
  AND EXISTS (
      SELECT 1
      FROM intent_candidate_events ce
      JOIN capture_events e
        ON e.seq=ce.event_seq AND e.state='pending'
      WHERE ce.candidate_id=c.id AND ce.membership_state='active'
  )`,
	).Scan(&projection.VerificationAttention); err != nil {
		return IntentV2ReadOnlyProjection{}, fmt.Errorf("state: project intent verification attention: %w", err)
	}
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_repairs
WHERE status IN ('prepared','git_applied')`,
	).Scan(&projection.RecoverableRepairs); err != nil {
		return IntentV2ReadOnlyProjection{}, fmt.Errorf("state: project recoverable intent repairs: %w", err)
	}
	if err := conn.QueryRowContext(ctx, `
SELECT COALESCE(MAX(epoch), 0) FROM intent_activity_boundaries`,
	).Scan(&projection.LastBoundaryEpoch); err != nil {
		return IntentV2ReadOnlyProjection{}, fmt.Errorf("state: project intent boundary epoch: %w", err)
	}
	return projection, nil
}

func validateIntentCandidate(candidate IntentCandidate) error {
	if err := boundedIntentLabel("candidate id", candidate.ID, 128, true); err != nil {
		return err
	}
	if candidate.BranchRef == "" || candidate.BranchGeneration < 0 {
		return errors.New("state: candidate exact branch pair is required")
	}
	if !validIntentCandidateStatus(candidate.Status) {
		return fmt.Errorf("state: invalid intent candidate status %q", candidate.Status)
	}
	if candidate.Readiness != IntentReadinessReady &&
		candidate.Readiness != IntentReadinessWait {
		return fmt.Errorf("state: invalid intent candidate readiness %q", candidate.Readiness)
	}
	if err := boundedIntentSummary("candidate purpose", candidate.Purpose,
		IntentCandidatePurposeMaxChars); err != nil {
		return err
	}
	if err := boundedIntentSummary("candidate missing companions",
		candidate.MissingCompanions, IntentCandidateSummaryMaxChars); err != nil {
		return err
	}
	if err := boundedIntentSummary("candidate atomicity summary",
		candidate.AtomicitySummary, IntentCandidateSummaryMaxChars); err != nil {
		return err
	}
	for name, label := range map[string]sql.NullString{
		"atomicity status":     candidate.AtomicityStatus,
		"provider":             candidate.Provider,
		"model":                candidate.Model,
		"planner protocol":     candidate.PlannerProtocol,
		"config profile":       candidate.ConfigProfile,
		"preset id":            candidate.PresetID,
		"verification status":  candidate.VerificationStatus,
		"published commit oid": candidate.PublishedCommitOID,
	} {
		if label.Valid {
			if err := boundedIntentLabel(name, label.String, 128, false); err != nil {
				return err
			}
		}
	}
	if candidate.PresetVersion.Valid && candidate.PresetVersion.Int64 <= 0 {
		return errors.New("state: candidate preset version must be positive")
	}
	if len(candidate.Events) == 0 ||
		len(candidate.Events) > IntentCandidateMaxCaptures {
		return fmt.Errorf("state: candidate requires 1..%d captures",
			IntentCandidateMaxCaptures)
	}
	seen := make(map[int64]struct{}, len(candidate.Events))
	for _, event := range candidate.Events {
		if event.EventSeq <= 0 {
			return errors.New("state: candidate event seq must be positive")
		}
		if _, exists := seen[event.EventSeq]; exists {
			return fmt.Errorf("state: duplicate candidate event seq %d", event.EventSeq)
		}
		seen[event.EventSeq] = struct{}{}
		if err := boundedIntentLabel("candidate event role", event.EventRole, 128, true); err != nil {
			return err
		}
	}
	return nil
}

func validateIntentDependency(dependency IntentCaptureDependency) error {
	if dependency.BranchRef == "" || dependency.BranchGeneration < 0 ||
		dependency.PrerequisiteSeq <= 0 || dependency.DependentSeq <= 0 ||
		dependency.PrerequisiteSeq == dependency.DependentSeq {
		return errors.New("state: invalid intent dependency provenance")
	}
	if dependency.Strength != IntentDependencyHard &&
		dependency.Strength != IntentDependencySoft {
		return fmt.Errorf("state: invalid intent dependency strength %q", dependency.Strength)
	}
	if err := boundedIntentLabel("dependency kind", dependency.Kind, 128, true); err != nil {
		return err
	}
	if err := boundedIntentSummary("dependency evidence", dependency.Evidence,
		IntentCandidateSummaryMaxChars); err != nil {
		return err
	}
	return nil
}

func validateIntentBoundary(boundary IntentActivityBoundary) error {
	if boundary.Epoch < 0 {
		return errors.New("state: activity boundary epoch must be non-negative")
	}
	if boundary.Kind != IntentBoundarySoft && boundary.Kind != IntentBoundaryHard {
		return fmt.Errorf("state: invalid activity boundary kind %q", boundary.Kind)
	}
	if err := boundedIntentLabel("activity boundary source", boundary.Source, 128, true); err != nil {
		return err
	}
	if boundary.BranchRef.Valid != boundary.BranchGeneration.Valid {
		return errors.New("state: activity boundary branch pair must be both set or both absent")
	}
	if boundary.BranchRef.Valid &&
		(boundary.BranchRef.String == "" || boundary.BranchGeneration.Int64 < 0) {
		return errors.New("state: invalid activity boundary branch pair")
	}
	return nil
}

func validateIntentRepair(repair IntentRepair) error {
	if err := boundedIntentLabel("intent repair id", repair.ID, 128, true); err != nil {
		return err
	}
	if repair.BranchRef == "" || repair.BranchGeneration < 0 ||
		strings.TrimSpace(repair.ExpectedHead) == "" {
		return errors.New("state: intent repair provenance is incomplete")
	}
	if !strings.HasPrefix(repair.PlanDigest, "sha256:") ||
		len(repair.PlanDigest) != len("sha256:")+64 {
		return errors.New("state: intent repair plan digest is invalid")
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(
		repair.PlanDigest, "sha256:")); err != nil {
		return errors.New("state: intent repair plan digest is invalid")
	}
	if err := boundedIntentSummary("intent repair error", repair.Error,
		IntentCandidateSummaryMaxChars); err != nil {
		return err
	}
	if len(repair.Commits) == 0 || len(repair.Commits) > IntentRepairMaxCommits {
		return fmt.Errorf("state: intent repair requires 1..%d commits",
			IntentRepairMaxCommits)
	}
	if err := validateIntentRepairMembers(repair); err != nil {
		return err
	}
	return nil
}

func validateIntentRepairMembers(repair IntentRepair) error {
	switch repair.MembershipMode {
	case IntentRepairMembershipLegacy, IntentRepairMembershipNone:
		if len(repair.Members) != 0 {
			return fmt.Errorf(
				"state: intent repair membership mode %s requires no members",
				repair.MembershipMode)
		}
		return nil
	case IntentRepairMembershipFrozen:
		if len(repair.Members) == 0 {
			return errors.New(
				"state: frozen intent repair membership is empty")
		}
	default:
		return fmt.Errorf("state: invalid intent repair membership mode %q",
			repair.MembershipMode)
	}
	if len(repair.Members) > IntentRepairMaxMembers {
		return fmt.Errorf("state: intent repair member cap %d exceeded",
			IntentRepairMaxMembers)
	}
	repairCandidates := make(map[string]struct{})
	for _, commit := range repair.Commits {
		if !commit.CandidateID.Valid ||
			strings.TrimSpace(commit.CandidateID.String) == "" {
			return errors.New(
				"state: immutable intent repair membership requires candidate mappings")
		}
		repairCandidates[commit.CandidateID.String] = struct{}{}
	}
	memberCandidates := make(map[string]struct{})
	seenEvents := make(map[int64]struct{}, len(repair.Members))
	for ord, member := range repair.Members {
		if member.RepairID != repair.ID || member.Ord != ord ||
			member.EventSeq <= 0 {
			return fmt.Errorf("state: invalid intent repair member %d identity", ord)
		}
		if err := boundedIntentLabel("intent repair member candidate id",
			member.CandidateID, 128, true); err != nil {
			return err
		}
		if member.PriorState != EventStatePending &&
			member.PriorState != EventStatePublished {
			return fmt.Errorf("state: invalid intent repair member %d prior state %q",
				ord, member.PriorState)
		}
		if _, duplicate := seenEvents[member.EventSeq]; duplicate {
			return fmt.Errorf("state: duplicate intent repair event %d",
				member.EventSeq)
		}
		seenEvents[member.EventSeq] = struct{}{}
		if _, mapped := repairCandidates[member.CandidateID]; !mapped {
			return fmt.Errorf("state: intent repair member candidate %s has no commit mapping",
				member.CandidateID)
		}
		memberCandidates[member.CandidateID] = struct{}{}
	}
	for candidateID := range repairCandidates {
		if _, exists := memberCandidates[candidateID]; !exists {
			return fmt.Errorf("state: intent repair candidate %s has no immutable membership",
				candidateID)
		}
	}
	return nil
}

func validIntentCandidateStatus(status string) bool {
	switch status {
	case IntentCandidateOpen, IntentCandidateWaiting, IntentCandidateReady,
		IntentCandidateSoftPublished, IntentCandidatePublished,
		IntentCandidateSuperseded, IntentCandidateBlocked,
		IntentCandidateFailed:
		return true
	default:
		return false
	}
}

func isTerminalIntentCandidateStatus(status string) bool {
	return status == IntentCandidatePublished ||
		status == IntentCandidateSuperseded ||
		status == IntentCandidateFailed
}

func validIntentRepairTransition(from, to string) bool {
	switch from {
	case IntentRepairPrepared:
		return to == IntentRepairGitApplied || to == IntentRepairSkipped ||
			to == IntentRepairFailed
	case IntentRepairGitApplied:
		return to == IntentRepairCompleted || to == IntentRepairFailed
	default:
		return false
	}
}

func boundedIntentLabel(name, value string, max int, required bool) error {
	value = strings.TrimSpace(value)
	if required && value == "" {
		return fmt.Errorf("state: %s is required", name)
	}
	if countRunes(value) > max || strings.IndexFunc(value, func(r rune) bool {
		return r == '\x1b' || !unicode.IsPrint(r)
	}) >= 0 {
		return fmt.Errorf("state: %s contains unsafe or overlong text", name)
	}
	return nil
}

func boundedIntentSummary(name, value string, max int) error {
	if countRunes(value) > max || strings.IndexFunc(value, func(r rune) bool {
		return r == '\x1b' || r == '\u007f' ||
			(!unicode.IsPrint(r) && r != '\n' && r != '\t')
	}) >= 0 {
		return fmt.Errorf("state: %s contains unsafe or overlong text", name)
	}
	return nil
}

func countRunes(value string) int {
	return utf8.RuneCountInString(value)
}

func sanitizedOutputTail(value string) string {
	value = strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\t':
			return r
		case unicode.IsPrint(r) && r != '\u007f' && r != '\x1b':
			return r
		default:
			return ' '
		}
	}, value)
	if len(value) <= IntentVerificationOutputMaxBytes {
		return value
	}
	value = value[len(value)-IntentVerificationOutputMaxBytes:]
	for len(value) > 0 && !utf8.ValidString(value) {
		value = value[1:]
	}
	return value
}
