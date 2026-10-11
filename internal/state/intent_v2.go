package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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
	IntentRepairMaxMappings              = IntentRepairMaxCommits * IntentRepairMaxCommits
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

type intentCandidateScanner interface {
	Scan(dest ...any) error
}

type intentV2Queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
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

// IntentRepairByIDBounded loads immutable commit and member evidence for one
// repair with a shared row budget. evidenceRows excludes the repair header and
// membership seal; callers that traverse several repairs can subtract it from
// one aggregate proof budget. A limit sentinel fails closed before returning a
// partial repair.
func IntentRepairByIDBounded(
	ctx context.Context,
	d *DB,
	id string,
	limit int,
) (repair IntentRepair, ok bool, evidenceRows int, err error) {
	if d == nil || strings.TrimSpace(id) == "" || limit < 0 ||
		limit > CompletedBranchTransitionProofLimit {
		return IntentRepair{}, false, 0, errors.New(
			"state: IntentRepairByIDBounded: invalid input")
	}
	repair, err = scanIntentRepair(d.readSQL().QueryRowContext(ctx,
		intentRepairSelect+` WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return IntentRepair{}, false, 0, nil
	}
	if err != nil {
		return IntentRepair{}, false, 0, err
	}
	commits, overflow, err := loadIntentRepairCommitsBounded(
		ctx, d.readSQL(), id, limit)
	if err != nil {
		return IntentRepair{}, false, 0, err
	}
	if overflow {
		return IntentRepair{}, false, 0, completedBranchTransitionProofError(
			"intent repair %s exceeds remaining evidence budget %d",
			id, limit)
	}
	repair.Commits = commits
	evidenceRows = len(commits)
	members, overflow, err := loadIntentRepairMembersBounded(
		ctx, d.readSQL(), id, limit-evidenceRows)
	if err != nil {
		return IntentRepair{}, false, 0, err
	}
	if overflow {
		return IntentRepair{}, false, 0, completedBranchTransitionProofError(
			"intent repair %s exceeds remaining evidence budget %d",
			id, limit)
	}
	repair.Members = members
	evidenceRows += len(members)
	mode, sealedCount, err := loadIntentRepairMembershipSeal(
		ctx, d.readSQL(), id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return IntentRepair{}, false, 0,
				completedBranchTransitionProofError(
					"intent repair %s lacks its membership seal", id)
		}
		return IntentRepair{}, false, 0, err
	}
	if sealedCount != len(members) {
		return IntentRepair{}, false, 0,
			completedBranchTransitionProofError(
				"intent repair %s membership seal=%d members=%d",
				id, sealedCount, len(members))
	}
	repair.MembershipMode = mode
	return repair, true, evidenceRows, nil
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

func loadIntentRepairCommits(ctx context.Context, q intentV2Queryer, repairID string) ([]IntentRepairCommit, error) {
	table, err := intentRepairMappingsTable(ctx, q)
	if err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, `
SELECT repair_id, ord, candidate_id, old_oid, new_oid
FROM `+table+`
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

func loadIntentRepairCommitsBounded(
	ctx context.Context,
	q intentV2Queryer,
	repairID string,
	limit int,
) ([]IntentRepairCommit, bool, error) {
	table, err := intentRepairMappingsTable(ctx, q)
	if err != nil {
		return nil, false, err
	}
	rows, err := q.QueryContext(ctx, `
SELECT repair_id, ord, candidate_id, old_oid, new_oid
FROM `+table+`
WHERE repair_id=? ORDER BY ord
LIMIT ?`, repairID, limit+1)
	if err != nil {
		return nil, false, fmt.Errorf(
			"state: query bounded intent repair commits: %w", err)
	}
	defer rows.Close()
	var out []IntentRepairCommit
	for rows.Next() {
		var commit IntentRepairCommit
		if err := rows.Scan(&commit.RepairID, &commit.Ord,
			&commit.CandidateID, &commit.OldOID, &commit.NewOID); err != nil {
			return nil, false, fmt.Errorf(
				"state: scan bounded intent repair commit: %w", err)
		}
		out = append(out, commit)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf(
			"state: iterate bounded intent repair commits: %w", err)
	}
	return out, len(out) > limit, nil
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

func loadIntentRepairMembersBounded(
	ctx context.Context,
	q intentV2Queryer,
	repairID string,
	limit int,
) ([]IntentRepairMember, bool, error) {
	rows, err := q.QueryContext(ctx, `
SELECT repair_id, ord, candidate_id, event_seq, prior_state
FROM intent_repair_members
WHERE repair_id=? ORDER BY ord
LIMIT ?`, repairID, limit+1)
	if err != nil {
		return nil, false, fmt.Errorf(
			"state: query bounded intent repair members: %w", err)
	}
	defer rows.Close()
	var out []IntentRepairMember
	for rows.Next() {
		var member IntentRepairMember
		if err := rows.Scan(&member.RepairID, &member.Ord,
			&member.CandidateID, &member.EventSeq, &member.PriorState); err != nil {
			return nil, false, fmt.Errorf(
				"state: scan bounded intent repair member: %w", err)
		}
		out = append(out, member)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf(
			"state: iterate bounded intent repair members: %w", err)
	}
	return out, len(out) > limit, nil
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

func countRunes(value string) int {
	return utf8.RuneCountInString(value)
}

func intentRepairMappingsTable(ctx context.Context, q intentV2Queryer) (string, error) {
	var exists int
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='view' AND name='intent_repair_commit_mappings')`).Scan(&exists); err != nil {
		return "", err
	}
	if exists != 0 {
		return "intent_repair_commit_mappings", nil
	}
	return "intent_repair_commits", nil
}
