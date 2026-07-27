package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	SelfPublicationPrepared   = "prepared"
	SelfPublicationGitApplied = "git_applied"
	SelfPublicationCompleted  = "completed"
	SelfPublicationAbandoned  = "abandoned"

	// SelfPublicationMaxMembers matches the maximum capture window accepted
	// by Intent v2. Publication recovery stays bounded even if a caller passes
	// a malformed or unexpectedly large membership set.
	SelfPublicationMaxMembers = 256

	// SelfPublicationMaxPruneBatch bounds terminal journal maintenance so it
	// cannot monopolize the single SQLite writer or run-loop work budget.
	SelfPublicationMaxPruneBatch = 100
)

const (
	selfPublicationMetaBranchGeneration = "branch.generation"
	selfPublicationMetaBranchHead       = "branch.head"
	selfPublicationMetaBranchToken      = "branch_token"
)

var (
	ErrSelfPublicationIdentityMismatch = errors.New("state: self-publication identity mismatch")
	ErrSelfPublicationPhaseConflict    = errors.New("state: illegal self-publication phase transition")
	ErrSelfPublicationOwnershipChanged = errors.New("state: self-publication ownership changed")
)

// SelfPublicationMember is one exact capture owner in a publication. Intent
// publications also carry the candidate that owned the event at prepare time.
// Event-mode publications leave CandidateID invalid.
type SelfPublicationMember struct {
	Ord         int
	EventSeq    int64
	CandidateID sql.NullString
}

// SelfPublication is one immutable Git publication identity plus its monotonic
// SQLite phase. Members are ordered and included in MembershipDigest.
type SelfPublication struct {
	ID               string
	BranchRef        string
	BranchGeneration int64
	SourceHead       string
	TargetCommitOID  string
	TargetTreeOID    string
	MembershipDigest string
	MemberCount      int
	Phase            string
	CreatedTS        float64
	UpdatedTS        float64
	GitAppliedTS     sql.NullFloat64
	CompletedTS      sql.NullFloat64
	AbandonedTS      sql.NullFloat64
	Error            string
	Members          []SelfPublicationMember
	Completion       SelfPublicationCompletion
}

// SelfPublicationCompletion controls the mutable candidate and user-facing
// fields written by CompleteSelfPublication. CandidateStatus defaults to
// published; soft_published requires a positive SoftPublicationDeadline.
type SelfPublicationCompletion struct {
	PublishedTS             float64
	Message                 sql.NullString
	CandidateStatus         string
	SoftPublicationDeadline sql.NullFloat64
	BranchToken             string
}

// SelfPublicationReadOnlyProjection is safe for status/diagnostic callers.
// Pre-v18 databases return Available=false without creating or migrating
// anything. Recoverable is capped and ordered oldest-first.
type SelfPublicationReadOnlyProjection struct {
	Available     bool
	SchemaVersion int
	Prepared      int
	GitApplied    int
	Completed     int
	Abandoned     int
	Recoverable   []SelfPublication
}

// SelfPublicationMembershipDigest returns the canonical identity digest for
// an ordered membership set. Candidate absence is encoded distinctly from an
// empty candidate ID, and duplicate/non-positive event IDs fail closed.
func SelfPublicationMembershipDigest(members []SelfPublicationMember) (string, error) {
	if len(members) == 0 || len(members) > SelfPublicationMaxMembers {
		return "", fmt.Errorf("state: self-publication requires 1..%d members",
			SelfPublicationMaxMembers)
	}
	h := sha256.New()
	seen := make(map[int64]struct{}, len(members))
	for ord, member := range members {
		if member.EventSeq <= 0 {
			return "", fmt.Errorf("state: self-publication member %d has invalid event_seq", ord)
		}
		if _, exists := seen[member.EventSeq]; exists {
			return "", fmt.Errorf("state: duplicate self-publication event_seq %d", member.EventSeq)
		}
		seen[member.EventSeq] = struct{}{}
		if member.CandidateID.Valid {
			candidateID := strings.TrimSpace(member.CandidateID.String)
			if candidateID == "" || len(candidateID) > 128 {
				return "", fmt.Errorf("state: self-publication member %d has invalid candidate_id", ord)
			}
			fmt.Fprintf(h, "%d\x00%d\x001\x00%s\x00", ord, member.EventSeq, candidateID)
		} else {
			fmt.Fprintf(h, "%d\x00%d\x000\x00", ord, member.EventSeq)
		}
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// PrepareSelfPublication persists immutable publication identity before any
// Git mutation. An exact duplicate is an idempotent no-op (created=false);
// reuse of an ID or target for different identity fails closed.
func PrepareSelfPublication(ctx context.Context, d *DB, publication SelfPublication) (created bool, err error) {
	if d == nil {
		return false, errors.New("state: PrepareSelfPublication: nil db")
	}
	normalized, err := normalizeSelfPublication(publication)
	if err != nil {
		return false, err
	}

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("state: begin self-publication prepare: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	existing, ok, err := selfPublicationByIDQuery(ctx, tx, normalized.ID)
	if err != nil {
		return false, err
	}
	if ok {
		existing.Members, err = loadSelfPublicationMembers(ctx, tx, existing.ID)
		if err != nil {
			return false, err
		}
		if !sameSelfPublicationIdentity(existing, normalized) {
			return false, ErrSelfPublicationIdentityMismatch
		}
		return false, nil
	}

	if err := validateSelfPublicationOwnership(ctx, tx, normalized, true); err != nil {
		return false, err
	}
	ts := normalized.CreatedTS
	if ts <= 0 {
		ts = nowSeconds()
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO self_publications(
    id, branch_ref, branch_generation, source_head, target_commit_oid,
    target_tree_oid, membership_digest, member_count, phase, created_ts,
    updated_ts, error, completion_published_ts,
    completion_candidate_status, completion_soft_deadline
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?)`,
		normalized.ID, normalized.BranchRef, normalized.BranchGeneration,
		normalized.SourceHead, normalized.TargetCommitOID,
		normalized.TargetTreeOID, normalized.MembershipDigest,
		len(normalized.Members), SelfPublicationPrepared, ts, ts,
		normalized.Completion.PublishedTS,
		normalized.Completion.CandidateStatus,
		normalized.Completion.SoftPublicationDeadline); err != nil {
		if strings.Contains(err.Error(), "idx_self_publications_pair_target") ||
			strings.Contains(err.Error(), "self_publications.branch_ref") {
			return false, ErrSelfPublicationIdentityMismatch
		}
		return false, fmt.Errorf("state: insert self-publication: %w", err)
	}
	for ord, member := range normalized.Members {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO self_publication_members(
    publication_id, ord, event_seq, candidate_id
) VALUES (?, ?, ?, ?)`,
			normalized.ID, ord, member.EventSeq, member.CandidateID); err != nil {
			return false, fmt.Errorf("state: insert self-publication member %d: %w", ord, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("state: commit self-publication prepare: %w", err)
	}
	return true, nil
}

// MarkSelfPublicationGitApplied advances prepared -> git_applied using all
// immutable identity fields as a compare-and-set guard.
func MarkSelfPublicationGitApplied(ctx context.Context, d *DB, publication SelfPublication, appliedTS float64) (bool, error) {
	return transitionSelfPublication(ctx, d, publication,
		SelfPublicationPrepared, SelfPublicationGitApplied, appliedTS, "")
}

// AbandonSelfPublication advances prepared -> abandoned. A git_applied
// publication cannot be abandoned because its ref mutation must be proved and
// completed or escalated by recovery. A caller retrying the same deterministic
// target after abandon must allocate a new publication ID; abandoned attempts
// no longer reserve target or event ownership.
func AbandonSelfPublication(ctx context.Context, d *DB, publication SelfPublication, reason string, abandonedTS float64) (bool, error) {
	return transitionSelfPublication(ctx, d, publication,
		SelfPublicationPrepared, SelfPublicationAbandoned, abandonedTS, reason)
}

// CompleteSelfPublication atomically advances git_applied -> completed and
// settles the exact captured events, candidate ownership, publish_state, and
// durable branch head/token metadata. A previously-completed exact identity is
// an idempotent no-op (completed=false).
func CompleteSelfPublication(
	ctx context.Context,
	d *DB,
	publication SelfPublication,
	completion SelfPublicationCompletion,
) (completed bool, err error) {
	if d == nil {
		return false, errors.New("state: CompleteSelfPublication: nil db")
	}
	normalized, err := normalizeSelfPublication(publication)
	if err != nil {
		return false, err
	}
	// Publication semantics are immutable prepare-time identity. Recovery must
	// not extend repair horizons or reinterpret a landed target from current
	// runtime settings after restart.
	completion.PublishedTS = normalized.Completion.PublishedTS
	completion.CandidateStatus = normalized.Completion.CandidateStatus
	completion.SoftPublicationDeadline =
		normalized.Completion.SoftPublicationDeadline
	if completion.CandidateStatus != IntentCandidatePublished &&
		completion.CandidateStatus != IntentCandidateSoftPublished {
		return false, fmt.Errorf("state: invalid self-publication candidate status %q",
			completion.CandidateStatus)
	}
	if completion.CandidateStatus == IntentCandidateSoftPublished &&
		(!completion.SoftPublicationDeadline.Valid ||
			completion.SoftPublicationDeadline.Float64 <= completion.PublishedTS) {
		return false, errors.New("state: soft-published self-publication requires a future deadline")
	}
	if completion.CandidateStatus == IntentCandidatePublished {
		completion.SoftPublicationDeadline = sql.NullFloat64{}
	}
	if completion.BranchToken == "" {
		completion.BranchToken = "rev:" + normalized.TargetCommitOID + " " + normalized.BranchRef
	}
	canonicalBranchToken := "rev:" + normalized.TargetCommitOID + " " + normalized.BranchRef
	if completion.BranchToken != canonicalBranchToken {
		return false, errors.New("state: self-publication branch token does not match identity")
	}

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("state: begin self-publication completion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	current, ok, err := selfPublicationByIDQuery(ctx, tx, normalized.ID)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, ErrSelfPublicationIdentityMismatch
	}
	current.Members, err = loadSelfPublicationMembers(ctx, tx, current.ID)
	if err != nil {
		return false, err
	}
	if !sameSelfPublicationIdentity(current, normalized) {
		return false, ErrSelfPublicationIdentityMismatch
	}
	if current.Phase == SelfPublicationCompleted {
		return false, nil
	}
	if current.Phase != SelfPublicationGitApplied {
		return false, fmt.Errorf("%w: %s -> %s",
			ErrSelfPublicationPhaseConflict, current.Phase, SelfPublicationCompleted)
	}
	if err := validateStoredSelfPublicationOwnership(ctx, tx, current); err != nil {
		return false, err
	}

	res, err := tx.ExecContext(ctx, `
UPDATE capture_events
SET state=?, commit_oid=?, error=NULL, message=?, published_ts=?
WHERE seq IN (
    SELECT event_seq FROM self_publication_members WHERE publication_id=?
)
  AND branch_ref=? AND branch_generation=? AND state=?`,
		EventStatePublished, current.TargetCommitOID, completion.Message,
		completion.PublishedTS, current.ID, current.BranchRef,
		current.BranchGeneration, EventStatePending)
	if err != nil {
		return false, fmt.Errorf("state: settle self-publication events: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("state: count settled self-publication events: %w", err)
	}
	if n != int64(len(current.Members)) {
		return false, fmt.Errorf("%w: settled events=%d want=%d",
			ErrSelfPublicationOwnershipChanged, n, len(current.Members))
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM planner_state
WHERE event_seq IN (
    SELECT event_seq FROM self_publication_members WHERE publication_id=?
)`, current.ID); err != nil {
		return false, fmt.Errorf("state: clear self-publication planner state: %w", err)
	}

	candidates := distinctSelfPublicationCandidates(current.Members)
	if len(candidates) > 0 {
		res, err = tx.ExecContext(ctx, `
UPDATE intent_candidates
SET status=?, published_commit_oid=?, soft_publication_deadline=?,
    updated_ts=?
WHERE id IN (
    SELECT DISTINCT candidate_id
    FROM self_publication_members
    WHERE publication_id=? AND candidate_id IS NOT NULL
)
  AND branch_ref=? AND branch_generation=? AND status='ready'`,
			completion.CandidateStatus, current.TargetCommitOID,
			completion.SoftPublicationDeadline, completion.PublishedTS,
			current.ID, current.BranchRef, current.BranchGeneration)
		if err != nil {
			return false, fmt.Errorf("state: settle self-publication candidates: %w", err)
		}
		n, err = res.RowsAffected()
		if err != nil {
			return false, fmt.Errorf("state: count settled self-publication candidates: %w",
				err)
		}
		if n != int64(len(candidates)) {
			return false, fmt.Errorf("%w: settled candidates=%d want=%d",
				ErrSelfPublicationOwnershipChanged, n, len(candidates))
		}
	}

	lastEventSeq := current.Members[len(current.Members)-1].EventSeq
	if _, err := tx.ExecContext(ctx, `
INSERT INTO publish_state(
    id, event_seq, branch_ref, branch_generation, source_head,
    target_commit_oid, status, error, updated_ts
) VALUES (1, ?, ?, ?, ?, ?, 'published', NULL, ?)
ON CONFLICT(id) DO UPDATE SET
    event_seq=excluded.event_seq,
    branch_ref=excluded.branch_ref,
    branch_generation=excluded.branch_generation,
    source_head=excluded.source_head,
    target_commit_oid=excluded.target_commit_oid,
    status=excluded.status,
    error=NULL,
    updated_ts=excluded.updated_ts`,
		lastEventSeq, current.BranchRef, current.BranchGeneration,
		current.SourceHead, current.TargetCommitOID,
		completion.PublishedTS); err != nil {
		return false, fmt.Errorf("state: settle self-publication publish_state: %w", err)
	}
	for key, value := range map[string]string{
		selfPublicationMetaBranchGeneration: strconv.FormatInt(current.BranchGeneration, 10),
		selfPublicationMetaBranchHead:       current.TargetCommitOID,
		selfPublicationMetaBranchToken:      completion.BranchToken,
	} {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO daemon_meta(key, value, updated_ts) VALUES (?, ?, ?)
ON CONFLICT(key) DO UPDATE SET
    value=excluded.value, updated_ts=excluded.updated_ts`,
			key, value, completion.PublishedTS); err != nil {
			return false, fmt.Errorf("state: settle self-publication meta %s: %w", key, err)
		}
	}
	res, err = tx.ExecContext(ctx, `
UPDATE self_publications
SET phase=?, updated_ts=?, completed_ts=?, error=''
WHERE id=? AND phase=?
  AND branch_ref=? AND branch_generation=? AND source_head=?
  AND target_commit_oid=? AND target_tree_oid=?
  AND membership_digest=? AND member_count=?`,
		SelfPublicationCompleted, completion.PublishedTS,
		completion.PublishedTS, current.ID, SelfPublicationGitApplied,
		current.BranchRef, current.BranchGeneration, current.SourceHead,
		current.TargetCommitOID, current.TargetTreeOID,
		current.MembershipDigest, current.MemberCount)
	if err != nil {
		return false, fmt.Errorf("state: complete self-publication journal: %w", err)
	}
	n, err = res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("state: count completed self-publication: %w", err)
	}
	if n != 1 {
		return false, ErrSelfPublicationIdentityMismatch
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("state: commit self-publication completion: %w", err)
	}
	return true, nil
}

// SelfPublicationByID loads exact identity and ordered membership.
func SelfPublicationByID(ctx context.Context, d *DB, id string) (SelfPublication, bool, error) {
	if d == nil || strings.TrimSpace(id) == "" {
		return SelfPublication{}, false, errors.New("state: SelfPublicationByID: invalid input")
	}
	publication, ok, err := selfPublicationByIDQuery(ctx, d.readSQL(), id)
	if err != nil || !ok {
		return publication, ok, err
	}
	publication.Members, err = loadSelfPublicationMembers(ctx, d.readSQL(), id)
	if err != nil {
		return SelfPublication{}, false, err
	}
	return publication, true, nil
}

// RecoverableSelfPublications returns bounded prepared/git_applied rows in
// creation order. Both phases require restart inspection; only git_applied may
// be completed after exact Git parent/tree/ref proof.
func RecoverableSelfPublications(ctx context.Context, d *DB, limit int) ([]SelfPublication, error) {
	if d == nil {
		return nil, errors.New("state: RecoverableSelfPublications: nil db")
	}
	if limit <= 0 || limit > 50 {
		limit = 50
	}
	return queryRecoverableSelfPublications(ctx, d.readSQL(), limit)
}

// PruneTerminalSelfPublicationsBefore removes at most limit completed or
// abandoned journal rows older than cutoff, including their exact membership.
// Active prepared/git_applied crash evidence is never eligible.
func PruneTerminalSelfPublicationsBefore(
	ctx context.Context,
	d *DB,
	cutoff float64,
	limit int,
) (int64, error) {
	if d == nil {
		return 0, errors.New("state: PruneTerminalSelfPublicationsBefore: nil db")
	}
	if cutoff <= 0 {
		return 0, errors.New("state: self-publication prune cutoff must be positive")
	}
	if limit <= 0 || limit > SelfPublicationMaxPruneBatch {
		limit = SelfPublicationMaxPruneBatch
	}
	res, err := d.conn.ExecContext(ctx, `
DELETE FROM self_publications
WHERE id IN (
    SELECT id
    FROM self_publications
    WHERE phase IN ('completed','abandoned') AND updated_ts < ?
    ORDER BY updated_ts, id
    LIMIT ?
)`, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("state: prune terminal self-publications: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("state: count pruned self-publications: %w", err)
	}
	return n, nil
}

// LoadSelfPublicationStateReadOnly projects v18 journal state without calling
// Open, applying DDL, or changing PRAGMA user_version.
func LoadSelfPublicationStateReadOnly(ctx context.Context, dbPath string) (SelfPublicationReadOnlyProjection, error) {
	var projection SelfPublicationReadOnlyProjection
	if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
		return projection, nil
	} else if err != nil {
		return projection, fmt.Errorf("state: stat self-publication state.db: %w", err)
	}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("mode", "ro")
	conn, err := sql.Open(driverName, "file:"+dbPath+"?"+q.Encode())
	if err != nil {
		return projection, fmt.Errorf("state: open self-publication state.db read-only: %w", err)
	}
	defer func() { _ = conn.Close() }()
	conn.SetMaxOpenConns(1)
	if err := conn.PingContext(ctx); err != nil {
		return projection, fmt.Errorf("state: ping self-publication state.db read-only: %w", err)
	}
	if err := conn.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&projection.SchemaVersion); err != nil {
		return projection, fmt.Errorf("state: read self-publication schema version: %w", err)
	}
	if projection.SchemaVersion < 18 {
		return projection, nil
	}
	var tables int
	if err := conn.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sqlite_master
WHERE type='table' AND name IN (
    'self_publications','self_publication_members'
)`).Scan(&tables); err != nil {
		return projection, fmt.Errorf("state: inspect self-publication tables: %w", err)
	}
	if tables != 2 {
		return projection, nil
	}
	projection.Available = true
	rows, err := conn.QueryContext(ctx, `
SELECT phase, COUNT(*) FROM self_publications GROUP BY phase`)
	if err != nil {
		return SelfPublicationReadOnlyProjection{},
			fmt.Errorf("state: project self-publication phases: %w", err)
	}
	for rows.Next() {
		var phase string
		var count int
		if err := rows.Scan(&phase, &count); err != nil {
			rows.Close()
			return SelfPublicationReadOnlyProjection{},
				fmt.Errorf("state: scan self-publication phase: %w", err)
		}
		switch phase {
		case SelfPublicationPrepared:
			projection.Prepared = count
		case SelfPublicationGitApplied:
			projection.GitApplied = count
		case SelfPublicationCompleted:
			projection.Completed = count
		case SelfPublicationAbandoned:
			projection.Abandoned = count
		}
	}
	if err := rows.Close(); err != nil {
		return SelfPublicationReadOnlyProjection{},
			fmt.Errorf("state: close self-publication phase rows: %w", err)
	}
	if projection.SchemaVersion >= 19 {
		projection.Recoverable, err =
			queryRecoverableSelfPublications(ctx, conn, 50)
	} else {
		projection.Recoverable, err =
			queryRecoverableSelfPublicationsV18(ctx, conn, 50)
	}
	if err != nil {
		return SelfPublicationReadOnlyProjection{}, err
	}
	return projection, nil
}

func transitionSelfPublication(
	ctx context.Context,
	d *DB,
	publication SelfPublication,
	from, to string,
	ts float64,
	reason string,
) (bool, error) {
	if d == nil {
		return false, errors.New("state: transition self-publication: nil db")
	}
	normalized, err := normalizeSelfPublication(publication)
	if err != nil {
		return false, err
	}
	if ts <= 0 {
		ts = nowSeconds()
	}
	if len(reason) > 2048 {
		return false, errors.New("state: self-publication reason exceeds 2048 bytes")
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("state: begin self-publication transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, ok, err := selfPublicationByIDQuery(ctx, tx, normalized.ID)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, ErrSelfPublicationIdentityMismatch
	}
	current.Members, err = loadSelfPublicationMembers(ctx, tx, current.ID)
	if err != nil {
		return false, err
	}
	if !sameSelfPublicationIdentity(current, normalized) {
		return false, ErrSelfPublicationIdentityMismatch
	}
	if current.Phase == to {
		return false, nil
	}
	if current.Phase != from {
		return false, fmt.Errorf("%w: %s -> %s",
			ErrSelfPublicationPhaseConflict, current.Phase, to)
	}
	setColumn := "git_applied_ts"
	if to == SelfPublicationAbandoned {
		setColumn = "abandoned_ts"
	}
	query := fmt.Sprintf(`
UPDATE self_publications
SET phase=?, updated_ts=?, %s=?, error=?
WHERE id=? AND phase=?
  AND branch_ref=? AND branch_generation=? AND source_head=?
  AND target_commit_oid=? AND target_tree_oid=?
  AND membership_digest=? AND member_count=?`, setColumn)
	res, err := tx.ExecContext(ctx, query,
		to, ts, ts, reason, current.ID, from, current.BranchRef,
		current.BranchGeneration, current.SourceHead, current.TargetCommitOID,
		current.TargetTreeOID, current.MembershipDigest, current.MemberCount)
	if err != nil {
		return false, fmt.Errorf("state: advance self-publication to %s: %w", to, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("state: count self-publication transition: %w", err)
	}
	if n != 1 {
		return false, ErrSelfPublicationIdentityMismatch
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("state: commit self-publication transition: %w", err)
	}
	return true, nil
}

func normalizeSelfPublication(publication SelfPublication) (SelfPublication, error) {
	publication.ID = strings.TrimSpace(publication.ID)
	publication.BranchRef = strings.TrimSpace(publication.BranchRef)
	publication.SourceHead = strings.TrimSpace(publication.SourceHead)
	publication.TargetCommitOID = strings.TrimSpace(publication.TargetCommitOID)
	publication.TargetTreeOID = strings.TrimSpace(publication.TargetTreeOID)
	if publication.ID == "" || len(publication.ID) > 128 ||
		publication.BranchRef == "" || len(publication.BranchRef) > 1024 ||
		publication.BranchGeneration < 0 ||
		len(publication.SourceHead) > 128 ||
		publication.TargetCommitOID == "" || len(publication.TargetCommitOID) > 128 ||
		publication.TargetTreeOID == "" || len(publication.TargetTreeOID) > 128 {
		return SelfPublication{}, errors.New("state: self-publication identity is incomplete or overlong")
	}
	for i := range publication.Members {
		publication.Members[i].Ord = i
		if publication.Members[i].CandidateID.Valid {
			publication.Members[i].CandidateID.String =
				strings.TrimSpace(publication.Members[i].CandidateID.String)
		}
	}
	digest, err := SelfPublicationMembershipDigest(publication.Members)
	if err != nil {
		return SelfPublication{}, err
	}
	if publication.MembershipDigest != "" && publication.MembershipDigest != digest {
		return SelfPublication{}, ErrSelfPublicationIdentityMismatch
	}
	publication.MembershipDigest = digest
	publication.MemberCount = len(publication.Members)
	if publication.Completion.CandidateStatus == "" {
		publication.Completion.CandidateStatus = IntentCandidatePublished
	}
	if publication.Completion.CandidateStatus != IntentCandidatePublished &&
		publication.Completion.CandidateStatus != IntentCandidateSoftPublished {
		return SelfPublication{}, fmt.Errorf(
			"state: invalid self-publication completion status %q",
			publication.Completion.CandidateStatus)
	}
	if publication.Completion.CandidateStatus == IntentCandidateSoftPublished {
		if !publication.Completion.SoftPublicationDeadline.Valid ||
			publication.Completion.SoftPublicationDeadline.Float64 <=
				publication.Completion.PublishedTS {
			return SelfPublication{}, errors.New(
				"state: soft self-publication completion requires a future deadline")
		}
	} else {
		publication.Completion.SoftPublicationDeadline = sql.NullFloat64{}
	}
	return publication, nil
}

func validateSelfPublicationOwnership(
	ctx context.Context,
	q selfPublicationQueryer,
	publication SelfPublication,
	requirePending bool,
) error {
	for _, member := range publication.Members {
		var state string
		if err := q.QueryRowContext(ctx, `
SELECT state FROM capture_events
WHERE seq=? AND branch_ref=? AND branch_generation=?`,
			member.EventSeq, publication.BranchRef,
			publication.BranchGeneration).Scan(&state); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: event %d is outside the exact branch pair",
					ErrSelfPublicationOwnershipChanged, member.EventSeq)
			}
			return fmt.Errorf("state: inspect self-publication event %d: %w",
				member.EventSeq, err)
		}
		if requirePending && state != EventStatePending {
			return fmt.Errorf("%w: event %d is %s",
				ErrSelfPublicationOwnershipChanged, member.EventSeq, state)
		}
		var overlapping int
		if err := q.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM self_publication_members membership
JOIN self_publications publication
  ON publication.id=membership.publication_id
WHERE membership.event_seq=?
  AND membership.publication_id<>?
  AND publication.phase IN ('prepared','git_applied','completed')`,
			member.EventSeq, publication.ID).Scan(&overlapping); err != nil {
			return fmt.Errorf("state: inspect overlapping self-publication ownership: %w", err)
		}
		if overlapping != 0 {
			return fmt.Errorf("%w: event %d already has a live publication",
				ErrSelfPublicationOwnershipChanged, member.EventSeq)
		}
		if !member.CandidateID.Valid {
			var activeCandidateCount int
			if err := q.QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_candidate_events
WHERE event_seq=? AND membership_state='active'`,
				member.EventSeq).Scan(&activeCandidateCount); err != nil {
				return fmt.Errorf("state: inspect event-only self-publication ownership: %w", err)
			}
			if activeCandidateCount != 0 {
				return fmt.Errorf("%w: event %d has active candidate ownership",
					ErrSelfPublicationOwnershipChanged, member.EventSeq)
			}
			continue
		}
		var candidateStatus string
		if err := q.QueryRowContext(ctx, `
SELECT status FROM intent_candidates
WHERE id=? AND branch_ref=? AND branch_generation=?`,
			member.CandidateID.String, publication.BranchRef,
			publication.BranchGeneration).Scan(&candidateStatus); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%w: candidate %s is outside the exact branch pair",
					ErrSelfPublicationOwnershipChanged, member.CandidateID.String)
			}
			return fmt.Errorf("state: inspect self-publication candidate %s: %w",
				member.CandidateID.String, err)
		}
		if candidateStatus != IntentCandidateReady {
			return fmt.Errorf("%w: candidate %s is %s",
				ErrSelfPublicationOwnershipChanged, member.CandidateID.String,
				candidateStatus)
		}
		var active int
		if err := q.QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_candidate_events
WHERE candidate_id=? AND event_seq=? AND membership_state='active'`,
			member.CandidateID.String, member.EventSeq).Scan(&active); err != nil {
			return fmt.Errorf("state: inspect self-publication membership: %w", err)
		}
		if active != 1 {
			return fmt.Errorf("%w: candidate %s does not own event %d",
				ErrSelfPublicationOwnershipChanged, member.CandidateID.String,
				member.EventSeq)
		}
	}
	for _, candidateID := range distinctSelfPublicationCandidates(publication.Members) {
		var activeCount int
		if err := q.QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_candidate_events
WHERE candidate_id=? AND membership_state='active'`,
			candidateID).Scan(&activeCount); err != nil {
			return fmt.Errorf("state: count candidate %s membership: %w", candidateID, err)
		}
		expected := 0
		for _, member := range publication.Members {
			if member.CandidateID.Valid && member.CandidateID.String == candidateID {
				expected++
			}
		}
		if activeCount != expected {
			return fmt.Errorf("%w: candidate %s membership count=%d want=%d",
				ErrSelfPublicationOwnershipChanged, candidateID, activeCount, expected)
		}
	}
	return nil
}

// validateStoredSelfPublicationOwnership validates a persisted journal with
// set-based queries. Completion holds SQLite's single writer transaction, so
// avoiding per-member reads keeps the atomic settlement below the daemon
// heartbeat interval even at the 256-event/128-candidate caps.
func validateStoredSelfPublicationOwnership(
	ctx context.Context,
	q selfPublicationQueryer,
	publication SelfPublication,
) error {
	var exactPending int
	if err := q.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM self_publication_members membership
JOIN capture_events event
  ON event.seq=membership.event_seq
 AND event.branch_ref=?
 AND event.branch_generation=?
 AND event.state=?
WHERE membership.publication_id=?`,
		publication.BranchRef, publication.BranchGeneration,
		EventStatePending, publication.ID).Scan(&exactPending); err != nil {
		return fmt.Errorf("state: inspect stored self-publication events: %w", err)
	}
	if exactPending != len(publication.Members) {
		return fmt.Errorf("%w: exact pending events=%d want=%d",
			ErrSelfPublicationOwnershipChanged, exactPending,
			len(publication.Members))
	}

	var overlapping int
	if err := q.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM self_publication_members owned
JOIN self_publication_members other
  ON other.event_seq=owned.event_seq
 AND other.publication_id<>owned.publication_id
JOIN self_publications publication
  ON publication.id=other.publication_id
 AND publication.phase IN ('prepared','git_applied','completed')
WHERE owned.publication_id=?`, publication.ID).Scan(&overlapping); err != nil {
		return fmt.Errorf("state: inspect overlapping self-publication ownership: %w", err)
	}
	if overlapping != 0 {
		return fmt.Errorf("%w: %d events have overlapping live publications",
			ErrSelfPublicationOwnershipChanged, overlapping)
	}

	var eventOnlyActive int
	if err := q.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM self_publication_members owned
JOIN intent_candidate_events membership
  ON membership.event_seq=owned.event_seq
 AND membership.membership_state='active'
WHERE owned.publication_id=? AND owned.candidate_id IS NULL`,
		publication.ID).Scan(&eventOnlyActive); err != nil {
		return fmt.Errorf("state: inspect event-only self-publication ownership: %w", err)
	}
	if eventOnlyActive != 0 {
		return fmt.Errorf("%w: %d event-only members have active candidate ownership",
			ErrSelfPublicationOwnershipChanged, eventOnlyActive)
	}

	expectedCandidateMembers := 0
	for _, member := range publication.Members {
		if member.CandidateID.Valid {
			expectedCandidateMembers++
		}
	}
	if expectedCandidateMembers == 0 {
		return nil
	}
	var readyCandidateMembers int
	if err := q.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM self_publication_members owned
JOIN intent_candidates candidate
  ON candidate.id=owned.candidate_id
 AND candidate.branch_ref=?
 AND candidate.branch_generation=?
 AND candidate.status='ready'
WHERE owned.publication_id=? AND owned.candidate_id IS NOT NULL`,
		publication.BranchRef, publication.BranchGeneration,
		publication.ID).Scan(&readyCandidateMembers); err != nil {
		return fmt.Errorf("state: inspect self-publication candidates: %w", err)
	}
	if readyCandidateMembers != expectedCandidateMembers {
		return fmt.Errorf("%w: ready candidate members=%d want=%d",
			ErrSelfPublicationOwnershipChanged, readyCandidateMembers,
			expectedCandidateMembers)
	}

	var exactActiveMemberships int
	if err := q.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM self_publication_members owned
JOIN intent_candidate_events membership
  ON membership.candidate_id=owned.candidate_id
 AND membership.event_seq=owned.event_seq
 AND membership.membership_state='active'
WHERE owned.publication_id=? AND owned.candidate_id IS NOT NULL`,
		publication.ID).Scan(&exactActiveMemberships); err != nil {
		return fmt.Errorf("state: inspect self-publication memberships: %w", err)
	}
	if exactActiveMemberships != expectedCandidateMembers {
		return fmt.Errorf("%w: exact active memberships=%d want=%d",
			ErrSelfPublicationOwnershipChanged, exactActiveMemberships,
			expectedCandidateMembers)
	}

	var incompleteCandidates int
	if err := q.QueryRowContext(ctx, `
WITH expected AS (
    SELECT candidate_id, COUNT(*) AS member_count
    FROM self_publication_members
    WHERE publication_id=? AND candidate_id IS NOT NULL
    GROUP BY candidate_id
),
active AS (
    SELECT candidate_id, COUNT(*) AS member_count
    FROM intent_candidate_events
    WHERE membership_state='active'
    GROUP BY candidate_id
)
SELECT COUNT(*)
FROM expected
LEFT JOIN active USING(candidate_id)
WHERE COALESCE(active.member_count, 0)<>expected.member_count`,
		publication.ID).Scan(&incompleteCandidates); err != nil {
		return fmt.Errorf("state: inspect complete candidate ownership: %w", err)
	}
	if incompleteCandidates != 0 {
		return fmt.Errorf("%w: %d candidates have incomplete membership",
			ErrSelfPublicationOwnershipChanged, incompleteCandidates)
	}
	return nil
}

func sameSelfPublicationIdentity(a, b SelfPublication) bool {
	if a.ID != b.ID || a.BranchRef != b.BranchRef ||
		a.BranchGeneration != b.BranchGeneration ||
		a.SourceHead != b.SourceHead ||
		a.TargetCommitOID != b.TargetCommitOID ||
		a.TargetTreeOID != b.TargetTreeOID ||
		a.MembershipDigest != b.MembershipDigest ||
		a.MemberCount != b.MemberCount ||
		a.Completion.PublishedTS != b.Completion.PublishedTS ||
		a.Completion.CandidateStatus != b.Completion.CandidateStatus ||
		a.Completion.SoftPublicationDeadline !=
			b.Completion.SoftPublicationDeadline ||
		len(a.Members) != len(b.Members) {
		return false
	}
	for i := range a.Members {
		if a.Members[i].Ord != b.Members[i].Ord ||
			a.Members[i].EventSeq != b.Members[i].EventSeq ||
			a.Members[i].CandidateID != b.Members[i].CandidateID {
			return false
		}
	}
	return true
}

func distinctSelfPublicationCandidates(members []SelfPublicationMember) []string {
	seen := make(map[string]struct{})
	for _, member := range members {
		if member.CandidateID.Valid {
			seen[member.CandidateID.String] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for candidateID := range seen {
		out = append(out, candidateID)
	}
	sort.Strings(out)
	return out
}

type selfPublicationQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

const selfPublicationSelect = `
SELECT id, branch_ref, branch_generation, source_head, target_commit_oid,
       target_tree_oid, membership_digest, member_count, phase, created_ts,
       updated_ts, git_applied_ts, completed_ts, abandoned_ts, error,
       completion_published_ts, completion_candidate_status,
       completion_soft_deadline
FROM self_publications`

func selfPublicationByIDQuery(
	ctx context.Context,
	q selfPublicationQueryer,
	id string,
) (SelfPublication, bool, error) {
	var publication SelfPublication
	err := q.QueryRowContext(ctx, selfPublicationSelect+` WHERE id=?`, id).Scan(
		&publication.ID, &publication.BranchRef,
		&publication.BranchGeneration, &publication.SourceHead,
		&publication.TargetCommitOID, &publication.TargetTreeOID,
		&publication.MembershipDigest, &publication.MemberCount,
		&publication.Phase, &publication.CreatedTS, &publication.UpdatedTS,
		&publication.GitAppliedTS, &publication.CompletedTS,
		&publication.AbandonedTS, &publication.Error,
		&publication.Completion.PublishedTS,
		&publication.Completion.CandidateStatus,
		&publication.Completion.SoftPublicationDeadline)
	if errors.Is(err, sql.ErrNoRows) {
		return SelfPublication{}, false, nil
	}
	if err != nil {
		return SelfPublication{}, false,
			fmt.Errorf("state: load self-publication: %w", err)
	}
	return publication, true, nil
}

func loadSelfPublicationMembers(
	ctx context.Context,
	q selfPublicationQueryer,
	id string,
) ([]SelfPublicationMember, error) {
	rows, err := q.QueryContext(ctx, `
SELECT ord, event_seq, candidate_id
FROM self_publication_members
WHERE publication_id=? ORDER BY ord`, id)
	if err != nil {
		return nil, fmt.Errorf("state: query self-publication members: %w", err)
	}
	defer rows.Close()
	var members []SelfPublicationMember
	for rows.Next() {
		var member SelfPublicationMember
		if err := rows.Scan(&member.Ord, &member.EventSeq,
			&member.CandidateID); err != nil {
			return nil, fmt.Errorf("state: scan self-publication member: %w", err)
		}
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate self-publication members: %w", err)
	}
	return members, nil
}

func queryRecoverableSelfPublications(
	ctx context.Context,
	q selfPublicationQueryer,
	limit int,
) ([]SelfPublication, error) {
	rows, err := q.QueryContext(ctx, selfPublicationSelect+`
 WHERE phase IN ('prepared','git_applied')
 ORDER BY created_ts, id LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("state: query recoverable self-publications: %w", err)
	}
	defer rows.Close()
	var publications []SelfPublication
	for rows.Next() {
		var publication SelfPublication
		if err := rows.Scan(
			&publication.ID, &publication.BranchRef,
			&publication.BranchGeneration, &publication.SourceHead,
			&publication.TargetCommitOID, &publication.TargetTreeOID,
			&publication.MembershipDigest, &publication.MemberCount,
			&publication.Phase, &publication.CreatedTS, &publication.UpdatedTS,
			&publication.GitAppliedTS, &publication.CompletedTS,
			&publication.AbandonedTS, &publication.Error,
			&publication.Completion.PublishedTS,
			&publication.Completion.CandidateStatus,
			&publication.Completion.SoftPublicationDeadline,
		); err != nil {
			return nil, fmt.Errorf("state: scan recoverable self-publication: %w", err)
		}
		publications = append(publications, publication)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate recoverable self-publications: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("state: close recoverable self-publications: %w", err)
	}
	for i := range publications {
		publications[i].Members, err =
			loadSelfPublicationMembers(ctx, q, publications[i].ID)
		if err != nil {
			return nil, err
		}
	}
	return publications, nil
}

func queryRecoverableSelfPublicationsV18(
	ctx context.Context,
	q selfPublicationQueryer,
	limit int,
) ([]SelfPublication, error) {
	rows, err := q.QueryContext(ctx, `
SELECT id, branch_ref, branch_generation, source_head, target_commit_oid,
       target_tree_oid, membership_digest, member_count, phase, created_ts,
       updated_ts, git_applied_ts, completed_ts, abandoned_ts, error
FROM self_publications
WHERE phase IN ('prepared','git_applied')
ORDER BY created_ts, id LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf(
			"state: query v18 recoverable self-publications: %w", err)
	}
	defer rows.Close()
	var publications []SelfPublication
	for rows.Next() {
		var publication SelfPublication
		if err := rows.Scan(
			&publication.ID, &publication.BranchRef,
			&publication.BranchGeneration, &publication.SourceHead,
			&publication.TargetCommitOID, &publication.TargetTreeOID,
			&publication.MembershipDigest, &publication.MemberCount,
			&publication.Phase, &publication.CreatedTS,
			&publication.UpdatedTS, &publication.GitAppliedTS,
			&publication.CompletedTS, &publication.AbandonedTS,
			&publication.Error); err != nil {
			return nil, fmt.Errorf(
				"state: scan v18 recoverable self-publication: %w", err)
		}
		publication.Completion = SelfPublicationCompletion{
			PublishedTS:     publication.CreatedTS,
			CandidateStatus: IntentCandidatePublished,
		}
		publications = append(publications, publication)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"state: iterate v18 recoverable self-publications: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf(
			"state: close v18 recoverable self-publications: %w", err)
	}
	for i := range publications {
		publications[i].Members, err =
			loadSelfPublicationMembers(ctx, q, publications[i].ID)
		if err != nil {
			return nil, err
		}
	}
	return publications, nil
}
