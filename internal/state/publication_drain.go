package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode"
)

const (
	PublicationDrainCheckpointing = "checkpointing"
	PublicationDrainSemantic      = "semantic"
	PublicationDrainNormalizing   = "normalizing"
	PublicationDrainEventFallback = "event_fallback"
	PublicationDrainCompleted     = "completed"
	PublicationDrainNeedsAction   = "needs_action"
)

var (
	ErrPublicationDrainNotFound = errors.New("state: publication drain not found")
	ErrPublicationDrainIdentity = errors.New("state: publication drain identity mismatch")
	ErrPublicationDrainPhase    = errors.New("state: publication drain phase conflict")
	ErrPublicationDrainProgress = errors.New("state: publication drain progress regression")
)

// PublicationDrain is one resumable publication operation over the immutable
// event membership of CheckpointID. EventSeqs is loaded from checkpoint_events;
// it is never duplicated into mutable drain state.
type PublicationDrain struct {
	ID                      string
	CheckpointID            string
	WorktreeID              string
	BranchRef               string
	BranchGeneration        int64
	Phase                   string
	TargetEventCount        int64
	PublishedEventCount     int64
	SemanticRebuildAttempts int64
	EventFallbackCount      int64
	CommitCount             int64
	FallbackMode            string
	LastError               string
	StagedConsent           bool
	StagedConsumed          bool
	CreatedTS               float64
	UpdatedTS               float64
	LastProgressTS          float64
	CompletedTS             sql.NullFloat64
	EventSeqs               []int64
}

type PublicationDrainUpdate struct {
	ExpectedPhase           string
	Phase                   string
	PublishedEventCount     int64
	SemanticRebuildAttempts int64
	EventFallbackCount      int64
	CommitCount             int64
	FallbackMode            string
	LastError               string
	StagedConsent           bool
	StagedConsumed          bool
	UpdatedTS               float64
	LastProgressTS          float64
	CompletedTS             sql.NullFloat64
}

type PublicationDrainReadOnlyProjection struct {
	Available     bool
	SchemaVersion int
	Active        []PublicationDrain
	Latest        *PublicationDrain
}

// PreparePublicationDrain inserts one immutable drain identity. Repeating the
// exact request is idempotent; a changed identity for either ID fails closed.
func PreparePublicationDrain(
	ctx context.Context,
	db *DB,
	drain PublicationDrain,
) (bool, error) {
	if db == nil {
		return false, errors.New("state: PreparePublicationDrain: nil db")
	}
	if err := validatePublicationDrainIdentity(drain); err != nil {
		return false, err
	}
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("state: begin publication drain prepare: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	checkpoint, ok, err := checkpointByIDQuery(ctx, tx, drain.CheckpointID, true)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, ErrCheckpointNotFound
	}
	if checkpoint.Phase != CheckpointCompleted {
		return false, ErrPublicationDrainIdentity
	}
	if len(drain.EventSeqs) == 0 {
		drain.EventSeqs = append([]int64(nil), checkpoint.EventSeqs...)
	}
	if checkpoint.WorktreeID != drain.WorktreeID ||
		checkpoint.ObservedRef != drain.BranchRef ||
		int64(len(drain.EventSeqs)) != drain.TargetEventCount {
		return false, ErrPublicationDrainIdentity
	}
	seenEvents := make(map[int64]struct{}, len(drain.EventSeqs))
	for _, seq := range drain.EventSeqs {
		if seq <= 0 {
			return false, ErrPublicationDrainIdentity
		}
		if _, duplicate := seenEvents[seq]; duplicate {
			return false, ErrPublicationDrainIdentity
		}
		seenEvents[seq] = struct{}{}
	}
	if err := validatePublicationDrainEvents(
		ctx, tx, drain, seenEvents); err != nil {
		return false, err
	}
	if existing, ok, err := publicationDrainByIDQuery(ctx, tx, drain.ID, true); err != nil {
		return false, err
	} else if ok {
		if !samePublicationDrainIdentity(existing, drain) {
			return false, ErrPublicationDrainIdentity
		}
		return false, nil
	}
	if _, ok, err := publicationDrainByCheckpointQuery(
		ctx, tx, drain.CheckpointID, false); err != nil {
		return false, err
	} else if ok {
		return false, ErrPublicationDrainIdentity
	}
	drain.LastError = sanitizePublicationDrainError(drain.LastError)
	if drain.UpdatedTS == 0 {
		drain.UpdatedTS = drain.CreatedTS
	}
	if drain.LastProgressTS == 0 {
		drain.LastProgressTS = drain.CreatedTS
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO publication_drains(
 id,checkpoint_id,worktree_id,branch_ref,branch_generation,phase,
 target_event_count,published_event_count,semantic_rebuild_attempts,
 event_fallback_count,commit_count,fallback_mode,last_error,staged_consent,staged_consumed,
 created_ts,updated_ts,last_progress_ts,completed_ts
) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		drain.ID, drain.CheckpointID, drain.WorktreeID, drain.BranchRef,
		drain.BranchGeneration, drain.Phase, drain.TargetEventCount,
		drain.PublishedEventCount, drain.SemanticRebuildAttempts,
		drain.EventFallbackCount, drain.CommitCount, drain.FallbackMode,
		drain.LastError, drain.StagedConsent, drain.StagedConsumed,
		drain.CreatedTS, drain.UpdatedTS,
		drain.LastProgressTS, drain.CompletedTS); err != nil {
		return false, fmt.Errorf("state: insert publication drain: %w", err)
	}
	for ord, seq := range drain.EventSeqs {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO publication_drain_events(drain_id,ord,event_seq) VALUES(?,?,?)`,
			drain.ID, ord, seq); err != nil {
			return false, fmt.Errorf(
				"state: insert publication drain event %d: %w", ord, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("state: commit publication drain prepare: %w", err)
	}
	return true, nil
}

func validatePublicationDrainEvents(
	ctx context.Context,
	tx *sql.Tx,
	drain PublicationDrain,
	seenEvents map[int64]struct{},
) error {
	checkpointRows, err := tx.QueryContext(ctx, `
SELECT event_seq FROM checkpoint_events WHERE checkpoint_id=?`, drain.CheckpointID)
	if err != nil {
		return fmt.Errorf("state: load publication drain checkpoint membership: %w", err)
	}
	for checkpointRows.Next() {
		var seq int64
		if err := checkpointRows.Scan(&seq); err != nil {
			checkpointRows.Close()
			return err
		}
		if _, ok := seenEvents[seq]; !ok {
			checkpointRows.Close()
			return ErrPublicationDrainIdentity
		}
	}
	if err := checkpointRows.Close(); err != nil {
		return err
	}

	const batchSize = 500
	validated := 0
	for start := 0; start < len(drain.EventSeqs); start += batchSize {
		end := min(start+batchSize, len(drain.EventSeqs))
		placeholders := make([]string, 0, end-start)
		args := make([]any, 0, end-start+2)
		for _, seq := range drain.EventSeqs[start:end] {
			placeholders = append(placeholders, "?")
			args = append(args, seq)
		}
		args = append(args, drain.BranchRef, drain.BranchGeneration)
		var count int
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM capture_events
WHERE seq IN (`+strings.Join(placeholders, ",")+`)
  AND branch_ref=? AND branch_generation=?`, args...).Scan(&count); err != nil {
			return fmt.Errorf("state: validate publication drain membership: %w", err)
		}
		validated += count
	}
	if validated != len(drain.EventSeqs) {
		return ErrPublicationDrainIdentity
	}
	return nil
}

// AdvancePublicationDrain performs a compare-and-swap phase transition and
// rejects counter or timestamp regression. Repeating the resulting state is
// idempotent even after a client reconnects and no longer knows the old phase.
func AdvancePublicationDrain(
	ctx context.Context,
	db *DB,
	id string,
	update PublicationDrainUpdate,
) (PublicationDrain, error) {
	if db == nil {
		return PublicationDrain{}, errors.New("state: AdvancePublicationDrain: nil db")
	}
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return PublicationDrain{}, fmt.Errorf("state: begin publication drain update: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, ok, err := publicationDrainByIDQuery(ctx, tx, id, true)
	if err != nil {
		return PublicationDrain{}, err
	}
	if !ok {
		return PublicationDrain{}, ErrPublicationDrainNotFound
	}
	update.LastError = sanitizePublicationDrainError(update.LastError)
	if samePublicationDrainUpdate(current, update) {
		return current, nil
	}
	if update.ExpectedPhase != "" && current.Phase != update.ExpectedPhase {
		return PublicationDrain{}, ErrPublicationDrainPhase
	}
	if !validPublicationDrainTransition(current.Phase, update.Phase) {
		return PublicationDrain{}, ErrPublicationDrainPhase
	}
	if update.PublishedEventCount < current.PublishedEventCount ||
		update.PublishedEventCount > current.TargetEventCount ||
		update.SemanticRebuildAttempts < current.SemanticRebuildAttempts ||
		update.EventFallbackCount < current.EventFallbackCount ||
		update.CommitCount < current.CommitCount ||
		update.UpdatedTS < current.UpdatedTS ||
		update.LastProgressTS < current.LastProgressTS ||
		(current.StagedConsent && !update.StagedConsent) ||
		(current.StagedConsumed && !update.StagedConsumed) {
		return PublicationDrain{}, ErrPublicationDrainProgress
	}
	if update.Phase == PublicationDrainCompleted &&
		update.PublishedEventCount != current.TargetEventCount {
		return PublicationDrain{}, ErrPublicationDrainProgress
	}
	result, err := tx.ExecContext(ctx, `
UPDATE publication_drains SET
 phase=?,published_event_count=?,semantic_rebuild_attempts=?,
 event_fallback_count=?,commit_count=?,fallback_mode=?,last_error=?,
 staged_consent=?,staged_consumed=?,updated_ts=?,last_progress_ts=?,completed_ts=?
WHERE id=? AND phase=?`, update.Phase, update.PublishedEventCount,
		update.SemanticRebuildAttempts, update.EventFallbackCount,
		update.CommitCount, update.FallbackMode, update.LastError,
		update.StagedConsent, update.StagedConsumed, update.UpdatedTS,
		update.LastProgressTS,
		update.CompletedTS, id, current.Phase)
	if err != nil {
		return PublicationDrain{}, fmt.Errorf("state: update publication drain: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return PublicationDrain{}, ErrPublicationDrainPhase
	}
	if err := tx.Commit(); err != nil {
		return PublicationDrain{}, fmt.Errorf("state: commit publication drain update: %w", err)
	}
	return PublicationDrainByID(ctx, db, id)
}

// ReopenPublicationDrainCheckpointing retries one previously blocked
// checkpoint transition after the caller has independently proved that the
// recorded safety error is no longer applicable. The exact error match keeps
// unrelated needs-action drains terminal.
func ReopenPublicationDrainCheckpointing(
	ctx context.Context,
	db *DB,
	id string,
	expectedError string,
	updatedTS float64,
) (PublicationDrain, error) {
	if db == nil {
		return PublicationDrain{}, errors.New(
			"state: ReopenPublicationDrainCheckpointing: nil db")
	}
	result, err := db.conn.ExecContext(ctx, `
UPDATE publication_drains
SET phase='checkpointing',last_error='',updated_ts=?
WHERE id=? AND phase='needs_action' AND last_error=? AND updated_ts<=?`,
		updatedTS, id, sanitizePublicationDrainError(expectedError), updatedTS)
	if err != nil {
		return PublicationDrain{}, fmt.Errorf(
			"state: reopen publication drain checkpointing: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return PublicationDrain{}, fmt.Errorf(
			"state: count reopened publication drain: %w", err)
	}
	if changed != 1 {
		return PublicationDrain{}, ErrPublicationDrainPhase
	}
	return PublicationDrainByID(ctx, db, id)
}

func PublicationDrainByID(
	ctx context.Context,
	db *DB,
	id string,
) (PublicationDrain, error) {
	if db == nil {
		return PublicationDrain{}, errors.New("state: PublicationDrainByID: nil db")
	}
	drain, ok, err := publicationDrainByIDQuery(ctx, db.conn, id, true)
	if err != nil {
		return PublicationDrain{}, err
	}
	if !ok {
		return PublicationDrain{}, ErrPublicationDrainNotFound
	}
	return drain, nil
}

func PublicationDrainByCheckpoint(
	ctx context.Context,
	db *DB,
	checkpointID string,
) (PublicationDrain, bool, error) {
	if db == nil {
		return PublicationDrain{}, false,
			errors.New("state: PublicationDrainByCheckpoint: nil db")
	}
	return publicationDrainByCheckpointQuery(ctx, db.conn, checkpointID, true)
}

func ActivePublicationDrains(
	ctx context.Context,
	db *DB,
) ([]PublicationDrain, error) {
	if db == nil {
		return nil, errors.New("state: ActivePublicationDrains: nil db")
	}
	return publicationDrainsQuery(ctx, db.conn, true)
}

// ReadPublicationDrainProjection opens an existing DB read-only. Pre-v21
// repositories report Available=false and are never migrated as a side effect.
func ReadPublicationDrainProjection(
	ctx context.Context,
	dbPath string,
) (PublicationDrainReadOnlyProjection, error) {
	projection := PublicationDrainReadOnlyProjection{}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("mode", "ro")
	conn, err := sql.Open(driverName, "file:"+dbPath+"?"+q.Encode())
	if err != nil {
		return projection, fmt.Errorf("state: open publication drain projection: %w", err)
	}
	defer conn.Close()
	if err := conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(
		&projection.SchemaVersion); err != nil {
		return projection, fmt.Errorf("state: read publication drain schema: %w", err)
	}
	if projection.SchemaVersion < 21 {
		return projection, nil
	}
	projection.Available = true
	active, err := publicationDrainsQuery(ctx, conn, true)
	if err != nil {
		return projection, err
	}
	projection.Active = active
	latest, ok, err := publicationDrainLatestQuery(ctx, conn, true)
	if err != nil {
		return projection, err
	}
	if ok {
		projection.Latest = &latest
	}
	return projection, nil
}

const publicationDrainSelect = `
SELECT id,checkpoint_id,worktree_id,branch_ref,branch_generation,phase,
 target_event_count,published_event_count,semantic_rebuild_attempts,
 event_fallback_count,commit_count,fallback_mode,last_error,staged_consent,staged_consumed,
 created_ts,updated_ts,last_progress_ts,completed_ts
FROM publication_drains`

func publicationDrainByIDQuery(
	ctx context.Context,
	query checkpointQuery,
	id string,
	loadEvents bool,
) (PublicationDrain, bool, error) {
	drain, err := scanPublicationDrain(query.QueryRowContext(
		ctx, publicationDrainSelect+" WHERE id=?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return PublicationDrain{}, false, nil
	}
	if err != nil {
		return PublicationDrain{}, false, err
	}
	if loadEvents {
		if err := loadPublicationDrainEvents(ctx, query, &drain); err != nil {
			return PublicationDrain{}, false, err
		}
	}
	return drain, true, nil
}

func publicationDrainByCheckpointQuery(
	ctx context.Context,
	query checkpointQuery,
	checkpointID string,
	loadEvents bool,
) (PublicationDrain, bool, error) {
	drain, err := scanPublicationDrain(query.QueryRowContext(
		ctx, publicationDrainSelect+" WHERE checkpoint_id=?", checkpointID))
	if errors.Is(err, sql.ErrNoRows) {
		return PublicationDrain{}, false, nil
	}
	if err != nil {
		return PublicationDrain{}, false, err
	}
	if loadEvents {
		if err := loadPublicationDrainEvents(ctx, query, &drain); err != nil {
			return PublicationDrain{}, false, err
		}
	}
	return drain, true, nil
}

func publicationDrainLatestQuery(
	ctx context.Context,
	query checkpointQuery,
	loadEvents bool,
) (PublicationDrain, bool, error) {
	drain, err := scanPublicationDrain(query.QueryRowContext(
		ctx, publicationDrainSelect+" ORDER BY created_ts DESC,id DESC LIMIT 1"))
	if errors.Is(err, sql.ErrNoRows) {
		return PublicationDrain{}, false, nil
	}
	if err != nil {
		return PublicationDrain{}, false, err
	}
	if loadEvents {
		if err := loadPublicationDrainEvents(ctx, query, &drain); err != nil {
			return PublicationDrain{}, false, err
		}
	}
	return drain, true, nil
}

func publicationDrainsQuery(
	ctx context.Context,
	query interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	},
	activeOnly bool,
) ([]PublicationDrain, error) {
	statement := publicationDrainSelect
	if activeOnly {
		statement += " WHERE phase NOT IN ('completed','needs_action')"
	}
	statement += " ORDER BY created_ts,id"
	rows, err := query.QueryContext(ctx, statement)
	if err != nil {
		return nil, fmt.Errorf("state: query publication drains: %w", err)
	}
	defer rows.Close()
	var drains []PublicationDrain
	for rows.Next() {
		drain, err := scanPublicationDrain(rows)
		if err != nil {
			return nil, err
		}
		drains = append(drains, drain)
	}
	return drains, rows.Err()
}

func loadPublicationDrainEvents(
	ctx context.Context,
	query checkpointQuery,
	drain *PublicationDrain,
) error {
	rowsQuery, ok := query.(interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	})
	if !ok {
		return errors.New("state: publication drain query cannot load membership")
	}
	rows, err := rowsQuery.QueryContext(ctx, `
SELECT event_seq FROM publication_drain_events WHERE drain_id=? ORDER BY ord`,
		drain.ID)
	if err != nil {
		return fmt.Errorf("state: load publication drain events: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return err
		}
		drain.EventSeqs = append(drain.EventSeqs, seq)
	}
	return rows.Err()
}

func scanPublicationDrain(row checkpointRows) (PublicationDrain, error) {
	var drain PublicationDrain
	if err := row.Scan(&drain.ID, &drain.CheckpointID, &drain.WorktreeID,
		&drain.BranchRef, &drain.BranchGeneration, &drain.Phase,
		&drain.TargetEventCount, &drain.PublishedEventCount,
		&drain.SemanticRebuildAttempts, &drain.EventFallbackCount,
		&drain.CommitCount, &drain.FallbackMode, &drain.LastError,
		&drain.StagedConsent, &drain.StagedConsumed, &drain.CreatedTS, &drain.UpdatedTS,
		&drain.LastProgressTS, &drain.CompletedTS); err != nil {
		return PublicationDrain{}, err
	}
	return drain, nil
}

func validatePublicationDrainIdentity(drain PublicationDrain) error {
	if strings.TrimSpace(drain.ID) == "" || len(drain.ID) > 128 ||
		strings.TrimSpace(drain.CheckpointID) == "" ||
		len(drain.WorktreeID) != 16 || strings.TrimSpace(drain.BranchRef) == "" ||
		len(drain.BranchRef) > 1024 || drain.BranchGeneration < 0 ||
		drain.TargetEventCount < 0 || drain.PublishedEventCount < 0 ||
		drain.PublishedEventCount > drain.TargetEventCount ||
		drain.CreatedTS <= 0 || !validPublicationDrainPhase(drain.Phase) {
		return ErrPublicationDrainIdentity
	}
	return nil
}

func samePublicationDrainIdentity(left, right PublicationDrain) bool {
	return left.ID == right.ID && left.CheckpointID == right.CheckpointID &&
		left.WorktreeID == right.WorktreeID && left.BranchRef == right.BranchRef &&
		left.BranchGeneration == right.BranchGeneration &&
		left.TargetEventCount == right.TargetEventCount &&
		reflectPublicationDrainEventsEqual(left.EventSeqs, right.EventSeqs)
}

func reflectPublicationDrainEventsEqual(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func samePublicationDrainUpdate(
	current PublicationDrain,
	update PublicationDrainUpdate,
) bool {
	return current.Phase == update.Phase &&
		current.PublishedEventCount == update.PublishedEventCount &&
		current.SemanticRebuildAttempts == update.SemanticRebuildAttempts &&
		current.EventFallbackCount == update.EventFallbackCount &&
		current.CommitCount == update.CommitCount &&
		current.FallbackMode == update.FallbackMode &&
		current.LastError == update.LastError &&
		current.StagedConsent == update.StagedConsent &&
		current.StagedConsumed == update.StagedConsumed &&
		current.UpdatedTS == update.UpdatedTS &&
		current.LastProgressTS == update.LastProgressTS &&
		current.CompletedTS == update.CompletedTS
}

func validPublicationDrainPhase(phase string) bool {
	switch phase {
	case PublicationDrainCheckpointing, PublicationDrainSemantic,
		PublicationDrainNormalizing, PublicationDrainEventFallback,
		PublicationDrainCompleted, PublicationDrainNeedsAction:
		return true
	default:
		return false
	}
}

func validPublicationDrainTransition(from, to string) bool {
	if !validPublicationDrainPhase(to) ||
		from == PublicationDrainCompleted || from == PublicationDrainNeedsAction {
		return false
	}
	if to == PublicationDrainNeedsAction {
		return true
	}
	order := map[string]int{
		PublicationDrainCheckpointing: 0,
		PublicationDrainSemantic:      1,
		PublicationDrainNormalizing:   2,
		PublicationDrainEventFallback: 3,
		PublicationDrainCompleted:     4,
	}
	return order[to] >= order[from]
}

func sanitizePublicationDrainError(message string) string {
	message = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			return -1
		}
		return r
	}, strings.TrimSpace(message))
	if len(message) > 2048 {
		message = message[:2048]
	}
	return message
}
