package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
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
	ErrPublicationDrainRuntime  = errors.New("state: publication drain runtime contract unavailable")
	ErrPublicationDrainBarrier  = errors.New("state: unresolved publication drain blocks new drain")
)

// PublicationDrain is one resumable publication operation over the immutable
// event membership of CheckpointID. EventSeqs is loaded from checkpoint_events;
// it is never duplicated into mutable drain state.
type PublicationDrain struct {
	ID                  string
	CheckpointID        string
	WorktreeID          string
	BranchRef           string
	BranchGeneration    int64
	CommitStrategy      string
	CommitFormat        string
	ConfigRevisionID    int64
	Provider            string
	ProviderModel       string
	ProviderFingerprint string
	Phase               string
	TargetEventCount    int64
	// PublishedEventCount is the durable resolved-member count. The legacy
	// column name also includes events preserved by exact recovery and then
	// recaptured for a bounded follow-up drain.
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

// PublicationDrainReconciliation describes a drain whose immutable target is
// already fully resolved. PreviousPhase records the durable phase observed
// before completion.
type PublicationDrainReconciliation struct {
	ID             string
	CheckpointID   string
	PreviousPhase  string
	TargetEvents   int64
	ResolvedEvents int64
	CommitCount    int64
}

// ResolvedPublicationDrainCandidates returns drains that can be completed
// without touching Git. Callers use this for read-only recovery planning.
func ResolvedPublicationDrainCandidates(
	ctx context.Context,
	conn *sql.DB,
) ([]PublicationDrainReconciliation, error) {
	if conn == nil {
		return nil, errors.New(
			"state: ResolvedPublicationDrainCandidates: nil database")
	}
	return resolvedPublicationDrainCandidates(ctx, conn)
}

// ReconcileResolvedPublicationDrains completes every drain whose frozen
// membership is already fully published or recovered. The proof and phase
// transition share one transaction so concurrent publication cannot produce
// a partial result.
func ReconcileResolvedPublicationDrains(
	ctx context.Context,
	db *DB,
	completedTS float64,
) ([]PublicationDrainReconciliation, error) {
	if db == nil {
		return nil, errors.New(
			"state: ReconcileResolvedPublicationDrains: nil db")
	}
	if completedTS <= 0 {
		completedTS = nowSeconds()
	}
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf(
			"state: begin resolved publication drain reconciliation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	candidates, err := resolvedPublicationDrainCandidates(ctx, tx)
	if err != nil {
		return nil, err
	}
	reconciled := make([]PublicationDrainReconciliation, 0, len(candidates))
	for _, candidate := range candidates {
		result, err := tx.ExecContext(ctx, `
UPDATE publication_drains
SET phase='completed', published_event_count=?, commit_count=?,
    last_error='', updated_ts=MAX(updated_ts,?),
    last_progress_ts=MAX(last_progress_ts,?), completed_ts=?
WHERE id=? AND phase=? AND target_event_count=?`,
			candidate.ResolvedEvents, candidate.CommitCount,
			completedTS, completedTS, completedTS, candidate.ID,
			candidate.PreviousPhase, candidate.TargetEvents)
		if err != nil {
			return nil, fmt.Errorf(
				"state: complete resolved publication drain %s: %w",
				candidate.ID, err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf(
				"state: count resolved publication drain %s: %w",
				candidate.ID, err)
		}
		if changed != 1 {
			return nil, ErrPublicationDrainPhase
		}
		reconciled = append(reconciled, candidate)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf(
			"state: commit resolved publication drain reconciliation: %w", err)
	}
	return reconciled, nil
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
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("state: begin publication drain prepare: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := resolvePublicationDrainRuntimeContract(ctx, tx, &drain); err != nil {
		return false, err
	}
	if err := validatePublicationDrainIdentity(drain); err != nil {
		return false, err
	}

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
	var barrierID string
	barrierErr := tx.QueryRowContext(ctx, `
SELECT id FROM publication_drains
WHERE branch_ref=? AND branch_generation=? AND phase!='completed'
  AND id<>?
ORDER BY created_ts,id LIMIT 1`,
		drain.BranchRef, drain.BranchGeneration, drain.ID).Scan(&barrierID)
	if barrierErr == nil {
		return false, ErrPublicationDrainBarrier
	}
	if !errors.Is(barrierErr, sql.ErrNoRows) {
		return false, fmt.Errorf("state: inspect publication drain barrier: %w", barrierErr)
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
 id,checkpoint_id,worktree_id,branch_ref,branch_generation,
 commit_strategy,commit_format,config_revision_id,provider,provider_model,
 provider_fingerprint,phase,
 target_event_count,published_event_count,semantic_rebuild_attempts,
 event_fallback_count,commit_count,fallback_mode,last_error,staged_consent,staged_consumed,
 created_ts,updated_ts,last_progress_ts,completed_ts
) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		drain.ID, drain.CheckpointID, drain.WorktreeID, drain.BranchRef,
		drain.BranchGeneration, drain.CommitStrategy, drain.CommitFormat,
		drain.ConfigRevisionID, drain.Provider, drain.ProviderModel,
		drain.ProviderFingerprint,
		drain.Phase, drain.TargetEventCount,
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

func resolvePublicationDrainRuntimeContract(
	ctx context.Context,
	tx *sql.Tx,
	drain *PublicationDrain,
) error {
	if drain == nil {
		return ErrPublicationDrainRuntime
	}
	// An idempotent retry keeps the original immutable contract even if the
	// active runtime changed after the drain was first prepared.
	if existing, ok, err := publicationDrainByIDQuery(
		ctx, tx, drain.ID, false); err != nil {
		return err
	} else if ok {
		if drain.CommitStrategy == "" {
			drain.CommitStrategy = existing.CommitStrategy
		}
		if drain.CommitFormat == "" {
			drain.CommitFormat = existing.CommitFormat
		}
		if drain.ConfigRevisionID == 0 {
			drain.ConfigRevisionID = existing.ConfigRevisionID
		}
		if drain.Provider == "" {
			drain.Provider = existing.Provider
		}
		if drain.ProviderModel == "" {
			drain.ProviderModel = existing.ProviderModel
		}
		if drain.ProviderFingerprint == "" {
			drain.ProviderFingerprint = existing.ProviderFingerprint
		}
		drain.CommitStrategy = strings.TrimSpace(drain.CommitStrategy)
		drain.CommitFormat = strings.TrimSpace(drain.CommitFormat)
		drain.Provider = strings.TrimSpace(drain.Provider)
		drain.ProviderModel = strings.TrimSpace(drain.ProviderModel)
		drain.ProviderFingerprint = strings.TrimSpace(
			drain.ProviderFingerprint)
		if publicationDrainRuntimeContractComplete(*drain) {
			return nil
		}
	}
	var runtimeReady string
	readyErr := tx.QueryRowContext(ctx, `
SELECT value FROM daemon_meta
WHERE key='publication.runtime.ready'`).Scan(&runtimeReady)
	if readyErr == nil {
		if strings.TrimSpace(runtimeReady) != "true" {
			return ErrPublicationDrainRuntime
		}
	} else if !errors.Is(readyErr, sql.ErrNoRows) {
		return fmt.Errorf("state: read publication drain runtime readiness: %w", readyErr)
	} else {
		// A tuple written by an older binary has no readiness proof. Refuse to
		// freeze it: it may be the stale last usable runtime left behind after a
		// blocked desired bundle became active.
		var projected int
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM daemon_meta
WHERE key IN (
 'publication.runtime.commit_strategy',
 'publication.runtime.commit_format',
 'publication.runtime.config_revision_id',
 'publication.runtime.provider',
 'publication.runtime.provider_model',
 'publication.runtime.provider_fingerprint'
)`).Scan(&projected); err != nil {
			return fmt.Errorf("state: inspect publication drain runtime projection: %w", err)
		}
		if projected > 0 {
			return ErrPublicationDrainRuntime
		}
	}
	if drain.CommitStrategy == "" {
		_ = tx.QueryRowContext(ctx, `
SELECT value FROM daemon_meta
WHERE key='publication.runtime.commit_strategy'`).Scan(
			&drain.CommitStrategy)
	}
	if drain.CommitFormat == "" {
		_ = tx.QueryRowContext(ctx, `
SELECT value FROM daemon_meta
WHERE key='publication.runtime.commit_format'`).Scan(&drain.CommitFormat)
	}
	if drain.ConfigRevisionID == 0 {
		var rawRevision string
		if err := tx.QueryRowContext(ctx, `
SELECT value FROM daemon_meta
WHERE key='publication.runtime.config_revision_id'`).Scan(
			&rawRevision); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("state: read publication drain runtime revision: %w", err)
		} else if err == nil {
			revision, parseErr := strconv.ParseInt(
				strings.TrimSpace(rawRevision), 10, 64)
			if parseErr != nil || revision < 0 {
				return ErrPublicationDrainRuntime
			}
			drain.ConfigRevisionID = revision
		}
	}
	if drain.Provider == "" {
		_ = tx.QueryRowContext(ctx, `
SELECT value FROM daemon_meta
WHERE key='publication.runtime.provider'`).Scan(&drain.Provider)
	}
	if drain.ProviderModel == "" {
		_ = tx.QueryRowContext(ctx, `
SELECT value FROM daemon_meta
WHERE key='publication.runtime.provider_model'`).Scan(&drain.ProviderModel)
	}
	if drain.ProviderFingerprint == "" {
		_ = tx.QueryRowContext(ctx, `
SELECT value FROM daemon_meta
WHERE key='publication.runtime.provider_fingerprint'`).Scan(
			&drain.ProviderFingerprint)
	}
	drain.CommitStrategy = strings.TrimSpace(drain.CommitStrategy)
	drain.CommitFormat = strings.TrimSpace(drain.CommitFormat)
	drain.Provider = strings.TrimSpace(drain.Provider)
	drain.ProviderModel = strings.TrimSpace(drain.ProviderModel)
	drain.ProviderFingerprint = strings.TrimSpace(drain.ProviderFingerprint)
	if publicationDrainRuntimeContractComplete(*drain) {
		if drain.ConfigRevisionID > 0 {
			var applied sql.NullInt64
			if err := tx.QueryRowContext(ctx, `
SELECT applied_revision_id FROM runtime_config_state WHERE id=1`).Scan(
				&applied); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("state: read applied publication runtime: %w", err)
			}
			if !applied.Valid || applied.Int64 != drain.ConfigRevisionID {
				return ErrPublicationDrainRuntime
			}
		}
		return nil
	}

	// Repositories without a persisted runtime revision use the legacy Event
	// default. Once any runtime revision exists, an incomplete contract means
	// startup or validation has not produced a publishable bundle yet and the
	// drain must wait instead of guessing.
	var configured int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM runtime_config_state
WHERE id=1 AND (desired_revision_id IS NOT NULL OR
                applied_revision_id IS NOT NULL OR
                last_known_good_revision_id IS NOT NULL)`).Scan(
		&configured); err != nil {
		return fmt.Errorf("state: inspect publication drain runtime state: %w", err)
	}
	if configured > 0 {
		return ErrPublicationDrainRuntime
	}
	if drain.CommitStrategy == "" {
		_ = tx.QueryRowContext(ctx, `
SELECT value FROM daemon_meta WHERE key='commit.strategy'`).Scan(
			&drain.CommitStrategy)
	}
	if drain.Provider == "" {
		_ = tx.QueryRowContext(ctx, `
SELECT value FROM daemon_meta WHERE key='ai.provider'`).Scan(&drain.Provider)
	}
	if drain.CommitFormat == "" {
		_ = tx.QueryRowContext(ctx, `
SELECT value FROM daemon_meta WHERE key='commit.format'`).Scan(
			&drain.CommitFormat)
	}
	if strings.TrimSpace(drain.CommitStrategy) == "" {
		drain.CommitStrategy = "event"
	}
	if strings.TrimSpace(drain.Provider) == "" &&
		drain.CommitStrategy == "event" {
		drain.Provider = "deterministic"
	}
	if strings.TrimSpace(drain.CommitFormat) == "" {
		drain.CommitFormat = "imperative"
	}
	drain.CommitStrategy = strings.TrimSpace(drain.CommitStrategy)
	drain.CommitFormat = strings.TrimSpace(drain.CommitFormat)
	drain.Provider = strings.TrimSpace(drain.Provider)
	drain.ProviderModel = strings.TrimSpace(drain.ProviderModel)
	drain.ProviderFingerprint = strings.TrimSpace(drain.ProviderFingerprint)
	if !publicationDrainRuntimeContractComplete(*drain) {
		return ErrPublicationDrainRuntime
	}
	return nil
}

func publicationDrainRuntimeContractComplete(drain PublicationDrain) bool {
	return drain.CommitStrategy != "" && drain.CommitFormat != "" &&
		drain.Provider != "" &&
		(drain.CommitStrategy != "intent" || drain.ConfigRevisionID > 0 ||
			drain.ProviderFingerprint != "")
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

// ActivePublicationDrainsForPair returns at most two active drains for one
// exact branch generation. The second row is a fail-closed sentinel for
// callers that require unique ownership without scanning unrelated drains.
func ActivePublicationDrainsForPair(
	ctx context.Context,
	db *DB,
	branchRef string,
	branchGeneration int64,
) ([]PublicationDrain, error) {
	if db == nil {
		return nil, errors.New(
			"state: ActivePublicationDrainsForPair: nil db")
	}
	rows, err := db.ReadSQL().QueryContext(ctx, publicationDrainSelect+`
 WHERE branch_ref=? AND branch_generation=?
   AND phase NOT IN ('completed','needs_action')
 ORDER BY created_ts,id LIMIT 2`, branchRef, branchGeneration)
	if err != nil {
		return nil, fmt.Errorf(
			"state: query active publication drains for branch generation: %w", err)
	}
	defer rows.Close()
	drains := make([]PublicationDrain, 0, 2)
	for rows.Next() {
		drain, err := scanPublicationDrain(rows)
		if err != nil {
			return nil, fmt.Errorf(
				"state: scan active publication drain for branch generation: %w", err)
		}
		drains = append(drains, drain)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"state: iterate active publication drains for branch generation: %w", err)
	}
	return drains, nil
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
	if projection.SchemaVersion < 25 {
		active, err := legacyPublicationDrainsQuery(ctx, conn, true)
		if err != nil {
			return projection, err
		}
		projection.Active = active
		latest, ok, err := legacyPublicationDrainLatestQuery(ctx, conn, true)
		if err != nil {
			return projection, err
		}
		if ok {
			projection.Latest = &latest
		}
		return projection, nil
	}
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
 commit_strategy,commit_format,config_revision_id,provider,provider_model,
 provider_fingerprint,
 target_event_count,published_event_count,semantic_rebuild_attempts,
 event_fallback_count,commit_count,fallback_mode,last_error,staged_consent,staged_consumed,
 created_ts,updated_ts,last_progress_ts,completed_ts
FROM publication_drains`

const legacyPublicationDrainSelect = `
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

func legacyPublicationDrainsQuery(
	ctx context.Context,
	query interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	},
	activeOnly bool,
) ([]PublicationDrain, error) {
	statement := legacyPublicationDrainSelect
	if activeOnly {
		statement += " WHERE phase NOT IN ('completed','needs_action')"
	}
	statement += " ORDER BY created_ts,id"
	rows, err := query.QueryContext(ctx, statement)
	if err != nil {
		return nil, fmt.Errorf("state: query legacy publication drains: %w", err)
	}
	defer rows.Close()
	var drains []PublicationDrain
	for rows.Next() {
		drain, err := scanLegacyPublicationDrain(rows)
		if err != nil {
			return nil, err
		}
		drains = append(drains, drain)
	}
	return drains, rows.Err()
}

func legacyPublicationDrainLatestQuery(
	ctx context.Context,
	query checkpointQuery,
	loadEvents bool,
) (PublicationDrain, bool, error) {
	drain, err := scanLegacyPublicationDrain(query.QueryRowContext(
		ctx, legacyPublicationDrainSelect+
			" ORDER BY created_ts DESC,id DESC LIMIT 1"))
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

type publicationDrainCandidateQuery interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func resolvedPublicationDrainCandidates(
	ctx context.Context,
	query publicationDrainCandidateQuery,
) ([]PublicationDrainReconciliation, error) {
	rows, err := query.QueryContext(ctx, `
SELECT d.id,d.checkpoint_id,d.phase,d.target_event_count,
       COUNT(e.seq) AS resolved_events,
       COUNT(DISTINCT CASE
           WHEN e.state='published' AND COALESCE(e.commit_oid,'')!=''
           THEN e.commit_oid END) AS commit_count
FROM publication_drains d
JOIN checkpoints c ON c.id=d.checkpoint_id
LEFT JOIN publication_drain_events de ON de.drain_id=d.id
LEFT JOIN capture_events e ON e.seq=de.event_seq
WHERE d.phase!='completed'
  AND c.phase='completed'
  AND (d.staged_consent=0 OR d.staged_consumed=1)
GROUP BY d.id,d.checkpoint_id,d.phase,d.target_event_count
HAVING COUNT(de.event_seq)=d.target_event_count
   AND COUNT(e.seq)=d.target_event_count
   AND COUNT(CASE WHEN e.state IN ('published','recovered') THEN 1 END)=
       d.target_event_count
ORDER BY d.created_ts,d.id`)
	if err != nil {
		return nil, fmt.Errorf(
			"state: query resolved publication drains: %w", err)
	}
	defer rows.Close()
	var candidates []PublicationDrainReconciliation
	for rows.Next() {
		var candidate PublicationDrainReconciliation
		if err := rows.Scan(
			&candidate.ID, &candidate.CheckpointID,
			&candidate.PreviousPhase, &candidate.TargetEvents,
			&candidate.ResolvedEvents, &candidate.CommitCount,
		); err != nil {
			return nil, fmt.Errorf(
				"state: scan resolved publication drain: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"state: iterate resolved publication drains: %w", err)
	}
	return candidates, nil
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
		&drain.CommitStrategy, &drain.CommitFormat, &drain.ConfigRevisionID,
		&drain.Provider, &drain.ProviderModel, &drain.ProviderFingerprint,
		&drain.TargetEventCount, &drain.PublishedEventCount,
		&drain.SemanticRebuildAttempts, &drain.EventFallbackCount,
		&drain.CommitCount, &drain.FallbackMode, &drain.LastError,
		&drain.StagedConsent, &drain.StagedConsumed, &drain.CreatedTS, &drain.UpdatedTS,
		&drain.LastProgressTS, &drain.CompletedTS); err != nil {
		return PublicationDrain{}, err
	}
	return drain, nil
}

func scanLegacyPublicationDrain(row checkpointRows) (PublicationDrain, error) {
	var drain PublicationDrain
	if err := row.Scan(&drain.ID, &drain.CheckpointID, &drain.WorktreeID,
		&drain.BranchRef, &drain.BranchGeneration, &drain.Phase,
		&drain.TargetEventCount, &drain.PublishedEventCount,
		&drain.SemanticRebuildAttempts, &drain.EventFallbackCount,
		&drain.CommitCount, &drain.FallbackMode, &drain.LastError,
		&drain.StagedConsent, &drain.StagedConsumed, &drain.CreatedTS,
		&drain.UpdatedTS, &drain.LastProgressTS, &drain.CompletedTS); err != nil {
		return PublicationDrain{}, err
	}
	return drain, nil
}

func validatePublicationDrainIdentity(drain PublicationDrain) error {
	if strings.TrimSpace(drain.ID) == "" || len(drain.ID) > 128 ||
		strings.TrimSpace(drain.CheckpointID) == "" ||
		len(drain.WorktreeID) != 16 || strings.TrimSpace(drain.BranchRef) == "" ||
		len(drain.BranchRef) > 1024 || drain.BranchGeneration < 0 ||
		(drain.CommitStrategy != "event" && drain.CommitStrategy != "intent") ||
		(drain.CommitFormat != "imperative" &&
			drain.CommitFormat != "conventional") ||
		drain.ConfigRevisionID < 0 || strings.TrimSpace(drain.Provider) == "" ||
		len(drain.Provider) > 128 ||
		len(drain.ProviderModel) > 256 ||
		len(drain.ProviderFingerprint) > 71 ||
		(drain.CommitStrategy == "intent" && drain.ConfigRevisionID == 0 &&
			drain.ProviderFingerprint == "") ||
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
		left.CommitStrategy == right.CommitStrategy &&
		left.CommitFormat == right.CommitFormat &&
		left.ConfigRevisionID == right.ConfigRevisionID &&
		left.Provider == right.Provider &&
		left.ProviderModel == right.ProviderModel &&
		left.ProviderFingerprint == right.ProviderFingerprint &&
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
