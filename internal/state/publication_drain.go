package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	ReasonCode              string
	ReasonEvidence          string
	ExpectedIndexDigest     string
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
	ReasonCode              string
	ReasonEvidence          string
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
    last_error='', reason_code='',reason_evidence='', updated_ts=MAX(updated_ts,?),
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

const publicationDrainSelect = `
SELECT id,checkpoint_id,worktree_id,branch_ref,branch_generation,phase,
 commit_strategy,commit_format,config_revision_id,provider,provider_model,
 provider_fingerprint,
 target_event_count,published_event_count,semantic_rebuild_attempts,
 event_fallback_count,commit_count,fallback_mode,last_error,staged_consent,staged_consumed,
 reason_code,reason_evidence,expected_index_digest,created_ts,updated_ts,last_progress_ts,completed_ts
FROM publication_drains`

const legacyPublicationDrainSelect = `
SELECT id,checkpoint_id,worktree_id,branch_ref,branch_generation,phase,
 target_event_count,published_event_count,semantic_rebuild_attempts,
 event_fallback_count,commit_count,fallback_mode,last_error,staged_consent,staged_consumed,
 created_ts,updated_ts,last_progress_ts,completed_ts
FROM publication_drains`

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

func scanPublicationDrain(row checkpointRows) (PublicationDrain, error) {
	var drain PublicationDrain
	if err := row.Scan(&drain.ID, &drain.CheckpointID, &drain.WorktreeID,
		&drain.BranchRef, &drain.BranchGeneration, &drain.Phase,
		&drain.CommitStrategy, &drain.CommitFormat, &drain.ConfigRevisionID,
		&drain.Provider, &drain.ProviderModel, &drain.ProviderFingerprint,
		&drain.TargetEventCount, &drain.PublishedEventCount,
		&drain.SemanticRebuildAttempts, &drain.EventFallbackCount,
		&drain.CommitCount, &drain.FallbackMode, &drain.LastError,
		&drain.StagedConsent, &drain.StagedConsumed, &drain.ReasonCode, &drain.ReasonEvidence, &drain.ExpectedIndexDigest, &drain.CreatedTS, &drain.UpdatedTS,
		&drain.LastProgressTS, &drain.CompletedTS); err != nil {
		return PublicationDrain{}, err
	}
	return drain, nil
}
