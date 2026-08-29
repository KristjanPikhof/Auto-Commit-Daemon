package state

import (
	"context"
	"database/sql"
	"fmt"
)

const runtimeConfigV14IndexesDDL = `
CREATE INDEX IF NOT EXISTS idx_intent_planner_windows_revision_id
    ON intent_planner_windows(config_revision_id, id);
CREATE INDEX IF NOT EXISTS idx_intent_planner_windows_experiment_id
    ON intent_planner_windows(experiment_id, id);
CREATE INDEX IF NOT EXISTS idx_decision_records_config_revision_id
    ON decision_records(config_revision_id, id);
`

const decisionRecordsV6MigrationDDL = `
DROP TABLE IF EXISTS decision_records_v6;

CREATE TABLE decision_records_v6(
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_ts          REAL NOT NULL,
    kind                TEXT NOT NULL,
    path                TEXT,
    reason              TEXT,
    event_seq           INTEGER,
    head_sha            TEXT,
    commit_oid          TEXT,
    branch_ref          TEXT,
    branch_generation   INTEGER,
    action_taken        TEXT,
    user_message        TEXT
);

INSERT OR IGNORE INTO decision_records_v6(
    id, decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
    branch_ref, branch_generation, action_taken, user_message
)
SELECT
    id, decision_ts, kind, path, reason, event_seq, head_sha, commit_oid,
    branch_ref, branch_generation, action_taken, user_message
FROM decision_records;

DROP TABLE decision_records;

ALTER TABLE decision_records_v6 RENAME TO decision_records;
`

// Migrate brings the database forward from whatever PRAGMA user_version it
// currently reports up to SchemaVersion.
//
// v1 was the first acd release. v2 adds idempotent indexes through schemaDDL.
// v3 adds idx_capture_events_barrier (a covering index that keeps the
// PendingEvents barrier subquery off a full-table scan during long pauses).
// v4 adds idx_flush_requests_status_id so ClaimNextFlushRequest's
// pending-by-id scan stays index-backed after long uptime. v5 adds
// decision_records, an append-only ledger for product-facing decisions. v6
// rebuilds decision_records without the event_seq foreign key so capture_event
// pruning cannot erase denormalized ledger identity. v7 adds planner_state for
// bounded intent-planner deferrals. v8 adds reusable rewrite plan storage.
// v9 adds rewrite_plans.validation_error for structured proposal failures.
// v10 adds rewrite_plans.commit_format so saved plans preserve the validation
// policy used when they were generated or edited. v11 adds
// intent_planner_windows and intent_planner_window_events for lossless,
// privacy-safe planner-window observability. v12 adds recovery_snapshots and
// recovery_snapshot_events. v13 adds idx_capture_events_recovery_prefix for
// bounded published-prefix pruning. v14 adds the runtime settings ledger and
// additive planner/decision metadata. v15 adds the durable Intent v2
// candidate, dependency, boundary, and repair ledgers. v16 adds durable setup
// validation attempts without backfilling already-applied revisions. v17 adds
// the candidate-lineage ledger without rewriting existing candidates. v18
// adds the immutable self-publication journal without backfilling historical
// publishes. v19 persists prepare-time publication completion semantics so
// restart recovery cannot reinterpret repair state. v20 adds the general
// operation/checkpoint ledger and correlates new self-publication rows with
// it. v21 adds publication_drains without backfilling historical checkpoints.
// v22 adds durable adaptive planner attempts and additive window summaries.
// v23 preserves the bounded resolved plan for restart-safe reuse. v24 adds
// immutable Intent repair membership and zero-member seals for historical
// repairs without inventing historical membership. v25 freezes the runtime
// strategy, active revision, and provider used by publication drains.
// New tables are pure DDL;
// columns on existing tables are added
// explicitly for upgraded databases.
// Future migrations are append-only for daily_rollups (D9) — only ALTER TABLE
// ADD COLUMN. Schema-changing helpers belong here, not in db.go.
//
// Open's runBootstrap re-applies the idempotent schemaDDL whenever the
// stored user_version is below SchemaVersion, so simply bumping SchemaVersion
// and adding idempotent statements to schemaDDL is sufficient for pure-DDL
// migrations (such as v2→v3). v6 uses an explicit table rebuild for only
// pre-v6 databases whose decision_records table still has the old event_seq
// foreign key. v7, v8, v11, v12, and v13 are pure DDL migrations through
// schemaDDL. v15, v16, v17, and v18 are additive and deliberately have no
// data backfill:
// existing intent repositories are cut over by runtime configuration
// orchestration, not by mutating their capture ledger during schema bootstrap.
// Migrate is wired now so future phases requiring separate data backfill have
// one entry point.
func (d *DB) Migrate(ctx context.Context) error {
	cur, err := d.UserVersion(ctx)
	if err != nil {
		return err
	}
	if cur > SchemaVersion {
		return fmt.Errorf("state: db user_version=%d is newer than this binary's SchemaVersion=%d", cur, SchemaVersion)
	}
	if cur == SchemaVersion {
		return nil
	}
	// Open applies the idempotent schemaDDL for older databases before it stamps
	// SchemaVersion, so no explicit post-open migration step exists yet.
	return nil
}

func applyVersionedMigrations(ctx context.Context, tx *sql.Tx, cur int) error {
	if cur < 6 {
		rebuilt, err := migrateDecisionRecordsV6(ctx, tx)
		if err != nil {
			return err
		}
		if rebuilt {
			if _, err := tx.ExecContext(ctx, schemaDDL); err != nil {
				return fmt.Errorf("state: reapply schema after v6 decision_records migration: %w", err)
			}
		}
	}
	if cur < 9 {
		if err := addColumnIfMissing(ctx, tx, "rewrite_plans", "validation_error", "TEXT"); err != nil {
			return err
		}
	}
	if cur < 10 {
		if err := addColumnIfMissing(ctx, tx, "rewrite_plans", "commit_format", "TEXT NOT NULL DEFAULT 'imperative'"); err != nil {
			return err
		}
	}
	if cur < 14 {
		columns := []struct {
			table, column, typ string
		}{
			{"intent_planner_windows", "config_revision_id", "INTEGER"},
			{"intent_planner_windows", "config_profile", "TEXT"},
			{"intent_planner_windows", "duration_ms", "INTEGER"},
			{"intent_planner_windows", "retry_count", "INTEGER NOT NULL DEFAULT 0"},
			{"intent_planner_windows", "fallback_used", "INTEGER NOT NULL DEFAULT 0"},
			{"intent_planner_windows", "outcome", "TEXT"},
			{"intent_planner_windows", "experiment_id", "INTEGER"},
			{"intent_planner_windows", "experiment_consumed", "INTEGER NOT NULL DEFAULT 0"},
			{"decision_records", "config_revision_id", "INTEGER"},
			{"decision_records", "config_profile", "TEXT"},
		}
		for _, col := range columns {
			if err := addColumnIfMissing(ctx, tx, col.table, col.column, col.typ); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, runtimeConfigV14IndexesDDL); err != nil {
			return fmt.Errorf("state: add v14 runtime config indexes: %w", err)
		}
	}
	if cur < 15 {
		// Only repositories with pre-existing runtime/capture evidence need
		// the one-time Intent v2 cutover. A brand-new v15 database must not
		// synthesize legacy configuration merely because its initial
		// user_version is zero.
		if _, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO daemon_meta(key, value, updated_ts)
SELECT 'intent.v2.cutover_required', 'true',
       CAST(strftime('%s','now') AS REAL)
WHERE EXISTS(SELECT 1 FROM capture_events LIMIT 1)
   OR EXISTS(SELECT 1 FROM config_revisions LIMIT 1)
   OR EXISTS(SELECT 1 FROM daemon_meta WHERE key='commit.strategy')`); err != nil {
			return fmt.Errorf("state: mark Intent v2 cutover: %w", err)
		}
	}
	if cur < 19 {
		for _, col := range []struct {
			name string
			typ  string
		}{
			{"completion_published_ts", "REAL NOT NULL DEFAULT 0"},
			{"completion_candidate_status",
				"TEXT NOT NULL DEFAULT 'unknown'"},
			{"completion_soft_deadline", "REAL"},
		} {
			if err := addColumnIfMissing(
				ctx, tx, "self_publications", col.name, col.typ); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `
DROP TRIGGER IF EXISTS self_publications_identity_immutable;
CREATE TRIGGER self_publications_identity_immutable
BEFORE UPDATE OF branch_ref, branch_generation, source_head,
                 target_commit_oid, target_tree_oid, membership_digest,
                 member_count, completion_published_ts,
                 completion_candidate_status, completion_soft_deadline
ON self_publications
BEGIN
    SELECT RAISE(ABORT, 'self-publication identity is immutable');
END;`); err != nil {
			return fmt.Errorf(
				"state: migrate self-publication completion identity: %w", err)
		}
	}
	if cur < 20 {
		if err := addColumnIfMissing(ctx, tx, "self_publications", "operation_id", "TEXT REFERENCES operations(id)"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
DROP TRIGGER IF EXISTS self_publications_identity_immutable;
CREATE TRIGGER self_publications_identity_immutable
BEFORE UPDATE OF operation_id, branch_ref, branch_generation, source_head,
                 target_commit_oid, target_tree_oid, membership_digest,
                 member_count, completion_published_ts,
                 completion_candidate_status, completion_soft_deadline
ON self_publications
BEGIN
    SELECT RAISE(ABORT, 'self-publication identity is immutable');
END;`); err != nil {
			return fmt.Errorf("state: migrate self-publication operation identity: %w", err)
		}
	}
	if cur < 22 {
		for _, col := range []struct {
			name string
			typ  string
		}{
			{"plan_fingerprint", "TEXT"},
			{"plan_attempt", "INTEGER NOT NULL DEFAULT 0"},
			{"plan_attempt_limit", "INTEGER NOT NULL DEFAULT 0"},
			{"unresolved_capture_count", "INTEGER NOT NULL DEFAULT 0"},
			{"preserved_group_count", "INTEGER NOT NULL DEFAULT 0"},
			{"resolution_mode", "TEXT"},
			{"singleton_count", "INTEGER NOT NULL DEFAULT 0"},
		} {
			if err := addColumnIfMissing(ctx, tx, "intent_planner_windows", col.name, col.typ); err != nil {
				return err
			}
		}
	}
	if cur < 23 {
		if err := addColumnIfMissing(ctx, tx, "intent_plan_runs",
			"resolved_plan_json", "TEXT"); err != nil {
			return err
		}
	}
	if cur < 24 {
		if _, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO intent_repair_member_seals(
    repair_id, membership_mode, member_count
)
SELECT repair.id, 'legacy', COUNT(member.event_seq)
FROM intent_repairs repair
LEFT JOIN intent_repair_members member ON member.repair_id=repair.id
GROUP BY repair.id`); err != nil {
			return fmt.Errorf("state: seal legacy intent repair membership: %w", err)
		}
	}
	if cur < 25 {
		for _, col := range []struct {
			name string
			typ  string
		}{
			{"commit_strategy", "TEXT NOT NULL DEFAULT ''"},
			{"commit_format", "TEXT NOT NULL DEFAULT ''"},
			{"config_revision_id", "INTEGER NOT NULL DEFAULT 0"},
			{"provider", "TEXT NOT NULL DEFAULT ''"},
			{"provider_model", "TEXT NOT NULL DEFAULT ''"},
			{"provider_fingerprint", "TEXT NOT NULL DEFAULT ''"},
		} {
			if err := addColumnIfMissing(
				ctx, tx, "publication_drains", col.name, col.typ); err != nil {
				return err
			}
		}
		// Legacy drains did not persist their runtime contract. Adopt one only
		// when a completed provider plan selected a frozen target event after
		// drain preparation and every such proof has one revision-backed tuple.
		// Current daemon_meta is deliberately excluded because settings may have
		// changed after drain preparation.
		if _, err := tx.ExecContext(ctx, `
WITH tuple_proofs AS (
    SELECT d.id AS drain_id,
           w.provider AS provider,
           COALESCE(w.model, '') AS provider_model,
           w.commit_format AS commit_format,
           COALESCE(w.config_revision_id, 0) AS config_revision_id
    FROM publication_drains d
    JOIN publication_drain_events de ON de.drain_id=d.id
    JOIN intent_planner_window_events we ON we.event_seq=de.event_seq
    JOIN intent_planner_windows w ON w.id=we.window_id
    JOIN config_revisions r ON r.id=w.config_revision_id
    WHERE d.phase!='completed'
      AND w.planned_ts>=d.created_ts
      AND we.selected=1
      AND w.outcome='selected'
      AND COALESCE(w.validation_failure, '')=''
      AND w.resolution_mode='provider'
      AND COALESCE(w.provider, '')!=''
      AND w.provider!='deterministic'
      AND w.provider NOT LIKE 'publication-drain-%'
      AND w.commit_format IN ('imperative','conventional')
      AND w.branch_ref=d.branch_ref
      AND w.branch_generation=d.branch_generation
      AND w.config_revision_id>0
      AND json_extract(r.snapshot_json, '$."commit.strategy"')='intent'
      AND json_extract(r.snapshot_json, '$."ai.provider"')=w.provider
      AND COALESCE(json_extract(r.snapshot_json, '$."ai.model"'), '')=
          COALESCE(w.model, '')
      AND json_extract(r.snapshot_json, '$."commit.format"')=w.commit_format
    GROUP BY d.id,w.provider,COALESCE(w.model,''),w.commit_format,
             COALESCE(w.config_revision_id,0)
), unambiguous AS (
    SELECT drain_id,MIN(provider) AS provider,
           MIN(provider_model) AS provider_model,
           MIN(commit_format) AS commit_format,
           MIN(config_revision_id) AS config_revision_id
    FROM tuple_proofs
    GROUP BY drain_id
    HAVING COUNT(*)=1
)
UPDATE publication_drains
SET commit_strategy='intent',
    commit_format=(SELECT commit_format FROM unambiguous
                   WHERE drain_id=publication_drains.id),
    config_revision_id=(SELECT config_revision_id FROM unambiguous
                        WHERE drain_id=publication_drains.id),
    provider=(SELECT provider FROM unambiguous
              WHERE drain_id=publication_drains.id),
    provider_model=(SELECT provider_model FROM unambiguous
                    WHERE drain_id=publication_drains.id)
WHERE id IN (SELECT drain_id FROM unambiguous)`); err != nil {
			return fmt.Errorf("state: freeze publication drain runtime contract: %w", err)
		}
	}
	return nil
}

func addColumnIfMissing(ctx context.Context, tx *sql.Tx, table, column, typ string) error {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return fmt.Errorf("state: inspect %s columns: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notNull int
		var dflt any
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dflt, &pk); err != nil {
			return fmt.Errorf("state: scan %s columns: %w", table, err)
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("state: iterate %s columns: %w", table, err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, typ)); err != nil {
		return fmt.Errorf("state: add %s.%s: %w", table, column, err)
	}
	return nil
}

func migrateDecisionRecordsV6(ctx context.Context, tx *sql.Tx) (bool, error) {
	hasFK, err := decisionRecordsHasForeignKeys(ctx, tx)
	if err != nil {
		return false, err
	}
	if !hasFK {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, decisionRecordsV6MigrationDDL); err != nil {
		return false, fmt.Errorf("state: migrate decision_records v6: %w", err)
	}
	return true, nil
}

func decisionRecordsHasForeignKeys(ctx context.Context, tx *sql.Tx) (bool, error) {
	var n int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_foreign_key_list('decision_records')`).Scan(&n); err != nil {
		return false, fmt.Errorf("state: inspect decision_records foreign keys: %w", err)
	}
	return n > 0, nil
}
