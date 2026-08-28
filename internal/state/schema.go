// Package state owns the per-repo SQLite layer (open, migrate, CRUD).
//
// Schema reference: .plan/acd.md §6.1. The DDL below mirrors that section
// verbatim. The daily_rollups table is the long-term backward-compat anchor
// (D9): future migrations may only ALTER TABLE ADD COLUMN — never rename,
// remove, or reorder.
package state

// SchemaVersion is the current PRAGMA user_version value for the per-repo
// state DB. Bumping this triggers a migration step in migrate.go. v1 was the
// first acd release; v2 adds capture_events indexes used by replay barriers
// and pruning; v3 adds idx_capture_events_barrier — a covering index that
// keeps the PendingEvents barrier subquery off a full-table scan when
// long-running pauses fan capture_events into tens of thousands of rows;
// v4 adds idx_flush_requests_status_id so ClaimNextFlushRequest's
// `status='pending' ORDER BY id ASC` lookup stays O(log n) after the queue
// accumulates completed/acknowledged rows over a long uptime; v5 adds
// append-only product decision records for explainable capture/replay/CLI UX;
// v6 rebuilds decision_records so event_seq is denormalized ledger data rather
// than a foreign key cleared by capture_events pruning; v7 adds planner_state
// for bounded intent-planner deferrals; v8 adds reusable rewrite plan storage;
// v9 adds structured rewrite proposal failure storage; v10 preserves the
// commit-message format used to validate rewrite plans; v11 adds durable
// intent planner-window summaries for captured-vs-offered observability; v12
// adds immutable recovery snapshots with ordered, exact event membership; v13
// adds an index for published-prefix retention while unresolved same-base
// recovery chains remain in the ledger; v14 adds immutable runtime
// configuration revisions, activation and experiment ledgers, and revision
// metadata on planner windows and decisions; v15 adds the durable Intent v2
// candidate, dependency, boundary, verification, and repair ledgers; v16 adds
// durable background setup-validation attempts without backfilling existing
// applied revisions; v17 adds durable candidate lineage for dependency-driven
// canonical merges; v18 adds an immutable, crash-recoverable self-publication
// journal spanning the Git-applied and SQLite-completed boundary; v19 makes
// prepare-time publication completion semantics immutable across restart;
// v20 adds the general operation journal and immutable checkpoint ledger;
// v21 adds durable publication drains over immutable checkpoint membership;
// v22 adds restart-stable adaptive Intent planning runs; v23 preserves the
// bounded resolved plan so a completed run can be reused after restart; v24
// adds immutable event membership for newly prepared Intent repairs.
const SchemaVersion = 24

// schemaDDL is the canonical per-repo state.db schema (§6.1).
//
// All CREATE statements use IF NOT EXISTS so the DDL is idempotent and safe
// to re-run on every Open. PRAGMAs are applied separately in db.go because
// some of them (journal_mode) return rows that exec() ignores cleanly but
// other tooling expects to see acknowledged.
const schemaDDL = `
CREATE TABLE IF NOT EXISTS daemon_state(
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    pid             INTEGER NOT NULL DEFAULT 0,
    mode            TEXT NOT NULL DEFAULT 'stopped',
    heartbeat_ts    REAL NOT NULL DEFAULT 0,
    branch_ref      TEXT,
    branch_generation INTEGER,
    note            TEXT,
    daemon_token    TEXT,
    daemon_fingerprint TEXT,
    updated_ts      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS daemon_clients(
    session_id      TEXT PRIMARY KEY,
    harness         TEXT NOT NULL,
    watch_pid       INTEGER,
    watch_fp        TEXT,
    registered_ts   REAL NOT NULL,
    last_seen_ts    REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_clients_last_seen
    ON daemon_clients(last_seen_ts);

CREATE TABLE IF NOT EXISTS shadow_paths(
    branch_ref       TEXT NOT NULL,
    branch_generation INTEGER NOT NULL,
    path             TEXT NOT NULL,
    operation        TEXT NOT NULL,
    mode             TEXT,
    oid              TEXT,
    old_path         TEXT,
    base_head        TEXT NOT NULL,
    fidelity         TEXT NOT NULL,
    updated_ts       REAL NOT NULL,
    PRIMARY KEY (branch_ref, branch_generation, path)
);

CREATE TABLE IF NOT EXISTS capture_events(
    seq              INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_ref       TEXT NOT NULL,
    branch_generation INTEGER NOT NULL,
    base_head        TEXT NOT NULL,
    operation        TEXT NOT NULL,
    path             TEXT NOT NULL,
    old_path         TEXT,
    fidelity         TEXT NOT NULL,
    captured_ts      REAL NOT NULL,
    published_ts     REAL,
    state            TEXT NOT NULL DEFAULT 'pending',
    commit_oid       TEXT,
    error            TEXT,
    message          TEXT
);

CREATE INDEX IF NOT EXISTS idx_capture_events_state_captured
    ON capture_events(state, captured_ts);

CREATE INDEX IF NOT EXISTS idx_capture_events_branch_generation_seq_state
    ON capture_events(branch_ref, branch_generation, seq, state);

-- v3: barrier-friendly leading-state covering index. PendingEvents and the
-- pending-depth cap both filter by (branch_ref, branch_generation, state)
-- and order/aggregate on seq, so leading state lets SQLite jump straight to
-- the matching rows without scanning unrelated branch_ref/generation pairs.
CREATE INDEX IF NOT EXISTS idx_capture_events_barrier
    ON capture_events(branch_ref, branch_generation, state, seq);

-- v13: recovery-prefix pruning correlates an aged published row with later
-- unresolved rows from the same exact immutable base. Keep that minute-level
-- maintenance query bounded even when the pending ledger reaches its cap.
CREATE INDEX IF NOT EXISTS idx_capture_events_recovery_prefix
    ON capture_events(branch_ref, branch_generation, base_head, state, seq);

CREATE TABLE IF NOT EXISTS capture_ops(
    event_seq    INTEGER NOT NULL,
    ord          INTEGER NOT NULL,
    op           TEXT NOT NULL,
    path         TEXT NOT NULL,
    old_path     TEXT,
    before_oid   TEXT,
    before_mode  TEXT,
    after_oid    TEXT,
    after_mode   TEXT,
    fidelity     TEXT NOT NULL,
    PRIMARY KEY (event_seq, ord),
    FOREIGN KEY (event_seq) REFERENCES capture_events(seq) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS planner_state(
    event_seq          INTEGER PRIMARY KEY,
    defer_count        INTEGER NOT NULL DEFAULT 0 CHECK (defer_count >= 0),
    last_planned_ts    REAL NOT NULL DEFAULT 0,
    last_defer_reason  TEXT,
    last_plan_error    TEXT,
    FOREIGN KEY (event_seq) REFERENCES capture_events(seq) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_planner_state_defer_count_planned
    ON planner_state(defer_count, last_planned_ts, event_seq);

CREATE INDEX IF NOT EXISTS idx_planner_state_last_planned
    ON planner_state(last_planned_ts, event_seq);

CREATE TABLE IF NOT EXISTS intent_planner_windows(
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    planned_ts            REAL NOT NULL,
    provider              TEXT,
    model                 TEXT,
    branch_ref            TEXT NOT NULL,
    branch_generation     INTEGER NOT NULL,
    source                TEXT,
    commit_format         TEXT,
    forced                INTEGER NOT NULL DEFAULT 0,
    forced_reason         TEXT,
    validation_failure    TEXT,
    offered_seqs          TEXT NOT NULL,
    visible_original_seqs TEXT NOT NULL,
    hidden_seqs           TEXT NOT NULL,
    selected_groups       TEXT NOT NULL,
    deferred_seqs         TEXT NOT NULL,
    deferred_reasons      TEXT NOT NULL,
    config_revision_id    INTEGER,
    config_profile        TEXT,
    duration_ms           INTEGER,
    retry_count           INTEGER NOT NULL DEFAULT 0,
    fallback_used         INTEGER NOT NULL DEFAULT 0,
    outcome               TEXT,
    plan_fingerprint      TEXT,
    plan_attempt          INTEGER NOT NULL DEFAULT 0,
    plan_attempt_limit    INTEGER NOT NULL DEFAULT 0,
    unresolved_capture_count INTEGER NOT NULL DEFAULT 0,
    preserved_group_count INTEGER NOT NULL DEFAULT 0,
    resolution_mode       TEXT,
    singleton_count       INTEGER NOT NULL DEFAULT 0,
    experiment_id         INTEGER,
    experiment_consumed   INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_intent_planner_windows_ts_id
    ON intent_planner_windows(planned_ts, id);

CREATE INDEX IF NOT EXISTS idx_intent_planner_windows_branch_id
    ON intent_planner_windows(branch_ref, branch_generation, id);

CREATE TABLE IF NOT EXISTS intent_plan_runs(
    fingerprint              TEXT PRIMARY KEY,
    branch_ref               TEXT NOT NULL,
    branch_generation        INTEGER NOT NULL,
    provider                 TEXT,
    model                    TEXT,
    config_revision_id       INTEGER,
    attempt_count            INTEGER NOT NULL DEFAULT 0,
    attempt_limit            INTEGER NOT NULL,
    preserved_groups         TEXT NOT NULL DEFAULT '[]',
    unresolved_seqs          TEXT NOT NULL DEFAULT '[]',
    finding_codes            TEXT NOT NULL DEFAULT '[]',
    normalized_partition     TEXT,
    progress_state           TEXT,
    resolution_mode          TEXT,
    resolved_plan_json       TEXT CHECK (
                              resolved_plan_json IS NULL OR
                              length(resolved_plan_json) <= 524288),
    completed                INTEGER NOT NULL DEFAULT 0,
    created_ts               REAL NOT NULL,
    updated_ts               REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_intent_plan_runs_branch_updated
    ON intent_plan_runs(branch_ref, branch_generation, updated_ts);

CREATE TABLE IF NOT EXISTS intent_planner_window_events(
    window_id  INTEGER NOT NULL,
    event_seq  INTEGER NOT NULL,
    offered    INTEGER NOT NULL DEFAULT 0,
    hidden     INTEGER NOT NULL DEFAULT 0,
    selected   INTEGER NOT NULL DEFAULT 0,
    deferred   INTEGER NOT NULL DEFAULT 0,
    group_ord  INTEGER,
    PRIMARY KEY (window_id, event_seq),
    FOREIGN KEY (window_id) REFERENCES intent_planner_windows(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_intent_planner_window_events_seq_window
    ON intent_planner_window_events(event_seq, window_id);

CREATE TABLE IF NOT EXISTS rewrite_plans(
    id                  TEXT PRIMARY KEY,
    created_ts          REAL NOT NULL,
    updated_ts          REAL NOT NULL,
    base_plan_id        TEXT,
    revision            INTEGER NOT NULL DEFAULT 1,
    branch_ref          TEXT NOT NULL,
    expected_head       TEXT NOT NULL,
    provider            TEXT,
    model               TEXT,
    commit_format       TEXT NOT NULL DEFAULT 'imperative',
    validation_status   TEXT NOT NULL,
    validation_error    TEXT,
    edited              INTEGER NOT NULL DEFAULT 0,
    apply_status        TEXT NOT NULL DEFAULT 'pending',
    FOREIGN KEY (base_plan_id) REFERENCES rewrite_plans(id)
);

CREATE INDEX IF NOT EXISTS idx_rewrite_plans_branch_head_status
    ON rewrite_plans(branch_ref, expected_head, apply_status);

CREATE INDEX IF NOT EXISTS idx_rewrite_plans_base_revision
    ON rewrite_plans(base_plan_id, revision);

CREATE TABLE IF NOT EXISTS rewrite_plan_commits(
    plan_id             TEXT NOT NULL,
    ord                 INTEGER NOT NULL,
    old_oid             TEXT NOT NULL,
    proposed_message    TEXT NOT NULL,
    original_message    TEXT NOT NULL,
    PRIMARY KEY (plan_id, ord),
    FOREIGN KEY (plan_id) REFERENCES rewrite_plans(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS flush_requests(
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    command          TEXT NOT NULL,
    non_blocking     INTEGER NOT NULL DEFAULT 0,
    requested_ts     REAL NOT NULL,
    acknowledged_ts  REAL,
    completed_ts     REAL,
    status           TEXT NOT NULL DEFAULT 'pending',
    note             TEXT
);

-- v4: ClaimNextFlushRequest filters status='pending' and orders by id ASC.
-- Without an index it scans the full table, which becomes expensive after a
-- long uptime accumulates acknowledged/completed/failed rows.
CREATE INDEX IF NOT EXISTS idx_flush_requests_status_id
    ON flush_requests(status, id);

CREATE TABLE IF NOT EXISTS decision_records(
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
    user_message        TEXT,
    config_revision_id  INTEGER,
    config_profile      TEXT
);

CREATE INDEX IF NOT EXISTS idx_decision_records_ts_id
    ON decision_records(decision_ts, id);

CREATE INDEX IF NOT EXISTS idx_decision_records_path_id
    ON decision_records(path, id);

CREATE INDEX IF NOT EXISTS idx_decision_records_event_seq_id
    ON decision_records(event_seq, id);

CREATE INDEX IF NOT EXISTS idx_decision_records_commit_oid_id
    ON decision_records(commit_oid, id);

-- v14 runtime settings ledger. Snapshots contain only canonical, sanitized
-- JSON. Triggers make the append-only contract explicit even for callers that
-- bypass package helpers.
CREATE TABLE IF NOT EXISTS config_revisions(
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    created_ts          REAL NOT NULL,
    profile             TEXT NOT NULL,
    scope               TEXT NOT NULL,
    snapshot_json       TEXT NOT NULL,
    snapshot_hash       TEXT NOT NULL,
    source_generation   INTEGER NOT NULL CHECK (source_generation >= 0),
    reason              TEXT
);

CREATE INDEX IF NOT EXISTS idx_config_revisions_scope_created
    ON config_revisions(scope, created_ts, id);

CREATE TRIGGER IF NOT EXISTS config_revisions_no_update
BEFORE UPDATE ON config_revisions
BEGIN
    SELECT RAISE(ABORT, 'config revisions are immutable');
END;

CREATE TRIGGER IF NOT EXISTS config_revisions_no_delete
BEFORE DELETE ON config_revisions
BEGIN
    SELECT RAISE(ABORT, 'config revisions are immutable');
END;

CREATE TABLE IF NOT EXISTS runtime_config_state(
    id                       INTEGER PRIMARY KEY CHECK (id = 1),
    desired_revision_id      INTEGER,
    applied_revision_id      INTEGER,
    last_known_good_revision_id INTEGER,
    desired_request_id       INTEGER,
    desired_ts               REAL,
    applied_ts               REAL,
    last_error               TEXT,
    updated_ts               REAL NOT NULL,
    FOREIGN KEY (desired_revision_id) REFERENCES config_revisions(id),
    FOREIGN KEY (applied_revision_id) REFERENCES config_revisions(id),
    FOREIGN KEY (last_known_good_revision_id) REFERENCES config_revisions(id)
);

CREATE TABLE IF NOT EXISTS config_activation_requests(
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    revision_id          INTEGER NOT NULL,
    prior_desired_revision_id INTEGER,
    status               TEXT NOT NULL CHECK (status IN
                           ('pending','acknowledged','applied','rejected','cancelled')),
    requested_ts         REAL NOT NULL,
    acknowledged_ts      REAL,
    completed_ts         REAL,
    error                TEXT,
    FOREIGN KEY (revision_id) REFERENCES config_revisions(id),
    FOREIGN KEY (prior_desired_revision_id) REFERENCES config_revisions(id)
);

CREATE INDEX IF NOT EXISTS idx_config_activation_requests_status_id
    ON config_activation_requests(status, id);

CREATE INDEX IF NOT EXISTS idx_config_activation_requests_revision_id
    ON config_activation_requests(revision_id, id);

-- v16 durable setup validation. Commands remain in immutable runtime
-- revisions; this ledger stores only provenance and a digest.
CREATE TABLE IF NOT EXISTS config_validation_runs(
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    activation_request_id INTEGER NOT NULL,
    revision_id           INTEGER NOT NULL,
    attempt               INTEGER NOT NULL CHECK (attempt > 0),
    branch_ref            TEXT NOT NULL,
    branch_generation     INTEGER NOT NULL CHECK (branch_generation >= 0),
    expected_head         TEXT NOT NULL,
    mode                  TEXT NOT NULL CHECK (mode IN ('structural','fast','full')),
    command_source        TEXT NOT NULL,
    command_digest        TEXT NOT NULL,
    approval_id           TEXT NOT NULL,
    status                TEXT NOT NULL CHECK (status IN
                          ('queued','running','passed','failed','timed_out','cancelled')),
    owner_pid             INTEGER,
    requested_ts          REAL NOT NULL,
    started_ts            REAL,
    completed_ts          REAL,
    exit_code             INTEGER,
    sanitized_output      TEXT NOT NULL DEFAULT '',
    bounded_error         TEXT,
    FOREIGN KEY (activation_request_id) REFERENCES config_activation_requests(id),
    FOREIGN KEY (revision_id) REFERENCES config_revisions(id),
    UNIQUE (activation_request_id, attempt)
);

CREATE INDEX IF NOT EXISTS idx_config_validation_runs_status_id
    ON config_validation_runs(status, id);

CREATE INDEX IF NOT EXISTS idx_config_validation_runs_request_attempt
    ON config_validation_runs(activation_request_id, attempt);

CREATE TABLE IF NOT EXISTS config_experiments(
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    baseline_revision_id INTEGER NOT NULL,
    candidate_revision_id INTEGER NOT NULL,
    window_budget        INTEGER NOT NULL CHECK (window_budget > 0),
    completed_windows    INTEGER NOT NULL DEFAULT 0 CHECK (completed_windows >= 0),
    expires_ts           REAL,
    failure_policy       TEXT NOT NULL,
    status               TEXT NOT NULL CHECK (status IN
                           ('active','completed','expired','failed','cancelled')),
    created_ts           REAL NOT NULL,
    updated_ts           REAL NOT NULL,
    completed_ts         REAL,
    terminal_reason      TEXT,
    FOREIGN KEY (baseline_revision_id) REFERENCES config_revisions(id),
    FOREIGN KEY (candidate_revision_id) REFERENCES config_revisions(id),
    CHECK (baseline_revision_id <> candidate_revision_id),
    CHECK (completed_windows <= window_budget)
);

CREATE INDEX IF NOT EXISTS idx_config_experiments_status_expiry
    ON config_experiments(status, expires_ts, id);

CREATE INDEX IF NOT EXISTS idx_config_experiments_candidate_status
    ON config_experiments(candidate_revision_id, status, id);

-- v15 Intent v2 state. Candidate rows contain only bounded, privacy-safe
-- summaries. Source material remains in immutable capture_ops and is
-- reconstructed when a planner or verifier needs it.
CREATE TABLE IF NOT EXISTS intent_candidates(
    id                       TEXT PRIMARY KEY,
    branch_ref               TEXT NOT NULL,
    branch_generation        INTEGER NOT NULL,
    status                   TEXT NOT NULL CHECK (status IN
                              ('open','waiting','ready','soft_published',
                               'published','superseded','blocked','failed')),
    purpose                  TEXT NOT NULL DEFAULT '',
    created_ts               REAL NOT NULL,
    updated_ts               REAL NOT NULL,
    ready_ts                 REAL,
    readiness                TEXT NOT NULL DEFAULT 'wait'
                              CHECK (readiness IN ('ready','wait')),
    missing_companions       TEXT NOT NULL DEFAULT '',
    atomicity_status         TEXT,
    atomicity_summary        TEXT NOT NULL DEFAULT '',
    atomicity_checked_ts     REAL,
    provider                 TEXT,
    model                    TEXT,
    planner_protocol         TEXT,
    config_revision_id       INTEGER,
    config_profile           TEXT,
    preset_id                TEXT,
    preset_version           INTEGER,
    soft_publication_deadline REAL,
    verification_status      TEXT,
    verification_output      TEXT NOT NULL DEFAULT '',
    verification_ts          REAL,
    published_commit_oid     TEXT
);

CREATE INDEX IF NOT EXISTS idx_intent_candidates_pair_status_updated
    ON intent_candidates(branch_ref, branch_generation, status, updated_ts, id);

CREATE INDEX IF NOT EXISTS idx_intent_candidates_published_oid
    ON intent_candidates(published_commit_oid, id);

CREATE INDEX IF NOT EXISTS idx_intent_candidates_status_updated
    ON intent_candidates(status, updated_ts DESC, id);

CREATE INDEX IF NOT EXISTS idx_intent_candidates_updated
    ON intent_candidates(updated_ts DESC, id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_intent_candidates_id_pair
    ON intent_candidates(id, branch_ref, branch_generation);

CREATE TABLE IF NOT EXISTS intent_candidate_events(
    candidate_id       TEXT NOT NULL,
    ord                INTEGER NOT NULL CHECK (ord >= 0),
    event_seq          INTEGER NOT NULL CHECK (event_seq > 0),
    event_role         TEXT NOT NULL,
    membership_state   TEXT NOT NULL DEFAULT 'active'
                       CHECK (membership_state IN ('active','superseded')),
    PRIMARY KEY (candidate_id, event_seq),
    FOREIGN KEY (candidate_id) REFERENCES intent_candidates(id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_intent_candidate_events_candidate_state_ord
    ON intent_candidate_events(candidate_id, membership_state, ord);

CREATE INDEX IF NOT EXISTS idx_intent_candidate_events_seq_candidate
    ON intent_candidate_events(event_seq, candidate_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_intent_candidate_events_active_seq
    ON intent_candidate_events(event_seq)
    WHERE membership_state = 'active';

-- v17 durable candidate lineage. This ledger contains only candidate
-- identities, bounded status/reason metadata, and an optional published
-- commit OID. Raw source or diff content remains in the capture ledger.
CREATE TABLE IF NOT EXISTS intent_candidate_lineage(
    branch_ref                  TEXT NOT NULL,
    branch_generation          INTEGER NOT NULL,
    target_candidate_id        TEXT NOT NULL,
    source_candidate_id        TEXT NOT NULL,
    source_status              TEXT NOT NULL CHECK (source_status IN
                                ('open','waiting','ready','soft_published',
                                 'published','superseded','blocked','failed')),
    source_published_commit_oid TEXT,
    reason                      TEXT NOT NULL,
    created_ts                  REAL NOT NULL,
    PRIMARY KEY (branch_ref, branch_generation, source_candidate_id),
    CHECK (target_candidate_id <> source_candidate_id),
    CHECK (length(target_candidate_id) BETWEEN 1 AND 128),
    CHECK (length(source_candidate_id) BETWEEN 1 AND 128),
    CHECK (length(reason) BETWEEN 1 AND 512),
    CHECK (source_published_commit_oid IS NULL OR
           length(source_published_commit_oid) BETWEEN 1 AND 128),
    FOREIGN KEY (target_candidate_id, branch_ref, branch_generation)
        REFERENCES intent_candidates(id, branch_ref, branch_generation),
    FOREIGN KEY (source_candidate_id, branch_ref, branch_generation)
        REFERENCES intent_candidates(id, branch_ref, branch_generation)
);

CREATE INDEX IF NOT EXISTS idx_intent_candidate_lineage_target_created
    ON intent_candidate_lineage(
        branch_ref, branch_generation, target_candidate_id,
        created_ts, source_candidate_id);

CREATE TABLE IF NOT EXISTS intent_capture_dependencies(
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    branch_ref          TEXT NOT NULL,
    branch_generation   INTEGER NOT NULL,
    prerequisite_seq    INTEGER NOT NULL CHECK (prerequisite_seq > 0),
    dependent_seq       INTEGER NOT NULL CHECK (dependent_seq > 0),
    strength            TEXT NOT NULL CHECK (strength IN ('hard','soft')),
    kind                TEXT NOT NULL,
    evidence            TEXT NOT NULL DEFAULT '',
    created_ts          REAL NOT NULL,
    CHECK (prerequisite_seq <> dependent_seq),
    UNIQUE (branch_ref, branch_generation, prerequisite_seq,
            dependent_seq, strength, kind)
);

CREATE INDEX IF NOT EXISTS idx_intent_capture_dependencies_pair_from
    ON intent_capture_dependencies(
        branch_ref, branch_generation, prerequisite_seq, dependent_seq);

CREATE INDEX IF NOT EXISTS idx_intent_capture_dependencies_pair_to
    ON intent_capture_dependencies(
        branch_ref, branch_generation, dependent_seq, prerequisite_seq);

CREATE TABLE IF NOT EXISTS intent_activity_boundaries(
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    epoch               INTEGER NOT NULL UNIQUE CHECK (epoch > 0),
    kind                TEXT NOT NULL CHECK (kind IN ('soft','hard')),
    source              TEXT NOT NULL,
    branch_ref          TEXT,
    branch_generation   INTEGER,
    created_ts          REAL NOT NULL,
    consumed_ts         REAL,
    CHECK ((branch_ref IS NULL AND branch_generation IS NULL) OR
           (branch_ref IS NOT NULL AND branch_generation IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_intent_activity_boundaries_consumed_epoch
    ON intent_activity_boundaries(consumed_ts, epoch);

CREATE TABLE IF NOT EXISTS intent_repairs(
    id                  TEXT PRIMARY KEY,
    branch_ref          TEXT NOT NULL,
    branch_generation   INTEGER NOT NULL,
    status              TEXT NOT NULL CHECK (status IN
                           ('prepared','git_applied','completed','skipped','failed')),
    expected_head       TEXT NOT NULL,
    plan_digest         TEXT NOT NULL,
    backup_ref          TEXT,
    old_head            TEXT,
    new_head            TEXT,
    created_ts          REAL NOT NULL,
    updated_ts          REAL NOT NULL,
    git_applied_ts      REAL,
    completed_ts        REAL,
    error               TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_intent_repairs_pair_status_updated
    ON intent_repairs(branch_ref, branch_generation, status, updated_ts, id);

CREATE INDEX IF NOT EXISTS idx_intent_repairs_backup_ref
    ON intent_repairs(backup_ref, id);

CREATE INDEX IF NOT EXISTS idx_intent_repairs_status_updated
    ON intent_repairs(status, updated_ts DESC, id);

CREATE INDEX IF NOT EXISTS idx_intent_repairs_updated
    ON intent_repairs(updated_ts DESC, id);

CREATE TABLE IF NOT EXISTS intent_repair_commits(
    repair_id           TEXT NOT NULL,
    ord                 INTEGER NOT NULL CHECK (ord >= 0),
    candidate_id        TEXT,
    old_oid             TEXT NOT NULL,
    new_oid             TEXT,
    PRIMARY KEY (repair_id, ord),
    UNIQUE (repair_id, old_oid),
    FOREIGN KEY (repair_id) REFERENCES intent_repairs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_intent_repair_commits_old_oid
    ON intent_repair_commits(old_oid, repair_id);

CREATE INDEX IF NOT EXISTS idx_intent_repair_commits_candidate
    ON intent_repair_commits(candidate_id, repair_id);

-- v24: immutable active candidate membership captured before an Intent repair
-- may change Git. Legacy repairs deliberately have no rows here; consumers can
-- distinguish their weaker historical evidence without synthesizing identity.
CREATE TABLE IF NOT EXISTS intent_repair_members(
    repair_id           TEXT NOT NULL,
    ord                 INTEGER NOT NULL CHECK (ord >= 0),
    candidate_id        TEXT NOT NULL,
    event_seq           INTEGER NOT NULL CHECK (event_seq > 0),
    prior_state         TEXT NOT NULL CHECK (prior_state IN ('pending','published')),
    PRIMARY KEY (repair_id, ord),
    UNIQUE (repair_id, event_seq),
    CHECK (length(candidate_id) BETWEEN 1 AND 128),
    FOREIGN KEY (repair_id) REFERENCES intent_repairs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_intent_repair_members_candidate
    ON intent_repair_members(repair_id, candidate_id, ord);

CREATE INDEX IF NOT EXISTS idx_intent_repair_members_event
    ON intent_repair_members(event_seq, repair_id);

CREATE TRIGGER IF NOT EXISTS intent_repair_members_prepared_insert
BEFORE INSERT ON intent_repair_members
WHEN NOT EXISTS (
    SELECT 1 FROM intent_repairs repair
    WHERE repair.id = NEW.repair_id AND repair.status = 'prepared'
)
BEGIN
    SELECT RAISE(ABORT, 'intent repair membership requires prepared repair');
END;

CREATE TRIGGER IF NOT EXISTS intent_repair_members_immutable_update
BEFORE UPDATE ON intent_repair_members
BEGIN
    SELECT RAISE(ABORT, 'intent repair membership is immutable');
END;

CREATE TRIGGER IF NOT EXISTS intent_repair_members_immutable_delete
BEFORE DELETE ON intent_repair_members
WHEN EXISTS (
    SELECT 1 FROM intent_repairs repair WHERE repair.id = OLD.repair_id
)
BEGIN
    SELECT RAISE(ABORT, 'intent repair membership is immutable');
END;

-- v20: one general mutation journal shared by checkpoint, publication,
-- restore, migration, setup, and uninstall orchestration. Specialized
-- journals retain their domain-specific proof; operation_id correlates them.
CREATE TABLE IF NOT EXISTS operations(
    id                  TEXT PRIMARY KEY,
    kind                TEXT NOT NULL,
    worktree_id         TEXT,
    phase               TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN
                           ('prepared','active','completed','failed',
                            'needs_action','rolled_back')),
    plan_digest         TEXT NOT NULL DEFAULT '',
    error               TEXT NOT NULL DEFAULT '',
    created_ts          REAL NOT NULL,
    updated_ts          REAL NOT NULL,
    completed_ts        REAL,
    CHECK (length(id) BETWEEN 1 AND 128),
    CHECK (length(kind) BETWEEN 1 AND 64),
    CHECK (worktree_id IS NULL OR length(worktree_id) = 16),
    CHECK (length(phase) BETWEEN 1 AND 64),
    CHECK (plan_digest = '' OR
           (length(plan_digest) = 71 AND substr(plan_digest, 1, 7) = 'sha256:')),
    CHECK (length(error) <= 2048)
);

CREATE INDEX IF NOT EXISTS idx_operations_status_created
    ON operations(status, created_ts, id);

CREATE INDEX IF NOT EXISTS idx_operations_worktree_created
    ON operations(worktree_id, created_ts, id);

CREATE TABLE IF NOT EXISTS operation_steps(
    operation_id        TEXT NOT NULL,
    ord                 INTEGER NOT NULL CHECK (ord >= 0),
    kind                TEXT NOT NULL,
    target              TEXT NOT NULL DEFAULT '',
    phase               TEXT NOT NULL,
    before_digest       TEXT NOT NULL DEFAULT '',
    after_digest        TEXT NOT NULL DEFAULT '',
    proof_id            TEXT NOT NULL DEFAULT '',
    detail              TEXT NOT NULL DEFAULT '',
    completed_ts        REAL,
    PRIMARY KEY (operation_id, ord),
    CHECK (length(kind) BETWEEN 1 AND 64),
    CHECK (length(phase) BETWEEN 1 AND 64),
    CHECK (length(target) <= 4096),
    CHECK (length(detail) <= 2048),
    FOREIGN KEY (operation_id) REFERENCES operations(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS checkpoints(
    id                  TEXT PRIMARY KEY,
    seq                 INTEGER NOT NULL UNIQUE,
    operation_id        TEXT NOT NULL UNIQUE,
    worktree_id         TEXT NOT NULL,
    reason              TEXT NOT NULL CHECK (reason IN
                           ('watch','poll','hint','migration','migration_recovery',
                            'pre_restore','restore','manual_barrier')),
    observation_epoch   INTEGER NOT NULL CHECK (observation_epoch >= 0),
    coverage_epoch      INTEGER NOT NULL CHECK (coverage_epoch >= 0),
    observed_head       TEXT NOT NULL DEFAULT '',
    observed_ref        TEXT NOT NULL DEFAULT '',
    tree_oid            TEXT NOT NULL,
    commit_oid          TEXT NOT NULL,
    checkpoint_ref      TEXT NOT NULL UNIQUE,
    phase               TEXT NOT NULL CHECK (phase IN
                           ('prepared','completed','needs_action')),
    created_ts          REAL NOT NULL,
    completed_ts        REAL,
	retained            INTEGER NOT NULL DEFAULT 1 CHECK (retained IN (0,1)),
	pruned_ts           REAL,
    error               TEXT NOT NULL DEFAULT '',
    CHECK (length(worktree_id) = 16),
    CHECK (coverage_epoch <= observation_epoch),
    CHECK (length(observed_head) <= 128),
    CHECK (length(observed_ref) <= 1024),
    CHECK (length(tree_oid) BETWEEN 1 AND 128),
    CHECK (length(commit_oid) BETWEEN 1 AND 128),
    CHECK (length(checkpoint_ref) BETWEEN 1 AND 2048),
    CHECK (length(error) <= 2048),
    FOREIGN KEY (operation_id) REFERENCES operations(id)
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_worktree_seq
    ON checkpoints(worktree_id, seq DESC);

CREATE INDEX IF NOT EXISTS idx_checkpoints_phase_created
    ON checkpoints(phase, created_ts, id);

CREATE TABLE IF NOT EXISTS checkpoint_events(
    checkpoint_id       TEXT NOT NULL,
    ord                 INTEGER NOT NULL CHECK (ord >= 0),
    event_seq           INTEGER NOT NULL CHECK (event_seq > 0),
    PRIMARY KEY (checkpoint_id, ord),
    UNIQUE (checkpoint_id, event_seq),
    UNIQUE (event_seq),
    FOREIGN KEY (checkpoint_id) REFERENCES checkpoints(id) ON DELETE CASCADE,
    FOREIGN KEY (event_seq) REFERENCES capture_events(seq)
);

CREATE INDEX IF NOT EXISTS idx_checkpoint_events_event
    ON checkpoint_events(event_seq, checkpoint_id);

CREATE TABLE IF NOT EXISTS checkpoint_exclusions(
    checkpoint_id       TEXT NOT NULL,
    category            TEXT NOT NULL,
    count               INTEGER NOT NULL CHECK (count >= 0),
    PRIMARY KEY (checkpoint_id, category),
    CHECK (length(category) BETWEEN 1 AND 64),
    FOREIGN KEY (checkpoint_id) REFERENCES checkpoints(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS checkpoint_publications(
    checkpoint_id       TEXT NOT NULL,
    event_seq           INTEGER NOT NULL CHECK (event_seq > 0),
    commit_oid          TEXT NOT NULL,
    published_ts        REAL NOT NULL,
    PRIMARY KEY (checkpoint_id, event_seq),
    CHECK (length(commit_oid) BETWEEN 1 AND 128),
    FOREIGN KEY (checkpoint_id) REFERENCES checkpoints(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_checkpoint_publications_commit
    ON checkpoint_publications(commit_oid, checkpoint_id);

CREATE TABLE IF NOT EXISTS publication_drains(
    id                  TEXT PRIMARY KEY,
    checkpoint_id       TEXT NOT NULL UNIQUE,
    worktree_id         TEXT NOT NULL,
    branch_ref          TEXT NOT NULL,
    branch_generation   INTEGER NOT NULL CHECK (branch_generation >= 0),
    phase               TEXT NOT NULL CHECK (phase IN
                           ('checkpointing','semantic','normalizing',
                            'event_fallback','completed','needs_action')),
    target_event_count  INTEGER NOT NULL CHECK (target_event_count >= 0),
    published_event_count INTEGER NOT NULL DEFAULT 0
                          CHECK (published_event_count >= 0),
    semantic_rebuild_attempts INTEGER NOT NULL DEFAULT 0
                              CHECK (semantic_rebuild_attempts >= 0),
    event_fallback_count INTEGER NOT NULL DEFAULT 0
                         CHECK (event_fallback_count >= 0),
    commit_count        INTEGER NOT NULL DEFAULT 0 CHECK (commit_count >= 0),
    fallback_mode       TEXT NOT NULL DEFAULT '',
    last_error          TEXT NOT NULL DEFAULT '',
    staged_consent      INTEGER NOT NULL DEFAULT 0 CHECK (staged_consent IN (0,1)),
    staged_consumed     INTEGER NOT NULL DEFAULT 0 CHECK (staged_consumed IN (0,1)),
    created_ts          REAL NOT NULL,
    updated_ts          REAL NOT NULL,
    last_progress_ts    REAL NOT NULL,
    completed_ts        REAL,
    CHECK (published_event_count <= target_event_count),
    CHECK (length(id) BETWEEN 1 AND 128),
    CHECK (length(worktree_id) = 16),
    CHECK (length(branch_ref) BETWEEN 1 AND 1024),
    CHECK (length(fallback_mode) <= 64),
    CHECK (length(last_error) <= 2048),
    FOREIGN KEY (checkpoint_id) REFERENCES checkpoints(id)
);

CREATE INDEX IF NOT EXISTS idx_publication_drains_active
    ON publication_drains(phase, updated_ts, id);

CREATE TABLE IF NOT EXISTS publication_drain_events(
    drain_id            TEXT NOT NULL,
    ord                 INTEGER NOT NULL CHECK (ord >= 0),
    event_seq           INTEGER NOT NULL CHECK (event_seq > 0),
    PRIMARY KEY (drain_id, ord),
    UNIQUE (drain_id, event_seq),
    FOREIGN KEY (drain_id) REFERENCES publication_drains(id) ON DELETE CASCADE,
    FOREIGN KEY (event_seq) REFERENCES capture_events(seq)
);

CREATE INDEX IF NOT EXISTS idx_publication_drain_events_event
    ON publication_drain_events(event_seq, drain_id);

CREATE TRIGGER IF NOT EXISTS checkpoint_publications_track_event
AFTER UPDATE OF state, commit_oid, published_ts ON capture_events
WHEN NEW.state = 'published'
 AND NEW.commit_oid IS NOT NULL
 AND EXISTS (SELECT 1 FROM checkpoint_events WHERE event_seq = NEW.seq)
BEGIN
    INSERT INTO checkpoint_publications(
        checkpoint_id, event_seq, commit_oid, published_ts
    )
    SELECT checkpoint_id, NEW.seq, NEW.commit_oid,
           COALESCE(NEW.published_ts, CAST(strftime('%s','now') AS REAL))
    FROM checkpoint_events
    WHERE event_seq = NEW.seq
    ON CONFLICT(checkpoint_id, event_seq) DO UPDATE SET
        commit_oid = excluded.commit_oid,
        published_ts = excluded.published_ts;
END;

CREATE TABLE IF NOT EXISTS restore_operations(
    operation_id        TEXT PRIMARY KEY,
    target_checkpoint_id TEXT NOT NULL,
    pre_restore_checkpoint_id TEXT,
    post_restore_checkpoint_id TEXT,
    plan_digest         TEXT NOT NULL,
    phase               TEXT NOT NULL CHECK (phase IN
                           ('prepared','pre_checkpointed','applying',
                            'applied','completed','rolled_back','needs_action')),
    created_ts          REAL NOT NULL,
    updated_ts          REAL NOT NULL,
    CHECK (length(plan_digest) = 71 AND substr(plan_digest, 1, 7) = 'sha256:'),
    FOREIGN KEY (operation_id) REFERENCES operations(id),
    FOREIGN KEY (target_checkpoint_id) REFERENCES checkpoints(id),
    FOREIGN KEY (pre_restore_checkpoint_id) REFERENCES checkpoints(id),
    FOREIGN KEY (post_restore_checkpoint_id) REFERENCES checkpoints(id)
);

-- v18: crash-safe journal for ACD-authored branch advances. Identity columns
-- are immutable after prepare; only phase/timestamp/error columns advance.
-- Exact event/candidate membership is retained in the child ledger so
-- completion and restart recovery never infer ownership from a moving queue.
CREATE TABLE IF NOT EXISTS self_publications(
    id                  TEXT PRIMARY KEY,
    operation_id        TEXT,
    branch_ref          TEXT NOT NULL,
    branch_generation   INTEGER NOT NULL CHECK (branch_generation >= 0),
    source_head         TEXT NOT NULL,
    target_commit_oid   TEXT NOT NULL,
    target_tree_oid     TEXT NOT NULL,
    membership_digest   TEXT NOT NULL,
    member_count        INTEGER NOT NULL
                        CHECK (member_count BETWEEN 1 AND 256),
    phase               TEXT NOT NULL CHECK (phase IN
                           ('prepared','git_applied','completed','abandoned')),
    created_ts          REAL NOT NULL,
    updated_ts          REAL NOT NULL,
    git_applied_ts      REAL,
    completed_ts        REAL,
    abandoned_ts        REAL,
    error               TEXT NOT NULL DEFAULT '',
    completion_published_ts REAL NOT NULL,
    completion_candidate_status TEXT NOT NULL CHECK (
                        completion_candidate_status IN
                        ('unknown','published','soft_published')),
    completion_soft_deadline REAL,
    CHECK (length(id) BETWEEN 1 AND 128),
    CHECK (length(branch_ref) BETWEEN 1 AND 1024),
    -- Empty source_head is the exact identity for an unborn branch whose
    -- publication creates the initial commit.
    CHECK (length(source_head) BETWEEN 0 AND 128),
    CHECK (length(target_commit_oid) BETWEEN 1 AND 128),
    CHECK (length(target_tree_oid) BETWEEN 1 AND 128),
    CHECK (length(membership_digest) = 71
           AND substr(membership_digest, 1, 7) = 'sha256:'),
    CHECK (
        (completion_candidate_status = 'unknown'
         AND completion_published_ts = 0
         AND completion_soft_deadline IS NULL)
        OR
        (completion_candidate_status = 'published'
         AND completion_soft_deadline IS NULL)
        OR
        (completion_candidate_status = 'soft_published'
         AND completion_soft_deadline > completion_published_ts)
    ),
    CHECK (length(error) <= 2048),
    FOREIGN KEY (operation_id) REFERENCES operations(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_self_publications_pair_target
    ON self_publications(branch_ref, branch_generation, target_commit_oid)
    WHERE phase <> 'abandoned';

CREATE INDEX IF NOT EXISTS idx_self_publications_pair_phase_created
    ON self_publications(
        branch_ref, branch_generation, phase, created_ts, id);

CREATE INDEX IF NOT EXISTS idx_self_publications_phase_created
    ON self_publications(phase, created_ts, id);

CREATE TABLE IF NOT EXISTS self_publication_members(
    publication_id      TEXT NOT NULL,
    ord                 INTEGER NOT NULL CHECK (ord >= 0),
    event_seq           INTEGER NOT NULL CHECK (event_seq > 0),
    candidate_id        TEXT,
    PRIMARY KEY (publication_id, ord),
    UNIQUE (publication_id, event_seq),
    CHECK (candidate_id IS NULL OR
           length(candidate_id) BETWEEN 1 AND 128),
    FOREIGN KEY (publication_id) REFERENCES self_publications(id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_self_publication_members_event
    ON self_publication_members(event_seq, publication_id);

CREATE INDEX IF NOT EXISTS idx_self_publication_members_candidate
    ON self_publication_members(candidate_id, publication_id, ord)
    WHERE candidate_id IS NOT NULL;

-- Defense in depth for callers that bypass package APIs. SQLite triggers
-- reject every attempted change to immutable publication identity, illegal
-- phase movement, or overlapping live ownership.
CREATE TRIGGER IF NOT EXISTS self_publications_prepare_only
BEFORE INSERT ON self_publications
WHEN NEW.phase <> 'prepared'
BEGIN
    SELECT RAISE(ABORT, 'self-publication must start prepared');
END;

CREATE TRIGGER IF NOT EXISTS self_publications_identity_immutable
BEFORE UPDATE OF operation_id, branch_ref, branch_generation, source_head,
                 target_commit_oid, target_tree_oid, membership_digest,
                 member_count, completion_published_ts,
                 completion_candidate_status, completion_soft_deadline
ON self_publications
BEGIN
    SELECT RAISE(ABORT, 'self-publication identity is immutable');
END;

CREATE TRIGGER IF NOT EXISTS self_publications_phase_monotonic
BEFORE UPDATE OF phase ON self_publications
WHEN NOT (
    OLD.phase = NEW.phase
    OR (OLD.phase = 'prepared'
        AND NEW.phase IN ('git_applied','abandoned'))
    OR (OLD.phase = 'git_applied' AND NEW.phase = 'completed')
)
BEGIN
    SELECT RAISE(ABORT, 'illegal self-publication phase transition');
END;

CREATE TRIGGER IF NOT EXISTS self_publication_members_no_live_overlap
BEFORE INSERT ON self_publication_members
WHEN EXISTS (
    SELECT 1
    FROM self_publication_members member
    JOIN self_publications publication
      ON publication.id = member.publication_id
    WHERE member.event_seq = NEW.event_seq
      AND member.publication_id <> NEW.publication_id
      AND publication.phase IN ('prepared','git_applied','completed')
)
BEGIN
    SELECT RAISE(ABORT, 'self-publication event already owned');
END;

CREATE TRIGGER IF NOT EXISTS self_publication_members_immutable_update
BEFORE UPDATE ON self_publication_members
BEGIN
    SELECT RAISE(ABORT, 'self-publication membership is immutable');
END;

CREATE TRIGGER IF NOT EXISTS self_publication_members_immutable_delete
BEFORE DELETE ON self_publication_members
WHEN EXISTS (
    SELECT 1 FROM self_publications publication
    WHERE publication.id = OLD.publication_id
)
BEGIN
    SELECT RAISE(ABORT, 'self-publication membership is immutable');
END;

-- v12: one durable record for an all-or-none unpublished-chain transition.
-- capture event provenance remains on capture_events; this table records the
-- terminal recovery outcome and the commit that makes the chain reachable.
CREATE TABLE IF NOT EXISTS recovery_snapshots(
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    created_ts          REAL NOT NULL,
    outcome             TEXT NOT NULL CHECK (outcome IN ('published', 'recovered')),
    branch_ref          TEXT NOT NULL,
    branch_generation   INTEGER NOT NULL,
    first_event_seq     INTEGER NOT NULL,
    last_event_seq      INTEGER NOT NULL,
    event_count         INTEGER NOT NULL CHECK (event_count > 0),
    commit_oid          TEXT NOT NULL,
    recovery_ref        TEXT NOT NULL CHECK (recovery_ref <> ''),
    reason              TEXT,
    CHECK (first_event_seq > 0 AND last_event_seq >= first_event_seq),
    CHECK (recovery_ref <> '')
);

CREATE INDEX IF NOT EXISTS idx_recovery_snapshots_anchor_id
    ON recovery_snapshots(branch_ref, branch_generation, id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_recovery_snapshots_recovery_ref
    ON recovery_snapshots(recovery_ref)
    WHERE recovery_ref IS NOT NULL;

-- event_seq is deliberately denormalized without a capture_events foreign
-- key, matching decision_records v6: snapshot membership must survive any
-- future pruning of terminal capture rows. ord preserves exact chain order.
CREATE TABLE IF NOT EXISTS recovery_snapshot_events(
    snapshot_id         INTEGER NOT NULL,
    ord                 INTEGER NOT NULL CHECK (ord >= 0),
    event_seq           INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, ord),
    FOREIGN KEY (snapshot_id) REFERENCES recovery_snapshots(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_recovery_snapshot_events_event_seq
    ON recovery_snapshot_events(event_seq);

CREATE TABLE IF NOT EXISTS publish_state(
    id                  INTEGER PRIMARY KEY CHECK (id = 1),
    event_seq           INTEGER,
    branch_ref          TEXT,
    branch_generation   INTEGER,
    source_head         TEXT,
    target_commit_oid   TEXT,
    status              TEXT NOT NULL DEFAULT 'idle',
    error               TEXT,
    updated_ts          REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS daemon_meta(
    key           TEXT PRIMARY KEY,
    value         TEXT NOT NULL,
    updated_ts    REAL NOT NULL
);

-- Long-lived stats anchor. SCHEMA IS APPEND-ONLY across versions.
-- Future migrations may ONLY use ALTER TABLE ADD COLUMN. See §6.1.
CREATE TABLE IF NOT EXISTS daily_rollups(
    day            TEXT NOT NULL,
    repo_root      TEXT NOT NULL,
    events_total   INTEGER NOT NULL DEFAULT 0,
    commits_total  INTEGER NOT NULL DEFAULT 0,
    files_changed  INTEGER NOT NULL DEFAULT 0,
    bytes_changed  INTEGER NOT NULL DEFAULT 0,
    errors_total   INTEGER NOT NULL DEFAULT 0,
    sessions_seen  INTEGER NOT NULL DEFAULT 0,
    daemon_uptime_seconds INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (day, repo_root)
);
`
