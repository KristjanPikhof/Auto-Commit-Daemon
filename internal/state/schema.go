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
// recovery chains remain in the ledger.
const SchemaVersion = 13

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
    deferred_reasons      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_intent_planner_windows_ts_id
    ON intent_planner_windows(planned_ts, id);

CREATE INDEX IF NOT EXISTS idx_intent_planner_windows_branch_id
    ON intent_planner_windows(branch_ref, branch_generation, id);

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
    user_message        TEXT
);

CREATE INDEX IF NOT EXISTS idx_decision_records_ts_id
    ON decision_records(decision_ts, id);

CREATE INDEX IF NOT EXISTS idx_decision_records_path_id
    ON decision_records(path, id);

CREATE INDEX IF NOT EXISTS idx_decision_records_event_seq_id
    ON decision_records(event_seq, id);

CREATE INDEX IF NOT EXISTS idx_decision_records_commit_oid_id
    ON decision_records(commit_oid, id);

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
