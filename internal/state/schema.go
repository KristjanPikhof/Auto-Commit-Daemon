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
// than a foreign key cleared by capture_events pruning.
const SchemaVersion = 6

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

-- v6: decision_records.event_seq is a durable historical cursor. Rebuild the
-- table when bootstrapping older databases so v5's ON DELETE SET NULL foreign
-- key cannot erase the event identity when capture_events rows are pruned.
CREATE TABLE IF NOT EXISTS decision_records_v6(
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

CREATE INDEX IF NOT EXISTS idx_decision_records_ts_id
    ON decision_records(decision_ts, id);

CREATE INDEX IF NOT EXISTS idx_decision_records_path_id
    ON decision_records(path, id);

CREATE INDEX IF NOT EXISTS idx_decision_records_event_seq_id
    ON decision_records(event_seq, id);

CREATE INDEX IF NOT EXISTS idx_decision_records_commit_oid_id
    ON decision_records(commit_oid, id);

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
