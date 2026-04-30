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
// long-running pauses fan capture_events into tens of thousands of rows.
const SchemaVersion = 3

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
