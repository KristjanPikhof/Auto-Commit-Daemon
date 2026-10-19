// Package state owns the per-repo SQLite layer (open, migrate, CRUD).
//
// The DDL below is the canonical state contract. The daily_rollups table
// is the long-term backward-compat anchor
// (D9): future migrations may only ALTER TABLE ADD COLUMN — never rename,
// remove, or reorder.
package state

// SchemaVersion v33 initializes only current protection and explicit requests.
// Supported older databases retain their immutable evidence; see schema_legacy.go.
const SchemaVersion = 33

// schemaDDL contains only current protection, configuration and explicit requests.
const schemaDDL = `CREATE TABLE IF NOT EXISTS daemon_state(
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
    coverage_complete   INTEGER NOT NULL DEFAULT 1 CHECK (coverage_complete IN (0,1)),
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
    FOREIGN KEY (checkpoint_id) REFERENCES checkpoints(id) ON DELETE CASCADE
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
` + checkpointCaptureIssuesDDL + finishDDL + finishLegacyDDL + finishHistoryDDL

const checkpointCaptureIssuesDDL = `
CREATE TABLE IF NOT EXISTS checkpoint_capture_issues(
    checkpoint_id TEXT NOT NULL,
    path TEXT NOT NULL,
    subtree INTEGER NOT NULL CHECK (subtree IN (0,1)),
    reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 64),
    PRIMARY KEY (checkpoint_id, path),
    FOREIGN KEY (checkpoint_id) REFERENCES checkpoints(id) ON DELETE CASCADE
);`
