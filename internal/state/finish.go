package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

const (
	FinishVersion        = 2
	FinishRunning        = "running"
	FinishPrepared       = "prepared"
	FinishCompleted      = "completed"
	FinishFailed         = "failed"
	FinishNeedsAttention = "needs_attention"
	FinishJSONByteCap    = 8 << 20
)

var (
	ErrFinishNotFound = errors.New("state: finish request not found")
	ErrFinishBusy     = errors.New("state: another finish request owns this worktree")
	ErrFinishIdentity = errors.New("state: finish target identity mismatch")
	ErrFinishState    = errors.New("state: finish state or attempt changed")
	ErrFinishBudget   = errors.New("state: finish planning budget exhausted")
)

// FinishTarget is immutable across attempts. TargetRef pins the complete finish tree;
// the source checkpoint remains the proof of the original worktree bytes.
type FinishTarget struct {
	Version         int    `json:"version"`
	RepositoryID    string `json:"repository_id"`
	WorktreeID      string `json:"worktree_id"`
	BranchRef       string `json:"branch_ref"`
	ExpectedHead    string `json:"expected_head"`
	CheckpointID    string `json:"checkpoint_id"`
	CheckpointTree  string `json:"checkpoint_tree"`
	TargetTree      string `json:"target_tree"`
	TargetRef       string `json:"target_ref,omitempty"`
	TargetCommitOID string `json:"target_commit_oid,omitempty"`
	IndexDigest     string `json:"index_digest"`
	IncludeStaged   bool   `json:"include_staged"`
	Mode            string `json:"mode"`
	Style           string `json:"style"`
	ConfigDigest    string `json:"config_digest"`
	Purpose         string `json:"purpose,omitempty"`
	Message         string `json:"message,omitempty"`

	Checkpoints []FinishCheckpoint `json:"checkpoints,omitempty"`
}

// FinishCheckpoint pins an exact recorded version independently of checkpoint
// retention. The private alias and identity stay fixed across explicit retries.
type FinishCheckpoint struct {
	ID        string `json:"id"`
	Ref       string `json:"ref"`
	CommitOID string `json:"commit_oid"`
	TreeOID   string `json:"tree_oid"`
}

const FinishCheckpointLimit = 256

// FinishRequest stores one attempt, not a background retry queue. Loading an ID
// returns its latest attempt; older attempts retain their original evidence.
type FinishRequest struct {
	DryRun        bool            `json:"dry_run"`
	ID            string          `json:"request_id"`
	Attempt       int             `json:"attempt"`
	Target        FinishTarget    `json:"target"`
	State         string          `json:"state"`
	Stage         string          `json:"stage"`
	DeadlineTS    float64         `json:"deadline_ts"`
	PlanningCalls int             `json:"planning_calls"`
	PlanID        string          `json:"plan_id,omitempty"`
	ResultID      string          `json:"result_id,omitempty"`
	Plan          json.RawMessage `json:"plan,omitempty"`
	Result        json.RawMessage `json:"result,omitempty"`
	Publication   json.RawMessage `json:"publication,omitempty"`
	ReasonCode    string          `json:"reason_code,omitempty"`
	Reason        string          `json:"reason,omitempty"`
	CreatedTS     float64         `json:"created_ts"`
	UpdatedTS     float64         `json:"updated_ts"`
	SupersededBy  string          `json:"superseded_by,omitempty"`
}

// FinishUpdate only exposes mutable evidence. Attempt and state are the CAS
// identity, so a stale worker cannot overwrite an explicit retry's results.
type FinishUpdate struct {
	State, Stage              string
	Plan, Result, Publication json.RawMessage
	ReasonCode, Reason        string
}

const finishDDL = `
CREATE TABLE IF NOT EXISTS finish_requests(
 request_id TEXT NOT NULL,
 attempt INTEGER NOT NULL CHECK(attempt>0),
 worktree_id TEXT NOT NULL,
 checkpoint_id TEXT NOT NULL REFERENCES checkpoints(id),
 target_json TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('running','prepared','completed','failed','needs_attention')),
 stage TEXT NOT NULL DEFAULT '',
 dry_run INTEGER NOT NULL CHECK(dry_run IN (0,1)),
 deadline_ts REAL NOT NULL CHECK(deadline_ts>0),
 planning_calls INTEGER NOT NULL DEFAULT 0 CHECK(planning_calls BETWEEN 0 AND 2),
 plan_json TEXT NOT NULL DEFAULT '',
 result_json TEXT NOT NULL DEFAULT '',
 publication_json TEXT NOT NULL DEFAULT '',
 reason_code TEXT NOT NULL DEFAULT '',
 reason TEXT NOT NULL DEFAULT '',
 created_ts REAL NOT NULL,
 updated_ts REAL NOT NULL,
 superseded_by TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(request_id,attempt)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_finish_active_worktree
 ON finish_requests(worktree_id) WHERE state IN ('running','prepared','needs_attention');
CREATE INDEX IF NOT EXISTS idx_finish_checkpoint ON finish_requests(checkpoint_id,state);
CREATE TRIGGER IF NOT EXISTS finish_immutable_target BEFORE UPDATE OF
 request_id,attempt,worktree_id,checkpoint_id,target_json,dry_run,deadline_ts,created_ts ON finish_requests
BEGIN SELECT RAISE(ABORT,'immutable finish identity'); END;
CREATE TRIGGER IF NOT EXISTS finish_immutable_delete BEFORE DELETE ON finish_requests
BEGIN SELECT RAISE(ABORT,'finish evidence must be retained'); END;
CREATE TRIGGER IF NOT EXISTS finish_checkpoint_pin_prune BEFORE INSERT ON operation_steps
WHEN NEW.kind='delete_checkpoint_ref' AND EXISTS(
 SELECT 1 FROM finish_requests f WHERE f.checkpoint_id=NEW.proof_id AND f.state<>'completed'
)
BEGIN SELECT RAISE(ABORT,'checkpoint pinned by finish'); END;
CREATE TRIGGER IF NOT EXISTS finish_checkpoint_pin_retained BEFORE UPDATE OF retained ON checkpoints
WHEN NEW.retained=0 AND EXISTS(
 SELECT 1 FROM finish_requests f WHERE f.checkpoint_id=NEW.id AND f.state<>'completed'
)
BEGIN SELECT RAISE(ABORT,'checkpoint pinned by finish'); END;
`

const finishSelect = `SELECT request_id,attempt,target_json,state,stage,dry_run,deadline_ts,
 planning_calls,plan_json,result_json,publication_json,reason_code,reason,
 created_ts,updated_ts,superseded_by FROM finish_requests`

// Every finish boundary is FULL-synchronous before external provider/Git work.
func beginFinishWrite(ctx context.Context, db *DB) (*sql.Tx, func(), error) {
	if db == nil {
		return nil, nil, errors.New("state: finish requires a database")
	}
	conn, err := db.conn.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { _, _ = conn.ExecContext(context.Background(), "PRAGMA synchronous=NORMAL"); _ = conn.Close() }
	if _, err := conn.ExecContext(ctx, "PRAGMA synchronous=FULL"); err != nil {
		cleanup()
		return nil, nil, err
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE finish_requests SET updated_ts=updated_ts WHERE 0"); err != nil {
		_ = tx.Rollback()
		cleanup()
		return nil, nil, err
	}
	return tx, func() { _ = tx.Rollback(); cleanup() }, nil
}

func validFinishDeadline(deadline float64) bool {
	return !math.IsNaN(deadline) && !math.IsInf(deadline, 0) && deadline > nowSeconds()
}

func finishTargetJSON(target *FinishTarget) (string, error) {
	if target.Version == 0 {
		target.Version = FinishVersion
	}
	if (target.Version != 1 && target.Version != FinishVersion) || (target.Version == 1 && len(target.Checkpoints) > 0) || target.RepositoryID == "" || target.WorktreeID == "" ||
		!strings.HasPrefix(target.BranchRef, "refs/heads/") || target.CheckpointID == "" ||
		target.CheckpointTree == "" || target.TargetTree == "" || target.IndexDigest == "" ||
		target.ConfigDigest == "" || (target.Mode != "local" && target.Mode != "intent") ||
		(target.Style != "imperative" && target.Style != "conventional") {
		return "", ErrFinishIdentity
	}
	if target.TargetTree != target.CheckpointTree &&
		(target.TargetRef == "" || target.TargetCommitOID == "") {
		return "", ErrFinishIdentity
	}
	if target.TargetRef == "" && target.TargetCommitOID != "" {
		return "", ErrFinishIdentity
	}
	if target.TargetRef != "" && (!strings.HasPrefix(target.TargetRef, "refs/acd/") || target.TargetCommitOID == "") {
		return "", ErrFinishIdentity
	}
	if len(target.Checkpoints) > FinishCheckpointLimit || (len(target.Checkpoints) > 0 && (target.Mode != "intent" || target.Message != "" || target.IncludeStaged)) {
		return "", ErrFinishIdentity
	}
	seen := make(map[string]bool)
	for _, cp := range target.Checkpoints {
		if cp.ID == "" || cp.TreeOID == "" || cp.CommitOID == "" || seen[cp.ID] ||
			!strings.HasPrefix(cp.Ref, "refs/acd/finish/"+target.WorktreeID+"/") {
			return "", ErrFinishIdentity
		}
		seen[cp.ID] = true
	}
	raw, err := json.Marshal(target)
	if err != nil {
		return "", err
	}
	if len(raw) > FinishJSONByteCap {
		return "", ErrFinishIdentity
	}
	return string(raw), nil
}

func scanFinish(row interface{ Scan(...any) error }) (FinishRequest, error) {
	var r FinishRequest
	var target, plan, result, publication string
	err := row.Scan(&r.ID, &r.Attempt, &target, &r.State, &r.Stage, &r.DryRun, &r.DeadlineTS,
		&r.PlanningCalls, &plan, &result, &publication, &r.ReasonCode, &r.Reason,
		&r.CreatedTS, &r.UpdatedTS, &r.SupersededBy)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrFinishNotFound
	}
	if err != nil {
		return r, err
	}
	if err = json.Unmarshal([]byte(target), &r.Target); err != nil {
		return r, err
	}
	r.Plan = json.RawMessage(plan)
	r.Result = json.RawMessage(result)
	r.Publication = json.RawMessage(publication)
	if len(r.Plan) > 0 {
		r.PlanID = fmt.Sprintf("sha256:%x", sha256.Sum256(r.Plan))
	}
	if len(r.Result) > 0 {
		r.ResultID = fmt.Sprintf("sha256:%x", sha256.Sum256(r.Result))
	}
	return r, nil
}

type finishReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadFinish(ctx context.Context, q finishReader, id string) (FinishRequest, error) {
	return scanFinish(q.QueryRowContext(ctx, finishSelect+` WHERE request_id=? ORDER BY attempt DESC LIMIT 1`, id))
}

func finishReadVersion(ctx context.Context, db *DB) (int, error) {
	var version int
	err := db.readSQL().QueryRowContext(ctx, "PRAGMA user_version").Scan(&version)
	return version, err
}

func LoadFinishRequest(ctx context.Context, db *DB, id string) (FinishRequest, error) {
	if version, err := finishReadVersion(ctx, db); err != nil {
		return FinishRequest{}, err
	} else if version < 31 {
		return FinishRequest{}, ErrSetupRequired
	}
	return loadFinish(ctx, db.readSQL(), id)
}

func activeFinish(ctx context.Context, q finishReader, worktree string) (FinishRequest, error) {
	return scanFinish(q.QueryRowContext(ctx, finishSelect+` WHERE worktree_id=? AND state IN ('running','prepared','needs_attention')`, worktree))
}

// ReadActiveFinishRequest inspects existing evidence without opening a writer.
func ReadActiveFinishRequest(ctx context.Context, conn *sql.DB, worktree string) (FinishRequest, error) {
	return activeFinish(ctx, conn, worktree)
}

func ActiveFinishRequest(ctx context.Context, db *DB, worktree string) (FinishRequest, error) {
	if version, err := finishReadVersion(ctx, db); err != nil {
		return FinishRequest{}, err
	} else if version < 31 {
		return FinishRequest{}, ErrSetupRequired
	}
	return activeFinish(ctx, db.readSQL(), worktree)
}

// LatestFinishRequest reports terminal failures as well as active work.
func LatestFinishRequest(ctx context.Context, db *DB, worktree string) (FinishRequest, error) {
	if version, err := finishReadVersion(ctx, db); err != nil {
		return FinishRequest{}, err
	} else if version < 31 {
		return FinishRequest{}, ErrSetupRequired
	}
	return scanFinish(db.readSQL().QueryRowContext(ctx, finishSelect+` WHERE worktree_id=? ORDER BY created_ts DESC,request_id,attempt DESC LIMIT 1`, worktree))
}

func validateFinishCheckpoint(ctx context.Context, tx *sql.Tx, target FinishTarget) error {
	var worktree, tree, phase, branch, head string
	var retained, complete bool
	err := tx.QueryRowContext(ctx, `SELECT worktree_id,tree_oid,phase,retained,coverage_complete,observed_ref,observed_head FROM checkpoints WHERE id=?`, target.CheckpointID).Scan(&worktree, &tree, &phase, &retained, &complete, &branch, &head)
	if err != nil {
		return err
	}
	if worktree != target.WorktreeID || tree != target.CheckpointTree || phase != CheckpointCompleted || !retained || !complete || branch != target.BranchRef || head != target.ExpectedHead {
		return ErrFinishIdentity
	}
	var pruning int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operations o JOIN operation_steps s ON s.operation_id=o.id WHERE o.kind='checkpoint_gc' AND o.status IN ('prepared','needs_action') AND s.proof_id=?`, target.CheckpointID).Scan(&pruning); err != nil {
		return err
	}
	if pruning > 0 {
		return ErrFinishIdentity
	}
	return nil
}

func validateFinishCheckpointVersions(ctx context.Context, tx *sql.Tx, target FinishTarget) error {
	var lastSeq int64
	for _, cp := range target.Checkpoints {
		var seq int64
		err := tx.QueryRowContext(ctx, `SELECT seq FROM checkpoints WHERE id=? AND worktree_id=? AND tree_oid=? AND commit_oid=?
 AND observed_ref=? AND observed_head=? AND phase='completed' AND retained=1 AND coverage_complete=1
 AND seq<(SELECT seq FROM checkpoints WHERE id=?)`, cp.ID, target.WorktreeID, cp.TreeOID, cp.CommitOID,
			target.BranchRef, target.ExpectedHead, target.CheckpointID).Scan(&seq)
		if err != nil || seq <= lastSeq {
			return ErrFinishIdentity
		}
		lastSeq = seq
	}
	return nil
}

func insertFinish(ctx context.Context, tx *sql.Tx, r FinishRequest, targetJSON string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO finish_requests(request_id,attempt,worktree_id,checkpoint_id,target_json,state,stage,dry_run,deadline_ts,created_ts,updated_ts)
 VALUES(?,?,?,?,?,'running','preparing',?,?,?,?)`, r.ID, r.Attempt, r.Target.WorktreeID, r.Target.CheckpointID, targetJSON, r.DryRun, r.DeadlineTS, r.CreatedTS, r.UpdatedTS)
	return err
}

// StartFinishRequest reconnects identical active work. A fresh request may
// supersede only a failed untouched attempt; prepared/ambiguous work owns its lane.
func StartFinishRequest(ctx context.Context, db *DB, r FinishRequest) (FinishRequest, bool, error) {
	targetJSON, err := finishTargetJSON(&r.Target)
	if err != nil || r.ID == "" {
		return FinishRequest{}, false, ErrFinishIdentity
	}
	tx, cleanup, err := beginFinishWrite(ctx, db)
	if err != nil {
		return FinishRequest{}, false, err
	}
	defer cleanup()
	active, err := activeFinish(ctx, tx, r.Target.WorktreeID)
	if err == nil {
		activeJSON, _ := finishTargetJSON(&active.Target)
		if activeJSON != targetJSON || active.DryRun != r.DryRun {
			return active, false, ErrFinishBusy
		}
		return active, false, nil
	}
	if !errors.Is(err, ErrFinishNotFound) {
		return FinishRequest{}, false, err
	}
	if existing, err := loadFinish(ctx, tx, r.ID); err == nil {
		return existing, false, ErrFinishState
	} else if !errors.Is(err, ErrFinishNotFound) {
		return FinishRequest{}, false, err
	}
	if !validFinishDeadline(r.DeadlineTS) {
		return FinishRequest{}, false, ErrFinishBudget
	}
	if err := validateFinishCheckpoint(ctx, tx, r.Target); err != nil {
		return FinishRequest{}, false, err
	}
	if err := validateFinishCheckpointVersions(ctx, tx, r.Target); err != nil {
		return FinishRequest{}, false, err
	}
	r.Attempt = 1
	r.State = FinishRunning
	r.Stage = "preparing"
	r.CreatedTS = nowSeconds()
	r.UpdatedTS = r.CreatedTS
	if err := insertFinish(ctx, tx, r, targetJSON); err != nil {
		return FinishRequest{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE finish_requests SET superseded_by=?,updated_ts=? WHERE worktree_id=? AND superseded_by='' AND publication_json='' AND
 (state='failed' OR (state='completed' AND dry_run=1 AND attempt>1))`, r.ID, r.UpdatedTS, r.Target.WorktreeID); err != nil {
		return FinishRequest{}, false, err
	}
	// Load before committing: a client deadline after durable acceptance must
	// not turn an accepted request into an error and strand its worker job.
	saved, err := loadFinish(ctx, tx, r.ID)
	if err != nil {
		return FinishRequest{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return FinishRequest{}, false, err
	}
	return saved, true, nil
}

func RetryFinishRequest(ctx context.Context, db *DB, id string, deadline float64, dryRun bool) (FinishRequest, error) {
	tx, cleanup, err := beginFinishWrite(ctx, db)
	if err != nil {
		return FinishRequest{}, err
	}
	defer cleanup()
	r, err := loadFinish(ctx, tx, id)
	if err != nil {
		return r, err
	}
	if r.State == FinishRunning || r.State == FinishPrepared {
		if r.DryRun != dryRun {
			return r, ErrFinishBusy
		}
		return r, nil
	}
	if (r.State != FinishFailed && !(r.Attempt > 1 && r.State == FinishCompleted && r.DryRun)) || r.SupersededBy != "" || len(r.Publication) > 0 {
		return r, ErrFinishState
	}
	if _, err := activeFinish(ctx, tx, r.Target.WorktreeID); err == nil {
		return r, ErrFinishBusy
	} else if !errors.Is(err, ErrFinishNotFound) {
		return r, err
	}
	if !validFinishDeadline(deadline) {
		return r, ErrFinishBudget
	}
	if err := validateFinishCheckpoint(ctx, tx, r.Target); err != nil {
		return r, err
	}
	targetJSON, err := finishTargetJSON(&r.Target)
	if err != nil {
		return r, err
	}
	r.Attempt++
	r.DryRun = dryRun
	r.DeadlineTS = deadline
	r.CreatedTS = nowSeconds()
	r.UpdatedTS = r.CreatedTS
	if err := insertFinish(ctx, tx, r, targetJSON); err != nil {
		return r, err
	}
	saved, err := loadFinish(ctx, tx, id)
	if err != nil {
		return r, err
	}
	if err := tx.Commit(); err != nil {
		return r, err
	}
	return saved, nil
}

func validFinishTransition(from, to string) bool {
	if from == to {
		return from == FinishRunning || from == FinishPrepared || from == FinishNeedsAttention
	}
	switch from {
	case FinishRunning:
		return to == FinishPrepared || to == FinishCompleted || to == FinishFailed
	case FinishPrepared:
		return to == FinishCompleted || to == FinishNeedsAttention
	case FinishNeedsAttention:
		return to == FinishCompleted
	}
	return false
}

func validateFinishJSON(raw json.RawMessage) error {
	if len(raw) > FinishJSONByteCap || (len(raw) > 0 && !json.Valid(raw)) {
		return fmt.Errorf("state: invalid or oversized finish evidence")
	}
	return nil
}

func UpdateFinishRequest(ctx context.Context, db *DB, id string, attempt int, expectedState string, u FinishUpdate) (FinishRequest, error) {
	tx, cleanup, err := beginFinishWrite(ctx, db)
	if err != nil {
		return FinishRequest{}, err
	}
	defer cleanup()
	r, err := loadFinish(ctx, tx, id)
	if err != nil {
		return r, err
	}
	if r.Attempt != attempt || r.State != expectedState || !validFinishTransition(r.State, u.State) {
		return r, ErrFinishState
	}
	if len(u.Stage) > 64 || len(u.ReasonCode) > 64 || len(u.Reason) > 64*1024 {
		return r, ErrFinishIdentity
	}
	for _, raw := range []json.RawMessage{u.Plan, u.Result, u.Publication} {
		if err := validateFinishJSON(raw); err != nil {
			return r, err
		}
	}
	if u.Plan != nil {
		if len(r.Plan) > 0 && string(r.Plan) != string(u.Plan) {
			return r, ErrFinishIdentity
		}
		r.Plan = u.Plan
	}
	if u.Publication != nil {
		if u.State != FinishPrepared && r.State != FinishPrepared && r.State != FinishNeedsAttention {
			return r, ErrFinishState
		}
		if len(r.Publication) > 0 && string(r.Publication) != string(u.Publication) {
			return r, ErrFinishIdentity
		}
		r.Publication = u.Publication
	}
	if u.Result != nil {
		r.Result = u.Result
	}
	if u.State == FinishPrepared && (len(r.Plan) == 0 || len(r.Publication) == 0 || r.DryRun) {
		return r, ErrFinishIdentity
	}
	if u.State == FinishCompleted && r.State == FinishRunning && !r.DryRun {
		var result struct {
			NoChanges bool `json:"no_changes"`
		}
		if len(r.Result) == 0 || json.Unmarshal(r.Result, &result) != nil || !result.NoChanges {
			return r, ErrFinishState
		}
	}
	if u.State == FinishFailed && len(r.Publication) > 0 {
		return r, ErrFinishState
	}
	if (u.State == FinishFailed || u.State == FinishNeedsAttention) && u.ReasonCode == "" {
		return r, ErrFinishState
	}
	_, err = tx.ExecContext(ctx, `UPDATE finish_requests SET state=?,stage=?,plan_json=?,result_json=?,publication_json=?,reason_code=?,reason=?,updated_ts=? WHERE request_id=? AND attempt=?`,
		u.State, u.Stage, string(r.Plan), string(r.Result), string(r.Publication), u.ReasonCode, u.Reason, nowSeconds(), id, attempt)
	if err != nil {
		return r, err
	}
	if err = tx.Commit(); err != nil {
		return r, err
	}
	return LoadFinishRequest(ctx, db, id)
}

// ConsumeFinishPlanningCall durably reserves a call before provider execution.
// Disconnect or restart cannot reset the two-call budget or the deadline.
func ConsumeFinishPlanningCall(ctx context.Context, db *DB, id string, attempt int) (int, error) {
	tx, cleanup, err := beginFinishWrite(ctx, db)
	if err != nil {
		return 0, err
	}
	defer cleanup()
	result, err := tx.ExecContext(ctx, `UPDATE finish_requests SET planning_calls=planning_calls+1,updated_ts=? WHERE request_id=? AND attempt=? AND state='running' AND planning_calls<2 AND deadline_ts>?`, nowSeconds(), id, attempt, nowSeconds())
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if n != 1 {
		return 0, ErrFinishBudget
	}
	var calls int
	if err := tx.QueryRowContext(ctx, `SELECT planning_calls FROM finish_requests WHERE request_id=? AND attempt=?`, id, attempt).Scan(&calls); err != nil {
		return 0, err
	}
	return calls, tx.Commit()
}

// InterruptFinishRequests is called by the owning worker on restart only.
// Prepared attempts remain journal consumers; protection never retries AI.
func InterruptFinishRequests(ctx context.Context, db *DB, worktree string) (int64, error) {
	tx, cleanup, err := beginFinishWrite(ctx, db)
	if err != nil {
		return 0, err
	}
	defer cleanup()
	result, err := tx.ExecContext(ctx, `UPDATE finish_requests SET state='failed',stage='',reason_code='finish_interrupted',reason='Worker restarted before publication; retry explicitly',updated_ts=? WHERE worktree_id=? AND state='running' AND publication_json=''`, nowSeconds(), worktree)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, tx.Commit()
}

// FinishObjectPins describes private refs which must outlive failed or active
// requests. The caller proves/refuses Git movement; SQLite never creates refs.
type FinishObjectPin struct {
	RequestID      string
	Attempt        int
	Ref, CommitOID string
}

func FinishObjectPins(ctx context.Context, db *DB, worktree string) ([]FinishObjectPin, error) {
	if version, err := finishReadVersion(ctx, db); err != nil {
		return nil, err
	} else if version < 31 {
		return nil, ErrSetupRequired
	}
	rows, err := db.readSQL().QueryContext(ctx, finishSelect+` WHERE worktree_id=? AND state<>'completed'`, worktree)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pins []FinishObjectPin
	for rows.Next() {
		r, err := scanFinish(rows)
		if err != nil {
			return nil, err
		}
		if r.Target.TargetRef != "" {
			pins = append(pins, FinishObjectPin{r.ID, r.Attempt, r.Target.TargetRef, r.Target.TargetCommitOID})
		}
		for _, cp := range r.Target.Checkpoints {
			pins = append(pins, FinishObjectPin{r.ID, r.Attempt, cp.Ref, cp.CommitOID})
		}
		if len(r.Publication) > 0 {
			var pin struct {
				Ref string `json:"object_ref"`
				OID string `json:"object_oid"`
			}
			if err := json.Unmarshal(r.Publication, &pin); err != nil {
				return nil, err
			}
			if pin.Ref != "" {
				pins = append(pins, FinishObjectPin{r.ID, r.Attempt, pin.Ref, pin.OID})
			}
		}
	}
	return pins, rows.Err()
}
