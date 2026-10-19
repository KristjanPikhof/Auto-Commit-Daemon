package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const finishLegacyDDL = `
CREATE TABLE IF NOT EXISTS finish_legacy_cutover(
 id INTEGER PRIMARY KEY CHECK(id=1), worktree_id TEXT NOT NULL,
 manifest_json TEXT NOT NULL, manifest_digest TEXT NOT NULL,
 snapshot_ref TEXT NOT NULL DEFAULT '', snapshot_oid TEXT NOT NULL DEFAULT '',
 state TEXT NOT NULL CHECK(state IN ('pending','completed','needs_attention')),
 reason_code TEXT NOT NULL DEFAULT '', reason TEXT NOT NULL DEFAULT '',
 unpublished INTEGER NOT NULL DEFAULT 0, created_ts REAL NOT NULL, updated_ts REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS finish_legacy_object_pins(
 object_oid TEXT PRIMARY KEY, object_type TEXT NOT NULL CHECK(object_type IN ('blob','commit')),
 pin_ref TEXT NOT NULL, pin_oid TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS finish_legacy_retirements(
 kind TEXT NOT NULL, legacy_id TEXT NOT NULL, previous_state TEXT NOT NULL,
 PRIMARY KEY(kind,legacy_id)
);
CREATE TABLE IF NOT EXISTS finish_legacy_captures(event_seq INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS finish_legacy_activation_failure(
 id INTEGER PRIMARY KEY CHECK(id=1),reason_code TEXT NOT NULL,reason TEXT NOT NULL,
 snapshot_ref TEXT NOT NULL,snapshot_oid TEXT NOT NULL,updated_ts REAL NOT NULL
);
CREATE TRIGGER IF NOT EXISTS finish_legacy_cutover_identity BEFORE UPDATE OF
 worktree_id,manifest_json,manifest_digest,snapshot_ref,snapshot_oid,created_ts ON finish_legacy_cutover
 BEGIN SELECT RAISE(ABORT,'immutable legacy cutover'); END;
CREATE TRIGGER IF NOT EXISTS finish_legacy_cutover_delete BEFORE DELETE ON finish_legacy_cutover
 BEGIN SELECT RAISE(ABORT,'legacy evidence must be retained'); END;
CREATE TRIGGER IF NOT EXISTS finish_legacy_pin_update BEFORE UPDATE ON finish_legacy_object_pins
 BEGIN SELECT RAISE(ABORT,'immutable legacy object pin'); END;
CREATE TRIGGER IF NOT EXISTS finish_legacy_pin_delete BEFORE DELETE ON finish_legacy_object_pins
 BEGIN SELECT RAISE(ABORT,'legacy object pin must be retained'); END;
CREATE TRIGGER IF NOT EXISTS finish_legacy_retirement_update BEFORE UPDATE ON finish_legacy_retirements
 BEGIN SELECT RAISE(ABORT,'immutable legacy retirement'); END;
CREATE TRIGGER IF NOT EXISTS finish_legacy_retirement_delete BEFORE DELETE ON finish_legacy_retirements
 BEGIN SELECT RAISE(ABORT,'legacy retirement must be retained'); END;
CREATE TRIGGER IF NOT EXISTS finish_legacy_capture_pin_delete BEFORE DELETE ON finish_legacy_captures
 BEGIN SELECT RAISE(ABORT,'legacy capture pin must be retained'); END;
`

// These seals apply only to upgraded databases containing original captures.
const finishLegacyCaptureSealsDDL = `
CREATE TRIGGER IF NOT EXISTS finish_legacy_capture_delete BEFORE DELETE ON capture_events
 WHEN EXISTS(SELECT 1 FROM finish_legacy_captures WHERE event_seq=OLD.seq)
 BEGIN SELECT RAISE(ABORT,'legacy capture provenance must be retained'); END;
CREATE TRIGGER IF NOT EXISTS finish_legacy_ops_delete BEFORE DELETE ON capture_ops
 WHEN EXISTS(SELECT 1 FROM finish_legacy_captures WHERE event_seq=OLD.event_seq)
 BEGIN SELECT RAISE(ABORT,'legacy capture provenance must be retained'); END;
CREATE TRIGGER IF NOT EXISTS finish_legacy_ops_update BEFORE UPDATE ON capture_ops
 WHEN EXISTS(SELECT 1 FROM finish_legacy_captures WHERE event_seq=OLD.event_seq)
 BEGIN SELECT RAISE(ABORT,'immutable legacy capture provenance'); END;
`

type FinishLegacyObject struct {
	OID  string `json:"oid"`
	Type string `json:"type"`
}
type FinishLegacyRetirement struct {
	Kind          string `json:"kind"`
	ID            string `json:"id"`
	PreviousState string `json:"previous_state"`
}
type FinishLegacyManifest struct {
	Objects     []FinishLegacyObject     `json:"objects"`
	Retirements []FinishLegacyRetirement `json:"retirements"`
	Captures    []int64                  `json:"captures"`
}
type FinishLegacyPin struct{ ObjectOID, ObjectType, PinRef, PinOID string }
type FinishLegacyCutover struct {
	Available      bool                 `json:"available"`
	WorktreeID     string               `json:"worktree_id,omitempty"`
	State          string               `json:"state,omitempty"`
	ReasonCode     string               `json:"reason_code,omitempty"`
	Reason         string               `json:"reason,omitempty"`
	SnapshotRef    string               `json:"snapshot_ref,omitempty"`
	SnapshotOID    string               `json:"snapshot_oid,omitempty"`
	ManifestDigest string               `json:"manifest_digest,omitempty"`
	RetiredCount   int64                `json:"retired_count"`
	RetiredDrains  int64                `json:"retired_drains"`
	Unpublished    int64                `json:"unpublished"`
	CreatedTS      float64              `json:"created_ts,omitempty"`
	UpdatedTS      float64              `json:"updated_ts,omitempty"`
	Manifest       FinishLegacyManifest `json:"-"`
}

// InventoryFinishLegacy freezes provenance, not a plan to replay old work.
func InventoryFinishLegacy(ctx context.Context, db *DB) (FinishLegacyManifest, error) {
	var m FinishLegacyManifest
	objectQuery, err := legacyUnion(ctx, db.ReadSQL(), []legacySelect{
		{"capture_ops", `SELECT before_oid oid,'blob' type FROM capture_ops WHERE COALESCE(before_mode,'')<>'160000'`},
		{"capture_ops", `SELECT after_oid oid,'blob' type FROM capture_ops WHERE COALESCE(after_mode,'')<>'160000'`},
		{"shadow_paths", `SELECT oid oid,'blob' type FROM shadow_paths WHERE COALESCE(mode,'')<>'160000'`},
		{"capture_events", `SELECT base_head oid,'commit' type FROM capture_events`},
		{"capture_events", `SELECT commit_oid oid,'commit' type FROM capture_events`},
		{"checkpoints", `SELECT commit_oid oid,'commit' type FROM checkpoints WHERE EXISTS(SELECT 1 FROM sqlite_master WHERE name IN ('capture_events','self_publications','intent_repairs'))`},

		{"recovery_snapshots", `SELECT commit_oid oid,'commit' type FROM recovery_snapshots`},
		{"self_publications", `SELECT source_head oid,'commit' type FROM self_publications`},
		{"self_publications", `SELECT target_commit_oid oid,'commit' type FROM self_publications`},
		{"intent_repairs", `SELECT expected_head oid,'commit' type FROM intent_repairs`},
		{"intent_repairs", `SELECT old_head oid,'commit' type FROM intent_repairs`},
		{"intent_repairs", `SELECT new_head oid,'commit' type FROM intent_repairs`},
		{"intent_repair_commit_mappings", `SELECT old_oid oid,'commit' type FROM intent_repair_commit_mappings`},
		{"intent_repair_commit_mappings", `SELECT new_oid oid,'commit' type FROM intent_repair_commit_mappings`},
		{"intent_candidates", `SELECT published_commit_oid oid,'commit' type FROM intent_candidates`},
		{"rewrite_plan_commits", `SELECT old_oid oid,'commit' type FROM rewrite_plan_commits`},
		{"rewrite_plans", `SELECT expected_head oid,'commit' type FROM rewrite_plans`},
	})
	if err != nil {
		return m, err
	}
	if objectQuery == "" {
		objectQuery = "SELECT NULL oid,NULL type WHERE 0"
	}
	rows, err := db.ReadSQL().QueryContext(ctx, "SELECT oid,type FROM ("+objectQuery+") WHERE oid IS NOT NULL AND oid<>'' ORDER BY type,oid")
	if err != nil {
		return m, err
	}
	defer rows.Close()
	for rows.Next() {
		var o FinishLegacyObject
		if err = rows.Scan(&o.OID, &o.Type); err != nil {
			return m, err
		}
		m.Objects = append(m.Objects, o)
	}
	if err = rows.Err(); err != nil {
		return m, err
	}
	rows.Close()
	retirementQuery, err := legacyUnion(ctx, db.ReadSQL(), []legacySelect{
		{"publication_drains", `SELECT 'drain' kind,id,phase state FROM publication_drains WHERE phase<>'completed'`},
		{"planner_state", `SELECT 'planner' kind,CAST(event_seq AS TEXT) id,COALESCE(last_defer_reason,'') state FROM planner_state`},
		{"intent_plan_runs", `SELECT 'plan' kind,fingerprint id,COALESCE(progress_state,'') state FROM intent_plan_runs WHERE completed=0`},
		{"intent_candidates", `SELECT 'candidate' kind,id id,status state FROM intent_candidates WHERE status NOT IN ('published','superseded')`},
		{"flush_requests", `SELECT 'flush' kind,CAST(id AS TEXT) id,status state FROM flush_requests WHERE status IN ('pending','acknowledged')`},
		{"self_publications", `SELECT 'self_publication' kind,id id,phase state FROM self_publications WHERE phase IN ('prepared','git_applied')`},
		{"intent_repairs", `SELECT 'repair' kind,id id,status state FROM intent_repairs WHERE status IN ('prepared','git_applied')`},
	})
	if err != nil {
		return m, err
	}
	if retirementQuery == "" {
		retirementQuery = "SELECT NULL kind,NULL id,NULL state WHERE 0"
	}
	rows, err = db.ReadSQL().QueryContext(ctx, "SELECT kind,id,state FROM ("+retirementQuery+") ORDER BY kind,id")
	if err != nil {
		return m, err
	}
	defer rows.Close()
	for rows.Next() {
		var r FinishLegacyRetirement
		if err = rows.Scan(&r.Kind, &r.ID, &r.PreviousState); err != nil {
			return m, err
		}
		m.Retirements = append(m.Retirements, r)
	}
	if err = rows.Err(); err != nil {
		return m, err
	}
	rows.Close()
	hasCaptures, err := legacyTableExists(ctx, db.ReadSQL(), "capture_events")
	if err != nil {
		return m, err
	}
	if hasCaptures {
		rows, err = db.ReadSQL().QueryContext(ctx, "SELECT seq FROM capture_events ORDER BY seq")
		if err != nil {
			return m, err
		}
		defer rows.Close()
		for rows.Next() {
			var seq int64
			if err = rows.Scan(&seq); err != nil {
				return m, err
			}
			m.Captures = append(m.Captures, seq)
		}
		if err = rows.Err(); err != nil {
			return m, err
		}
		rows.Close()
	}

	groupObjects, inventoryErr := legacyRewriteGroupObjects(ctx, db)
	m.Objects = append(m.Objects, groupObjects...)
	plans, err := SavedIntentHistoryPlans(ctx, db)
	for _, plan := range plans {
		for _, oid := range append(append([]string{plan.ExpectedHead}, plan.SourceChain...), plan.RecreateUnchanged...) {
			m.Objects = append(m.Objects, FinishLegacyObject{OID: oid, Type: "commit"})
		}
	}
	sort.Slice(m.Objects, func(i, j int) bool {
		if m.Objects[i].Type != m.Objects[j].Type {
			return m.Objects[i].Type < m.Objects[j].Type
		}
		return m.Objects[i].OID < m.Objects[j].OID
	})
	unique := m.Objects[:0]
	for _, o := range m.Objects {
		if len(unique) == 0 || unique[len(unique)-1] != o {
			unique = append(unique, o)
		}
	}
	m.Objects = unique
	return m, errors.Join(inventoryErr, err)
}

// Grouped legacy proposals store member objects, not bare OID strings. Decode
// each member independently so a bad member cannot hide valid retained commits.
func legacyRewriteGroupObjects(ctx context.Context, db *DB) ([]FinishLegacyObject, error) {
	exists, err := legacyTableExists(ctx, db.ReadSQL(), "rewrite_plan_groups")
	if err != nil || !exists {
		return nil, err
	}
	rows, err := db.ReadSQL().QueryContext(ctx, "SELECT plan_id,ord,members_json FROM rewrite_plan_groups ORDER BY plan_id,ord")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var objects []FinishLegacyObject
	var invalid error
	for rows.Next() {
		var planID, raw string
		var ord int
		if err := rows.Scan(&planID, &ord, &raw); err != nil {
			return objects, errors.Join(invalid, err)
		}
		var members []json.RawMessage
		if err := json.Unmarshal([]byte(raw), &members); err != nil || len(members) == 0 {
			invalid = errors.Join(invalid, fmt.Errorf("%w: archived rewrite group %s/%d has invalid members", ErrFinishIdentity, planID, ord))
			continue
		}
		for index, rawMember := range members {
			var member RewritePlanMember
			if err := json.Unmarshal(rawMember, &member); err != nil || !validFinishOID(member.OldOID) {
				invalid = errors.Join(invalid, fmt.Errorf("%w: archived rewrite group %s/%d member %d has invalid commit identity", ErrFinishIdentity, planID, ord, index))
				if !validFinishOID(member.OldOID) {
					continue
				}
			}
			objects = append(objects, FinishLegacyObject{OID: member.OldOID, Type: "commit"})
		}
	}
	return objects, errors.Join(invalid, rows.Err())
}

// SavedIntentHistoryPlans preserves independently valid selections even when
// another record is malformed. Cleanup still receives the validation error.
func SavedIntentHistoryPlans(ctx context.Context, db *DB) ([]IntentHistoryPlan, error) {
	rows, err := db.ReadSQL().QueryContext(ctx, "SELECT key,value FROM daemon_meta WHERE key LIKE 'intent.history.plan.%' ORDER BY key")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var plans []IntentHistoryPlan
	var invalid error
	for rows.Next() {
		var key, raw string
		if err = rows.Scan(&key, &raw); err != nil {
			return plans, errors.Join(invalid, err)
		}
		p, err := decodeRetentionHistoryPlan(key, raw)
		if err != nil {
			if invalid == nil {
				invalid = err
			}
			continue
		}
		plans = append(plans, p)
	}
	return plans, errors.Join(invalid, rows.Err())
}

func decodeRetentionHistoryPlan(key, raw string) (IntentHistoryPlan, error) {
	var p IntentHistoryPlan
	if len(raw) > IntentHistoryPlanByteCap || json.Unmarshal([]byte(raw), &p) != nil || (p.Version != 1 && p.Version != IntentHistoryPlanVersion) || p.ID != strings.TrimPrefix(key, "intent.history.plan.") || p.ID == "" {
		return p, fmt.Errorf("state: saved history plan identity is invalid: %s", key)
	}
	for _, oid := range append(append([]string{p.ExpectedHead}, p.SourceChain...), p.RecreateUnchanged...) {
		if !validFinishOID(oid) {
			return p, fmt.Errorf("state: saved history plan commit identity is invalid: %s", key)
		}
	}
	if p.SourceRef != "" && !validFinishRef(p.SourceRef, "refs/acd/") {
		return p, ErrFinishIdentity
	}
	return p, nil
}

func FinishLegacyDigest(m FinishLegacyManifest) string {
	raw, _ := json.Marshal(m)
	return fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
}

func readFinishLegacy(ctx context.Context, conn *sql.DB) (FinishLegacyCutover, error) {
	r := FinishLegacyCutover{Available: true, State: "pending"}
	var raw string
	err := conn.QueryRowContext(ctx, `SELECT worktree_id,manifest_json,manifest_digest,snapshot_ref,snapshot_oid,state,reason_code,reason,unpublished,created_ts,updated_ts FROM finish_legacy_cutover WHERE id=1`).Scan(&r.WorktreeID, &raw, &r.ManifestDigest, &r.SnapshotRef, &r.SnapshotOID, &r.State, &r.ReasonCode, &r.Reason, &r.Unpublished, &r.CreatedTS, &r.UpdatedTS)
	if errors.Is(err, sql.ErrNoRows) {
		failureErr := conn.QueryRowContext(ctx, `SELECT reason_code,reason,snapshot_ref,snapshot_oid,updated_ts FROM finish_legacy_activation_failure WHERE id=1`).Scan(&r.ReasonCode, &r.Reason, &r.SnapshotRef, &r.SnapshotOID, &r.UpdatedTS)
		if failureErr == nil {
			r.State = "needs_attention"
			return r, nil
		}
		if !errors.Is(failureErr, sql.ErrNoRows) {
			return r, failureErr
		}
		query, err := legacyUnion(ctx, conn, []legacySelect{
			{"capture_events", "SELECT COUNT(*) n FROM capture_events WHERE state<>'published'"},
			{"self_publications", "SELECT COUNT(*) n FROM self_publications WHERE phase IN ('prepared','git_applied')"},
			{"intent_repairs", "SELECT COUNT(*) n FROM intent_repairs WHERE status IN ('prepared','git_applied')"},
			{"publication_drains", "SELECT COUNT(*) n FROM publication_drains WHERE phase<>'completed'"},
		})
		if err != nil {
			return r, err
		}
		var unresolved int64
		if query != "" {
			if err = conn.QueryRowContext(ctx, "SELECT SUM(n) FROM ("+query+")").Scan(&unresolved); err != nil {
				return r, err
			}
		}

		if unresolved > 0 {
			r.State = "needs_attention"
			r.ReasonCode = "legacy_activation_required"
			r.Reason = "Legacy archive is retained. Setup can verify its available objects independently of current work."
		}
		return r, nil
	}
	if err != nil {
		return r, err
	}
	if err = json.Unmarshal([]byte(raw), &r.Manifest); err != nil {
		return r, err
	}
	if FinishLegacyDigest(r.Manifest) != r.ManifestDigest {
		return r, ErrFinishIdentity
	}
	for _, v := range r.Manifest.Retirements {
		r.RetiredCount++
		if v.Kind == "drain" {
			r.RetiredDrains++
		}
	}
	return r, nil
}

// ReadFinishLegacyCutover does not create, upgrade, or settle state.
func ReadFinishLegacyCutover(ctx context.Context, path string) (FinishLegacyCutover, error) {
	version, err := ReadUserVersion(ctx, path)
	if err != nil {
		return FinishLegacyCutover{}, err
	}
	if version < 32 {
		return FinishLegacyCutover{}, nil
	}
	if version > SchemaVersion {
		return FinishLegacyCutover{}, ErrSetupRequired
	}
	db, err := OpenReadOnly(ctx, path)
	if err != nil {
		return FinishLegacyCutover{}, err
	}
	defer db.Close()
	return readFinishLegacy(ctx, db.ReadSQL())
}
func LoadFinishLegacyCutover(ctx context.Context, db *DB) (FinishLegacyCutover, error) {
	return readFinishLegacy(ctx, db.ReadSQL())
}

// RecordFinishLegacyActivationFailure preserves a failed activation's available
// object proof before there is enough evidence to prepare the cutover.
func RecordFinishLegacyActivationFailure(ctx context.Context, db *DB, code, reason, ref, oid string) (FinishLegacyCutover, error) {
	if code == "" || (ref != "" && (!validFinishRef(ref, "refs/acd/migration/finish-v1/") || !validFinishOID(oid))) || (ref == "" && oid != "") {
		return FinishLegacyCutover{}, ErrFinishIdentity
	}
	tx, cleanup, err := beginFinishWrite(ctx, db)
	if err != nil {
		return FinishLegacyCutover{}, err
	}
	defer cleanup()
	_, err = tx.ExecContext(ctx, `INSERT INTO finish_legacy_activation_failure VALUES(1,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET reason_code=excluded.reason_code,reason=excluded.reason,snapshot_ref=excluded.snapshot_ref,snapshot_oid=excluded.snapshot_oid,updated_ts=excluded.updated_ts`, code, reason, ref, oid, nowSeconds())
	if err != nil {
		return FinishLegacyCutover{}, err
	}
	hasCaptures, err := legacyTableExists(ctx, tx, "capture_events")
	if err != nil {
		return FinishLegacyCutover{}, err
	}
	if hasCaptures {
		if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO finish_legacy_captures SELECT seq FROM capture_events"); err != nil {
			return FinishLegacyCutover{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return FinishLegacyCutover{}, err
	}
	return LoadFinishLegacyCutover(ctx, db)
}

// PrepareFinishLegacyCutover is durable before any legacy journal settlement.
func PrepareFinishLegacyCutover(ctx context.Context, db *DB, worktree string, m FinishLegacyManifest, ref, oid string, pins []FinishLegacyPin) (FinishLegacyCutover, error) {
	if !validHexID(worktree, 16) || !validFinishOID(oid) || !validFinishRef(ref, "refs/acd/migration/finish-v1/"+worktree+"/") || len(pins) != len(m.Objects) {
		return FinishLegacyCutover{}, ErrFinishIdentity
	}
	objects := map[string]string{}
	for _, o := range m.Objects {
		if !validFinishOID(o.OID) || (o.Type != "blob" && o.Type != "commit") || objects[o.OID] != "" {
			return FinishLegacyCutover{}, ErrFinishIdentity
		}
		objects[o.OID] = o.Type
	}
	for _, p := range pins {
		if objects[p.ObjectOID] != p.ObjectType || !validFinishOID(p.PinOID) || !validFinishRef(p.PinRef, "refs/acd/migration/finish-v1/"+worktree+"/") {
			return FinishLegacyCutover{}, ErrFinishIdentity
		}
		if p.ObjectType == "blob" && (p.PinRef != ref || p.PinOID != oid) {
			return FinishLegacyCutover{}, ErrFinishIdentity
		}
		if p.ObjectType == "commit" && p.PinOID != p.ObjectOID {
			return FinishLegacyCutover{}, ErrFinishIdentity
		}
		delete(objects, p.ObjectOID)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return FinishLegacyCutover{}, err
	}
	tx, cleanup, err := beginFinishWrite(ctx, db)
	if err != nil {
		return FinishLegacyCutover{}, err
	}
	defer cleanup()
	var digest, savedWorktree, savedRef, savedOID string
	err = tx.QueryRowContext(ctx, "SELECT manifest_digest,worktree_id,snapshot_ref,snapshot_oid FROM finish_legacy_cutover WHERE id=1").Scan(&digest, &savedWorktree, &savedRef, &savedOID)
	if err == nil {
		if digest != FinishLegacyDigest(m) || savedWorktree != worktree || savedRef != ref || savedOID != oid {
			return FinishLegacyCutover{}, ErrFinishIdentity
		}
		return LoadFinishLegacyCutover(ctx, db)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return FinishLegacyCutover{}, err
	}
	now := nowSeconds()
	if _, err = tx.ExecContext(ctx, `INSERT INTO finish_legacy_cutover(id,worktree_id,manifest_json,manifest_digest,snapshot_ref,snapshot_oid,state,created_ts,updated_ts) VALUES(1,?,?,?,?,?,'pending',?,?)`, worktree, string(raw), FinishLegacyDigest(m), ref, oid, now, now); err != nil {
		return FinishLegacyCutover{}, err
	}
	for _, p := range pins {
		if _, err = tx.ExecContext(ctx, "INSERT INTO finish_legacy_object_pins VALUES(?,?,?,?)", p.ObjectOID, p.ObjectType, p.PinRef, p.PinOID); err != nil {
			return FinishLegacyCutover{}, err
		}
	}
	for _, r := range m.Retirements {
		if _, err = tx.ExecContext(ctx, "INSERT INTO finish_legacy_retirements VALUES(?,?,?)", r.Kind, r.ID, r.PreviousState); err != nil {
			return FinishLegacyCutover{}, err
		}
	}
	for _, seq := range m.Captures {
		if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO finish_legacy_captures VALUES(?)", seq); err != nil {
			return FinishLegacyCutover{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return FinishLegacyCutover{}, err
	}
	return LoadFinishLegacyCutover(ctx, db)
}

func CompleteFinishLegacyCutover(ctx context.Context, db *DB, reasonCode, reason string) (FinishLegacyCutover, error) {
	tx, cleanup, err := beginFinishWrite(ctx, db)
	if err != nil {
		return FinishLegacyCutover{}, err
	}
	defer cleanup()
	phase := "completed"
	if reasonCode != "" {
		phase = "needs_attention"
	}
	unpublished, err := legacyCaptureCount(ctx, tx, "WHERE state<>'published'")
	if err != nil {
		return FinishLegacyCutover{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE finish_legacy_cutover SET state=?,reason_code=?,reason=?,unpublished=?,updated_ts=? WHERE id=1`, phase, reasonCode, reason, unpublished, nowSeconds())
	if err != nil {
		return FinishLegacyCutover{}, err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return FinishLegacyCutover{}, ErrFinishNotFound
	}
	if err = tx.Commit(); err != nil {
		return FinishLegacyCutover{}, err
	}
	return LoadFinishLegacyCutover(ctx, db)
}

func FinishLegacyObjectPins(ctx context.Context, db *DB) ([]FinishLegacyPin, error) {
	rows, err := db.ReadSQL().QueryContext(ctx, "SELECT object_oid,object_type,pin_ref,pin_oid FROM finish_legacy_object_pins ORDER BY object_oid")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pins []FinishLegacyPin
	for rows.Next() {
		var p FinishLegacyPin
		if err = rows.Scan(&p.ObjectOID, &p.ObjectType, &p.PinRef, &p.PinOID); err != nil {
			return nil, err
		}
		pins = append(pins, p)
	}
	return pins, rows.Err()
}
