package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

const IntentHistoryPlanVersion = 1
const IntentHistoryPlanByteCap = 8 << 20
const MetaKeyIntentHistoryRequest = "intent.history.request"
const MetaKeyIntentHistoryWorker = "intent.history.worker"

type IntentHistoryWorker struct {
	PID         int    `json:"pid"`
	Fingerprint string `json:"fingerprint"`
	Protocol    int    `json:"protocol"`
}

// IntentHistoryVersion identifies an existing Git path version. Plans retain
// object IDs and lineage, never source contents or provider responses.
type IntentHistoryVersion struct {
	OID  string `json:"oid,omitempty"`
	Mode string `json:"mode,omitempty"`
}

type IntentHistoryUnit struct {
	OldOID string               `json:"old_oid"`
	Path   string               `json:"path"`
	Before IntentHistoryVersion `json:"before"`
	After  IntentHistoryVersion `json:"after"`
}

type IntentHistoryGoal struct {
	ID      string `json:"id"`
	Purpose string `json:"purpose"`
	Message string `json:"message"`
	Reason  string `json:"reason"`
	Units   []int  `json:"units"`
	TreeOID string `json:"tree_oid"`
}

// IntentHistoryPlan freezes both original provenance and proposed goal trees.
// Explicit reconstruction creates a new branch and leaves the source untouched.
type IntentHistoryPlan struct {
	Version         int                 `json:"version"`
	ID              string              `json:"id"`
	SourceBranchRef string              `json:"source_branch_ref"`
	TargetBranchRef string              `json:"target_branch_ref"`
	ExpectedHead    string              `json:"expected_head"`
	CommitFormat    string              `json:"commit_format"`
	BaseTree        string              `json:"base_tree"`
	SourceChain     []string            `json:"source_chain"`
	Units           []IntentHistoryUnit `json:"units"`
	Goals           []IntentHistoryGoal `json:"goals"`
}

type IntentHistoryRequest struct {
	PlanID    string  `json:"plan_id"`
	Status    string  `json:"status"`
	Error     string  `json:"error,omitempty"`
	NewHead   string  `json:"new_head,omitempty"`
	BackupRef string  `json:"backup_ref,omitempty"`
	UpdatedTS float64 `json:"updated_ts"`
}

func SaveIntentHistoryPlan(ctx context.Context, db *DB, plan IntentHistoryPlan) (IntentHistoryPlan, error) {
	if plan.ID == "" {
		id, err := newRewritePlanID()
		if err != nil {
			return plan, err
		}
		plan.ID = id
	}
	plan.Version = IntentHistoryPlanVersion
	raw, err := json.Marshal(plan)
	if err != nil {
		return plan, err
	}
	if len(raw) > IntentHistoryPlanByteCap {
		return plan, errors.New("state: history plan exceeds size limit")
	}
	if len(plan.ID) > 128 {
		return plan, errors.New("state: invalid history plan identity")
	}
	key := "intent.history.plan." + plan.ID
	if _, err := db.conn.ExecContext(ctx, `INSERT INTO daemon_meta(key,value,updated_ts) VALUES(?,?,?) ON CONFLICT(key) DO NOTHING`, key, string(raw), nowSeconds()); err != nil {
		return plan, err
	}
	stored, ok, err := MetaGet(ctx, db, key)
	if err != nil {
		return plan, err
	}
	if !ok || stored != string(raw) {
		return plan, errors.New("state: history plans are immutable; create a new plan revision")
	}
	return plan, nil
}

func LoadIntentHistoryPlan(ctx context.Context, db *DB, id string) (IntentHistoryPlan, bool, error) {
	var plan IntentHistoryPlan
	raw, ok, err := MetaGet(ctx, db, "intent.history.plan."+id)
	if err != nil || !ok {
		return plan, ok, err
	}
	if len(raw) > IntentHistoryPlanByteCap {
		return plan, false, errors.New("state: history plan exceeds size limit")
	}
	if err := json.Unmarshal([]byte(raw), &plan); err != nil {
		return plan, false, err
	}
	if plan.Version != IntentHistoryPlanVersion || plan.ID != id {
		return plan, false, errors.New("state: invalid history plan identity")
	}
	return plan, true, nil
}

// EnqueueIntentHistoryRequest serializes explicit repair with the canonical
// worker. One immutable plan may be pending; completed requests are reusable.
func EnqueueIntentHistoryRequest(ctx context.Context, db *DB, planID string) error {
	if _, ok, err := LoadIntentHistoryPlan(ctx, db, planID); err != nil {
		return err
	} else if !ok {
		return errors.New("state: history plan missing")
	}
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var raw string
	err = tx.QueryRowContext(ctx, "SELECT value FROM daemon_meta WHERE key=?", MetaKeyIntentHistoryRequest).Scan(&raw)
	if err == nil {
		var current IntentHistoryRequest
		if err := json.Unmarshal([]byte(raw), &current); err != nil {
			return err
		}
		if current.Status == "completed" && current.PlanID == planID {
			return tx.Commit()
		}
		if current.Status == "pending" || current.Status == "running" {
			if current.PlanID != planID {
				return fmt.Errorf("state: another history request is active: %s", current.PlanID)
			}
			return tx.Commit()
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	request := IntentHistoryRequest{PlanID: planID, Status: "pending", UpdatedTS: nowSeconds()}
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO daemon_meta(key,value,updated_ts) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_ts=excluded.updated_ts`, MetaKeyIntentHistoryRequest, string(body), request.UpdatedTS); err != nil {
		return err
	}
	return tx.Commit()
}

func LoadIntentHistoryRequest(ctx context.Context, db *DB) (IntentHistoryRequest, bool, error) {
	var request IntentHistoryRequest
	ok, err := MetaGetJSON(ctx, db, MetaKeyIntentHistoryRequest, &request)
	return request, ok, err
}

func SaveIntentHistoryRequest(ctx context.Context, db *DB, request IntentHistoryRequest) error {
	request.UpdatedTS = nowSeconds()
	return MetaSetJSON(ctx, db, MetaKeyIntentHistoryRequest, request)
}
