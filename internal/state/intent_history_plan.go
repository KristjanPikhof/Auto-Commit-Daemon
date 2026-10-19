package state

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

const IntentHistoryPlanVersion = 2
const IntentHistoryPlanByteCap = 8 << 20

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
	ID                  string   `json:"id"`
	Purpose             string   `json:"purpose"`
	Message             string   `json:"message"`
	Reason              string   `json:"reason"`
	Units               []int    `json:"units"`
	TreeOID             string   `json:"tree_oid"`
	DependsOnCandidates []string `json:"depends_on_candidates,omitempty"`
	OldOIDs             []string `json:"old_oids,omitempty"`
	AuthorOID           string   `json:"author_oid,omitempty"`
}

// IntentHistoryPlan freezes both original provenance and proposed goal trees.
// The saved target chooses an in-place rewrite or a new branch.
type IntentHistoryPlan struct {
	Version                  int                 `json:"version"`
	ID                       string              `json:"id"`
	SourceBranchRef          string              `json:"source_branch_ref"`
	TargetBranchRef          string              `json:"target_branch_ref"`
	ExpectedHead             string              `json:"expected_head"`
	CommitFormat             string              `json:"commit_format"`
	BaseTree                 string              `json:"base_tree"`
	SourceChain              []string            `json:"source_chain"`
	Units                    []IntentHistoryUnit `json:"units"`
	Goals                    []IntentHistoryGoal `json:"goals"`
	Mode                     string              `json:"mode"`
	MessagesOnly             bool                `json:"messages_only"`
	RecreateUnchanged        []string            `json:"recreate_unchanged,omitempty"`
	SourceRef                string              `json:"source_ref"`
	VerificationConfigDigest string              `json:"verification_config_digest"`
	Disclosure               json.RawMessage     `json:"disclosure,omitempty"`
	PlanningDeadlineTS       float64             `json:"planning_deadline_ts,omitempty"`
	PlanningCalls            int                 `json:"planning_calls,omitempty"`
	SharedPlan               *finish.Plan        `json:"shared_plan,omitempty"`
}

// PrepareIntentHistoryPlan gives a standalone preview the same identity as a
// database plan without opening or migrating a repository's state.
func PrepareIntentHistoryPlan(plan IntentHistoryPlan) (IntentHistoryPlan, error) {
	if plan.ID == "" {
		id, err := newRewritePlanID()
		if err != nil {
			return plan, err
		}
		plan.ID = id
	}
	plan.Version = IntentHistoryPlanVersion
	return plan, nil
}

func SaveIntentHistoryPlan(ctx context.Context, db *DB, plan IntentHistoryPlan) (IntentHistoryPlan, error) {
	plan, err := PrepareIntentHistoryPlan(plan)
	if err != nil {
		return plan, err
	}
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
	if (plan.Version != 1 && plan.Version != IntentHistoryPlanVersion) || plan.ID != id {
		return plan, false, errors.New("state: invalid history plan identity")
	}
	return plan, true, nil
}
