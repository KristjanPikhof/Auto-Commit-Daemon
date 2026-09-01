package state

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

const (
	RewritePlanValidationDraft   = "draft"
	RewritePlanValidationValid   = "valid"
	RewritePlanValidationInvalid = "invalid"

	RewritePlanApplyPending = "pending"
	RewritePlanApplyApplied = "applied"
	RewritePlanApplyFailed  = "failed"
)

// RewritePlan is a reusable commit-message rewrite plan. It is intentionally
// provider-agnostic so apply/editor flows can reload stored proposals without
// making another AI call.
type RewritePlan struct {
	ID               string
	CreatedTS        float64
	UpdatedTS        float64
	BasePlanID       sql.NullString
	Revision         int
	BranchRef        string
	ExpectedHead     string
	Provider         sql.NullString
	Model            sql.NullString
	CommitFormat     string
	ValidationStatus string
	ValidationError  sql.NullString
	Edited           bool
	ApplyStatus      string
	Groups           []RewritePlanGroup
	// Commits is retained only for reading plans created before grouped
	// history rewrites. New plans are stored in Groups.
	Commits []RewritePlanCommit
}

// RewritePlanGroup is one output commit and its ordered input commits.
type RewritePlanGroup struct {
	PlanID          string
	Ord             int
	Members         []RewritePlanMember
	ProposedMessage string
	GroupingReason  string
}

// RewritePlanMember preserves the immutable identity and original message of
// one commit selected for a history rewrite.
type RewritePlanMember struct {
	OldOID          string `json:"old_oid"`
	OriginalMessage string `json:"original_message"`
}

// RewritePlanCommit is one commit row in a rewrite plan, ordered by Ord.
type RewritePlanCommit struct {
	PlanID          string
	Ord             int
	OldOID          string
	ProposedMessage string
	OriginalMessage string
}

// SaveRewritePlan inserts a new rewrite plan and all commit rows atomically.
// If plan.ID is empty, a stable random id is generated and returned.
func SaveRewritePlan(ctx context.Context, d *DB, plan RewritePlan) (string, error) {
	if plan.ID == "" {
		id, err := newRewritePlanID()
		if err != nil {
			return "", err
		}
		plan.ID = id
	}
	if plan.Revision == 0 {
		plan.Revision = 1
	}
	if plan.ValidationStatus == "" {
		plan.ValidationStatus = RewritePlanValidationDraft
	}
	if plan.CommitFormat == "" {
		plan.CommitFormat = "imperative"
	}
	if plan.ApplyStatus == "" {
		plan.ApplyStatus = RewritePlanApplyPending
	}
	if plan.CreatedTS == 0 {
		plan.CreatedTS = nowSeconds()
	}
	if plan.UpdatedTS == 0 {
		plan.UpdatedTS = plan.CreatedTS
	}
	groups, err := RewritePlanGroups(plan)
	if err != nil {
		return "", err
	}
	plan.Groups = groups
	plan.Commits = nil
	if err := validateRewritePlan(plan); err != nil {
		return "", err
	}

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("state: begin rewrite plan save: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := insertRewritePlan(ctx, tx, plan); err != nil {
		return "", err
	}
	if err := insertRewritePlanGroups(ctx, tx, plan.ID, plan.Groups); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("state: commit rewrite plan save: %w", err)
	}
	return plan.ID, nil
}

// LoadRewritePlan returns a saved plan and its commit rows, preserving commit
// order. Callers can apply this result directly without re-invoking a provider.
func LoadRewritePlan(ctx context.Context, d *DB, id string) (RewritePlan, bool, error) {
	if id == "" {
		return RewritePlan{}, false, fmt.Errorf("state: LoadRewritePlan: empty id")
	}
	const q = `
SELECT id, created_ts, updated_ts, base_plan_id, revision, branch_ref,
       expected_head, provider, model, commit_format, validation_status, validation_error, edited, apply_status
FROM rewrite_plans
WHERE id = ?`
	var plan RewritePlan
	var edited int
	err := d.readSQL().QueryRowContext(ctx, q, id).Scan(
		&plan.ID, &plan.CreatedTS, &plan.UpdatedTS, &plan.BasePlanID, &plan.Revision,
		&plan.BranchRef, &plan.ExpectedHead, &plan.Provider, &plan.Model,
		&plan.CommitFormat, &plan.ValidationStatus, &plan.ValidationError, &edited, &plan.ApplyStatus,
	)
	if err == sql.ErrNoRows {
		return RewritePlan{}, false, nil
	}
	if err != nil {
		return RewritePlan{}, false, fmt.Errorf("state: load rewrite plan: %w", err)
	}
	plan.Edited = edited != 0
	if plan.CommitFormat == "" {
		plan.CommitFormat = "imperative"
	}

	groups, err := loadRewritePlanGroups(ctx, d.readSQL(), id)
	if err != nil {
		return RewritePlan{}, false, err
	}
	if len(groups) == 0 {
		commits, err := loadRewritePlanCommits(ctx, d.readSQL(), id)
		if err != nil {
			return RewritePlan{}, false, err
		}
		plan.Commits = commits
		groups, err = RewritePlanGroups(plan)
		if err != nil {
			return RewritePlan{}, false, err
		}
	}
	plan.Groups = groups
	plan.Commits = nil
	return plan, true, nil
}

// CreateEditedRewritePlanRevision copies immutable plan metadata from an
// existing plan and stores an edited, validated successor revision atomically.
func CreateEditedRewritePlanRevision(ctx context.Context, d *DB, basePlanID string, groups []RewritePlanGroup, validationStatus string) (string, error) {
	base, ok, err := LoadRewritePlan(ctx, d, basePlanID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("state: CreateEditedRewritePlanRevision: base plan %q not found", basePlanID)
	}
	if validationStatus == "" {
		validationStatus = RewritePlanValidationValid
	}
	return SaveRewritePlan(ctx, d, RewritePlan{
		BasePlanID:       sql.NullString{String: base.ID, Valid: true},
		Revision:         base.Revision + 1,
		BranchRef:        base.BranchRef,
		ExpectedHead:     base.ExpectedHead,
		Provider:         base.Provider,
		Model:            base.Model,
		CommitFormat:     base.CommitFormat,
		ValidationStatus: validationStatus,
		ValidationError:  base.ValidationError,
		Edited:           true,
		ApplyStatus:      RewritePlanApplyPending,
		Groups:           groups,
	})
}

// UpdateRewritePlanDraft replaces the commit rows and editable status fields
// for an existing draft in one transaction.
func UpdateRewritePlanDraft(ctx context.Context, d *DB, plan RewritePlan) error {
	if plan.ID == "" {
		return fmt.Errorf("state: UpdateRewritePlanDraft: empty id")
	}
	if plan.ValidationStatus == "" {
		plan.ValidationStatus = RewritePlanValidationValid
	}
	if plan.ApplyStatus == "" {
		plan.ApplyStatus = RewritePlanApplyPending
	}
	groups, err := RewritePlanGroups(plan)
	if err != nil {
		return err
	}
	plan.Groups = groups
	plan.Commits = nil
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("state: begin rewrite plan draft update: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, `
UPDATE rewrite_plans SET
    updated_ts = ?, commit_format = COALESCE(NULLIF(?, ''), commit_format), validation_status = ?, validation_error = ?, edited = 1, apply_status = ?
WHERE id = ?`, nowSeconds(), plan.CommitFormat, plan.ValidationStatus, plan.ValidationError, plan.ApplyStatus, plan.ID)
	if err != nil {
		return fmt.Errorf("state: update rewrite plan draft: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("state: rewrite plan draft rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("state: UpdateRewritePlanDraft: plan %q not found", plan.ID)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM rewrite_plan_commits WHERE plan_id = ?`, plan.ID); err != nil {
		return fmt.Errorf("state: clear rewrite plan commits: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM rewrite_plan_groups WHERE plan_id = ?`, plan.ID); err != nil {
		return fmt.Errorf("state: clear rewrite plan groups: %w", err)
	}
	if err := insertRewritePlanGroups(ctx, tx, plan.ID, plan.Groups); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("state: commit rewrite plan draft update: %w", err)
	}
	return nil
}

// MarkRewritePlanApplyStatus records apply lifecycle without mutating the
// saved provider proposal.
func MarkRewritePlanApplyStatus(ctx context.Context, d *DB, id, status string) error {
	if id == "" || status == "" {
		return fmt.Errorf("state: MarkRewritePlanApplyStatus: required field missing")
	}
	res, err := d.conn.ExecContext(ctx, `UPDATE rewrite_plans SET updated_ts = ?, apply_status = ? WHERE id = ?`, nowSeconds(), status, id)
	if err != nil {
		return fmt.Errorf("state: mark rewrite plan apply status: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("state: rewrite plan apply status rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("state: MarkRewritePlanApplyStatus: plan %q not found", id)
	}
	return nil
}

// RewriteOIDReconcileResult counts exact old->new OID substitutions made after
// a successful commit-message rewrite. Only direct references to recreated
// commit OIDs are changed; rows with unrelated or NULL OIDs are left untouched.
type RewriteOIDReconcileResult struct {
	CaptureEvents          int64
	DecisionRecords        int64
	PublishTargetCommitOID int64
	PublishSourceHead      int64
}

// ReconcileRewriteCommitOIDs updates ACD state references that safely point at
// commits recreated by a rewrite apply. It performs exact-value substitutions
// inside one transaction for capture_events.commit_oid,
// decision_records.commit_oid, and publish_state source/target OID fields.
func ReconcileRewriteCommitOIDs(ctx context.Context, d *DB, oidMap map[string]string) (RewriteOIDReconcileResult, error) {
	var out RewriteOIDReconcileResult
	if len(oidMap) == 0 {
		return out, nil
	}
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return out, fmt.Errorf("state: begin rewrite oid reconciliation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE IF NOT EXISTS acd_rewrite_oid_map(old_oid TEXT PRIMARY KEY, new_oid TEXT NOT NULL)`); err != nil {
		return out, fmt.Errorf("state: create rewrite oid map: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM acd_rewrite_oid_map`); err != nil {
		return out, fmt.Errorf("state: clear rewrite oid map: %w", err)
	}
	for oldOID, newOID := range oidMap {
		if oldOID == "" || newOID == "" || oldOID == newOID {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO acd_rewrite_oid_map(old_oid, new_oid) VALUES (?, ?)`, oldOID, newOID); err != nil {
			return out, fmt.Errorf("state: populate rewrite oid map: %w", err)
		}
	}
	updates := []struct {
		name  string
		query string
		out   *int64
	}{
		{"capture_events commit_oid", `UPDATE capture_events SET commit_oid = (SELECT new_oid FROM acd_rewrite_oid_map WHERE old_oid = capture_events.commit_oid) WHERE commit_oid IN (SELECT old_oid FROM acd_rewrite_oid_map)`, &out.CaptureEvents},
		{"decision_records commit_oid", `UPDATE decision_records SET commit_oid = (SELECT new_oid FROM acd_rewrite_oid_map WHERE old_oid = decision_records.commit_oid) WHERE commit_oid IN (SELECT old_oid FROM acd_rewrite_oid_map)`, &out.DecisionRecords},
		{"publish_state target_commit_oid", `UPDATE publish_state SET target_commit_oid = (SELECT new_oid FROM acd_rewrite_oid_map WHERE old_oid = publish_state.target_commit_oid) WHERE target_commit_oid IN (SELECT old_oid FROM acd_rewrite_oid_map)`, &out.PublishTargetCommitOID},
		{"publish_state source_head", `UPDATE publish_state SET source_head = (SELECT new_oid FROM acd_rewrite_oid_map WHERE old_oid = publish_state.source_head) WHERE source_head IN (SELECT old_oid FROM acd_rewrite_oid_map)`, &out.PublishSourceHead},
	}
	for _, update := range updates {
		n, err := execCount(ctx, tx, update.query)
		if err != nil {
			return out, fmt.Errorf("state: reconcile %s: %w", update.name, err)
		}
		*update.out = n
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE acd_rewrite_oid_map`); err != nil {
		return out, fmt.Errorf("state: drop rewrite oid map: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return out, fmt.Errorf("state: commit rewrite oid reconciliation: %w", err)
	}
	return out, nil
}

func execCount(ctx context.Context, tx *sql.Tx, q string, args ...any) (int64, error) {
	res, err := tx.ExecContext(ctx, q, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func insertRewritePlan(ctx context.Context, tx *sql.Tx, plan RewritePlan) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO rewrite_plans(
    id, created_ts, updated_ts, base_plan_id, revision, branch_ref,
    expected_head, provider, model, commit_format, validation_status, validation_error, edited, apply_status
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		plan.ID, plan.CreatedTS, plan.UpdatedTS, plan.BasePlanID, plan.Revision,
		plan.BranchRef, plan.ExpectedHead, plan.Provider, plan.Model,
		plan.CommitFormat, plan.ValidationStatus, plan.ValidationError, boolToInt(plan.Edited), plan.ApplyStatus,
	)
	if err != nil {
		return fmt.Errorf("state: insert rewrite plan: %w", err)
	}
	return nil
}

func insertRewritePlanCommits(ctx context.Context, tx *sql.Tx, planID string, commits []RewritePlanCommit) error {
	if err := validateRewritePlanCommits(commits); err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO rewrite_plan_commits(
    plan_id, ord, old_oid, proposed_message, original_message
) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("state: prepare rewrite plan commits insert: %w", err)
	}
	defer stmt.Close()
	for i, c := range commits {
		if _, err := stmt.ExecContext(ctx, planID, i, c.OldOID, c.ProposedMessage, c.OriginalMessage); err != nil {
			return fmt.Errorf("state: insert rewrite plan commit %d: %w", i, err)
		}
	}
	return nil
}

func insertRewritePlanGroups(ctx context.Context, tx *sql.Tx, planID string, groups []RewritePlanGroup) error {
	if err := validateRewritePlanGroups(groups); err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO rewrite_plan_groups(
    plan_id, ord, members_json, proposed_message, grouping_reason
) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("state: prepare rewrite plan groups insert: %w", err)
	}
	defer stmt.Close()
	for i, group := range groups {
		members, err := json.Marshal(group.Members)
		if err != nil {
			return fmt.Errorf("state: marshal rewrite plan group %d members: %w", i, err)
		}
		if _, err := stmt.ExecContext(ctx, planID, i, members, group.ProposedMessage, group.GroupingReason); err != nil {
			return fmt.Errorf("state: insert rewrite plan group %d: %w", i, err)
		}
	}
	return nil
}

func loadRewritePlanGroups(ctx context.Context, q queryer, planID string) ([]RewritePlanGroup, error) {
	rows, err := q.QueryContext(ctx, `
SELECT plan_id, ord, members_json, proposed_message, grouping_reason
FROM rewrite_plan_groups
WHERE plan_id = ?
ORDER BY ord ASC`, planID)
	if err != nil {
		return nil, fmt.Errorf("state: query rewrite plan groups: %w", err)
	}
	defer rows.Close()
	var out []RewritePlanGroup
	for rows.Next() {
		var group RewritePlanGroup
		var members []byte
		if err := rows.Scan(&group.PlanID, &group.Ord, &members, &group.ProposedMessage, &group.GroupingReason); err != nil {
			return nil, fmt.Errorf("state: scan rewrite plan group: %w", err)
		}
		if err := json.Unmarshal(members, &group.Members); err != nil {
			return nil, fmt.Errorf("state: unmarshal rewrite plan group %d members: %w", group.Ord, err)
		}
		out = append(out, group)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate rewrite plan groups: %w", err)
	}
	return out, nil
}

func loadRewritePlanCommits(ctx context.Context, q queryer, planID string) ([]RewritePlanCommit, error) {
	rows, err := q.QueryContext(ctx, `
SELECT plan_id, ord, old_oid, proposed_message, original_message
FROM rewrite_plan_commits
WHERE plan_id = ?
ORDER BY ord ASC`, planID)
	if err != nil {
		return nil, fmt.Errorf("state: query rewrite plan commits: %w", err)
	}
	defer rows.Close()
	var out []RewritePlanCommit
	for rows.Next() {
		var c RewritePlanCommit
		if err := rows.Scan(&c.PlanID, &c.Ord, &c.OldOID, &c.ProposedMessage, &c.OriginalMessage); err != nil {
			return nil, fmt.Errorf("state: scan rewrite plan commit: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate rewrite plan commits: %w", err)
	}
	return out, nil
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func validateRewritePlan(plan RewritePlan) error {
	if plan.ID == "" || plan.BranchRef == "" || plan.ExpectedHead == "" || plan.ValidationStatus == "" || plan.ApplyStatus == "" {
		return fmt.Errorf("state: rewrite plan required field missing")
	}
	if plan.CommitFormat == "" {
		return fmt.Errorf("state: rewrite plan commit format missing")
	}
	return validateRewritePlanGroups(plan.Groups)
}

// RewritePlanGroups returns a plan's canonical groups. Legacy commit rows are
// projected to singleton groups at the read boundary.
func RewritePlanGroups(plan RewritePlan) ([]RewritePlanGroup, error) {
	if len(plan.Groups) > 0 && len(plan.Commits) > 0 {
		return nil, fmt.Errorf("state: rewrite plan cannot include both groups and legacy commits")
	}
	if len(plan.Groups) > 0 {
		groups := cloneRewritePlanGroups(plan.Groups)
		if err := validateRewritePlanGroups(groups); err != nil {
			return nil, err
		}
		return groups, nil
	}
	if err := validateRewritePlanCommits(plan.Commits); err != nil {
		return nil, err
	}
	groups := make([]RewritePlanGroup, 0, len(plan.Commits))
	for _, commit := range plan.Commits {
		groups = append(groups, RewritePlanGroup{
			Members: []RewritePlanMember{{
				OldOID:          commit.OldOID,
				OriginalMessage: commit.OriginalMessage,
			}},
			ProposedMessage: commit.ProposedMessage,
			GroupingReason:  "message-only rewrite",
		})
	}
	return groups, nil
}

func cloneRewritePlanGroups(groups []RewritePlanGroup) []RewritePlanGroup {
	out := make([]RewritePlanGroup, len(groups))
	for i, group := range groups {
		out[i] = group
		out[i].Members = append([]RewritePlanMember(nil), group.Members...)
	}
	return out
}

func validateRewritePlanGroups(groups []RewritePlanGroup) error {
	if len(groups) == 0 {
		return fmt.Errorf("state: rewrite plan must include at least one group")
	}
	seen := make(map[string]struct{})
	for i, group := range groups {
		if len(group.Members) == 0 || group.ProposedMessage == "" || group.GroupingReason == "" {
			return fmt.Errorf("state: rewrite plan group %d: required field missing", i)
		}
		for j, member := range group.Members {
			if member.OldOID == "" || member.OriginalMessage == "" {
				return fmt.Errorf("state: rewrite plan group %d member %d: required field missing", i, j)
			}
			if _, ok := seen[member.OldOID]; ok {
				return fmt.Errorf("state: duplicate rewrite plan member %s", member.OldOID)
			}
			seen[member.OldOID] = struct{}{}
		}
	}
	return nil
}

func validateRewritePlanCommits(commits []RewritePlanCommit) error {
	if len(commits) == 0 {
		return fmt.Errorf("state: rewrite plan must include at least one commit")
	}
	for i, c := range commits {
		if c.OldOID == "" || c.ProposedMessage == "" || c.OriginalMessage == "" {
			return fmt.Errorf("state: rewrite plan commit %d: required field missing", i)
		}
	}
	return nil
}

func newRewritePlanID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("state: generate rewrite plan id: %w", err)
	}
	return "rp_" + hex.EncodeToString(b[:]), nil
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
