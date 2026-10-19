package state

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	RewritePlanValidationDraft   = "draft"
	RewritePlanValidationValid   = "valid"
	RewritePlanValidationInvalid = "invalid"

	RewritePlanApplyPending = "pending"
	RewritePlanApplyApplied = "applied"
	RewritePlanApplyFailed  = "failed"
)

// RewritePlan decodes an archived commit-message proposal without changing it.
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

// LoadRewritePlan reads an archived proposal in its original commit order.
func LoadRewritePlan(ctx context.Context, d *DB, id string) (RewritePlan, bool, error) {
	exists, err := legacyTableExists(ctx, d.ReadSQL(), "rewrite_plans")
	if err != nil || !exists {
		return RewritePlan{}, false, err
	}

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
	err = d.readSQL().QueryRowContext(ctx, q, id).Scan(
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
		if len(group.Members) == 0 || strings.TrimSpace(group.ProposedMessage) == "" || strings.TrimSpace(group.GroupingReason) == "" {
			return fmt.Errorf("state: rewrite plan group %d: required field missing", i)
		}
		for j, member := range group.Members {
			if strings.TrimSpace(member.OldOID) == "" || strings.TrimSpace(member.OriginalMessage) == "" {
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
