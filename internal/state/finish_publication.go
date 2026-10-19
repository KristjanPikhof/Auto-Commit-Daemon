package state

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
)

const FinishPublicationVersion = 1

type FinishPublicationCommit struct {
	OID       string `json:"oid"`
	ParentOID string `json:"parent_oid"`
	TreeOID   string `json:"tree_oid"`
}

// FinishPublicationProof freezes the complete private chain and the exact
// branch/index transition. ObjectRef retains all commits before branch CAS.
// LockToken identifies this attempt's Git lock markers after process death.
type FinishPublicationProof struct {
	Version          int                       `json:"version"`
	RequestID        string                    `json:"request_id"`
	Attempt          int                       `json:"attempt"`
	WorktreeID       string                    `json:"worktree_id"`
	GitDir           string                    `json:"git_dir"`
	BranchRef        string                    `json:"branch_ref"`
	ExpectedHead     string                    `json:"expected_head"`
	NewHead          string                    `json:"new_head"`
	TargetTree       string                    `json:"target_tree"`
	IndexDigest      string                    `json:"index_digest"`
	CleanIndexDigest string                    `json:"clean_index_digest"`
	IncludeStaged    bool                      `json:"include_staged"`
	ObjectRef        string                    `json:"object_ref"`
	ObjectOID        string                    `json:"object_oid"`
	LockToken        string                    `json:"lock_token"`
	Commits          []FinishPublicationCommit `json:"commits"`
}

type FinishPublicationResult struct {
	Published       bool   `json:"published"`
	NewHead         string `json:"new_head"`
	CommitsCreated  int    `json:"commits_created"`
	IndexReconciled bool   `json:"index_reconciled"`
	StagedConsumed  bool   `json:"staged_consumed"`
}

func ValidateFinishPublicationProof(p FinishPublicationProof) error {
	if p.Version != FinishPublicationVersion || p.RequestID == "" || len(p.RequestID) > 128 || p.Attempt < 1 || !validHexID(p.WorktreeID, 16) || !filepath.IsAbs(p.GitDir) || filepath.Clean(p.GitDir) != p.GitDir ||
		!validFinishRef(p.BranchRef, "refs/heads/") || !validFinishOID(p.NewHead) || !validFinishOID(p.TargetTree) || (p.ExpectedHead != "" && !validFinishOID(p.ExpectedHead)) ||
		p.IndexDigest == "" || p.CleanIndexDigest == "" || !validFinishRef(p.ObjectRef, "refs/acd/finish/") ||
		p.ObjectOID != p.NewHead || !validHexID(p.LockToken, 64) || len(p.Commits) == 0 {
		return ErrFinishIdentity
	}
	parent := p.ExpectedHead
	seen := map[string]bool{}
	for _, commit := range p.Commits {
		if !validFinishOID(commit.OID) || !validFinishOID(commit.TreeOID) || len(commit.OID) != len(p.NewHead) || len(commit.TreeOID) != len(p.NewHead) || commit.ParentOID != parent || commit.OID == parent || seen[commit.OID] {
			return ErrFinishIdentity
		}
		seen[commit.OID] = true
		parent = commit.OID
	}
	last := p.Commits[len(p.Commits)-1]
	if last.OID != p.NewHead || last.TreeOID != p.TargetTree || (p.ExpectedHead != "" && len(p.ExpectedHead) != len(p.NewHead)) {
		return ErrFinishIdentity
	}
	return nil
}

func validFinishOID(oid string) bool { return validHexID(oid, 40) || validHexID(oid, 64) }

func validFinishRef(ref, prefix string) bool {
	if !strings.HasPrefix(ref, prefix) || len(ref) == len(prefix) || strings.HasSuffix(ref, "/") || strings.HasSuffix(ref, ".") || strings.Contains(ref, "..") || strings.Contains(ref, "@{") || strings.Contains(ref, "//") {
		return false
	}
	for _, r := range ref {
		if r <= 32 || r == 127 || strings.ContainsRune("~^:?*[\\", r) {
			return false
		}
	}
	for _, component := range strings.Split(ref, "/") {
		if strings.HasPrefix(component, ".") || strings.HasSuffix(component, ".lock") {
			return false
		}
	}
	return true
}

func finishPublicationMatchesTarget(r FinishRequest, p FinishPublicationProof) bool {
	return !r.DryRun && p.RequestID == r.ID && p.Attempt == r.Attempt && p.WorktreeID == r.Target.WorktreeID &&
		p.BranchRef == r.Target.BranchRef && p.ExpectedHead == r.Target.ExpectedHead && p.TargetTree == r.Target.TargetTree &&
		p.IndexDigest == r.Target.IndexDigest && p.IncludeStaged == r.Target.IncludeStaged
}

func PrepareFinishPublication(ctx context.Context, db *DB, id string, attempt int, p FinishPublicationProof, plan json.RawMessage) (FinishRequest, error) {
	if err := ValidateFinishPublicationProof(p); err != nil {
		return FinishRequest{}, err
	}
	r, err := LoadFinishRequest(ctx, db, id)
	if err != nil {
		return r, err
	}
	if r.Attempt != attempt || !finishPublicationMatchesTarget(r, p) {
		return r, ErrFinishIdentity
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return r, err
	}
	if r.State == FinishPrepared && string(r.Publication) == string(raw) {
		return r, nil
	}
	return UpdateFinishRequest(ctx, db, id, attempt, FinishRunning, FinishUpdate{State: FinishPrepared, Stage: "publishing", Plan: plan, Publication: raw})
}

func LoadFinishPublication(ctx context.Context, db *DB, id string) (FinishPublicationProof, error) {
	r, err := LoadFinishRequest(ctx, db, id)
	if err != nil {
		return FinishPublicationProof{}, err
	}
	var p FinishPublicationProof
	if err := json.Unmarshal(r.Publication, &p); err != nil {
		return p, ErrFinishIdentity
	}
	if err := ValidateFinishPublicationProof(p); err != nil {
		return p, err
	}
	if !finishPublicationMatchesTarget(r, p) {
		return p, ErrFinishIdentity
	}
	return p, nil
}

// CompleteFinishPublication requires the caller to hold exact Git ref/index
// proof while recording this terminal result. SQLite cannot prove Git itself.
func CompleteFinishPublication(ctx context.Context, db *DB, id string, attempt int, result FinishPublicationResult) (FinishRequest, error) {
	p, err := LoadFinishPublication(ctx, db, id)
	if err != nil {
		return FinishRequest{}, err
	}
	r, err := LoadFinishRequest(ctx, db, id)
	if err != nil {
		return r, err
	}
	if r.State != FinishPrepared && r.State != FinishNeedsAttention && r.State != FinishCompleted {
		return r, ErrFinishState
	}
	if p.Attempt != attempt || !validFinishPublicationResult(p, result) || !result.IndexReconciled {
		return r, ErrFinishIdentity
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return r, err
	}
	if r.State == FinishCompleted && string(r.Result) == string(raw) {
		return r, nil
	}
	return UpdateFinishRequest(ctx, db, id, attempt, r.State, FinishUpdate{State: FinishCompleted, Stage: "", Result: raw})
}

func MarkFinishPublicationAttention(ctx context.Context, db *DB, id string, attempt int, reason string, result FinishPublicationResult) (FinishRequest, error) {
	p, err := LoadFinishPublication(ctx, db, id)
	if err != nil {
		return FinishRequest{}, err
	}
	r, err := LoadFinishRequest(ctx, db, id)
	if err != nil {
		return r, err
	}
	if r.State != FinishPrepared && r.State != FinishNeedsAttention {
		return r, ErrFinishState
	}
	if attempt != p.Attempt || !validFinishPublicationResult(p, result) {
		return r, ErrFinishIdentity
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return r, err
	}
	return UpdateFinishRequest(ctx, db, id, attempt, r.State, FinishUpdate{State: FinishNeedsAttention, ReasonCode: "finish_needs_attention", Reason: reason, Result: raw})
}

func validFinishPublicationResult(p FinishPublicationProof, result FinishPublicationResult) bool {
	if !result.Published {
		return result.NewHead == "" && result.CommitsCreated == 0 && !result.IndexReconciled && !result.StagedConsumed
	}
	return result.NewHead == p.NewHead && result.CommitsCreated == len(p.Commits) &&
		((result.IndexReconciled && result.StagedConsumed == p.IncludeStaged) || (!result.IndexReconciled && !result.StagedConsumed))
}
