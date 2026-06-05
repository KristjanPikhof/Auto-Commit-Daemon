package state

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestRewritePlanSaveLoadAndApplyStatus(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	id, err := SaveRewritePlan(ctx, d, RewritePlan{
		BranchRef:        "refs/heads/main",
		ExpectedHead:     "head123",
		Provider:         sql.NullString{String: "openai", Valid: true},
		Model:            sql.NullString{String: "gpt-test", Valid: true},
		ValidationStatus: RewritePlanValidationValid,
		Commits: []RewritePlanCommit{
			{OldOID: "old1", ProposedMessage: "new message 1", OriginalMessage: "old message 1"},
			{OldOID: "old2", ProposedMessage: "new message 2", OriginalMessage: "old message 2"},
		},
	})
	if err != nil {
		t.Fatalf("SaveRewritePlan: %v", err)
	}
	if id == "" {
		t.Fatalf("SaveRewritePlan returned empty id")
	}

	got, ok, err := LoadRewritePlan(ctx, d, id)
	if err != nil || !ok {
		t.Fatalf("LoadRewritePlan: ok=%v err=%v", ok, err)
	}
	if got.ID != id || got.BranchRef != "refs/heads/main" || got.ExpectedHead != "head123" {
		t.Fatalf("loaded metadata mismatch: %+v", got)
	}
	if !got.Provider.Valid || got.Provider.String != "openai" || !got.Model.Valid || got.Model.String != "gpt-test" {
		t.Fatalf("provider/model mismatch: provider=%+v model=%+v", got.Provider, got.Model)
	}
	if got.ValidationStatus != RewritePlanValidationValid || got.Edited || got.ApplyStatus != RewritePlanApplyPending {
		t.Fatalf("loaded status mismatch: %+v", got)
	}
	if got.CommitFormat != "imperative" {
		t.Fatalf("CommitFormat=%q want imperative", got.CommitFormat)
	}
	if len(got.Commits) != 2 || got.Commits[0].Ord != 0 || got.Commits[1].OldOID != "old2" || got.Commits[1].ProposedMessage != "new message 2" {
		t.Fatalf("loaded commits mismatch: %+v", got.Commits)
	}

	if err := MarkRewritePlanApplyStatus(ctx, d, id, RewritePlanApplyApplied); err != nil {
		t.Fatalf("MarkRewritePlanApplyStatus: %v", err)
	}
	applied, ok, err := LoadRewritePlan(ctx, d, id)
	if err != nil || !ok {
		t.Fatalf("reload applied: ok=%v err=%v", ok, err)
	}
	if applied.ApplyStatus != RewritePlanApplyApplied {
		t.Fatalf("apply status=%q want %q", applied.ApplyStatus, RewritePlanApplyApplied)
	}
}

func TestRewritePlanEditedRevisionAndDraftUpdateAreAtomic(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	baseID, err := SaveRewritePlan(ctx, d, RewritePlan{
		BranchRef:    "refs/heads/feature",
		ExpectedHead: "head456",
		Provider:     sql.NullString{String: "anthropic", Valid: true},
		Model:        sql.NullString{String: "claude-test", Valid: true},
		CommitFormat: "conventional",
		Commits: []RewritePlanCommit{
			{OldOID: "old-a", ProposedMessage: "draft proposal", OriginalMessage: "original"},
		},
	})
	if err != nil {
		t.Fatalf("SaveRewritePlan base: %v", err)
	}

	updated := RewritePlan{
		ID:               baseID,
		ValidationStatus: RewritePlanValidationValid,
		Commits: []RewritePlanCommit{
			{OldOID: "old-a", ProposedMessage: "edited in place", OriginalMessage: "original"},
			{OldOID: "old-b", ProposedMessage: "added row", OriginalMessage: "original b"},
		},
	}
	if err := UpdateRewritePlanDraft(ctx, d, updated); err != nil {
		t.Fatalf("UpdateRewritePlanDraft: %v", err)
	}
	base, ok, err := LoadRewritePlan(ctx, d, baseID)
	if err != nil || !ok {
		t.Fatalf("LoadRewritePlan updated base: ok=%v err=%v", ok, err)
	}
	if !base.Edited || base.ValidationStatus != RewritePlanValidationValid || len(base.Commits) != 2 || base.Commits[0].ProposedMessage != "edited in place" {
		t.Fatalf("draft update mismatch: %+v commits=%+v", base, base.Commits)
	}
	if base.CommitFormat != "conventional" {
		t.Fatalf("draft CommitFormat=%q want conventional", base.CommitFormat)
	}

	revID, err := CreateEditedRewritePlanRevision(ctx, d, baseID, []RewritePlanCommit{
		{OldOID: "old-a", ProposedMessage: "revision proposal", OriginalMessage: "original"},
	}, RewritePlanValidationValid)
	if err != nil {
		t.Fatalf("CreateEditedRewritePlanRevision: %v", err)
	}
	rev, ok, err := LoadRewritePlan(ctx, d, revID)
	if err != nil || !ok {
		t.Fatalf("LoadRewritePlan revision: ok=%v err=%v", ok, err)
	}
	if !rev.BasePlanID.Valid || rev.BasePlanID.String != baseID || rev.Revision != base.Revision+1 {
		t.Fatalf("revision ancestry mismatch: base=%+v rev=%+v", base, rev)
	}
	if rev.BranchRef != base.BranchRef || rev.ExpectedHead != base.ExpectedHead || rev.Provider.String != base.Provider.String || rev.Model.String != base.Model.String {
		t.Fatalf("revision metadata mismatch: base=%+v rev=%+v", base, rev)
	}
	if rev.CommitFormat != "conventional" {
		t.Fatalf("revision CommitFormat=%q want conventional", rev.CommitFormat)
	}
	if !rev.Edited || rev.ValidationStatus != RewritePlanValidationValid || rev.ApplyStatus != RewritePlanApplyPending {
		t.Fatalf("revision status mismatch: %+v", rev)
	}

	if err := UpdateRewritePlanDraft(ctx, d, RewritePlan{
		ID:               baseID,
		ValidationStatus: RewritePlanValidationValid,
		Commits:          []RewritePlanCommit{{OldOID: "", ProposedMessage: "bad", OriginalMessage: "bad"}},
	}); err == nil {
		t.Fatalf("invalid draft update returned nil error")
	}
	afterFailedUpdate, ok, err := LoadRewritePlan(ctx, d, baseID)
	if err != nil || !ok {
		t.Fatalf("reload after failed draft update: ok=%v err=%v", ok, err)
	}
	if len(afterFailedUpdate.Commits) != 2 {
		t.Fatalf("failed update was not atomic; commits=%+v", afterFailedUpdate.Commits)
	}
}

func TestReconcileRewriteCommitOIDs(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	if _, err := d.SQL().ExecContext(ctx, `
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state, commit_oid)
VALUES ('refs/heads/main', 1, 'base', 'write', 'a.txt', 'full', 1, 'published', 'old-a'),
       ('refs/heads/main', 1, 'base', 'write', 'b.txt', 'full', 2, 'published', 'unrelated');
INSERT INTO decision_records(decision_ts, kind, commit_oid, branch_ref)
VALUES (1, 'test', 'old-a', 'refs/heads/main'),
       (2, 'test', 'old-b', 'refs/heads/main');
INSERT INTO publish_state(id, event_seq, branch_ref, branch_generation, source_head, target_commit_oid, status, updated_ts)
VALUES (1, 1, 'refs/heads/main', 1, 'old-a', 'old-b', 'published', 1);
`); err != nil {
		t.Fatalf("seed oid refs: %v", err)
	}

	res, err := ReconcileRewriteCommitOIDs(ctx, d, map[string]string{"old-a": "new-a", "old-b": "new-b"})
	if err != nil {
		t.Fatalf("ReconcileRewriteCommitOIDs: %v", err)
	}
	if res.CaptureEvents != 1 || res.DecisionRecords != 2 || res.PublishSourceHead != 1 || res.PublishTargetCommitOID != 1 {
		t.Fatalf("unexpected reconcile counts: %+v", res)
	}
	for _, tc := range []struct{ table, column, want string }{
		{"capture_events", "commit_oid", "new-a"},
		{"decision_records", "commit_oid", "new-a"},
		{"publish_state", "source_head", "new-a"},
		{"publish_state", "target_commit_oid", "new-b"},
	} {
		var got string
		q := "SELECT " + tc.column + " FROM " + tc.table + " WHERE " + tc.column + " = ? LIMIT 1"
		if err := d.SQL().QueryRowContext(ctx, q, tc.want).Scan(&got); err != nil {
			t.Fatalf("%s.%s not reconciled to %s: %v", tc.table, tc.column, tc.want, err)
		}
	}
	var unrelated int
	if err := d.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events WHERE commit_oid = 'unrelated'`).Scan(&unrelated); err != nil || unrelated != 1 {
		t.Fatalf("unrelated capture event changed: n=%d err=%v", unrelated, err)
	}
}

func TestRewritePlanSchemaMigratesFromV7(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	gitDir := filepath.Join(t.TempDir(), ".git")
	dbPath := DBPathFromGitDir(gitDir)
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	raw, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatalf("sql.Open raw: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `
CREATE TABLE capture_events(
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
PRAGMA user_version = 7;`); err != nil {
		_ = raw.Close()
		t.Fatalf("seed v7 db: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	d, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open migrated v7: %v", err)
	}
	defer d.Close()
	uv, err := d.UserVersion(ctx)
	if err != nil {
		t.Fatalf("UserVersion: %v", err)
	}
	if uv != SchemaVersion {
		t.Fatalf("user_version = %d, want %d", uv, SchemaVersion)
	}

	id, err := SaveRewritePlan(ctx, d, RewritePlan{
		BranchRef:    "refs/heads/main",
		ExpectedHead: "migrated-head",
		Commits:      []RewritePlanCommit{{OldOID: "old", ProposedMessage: "new", OriginalMessage: "orig"}},
	})
	if err != nil {
		t.Fatalf("SaveRewritePlan after migration: %v", err)
	}
	loaded, ok, err := LoadRewritePlan(ctx, d, id)
	if err != nil || !ok {
		t.Fatalf("LoadRewritePlan after migration: ok=%v err=%v", ok, err)
	}
	if loaded.CommitFormat != "imperative" {
		t.Fatalf("migrated CommitFormat=%q want imperative", loaded.CommitFormat)
	}
}
