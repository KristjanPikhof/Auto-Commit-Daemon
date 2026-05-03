package state

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestDecisionRecordsRoundTripAndQueries(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	evSeq, err := AppendCaptureEvent(ctx, d, CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 3,
		BaseHead:         "abc123",
		Operation:        "modify",
		Path:             "src/app.go",
		Fidelity:         "exact",
	}, []CaptureOp{{Op: "modify", Path: "src/app.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}

	firstID, err := AppendDecision(ctx, d, DecisionRecord{
		DecisionTS:       10,
		Kind:             DecisionKindCaptured,
		Path:             sqlNullStr("src/app.go"),
		Reason:           sqlNullStr("worktree changed"),
		EventSeq:         sql.NullInt64{Int64: evSeq, Valid: true},
		HeadSHA:          sqlNullStr("abc123"),
		BranchRef:        sqlNullStr("refs/heads/main"),
		BranchGeneration: sql.NullInt64{Int64: 3, Valid: true},
		ActionTaken:      sqlNullStr("queued"),
		UserMessage:      sqlNullStr("Captured src/app.go for replay."),
	})
	if err != nil {
		t.Fatalf("AppendDecision first: %v", err)
	}
	secondID, err := AppendDecision(ctx, d, DecisionRecord{
		DecisionTS:       11,
		Kind:             DecisionKindCommitted,
		Path:             sqlNullStr("src/app.go"),
		EventSeq:         sql.NullInt64{Int64: evSeq, Valid: true},
		HeadSHA:          sqlNullStr("abc123"),
		CommitOID:        sqlNullStr("def456"),
		BranchRef:        sqlNullStr("refs/heads/main"),
		BranchGeneration: sql.NullInt64{Int64: 3, Valid: true},
		ActionTaken:      sqlNullStr("committed"),
		UserMessage:      sqlNullStr("Committed src/app.go."),
	})
	if err != nil {
		t.Fatalf("AppendDecision second: %v", err)
	}
	if secondID <= firstID {
		t.Fatalf("decision ids = %d,%d; want monotonic", firstID, secondID)
	}

	recent, err := RecentDecisions(ctx, d, 10)
	if err != nil {
		t.Fatalf("RecentDecisions: %v", err)
	}
	if len(recent) != 2 || recent[0].ID != secondID || recent[1].ID != firstID {
		t.Fatalf("recent order = %+v, want newest first", recent)
	}

	forPath, err := DecisionsForPath(ctx, d, "src/app.go", 1)
	if err != nil {
		t.Fatalf("DecisionsForPath: %v", err)
	}
	if len(forPath) != 1 || forPath[0].ID != secondID {
		t.Fatalf("path query = %+v, want latest matching row", forPath)
	}

	forEvent, err := DecisionsForEvent(ctx, d, evSeq, 10)
	if err != nil {
		t.Fatalf("DecisionsForEvent: %v", err)
	}
	if len(forEvent) != 2 || forEvent[0].ID != firstID || forEvent[1].ID != secondID {
		t.Fatalf("event query = %+v, want lifecycle order", forEvent)
	}

	forCommit, err := DecisionsForCommit(ctx, d, "def456", 10)
	if err != nil {
		t.Fatalf("DecisionsForCommit: %v", err)
	}
	if len(forCommit) != 1 || forCommit[0].Kind != DecisionKindCommitted {
		t.Fatalf("commit query = %+v, want committed decision", forCommit)
	}

	since, err := DecisionsSince(ctx, d, firstID, 10)
	if err != nil {
		t.Fatalf("DecisionsSince: %v", err)
	}
	if len(since) != 1 || since[0].ID != secondID {
		t.Fatalf("since query = %+v, want only second row", since)
	}

	pathSince, err := DecisionsForPathSince(ctx, d, "src/app.go", firstID, 10)
	if err != nil {
		t.Fatalf("DecisionsForPathSince: %v", err)
	}
	if len(pathSince) != 1 || pathSince[0].ID != secondID {
		t.Fatalf("path since query = %+v, want only second row", pathSince)
	}
	latest, err := LatestDecisionID(ctx, d)
	if err != nil {
		t.Fatalf("LatestDecisionID: %v", err)
	}
	if latest != secondID {
		t.Fatalf("latest decision id = %d, want %d", latest, secondID)
	}
}

func TestDecisionRecordsValidateAndClamp(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	if _, err := AppendDecision(ctx, d, DecisionRecord{}); err == nil {
		t.Fatalf("AppendDecision without kind succeeded")
	}
	if _, err := DecisionsForPath(ctx, d, "", 1); err == nil {
		t.Fatalf("DecisionsForPath with empty path succeeded")
	}
	if _, err := DecisionsForPathSince(ctx, d, "", 0, 1); err == nil {
		t.Fatalf("DecisionsForPathSince with empty path succeeded")
	}
	if _, err := DecisionsForPathSince(ctx, d, "x", -1, 1); err == nil {
		t.Fatalf("DecisionsForPathSince with negative cursor succeeded")
	}
	if _, err := DecisionsForEvent(ctx, d, 0, 1); err == nil {
		t.Fatalf("DecisionsForEvent with zero seq succeeded")
	}
	if _, err := DecisionsForCommit(ctx, d, "", 1); err == nil {
		t.Fatalf("DecisionsForCommit with empty oid succeeded")
	}
	if _, err := DecisionsSince(ctx, d, -1, 1); err == nil {
		t.Fatalf("DecisionsSince with negative cursor succeeded")
	}
	for i := 0; i < maxDecisionLimit+5; i++ {
		if _, err := AppendDecision(ctx, d, DecisionRecord{Kind: DecisionKindSkipped}); err != nil {
			t.Fatalf("AppendDecision %d: %v", i, err)
		}
	}
	got, err := RecentDecisions(ctx, d, maxDecisionLimit+100)
	if err != nil {
		t.Fatalf("RecentDecisions capped: %v", err)
	}
	if len(got) != maxDecisionLimit {
		t.Fatalf("capped recent len = %d, want %d", len(got), maxDecisionLimit)
	}
}

func TestDecisionRecordsSchemaMigratesFromV4(t *testing.T) {
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
CREATE TABLE daemon_meta(
    key           TEXT PRIMARY KEY,
    value         TEXT NOT NULL,
    updated_ts    REAL NOT NULL
);
INSERT INTO daemon_meta(key, value, updated_ts) VALUES('kept', 'v4', 1);
PRAGMA user_version = 4;`); err != nil {
		_ = raw.Close()
		t.Fatalf("seed v4 db: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	d, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open migrated v4: %v", err)
	}
	defer d.Close()
	uv, err := d.UserVersion(ctx)
	if err != nil {
		t.Fatalf("UserVersion: %v", err)
	}
	if uv != SchemaVersion {
		t.Fatalf("user_version = %d, want %d", uv, SchemaVersion)
	}
	value, ok, err := MetaGet(ctx, d, "kept")
	if err != nil || !ok || value != "v4" {
		t.Fatalf("preserved meta = (%q, %v, %v), want (v4, true, nil)", value, ok, err)
	}
	id, err := AppendDecision(ctx, d, DecisionRecord{Kind: DecisionKindResumed})
	if err != nil {
		t.Fatalf("AppendDecision after migration: %v", err)
	}
	got, err := DecisionsSince(ctx, d, 0, 10)
	if err != nil {
		t.Fatalf("DecisionsSince after migration: %v", err)
	}
	if len(got) != 1 || got[0].ID != id {
		t.Fatalf("migrated decision query = %+v, want id %d", got, id)
	}
}

func TestDecisionRecordsConcurrentAppendAndCursor(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	const writers = 8
	const perWriter = 25
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if _, err := AppendDecision(ctx, d, DecisionRecord{Kind: DecisionKindCaptured}); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent append: %v", err)
		}
	}

	got, err := DecisionsSince(ctx, d, 0, writers*perWriter)
	if err != nil {
		t.Fatalf("DecisionsSince: %v", err)
	}
	if len(got) != writers*perWriter {
		t.Fatalf("decision count = %d, want %d", len(got), writers*perWriter)
	}
	for i := 1; i < len(got); i++ {
		if got[i].ID <= got[i-1].ID {
			t.Fatalf("cursor order regressed at %d: %d after %d", i, got[i].ID, got[i-1].ID)
		}
	}
}
