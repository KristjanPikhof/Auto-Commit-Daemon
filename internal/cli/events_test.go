package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestEventsJSONFiltersAndCursor(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, dbPath, db := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	ctx := context.Background()

	firstID, err := state.AppendDecision(ctx, db, state.DecisionRecord{
		DecisionTS:  10,
		Kind:        state.DecisionKindSkipped,
		Path:        sqlNullStr("ignored.log"),
		Reason:      sqlNullStr("safe-ignore"),
		ActionTaken: sqlNullStr("left uncommitted"),
		UserMessage: sqlNullStr("ignored.log is ignored by policy"),
	})
	if err != nil {
		t.Fatalf("AppendDecision first: %v", err)
	}
	secondID, err := state.AppendDecision(ctx, db, state.DecisionRecord{
		DecisionTS:  11,
		Kind:        state.DecisionKindCaptured,
		Path:        sqlNullStr("src/app.go"),
		ActionTaken: sqlNullStr("queued"),
	})
	if err != nil {
		t.Fatalf("AppendDecision second: %v", err)
	}
	thirdID, err := state.AppendDecision(ctx, db, state.DecisionRecord{
		DecisionTS:  12,
		Kind:        state.DecisionKindCommitted,
		Path:        sqlNullStr("src/app.go"),
		CommitOID:   sqlNullStr("abc123"),
		ActionTaken: sqlNullStr("committed"),
	})
	if err != nil {
		t.Fatalf("AppendDecision third: %v", err)
	}

	var out bytes.Buffer
	if err := runEvents(ctx, &out, repo, "", 0, 2, false, time.Millisecond, true); err != nil {
		t.Fatalf("runEvents recent json: %v", err)
	}
	var recent eventsReport
	if err := json.Unmarshal(out.Bytes(), &recent); err != nil {
		t.Fatalf("decode recent: %v\n%s", err, out.String())
	}
	if recent.Cursor != thirdID || len(recent.Events) != 2 {
		t.Fatalf("recent = %+v, want cursor %d and 2 events", recent, thirdID)
	}
	if recent.Events[0].ID != thirdID || recent.Events[1].ID != secondID {
		t.Fatalf("recent order = %+v, want newest first", recent.Events)
	}

	out.Reset()
	if err := runEvents(ctx, &out, repo, "src/app.go", secondID, 10, false, time.Millisecond, true); err != nil {
		t.Fatalf("runEvents path since json: %v", err)
	}
	var filtered eventsReport
	if err := json.Unmarshal(out.Bytes(), &filtered); err != nil {
		t.Fatalf("decode filtered: %v\n%s", err, out.String())
	}
	if len(filtered.Events) != 1 || filtered.Events[0].ID != thirdID || filtered.Events[0].Path != "src/app.go" {
		t.Fatalf("filtered = %+v, want only third path event after cursor", filtered.Events)
	}

	out.Reset()
	if err := runEvents(ctx, &out, repo, "missing.go", firstID, 10, false, time.Millisecond, false); err != nil {
		t.Fatalf("runEvents empty human: %v", err)
	}
	if !strings.Contains(out.String(), "No decisions recorded yet.") {
		t.Fatalf("empty human output missing empty-state copy:\n%s", out.String())
	}
}

func TestEventsWatchStreamsAppendedRowsOnce(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, dbPath, db := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var out lockedBuffer
	errCh := make(chan error, 1)
	go func() {
		errCh <- runEvents(ctx, &out, repo, "", 0, 10, true, 10*time.Millisecond, true)
	}()

	time.Sleep(30 * time.Millisecond)
	if _, err := state.AppendDecision(context.Background(), db, state.DecisionRecord{
		DecisionTS:       20,
		Kind:             state.DecisionKindCaptured,
		Path:             sqlNullStr("a.txt"),
		UserMessage:      sqlNullStr("first append"),
		ActionTaken:      sqlNullStr("queued"),
		BranchRef:        sqlNullStr("refs/heads/main"),
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if _, err := state.AppendDecision(context.Background(), db, state.DecisionRecord{
		DecisionTS:  21,
		Kind:        state.DecisionKindCommitted,
		Path:        sqlNullStr("b.txt"),
		UserMessage: sqlNullStr("second append"),
		ActionTaken: sqlNullStr("committed"),
	}); err != nil {
		t.Fatalf("append second: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for {
		if strings.Contains(out.String(), "second append") {
			cancel()
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatalf("watch did not stream appended decisions:\n%s", out.String())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := <-errCh; err != nil {
		t.Fatalf("runEvents watch: %v", err)
	}
	got := out.String()
	if strings.Count(got, "first append") != 1 || strings.Count(got, "second append") != 1 {
		t.Fatalf("watch output did not stream each row once:\n%s", got)
	}
}

func TestEventsErrorsAndCommandHelp(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, dbPath, db := makeRepoStateDB(t)
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	registerRepo(t, roots, repo, dbPath+".missing", "codex")

	var out bytes.Buffer
	err := runEvents(context.Background(), &out, repo, "", 0, 10, false, time.Millisecond, false)
	if err == nil || !strings.Contains(err.Error(), "state.db missing") {
		t.Fatalf("missing state error = %v, want actionable missing state.db error", err)
	}
	if err := runEvents(context.Background(), io.Discard, repo, "", -1, 10, false, time.Millisecond, false); err == nil {
		t.Fatalf("negative --since returned nil error")
	}
	if err := runEvents(context.Background(), io.Discard, repo, "", 0, 0, false, time.Millisecond, false); err == nil {
		t.Fatalf("zero --limit returned nil error")
	}

	root := newRootCmd()
	var helpOut, helpErr bytes.Buffer
	root.SetOut(&helpOut)
	root.SetErr(&helpErr)
	root.SetArgs([]string{"events", "--help"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("events help: %v\nstderr:\n%s", err, helpErr.String())
	}
	help := helpOut.String()
	for _, want := range []string{"Show product-facing ACD decisions", "--watch", "--path", "--since", "--limit"} {
		if !strings.Contains(help, want) {
			t.Fatalf("events help missing %q:\n%s", want, help)
		}
	}
}

func TestEventsMissingDecisionLedgerIsReadOnly(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, dbPath, db := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	preparePreDecisionLedgerDB(t, db, dbPath)

	before := mustSHA256(t, dbPath)
	versionBefore := readUserVersionReadOnly(t, dbPath)

	var out bytes.Buffer
	if err := runEvents(context.Background(), &out, repo, "", 0, 10, false, time.Millisecond, true); err != nil {
		t.Fatalf("runEvents missing ledger: %v\n%s", err, out.String())
	}
	var rep eventsReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("decode events: %v\n%s", err, out.String())
	}
	if len(rep.Events) != 0 || !strings.Contains(rep.Message, "Decision ledger is not available") {
		t.Fatalf("unexpected events report: %+v", rep)
	}
	if after := mustSHA256(t, dbPath); after != before {
		t.Fatalf("state.db checksum changed: before=%s after=%s", before, after)
	}
	if got := readUserVersionReadOnly(t, dbPath); got != versionBefore {
		t.Fatalf("user_version changed: before=%d after=%d", versionBefore, got)
	}
}

func preparePreDecisionLedgerDB(t *testing.T, db *state.DB, dbPath string) {
	t.Helper()
	if _, err := db.SQL().ExecContext(context.Background(), `DROP TABLE decision_records`); err != nil {
		t.Fatalf("drop decision_records: %v", err)
	}
	if _, err := db.SQL().ExecContext(context.Background(), `PRAGMA user_version = 4`); err != nil {
		t.Fatalf("set user_version: %v", err)
	}
	if _, err := db.SQL().ExecContext(context.Background(), `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("checkpoint db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
}

func readUserVersionReadOnly(t *testing.T, dbPath string) int {
	t.Helper()
	conn, err := openStateDBReadOnly(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open read-only: %v", err)
	}
	defer conn.Close()
	var version int
	if err := conn.QueryRowContext(context.Background(), `PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	return version
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func sqlNullStr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}
