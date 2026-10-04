package daemon

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func newPartialPublicationFixture(t *testing.T) *captureFixture {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("requires enforced file permissions")
	}
	f := newCaptureFixture(t)
	capturePublicationFiles(t, f)
	sum, err := Replay(context.Background(), f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyEvent})
	if err != nil {
		t.Fatal(err)
	}
	f.cctx.BaseHead = sum.BaseHead
	blocked := filepath.Join(f.dir, "blocked.go")
	if err := os.WriteFile(blocked, []byte("package fixture\n"), 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(blocked, 0644) })
	return f
}

func capturePublicationFiles(t *testing.T, f *captureFixture) CaptureSummary {
	t.Helper()
	store := checkpoint.Store{DB: f.db}
	sum, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{CheckpointStore: &store, IgnoreChecker: f.ig, SortByPath: true})
	if err != nil && !sum.Partial {
		t.Fatal(err)
	}
	return sum
}

func writePublicationFile(t *testing.T, f *captureFixture, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(f.dir, name), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func replayPublicationPage(t *testing.T, f *captureFixture, drain *state.PublicationDrain) ReplaySummary {
	t.Helper()
	sum, err := Replay(context.Background(), f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyEvent, Limit: 1,
		RequireCompletedCheckpoint: true, PublicationDrain: drain,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sum.BaseHead != "" {
		f.cctx.BaseHead = sum.BaseHead
	}
	return sum
}

func TestCapturePublicationPagesPastHeldPrefixAfterRestart(t *testing.T) {
	f := newPartialPublicationFixture(t)
	ctx := context.Background()
	writePublicationFile(t, f, "a.go", "package fixture\n")
	writePublicationFile(t, f, "b.go", "package fixture\n")
	writePublicationFile(t, f, "z.md", "Independent documentation.\n")
	if sum := capturePublicationFiles(t, f); !sum.Partial || sum.EventsAppended != 3 {
		t.Fatalf("capture=%+v", sum)
	}
	originalIgnore, err := os.ReadFile(filepath.Join(f.dir, ".gitignore"))
	if err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, ".gitignore", "deliberately staged\n")
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", ".gitignore"); err != nil {
		t.Fatal(err)
	}
	staged, err := git.LsFilesStaged(ctx, f.dir, ".gitignore")
	if err != nil {
		t.Fatal(err)
	}
	if sum := replayPublicationPage(t, f, nil); sum.Published != 0 || !sum.HasMore || sum.Disposition != ReplayDispositionTransientWait {
		t.Fatalf("first held page=%+v", sum)
	}
	// Reopen the writer to prove the scan cursor survives a worker restart.
	dbPath := f.db.Path()
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	f.db, err = state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.db.Close() })
	if sum := replayPublicationPage(t, f, nil); sum.Published != 0 || !sum.HasMore {
		t.Fatalf("second held page=%+v", sum)
	}
	if sum := replayPublicationPage(t, f, nil); sum.Published != 1 || sum.HasMore {
		t.Fatalf("independent document=%+v", sum)
	}
	remaining, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(remaining) != 2 || remaining[0].Path != "a.go" || remaining[1].Path != "b.go" {
		t.Fatalf("protected source=%+v err=%v", remaining, err)
	}
	if got, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "show", "HEAD:z.md"); err != nil || string(got) != "Independent documentation.\n" {
		t.Fatalf("published document=%q err=%v", got, err)
	}
	after, err := git.LsFilesStaged(ctx, f.dir, ".gitignore")
	if err != nil || !reflect.DeepEqual(staged, after) {
		t.Fatalf("user staging changed: before=%+v after=%+v err=%v", staged, after, err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, ".gitignore"), originalIgnore, 0644); err != nil {
		t.Fatal(err)
	}
	// The completed rotation wraps instead of overlooking newly captured work.
	writePublicationFile(t, f, "later.md", "Another independent document.\n")
	capturePublicationFiles(t, f)
	for i := 0; i < 2; i++ {
		if sum := replayPublicationPage(t, f, nil); sum.Published != 0 || !sum.HasMore {
			t.Fatalf("wrapped source page %d=%+v", i, sum)
		}
	}
	if sum := replayPublicationPage(t, f, nil); sum.Published != 1 {
		t.Fatalf("new capture=%+v", sum)
	}
	if got, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "show", "HEAD:later.md"); err != nil || string(got) != "Another independent document.\n" {
		t.Fatalf("new document=%q err=%v", got, err)
	}
}

func TestCapturePublicationPagingPreservesHeldPathOrder(t *testing.T) {
	f := newPartialPublicationFixture(t)
	unchanged := "References blocked.go.\n" + strings.Repeat("Unchanged paragraph.\n", 50)
	writePublicationFile(t, f, "a.md", unchanged+"Old trailing section.\n")
	capturePublicationFiles(t, f)
	writePublicationFile(t, f, "b.md", "Independent document.\n")
	capturePublicationFiles(t, f)
	// This edit's diff has no failed-path reference, but its create predecessor
	// is still held. Publishing the modify first would break before-state order.
	writePublicationFile(t, f, "a.md", unchanged+"New trailing section.\n")
	capturePublicationFiles(t, f)
	if sum := replayPublicationPage(t, f, nil); sum.Published != 0 || sum.Conflicts != 0 {
		t.Fatalf("referencing predecessor=%+v", sum)
	}
	if sum := replayPublicationPage(t, f, nil); sum.Published != 1 {
		t.Fatalf("independent document=%+v", sum)
	}
	if sum := replayPublicationPage(t, f, nil); sum.Published != 0 || sum.Conflicts != 0 || sum.Disposition != ReplayDispositionTransientWait {
		t.Fatalf("same-path successor overtook held revision=%+v", sum)
	}
	remaining, err := state.PendingEvents(context.Background(), f.db, 0)
	if err != nil || len(remaining) != 2 || remaining[0].Path != "a.md" || remaining[1].Path != "a.md" {
		t.Fatalf("ordered pending revisions=%+v err=%v", remaining, err)
	}
}

func TestCapturePublicationPagingKeepsPathOrderWhenCoverageRecovers(t *testing.T) {
	f := newPartialPublicationFixture(t)
	ctx := context.Background()
	unchanged := "References blocked.go.\n" + strings.Repeat("Unchanged paragraph.\n", 50)
	writePublicationFile(t, f, "a.md", unchanged+"Old trailing section.\n")
	capturePublicationFiles(t, f)
	writePublicationFile(t, f, "b.md", "Independent document.\n")
	capturePublicationFiles(t, f)
	writePublicationFile(t, f, "a.md", unchanged+"New trailing section.\n")
	capturePublicationFiles(t, f)
	if sum := replayPublicationPage(t, f, nil); sum.Published != 0 || !sum.HasMore {
		t.Fatalf("held create=%+v", sum)
	}
	store := checkpoint.Store{DB: f.db}
	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyEvent, Limit: 2, RequireCompletedCheckpoint: true,
		MessageFn: func(ctx context.Context, event EventContext) (string, error) {
			if event.Event.Path != "b.md" {
				t.Fatalf("successor overtook held create: %s", event.Event.Path)
			}
			if err := os.Chmod(filepath.Join(f.dir, "blocked.go"), 0644); err != nil {
				t.Fatal(err)
			}
			coverage, err := ProtectWorktree(ctx, f.dir, f.db, f.cctx, CaptureOpts{CheckpointStore: &store, IgnoreChecker: f.ig})
			if err != nil || !coverage.Protected {
				t.Fatalf("restored coverage=%+v err=%v", coverage, err)
			}
			return DeterministicMessage(ctx, event)
		},
	})
	if err != nil || sum.Published != 1 || sum.Conflicts != 0 || sum.Skipped {
		t.Fatalf("recovered page=%+v err=%v", sum, err)
	}
	f.cctx.BaseHead = sum.BaseHead
	for i := 0; i < 2; i++ {
		if sum := replayPublicationPage(t, f, nil); sum.Published != 1 || sum.Conflicts != 0 {
			t.Fatalf("complete-coverage FIFO step %d=%+v", i, sum)
		}
	}
	if got, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "show", "HEAD:a.md"); err != nil || string(got) != unchanged+"New trailing section.\n" {
		t.Fatalf("final ordered revision=%q err=%v", got, err)
	}
}

func TestCapturePublicationPageRetainsCheckpointAndTerminalBarriers(t *testing.T) {
	f := newPartialPublicationFixture(t)
	ctx := context.Background()
	writePublicationFile(t, f, "a.go", "package fixture\n")
	writePublicationFile(t, f, "b.go", "package fixture\n")
	writePublicationFile(t, f, "z.md", "Independent document.\n")
	capturePublicationFiles(t, f)
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 3 {
		t.Fatalf("captures=%+v err=%v", pending, err)
	}
	seq, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		BaseHead: f.cctx.BaseHead, Operation: "create", Path: "unowned.md", Fidelity: "exact",
	}, []state.CaptureOp{{Op: "create", Path: "unowned.md", Fidelity: "exact"}})
	if err != nil {
		t.Fatal(err)
	}
	page, err := state.PendingEventsAfter(ctx, f.db, pending[0].Seq, 10, true)
	if err != nil || len(page) != 2 || page[0].Seq != pending[1].Seq || page[1].Seq != pending[2].Seq {
		t.Fatalf("checkpoint page included unowned seq=%d: %+v err=%v", seq, page, err)
	}
	if err := state.MarkEventPublished(ctx, f.db, pending[1].Seq, state.EventStateFailed, sql.NullString{}, sql.NullString{}, sql.NullString{}, 1); err != nil {
		t.Fatal(err)
	}
	page, err = state.PendingEventsAfter(ctx, f.db, pending[0].Seq, 10, true)
	if err != nil || len(page) != 0 {
		t.Fatalf("paging crossed terminal predecessor=%+v err=%v", page, err)
	}
}

func TestCapturePublicationRechecksCoverageBetweenEvents(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires enforced file permissions")
	}
	f := newCaptureFixture(t)
	ctx := context.Background()
	capturePublicationFiles(t, f)
	baseline, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyEvent})
	if err != nil {
		t.Fatal(err)
	}
	f.cctx.BaseHead = baseline.BaseHead
	writePublicationFile(t, f, "a.md", "Independent document.\n")
	writePublicationFile(t, f, "b.go", "package fixture\n")
	writePublicationFile(t, f, "blocked.go", "package fixture\n")
	if sum := capturePublicationFiles(t, f); !sum.Protected {
		t.Fatalf("initial complete capture=%+v", sum)
	}
	blocked := filepath.Join(f.dir, "blocked.go")
	t.Cleanup(func() { _ = os.Chmod(blocked, 0644) })
	store := checkpoint.Store{DB: f.db}
	calls := 0
	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyEvent, RequireCompletedCheckpoint: true,
		MessageFn: func(ctx context.Context, event EventContext) (string, error) {
			calls++
			if event.Event.Path != "a.md" {
				t.Fatalf("source passed stale coverage proof: %s", event.Event.Path)
			}
			// Protection can run while message generation waits. The next
			// queued event must observe its newly incomplete checkpoint.
			if err := os.Chmod(blocked, 0); err != nil {
				t.Fatal(err)
			}
			partial, err := ProtectWorktree(ctx, f.dir, f.db, f.cctx, CaptureOpts{CheckpointStore: &store, IgnoreChecker: f.ig})
			if err == nil || !partial.Partial {
				t.Fatalf("changed coverage=%+v err=%v", partial, err)
			}
			return DeterministicMessage(ctx, event)
		},
	})
	if err != nil || sum.Published != 1 || sum.Conflicts != 0 || sum.Skipped || calls != 1 {
		t.Fatalf("coverage change replay=%+v calls=%d err=%v", sum, calls, err)
	}
	remaining, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(remaining) != 2 || remaining[0].Path != "b.go" || remaining[1].Path != "blocked.go" {
		t.Fatalf("source protection=%+v err=%v", remaining, err)
	}
}

func TestCapturePublicationPagingKeepsFrozenTarget(t *testing.T) {
	f := newPartialPublicationFixture(t)
	writePublicationFile(t, f, "a.go", "package fixture\n")
	writePublicationFile(t, f, "target.md", "Independent target.\n")
	capturePublicationFiles(t, f)
	pending, err := state.PendingEvents(context.Background(), f.db, 0)
	if err != nil || len(pending) != 2 {
		t.Fatalf("target=%+v err=%v", pending, err)
	}
	drain := state.PublicationDrain{ID: "frozen-event-target", BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		EventSeqs: []int64{pending[0].Seq, pending[1].Seq}}
	writePublicationFile(t, f, "later.md", "Outside the target.\n")
	capturePublicationFiles(t, f)
	if sum := replayPublicationPage(t, f, &drain); sum.Published != 0 || !sum.HasMore {
		t.Fatalf("frozen held prefix=%+v", sum)
	}
	if sum := replayPublicationPage(t, f, &drain); sum.Published != 1 {
		t.Fatalf("frozen independent document=%+v", sum)
	}
	remaining, err := state.PendingEvents(context.Background(), f.db, 0)
	if err != nil || len(remaining) != 2 || remaining[0].Path != "a.go" || remaining[1].Path != "later.md" {
		t.Fatalf("target expanded or source lost=%+v err=%v", remaining, err)
	}
}

func TestCapturePublicationScanCursorScopesIdentity(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	key, cursor, err := loadCaptureEventScanCursor(ctx, f.db, f.dir, f.cctx, "drain:first")
	if err != nil {
		t.Fatal(err)
	}
	cursor.Seq = 99
	if err := state.MetaSetJSON(ctx, f.db, key, cursor); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, repo, branch, target string
		generation                 int64
	}{
		{"branch", f.dir, "refs/heads/other", "drain:first", f.cctx.BranchGeneration},
		{"generation", f.dir, f.cctx.BranchRef, "drain:first", f.cctx.BranchGeneration + 1},
		{"target", f.dir, f.cctx.BranchRef, "drain:second", f.cctx.BranchGeneration},
		{"worktree", filepath.Join(f.dir, "other-worktree"), f.cctx.BranchRef, "drain:first", f.cctx.BranchGeneration},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, got, err := loadCaptureEventScanCursor(ctx, f.db, test.repo, CaptureContext{BranchRef: test.branch, BranchGeneration: test.generation}, test.target)
			if err != nil || got.Seq != 0 {
				t.Fatalf("unrelated identity reused cursor=%+v err=%v", got, err)
			}
		})
	}
}

func TestCapturePublicationHoldRejectsSourceBeforeReadingEvidence(t *testing.T) {
	bin := t.TempDir()
	marker := filepath.Join(bin, "git-was-called")
	quotedMarker := "'" + strings.ReplaceAll(marker, "'", "'\\''") + "'"
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/sh\n: > "+quotedMarker+"\nexit 1\n"), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	issues := []state.CheckpointCaptureIssue{{Path: "blocked.go", Reason: "unreadable"}}
	hold, err := capturePublicationHoldOpsForIssues(context.Background(), bin, []state.CaptureOp{{Op: "create", Path: "source.go", AfterOID: sql.NullString{String: strings.Repeat("a", 40), Valid: true}}}, []string{"source.go"}, issues)
	if err != nil || hold != "candidate independence from incomplete capture is unproven" {
		t.Fatalf("source hold=%q err=%v", hold, err)
	}
	for _, op := range []string{"delete", "rename"} {
		hold, err := capturePublicationHoldOpsForIssues(context.Background(), "/does-not-exist", []state.CaptureOp{{Op: op, Path: "document.md"}}, []string{"document.md"}, issues)
		if err != nil || hold != "capture may be missing a rename or deletion companion" {
			t.Fatalf("%s hold=%q err=%v", op, hold, err)
		}
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("held captures invoked Git to build unused evidence: %v", err)
	}
}
