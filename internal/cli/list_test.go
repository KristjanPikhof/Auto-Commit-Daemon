package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestListUseWatchMode(t *testing.T) {
	t.Parallel()
	if listUseWatchMode(nil, true, false) {
		t.Fatal("--once should disable watch")
	}
	if !listUseWatchMode(nil, false, true) {
		t.Fatal("explicit --watch should enable watch without stdout")
	}
	if listUseWatchMode(nil, false, false) {
		t.Fatal("nil stdout without flags should not watch")
	}
}

func TestList_WatchAndJSONRejected(t *testing.T) {
	withIsolatedHome(t)

	cmd := newRootCmd()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"list", "--watch", "--json"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for --watch --json")
	}
	if !strings.Contains(err.Error(), "--watch does not support --json") {
		t.Fatalf("error=%q, want substring --watch does not support --json", err.Error())
	}
}

func TestListInteractiveRoutesToRepoManager(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	_ = db.Close()
	registerRepo(t, roots, repo, stateDB, "")

	cmd := newRootCmd()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetIn(strings.NewReader("q\n"))
	cmd.SetArgs([]string{"list", "--interactive"})
	if err := cmd.ExecuteContext(ctx); err != nil {
		t.Fatalf("list interactive: %v\nstderr:%s\nstdout:%s", err, stderr.String(), stdout.String())
	}
	for _, want := range []string{"STATE", "repo manage>", "done"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("interactive list output missing %q:\n%s", want, stdout.String())
		}
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	rec, ok := reg.FindRepo(repo, stateDB)
	if !ok || rec.LifecycleDisabled() {
		t.Fatalf("list --interactive q mutated repo: ok=%v rec=%+v", ok, rec)
	}
}

func TestList_OnceProducesSingleSnapshot(t *testing.T) {
	withIsolatedHome(t)

	cmd := newRootCmd()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"list", "--once"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("acd list --once: %v", err)
	}
	got := stdout.String()
	if strings.Contains(got, "Updated:") {
		t.Fatalf("--once should not use watch frames:\n%s", got)
	}
	for _, want := range []string{"REPO", "SAFE", "MODE", "QUEUE", "TARGET", "LAST MOVE", "PHASE", "STATUS"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected compact table header %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "DAEMON") {
		t.Fatalf("expected compact table header:\n%s", got)
	}
}

func TestList_JSONOnTTYUsesOneShot(t *testing.T) {
	withIsolatedHome(t)

	cmd := newRootCmd()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"list", "--json"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("acd list --json: %v", err)
	}
	if strings.Contains(stdout.String(), "Updated:") {
		t.Fatalf("json must not select watch mode:\n%s", stdout.String())
	}
	var got struct {
		OK    bool         `json:"ok"`
		State productState `json:"state"`
		Data  struct {
			Repos []productListEntry `json:"repos"`
		} `json:"data"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("expected JSON output, got: %q err=%v", stdout.String(), err)
	}
	if !got.OK || got.State != productStateProtected || got.Data.Repos == nil {
		t.Fatalf("unexpected product envelope: %+v", got)
	}
}

func TestSummarizeRepoCountsOnlyLiveClients(t *testing.T) {
	withIsolatedHome(t)
	_, dbPath, db := makeRepoStateDB(t)
	ctx := context.Background()
	now := time.Now()
	for _, client := range []state.Client{
		{SessionID: "expired", Harness: "codex", LastSeenTS: float64(now.Add(-2 * time.Hour).Unix())},
		{SessionID: "live", Harness: "codex", LastSeenTS: float64(now.Unix())},
	} {
		if err := state.RegisterClient(ctx, db, client); err != nil {
			t.Fatal(err)
		}
	}
	summary, err := summarizeRepo(ctx, dbPath, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if summary.clients != 1 {
		t.Fatalf("live clients=%d, want 1", summary.clients)
	}
}

func TestSummarizeRepoIntentWait(t *testing.T) {
	for _, settle := range []bool{false, true} {
		name := "batch"
		if settle {
			name = "settle"
		}
		t.Run(name, func(t *testing.T) {
			withIsolatedHome(t)
			_, dbPath, db := makeRepoStateDB(t)
			ctx := context.Background()
			now := time.Now()
			minPending, reason, maxWait := "3", "skipped_due_intent_batch_wait", int64(120)
			if settle {
				minPending, reason, maxWait = "2", "skipped_due_intent_settle_window", 60
			}
			if err := state.MetaSetMany(ctx, db, map[string]string{
				"commit.strategy": "intent", "intent.window": "2",
				"intent.min_pending": minPending, "intent.settle_window": "1m",
				"intent.max_pending_age": "2m", "intent.defer_limit": "1",
			}); err != nil {
				t.Fatal(err)
			}
			appendIntentPendingEvent(t, ctx, db, "a.go", float64(now.Unix())-10)
			appendIntentPendingEvent(t, ctx, db, "b.go", float64(now.Unix())-5)
			summary, err := summarizeRepo(ctx, dbPath, now, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			wait := summary.intentWait
			if wait == nil || wait.reason != reason || wait.visiblePending != 2 ||
				wait.waitSeconds <= 0 || wait.waitSeconds > maxWait {
				t.Fatalf("intent wait=%+v, want %s with 2 captures and 1..%d seconds", wait, reason, maxWait)
			}
		})
	}
}

func TestProductListDisabledRepositoryIsReadOnly(t *testing.T) {
	for _, missingDB := range []bool{false, true} {
		name := "existing state"
		if missingDB {
			name = "missing state"
		}
		t.Run(name, func(t *testing.T) {
			roots := withIsolatedHome(t)
			repo, dbPath, db := makeRepoStateDB(t)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			registerRepo(t, roots, repo, dbPath, "codex")
			disableRepoLifecycleForListTest(t, roots, repo)
			before := fileDigest(t, dbPath)
			if missingDB {
				if err := os.Remove(dbPath); err != nil {
					t.Fatal(err)
				}
			}
			data, _, err := collectProductList(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(data.Repos) != 0 {
				t.Fatalf("disabled repository appeared in list: %+v", data.Repos)
			}
			if missingDB {
				if fileExists(dbPath) {
					t.Fatal("list recreated disabled repository state")
				}
			} else if after := fileDigest(t, dbPath); after != before {
				t.Fatal("list changed disabled repository state")
			}
		})
	}
}

func disableRepoLifecycleForListTest(t *testing.T, roots paths.Roots, repo string) {
	t.Helper()
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		for i := range reg.Repos {
			if central.SameRepoPath(reg.Repos[i].Path, repo) {
				reg.Repos[i].LifecycleState = central.RepoLifecycleDisabled
				reg.Repos[i].LifecycleUpdatedTS = time.Now().Unix()
				return nil
			}
		}
		t.Fatalf("repo %s not registered", repo)
		return nil
	}); err != nil {
		t.Fatalf("disable lifecycle: %v", err)
	}
}

func writePauseMarkerForStateDB(t *testing.T, stateDBPath string, marker pausepkg.Marker) {
	t.Helper()
	gitDir := filepath.Dir(filepath.Dir(stateDBPath))
	if _, err := pausepkg.Write(pausepkg.Path(gitDir), marker, true); err != nil {
		t.Fatalf("write pause marker: %v", err)
	}
}

func TestListWatchIntervalFlagParsesGoDuration(t *testing.T) {
	withIsolatedHome(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := newRootCmd()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"list", "--watch", "--interval", "250ms"})
	if err := cmd.ExecuteContext(ctx); err != nil {
		t.Fatalf("ExecuteContext valid duration: %v", err)
	}

	cmd = newRootCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"list", "--watch", "--interval", "250bad"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("Execute invalid duration: got nil, want error")
	}
}

type cancelAfterFramesWriter struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	done   chan struct{}
	cancel context.CancelFunc
	frames int
	want   int
	once   sync.Once
}

func newCancelAfterFramesWriter(cancel context.CancelFunc, want int) *cancelAfterFramesWriter {
	return &cancelAfterFramesWriter{
		done:   make(chan struct{}),
		cancel: cancel,
		want:   want,
	}
}

func (w *cancelAfterFramesWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buf.Write(p)
	w.frames += strings.Count(string(p), "Updated:")
	if w.frames >= w.want {
		w.once.Do(func() {
			w.cancel()
			close(w.done)
		})
	}
	return n, err
}

func (w *cancelAfterFramesWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func (w *cancelAfterFramesWriter) frameCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.frames
}

func TestPauseState_GitDirDerivation_Pinned(t *testing.T) {
	cases := []struct {
		name    string
		stateDB string
		want    string
	}{
		{
			name:    "unix",
			stateDB: "/repo/.git/acd/state.db",
			want:    "/repo/.git",
		},
		{
			name:    "deeper-path",
			stateDB: "/srv/repos/foo/.git/acd/state.db",
			want:    "/srv/repos/foo/.git",
		},
		{
			name:    "linked-worktree",
			stateDB: "/repo/.git/worktrees/wt/acd/state.db",
			want:    "/repo/.git/worktrees/wt",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := gitDirFromStateDB(tc.stateDB)
			if got != tc.want {
				t.Fatalf("gitDirFromStateDB(%q)=%q want %q", tc.stateDB, got, tc.want)
			}
		})
	}
}
