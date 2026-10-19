package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
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
	for _, want := range []string{"REPO", "SAFE", "MODE", "WORK", "TARGET", "LAST MOVE", "PHASE", "STATUS"} {
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
