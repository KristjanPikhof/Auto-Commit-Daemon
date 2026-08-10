package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestProductListOnceNeedsActionHumanRendersThenExitsThree(t *testing.T) {
	registerProductListNeedsActionRepo(t)

	var out bytes.Buffer
	err := runProductListOnce(context.Background(), &out, false, false)
	if ExitCode(err) != ExitActionRequired || !ErrorRendered(err) {
		t.Fatalf("exit=%d rendered=%v err=%v, want rendered exit %d", ExitCode(err), ErrorRendered(err), err, ExitActionRequired)
	}
	for _, want := range []string{"REPOSITORY", "ACTION", "needs_action", "yes"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("human output missing %q:\n%s", want, out.String())
		}
	}
}

func TestProductListOnceNeedsActionJSONRendersThenExitsThree(t *testing.T) {
	repo := registerProductListNeedsActionRepo(t)

	var out bytes.Buffer
	err := runProductListOnce(context.Background(), &out, true, false)
	if ExitCode(err) != ExitActionRequired || !ErrorRendered(err) {
		t.Fatalf("exit=%d rendered=%v err=%v, want rendered exit %d", ExitCode(err), ErrorRendered(err), err, ExitActionRequired)
	}
	var got struct {
		OK    bool            `json:"ok"`
		State productState    `json:"state"`
		Data  productListData `json:"data"`
		Error *productError   `json:"error"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal JSON: %v\n%s", err, out.String())
	}
	if !got.OK || got.State != productStateNeedsAction || got.Error != nil {
		t.Fatalf("unexpected envelope: %+v", got)
	}
	if len(got.Data.Repos) != 1 {
		t.Fatalf("repos=%d, want 1: %+v", len(got.Data.Repos), got.Data.Repos)
	}
	entry := got.Data.Repos[0]
	if entry.Repo != repo || !entry.Enabled || !entry.ActionRequired || entry.State != productStateNeedsAction {
		t.Fatalf("unexpected repository aggregate: %+v", entry)
	}
}

func TestProductListOnceOffRepositoryRequiresAction(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := registerProductListOffRepo(t, roots, materializeTestRepo(t, false))

	var out bytes.Buffer
	err := runProductListOnce(context.Background(), &out, true, false)
	if ExitCode(err) != ExitActionRequired || !ErrorRendered(err) {
		t.Fatalf("exit=%d rendered=%v err=%v, want rendered exit %d", ExitCode(err), ErrorRendered(err), err, ExitActionRequired)
	}
	var got struct {
		State productState    `json:"state"`
		Data  productListData `json:"data"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal JSON: %v\n%s", err, out.String())
	}
	if got.State != productStateNeedsAction || len(got.Data.Repos) != 1 {
		t.Fatalf("unexpected aggregate: %+v", got)
	}
	entry := got.Data.Repos[0]
	if entry.Repo != repo || entry.Enabled || !entry.ActionRequired || entry.State != productStateNeedsAction {
		t.Fatalf("off repository was not classified as action-required: %+v", entry)
	}
}

func TestProductListNeedsActionDominatesMixedStates(t *testing.T) {
	roots := withIsolatedHome(t)
	registerProductListOffRepo(t, roots, materializeTestRepo(t, false))
	registerProductListNeedsActionRepoWithRoots(t, roots)

	data, stateName, err := collectProductList(context.Background())
	if err != nil {
		t.Fatalf("collect product list: %v", err)
	}
	if stateName != productStateNeedsAction || len(data.Repos) != 2 {
		t.Fatalf("state=%q repos=%d, want needs_action with two repos", stateName, len(data.Repos))
	}
	for _, entry := range data.Repos {
		if !entry.ActionRequired || entry.State != productStateNeedsAction {
			t.Fatalf("entry did not retain needs_action classification: %+v", entry)
		}
	}
}

func TestProductListGitStateLabels(t *testing.T) {
	entries := []productListEntry{
		{Repo: "/published", Enabled: true, Protected: true, Published: true, State: productStateProtected},
		{Repo: "/off", ActionRequired: true, State: productStateNeedsAction},
		{Repo: "/blocked", Enabled: true, ActionRequired: true, State: productStateNeedsAction},
		{Repo: "/unprotected", Enabled: true, State: productStateProtected},
		{Repo: "/waiting", Enabled: true, Protected: true, State: productStateWaiting},
	}

	var out bytes.Buffer
	if err := renderProductListTable(&out, entries, false); err != nil {
		t.Fatalf("render table: %v", err)
	}
	for _, row := range []struct {
		repo, gitState string
	}{
		{"/published", "published"},
		{"/off", "off"},
		{"/blocked", "blocked"},
		{"/unprotected", "blocked"},
		{"/waiting", "not-published"},
	} {
		line := productListLineForRepo(t, out.String(), row.repo)
		if !strings.Contains(line, row.gitState) {
			t.Fatalf("row %q missing Git state %q:\n%s", row.repo, row.gitState, line)
		}
	}
}

func TestProductListNextActionTargetsQuotedRepository(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := materializeProductListRepoWithSpaces(t)
	registerProductListOffRepo(t, roots, repo)

	data, _, err := collectProductList(context.Background())
	if err != nil {
		t.Fatalf("collect product list: %v", err)
	}
	if len(data.Repos) != 1 {
		t.Fatalf("repos=%d, want 1", len(data.Repos))
	}
	want := "--repo " + productListShellQuote(repo)
	if !strings.Contains(data.Repos[0].NextAction, want) {
		t.Fatalf("next action=%q, want targeted quoted repository %q", data.Repos[0].NextAction, want)
	}
	if got := productListTargetAction("Run `acd doctor`.", "/tmp/repo with ' quote"); got != "Run `acd doctor --repo '/tmp/repo with '\"'\"' quote'`." {
		t.Fatalf("single-quote escaping drifted: %q", got)
	}
}

func TestProductListWatchNeedsActionContinuesRefreshing(t *testing.T) {
	registerProductListNeedsActionRepo(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := &productListFrameWriter{cancel: cancel, want: 2}

	if err := runProductListWatch(ctx, out, time.Millisecond, false); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if frames := out.frameCount(); frames < 2 {
		t.Fatalf("frames=%d, want at least 2", frames)
	}
	if !strings.Contains(out.String(), "needs_action") {
		t.Fatalf("watch output missing needs_action:\n%s", out.String())
	}
}

func TestProductListPersistentFlagsAreHandled(t *testing.T) {
	t.Run("repo rejected", func(t *testing.T) {
		withIsolatedHome(t)
		cmd := newRootCmd()
		cmd.SetArgs([]string{"list", "--once", "--repo", t.TempDir()})
		if err := cmd.Execute(); ExitCode(err) != ExitInvalid {
			t.Fatalf("exit=%d err=%v, want %d", ExitCode(err), err, ExitInvalid)
		}
	})

	t.Run("json honored", func(t *testing.T) {
		withIsolatedHome(t)
		cmd := newRootCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetArgs([]string{"list", "--json"})
		if err := cmd.Execute(); err != nil {
			t.Fatalf("list --json: %v", err)
		}
		var envelope productEnvelope
		if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
			t.Fatalf("JSON output: %v\n%s", err, out.String())
		}
	})

	t.Run("quiet preserves final result", func(t *testing.T) {
		withIsolatedHome(t)
		cmd := newRootCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetArgs([]string{"list", "--once", "--quiet"})
		if err := cmd.Execute(); err != nil {
			t.Fatalf("list --quiet: %v", err)
		}
		if !strings.Contains(out.String(), "REPOSITORY") {
			t.Fatalf("quiet suppressed the final result:\n%s", out.String())
		}
	})

	t.Run("log level validated", func(t *testing.T) {
		withIsolatedHome(t)
		cmd := newRootCmd()
		cmd.SetArgs([]string{"list", "--once", "--log-level", "loud"})
		if err := cmd.Execute(); ExitCode(err) != ExitInvalid {
			t.Fatalf("exit=%d err=%v, want %d", ExitCode(err), err, ExitInvalid)
		}
	})
}

func registerProductListNeedsActionRepo(t *testing.T) string {
	t.Helper()
	roots := withIsolatedHome(t)
	return registerProductListNeedsActionRepoWithRoots(t, roots)
}

func registerProductListNeedsActionRepoWithRoots(t *testing.T, roots paths.Roots) string {
	t.Helper()
	repo := materializeTestRepo(t, false)
	missingStateDB := filepath.Join(repo, ".git", "acd", "missing.db")
	registerRepo(t, roots, repo, missingStateDB, "codex")
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		for i := range registry.Repos {
			if registry.Repos[i].Path == repo {
				registry.Repos[i].RepositoryID = "repository-id"
				registry.Repos[i].WorktreeID = "worktree-id"
				return nil
			}
		}
		return fmt.Errorf("registered repository %s not found", repo)
	}); err != nil {
		t.Fatalf("update registry identity: %v", err)
	}
	return repo
}

func registerProductListOffRepo(t *testing.T, roots paths.Roots, repo string) string {
	t.Helper()
	registerRepo(t, roots, repo, filepath.Join(repo, ".git", "acd", "missing.db"), "codex")
	return repo
}

func materializeProductListRepoWithSpaces(t *testing.T) string {
	t.Helper()
	source := materializeTestRepo(t, false)
	target := filepath.Join(t.TempDir(), "repo with spaces")
	if err := os.Rename(source, target); err != nil {
		t.Fatalf("move repository to spaced path: %v", err)
	}
	realTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatalf("canonicalize spaced repository path: %v", err)
	}
	return realTarget
}

func productListLineForRepo(t *testing.T, output, repo string) string {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, repo+" ") {
			return line
		}
	}
	t.Fatalf("repository row %q missing:\n%s", repo, output)
	return ""
}

type productListFrameWriter struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	cancel context.CancelFunc
	frames int
	want   int
}

func (w *productListFrameWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buf.Write(p)
	w.frames += strings.Count(string(p), "Updated:")
	if w.frames >= w.want {
		w.cancel()
	}
	return n, err
}

func (w *productListFrameWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func (w *productListFrameWriter) frameCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.frames
}
