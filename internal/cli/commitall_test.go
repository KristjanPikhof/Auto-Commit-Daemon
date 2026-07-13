package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// TestCommitAll_FlagsRegistered ensures the command surfaces all required
// flags. Full behavior coverage lives in the t-unit task.
func TestCommitAll_FlagsRegistered(t *testing.T) {
	cmd := newCommitAllCmd()
	for _, name := range []string{"yes", "dry-run"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Fatalf("commit-all: missing flag --%s", name)
		}
	}
}

// TestCommitAll_HelpExposesCommand verifies the root command tree wires the
// command in and the help text mentions it.
func TestCommitAll_HelpExposesCommand(t *testing.T) {
	root := newRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"--help"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("root help: %v", err)
	}
	if !strings.Contains(out.String(), "acd commit-all") {
		t.Fatalf("root help missing commit-all entry:\n%s", out.String())
	}
}

// TestCommitAll_RefusesDetachedHEAD pins the detached-HEAD refusal guard.
func TestCommitAll_RefusesDetachedHEAD(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "checkout", "--detach", head); err != nil {
		t.Fatalf("git checkout --detach: %v", err)
	}

	var out bytes.Buffer
	err = runCommitAll(ctx, &out, nil, repo, true, false, true)
	if err == nil {
		t.Fatalf("runCommitAll on detached HEAD returned nil; want refusal error")
	}
	if !strings.Contains(err.Error(), "detached HEAD") {
		t.Fatalf("expected detached HEAD message, got: %v", err)
	}
}

// TestCommitAll_RefusesGitOperationInProgress pins the rebase/merge marker
// refusal guard.
func TestCommitAll_RefusesGitOperationInProgress(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	gitDir := filepath.Join(repo, ".git")
	if err := os.WriteFile(filepath.Join(gitDir, "MERGE_HEAD"), []byte("deadbeef\n"), 0o644); err != nil {
		t.Fatalf("write MERGE_HEAD: %v", err)
	}

	var out bytes.Buffer
	err := runCommitAll(ctx, &out, nil, repo, true, false, true)
	if err == nil {
		t.Fatalf("expected refusal while MERGE_HEAD is present")
	}
	if !strings.Contains(err.Error(), "git operation") {
		t.Fatalf("expected git operation refusal, got: %v", err)
	}
}

// TestCommitAll_RefusesManualPauseMarker pins the pause-marker refusal guard.
func TestCommitAll_RefusesManualPauseMarker(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	gitDir := filepath.Join(repo, ".git")
	markerPath := pausepkg.Path(gitDir)
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
		t.Fatalf("mkdir marker parent: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte(`{"reason":"test","set_at":"now","set_by":"test"}`), 0o600); err != nil {
		t.Fatalf("write pause marker: %v", err)
	}

	var out bytes.Buffer
	err := runCommitAll(ctx, &out, nil, repo, true, false, true)
	if err == nil {
		t.Fatalf("expected refusal while manual pause marker is present")
	}
	if !strings.Contains(err.Error(), "pause") {
		t.Fatalf("expected pause refusal, got: %v", err)
	}
}

// TestCommitAll_RefusesWhileDaemonLockHeld pins the daemon-alive refusal
// guard. Holding daemon.lock simulates a live daemon.
func TestCommitAll_RefusesWhileDaemonLockHeld(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "dirty.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	held, err := daemon.AcquireDaemonLock(filepath.Join(repo, ".git"))
	if err != nil {
		t.Fatalf("pre-acquire daemon.lock: %v", err)
	}
	defer func() { _ = held.Release() }()

	var out bytes.Buffer
	err = runCommitAll(ctx, &out, nil, repo, true, false, true)
	if err == nil {
		t.Fatalf("expected refusal while daemon.lock is held")
	}
	if !strings.Contains(err.Error(), "daemon") {
		t.Fatalf("expected daemon-alive refusal, got: %v", err)
	}
}

func TestCommitAll_DryRunAllowedWhileDaemonLockHeld(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "dirty.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	held, err := daemon.AcquireDaemonLock(filepath.Join(repo, ".git"))
	if err != nil {
		t.Fatalf("pre-acquire daemon.lock: %v", err)
	}
	defer func() { _ = held.Release() }()

	var out bytes.Buffer
	if err := runCommitAll(ctx, &out, nil, repo, false, true, true); err != nil {
		t.Fatalf("runCommitAll dry-run with daemon.lock held: %v", err)
	}
	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !got.OK || !got.DryRun || got.PendingBefore == 0 {
		t.Fatalf("unexpected dry-run result: %+v", got)
	}
}

func TestCommitAll_CleanNoOpAllowedWhileDaemonLockHeld(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()
	held, err := daemon.AcquireDaemonLock(filepath.Join(repo, ".git"))
	if err != nil {
		t.Fatalf("pre-acquire daemon.lock: %v", err)
	}
	defer func() { _ = held.Release() }()

	var out bytes.Buffer
	if err := runCommitAll(ctx, &out, nil, repo, true, false, true); err != nil {
		t.Fatalf("runCommitAll clean no-op with daemon.lock held: %v", err)
	}
	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !got.OK || got.DryRun || got.PendingBefore != 0 {
		t.Fatalf("unexpected clean no-op result: %+v", got)
	}
}

// TestCommitAll_CleanWorktreeNoOp covers the success path on a clean worktree:
// capture finds no events, command exits zero with PendingBefore=0.
func TestCommitAll_CleanWorktreeNoOp(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()

	var out bytes.Buffer
	if err := runCommitAll(ctx, &out, nil, repo, true, false, true); err != nil {
		t.Fatalf("runCommitAll clean worktree: %v", err)
	}
	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !got.OK || got.PendingBefore != 0 {
		t.Fatalf("clean worktree result: %+v", got)
	}
	if got.Strategy == "" {
		t.Fatalf("strategy must be reported even on no-op, got: %+v", got)
	}
}

// TestCommitAll_DryRunNeverCommits pins that --dry-run leaves HEAD unchanged
// even with a dirty worktree.
func TestCommitAll_DryRunNeverCommits(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "new.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	headBefore, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	dbBefore, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum state.db before: %v", err)
	}
	refsBefore := commitAllRecoveryRefs(t, ctx, repo)

	var out bytes.Buffer
	if err := runCommitAll(ctx, &out, nil, repo, true, true, true); err != nil {
		t.Fatalf("runCommitAll dry-run: %v", err)
	}
	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.DryRun {
		t.Fatalf("expected DryRun=true, got %+v", got)
	}
	headAfter, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse after: %v", err)
	}
	if headAfter != headBefore {
		t.Fatalf("dry-run mutated HEAD: before=%s after=%s", headBefore, headAfter)
	}
	dbAfter, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum state.db after: %v", err)
	}
	if dbAfter != dbBefore {
		t.Fatalf("dry-run mutated state.db: before=%s after=%s", dbBefore, dbAfter)
	}
	if refsAfter := commitAllRecoveryRefs(t, ctx, repo); refsAfter != refsBefore {
		t.Fatalf("dry-run mutated recovery refs: before=%q after=%q", refsBefore, refsAfter)
	}
}

func TestCommitAll_PreviewAndDeclineDoNotBuildProvider(t *testing.T) {
	for _, tc := range []struct {
		name   string
		yes    bool
		dryRun bool
		input  string
	}{
		{name: "dry-run", yes: true, dryRun: true},
		{name: "decline", input: "n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ACD_AI_PROVIDER", "openai-compat")
			t.Setenv("ACD_AI_API_KEY", "sk-test")
			t.Setenv("ACD_AI_BASE_URL", "http://insecure.example/v1")
			repo, _, db := makeRegisteredGitRepoStateDB(t)
			_ = db.Close()
			if err := os.WriteFile(filepath.Join(repo, "dirty.txt"), []byte("dirty\n"), 0o644); err != nil {
				t.Fatalf("write dirty file: %v", err)
			}

			var out bytes.Buffer
			err := runCommitAll(context.Background(), &out, strings.NewReader(tc.input), repo, tc.yes, tc.dryRun, false)
			if tc.dryRun && err != nil {
				t.Fatalf("dry-run built invalid provider: %v", err)
			}
			if !tc.dryRun && !errors.Is(err, errCommitAllAborted) {
				t.Fatalf("decline built invalid provider: %v", err)
			}
		})
	}
}

// TestCommitAll_JSONRequiresYesWhenInteractive pins that --json without --yes
// refuses because there is no interactive prompt available.
func TestCommitAll_JSONRequiresYesWhenInteractive(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "new.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	dbBefore, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}
	refsBefore := commitAllRecoveryRefs(t, ctx, repo)

	var out bytes.Buffer
	err = runCommitAll(ctx, &out, nil, repo, false, false, true)
	if err == nil {
		t.Fatalf("expected --json without --yes to refuse")
	}
	if !strings.Contains(err.Error(), "--yes") {
		t.Fatalf("expected --yes prompt error, got: %v", err)
	}
	dbAfter, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if dbAfter != dbBefore || commitAllRecoveryRefs(t, ctx, repo) != refsBefore {
		t.Fatalf("--json refusal mutated state: db %s->%s refs %q->%q",
			dbBefore, dbAfter, refsBefore, commitAllRecoveryRefs(t, ctx, repo))
	}
}

// TestCommitAll_RefusesAllGitOperationMarkers covers every marker
// daemon.GitOperationInProgress recognises: rebase-merge/, rebase-apply/,
// MERGE_HEAD, CHERRY_PICK_HEAD, BISECT_LOG. One subtest per marker so a
// regression in any single guard fails its own row, not the whole table.
func TestCommitAll_RefusesAllGitOperationMarkers(t *testing.T) {
	cases := []struct {
		marker string
		dir    bool
	}{
		{marker: "rebase-merge", dir: true},
		{marker: "rebase-apply", dir: true},
		{marker: "MERGE_HEAD"},
		{marker: "CHERRY_PICK_HEAD"},
		{marker: "BISECT_LOG"},
	}
	for _, tc := range cases {
		t.Run(tc.marker, func(t *testing.T) {
			repo, _, db := makeRegisteredGitRepoStateDB(t)
			_ = db.Close()
			ctx := context.Background()
			gitDir := filepath.Join(repo, ".git")
			markerPath := filepath.Join(gitDir, tc.marker)
			if tc.dir {
				if err := os.MkdirAll(markerPath, 0o755); err != nil {
					t.Fatalf("mkdir %s: %v", tc.marker, err)
				}
			} else {
				if err := os.WriteFile(markerPath, []byte("x\n"), 0o644); err != nil {
					t.Fatalf("write %s: %v", tc.marker, err)
				}
			}
			var out bytes.Buffer
			err := runCommitAll(ctx, &out, nil, repo, true, false, true)
			if err == nil {
				t.Fatalf("%s: expected refusal", tc.marker)
			}
			if !strings.Contains(err.Error(), "git operation") {
				t.Fatalf("%s: expected git operation refusal, got: %v", tc.marker, err)
			}
		})
	}
}

// TestCommitAll_YesSkipsPromptDoesNotReadStdin confirms that --yes path
// never touches the stdin reader. We pass an io.Reader that fails on read;
// if the prompt path is taken, the read error would propagate. Combined
// with a clean worktree (PendingBefore=0) we exercise the early-return
// branch *and* prove no prompt was rendered.
func TestCommitAll_YesSkipsPromptDoesNotReadStdin(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()

	// Dirty the worktree so PendingBefore > 0 and the confirmation path
	// would otherwise be reached.
	if err := os.WriteFile(filepath.Join(repo, "new.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty: %v", err)
	}

	var out bytes.Buffer
	in := &errOnReadReader{}
	// dryRun=true so we don't actually mutate the repo, yes=true so prompt
	// is skipped. errOnReadReader returns an error if Read is ever called.
	if err := runCommitAll(ctx, &out, in, repo, true, true, true); err != nil {
		t.Fatalf("runCommitAll --yes --dry-run: %v", err)
	}
	if in.reads != 0 {
		t.Fatalf("--yes path read stdin %d times; must be zero", in.reads)
	}
	// Prompt rendering also writes to stdout — verify the JSON payload is
	// the only thing on stdout (no "Proceed? [y/N]" leak).
	if strings.Contains(out.String(), "Proceed?") {
		t.Fatalf("--yes path rendered prompt: %s", out.String())
	}
}

// TestCommitAll_JSONYesEmitsValidJSON pins that --json --yes produces a
// well-formed commitAllResult document with the canonical fields set.
func TestCommitAll_JSONYesEmitsValidJSON(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()

	var out bytes.Buffer
	if err := runCommitAll(ctx, &out, nil, repo, true, false, true); err != nil {
		t.Fatalf("runCommitAll: %v", err)
	}
	// Parse as generic map first to verify we are emitting JSON (not
	// human text). Then unmarshal into the typed struct.
	var generic map[string]any
	if err := json.Unmarshal(out.Bytes(), &generic); err != nil {
		t.Fatalf("--json --yes did not produce JSON: %v\n%s", err, out.String())
	}
	for _, k := range []string{"ok", "repo", "branch_ref", "strategy", "duration_ms"} {
		if _, ok := generic[k]; !ok {
			t.Fatalf("JSON missing required field %q: %v", k, generic)
		}
	}
	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("typed unmarshal: %v", err)
	}
	if !got.OK || got.Repo == "" || got.BranchRef == "" || got.Strategy == "" {
		t.Fatalf("invalid JSON shape: %+v", got)
	}
}

// TestResolveEffectiveCommitStrategy_DaemonMetaWins covers the priority
// chain: daemon meta `commit.strategy` > env ACD_COMMIT_STRATEGY > default
// (event). Three subtests, one per source.
func TestResolveEffectiveCommitStrategy_PriorityChain(t *testing.T) {
	t.Run("default_is_event", func(t *testing.T) {
		_, _, db := makeRegisteredGitRepoStateDB(t)
		ctx := context.Background()
		t.Setenv("ACD_COMMIT_STRATEGY", "")

		got, err := ResolveEffectiveCommitStrategy(ctx, db.SQL())
		if err != nil {
			t.Fatalf("ResolveEffectiveCommitStrategy: %v", err)
		}
		if got != ai.CommitStrategyEvent {
			t.Fatalf("default = %q, want %q", got, ai.CommitStrategyEvent)
		}
	})

	t.Run("env_overrides_default", func(t *testing.T) {
		_, _, db := makeRegisteredGitRepoStateDB(t)
		ctx := context.Background()
		t.Setenv("ACD_COMMIT_STRATEGY", "intent")

		got, err := ResolveEffectiveCommitStrategy(ctx, db.SQL())
		if err != nil {
			t.Fatalf("ResolveEffectiveCommitStrategy: %v", err)
		}
		if got != ai.CommitStrategyIntent {
			t.Fatalf("env override = %q, want %q", got, ai.CommitStrategyIntent)
		}
	})

	t.Run("daemon_meta_overrides_env", func(t *testing.T) {
		_, _, db := makeRegisteredGitRepoStateDB(t)
		ctx := context.Background()
		// Env says intent, but daemon meta says event — meta must win.
		t.Setenv("ACD_COMMIT_STRATEGY", "intent")
		if err := state.MetaSet(ctx, db, "commit.strategy", "event"); err != nil {
			t.Fatalf("MetaSet: %v", err)
		}

		got, err := ResolveEffectiveCommitStrategy(ctx, db.SQL())
		if err != nil {
			t.Fatalf("ResolveEffectiveCommitStrategy: %v", err)
		}
		if got != ai.CommitStrategyEvent {
			t.Fatalf("daemon meta override = %q, want %q", got, ai.CommitStrategyEvent)
		}
	})

	t.Run("daemon_meta_unknown_value_falls_back_to_env", func(t *testing.T) {
		_, _, db := makeRegisteredGitRepoStateDB(t)
		ctx := context.Background()
		t.Setenv("ACD_COMMIT_STRATEGY", "intent")
		if err := state.MetaSet(ctx, db, "commit.strategy", "garbage-value"); err != nil {
			t.Fatalf("MetaSet: %v", err)
		}

		got, err := ResolveEffectiveCommitStrategy(ctx, db.SQL())
		if err != nil {
			t.Fatalf("ResolveEffectiveCommitStrategy: %v", err)
		}
		if got != ai.CommitStrategyIntent {
			t.Fatalf("garbage meta should fall back to env (%q), got %q", ai.CommitStrategyIntent, got)
		}
	})

	t.Run("nil_conn_uses_env_only", func(t *testing.T) {
		ctx := context.Background()
		t.Setenv("ACD_COMMIT_STRATEGY", "intent")

		got, err := ResolveEffectiveCommitStrategy(ctx, nil)
		if err != nil {
			t.Fatalf("ResolveEffectiveCommitStrategy(nil): %v", err)
		}
		if got != ai.CommitStrategyIntent {
			t.Fatalf("nil conn should still honour env, got %q", got)
		}
	})
}

// TestCommitAllEstimatePasses_Boundaries covers the math of the estimator:
// zero pending, event strategy (1:1 with pending), intent with non-zero
// window (ceil division), intent with zero window (degenerates to pending).
func TestCommitAllEstimatePasses_Boundaries(t *testing.T) {
	cases := []struct {
		name     string
		strategy ai.CommitStrategy
		pending  int
		window   int
		want     int
	}{
		{"zero_pending_event", ai.CommitStrategyEvent, 0, 0, 0},
		{"zero_pending_intent", ai.CommitStrategyIntent, 0, 5, 0},
		{"event_one_each", ai.CommitStrategyEvent, 7, 5, 7},
		{"event_window_ignored", ai.CommitStrategyEvent, 12, 4, 12},
		{"intent_exact_division", ai.CommitStrategyIntent, 10, 5, 2},
		{"intent_ceil_division", ai.CommitStrategyIntent, 11, 5, 3},
		{"intent_window_smaller_than_pending", ai.CommitStrategyIntent, 1, 5, 1},
		{"intent_zero_window_falls_to_pending", ai.CommitStrategyIntent, 4, 0, 4},
		{"intent_negative_window_falls_to_pending", ai.CommitStrategyIntent, 4, -1, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := commitAllEstimatePasses(tc.strategy, tc.pending, tc.window)
			if got != tc.want {
				t.Fatalf("commitAllEstimatePasses(%s, pending=%d, window=%d) = %d, want %d",
					tc.strategy, tc.pending, tc.window, got, tc.want)
			}
		})
	}
}

// TestCommitAll_DryRunWithPendingPreservesHEAD asserts that even when the
// worktree is dirty and the read-only estimate is non-zero, --dry-run does
// not mutate HEAD or publish commits.
func TestCommitAll_DryRunWithPendingPreservesHEAD(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()

	// Drop two dirty files in different dirs so capture has to do work.
	if err := os.MkdirAll(filepath.Join(repo, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repo, "a.txt"), []byte("aa\n"), 0o644); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repo, "sub", "b.txt"), []byte("bb\n"), 0o644); err != nil {
		t.Fatalf("write sub/b.txt: %v", err)
	}
	headBefore, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	var out bytes.Buffer
	if err := runCommitAll(ctx, &out, nil, repo, true, true, true); err != nil {
		t.Fatalf("runCommitAll: %v", err)
	}
	headAfter, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD after: %v", err)
	}
	if headAfter != headBefore {
		t.Fatalf("dry-run mutated HEAD: %s -> %s", headBefore, headAfter)
	}

	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !got.DryRun {
		t.Fatalf("DryRun must be true: %+v", got)
	}
	if got.Commits != 0 {
		t.Fatalf("dry-run produced %d commits; must be zero", got.Commits)
	}
	if got.PendingBefore < 2 {
		t.Fatalf("expected at least 2 pending captures, got %d", got.PendingBefore)
	}
}

func TestCommitAll_DryRunReportsPreexistingPairWithoutReconciling(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	_ = db.Close()
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}

	var out bytes.Buffer
	if err := runCommitAll(ctx, &out, nil, repo, true, true, true); err != nil {
		t.Fatalf("runCommitAll dry-run: %v\n%s", err, out.String())
	}
	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if got.PreservedPending != 2 || got.DroppedStalePending != 0 || len(got.RecoveryRefs) != 0 {
		t.Fatalf("dry-run preservation report=%+v", got)
	}
	if !containsStringWith(got.Notes, "would preserve 2 pre-existing event(s)") {
		t.Fatalf("dry-run notes=%v", got.Notes)
	}
	after, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if before != after {
		t.Fatalf("dry-run reconciled state: before=%s after=%s", before, after)
	}
	db2, err := state.Open(ctx, stateDB)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db2.Close()
	assertFixEventState(t, ctx, db2, first, state.EventStateBlockedConflict)
	assertFixEventState(t, ctx, db2, second, state.EventStatePending)
	if got := countRowsWhere(t, db2, "recovery_snapshots", "1 = 1"); got != 0 {
		t.Fatalf("dry-run created recovery snapshot: %d", got)
	}
}

func TestCommitAll_PreservesBarrierThenCommitsDirtyWork(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	if err := os.WriteFile(filepath.Join(repo, "dirty-a.txt"), []byte("aa\n"), 0o644); err != nil {
		t.Fatalf("write dirty-a: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repo, "dirty-b.txt"), []byte("bb\n"), 0o644); err != nil {
		t.Fatalf("write dirty-b: %v", err)
	}
	_ = db.Close()

	var out bytes.Buffer
	if err := runCommitAll(ctx, &out, nil, repo, true, false, true); err != nil {
		t.Fatalf("runCommitAll apply: %v\n%s", err, out.String())
	}
	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if got.PreservedPending != 2 || got.DroppedStalePending != 0 || len(got.RecoveryRefs) != 1 {
		t.Fatalf("preservation result=%+v", got)
	}
	if got.PendingAfter != 0 || got.Commits < 2 {
		t.Fatalf("commit-all did not converge: %+v", got)
	}
	db2, err := state.Open(ctx, state.DBPathFromGitDir(filepath.Join(repo, ".git")))
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db2.Close()
	assertFixEventState(t, ctx, db2, first, state.EventStateRecovered)
	assertFixEventState(t, ctx, db2, second, state.EventStateRecovered)
	for _, path := range []string{"dirty-a.txt", "dirty-b.txt"} {
		if _, err := git.LsTreeBlobOID(ctx, repo, "HEAD", path); err != nil {
			t.Fatalf("HEAD missing %s: %v", path, err)
		}
	}
	status, err := git.Run(ctx, git.RunOpts{Dir: repo}, "status", "--porcelain")
	if err != nil {
		t.Fatalf("git status: %v", err)
	}
	if strings.TrimSpace(string(status)) != "" {
		t.Fatalf("commit-all left dirty worktree: %s", status)
	}
}

func TestCommitAll_PreservesNonActivePairBeforeActiveDirtyWork(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	staleFirst, staleSecond := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/stale", 4)
	if err := os.WriteFile(filepath.Join(repo, "active-dirty.txt"), []byte("active\n"), 0o644); err != nil {
		t.Fatalf("write active dirty file: %v", err)
	}
	_ = db.Close()

	var out bytes.Buffer
	if err := runCommitAll(ctx, &out, nil, repo, true, false, true); err != nil {
		t.Fatalf("runCommitAll: %v\n%s", err, out.String())
	}
	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if got.PreservedPending != 2 || len(got.RecoveryRefs) != 1 || got.PendingAfter != 0 || got.Commits < 1 {
		t.Fatalf("nonactive preservation result=%+v", got)
	}
	db2, err := state.Open(ctx, state.DBPathFromGitDir(filepath.Join(repo, ".git")))
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db2.Close()
	assertFixEventState(t, ctx, db2, staleFirst, state.EventStateRecovered)
	assertFixEventState(t, ctx, db2, staleSecond, state.EventStateRecovered)
	var branchRef string
	var generation int64
	if err := db2.SQL().QueryRowContext(ctx,
		`SELECT branch_ref, branch_generation FROM capture_events WHERE seq = ?`, staleFirst,
	).Scan(&branchRef, &generation); err != nil {
		t.Fatalf("query stale provenance: %v", err)
	}
	if branchRef != "refs/heads/stale" || generation != 4 {
		t.Fatalf("stale pair was retargeted: %s/g%d", branchRef, generation)
	}
	if _, err := git.LsTreeBlobOID(ctx, repo, "HEAD", "active-dirty.txt"); err != nil {
		t.Fatalf("active dirty file not committed: %v", err)
	}
}

func TestCommitAll_PreflightRecoveryLeavesGitStateUntouched(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, _ := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	if err := os.WriteFile(filepath.Join(repo, "staged-user.txt"), []byte("staged\n"), 0o644); err != nil {
		t.Fatalf("write staged file: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "add", "staged-user.txt"); err != nil {
		t.Fatalf("git add: %v", err)
	}
	headBefore, _ := git.RevParse(ctx, repo, "HEAD")
	indexBefore, err := git.Run(ctx, git.RunOpts{Dir: repo}, "diff", "--cached", "--name-status")
	if err != nil {
		t.Fatalf("index before: %v", err)
	}
	worktreeBefore, err := os.ReadFile(filepath.Join(repo, "staged-user.txt"))
	if err != nil {
		t.Fatalf("read worktree before: %v", err)
	}

	result, err := reconcileCommitAllExistingPair(ctx, repo, filepath.Join(repo, ".git"), db, commitAllExistingPair{
		BranchRef: "refs/heads/main", Generation: 1, FirstSeq: first, EventCount: 2, Active: true,
	})
	if err != nil {
		t.Fatalf("reconcile preflight: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered || result.RecoveryRef == "" {
		t.Fatalf("reconcile result=%+v", result)
	}
	headAfter, _ := git.RevParse(ctx, repo, "HEAD")
	indexAfter, _ := git.Run(ctx, git.RunOpts{Dir: repo}, "diff", "--cached", "--name-status")
	worktreeAfter, _ := os.ReadFile(filepath.Join(repo, "staged-user.txt"))
	if headAfter != headBefore || string(indexAfter) != string(indexBefore) || string(worktreeAfter) != string(worktreeBefore) {
		t.Fatalf("preflight mutated Git state: HEAD %s->%s index %q->%q worktree %q->%q",
			headBefore, headAfter, indexBefore, indexAfter, worktreeBefore, worktreeAfter)
	}
}

func TestCommitAll_MissingPreexistingObjectFailsClosed(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	seq := appendFixEvent(t, ctx, db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
		Operation: "create", Path: "missing-preflight.txt", Fidelity: "exact",
		State: state.EventStateFailed, Error: sql.NullString{String: "missing object", Valid: true},
	}, []state.CaptureOp{{
		Op: "create", Path: "missing-preflight.txt", Fidelity: "exact",
		AfterOID:  sql.NullString{String: strings.Repeat("f", 40), Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	}})
	_ = db.Close()

	var out bytes.Buffer
	err = runCommitAll(ctx, &out, nil, repo, true, false, true)
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("runCommitAll err=%v want missing-object refusal\n%s", err, out.String())
	}
	db2, err := state.Open(ctx, state.DBPathFromGitDir(filepath.Join(repo, ".git")))
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer db2.Close()
	assertFixEventState(t, ctx, db2, seq, state.EventStateFailed)
	if got := countRowsWhere(t, db2, "recovery_snapshots", "1 = 1"); got != 0 {
		t.Fatalf("missing object wrote snapshot: %d", got)
	}
}

func TestCommitAll_RecaptureLoopFailsWhenRecoveryDoesNotConverge(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "never-converges.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	calls := 0
	replayFn := func(context.Context, string, *state.DB, daemon.CaptureContext, daemon.ReplayOpts) (daemon.ReplaySummary, error) {
		calls++
		return daemon.ReplaySummary{RecaptureRequired: true, BaseHead: head}, nil
	}
	var notes []string
	_, _, _, _, _, err = commitAllReplayLoopWith(ctx, repo, filepath.Join(repo, ".git"), db, daemon.CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
	}, ai.CommitStrategyEvent, ai.ProviderConfig{}, nil, 1, replayFn, &notes)
	if err == nil || !strings.Contains(err.Error(), "did not converge") {
		t.Fatalf("commitAllReplayLoopWith err=%v want bounded nonconvergence", err)
	}
	if calls != commitAllRecaptureLimit+1 {
		t.Fatalf("replay calls=%d want %d", calls, commitAllRecaptureLimit+1)
	}
}

func TestCommitAll_RecaptureLoopReplaysFreshCapture(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "recaptured.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	calls := 0
	replayFn := func(ctx context.Context, repo string, db *state.DB, cctx daemon.CaptureContext, opts daemon.ReplayOpts) (daemon.ReplaySummary, error) {
		calls++
		if calls == 1 {
			return daemon.ReplaySummary{RecaptureRequired: true, BaseHead: head}, nil
		}
		return daemon.Replay(ctx, repo, db, cctx, opts)
	}
	var notes []string
	commits, _, conflicts, failed, after, err := commitAllReplayLoopWith(
		ctx, repo, filepath.Join(repo, ".git"), db,
		daemon.CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head},
		ai.CommitStrategyEvent, ai.ProviderConfig{}, nil, 1, replayFn, &notes,
	)
	if err != nil {
		t.Fatalf("commitAllReplayLoopWith: %v", err)
	}
	if calls < 2 || commits != 1 || conflicts != 0 || failed != 0 || after != 0 {
		t.Fatalf("recapture result calls=%d commits=%d conflicts=%d failed=%d after=%d notes=%v",
			calls, commits, conflicts, failed, after, notes)
	}
	if !containsStringWith(notes, "recaptured 1 event(s)") {
		t.Fatalf("recapture note missing: %v", notes)
	}
	if _, err := git.LsTreeBlobOID(ctx, repo, "HEAD", "recaptured.txt"); err != nil {
		t.Fatalf("recaptured file not committed: %v", err)
	}
}

// errOnReadReader is an io.Reader whose Read always returns an error, used
// to detect any accidental stdin consumption in commit-all paths that are
// supposed to skip the prompt.
type errOnReadReader struct {
	reads int
}

func (e *errOnReadReader) Read(p []byte) (int, error) {
	e.reads++
	return 0, errStdinUnexpected
}

// errStdinUnexpected is a sentinel returned by errOnReadReader.
var errStdinUnexpected = errors.New("stdin must not be read on this path")

type commitAllPromptHookReader struct {
	hook   func() error
	reader *strings.Reader
	ran    bool
}

func (r *commitAllPromptHookReader) Read(p []byte) (int, error) {
	if !r.ran {
		r.ran = true
		if err := r.hook(); err != nil {
			return 0, err
		}
	}
	return r.reader.Read(p)
}

func TestCommitAll_RechecksHEADAfterConfirmation(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "dirty.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	dbBefore, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}
	refsBefore := commitAllRecoveryRefs(t, ctx, repo)
	reader := &commitAllPromptHookReader{
		reader: strings.NewReader("y\n"),
		hook: func() error {
			_, err := git.Run(ctx, git.RunOpts{Dir: repo},
				"-c", "user.name=ACD Test", "-c", "user.email=acd@example.invalid",
				"commit", "--allow-empty", "-m", "advance while prompting")
			return err
		},
	}

	var out bytes.Buffer
	err = runCommitAll(ctx, &out, reader, repo, false, false, false)
	if err == nil || !strings.Contains(err.Error(), "HEAD changed after preflight") {
		t.Fatalf("runCommitAll err=%v want post-prompt HEAD refusal\n%s", err, out.String())
	}
	dbAfter, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if dbAfter != dbBefore || commitAllRecoveryRefs(t, ctx, repo) != refsBefore {
		t.Fatalf("post-prompt HEAD refusal mutated ACD state")
	}
}

func TestCommitAll_AcquiresDaemonLockAfterConfirmation(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "dirty.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	dbBefore, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}
	refsBefore := commitAllRecoveryRefs(t, ctx, repo)
	var held *daemon.DaemonLock
	reader := &commitAllPromptHookReader{
		reader: strings.NewReader("y\n"),
		hook: func() error {
			var err error
			held, err = daemon.AcquireDaemonLock(filepath.Join(repo, ".git"))
			return err
		},
	}
	defer func() {
		if held != nil {
			_ = held.Release()
		}
	}()

	var out bytes.Buffer
	err = runCommitAll(ctx, &out, reader, repo, false, false, false)
	if err == nil || !strings.Contains(err.Error(), "per-repo daemon is alive") {
		t.Fatalf("runCommitAll err=%v want post-consent daemon-lock refusal\n%s", err, out.String())
	}
	dbAfter, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if dbAfter != dbBefore || commitAllRecoveryRefs(t, ctx, repo) != refsBefore {
		t.Fatalf("daemon-lock refusal mutated ACD state")
	}
}

func TestCommitAll_StopsBeforeRecoveryWhenPauseAppears(t *testing.T) {
	t.Setenv(ai.EnvProvider, "deterministic")
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyEvent))
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	_ = db.Close()
	headBefore, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("HEAD before: %v", err)
	}
	refsBefore := commitAllRecoveryRefs(t, ctx, repo)
	gitDir := filepath.Join(repo, ".git")
	paused := false
	hooks := commitAllHooks{BeforeMutationCheck: func(_ context.Context, stage commitAllMutationStage) error {
		if stage != commitAllStageRecoveryPair || paused {
			return nil
		}
		paused = true
		_, err := pausepkg.Write(pausepkg.Path(gitDir), pausepkg.Marker{
			Reason: "operator paused during commit-all",
			SetBy:  "test",
		}, false)
		return err
	}}

	var out bytes.Buffer
	err = runCommitAllWithHooks(ctx, &out, nil, repo, true, false, true, hooks)
	if err == nil || !strings.Contains(err.Error(), "manual pause marker") {
		t.Fatalf("runCommitAllWithHooks err=%v want pause refusal\n%s", err, out.String())
	}
	if !paused {
		t.Fatal("recovery-stage hook did not run")
	}
	if refsAfter := commitAllRecoveryRefs(t, ctx, repo); refsAfter != refsBefore {
		t.Fatalf("pause refusal wrote recovery ref: before=%q after=%q", refsBefore, refsAfter)
	}
	headAfter, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil || headAfter != headBefore {
		t.Fatalf("pause refusal changed HEAD: %q -> %q err=%v", headBefore, headAfter, err)
	}
	dbAfter, err := state.Open(ctx, state.DBPathFromGitDir(gitDir))
	if err != nil {
		t.Fatalf("reopen state: %v", err)
	}
	defer dbAfter.Close()
	assertFixEventState(t, ctx, dbAfter, first, state.EventStateBlockedConflict)
	assertFixEventState(t, ctx, dbAfter, second, state.EventStatePending)
	if got := countRowsWhere(t, dbAfter, "recovery_snapshots", "1 = 1"); got != 0 {
		t.Fatalf("pause refusal wrote %d recovery snapshot(s)", got)
	}
}

func TestCommitAll_StopsBeforeCaptureWhenPauseAppears(t *testing.T) {
	t.Setenv(ai.EnvProvider, "deterministic")
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyEvent))
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "paused-before-capture.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	headBefore, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("HEAD before: %v", err)
	}
	gitDir := filepath.Join(repo, ".git")
	paused := false
	hooks := commitAllHooks{BeforeMutationCheck: func(_ context.Context, stage commitAllMutationStage) error {
		if stage != commitAllStageCapture || paused {
			return nil
		}
		paused = true
		_, err := pausepkg.Write(pausepkg.Path(gitDir), pausepkg.Marker{
			Reason: "operator paused before capture",
			SetBy:  "test",
		}, false)
		return err
	}}

	var out bytes.Buffer
	err = runCommitAllWithHooks(ctx, &out, nil, repo, true, false, true, hooks)
	if err == nil || !strings.Contains(err.Error(), "manual pause marker") {
		t.Fatalf("runCommitAllWithHooks err=%v want pause refusal", err)
	}
	if !paused {
		t.Fatal("capture-stage hook did not run")
	}
	dbAfter, err := state.Open(ctx, state.DBPathFromGitDir(gitDir))
	if err != nil {
		t.Fatalf("reopen state: %v", err)
	}
	defer dbAfter.Close()
	if got := countRowsWhere(t, dbAfter, "capture_events", "1 = 1"); got != 0 {
		t.Fatalf("pause refusal captured %d event(s)", got)
	}
	headAfter, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil || headAfter != headBefore {
		t.Fatalf("pause refusal changed HEAD: %q -> %q err=%v", headBefore, headAfter, err)
	}
	if _, err := os.Stat(filepath.Join(repo, "paused-before-capture.txt")); err != nil {
		t.Fatalf("dirty worktree file was not preserved: %v", err)
	}
}

func TestCommitAll_RechecksGitOperationBeforeEveryReplayPass(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
		Operation: "create", Path: "pending.txt", Fidelity: "full",
	}, []state.CaptureOp{{Op: "create", Path: "pending.txt", Fidelity: "full"}})
	if err != nil {
		t.Fatalf("seed pending: %v", err)
	}
	gitDir := filepath.Join(repo, ".git")
	replayCalls := 0
	stub := func(context.Context, string, *state.DB, daemon.CaptureContext, daemon.ReplayOpts) (daemon.ReplaySummary, error) {
		replayCalls++
		return daemon.ReplaySummary{}, nil
	}
	checks := 0
	guard := &commitAllMutationGuard{
		repo: repo, gitDir: gitDir, expectedBranch: "refs/heads/main",
		hooks: commitAllHooks{BeforeMutationCheck: func(_ context.Context, stage commitAllMutationStage) error {
			if stage != commitAllStageReplay {
				return nil
			}
			checks++
			if checks == 2 {
				return os.WriteFile(filepath.Join(gitDir, "MERGE_HEAD"), []byte(head+"\n"), 0o600)
			}
			return nil
		}},
	}
	_, _, _, _, _, err = commitAllReplayLoopWithSafety(
		ctx, repo, gitDir, db,
		daemon.CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head},
		ai.CommitStrategyEvent, ai.ProviderConfig{}, nil, 1, stub, nil, guard,
	)
	if err == nil || !strings.Contains(err.Error(), "git operation") {
		t.Fatalf("replay loop err=%v want git-operation refusal", err)
	}
	if replayCalls != 1 {
		t.Fatalf("replay called %d times; want one call before marker appeared", replayCalls)
	}
	assertFixEventState(t, ctx, db, seq, state.EventStatePending)
}

// TestCommitAll_RefusesOrphanBranch covers P1-2: an empty repo with a
// branch ref pointing to no commits (orphan branch) used to silently
// produce empty BaseHead and crash deep in replay. We now refuse with a
// clear message before doing any state work.
func TestCommitAll_RefusesOrphanBranch(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, stateDB, db := makeRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()
	if err := git.Init(ctx, repo); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref: %v", err)
	}
	registerRepo(t, roots, repo, stateDB, "test")
	// Drop a dirty file so the orphan refusal triggers in the rev-parse
	// stage (NOT the clean-worktree no-op short-circuit).
	if err := os.WriteFile(filepath.Join(repo, "new.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty: %v", err)
	}

	var out bytes.Buffer
	err := runCommitAll(ctx, &out, nil, repo, true, false, true)
	if err == nil {
		t.Fatalf("expected refusal on orphan branch, got nil")
	}
	if !strings.Contains(err.Error(), "no commits on branch yet") {
		t.Fatalf("expected orphan refusal message, got: %v", err)
	}
}

// TestCommitAll_UserDeclineExitsNonZero covers P1-3: when the interactive
// prompt receives "no" the renderer must still emit a payload, but the
// caller must observe a non-nil sentinel error so cobra exits non-zero.
func TestCommitAll_UserDeclineExitsNonZero(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	_ = db.Close()
	if err := os.WriteFile(filepath.Join(repo, "new.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty: %v", err)
	}
	dbBefore, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}
	refsBefore := commitAllRecoveryRefs(t, ctx, repo)

	var out bytes.Buffer
	in := strings.NewReader("n\n")
	err = runCommitAll(ctx, &out, in, repo, false, false, false)
	if err == nil {
		t.Fatalf("expected non-nil error on user decline; got nil")
	}
	if !errors.Is(err, errCommitAllAborted) {
		t.Fatalf("expected errCommitAllAborted sentinel, got: %v", err)
	}
	// The renderer must still have written a "aborted by user" line so
	// the user sees what happened on stdout.
	if !strings.Contains(out.String(), "aborted by user") {
		t.Fatalf("decline output missing aborted note: %q", out.String())
	}
	if strings.Contains(out.String(), "commit-all complete") {
		t.Fatalf("decline output claimed completion: %q", out.String())
	}
	dbAfter, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	refsAfter := commitAllRecoveryRefs(t, ctx, repo)
	if dbAfter != dbBefore || refsAfter != refsBefore {
		t.Fatalf("decline mutated state: db %s->%s refs %q->%q", dbBefore, dbAfter, refsBefore, refsAfter)
	}
	db2, err := state.Open(ctx, stateDB)
	if err != nil {
		t.Fatalf("reopen state DB: %v", err)
	}
	defer db2.Close()
	assertFixEventState(t, ctx, db2, first, state.EventStateBlockedConflict)
	assertFixEventState(t, ctx, db2, second, state.EventStatePending)
	if got := countRowsWhere(t, db2, "recovery_snapshots", "1 = 1"); got != 0 {
		t.Fatalf("decline created recovery snapshot: %d", got)
	}
}

func TestFinishCommitAll_IncompleteReturnsTypedError(t *testing.T) {
	res := commitAllResult{
		OK:           true,
		Repo:         "/tmp/repo",
		BranchRef:    "refs/heads/main",
		PendingAfter: 2,
		Conflicts:    1,
		Failed:       1,
		RecoveryRefs: []string{"refs/acd/recovery/example"},
	}
	var out bytes.Buffer
	err := finishCommitAll(&out, res, true)
	var incomplete *commitAllIncompleteError
	if !errors.As(err, &incomplete) {
		t.Fatalf("finishCommitAll err=%v want *commitAllIncompleteError", err)
	}
	if incomplete.PendingAfter != 2 || incomplete.Conflicts != 1 || incomplete.Failed != 1 {
		t.Fatalf("typed incomplete counts=%+v", incomplete)
	}
	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if got.OK || !got.Incomplete || got.PendingAfter != 2 || len(got.RecoveryRefs) != 1 {
		t.Fatalf("incomplete result=%+v", got)
	}

	out.Reset()
	if err := finishCommitAll(&out, res, false); err == nil {
		t.Fatal("human incomplete result returned nil")
	}
	if strings.Contains(out.String(), "commit-all complete") || !strings.Contains(out.String(), "commit-all incomplete") {
		t.Fatalf("human incomplete wording=%q", out.String())
	}
}

func TestFinishCommitAll_RecoveredThenDrainedSucceeds(t *testing.T) {
	res := commitAllResult{
		OK:           true,
		Repo:         "/tmp/repo",
		BranchRef:    "refs/heads/main",
		PendingAfter: 0,
		Conflicts:    1,
		Failed:       1,
		RecoveryRefs: []string{"refs/acd/recovery/example"},
	}
	var out bytes.Buffer
	if err := finishCommitAll(&out, res, true); err != nil {
		t.Fatalf("recovered-and-drained result: %v", err)
	}
	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !got.OK || got.Incomplete || got.PendingAfter != 0 || len(got.RecoveryRefs) != 1 {
		t.Fatalf("recovered-and-drained result=%+v", got)
	}
}

func TestFinishCommitAllReplay_RendersPartialProgress(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	cctx := daemon.CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         head,
	}
	if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        cctx.BranchRef,
		BranchGeneration: cctx.BranchGeneration,
		BaseHead:         head,
		Operation:        "create",
		Path:             "still-pending.txt",
		Fidelity:         "exact",
	}, []state.CaptureOp{{
		Op: "create", Path: "still-pending.txt", Fidelity: "exact",
		AfterOID:  sql.NullString{String: strings.Repeat("a", 40), Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	}}); err != nil {
		t.Fatalf("append pending event: %v", err)
	}
	replayFailure := errors.New("provider stopped after first commit")
	res := commitAllResult{
		OK:            true,
		Repo:          repo,
		BranchRef:     cctx.BranchRef,
		HeadBefore:    head,
		PendingBefore: 2,
		Commits:       1,
		Drained:       1,
		RecoveryRefs:  []string{"refs/acd/recovery/protected"},
	}
	var out bytes.Buffer
	err = finishCommitAllReplay(ctx, &out, repo, db, cctx, res, true, replayFailure)
	var partial *commitAllReplayError
	if !errors.As(err, &partial) || !errors.Is(err, replayFailure) {
		t.Fatalf("finishCommitAllReplay err=%v want wrapped *commitAllReplayError", err)
	}
	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal partial result: %v\n%s", err, out.String())
	}
	if got.OK || !got.Incomplete || got.Error != replayFailure.Error() ||
		got.PendingAfter != 1 || got.HeadAfter != head || got.Commits != 1 || got.Drained != 1 ||
		len(got.RecoveryRefs) != 1 {
		t.Fatalf("partial replay result=%+v", got)
	}
}

func TestCommitAll_RendersPartialPreexistingRecovery(t *testing.T) {
	t.Setenv(ai.EnvProvider, "deterministic")
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyEvent))
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	validOIDRaw, err := git.Run(ctx, git.RunOpts{
		Dir:   repo,
		Stdin: strings.NewReader("protected first pair\n"),
	}, "hash-object", "-w", "--stdin")
	if err != nil {
		t.Fatalf("hash valid recovery object: %v", err)
	}
	validOID := strings.TrimSpace(string(validOIDRaw))
	firstSeq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/old-first",
		BranchGeneration: 2,
		BaseHead:         head,
		Operation:        "create",
		Path:             "first-protected.txt",
		Fidelity:         "exact",
	}, []state.CaptureOp{{
		Op: "create", Path: "first-protected.txt", Fidelity: "exact",
		AfterOID:  sql.NullString{String: validOID, Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	}})
	if err != nil {
		t.Fatalf("append valid first pair: %v", err)
	}
	secondSeq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/old-second",
		BranchGeneration: 3,
		BaseHead:         head,
		Operation:        "create",
		Path:             "second-missing.txt",
		Fidelity:         "exact",
	}, []state.CaptureOp{{
		Op: "create", Path: "second-missing.txt", Fidelity: "exact",
		AfterOID:  sql.NullString{String: strings.Repeat("b", 40), Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	}})
	if err != nil {
		t.Fatalf("append missing-object second pair: %v", err)
	}

	var out bytes.Buffer
	err = runCommitAll(ctx, &out, strings.NewReader(""), repo, true, false, true)
	var partial *commitAllRecoveryError
	if !errors.As(err, &partial) {
		t.Fatalf("runCommitAll err=%v want *commitAllRecoveryError\n%s", err, out.String())
	}
	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("decode partial recovery result: %v\n%s", err, out.String())
	}
	if got.OK || !got.Incomplete || got.PendingBefore != 2 || got.PendingAfter != 1 ||
		got.Drained != 1 || len(got.RecoveryRefs) != 1 || got.Error == "" {
		t.Fatalf("partial recovery result=%+v", got)
	}
	if !strings.Contains(got.Error, "missing blob object") {
		t.Fatalf("partial recovery error=%q", got.Error)
	}
	var firstState, secondState string
	if err := db.ReadSQL().QueryRowContext(ctx,
		`SELECT state FROM capture_events WHERE seq = ?`, firstSeq).Scan(&firstState); err != nil {
		t.Fatalf("read first pair state: %v", err)
	}
	if err := db.ReadSQL().QueryRowContext(ctx,
		`SELECT state FROM capture_events WHERE seq = ?`, secondSeq).Scan(&secondState); err != nil {
		t.Fatalf("read second pair state: %v", err)
	}
	if firstState != state.EventStateRecovered {
		t.Fatalf("first pair state=%q want recovered", firstState)
	}
	if secondState != state.EventStatePending {
		t.Fatalf("second pair state=%q want pending", secondState)
	}
	if _, err := git.RevParse(ctx, repo, got.RecoveryRefs[0]); err != nil {
		t.Fatalf("completed recovery ref is not reachable: %v", err)
	}
}

func commitAllRecoveryRefs(t *testing.T, ctx context.Context, repo string) string {
	t.Helper()
	out, err := git.Run(ctx, git.RunOpts{Dir: repo},
		"for-each-ref", "--format=%(refname):%(objectname)", "refs/acd/recovery/")
	if err != nil {
		t.Fatalf("list recovery refs: %v", err)
	}
	return string(out)
}

// fakePlannerProvider is an ai.Provider + IntentPlanner whose calls are
// recorded so the dry-run-airgap test can assert PlanIntent is NEVER
// invoked when the provider is network-bound.
type fakePlannerProvider struct {
	name        string
	needsDiff   bool
	planCalls   int
	genCalls    int
	planSubject string
	planErr     error
}

func (p *fakePlannerProvider) Name() string    { return p.name }
func (p *fakePlannerProvider) NeedsDiff() bool { return p.needsDiff }
func (p *fakePlannerProvider) Generate(ctx context.Context, cc ai.CommitContext) (ai.Result, error) {
	p.genCalls++
	return ai.Result{Subject: "fake: " + cc.Path}, nil
}
func (p *fakePlannerProvider) PlanIntent(ctx context.Context, req ai.IntentPlanRequest) (ai.IntentPlan, error) {
	p.planCalls++
	if p.planErr != nil {
		return ai.IntentPlan{}, p.planErr
	}
	seqs := make([]int64, 0, len(req.OfferedCaptures))
	for _, c := range req.OfferedCaptures {
		seqs = append(seqs, c.Seq)
	}
	return ai.IntentPlan{SelectedSeqs: seqs, Subject: p.planSubject}, nil
}

func TestCommitAllReplayLoop_ReusesPlannerHealthAfterTransportFailure(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	cctx := daemon.CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
	}
	if _, err := daemon.BootstrapShadow(ctx, repo, db, cctx); err != nil {
		t.Fatalf("bootstrap shadow: %v", err)
	}
	for _, path := range []string{"transport-a.txt", "transport-b.txt"} {
		if err := os.WriteFile(filepath.Join(repo, path), []byte(path+"\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	checker := git.NewIgnoreChecker(repo)
	defer func() { _ = checker.Close() }()
	if summary, err := daemon.Capture(ctx, repo, db, cctx, daemon.CaptureOpts{
		IgnoreChecker:     checker,
		SensitiveMatcher:  state.NewSensitiveMatcher(),
		SafeIgnoreMatcher: state.NewSafeIgnoreMatcher(),
		GitDir:            filepath.Join(repo, ".git"),
		SortByPath:        true,
		DisablePendingCap: true,
	}); err != nil {
		t.Fatalf("capture: %v", err)
	} else if summary.EventsAppended < 2 {
		t.Fatalf("captured %d events; want at least two passes", summary.EventsAppended)
	}

	planner := &fakePlannerProvider{
		name:      "openai-compat",
		planErr:   errors.New("planner transport unavailable"),
		needsDiff: false,
	}
	cfg := ai.LoadProviderConfigFromEnv()
	cfg.CommitStrategy = ai.CommitStrategyIntent
	cfg.CommitFormat = ai.CommitFormatImperative
	cfg.IntentWindow = 1
	cfg.IntentMinPending = 1
	cfg.IntentDeferLimit = 1
	cfg.Model = "test-model"
	cfg.BaseURL = "https://user:secret@planner.example/v1?token=hidden"

	commits, _, conflicts, failed, after, err := commitAllReplayLoopWithSafety(
		ctx, repo, filepath.Join(repo, ".git"), db, cctx,
		ai.CommitStrategyIntent, cfg, planner, 2,
		commitAllRunReplayDefault, nil, nil,
	)
	if err != nil {
		t.Fatalf("commitAllReplayLoopWithSafety: %v", err)
	}
	if commits < 2 || conflicts != 0 || failed != 0 || after != 0 {
		t.Fatalf("replay result commits=%d conflicts=%d failed=%d after=%d", commits, conflicts, failed, after)
	}
	if planner.planCalls != 1 {
		t.Fatalf("primary planner called %d times; want once before circuit bypass", planner.planCalls)
	}
	raw, ok, err := state.MetaGet(ctx, db, daemon.MetaKeyIntentPlannerHealth)
	if err != nil || !ok {
		t.Fatalf("load planner health ok=%v err=%v", ok, err)
	}
	health, err := daemon.DecodeIntentPlannerHealthSnapshot(raw)
	if err != nil {
		t.Fatalf("decode planner health: %v", err)
	}
	wantFingerprint := daemon.IntentPlannerProviderFingerprint(daemon.IntentPlannerProviderIdentity{
		Provider: "openai-compat",
		Model:    cfg.Model,
		Endpoint: cfg.BaseURL,
	})
	if health.State != daemon.IntentPlannerCircuitOpen || health.BypassCount < 1 ||
		health.ProviderFingerprint != wantFingerprint {
		t.Fatalf("planner health=%+v want open shared circuit fingerprint=%q", health, wantFingerprint)
	}
}

// fakeProviderForReplay implements ai.Provider with NeedsDiff=false so
// commitAllReplayLoopWith builds a real msgFn but no diff egress.
// Generate counts how many times it is called from the per-event
// MessageFn — proving the provider closure is wired up rather than
// hard-coded to DeterministicMessage.
type fakeProviderForReplay struct {
	name      string
	genCalls  int
	subject   string
	needsDiff bool
}

func (p *fakeProviderForReplay) Name() string    { return p.name }
func (p *fakeProviderForReplay) NeedsDiff() bool { return p.needsDiff }
func (p *fakeProviderForReplay) Generate(ctx context.Context, cc ai.CommitContext) (ai.Result, error) {
	p.genCalls++
	subj := p.subject
	if subj == "" {
		subj = "fake: " + cc.Path
	}
	return ai.Result{Subject: subj}, nil
}

// TestCommitAllReplayLoop_UsesProviderMessageFn covers P1-1: the loop
// must build a provider-driven MessageFn (via daemon.ProviderMessageFn)
// instead of hard-coding daemon.DeterministicMessage. We inject a fake
// replayer that drives a single event through the real msgFn so the
// fake provider records a Generate call.
func TestCommitAllReplayLoop_UsesProviderMessageFn(t *testing.T) {
	ctx := context.Background()
	provider := &fakeProviderForReplay{name: "fake-replay"}

	// Stub Replay: invoke the supplied MessageFn once with a synthetic
	// EventContext so Generate is observed, then return a summary that
	// terminates the loop (Published=1, no pending left).
	replayCalls := 0
	stub := func(ctx context.Context, repo string, db *state.DB, cctx daemon.CaptureContext, opts daemon.ReplayOpts) (daemon.ReplaySummary, error) {
		replayCalls++
		if opts.MessageFn != nil {
			ec := daemon.EventContext{
				Event: state.CaptureEvent{
					Seq:       1,
					BranchRef: cctx.BranchRef,
					Path:      "fake/path.txt",
					Operation: "create",
				},
				Ops: []state.CaptureOp{{Op: "create", Path: "fake/path.txt"}},
			}
			if _, err := opts.MessageFn(ctx, ec); err != nil {
				return daemon.ReplaySummary{}, err
			}
		}
		// Terminate the loop after one pass.
		return daemon.ReplaySummary{Published: 1}, nil
	}

	// Use a real but minimal repo so git.RevParse(HEAD) between passes
	// can resolve. We rely on the fixture's seed commit; PendingEvents
	// will return zero rows and the loop exits after one pass.
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	_ = db
	cctx := daemon.CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         "0000000000000000000000000000000000000000",
	}
	cfg := ai.LoadProviderConfigFromEnv()
	commits, drained, conflicts, failed, after, err := commitAllReplayLoopWith(
		ctx, repo, filepath.Join(repo, ".git"), db, cctx,
		ai.CommitStrategyEvent, cfg, provider, 1, stub, nil)
	if err != nil {
		t.Fatalf("commitAllReplayLoopWith: %v", err)
	}
	if provider.genCalls < 1 {
		t.Fatalf("provider Generate not called; want >=1 (msgFn must wire through provider). genCalls=%d", provider.genCalls)
	}
	if commits != 1 {
		t.Fatalf("commits=%d, want 1", commits)
	}
	_ = drained
	_ = conflicts
	_ = failed
	_ = after
	_ = replayCalls
}

func TestCommitAllReplayLoop_PreservesPartialSummaryOnError(t *testing.T) {
	ctx := context.Background()
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	replayFailure := errors.New("replay stopped after partial pass")
	stub := func(context.Context, string, *state.DB, daemon.CaptureContext, daemon.ReplayOpts) (daemon.ReplaySummary, error) {
		return daemon.ReplaySummary{Published: 2, Conflicts: 1, Failed: 1}, replayFailure
	}
	commits, _, conflicts, failed, _, err := commitAllReplayLoopWith(
		ctx, repo, filepath.Join(repo, ".git"), db,
		daemon.CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head},
		ai.CommitStrategyEvent, ai.ProviderConfig{}, nil, 4, stub, nil,
	)
	if !errors.Is(err, replayFailure) {
		t.Fatalf("commitAllReplayLoopWith err=%v want wrapped replay failure", err)
	}
	if commits != 2 || conflicts != 1 || failed != 1 {
		t.Fatalf("partial summary lost: commits=%d conflicts=%d failed=%d", commits, conflicts, failed)
	}
}

// TestCommitAllReplayLoop_ZeroProgressEscape covers P2-6: when Replay
// reports Published=0 with pending still present, the loop must exit
// after exactly commitAllZeroProgressLimit (3) consecutive zero-progress
// passes and emit a clear note explaining the escape.
func TestCommitAllReplayLoop_ZeroProgressEscape(t *testing.T) {
	ctx := context.Background()
	provider := &fakeProviderForReplay{name: "fake-zero"}

	// Force PendingEvents to keep returning >0 rows. The simplest way
	// without touching the schema is to seed one pending capture_events
	// row in the fixture DB and let the stub keep Published at 0.
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         "deadbeef",
		Operation:        "create",
		Path:             "stuck.txt",
		Fidelity:         "full",
	}, []state.CaptureOp{{Op: "create", Path: "stuck.txt", Fidelity: "full"}}); err != nil {
		t.Fatalf("seed pending: %v", err)
	}

	passCount := 0
	stub := func(ctx context.Context, repo string, db *state.DB, cctx daemon.CaptureContext, opts daemon.ReplayOpts) (daemon.ReplaySummary, error) {
		passCount++
		return daemon.ReplaySummary{Published: 0}, nil
	}

	// Use the real seed-commit HEAD so the post-pass RevParse doesn't
	// flip BaseHead and reset the zero-progress counter prematurely.
	realHead, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	cctx := daemon.CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         realHead,
	}
	cfg := ai.LoadProviderConfigFromEnv()
	var notes []string
	commits, _, _, _, after, err := commitAllReplayLoopWith(
		ctx, repo, filepath.Join(repo, ".git"), db, cctx,
		ai.CommitStrategyEvent, cfg, provider, 1, stub, &notes)
	if err != nil {
		t.Fatalf("commitAllReplayLoopWith: %v", err)
	}
	if passCount != commitAllZeroProgressLimit {
		t.Fatalf("passCount=%d, want exactly %d zero-progress passes", passCount, commitAllZeroProgressLimit)
	}
	if commits != 0 {
		t.Fatalf("commits=%d, want 0 on zero-progress escape", commits)
	}
	if after < 1 {
		t.Fatalf("after=%d, want >=1 pending row remaining", after)
	}
	gotNote := false
	for _, n := range notes {
		if strings.Contains(n, "made no progress") && strings.Contains(n, "stopping") {
			gotNote = true
			break
		}
	}
	if !gotNote {
		t.Fatalf("expected zero-progress escape note, got: %+v", notes)
	}
}
