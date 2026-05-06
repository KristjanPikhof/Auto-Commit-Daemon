package cli

import (
	"bytes"
	"context"
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
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "new.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	headBefore, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

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
}

// TestCommitAll_JSONRequiresYesWhenInteractive pins that --json without --yes
// refuses because there is no interactive prompt available.
func TestCommitAll_JSONRequiresYesWhenInteractive(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "new.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}

	var out bytes.Buffer
	err := runCommitAll(ctx, &out, nil, repo, false, false, true)
	if err == nil {
		t.Fatalf("expected --json without --yes to refuse")
	}
	if !strings.Contains(err.Error(), "--yes") {
		t.Fatalf("expected --yes prompt error, got: %v", err)
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

// TestPreviewIntentDryRun_NonIntentNoPlannerCalls confirms previewIntentDryRun
// is a no-op planner-wise when strategy is event: it adds the standard
// "would be processed" note and returns without consulting the planner.
func TestPreviewIntentDryRun_EventStrategyAddsBaseNoteOnly(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()

	// Dirty the worktree and let runCommitAll do bootstrap + capture so
	// pending > 0, then call previewIntentDryRun directly.
	if err := os.WriteFile(filepath.Join(repo, "new.txt"), []byte("x\n"), 0o644); err != nil {
		t.Fatalf("write dirty: %v", err)
	}
	gitDir := filepath.Join(repo, ".git")
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	cctx := daemon.CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head}
	if _, err := daemon.BootstrapShadow(ctx, repo, db, cctx); err != nil {
		t.Fatalf("BootstrapShadow: %v", err)
	}
	checker := git.NewIgnoreChecker(repo)
	defer func() { _ = checker.Close() }()
	if _, err := daemon.Capture(ctx, repo, db, cctx, daemon.CaptureOpts{
		IgnoreChecker:    checker,
		SensitiveMatcher: state.NewSensitiveMatcher(),
		GitDir:           gitDir,
	}); err != nil {
		t.Fatalf("Capture: %v", err)
	}

	cfg := ai.LoadProviderConfigFromEnv()
	provider, closer, err := ai.BuildProvider(cfg)
	if err != nil {
		t.Fatalf("BuildProvider: %v", err)
	}
	if closer != nil {
		defer func() { _ = closer.Close() }()
	}

	res := commitAllResult{PendingBefore: 1, Strategy: string(ai.CommitStrategyEvent)}
	previewIntentDryRun(ctx, repo, db, cctx, ai.CommitStrategyEvent, cfg, provider, &res)
	if len(res.Notes) != 1 {
		t.Fatalf("event strategy should add exactly one base note, got: %+v", res.Notes)
	}
	if !strings.Contains(res.Notes[0], "would be processed") {
		t.Fatalf("note format changed: %q", res.Notes[0])
	}
}

// TestCommitAll_DryRunWithPendingPreservesHEAD asserts that even when the
// worktree is dirty and pending > 0, --dry-run does NOT mutate HEAD or
// touch capture_events state beyond the normal capture pass.
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
