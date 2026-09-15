package cli

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// TestCommitAll_FlagsRegistered ensures the command surfaces all required
// flags. Publication behavior is covered at the worker and integration boundaries.
func TestCommitAll_FlagsRegistered(t *testing.T) {
	cmd := newProductCommitAllCmd()
	for _, name := range []string{"yes", "dry-run"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Fatalf("commit-all: missing flag --%s", name)
		}
	}
}

// TestCommitAll_HelpExposesCommand verifies the worker-driven command is a
// supported product surface.
func TestCommitAll_HelpExposesCommand(t *testing.T) {
	root := newRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"--help"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("root help: %v", err)
	}
	if !strings.Contains(out.String(), "commit-all") {
		t.Fatalf("root help omits commit-all:\n%s", out.String())
	}
	command, _, err := root.Find([]string{"commit-all"})
	if err != nil || command == nil || command.Hidden {
		t.Fatalf("commit-all product command missing or hidden: command=%v err=%v", command, err)
	}
}

func TestCommitAll_IncompleteTargetRendersThenExitsThree(t *testing.T) {
	for _, jsonOut := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", jsonOut), func(t *testing.T) {
			var out bytes.Buffer
			err := finishProductCommitAll(&out, productCommitAllResult{
				Repo: "/repo", CheckpointID: "cp-1", Protected: true,
				TargetEvents: 3, PublishedEvents: 2, RemainingEvents: 1,
				PublicationDrained: false, WaitingReason: "publication made no progress",
			}, jsonOut)
			if ExitCode(err) != ExitActionRequired || !ErrorRendered(err) {
				t.Fatalf("exit=%d rendered=%t err=%v", ExitCode(err), ErrorRendered(err), err)
			}
			if !strings.Contains(out.String(), "publication made no progress") {
				t.Fatalf("output omitted wait reason: %s", out.String())
			}
		})
	}
}

func TestCommitAllReconnectSelectsOnlyTheCurrentWorktreeDrain(t *testing.T) {
	startedAt := time.Unix(100, 0)
	projection := state.PublicationDrainReadOnlyProjection{
		Active: []state.PublicationDrain{
			{ID: "other", WorktreeID: "other-worktree", CreatedTS: 101},
			{ID: "current", WorktreeID: "current-worktree", CreatedTS: 102},
		},
	}
	drain := selectReconnectPublicationDrain(
		projection, "current-worktree", startedAt)
	if drain == nil || drain.ID != "current" {
		t.Fatalf("selected=%+v, want current worktree drain", drain)
	}

	projection.Active = nil
	projection.Latest = &state.PublicationDrain{
		ID: "completed-after-disconnect", WorktreeID: "current-worktree",
		Phase: state.PublicationDrainCompleted, CreatedTS: 100.5,
	}
	drain = selectReconnectPublicationDrain(
		projection, "current-worktree", startedAt)
	if drain == nil || drain.ID != "completed-after-disconnect" {
		t.Fatalf("selected completed=%+v", drain)
	}

	projection.Latest.CreatedTS = 90
	if drain := selectReconnectPublicationDrain(
		projection, "current-worktree", startedAt); drain != nil {
		t.Fatalf("selected stale completed drain=%+v", drain)
	}

	projection.Latest = nil
	projection.Active = []state.PublicationDrain{{
		ID: "stale-active", WorktreeID: "current-worktree", CreatedTS: 90,
	}}
	if drain := selectReconnectPublicationDrain(
		projection, "current-worktree", startedAt); drain != nil {
		t.Fatalf("selected stale active drain=%+v", drain)
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
