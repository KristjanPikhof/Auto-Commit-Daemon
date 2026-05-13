package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRepoLookupCommandsCanonicalizeSubdirBeforeRegistryLookup(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	registerRepo(t, roots, repo, stateDB, "codex")

	if err := state.SaveDaemonState(ctx, db, state.DaemonState{PID: 0, Mode: "stopped", HeartbeatTS: nowFloat()}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	if _, err := state.AppendDecision(ctx, db, state.DecisionRecord{
		DecisionTS:  nowFloat(),
		Kind:        state.DecisionKindCaptured,
		Path:        sql.NullString{String: "nested/file.txt", Valid: true},
		ActionTaken: sql.NullString{String: "queued", Valid: true},
	}); err != nil {
		t.Fatalf("append decision: %v", err)
	}
	writeRepoLog(t, roots, repo, `{"msg":"canonical"}`+"\n")

	subdir := filepath.Join(repo, "nested", "deeper")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}

	t.Run("status", func(t *testing.T) {
		var out bytes.Buffer
		if err := runStatus(ctx, &out, subdir, true); err != nil {
			t.Fatalf("runStatus from subdir: %v", err)
		}
		var rep statusReport
		if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
			t.Fatalf("decode status: %v\n%s", err, out.String())
		}
		if rep.Repo != repo {
			t.Fatalf("status repo=%q want canonical root %q", rep.Repo, repo)
		}
	})

	t.Run("logs", func(t *testing.T) {
		var out bytes.Buffer
		if err := runLogs(ctx, &out, subdir, 1, false); err != nil {
			t.Fatalf("runLogs from subdir: %v", err)
		}
		if !strings.Contains(out.String(), "canonical") {
			t.Fatalf("logs output missing canonical line: %s", out.String())
		}
	})

	t.Run("diagnose", func(t *testing.T) {
		var out bytes.Buffer
		if err := runDiagnose(ctx, &out, subdir, true); err != nil {
			t.Fatalf("runDiagnose from subdir: %v", err)
		}
		var rep diagnoseReport
		if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
			t.Fatalf("decode diagnose: %v\n%s", err, out.String())
		}
		if rep.Repo != repo {
			t.Fatalf("diagnose repo=%q want canonical root %q", rep.Repo, repo)
		}
	})

	t.Run("events", func(t *testing.T) {
		var out bytes.Buffer
		if err := runEvents(ctx, &out, subdir, "", 0, 10, false, time.Millisecond, true); err != nil {
			t.Fatalf("runEvents from subdir: %v", err)
		}
		var rep eventsReport
		if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
			t.Fatalf("decode events: %v\n%s", err, out.String())
		}
		if rep.Repo != repo {
			t.Fatalf("events repo=%q want canonical root %q", rep.Repo, repo)
		}
	})

	t.Run("wake", func(t *testing.T) {
		var out bytes.Buffer
		if err := runWake(ctx, &out, subdir, "wake-session", true); err != nil {
			t.Fatalf("runWake from subdir: %v", err)
		}
		var rep wakeResult
		if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
			t.Fatalf("decode wake: %v\n%s", err, out.String())
		}
		if rep.Repo != repo {
			t.Fatalf("wake repo=%q want canonical root %q", rep.Repo, repo)
		}
	})

	t.Run("touch", func(t *testing.T) {
		var out bytes.Buffer
		if err := runTouch(ctx, &out, subdir, "touch-session", true); err != nil {
			t.Fatalf("runTouch from subdir: %v", err)
		}
		var rep touchResult
		if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
			t.Fatalf("decode touch: %v\n%s", err, out.String())
		}
		if rep.Repo != repo {
			t.Fatalf("touch repo=%q want canonical root %q", rep.Repo, repo)
		}
	})

	t.Run("stop", func(t *testing.T) {
		if err := state.RegisterClient(ctx, db, state.Client{SessionID: "stop-session", Harness: "codex", LastSeenTS: nowFloat()}); err != nil {
			t.Fatalf("register stop session: %v", err)
		}
		if err := state.RegisterClient(ctx, db, state.Client{SessionID: "peer-session", Harness: "codex", LastSeenTS: nowFloat()}); err != nil {
			t.Fatalf("register peer session: %v", err)
		}
		var out bytes.Buffer
		if err := runStop(ctx, &out, subdir, "stop-session", false, false, true); err != nil {
			t.Fatalf("runStop from subdir: %v", err)
		}
		var rep stopRepoResult
		if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
			t.Fatalf("decode stop: %v\n%s", err, out.String())
		}
		if rep.Repo != repo {
			t.Fatalf("stop repo=%q want canonical root %q", rep.Repo, repo)
		}
		if !rep.Deferred {
			t.Fatalf("stop should defer with peer still registered, got %+v", rep)
		}
	})
}

func TestRepoLookupRejectsNonGitWithoutRegistryMutation(t *testing.T) {
	roots := withIsolatedHome(t)
	nonGit := t.TempDir()

	for name, run := range map[string]func() error{
		"status": func() error { return runStatus(context.Background(), bytes.NewBuffer(nil), nonGit, true) },
		"logs":   func() error { return runLogs(context.Background(), bytes.NewBuffer(nil), nonGit, 1, false) },
		"events": func() error {
			return runEvents(context.Background(), bytes.NewBuffer(nil), nonGit, "", 0, 1, false, time.Millisecond, true)
		},
		"diagnose": func() error { return runDiagnose(context.Background(), bytes.NewBuffer(nil), nonGit, true) },
	} {
		t.Run(name, func(t *testing.T) {
			err := run()
			if err == nil {
				t.Fatalf("expected non-Git error")
			}
			if !strings.Contains(err.Error(), "not inside a Git worktree") {
				t.Fatalf("expected clear non-Git error, got %v", err)
			}
			if _, statErr := os.Stat(roots.RegistryPath()); !os.IsNotExist(statErr) {
				t.Fatalf("registry should not be created or mutated, stat err=%v", statErr)
			}
		})
	}
}

var _ paths.Roots
