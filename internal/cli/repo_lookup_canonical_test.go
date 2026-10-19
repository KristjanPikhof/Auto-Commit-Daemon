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

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
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
		if err := writeStatusProjectionFixture(ctx, &out, subdir, true); err != nil {
			t.Fatalf("writeStatusProjectionFixture from subdir: %v", err)
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

	t.Run("off", func(t *testing.T) {
		lookup, err := loadControlRepo(ctx, subdir)
		if err != nil {
			t.Fatalf("load off repository from subdir: %v", err)
		}
		if !lookup.Registered || lookup.Worktree.Root != repo || lookup.Record.StateDB != stateDB {
			t.Fatalf("off lookup did not find canonical registered repository: %+v", lookup)
		}
		registryBefore := mustSHA256(t, roots.RegistryPath())
		stateBefore := mustSHA256(t, stateDB)
		var out bytes.Buffer
		err = runControlOffWithForce(ctx, &out, subdir, true, false)
		if ExitCode(err) != ExitActionRequired || !strings.Contains(err.Error(), "final checkpoint barrier failed") || !strings.Contains(err.Error(), "acd setup") {
			t.Fatalf("off must require a proven checkpoint before disabling without an installed runtime: %v", err)
		}
		if out.Len() != 0 || mustSHA256(t, roots.RegistryPath()) != registryBefore || mustSHA256(t, stateDB) != stateBefore {
			t.Fatal("failed off changed registry/state or reported success")
		}
	})
}

func TestRepoLookupCommandsFindLegacySubdirRegistryRowByStateDBReadOnly(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
	legacySubdir := filepath.Join(repo, "legacy", "subdir")
	if err := os.MkdirAll(legacySubdir, 0o755); err != nil {
		t.Fatalf("mkdir legacy subdir: %v", err)
	}
	repoHash, err := paths.RepoHash(repo)
	if err != nil {
		t.Fatalf("repo hash: %v", err)
	}
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.Repos = []central.RepoRecord{{
			Path:              legacySubdir,
			RepoHash:          repoHash,
			StateDB:           stateDB,
			FirstRegisteredTS: 10,
			LastSeenTS:        20,
			Harnesses:         []string{"codex"},
		}}
		return nil
	}); err != nil {
		t.Fatalf("seed legacy registry: %v", err)
	}

	if err := state.SaveDaemonState(ctx, db, state.DaemonState{PID: 0, Mode: "stopped", HeartbeatTS: nowFloat()}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	if _, err := state.AppendDecision(ctx, db, state.DecisionRecord{
		DecisionTS:  nowFloat(),
		Kind:        state.DecisionKindCaptured,
		Path:        sql.NullString{String: "legacy/file.txt", Valid: true},
		ActionTaken: sql.NullString{String: "queued", Valid: true},
	}); err != nil {
		t.Fatalf("append decision: %v", err)
	}
	writeRepoLog(t, roots, repo, `{"msg":"legacy"}`+"\n")

	before, err := os.ReadFile(roots.RegistryPath())
	if err != nil {
		t.Fatalf("read registry before: %v", err)
	}

	for name, run := range map[string]func(string) (string, error){
		"status-root": func(path string) (string, error) {
			var out bytes.Buffer
			err := writeStatusProjectionFixture(ctx, &out, path, true)
			return out.String(), err
		},
		"status-subdir": func(path string) (string, error) {
			var out bytes.Buffer
			err := writeStatusProjectionFixture(ctx, &out, path, true)
			return out.String(), err
		},
		"logs-root": func(path string) (string, error) {
			var out bytes.Buffer
			err := runLogs(ctx, &out, path, 1, false)
			return out.String(), err
		},
		"events-subdir": func(path string) (string, error) {
			var out bytes.Buffer
			err := runEvents(ctx, &out, path, "", 0, 10, false, time.Millisecond, true)
			return out.String(), err
		},
		"diagnose-root": func(path string) (string, error) {
			var out bytes.Buffer
			err := runDiagnose(ctx, &out, path, true)
			return out.String(), err
		},
	} {
		t.Run(name, func(t *testing.T) {
			path := repo
			if strings.Contains(name, "subdir") {
				path = legacySubdir
			}
			out, err := run(path)
			if err != nil {
				t.Fatalf("%s failed: %v", name, err)
			}
			if strings.Contains(name, "logs") {
				if !strings.Contains(out, "legacy") {
					t.Fatalf("logs output missing legacy line: %s", out)
				}
				return
			}
			if !strings.Contains(out, repo) {
				t.Fatalf("output did not include canonical repo %q: %s", repo, out)
			}
		})
	}

	after, err := os.ReadFile(roots.RegistryPath())
	if err != nil {
		t.Fatalf("read registry after: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("read-only lookup commands mutated registry\nbefore: %s\nafter: %s", before, after)
	}
}

func TestRepoLookupRejectsNonGitWithoutRegistryMutation(t *testing.T) {
	roots := withIsolatedHome(t)
	nonGit := t.TempDir()

	for name, run := range map[string]func() error{
		"status": func() error {
			return writeStatusProjectionFixture(context.Background(), bytes.NewBuffer(nil), nonGit, true)
		},
		"logs": func() error { return runLogs(context.Background(), bytes.NewBuffer(nil), nonGit, 1, false) },
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
