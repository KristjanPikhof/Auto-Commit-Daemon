package cli

import (
	"bytes"
	"context"

	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestFinishIntegrationHintsInactivePathsAreReadOnly(t *testing.T) {
	tests := []struct {
		name  string
		state integrationRepoState
		setup func(*testing.T, paths.Roots) string
	}{
		{
			name: "non-git", state: integrationRepoInactiveNoGit,
			setup: func(t *testing.T, _ paths.Roots) string { return t.TempDir() },
		},
		{
			name: "unregistered", state: integrationRepoInactiveUnregistered,
			setup: func(t *testing.T, _ paths.Roots) string { return makeUnregisteredStartRepo(t) },
		},
		{
			name: "disabled", state: integrationRepoInactiveDisabled,
			setup: func(t *testing.T, roots paths.Roots) string {
				repo := makeUnregisteredStartRepo(t)
				registerDisabledRepo(t, roots, repo)
				return repo
			},
		},
		{
			name: "unactivated", state: integrationRepoInactiveUnactivated,
			setup: func(t *testing.T, roots paths.Roots) string {
				repo := makeUnregisteredStartRepo(t)
				registerRepo(t, roots, repo, filepath.Join(repo, ".git", "acd", "state.db"), "codex")
				return repo
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			roots := withIsolatedHome(t)
			repo := tc.setup(t, roots)
			statePath := filepath.Join(repo, ".git", "acd")
			stateExisted := pathExists(statePath)

			decision := evaluateIntegrationRepo(context.Background(), repo)
			if decision.State != tc.state {
				t.Fatalf("decision=%+v want state %s", decision, tc.state)
			}
			if pathExists(statePath) != stateExisted {
				t.Fatalf("integration gate changed repository state at %s", statePath)
			}
			if pathExists(roots.SupervisorSocketPath()) {
				t.Fatalf("integration gate created supervisor socket %s", roots.SupervisorSocketPath())
			}
			if pathExists(harnessHookLogPath("codex")) {
				t.Fatal("inactive integration gate wrote a hook log")
			}
		})
	}
}

func TestEvaluateIntegrationRepoUnregisteredPathDoesNotRunGit(t *testing.T) {
	_ = withIsolatedHome(t)
	repo := makeUnregisteredStartRepo(t)
	binDir := t.TempDir()
	marker := filepath.Join(t.TempDir(), "git-ran")
	script := filepath.Join(binDir, "git")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntouch \"$ACD_TEST_GIT_MARKER\"\nexit 99\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	t.Setenv("ACD_TEST_GIT_MARKER", marker)

	decision := evaluateIntegrationRepo(context.Background(), repo)
	if decision.State != integrationRepoInactiveUnregistered {
		t.Fatalf("decision=%+v", decision)
	}
	if pathExists(marker) {
		t.Fatal("inactive repository gate launched git")
	}
}

func TestEvaluateIntegrationRepoUsesNearestNestedGitRoot(t *testing.T) {
	_ = withIsolatedHome(t)
	outer := makeUnregisteredStartRepo(t)
	registerEnabledStartRepo(t, outer)
	inner := filepath.Join(outer, "nested", "mssql")
	if err := os.MkdirAll(inner, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := gitpkg.Init(context.Background(), inner); err != nil {
		t.Fatal(err)
	}

	decision := evaluateIntegrationRepo(context.Background(), inner)
	if decision.State != integrationRepoInactiveUnregistered || !central.SameRepoPath(decision.Root, inner) {
		t.Fatalf("nested decision=%+v", decision)
	}
	if err := sendInternalHint(context.Background(), inner); err != nil {
		t.Fatalf("legacy hint should silently skip nested unregistered repo: %v", err)
	}
	if pathExists(filepath.Join(inner, ".git", "acd")) {
		t.Fatal("legacy hint created state in nested unregistered repo")
	}
}

func TestEvaluateIntegrationRepoActiveSubdirectoryAndSymlink(t *testing.T) {
	_ = withIsolatedHome(t)
	repo := makeUnregisteredStartRepo(t)
	registerEnabledStartRepo(t, repo)
	subdir := filepath.Join(repo, "one", "two")
	if err := os.MkdirAll(subdir, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "repo-link")
	if err := os.Symlink(repo, link); err != nil {
		t.Fatal(err)
	}

	for _, candidate := range []string{subdir, link} {
		decision := evaluateIntegrationRepo(context.Background(), candidate)
		if decision.State != integrationRepoActive || !central.SameRepoPath(decision.Root, repo) {
			t.Fatalf("candidate %s decision=%+v", candidate, decision)
		}
	}
}

func TestEvaluateIntegrationRepoSupportsLinkedWorktreeGitFile(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := materializeTestRepo(t, true)
	linked := filepath.Join(t.TempDir(), "linked")
	if _, err := gitpkg.Run(context.Background(), gitpkg.RunOpts{Dir: repo},
		"worktree", "add", "-q", "-b", "linked", linked); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = gitpkg.Run(context.Background(), gitpkg.RunOpts{Dir: repo}, "worktree", "remove", "--force", linked)
	})
	registerResolvedIntegrationRepo(t, roots, linked)

	decision := evaluateIntegrationRepo(context.Background(), linked)
	if decision.State != integrationRepoActive {
		t.Fatalf("linked worktree decision=%+v", decision)
	}
	info, err := os.Stat(filepath.Join(linked, ".git"))
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("linked worktree .git marker is not a file: info=%v err=%v", info, err)
	}
}

func TestEvaluateIntegrationRepoSupportsSubmoduleGitFile(t *testing.T) {
	roots := withIsolatedHome(t)
	parent := materializeTestRepo(t, true)
	source := materializeTestRepo(t, true)
	if _, err := gitpkg.Run(context.Background(), gitpkg.RunOpts{Dir: parent},
		"-c", "protocol.file.allow=always", "submodule", "add", "-q", source, "modules/child"); err != nil {
		t.Fatal(err)
	}
	submodule := filepath.Join(parent, "modules", "child")
	registerResolvedIntegrationRepo(t, roots, submodule)

	decision := evaluateIntegrationRepo(context.Background(), submodule)
	if decision.State != integrationRepoActive {
		t.Fatalf("submodule decision=%+v", decision)
	}
	info, err := os.Stat(filepath.Join(submodule, ".git"))
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("submodule .git marker is not a file: info=%v err=%v", info, err)
	}
}

func TestEvaluateIntegrationRepoRegistryFailuresAreIndeterminate(t *testing.T) {
	for name, body := range map[string]string{
		"corrupt":     "{bad json",
		"unsupported": `{"version":999,"repos":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			roots := withIsolatedHome(t)
			repo := makeUnregisteredStartRepo(t)
			if err := os.MkdirAll(filepath.Dir(roots.RegistryPath()), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(roots.RegistryPath(), []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			decision := evaluateIntegrationRepo(context.Background(), repo)
			if decision.State != integrationRepoIndeterminate || decision.Err == nil {
				t.Fatalf("decision=%+v", decision)
			}

			activity := integrationEvent{Harness: "codex", Kind: "activity", Repo: repo, SessionID: "session"}
			if err := runIntegrationEvent(context.Background(), bytes.NewReader(nil), activity); err != nil {
				t.Fatal(err)
			}
			if pathExists(harnessHookLogPath("codex")) {
				t.Fatal("routine indeterminate event wrote a log")
			}
			activity.Kind = "session_open"
			if err := runIntegrationEvent(context.Background(), bytes.NewReader(nil), activity); err != nil {
				t.Fatal(err)
			}
			if !pathExists(harnessHookLogPath("codex")) {
				t.Fatal("session open did not log indeterminate repository state")
			}
		})
	}
}

func TestEvaluateIntegrationRepoInvalidCanonicalIdentityIsIndeterminate(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := makeUnregisteredStartRepo(t)
	registerEnabledStartRepo(t, repo)
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		record, ok := registry.FindRepo(repo)
		if !ok {
			return errors.New("registered repository missing")
		}
		for i := range registry.Repos {
			if central.SameRepoPath(registry.Repos[i].Path, record.Path) {
				registry.Repos[i].RepositoryID = "invalid"
				return nil
			}
		}
		return errors.New("registered repository row missing")
	}); err != nil {
		t.Fatal(err)
	}

	decision := evaluateIntegrationRepo(context.Background(), repo)
	if decision.State != integrationRepoIndeterminate || decision.Err == nil {
		t.Fatalf("decision=%+v", decision)
	}
}

func TestFinishIntegrationHintsEveryEventOnlyWakes(t *testing.T) {
	for harness := range supportedIntegrationHarnesses {
		for event := range supportedIntegrationEvents {
			t.Run(harness+"/"+event, func(t *testing.T) {
				calls := 0
				sender := func(_ context.Context, repo string) error {
					calls++
					if repo != "/repo" {
						t.Fatalf("unexpected hook repository: %q", repo)
					}
					return nil
				}

				err := runIntegrationEventWithSender(t.Context(), strings.NewReader("ignored prompt text"), integrationEvent{Harness: harness, Kind: event, Repo: "/repo", SessionID: "ignored-session", WatchPID: 42}, sender)
				if err != nil || calls != 1 {
					t.Fatalf("run=%v calls=%d", err, calls)
				}
			})
		}
	}
}

func TestFinishIntegrationHintsFailOpenQuietly(t *testing.T) {
	_ = withIsolatedHome(t)
	sender := func(context.Context, string) error {
		return errors.New("supervisor unavailable")
	}
	logPath := harnessHookLogPath("codex")
	activity := integrationEvent{Harness: "codex", Kind: "activity", Repo: "/repo", SessionID: "session"}
	if err := runIntegrationEventWithSender(context.Background(), bytes.NewReader(nil), activity, sender); err != nil {
		t.Fatal(err)
	}
	if pathExists(logPath) {
		t.Fatal("routine activity failure wrote a hook log")
	}

	open := activity
	open.Kind = "session_open"
	if err := runIntegrationEventWithSender(context.Background(), bytes.NewReader(nil), open, sender); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "integration event failed") || !strings.Contains(string(body), "supervisor unavailable") {
		t.Fatalf("log=%s", body)
	}
}

func TestFinishIntegrationHintsPayloadFailuresFailOpen(t *testing.T) {
	_ = withIsolatedHome(t)
	senderCalls := 0
	sender := func(context.Context, string) error {
		senderCalls++
		return nil
	}
	badPayloads := []struct {
		harness string
		payload string
	}{
		{harness: "codex", payload: `{"cwd":42}`},
		{harness: "codex", payload: `{"cwd":"bad\npath"}`},
		{harness: "cursor", payload: `{"conversation_id":"session","workspace_roots":["bad\npath"]}`},
		{harness: "codex", payload: strings.Repeat("x", hookStdinLimit+1)},
	}
	for _, tc := range badPayloads {
		event := integrationEvent{Harness: tc.harness, Kind: "activity"}
		if err := runIntegrationEventWithSender(context.Background(), strings.NewReader(tc.payload), event, sender); err != nil {
			t.Fatalf("payload failure escaped to harness: %v", err)
		}
	}
	if senderCalls != 0 {
		t.Fatalf("malformed payloads reached sender %d time(s)", senderCalls)
	}
	if pathExists(harnessHookLogPath("codex")) {
		t.Fatal("routine malformed payload wrote a hook log")
	}
	open := integrationEvent{Harness: "codex", Kind: "session_open"}
	if err := runIntegrationEventWithSender(context.Background(), strings.NewReader(`{"cwd":42}`), open, sender); err != nil {
		t.Fatal(err)
	}
	if !pathExists(harnessHookLogPath("codex")) {
		t.Fatal("malformed session-open payload did not write a diagnostic")
	}
}

func TestRunIntegrationEventRejectsInvalidConfiguration(t *testing.T) {
	sender := func(context.Context, string) error { return nil }
	for _, event := range []integrationEvent{
		{Harness: "unknown", Kind: "activity", Repo: "/repo", SessionID: "session"},
		{Harness: "codex", Kind: "unknown", Repo: "/repo", SessionID: "session"},
	} {
		if err := runIntegrationEventWithSender(context.Background(), bytes.NewReader(nil), event, sender); err == nil {
			t.Fatalf("invalid event succeeded: %+v", event)
		}
	}
}

func TestFinishIntegrationHintsNeedNoSession(t *testing.T) {
	for harness := range supportedIntegrationHarnesses {
		event, err := normalizeIntegrationEvent(strings.NewReader("unread prompt"), integrationEvent{Harness: harness, Kind: "activity", Repo: "/repo"})
		if err != nil || event.Repo != "/repo" || event.SessionID != "" {
			t.Fatalf("%s: %+v err=%v", harness, event, err)
		}
	}
}

func registerResolvedIntegrationRepo(t *testing.T, roots paths.Roots, repo string) {
	t.Helper()
	worktree, err := gitpkg.ResolveWorktree(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		registration, err := registry.RegisterResolvedRepo(worktree, "codex", time.Now().Unix())
		if err != nil {
			return err
		}
		registry.EnableRepo(central.RepoRemovalTarget{
			Path: registration.Record.Path, StateDB: registration.Record.StateDB,
		}, time.Now().Unix())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
