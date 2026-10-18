//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

type finishWorkflowResult struct {
	state.FinishRequest
	Published      bool                 `json:"published"`
	NoChanges      bool                 `json:"no_changes"`
	CommitsCreated int                  `json:"commits_created"`
	NewHead        string               `json:"new_head"`
	Verification   []daemon.FinishCheck `json:"verification"`
}

func decodeFinishWorkflow(t *testing.T, result ExecResult) finishWorkflowResult {
	t.Helper()
	var envelope struct {
		OK   bool                 `json:"ok"`
		Data finishWorkflowResult `json:"data"`
	}
	finishDecodeJSON(t, result, &envelope)
	if !envelope.OK {
		t.Fatalf("finish returned an unsuccessful envelope: %s", result.Stdout)
	}
	return envelope.Data
}

func TestFinishWorkflowRepeatedReleaseBumps(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	f := newFinishFixture(t)
	for build := 174; build <= 175; build++ {
		body := fmt.Sprintf("CURRENT_PROJECT_VERSION = %d;\n", build)
		writeFile(t, filepath.Join(f.Repo, "project.pbxproj"), body)
		f.Barrier(t, ctx)
		result := decodeFinishWorkflow(t, f.Run(t, ctx, "finish", "Update release build number", "--json"))
		if result.State != state.FinishCompleted || !result.Published || result.CommitsCreated != 1 {
			t.Fatalf("build %d finish: %+v", build, result)
		}
		if got := runGitOK(t, f.Repo, "show", "HEAD:project.pbxproj"); got != body {
			t.Fatalf("build %d committed %q", build, got)
		}
		if dirty := runGitOK(t, f.Repo, "status", "--porcelain"); dirty != "" {
			t.Fatalf("build %d left changes: %s", build, dirty)
		}
		assertFinishWorkflowStatus(t, ctx, f, true)
	}
}

func TestFinishWorkflowCommittedWorkIgnoresOldFailedRequest(t *testing.T) {
	var calls atomic.Int32
	f := finishIntentFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeFinishChatArguments(t, w, `{"goals":[{"purpose":"Unrecorded work","message":"Complete unrecorded work","change_ids":["not-recorded"]}]}`)
	}, "10s")
	writeFile(t, filepath.Join(f.Repo, "version.txt"), "174\n")
	failed := finishRunJSON(t, f, "finish", "Update release version")
	if failed.OK || failed.Data.State != state.FinishFailed || calls.Load() != 2 {
		t.Fatalf("request did not fail as expected: %+v, calls=%d", failed, calls.Load())
	}
	writeFile(t, filepath.Join(f.Repo, "version.txt"), "175\n")
	runGitOK(t, f.Repo, "add", "version.txt")
	runGitOK(t, f.Repo, "commit", "-m", "Update release version")
	f.Barrier(t, t.Context())
	assertFinishWorkflowStatus(t, t.Context(), f, true)
	for _, command := range []string{"status", "on"} {
		result := f.Run(t, t.Context(), command)
		if result.ExitCode != 0 || !strings.Contains(result.Stdout, "No action needed.") {
			t.Fatalf("%s lost current state: %+v", command, result)
		}
		for _, unwanted := range []string{"--retry", failed.Data.ID, "invalid_provider_plan", "Later changes:"} {
			if strings.Contains(result.Stdout, unwanted) {
				t.Fatalf("%s presented old request as a task:\n%s", command, result.Stdout)
			}
		}
		t.Logf("%s:\n%s", command, result.Stdout)
	}
	detailed := f.Run(t, t.Context(), "status", "--verbose")
	if detailed.ExitCode != 0 || !strings.Contains(detailed.Stdout, failed.Data.ID) {
		t.Fatalf("verbose output lost request evidence: %+v", detailed)
	}
	if calls.Load() != 2 {
		t.Fatal("status or protection retried the provider automatically")
	}
}

func TestFinishWorkflowModesStylesAndCompleteGoals(t *testing.T) {
	for _, mode := range []string{"local", "intent"} {
		for _, style := range []string{finish.StyleImperative, finish.StyleConventional} {
			t.Run(mode+"/"+style, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				var calls atomic.Int32
				message := func(subject, why string) string {
					if style == finish.StyleConventional {
						subject = "feat: " + subject
					}
					return subject + "\n\n- " + why
				}
				server, trust := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					request := decodeFinishChatRequest(t, r).Plan
					goals := []finish.Goal{
						{Purpose: "Preserve settings across reloads", Boundary: "Settings storage is an independent behavior", Message: message("Preserve settings across reloads", "Keep stored fields when settings are loaded")},
						{Purpose: "Show disabled protection status", Boundary: "The status badge can be reviewed independently", Message: message("Show disabled protection status", "Explain when protection is disabled")},
					}
					for _, change := range request.Changes {
						group := 0
						if strings.HasPrefix(change.Path, "badge") {
							group = 1
						}
						goals[group].ChangeIDs = append(goals[group].ChangeIDs, change.ID)
					}
					response, _ := json.Marshal(map[string]any{"goals": goals})
					writeFinishChatArguments(t, w, string(response))
				}))
				t.Cleanup(server.Close)
				f := newFinishFixture(t, "ACD_AI_API_KEY=fixture-key")
				f.Apply(t, ctx, map[string]string{config.FieldCommitStrategy: mode, config.FieldCommitFormat: style, config.FieldProvider: "openai-compat", config.FieldBaseURL: server.URL, config.FieldCAFile: strings.TrimPrefix(trust, "ACD_AI_CA_FILE="), config.FieldModel: "workflow", config.FieldDiffEgress: "true"}, ai.ConfirmationEndpointCredentials, ai.ConfirmationDiffEgress)
				index := runGitOK(t, f.Repo, "ls-files", "--stage")
				for version := 1; version <= 3; version++ {
					writeFile(t, filepath.Join(f.Repo, "settings.go"), fmt.Sprintf("package example\n\nfunc Load() int { return %d }\n", version))
					f.Barrier(t, ctx)
				}
				files := map[string]string{
					"settings_test.go": "package example\n\n// Load preserves configured settings.\n",
					"docs/settings.md": "Reloading settings preserves stored fields.\n",
					"badge.go":         "package example\n\nfunc Badge() string { return \"Protection disabled\" }\n",
					"badge_test.go":    "package example\n\n// Badge explains disabled protection.\n",
				}
				for path, body := range files {
					writeFile(t, filepath.Join(f.Repo, path), body)
				}
				f.Barrier(t, ctx)
				if head := strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")); head != f.BaseHead {
					t.Fatal("saving files moved HEAD")
				}
				if calls.Load() != 0 {
					t.Fatal("idle protection called a provider")
				}
				db := f.OpenDB(t)
				var requests int
				if err := db.SQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM finish_requests").Scan(&requests); err != nil || requests != 0 {
					t.Fatalf("idle protection started finish/check work: count=%d err=%v", requests, err)
				}
				assertFinishWorkflowStatus(t, ctx, f, false)
				purpose := "Preserve settings and show protection status"
				preview := decodeFinishWorkflow(t, f.Run(t, ctx, "finish", purpose, "--dry-run", "--json"))
				if preview.State != state.FinishCompleted || preview.Published || preview.CommitsCreated != 0 || preview.Target.Style != style {
					t.Fatalf("invalid preview: %+v", preview)
				}
				if head := strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")); head != f.BaseHead || runGitOK(t, f.Repo, "ls-files", "--stage") != index {
					t.Fatal("preview changed branch or index")
				}
				published := decodeFinishWorkflow(t, f.Run(t, ctx, "finish", purpose, "--json"))
				wantCommits := 1
				if mode == "intent" {
					wantCommits = 2
				}
				if !published.Published || published.State != state.FinishCompleted || published.CommitsCreated != wantCommits {
					t.Fatalf("invalid publication: %+v", published)
				}
				commits := strings.Fields(runGitOK(t, f.Repo, "rev-list", "--reverse", f.BaseHead+"..HEAD"))
				if len(commits) != wantCommits {
					t.Fatalf("commits=%v", commits)
				}
				for _, oid := range commits {
					if err := finish.ValidateMessage(style, strings.TrimSpace(runGitOK(t, f.Repo, "log", "-1", "--format=%B", oid))); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "intent" {
					first := runGitOK(t, f.Repo, "ls-tree", "-r", "--name-only", commits[0])
					for _, path := range []string{"settings.go", "settings_test.go", "docs/settings.md"} {
						if !strings.Contains(first, path+"\n") {
							t.Fatalf("incomplete settings goal: %s", first)
						}
					}
					if strings.Contains(first, "badge") {
						t.Fatalf("independent goal leaked into first tree: %s", first)
					}
					if calls.Load() != 2 {
						t.Fatalf("provider exchanges=%d", calls.Load())
					}
				} else if calls.Load() != 0 {
					t.Fatal("Local finish called a provider")
				}
				for _, check := range published.Verification {
					if check.Executed {
						t.Fatal("structural policy executed a project command")
					}
				}
				if strings.TrimSpace(runGitOK(t, f.Repo, "status", "--porcelain")) != "" {
					t.Fatal("finish did not reconcile exact final tree/index")
				}
				assertFinishWorkflowStatus(t, ctx, f, true)
				empty := decodeFinishWorkflow(t, f.Run(t, ctx, "finish", "--json"))
				if !empty.NoChanges || empty.Published || empty.CommitsCreated != 0 {
					t.Fatalf("no-change finish: %+v", empty)
				}
				if result := f.Run(t, ctx, "off", "--json"); result.ExitCode != 0 {
					t.Fatalf("off: %+v", result)
				}
			})
		}
	}
}

func assertFinishWorkflowStatus(t *testing.T, ctx context.Context, f *finishFixture, committed bool) {
	t.Helper()
	type outcome struct {
		Protected       bool  `json:"protected"`
		BranchCommitted *bool `json:"branch_committed"`
		PendingChanges  int   `json:"pending_changes"`
	}
	var status struct {
		OK   bool `json:"ok"`
		Data struct {
			Publication outcome `json:"publication_outcome"`
		} `json:"data"`
	}
	var list struct {
		Data struct {
			Repos []struct {
				Repo        string  `json:"repo"`
				Publication outcome `json:"publication_outcome"`
			} `json:"repos"`
		} `json:"data"`
	}
	// A delayed watcher event can advance observation after a barrier. Wait
	// for current durable protection, then require both read-only views agree.
	waitFor(t, "current protection in status/list", 15*time.Second, func() bool {
		finishDecodeJSON(t, f.Run(t, ctx, "status", "--json"), &status)
		finishDecodeJSON(t, runAcd(t, ctx, f.Env, "list", "--once", "--json"), &list)
		current := status.Data.Publication
		if !status.OK || !current.Protected || current.BranchCommitted == nil || *current.BranchCommitted != committed {
			return false
		}
		for _, row := range list.Data.Repos {
			if row.Repo == f.Repo {
				return row.Publication.Protected && row.Publication.BranchCommitted != nil &&
					*row.Publication.BranchCommitted == committed && row.Publication.PendingChanges == current.PendingChanges
			}
		}
		return false
	})
}

func TestFinishWorkflowUnbornBranchAndRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	f := newFinishFixture(t)
	writeFile(t, filepath.Join(f.Repo, "draft.txt"), "protected first draft\n")
	f.Barrier(t, ctx)
	db := f.OpenDB(t)
	projection, err := state.ReadCheckpointProjection(ctx, f.DBPath, 100)
	if err != nil || projection.Latest == nil || projection.Latest.Phase != state.CheckpointCompleted {
		t.Fatalf("checkpoint: %+v err=%v", projection, err)
	}
	checkpointID := projection.Latest.ID
	writeFile(t, filepath.Join(f.Repo, "draft.txt"), "later draft\n")
	f.Barrier(t, ctx)
	index := runGitOK(t, f.Repo, "ls-files", "--stage")
	preview := f.Run(t, ctx, "restore", checkpointID, "--json")
	if preview.ExitCode != 0 {
		t.Fatalf("restore preview: %+v", preview)
	}
	if body, _ := os.ReadFile(filepath.Join(f.Repo, "draft.txt")); string(body) != "later draft\n" || runGitOK(t, f.Repo, "ls-files", "--stage") != index {
		t.Fatal("restore preview changed files/index")
	}
	if result := f.Run(t, ctx, "restore", checkpointID, "--yes", "--json"); result.ExitCode != 0 {
		t.Fatalf("restore apply: %+v", result)
	}
	if body, _ := os.ReadFile(filepath.Join(f.Repo, "draft.txt")); string(body) != "protected first draft\n" || runGitOK(t, f.Repo, "ls-files", "--stage") != index {
		t.Fatal("restore did not preserve index/exact protected files")
	}
	// Restore needs an existing HEAD; the later explicit finish also proves
	// first publication on an unborn branch.
	runGitOK(t, f.Repo, "update-ref", "-d", "refs/heads/main", f.BaseHead)
	runGitOK(t, f.Repo, "read-tree", "--empty")
	f.Barrier(t, ctx)
	result := decodeFinishWorkflow(t, f.Run(t, ctx, "finish", "Preserve the first draft", "--json"))
	if !result.Published || result.CommitsCreated != 1 || result.Target.ExpectedHead != "" {
		t.Fatalf("unborn finish: %+v", result)
	}
	if parents := strings.Fields(runGitOK(t, f.Repo, "rev-list", "--parents", "-1", "HEAD")); len(parents) != 1 {
		t.Fatalf("unborn commit has a parent: %v", parents)
	}
	var retained int
	if err := db.SQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM checkpoints WHERE retained=1").Scan(&retained); err != nil || retained < 2 {
		t.Fatalf("restore protection lost: count=%d err=%v", retained, err)
	}
}

func TestFinishWorkflowFreshOnWithoutUserManager(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux no-manager bootstrap refusal")
	}
	if _, err := os.Stat(fmt.Sprintf("/run/user/%d/systemd/private", os.Getuid())); err == nil {
		t.Skip("requires an absent user manager; Linux bootstrap success is not claimed")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	repo := tempRepo(t)
	env := finishIsolatedEnv(t)
	home := envValue(env, "HOME")
	roots := paths.Roots{State: filepath.Join(envValue(env, "XDG_STATE_HOME"), "acd"), Share: filepath.Join(home, ".local", "share", "acd"), Config: filepath.Join(home, ".config", "acd")}
	shimDir := t.TempDir()
	unexpected := filepath.Join(shimDir, "unexpected-systemctl")
	if err := os.WriteFile(filepath.Join(shimDir, "systemctl"), []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" >> '"+unexpected+"'\nexit 97\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	env = envWith(env, "PATH="+shimDir+string(os.PathListSeparator)+envValue(env, "PATH"))
	before := runGitOK(t, repo, "rev-parse", "HEAD")
	result := runAcd(t, ctx, env, "on", "--repo", repo, "--json")
	if result.ExitCode == 0 || !strings.Contains(result.Stderr+result.Stdout, "systemd user manager is unavailable") {
		t.Fatalf("fresh no-manager failure: %+v", result)
	}
	for _, path := range []string{roots.ManagedBinaryPath(), roots.RegistryPath(), filepath.Join(repo, ".git", "acd", "state.db"), unexpected} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("failed bootstrap mutated %s or invoked service manager: %v", path, err)
		}
	}
	if runGitOK(t, repo, "rev-parse", "HEAD") != before {
		t.Fatal("failed bootstrap moved HEAD")
	}
}

func TestFinishWorkflowFreshOnOffline(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("actual fresh bootstrap uses the macOS session backend; Linux success requires an isolated service manager")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	repo := tempRepo(t)
	env := finishIsolatedEnv(t)
	home := envValue(env, "HOME")
	roots := paths.Roots{State: filepath.Join(envValue(env, "XDG_STATE_HOME"), "acd"), Share: filepath.Join(home, ".local", "share", "acd"), Config: filepath.Join(home, ".config", "acd")}
	env = finishLaunchctlEnv(t, env)
	t.Cleanup(func() { shutdownSupervisorScaleTest(roots) })
	result := runAcd(t, ctx, env, "on", "--repo", repo, "--json")
	if result.ExitCode != 0 {
		t.Fatalf("actual fresh on: %+v", result)
	}
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := central.Load(roots)
	if err != nil || len(registry.Repos) != 1 {
		t.Fatalf("fresh registration: registry=%+v err=%v", registry, err)
	}
	repositoryID := registry.Repos[0].RepositoryID
	var runtimeStatus supervisor.Status
	response, err := (supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: time.Second}).Do(ctx, supervisor.Request{Version: supervisor.ProtocolVersion, ID: "fresh-on-status", Method: "status"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(response.Data)
	if err := json.Unmarshal(raw, &runtimeStatus); err != nil {
		t.Fatal(err)
	}
	process, err := captureCheckpointProcessIdentity(runtimeStatus.PID)
	if err != nil {
		t.Fatal(err)
	}
	registerCheckpointRuntimeWatchdog(t, process)
	t.Cleanup(func() { shutdownCheckpointRuntime(t, roots, supervisor.WorkerSocketPath(roots, repositoryID), process) })
	f := &finishFixture{Repo: wt.Root, Env: env, Bin: buildAcdBinary(t), Roots: roots, RepositoryID: repositoryID, WorktreeID: checkpoint.WorktreeID(wt.Root), DBPath: state.DBPathFromGitDir(wt.GitDir), BaseHead: strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))}
	writeFile(t, filepath.Join(repo, "offline.txt"), "offline protected work\n")
	// Fresh on already starts the real worker; request a barrier using its
	// canonical ID rather than pre-registering or starting another runtime.
	f.Barrier(t, ctx)
	assertFinishWorkflowStatus(t, ctx, f, false)
	finished := decodeFinishWorkflow(t, f.Run(t, ctx, "finish", "Preserve offline work", "--json"))
	if !finished.Published || finished.Target.Mode != "local" || finished.Target.Style != finish.StyleImperative {
		t.Fatalf("fresh offline defaults: %+v", finished)
	}
	if result := f.Run(t, ctx, "off", "--json"); result.ExitCode != 0 {
		t.Fatalf("fresh off: %+v", result)
	}
}
