package installer

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version"
)

func TestShouldUpgradeRuntimeUsesCompatibilityAndOrdering(t *testing.T) {
	compatibility := RuntimeCompatibility()
	options := RuntimeUpgradeOptions{SourceVersion: "v2026-08-07-180-gabcdef0", Compatibility: compatibility}
	tests := []struct {
		name   string
		status supervisor.Status
		want   bool
		err    string
	}{
		{name: "newer source", status: supervisor.Status{Version: "v2026-08-07-179-g1234567", BinaryDigest: "old", Compatibility: compatibility}, want: true},
		{name: "same build", status: supervisor.Status{Version: options.SourceVersion, BinaryDigest: "source", Compatibility: compatibility}},
		{name: "same version new bytes", status: supervisor.Status{Version: options.SourceVersion, BinaryDigest: "other", Compatibility: compatibility}, want: true},
		{name: "newer runtime", status: supervisor.Status{Version: "v2026-08-07-181-g1234567", BinaryDigest: "new", Compatibility: compatibility}},
		{name: "legacy runtime", status: supervisor.Status{Version: "v2026-08-07-179-g1234567"}, err: "run `acd setup` once"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := shouldUpgradeRuntime(test.status, "source", options)
			if got != test.want || test.err == "" && err != nil || test.err != "" && (err == nil || !strings.Contains(err.Error(), test.err)) {
				t.Fatalf("shouldUpgradeRuntime=(%v,%v), want (%v,%q)", got, err, test.want, test.err)
			}
		})
	}
}

func TestBuildPlanUsesBoundedCompatibleUpgrade(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS session supervisor plan")
	}
	root, err := os.MkdirTemp("/tmp", "acd-upgrade-plan-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	repo := filepath.Join(root, "repo")
	if err := os.MkdirAll(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	command := gitCommand(t, repo, "init")
	if command != "" {
		t.Fatal(command)
	}
	if command := gitCommand(t, repo, "symbolic-ref", "HEAD", "refs/heads/main"); command != "" {
		t.Fatal(command)
	}
	wt, err := git.ResolveWorktree(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	roots := paths.Roots{State: filepath.Join(root, "state", "acd"), Share: filepath.Join(root, "share", "acd"), Config: filepath.Join(root, "config", "acd")}
	registry := central.NewRegistry()
	registration, err := registry.RegisterResolvedRepo(wt, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	registry.EnableRepo(central.RepoRemovalTarget{Path: registration.Record.Path, StateDB: registration.Record.StateDB}, 1)
	if err := central.Save(roots, registry); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(root, "acd")
	if err := os.WriteFile(executable, []byte("compatible"), 0o755); err != nil {
		t.Fatal(err)
	}
	digest, err := version.FileDigest(executable)
	if err != nil {
		t.Fatal(err)
	}
	serveRuntimeStatus(t, roots, supervisor.Status{PID: os.Getpid(), Version: version.String(), BinaryDigest: digest,
		Ownership: userOwnershipForTest(), Compatibility: RuntimeCompatibility()})

	plan, err := BuildPlan(context.Background(), roots, Options{Repo: repo, Executable: executable, Integrations: "none", SkipServiceCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != "compatible_upgrade" || len(plan.Repositories) != 0 {
		t.Fatalf("plan mode=%q repositories=%d, want bounded compatible upgrade", plan.Mode, len(plan.Repositories))
	}
	if plan.RequiresExpected || len(plan.Actions) != 1 || plan.Actions[0].Kind != "verify_compatible_runtime" {
		t.Fatalf("current compatible plan should be a no-op: %+v", plan.Actions)
	}
	for _, action := range plan.Actions {
		if action.Kind == "migrate" || action.Kind == "self_test" {
			t.Fatalf("compatible plan contains full setup action: %+v", action)
		}
	}
}

func serveRuntimeStatus(t *testing.T, roots paths.Roots, status supervisor.Status) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(roots.SupervisorSocketPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", roots.SupervisorSocketPath())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			var request supervisor.Request
			if json.NewDecoder(bufio.NewReader(conn)).Decode(&request) == nil {
				_ = json.NewEncoder(conn).Encode(supervisor.Response{Version: supervisor.ProtocolVersion, ID: request.ID, OK: true, Data: status})
			}
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-done })
}

func gitCommand(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	if err != nil {
		return err.Error() + ": " + strings.TrimSpace(string(output))
	}
	return ""
}

func userOwnershipForTest() string { return "user:" + strconv.Itoa(os.Getuid()) }
