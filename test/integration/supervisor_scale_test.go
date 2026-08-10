//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestSupervisorAdmitsManyRepositoriesInBatches(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "acd-supervisor-scale-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	roots := paths.Roots{
		State: filepath.Join(root, "state", "acd"), Share: filepath.Join(root, "data", "acd"),
		Config: filepath.Join(root, "config", "acd"),
	}
	registry := central.NewRegistry()
	records := make([]central.RepoRecord, 0, 8)
	for index := 0; index < 8; index++ {
		repo := filepath.Join(root, fmt.Sprintf("repo-%d", index))
		if err := os.MkdirAll(repo, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := gitpkg.Init(context.Background(), repo); err != nil {
			t.Fatal(err)
		}
		if _, err := gitpkg.Run(context.Background(), gitpkg.RunOpts{Dir: repo}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
			t.Fatal(err)
		}
		for fileIndex := 0; fileIndex < 64; fileIndex++ {
			path := filepath.Join(repo, fmt.Sprintf("seed-%03d.txt", fileIndex))
			if err := os.WriteFile(path, []byte(fmt.Sprintf("seed %d\n", fileIndex)), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := gitpkg.Run(context.Background(), gitpkg.RunOpts{Dir: repo}, "add", "."); err != nil {
			t.Fatal(err)
		}
		if _, err := gitpkg.Run(context.Background(), gitpkg.RunOpts{Dir: repo}, "-c", "user.name=ACD Scale Test", "-c", "user.email=scale@localhost", "commit", "-m", "seed"); err != nil {
			t.Fatal(err)
		}
		worktree, err := gitpkg.ResolveWorktree(context.Background(), repo)
		if err != nil {
			t.Fatal(err)
		}
		db, err := state.Open(context.Background(), state.DBPathFromGitDir(worktree.GitDir))
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		registration, err := registry.RegisterResolvedRepo(worktree, "scale-test", time.Now().Unix())
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, registration.Record)
	}
	if err := central.Save(roots, registry); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, buildAcdBinary(t), "internal", "supervisor", "run")
	command.Env = append(os.Environ(),
		"XDG_STATE_HOME="+filepath.Dir(roots.State),
		"XDG_DATA_HOME="+filepath.Dir(roots.Share),
		"XDG_CONFIG_HOME="+filepath.Dir(roots.Config),
		"ACD_FSNOTIFY_ENABLED=0", "ACD_COMMIT_STRATEGY=event",
	)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var processErr error
	go func() {
		processErr = command.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		if runtime.GOOS != "darwin" {
			return
		}
		for _, record := range records {
			target := fmt.Sprintf("gui/%d/io.github.kristjanpikhof.acd.worker.%s", os.Getuid(), record.RepositoryID)
			if exec.Command("launchctl", "print", target).Run() == nil {
				t.Errorf("worker job still loaded after supervisor shutdown: %s", target)
			}
		}
	})
	t.Cleanup(func() {
		shutdownSupervisorScaleTest(roots)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = command.Process.Kill()
			<-done
		}
	})

	client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: time.Second}
	ready := make(map[string]bool, len(records))
	for len(ready) < len(records) {
		select {
		case <-done:
			t.Fatalf("supervisor exited before readiness: %v", processErr)
		case <-ctx.Done():
			t.Fatalf("ready repositories=%d of %d: %v", len(ready), len(records), ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
		response, err := client.Do(ctx, supervisor.Request{Version: supervisor.ProtocolVersion, ID: "scale-status", Method: "status"})
		if err != nil || !response.OK {
			continue
		}
		var status supervisor.Status
		if err := decodeSupervisorScaleStatus(response.Data, &status); err != nil {
			t.Fatal(err)
		}
		for _, worker := range status.Workers {
			if worker.State == "running" {
				workerResponse, workerErr := supervisor.DoWorker(ctx,
					supervisor.WorkerSocketPath(roots, worker.RepositoryID),
					supervisor.Request{Version: supervisor.ProtocolVersion,
						ID: "scale-worker-status", Method: "status",
						RepositoryID: worker.RepositoryID}, time.Second)
				if workerErr == nil && workerResponse.OK {
					ready[worker.RepositoryID] = true
				}
			}
		}
	}
	if runtime.GOOS == "darwin" {
		for _, record := range records {
			target := fmt.Sprintf("gui/%d/io.github.kristjanpikhof.acd.worker.%s", os.Getuid(), record.RepositoryID)
			if exec.Command("launchctl", "print", target).Run() == nil {
				t.Fatalf("session-owned worker unexpectedly registered with launchd: %s", target)
			}
		}
	}
}

func decodeSupervisorScaleStatus(value any, target any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, target)
}

func shutdownSupervisorScaleTest(roots paths.Roots) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 10 * time.Second}
	_, _ = client.Do(ctx, supervisor.Request{Version: supervisor.ProtocolVersion, ID: "scale-shutdown", Method: "shutdown"})
}
