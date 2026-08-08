package cli

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestWorkerAccessCheckReportsCompletedRepositoryRead(t *testing.T) {
	repo := materializeTestRepo(t, false)
	result := filepath.Join(t.TempDir(), "access.json")
	if err := runWorkerAccessCheck(context.Background(), []string{repo}, result); err != nil {
		t.Fatal(err)
	}
	status, err := supervisor.ReadServiceAccessStatus(result)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != "completed" || status.Target != "" {
		t.Fatalf("status=%+v", status)
	}
}

func TestWorkerAccessCheckIdentifiesUnreadableRepository(t *testing.T) {
	target := t.TempDir()
	result := filepath.Join(t.TempDir(), "access.json")
	if err := runWorkerAccessCheck(context.Background(), []string{target}, result); err == nil {
		t.Fatal("non-repository access check unexpectedly succeeded")
	}
	status, err := supervisor.ReadServiceAccessStatus(result)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != "failed" || status.Target != target || status.Error == "" {
		t.Fatalf("status=%+v", status)
	}
}

func TestSupervisorWorkerEnvironmentRequiresEnabledRepository(t *testing.T) {
	repo := materializeTestRepo(t, false)
	wt, err := gitpkg.ResolveWorktree(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	roots := paths.Roots{
		State: filepath.Join(root, "state"), Share: filepath.Join(root, "share"),
		Config: filepath.Join(root, "config"),
	}
	var repositoryID string
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		registry.Version = central.RegistryVersion
		registration, err := registry.RegisterResolvedRepo(wt, "test", time.Now().Unix())
		repositoryID = registration.Record.RepositoryID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	handler := cliSupervisorHandler{
		roots: roots,
		environment: map[string]string{
			"ACD_AI_API_KEY": "secret-test-value", "PATH": "/test/bin",
		},
	}
	data, protocolErr := handler.HandleSupervisorRequest(context.Background(), supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: "environment", Method: "worker_environment",
		RepositoryID: repositoryID,
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	values, ok := data.(map[string]string)
	if !ok || values["ACD_AI_API_KEY"] != "secret-test-value" || values["PATH"] != "/test/bin" {
		t.Fatalf("environment=%T %v", data, data)
	}
	values["PATH"] = "mutated"
	if handler.environment["PATH"] != "/test/bin" {
		t.Fatal("handler returned its mutable environment map")
	}
	_, protocolErr = handler.HandleSupervisorRequest(context.Background(), supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: "environment-missing", Method: "worker_environment",
		RepositoryID: "ffffffffffffffff",
	})
	if protocolErr == nil || protocolErr.Code != "repository_not_enabled" {
		t.Fatalf("missing repository error=%+v", protocolErr)
	}
}
