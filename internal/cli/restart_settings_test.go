package cli

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestApplyRestartEnvironmentUsesSavedRepositorySettings(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, _, _ := makeRepoStateDB(t)
	repoHash, err := paths.RepoHash(repo)
	if err != nil {
		t.Fatal(err)
	}
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Global["capture.max_file_bytes"] = json.RawMessage(`"2048"`)
		doc.Settings.Profiles["large"] = config.Profile{Fields: config.Overrides{
			"capture.max_file_bytes":  json.RawMessage(`"4096"`),
			"capture.sensitive_globs": json.RawMessage(`"private/**"`),
			"client.ttl":              json.RawMessage(`"42"`),
		}}
		doc.Settings.Repositories[repoHash] = config.RepositorySettings{
			Profile: "large",
			Fields: config.Overrides{
				"capture.max_file_bytes": json.RawMessage(`"8192"`),
			},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ACD_MAX_FILE_BYTES", "1024")
	t.Setenv("ACD_SENSITIVE_GLOBS", "environment/**")

	restore, err := applyRestartEnvironment(repo)
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	if got := os.Getenv("ACD_MAX_FILE_BYTES"); got != "8192" {
		t.Fatalf("max file bytes=%q", got)
	}
	if got := os.Getenv("ACD_SENSITIVE_GLOBS"); got != "private/**" {
		t.Fatalf("sensitive globs=%q", got)
	}
	if got := clientTTLForRepo(repo); got != 42*time.Second {
		t.Fatalf("client TTL=%v", got)
	}

	restore()
	if got := os.Getenv("ACD_MAX_FILE_BYTES"); got != "1024" {
		t.Fatalf("restored max file bytes=%q", got)
	}
	if got := os.Getenv("ACD_SENSITIVE_GLOBS"); got != "environment/**" {
		t.Fatalf("restored sensitive globs=%q", got)
	}
}
