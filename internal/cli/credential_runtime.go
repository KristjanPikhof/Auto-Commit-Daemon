package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

var openCredentialSettingsService = settings.NewService

// Refresh only the selected, already enabled repository. Shared credential
// storage does not authorize changing configuration or starting other workers.
func refreshCredentialRuntime(ctx context.Context, out io.Writer, roots paths.Roots, repo string) error {
	if strings.TrimSpace(os.Getenv(ai.EnvAPIKey)) != "" {
		fmt.Fprintln(out, "ACD_AI_API_KEY overrides the stored credential; runtime settings were not changed.")
		return nil
	}
	worktree, err := git.ResolveWorktree(ctx, repo)
	if errors.Is(err, git.ErrNotWorktree) {
		return nil
	}
	if err != nil {
		return err
	}
	registry, err := central.Load(roots)
	if err != nil {
		return err
	}
	record, found := findRepo(registry, worktree.Root, state.DBPathFromGitDir(worktree.GitDir))
	if !found || record.LifecycleDisabled() {
		return nil
	}
	service, err := openCredentialSettingsService(ctx, configureSettingsOptions(roots, worktree.Root, os.LookupEnv))
	if err != nil {
		return err
	}
	defer service.Close()
	snapshot, err := service.Snapshot(ctx, settings.ScopeRepository, "")
	if err != nil {
		return err
	}
	if snapshot.AppliedRevisionID == 0 {
		fmt.Fprintf(out, "Credential saved. Run `acd config edit --repo %s` to review runtime settings.\n", productListShellQuote(worktree.Root))
		return nil
	}
	if snapshot.DesiredRevisionID != snapshot.AppliedRevisionID {
		return errors.New("credential saved, but another configuration is pending; finish it before refreshing credentials")
	}
	connection, err := openStateDBReadOnly(ctx, record.StateDB)
	if err != nil {
		return err
	}
	defer connection.Close()
	var encoded string
	if err := connection.QueryRowContext(ctx, "SELECT snapshot_json FROM config_revisions WHERE id=?", snapshot.AppliedRevisionID).Scan(&encoded); err != nil {
		return err
	}
	values, confirmations, err := credentialRuntimeContract(encoded)
	if err != nil {
		return err
	}
	if values[config.FieldProvider] != "openai-compat" {
		return nil
	}
	validated, err := service.Validate(ctx, values, confirmations)
	if err != nil {
		return err
	}
	for key, value := range values {
		if validated.ResolvedHot[key] != value {
			return fmt.Errorf("credential saved, but active setting %s cannot be preserved; use acd config edit", key)
		}
	}
	tested, err := service.TestProvider(ctx, values, confirmations)
	if err != nil {
		return err
	}
	if !tested.Success {
		return errors.New("credential probe did not succeed; active runtime settings were preserved")
	}
	applied, err := service.Apply(ctx, settings.ApplyRequest{
		Values: values, Confirmations: confirmations, TestedFingerprint: tested.Fingerprint,
		ExpectedGeneration: snapshot.SavedGeneration, ExpectedDesiredRevision: snapshot.DesiredRevisionID,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "Credential verified. Runtime revision %d queued for %s; existing settings and approvals are preserved.\n", applied.RevisionID, worktree.Root)
	return nil
}

func credentialRuntimeContract(encoded string) (map[string]string, []ai.ConfirmationRequirement, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(encoded), &raw); err != nil {
		return nil, nil, errors.New("active runtime settings are unreadable")
	}
	var confirmations []ai.ConfirmationRequirement
	if data := raw["confirmations"]; len(data) > 0 {
		if err := json.Unmarshal(data, &confirmations); err != nil {
			return nil, nil, errors.New("active runtime approvals are unreadable")
		}
	}
	values := make(map[string]string)
	for _, field := range config.Catalog() {
		if field.Sensitive {
			continue
		}
		data, found := raw[field.Name]
		if !found {
			continue
		}
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return nil, nil, fmt.Errorf("active runtime setting %s is unreadable", field.Name)
		}
		values[field.Name] = value
	}
	return values, confirmations, nil
}
