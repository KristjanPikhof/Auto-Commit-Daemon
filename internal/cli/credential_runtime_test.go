package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestCredentialSetRefreshesOnlyApprovedRuntime(t *testing.T) {
	for _, scenario := range []string{"corrected", "rejected", "environment_override", "pending_activation"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			repo, _, db := makeRegisteredGitRepoStateDB(t)
			roots, err := paths.Resolve()
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv(ai.EnvAPIKey, "")
			if err := credentials.NewStore(roots).Set("old-test-key"); err != nil {
				t.Fatal(err)
			}
			store := config.NewStore(roots)
			if err := store.Update(func(doc *config.Document) error {
				doc.Settings.Global[config.FieldProvider] = json.RawMessage(`"openai-compat"`)
				doc.Settings.Global[config.FieldModel] = json.RawMessage(`"kept-model"`)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			var probeKey string
			reject := false
			nudges := 0
			oldOpen := openCredentialSettingsService
			openCredentialSettingsService = func(ctx context.Context, opts settings.Options) (*settings.Service, error) {
				opts.Probe = func(_ context.Context, cfg ai.ProviderConfig) (ai.ProviderProbeResult, error) {
					probeKey = cfg.APIKey
					if reject {
						return ai.ProviderProbeResult{}, errors.New("rejected test credential " + cfg.APIKey)
					}
					return ai.ProviderProbeResult{Provider: "openai-compat", Success: true}, nil
				}
				opts.Nudge = func(context.Context, state.DaemonState) error { nudges++; return nil }
				return settings.NewService(ctx, opts)
			}
			t.Cleanup(func() { openCredentialSettingsService = oldOpen })
			service, err := openCredentialSettingsService(ctx, configureSettingsOptions(roots, repo, os.LookupEnv))
			if err != nil {
				t.Fatal(err)
			}
			validation, err := service.Validate(ctx, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			confirmed := validation.Confirmations
			tested, err := service.TestProvider(ctx, nil, confirmed)
			if err != nil {
				t.Fatal(err)
			}
			baseline, err := service.Apply(ctx, settings.ApplyRequest{TestedFingerprint: tested.Fingerprint, Confirmations: confirmed, ExpectedGeneration: validation.SourceGeneration})
			if err != nil {
				t.Fatal(err)
			}
			service.Close()
			if ok, err := state.AcknowledgeConfigActivation(ctx, db, baseline.RequestID, baseline.RevisionID); err != nil || !ok {
				t.Fatalf("ack: %t %v", ok, err)
			}
			if ok, err := state.ApplyConfigActivation(ctx, db, baseline.RequestID, baseline.RevisionID); err != nil || !ok {
				t.Fatalf("apply: %t %v", ok, err)
			}
			if err := state.SaveDaemonState(ctx, db, state.DaemonState{PID: os.Getpid(), Mode: "running"}); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(roots.ConfigPath())
			if err != nil {
				t.Fatal(err)
			}
			expectedDesired := baseline.RevisionID
			if scenario == "pending_activation" {
				prior, err := state.ConfigRevisionByID(ctx, db, baseline.RevisionID)
				if err != nil {
					t.Fatal(err)
				}
				pending, err := state.InsertConfigRevision(ctx, db, state.ConfigRevisionInput{Snapshot: []byte(prior.SnapshotJSON), Profile: prior.Profile, Scope: prior.Scope, SourceGeneration: prior.SourceGeneration, Reason: "pending test change"})
				if err != nil {
					t.Fatal(err)
				}
				if _, ok, err := state.RequestConfigActivation(ctx, db, pending.ID, sql.NullInt64{Int64: baseline.RevisionID, Valid: true}); err != nil || !ok {
					t.Fatalf("pending activation: %t %v", ok, err)
				}
				expectedDesired = pending.ID
			}
			probeKey = ""
			reject = scenario == "rejected"
			if scenario == "environment_override" {
				t.Setenv(ai.EnvAPIKey, "environment-test-key")
			}
			out, _, commandErr := executeAuth(t, "corrected-test-key\n", "config", "credentials", "set", "--stdin", "--repo", repo)
			if strings.Contains(out, "corrected-test-key") || commandErr != nil && strings.Contains(commandErr.Error(), "corrected-test-key") {
				t.Fatal("credential leaked")
			}
			activation, err := state.RuntimeConfigActivationState(ctx, db)
			if err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(roots.ConfigPath())
			if err != nil || string(before) != string(after) {
				t.Fatal("credential refresh changed saved choices or inheritance")
			}
			if scenario != "corrected" {
				if activation.DesiredRevisionID.Int64 != expectedDesired || nudges != 0 {
					t.Fatalf("failed/shadowed credential activated: %+v nudges=%d", activation, nudges)
				}
				if scenario == "pending_activation" && (commandErr == nil || probeKey != "") {
					t.Fatalf("pending activation was not preserved: err=%v probed=%t", commandErr, probeKey != "")
				}
				if scenario == "rejected" && commandErr == nil {
					t.Fatal("failed probe reported success")
				}
				if scenario == "environment_override" && (commandErr != nil || probeKey != "") {
					t.Fatalf("environment override probed: keyset=%t err=%v", probeKey != "", commandErr)
				}
				return
			}
			if commandErr != nil {
				t.Fatal(commandErr)
			}
			if probeKey != "corrected-test-key" || activation.DesiredRevisionID.Int64 <= baseline.RevisionID || activation.AppliedRevisionID.Int64 != baseline.RevisionID || nudges != 1 {
				t.Fatalf("refresh state=%+v keyset=%t nudges=%d", activation, probeKey != "", nudges)
			}
			previous, _ := state.ConfigRevisionByID(ctx, db, baseline.RevisionID)
			refreshed, err := state.ConfigRevisionByID(ctx, db, activation.DesiredRevisionID.Int64)
			if err != nil || previous.SnapshotJSON != refreshed.SnapshotJSON {
				t.Fatalf("refresh altered approved contract: %v\nbefore=%s\nafter=%s", err, previous.SnapshotJSON, refreshed.SnapshotJSON)
			}
			if !strings.Contains(out, repo) || !strings.Contains(out, "queued") {
				t.Fatalf("missing scoped activation outcome: %s", out)
			}
		})
	}
}
