package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	restorepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/restore"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func finishConfigFixture(t *testing.T) (paths.Roots, *workerRuntime) {
	t.Helper()
	roots := withIsolatedHome(t)
	for _, field := range config.Catalog() {
		t.Setenv(field.Environment, field.Default)
	}
	repo, _, db := makeRepoStateDB(t)
	wt, err := git.ResolveWorktree(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	return roots, &workerRuntime{worktree: wt, db: db, policy: restorepkg.ProtectionPolicy{MaxFileBytes: 123456, Sensitive: state.NewSensitiveMatcher(), SafeIgnore: state.NewSafeIgnoreMatcher()}}
}

func finishConfigRevision(t *testing.T, db *state.DB, payload map[string]any, expected sql.NullInt64, apply bool) (state.ConfigRevision, state.ConfigActivationRequest) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	revision, err := state.InsertConfigRevision(ctx, db, state.ConfigRevisionInput{Snapshot: body, Scope: "repo", Profile: "default"})
	if err != nil {
		t.Fatal(err)
	}
	request, ok, err := state.RequestConfigActivation(ctx, db, revision.ID, expected)
	if err != nil || !ok {
		t.Fatalf("request %t: %v", ok, err)
	}
	if apply {
		if ok, err := state.AcknowledgeConfigActivation(ctx, db, request.ID, revision.ID); err != nil || !ok {
			t.Fatalf("acknowledge %t: %v", ok, err)
		}
		if ok, err := state.ApplyConfigActivation(ctx, db, request.ID, revision.ID); err != nil || !ok {
			t.Fatalf("apply %t: %v", ok, err)
		}
	}
	return revision, request
}

func TestFinishCLIConfigLocalIsOfflineAndUsesApprovedSnapshot(t *testing.T) {
	roots, runtime := finishConfigFixture(t)
	ctx := context.Background()
	payload := map[string]any{config.FieldCommitStrategy: " Event ", config.FieldCommitFormat: "conventional", config.FieldProvider: "openai-compat", config.FieldDiffEgress: true, config.FieldCAFile: "/missing/trust.pem", "intent.window": map[string]any{"retired": true}, "preset_version": 1}
	revision, _ := finishConfigRevision(t, runtime.db, payload, sql.NullInt64{}, true)
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Global[config.FieldCommitStrategy] = json.RawMessage(`"intent"`)
		doc.Settings.Global[config.FieldCommitFormat] = json.RawMessage(`"imperative"`)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ACD_AI_API_KEY", "invalid\nsecret")
	t.Setenv("ACD_COMMIT_STRATEGY", "intent")
	if err := os.WriteFile(credentials.NewStore(roots).Path(), []byte("malformed credential"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(roots.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolveFinishWorkerConfig(ctx, roots, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode != "local" || got.Style != "conventional" || got.Provider.Mode != "deterministic" || got.Provider.APIKey != "" || got.Provider.DiffEgress || got.ConfigDigest != fmt.Sprintf("revision:%d:%s", revision.ID, revision.SnapshotHash) {
		t.Fatalf("approved local inputs changed: %+v", got)
	}
	if got.AuthorName != "ACD Test" || got.AuthorEmail != "acd@example.invalid" || got.Capture.MaxFileBytes != runtime.policy.MaxFileBytes || got.Capture.SensitiveMatcher != runtime.policy.Sensitive {
		t.Fatalf("worker identity/policy lost: %+v", got)
	}
	after, _ := os.ReadFile(roots.ConfigPath())
	if string(before) != string(after) {
		t.Fatal("resolver wrote authoring configuration")
	}
}

func TestFinishCLIConfigOnlyAppliedRevisionsAreApproved(t *testing.T) {
	for _, baseline := range []bool{false, true} {
		for _, status := range []string{"pending", "acknowledged", "rejected", "applied"} {
			t.Run(fmt.Sprintf("baseline_%t/%s", baseline, status), func(t *testing.T) {
				roots, runtime := finishConfigFixture(t)
				ctx := context.Background()
				var expected sql.NullInt64
				if baseline {
					revision, _ := finishConfigRevision(t, runtime.db, map[string]any{config.FieldCommitFormat: "imperative"}, expected, true)
					expected = sql.NullInt64{Int64: revision.ID, Valid: true}
				}
				revision, request := finishConfigRevision(t, runtime.db, map[string]any{config.FieldCommitFormat: "conventional"}, expected, status == "applied")
				if status == "acknowledged" {
					if ok, err := state.AcknowledgeConfigActivation(ctx, runtime.db, request.ID, revision.ID); err != nil || !ok {
						t.Fatalf("acknowledge %t: %v", ok, err)
					}
				}
				if status == "rejected" {
					if ok, err := state.RejectConfigActivation(ctx, runtime.db, request.ID, revision.ID, "not approved"); err != nil || !ok {
						t.Fatalf("reject %t: %v", ok, err)
					}
				}
				got, err := resolveFinishWorkerConfig(ctx, roots, runtime)
				if status != "applied" && !baseline {
					if err == nil || !strings.Contains(err.Error(), "not approved") {
						t.Fatalf("unapproved initial config used %+v: %v", got, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				want := "imperative"
				if status == "applied" {
					want = "conventional"
				}
				if got.Style != want {
					t.Fatalf("style=%s want=%s", got.Style, want)
				}
			})
		}
	}
}

func TestFinishCLIConfigExactCommandConsentAndCredentials(t *testing.T) {
	for _, consent := range []bool{false, true} {
		t.Run(fmt.Sprintf("consent_%t", consent), func(t *testing.T) {
			roots, runtime := finishConfigFixture(t)
			confirmed := []string{string(ai.ConfirmationDiffEgress)}
			if consent {
				confirmed = append(confirmed, string(ai.ConfirmationVerificationCommand))
			}
			finishConfigRevision(t, runtime.db, map[string]any{config.FieldCommitStrategy: "intent", config.FieldProvider: "openai-compat", config.FieldDiffEgress: true, config.FieldIntentVerification: "fast", config.FieldVerificationFastCommand: "printf approved", config.FieldVerificationFastTimeout: "17s", "confirmations": confirmed}, sql.NullInt64{}, true)
			t.Setenv("ACD_AI_API_KEY", "first-token")
			got, err := resolveFinishWorkerConfig(context.Background(), roots, runtime)
			if !consent {
				if err == nil || !strings.Contains(err.Error(), "verification_command") {
					t.Fatalf("unapproved command accepted: %v", err)
				}
				return
			}
			if err != nil || got.Provider.APIKey != "first-token" || got.Command.Identity().Timeout != 17*time.Second || got.Command.Identity().ApprovalID != got.ConfigDigest {
				t.Fatalf("exact approved config %+v: %v", got, err)
			}
			t.Setenv("ACD_AI_API_KEY", "refreshed-token")
			retry, err := resolveFinishWorkerConfig(context.Background(), roots, runtime)
			if err != nil || retry.ConfigDigest != got.ConfigDigest || retry.Provider.APIKey != "refreshed-token" || retry.Command.Identity() != got.Command.Identity() {
				t.Fatalf("credential refresh changed approved identity: %v", err)
			}
		})
	}
}

func TestFinishCLIConfigGlobalApprovalBindsEffectiveValues(t *testing.T) {
	roots, runtime := finishConfigFixture(t)
	store := config.NewStore(roots)
	if err := store.Update(func(doc *config.Document) error {
		doc.Settings.Global[config.FieldCommitStrategy] = json.RawMessage(`"intent"`)
		doc.Settings.Global[config.FieldProvider] = json.RawMessage(`"subprocess:approved"`)
		doc.Settings.Global[config.FieldDiffEgress] = json.RawMessage(`true`)
		values, err := finishResolveValues(config.ResolveInput{Global: doc.Settings.Global})
		if err != nil {
			return err
		}
		fingerprint, err := config.SettingsFingerprint(values)
		doc.Settings.GlobalSetupApproval = &config.GlobalSetupApproval{Generation: doc.Generation + 1, Fingerprint: fingerprint, Confirmations: []string{"subprocess_execution", "diff_egress"}}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := resolveFinishWorkerConfig(context.Background(), roots, runtime); err != nil || got.Provider.Mode != "subprocess:approved" || len(got.Approved) != 2 {
		t.Fatalf("exact global approval rejected %+v: %v", got, err)
	}
	if err := store.Update(func(doc *config.Document) error {
		doc.Settings.Repositories[central.CanonicalID(runtime.worktree.Root)] = config.RepositorySettings{Fields: config.Overrides{config.FieldModel: json.RawMessage(`"different-model"`)}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveFinishWorkerConfig(context.Background(), roots, runtime); err == nil || !strings.Contains(err.Error(), "subprocess_execution") {
		t.Fatalf("changed effective settings reused approval: %v", err)
	}
}

func TestFinishCLIConfigLegacyDefaultsAndAbsentAuthor(t *testing.T) {
	roots, runtime := finishConfigFixture(t)
	if err := os.MkdirAll(roots.Config, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := []byte(`{"version":1,"settings":{"global":{"commit.strategy":"intent","commit.preset":"quality","verification.full.command":"printf approved"}}}`)
	if err := os.WriteFile(roots.ConfigPath(), legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	// Clear only these overrides so the archived choices are effective.
	for _, name := range []string{"ACD_INTENT_VERIFICATION", "ACD_AI_DIFF_EGRESS"} {
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	values, approved, err := finishSavedValues(roots, runtime.worktree.Root)
	if err != nil || values[config.FieldIntentVerification] != "full" || values[config.FieldDiffEgress] != "true" || len(approved) != 0 {
		t.Fatalf("legacy choices/consent changed %v %v: %v", values, approved, err)
	}
	if _, err := resolveFinishWorkerConfig(context.Background(), roots, runtime); err == nil || !strings.Contains(err.Error(), "approval") {
		t.Fatalf("migration granted consent: %v", err)
	}
	if err := os.Remove(roots.ConfigPath()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"user.name", "user.email"} {
		if _, err := git.Run(context.Background(), git.RunOpts{Dir: runtime.worktree.Root}, "config", "--unset", name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := git.Run(context.Background(), git.RunOpts{Dir: runtime.worktree.Root}, "config", "user.useConfigOnly", "true"); err != nil {
		t.Fatal(err)
	}
	got, err := resolveFinishWorkerConfig(context.Background(), roots, runtime)
	if err != nil || got.AuthorName != "" || got.AuthorEmail != "" {
		t.Fatalf("missing author blocked empty finish %+v: %v", got, err)
	}
	after, err := os.ReadFile(roots.ConfigPath())
	if !os.IsNotExist(err) || len(after) != 0 {
		t.Fatal("resolution recreated authoring config")
	}
}

func TestFinishCLIConfigRejectsUnsharedCodeBeforeReadingCredentials(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			roots, runtime := finishConfigFixture(t)
			finishConfigRevision(t, runtime.db, map[string]any{
				config.FieldCommitStrategy: "intent", config.FieldProvider: "openai-compat",
				config.FieldDiffEgress: enabled,
			}, sql.NullInt64{}, true)
			t.Setenv(ai.EnvAPIKey, "invalid\ncredential")
			_, err := resolveFinishWorkerConfig(t.Context(), roots, runtime)
			if err == nil || !strings.Contains(err.Error(), "acd config") || !strings.Contains(err.Error(), "diff") {
				t.Fatalf("missing code sharing was not rejected before credentials: %v", err)
			}
		})
	}
}

func TestFinishCLIConfigSavedHistoryNeedsNoNewCodeApproval(t *testing.T) {
	roots, runtime := finishConfigFixture(t)
	finishConfigRevision(t, runtime.db, map[string]any{
		config.FieldCommitStrategy: "intent", config.FieldProvider: "openai-compat",
		config.FieldDiffEgress: false,
	}, sql.NullInt64{}, true)
	t.Setenv(ai.EnvAPIKey, "invalid\ncredential")
	cfg, err := resolveFinishWorkerConfigInputs(t.Context(), roots, runtime, false)
	if err != nil || cfg.Provider.DiffEgress || cfg.Provider.APIKey != "" {
		t.Fatalf("applying saved history required new code sharing: %+v %v", cfg, err)
	}
}
