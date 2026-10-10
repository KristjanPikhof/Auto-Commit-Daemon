package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestFinishOnboardingLocalDefaultsWithoutCredentials(t *testing.T) {
	for _, preview := range []bool{true, false} {
		t.Run(map[bool]string{true: "preview", false: "unattended"}[preview], func(t *testing.T) {
			roots := withIsolatedHome(t)
			store := credentials.NewStore(roots)
			if err := os.MkdirAll(filepath.Dir(store.Path()), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(store.Path(), []byte("invalid stored credential"), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := newSetupCommand(false)
			if !preview {
				if err := cmd.Flags().Set("expect-plan", "reviewed-plan"); err != nil {
					t.Fatal(err)
				}
			}
			state, err := prepareSetupOnboarding(cmd, roots, setupOnboardingOptions{}, preview, true)
			if err != nil {
				t.Fatal(err)
			}
			values := state.Configuration.Values
			if state.Selection.Strategy != "local" || values[config.FieldCommitFormat] != "imperative" || values[config.FieldIntentVerification] != "none" || values[config.FieldProvider] != "deterministic" || state.Credential != "" || state.Configuration.StoreCredential {
				t.Fatalf("defaults %+v", state)
			}
			if _, exists := values[config.FieldCommitPreset]; exists {
				t.Fatal("retired preset in setup")
			}
			if err := testSetupProvider(cmd, state); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(roots.ConfigPath()); !os.IsNotExist(err) {
				t.Fatalf("preparation wrote settings: %v", err)
			}
		})
	}
}

func TestFinishOnboardingExplicitIntentAndStyleRemainOptional(t *testing.T) {
	roots := withIsolatedHome(t)
	t.Setenv(ai.EnvAPIKey, "test-only-key")
	cmd := newSetupCommand(false)
	state, err := prepareSetupOnboarding(cmd, roots, setupOnboardingOptions{Strategy: "intent", Provider: "openai-compat", CommitFormat: "conventional", BaseURL: ai.DefaultOpenAIBaseURL, Model: "explicit-model"}, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if state.Draft[config.FieldCommitStrategy] != "intent" || state.Draft[config.FieldCommitFormat] != "conventional" || state.Draft[config.FieldModel] != "explicit-model" || state.Draft[config.FieldIntentVerification] != "none" || state.Configuration.StoreCredential {
		t.Fatalf("explicit choices %+v", state)
	}
	if _, err := os.Stat(roots.ConfigPath()); !os.IsNotExist(err) {
		t.Fatal("preview changed saved settings")
	}
}

func TestFinishOnboardingInitialSetupIsScopedAndFailsBeforeEnable(t *testing.T) {
	for _, scenario := range []string{"fresh", "existing", "interrupted"} {
		t.Run(scenario, func(t *testing.T) {
			roots := withIsolatedHome(t)
			repo := makeUnregisteredStartRepo(t)
			lookup := controlRepoLookup{Roots: roots}
			lookup.Worktree.Root = repo
			applied := false
			build := func(_ context.Context, got paths.Roots, opts installer.Options) (installer.Plan, error) {
				if got != roots || opts.Repo != repo || opts.Integrations != "none" || !opts.NonInteractive {
					t.Fatalf("setup scope %+v", opts)
				}
				return installer.Plan{FreshDefaults: scenario != "existing", ExistingInstall: scenario == "existing"}, nil
			}
			apply := func(_ context.Context, got paths.Roots, plan installer.Plan, _ installer.ApplyOptions) (installer.Result, error) {
				applied = true
				if got != roots || len(plan.Repositories) != 0 {
					t.Fatal("bootstrap affected repositories")
				}
				if scenario == "interrupted" {
					return installer.Result{}, errors.New("installation interrupted")
				}
				return installer.Result{}, nil
			}
			err := bootstrapControlSetupWith(t.Context(), lookup, build, apply)
			if scenario == "fresh" && (err != nil || !applied) {
				t.Fatalf("fresh setup: %v", err)
			}
			if scenario == "existing" && (ExitCode(err) != ExitActionRequired || applied || !strings.Contains(err.Error(), "acd setup")) {
				t.Fatalf("implicit upgrade: %v", err)
			}
			if scenario == "interrupted" && (err == nil || !strings.Contains(err.Error(), "not enabled")) {
				t.Fatalf("interrupted: %v", err)
			}
			if _, err := os.Stat(roots.RegistryPath()); !os.IsNotExist(err) {
				t.Fatal("bootstrap registered worktree before completed installation")
			}
		})
	}
}

func TestFinishOnboardingAccessibleDefaultNeedsOnlyModeAndStyle(t *testing.T) {
	roots := withIsolatedHome(t)
	cmd := newSetupCommand(false)
	cmd.SetIn(&onboardingByteReader{strings.NewReader("\n\n")})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	state, err := prepareSetupOnboarding(cmd, roots, setupOnboardingOptions{Accessible: true}, false, false)
	if err != nil {
		t.Fatalf("default prompts: %v\n%s", err, out.String())
	}
	if state.Selection.Strategy != "local" || state.Selection.CommitFormat != "imperative" {
		t.Fatalf("default %+v", state.Selection)
	}
	for _, forbidden := range []string{"API key", "Which provider", "Repair", "Strict review", "preset", "Model"} {
		if strings.Contains(out.String(), forbidden) {
			t.Fatalf("unnecessary %q prompt: %s", forbidden, out.String())
		}
	}
	if !strings.Contains(out.String(), "fix: handle setup retries") || !strings.Contains(out.String(), "Fix setup retries") {
		t.Fatal("style examples missing")
	}
}

type onboardingByteReader struct{ *strings.Reader }

func (r *onboardingByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.Reader.Read(p)
}
