package installer

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
)

func TestFinishOnboardingFreshDefaultsAndInstallerRollback(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh-local", true: "interrupted"}[fail], func(t *testing.T) {
			ctx := context.Background()
			roots, _, executable := installerFixture(t, ctx)
			plan, err := BuildPlan(ctx, roots, Options{Executable: executable, Integrations: "none", NonInteractive: true, SkipServiceCheck: true})
			if err != nil {
				t.Fatal(err)
			}
			options := ApplyOptions{Executor: &recordingExecutor{}, Ready: readyImmediately, SelfTest: selfTestImmediately}
			if fail {
				options.SelfTest = func(context.Context, Plan) error { return errors.New("interrupted scratch test") }
			}
			_, err = Apply(ctx, roots, plan, options)
			if fail {
				if err == nil {
					t.Fatal("interrupted install succeeded")
				}
				if _, err := os.Stat(roots.ManagedBinaryPath()); !os.IsNotExist(err) {
					t.Fatal("rollback left managed runtime")
				}
				if _, err := os.Stat(roots.ConfigPath()); !os.IsNotExist(err) {
					t.Fatal("rollback left defaults")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			doc, err := config.NewStore(roots).Load()
			if err != nil {
				t.Fatal(err)
			}
			for field, want := range map[string]string{config.FieldCommitStrategy: "local", config.FieldCommitFormat: "imperative", config.FieldProvider: "deterministic", config.FieldIntentVerification: "none"} {
				resolved, err := config.ResolveField(field, config.ResolveInput{Global: doc.Settings.Global})
				if err != nil || resolved.EffectiveValue() != want {
					t.Fatalf("%s=%s: %v", field, resolved.EffectiveValue(), err)
				}
			}
			if _, found := doc.Settings.Global[config.FieldCommitPreset]; found {
				t.Fatal("fresh install saved retired preset")
			}
		})
	}
}
