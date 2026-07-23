package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settingsui"
)

type fakeCLISettingsService struct {
	snapshot      settings.Snapshot
	seenScope     settings.Scope
	seenProfile   string
	confirmations []ai.ConfirmationRequirement
}

func (f *fakeCLISettingsService) Close() error { return nil }
func (f *fakeCLISettingsService) Snapshot(_ context.Context, scope settings.Scope, profile string) (settings.Snapshot, error) {
	f.seenScope, f.seenProfile = scope, profile
	return f.snapshot, nil
}
func (f *fakeCLISettingsService) Save(context.Context, settings.SaveRequest) (settings.SaveResult, error) {
	return settings.SaveResult{}, nil
}
func (f *fakeCLISettingsService) Validate(_ context.Context, _ map[string]string, c []ai.ConfirmationRequirement) (settings.Validation, error) {
	f.confirmations = append([]ai.ConfirmationRequirement(nil), c...)
	return settings.Validation{Fingerprint: "fp"}, nil
}
func (f *fakeCLISettingsService) TestProvider(_ context.Context, _ map[string]string, c []ai.ConfirmationRequirement) (settings.ProviderTestResult, error) {
	f.confirmations = append([]ai.ConfirmationRequirement(nil), c...)
	return settings.ProviderTestResult{Fingerprint: "fp", Provider: "deterministic", Success: true}, nil
}
func (f *fakeCLISettingsService) Apply(context.Context, settings.ApplyRequest) (settings.ApplyResult, error) {
	return settings.ApplyResult{}, nil
}
func (f *fakeCLISettingsService) Revert(context.Context, settings.RevertRequest) (settings.ApplyResult, error) {
	return settings.ApplyResult{}, nil
}

func withSettingsCommandFakes(t *testing.T, service *fakeCLISettingsService, runner func(settingsui.Backend, settingsui.Options) error) *int {
	t.Helper()
	oldOpen, oldRun, oldIn, oldOut := openSettingsCLIService, runSettingsUI, settingsInputTTY, settingsOutputTTY
	opened := 0
	openSettingsCLIService = func(context.Context, settings.Options) (settingsCLIService, error) { opened++; return service, nil }
	runSettingsUI = runner
	settingsInputTTY = func(io.Reader) bool { return true }
	settingsOutputTTY = func(io.Writer) bool { return true }
	t.Cleanup(func() {
		openSettingsCLIService, runSettingsUI, settingsInputTTY, settingsOutputTTY = oldOpen, oldRun, oldIn, oldOut
	})
	return &opened
}

func executeSettings(t *testing.T, args ...string) error {
	t.Helper()
	root := newRootCmd()
	root.SetIn(bytes.NewBufferString(""))
	root.SetOut(&bytes.Buffer{})
	root.SetErr(&bytes.Buffer{})
	root.SetArgs(append([]string{"settings"}, args...))
	return root.ExecuteContext(context.Background())
}

func TestSettingsRejectsFlagsAndNonTTYBeforeMutation(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	service := &fakeCLISettingsService{}
	opened := withSettingsCommandFakes(t, service, func(settingsui.Backend, settingsui.Options) error { return nil })
	settingsInputTTY = func(io.Reader) bool { return false }
	settingsOutputTTY = func(io.Writer) bool { return false }
	for _, args := range [][]string{{"--json"}, {"--global", "--repo", "/tmp/nope"}, {"--global", "--profile", "fast"}, {"--profile", " unsafe "}, {}} {
		if err := executeSettings(t, args...); err == nil {
			t.Fatalf("args %v succeeded", args)
		}
	}
	if *opened != 0 {
		t.Fatalf("service opened %d times", *opened)
	}
}

func TestSettingsScopeAccessibleAndTermDumbRouting(t *testing.T) {
	repo := materializeTestRepo(t, false)
	t.Chdir(repo)
	service := &fakeCLISettingsService{snapshot: settings.Snapshot{Scope: settings.ScopeRepository}}
	var got settingsui.Options
	opened := withSettingsCommandFakes(t, service, func(b settingsui.Backend, o settingsui.Options) error {
		got = o
		_, err := b.Snapshot(context.Background())
		return err
	})
	if err := executeSettings(t, "--accessible"); err != nil {
		t.Fatal(err)
	}
	if !got.Accessible || service.seenScope != settings.ScopeRepository {
		t.Fatalf("repo route %+v %s", got, service.seenScope)
	}
	if err := executeSettings(t, "--profile", "fast", "--repo", repo, "--accessible"); err != nil {
		t.Fatal(err)
	}
	if service.seenScope != settings.ScopeProfile || service.seenProfile != "fast" {
		t.Fatalf("profile route %s %q", service.seenScope, service.seenProfile)
	}
	if err := executeSettings(t, "--global", "--accessible"); err != nil {
		t.Fatal(err)
	}
	if service.seenScope != settings.ScopeGlobal {
		t.Fatalf("global route %s", service.seenScope)
	}
	t.Setenv("TERM", "dumb")
	if err := executeSettings(t, "--repo", repo); err != nil {
		t.Fatal(err)
	}
	if !got.Accessible {
		t.Fatal("TERM=dumb did not select accessible")
	}
	if *opened != 4 {
		t.Fatalf("opens=%d", *opened)
	}
}

func TestSettingsDistinctRiskConfirmationsReachAdapter(t *testing.T) {
	repo := materializeTestRepo(t, false)
	service := &fakeCLISettingsService{}
	withSettingsCommandFakes(t, service, func(b settingsui.Backend, _ settingsui.Options) error {
		_, err := b.Test(context.Background(), map[string]string{"ai.model": "m"})
		return err
	})
	err := executeSettings(t, "--repo", repo, "--accessible", "--confirm-endpoint-credentials", "--confirm-subprocess", "--confirm-diff-egress")
	if err != nil {
		t.Fatal(err)
	}
	want := []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials, ai.ConfirmationSubprocessExecution, ai.ConfirmationDiffEgress}
	if len(service.confirmations) != len(want) {
		t.Fatalf("confirmations=%v", service.confirmations)
	}
	for i := range want {
		if service.confirmations[i] != want[i] {
			t.Fatalf("confirmations=%v", service.confirmations)
		}
	}
}

func TestSettingsTerminalRunnerErrorIsReturned(t *testing.T) {
	repo := materializeTestRepo(t, false)
	withSettingsCommandFakes(t, &fakeCLISettingsService{}, func(settingsui.Backend, settingsui.Options) error { return errors.New("terminal restored: failure") })
	if err := executeSettings(t, "--repo", repo, "--accessible"); err == nil || err.Error() != "terminal restored: failure" {
		t.Fatalf("err=%v", err)
	}
}

func TestSettingsHelpDescribesScopesRisksAndSyntheticPrivacy(t *testing.T) {
	help := commandHelp(t, "settings")
	for _, want := range []string{"--profile", "--global", "--accessible", "--confirm-endpoint-credentials", "--confirm-subprocess", "--confirm-diff-egress", "synthetic content only", "set or unset", "next safe work boundary", "without changing running repositories"} {
		if !bytes.Contains([]byte(help), []byte(want)) {
			t.Errorf("help missing %q", want)
		}
	}
}
