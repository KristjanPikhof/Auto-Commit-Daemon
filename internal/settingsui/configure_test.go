package settingsui

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
)

func TestConfigureWizardAccessibleStagesGlobalIntentBalancedWithoutTests(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	input := strings.Join([]string{
		"", // Everyday
		"", // openai-compat
		"https://gateway.example/v1",
		"gateway-model",
		"staged-secret",
		"",
	}, "\n")
	var out bytes.Buffer
	selection, err := RunConfigureWizard(context.Background(), ConfigureWizardOptions{
		Input: &bytewiseReader{r: strings.NewReader(input)}, Output: &out,
		Accessible: true, DetectedQuickCommand: "go test ./... -run '^$'",
		DetectedQuickSource: "Go language default",
		Defaults: map[string]string{
			"commit.strategy": "intent", "commit.preset": "balanced",
			"commit.format": "imperative", "ai.provider": "openai-compat",
			"ai.model": "gpt-5.4-mini", "ai.base_url": "https://api.openai.com/v1",
			"ai.timeout": "30s",
		},
	})
	if err != nil {
		t.Fatalf("wizard: %v\n%s", err, out.String())
	}
	if selection.Strategy != "intent" || selection.Preset != "balanced" ||
		selection.VerificationMode != "structural" ||
		selection.VerificationCommand != "" ||
		selection.ExecutionMode != "immediate" ||
		selection.VerificationApproved || !selection.DiffContextApproved ||
		selection.Credential != "staged-secret" ||
		selection.BaseURL != "https://gateway.example/v1" ||
		selection.Model != "gateway-model" {
		t.Fatalf("selection=%+v\n%s", selection, out.String())
	}
	if strings.Contains(out.String(), "staged-secret") || strings.Contains(out.String(), "\x1b[") {
		t.Fatalf("accessible output leaked secret or color: %q", out.String())
	}
	for _, want := range []string{
		"How should ACD work?", "Everyday work", "Commit message provider",
		"OpenAI-compatible endpoint", "Model", "API key",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output missing %q\n%s", want, out.String())
		}
	}
	for _, forbidden := range []string{
		"verification command", "Commit message format", "Provider timeout",
		"Strict review", "background check",
	} {
		if strings.Contains(out.String(), forbidden) {
			t.Errorf("regular wizard rendered technical prompt %q:\n%s",
				forbidden, out.String())
		}
	}
}

func TestConfigureWizardBalancedIgnoresSavedAndDetectedVerification(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	input := "\n"
	var out bytes.Buffer
	selection, err := RunConfigureWizard(context.Background(), ConfigureWizardOptions{
		Input: &bytewiseReader{r: strings.NewReader(input)}, Output: &out,
		Accessible: true, DetectedCommand: "make test", HasCredential: true,
		ProviderConfigured: true, OpenAIConfigured: true,
		Defaults: map[string]string{
			"commit.strategy": "intent", "commit.preset": "balanced",
			"commit.format": "imperative", "ai.provider": "openai-compat",
			"ai.model": "gpt-5.4-mini", "ai.base_url": "https://api.openai.com/v1",
			"ai.timeout": "30s", "verification.fast.command": "go test ./...",
		},
	})
	if err != nil {
		t.Fatalf("wizard: %v\n%s", err, out.String())
	}
	if selection.VerificationMode != "structural" || selection.VerificationCommand != "" ||
		selection.VerificationSource != "" {
		t.Fatalf("selection=%+v", selection)
	}
	for _, forbidden := range []string{
		"Commit message provider", "API key", "verification command",
		"Model", "base URL", "Provider timeout",
	} {
		if strings.Contains(out.String(), forbidden) {
			t.Fatalf("existing provider flow rendered %q:\n%s",
				forbidden, out.String())
		}
	}
}

func TestConfigureWizardStrictUnavailableWithoutFullCheck(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	var out bytes.Buffer
	_, err := RunConfigureWizard(context.Background(), ConfigureWizardOptions{
		Input:  &bytewiseReader{r: strings.NewReader("3\n")},
		Output: &out, Accessible: true, HasCredential: true,
		RepositoryScoped:   true,
		ProviderConfigured: true, OpenAIConfigured: true,
		Defaults: map[string]string{
			"commit.strategy": "intent", "commit.preset": "balanced",
			"commit.format": "imperative", "ai.provider": "openai-compat",
			"ai.model":    "gpt-5.4-mini",
			"ai.base_url": "https://api.openai.com/v1",
			"ai.timeout":  "30s",
		},
	})
	if err == nil || !strings.Contains(err.Error(),
		"Strict Review is unavailable") ||
		!strings.Contains(err.Error(), "acd settings") {
		t.Fatalf("err=%v\n%s", err, out.String())
	}
}

func TestConfigureWizardGlobalExplicitStrictIsUnavailable(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	var out bytes.Buffer
	_, err := RunConfigureWizard(context.Background(), ConfigureWizardOptions{
		Input: &bytewiseReader{r: strings.NewReader("")}, Output: &out,
		Accessible: true, HasCredential: true,
		ExplicitMode:       true,
		ProviderConfigured: true, OpenAIConfigured: true,
		Defaults: map[string]string{
			"commit.strategy": "intent", "commit.preset": "quality",
			"commit.format": "imperative", "ai.provider": "openai-compat",
			"ai.model": "gpt-5.4-mini", "ai.base_url": "https://api.openai.com/v1",
			"ai.timeout": "30s",
		},
	})
	if err == nil || !strings.Contains(err.Error(),
		"Strict Review is available only for repository-scoped setup") {
		t.Fatalf("err=%v\n%s", err, out.String())
	}
}

func TestConfigureWizardRepositoryStrictUsesDetectedFullCheck(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	var out bytes.Buffer
	selection, err := RunConfigureWizard(context.Background(), ConfigureWizardOptions{
		Input:  &bytewiseReader{r: strings.NewReader("3\n")},
		Output: &out, Accessible: true, HasCredential: true,
		RepositoryScoped:   true,
		ProviderConfigured: true, OpenAIConfigured: true,
		DetectedQuickCommand: "go test ./... -run '^$'",
		DetectedQuickSource:  "Go language default",
		DetectedFullCommand:  "make test",
		DetectedFullSource:   "Makefile target",
		Defaults: map[string]string{
			"commit.strategy": "intent", "commit.preset": "balanced",
			"commit.format": "imperative", "ai.provider": "openai-compat",
			"ai.model": "gpt-5.4-mini", "ai.base_url": "https://api.openai.com/v1",
			"ai.timeout": "30s",
		},
	})
	if err != nil {
		t.Fatalf("wizard: %v\n%s", err, out.String())
	}
	if selection.Experience != "strict" ||
		selection.Strategy != "intent" || selection.Preset != "quality" ||
		selection.VerificationMode != "full" ||
		selection.VerificationCommand != "make test" ||
		selection.VerificationSource != "Makefile target" ||
		selection.ExecutionMode != "background activation gate" {
		t.Fatalf("selection=%+v\n%s", selection, out.String())
	}
	if !strings.Contains(out.String(), "Strict review") {
		t.Fatalf("repository setup did not offer Strict Review:\n%s", out.String())
	}
}

func TestSettingsModeIsFirstClassBeforeProviderAndAdvanced(t *testing.T) {
	view := AccessibleStartTranscript(nil)
	mode := strings.Index(view, "Change strategy or preset")
	quick := strings.Index(view, "Quick provider")
	advanced := strings.Index(view, "Advanced settings")
	if mode < 0 || quick < 0 || advanced < 0 || !(mode < quick && quick < advanced) {
		t.Fatalf("action order:\n%s", view)
	}
}

func TestRichSettingsModeKeyCyclesStrategyAndPreset(t *testing.T) {
	m := New(&fakeBackend{})
	m.Draft = map[string]string{"commit.strategy": "intent", "commit.preset": "balanced"}
	next, _ := updated(t, m, keyMsg("m"))
	if next.Draft["commit.strategy"] != "intent" || next.Draft["commit.preset"] != "fast" {
		t.Fatalf("draft=%v", next.Draft)
	}
	if !next.Dirty["commit.strategy"] || !next.Dirty["commit.preset"] ||
		!strings.Contains(next.Status, "MODE:") {
		t.Fatalf("mode state=%+v", next)
	}
	if !strings.Contains(next.Render(), "strategy/preset") {
		t.Fatalf("rich mode action not rendered:\n%s", next.Render())
	}
}

func TestRichFastToQualityResetsOnlyPresetSources(t *testing.T) {
	m := New(&fakeBackend{})
	m.Draft = map[string]string{
		config.FieldCommitStrategy:          "intent",
		config.FieldCommitPreset:            "fast",
		config.FieldIntentWindow:            "10",
		config.FieldIntentVerification:      "none",
		config.FieldIntentRepairHorizon:     "7m",
		config.FieldVerificationFullCommand: "make quality",
	}
	m.Snapshot.Fields = []FieldValue{
		{Key: config.FieldIntentWindow, Value: "10", Source: string(config.SourcePreset)},
		{Key: config.FieldIntentVerification, Value: "none", Source: string(config.SourcePreset)},
		{Key: config.FieldIntentRepairHorizon, Value: "7m", Source: string(config.SourceRepository)},
		{Key: config.FieldVerificationFullCommand, Value: "make quality", Source: string(config.SourceRepository)},
	}
	next, _ := updated(t, m, keyMsg("m"))
	if next.Draft[config.FieldCommitPreset] != "quality" ||
		next.Draft[config.FieldIntentWindow] != "30" ||
		next.Draft[config.FieldIntentVerification] != "full" ||
		next.Draft[config.FieldIntentRepairHorizon] != "7m" ||
		next.Draft[config.FieldVerificationFullCommand] != "make quality" {
		t.Fatalf("draft=%v", next.Draft)
	}
}

func TestAccessibleFastToQualityResetsOnlyPresetSources(t *testing.T) {
	draft := map[string]string{
		config.FieldCommitStrategy:      "intent",
		config.FieldCommitPreset:        "fast",
		config.FieldIntentWindow:        "10",
		config.FieldIntentVerification:  "none",
		config.FieldIntentRepairHorizon: "7m",
	}
	fields := []FieldValue{
		{Key: config.FieldIntentWindow, Source: string(config.SourcePreset)},
		{Key: config.FieldIntentVerification, Source: string(config.SourcePreset)},
		{Key: config.FieldIntentRepairHorizon, Source: string(config.SourceRepository)},
	}
	values := finalizeAccessibleValues(newAccessibleValues(draft))
	applyAccessibleModeSelection(&values, fields, "intent", "quality")
	if values.Values[config.FieldCommitPreset] != "quality" ||
		values.Values[config.FieldIntentWindow] != "30" ||
		values.Values[config.FieldIntentVerification] != "full" ||
		values.Values[config.FieldIntentRepairHorizon] != "7m" {
		t.Fatalf("values=%v", values.Values)
	}
}

func TestConfigureFinalApprovalBindsCommandAndRepair(t *testing.T) {
	var out bytes.Buffer
	approval, err := ConfirmConfigurePreview(context.Background(),
		&bytewiseReader{r: strings.NewReader("y\n")}, &out, true,
		ConfigurePreviewApprovalOptions{
			VerificationMode: "full", VerificationCommand: "make test\nunsafe",
			RepairEnabled: true, RepairHorizon: "30m", RepairMaxCommits: "5",
		})
	if err != nil || !approval.Verification || !approval.Repair || !approval.Apply {
		t.Fatalf("approval=%+v err=%v\n%s", approval, err, out.String())
	}
	view := out.String()
	if !strings.Contains(view, "Approve every permission shown above") {
		t.Errorf("single approval missing:\n%s", view)
	}
	if strings.Contains(view, "make test") || strings.Contains(view, "30m") {
		t.Fatalf("single approval repeated preview details: %q", view)
	}
	if strings.Contains(view, "\x1b[") {
		t.Fatalf("final approval used rich terminal rendering: %q", view)
	}
}

func TestConfigureGlobalFinalApprovalDoesNotPromiseActivation(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	var out bytes.Buffer
	approval, err := ConfirmConfigurePreview(
		context.Background(),
		&bytewiseReader{r: strings.NewReader("y\n")},
		&out,
		true,
		ConfigurePreviewApprovalOptions{Global: true, RepairEnabled: true},
	)
	if err != nil || !approval.Apply || !approval.Repair {
		t.Fatalf("approval=%+v err=%v\n%s", approval, err, out.String())
	}
	if !strings.Contains(out.String(), "save these global defaults") ||
		strings.Contains(out.String(), "enable ACD") {
		t.Fatalf("global approval prompt=%q", out.String())
	}
}

func TestConfigureHelpersKeepLocalProviderAndPresetVerification(t *testing.T) {
	kind, name := configureProviderParts("subprocess:planner")
	if kind != "subprocess" || name != "planner" {
		t.Fatalf("provider=%q %q", kind, name)
	}
	for _, test := range []struct {
		strategy string
		preset   string
		want     string
	}{
		{"event", "quality", "none"},
		{"intent", "fast", "none"},
		{"intent", "balanced", "structural"},
		{"intent", "quality", "full"},
	} {
		if got := configureVerificationMode(test.strategy, test.preset); got != test.want {
			t.Errorf("%s/%s=%s want %s", test.strategy, test.preset, got, test.want)
		}
	}
}

func TestConfigureSecretReaderDoesNotConsumeFollowingAnswers(t *testing.T) {
	input := strings.NewReader("staged-secret\nnext-answer\n")
	value, err := readConfigureSecretLine(input, 64)
	if err != nil || value != "staged-secret" {
		t.Fatalf("value=%q err=%v", value, err)
	}
	remaining := new(strings.Builder)
	if _, err := io.Copy(remaining, input); err != nil || remaining.String() != "next-answer\n" {
		t.Fatalf("remaining=%q err=%v", remaining.String(), err)
	}
}
