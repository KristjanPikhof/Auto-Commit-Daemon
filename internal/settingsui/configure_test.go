package settingsui

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
)

func TestConfigureWizardAccessibleStagesReviewedIntentBalanced(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	input := strings.Join([]string{
		"", // intent
		"", // balanced
		"", // imperative
		"", // openai-compat
		"", // model
		"", // base URL
		"", // timeout
		"staged-secret",
		"y", // approve network diff context
		"",
	}, "\n")
	var out bytes.Buffer
	selection, err := RunConfigureWizard(context.Background(), ConfigureWizardOptions{
		Input: &bytewiseReader{r: strings.NewReader(input)}, Output: &out,
		Accessible: true, DetectedCommand: "make test",
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
		selection.VerificationMode != "fast" ||
		selection.VerificationCommand != "make test" ||
		selection.VerificationApproved || !selection.DiffContextApproved ||
		selection.Credential != "staged-secret" {
		t.Fatalf("selection=%+v\n%s", selection, out.String())
	}
	if strings.Contains(out.String(), "staged-secret") || strings.Contains(out.String(), "\x1b[") {
		t.Fatalf("accessible output leaked secret or color: %q", out.String())
	}
	for _, want := range []string{"Commit strategy", "Balanced (recommended)", "redacted repository diff context"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output missing %q\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "verification command") {
		t.Errorf("regular wizard asked the user to enter a verification command:\n%s", out.String())
	}
}

func TestConfigureWizardUsesSavedVerificationBeforeDetection(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	input := strings.Join([]string{
		"", "", "", "", // intent, balanced, imperative, provider
		"", "", "", // model, base URL, timeout
		"y", // approve diff context
		"",
	}, "\n")
	var out bytes.Buffer
	selection, err := RunConfigureWizard(context.Background(), ConfigureWizardOptions{
		Input: &bytewiseReader{r: strings.NewReader(input)}, Output: &out,
		Accessible: true, DetectedCommand: "make test", HasCredential: true,
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
	if selection.VerificationCommand != "go test ./..." {
		t.Fatalf("verification command=%q", selection.VerificationCommand)
	}
	if strings.Contains(out.String(), "verification command") {
		t.Fatalf("regular wizard asked for command input:\n%s", out.String())
	}
}

func TestConfigureWizardRequiresDetectedVerification(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	input := strings.Join([]string{
		"", "", "", "", // intent, balanced, imperative, provider
		"", "", "", // model, base URL, timeout
	}, "\n")
	var out bytes.Buffer
	_, err := RunConfigureWizard(context.Background(), ConfigureWizardOptions{
		Input: &bytewiseReader{r: strings.NewReader(input)}, Output: &out,
		Accessible: true, HasCredential: true,
		Defaults: map[string]string{
			"commit.strategy": "intent", "commit.preset": "balanced",
			"commit.format": "imperative", "ai.provider": "openai-compat",
			"ai.model": "gpt-5.4-mini", "ai.base_url": "https://api.openai.com/v1",
			"ai.timeout": "30s",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "choose Intent Fast") {
		t.Fatalf("err=%v\n%s", err, out.String())
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
		&bytewiseReader{r: strings.NewReader("y\ny\ny\n")}, &out, true,
		ConfigurePreviewApprovalOptions{
			VerificationMode: "full", VerificationCommand: "make test\nunsafe",
			RepairEnabled: true, RepairHorizon: "30m", RepairMaxCommits: "5",
		})
	if err != nil || !approval.Verification || !approval.Repair || !approval.Apply {
		t.Fatalf("approval=%+v err=%v\n%s", approval, err, out.String())
	}
	view := out.String()
	for _, want := range []string{"make test unsafe", "within 30m", "up to 5 commits"} {
		if !strings.Contains(view, want) {
			t.Errorf("missing %q\n%s", want, view)
		}
	}
	if strings.Contains(view, "make test\nunsafe") {
		t.Fatalf("unsanitized command rendered: %q", view)
	}
	if strings.Contains(view, "\x1b[") {
		t.Fatalf("final approval used rich terminal rendering: %q", view)
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
		{"intent", "balanced", "fast"},
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
