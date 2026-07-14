package settingsui

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestAccessibleFormUsesSharedDescriptors(t *testing.T) {
	var out bytes.Buffer
	form, values := AccessibleForm(map[string]string{"ai.model": "gpt-test", "ai.api_key": "never-render"}, strings.NewReader(""), &out)
	if form == nil || values.Values["ai.model"] != "gpt-test" {
		t.Fatal("accessible draft not initialized")
	}
	if _, ok := values.Values["ai.api_key"]; ok {
		t.Fatal("sensitive descriptor included")
	}
	view := form.View()
	if strings.Contains(view, "never-render") {
		t.Fatal("secret rendered")
	}
}

func TestAccessibleFormShowsRetainedValueForEveryPrompt(t *testing.T) {
	draft := map[string]string{"ai.api_key": "never-render", "trace.prompt": "never-render"}
	wantTitles := make([]string, 0, len(Fields()))
	nonSensitiveFields := 0
	for _, desc := range Fields() {
		if desc.Sensitive {
			continue
		}
		nonSensitiveFields++
		value := "value-for-" + strings.ReplaceAll(desc.Key, ".", "-")
		draft[desc.Key] = value
		wantTitles = append(wantTitles, accessibleRetainedValueTitle(desc.Label+" ("+desc.Apply+")", value))
	}

	// Keep every field, profile, experiment value, and policy; select the
	// default save action; then confirm. Huh retains input defaults on blank
	// lines, so the output must make every retained value visible.
	const blankNonFieldPrompts = 5 // profile, budget, expiry, policy, and action
	input := strings.Repeat("\n", nonSensitiveFields+blankNonFieldPrompts) + "y\n"
	var out bytes.Buffer
	values, err := RunAccessible(context.Background(), draft, &bytewiseReader{r: strings.NewReader(input)}, &out)
	if err != nil {
		t.Fatalf("accessible form: %v\n%s", err, out.String())
	}
	if values.Action != "save" || values.ExperimentBudget != "10" || values.ExperimentExpiry != "none" || values.ExperimentPolicy != "continue" {
		t.Fatalf("defaults not retained: %#v", values)
	}
	view := out.String()
	for _, title := range wantTitles {
		if !strings.Contains(view, title) {
			t.Errorf("missing retained-value prompt %q", title)
		}
	}
	for _, title := range []string{
		"Experiment window budget (1-1000) [current: 10; Enter keeps current]",
		"Experiment expiry (none, 15m, or 1h) [current: none; Enter keeps current]",
		"Experiment failure policy [current: continue; Enter keeps current]",
		"Next action [current: save draft only; Enter keeps current]",
		"Save this draft without testing or applying it?",
	} {
		if !strings.Contains(view, title) {
			t.Errorf("missing accessible default guidance %q", title)
		}
	}
	if strings.Contains(view, "never-render") || strings.Contains(view, "API key [current:") || strings.Contains(view, "Prompt trace [current:") {
		t.Fatal("sensitive retained value rendered")
	}
}

type bytewiseReader struct{ r io.Reader }

func (r *bytewiseReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return r.r.Read(p[:1])
}

func TestAccessibleNoColorControlSanitize(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	view := AccessibleTranscript(map[string]string{"ai.model": "safe\x1b[31m\x00"})
	if strings.Contains(view, "\x1b[") || strings.Contains(view, "\x00") {
		t.Fatalf("control output %q", view)
	}
	if !strings.Contains(view, "safe") {
		t.Fatal("sanitized value absent")
	}
}

func TestAccessibleFormLabelsTestAndApplyRisk(t *testing.T) {
	view := AccessibleTranscript(nil)
	for _, s := range []string{"Next action", "strict synthetic test", "next safe boundary", "paid synthetic request"} {
		if !strings.Contains(strings.ToLower(view), strings.ToLower(s)) {
			t.Errorf("missing %q", s)
		}
	}
}

func TestAccessibleActionDefaultsToTestCurrentSettings(t *testing.T) {
	var out bytes.Buffer
	action, err := RunAccessibleAction(context.Background(), &bytewiseReader{r: strings.NewReader("\n")}, &out)
	if err != nil || action != "test" {
		t.Fatalf("action=%q err=%v\n%s", action, err, out.String())
	}
	if !strings.Contains(out.String(), "Test current settings (recommended)") {
		t.Fatalf("action-first prompt missing\n%s", out.String())
	}
}

func TestAccessibleQuickSetupOnlyPromptsProviderEssentials(t *testing.T) {
	draft := map[string]string{"ai.provider": "deterministic", "ai.model": "model", "intent.min_pending": "2"}
	var out bytes.Buffer
	values, err := RunAccessibleQuick(context.Background(), draft,
		&bytewiseReader{r: strings.NewReader(strings.Repeat("\n", len(quickProviderFields)+1))}, &out)
	if err != nil || values.Action != "test" || values.Values["intent.min_pending"] != "2" {
		t.Fatalf("values=%+v err=%v\n%s", values, err, out.String())
	}
	view := out.String()
	for _, want := range []string{"Provider", "Model", "Base URL", "Timeout", "CA file", "After quick setup"} {
		if !strings.Contains(view, want) {
			t.Errorf("quick setup missing %q", want)
		}
	}
	if strings.Contains(view, "Minimum pending") || strings.Contains(view, "Experiment window budget") {
		t.Fatalf("quick setup exposed advanced fields\n%s", view)
	}
}

func TestAccessibleTestCurrentSkipsAdvancedCatalog(t *testing.T) {
	b := &fakeBackend{snapshot: baseSnapshot()}
	var out bytes.Buffer
	input := &bytewiseReader{r: strings.NewReader("\ny\n")}
	if err := runAccessibleBackend(context.Background(), b, Options{Input: input, Output: &out, Accessible: true}); err != nil {
		t.Fatal(err)
	}
	view := out.String()
	if !strings.Contains(view, "TESTED:") || !strings.Contains(view, "What do you want to do?") {
		t.Fatalf("test-current output missing\n%s", view)
	}
	if strings.Contains(view, "Minimum pending (next safe boundary) [current:") {
		t.Fatalf("test-current traversed advanced fields\n%s", view)
	}
}

func TestAccessibleCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := RunAccessible(ctx, nil, strings.NewReader(""), &bytes.Buffer{})
	if err == nil {
		t.Fatal("cancelled form succeeded")
	}
}

func TestAccessibleDispatchSaveTestAndApply(t *testing.T) {
	b := &fakeBackend{snapshot: baseSnapshot()}
	for _, action := range []string{"save", "test", "apply"} {
		var out bytes.Buffer
		if err := dispatchAccessible(context.Background(), b, AccessibleValues{Action: action, Values: map[string]string{"ai.model": "safe"}}, &out); err != nil {
			t.Fatalf("%s: %v", action, err)
		}
		if action == "apply" && !strings.Contains(out.String(), "QUEUED:") {
			t.Fatalf("apply output=%q", out.String())
		}
	}
}

func TestAccessibleDispatchSanitizesBackendError(t *testing.T) {
	b := &errorBackend{fakeBackend: fakeBackend{snapshot: baseSnapshot()}}
	err := dispatchAccessible(context.Background(), b, AccessibleValues{Action: "test"}, &bytes.Buffer{})
	if err == nil || strings.Contains(err.Error(), "\x1b") || strings.Contains(err.Error(), "\x00") {
		t.Fatalf("error=%q", err)
	}
}

type errorBackend struct{ fakeBackend }

func (b *errorBackend) Test(context.Context, map[string]string) (TestResult, error) {
	return TestResult{}, fmt.Errorf("rejected\x1b[2J\x00")
}

type accessibleRiskBackend struct {
	fakeBackend
	confirmed  map[string]bool
	testCalls  int
	applyCalls int
}

func (b *accessibleRiskBackend) Confirm(requirements []ConfirmationRequirement) {
	if b.confirmed == nil {
		b.confirmed = map[string]bool{}
	}
	for _, requirement := range requirements {
		b.confirmed[requirement.ID] = true
	}
}

func (b *accessibleRiskBackend) Test(context.Context, map[string]string) (TestResult, error) {
	b.testCalls++
	if !b.confirmed["endpoint_credentials"] {
		return TestResult{}, &ConfirmationRequiredError{Missing: []ConfirmationRequirement{{
			ID: "endpoint_credentials", Label: "send credentials to a non-default endpoint",
		}}}
	}
	return TestResult{OK: true, Fingerprint: "tested", Summary: "synthetic request passed"}, nil
}

func (b *accessibleRiskBackend) Apply(context.Context, map[string]string, string) (ApplyResult, error) {
	b.applyCalls++
	if !b.confirmed["diff_egress"] {
		return ApplyResult{}, &ConfirmationRequiredError{Missing: []ConfirmationRequirement{{
			ID: "diff_egress", Label: "allow redacted repository diff egress",
		}}}
	}
	return ApplyResult{DesiredRevision: 2, AppliedRevision: 1, Queued: true, Summary: "next safe boundary"}, nil
}

func TestAccessibleApplyConfirmsEndpointThenDiffWithoutRetesting(t *testing.T) {
	b := &accessibleRiskBackend{fakeBackend: fakeBackend{snapshot: baseSnapshot()}}
	values := AccessibleValues{Action: "apply", Values: map[string]string{"ai.model": "model"}, Confirm: true}
	var out bytes.Buffer
	input := &bytewiseReader{r: strings.NewReader("y\ny\n")}
	if err := dispatchAccessibleWithInput(context.Background(), b, values, input, &out); err != nil {
		t.Fatal(err)
	}
	if b.testCalls != 2 || b.applyCalls != 2 {
		t.Fatalf("calls test=%d apply=%d", b.testCalls, b.applyCalls)
	}
	view := out.String()
	if !strings.Contains(view, "send credentials to a non-default endpoint") || !strings.Contains(view, "allow redacted repository diff egress") || !strings.Contains(view, "QUEUED:") {
		t.Fatalf("risk flow output=%q", view)
	}
}

func TestAccessibleApplyDeclineReportsCompletedTest(t *testing.T) {
	b := &accessibleRiskBackend{fakeBackend: fakeBackend{snapshot: baseSnapshot()},
		confirmed: map[string]bool{"endpoint_credentials": true}}
	values := AccessibleValues{Action: "apply", Values: map[string]string{"ai.model": "model"}, Confirm: true}
	var out bytes.Buffer
	err := dispatchAccessibleWithInput(context.Background(), b, values,
		&bytewiseReader{r: strings.NewReader("n\n")}, &out)
	if err == nil || !strings.Contains(err.Error(), "synthetic test completed") ||
		strings.Contains(err.Error(), "no request or change") {
		t.Fatalf("decline error=%v\n%s", err, out.String())
	}
	if b.testCalls != 1 || b.applyCalls != 1 {
		t.Fatalf("testCalls=%d applyCalls=%d", b.testCalls, b.applyCalls)
	}
}

type missingKeyBackend struct{ fakeBackend }

func (b *missingKeyBackend) Test(context.Context, map[string]string) (TestResult, error) {
	return TestResult{}, errors.New("openai-compat: missing API key")
}

func TestAccessibleMissingAPIKeyIsActionable(t *testing.T) {
	err := dispatchAccessibleWithInput(context.Background(), &missingKeyBackend{}, AccessibleValues{Action: "test"}, strings.NewReader(""), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "set ACD_AI_API_KEY") || !strings.Contains(err.Error(), "never stored") {
		t.Fatalf("error=%v", err)
	}
}
