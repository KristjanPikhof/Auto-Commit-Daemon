package settingsui

import (
	"bytes"
	"context"
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
		"Enter keeps no",
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
