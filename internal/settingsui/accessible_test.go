package settingsui

import (
	"bytes"
	"context"
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
