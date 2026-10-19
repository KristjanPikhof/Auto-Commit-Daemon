package finish

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func localEvidence() Evidence {
	base := Tree{"app": planEntry("a"), "removed": planEntry("b"), "kept": planEntry("c")}
	target := Tree{"app": planEntry("d"), "kept": planEntry("c"), "link": {Mode: "120000", OID: strings.Repeat("e", 40)}}
	return Evidence{Base: base, Target: target, Changes: NetChanges(base, target, "author")}
}

func TestFinishLocalOfflineBatch(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "provider must not run", http.StatusInternalServerError)
	}))
	defer server.Close()
	t.Setenv("ACD_AI_PROVIDER", "openai")
	t.Setenv("ACD_AI_BASE_URL", server.URL)
	t.Setenv("ACD_AI_API_KEY", "")
	t.Setenv("ACD_AI_SUBPROCESS", "/provider-must-not-start")
	for _, style := range []string{StyleImperative, StyleConventional} {
		t.Run(style, func(t *testing.T) {
			evidence := localEvidence()
			plan, err := Local(evidence, LocalOptions{Style: style})
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Steps) != 1 || !maps.Equal(plan.Steps[0].Tree, evidence.Target) || len(plan.Steps[0].ChangeIDs) != 3 {
				t.Fatalf("not a complete batch: %+v", plan)
			}
			if !strings.Contains(plan.Steps[0].Message, "1 added, 1 updated, and 1 removed") {
				t.Fatalf("missing grounded evidence: %q", plan.Steps[0].Message)
			}
			again, err := Local(evidence, LocalOptions{Style: style})
			if err != nil || !reflect.DeepEqual(plan, again) {
				t.Fatalf("nondeterministic output: %+v, %v", again, err)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("local planning called provider %d times", calls.Load())
	}
}

func TestFinishLocalPurposeAndExactMessage(t *testing.T) {
	evidence := localEvidence()
	for _, style := range []string{StyleImperative, StyleConventional} {
		plan, err := Local(evidence, LocalOptions{Style: style, Purpose: "staged content safety"})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(plan.Steps[0].Message, "Complete staged content safety") && !strings.Contains(plan.Steps[0].Message, "complete staged content safety") {
			t.Fatalf("purpose was lost: %q", plan.Steps[0].Message)
		}
		message := "Preserve staged content\n\n- Keep the approved staged versions during publication.\n"
		if style == StyleConventional {
			message = "fix: preserve staged content\n\n- Keep the approved staged versions during publication.\n"
		}
		plan, err = Local(evidence, LocalOptions{Style: style, Purpose: "original context", Message: message})
		if err != nil || plan.Steps[0].Message != message || plan.Steps[0].Purpose != "original context" {
			t.Fatalf("changed supplied message/context: %+v, %v", plan, err)
		}
	}
}

func TestFinishLocalLongPurposeIsRetained(t *testing.T) {
	purpose := strings.Repeat("õ", 160)
	for _, style := range []string{StyleImperative, StyleConventional} {
		plan, err := Local(localEvidence(), LocalOptions{Style: style, Purpose: purpose})
		if err != nil {
			t.Fatal(err)
		}
		message := plan.Steps[0].Message
		if !strings.Contains(strings.ReplaceAll(message, "\n  ", ""), purpose) || plan.Steps[0].Purpose != purpose {
			t.Fatalf("long Unicode purpose was clipped: %q", message)
		}
	}
	if _, err := Local(localEvidence(), LocalOptions{Purpose: "..."}); err != nil {
		t.Fatal(err)
	}
}

func TestFinishLocalPurposeWithColon(t *testing.T) {
	for _, style := range []string{StyleImperative, StyleConventional} {
		purpose := "Settings: preserve scopes"
		plan, err := Local(localEvidence(), LocalOptions{Style: style, Purpose: purpose})
		if err != nil || len(plan.Steps) != 1 || !strings.Contains(plan.Steps[0].Message, purpose) {
			t.Fatalf("valid context rejected or lost: %+v, %v", plan, err)
		}
	}
	if err := ValidateMessage(StyleImperative, "Fix settings: preserve scope precedence"); err != nil {
		t.Fatal(err)
	}
}

func TestFinishLocalRecordedSavesMakeOneCommit(t *testing.T) {
	evidence := Evidence{Base: Tree{"app": planEntry("a")}, Target: Tree{"app": planEntry("d")}, Changes: []Change{
		{ID: "save1", Path: "app", Before: planEntry("a"), After: planEntry("b"), Author: "one"},
		{ID: "save2", Path: "app", Before: planEntry("b"), After: planEntry("c"), Author: "one"},
		{ID: "save3", Path: "app", Before: planEntry("c"), After: planEntry("d"), Author: "one"},
	}}
	plan, err := Local(evidence, LocalOptions{})
	if err != nil || len(plan.Steps) != 1 || len(plan.Steps[0].ChangeIDs) != 3 || !maps.Equal(plan.Steps[0].Tree, evidence.Target) {
		t.Fatalf("saved chronology leaked into commit boundaries: %+v, %v", plan, err)
	}
	evidence.Changes[1].Author = "other"
	if _, err := Local(evidence, LocalOptions{}); err == nil {
		t.Fatal("local batch erased a recorded author boundary")
	}
}

func TestFinishLocalNoChanges(t *testing.T) {
	evidence := Evidence{Base: Tree{"kept": planEntry("a")}, Target: Tree{"kept": planEntry("a")}}
	plan, err := Local(evidence, LocalOptions{})
	if err != nil || len(plan.Steps) != 0 || !maps.Equal(plan.Target, evidence.Target) {
		t.Fatalf("no-change batch created history: %+v, %v", plan, err)
	}
	evidence.Changes = []Change{
		{ID: "save", Path: "kept", Before: planEntry("a"), After: planEntry("b")},
		{ID: "undo", Path: "kept", Before: planEntry("b"), After: planEntry("a")},
	}
	plan, err = Local(evidence, LocalOptions{})
	if err != nil || len(plan.Steps) != 0 || len(plan.NeutralChangeIDs) != 2 {
		t.Fatalf("reverted saves created history: %+v, %v", plan, err)
	}
}

func TestFinishLocalRejectUnsafeMessages(t *testing.T) {
	cases := []struct{ style, message string }{
		{StyleImperative, ""}, {StyleImperative, "   "},
		{StyleImperative, "Save work\x1b[31m"}, {StyleImperative, "Save\twork"},
		{StyleImperative, "Save \xffwork"}, {StyleImperative, "Save work."},
		{StyleImperative, "Save " + strings.Repeat("x", 60)},
		{StyleImperative, "Save work\nBody without a separator"},
		{StyleImperative, "Save work\n\nParagraph without a bullet"},
		{StyleImperative, "Save work\n\n  Continuation without a bullet"},
		{StyleImperative, "Save work\n\n- " + strings.Repeat("x", 73)},
		{StyleImperative, "fix: preserve staged content"},
		{StyleConventional, "fix(cli): preserve staged content"},
		{StyleConventional, "unknown: preserve staged content"},
		{StyleConventional, "fix: "}, {"unknown", "Save work"},
	}
	for _, test := range cases {
		if err := ValidateMessage(test.style, test.message); err == nil {
			t.Errorf("accepted unsafe/style-invalid message %q in %q", test.message, test.style)
		}
		if test.message != "" {
			if _, err := Local(localEvidence(), LocalOptions{Style: test.style, Message: test.message}); err == nil {
				t.Errorf("local accepted invalid message %q", test.message)
			}
		}
	}
	if _, err := Local(localEvidence(), LocalOptions{Purpose: "context\x00bad"}); err == nil {
		t.Fatal("accepted control characters in purpose")
	}
}
