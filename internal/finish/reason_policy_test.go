package finish

import (
	"errors"
	"strings"
	"testing"
)

func TestFinishLocalRequiresCompletionReason(t *testing.T) {
	for _, purpose := range []string{"", "   ", "..."} {
		if _, err := Local(localEvidence(), LocalOptions{Purpose: purpose}); !errors.Is(err, ErrCompletionContextRequired) {
			t.Fatalf("unexplained local commit: purpose=%q error=%v", purpose, err)
		}
	}
	purpose := "Keep the approved staged version when later edits arrive."
	plan, err := Local(localEvidence(), LocalOptions{Purpose: purpose})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 || plan.Steps[0].Reason != purpose || !strings.Contains(strings.Join(strings.Fields(plan.Steps[0].Message), " "), purpose) {
		t.Fatalf("caller reason was lost: %+v", plan)
	}
	for _, filler := range []string{"changed paths", "added,", "Save the entire frozen"} {
		if strings.Contains(plan.Steps[0].Message, filler) {
			t.Fatalf("machine summary replaced the reason: %s", plan.Steps[0].Message)
		}
	}
}

func TestFinishExactMessageRequiresAnExplanation(t *testing.T) {
	for _, style := range []string{StyleImperative, StyleConventional} {
		subject := "Preserve staged content"
		if style == StyleConventional {
			subject = "fix: preserve staged content"
		}
		for _, message := range []string{subject, subject + "\n\n- ...", subject + "\n\n- 123"} {
			if ValidatePublicationMessage(style, message) == nil {
				t.Fatalf("unexplained message accepted: %q", message)
			}
			if _, err := Local(localEvidence(), LocalOptions{Style: style, Message: message}); err == nil {
				t.Fatalf("exact message bypassed reason gate: %q", message)
			}
		}
		message := subject + "\n\n- Keep the approved version when later edits arrive.\n"
		plan, err := Local(localEvidence(), LocalOptions{Style: style, Message: message})
		if err != nil || plan.Steps[0].Message != message {
			t.Fatalf("exact explained message changed: %+v %v", plan, err)
		}
		if err := ValidateMessage(style, subject); err != nil {
			t.Fatalf("legacy format-only validation changed: %v", err)
		}
	}
}
