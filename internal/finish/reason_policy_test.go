package finish

import (
	"strings"
	"testing"
)

func TestFinishLocalContextIsOptional(t *testing.T) {
	for _, purpose := range []string{"", "   ", "..."} {
		plan, err := Local(localEvidence(), LocalOptions{Purpose: purpose})
		if err != nil || len(plan.Steps) != 1 {
			t.Fatalf("optional context blocked Local: purpose=%q plan=%+v error=%v", purpose, plan, err)
		}
		if strings.TrimSpace(purpose) == "" && (plan.Steps[0].Reason != "" || !strings.Contains(plan.Steps[0].Message, "offline commit")) {
			t.Fatalf("Local invented motivation: %+v", plan.Steps[0])
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

func TestFinishExactMessagePreservesCallerWording(t *testing.T) {
	for _, style := range []string{StyleImperative, StyleConventional} {
		subject := "Preserve staged content"
		if style == StyleConventional {
			subject = "fix: preserve staged content"
		}
		for _, message := range []string{subject, subject + "\n\n- ...", subject + "\n\n- 123"} {
			plan, err := Local(localEvidence(), LocalOptions{Style: style, Message: message})
			if err != nil || plan.Steps[0].Message != message {
				t.Fatalf("exact override was rejected or rewritten: %+v %v", plan, err)
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
