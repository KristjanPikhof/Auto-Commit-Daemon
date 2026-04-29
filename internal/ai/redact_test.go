package ai

import (
	"strings"
	"testing"
)

func TestRedactDiffSecrets_ScrubsObviousTokens(t *testing.T) {
	input := strings.Join([]string{
		"diff --git a/config/prod.yaml b/config/prod.yaml",
		"+aws_access_key_id: AKIAIOSFODNN7EXAMPLE",
		"+slack: xoxb-123456789012-abcdefSECRET",
		"+github: ghp_abcdefghijklmnopqrstuvwxyz123456",
		"+auth: Bearer abcdefghij.klmnopqrst.uvwxyz123456",
		"+jwt: eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
		"+-----BEGIN PRIVATE KEY-----",
		"+password = \"correct-horse-battery\"",
		"+token = plainsecretvalue",
		"+opaque = AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/=",
		"",
	}, "\n")

	got := RedactDiffSecrets(input)
	for _, leaked := range []string{
		"AKIAIOSFODNN7EXAMPLE",
		"xoxb-123456789012-abcdefSECRET",
		"ghp_abcdefghijklmnopqrstuvwxyz123456",
		"Bearer abcdefghij.klmnopqrst.uvwxyz123456",
		"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
		"BEGIN PRIVATE KEY",
		"correct-horse-battery",
		"plainsecretvalue",
		"AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/=",
	} {
		if strings.Contains(got, leaked) {
			t.Fatalf("redacted diff leaked %q:\n%s", leaked, got)
		}
	}
	if !strings.Contains(got, "diff --git a/config/prod.yaml b/config/prod.yaml") {
		t.Fatalf("redaction removed diff header:\n%s", got)
	}
	if strings.Count(got, redactedSecret) < 8 {
		t.Fatalf("expected several redactions, got:\n%s", got)
	}
	if !strings.Contains(got, "password = \"[REDACTED_SECRET]\"") {
		t.Fatalf("assignment redaction did not preserve syntax:\n%s", got)
	}
}

func TestRedactDiffSecrets_LeavesOrdinaryDiffText(t *testing.T) {
	input := "diff --git a/src/main.go b/src/main.go\n@@\n-old\n+new\n"
	if got := RedactDiffSecrets(input); got != input {
		t.Fatalf("RedactDiffSecrets changed ordinary diff:\n%s", got)
	}
}
