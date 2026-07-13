package ai

import (
	"strings"
	"testing"
	"unicode"
)

func TestSanitizePlannerErrorRedactsQuotedJSONCredentials(t *testing.T) {
	raw := `openai-compat: http 400: {"error":{"message":"denied","token":"quoted-token","api_key":"quoted-key","headers":{"Authorization":"Bearer auth-token"}},"password":"quoted-pass","usage":{"max_tokens":100}}`
	clean := SanitizePlannerError(raw)
	for _, secret := range []string{"quoted-token", "quoted-key", "auth-token", "quoted-pass"} {
		if strings.Contains(clean, secret) {
			t.Fatalf("sanitized error contains %q: %q", secret, clean)
		}
	}
	if !strings.Contains(clean, `"max_tokens":100`) {
		t.Fatalf("sanitizer removed non-credential metadata: %q", clean)
	}
}

func TestSanitizePlannerErrorRedactsMalformedJSONFallback(t *testing.T) {
	raw := `openai-compat: invalid response {"token":"truncated-secret`
	clean := SanitizePlannerError(raw)
	if strings.Contains(clean, "truncated-secret") || !strings.Contains(clean, "[REDACTED]") {
		t.Fatalf("sanitized malformed JSON=%q", clean)
	}
}

func TestSanitizePlannerErrorBoundsAndNormalizes(t *testing.T) {
	raw := "request https://alice:password@example.com/v1?api_key=query-secret#fragment " +
		"Authorization: Bearer bearer-secret token=token-secret api_key=key-secret " +
		"sk-1234567890abcdef\x00\n" + strings.Repeat("x", plannerErrorCap+100)
	clean := SanitizePlannerError(raw)
	for _, secret := range []string{
		"alice", "password", "query-secret", "bearer-secret", "token-secret",
		"key-secret", "sk-1234567890abcdef",
	} {
		if strings.Contains(clean, secret) {
			t.Fatalf("sanitized error contains %q: %q", secret, clean)
		}
	}
	if got := len([]rune(clean)); got > plannerErrorCap {
		t.Fatalf("sanitized error runes=%d want <=%d", got, plannerErrorCap)
	}
	for _, r := range clean {
		if unicode.IsControl(r) {
			t.Fatalf("sanitized error contains control rune %U", r)
		}
	}
}

func TestSanitizePlannerErrorRedactsURLPathAndPreservesPunctuation(t *testing.T) {
	raw := `openai-compat: Post "https://example.test/v1/path-secret/chat/completions": timeout`
	clean := SanitizePlannerError(raw)
	want := `openai-compat: Post "https://example.test": timeout`
	if clean != want {
		t.Fatalf("clean=%q want %q", clean, want)
	}
}
