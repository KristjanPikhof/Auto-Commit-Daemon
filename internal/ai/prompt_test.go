package ai

import (
	"strings"
	"testing"
)

// TestSanitize_Empty: empty input yields the safe placeholder.
func TestSanitize_Empty(t *testing.T) {
	for _, s := range []string{"", "   ", "\n\n\n", "\t \r\n"} {
		if got := SanitizeMessage(s); got != "Update files" {
			t.Fatalf("SanitizeMessage(%q)=%q want %q", s, got, "Update files")
		}
	}
}

// TestSanitize_TrimsTrailingPeriod: subjects do not end in "." after sanitize.
func TestSanitize_TrimsTrailingPeriod(t *testing.T) {
	got := SanitizeMessage("Update auth flow.")
	if got != "Update auth flow" {
		t.Fatalf("got=%q want %q", got, "Update auth flow")
	}
}

// TestSanitize_StripsBulletPrefix: a leading "- " on the subject is dropped.
func TestSanitize_StripsBulletPrefix(t *testing.T) {
	got := SanitizeMessage("- Add provider")
	if got != "Add provider" {
		t.Fatalf("got=%q want %q", got, "Add provider")
	}
}

// TestSanitize_ControlChars: ASCII control bytes are scrubbed before the
// sanitizer runs the rest of its pipeline. The ANSI residue (`[31m`) is
// not parsed structurally — the escape byte is removed but the printable
// trailing chars stay; that is acceptable since the commit subject still
// reads clean ASCII.
func TestSanitize_ControlChars(t *testing.T) {
	in := "Add\x00 \x07provider"
	got := SanitizeMessage(in)
	if got != "Add provider" {
		t.Fatalf("got=%q want %q", got, "Add provider")
	}
	// And: an ANSI escape's leading byte is stripped (defence-in-depth
	// against terminal-injection in commit logs).
	got2 := SanitizeMessage("Add\x1bprovider")
	if strings.ContainsRune(got2, '\x1b') {
		t.Fatalf("escape byte survived: %q", got2)
	}
}

// TestSanitize_SubjectCap: subjects longer than SubjectCap are truncated
// at a word boundary with `…`.
func TestSanitize_SubjectCap(t *testing.T) {
	long := "This is an extremely long commit subject that absolutely exceeds the seventy-two character cap"
	got := SanitizeMessage(long)
	if !strings.HasSuffix(got, "…") {
		t.Fatalf("got=%q want trailing ellipsis", got)
	}
	if len([]rune(got)) > SubjectCap {
		t.Fatalf("got len=%d > cap %d (%q)", len([]rune(got)), SubjectCap, got)
	}
}

// TestSanitize_BodyBullets: a multi-line body is normalized into "- "
// prefixed bullets with hanging indent on long bullets.
func TestSanitize_BodyBullets(t *testing.T) {
	in := "Update token expiry\n\n* refresh tokens now expire in 7 days\n  instead of 30\n- audit log entries record the new TTL"
	got := SanitizeMessage(in)
	parts := strings.SplitN(got, "\n\n", 2)
	if len(parts) != 2 {
		t.Fatalf("expected subject + body, got %q", got)
	}
	if parts[0] != "Update token expiry" {
		t.Fatalf("subject=%q", parts[0])
	}
	bodyLines := strings.Split(parts[1], "\n")
	if len(bodyLines) < 2 {
		t.Fatalf("body did not produce at least 2 bullets: %q", parts[1])
	}
	for _, l := range bodyLines {
		if !strings.HasPrefix(l, "- ") && !strings.HasPrefix(l, "  ") {
			t.Fatalf("bullet line %q lacks `- ` or `  ` prefix", l)
		}
	}
}

// TestSanitize_NoBodyWhenInputIsSubjectOnly: a single-line input never
// gets an empty body appended.
func TestSanitize_NoBody(t *testing.T) {
	got := SanitizeMessage("Add provider")
	if strings.Contains(got, "\n") {
		t.Fatalf("got=%q expected single-line", got)
	}
}

// TestTruncate_PassThrough: short diffs are returned verbatim.
func TestTruncate_PassThrough(t *testing.T) {
	d := "diff --git a/x b/x\nindex 1..2\n--- a/x\n+++ b/x\n@@ -1 +1 @@\n-old\n+new\n"
	if got := Truncate(d, 4000); got != d {
		t.Fatalf("Truncate changed a short diff")
	}
}

// TestTruncate_PreservesHeaders: long diffs keep headers and the sentinel.
func TestTruncate_PreservesHeaders(t *testing.T) {
	header := "diff --git a/x b/x\nindex abc..def 100644\n--- a/x\n+++ b/x\n@@ -1,5 +1,5 @@\n"
	body := strings.Repeat(" line filler line filler\n", 500)
	d := header + body
	got := Truncate(d, 200)
	if !strings.HasPrefix(got, "diff --git a/x b/x\n") {
		t.Fatalf("expected header preserved, got %q", got[:80])
	}
	if !strings.Contains(got, "<truncated>") {
		t.Fatalf("expected sentinel; got %q", got)
	}
	if len(got) > 200+len("\n... <truncated> ...\n") {
		t.Fatalf("truncated len=%d > budget", len(got))
	}
}

// TestTruncate_TinyBudget: pathological budgets use a hard prefix cut.
func TestTruncate_TinyBudget(t *testing.T) {
	d := "diff --git a/x b/x\nindex 1..2\n--- a/x\n+++ b/x\n@@ -1 +1 @@\n-old\n+new\n"
	got := Truncate(d, 5)
	if got != d[:5] {
		t.Fatalf("got=%q want %q", got, d[:5])
	}
}
