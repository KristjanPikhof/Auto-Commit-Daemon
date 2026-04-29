package state

import (
	"reflect"
	"testing"
)

// TestSensitiveDefaultsBlockSecrets verifies the canonical default-deny list
// catches the legacy daemon's known-bad paths.
func TestSensitiveDefaultsBlockSecrets(t *testing.T) {
	// These tests must NOT be parallel with the env-override tests below — we
	// would race on os.Getenv. Use a t.Setenv-free guard.
	t.Setenv(EnvSensitiveGlobs, "")

	cases := []struct {
		path string
		want bool
	}{
		{".env", true},
		{".env.local", true},
		{"src/.env", true},
		{"deep/nested/.env", true},
		{"deep/nested/.env.production", true},
		{".npmrc", true},
		{"home/.npmrc", true},
		{".pgpass", true},
		{".git-credentials", true},
		{"some/.aws/credentials", true},
		{"a/b/c/.kube/config", true},
		{"home/.ssh/id_rsa", true},
		{"home/.ssh/id_rsa.pub", true},
		{"home/.ssh/id_ed25519", true},
		{"some/path/leaf.pem", true},
		{"keys/server.key", true},
		{"keys/server.crt", true},
		{"creds/service-account-prod.json", true},
		{"creds/service-account.json", true},
		{"signed.gpg", true},
		{"a/b/secrets/x", true},
		{"secrets/x", true},
		{"credentials_backup", true},

		// Negative cases — make sure normal source files pass through.
		{"src/main.go", false},
		{"README.md", false},
		{"docs/architecture.md", false},
		{"templates/embed.go", false},
		{"deeply/nested/foo/bar.txt", false},
	}
	for _, tc := range cases {
		got := IsSensitivePath(tc.path)
		if got != tc.want {
			t.Errorf("IsSensitivePath(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

// TestSensitiveEmptyEnvFallsBackToDefaults is the security regression: an
// empty/whitespace ACD_SENSITIVE_GLOBS must NOT disable filtering. Mirrors
// the legacy test_state_shared_lane_regressions.py behaviour.
func TestSensitiveEmptyEnvFallsBackToDefaults(t *testing.T) {
	for _, raw := range []string{"", " ", "\t", "\n", "  \t  ", ",", " , ,"} {
		t.Run("override="+quoted(raw), func(t *testing.T) {
			t.Setenv(EnvSensitiveGlobs, raw)
			if !IsSensitivePath(".env") {
				t.Fatalf("expected default-deny for .env when override=%q (defaults must apply)", raw)
			}
			if !IsSensitivePath("home/.aws/credentials") {
				t.Fatalf("expected default-deny for AWS creds when override=%q", raw)
			}
		})
	}
}

// TestSensitiveExplicitOverrideReplacesDefaults: a non-empty override must
// fully replace the default list (the legacy daemon documents this loudly).
func TestSensitiveExplicitOverrideReplacesDefaults(t *testing.T) {
	t.Setenv(EnvSensitiveGlobs, "extra/*,**/leaked.txt")

	if IsSensitivePath(".env") {
		t.Fatalf("override should have replaced defaults; .env should now be allowed")
	}
	if !IsSensitivePath("extra/foo") {
		t.Fatalf("override pattern extra/* did not match extra/foo")
	}
	if !IsSensitivePath("a/b/leaked.txt") {
		t.Fatalf("override pattern **/leaked.txt did not match a/b/leaked.txt")
	}
	if !IsSensitivePath("leaked.txt") {
		t.Fatalf("override pattern **/leaked.txt should also match root-level leaked.txt (gitignore semantics)")
	}
}

// TestSensitiveMatcherSnapshot confirms the matcher snapshots the env var at
// construction time and ignores subsequent changes.
func TestSensitiveMatcherSnapshot(t *testing.T) {
	t.Setenv(EnvSensitiveGlobs, "snap/*")
	m := NewSensitiveMatcher()
	if !m.Match("snap/x") {
		t.Fatalf("matcher did not honour initial override")
	}
	if m.Match("other/x") {
		t.Fatalf("matcher matched outside of override scope")
	}
	// Change env after construction; matcher should still see the original.
	t.Setenv(EnvSensitiveGlobs, "other/*")
	if !m.Match("snap/x") {
		t.Fatalf("matcher lost original snapshot after env change")
	}
}

func TestSensitiveMatcherDirectoryPruningOnlyUsesLiteralDirNames(t *testing.T) {
	t.Setenv(EnvSensitiveGlobs, "credentials*,private")
	m := NewSensitiveMatcher()

	if m.MatchDirectory("credentials_repo") {
		t.Fatalf("wildcard file pattern should not prune credentials_repo directory")
	}
	if !m.Match("credentials_repo") {
		t.Fatalf("file matcher should still match credentials_repo")
	}
	if !m.MatchDirectory("nested/private") {
		t.Fatalf("literal directory pattern should prune nested/private")
	}
}

// TestExpandGlobs ports the legacy snapshot_state._expand_globs invariants:
// every "**/X" yields a paired bare "X", with no duplicates.
func TestExpandGlobs(t *testing.T) {
	in := []string{"**/secrets/*", "**/foo", ".env", "**/foo"}
	got := expandGlobs(in)
	want := []string{"**/secrets/*", "secrets/*", "**/foo", "foo", ".env"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expandGlobs = %v, want %v", got, want)
	}
}

// quoted is just for nicer subtest naming when raw contains whitespace.
func quoted(s string) string {
	if s == "" {
		return "<empty>"
	}
	out := []byte{}
	for _, r := range s {
		switch r {
		case ' ':
			out = append(out, '_')
		case '\t':
			out = append(out, []byte("\\t")...)
		case '\n':
			out = append(out, []byte("\\n")...)
		default:
			out = append(out, byte(r))
		}
	}
	return string(out)
}
