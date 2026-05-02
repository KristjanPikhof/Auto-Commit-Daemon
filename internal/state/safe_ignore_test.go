package state

import (
	"reflect"
	"testing"
)

func TestSafeIgnoreDefaultsMatchGeneratedTrees(t *testing.T) {
	t.Setenv(EnvSafeIgnore, "")
	t.Setenv(EnvSafeIgnoreExtra, "")
	m := NewSafeIgnoreMatcher()

	cases := []struct {
		path string
		want bool
	}{
		{"node_modules/react/index.js", true},
		{"frontend/node_modules/react/index.js", true},
		{"target/debug/app", true},
		{"pkg/target/debug/app", true},
		{".venv/bin/python", true},
		{"service/venv/bin/python", true},
		{"pkg/__pycache__/mod.pyc", true},
		{"pkg/.pytest_cache/v/cache/nodeids", true},
		{"pkg/.mypy_cache/3.12/mod.meta.json", true},
		{"pkg/.ruff_cache/content", true},
		{"android/.gradle/caches/modules-2", true},
		{"src/main.go", false},
		{"docs/node_modules.md", false},
		{"targeted/file.txt", false},
		{"vendor/pkg/file.go", false},
		{"build/output.js", false},
		{"dist/app.js", false},
	}
	for _, tc := range cases {
		if got := m.Match(tc.path); got != tc.want {
			t.Errorf("Match(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestSafeIgnoreDirectoryPruning(t *testing.T) {
	t.Setenv(EnvSafeIgnore, "")
	t.Setenv(EnvSafeIgnoreExtra, "dist/, web/build/")
	m := NewSafeIgnoreMatcher()

	for _, rel := range []string{"node_modules", "app/node_modules", "target", "pkg/target", "dist", "pkg/dist", "web/build"} {
		if !m.MatchDirectory(rel) {
			t.Fatalf("MatchDirectory(%q) = false, want true", rel)
		}
	}
	for _, rel := range []string{"node_modules_docs", "targeted", "pkg/build", "web/build-tools"} {
		if m.MatchDirectory(rel) {
			t.Fatalf("MatchDirectory(%q) = true, want false", rel)
		}
	}
}

func TestSafeIgnoreDisableEnv(t *testing.T) {
	for _, value := range []string{"0", "false", "FALSE", " no ", "off"} {
		t.Run("disable="+quoted(value), func(t *testing.T) {
			t.Setenv(EnvSafeIgnore, value)
			t.Setenv(EnvSafeIgnoreExtra, "dist/")
			m := NewSafeIgnoreMatcher()
			if m.Match("node_modules/pkg/index.js") {
				t.Fatalf("default pattern matched when %s=%q", EnvSafeIgnore, value)
			}
			if m.Match("dist/app.js") {
				t.Fatalf("extra pattern matched when %s=%q", EnvSafeIgnore, value)
			}
			if len(m.Patterns()) != 0 {
				t.Fatalf("Patterns() = %v, want empty when disabled", m.Patterns())
			}
		})
	}
}

func TestSafeIgnoreEnabledEnvKeepsDefaults(t *testing.T) {
	for _, value := range []string{"", "1", "true", "yes", "unexpected"} {
		t.Run("enable="+quoted(value), func(t *testing.T) {
			t.Setenv(EnvSafeIgnore, value)
			t.Setenv(EnvSafeIgnoreExtra, "")
			if !IsSafeIgnoredPath("node_modules/pkg/index.js") {
				t.Fatalf("defaults should be active when %s=%q", EnvSafeIgnore, value)
			}
		})
	}
}

func TestSafeIgnoreExtraAppendsDefaults(t *testing.T) {
	t.Setenv(EnvSafeIgnore, "")
	t.Setenv(EnvSafeIgnoreExtra, " dist/ , web/build/ ,cache*/ ")
	m := NewSafeIgnoreMatcher()

	if !m.Match("node_modules/pkg/index.js") {
		t.Fatalf("extra patterns should not replace defaults")
	}
	if !m.Match("dist/app.js") {
		t.Fatalf("extra dist/ pattern did not match")
	}
	if !m.Match("web/build/app.js") {
		t.Fatalf("extra web/build/ pattern did not match")
	}
	if m.Match("other/build/app.js") {
		t.Fatalf("web/build/ should not match other/build")
	}
	if !m.MatchDirectory("cache-v1") {
		t.Fatalf("extra wildcard directory pattern did not match cache-v1")
	}
}

func TestSafeIgnoreMalformedExtraFailsSafe(t *testing.T) {
	t.Setenv(EnvSafeIgnore, "")
	t.Setenv(EnvSafeIgnoreExtra, "[,../secret/,/absolute/, ,dist/")
	m := NewSafeIgnoreMatcher()

	if !m.Match("node_modules/pkg/index.js") {
		t.Fatalf("malformed extra must not disable defaults")
	}
	if !m.Match("dist/app.js") {
		t.Fatalf("valid extra entry should still apply")
	}
	if m.Match("../secret/file") {
		t.Fatalf("parent-relative extra pattern should be ignored")
	}
	if m.Match("absolute/file") {
		t.Fatalf("absolute extra pattern should be ignored")
	}
}

func TestSafeIgnoreMatcherSnapshot(t *testing.T) {
	t.Setenv(EnvSafeIgnore, "")
	t.Setenv(EnvSafeIgnoreExtra, "dist/")
	m := NewSafeIgnoreMatcher()

	t.Setenv(EnvSafeIgnore, "0")
	t.Setenv(EnvSafeIgnoreExtra, "")
	if !m.Match("dist/app.js") {
		t.Fatalf("matcher lost original extra pattern after env change")
	}
	if !m.Match("node_modules/pkg/index.js") {
		t.Fatalf("matcher lost original defaults after env change")
	}
}

func TestSafeIgnorePatternsReturnsCopy(t *testing.T) {
	t.Setenv(EnvSafeIgnore, "")
	t.Setenv(EnvSafeIgnoreExtra, "")
	m := NewSafeIgnoreMatcher()
	got := m.Patterns()
	if len(got) == 0 {
		t.Fatalf("Patterns() returned empty defaults")
	}
	got[0] = "mutated/"
	if reflect.DeepEqual(got, m.Patterns()) {
		t.Fatalf("Patterns() did not return a defensive copy")
	}
}
