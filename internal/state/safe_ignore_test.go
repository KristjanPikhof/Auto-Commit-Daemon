package state

import (
	"context"
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
		{"DerivedData/Build/Intermediates.noindex/cache.db", true},
		{".derivedData-provider-core/Index.noindex/DataStore/record", true},
		{"nested/.derivedData-tests/Build/cache.db", true},
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
		{"DerivedDataNotes/file.txt", false},
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

func TestSafeIgnoreDirectoryPatternsDoNotMatchSameNamedFiles(t *testing.T) {
	t.Setenv(EnvSafeIgnore, "")
	t.Setenv(EnvSafeIgnoreExtra, "dist/, web/build/")
	m := NewSafeIgnoreMatcher()

	for _, rel := range []string{"node_modules", "target", "pkg/target", ".derivedData-provider-core.md", "dist", "web/build"} {
		if m.MatchFile(rel) {
			t.Fatalf("MatchFile(%q) = true, want false for same-named file", rel)
		}
	}
	for _, rel := range []string{"node_modules/pkg/index.js", "pkg/target/debug/app", "dist/app.js", "web/build/app.js"} {
		if !m.MatchFile(rel) {
			t.Fatalf("MatchFile(%q) = false, want true for descendant", rel)
		}
	}
}

func TestSafeIgnoreMatchRootReturnsConcreteGeneratedRoot(t *testing.T) {
	t.Setenv(EnvSafeIgnore, "")
	t.Setenv(EnvSafeIgnoreExtra, "web/build/")
	m := NewSafeIgnoreMatcher()

	cases := []struct {
		path        string
		wantRoot    string
		wantPattern string
	}{
		{"frontend/node_modules/react/index.js", "frontend/node_modules", "node_modules/"},
		{".derivedData-provider-core/Index.noindex/cache.db", ".derivedData-provider-core", ".derivedData*/"},
		{"nested/.derivedData-tests/Build/cache.db", "nested/.derivedData-tests", ".derivedData*/"},
		{"web/build/app.js", "web/build", "web/build/"},
	}
	for _, tc := range cases {
		got, ok := m.MatchRoot(tc.path)
		if !ok {
			t.Fatalf("MatchRoot(%q) did not match", tc.path)
		}
		if got.Root != tc.wantRoot || got.Pattern != tc.wantPattern {
			t.Fatalf("MatchRoot(%q) = %+v, want root=%q pattern=%q", tc.path, got, tc.wantRoot, tc.wantPattern)
		}
	}
}

func TestScanGeneratedPendingDeletesGroupsSafeIgnoreRoots(t *testing.T) {
	t.Setenv(EnvSafeIgnore, "")
	t.Setenv(EnvSafeIgnoreExtra, "")
	db, _ := openTestDB(t)
	ctx := context.Background()
	head := "0123456789012345678901234567890123456789"

	appendEvent := func(op, path, st string) int64 {
		t.Helper()
		seq, err := AppendCaptureEvent(ctx, db, CaptureEvent{
			BranchRef:        "refs/heads/main",
			BranchGeneration: 1,
			BaseHead:         head,
			Operation:        op,
			Path:             path,
			Fidelity:         "full",
			State:            st,
		}, nil)
		if err != nil {
			t.Fatalf("AppendCaptureEvent(%s,%s,%s): %v", op, path, st, err)
		}
		return seq
	}

	seqOne := appendEvent("delete", "frontend/node_modules/react/index.js", EventStatePending)
	seqTwo := appendEvent("delete", "frontend/node_modules/react/package.json", EventStatePending)
	seqThree := appendEvent("delete", ".derivedData-provider-core/Index.noindex/cache.db", EventStatePending)
	appendEvent("delete", "build/output.js", EventStatePending)
	appendEvent("delete", "docs/node_modules.md", EventStatePending)
	appendEvent("create", "node_modules/new-file.js", EventStatePending)
	appendEvent("delete", "node_modules/published.js", EventStatePublished)

	groups, err := ScanGeneratedPendingDeletes(ctx, db.ReadSQL(), NewSafeIgnoreMatcher(), 0)
	if err != nil {
		t.Fatalf("ScanGeneratedPendingDeletes: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("groups=%+v, want 2 generated groups", groups)
	}
	if g := groups[0]; g.Root != "frontend/node_modules" || g.Pattern != "node_modules/" ||
		g.PendingCount != 2 || g.OldestSeq != seqOne || g.NewestSeq != seqTwo ||
		!reflect.DeepEqual(g.EventSeqs, []int64{seqOne, seqTwo}) {
		t.Fatalf("node_modules group=%+v", g)
	}
	if g := groups[1]; g.Root != ".derivedData-provider-core" || g.Pattern != ".derivedData*/" ||
		g.PendingCount != 1 || g.OldestSeq != seqThree || g.NewestSeq != seqThree ||
		!reflect.DeepEqual(g.EventSeqs, []int64{seqThree}) {
		t.Fatalf("derived data group=%+v", g)
	}
}

func TestScanGeneratedPendingDeletesRequiresQueryer(t *testing.T) {
	if _, err := ScanGeneratedPendingDeletes(context.Background(), nil, NewSafeIgnoreMatcher(), 0); err == nil {
		t.Fatalf("ScanGeneratedPendingDeletes nil queryer error = nil")
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
