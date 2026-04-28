package ai

import (
	"context"
	"testing"
)

// TestDeterministic_Subjects pins subject lines for every op kind. Mirrors
// the daemon-package test fixtures so swapping the daemon helper to call
// the ai package preserves exact output.
func TestDeterministic_Subjects(t *testing.T) {
	cases := []struct {
		name string
		cc   CommitContext
		want string
	}{
		{name: "create", cc: CommitContext{Op: "create", Path: "src/foo.go"}, want: "Add foo.go"},
		{name: "modify", cc: CommitContext{Op: "modify", Path: "src/foo.go"}, want: "Update foo.go"},
		{name: "delete", cc: CommitContext{Op: "delete", Path: "src/foo.go"}, want: "Remove foo.go"},
		{name: "rename", cc: CommitContext{Op: "rename", Path: "src/bar.go", OldPath: "src/foo.go"}, want: "Rename foo.go to bar.go"},
		{name: "mode", cc: CommitContext{Op: "mode", Path: "src/foo.go"}, want: "Update foo.go"},
		{name: "unknown", cc: CommitContext{Op: "wat", Path: "src/foo.go"}, want: "Update foo.go"},
		{name: "empty", cc: CommitContext{}, want: "Update files"},
	}
	p := DeterministicProvider{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := p.Generate(context.Background(), tc.cc)
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			if r.Subject != tc.want {
				t.Fatalf("subject=%q want %q", r.Subject, tc.want)
			}
			if r.Source != "deterministic" {
				t.Fatalf("source=%q want deterministic", r.Source)
			}
			if r.Body != "" {
				t.Fatalf("body=%q want empty for single-op", r.Body)
			}
		})
	}
}

// TestDeterministic_MultiOp pins multi-op subject + body. Body bullets must
// be byte-equal to the daemon-package implementation so replay's commit
// content does not drift when we swap the helper.
func TestDeterministic_MultiOp(t *testing.T) {
	cases := []struct {
		name        string
		ops         []OpItem
		wantSubject string
		wantBody    string
	}{
		{
			name: "shared-dir",
			ops: []OpItem{
				{Op: "modify", Path: "src/a.go"},
				{Op: "modify", Path: "src/b.go"},
			},
			wantSubject: "Update 2 files in src",
			wantBody:    "- Modify src/a.go\n- Modify src/b.go",
		},
		{
			name: "disjoint",
			ops: []OpItem{
				{Op: "create", Path: "a/foo.go"},
				{Op: "create", Path: "b/bar.go"},
			},
			wantSubject: "Update 2 files",
			wantBody:    "- Create a/foo.go\n- Create b/bar.go",
		},
		{
			name: "rename-bullet",
			ops: []OpItem{
				{Op: "rename", Path: "x/new.go", OldPath: "x/old.go"},
				{Op: "modify", Path: "x/y.go"},
			},
			wantSubject: "Update 2 files in x",
			wantBody:    "- Rename x/old.go -> x/new.go\n- Modify x/y.go",
		},
	}
	p := DeterministicProvider{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := p.Generate(context.Background(), CommitContext{MultiOp: tc.ops})
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			if r.Subject != tc.wantSubject {
				t.Fatalf("subject=%q want %q", r.Subject, tc.wantSubject)
			}
			if r.Body != tc.wantBody {
				t.Fatalf("body=%q want %q", r.Body, tc.wantBody)
			}
		})
	}
}

// TestDeterministic_CommonDir checks the full-prefix-equals-path edge case
// (paths fully nest -> drop trailing dir segment so we don't claim a file
// as a directory). Mirrors the legacy _common_dir tail behaviour.
func TestDeterministic_CommonDirNested(t *testing.T) {
	r, err := DeterministicProvider{}.Generate(context.Background(), CommitContext{
		MultiOp: []OpItem{
			{Op: "modify", Path: "a/b/c"},
			{Op: "modify", Path: "a/b/c"},
		},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	// Both paths are identical and full-prefix matches; common dir should
	// drop the last segment, producing "a/b".
	if r.Subject != "Update 2 files in a/b" {
		t.Fatalf("subject=%q want %q", r.Subject, "Update 2 files in a/b")
	}
}

// TestDeterministic_CtxCancel ensures cancellation is honoured before any
// work happens (the daemon may cancel between drains).
func TestDeterministic_CtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (DeterministicProvider{}).Generate(ctx, CommitContext{Op: "modify", Path: "x"}); err == nil {
		t.Fatalf("expected ctx error")
	}
}
