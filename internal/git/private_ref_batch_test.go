package git

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrivateRefBatchRejectsMovedAndSymbolicPins(t *testing.T) {
	for _, scenario := range []struct {
		kind     string
		badFirst bool
	}{
		{"moved", false}, {"same-OID-alias", false}, {"dangling-alias", false}, {"dangling-alias-primary", true},
	} {
		kind := scenario.kind
		t.Run(kind, func(t *testing.T) {
			repo := initRepo(t)
			tree, err := WriteTreeDurable(t.Context(), repo, filepath.Join(t.TempDir(), "pin.index"), nil)
			if err != nil {
				t.Fatal(err)
			}
			commit, err := CommitTreeDurable(t.Context(), repo, tree, "captured", "Original", "original@example.com")
			if err != nil {
				t.Fatal(err)
			}
			wrong, err := CommitTreeDurable(t.Context(), repo, tree, "different", "Original", "original@example.com")
			if err != nil {
				t.Fatal(err)
			}
			prefix := "refs/acd/migration/batch/"
			good, bad := RefExpectation{Ref: prefix + "good", OID: commit}, RefExpectation{Ref: prefix + "bad", OID: commit}
			refs := []RefExpectation{good, bad}
			if scenario.badFirst {
				refs = []RefExpectation{bad, good}
			}
			if kind == "moved" {
				err = UpdateRef(t.Context(), repo, bad.Ref, wrong, "")
			} else {
				if kind == "same-OID-alias" {
					if err := UpdateRef(t.Context(), repo, prefix+"target", commit, ""); err != nil {
						t.Fatal(err)
					}
				}
				_, err = Run(t.Context(), RunOpts{Dir: repo}, "symbolic-ref", bad.Ref, prefix+"target")
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = EnsurePrivateRefsDurable(t.Context(), repo, "refs/acd/migration/", refs); !errors.Is(err, ErrCheckpointRefCollision) {
				t.Fatalf("unsafe batch accepted: %v", err)
			}
			if got, err := RevParse(t.Context(), repo, good.Ref); err != nil || got != commit {
				t.Fatalf("independent valid pin was not preserved: %s %v", got, err)
			}
			if err := VerifyPrivateRefs(t.Context(), repo, refs); err == nil {
				t.Fatal("unsafe verification accepted")
			}
			if kind == "moved" {
				if got, err := RevParse(t.Context(), repo, bad.Ref); err != nil || got != wrong {
					t.Fatal("moved pin overwritten")
				}
			} else {
				out, err := Run(t.Context(), RunOpts{Dir: repo}, "symbolic-ref", "--no-recurse", bad.Ref)
				if err != nil || strings.TrimSpace(string(out)) != prefix+"target" {
					t.Fatal("symbolic alias overwritten")
				}
			}
		})
	}
}

func TestPrivateRefBatchRejectsPostCreateAlias(t *testing.T) {
	repo := initRepo(t)
	tree, err := WriteTreeDurable(t.Context(), repo, filepath.Join(t.TempDir(), "pin.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := CommitTreeDurable(t.Context(), repo, tree, "captured", "Original", "original@example.com")
	if err != nil {
		t.Fatal(err)
	}
	ref, target := "refs/acd/migration/batch/pin", "refs/acd/migration/batch/target"
	if err := UpdateRef(t.Context(), repo, target, commit, ""); err != nil {
		t.Fatal(err)
	}
	hook := "#!/bin/sh\nif [ \"$1\" = committed ] && [ ! -e .git/alias-replaced ]; then\n : > .git/alias-replaced\n git -c core.hooksPath=/dev/null symbolic-ref " + ref + " " + target + " || exit 1\nfi\n"
	if err := os.WriteFile(filepath.Join(repo, ".git", "hooks", "reference-transaction"), []byte(hook), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := EnsurePrivateRefsDurable(t.Context(), repo, "refs/acd/migration/", []RefExpectation{{Ref: ref, OID: commit}}); !errors.Is(err, ErrCheckpointRefCollision) {
		t.Fatalf("post-create symbolic replacement accepted: %v", err)
	}
	out, err := Run(t.Context(), RunOpts{Dir: repo}, "symbolic-ref", "--no-recurse", ref)
	if err != nil || strings.TrimSpace(string(out)) != target {
		t.Fatal("unsafe alias changed during refusal")
	}
}

func TestPrivateRefBatchAdditionalLocksRemainHeldThroughCallback(t *testing.T) {
	repo := initRepo(t)
	tree, err := WriteTreeDurable(t.Context(), repo, filepath.Join(t.TempDir(), "pin.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := CommitTreeDurable(t.Context(), repo, tree, "captured", "Original", "original@example.com")
	if err != nil {
		t.Fatal(err)
	}
	other, err := CommitTreeDurable(t.Context(), repo, tree, "other", "Original", "original@example.com")
	if err != nil {
		t.Fatal(err)
	}
	refs := []RefExpectation{{Ref: "refs/acd/migration/locks/first", OID: commit}, {Ref: "refs/acd/migration/locks/second", OID: commit}}
	if err := EnsurePrivateRefsDurable(t.Context(), repo, "refs/acd/migration/", refs); err != nil {
		t.Fatal(err)
	}
	called := false
	err = withLockedExpectedRefsTransaction(t.Context(), repo, refs[0].Ref, commit, "", "", false, DefaultWriteTimeout, func(ctx context.Context) error {
		called = true
		if err := UpdateRef(ctx, repo, refs[1].Ref, other, commit); err == nil {
			return errors.New("extra native ref lock released before callback")
		}
		return nil
	}, nil, map[string]string{refs[1].Ref: commit})
	if err != nil || !called {
		t.Fatalf("locked callback: %v, called=%v", err, called)
	}
	if err := VerifyPrivateRefs(t.Context(), repo, refs); err != nil {
		t.Fatal(err)
	}
}

func TestPrivateRefBatchRejectsMissingPinnedObject(t *testing.T) {
	repo := initRepo(t)
	tree, err := WriteTreeDurable(t.Context(), repo, filepath.Join(t.TempDir(), "pin.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := CommitTreeDurable(t.Context(), repo, tree, "captured", "Original", "original@example.com")
	if err != nil {
		t.Fatal(err)
	}
	refs := []RefExpectation{{Ref: "refs/acd/migration/missing/pin", OID: commit}}
	if err := EnsurePrivateRefsDurable(t.Context(), repo, "refs/acd/migration/", refs); err != nil {
		t.Fatal(err)
	}
	objectPath := filepath.Join(repo, ".git", "objects", commit[:2], commit[2:])
	original, err := os.ReadFile(objectPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(objectPath); err != nil {
		t.Fatal(err)
	}
	if err := VerifyPrivateRefs(t.Context(), repo, refs); err == nil {
		t.Fatal("missing pinned commit was accepted")
	}
	if got, err := RevParse(t.Context(), repo, refs[0].Ref); err != nil || got != commit {
		t.Fatal("missing-object refusal moved immutable pin")
	}
	if err := os.WriteFile(objectPath, original, 0444); err != nil {
		t.Fatal(err)
	}
	if err := VerifyPrivateRefs(t.Context(), repo, refs); err != nil {
		t.Fatalf("exact restored object cannot retry: %v", err)
	}
}
