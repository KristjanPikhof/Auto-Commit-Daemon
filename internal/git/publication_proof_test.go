package git

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPublicationProofPoliciesAndReadOnlyState(t *testing.T) {
	repo := initRepo(t)
	ctx := t.Context()
	blob, err := HashObjectStdin(ctx, repo, []byte("published\n"))
	if err != nil {
		t.Fatal(err)
	}
	tree, err := Mktree(ctx, repo, []MktreeEntry{{Mode: RegularFileMode, Type: "blob", OID: blob, Path: "published.txt"}})
	if err != nil {
		t.Fatal(err)
	}
	head, err := CommitTree(ctx, repo, tree, "Publish captured work")
	if err != nil {
		t.Fatal(err)
	}
	if err := UpdateRef(ctx, repo, "refs/heads/main", head, ""); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "user.txt"), []byte("user staging\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, RunOpts{Dir: repo}, "add", "user.txt"); err != nil {
		t.Fatal(err)
	}
	before, err := Run(ctx, RunOpts{Dir: repo}, "status", "--porcelain=v1")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		op        PublicationOp
		policy    PublicationProofPolicy
		published bool
		wantError bool
	}{
		{name: "matching modify", op: PublicationOp{Operation: "modify", Path: "published.txt", AfterOID: blob, AfterMode: RegularFileMode}, published: true},
		{name: "mode mismatch", op: PublicationOp{Operation: "mode", Path: "published.txt", AfterOID: blob, AfterMode: "100755"}},
		{name: "CLI cannot settle delete", op: PublicationOp{Operation: "delete", Path: "absent.txt"}},
		{name: "worker settles absent delete", op: PublicationOp{Operation: "delete", Path: "absent.txt"}, policy: PublicationProofPolicy{AllowDeletes: true}, published: true},
		{name: "worker refuses existing delete", op: PublicationOp{Operation: "delete", Path: "published.txt"}, policy: PublicationProofPolicy{AllowDeletes: true}},
		{name: "CLI missing path is mismatch", op: PublicationOp{Operation: "modify", Path: "absent.txt", AfterOID: blob}, policy: PublicationProofPolicy{MissingPathIsMismatch: true}},
		{name: "worker missing path is error", op: PublicationOp{Operation: "modify", Path: "absent.txt", AfterOID: blob}, wantError: true},
		{name: "rename source still present", op: PublicationOp{Operation: "rename", Path: "published.txt", OldPath: "published.txt", BeforeOID: blob, AfterOID: blob}},
		{name: "rename proven", op: PublicationOp{Operation: "rename", Path: "published.txt", OldPath: "old.txt", BeforeOID: blob, AfterOID: blob}, published: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, published, err := ProvePublicationAtHEAD(ctx, repo, head, head, []PublicationOp{test.op}, test.policy)
			if published != test.published || (err != nil) != test.wantError {
				t.Fatalf("published=%t err=%v", published, err)
			}
		})
	}
	after, err := Run(ctx, RunOpts{Dir: repo}, "status", "--porcelain=v1")
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatalf("proof changed user staging: before=%q after=%q", before, after)
	}
}

func TestPublicationProofRejectsStaleExpectedHEAD(t *testing.T) {
	repo := initRepo(t)
	ctx := t.Context()
	blob, err := HashObjectStdin(ctx, repo, []byte("same content\n"))
	if err != nil {
		t.Fatal(err)
	}
	tree, err := Mktree(ctx, repo, []MktreeEntry{{Mode: RegularFileMode, Type: "blob", OID: blob, Path: "file.txt"}})
	if err != nil {
		t.Fatal(err)
	}
	expected, err := CommitTree(ctx, repo, tree, "Expected parent")
	if err != nil {
		t.Fatal(err)
	}
	current, err := CommitTree(ctx, repo, tree, "Later commit", expected)
	if err != nil {
		t.Fatal(err)
	}
	if err := UpdateRef(ctx, repo, "refs/heads/main", current, ""); err != nil {
		t.Fatal(err)
	}
	got, published, err := ProvePublicationAtHEAD(ctx, repo, expected, expected, []PublicationOp{{Operation: "modify", Path: "file.txt", AfterOID: blob}}, PublicationProofPolicy{})
	if err != nil || published || got != current {
		t.Fatalf("head=%s published=%t err=%v", got, published, err)
	}
}
