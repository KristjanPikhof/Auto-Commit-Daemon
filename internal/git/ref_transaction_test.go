package git

import (
	"context"
	"testing"
)

func TestPrivateRefTransactionIsAllOrNothing(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	if err := Init(ctx, repo); err != nil {
		t.Fatal(err)
	}
	first, err := HashObjectStdin(ctx, repo, []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := HashObjectStdin(ctx, repo, []byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	refs := []RefExpectation{{Ref: "refs/acd/checkpoints/test/one", OID: first},
		{Ref: "refs/acd/checkpoints/test/two", OID: second}}
	if err := CreateRefsCAS(ctx, repo, refs); err != nil {
		t.Fatal(err)
	}
	wrong := append([]RefExpectation(nil), refs...)
	wrong[1].OID = first
	if err := DeleteRefsCAS(ctx, repo, wrong); err == nil {
		t.Fatal("expected mismatched CAS transaction to fail")
	}
	for _, item := range refs {
		got, err := RevParse(ctx, repo, item.Ref)
		if err != nil || got != item.OID {
			t.Fatalf("%s=(%s,%v), want %s", item.Ref, got, err, item.OID)
		}
	}
	if err := DeleteRefsCAS(ctx, repo, refs); err != nil {
		t.Fatal(err)
	}
	if err := CreateRefsCAS(ctx, repo, refs); err != nil {
		t.Fatal(err)
	}
}
