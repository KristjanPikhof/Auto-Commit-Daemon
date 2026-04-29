package daemon

import (
	"context"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

func TestClassifyTokenTransition_LegacyTokenForcesDiverged(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	got, err := ClassifyTokenTransition(ctx, f.dir,
		"rev:"+head,
		branchTokenRev(head, "refs/heads/main"),
	)
	if err != nil {
		t.Fatalf("ClassifyTokenTransition: %v", err)
	}
	if got != TokenTransitionDiverged {
		t.Fatalf("got %v want %v", got, TokenTransitionDiverged)
	}
}

func TestClassifyTokenTransition_LegacyMissingForcesDiverged(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	got, err := ClassifyTokenTransition(ctx, f.dir,
		BranchTokenMissing,
		branchTokenMissing("refs/heads/main"),
	)
	if err != nil {
		t.Fatalf("ClassifyTokenTransition: %v", err)
	}
	if got != TokenTransitionDiverged {
		t.Fatalf("got %v want %v", got, TokenTransitionDiverged)
	}
}
