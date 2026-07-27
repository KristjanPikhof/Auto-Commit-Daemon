package daemon

import (
	"context"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const selfPublicationStressSeed uint64 = 0xACD18

// TestSelfPublicationRecoveryOrderingStress exercises the production
// Git/SQLite recovery primitive under the required deterministic
// GOMAXPROCS=1, -count=50 stress gate. Each repetition starts with a real
// git_applied journal, completes it exactly once, and proves that a second
// recovery pass is an idempotent no-op.
func TestSelfPublicationRecoveryOrderingStress(t *testing.T) {
	ctx := context.Background()
	f := newCaptureFixture(t)
	publication, seq, target := seedRecoverableSelfPublication(
		t, ctx, f, state.SelfPublicationGitApplied)

	summary, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, ReplayOpts{})
	if err != nil {
		t.Fatalf("RecoverSelfPublications: %v", err)
	}
	if summary.Inspected != 1 || summary.Completed != 1 ||
		summary.Abandoned != 0 || summary.FinalTargetOID != target {
		t.Fatalf("recovery summary=%+v", summary)
	}
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, publication.ID, state.SelfPublicationCompleted)
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, seq, state.EventStatePublished, target)
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("RevParse HEAD: %v", err)
	}
	if head != target {
		t.Fatalf("HEAD=%s want target=%s", head, target)
	}

	again, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, ReplayOpts{})
	if err != nil {
		t.Fatalf("idempotent RecoverSelfPublications: %v", err)
	}
	if again.Inspected != 0 || again.Completed != 0 ||
		again.Abandoned != 0 {
		t.Fatalf("idempotent recovery summary=%+v", again)
	}
	t.Logf("seed=%x publication=%s event=%d target=%s",
		selfPublicationStressSeed, publication.ID, seq, target)
}
