package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishLegacySymbolicBranchCannotSettleOldJournal(t *testing.T) {
	for _, phase := range []string{state.SelfPublicationPrepared, state.SelfPublicationGitApplied} {
		t.Run(phase, func(t *testing.T) {
			f := newCaptureFixture(t)
			ctx := context.Background()
			publication, seq, _ := seedRecoverableSelfPublication(t, ctx, f, phase)
			target := publication.SourceHead
			if phase == state.SelfPublicationGitApplied {
				target = publication.TargetCommitOID
			}
			if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "update-ref", "refs/heads/alias-target", target); err != nil {
				t.Fatal(err)
			}
			if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "symbolic-ref", publication.BranchRef, "refs/heads/alias-target"); err != nil {
				t.Fatal(err)
			}
			_, err := RecoverSelfPublications(ctx, f.dir, f.db, f.cctx, ReplayOpts{})
			if !errors.Is(err, ErrSelfPublicationRecoveryAmbiguous) {
				t.Fatalf("branch alias accepted: %v", err)
			}
			assertSelfPublicationRecoveryPhase(t, ctx, f.db, publication.ID, phase)
			assertSelfPublicationRecoveryEvent(t, ctx, f.db, seq, state.EventStatePending, "")
		})
	}
}
