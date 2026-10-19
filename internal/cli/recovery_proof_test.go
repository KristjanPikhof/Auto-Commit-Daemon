package cli

import (
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
)

func TestCLIPublicationProofPreservesEvidenceValidity(t *testing.T) {
	withIsolatedHome(t)
	repo := initRepoForRepoLifecycle(t)
	head := commitStartRepoSeed(t, repo)
	blob, err := git.LsTreeBlobOID(t.Context(), repo, head, "seed.txt")
	if err != nil {
		t.Fatal(err)
	}
	// Support proofs require actual immutable evidence, not an optional field
	// whose value happens to match HEAD.
	op := git.PublicationOp{Operation: "modify", Path: "seed.txt"}
	policy := git.PublicationProofPolicy{MissingRefIsMismatch: true}
	if _, published, err := git.ProvePublicationAtHEAD(t.Context(), repo, head, head, []git.PublicationOp{op}, policy); err != nil || published {
		t.Fatalf("missing after evidence published=%t err=%v", published, err)
	}
	op.AfterOID = blob
	if got, published, err := git.ProvePublicationAtHEAD(t.Context(), repo, head, head, []git.PublicationOp{op}, policy); err != nil || !published || got != head {
		t.Fatalf("valid evidence head=%s published=%t err=%v", got, published, err)
	}
	op = git.PublicationOp{Operation: "delete", Path: "missing.txt"}
	if _, published, err := git.ProvePublicationAtHEAD(t.Context(), repo, head, head, []git.PublicationOp{op}, policy); err != nil || published {
		t.Fatalf("support must retain delete recovery boundary: published=%t err=%v", published, err)
	}
}

func TestFinishStatusNativeRecaptureAndBranchMovement(t *testing.T) {
	rec, db, _ := finishStatusFixture(t)
	base := finishStatusGit(t, rec.Path, "rev-parse", "HEAD")
	finishStatusWrite(t, rec.Path, "recovered.txt", "preserved work")
	cp := finishStatusCheckpoint(t, rec, db)
	seq, err := testutil.SeedLegacyCapture(t.Context(), db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BaseHead: base, Operation: "create",
		Path: "recovered.txt", Fidelity: "exact", State: state.EventStateRecovered,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(t.Context(), `INSERT INTO checkpoint_events(checkpoint_id,ord,event_seq) VALUES(?,0,?)`, cp.CheckpointID, seq); err != nil {
		t.Fatal(err)
	}
	finishStatusGit(t, rec.Path, "add", "recovered.txt")
	finishStatusGit(t, rec.Path, "commit", "-m", "Restore preserved work")
	outcome := finishStatusRead(t, rec, db).PublicationOutcome
	if outcome.BranchCommitted == nil || !*outcome.BranchCommitted || outcome.PendingChanges != 0 || outcome.RecoveredChanges != 1 {
		t.Fatalf("native recapture lost current tree or immutable recovery evidence: %+v", outcome)
	}
	finishStatusGit(t, rec.Path, "checkout", "-b", "other", base)
	outcome = finishStatusRead(t, rec, db).PublicationOutcome
	if outcome.BranchCommitted == nil || *outcome.BranchCommitted || outcome.PendingChanges != 1 || outcome.RecoveredChanges != 1 {
		t.Fatalf("branch movement reused old commitment or lost preserved work: %+v", outcome)
	}
}
