package cli

import (
	"database/sql"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestCLIPublicationProofPreservesEvidenceValidity(t *testing.T) {
	withIsolatedHome(t)
	repo := initRepoForRepoLifecycle(t)
	head := commitStartRepoSeed(t, repo)
	blob, err := git.LsTreeBlobOID(t.Context(), repo, head, "seed.txt")
	if err != nil {
		t.Fatal(err)
	}
	op := state.CaptureOp{Op: "modify", Path: "seed.txt", AfterOID: sql.NullString{String: blob}}
	if _, published, err := cliAlreadyPublishedAtHEAD(t.Context(), repo, head, head, []state.CaptureOp{op}); err != nil || published {
		t.Fatalf("invalid after evidence published=%t err=%v", published, err)
	}
	op.AfterOID.Valid = true
	if got, published, err := cliAlreadyPublishedAtHEAD(t.Context(), repo, head, head, []state.CaptureOp{op}); err != nil || !published || got != head {
		t.Fatalf("valid evidence head=%s published=%t err=%v", got, published, err)
	}
	op = state.CaptureOp{Op: "delete", Path: "missing.txt"}
	if _, published, err := cliAlreadyPublishedAtHEAD(t.Context(), repo, head, head, []state.CaptureOp{op}); err != nil || published {
		t.Fatalf("CLI must retain delete recovery boundary: published=%t err=%v", published, err)
	}
}
