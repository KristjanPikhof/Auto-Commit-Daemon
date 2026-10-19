package git

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFinishHistoryRewriteCASRejectsHEADAttachmentMovement(t *testing.T) {
	repo := initRepo(t)
	finishGitRun(t, repo, "commit", "--allow-empty", "-qm", "Record initial source")
	head := finishGitRun(t, repo, "rev-parse", "HEAD")
	tree := finishGitRun(t, repo, "rev-parse", "HEAD^{tree}")
	wt, err := ResolveWorktree(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte(wt.Root))
	worktree := hex.EncodeToString(hash[:8])
	index, err := IndexContentDigest(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := BuildFinishHistoryChain(t.Context(), repo, FinishHistoryBuildOptions{PlanID: "head-movement", WorktreeID: worktree, SourceBranchRef: "refs/heads/main", TargetBranchRef: "refs/heads/main", ExpectedHead: head, IndexDigest: index, SourceRef: FinishRefPrefix + worktree + "/history/head-movement/source", LockToken: "head-movement-1", SourceChain: []string{head}, Goals: []FinishHistoryGoal{{TreeOID: tree, Message: "Clarify initial source", AuthorOID: head, OldOIDs: []string{head}}}})
	if err != nil {
		t.Fatal(err)
	}
	// A reference hook can bypass normal Git locks. Replacing HEAD during prepare
	// leaves the same text but changes its identity; the transaction must abort.
	hook := filepath.Join(wt.GitDir, "hooks", "reference-transaction")
	script := "#!/bin/sh\nif [ \"$1\" = prepared ] && [ ! -f .git/head-moved ]; then\n cp .git/HEAD .git/HEAD.new\n mv .git/HEAD.new .git/HEAD\n touch .git/head-moved\nfi\n"
	if err := os.WriteFile(hook, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := ApplyFinishHistoryProof(t.Context(), repo, proof, false, nil); !errors.Is(err, ErrFinishUnsafe) {
		t.Fatalf("attachment change accepted: %v", err)
	}
	if got := finishGitRun(t, repo, "rev-parse", "refs/heads/main"); got != head {
		t.Fatal("CAS moved branch after HEAD replacement")
	}
	if _, err := RevParse(t.Context(), repo, proof.BackupRef); err == nil {
		t.Fatal("backup escaped aborted atomic transaction")
	}
}
