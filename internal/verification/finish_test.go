package verification

import (
	"context"
	"testing"
	"time"
)

func TestFinishWorkerVerificationEvidence(t *testing.T) {
	repo := initVerificationRepo(t)
	oid := candidateCommitWithFile(t, repo, "candidate.txt", "exact candidate\n")
	command, err := NewApprovedCommand(repo, "finish-approved-v1", ModeFast, "test \"$(cat candidate.txt)\" = 'exact candidate' && printf checked", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := (Runner{}).Run(context.Background(), Request{RepoPath: repo, CandidateID: "finish-check", CommitOID: oid, Command: command})
	if err != nil || result.Status != StatusPassed || !result.Executed || result.EnvironmentDigest == "" || result.CommitOID != oid || result.CommandDigest != command.Identity().Digest || result.ApprovalID != command.Identity().ApprovalID || result.Output != "checked" {
		t.Fatalf("executed evidence %+v: %v", result, err)
	}
	structural, err := (Runner{}).CheckStructural(context.Background(), StructuralRequest{RepoPath: repo, CandidateID: "finish-structural", CommitOID: oid})
	if err != nil || structural.Status != StatusPassed || structural.Executed || structural.EnvironmentDigest != "" || structural.CommandDigest != "" {
		t.Fatalf("structural evidence claims command execution %+v: %v", structural, err)
	}
}
