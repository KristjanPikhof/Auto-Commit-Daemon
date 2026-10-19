package state

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func finishPublicationFixture(t *testing.T, db *DB) (FinishRequest, FinishPublicationProof) {
	t.Helper()
	ctx := context.Background()
	head, tree, tip := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	cp := Checkpoint{ID: "cp-1-0123456789abcdef", OperationID: "publication-protection", WorktreeID: "0123456789abcdef", Reason: CheckpointReasonManualBarrier, ObservedRef: "refs/heads/main", ObservedHead: head, TreeOID: tree, CommitOID: strings.Repeat("d", 40), CreatedTS: nowSeconds()}
	cp.Ref = "refs/acd/checkpoints/v1/" + cp.WorktreeID + "/" + cp.ID
	if _, err := PrepareCheckpoint(ctx, db, cp, checkpointTestDigest); err != nil {
		t.Fatal(err)
	}
	if err := CompleteCheckpoint(ctx, db, cp.ID, cp.Ref, cp.CommitOID, nowSeconds()); err != nil {
		t.Fatal(err)
	}
	r := startFinishFixture(t, db, FinishRequest{ID: "publication", DeadlineTS: nowSeconds() + 300, Target: FinishTarget{Version: 1, RepositoryID: "repo", WorktreeID: cp.WorktreeID, BranchRef: cp.ObservedRef, ExpectedHead: head, CheckpointID: cp.ID, CheckpointTree: tree, TargetTree: tree, IndexDigest: "frozen-index", Mode: "local", Style: "imperative", ConfigDigest: "config", IncludeStaged: true}})
	p := FinishPublicationProof{Version: 1, RequestID: r.ID, Attempt: r.Attempt, WorktreeID: r.Target.WorktreeID, GitDir: filepath.Join(t.TempDir(), ".git"), BranchRef: r.Target.BranchRef, ExpectedHead: head, NewHead: tip, TargetTree: tree, IndexDigest: r.Target.IndexDigest, CleanIndexDigest: "clean-index", IncludeStaged: true, ObjectRef: "refs/acd/finish/publication/1", ObjectOID: tip, LockToken: strings.Repeat("e", 64), Commits: []FinishPublicationCommit{{OID: tip, ParentOID: head, TreeOID: tree}}}
	return r, p
}

func TestFinishPublicationJournalAndCompletion(t *testing.T) {
	ctx := context.Background()
	db, _ := openTestDB(t)
	r, p := finishPublicationFixture(t, db)
	if _, err := CompleteFinishPublication(ctx, db, r.ID, r.Attempt, FinishPublicationResult{}); err == nil {
		t.Fatal("completed before prepared")
	}
	bad := p
	bad.IndexDigest = "other index"
	if _, err := PrepareFinishPublication(ctx, db, r.ID, r.Attempt, bad, json.RawMessage(`{"steps":1}`)); !errors.Is(err, ErrFinishIdentity) {
		t.Fatalf("mismatched frozen index: %v", err)
	}
	prepared, err := PrepareFinishPublication(ctx, db, r.ID, r.Attempt, p, json.RawMessage(`{"steps":1}`))
	if err != nil || prepared.State != FinishPrepared {
		t.Fatalf("prepare %+v: %v", prepared, err)
	}
	if _, err := PrepareFinishPublication(ctx, db, r.ID, r.Attempt, p, json.RawMessage(`{"steps":1}`)); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	loaded, err := LoadFinishPublication(ctx, db, r.ID)
	if err != nil || loaded.ObjectRef != p.ObjectRef || loaded.LockToken != p.LockToken {
		t.Fatalf("durable proof %+v: %v", loaded, err)
	}
	pins, err := FinishObjectPins(ctx, db, p.WorktreeID)
	if err != nil || len(pins) != 1 || pins[0].Ref != p.ObjectRef || pins[0].CommitOID != p.ObjectOID {
		t.Fatalf("chain not retained %+v: %v", pins, err)
	}
	partial := FinishPublicationResult{Published: true, NewHead: p.NewHead, CommitsCreated: 1}
	if _, err := CompleteFinishPublication(ctx, db, r.ID, r.Attempt, partial); !errors.Is(err, ErrFinishIdentity) {
		t.Fatalf("completion without index proof: %v", err)
	}
	attention, err := MarkFinishPublicationAttention(ctx, db, r.ID, r.Attempt, "Later staging was preserved", partial)
	if err != nil || attention.State != FinishNeedsAttention {
		t.Fatalf("attention %+v: %v", attention, err)
	}
	var reported FinishPublicationResult
	if err := json.Unmarshal(attention.Result, &reported); err != nil || !reported.Published || reported.IndexReconciled {
		t.Fatalf("branch outcome lost %+v: %v", reported, err)
	}
	result := partial
	result.IndexReconciled = true
	result.StagedConsumed = true
	completed, err := CompleteFinishPublication(ctx, db, r.ID, r.Attempt, result)
	if err != nil || completed.State != FinishCompleted {
		t.Fatalf("completed %+v: %v", completed, err)
	}
	if _, err := CompleteFinishPublication(ctx, db, r.ID, r.Attempt, result); err != nil {
		t.Fatalf("repeated completion: %v", err)
	}
	if _, err := MarkFinishPublicationAttention(ctx, db, r.ID, r.Attempt, "stale", partial); !errors.Is(err, ErrFinishState) {
		t.Fatalf("terminal reopened: %v", err)
	}
}

func TestFinishPublicationRejectsMalformedProofAndResult(t *testing.T) {
	db, _ := openTestDB(t)
	r, p := finishPublicationFixture(t, db)
	for _, scenario := range []string{"abbreviated_oid", "mixed_format", "ref_injection", "wrong_namespace", "broken_parent", "wrong_tip", "relative_gitdir", "missing_lock"} {
		t.Run(scenario, func(t *testing.T) {
			bad := p
			bad.Commits = append([]FinishPublicationCommit(nil), p.Commits...)
			switch scenario {
			case "abbreviated_oid":
				bad.NewHead = "abc"
			case "mixed_format":
				bad.Commits[0].TreeOID = strings.Repeat("b", 64)
				bad.TargetTree = bad.Commits[0].TreeOID
			case "ref_injection":
				bad.BranchRef = "refs/heads/main\nupdate refs/heads/other"
			case "wrong_namespace":
				bad.ObjectRef = "refs/heads/private"
			case "broken_parent":
				bad.Commits[0].ParentOID = ""
			case "wrong_tip":
				bad.ObjectOID = strings.Repeat("f", 40)
			case "relative_gitdir":
				bad.GitDir = ".git"
			case "missing_lock":
				bad.LockToken = ""
			}
			if err := ValidateFinishPublicationProof(bad); !errors.Is(err, ErrFinishIdentity) {
				t.Fatalf("malformed proof accepted: %v", err)
			}
		})
	}
	ctx := context.Background()
	if _, err := PrepareFinishPublication(ctx, db, r.ID, r.Attempt, p, json.RawMessage(`{"steps":1}`)); err != nil {
		t.Fatal(err)
	}
	for _, result := range []FinishPublicationResult{{Published: false, IndexReconciled: true}, {Published: true, NewHead: p.NewHead, CommitsCreated: 1, StagedConsumed: true}, {Published: true, NewHead: strings.Repeat("f", 40), CommitsCreated: 1}} {
		if _, err := MarkFinishPublicationAttention(ctx, db, r.ID, r.Attempt, "Ambiguous publication", result); !errors.Is(err, ErrFinishIdentity) {
			t.Fatalf("false result accepted %+v: %v", result, err)
		}
	}
	if _, err := MarkFinishPublicationAttention(ctx, db, r.ID, r.Attempt+1, "stale", FinishPublicationResult{}); !errors.Is(err, ErrFinishIdentity) {
		t.Fatalf("stale attempt: %v", err)
	}
	bad := p
	bad.LockToken = strings.Repeat("f", 64)
	if _, err := PrepareFinishPublication(ctx, db, r.ID, r.Attempt, bad, json.RawMessage(`{"steps":1}`)); !errors.Is(err, ErrFinishState) {
		t.Fatalf("journal rewritten: %v", err)
	}
}
