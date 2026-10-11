package state

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestFinishHistoryRewriteJournalRetainsAttemptsAndImmutableMappings(t *testing.T) {
	db, _ := openTestDB(t)
	ctx := t.Context()
	seed := FinishHistoryRequest{PlanID: "plan", WorktreeID: "worktree", DeadlineTS: nowSeconds() + 300, IndexDigest: "index", ConfigDigest: "approved"}
	r, created, err := StartFinishHistoryRequest(ctx, db, seed)
	if err != nil || !created {
		t.Fatal(err)
	}
	reconnect, created, err := StartFinishHistoryRequest(ctx, db, seed)
	if err != nil || created || reconnect.Attempt != r.Attempt {
		t.Fatal("reconnect renewed attempt")
	}
	other := seed
	other.PlanID = "other"
	if _, _, err := StartFinishHistoryRequest(ctx, db, other); !errors.Is(err, ErrFinishBusy) {
		t.Fatal("parallel history request accepted")
	}
	r.Status = "failed"
	if err := UpdateFinishHistoryRequest(ctx, db, r, "running"); err != nil {
		t.Fatal(err)
	}
	r, created, err = StartFinishHistoryRequest(ctx, db, seed)
	if err != nil || !created || r.Attempt != 2 {
		t.Fatal("retry lost attempt evidence")
	}
	proof := FinishHistoryProof{PlanID: r.PlanID, WorktreeID: r.WorktreeID, Mappings: []FinishHistoryMapping{{OldOID: "source", NewOID: "output"}, {OldOID: "neutral"}}}
	r.Proof, _ = json.Marshal(proof)
	r.Status = "prepared"
	if err := UpdateFinishHistoryRequest(ctx, db, r, "running"); err != nil {
		t.Fatal(err)
	}
	if err := InterruptFinishHistoryRequests(ctx, db, r.WorktreeID); err != nil {
		t.Fatal(err)
	}
	unchanged, _, err := LoadFinishHistoryRequest(ctx, db, r.PlanID)
	if err != nil || unchanged.Status != "prepared" {
		t.Fatal("restart discarded prepared proof")
	}
	for _, sql := range []string{"UPDATE finish_history_requests SET request_json='{}'", "UPDATE finish_history_requests SET proof_json='{}' WHERE status='prepared'", "DELETE FROM finish_history_requests"} {
		if _, err := db.SQL().ExecContext(ctx, sql); err == nil {
			t.Fatalf("immutable evidence accepted %s", sql)
		}
	}
	r.Status = "completed"
	if err := UpdateFinishHistoryRequest(ctx, db, r, "prepared"); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{"UPDATE finish_history_mappings SET new_oid='fabricated'", "DELETE FROM finish_history_mappings"} {
		if _, err := db.SQL().ExecContext(ctx, sql); err == nil {
			t.Fatal("mapping mutated")
		}
	}
	var n int
	if err := db.ReadSQL().QueryRowContext(ctx, "SELECT count(*) FROM finish_history_requests").Scan(&n); err != nil || n != 2 {
		t.Fatal("attempt provenance lost")
	}
	if err := db.ReadSQL().QueryRowContext(ctx, "SELECT count(*) FROM finish_history_mappings").Scan(&n); err != nil || n != 2 {
		t.Fatal("neutral/source mapping lost")
	}
}
