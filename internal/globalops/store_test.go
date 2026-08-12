package globalops

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"database/sql"
)

func TestOperationQueriesAndConditionalAdvance(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "operations.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	steps := []Step{{Sequence: 1, Kind: "write_registry", Target: "/registry", Phase: "planned"}}
	if err := store.Prepare(ctx, Operation{ID: "old", Kind: "setup", Phase: "planned", PlanDigest: "old-digest"}, steps); err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(ctx, "old", "needs_attention", "interrupted", true); err != nil {
		t.Fatal(err)
	}
	if err := store.Prepare(ctx, Operation{ID: "new", Kind: "setup", Phase: "planned", PlanDigest: "new-digest"}, steps); err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(ctx, "new", "committed", "", true); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	readOnly, err := OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	unresolved, err := readOnly.OperationsByPhase(ctx, "needs_attention")
	if err != nil || len(unresolved) != 1 || unresolved[0].ID != "old" {
		t.Fatalf("unresolved=%+v err=%v", unresolved, err)
	}
	latest, ok, err := readOnly.LatestCommittedSetup(ctx)
	if err != nil || !ok || latest.ID != "new" {
		t.Fatalf("latest=%+v ok=%t err=%v", latest, ok, err)
	}
	gotSteps, err := readOnly.Steps(ctx, "old")
	if err != nil || len(gotSteps) != 1 || gotSteps[0].Target != "/registry" {
		t.Fatalf("steps=%+v err=%v", gotSteps, err)
	}
	if err := readOnly.Close(); err != nil {
		t.Fatal(err)
	}

	writable, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer writable.Close()
	if err := writable.AdvanceIfPhase(ctx, "old", "planned", "superseded", "", true); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("stale conditional advance error=%v", err)
	}
	if err := writable.AdvanceIfPhase(ctx, "old", "needs_attention", "superseded", "proved", true); err != nil {
		t.Fatal(err)
	}
}
