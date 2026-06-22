package daemon

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// fakeOpsLoader returns a stable per-seq op slice without touching any DB.
// Tests build the slice directly so they can exercise coalesce edge cases
// (multi-path events, deletes, renames) without going through capture.
type fakeOpsLoader map[int64][]state.CaptureOp

func (m fakeOpsLoader) load(_ context.Context, _ *state.DB, seq int64) ([]state.CaptureOp, error) {
	return m[seq], nil
}

func captureEv(seq int64, branch string, gen int64, base, path string) state.CaptureEvent {
	return state.CaptureEvent{
		Seq:              seq,
		BranchRef:        branch,
		BranchGeneration: gen,
		BaseHead:         base,
		Operation:        "modify",
		Path:             path,
	}
}

func modifyOp(seq int64, path, beforeOID, afterOID string) state.CaptureOp {
	return state.CaptureOp{
		EventSeq:   seq,
		Ord:        0,
		Op:         "modify",
		Path:       path,
		BeforeOID:  sql.NullString{String: beforeOID, Valid: true},
		BeforeMode: sql.NullString{String: "100644", Valid: true},
		AfterOID:   sql.NullString{String: afterOID, Valid: true},
		AfterMode:  sql.NullString{String: "100644", Valid: true},
		Fidelity:   "exact",
	}
}

func createOp(seq int64, path, afterOID string) state.CaptureOp {
	return state.CaptureOp{
		EventSeq:  seq,
		Ord:       0,
		Op:        "create",
		Path:      path,
		AfterOID:  sql.NullString{String: afterOID, Valid: true},
		AfterMode: sql.NullString{String: "100644", Valid: true},
		Fidelity:  "exact",
	}
}

func deleteOp(seq int64, path, beforeOID string) state.CaptureOp {
	return state.CaptureOp{
		EventSeq:   seq,
		Ord:        0,
		Op:         "delete",
		Path:       path,
		BeforeOID:  sql.NullString{String: beforeOID, Valid: true},
		BeforeMode: sql.NullString{String: "100644", Valid: true},
		Fidelity:   "exact",
	}
}

func TestCoalesceIntentWindow_DisabledPassesThroughUnchanged(t *testing.T) {
	ctx := context.Background()
	events := []state.CaptureEvent{
		captureEv(1, "refs/heads/main", 1, "head1", "p.txt"),
		captureEv(2, "refs/heads/main", 1, "head1", "p.txt"),
		captureEv(3, "refs/heads/main", 1, "head1", "p.txt"),
	}
	loader := fakeOpsLoader{
		1: {modifyOp(1, "p.txt", "A", "B")},
		2: {modifyOp(2, "p.txt", "B", "C")},
		3: {modifyOp(3, "p.txt", "C", "D")},
	}
	out, err := coalesceIntentWindow(ctx, nil, events, false, loader.load)
	if err != nil {
		t.Fatalf("coalesceIntentWindow: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("offers=%d want 3 (passthrough)", len(out))
	}
	for i, offer := range out {
		if offer.Primary.Seq != events[i].Seq {
			t.Fatalf("offer[%d] primary seq=%d want %d", i, offer.Primary.Seq, events[i].Seq)
		}
		if len(offer.Token.OriginalSeqs) != 1 || offer.Token.OriginalSeqs[0] != events[i].Seq {
			t.Fatalf("offer[%d] OriginalSeqs=%v want [%d]", i, offer.Token.OriginalSeqs, events[i].Seq)
		}
		if len(offer.Token.Covered) != 0 {
			t.Fatalf("offer[%d] Covered len=%d want 0 (disabled)", i, len(offer.Token.Covered))
		}
	}
}

func TestCoalesceIntentWindow_FoldsConsecutiveSamePathRun(t *testing.T) {
	ctx := context.Background()
	events := []state.CaptureEvent{
		captureEv(1, "refs/heads/main", 1, "head1", "p.txt"),
		captureEv(2, "refs/heads/main", 1, "head1", "p.txt"),
		captureEv(3, "refs/heads/main", 1, "head1", "p.txt"),
		captureEv(4, "refs/heads/main", 1, "head1", "p.txt"),
	}
	loader := fakeOpsLoader{
		1: {modifyOp(1, "p.txt", "A", "B")},
		2: {modifyOp(2, "p.txt", "B", "C")},
		3: {modifyOp(3, "p.txt", "C", "D")},
		4: {modifyOp(4, "p.txt", "D", "E")},
	}
	out, err := coalesceIntentWindow(ctx, nil, events, true, loader.load)
	if err != nil {
		t.Fatalf("coalesceIntentWindow: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("offers=%d want 1 (coalesced run)", len(out))
	}
	offer := out[0]
	if offer.Primary.Seq != 1 {
		t.Fatalf("primary seq=%d want 1 (earliest)", offer.Primary.Seq)
	}
	wantSeqs := []int64{1, 2, 3, 4}
	if !reflect.DeepEqual(offer.Token.OriginalSeqs, wantSeqs) {
		t.Fatalf("OriginalSeqs=%v want %v", offer.Token.OriginalSeqs, wantSeqs)
	}
	if len(offer.Token.Covered) != 3 {
		t.Fatalf("Covered len=%d want 3", len(offer.Token.Covered))
	}
	if len(offer.MergedOps) != 1 {
		t.Fatalf("MergedOps len=%d want 1", len(offer.MergedOps))
	}
	merged := offer.MergedOps[0]
	if merged.Path != "p.txt" || merged.Op != "modify" {
		t.Fatalf("merged op=%q path=%q want modify p.txt", merged.Op, merged.Path)
	}
	if merged.BeforeOID.String != "A" {
		t.Fatalf("merged BeforeOID=%q want A (first event's before-state)", merged.BeforeOID.String)
	}
	if merged.AfterOID.String != "E" {
		t.Fatalf("merged AfterOID=%q want E (last event's after-state)", merged.AfterOID.String)
	}
}

func TestCoalesceIntentWindow_PQPDoesNotCoalesce(t *testing.T) {
	ctx := context.Background()
	events := []state.CaptureEvent{
		captureEv(1, "refs/heads/main", 1, "head1", "p.txt"),
		captureEv(2, "refs/heads/main", 1, "head1", "q.txt"),
		captureEv(3, "refs/heads/main", 1, "head1", "p.txt"),
	}
	loader := fakeOpsLoader{
		1: {modifyOp(1, "p.txt", "A", "B")},
		2: {modifyOp(2, "q.txt", "X", "Y")},
		3: {modifyOp(3, "p.txt", "B", "C")},
	}
	out, err := coalesceIntentWindow(ctx, nil, events, true, loader.load)
	if err != nil {
		t.Fatalf("coalesceIntentWindow: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("offers=%d want 3 (no coalesce across other-path interleave)", len(out))
	}
	for i, offer := range out {
		if offer.Primary.Seq != events[i].Seq {
			t.Fatalf("offer[%d] primary seq=%d want %d", i, offer.Primary.Seq, events[i].Seq)
		}
		if len(offer.Token.Covered) != 0 {
			t.Fatalf("offer[%d] Covered len=%d want 0 (P/Q/P must split)", i, len(offer.Token.Covered))
		}
	}
}

func TestCoalesceIntentWindow_BranchTokenChangeStopsRun(t *testing.T) {
	ctx := context.Background()
	// Same path, but branch_generation flips between seq 2 and seq 3 — the
	// daemon classified a divergence, so the runs must NOT coalesce across
	// the seam.
	events := []state.CaptureEvent{
		captureEv(1, "refs/heads/main", 1, "head1", "p.txt"),
		captureEv(2, "refs/heads/main", 1, "head1", "p.txt"),
		captureEv(3, "refs/heads/main", 2, "head2", "p.txt"),
		captureEv(4, "refs/heads/main", 2, "head2", "p.txt"),
	}
	loader := fakeOpsLoader{
		1: {modifyOp(1, "p.txt", "A", "B")},
		2: {modifyOp(2, "p.txt", "B", "C")},
		3: {modifyOp(3, "p.txt", "X", "Y")},
		4: {modifyOp(4, "p.txt", "Y", "Z")},
	}
	out, err := coalesceIntentWindow(ctx, nil, events, true, loader.load)
	if err != nil {
		t.Fatalf("coalesceIntentWindow: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("offers=%d want 2 (one per generation)", len(out))
	}
	if !reflect.DeepEqual(out[0].Token.OriginalSeqs, []int64{1, 2}) {
		t.Fatalf("offer[0] seqs=%v want [1 2]", out[0].Token.OriginalSeqs)
	}
	if !reflect.DeepEqual(out[1].Token.OriginalSeqs, []int64{3, 4}) {
		t.Fatalf("offer[1] seqs=%v want [3 4]", out[1].Token.OriginalSeqs)
	}
	if out[0].MergedOps[0].AfterOID.String != "C" {
		t.Fatalf("offer[0] merged AfterOID=%q want C", out[0].MergedOps[0].AfterOID.String)
	}
	if out[1].MergedOps[0].AfterOID.String != "Z" {
		t.Fatalf("offer[1] merged AfterOID=%q want Z", out[1].MergedOps[0].AfterOID.String)
	}
}

func TestCoalesceIntentWindow_BranchRefChangeStopsRun(t *testing.T) {
	ctx := context.Background()
	events := []state.CaptureEvent{
		captureEv(1, "refs/heads/main", 1, "head1", "p.txt"),
		captureEv(2, "refs/heads/feature", 1, "head1", "p.txt"),
	}
	loader := fakeOpsLoader{
		1: {modifyOp(1, "p.txt", "A", "B")},
		2: {modifyOp(2, "p.txt", "B", "C")},
	}
	out, err := coalesceIntentWindow(ctx, nil, events, true, loader.load)
	if err != nil {
		t.Fatalf("coalesceIntentWindow: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("offers=%d want 2 (different branch_ref)", len(out))
	}
}

func TestCoalesceIntentWindow_BaseHeadChangeStopsRun(t *testing.T) {
	ctx := context.Background()
	// Same branch + generation but base_head shifted — an external
	// committer landed something between seq 2 and seq 3. Squashing across
	// would chain the merged ops off a stale anchor.
	events := []state.CaptureEvent{
		captureEv(1, "refs/heads/main", 1, "head1", "p.txt"),
		captureEv(2, "refs/heads/main", 1, "head1", "p.txt"),
		captureEv(3, "refs/heads/main", 1, "head2", "p.txt"),
	}
	loader := fakeOpsLoader{
		1: {modifyOp(1, "p.txt", "A", "B")},
		2: {modifyOp(2, "p.txt", "B", "C")},
		3: {modifyOp(3, "p.txt", "C", "D")},
	}
	out, err := coalesceIntentWindow(ctx, nil, events, true, loader.load)
	if err != nil {
		t.Fatalf("coalesceIntentWindow: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("offers=%d want 2 (base_head divergence breaks run)", len(out))
	}
	if !reflect.DeepEqual(out[0].Token.OriginalSeqs, []int64{1, 2}) {
		t.Fatalf("offer[0] seqs=%v want [1 2]", out[0].Token.OriginalSeqs)
	}
	if out[1].Primary.Seq != 3 {
		t.Fatalf("offer[1] primary=%d want 3", out[1].Primary.Seq)
	}
}

func TestCoalesceIntentWindow_RenameDoesNotCoalesce(t *testing.T) {
	ctx := context.Background()
	events := []state.CaptureEvent{
		captureEv(1, "refs/heads/main", 1, "head1", "p.txt"),
		{
			Seq:              2,
			BranchRef:        "refs/heads/main",
			BranchGeneration: 1,
			BaseHead:         "head1",
			Operation:        "rename",
			Path:             "p.txt",
			OldPath:          sql.NullString{String: "old.txt", Valid: true},
		},
		captureEv(3, "refs/heads/main", 1, "head1", "p.txt"),
	}
	loader := fakeOpsLoader{
		1: {modifyOp(1, "p.txt", "A", "B")},
		2: {{
			EventSeq:   2,
			Op:         "rename",
			Path:       "p.txt",
			OldPath:    sql.NullString{String: "old.txt", Valid: true},
			BeforeOID:  sql.NullString{String: "B", Valid: true},
			BeforeMode: sql.NullString{String: "100644", Valid: true},
			AfterOID:   sql.NullString{String: "C", Valid: true},
			AfterMode:  sql.NullString{String: "100644", Valid: true},
			Fidelity:   "exact",
		}},
		3: {modifyOp(3, "p.txt", "C", "D")},
	}
	out, err := coalesceIntentWindow(ctx, nil, events, true, loader.load)
	if err != nil {
		t.Fatalf("coalesceIntentWindow: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("offers=%d want 3 (rename never folds)", len(out))
	}
}

func TestCoalesceIntentWindow_DeleteDoesNotCoalesce(t *testing.T) {
	ctx := context.Background()
	events := []state.CaptureEvent{
		captureEv(1, "refs/heads/main", 1, "head1", "p.txt"),
		{
			Seq:              2,
			BranchRef:        "refs/heads/main",
			BranchGeneration: 1,
			BaseHead:         "head1",
			Operation:        "delete",
			Path:             "p.txt",
		},
		captureEv(3, "refs/heads/main", 1, "head1", "p.txt"),
	}
	loader := fakeOpsLoader{
		1: {modifyOp(1, "p.txt", "A", "B")},
		2: {deleteOp(2, "p.txt", "B")},
		3: {createOp(3, "p.txt", "C")},
	}
	out, err := coalesceIntentWindow(ctx, nil, events, true, loader.load)
	if err != nil {
		t.Fatalf("coalesceIntentWindow: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("offers=%d want 3 (delete breaks the chain — path existence flipped)", len(out))
	}
}

func TestCoalesceIntentWindow_CreateThenModifiesPromoteAndDropBefore(t *testing.T) {
	ctx := context.Background()
	events := []state.CaptureEvent{
		{
			Seq:              1,
			BranchRef:        "refs/heads/main",
			BranchGeneration: 1,
			BaseHead:         "head1",
			Operation:        "create",
			Path:             "new.txt",
		},
		captureEv(2, "refs/heads/main", 1, "head1", "new.txt"),
		captureEv(3, "refs/heads/main", 1, "head1", "new.txt"),
	}
	loader := fakeOpsLoader{
		1: {createOp(1, "new.txt", "A")},
		2: {modifyOp(2, "new.txt", "A", "B")},
		3: {modifyOp(3, "new.txt", "B", "C")},
	}
	out, err := coalesceIntentWindow(ctx, nil, events, true, loader.load)
	if err != nil {
		t.Fatalf("coalesceIntentWindow: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("offers=%d want 1 (create+modify chain)", len(out))
	}
	merged := out[0].MergedOps[0]
	if merged.Op != "create" {
		t.Fatalf("merged op=%q want create (first-was-create promotion)", merged.Op)
	}
	if merged.BeforeOID.Valid {
		t.Fatalf("merged BeforeOID valid=%v want invalid (create has no before)", merged.BeforeOID)
	}
	if merged.AfterOID.String != "C" {
		t.Fatalf("merged AfterOID=%q want C", merged.AfterOID.String)
	}
}

func TestCoalesceIntentWindow_MultiPathEventDoesNotCoalesce(t *testing.T) {
	ctx := context.Background()
	// Single capture row touches 2 paths via its ops slice. Coalesce must
	// pass it through as its own offer rather than fold it into a same-
	// path neighbour.
	events := []state.CaptureEvent{
		captureEv(1, "refs/heads/main", 1, "head1", "p.txt"),
		captureEv(2, "refs/heads/main", 1, "head1", "p.txt"),
		captureEv(3, "refs/heads/main", 1, "head1", "p.txt"),
	}
	loader := fakeOpsLoader{
		1: {modifyOp(1, "p.txt", "A", "B")},
		2: { // multi-path captured row
			modifyOp(2, "p.txt", "B", "C"),
			modifyOp(2, "q.txt", "X", "Y"),
		},
		3: {modifyOp(3, "p.txt", "C", "D")},
	}
	out, err := coalesceIntentWindow(ctx, nil, events, true, loader.load)
	if err != nil {
		t.Fatalf("coalesceIntentWindow: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("offers=%d want 3 (multi-path row breaks the run)", len(out))
	}
}

func TestCoalesceIntentWindow_PathCoalesceEnabledRespectsEnv(t *testing.T) {
	t.Setenv(envIntentPathCoalesce, "")
	if pathCoalesceEnabled() {
		t.Fatal("empty env should default OFF")
	}
	for _, off := range []string{"0", "false", "FALSE", "no", "NO", "off", "Off", "anything-else"} {
		t.Setenv(envIntentPathCoalesce, off)
		if pathCoalesceEnabled() {
			t.Fatalf("env=%q should disable coalesce", off)
		}
	}
	for _, on := range []string{"1", "true", "TRUE", "yes", "on"} {
		t.Setenv(envIntentPathCoalesce, on)
		if !pathCoalesceEnabled() {
			t.Fatalf("env=%q should enable coalesce", on)
		}
	}
}

func TestCoalesceIntentWindow_EmptyWindowReturnsNil(t *testing.T) {
	ctx := context.Background()
	out, err := coalesceIntentWindow(ctx, nil, nil, true, nil)
	if err != nil {
		t.Fatalf("coalesceIntentWindow: %v", err)
	}
	if out != nil {
		t.Fatalf("offers=%v want nil", out)
	}
}
