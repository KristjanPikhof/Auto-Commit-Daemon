package daemon

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestCaptureResilienceFourLargeAssets(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	store := checkpointpkg.Store{DB: f.db}
	names := []string{"eng_autocomplete.bin", "est_autocomplete.bin", "rus_autocomplete.bin", "spa_autocomplete.bin"}
	sizes := []int{14832809, 21685601, 18343302, 15617454}
	for i, name := range names {
		if err := os.WriteFile(filepath.Join(f.dir, name), bytes.Repeat([]byte{byte(i)}, sizes[i]), 0644); err != nil {
			t.Fatal(err)
		}
	}
	summary, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{CheckpointStore: &store, IgnoreChecker: f.ig})
	if err != nil || !summary.Protected || summary.Oversize != 0 || summary.Errors != 0 {
		t.Fatalf("capture=%+v err=%v", summary, err)
	}
	projection, err := state.ReadCheckpointProjection(ctx, f.db.Path(), 1)
	if err != nil || projection.Latest == nil || projection.Latest.Partial {
		t.Fatalf("projection=%+v err=%v", projection, err)
	}
	for _, name := range names {
		file, err := os.Open(filepath.Join(f.dir, name))
		if err != nil {
			t.Fatal(err)
		}
		want, err := git.HashObjectReaderReadOnly(ctx, f.dir, file)
		file.Close()
		if err != nil {
			t.Fatal(err)
		}
		got, err := git.RevParse(ctx, f.dir, projection.Latest.CommitOID+":"+name)
		if err != nil || got != want {
			t.Fatalf("%s blob=%s want=%s err=%v", name, got, want, err)
		}
	}
	var captures []IntentCandidateCapture
	rows, err := f.db.ReadSQL().QueryContext(ctx, `SELECT seq,path FROM capture_events WHERE path LIKE '%_autocomplete.bin' ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var seq int64
		var name string
		if err := rows.Scan(&seq, &name); err != nil {
			t.Fatal(err)
		}
		captures = append(captures, IntentCandidateCapture{Event: state.CaptureEvent{Seq: seq, Path: name}})
	}
	rows.Close()
	for i := range captures {
		captures[i].Ops, err = state.LoadCaptureOps(ctx, f.db, captures[i].Event.Seq)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := attachIntentFileMetadata(ctx, f.dir, captures); err != nil {
		t.Fatal(err)
	}
	if len(captures) != 4 {
		t.Fatalf("captures=%d", len(captures))
	}
	for _, capture := range captures {
		if capture.FileMetadata.Kind != "binary" || capture.FileMetadata.AfterBytes != map[string]int64{names[0]: int64(sizes[0]), names[1]: int64(sizes[1]), names[2]: int64(sizes[2]), names[3]: int64(sizes[3])}[capture.Event.Path] || capture.CapturedDiff != "" {
			t.Fatalf("metadata=%+v", capture)
		}
	}
}

func TestCaptureResiliencePartialProtectionPreservesShadow(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires file permissions enforced for a non-root user")
	}
	f := newCaptureFixture(t)
	ctx := context.Background()
	store := checkpointpkg.Store{DB: f.db}
	blocked := filepath.Join(f.dir, "unreadable.go")
	if err := os.WriteFile(blocked, []byte("package fixture\n"), 0644); err != nil {
		t.Fatal(err)
	}
	opts := CaptureOpts{CheckpointStore: &store, IgnoreChecker: f.ig}
	complete, err := Capture(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(blocked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(blocked, 0644) })
	if err := os.WriteFile(filepath.Join(f.dir, "independent.md"), []byte("Independent documentation.\n"), 0644); err != nil {
		t.Fatal(err)
	}
	partial, err := Capture(ctx, f.dir, f.db, f.cctx, opts)
	if err == nil || !partial.Partial || partial.Protected || partial.CheckpointID == "" || partial.EventsAppended != 1 {
		t.Fatalf("partial=%+v err=%v", partial, err)
	}
	if _, err := state.ResolveCheckpoint(ctx, f.db.Path(), partial.CheckpointID); !errors.Is(err, state.ErrCheckpointPartial) {
		t.Fatalf("partial restore err=%v", err)
	}
	if _, err := state.ResolveCheckpoint(ctx, f.db.Path(), complete.CheckpointID); err != nil {
		t.Fatal(err)
	}
	for _, op := range pendingOps(t, f.db) {
		if op.Path == "unreadable.go" && op.Op == "delete" {
			t.Fatal("failed read became deletion")
		}
	}
	if hold, err := capturePublicationHold(ctx, f.db, []string{"independent.md"}, "Independent documentation."); err != nil || hold != "" {
		t.Fatalf("independent hold=%s err=%v", hold, err)
	}
	if hold, err := capturePublicationHold(ctx, f.db, []string{"dependent.go"}, ""); err != nil || hold == "" {
		t.Fatalf("dependent hold=%s err=%v", hold, err)
	}
	if hold, err := capturePublicationHold(ctx, f.db, []string{"related.md"}, "unreadable.go"); err != nil || hold == "" {
		t.Fatalf("reference hold=%s err=%v", hold, err)
	}
	repeat, err := Capture(ctx, f.dir, f.db, f.cctx, opts)
	if err == nil || repeat.CheckpointID != partial.CheckpointID || repeat.EventsAppended != 0 {
		t.Fatalf("repeat=%+v err=%v", repeat, err)
	}
	if err := os.Chmod(blocked, 0644); err != nil {
		t.Fatal(err)
	}
	recovered, err := Capture(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || !recovered.Protected {
		t.Fatalf("recovered=%+v err=%v", recovered, err)
	}
}

func TestCaptureResilienceLocalMessagesNeverCallAI(t *testing.T) {
	req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2, OfferedCaptures: []ai.OfferedCapture{{Seq: 1, Path: "assets/eng_autocomplete.bin", Op: "modify"}}}
	plan := deterministicIntentCandidatePlan(req, true, false)
	out, _, ready, err := applyIntentFallbackMessageQuality(context.Background(), nil, nil, req, plan, "offline")
	if err != nil || !ready || len(out.Candidates) != 1 || out.Candidates[0].Body == "" {
		t.Fatalf("local fallback=%+v ready=%t err=%v", out, ready, err)
	}
}
