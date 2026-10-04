package daemon

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
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

func TestCaptureResilienceNonRegularDoesNotBlockProtection(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	store := checkpointpkg.Store{DB: f.db}
	opts := CaptureOpts{CheckpointStore: &store, IgnoreChecker: f.ig}
	changed := filepath.Join(f.dir, "replaced.txt")
	if err := os.WriteFile(changed, []byte("protected regular file\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, opts); err != nil {
		t.Fatal(err)
	}
	before, err := os.Lstat(changed)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(changed); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(changed, 0644); err != nil {
		t.Fatal(err)
	}
	// A stale regular-file candidate must reject the FIFO without waiting
	// for another process to open it.
	if _, ok, reason, err := hashCandidate(ctx, f.dir, candidateLike{rel: "replaced.txt", full: changed, fi: before}, walkOpts{}); err != nil || ok || reason != "unstable" {
		t.Fatalf("replaced candidate ok=%t reason=%q err=%v", ok, reason, err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "independent.go"), []byte("package fixture\n"), 0644); err != nil {
		t.Fatal(err)
	}
	summary, err := Capture(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || !summary.Protected || summary.Partial || summary.EventsAppended != 1 {
		t.Fatalf("non-regular exclusion=%+v err=%v", summary, err)
	}
	for _, op := range pendingOps(t, f.db) {
		if op.Path == "replaced.txt" && op.Op == "delete" {
			t.Fatal("non-regular replacement became deletion")
		}
	}
	if hold, err := capturePublicationHold(ctx, f.db, []string{"independent.go"}, ""); err != nil || hold != "" {
		t.Fatalf("non-regular exclusion blocks publication: %q %v", hold, err)
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
	indexBefore, err := git.LsFilesStaged(ctx, f.dir, "README.md")
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir})
	if err != nil || replayed.Published != 1 {
		t.Fatalf("partial replay=%+v err=%v", replayed, err)
	}
	indexAfter, err := git.LsFilesStaged(ctx, f.dir, "README.md")
	if err != nil || !reflect.DeepEqual(indexBefore, indexAfter) {
		t.Fatalf("live index changed: %v", err)
	}
	if got, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "show", "HEAD:independent.md"); err != nil || string(got) != "Independent documentation.\n" {
		t.Fatalf("independent publication=%q err=%v", got, err)
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
	out, err := applyIntentFallbackMessageQuality(req, plan)
	if err != nil || len(out.Candidates) != 1 || out.Candidates[0].Body == "" {
		t.Fatalf("local fallback=%+v err=%v", out, err)
	}
}

func TestCaptureResilienceProviderBudgetStopsUnchangedCalls(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	planner := &blockingSemanticPlanner{entered: make(chan ai.IntentPlanRequestV2, 1)}
	req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2, OfferedCaptures: []ai.OfferedCapture{{Seq: 1, Path: "independent.md", Op: "create"}}}
	input := IntentCandidateEvaluation{BranchRef: "refs/heads/main", BranchGeneration: 1, ProviderBudget: 200 * time.Millisecond}
	started := time.Now()
	plan, fallback, _, _, _, _, first, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 2, config.PresetFast, nil, db, input)
	if err != nil || fallback != "evidence_partition" || len(plan.Candidates) != 1 || !first.Completed {
		t.Fatalf("budget result=%+v fallback=%s err=%v", first, fallback, err)
	}
	if time.Since(started) > 5*time.Second {
		t.Fatal("provider budget did not bound wait")
	}
	if len(planner.entered) != 1 {
		t.Fatal("provider call was not observed")
	}
	_, _, _, _, _, _, second, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 2, config.PresetFast, nil, db, input)
	if err != nil || second.ResolutionMode.String != "completed_plan_reuse" || len(planner.entered) != 1 || second.ProviderDeadlineTS != first.ProviderDeadlineTS {
		t.Fatalf("budget reuse=%+v err=%v", second, err)
	}
}

func TestCaptureResilienceUnreadableRootIsUnknownScope(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires enforced file permissions")
	}
	f := newCaptureFixture(t)
	ctx := context.Background()
	store := checkpointpkg.Store{DB: f.db}
	opts := CaptureOpts{CheckpointStore: &store, IgnoreChecker: f.ig}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, opts); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(f.dir, 0111); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(f.dir, 0755) })
	partial, err := Capture(ctx, f.dir, f.db, f.cctx, opts)
	if err == nil || !partial.Partial || partial.Protected || partial.EventsAppended != 0 {
		t.Fatalf("unknown root=%+v err=%v", partial, err)
	}
	issues, err := state.CurrentCaptureIssues(ctx, f.db)
	if err != nil || len(issues) != 1 || issues[0].Path != "" || !issues[0].Subtree {
		t.Fatalf("unknown scope=%+v err=%v", issues, err)
	}
	if hold, err := capturePublicationHold(ctx, f.db, []string{"independent.md"}, ""); err != nil || hold == "" {
		t.Fatalf("unknown scope permits publication: %q %v", hold, err)
	}
}

type interruptedPartialPlanner struct {
	partialReplanIntentCandidatePlannerStub
	cancel context.CancelFunc
}

func (p *interruptedPartialPlanner) PlanIntentV2(ctx context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	if p.calls == 1 {
		p.cancel()
		return ai.IntentPlanV2{}, ctx.Err()
	}
	return p.partialReplanIntentCandidatePlannerStub.PlanIntentV2(ctx, req)
}

func TestCaptureResiliencePreservesValidatedMessagesAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := state.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	planner := &interruptedPartialPlanner{cancel: cancel}
	req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2, OfferedCaptures: []ai.OfferedCapture{{Seq: 1, Path: "a.go", Op: "create"}, {Seq: 2, Path: "b.go", Op: "create"}}}
	input := IntentCandidateEvaluation{BranchRef: "refs/heads/main", BranchGeneration: 1, Provider: planner.Name()}
	if _, _, _, _, _, _, _, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 2, config.PresetFast, nil, db, input); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted correction err=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = state.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	restarted := &partialReplanIntentCandidatePlannerStub{calls: 1}
	plan, _, _, _, _, _, run, err := chooseIntentCandidatePlan(context.Background(), req, restarted, nil, 2, config.PresetFast, nil, db, input)
	if err != nil || run.ResolutionMode.String != "partial_replan" || len(plan.Candidates) != 2 || len(restarted.reqs) != 1 || len(restarted.reqs[0].OfferedCaptures) != 1 {
		t.Fatalf("restart groups=%+v run=%+v req=%+v err=%v", plan, run, restarted.reqs, err)
	}
	if plan.Candidates[0].CandidateID != "locked-a" || plan.Candidates[0].Subject != "Update source change" {
		t.Fatalf("validated message changed: %+v", plan.Candidates)
	}
}
