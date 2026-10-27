package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishWorkerRetryPreviewRetainsAcceptedReviews(t *testing.T) {
	ctx := t.Context()
	f, w := finishWorkerFixture(t)
	large := strings.Repeat("Complete recorded library value\n", 22000)
	finishWrite(t, f.dir, "library.txt", large)
	finishWrite(t, f.dir, "consumer.txt", "Use the completed library\n")
	head := finishWorkerGit(t, f.dir, "rev-parse", "HEAD")
	w.Config.Mode = "intent"
	w.Config.Provider.DiffEgress = true
	w.Config.Provider.CommitStrategy = ai.CommitStrategyIntent
	w.Config.Provider.Timeout = time.Minute
	var reviews, plans atomic.Int32
	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		if request.Review != nil {
			reviews.Add(1)
			var observations []map[string]string
			for _, part := range request.Review.Parts {
				observations = append(observations, map[string]string{"id": part.ID, "summary": "Preserve the complete library and its consumer."})
			}
			return json.Marshal(map[string]any{"observations": observations})
		}
		if plans.Add(1) == 1 {
			return nil, errors.New("provider unavailable after complete evidence review")
		}
		return completeFinishTestPlan(request), nil
	}}
	r := startFinishWorker(t, w, FinishOptions{Purpose: "Publish the completed library and consumer"})
	failed, err := w.Execute(ctx, r.ID)
	if err == nil || failed.State != state.FinishFailed || len(failed.ReviewProgress.Observations) == 0 || reviews.Load() < 2 {
		t.Fatalf("failed planning lost accepted reviews: %+v %v", failed, err)
	}
	acceptedReviews := reviews.Load()
	if finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != head {
		t.Fatal("failed planning moved the branch")
	}

	later := "Later library edits remain outside the frozen request\n"
	finishWrite(t, f.dir, "library.txt", later)
	finishWrite(t, f.dir, "later.txt", "Keep this new file for the next finish\n")
	wt, cfg, _, err := w.resolved(ctx)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := w.protect(ctx, wt, cfg, false)
	if err != nil || checkpoint.TreeOID == r.Target.TargetTree {
		t.Fatalf("later edits were not protected separately: %+v %v", checkpoint, err)
	}

	preview, created, err := w.Start(ctx, FinishOptions{RetryID: r.ID, DryRun: true})
	if err != nil || !created || !reflect.DeepEqual(preview.Target, r.Target) {
		t.Fatalf("preview changed the frozen request: %+v %v", preview, err)
	}
	previewed, err := w.Execute(ctx, r.ID)
	if err != nil || previewed.State != state.FinishCompleted || reviews.Load() != acceptedReviews || previewed.ReusedFragments != len(failed.ReviewProgress.Observations) {
		t.Fatalf("preview repeated accepted evidence: %+v reviews=%d %v", previewed, reviews.Load(), err)
	}
	if previewed.ReviewProgress.Fingerprint != failed.ReviewProgress.Fingerprint {
		t.Fatal("completed preview lost the keyed review journal")
	}
	if finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != head {
		t.Fatal("preview published branch commits")
	}

	retry, created, err := w.Start(ctx, FinishOptions{RetryID: r.ID})
	if err != nil || !created || !reflect.DeepEqual(retry.Target, r.Target) {
		t.Fatalf("publication retry changed the frozen request: %+v %v", retry, err)
	}
	completed, err := w.Execute(ctx, r.ID)
	if err != nil || completed.State != state.FinishCompleted || reviews.Load() != acceptedReviews || completed.EvidenceCalls != 0 || completed.PlanningCalls != 1 || completed.ReusedFragments != len(failed.ReviewProgress.Observations) || plans.Load() != 3 {
		t.Fatalf("publication repeated accepted evidence: %+v reviews=%d plans=%d %v", completed, reviews.Load(), plans.Load(), err)
	}
	if finishWorkerGit(t, f.dir, "rev-parse", "HEAD^{tree}") != r.Target.TargetTree {
		t.Fatal("publication did not match the frozen tree")
	}
	body, err := os.ReadFile(filepath.Join(f.dir, "library.txt"))
	if err != nil || string(body) != later {
		t.Fatal("publication overwrote later library edits", err)
	}
	if !finishProtectionHas(t, f, "library.txt", later) || !finishProtectionHas(t, f, "later.txt", "Keep this new file for the next finish\n") {
		t.Fatal("publication lost protection of later edits")
	}
	if finishWorkerGit(t, f.dir, "diff", "--cached", "--name-only") != "" {
		t.Fatal("publication created staged changes")
	}
}
