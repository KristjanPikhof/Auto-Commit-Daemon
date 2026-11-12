package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestFinishCLIShowsCheckpointScanAndCommitProgress(t *testing.T) {
	lookup, _, _ := finishCLIFixture(t)
	var poll atomic.Int32
	serveIntentHistoryWorker(t, lookup, controlTestRPC(func(ctx context.Context, req supervisor.Request) (any, *supervisor.ProtocolError) {
		r := finishWorkerResponse{FinishRequest: state.FinishRequest{ID: "finish-progress", State: state.FinishRunning, Stage: "preparing", Target: state.FinishTarget{Mode: "intent"}}}
		if req.Method == "finish_start" {
			poll.Store(0)
			supervisor.ReportWorkerProgress(ctx, daemon.FinishCheckpointProgress{})
			supervisor.ReportWorkerProgress(ctx, daemon.FinishCheckpointProgress{Scanned: 256, Kept: 12})
			supervisor.ReportWorkerProgress(ctx, daemon.FinishCheckpointProgress{Scanned: 512, Kept: 18})
			supervisor.ReportWorkerProgress(ctx, daemon.FinishCheckpointProgress{Scanned: 600, Kept: 19, Complete: true})
			return r, nil
		}
		switch poll.Add(1) {
		case 1:
			r.Stage = "reviewing_evidence"
			r.ReviewedFragments, r.ReviewFragments, r.ReusedFragments = 128, 801, 64
		case 2:
			r.Stage = "planning"
			r.PlanningCalls = 1
		case 3:
			r.Stage = "verifying"
			r.Checking = &finishCheckProgress{Commit: 2, Total: 2}
		case 4:
			r.State, r.Stage = state.FinishPrepared, "publishing"
			r.Publication = json.RawMessage(`{"commits":[{},{}]}`)
		default:
			r.State, r.Stage = state.FinishCompleted, ""
			r.Publication = json.RawMessage(`{"commits":[{},{}]}`)
			r.Result = json.RawMessage(`{"published":true,"commits_created":2}`)
		}
		return r, nil
	}))

	for _, mode := range []string{"human", "quiet", "json"} {
		t.Run(mode, func(t *testing.T) {
			var out, progress bytes.Buffer
			if err := runFinish(t.Context(), &out, &progress, lookup.Worktree.Root, finishStartParams{}, mode == "json", mode == "quiet"); err != nil {
				t.Fatal(err)
			}
			if mode == "json" && !json.Valid(out.Bytes()) {
				t.Fatalf("JSON output was interrupted by progress: %q", out.String())
			}
			if mode != "human" {
				if progress.Len() > 0 {
					t.Fatalf("%s mode printed progress: %q", mode, progress.String())
				}
				return
			}
			remaining := progress.String()
			for _, want := range []string{
				"Scanning saved checkpoints.", "Scanned 256 checkpoints.", "Scanned 512 checkpoints.",
				"Scanned 600 checkpoints and kept 19 intermediate versions for planning.",
				"Preparing saved changes for Intent.", "Reviewing saved changes (128 of 801 fragments, 64 reused).",
				"Waiting for Intent to propose commits.", "Checking commit 2 of 2.", "Publishing 2 verified commits to your branch.",
			} {
				_, after, found := strings.Cut(remaining, want)
				if !found {
					t.Fatalf("missing progress in order %q:\n%s", want, progress.String())
				}
				remaining = after
			}
		})
	}
}

func TestFinishProgressShowsLongWaitWithoutDuplicateUpdates(t *testing.T) {
	var out bytes.Buffer
	display := finishProgress{out: &out}
	now := time.Unix(1800000000, 0)
	r := finishWorkerResponse{FinishRequest: state.FinishRequest{Stage: "planning", PlanningCalls: 1, DeadlineTS: float64(now.Add(2 * time.Minute).Unix())}}
	display.update(r, now)
	display.update(r, now.Add(time.Second))
	display.update(r, now.Add(finishProgressInterval))
	if got := out.String(); got != "Waiting for Intent to propose commits (2m remaining).\nWaiting for Intent to propose commits (1m 30s remaining).\n" {
		t.Fatalf("unexpected progress during provider wait: %q", got)
	}
}
