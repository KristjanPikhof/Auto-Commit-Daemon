package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type partialCachedMessageProvider struct {
	calls      map[int64]int
	failSecond bool
}

func (p *partialCachedMessageProvider) Name() string { return "semantic-test" }
func (p *partialCachedMessageProvider) RewriteIntentMessage(_ context.Context, req ai.IntentMessageRewriteRequest) (ai.Result, error) {
	seq := req.LockedPlan.SelectedSeqs[0]
	p.calls[seq]++
	if seq == 2 && p.failSecond {
		return ai.Result{}, errors.New("temporary outage")
	}
	return ai.Result{Subject: "Preserve feature behavior", Body: "- Keep related changes coherent"}, nil
}
func TestPublicationMessageCacheRetainsPartialSuccessAcrossRestart(t *testing.T) {
	base := context.Background()
	path := filepath.Join(t.TempDir(), "cache.db")
	provider := &partialCachedMessageProvider{calls: map[int64]int{}, failSecond: true}
	req := ai.IntentPlanRequest{OfferedCaptures: []ai.OfferedCapture{{Seq: 1, Path: "feature.go", Op: "modify"}, {Seq: 2, Path: "other.go", Op: "modify"}}}
	run := func(change bool) {
		t.Helper()
		db, err := state.Open(base, path)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		ctx, cancel := context.WithCancel(base)
		defer cancel()
		ctx = context.WithValue(ctx, publicationEvaluationKey{}, &publicationEvaluation{db: db, cancel: cancel, identity: func(context.Context) (string, error) { return "fixed", nil }, protect: func(context.Context) error { return nil }})
		cache, err := loadPublicationMessageCache(ctx, provider, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = evaluatePublication(ctx, func(jobCtx context.Context) (bool, error) {
			for _, seq := range []int64{1, 2} {
				plan := ai.IntentPlan{SelectedSeqs: []int64{seq}, Subject: "Update file"}
				if change && seq == 1 {
					plan.GroupingReason = "changed immutable evidence"
				}
				request := ai.NewIntentMessageRewriteRequest(req, plan, ai.EvaluateIntentPlanMessageQuality(req, plan))
				if _, err := cache.RewriteIntentMessage(jobCtx, request); err != nil {
					return false, err
				}
			}
			return true, nil
		})
		if err != nil && !provider.failSecond {
			t.Fatal(err)
		}
		if err := cache.persist(ctx); err != nil {
			t.Fatal(err)
		}
	}
	run(false)
	provider.failSecond = false
	run(false)
	if provider.calls[1] != 1 || provider.calls[2] != 2 {
		t.Fatalf("cached successes not reused: calls=%v", provider.calls)
	}
	run(true)
	if provider.calls[1] != 2 || provider.calls[2] != 2 {
		t.Fatalf("exact request invalidation failed: calls=%v", provider.calls)
	}
}
