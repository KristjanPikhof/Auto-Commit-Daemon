package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type historyAuditPlanner struct {
	calls   int
	failure error
	onPlan  func()
}

func (*historyAuditPlanner) Name() string { return "history-audit-provider" }
func (*historyAuditPlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("expected goal planner")
}
func (p *historyAuditPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.calls++
	if p.onPlan != nil {
		p.onPlan()
	}
	if p.failure != nil {
		return ai.IntentPlanV2{}, p.failure
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for _, path := range []string{"a.txt", "b.txt"} {
		var seqs []int64
		for _, capture := range req.OfferedCaptures {
			if capture.Path == path {
				seqs = append(seqs, capture.Seq)
			}
		}
		plan.Candidates = append(plan.Candidates, ai.IntentCandidateAssignment{CandidateID: strings.TrimSuffix(path, ".txt"), SelectedSeqs: seqs, Purpose: "complete the " + path + " outcome", GroupingReason: "Keep the independent outcome and its available corrections together", Readiness: ai.IntentCandidateReady, Subject: "Complete " + path + " behavior", Body: "- Include the available corrections in one reviewable goal"})
	}
	return plan, nil
}

func TestIntentHistoryAuditDoesNotExtendExpiredRepairHorizon(t *testing.T) {
	f, planner, opts := newHistoryAuditFixture(t)
	planner.onPlan = func() {
		if _, err := f.repo.db.SQL().ExecContext(context.Background(), `UPDATE intent_candidates SET soft_publication_deadline=1 WHERE status='soft_published'`); err != nil {
			t.Fatal(err)
		}
	}
	result, err := MaybeRepairIntentHistory(context.Background(), f.repo.dir, f.repo.gitDir, f.repo.db, f.cctx, opts)
	if err != nil || result.Status != state.IntentRepairSkipped || result.Reason != "repair_horizon_expired" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if head, _ := git.RevParse(context.Background(), f.repo.dir, "HEAD"); head != f.oldA2 {
		t.Fatal("provider answer extended private rewrite horizon")
	}
	var reassigned int
	if err := f.repo.db.SQL().QueryRow(`SELECT COUNT(*) FROM intent_candidates WHERE id LIKE 'audit-%'`).Scan(&reassigned); err != nil || reassigned != 0 {
		t.Fatalf("expired reply reassigned capture ownership: count=%d err=%v", reassigned, err)
	}
}

func TestIntentHistoryAuditPreservesTypedSemanticFailure(t *testing.T) {
	failure := &IntentPlannerValidationFailure{Err: errors.New("recorded goals are incomplete")}
	classified := classifyIntentHistoryAuditFailure(failure)
	if kind, ok := classifyIntentPlannerFailure(classified); !ok || kind != IntentPlannerFailureValidation {
		t.Fatalf("semantic failure became provider transport outage: %v", classified)
	}
}

func TestIntentHistoryAuditDistinguishesPlanRejectionFromProviderConfiguration(t *testing.T) {
	for _, configuration := range []bool{false, true} {
		t.Run(fmt.Sprint(configuration), func(t *testing.T) {
			f, planner, opts := newHistoryAuditFixture(t)
			opts.IntentHealth = NewIntentPlannerHealth(context.Background(), f.repo.db, IntentPlannerHealthOptions{Provider: IntentPlannerProviderIdentity{Provider: "audit-provider"}})
			planner.failure = &ai.IntentPlanV2ValidationError{Message: "response has incomplete goals"}
			if configuration {
				planner.failure = &ai.ProviderHTTPError{StatusCode: 401, Detail: "credentials rejected"}
			}
			result, err := MaybeRepairIntentHistory(context.Background(), f.repo.dir, f.repo.gitDir, f.repo.db, f.cctx, opts)
			if result.Status != state.IntentRepairSkipped {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if configuration && !ai.ProviderNeedsConfiguration(err) {
				t.Fatalf("configuration was hidden as retry wait: %v", err)
			}
			if !configuration && err != nil {
				t.Fatal(err)
			}
			if snapshot := opts.IntentHealth.Snapshot(); snapshot.State != IntentPlannerCircuitClosed {
				t.Fatalf("reachable provider consumed outage backoff: %+v", snapshot)
			}
			var record intentHistoryAuditRecord
			if _, err := state.MetaGetJSON(context.Background(), f.repo.db, metaIntentHistoryAudit, &record); err != nil {
				t.Fatal(err)
			}
			if configuration && (record.Outcome != "needs_attention" || record.NextAttemptTS != 0) {
				t.Fatalf("configuration outcome=%+v", record)
			}
			if !configuration && (record.Outcome != "planning_wait" || record.NextAttemptTS == 0) {
				t.Fatalf("semantic plan outcome=%+v", record)
			}
		})
	}
}

func newHistoryAuditFixture(t *testing.T) (noncontiguousIntentRepairFixture, *historyAuditPlanner, ReplayOpts) {
	t.Helper()
	f := newNoncontiguousIntentRepairFixture(t)
	ctx := context.Background()
	tree, err := git.RevParse(ctx, f.repo.dir, f.oldA2+"^{tree}")
	if err != nil {
		t.Fatal(err)
	}
	parent := f.oldB1
	newHead, err := git.CommitTree(ctx, f.repo.dir, tree, "Update alpha code changes", parent)
	if err != nil {
		t.Fatal(err)
	}
	if err := git.UpdateRef(ctx, f.repo.dir, f.cctx.BranchRef, newHead, f.oldA2); err != nil {
		t.Fatal(err)
	}
	if _, err := f.repo.db.SQL().ExecContext(ctx, `UPDATE capture_events SET commit_oid=? WHERE commit_oid=?`, newHead, f.oldA2); err != nil {
		t.Fatal(err)
	}
	if _, err := f.repo.db.SQL().ExecContext(ctx, `UPDATE intent_candidates SET published_commit_oid=? WHERE published_commit_oid=?`, newHead, f.oldA2); err != nil {
		t.Fatal(err)
	}
	if _, err := f.repo.db.SQL().ExecContext(ctx, `UPDATE intent_candidates SET soft_publication_deadline=? WHERE status='soft_published'`, float64(time.Now().Add(30*time.Minute).UnixNano())/1e9); err != nil {
		t.Fatal(err)
	}
	f.oldA2 = newHead
	f.cctx.BaseHead = newHead
	planner := &historyAuditPlanner{}
	opts := ReplayOpts{CommitStrategy: ai.CommitStrategyIntent, CommitFormat: ai.CommitFormatImperative, IntentPlanner: planner, IntentRepairEnabled: true, IntentRepairMaxCommits: 3, IntentIncludeDiffs: true}
	return f, planner, opts
}

func TestIntentHistoryAuditRepairsPrivateQualityDebtOnce(t *testing.T) {
	f, planner, opts := newHistoryAuditFixture(t)
	ctx := context.Background()
	result, err := MaybeRepairIntentHistory(ctx, f.repo.dir, f.repo.gitDir, f.repo.db, f.cctx, opts)
	if err != nil || result.Status != state.IntentRepairCompleted || result.NewHead == f.oldA2 {
		t.Fatalf("result=%+v err=%v provider calls=%d", result, err, planner.calls)
	}
	if planner.calls != 1 {
		t.Fatalf("provider calls=%d", planner.calls)
	}
	if got := strings.Fields(mustGitOutput(t, f.repo.dir, "rev-list", "--first-parent", fmt.Sprintf("%s..HEAD", f.repo.head))); len(got) != 2 {
		t.Fatalf("did not reconstruct two completed goals: %v", got)
	}
	f.cctx.BaseHead = result.NewHead
	for range 3 {
		if _, err := MaybeRepairIntentHistory(ctx, f.repo.dir, f.repo.gitDir, f.repo.db, f.cctx, opts); err != nil {
			t.Fatal(err)
		}
	}
	if planner.calls != 1 {
		t.Fatalf("unchanged successful history was replanned: calls=%d", planner.calls)
	}
}

func TestIntentHistoryAuditKeepsSharedHistoryAndRetriesProviderOutage(t *testing.T) {
	for _, name := range []string{"shared history", "provider outage"} {
		t.Run(name, func(t *testing.T) {
			f, planner, opts := newHistoryAuditFixture(t)
			ctx := context.Background()
			if name == "shared history" {
				if err := git.UpdateRef(ctx, f.repo.dir, "refs/remotes/origin/shared", f.oldA2, ""); err != nil {
					t.Fatal(err)
				}
			} else {
				planner.failure = &IntentPlannerTransportFailure{Err: errors.New("HTTP 502")}
			}
			result, err := MaybeRepairIntentHistory(ctx, f.repo.dir, f.repo.gitDir, f.repo.db, f.cctx, opts)
			if err != nil || result.Status != state.IntentRepairSkipped {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			head, _ := git.RevParse(ctx, f.repo.dir, "HEAD")
			if head != f.oldA2 {
				t.Fatal("refused/waiting audit moved HEAD")
			}
			if name == "shared history" {
				if planner.calls != 0 {
					t.Fatal("shared history reached provider")
				}
				return
			}
			var record intentHistoryAuditRecord
			if ok, err := state.MetaGetJSON(ctx, f.repo.db, metaIntentHistoryAudit, &record); err != nil || !ok || record.NextAttemptTS <= float64(time.Now().Unix()) {
				t.Fatalf("retry record=%+v err=%v", record, err)
			}
			before := planner.calls
			if _, err := MaybeRepairIntentHistory(ctx, f.repo.dir, f.repo.gitDir, f.repo.db, f.cctx, opts); err != nil {
				t.Fatal(err)
			}
			if planner.calls != before {
				t.Fatal("provider outage caused an unchanged-evidence busy loop")
			}
			// Crossing the durable retry milestone permits another real attempt.
			record.NextAttemptTS = float64(time.Now().Add(-time.Minute).Unix())
			if err := state.MetaSetJSON(ctx, f.repo.db, metaIntentHistoryAudit, record); err != nil {
				t.Fatal(err)
			}
			planner.failure = nil
			result, err = MaybeRepairIntentHistory(ctx, f.repo.dir, f.repo.gitDir, f.repo.db, f.cctx, opts)
			if err != nil || result.Status != state.IntentRepairCompleted {
				t.Fatalf("provider return did not recover: %+v err=%v", result, err)
			}
		})
	}
}
