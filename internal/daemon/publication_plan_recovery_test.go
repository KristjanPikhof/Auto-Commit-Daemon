package daemon

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestPublicationDrainUnknownDependencyRestartsProtectedPlan(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := t.TempDir()
	initPublicationDrainTestRepo(t, ctx, repo)
	head := commitSingleFile(t, ctx, repo, "", "owned.txt", "base\n", "Seed baseline")
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "update-ref", "refs/heads/main", head); err != nil {
		t.Fatal(err)
	}
	db, events, drain := openPublicationDrainTestState(t, 3, 2)
	if _, err := db.SQL().ExecContext(ctx, `UPDATE checkpoints SET observed_head=? WHERE id=?`, head, drain.CheckpointID); err != nil {
		t.Fatal(err)
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: "worker-signals", SelectedSeqs: []int64{events[0].Seq}, DependsOnCandidates: []string{"discarded-baseline"},
		Purpose: "Preserve worker lifecycle signals", Subject: "Preserve worker lifecycle signals", Readiness: ai.IntentCandidateReady,
		GroupingReason: "The capture completes lifecycle signal handling",
	}}}
	validation := ai.ValidateIntentPlanV2(ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		OfferedCaptures: []ai.OfferedCapture{{Seq: events[0].Seq}}}, plan)
	if !intentPlanHasUnknownCandidateDependency(validation) {
		t.Fatalf("wrong fixture failure: %v", validation)
	}
	run, err := state.EnsureIntentPlanRun(ctx, db, state.IntentPlanRun{Fingerprint: "sha256:plan-dependency", BranchRef: drain.BranchRef,
		BranchGeneration: drain.BranchGeneration, AttemptLimit: 3})
	if err != nil {
		t.Fatal(err)
	}
	run.AttemptCount = 3
	run.PreservedGroups = intentAssignmentMembership(plan.Candidates)
	run.UnresolvedSeqs = append([]int64(nil), drain.EventSeqs...)
	run.ResolutionMode = sql.NullString{String: "waiting_semantic_retry", Valid: true}
	run.ProgressState = sql.NullString{String: "refining", Valid: true}
	if err := storeResolvedIntentPlanRun(&run, plan, nil); err != nil {
		t.Fatal(err)
	}
	if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(time.Second)
	if _, err := db.SQL().ExecContext(ctx, `UPDATE publication_drains SET phase='needs_action',last_error=?,reason_code='publication_failed',
 commit_strategy='intent',updated_ts=? WHERE id=?`, validation.Error(), intentPlannerHealthTimestamp(now), drain.ID); err != nil {
		t.Fatal(err)
	}
	path := db.Path()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = state.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	drain, err = state.PublicationDrainByID(ctx, db, drain.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"different-error", "outside-target", "wrong-pair", "unprotected", "terminal", "active-operation", "existing-prerequisite", "external-head"} {
		t.Run(scenario, func(t *testing.T) {
			blocked := drain
			switch scenario {
			case "different-error":
				blocked.LastError = "intent planner v2: candidate_dependency_unknown: depends_on candidate \"another-baseline\" is unknown"
			case "outside-target":
				blocked.EventSeqs = []int64{events[1].Seq, events[2].Seq}
			case "wrong-pair":
				blocked.BranchGeneration++
			case "unprotected":
				if _, err := db.SQL().ExecContext(ctx, `UPDATE checkpoints SET retained=0 WHERE id=?`, drain.CheckpointID); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_, _ = db.SQL().ExecContext(ctx, `UPDATE checkpoints SET retained=1 WHERE id=?`, drain.CheckpointID)
				}()
			case "terminal":
				if _, err := db.SQL().ExecContext(ctx, `UPDATE capture_events SET state='failed' WHERE seq=?`, events[0].Seq); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_, _ = db.SQL().ExecContext(ctx, `UPDATE capture_events SET state='pending' WHERE seq=?`, events[0].Seq)
				}()
			case "active-operation":
				if _, err := db.SQL().ExecContext(ctx, `INSERT INTO operations(id,kind,worktree_id,phase,status,created_ts,updated_ts)
VALUES('ongoing-recovery','recovery',?,'prepared','prepared',1,1)`, drain.WorktreeID); err != nil {
					t.Fatal(err)
				}
				defer func() { _, _ = db.SQL().ExecContext(ctx, `DELETE FROM operations WHERE id='ongoing-recovery'`) }()
			case "existing-prerequisite":
				if _, err := db.SQL().ExecContext(ctx, `INSERT INTO intent_candidates(id,branch_ref,branch_generation,status,created_ts,updated_ts)
VALUES('discarded-baseline',?,?,'waiting',1,1)`, drain.BranchRef, drain.BranchGeneration); err != nil {
					t.Fatal(err)
				}
				defer func() {
					_, _ = db.SQL().ExecContext(ctx, `DELETE FROM intent_candidates WHERE id='discarded-baseline'`)
				}()
			case "external-head":
				external := commitSingleFile(t, ctx, repo, head, "external.txt", "later\n", "External branch movement")
				if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "update-ref", "refs/heads/main", external); err != nil {
					t.Fatal(err)
				}
				defer func() { _, _ = gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "update-ref", "refs/heads/main", head) }()
			}
			resumed, err := RecoverUnknownIntentDependencyPublicationDrain(ctx, repo, db, blocked, now)
			if err != nil || resumed != nil {
				t.Fatalf("unproved recovery=%+v err=%v", resumed, err)
			}
		})
	}
	resumed, err := RecoverUnknownIntentDependencyPublicationDrain(ctx, repo, db, drain, now)
	if err != nil || resumed == nil || resumed.Phase != state.PublicationDrainSemantic || !reflect.DeepEqual(resumed.EventSeqs, drain.EventSeqs) {
		t.Fatalf("protected restart did not replan: %+v err=%v", resumed, err)
	}
	if pending, err := state.PendingEvents(ctx, db, 0); err != nil || len(pending) != 3 {
		t.Fatalf("later capture was consumed: %+v err=%v", pending, err)
	}
	if current, err := gitpkg.RevParse(ctx, repo, "HEAD"); err != nil || current != head {
		t.Fatalf("recovery moved HEAD: %s err=%v", current, err)
	}
	if intentPlanHasUnknownCandidateDependency(errors.Join(validation, errors.New("ambiguous Git state"))) {
		t.Fatal("joined Git ambiguity became a semantic wait")
	}
}
