package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishRecoveryAfterBranchAdvancePreservesCurrentWork(t *testing.T) {
	for _, scenario := range []string{"recover", "moved_chain", "missing_checkpoint", "changed_request"} {
		t.Run(scenario, func(t *testing.T) {
			rec, db, worker := finishStatusFixture(t)
			finishStatusWrite(t, rec.Path, "goal.txt", "finished work")
			r, _, err := worker.Start(t.Context(), daemon.FinishOptions{Purpose: "Complete goal"})
			if err != nil {
				t.Fatal(err)
			}
			r, err = worker.Execute(t.Context(), r.ID)
			if err != nil {
				t.Fatal(err)
			}
			p, err := state.LoadFinishPublication(t.Context(), db, r.ID)
			if err != nil {
				t.Fatal(err)
			}
			// Simulate interruption after the branch update, before journal
			// completion. Later commits and a branch switch happen before restart.
			partial, _ := json.Marshal(state.FinishPublicationResult{Published: true, NewHead: p.NewHead, CommitsCreated: 1})
			if _, err := db.SQL().ExecContext(t.Context(), "UPDATE finish_requests SET state='prepared',result_json=? WHERE request_id=?", string(partial), r.ID); err != nil {
				t.Fatal(err)
			}
			finishStatusWrite(t, rec.Path, "successor.txt", "later commit")
			finishStatusGit(t, rec.Path, "add", "successor.txt")
			finishStatusGit(t, rec.Path, "commit", "-m", "Add successor")
			finishStatusGit(t, rec.Path, "switch", "-c", "next-task")
			finishStatusWrite(t, rec.Path, "staged.txt", "new staging")
			finishStatusGit(t, rec.Path, "add", "staged.txt")
			finishStatusWrite(t, rec.Path, "goal.txt", "later unsaved edit")
			r, err = worker.RecoverPrepared(t.Context())
			if err == nil || r.State != state.FinishNeedsAttention {
				t.Fatalf("restart: %+v: %v", r, err)
			}
			// Pending protection used to hide this blocker from all next steps.
			if _, err := daemon.BeginProtectionObservation(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			report := finishStatusRead(t, rec, db)
			if report.Protected || !report.PublicationOutcome.Finish.Published {
				t.Fatalf("publication lost or pending scan hidden: %+v", report.PublicationOutcome)
			}
			var control controlResult
			applyControlStatusWithDaemonAlive(&control, report, true)
			if control.OK || !strings.Contains(control.NextAction, "recover --force --dry-run") {
				t.Fatalf("missing recovery: %+v", control)
			}
			var doctor doctorRepoReport
			if !readRepoState(t.Context(), &doctor, rec.Path, rec.StateDB) || !strings.Contains(strings.Join(doctor.Notes, " "), "recover --force --dry-run") {
				t.Fatalf("doctor lost blocker: %+v", doctor)
			}
			diagnosis, err := buildDiagnoseReport(t.Context(), rec)
			if err != nil || !strings.Contains(strings.Join(diagnosis.Remediation, " "), "recover --force --dry-run") {
				t.Fatalf("diagnose lost blocker: %+v: %v", diagnosis, err)
			}
			var output bytes.Buffer
			if err := renderProductDiagnoseReport(&output, diagnosis); err != nil {
				t.Fatal(err)
			}
			var envelope productEnvelope
			if err := json.Unmarshal(output.Bytes(), &envelope); err != nil || envelope.State != productStateNeedsAction || envelope.NextAction == nil {
				t.Fatalf("diagnostic JSON hid blocker: %s: %v", output.String(), err)
			}
			// The real mutation path owns the canonical lock; this fixture has
			// no worker process, so mark its supporting liveness record stopped.
			if err := state.SaveDaemonState(t.Context(), db, state.DaemonState{Mode: "stopped"}); err != nil {
				t.Fatal(err)
			}
			head := finishStatusGit(t, rec.Path, "rev-parse", "HEAD")
			indexPath := filepath.Join(rec.Path, ".git", "index")
			index, _ := os.ReadFile(indexPath)
			before, _ := fileSHA256(rec.StateDB)
			plan, err := buildFixPlan(t.Context(), rec.Path, rec.StateDB, true, false, false)
			if err != nil || len(plan.Unsafe) == 0 || !plan.ForceRequired {
				t.Fatalf("non-force preview: %+v: %v", plan, err)
			}
			plan, err = buildFixPlan(t.Context(), rec.Path, rec.StateDB, true, true, false)
			if err != nil || findFixAction(plan, fixActionPreserveFinish) == nil {
				t.Fatalf("force preview: %+v: %v", plan, err)
			}
			after, _ := fileSHA256(rec.StateDB)
			if before != after {
				t.Fatal("preview changed saved state")
			}
			cp, err := state.ResolveCheckpoint(t.Context(), rec.StateDB, r.Target.CheckpointID)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "moved_chain":
				finishStatusGit(t, rec.Path, "update-ref", p.ObjectRef, head)
			case "missing_checkpoint":
				finishStatusGit(t, rec.Path, "update-ref", "-d", cp.Ref)
			case "changed_request":
				plan.afterBackup = func() {
					if _, err := db.SQL().ExecContext(t.Context(), "UPDATE finish_requests SET reason='new evidence' WHERE request_id=?", r.ID); err != nil {
						t.Fatal(err)
					}
				}
			}
			plan.DryRun = false
			err = applyFixPlan(t.Context(), rec.StateDB, &plan)
			if scenario != "recover" {
				if err == nil {
					t.Fatal("unproven recovery accepted")
				}
				active, loadErr := state.ActiveFinishRequest(t.Context(), db, r.Target.WorktreeID)
				if loadErr != nil || active.State != state.FinishNeedsAttention {
					t.Fatalf("blocker discarded: %+v: %v", active, loadErr)
				}
			} else {
				if err != nil || plan.RowsChanged != 1 || plan.BackupPath == "" {
					t.Fatalf("apply: %+v: %v", plan, err)
				}
				saved, err := state.LoadFinishRequest(t.Context(), db, r.ID)
				if err != nil || saved.State != state.FinishFailed || saved.ReasonCode != "finish_preserved" || !reflect.DeepEqual(saved.Target, r.Target) || !bytes.Equal(saved.Publication, r.Publication) || !bytes.Equal(saved.Result, r.Result) {
					t.Fatalf("lost immutable evidence: %+v: %v", saved, err)
				}
				if _, err := worker.RecoverPrepared(t.Context()); err != nil {
					t.Fatalf("restart reopened preserved finish: %v", err)
				}
				if _, err := state.RetryFinishRequest(t.Context(), db, r.ID, float64(time.Now().Unix()+300), false); !errors.Is(err, state.ErrFinishState) {
					t.Fatalf("preserved publication retried: %v", err)
				}
				if finishStatusGit(t, rec.Path, "rev-parse", p.ObjectRef) != p.NewHead {
					t.Fatal("private chain lost")
				}
			}
			currentIndex, _ := os.ReadFile(indexPath)
			body, _ := os.ReadFile(filepath.Join(rec.Path, "goal.txt"))
			if !bytes.Equal(index, currentIndex) || string(body) != "later unsaved edit" || finishStatusGit(t, rec.Path, "rev-parse", "HEAD") != head || finishStatusGit(t, rec.Path, "branch", "--show-current") != "next-task" {
				t.Fatal("recovery changed current Git or worktree state")
			}
			if scenario == "recover" {
				next, created, err := worker.Start(t.Context(), daemon.FinishOptions{Purpose: "Complete next task", DryRun: true, IncludeStaged: true})
				if err != nil || !created || next.ID == r.ID {
					t.Fatalf("new finish still blocked: %+v: %v", next, err)
				}
			}
		})
	}
}
