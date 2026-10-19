package cli

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

func finishStatusFixture(t *testing.T) (central.RepoRecord, *state.DB, *daemon.FinishWorker) {
	t.Helper()
	roots := withIsolatedHome(t)
	repo, path, db := makeSeededRepoStateDB(t)
	registerProtectedControlRepo(t, roots, repo)
	w := &daemon.FinishWorker{RepoPath: repo, DB: db, ProtectionGate: &sync.Mutex{}, Config: daemon.FinishWorkerConfig{Mode: "local", Style: "imperative", ConfigDigest: "approved-status-fixture", AuthorName: "Status Author", AuthorEmail: "status@example.com", VerificationMode: "none", VerificationTimeout: time.Minute}}
	return central.RepoRecord{Path: repo, StateDB: path, RepoHash: central.CanonicalID(repo), RepositoryID: central.CanonicalID(repo), WorktreeID: checkpoint.WorktreeID(repo)}, db, w
}
func finishStatusWrite(t *testing.T, repo, path, body string) {
	t.Helper()
	full := filepath.Join(repo, path)
	if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}
func finishStatusGit(t *testing.T, repo string, args ...string) string {
	t.Helper()
	raw, err := git.Run(t.Context(), git.RunOpts{Dir: repo}, args...)
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(raw))
}
func finishStatusCheckpoint(t *testing.T, rec central.RepoRecord, db *state.DB, options ...daemon.CaptureOpts) daemon.CaptureSummary {
	t.Helper()
	ctx := t.Context()
	checker := git.NewIgnoreChecker(rec.Path)
	defer checker.Close()
	store := checkpoint.Store{DB: db}
	epoch, err := daemon.BeginProtectionObservation(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	head, _ := git.RevParse(ctx, rec.Path, "HEAD")
	opts := daemon.CaptureOpts{}
	if len(options) > 0 {
		opts = options[0]
	}
	opts.CheckpointStore = &store
	opts.IgnoreChecker = checker
	opts.ObservationEpoch = epoch
	opts.WorktreeID = rec.WorktreeID
	sum, captureErr := daemon.ProtectWorktree(ctx, rec.Path, db, daemon.CaptureContext{BranchRef: "refs/heads/main", BaseHead: head}, opts)
	failure := ""
	if captureErr != nil {
		failure = captureErr.Error()
	}
	if err := state.RecordCaptureHealth(ctx, db, failure, sum.CheckpointID, time.Now()); err != nil {
		t.Fatal(err)
	}
	return sum
}
func finishStatusStamp(t *testing.T, db *state.DB) {
	t.Helper()
	ts := float64(time.Now().UnixNano()) / 1e9
	if err := state.SaveDaemonState(t.Context(), db, state.DaemonState{PID: os.Getpid(), Mode: "running", HeartbeatTS: ts, UpdatedTS: ts}); err != nil {
		t.Fatal(err)
	}
}
func finishStatusRead(t *testing.T, rec central.RepoRecord, db *state.DB) statusReport {
	t.Helper()
	finishStatusStamp(t, db)
	report, err := buildStatusReport(t.Context(), rec, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return report
}
func finishStatusAssertViews(t *testing.T, rec central.RepoRecord, db *state.DB, expected statusReport) {
	t.Helper()
	wt, err := git.ResolveWorktree(t.Context(), rec.Path)
	if err != nil {
		t.Fatal(err)
	}
	indexBefore, err := fileSHA256(filepath.Join(wt.GitDir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	before, err := fileSHA256(rec.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	overview, err := readProductListRepo(t.Context(), rec, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var doctor doctorRepoReport
	if !readRepoState(t.Context(), &doctor, rec.Path, rec.StateDB) {
		t.Fatal("doctor read failed")
	}
	settings := collectProductDoctorSettings(t.Context(), rec.Path)
	if settings.Style != "imperative" || settings.Strategy != "local" || settings.Provider != "local" {
		t.Fatalf("doctor public settings %+v", settings)
	}
	diagnosis, err := buildDiagnoseReport(t.Context(), rec)
	if err != nil {
		t.Fatal(err)
	}
	for name, outcome := range map[string]publicationOutcome{"list": overview.report.PublicationOutcome, "doctor": doctor.PublicationOutcome, "diagnose": diagnosis.PublicationOutcome} {
		if !reflect.DeepEqual(outcome, expected.PublicationOutcome) {
			t.Fatalf("%s outcome differs: %+v vs %+v", name, outcome, expected.PublicationOutcome)
		}
	}
	var control controlResult
	applyControlStatusWithDaemonAlive(&control, expected, true)
	var output bytes.Buffer
	if err := renderProductEnvelope(&output, envelopeFromControl(control), true); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Data struct {
			Outcome publicationOutcome `json:"publication_outcome"`
		}
	}
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Data.Outcome.PendingChanges != expected.PublicationOutcome.PendingChanges {
		t.Fatal("JSON lost file count")
	}
	after, err := fileSHA256(rec.StateDB)
	if err != nil || after != before {
		t.Fatalf("read models mutated DB: %v", err)
	}
	indexAfter, err := fileSHA256(filepath.Join(wt.GitDir, "index"))
	if err != nil || indexAfter != indexBefore {
		t.Fatalf("read models mutated index: %v", err)
	}
	output.Reset()
	if err := renderProductEnvelope(&output, envelopeFromControl(control), false); err != nil {
		t.Fatal(err)
	}
	for _, retired := range []string{"queue", "batch wait", "event fallback", "automatic Intent"} {
		if strings.Contains(output.String(), retired) {
			t.Fatalf("retired phase in human status: %s", output.String())
		}
	}
}

func TestFinishStatusSavedWorkAndReadOnlyViews(t *testing.T) {
	rec, db, _ := finishStatusFixture(t)
	finishStatusWrite(t, rec.Path, "goal.txt", "saved")
	finishStatusCheckpoint(t, rec, db)
	report := finishStatusRead(t, rec, db)
	if !report.Protected || report.PublicationOutcome.PendingChanges != 1 || report.CheckpointPublishedByACD {
		t.Fatalf("saved outcome %+v", report.PublicationOutcome)
	}
	var control controlResult
	applyControlStatusWithDaemonAlive(&control, report, true)
	if !control.OK || !strings.Contains(control.NextAction, "acd finish") {
		t.Fatalf("next %+v", control)
	}
	finishStatusAssertViews(t, rec, db, report)
}

func TestFinishSchemaFreshReadOnlyViews(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := makeUnregisteredStartRepo(t)
	commitStartRepoSeed(t, repo)
	wt, err := git.ResolveWorktree(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	repo = wt.Root
	dbPath := state.DBPathFromGitDir(wt.GitDir)
	db, err := state.Open(t.Context(), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	registerProtectedControlRepo(t, roots, repo)
	rec := central.RepoRecord{Path: repo, StateDB: dbPath, RepoHash: central.CanonicalID(repo), RepositoryID: central.CanonicalID(repo), WorktreeID: checkpoint.WorktreeID(repo)}
	for _, table := range []string{"capture_events", "daemon_clients"} {
		exists, err := state.LegacyTableExists(t.Context(), db.ReadSQL(), table)
		if err != nil || exists {
			t.Fatalf("fresh schema contains %s: exists=%v, err=%v", table, exists, err)
		}
	}
	finishStatusWrite(t, repo, "goal.txt", "saved")
	finishStatusCheckpoint(t, rec, db)
	report := finishStatusRead(t, rec, db)
	if !report.Protected || report.PublicationOutcome.PendingChanges != 1 || report.PublicationOutcome.Resolved || len(report.Clients) != 0 {
		t.Fatalf("fresh saved outcome %+v, clients=%+v", report.PublicationOutcome, report.Clients)
	}
	finishStatusAssertViews(t, rec, db, report)
	before, err := fileSHA256(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := loadCheckpointHistory(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Published || entries[0].EventCount != 0 || entries[0].RecoveredEvents != 0 {
		t.Fatalf("fresh checkpoint history %+v", entries)
	}
	plan, err := buildFixPlan(t.Context(), repo, dbPath, true, false, false)
	if err != nil || len(plan.Actions) != 0 || len(plan.Unsafe) != 0 {
		t.Fatalf("fresh legacy recovery plan %+v: %v", plan, err)
	}
	if err := verifyFixPostApply(t.Context(), db.ReadSQL(), &plan); err != nil {
		t.Fatalf("fresh recovery verification: %v", err)
	}
	after, err := fileSHA256(dbPath)
	if err != nil || before != after {
		t.Fatalf("checkpoint history changed fresh database: %v", err)
	}
}

func TestFinishStatusPlanningCheckingFailedAndPublication(t *testing.T) {
	for _, scenario := range []string{"planning", "checking", "failed", "preview", "published", "later"} {
		t.Run(scenario, func(t *testing.T) {
			rec, db, w := finishStatusFixture(t)
			finishStatusWrite(t, rec.Path, "goal.txt", "selected")
			r, created, err := w.Start(t.Context(), daemon.FinishOptions{Purpose: "Complete goal", DryRun: scenario == "preview"})
			if err != nil || !created {
				t.Fatalf("start %v", err)
			}
			switch scenario {
			case "planning", "checking":
				plan, _ := json.Marshal(daemon.FinishSavedPlan{Version: 1, Steps: []daemon.FinishPlanStep{{Check: daemon.FinishCheck{TreeOID: r.Target.TargetTree, Policy: "full", Executed: true, Result: verification.Result{Status: verification.StatusPassed}}}}})
				_, err = state.UpdateFinishRequest(t.Context(), db, r.ID, r.Attempt, state.FinishRunning, state.FinishUpdate{State: state.FinishRunning, Stage: scenario, Plan: plan})
			case "failed":
				_, err = state.UpdateFinishRequest(t.Context(), db, r.ID, r.Attempt, state.FinishRunning, state.FinishUpdate{State: state.FinishFailed, ReasonCode: "provider_failed", Reason: "Provider unavailable"})
			default:
				_, err = w.Execute(t.Context(), r.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "later" {
				finishStatusWrite(t, rec.Path, "later.txt", "new work")
				finishStatusCheckpoint(t, rec, db)
			}
			report := finishStatusRead(t, rec, db)
			outcome := report.PublicationOutcome
			if outcome.Finish == nil || outcome.Finish.RequestID != r.ID || !report.Protected {
				t.Fatalf("finish outcome %+v", outcome)
			}
			control := controlResult{Enabled: true}
			applyControlStatusWithDaemonAlive(&control, report, true)
			if !control.OK {
				t.Fatalf("finish failure blocked protection: %+v", control)
			}
			switch scenario {
			case "planning", "checking":
				if !report.Busy || len(outcome.Finish.Verification) != 1 || outcome.Finish.Verification[0].Policy != "full" {
					t.Fatal("lost running check evidence")
				}
			case "failed":
				if report.Busy || !strings.Contains(control.NextAction, r.ID) {
					t.Fatal("failed retry target not shown")
				}
			case "preview":
				if outcome.Finish.Published || outcome.PendingChanges != 1 {
					t.Fatal("preview claimed publication")
				}
			case "published":
				if !outcome.Finish.Published || outcome.PendingChanges != 0 || outcome.BranchCommitted == nil || !*outcome.BranchCommitted {
					t.Fatal("publication proof lost")
				}
			case "later":
				if !outcome.Finish.Published || outcome.LaterChanges != 1 || outcome.PendingChanges != 1 || outcome.BranchCommitted == nil || *outcome.BranchCommitted {
					t.Fatal("later edits conflated with frozen publication")
				}
			}
			finishStatusAssertViews(t, rec, db, report)
			entries, err := loadCheckpointHistory(t.Context(), rec.Path)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, entry := range entries {
				if entry.ID != r.Target.CheckpointID {
					continue
				}
				found = true
				if entry.PublicationOutcome.Finish == nil || entry.PublicationOutcome.Finish.RequestID != r.ID {
					t.Fatal("history lost finish target")
				}
				if entry.Outcome == "published" && scenario != "published" && scenario != "later" {
					t.Fatal("history claimed preview/failed publication")
				}
				if (scenario == "published" || scenario == "later") && entry.Outcome != "published" {
					t.Fatal("history lost exact publication outcome")
				}
			}
			if !found {
				t.Fatal("history missing source checkpoint")
			}
		})
	}
}
func TestFinishStatusPartialCoverageAndRecovery(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("ordinary permissions required")
	}
	rec, db, _ := finishStatusFixture(t)
	finishStatusWrite(t, rec.Path, "unreadable.txt", "private failed read")
	path := filepath.Join(rec.Path, "unreadable.txt")
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0600) })
	sum := finishStatusCheckpoint(t, rec, db)
	if !sum.Partial {
		t.Fatal("fixture was not partial")
	}
	report := finishStatusRead(t, rec, db)
	if report.Protected || report.PublicationOutcome.CountsKnown || report.PublicationOutcome.BranchCommitted != nil || report.CaptureHealth.IssueCount == 0 {
		t.Fatal("partial checkpoint claimed coverage")
	}
	finishStatusAssertViews(t, rec, db, report)
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	finishStatusCheckpoint(t, rec, db)
	report = finishStatusRead(t, rec, db)
	if !report.Protected || report.CaptureHealth.Error != "" {
		t.Fatal("partial error did not clear")
	}
}
func TestFinishStatusExcludedTrackedFilesAreNotBranchDeletions(t *testing.T) {
	rec, db, _ := finishStatusFixture(t)
	finishStatusWrite(t, rec.Path, ".env", "private")
	finishStatusGit(t, rec.Path, "add", ".env")
	finishStatusGit(t, rec.Path, "commit", "-q", "-m", "Preserve private config")
	finishStatusCheckpoint(t, rec, db)
	report := finishStatusRead(t, rec, db)
	if report.PublicationOutcome.PendingChanges != 0 || report.PublicationOutcome.BranchCommitted == nil || !*report.PublicationOutcome.BranchCommitted {
		t.Fatal("excluded entry interpreted as branch deletion")
	}
}

func TestFinishStatusStoppedAndDisabledKeepProtectionEvidence(t *testing.T) {
	rec, db, _ := finishStatusFixture(t)
	finishStatusWrite(t, rec.Path, "saved.txt", "saved")
	finishStatusCheckpoint(t, rec, db)
	if err := state.SaveDaemonState(t.Context(), db, state.DaemonState{Mode: "stopped"}); err != nil {
		t.Fatal(err)
	}
	report, err := buildStatusReport(t.Context(), rec, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var control controlResult
	applyControlStatusWithDaemonAlive(&control, report, false)
	if !control.Protected || control.OK || control.PublicationProgress.WorkerResponsive {
		t.Fatalf("stopped truth %+v", control)
	}
	roots, err := paths.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.DisableRepo(central.RepoRemovalTarget{Path: rec.Path}, time.Now().Unix())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	disabled, err := inspectControl(t.Context(), rec.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !disabled.OK || disabled.Enabled || !disabled.Protected || disabled.Health != controlHealthOff || disabled.Published {
		t.Fatalf("disabled truth %+v", disabled)
	}
}

func TestFinishStatusRecoveredLegacyIsSavedNotBranchCommitted(t *testing.T) {
	rec, db, _ := finishStatusFixture(t)
	finishStatusWrite(t, rec.Path, "work.txt", "legacy preserved")
	cp := finishStatusCheckpoint(t, rec, db)
	head := finishStatusGit(t, rec.Path, "rev-parse", "HEAD")
	seq, err := testutil.SeedLegacyCapture(t.Context(), db, state.CaptureEvent{BranchRef: "refs/heads/main", BaseHead: head, Operation: "modify", Path: "work.txt", Fidelity: "full", State: state.EventStateRecovered}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(t.Context(), `INSERT INTO checkpoint_events(checkpoint_id,ord,event_seq) VALUES(?,0,?)`, cp.CheckpointID, seq); err != nil {
		t.Fatal(err)
	}
	report := finishStatusRead(t, rec, db)
	if !report.CheckpointPublishedByACD || !report.PublicationOutcome.Resolved || report.PublicationOutcome.RecoveredChanges != 1 || report.PublicationOutcome.BranchCommitted == nil || *report.PublicationOutcome.BranchCommitted {
		t.Fatalf("legacy truth %+v", report.PublicationOutcome)
	}
	var control controlResult
	applyControlStatusWithDaemonAlive(&control, report, true)
	if !control.OK || !strings.Contains(control.NextAction, "acd finish") || !strings.Contains(control.Summary, "Legacy work remains preserved") {
		t.Fatalf("migration attention blocked healthy protection %+v", control)
	}
	finishStatusAssertViews(t, rec, db, report)
	entries, err := loadCheckpointHistory(t.Context(), rec.Path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 || entries[0].Outcome != "recovered" || !entries[0].Published || entries[0].PublicationOutcome.BranchCommitted == nil || *entries[0].PublicationOutcome.BranchCommitted {
		t.Fatalf("history recovery %+v", entries)
	}
}

func TestFinishStatusActiveHistoryPlan(t *testing.T) {
	rec, db, _ := finishStatusFixture(t)
	finishStatusCheckpoint(t, rec, db)
	r := state.FinishHistoryRequest{PlanID: "reviewed-history", WorktreeID: rec.WorktreeID, ConfigDigest: "approved", IndexDigest: "sha256:index", DeadlineTS: float64(time.Now().Add(time.Minute).Unix()), DryRun: true}
	_, _, err := state.StartFinishHistoryRequest(t.Context(), db, r)
	if err != nil {
		t.Fatal(err)
	}
	report := finishStatusRead(t, rec, db)
	if !report.Busy || report.PublicationOutcome.History == nil || report.PublicationOutcome.History.PlanID != r.PlanID || !report.PublicationOutcome.History.DryRun {
		t.Fatalf("history request %+v", report.PublicationOutcome)
	}
	finishStatusAssertViews(t, rec, db, report)
}

func TestFinishStatusPublicationAttentionRequiresAction(t *testing.T) {
	for _, operation := range []string{"finish", "history"} {
		t.Run(operation, func(t *testing.T) {
			rec, db, worker := finishStatusFixture(t)
			finishStatusWrite(t, rec.Path, "saved.txt", "saved work")
			finishStatusCheckpoint(t, rec, db)
			if operation == "finish" {
				request, _, err := worker.Start(t.Context(), daemon.FinishOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := state.UpdateFinishRequest(t.Context(), db, request.ID, request.Attempt, state.FinishRunning, state.FinishUpdate{State: state.FinishPrepared, Plan: json.RawMessage(`{}`), Publication: json.RawMessage(`{}`)}); err != nil {
					t.Fatal(err)
				}
				if _, err := state.UpdateFinishRequest(t.Context(), db, request.ID, request.Attempt, state.FinishPrepared, state.FinishUpdate{State: state.FinishNeedsAttention, ReasonCode: "ambiguous_ref", Reason: "Branch publication could not be proven"}); err != nil {
					t.Fatal(err)
				}
			} else {
				request, _, err := state.StartFinishHistoryRequest(t.Context(), db, state.FinishHistoryRequest{PlanID: "history-attention", WorktreeID: rec.WorktreeID, ConfigDigest: "approved", IndexDigest: "saved-index", DeadlineTS: float64(time.Now().Add(time.Minute).Unix())})
				if err != nil {
					t.Fatal(err)
				}
				request.Status = "needs_attention"
				request.Error = "Branch publication could not be proven"
				if err := state.UpdateFinishHistoryRequest(t.Context(), db, request, "running"); err != nil {
					t.Fatal(err)
				}
			}
			report := finishStatusRead(t, rec, db)
			var control controlResult
			applyControlStatusWithDaemonAlive(&control, report, true)
			if !control.Protected || control.OK || control.Health != controlHealthNeedsAttention {
				t.Fatalf("publication attention hidden by healthy protection: %+v", control)
			}
			entry := productListEntryFromOverview(rec, supervisor.WorkerStatus{}, productListRepoOverview{report: report}, nil)
			if !entry.Protected || !entry.ActionRequired || entry.State != productStateNeedsAction {
				t.Fatalf("list hides publication attention: %+v", entry)
			}
			finishStatusAssertViews(t, rec, db, report)
		})
	}
}

func TestFinishStatusUsesSavedProtectionScope(t *testing.T) {
	rec, db, _ := finishStatusFixture(t)
	finishStatusWrite(t, rec.Path, "custom.secret", "excluded")
	finishStatusWrite(t, rec.Path, "generated/old.txt", "excluded")
	finishStatusGit(t, rec.Path, "add", "custom.secret", "generated/old.txt")
	finishStatusGit(t, rec.Path, "commit", "-m", "Seed excluded tracked entries")
	finishStatusWrite(t, rec.Path, "ordinary.txt", "eligible")
	opts := daemon.CaptureOpts{SensitiveMatcher: state.NewSensitiveMatcherFromValue("custom.secret"), SafeIgnoreMatcher: state.NewSafeIgnoreMatcherFromValues("true", "generated/")}
	cp := finishStatusCheckpoint(t, rec, db, opts)
	finishConfigRevision(t, db, map[string]any{config.FieldCommitStrategy: "local", config.FieldCommitFormat: "imperative"}, sql.NullInt64{}, true)
	roots, err := paths.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Global[config.FieldCommitStrategy] = json.RawMessage(`"intent"`)
		doc.Settings.Global[config.FieldCommitFormat] = json.RawMessage(`"conventional"`)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Authoring and environment changes cannot reinterpret the saved scan.
	finishStatusWrite(t, rec.Path, ".gitignore", "ordinary.txt\n")
	t.Setenv(state.EnvSensitiveGlobs, "ordinary.txt")
	t.Setenv(state.EnvSafeIgnore, "false")
	report := finishStatusRead(t, rec, db)
	if !report.PublicationOutcome.CountsKnown || report.PublicationOutcome.PendingChanges != 1 {
		t.Fatalf("scope changed %+v", report.PublicationOutcome)
	}
	scope, ok := readFinishProtectionScope(mustFinishStatusMeta(t, db, daemon.MetaKeyProtectionScope), cp.CheckpointID)
	if !ok || !state.NewSensitiveMatcherFromPatterns(scope.Sensitive, scope.SensitiveAllow).Match("custom.secret") || !state.NewSafeIgnoreMatcherFromPatterns(scope.SafeIgnore).MatchFile("generated/old.txt") {
		t.Fatal("scope did not round trip")
	}
	finishStatusAssertViews(t, rec, db, report)
	if err := state.MetaSet(t.Context(), db, daemon.MetaKeyProtectionScope, `{"version":1,"checkpoint_id":"another"}`); err != nil {
		t.Fatal(err)
	}
	unknown := finishStatusRead(t, rec, db)
	if unknown.PublicationOutcome.CountsKnown || unknown.PublicationOutcome.BranchCommitted != nil || unknown.CheckpointPublishedByACD {
		t.Fatalf("mismatched scope invented proof %+v", unknown.PublicationOutcome)
	}
	finishStatusAssertViews(t, rec, db, unknown)
}
func mustFinishStatusMeta(t *testing.T, db *state.DB, key string) string {
	t.Helper()
	value, _, err := state.MetaGet(t.Context(), db, key)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func TestFinishStatusDefaultAllowedTemplateScope(t *testing.T) {
	rec, db, _ := finishStatusFixture(t)
	finishStatusWrite(t, rec.Path, ".env.example", "template")
	finishStatusCheckpoint(t, rec, db)
	report := finishStatusRead(t, rec, db)
	if !report.PublicationOutcome.CountsKnown || report.PublicationOutcome.PendingChanges != 1 {
		t.Fatalf("template allow lost %+v", report.PublicationOutcome)
	}
	finishStatusAssertViews(t, rec, db, report)
}

func TestFinishStatusRejectsInvalidCheckpointRef(t *testing.T) {
	for _, scenario := range []string{"missing", "moved", "symbolic"} {
		t.Run(scenario, func(t *testing.T) {
			rec, db, _ := finishStatusFixture(t)
			finishStatusWrite(t, rec.Path, "saved.txt", "saved work")
			sum := finishStatusCheckpoint(t, rec, db)
			cp, err := state.ResolveCheckpoint(t.Context(), rec.StateDB, sum.CheckpointID)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "missing":
				finishStatusGit(t, rec.Path, "update-ref", "-d", cp.Ref)
			case "moved":
				finishStatusGit(t, rec.Path, "update-ref", cp.Ref, "HEAD")
			case "symbolic":
				aliasTarget := "refs/acd/status-test-target"
				finishStatusGit(t, rec.Path, "update-ref", aliasTarget, cp.CommitOID)
				finishStatusGit(t, rec.Path, "symbolic-ref", cp.Ref, aliasTarget)
			}
			report := finishStatusRead(t, rec, db)
			if report.Protected || report.PublicationOutcome.Protected || report.PublicationOutcome.CountsKnown || report.PublicationOutcome.BranchCommitted != nil || report.CaptureHealth.State != "blocked" {
				t.Fatalf("status accepted invalid checkpoint ref: %+v", report.PublicationOutcome)
			}
			entry := productListEntryFromOverview(rec, supervisor.WorkerStatus{}, productListRepoOverview{report: report}, nil)
			if entry.Protected || !entry.ActionRequired || entry.State != productStateNeedsAction {
				t.Fatalf("list accepted invalid checkpoint ref: %+v", entry)
			}
			finishStatusAssertViews(t, rec, db, report)
			refs, err := git.Run(t.Context(), git.RunOpts{Dir: rec.Path}, "for-each-ref", "--format=%(refname)%00%(objectname)%00%(symref)", cp.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if err := verifyFinishCheckpointRef(t.Context(), rec.Path, cp); err == nil {
				t.Fatalf("read-only diagnostics repaired the invalid ref: %q", refs)
			}
		})
	}
}

func TestFinishStatusIgnoreAmbiguityDoesNotClaimCommitment(t *testing.T) {
	rec, db, _ := finishStatusFixture(t)
	finishStatusWrite(t, rec.Path, "gone.txt", "tracked before capture")
	finishStatusGit(t, rec.Path, "add", "gone.txt")
	finishStatusGit(t, rec.Path, "commit", "-m", "Seed omitted tracked entry")
	if err := os.Remove(filepath.Join(rec.Path, "gone.txt")); err != nil {
		t.Fatal(err)
	}
	finishStatusGit(t, rec.Path, "rm", "--cached", "gone.txt")
	finishStatusWrite(t, rec.Path, ".gitignore", "gone.txt\n")
	finishStatusCheckpoint(t, rec, db)
	before := finishStatusGit(t, rec.Path, "ls-files", "--stage")
	report := finishStatusRead(t, rec, db)
	if !report.Protected || report.PublicationOutcome.CountsKnown || report.PublicationOutcome.BranchCommitted != nil || report.CheckpointPublishedByACD {
		t.Fatalf("ambiguous omission claimed exact commitment %+v", report.PublicationOutcome)
	}
	finishStatusAssertViews(t, rec, db, report)
	if after := finishStatusGit(t, rec.Path, "ls-files", "--stage"); after != before {
		t.Fatal("status mutated staged deletion")
	}
}

func TestFinishStatusSparseCheckoutCountsEligibleChanges(t *testing.T) {
	for _, sparseIndex := range []bool{false, true} {
		name := "full-index"
		if sparseIndex {
			name = "sparse-index"
		}
		t.Run(name, func(t *testing.T) {
			rec, db, worker := finishStatusFixture(t)
			finishStatusWrite(t, rec.Path, "visible/file.txt", "base")
			finishStatusWrite(t, rec.Path, "hidden/file.txt", "keep")
			finishStatusGit(t, rec.Path, "add", "visible", "hidden")
			finishStatusGit(t, rec.Path, "commit", "-m", "Seed sparse checkout")
			args := []string{"sparse-checkout", "init", "--cone"}
			if sparseIndex {
				args = append(args, "--sparse-index")
			}
			finishStatusGit(t, rec.Path, args...)
			finishStatusGit(t, rec.Path, "sparse-checkout", "set", "visible")
			finishStatusWrite(t, rec.Path, "visible/file.txt", "changed")
			finishStatusCheckpoint(t, rec, db)
			report := finishStatusRead(t, rec, db)
			if !report.PublicationOutcome.CountsKnown || report.PublicationOutcome.PendingChanges != 1 {
				t.Fatalf("sparse omission counted as deletion: %+v", report.PublicationOutcome)
			}
			request, _, err := worker.Start(t.Context(), daemon.FinishOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if completed, err := worker.Execute(t.Context(), request.ID); err != nil || completed.State != state.FinishCompleted {
				t.Fatalf("finish=%+v err=%v", completed, err)
			}
			finishStatusCheckpoint(t, rec, db)
			report = finishStatusRead(t, rec, db)
			if !report.Protected || !report.PublicationOutcome.CountsKnown || report.PublicationOutcome.PendingChanges != 0 || report.OperationalState != "up_to_date" {
				t.Fatalf("completed sparse finish still has pending work: %+v", report.PublicationOutcome)
			}
			finishStatusAssertViews(t, rec, db, report)
		})
	}
}

func TestFinishStatusCompletedHistoryCannotOverrideCurrentFinish(t *testing.T) {
	for _, phase := range []string{"planning", "failed"} {
		t.Run(phase, func(t *testing.T) {
			rec, db, w := finishStatusFixture(t)
			old, _, err := state.StartFinishHistoryRequest(t.Context(), db, state.FinishHistoryRequest{PlanID: "old-history", WorktreeID: rec.WorktreeID, ConfigDigest: "approved", IndexDigest: "saved-index", DeadlineTS: float64(time.Now().Add(time.Minute).Unix()), DryRun: true})
			if err != nil {
				t.Fatal(err)
			}
			old.Status = "completed"
			if err := state.UpdateFinishHistoryRequest(t.Context(), db, old, "running"); err != nil {
				t.Fatal(err)
			}
			finishStatusWrite(t, rec.Path, "current.txt", "current finish")
			request, _, err := w.Start(t.Context(), daemon.FinishOptions{})
			if err != nil {
				t.Fatal(err)
			}
			update := state.FinishUpdate{State: state.FinishRunning, Stage: "planning"}
			if phase == "failed" {
				update.State = state.FinishFailed
				update.Stage = "failed"
				update.ReasonCode = "provider_failed"
				update.Reason = "Provider unavailable"
			}
			if _, err := state.UpdateFinishRequest(t.Context(), db, request.ID, request.Attempt, state.FinishRunning, update); err != nil {
				t.Fatal(err)
			}
			report := finishStatusRead(t, rec, db)
			var control controlResult
			applyControlStatusWithDaemonAlive(&control, report, true)
			if strings.Contains(control.Summary, "old-history") || !control.OK || report.PublicationOutcome.History != nil {
				t.Fatalf("old history overrode finish %+v", control)
			}
			if phase == "planning" && (!strings.Contains(control.Summary, "planning") || !report.Busy) {
				t.Fatal("current planning phase lost")
			}
			if phase == "failed" && (!strings.Contains(control.NextAction, request.ID) || !strings.Contains(control.Summary, "Provider unavailable")) {
				t.Fatal("current retry target lost")
			}
			finishStatusAssertViews(t, rec, db, report)
		})
	}
}
