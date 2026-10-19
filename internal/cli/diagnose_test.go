package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
)

func TestDiagnoseLegacyFailuresRemainPreservedWithoutAutonomousReplay(t *testing.T) {
	for _, outcome := range []string{state.EventStateBlockedConflict, state.EventStateFailed, "legacy_metadata"} {
		t.Run(outcome, func(t *testing.T) {
			roots := withIsolatedHome(t)
			ctx := context.Background()
			repo, path, db := makeDiagnoseRepo(t, roots)
			head := seedDiagnoseCommit(t, repo)
			blob, err := git.RevParse(ctx, repo, "HEAD:seed.txt")
			if err != nil {
				t.Fatal(err)
			}
			seq, err := testutil.SeedLegacyCapture(ctx, db, state.CaptureEvent{
				BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
				Operation: "modify", Path: "seed.txt", Fidelity: "exact",
			}, []state.CaptureOp{{Op: "modify", Path: "seed.txt", Fidelity: "exact", AfterOID: sql.NullString{String: blob, Valid: true}}})
			if err != nil {
				t.Fatal(err)
			}
			stored := outcome
			if outcome == "legacy_metadata" {
				stored = state.EventStateBlockedConflict
				if err := state.MetaSet(ctx, db, "last_replay_conflict", "original legacy conflict metadata"); err != nil {
					t.Fatal(err)
				}
			}
			if err := testutil.SeedLegacyCaptureOutcome(ctx, db, seq, stored, sql.NullString{},
				sql.NullString{String: "original failure evidence", Valid: true}, sql.NullString{}, nowFloat()); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before := mustSHA256(t, path)
			var out bytes.Buffer
			if err := runDiagnose(ctx, &out, repo, true); err != nil {
				t.Fatal(err)
			}
			var report diagnoseReport
			if err := json.Unmarshal(out.Bytes(), &report); err != nil {
				t.Fatal(err)
			}
			legacy := report.PublicationOutcome.Legacy
			if legacy.State != "needs_attention" || legacy.Unpublished != 0 || legacy.ReasonCode != "legacy_activation_required" {
				t.Fatalf("legacy failure evidence hidden: %+v", legacy)
			}
			if !report.StateDBChecksumVerified || report.PendingDepth != 0 || report.AutoResolvableBlockedCount != 0 {
				t.Fatalf("diagnostics invented autonomous replay: %+v", report)
			}
			if mustSHA256(t, path) != before {
				t.Fatal("diagnostics changed original failure/provenance rows")
			}
			assertLegacyFailureProof(t, path, seq, stored, "original failure evidence", head, blob)
			if current, err := git.RevParse(ctx, repo, "HEAD"); err != nil || current != head {
				t.Fatal("diagnostics changed branch")
			}
		})
	}
}

func assertLegacyFailureProof(t *testing.T, path string, seq int64, outcome, message, head, blob string) {
	t.Helper()
	conn, err := openStateDBReadOnly(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var storedState, storedError, storedHead, storedBlob string
	if err := conn.QueryRowContext(t.Context(), `SELECT e.state,e.error,e.base_head,o.after_oid
		FROM capture_events e JOIN capture_ops o ON o.event_seq=e.seq AND o.ord=0 WHERE e.seq=?`, seq).
		Scan(&storedState, &storedError, &storedHead, &storedBlob); err != nil {
		t.Fatal(err)
	}
	if storedState != outcome || storedError != message || storedHead != head || storedBlob != blob {
		t.Fatalf("original failure proof changed: state=%s error=%s head=%s blob=%s", storedState, storedError, storedHead, storedBlob)
	}
}

func TestDiagnose_JSONOutput(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running", BranchRef: sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("save daemon_state: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	root := newRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"support", "diagnose", "--repo", repo, "--json"})
	if err := root.ExecuteContext(ctx); err != nil {
		t.Fatalf("execute diagnose: %v\nstderr:\n%s", err, errOut.String())
	}

	var envelope struct {
		Data diagnoseReport `json:"data"`
	}
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
		t.Fatalf("unmarshal diagnose JSON: %v\n%s", err, out.String())
	}
	rep := envelope.Data
	if rep.Repo != repo {
		t.Fatalf("repo=%q, want %q", rep.Repo, repo)
	}
	if !rep.PublicationOutcome.CheckpointAvailable || rep.PublicationOutcome.Protected || rep.PublicationOutcome.CountsKnown {
		t.Fatalf("diagnostics invented coverage without a completed observation: %+v", rep.PublicationOutcome)
	}
	if !rep.StateDBChecksumVerified || rep.StateDBChecksumBefore == "" || rep.StateDBChecksumAfter == "" {
		t.Fatalf("checksum fields not verified: before=%q after=%q verified=%v",
			rep.StateDBChecksumBefore, rep.StateDBChecksumAfter, rep.StateDBChecksumVerified)
	}
	if len(rep.Remediation) != 1 || !strings.Contains(rep.Remediation[0], "acd on") {
		t.Fatalf("unexpected remediation: %v", rep.Remediation)
	}
}

func seedDiagnoseCommit(t *testing.T, repoDir string) string {
	t.Helper()
	ctx := context.Background()
	for _, kv := range [][]string{
		{"user.email", "acd-test@example.com"},
		{"user.name", "ACD Test"},
		{"commit.gpgsign", "false"},
	} {
		if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "config", kv[0], kv[1]); err != nil {
			t.Fatalf("git config %s: %v", kv[0], err)
		}
	}
	if err := os.WriteFile(filepath.Join(repoDir, "seed.txt"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "add", "seed.txt"); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "commit", "-q", "-m", "seed"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	head, err := git.RevParse(ctx, repoDir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	return head
}

func makeDiagnoseRepo(t *testing.T, roots paths.Roots) (repoDir, dbPath string, d *state.DB) {
	t.Helper()
	ctx := context.Background()
	repoDir = t.TempDir()
	if err := git.Init(ctx, repoDir); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref HEAD: %v", err)
	}
	dbPath = state.DBPathFromGitDir(filepath.Join(repoDir, ".git"))
	var err error
	d, err = state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	registerRepo(t, roots, repoDir, dbPath, "codex")
	return repoDir, dbPath, d
}

func mustSHA256(t *testing.T, path string) string {
	t.Helper()
	got, err := fileSHA256(path)
	if err != nil {
		t.Fatalf("sha256 %s: %v", path, err)
	}
	return got
}

func TestDiagnose_DeadBranchPrune_Absent(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, true); err != nil {
		t.Fatalf("runDiagnose: %v", err)
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose: %v\n%s", err, out.String())
	}
	if rep.DeadBranchPruneLastRunTS != 0 {
		t.Fatalf("DeadBranchPruneLastRunTS=%d want 0", rep.DeadBranchPruneLastRunTS)
	}
	if rep.DeadBranchPruneLastCount != 0 {
		t.Fatalf("DeadBranchPruneLastCount=%d want 0", rep.DeadBranchPruneLastCount)
	}
	if rep.DeadBranchPruneLastRefs != nil {
		t.Fatalf("DeadBranchPruneLastRefs=%v want nil", rep.DeadBranchPruneLastRefs)
	}
	// The two int fields must be present (always-emit contract) with their
	// zero values. The slice (omitempty) must be absent when nil.
	for _, want := range []string{
		`"dead_branch_prune_last_run_ts": 0`,
		`"dead_branch_prune_last_count": 0`,
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("diagnose JSON missing %q in:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "dead_branch_prune_last_refs") {
		t.Fatalf("diagnose JSON unexpectedly contains 'dead_branch_prune_last_refs' (slice omitempty broken):\n%s", out.String())
	}
}

func TestDiagnose_RenderHumanOmitsDeadBranchPruneWhenZero(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, false); err != nil {
		t.Fatalf("runDiagnose human: %v", err)
	}
	if strings.Contains(out.String(), "Dead-branch recovery") {
		t.Fatalf("human renderer included dead-branch recovery line on never-ran repo:\n%s", out.String())
	}
}
