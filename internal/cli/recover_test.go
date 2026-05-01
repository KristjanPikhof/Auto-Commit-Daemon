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
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func makeRegisteredGitRepoStateDB(t *testing.T) (repoDir, stateDB string, db *state.DB) {
	t.Helper()
	roots := withIsolatedHome(t)
	repoDir, stateDB, db = makeRepoStateDB(t)
	if err := git.Init(context.Background(), repoDir); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if _, err := git.Run(context.Background(), git.RunOpts{Dir: repoDir}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "seed.txt"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	if _, err := git.Run(context.Background(), git.RunOpts{Dir: repoDir}, "add", "seed.txt"); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if _, err := git.Run(context.Background(), git.RunOpts{Dir: repoDir}, "-c", "user.name=ACD Test", "-c", "user.email=acd@example.invalid", "commit", "-m", "seed"); err != nil {
		t.Fatalf("git commit: %v", err)
	}
	registerRepo(t, roots, repoDir, stateDB, "test")
	return repoDir, stateDB, db
}

func TestRecover_DryRunNoMutation(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}
	if err := state.MetaSet(ctx, db, "last_replay_conflict", `{"seq":1,"error_class":"cas_fail"}`); err != nil {
		t.Fatalf("MetaSet: %v", err)
	}
	fixtureChecksum, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum fixture: %v", err)
	}

	var out bytes.Buffer
	if err := runRecover(ctx, &out, repo, true, true, false, true, false); err != nil {
		t.Fatalf("runRecover dry-run: %v", err)
	}
	var plan recoverPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal recover plan: %v\n%s", err, out.String())
	}
	if !plan.DryRun || plan.CurrentBranchRef != "refs/heads/main" {
		t.Fatalf("plan=%+v", plan)
	}
	after, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if fixtureChecksum != after {
		t.Fatalf("dry-run mutated state.db: before=%s after=%s", fixtureChecksum, after)
	}
	_ = before
}

func TestRecover_DryRunDoesNotBootstrapSchema(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()

	if _, err := db.SQL().ExecContext(ctx, `PRAGMA user_version = 1`); err != nil {
		t.Fatalf("lower user_version: %v", err)
	}
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}

	var out bytes.Buffer
	if err := runRecover(ctx, &out, repo, true, true, false, true, false); err != nil {
		t.Fatalf("runRecover dry-run: %v", err)
	}
	after, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if before != after {
		t.Fatalf("dry-run bootstrapped schema: before=%s after=%s", before, after)
	}
	var version int
	if err := db.SQL().QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("query user_version: %v", err)
	}
	if version != 1 {
		t.Fatalf("user_version=%d want 1", version)
	}
}

func TestRecover_AppliesBackupAndRetargetsIncident(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID:              999999,
		Mode:             "stopped",
		BranchRef:        sql.NullString{String: "refs/heads/stale", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 2, Valid: true},
	}); err != nil {
		t.Fatalf("SaveDaemonState: %v", err)
	}
	if err := state.MetaSet(ctx, db, "branch.generation", "2"); err != nil {
		t.Fatalf("MetaSet generation: %v", err)
	}
	if err := state.MetaSet(ctx, db, "last_replay_conflict", `{"seq":1,"error_class":"cas_fail"}`); err != nil {
		t.Fatalf("MetaSet conflict: %v", err)
	}
	for _, sp := range []state.ShadowPath{
		{
			BranchRef: "refs/heads/stale", BranchGeneration: 2,
			Path: "dup.txt", Operation: "modify", BaseHead: head, Fidelity: "exact",
			Mode: sql.NullString{String: git.RegularFileMode, Valid: true},
			OID:  sql.NullString{String: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Valid: true},
		},
		{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			Path: "dup.txt", Operation: "modify", BaseHead: head, Fidelity: "exact",
			Mode: sql.NullString{String: git.RegularFileMode, Valid: true},
			OID:  sql.NullString{String: "cccccccccccccccccccccccccccccccccccccccc", Valid: true},
		},
	} {
		if err := state.UpsertShadowPath(ctx, db, sp); err != nil {
			t.Fatalf("UpsertShadowPath: %v", err)
		}
	}
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/stale",
		BranchGeneration: 2,
		BaseHead:         head,
		Operation:        "create",
		Path:             "blocked.txt",
		Fidelity:         "full",
		State:            state.EventStateBlockedConflict,
		Error:            sql.NullString{String: "old conflict", Valid: true},
	}, []state.CaptureOp{{
		Op:        "create",
		Path:      "blocked.txt",
		Fidelity:  "full",
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:  sql.NullString{String: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Valid: true},
	}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	backupBefore, err := os.ReadFile(stateDB)
	if err != nil {
		t.Fatalf("read state db before: %v", err)
	}

	var out bytes.Buffer
	if err := runRecover(ctx, &out, repo, true, false, true, true); err != nil {
		t.Fatalf("runRecover apply: %v\n%s", err, out.String())
	}
	var plan recoverPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal result: %v\n%s", err, out.String())
	}
	if plan.BackupPath == "" {
		t.Fatalf("backup path empty")
	}
	gotBackup, err := os.ReadFile(plan.BackupPath)
	if err != nil {
		t.Fatalf("read backup: %v", err)
	}
	if !bytes.Equal(gotBackup, backupBefore) {
		t.Fatalf("backup does not match pre-apply state.db")
	}

	var branchRef, eventState string
	var gen int64
	var errMsg sql.NullString
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT branch_ref, branch_generation, state, error FROM capture_events WHERE seq = ?`, seq,
	).Scan(&branchRef, &gen, &eventState, &errMsg); err != nil {
		t.Fatalf("query event: %v", err)
	}
	if branchRef != "refs/heads/main" || gen != 2 || eventState != state.EventStatePending || errMsg.Valid {
		t.Fatalf("event after recover branch=%q gen=%d state=%q err=%v", branchRef, gen, eventState, errMsg)
	}
	if _, ok, err := state.MetaGet(ctx, db, "last_replay_conflict"); err != nil {
		t.Fatalf("MetaGet conflict: %v", err)
	} else if ok {
		t.Fatalf("last_replay_conflict was not cleared")
	}
	if tok, _, _ := state.MetaGet(ctx, db, "branch_token"); !strings.Contains(tok, "refs/heads/main") {
		t.Fatalf("branch_token=%q want current branch", tok)
	}
	var shadowRows int
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM shadow_paths WHERE branch_ref = 'refs/heads/main' AND branch_generation = 2 AND path = 'dup.txt'`,
	).Scan(&shadowRows); err != nil {
		t.Fatalf("count retargeted shadow rows: %v", err)
	}
	if shadowRows != 1 {
		t.Fatalf("retargeted shadow rows=%d want 1", shadowRows)
	}
}

func TestRecover_ClearsReplayPausedUntil(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()

	until := time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339)
	if err := state.MetaSet(ctx, db, daemon.MetaKeyReplayPausedUntil, until); err != nil {
		t.Fatalf("MetaSet replay.paused_until: %v", err)
	}

	var out bytes.Buffer
	if err := runRecover(ctx, &out, repo, true, false, true, false); err != nil {
		t.Fatalf("runRecover apply: %v", err)
	}

	if v, ok, err := state.MetaGet(ctx, db, daemon.MetaKeyReplayPausedUntil); err != nil {
		t.Fatalf("MetaGet: %v", err)
	} else if ok {
		t.Fatalf("replay.paused_until still set: %q", v)
	}
}

func TestRecover_RemovesManualPauseMarker(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()

	gitDir := filepath.Join(repo, ".git")
	markerPath := pausepkg.Path(gitDir)
	expiresAt := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	if _, err := pausepkg.Write(markerPath, pausepkg.Marker{
		Reason:    "manual",
		SetAt:     time.Now().UTC().Format(time.RFC3339),
		SetBy:     "test",
		ExpiresAt: &expiresAt,
	}, true); err != nil {
		t.Fatalf("pausepkg.Write: %v", err)
	}

	var out bytes.Buffer
	if err := runRecover(ctx, &out, repo, true, false, true, true); err != nil {
		t.Fatalf("runRecover apply: %v", err)
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("manual pause marker still on disk: stat err=%v", err)
	}

	var plan recoverPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal recover plan: %v\n%s", err, out.String())
	}
	if !plan.ManualMarkerRemoved {
		t.Fatalf("plan.ManualMarkerRemoved=false want true: %+v", plan)
	}
	if !strings.HasSuffix(plan.ManualMarkerPath, filepath.Join(".git", "acd", "paused")) {
		t.Fatalf("plan.ManualMarkerPath=%q want suffix .git/acd/paused", plan.ManualMarkerPath)
	}
}

func TestRecover_DryRun_ListsPauseStateActions(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()

	var out bytes.Buffer
	if err := runRecover(ctx, &out, repo, true, true, false, true); err != nil {
		t.Fatalf("runRecover dry-run: %v", err)
	}
	var plan recoverPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	wantSubstrs := []string{
		"clear daemon_meta " + daemon.MetaKeyReplayPausedUntil,
		"remove manual pause marker",
	}
	for _, want := range wantSubstrs {
		found := false
		for _, action := range plan.Actions {
			if strings.Contains(action, want) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("dry-run plan.Actions missing %q: %v", want, plan.Actions)
		}
	}
	if plan.ManualMarkerPath == "" {
		t.Fatalf("plan.ManualMarkerPath empty in dry-run output")
	}
	if plan.GitDir == "" {
		t.Fatalf("plan.GitDir empty in dry-run output")
	}
}

func TestRecover_RefusesWithDaemonAlive(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID:  os.Getpid(),
		Mode: "running",
	}); err != nil {
		t.Fatalf("SaveDaemonState: %v", err)
	}
	var out bytes.Buffer
	err := runRecover(ctx, &out, repo, true, false, true, false)
	if err == nil || !strings.Contains(err.Error(), "refusing while daemon") {
		t.Fatalf("runRecover err=%v want daemon refusal", err)
	}
}
