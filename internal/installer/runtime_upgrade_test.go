package installer

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/migration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version"
)

func TestShouldUpgradeRuntimeUsesCompatibilityAndOrdering(t *testing.T) {
	compatibility := RuntimeCompatibility()
	priorSchema := compatibility
	priorSchema.StateSchemaVersion--
	futureSchema := compatibility
	futureSchema.StateSchemaVersion++
	options := RuntimeUpgradeOptions{SourceVersion: "v2026-08-07-180-gabcdef0", Compatibility: compatibility}
	tests := []struct {
		name   string
		status supervisor.Status
		want   bool
		err    string
	}{
		{name: "newer source", status: supervisor.Status{Version: "v2026-08-07-179-g1234567", BinaryDigest: "old", Compatibility: compatibility}, want: true},
		{name: "same build", status: supervisor.Status{Version: options.SourceVersion, BinaryDigest: "source", Compatibility: compatibility}},
		{name: "same version new bytes", status: supervisor.Status{Version: options.SourceVersion, BinaryDigest: "other", Compatibility: compatibility}, want: true},
		{name: "newer runtime", status: supervisor.Status{Version: "v2026-08-07-181-g1234567", BinaryDigest: "new", Compatibility: compatibility}},
		{name: "additive schema upgrade", status: supervisor.Status{Version: options.SourceVersion, BinaryDigest: "source", Compatibility: priorSchema}, want: true},
		{name: "schema downgrade", status: supervisor.Status{Version: "v2026-08-07-181-g1234567", BinaryDigest: "new", Compatibility: futureSchema}, err: "run `acd setup` once"},
		{name: "legacy runtime", status: supervisor.Status{Version: "v2026-08-07-179-g1234567"}, err: "run `acd setup` once"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := shouldUpgradeRuntime(test.status, "source", options)
			if got != test.want || test.err == "" && err != nil || test.err != "" && (err == nil || !strings.Contains(err.Error(), test.err)) {
				t.Fatalf("shouldUpgradeRuntime=(%v,%v), want (%v,%q)", got, err, test.want, test.err)
			}
		})
	}
	legacy := supervisor.Status{Version: "v2026-08-07-179-g1234567"}
	legacyOptions := options
	legacyOptions.AllowUnadvertised = true
	if upgrade, err := shouldUpgradeRuntime(
		legacy, "source", legacyOptions); err != nil || !upgrade {
		t.Fatalf("probed legacy upgrade=(%t,%v), want true,nil", upgrade, err)
	}
	divergent := supervisor.Status{
		Version: "v2026-08-07-180-g1234567", BinaryDigest: "other",
		Compatibility: compatibility,
	}
	if upgrade, err := shouldUpgradeRuntime(
		divergent, "source", options); err == nil || upgrade {
		t.Fatalf("automatic divergent upgrade=(%t,%v), want false,error", upgrade, err)
	}
	setupOptions := options
	setupOptions.AllowSameDistanceReplacement = true
	if upgrade, err := shouldUpgradeRuntime(
		divergent, "source", setupOptions); err != nil || !upgrade {
		t.Fatalf("setup divergent upgrade=(%t,%v), want true,nil", upgrade, err)
	}
}

func TestCompatibleRuntimeReadinessAllowsAdmissionLimitedStartup(t *testing.T) {
	if supervisor.CheckpointBarrierTimeout <= time.Minute {
		t.Fatalf("compatible runtime readiness timeout=%s, want more than one minute",
			supervisor.CheckpointBarrierTimeout)
	}
}

func TestBuildPlanUsesBoundedCompatibleUpgrade(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS session supervisor plan")
	}
	root, err := os.MkdirTemp("/tmp", "acd-upgrade-plan-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	roots := paths.Roots{State: filepath.Join(root, "state", "acd"), Share: filepath.Join(root, "share", "acd"), Config: filepath.Join(root, "config", "acd")}
	executable := filepath.Join(root, "acd")
	if err := os.WriteFile(executable, []byte("compatible"), 0o755); err != nil {
		t.Fatal(err)
	}
	digest, err := version.FileDigest(executable)
	if err != nil {
		t.Fatal(err)
	}
	serveRuntimeStatus(t, roots, supervisor.Status{PID: os.Getpid(), Version: version.String(), BinaryDigest: digest,
		Ownership: userOwnershipForTest(), Compatibility: RuntimeCompatibility()})

	plan, err := BuildPlan(context.Background(), roots, Options{Executable: executable, Integrations: "none", SkipServiceCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Mode != "compatible_upgrade" || len(plan.Repositories) != 0 {
		t.Fatalf("plan mode=%q repositories=%d, want bounded compatible upgrade", plan.Mode, len(plan.Repositories))
	}
	if plan.Scope != "global" || plan.RepositoryID != "" || plan.WorktreeID != "" || plan.Repo != "" {
		t.Fatalf("compatible plan is not global: %+v", plan)
	}
	if plan.RequiresExpected || len(plan.Actions) != 1 || plan.Actions[0].Kind != "verify_compatible_runtime" {
		t.Fatalf("current compatible plan should be a no-op: %+v", plan.Actions)
	}
	for _, action := range plan.Actions {
		if action.Kind == "migrate" || action.Kind == "self_test" {
			t.Fatalf("compatible plan contains full setup action: %+v", action)
		}
	}
}

func TestCheckpointUpgradeRepositoriesDoesNotDrainPublication(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "acd-upgrade-checkpoint-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	roots := paths.Roots{State: filepath.Join(root, "state", "acd")}
	if err := os.MkdirAll(filepath.Dir(roots.SupervisorSocketPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", roots.SupervisorSocketPath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	requestC := make(chan supervisor.Request, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		var request supervisor.Request
		if json.NewDecoder(bufio.NewReader(conn)).Decode(&request) != nil {
			return
		}
		requestC <- request
		_ = json.NewEncoder(conn).Encode(supervisor.Response{
			Version: supervisor.ProtocolVersion, ID: request.ID, OK: true,
			Data: map[string]any{"protected": true},
		})
	}()

	registry := central.NewRegistry()
	registry.Repos = []central.RepoRecord{{
		Path: "/detached-worktree", RepositoryID: "repository", WorktreeID: "worktree",
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := checkpointUpgradeRepositories(ctx, roots, registry); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-requestC:
		if request.Method != "checkpoint_barrier" {
			t.Fatalf("method=%q want checkpoint_barrier", request.Method)
		}
		var params struct {
			DrainPublication bool `json:"drain_publication"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil {
			t.Fatal(err)
		}
		if params.DrainPublication {
			t.Fatal("compatible runtime upgrade must protect state without draining Git publication")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestCheckpointUpgradeRepositoriesUsesCleanGitDurabilityWhenWorkerIsDown(
	t *testing.T,
) {
	ctx := context.Background()
	repo := t.TempDir()
	if err := git.Init(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo},
		"symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "tracked.txt"), []byte("safe\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "add", "tracked.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo},
		"-c", "user.name=ACD Test", "-c", "user.email=acd@test.invalid",
		"commit", "-m", "Add tracked file"); err != nil {
		t.Fatal(err)
	}
	record := central.RepoRecord{
		Path: repo, RepositoryID: "0123456789abcdef",
		WorktreeID: "fedcba9876543210",
	}
	registry := central.NewRegistry()
	registry.Repos = []central.RepoRecord{record}
	roots := paths.Roots{State: filepath.Join(t.TempDir(), "state", "acd")}

	if err := checkpointUpgradeRepositories(ctx, roots, registry); err != nil {
		t.Fatalf("clean worker-down upgrade proof: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repo, "dirty.txt"), []byte("unsafe\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := checkpointUpgradeRepositories(ctx, roots, registry)
	if err == nil || !strings.Contains(err.Error(), "uncheckpointed changes") {
		t.Fatalf("dirty worker-down upgrade error=%v", err)
	}

	stateDB := filepath.Join(repo, ".git", "acd", "state.db")
	db, err := state.Open(ctx, stateDB)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := state.Checkpoint{
		ID:          "cp-1786517000000-0123456789abcdef",
		OperationID: "op-runtime-upgrade-protection",
		WorktreeID:  record.WorktreeID, Reason: state.CheckpointReasonManualBarrier,
		ObservationEpoch: 7, CoverageEpoch: 7, ObservedHead: "head",
		ObservedRef: "refs/heads/main", TreeOID: "tree", CommitOID: "commit",
		Ref: "refs/acd/checkpoints/v1/" + record.WorktreeID +
			"/cp-1786517000000-0123456789abcdef",
		CreatedTS: 1,
	}
	if created, err := state.PrepareCheckpoint(ctx, db, checkpoint,
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"); err != nil || !created {
		t.Fatalf("prepare protection checkpoint=(%t,%v)", created, err)
	}
	if err := state.CompleteCheckpoint(
		ctx, db, checkpoint.ID, checkpoint.Ref, checkpoint.CommitOID, 2); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSetMany(ctx, db, map[string]string{
		"protection.complete":          "true",
		"protection.observation_epoch": "7",
		"protection.covered_epoch":     "7",
		"protection.checkpoint_id":     checkpoint.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	registry.Repos[0].StateDB = stateDB
	if err := checkpointUpgradeRepositories(ctx, roots, registry); err != nil {
		t.Fatalf("protected dirty worker-down upgrade proof: %v", err)
	}
}

func TestBackupCompatibleRuntimeStateCoversEnabledSafeMigrations(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dbPath := filepath.Join(root, "repo", "state.db")
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `PRAGMA user_version = 20`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
DROP TABLE publication_drain_events;
DROP TABLE publication_drains;`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	registry := central.NewRegistry()
	registry.Repos = []central.RepoRecord{{
		Path: "/repo", StateDB: dbPath, RepositoryID: "0123456789abcdef",
		WorktreeID: "fedcba9876543210",
	}}
	plans, err := backupCompatibleRuntimeState(ctx, registry,
		filepath.Join(root, "backups"), state.SchemaVersion)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 1 || plans[0].FromVersion != 20 {
		t.Fatalf("plans=%+v, want one v20 backup", plans)
	}
	if version, err := state.ReadUserVersion(ctx, plans[0].BackupPath); err != nil || version != 20 {
		t.Fatalf("backup version=(%d,%v), want 20", version, err)
	}
	migrated, err := state.OpenRuntime(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	var migratedTable string
	if err := migrated.ReadSQL().QueryRowContext(ctx, `
SELECT name FROM sqlite_master WHERE type='table' AND name='publication_drains'`).
		Scan(&migratedTable); err != nil || migratedTable != "publication_drains" {
		t.Fatalf("migrated table=(%q,%v)", migratedTable, err)
	}
	if err := migrated.Close(); err != nil {
		t.Fatal(err)
	}
	if err := migration.Rollback(plans); err != nil {
		t.Fatal(err)
	}
	if version, err := state.ReadUserVersion(ctx, dbPath); err != nil || version != 20 {
		t.Fatalf("rolled back version=(%d,%v), want 20", version, err)
	}
	rolledBack, err := state.OpenReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer rolledBack.Close()
	if err := rolledBack.ReadSQL().QueryRowContext(ctx, `
SELECT name FROM sqlite_master WHERE type='table' AND name='publication_drains'`).
		Scan(&migratedTable); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rollback retained v21 table: err=%v", err)
	}
}

func serveRuntimeStatus(t *testing.T, roots paths.Roots, status supervisor.Status) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(roots.SupervisorSocketPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", roots.SupervisorSocketPath())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			var request supervisor.Request
			if json.NewDecoder(bufio.NewReader(conn)).Decode(&request) == nil {
				_ = json.NewEncoder(conn).Encode(supervisor.Response{Version: supervisor.ProtocolVersion, ID: request.ID, OK: true, Data: status})
			}
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-done })
}

func gitCommand(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	if err != nil {
		return err.Error() + ": " + strings.TrimSpace(string(output))
	}
	return ""
}

func userOwnershipForTest() string { return "user:" + strconv.Itoa(os.Getuid()) }
