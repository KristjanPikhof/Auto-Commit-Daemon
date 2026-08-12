package installer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

// ScratchSelfTest proves watcher-independent checkpointing, deterministic
// publication, restore, undo, Git object integrity, and SQLite integrity
// through the managed binary before setup commits.
func ScratchSelfTest(ctx context.Context, plan Plan) (returnErr error) {
	root, err := os.MkdirTemp(plan.BackupRoot, "scratch-")
	if err != nil {
		return err
	}
	defer func() {
		if returnErr == nil {
			_ = os.RemoveAll(root)
		}
	}()
	// Darwin limits Unix socket paths to roughly 104 bytes. Setup operation
	// directories are intentionally descriptive and may already exceed that
	// before the supervisor and worker socket names are appended. Keep every
	// scratch artifact in the operation directory, but reach it through a
	// private short-lived alias while subprocesses are running.
	aliasDir, err := os.MkdirTemp("/tmp", "acd-st-")
	if err != nil {
		return fmt.Errorf("setup self-test: create short runtime directory: %w", err)
	}
	defer os.RemoveAll(aliasDir)
	runtimeRoot := filepath.Join(aliasDir, "r")
	if err := os.Symlink(root, runtimeRoot); err != nil {
		return fmt.Errorf("setup self-test: create short runtime alias: %w", err)
	}
	home := filepath.Join(runtimeRoot, "home")
	scratchRoots := paths.Roots{State: filepath.Join(runtimeRoot, "state", "acd"), Share: filepath.Join(runtimeRoot, "data", "acd"), Config: filepath.Join(runtimeRoot, "config", "acd")}
	repo := filepath.Join(runtimeRoot, "repo")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		return err
	}
	if err := gitpkg.Init(ctx, repo); err != nil {
		return err
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		return err
	}
	file := filepath.Join(repo, "file.txt")
	if err := os.WriteFile(file, []byte("one\n"), 0o644); err != nil {
		return err
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "add", "file.txt"); err != nil {
		return err
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "-c", "user.name=ACD Self Test", "-c", "user.email=selftest@localhost", "commit", "-m", "seed"); err != nil {
		return err
	}
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		return err
	}
	db, err := state.Open(ctx, state.DBPathFromGitDir(wt.GitDir))
	if err != nil {
		return err
	}
	store := checkpoint.Store{DB: db}
	checker := gitpkg.NewIgnoreChecker(repo)
	entries, exclusions, _, err := daemon.ScanProtectedEntries(ctx, repo, daemon.CaptureOpts{IgnoreChecker: checker, CheckpointStore: &store})
	_ = checker.Close()
	if err != nil {
		db.Close()
		return err
	}
	head, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		db.Close()
		return err
	}
	initial, err := store.Create(ctx, checkpoint.Request{RepoRoot: repo, WorktreeID: checkpoint.WorktreeID(repo), Reason: state.CheckpointReasonMigration, ObservationEpoch: 1, CoverageEpoch: 1, ObservedHead: head, ObservedRef: "refs/heads/main", Entries: entries, Exclusions: exclusions})
	if err != nil {
		db.Close()
		return err
	}
	if err := state.MetaSetMany(ctx, db, map[string]string{daemon.MetaKeyProtectionObservationEpoch: "1", daemon.MetaKeyProtectionCoveredEpoch: "1", daemon.MetaKeyProtectionCheckpointID: initial.Checkpoint.ID, daemon.MetaKeyProtectionComplete: "true"}); err != nil {
		db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	registry := central.NewRegistry()
	registration, err := registry.RegisterResolvedRepo(wt, "selftest", time.Now().Unix())
	if err != nil {
		return err
	}
	if err := central.Save(scratchRoots, registry); err != nil {
		return err
	}
	if err := persistFreshDefaults(scratchRoots, registration.Record.WorktreeID); err != nil {
		return err
	}
	command := exec.CommandContext(ctx, plan.ManagedBinary, "internal", "supervisor", "run")
	command.Env = append(os.Environ(), "HOME="+home, "XDG_STATE_HOME="+filepath.Join(runtimeRoot, "state"), "XDG_DATA_HOME="+filepath.Join(runtimeRoot, "data"), "XDG_CONFIG_HOME="+filepath.Join(runtimeRoot, "config"), "ACD_FSNOTIFY_ENABLED=0")
	logPath := filepath.Join(root, "supervisor.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("setup self-test: open supervisor log: %w", err)
	}
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("setup self-test: start supervisor: %w", err)
	}
	processDone := make(chan struct{})
	var processErr error
	go func() {
		processErr = command.Wait()
		close(processDone)
	}()
	defer func() {
		stopSelfTestProcess(command, processDone)
		_ = logFile.Close()
	}()
	if err := waitSupervisorReady(ctx, scratchRoots, registration.Record.RepositoryID, processDone); err != nil {
		stopSelfTestProcess(command, processDone)
		_ = logFile.Sync()
		detail := readSelfTestLog(logPath)
		return fmt.Errorf("setup self-test: %w (scratch=%s, process=%v, log=%q)",
			err, root, processErr, detail)
	}
	client := supervisor.Client{SocketPath: scratchRoots.SupervisorSocketPath(), Timeout: 45 * time.Second}
	if err := os.WriteFile(file, []byte("two\n"), 0o644); err != nil {
		return err
	}
	if _, err := selfTestRequest(ctx, client, registration.Record, "checkpoint_barrier", nil); err != nil {
		return err
	}
	if err := os.WriteFile(file, []byte("three\n"), 0o644); err != nil {
		return err
	}
	drainParams, _ := json.Marshal(map[string]bool{"drain_publication": true})
	if _, err := selfTestRequest(ctx, client, registration.Record, "checkpoint_barrier", drainParams); err != nil {
		return err
	}
	// Keep publication from racing the restore assertions. The restore itself
	// still runs through the real worker and operation gate; this hold only
	// prevents the restoration checkpoint from being published between the
	// apply response and the byte-for-byte HEAD/index checks below.
	holdPath := scratchRoots.SetupPublicationHoldPath()
	if err := os.WriteFile(holdPath, []byte("self-test restore verification\n"), 0o600); err != nil {
		return fmt.Errorf("setup self-test: hold publication for restore verification: %w", err)
	}
	defer os.Remove(holdPath)
	headBefore, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		return err
	}
	if headBefore == head {
		return errors.New("setup self-test: deterministic publication did not advance Git history")
	}
	indexBefore, err := os.ReadFile(filepath.Join(wt.GitDir, "index"))
	if err != nil {
		return err
	}
	params, _ := json.Marshal(map[string]string{"id": initial.Checkpoint.ID})
	preview, err := selfTestRequest(ctx, client, registration.Record, "restore_plan", params)
	if err != nil {
		return err
	}
	var planData struct {
		PlanDigest string `json:"plan_digest"`
		CanApply   bool   `json:"can_apply"`
	}
	if err := decodeInto(preview.Data, &planData); err != nil || !planData.CanApply {
		return fmt.Errorf("setup self-test: restore preview invalid: %w", err)
	}
	params, _ = json.Marshal(map[string]string{"id": initial.Checkpoint.ID, "plan_digest": planData.PlanDigest})
	applied, err := selfTestRequest(ctx, client, registration.Record, "restore_apply", params)
	if err != nil {
		return err
	}
	var restoreResult struct {
		UndoCheckpoint string `json:"undo_checkpoint"`
	}
	if err := decodeInto(applied.Data, &restoreResult); err != nil || restoreResult.UndoCheckpoint == "" {
		return fmt.Errorf("setup self-test: restore result invalid: %w", err)
	}
	if headAfter, _ := gitpkg.RevParse(ctx, repo, "HEAD"); headAfter != headBefore {
		return errors.New("setup self-test: restore moved HEAD")
	}
	if indexAfter, readErr := os.ReadFile(filepath.Join(wt.GitDir, "index")); readErr != nil || string(indexAfter) != string(indexBefore) {
		return errors.New("setup self-test: restore changed index")
	}
	params, _ = json.Marshal(map[string]string{"id": restoreResult.UndoCheckpoint})
	undoPreview, err := selfTestRequest(ctx, client, registration.Record, "restore_plan", params)
	if err != nil {
		return err
	}
	if err := decodeInto(undoPreview.Data, &planData); err != nil {
		return err
	}
	params, _ = json.Marshal(map[string]string{"id": restoreResult.UndoCheckpoint, "plan_digest": planData.PlanDigest})
	if _, err := selfTestRequest(ctx, client, registration.Record, "restore_apply", params); err != nil {
		return err
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "fsck", "--no-dangling"); err != nil {
		return err
	}
	return state.QuickCheck(ctx, state.DBPathFromGitDir(wt.GitDir))
}

func stopSelfTestProcess(command *exec.Cmd, done <-chan struct{}) {
	select {
	case <-done:
		return
	default:
	}
	if command.Process != nil {
		_ = command.Process.Signal(os.Interrupt)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		if command.Process != nil {
			_ = command.Process.Kill()
		}
		<-done
	}
}

func readSelfTestLog(path string) string {
	const limit = int64(64 << 10)
	file, err := os.Open(path)
	if err != nil {
		return "unavailable: " + err.Error()
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "unavailable: " + err.Error()
	}
	if info.Size() > limit {
		if _, err := file.Seek(info.Size()-limit, 0); err != nil {
			return "unavailable: " + err.Error()
		}
	}
	body, err := io.ReadAll(file)
	if err != nil {
		return "unavailable: " + err.Error()
	}
	return strings.TrimSpace(string(body))
}

func selfTestRequest(ctx context.Context, client supervisor.Client, record central.RepoRecord, method string, params json.RawMessage) (supervisor.Response, error) {
	request := supervisor.Request{Version: supervisor.ProtocolVersion, ID: fmt.Sprintf("selftest-%s-%d", method, time.Now().UnixNano()), Method: method, RepositoryID: record.RepositoryID, WorktreeID: record.WorktreeID, DeadlineMS: time.Now().Add(45 * time.Second).UnixMilli(), Params: params}
	response, err := client.Do(ctx, request)
	if err != nil {
		return response, fmt.Errorf("setup self-test %s: %w", method, err)
	}
	if response.Error != nil {
		return response, fmt.Errorf("setup self-test %s: %s", method, response.Error.Message)
	}
	return response, nil
}
func decodeInto(value any, target any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, target)
}
