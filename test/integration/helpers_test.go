//go:build integration
// +build integration

// Package integration_test composes the production stack end-to-end with
// real subprocesses (`acd` binary) and real git worktrees. The build tag
// keeps the package out of the default test run; invoke with
//
//	go test ./test/integration/... -tags=integration -race -count=1
//
// (per §14.3). Helpers live here; one *_test.go file per scenario family.
package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

const (
	integrationSupervisorOwnership = "ACD_INTERNAL_SUPERVISOR_OWNERSHIP="
	checkpointWatchdogMode         = "ACD_INTEGRATION_CHECKPOINT_WATCHDOG"
	checkpointWatchdogParentPID    = "ACD_INTEGRATION_CHECKPOINT_PARENT_PID"
	checkpointWatchdogParentStart  = "ACD_INTEGRATION_CHECKPOINT_PARENT_START"
	checkpointWatchdogParentArgv   = "ACD_INTEGRATION_CHECKPOINT_PARENT_ARGV"
	checkpointWatchdogRegistry     = "ACD_INTEGRATION_CHECKPOINT_REGISTRY"
)

type checkpointRuntimeLimiter chan struct{}

type checkpointProcessIdentity struct {
	PID        int    `json:"pid"`
	StartTime  string `json:"start_time"`
	ArgvHash   string `json:"argv_hash"`
	Unregister bool   `json:"unregister,omitempty"`
}

func (process checkpointProcessIdentity) fingerprint() identity.Fingerprint {
	return identity.Fingerprint{StartTime: process.StartTime, ArgvHash: process.ArgvHash}
}

func (limiter checkpointRuntimeLimiter) acquire() func() {
	limiter <- struct{}{}
	var once sync.Once
	return func() { once.Do(func() { <-limiter }) }
}

var (
	checkpointRuntimeStartSlots = make(checkpointRuntimeLimiter, 4)
	checkpointWatchdogOnce      sync.Once
	checkpointWatchdogCommand   *exec.Cmd
	checkpointWatchdogIdentity  checkpointProcessIdentity
	checkpointWatchdogDir       string
	checkpointWatchdogFile      string
	checkpointWatchdogErr       error
	checkpointWatchdogMu        sync.Mutex
)

func ensureCheckpointRuntime(t *testing.T, env []string, repo, bin string) {
	ensureCheckpointRuntimeWithMode(t, env, repo, bin, false)
}

func ensureProductionCheckpointRuntime(t *testing.T, env []string, repo, bin string) {
	ensureCheckpointRuntimeWithMode(t, env, repo, bin, true)
}

func ensureCheckpointRuntimeWithMode(t *testing.T, env []string, repo, bin string, productionSession bool) {
	t.Helper()
	roots, repositoryID := prepareCheckpointRegistration(t, env, repo)
	if err := os.MkdirAll(filepath.Dir(roots.ManagedBinaryPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(roots.ManagedBinaryPath()); errors.Is(err, os.ErrNotExist) {
		if err := os.Link(bin, roots.ManagedBinaryPath()); err != nil {
			t.Fatalf("install integration managed binary: %v", err)
		}
		if err := os.Chmod(roots.ManagedBinaryPath(), 0o755); err != nil {
			t.Fatal(err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	workerSocket := supervisor.WorkerSocketPath(roots, repositoryID)
	if _, err := os.Stat(workerSocket); err == nil {
		assertCheckpointRuntimeOwnership(t, roots)
		return
	}
	if _, err := os.Stat(roots.SupervisorSocketPath()); err == nil {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(workerSocket); err == nil {
				assertCheckpointRuntimeOwnership(t, roots)
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatalf("integration supervisor did not reconcile worker %s", repositoryID)
	}
	releaseRuntimeSlot := checkpointRuntimeStartSlots.acquire()
	// Registered before runtime cleanup so LIFO ordering shuts down the
	// supervisor before another test may acquire this capacity.
	t.Cleanup(releaseRuntimeSlot)
	if productionSession && runtime.GOOS == "darwin" {
		startCheckpointSessionRuntime(t, roots, workerSocket)
		return
	}
	startCheckpointSupervisorRuntime(t, env, roots, repositoryID, workerSocket)
}

func startCheckpointSupervisorRuntime(t *testing.T, env []string, roots paths.Roots, repositoryID, workerSocket string) {
	t.Helper()
	if err := os.MkdirAll(roots.State, 0o700); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(roots.State, "integration-supervisor.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(roots.ManagedBinaryPath(), "internal", "supervisor", "run")
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	command.Env = supervisor.ProcessEnvironment(roots, env)
	if runtime.GOOS == "darwin" {
		command.Env = append(command.Env, integrationSupervisorOwnership+checkpointUserOwnership())
	}
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("start integration supervisor: %v", err)
	}
	process, err := captureOwnedCheckpointProcess(command, captureCheckpointProcessIdentity)
	if err != nil {
		_ = logFile.Close()
		t.Fatalf("capture integration supervisor identity: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	_ = logFile.Close()
	watchdogRegistered := false
	t.Cleanup(func() {
		shutdownCheckpointRuntime(t, roots, workerSocket, process)
		waitCheckpointSupervisorCommand(t, done)
		if matches, matchErr := checkpointProcessMatches(process); watchdogRegistered &&
			(matchErr == nil && !matches || matchErr != nil && !identity.Alive(process.PID)) {
			unregisterCheckpointRuntimeWatchdog(process)
		}
	})
	registerCheckpointRuntimeWatchdog(t, process)
	watchdogRegistered = true
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(workerSocket); err == nil {
			assertCheckpointRuntimeOwnership(t, roots)
			return
		}
		select {
		case err := <-done:
			body, _ := os.ReadFile(logPath)
			t.Fatalf("integration supervisor exited before worker %s was ready: %v\n%s", repositoryID, err, body)
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	body, _ := os.ReadFile(logPath)
	t.Fatalf("integration supervisor did not become ready: %s", body)
}

func startCheckpointSessionRuntime(t *testing.T, roots paths.Roots, workerSocket string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := supervisor.EnsureSession(ctx, roots, roots.ManagedBinaryPath(), roots.SupervisorLogPath()); err != nil {
		t.Fatalf("start production session-owned integration supervisor: %v", err)
	}
	status, err := checkpointRuntimeStatus(ctx, roots)
	if err != nil {
		t.Fatalf("inspect production session-owned integration supervisor: %v", err)
	}
	process := checkpointProcessIdentity{PID: status.PID}
	watchdogRegistered := false
	t.Cleanup(func() {
		shutdownCheckpointRuntime(t, roots, workerSocket, process)
		if matches, matchErr := checkpointProcessMatches(process); watchdogRegistered &&
			(matchErr == nil && !matches || matchErr != nil && !identity.Alive(process.PID)) {
			unregisterCheckpointRuntimeWatchdog(process)
		}
	})
	process, err = captureCheckpointProcessIdentity(status.PID)
	if err != nil {
		t.Fatalf("capture production session supervisor identity: %v", err)
	}
	registerCheckpointRuntimeWatchdog(t, process)
	watchdogRegistered = true
	waitFor(t, "session-owned integration worker ready", 10*time.Second, func() bool {
		_, err := os.Stat(workerSocket)
		return err == nil
	})
	assertCheckpointRuntimeOwnership(t, roots)
}

func assertCheckpointRuntimeOwnership(t *testing.T, roots paths.Roots) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	status, err := checkpointRuntimeStatus(ctx, roots)
	if err != nil {
		t.Fatalf("inspect integration supervisor ownership: %v", err)
	}
	if status.Ownership != checkpointUserOwnership() {
		t.Fatalf("integration supervisor ownership=%q, want %q", status.Ownership, checkpointUserOwnership())
	}
}

func checkpointRuntimeStatus(ctx context.Context, roots paths.Roots) (supervisor.Status, error) {
	response, err := (supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 5 * time.Second}).Do(ctx,
		supervisor.Request{Version: supervisor.ProtocolVersion, ID: "integration-ownership", Method: "status"})
	if err != nil {
		return supervisor.Status{}, err
	}
	if response.Error != nil {
		return supervisor.Status{}, errors.New(response.Error.Message)
	}
	raw, err := json.Marshal(response.Data)
	if err != nil {
		return supervisor.Status{}, err
	}
	var status supervisor.Status
	if err := json.Unmarshal(raw, &status); err != nil {
		return supervisor.Status{}, err
	}
	return status, nil
}

func shutdownCheckpointRuntime(t *testing.T, roots paths.Roots, workerSocket string, process checkpointProcessIdentity) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	response, err := (supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 15 * time.Second}).Do(ctx,
		supervisor.Request{Version: supervisor.ProtocolVersion, ID: "integration-shutdown", Method: "shutdown"})
	cancel()
	shutdownErr := err
	if shutdownErr == nil && response.Error != nil {
		shutdownErr = errors.New(response.Error.Message)
	}
	if shutdownErr == nil {
		raw, marshalErr := json.Marshal(response.Data)
		var status supervisor.ShutdownStatus
		if marshalErr != nil {
			shutdownErr = marshalErr
		} else if decodeErr := json.Unmarshal(raw, &status); decodeErr != nil {
			shutdownErr = decodeErr
		} else if !status.Stopped {
			shutdownErr = errors.New("shutdown response did not prove stopped")
		}
	}
	if shutdownErr != nil {
		_ = signalCheckpointProcess(process, syscall.SIGTERM)
	}
	if waitCheckpointRuntimeStopped(roots, workerSocket, process, 10*time.Second) {
		return
	}
	_ = signalCheckpointProcess(process, syscall.SIGKILL)
	if waitCheckpointRuntimeStopped(roots, workerSocket, process, 5*time.Second) {
		return
	}
	if shutdownErr != nil {
		t.Errorf("shut down integration supervisor: %v", shutdownErr)
	}
	t.Errorf("integration supervisor shutdown left pid %d or runtime sockets behind", process.PID)
}

func waitCheckpointRuntimeStopped(roots paths.Roots, workerSocket string, process checkpointProcessIdentity, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, supervisorErr := os.Stat(roots.SupervisorSocketPath())
		_, workerErr := os.Stat(workerSocket)
		if errors.Is(supervisorErr, os.ErrNotExist) && errors.Is(workerErr, os.ErrNotExist) {
			matches, err := checkpointProcessMatches(process)
			if err == nil && !matches || err != nil && !identity.Alive(process.PID) {
				return true
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

func waitCheckpointSupervisorCommand(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case <-done:
		return
	case <-time.After(5 * time.Second):
		t.Error("integration supervisor process was not reaped")
	}
}

func prepareCheckpointRegistration(t *testing.T, env []string, repo string) (paths.Roots, string) {
	t.Helper()
	home := envValue(env, "HOME")
	if home == "" {
		t.Fatal("checkpoint integration runtime requires HOME")
	}
	rootFor := func(name string, fallback ...string) string {
		value := envValue(env, name)
		if value != "" && filepath.IsAbs(value) {
			return value
		}
		return filepath.Join(append([]string{home}, fallback...)...)
	}
	roots := paths.Roots{
		State:  filepath.Join(rootFor("XDG_STATE_HOME", ".local", "state"), "acd"),
		Share:  filepath.Join(rootFor("XDG_DATA_HOME", ".local", "share"), "acd"),
		Config: filepath.Join(rootFor("XDG_CONFIG_HOME", ".config"), "acd"),
	}
	wt, err := gitpkg.ResolveWorktree(context.Background(), repo)
	if err != nil {
		t.Fatalf("resolve checkpoint integration worktree: %v", err)
	}
	db, err := state.Open(context.Background(), state.DBPathFromGitDir(wt.GitDir))
	if err != nil {
		t.Fatalf("prepare v20 integration state: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var repositoryID string
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		registry.Version = central.RegistryVersion
		result, err := registry.RegisterResolvedRepo(wt, "integration", time.Now().Unix())
		repositoryID = result.Record.RepositoryID
		return err
	}); err != nil {
		t.Fatalf("register checkpoint integration repo: %v", err)
	}
	return roots, repositoryID
}

func envValue(env []string, name string) string {
	prefix := name + "="
	for i := len(env) - 1; i >= 0; i-- {
		if strings.HasPrefix(env[i], prefix) {
			return strings.TrimPrefix(env[i], prefix)
		}
	}
	return ""
}

func registerCheckpointRuntimeWatchdog(t *testing.T, process checkpointProcessIdentity) {
	t.Helper()
	if err := startCheckpointRuntimeWatchdog(); err != nil {
		_ = signalCheckpointProcess(process, syscall.SIGTERM)
		t.Fatalf("start integration supervisor watchdog: %v", err)
	}
	if err := appendCheckpointWatchdogProcess(process); err != nil {
		t.Fatalf("register integration supervisor watchdog PID: %v", err)
	}
}

func startCheckpointRuntimeWatchdog() error {
	checkpointWatchdogOnce.Do(func() {
		checkpointWatchdogDir, checkpointWatchdogErr = os.MkdirTemp("", "acd-integration-watchdog-*")
		if checkpointWatchdogErr != nil {
			return
		}
		checkpointWatchdogFile = filepath.Join(checkpointWatchdogDir, "supervisors")
		parent, err := captureCheckpointProcessIdentity(os.Getpid())
		if err != nil {
			checkpointWatchdogErr = err
			return
		}
		checkpointWatchdogCommand = exec.Command(os.Args[0], "-test.run=^$")
		checkpointWatchdogCommand.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		checkpointWatchdogCommand.Env = append(os.Environ(),
			checkpointWatchdogMode+"=1",
			checkpointWatchdogParentPID+"="+strconv.Itoa(parent.PID),
			checkpointWatchdogParentStart+"="+parent.StartTime,
			checkpointWatchdogParentArgv+"="+parent.ArgvHash,
			checkpointWatchdogRegistry+"="+checkpointWatchdogFile,
		)
		checkpointWatchdogIdentity, checkpointWatchdogErr =
			startOwnedCheckpointWatchdog(checkpointWatchdogCommand, captureCheckpointProcessIdentity)
	})
	return checkpointWatchdogErr
}

func unregisterCheckpointRuntimeWatchdog(process checkpointProcessIdentity) {
	process.Unregister = true
	_ = appendCheckpointWatchdogProcess(process)
}

func appendCheckpointWatchdogProcess(process checkpointProcessIdentity) error {
	checkpointWatchdogMu.Lock()
	defer checkpointWatchdogMu.Unlock()
	file, err := os.OpenFile(checkpointWatchdogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	body, err := json.Marshal(process)
	if err != nil {
		_ = file.Close()
		return err
	}
	if _, err := fmt.Fprintln(file, string(body)); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func stopCheckpointRuntimeWatchdog() {
	command := checkpointWatchdogCommand
	if command == nil || command.Process == nil {
		if checkpointWatchdogDir != "" {
			_ = os.RemoveAll(checkpointWatchdogDir)
		}
		return
	}
	_ = signalCheckpointProcess(checkpointWatchdogIdentity, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_ = command.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = signalCheckpointProcess(checkpointWatchdogIdentity, syscall.SIGKILL)
		<-done
	}
	_ = os.RemoveAll(checkpointWatchdogDir)
}

func runCheckpointRuntimeWatchdog() bool {
	if os.Getenv(checkpointWatchdogMode) != "1" {
		return false
	}
	parentPID, parentErr := strconv.Atoi(os.Getenv(checkpointWatchdogParentPID))
	parent := checkpointProcessIdentity{
		PID:       parentPID,
		StartTime: os.Getenv(checkpointWatchdogParentStart),
		ArgvHash:  os.Getenv(checkpointWatchdogParentArgv),
	}
	registry := os.Getenv(checkpointWatchdogRegistry)
	if parentErr != nil || parent.PID <= 0 || parent.fingerprint().Empty() || registry == "" {
		return true
	}
	for {
		matches, err := checkpointProcessMatches(parent)
		if err == nil && !matches || err != nil && !identity.Alive(parent.PID) {
			break
		}
		time.Sleep(time.Second)
	}
	body, err := os.ReadFile(registry)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return true
	}
	active := make(map[int]checkpointProcessIdentity)
	for _, line := range bytes.Split(body, []byte{'\n'}) {
		var process checkpointProcessIdentity
		if len(bytes.TrimSpace(line)) == 0 || json.Unmarshal(line, &process) != nil ||
			process.PID <= 0 || process.fingerprint().Empty() {
			continue
		}
		if process.Unregister {
			if registered, ok := active[process.PID]; ok &&
				identity.Match(registered.fingerprint(), process.fingerprint()) {
				delete(active, process.PID)
			}
			continue
		}
		active[process.PID] = process
	}
	var cleanup sync.WaitGroup
	for _, process := range active {
		cleanup.Add(1)
		go func() {
			defer cleanup.Done()
			cleanupCheckpointWatchdogProcess(
				process, 10*time.Second, 100*time.Millisecond,
				checkpointProcessMatches, identity.Alive, signalCheckpointProcess,
			)
		}()
	}
	cleanup.Wait()
	return true
}

func cleanupCheckpointWatchdogProcess(
	process checkpointProcessIdentity,
	termGrace, retryInterval time.Duration,
	matches func(checkpointProcessIdentity) (bool, error),
	alive func(int) bool,
	signal func(checkpointProcessIdentity, syscall.Signal) error,
) {
	termSent := false
	var escalateAt time.Time
	for {
		matched, err := matches(process)
		if err != nil {
			if !alive(process.PID) {
				return
			}
			time.Sleep(retryInterval)
			continue
		}
		if !matched {
			return
		}
		if !termSent {
			if err := signal(process, syscall.SIGTERM); err == nil {
				termSent = true
				escalateAt = time.Now().Add(termGrace)
			}
			time.Sleep(retryInterval)
			continue
		}
		if time.Now().Before(escalateAt) {
			time.Sleep(retryInterval)
			continue
		}
		// Revalidation is performed by signal. A transient refusal keeps the
		// identity in this loop so escalation is retried instead of leaked.
		_ = signal(process, syscall.SIGKILL)
		time.Sleep(retryInterval)
	}
}

func checkpointUserOwnership() string { return "user:" + strconv.Itoa(os.Getuid()) }

func captureCheckpointProcessIdentity(pid int) (checkpointProcessIdentity, error) {
	fingerprint, err := identity.CaptureContext(context.Background(), pid)
	if err != nil {
		return checkpointProcessIdentity{}, err
	}
	return checkpointProcessIdentity{PID: pid, StartTime: fingerprint.StartTime, ArgvHash: fingerprint.ArgvHash}, nil
}

func captureOwnedCheckpointProcess(
	command *exec.Cmd,
	capture func(int) (checkpointProcessIdentity, error),
) (checkpointProcessIdentity, error) {
	if command == nil || command.Process == nil {
		return checkpointProcessIdentity{}, errors.New("checkpoint runtime command is not started")
	}
	var captureErr error
	for attempt := 0; attempt < 5; attempt++ {
		process, err := capture(command.Process.Pid)
		if err == nil {
			return process, nil
		}
		captureErr = err
		if attempt < 4 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if command.SysProcAttr != nil && command.SysProcAttr.Setsid {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	} else {
		_ = command.Process.Kill()
	}
	waitErr := command.Wait()
	if waitErr != nil {
		return checkpointProcessIdentity{}, fmt.Errorf("%w (owned child stopped: %v)", captureErr, waitErr)
	}
	return checkpointProcessIdentity{}, captureErr
}

func startOwnedCheckpointWatchdog(
	command *exec.Cmd,
	capture func(int) (checkpointProcessIdentity, error),
) (checkpointProcessIdentity, error) {
	if command == nil {
		return checkpointProcessIdentity{}, errors.New("checkpoint watchdog command is nil")
	}
	if err := command.Start(); err != nil {
		return checkpointProcessIdentity{}, err
	}
	return captureOwnedCheckpointProcess(command, capture)
}

func checkpointProcessMatches(expected checkpointProcessIdentity) (bool, error) {
	actual, err := identity.CaptureContext(context.Background(), expected.PID)
	if err != nil {
		return false, err
	}
	return identity.Match(expected.fingerprint(), actual), nil
}

func signalCheckpointProcess(expected checkpointProcessIdentity, signal syscall.Signal) error {
	actual, err := identity.CaptureContext(context.Background(), expected.PID)
	if err != nil {
		return err
	}
	if !identity.Match(expected.fingerprint(), actual) {
		return errors.New("checkpoint runtime process identity changed")
	}
	return syscall.Kill(expected.PID, signal)
}

// activateIntentV2Runtime installs an explicit, already-applied Intent Fast v2
// revision before daemon startup. Integration tests may still use environment
// variables to customize provider and timing fields, but never rely on the
// unsupported env-only v1 startup path.
func activateIntentV2Runtime(t *testing.T, repo string, extra ...string) []string {
	t.Helper()
	values := make(map[string]string, len(extra))
	cleaned := make([]string, 0, len(extra))
	for _, item := range extra {
		name, value, ok := strings.Cut(item, "=")
		if !ok {
			cleaned = append(cleaned, item)
			continue
		}
		values[name] = value
		if name != ai.EnvCommitStrategy && name != "ACD_COMMIT_PRESET" {
			cleaned = append(cleaned, item)
		}
	}
	if _, configured := values[ai.EnvProvider]; !configured {
		values[ai.EnvProvider] = "deterministic"
		cleaned = append(cleaned, ai.EnvProvider+"=deterministic")
	}
	lookup := func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
	overrides := config.Overrides{}
	overrides[config.FieldCommitStrategy], _ = json.Marshal("intent")
	overrides[config.FieldCommitPreset], _ = json.Marshal("fast")
	resolved, preset, err := config.ResolveAll(config.ResolveInput{
		Repository: overrides, LookupEnv: lookup,
	}, overrides)
	if err != nil {
		t.Fatalf("resolve Intent v2 integration revision: %v", err)
	}
	snapshot := map[string]any{
		"preset_id": preset.ID(), "preset_version": preset.Version(),
		"customized": preset.Customized,
	}
	for _, field := range config.Catalog() {
		if field.Boundary == config.ApplyHot && field.Persistable &&
			!field.Sensitive {
			snapshot[field.Name] = resolved[field.Name].EffectiveValue()
		}
	}
	provider := resolved[config.FieldProvider].EffectiveValue()
	confirmations := []string{string(ai.ConfirmationDiffEgress)}
	if strings.HasPrefix(provider, "subprocess:") {
		confirmations = append(confirmations,
			string(ai.ConfirmationSubprocessExecution))
	}
	if provider == "openai-compat" &&
		strings.TrimRight(resolved[config.FieldBaseURL].EffectiveValue(), "/") !=
			strings.TrimRight(ai.DefaultOpenAIBaseURL, "/") {
		confirmations = append(confirmations,
			string(ai.ConfirmationEndpointCredentials))
	}
	snapshot["confirmations"] = confirmations
	body, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("encode Intent v2 integration revision: %v", err)
	}
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	db, err := state.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open Intent v2 integration state: %v", err)
	}
	defer db.Close()
	revision, err := state.InsertConfigRevision(context.Background(), db,
		state.ConfigRevisionInput{
			Snapshot: body, Profile: "integration", Scope: "repository",
			SourceGeneration: 0, Reason: "integration Intent v2 fixture",
		})
	if err != nil {
		t.Fatalf("insert Intent v2 integration revision: %v", err)
	}
	request, ok, err := state.RequestConfigActivation(
		context.Background(), db, revision.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatalf("request Intent v2 integration revision: ok=%v err=%v",
			ok, err)
	}
	if ok, err := state.AcknowledgeConfigActivation(
		context.Background(), db, request.ID, revision.ID); err != nil || !ok {
		t.Fatalf("acknowledge Intent v2 integration revision: ok=%v err=%v",
			ok, err)
	}
	if ok, err := state.ApplyConfigActivation(
		context.Background(), db, request.ID, revision.ID); err != nil || !ok {
		t.Fatalf("apply Intent v2 integration revision: ok=%v err=%v",
			ok, err)
	}
	return cleaned
}

func assertIntentV2RuntimeActive(t *testing.T, repo string) {
	t.Helper()
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	got := sqliteScalar(t, dbPath, `
SELECT COALESCE((SELECT value FROM daemon_meta
                 WHERE key='intent.v2.migration_state'), '') || '|' ||
       COALESCE((SELECT value FROM daemon_meta
                 WHERE key='intent.v2.preset_id'), '') || '|' ||
       COALESCE((SELECT value FROM daemon_meta
                 WHERE key='intent.v2.needs_attention'), '')`)
	if !strings.HasPrefix(got, "active|intent.fast|") ||
		strings.TrimPrefix(got, "active|intent.fast|") != "" {
		t.Fatalf("Intent v2 runtime is not active: %q", got)
	}
}

// acdBinaryPath is the per-process build cache. We compile the `acd` binary
// once and reuse it across every integration scenario. The directory is
// registered for cleanup in TestMain so /tmp does not accumulate stale
// builds across `go test` runs.
var (
	acdBinaryOnce sync.Once
	acdBinary     string
	acdBinaryDir  string
	acdBinaryErr  error

	repoTemplateOnce sync.Once
	repoTemplateDir  string
	repoTemplateErr  error
)

// TestMain removes package-scoped binary and repository fixtures after the
// suite completes so /tmp stays clean.
func TestMain(m *testing.M) {
	if runCheckpointRuntimeWatchdog() {
		return
	}
	if err := startCheckpointRuntimeWatchdog(); err != nil {
		fmt.Fprintf(os.Stderr, "start integration checkpoint watchdog: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	stopCheckpointRuntimeWatchdog()
	if acdBinaryDir != "" {
		_ = os.RemoveAll(acdBinaryDir)
	}
	if repoTemplateDir != "" {
		_ = os.RemoveAll(repoTemplateDir)
	}
	os.Exit(code)
}

// buildAcdBinary builds (or returns the cached path of) the production `acd`
// binary. Subsequent calls within the same `go test` process reuse the
// existing binary; we never invalidate the cache because go's test driver
// gives us a fresh process per package.
//
// The build runs with the same flags as the Makefile (CGO_ENABLED=0,
// netgo+osusergo) so the binary is identical to a release build for the
// purpose of integration scenarios.
func buildAcdBinary(t *testing.T) string {
	t.Helper()
	acdBinaryOnce.Do(func() {
		// Resolve the repo root by climbing up from this file. test/integration
		// sits two directories below the module root.
		_, here, _, ok := runtime.Caller(0)
		if !ok {
			acdBinaryErr = errors.New("integration: cannot resolve test source path")
			return
		}
		repoRoot := filepath.Clean(filepath.Join(filepath.Dir(here), "..", ".."))

		outDir, err := os.MkdirTemp("", "acd-integration-bin-*")
		if err != nil {
			acdBinaryErr = fmt.Errorf("mkdtemp: %w", err)
			return
		}
		acdBinaryDir = outDir
		bin := filepath.Join(outDir, "acd")
		if runtime.GOOS == "windows" {
			bin += ".exe"
		}
		cmd := exec.Command("go", "build",
			"-tags=netgo,osusergo",
			"-trimpath",
			"-o", bin,
			"./cmd/acd",
		)
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		out, err := cmd.CombinedOutput()
		if err != nil {
			acdBinaryErr = fmt.Errorf("go build: %w\n%s", err, out)
			return
		}
		acdBinary = bin
	})
	if acdBinaryErr != nil {
		t.Fatalf("buildAcdBinary: %v", acdBinaryErr)
	}
	return acdBinary
}

// tempRepo creates a fresh git repo with one seed commit so HEAD resolves
// for capture+replay. Returns the absolute repo dir; the caller owns no
// cleanup beyond t.TempDir's automatic teardown.
func tempRepo(t *testing.T) string {
	t.Helper()
	repoTemplateOnce.Do(initRepoTemplate)
	if repoTemplateErr != nil {
		t.Fatalf("initialize integration repo template: %v", repoTemplateErr)
	}

	dir := t.TempDir()
	if err := copyRepoTemplate(repoTemplateDir, dir); err != nil {
		t.Fatalf("materialize integration repo template: %v", err)
	}
	return dir
}

func initRepoTemplate() {
	dir, err := os.MkdirTemp("", "acd-integration-repo-template-*")
	if err != nil {
		repoTemplateErr = err
		return
	}
	repoTemplateDir = dir

	if out, err := exec.Command("git", "init", "-q", dir).CombinedOutput(); err != nil {
		repoTemplateErr = fmt.Errorf("git init: %w\n%s", err, out)
		return
	}
	for _, args := range [][]string{
		{"symbolic-ref", "HEAD", "refs/heads/main"},
		{"config", "user.email", "acd-integration@example.com"},
		{"config", "user.name", "ACD Integration"},
		{"config", "commit.gpgsign", "false"},
	} {
		if out, err := runGit(dir, args...); err != nil {
			repoTemplateErr = fmt.Errorf("git %s: %w\n%s", strings.Join(args, " "), err, out)
			return
		}
	}
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("# acd integration seed\n"), 0o644); err != nil {
		repoTemplateErr = fmt.Errorf("write seed: %w", err)
		return
	}
	for _, args := range [][]string{{"add", ".gitignore"}, {"commit", "-q", "-m", "seed"}} {
		if out, err := runGit(dir, args...); err != nil {
			repoTemplateErr = fmt.Errorf("git %s: %w\n%s", strings.Join(args, " "), err, out)
			return
		}
	}
}

func copyRepoTemplate(src, dst string) error {
	return filepath.WalkDir(src, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		target := filepath.Join(dst, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if entry.Type()&os.ModeSymlink != 0 {
			linkTarget, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(linkTarget, target)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unsupported fixture entry %s with mode %s", rel, info.Mode())
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
		if err != nil {
			_ = in.Close()
			return err
		}
		_, copyErr := io.Copy(out, in)
		return errors.Join(copyErr, out.Close(), in.Close())
	})
}

// gitInit runs `git init -q dir`.
func gitInit(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if out, err := exec.Command("git", "init", "-q", dir).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, out)
	}
	// Force branch to main regardless of host's init.defaultBranch.
	if out, err := exec.Command("git", "-C", dir, "symbolic-ref", "HEAD", "refs/heads/main").CombinedOutput(); err != nil {
		t.Fatalf("symbolic-ref HEAD: %v\n%s", err, out)
	}
}

// runGitOK runs `git -C dir args...` and fails the test on non-zero exit.
func runGitOK(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := runGit(dir, args...)
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// runGit runs `git -C dir args...` and returns stdout (or stdout+stderr on
// failure). No t pointer so it's safe in goroutines / waitFor predicates.
func runGit(dir string, args ...string) (string, error) {
	full := append([]string{"-C", dir}, args...)
	cmd := exec.Command("git", full...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// ExecResult captures the output of an `acd` invocation.
type ExecResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

type synchronizedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *synchronizedBuffer) contains(value string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Contains(b.buf.Bytes(), []byte(value))
}

// runAcd execs the integration-built binary with `args` and returns its
// stdout/stderr/exit-code. Inherits HOME from the test process; callers that
// need an isolated XDG layout should set ACD_TEST_HOME via env when
// appropriate (we don't reach for this in v1).
func runAcd(t *testing.T, ctx context.Context, env []string, args ...string) ExecResult {
	return runAcdFromDir(t, ctx, env, "", args...)
}

func runAcdFromDir(t *testing.T, ctx context.Context, env []string, dir string, args ...string) ExecResult {
	t.Helper()
	bin := buildAcdBinary(t)
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = dir
	if env != nil {
		cmd.Env = env
	} else {
		cmd.Env = os.Environ()
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exit := 0
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			exit = ee.ExitCode()
		} else {
			// non-ExitError (e.g. binary missing) — propagate via Stderr so
			// the caller can decide.
			stderr.WriteString("\n[runAcd]: " + err.Error())
			exit = -1
		}
	}
	return ExecResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exit,
	}
}

// runPTYCommand executes a command behind the host's script(1) PTY. The
// wrapper sets a real kernel window size before exec and can resize it during
// the session, which exercises Bubble Tea's SIGWINCH path rather than merely
// setting COLUMNS/LINES. Inputs are written after startup so full-screen
// initialization is observable before keyboard-only interaction begins.
func runPTYCommand(t *testing.T, ctx context.Context, env []string, cols, rows int, resizeCols, resizeRows int, input string, args ...string) ExecResult {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("PTY integration is supported on macOS and Linux")
	}
	if _, err := exec.LookPath("script"); err != nil {
		t.Skip("script(1) PTY utility required")
	}
	body := "stty cols " + strconv.Itoa(cols) + " rows " + strconv.Itoa(rows)
	minInputDelay := 850 * time.Millisecond
	if resizeCols > 0 && resizeRows > 0 {
		body += "; (sleep 0.9; stty cols " + strconv.Itoa(resizeCols) + " rows " + strconv.Itoa(resizeRows) + " < /dev/tty; kill -WINCH $$) & exec \"$@\""
		minInputDelay = 1400 * time.Millisecond
	} else {
		body += "; exec \"$@\""
	}
	commandArgs := append([]string{"/bin/sh", "-c", body, "sh"}, args...)
	var scriptArgs []string
	if runtime.GOOS == "darwin" {
		scriptArgs = append([]string{"-q", "/dev/null"}, commandArgs...)
	} else {
		scriptArgs = []string{"-q", "-c", shellJoin(commandArgs), "/dev/null"}
	}
	cmd := exec.CommandContext(ctx, "script", scriptArgs...)
	cmd.Env = env
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("PTY stdin: %v", err)
	}
	var output synchronizedBuffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	startedAt := time.Now()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start PTY command: %v", err)
	}
	readyDeadline := time.Now().Add(5 * time.Second)
	for !output.contains("ACD SETTINGS") &&
		!output.contains("Commit strategy") &&
		time.Now().Before(readyDeadline) && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if remaining := minInputDelay - time.Since(startedAt); remaining > 0 {
		timer := time.NewTimer(remaining)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
	if input != "" {
		for i, chunk := range strings.Split(input, "\x00") {
			if i > 0 {
				time.Sleep(700 * time.Millisecond)
			}
			_, _ = io.WriteString(stdin, chunk)
		}
	}
	_ = stdin.Close()
	err = cmd.Wait()
	exit := 0
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			exit = ee.ExitCode()
		} else {
			exit = -1
		}
	}
	return ExecResult{Stdout: output.String(), ExitCode: exit}
}

func shellJoin(args []string) string {
	quoted := make([]string, len(args))
	for i, arg := range args {
		quoted[i] = "'" + strings.ReplaceAll(arg, "'", "'\\''") + "'"
	}
	return strings.Join(quoted, " ")
}

// waitFor polls pred at ~50ms intervals until it returns true or the
// timeout elapses. Fails the test on timeout with `name` in the message.
func waitFor(t *testing.T, name string, timeout time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("waitFor: %s did not become true within %v", name, timeout)
}

// withIsolatedHome returns the env slice for runAcd that points HOME at a
// per-test tmpdir so the central registry/stats live in isolation.
func withIsolatedHome(t *testing.T) []string {
	t.Helper()
	home := t.TempDir()
	shortState, err := os.MkdirTemp("/tmp", "acd-it-state-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shortState) })
	env := os.Environ()
	for i, kv := range env {
		if strings.HasPrefix(kv, "HOME=") || strings.HasPrefix(kv, "XDG_STATE_HOME=") ||
			strings.HasPrefix(kv, "XDG_DATA_HOME=") || strings.HasPrefix(kv, "XDG_CONFIG_HOME=") ||
			strings.HasPrefix(kv, "ACD_AI_") {
			env[i] = ""
		}
	}
	out := make([]string, 0, len(env)+4)
	for _, kv := range env {
		if kv != "" {
			out = append(out, kv)
		}
	}
	out = append(out,
		"HOME="+home,
		"XDG_STATE_HOME="+shortState,
		"XDG_DATA_HOME=",
		"XDG_CONFIG_HOME=",
	)
	return out
}

// envWith appends extra KEY=VALUE pairs to a base env (typically
// withIsolatedHome's return value).
func envWith(base []string, kvs ...string) []string {
	out := make([]string, 0, len(base)+len(kvs))
	out = append(out, base...)
	out = append(out, kvs...)
	return out
}

// readDaemonStateMode reads daemon_state.mode from <repo>/.git/acd/state.db
// using the sqlite3 binary. Falls back to "" if anything goes wrong (caller
// is expected to use waitFor + retry).
func readDaemonStateMode(repoDir string) string {
	return readDaemonStateScalar(repoDir, "SELECT mode FROM daemon_state WHERE id = 1")
}

// readDaemonStatePID returns daemon_state.pid (or 0).
func readDaemonStatePID(repoDir string) int {
	v := readDaemonStateScalar(repoDir, "SELECT pid FROM daemon_state WHERE id = 1")
	pid := 0
	fmt.Sscanf(v, "%d", &pid)
	return pid
}

func readDaemonStateScalar(repoDir, query string) string {
	dbPath := filepath.Join(repoDir, ".git", "acd", "state.db")
	if _, err := os.Stat(dbPath); err != nil {
		return ""
	}
	out, err := exec.Command("sqlite3", dbPath, query).CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// writeFile is shorthand for os.WriteFile + t.Fatalf.
func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func gitCommitAll(t *testing.T, repo, message string, paths ...string) string {
	t.Helper()
	if len(paths) == 0 {
		paths = []string{"."}
	}
	runGitOK(t, repo, append([]string{"add"}, paths...)...)
	runGitOK(t, repo, "commit", "-q", "-m", message)
	return strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
}

// waitForMetaCleared polls daemon_meta until the given key is absent or empty,
// with a 50ms tick and the supplied timeout. Fails the test if the key is still
// set when the deadline expires.
func waitForMetaCleared(t *testing.T, dbPath, key string, timeout time.Duration) {
	t.Helper()
	waitFor(t, fmt.Sprintf("daemon_meta[%s] cleared", key), timeout, func() bool {
		out, err := exec.Command("sqlite3", dbPath,
			fmt.Sprintf("SELECT value FROM daemon_meta WHERE key = %s", sqliteLiteral(key))).
			CombinedOutput()
		if err != nil {
			return false
		}
		return strings.TrimSpace(string(out)) == ""
	})
}

// sqliteLiteral returns a single-quoted SQLite string literal for key.
func sqliteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// selfPublicationOracle captures the cross-store invariants every transition
// row must prove. Durations are measured by the caller around the real
// operation; keeping them in the proof makes liveness a required assertion
// instead of an informational log line.
type selfPublicationOracle struct {
	SourceHead       string
	TargetHead       string
	EventCount       int
	BranchGeneration int64
	HeartbeatGap     time.Duration
	WakeLatency      time.Duration
	WantCleanQueue   bool
}

func assertSelfPublicationOracle(
	t *testing.T,
	repo string,
	db *state.DB,
	proof selfPublicationOracle,
) {
	t.Helper()
	ctx := context.Background()

	head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	if head != proof.TargetHead {
		t.Fatalf("HEAD=%s want target=%s", head, proof.TargetHead)
	}
	headTree := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD^{tree}"))
	indexTree := strings.TrimSpace(runGitOK(t, repo, "write-tree"))
	if headTree != indexTree {
		t.Fatalf("final tree=%s want worktree/index tree=%s", headTree, indexTree)
	}

	var total, published, uniqueOwners int
	if err := db.SQL().QueryRowContext(ctx, `
SELECT COUNT(*),
       SUM(CASE WHEN state='published' THEN 1 ELSE 0 END),
       COUNT(DISTINCT CASE WHEN state='published' THEN seq END)
FROM capture_events`).Scan(&total, &published, &uniqueOwners); err != nil {
		t.Fatalf("query event ownership oracle: %v", err)
	}
	if total != proof.EventCount || published != proof.EventCount ||
		uniqueOwners != proof.EventCount {
		t.Fatalf(
			"event ownership total=%d published=%d unique=%d want=%d",
			total, published, uniqueOwners, proof.EventCount)
	}
	rows, err := db.SQL().QueryContext(ctx, `
SELECT e.seq, e.operation, o.path, o.after_oid
FROM capture_events e
JOIN capture_ops o ON o.event_seq=e.seq
WHERE e.state='published'
ORDER BY e.seq, o.ord`)
	if err != nil {
		t.Fatalf("query published capture operations: %v", err)
	}
	defer rows.Close()
	eventsWithOps := make(map[int64]struct{}, proof.EventCount)
	for rows.Next() {
		var (
			seq       int64
			operation string
			path      string
			afterOID  sql.NullString
		)
		if err := rows.Scan(&seq, &operation, &path, &afterOID); err != nil {
			t.Fatalf("scan published capture operation: %v", err)
		}
		eventsWithOps[seq] = struct{}{}
		if operation == "create" && !afterOID.Valid {
			t.Fatalf("published create event %d path=%s has no after object",
				seq, path)
		}
		if !afterOID.Valid {
			continue
		}
		gotOID := strings.TrimSpace(runGitOK(
			t, repo, "rev-parse", proof.TargetHead+":"+path))
		if gotOID != afterOID.String {
			t.Fatalf(
				"published event %d path=%s object=%s want captured=%s",
				seq, path, gotOID, afterOID.String)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate published capture operations: %v", err)
	}
	if len(eventsWithOps) != proof.EventCount {
		t.Fatalf("published events with operations=%d want=%d",
			len(eventsWithOps), proof.EventCount)
	}

	chain := strings.Fields(runGitOK(
		t, repo, "rev-list", "--first-parent", "--reverse",
		proof.SourceHead+".."+proof.TargetHead))
	parent := proof.SourceHead
	for i, commit := range chain {
		gotParent := strings.TrimSpace(runGitOK(
			t, repo, "rev-parse", commit+"^"))
		if gotParent != parent {
			t.Fatalf("commit[%d]=%s parent=%s want=%s",
				i, commit, gotParent, parent)
		}
		parent = commit
	}
	if len(chain) == 0 || parent != proof.TargetHead {
		t.Fatalf("linear publication chain=%v target=%s", chain, proof.TargetHead)
	}

	var generation string
	if err := db.SQL().QueryRowContext(ctx, `
SELECT value FROM daemon_meta WHERE key='branch.generation'`,
	).Scan(&generation); err != nil {
		t.Fatalf("query branch generation: %v", err)
	}
	if generation != strconv.FormatInt(proof.BranchGeneration, 10) {
		t.Fatalf("branch generation=%s want=%d",
			generation, proof.BranchGeneration)
	}
	for name, query := range map[string]string{
		"recovery snapshots": `SELECT COUNT(*) FROM recovery_snapshots`,
		"recoverable journals": `SELECT COUNT(*) FROM self_publications
			WHERE phase IN ('prepared','git_applied')`,
		"multiply owned events": `SELECT COUNT(*) FROM (
			SELECT event_seq FROM self_publication_members
			GROUP BY event_seq HAVING COUNT(*) > 1)`,
	} {
		var count int
		if err := db.SQL().QueryRowContext(ctx, query).Scan(&count); err != nil {
			t.Fatalf("query %s: %v", name, err)
		}
		if count != 0 {
			t.Fatalf("%s=%d want 0", name, count)
		}
	}
	if proof.WantCleanQueue {
		var pending int
		if err := db.SQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM capture_events
WHERE state NOT IN ('published','superseded_external')`,
		).Scan(&pending); err != nil {
			t.Fatalf("query clean queue: %v", err)
		}
		if pending != 0 {
			t.Fatalf("unsettled event queue=%d want 0", pending)
		}
	}
	if status := strings.TrimSpace(runGitOK(
		t, repo, "status", "--porcelain")); status != "" {
		t.Fatalf("worktree not clean:\n%s", status)
	}
	if proof.HeartbeatGap > 3*time.Second {
		t.Fatalf("heartbeat gap=%s exceeds 3s", proof.HeartbeatGap)
	}
	if proof.WakeLatency > 60*time.Second {
		t.Fatalf("wake acknowledgement=%s exceeds 60s", proof.WakeLatency)
	}
}

// SeedFlushRequests inserts n flush_requests rows in 'pending' status using a
// single batched INSERT. Used by populated-state startup tests to simulate a
// real-world AI-Assistant repo where the daemon was killed mid-burst.
func SeedFlushRequests(t *testing.T, dbPath string, n int) {
	t.Helper()
	if n <= 0 {
		return
	}
	var sb strings.Builder
	sb.WriteString("BEGIN; ")
	now := nowFloatSeconds()
	const chunk = 500
	for start := 0; start < n; start += chunk {
		end := start + chunk
		if end > n {
			end = n
		}
		sb.WriteString("INSERT INTO flush_requests(command, non_blocking, requested_ts, status) VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "('wake', 0, %f, 'pending')", now+float64(i)*1e-6)
		}
		sb.WriteString("; ")
	}
	sb.WriteString("COMMIT;")
	if out, err := exec.Command("sqlite3", dbPath, sb.String()).CombinedOutput(); err != nil {
		t.Fatalf("seed flush_requests: %v\n%s", err, out)
	}
}

// SeedDaemonClients inserts n stale daemon_clients rows whose watch_pid
// targets PIDs that are extremely unlikely to be alive. The fingerprints are
// distinct so the daemon's GC sweep evicts them on its next pass.
func SeedDaemonClients(t *testing.T, dbPath string, n int) {
	t.Helper()
	if n <= 0 {
		return
	}
	now := nowFloatSeconds()
	var sb strings.Builder
	sb.WriteString("BEGIN; ")
	for i := 0; i < n; i++ {
		// Use very high PIDs that are vanishingly unlikely to be live so the
		// GC sweep classifies the row as stale immediately.
		pid := 2000000 + i
		fmt.Fprintf(&sb,
			"INSERT OR REPLACE INTO daemon_clients(session_id, harness, watch_pid, watch_fp, registered_ts, last_seen_ts) "+
				"VALUES ('stale-client-%04d', 'shell', %d, 'stale|fp|%d', %f, %f); ",
			i, pid, i, now-3600, now-3600)
	}
	sb.WriteString("COMMIT;")
	if out, err := exec.Command("sqlite3", dbPath, sb.String()).CombinedOutput(); err != nil {
		t.Fatalf("seed daemon_clients: %v\n%s", err, out)
	}
}

// SeedShadowGenerations writes `generations` distinct (branch_ref, branch_generation)
// shadow rows of `rowsPerGen` size each. The newest generation matches the
// caller's intent for the active branch generation; older ones are present to
// stress retention/pruning and bootstrap idempotency. Paths are deterministic
// so the seeded state is reproducible across runs.
func SeedShadowGenerations(t *testing.T, dbPath, branchRef string, generations, rowsPerGen int) {
	t.Helper()
	if generations <= 0 || rowsPerGen <= 0 {
		return
	}
	now := nowFloatSeconds()
	const chunk = 400
	for gen := 1; gen <= generations; gen++ {
		// Every (gen,path) needs a unique 40-char OID. Use sha-shaped padding.
		baseHead := fmt.Sprintf("%040x", gen)
		for start := 0; start < rowsPerGen; start += chunk {
			end := start + chunk
			if end > rowsPerGen {
				end = rowsPerGen
			}
			var sb strings.Builder
			sb.WriteString("BEGIN; INSERT INTO shadow_paths(branch_ref, branch_generation, path, operation, mode, oid, base_head, fidelity, updated_ts) VALUES ")
			for i := start; i < end; i++ {
				if i > start {
					sb.WriteString(", ")
				}
				oid := fmt.Sprintf("%040x", (gen<<24)|(i+1))
				path := fmt.Sprintf("seed/g%d/p%05d.txt", gen, i)
				fmt.Fprintf(&sb, "(%s, %d, %s, 'create', '100644', '%s', '%s', 'rescan', %f)",
					sqliteLiteral(branchRef), gen, sqliteLiteral(path), oid, baseHead, now)
			}
			sb.WriteString("; COMMIT;")
			if out, err := exec.Command("sqlite3", dbPath, sb.String()).CombinedOutput(); err != nil {
				t.Fatalf("seed shadow_paths gen=%d: %v\n%s", gen, err, out)
			}
		}
	}
}

// readHeartbeatTs reads daemon_state.heartbeat_ts (or 0).
func readHeartbeatTs(repoDir string) float64 {
	v := readDaemonStateScalar(repoDir, "SELECT heartbeat_ts FROM daemon_state WHERE id = 1")
	var f float64
	fmt.Sscanf(v, "%f", &f)
	return f
}

// initStateDBSchema brings <repo>/.git/acd/state.db into existence with the
// canonical schema applied without starting a supervisor-owned worker. This
// keeps fixture seeding deterministic under the checkpoint-first lifecycle.
//
// Returns the absolute path to the state.db.
func initStateDBSchema(t *testing.T, ctx context.Context, env []string, repo, sessionID string) string {
	t.Helper()
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("initialize v20 state fixture %s: %v", sessionID, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close v20 state fixture: %v", err)
	}
	prepareCheckpointRegistration(t, env, repo)
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("state.db not created by fixture bootstrap: %v", err)
	}
	_ = env
	return dbPath
}
