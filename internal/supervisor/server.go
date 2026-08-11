package supervisor

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

type WorkerStatus struct {
	RepositoryID string `json:"repository_id"`
	PID          int    `json:"pid,omitempty"`
	State        string `json:"state"`
	Restarts     int    `json:"restarts"`
	LastError    string `json:"last_error,omitempty"`
	Version      string `json:"version,omitempty"`
}

type Status struct {
	PID           int            `json:"pid"`
	Version       string         `json:"version"`
	BinaryDigest  string         `json:"binary_digest,omitempty"`
	Ownership     string         `json:"ownership"`
	Compatibility Compatibility  `json:"compatibility"`
	Workers       []WorkerStatus `json:"workers"`
}

// Compatibility is the explicit persisted-data and IPC contract required for
// a lightweight binary replacement. A zero value identifies a legacy runtime
// that must complete one final full setup before automatic upgrades are safe.
type Compatibility struct {
	ProtocolVersion    int `json:"protocol_version"`
	RegistryVersion    int `json:"registry_version"`
	StateSchemaVersion int `json:"state_schema_version"`
	IntegrationVersion int `json:"integration_version"`
}

func (c Compatibility) Equal(other Compatibility) bool {
	return c != (Compatibility{}) && c == other
}

type ShutdownStatus struct {
	Stopped bool `json:"stopped"`
}

type Handler interface {
	HandleSupervisorRequest(context.Context, Request) (any, *ProtocolError)
}

type Server struct {
	Roots         paths.Roots
	BinaryPath    string
	Version       string
	BinaryDigest  string
	Compatibility Compatibility
	Handler       Handler
	// LaunchdWorkers is retained only for cleaning up or testing the legacy
	// macOS worker wrapper. Product supervisors run workers as direct children
	// so they keep the invoking session's repository access.
	LaunchdWorkers bool

	mu      sync.Mutex
	workers map[string]*workerProcess
	closing bool
	cancel  context.CancelFunc
	command func(context.Context, string, ...string) ([]byte, error)
}

type workerProcess struct {
	id          string
	cmd         *exec.Cmd
	started     time.Time
	restarts    int
	nextStart   time.Time
	lastError   string
	intentional bool
	signature   string
	desired     string
	launchd     bool
}

var restartDelays = []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second}

// Starting every registered repository at once creates a Git/process storm on
// login and during setup cutover. Keep each reconcile batch small; the regular
// two-second reconcile tick starts the remaining workers promptly.
const (
	maxWorkerStartsPerReconcile = 4
	maxConcurrentWorkerStarts   = 8
)

func (s *Server) Run(ctx context.Context) error {
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	s.mu.Lock()
	s.cancel = runCancel
	s.mu.Unlock()
	ctx = runCtx
	if s.Roots.State == "" || s.Roots.Share == "" {
		return errors.New("supervisor: unresolved XDG roots")
	}
	if s.BinaryPath == "" {
		return errors.New("supervisor: empty binary path")
	}
	if s.workers == nil {
		s.workers = make(map[string]*workerProcess)
	}
	socket := s.Roots.SupervisorSocketPath()
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		return fmt.Errorf("supervisor: create run directory: %w", err)
	}
	if err := removeStaleSocket(socket); err != nil {
		return err
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("supervisor: listen: %w", err)
	}
	defer listener.Close()
	defer os.Remove(socket)
	if err := os.Chmod(socket, 0o600); err != nil {
		return fmt.Errorf("supervisor: chmod socket: %w", err)
	}

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	if err := s.reconcile(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = s.reconcile(ctx)
			}
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return fmt.Errorf("supervisor: accept: %w", err)
		}
		if err := validatePeerUser(conn); err != nil {
			_ = conn.Close()
			continue
		}
		go s.serveConnection(ctx, conn)
	}
	s.shutdownWorkers()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	shutdownErr := s.waitWorkersStopped(shutdownCtx)
	shutdownCancel()
	if shutdownErr == nil {
		return nil
	}
	s.forceKillWorkers()
	return s.waitWorkersStopped(context.Background())
}

func removeStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("supervisor: inspect socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("supervisor: refusing to replace non-socket %s", path)
	}
	conn, dialErr := net.DialTimeout("unix", path, 100*time.Millisecond)
	if dialErr == nil {
		_ = conn.Close()
		return fmt.Errorf("supervisor: socket already has a live owner")
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("supervisor: remove stale socket: %w", err)
	}
	return nil
}

func (s *Server) serveConnection(parent context.Context, conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(io.LimitReader(conn, 1<<20))
	var request Request
	if err := json.NewDecoder(reader).Decode(&request); err != nil {
		_ = json.NewEncoder(conn).Encode(Response{Version: ProtocolVersion, Error: &ProtocolError{Code: "invalid_request", Message: "invalid JSON request"}})
		return
	}
	response := Response{Version: ProtocolVersion, ID: request.ID}
	if err := request.Validate(); err != nil {
		response.Error = &ProtocolError{Code: "invalid_request", Message: err.Error()}
		_ = json.NewEncoder(conn).Encode(response)
		return
	}
	ctx := parent
	var cancel context.CancelFunc
	if request.DeadlineMS > 0 {
		ctx, cancel = context.WithDeadline(parent, time.UnixMilli(request.DeadlineMS))
	} else {
		ctx, cancel = context.WithTimeout(parent, 5*time.Second)
	}
	defer cancel()
	data, protocolErr := s.handle(ctx, request)
	response.OK = protocolErr == nil
	response.Data = data
	response.Error = protocolErr
	_ = json.NewEncoder(conn).Encode(response)
}

func (s *Server) handle(ctx context.Context, request Request) (any, *ProtocolError) {
	switch request.Method {
	case "status":
		return s.status(), nil
	case "shutdown":
		s.mu.Lock()
		s.closing = true
		cancel := s.cancel
		s.mu.Unlock()
		s.shutdownWorkers()
		if err := s.waitWorkersStopped(ctx); err != nil {
			return nil, &ProtocolError{
				Code: "shutdown_incomplete", Message: err.Error(), Retryable: true,
			}
		}
		if cancel != nil {
			time.AfterFunc(10*time.Millisecond, cancel)
		}
		return ShutdownStatus{Stopped: true}, nil
	}
	if s.Handler == nil {
		return nil, &ProtocolError{Code: "not_supported", Message: "request is not supported by this supervisor", Retryable: false}
	}
	return s.Handler.HandleSupervisorRequest(ctx, request)
}

func (s *Server) status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := Status{PID: os.Getpid(), Version: s.Version, BinaryDigest: s.BinaryDigest,
		Ownership: os.Getenv(supervisorOwnershipEnv), Compatibility: s.Compatibility,
		Workers: make([]WorkerStatus, 0, len(s.workers))}
	for _, worker := range s.workers {
		item := WorkerStatus{RepositoryID: worker.id, Restarts: worker.restarts, LastError: worker.lastError, State: "backoff", Version: s.Version}
		if worker.launchd {
			item.State = "starting"
			if runtimeStatus, err := ReadWorkerRuntimeStatus(s.Roots, worker.id); err == nil {
				item.PID = runtimeStatus.PID
				item.State = runtimeStatus.State
				item.Restarts = runtimeStatus.Restarts
				item.LastError = runtimeStatus.LastError
			}
		} else if worker.cmd != nil && worker.cmd.Process != nil {
			item.PID = worker.cmd.Process.Pid
			item.State = "running"
		}
		if item.Restarts >= 5 && item.LastError != "" {
			item.State = "needs_action"
		}
		status.Workers = append(status.Workers, item)
	}
	sort.Slice(status.Workers, func(i, j int) bool { return status.Workers[i].RepositoryID < status.Workers[j].RepositoryID })
	return status
}

func (s *Server) reconcile(ctx context.Context) error {
	registry, err := central.Load(s.Roots)
	if err != nil {
		return fmt.Errorf("supervisor: load registry: %w", err)
	}
	if registry.Version != central.RegistryVersion {
		return fmt.Errorf("supervisor: registry v%d requires `acd setup` before v%d workers can start",
			registry.Version, central.RegistryVersion)
	}
	desiredRecords := make(map[string][]central.RepoRecord)
	for _, record := range registry.Repos {
		if record.LifecycleDisabled() || record.RepositoryID == "" {
			continue
		}
		desiredRecords[record.RepositoryID] = append(desiredRecords[record.RepositoryID], record)
	}
	desired := make(map[string]string, len(desiredRecords))
	for id, records := range desiredRecords {
		sort.Slice(records, func(i, j int) bool { return records[i].WorktreeID < records[j].WorktreeID })
		var builder strings.Builder
		for _, record := range records {
			fmt.Fprintf(&builder, "%s\x00%s\x00%s\x00", record.WorktreeID, record.Path, record.StateDB)
		}
		desired[id] = builder.String()
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil
	}
	for id, worker := range s.workers {
		if _, enabled := desired[id]; !enabled {
			s.stopWorkerLocked(worker)
			if worker.launchd || worker.cmd != nil {
				continue
			}
			delete(s.workers, id)
		}
	}
	for id, signature := range desired {
		worker := s.workers[id]
		if worker == nil {
			worker = &workerProcess{id: id, signature: signature, desired: signature}
			if s.LaunchdWorkers && runtime.GOOS == "darwin" {
				worker.launchd = s.launchdWorkerExists(ctx, id)
			}
			s.workers[id] = worker
		}
		worker.desired = signature
		if worker.signature != signature {
			if worker.launchd || worker.cmd != nil {
				s.stopWorkerLocked(worker)
				if worker.launchd || worker.cmd != nil {
					continue
				}
			}
			if worker.intentional {
				continue
			}
			worker.signature = signature
			worker.restarts = 0
			worker.nextStart = time.Time{}
		}
	}
	startLimit := maxWorkerStartsPerReconcile
	if s.LaunchdWorkers && runtime.GOOS == "darwin" {
		startLimit = min(startLimit, max(0,
			maxConcurrentWorkerStarts-startingLaunchdWorkers(s.Roots, s.workers)))
	}
	for _, id := range startableWorkerIDs(s.workers, now, startLimit) {
		s.startWorkerLocked(ctx, s.workers[id])
	}
	return nil
}

func startingLaunchdWorkers(roots paths.Roots, workers map[string]*workerProcess) int {
	count := 0
	for _, worker := range workers {
		if worker == nil || !worker.launchd {
			continue
		}
		status, err := ReadWorkerRuntimeStatus(roots, worker.id)
		if err != nil || status.State == "starting" {
			count++
		}
	}
	return count
}

func startableWorkerIDs(workers map[string]*workerProcess, now time.Time, limit int) []string {
	if limit <= 0 {
		return nil
	}
	ids := make([]string, 0, len(workers))
	for id, worker := range workers {
		if worker != nil && worker.cmd == nil && !worker.launchd && !now.Before(worker.nextStart) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	return ids
}

func (s *Server) startWorkerLocked(ctx context.Context, worker *workerProcess) {
	args := []string{"internal", "worker", "run", "--repository-id", worker.id}
	if _, err := os.Stat(s.Roots.SetupPublicationHoldPath()); err == nil {
		args = append(args, "--publication-hold", s.Roots.SetupPublicationHoldPath())
	}
	if s.LaunchdWorkers && runtime.GOOS == "darwin" {
		s.startLaunchdWorkerLocked(ctx, worker, args)
		return
	}
	cmd := exec.CommandContext(ctx, s.BinaryPath, args...)
	cmd.Env = ProcessEnvironment(s.Roots, os.Environ())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		worker.lastError = err.Error()
		worker.restarts++
		worker.nextStart = time.Now().Add(restartDelay(worker.restarts))
		return
	}
	worker.cmd = cmd
	worker.started = time.Now()
	worker.intentional = false
	go func(id string, process *exec.Cmd) {
		err := process.Wait()
		s.mu.Lock()
		defer s.mu.Unlock()
		current := s.workers[id]
		if current == nil || current.cmd != process {
			return
		}
		current.cmd = nil
		if current.intentional || s.closing {
			current.intentional = false
			return
		}
		if time.Since(current.started) >= 5*time.Minute {
			current.restarts = 0
		}
		current.restarts++
		if err != nil {
			current.lastError = err.Error()
		} else {
			current.lastError = "worker exited unexpectedly"
		}
		current.nextStart = time.Now().Add(restartDelay(current.restarts))
	}(worker.id, cmd)
}

func (s *Server) startLaunchdWorkerLocked(ctx context.Context, worker *workerProcess, args []string) {
	label, err := workerLabel(worker.id)
	if err != nil {
		s.recordWorkerStartFailure(worker, err)
		return
	}
	_ = RemoveWorkerRuntimeStatus(s.Roots, worker.id)
	superviseArgs := append([]string(nil), args...)
	superviseArgs[2] = "supervise"
	superviseArgs = append(superviseArgs,
		"--state-root", s.Roots.State,
		"--share-root", s.Roots.Share,
		"--config-root", s.Roots.Config,
	)
	content, err := renderWorkerService(s.Roots, s.BinaryPath, worker.id, superviseArgs)
	if err != nil {
		s.recordWorkerStartFailure(worker, err)
		return
	}
	servicePath, err := writeWorkerService(s.Roots, worker.id, content)
	if err != nil {
		s.recordWorkerStartFailure(worker, err)
		return
	}
	output, err := s.runCommand(ctx, "launchctl", "bootstrap", fmt.Sprintf("gui/%d", os.Getuid()), servicePath)
	if err != nil {
		_ = os.Remove(servicePath)
		s.recordWorkerStartFailure(worker, fmt.Errorf("launchctl bootstrap %s: %w: %s", label, err, strings.TrimSpace(string(output))))
		return
	}
	worker.launchd = true
	worker.started = time.Now()
	worker.intentional = false
}

func (s *Server) recordWorkerStartFailure(worker *workerProcess, err error) {
	worker.lastError = err.Error()
	worker.restarts++
	worker.nextStart = time.Now().Add(restartDelay(worker.restarts))
}

func (s *Server) launchdWorkerExists(ctx context.Context, repositoryID string) bool {
	label, err := workerLabel(repositoryID)
	if err != nil {
		return false
	}
	_, err = s.runCommand(ctx, "launchctl", "print", fmt.Sprintf("gui/%d/%s", os.Getuid(), label))
	return err == nil
}

func (s *Server) stopWorkerLocked(worker *workerProcess) {
	worker.intentional = true
	if worker.launchd {
		label, err := workerLabel(worker.id)
		if err == nil {
			target := fmt.Sprintf("gui/%d/%s", os.Getuid(), label)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			output, stopErr := s.runCommand(ctx, "launchctl", "bootout", target)
			if stopErr != nil && s.launchdWorkerExists(ctx, worker.id) {
				worker.lastError = fmt.Sprintf("launchctl bootout %s: %v: %s", label, stopErr, strings.TrimSpace(string(output)))
				cancel()
				return
			}
			cancel()
		}
		worker.launchd = false
		_ = RemoveWorkerRuntimeStatus(s.Roots, worker.id)
		if servicePath, pathErr := workerServicePath(s.Roots, worker.id); pathErr == nil {
			_ = os.Remove(servicePath)
		}
	}
	if worker.cmd != nil && worker.cmd.Process != nil {
		_ = worker.cmd.Process.Signal(syscall.SIGTERM)
		return
	}
	worker.intentional = false
}

func (s *Server) runCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	if s.command != nil {
		return s.command(ctx, name, args...)
	}
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

func restartDelay(restarts int) time.Duration {
	index := restarts - 1
	if index < 0 {
		index = 0
	}
	if index >= len(restartDelays) {
		index = len(restartDelays) - 1
	}
	return restartDelays[index]
}

// WorkerRestartDelay is shared by the launchd worker wrapper so macOS and
// Linux preserve the same bounded restart contract.
func WorkerRestartDelay(restarts int) time.Duration { return restartDelay(restarts) }

func (s *Server) shutdownWorkers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closing = true
	for _, worker := range s.workers {
		s.stopWorkerLocked(worker)
	}
}

func (s *Server) waitWorkersStopped(ctx context.Context) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		remaining := 0
		for _, worker := range s.workers {
			if worker != nil && (worker.launchd || worker.cmd != nil) {
				remaining++
			}
		}
		s.mu.Unlock()
		if remaining == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("supervisor: %d worker(s) did not stop: %w", remaining, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *Server) forceKillWorkers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, worker := range s.workers {
		if worker != nil && worker.cmd != nil && worker.cmd.Process != nil {
			_ = worker.cmd.Process.Kill()
		}
	}
}
