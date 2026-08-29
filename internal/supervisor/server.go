package supervisor

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
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

type MaintenanceLease struct {
	RepositoryID string    `json:"repository_id"`
	Token        string    `json:"token"`
	ExpiresAt    time.Time `json:"expires_at"`
}

type MaintenanceStatus struct {
	Released bool `json:"released"`
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

	mu          sync.Mutex
	workers     map[string]*workerProcess
	maintenance map[string]MaintenanceLease
	closing     bool
	cancel      context.CancelFunc
	command     func(context.Context, string, ...string) ([]byte, error)
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
// login and during setup cutover. Two protection scans retain useful
// parallelism while the regular reconcile tick starts each remaining worker as
// soon as capacity is available. A worker that cannot finish its initial
// checkpoint must eventually release admission so two unhealthy repositories
// cannot prevent every later repository from starting.
const (
	maxConcurrentWorkerStarts   = 2
	workerStartupAdmissionLease = 2 * time.Minute
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
	case "maintenance_begin":
		return s.beginMaintenance(ctx, request.RepositoryID)
	case "maintenance_renew":
		return s.renewMaintenance(request.RepositoryID, request.Params)
	case "maintenance_end":
		return s.endMaintenance(ctx, request.RepositoryID, request.Params)
	case "restart_repository":
		return s.restartRepository(ctx, request.RepositoryID)
	case "stop_repository":
		return s.stopRepository(ctx, request.RepositoryID)
	}
	if s.Handler == nil {
		return nil, &ProtocolError{Code: "not_supported", Message: "request is not supported by this supervisor", Retryable: false}
	}
	return s.Handler.HandleSupervisorRequest(ctx, request)
}

func (s *Server) restartRepository(ctx context.Context, repositoryID string) (any, *ProtocolError) {
	value, protocolErr := s.beginMaintenance(ctx, repositoryID)
	if protocolErr != nil {
		return nil, protocolErr
	}
	lease := value.(MaintenanceLease)
	params, _ := json.Marshal(map[string]string{"token": lease.Token})
	if _, protocolErr := s.endMaintenance(ctx, repositoryID, params); protocolErr != nil {
		return nil, protocolErr
	}
	return MaintenanceStatus{Released: true}, nil
}

func (s *Server) stopRepository(ctx context.Context, repositoryID string) (any, *ProtocolError) {
	if !validRepositoryID(repositoryID) {
		return nil, &ProtocolError{Code: "invalid_repository", Message: "valid repository_id is required"}
	}
	if err := s.reconcile(ctx); err != nil {
		return nil, &ProtocolError{Code: "repository_stop_failed", Message: err.Error(), Retryable: true}
	}
	if err := s.waitRepositoryWorkerStopped(ctx, repositoryID); err != nil {
		return nil, &ProtocolError{Code: "repository_stop_incomplete", Message: err.Error(), Retryable: true}
	}
	return MaintenanceStatus{Released: true}, nil
}

const maintenanceLeaseTTL = 2 * time.Minute

func (s *Server) beginMaintenance(ctx context.Context, repositoryID string) (any, *ProtocolError) {
	if !validRepositoryID(repositoryID) {
		return nil, &ProtocolError{Code: "invalid_repository", Message: "valid repository_id is required"}
	}
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, &ProtocolError{Code: "maintenance_failed", Message: err.Error(), Retryable: true}
	}
	now := time.Now()
	lease := MaintenanceLease{RepositoryID: repositoryID, Token: hex.EncodeToString(tokenBytes), ExpiresAt: now.Add(maintenanceLeaseTTL)}
	s.mu.Lock()
	if s.maintenance == nil {
		s.maintenance = make(map[string]MaintenanceLease)
	}
	s.expireMaintenanceLocked(now)
	if existing, held := s.maintenance[repositoryID]; held {
		s.mu.Unlock()
		return nil, &ProtocolError{Code: "maintenance_busy", Message: fmt.Sprintf("repository maintenance is already active until %s", existing.ExpiresAt.Format(time.RFC3339)), Retryable: true}
	}
	s.maintenance[repositoryID] = lease
	s.mu.Unlock()
	if err := s.reconcile(ctx); err != nil {
		s.mu.Lock()
		delete(s.maintenance, repositoryID)
		s.mu.Unlock()
		return nil, &ProtocolError{Code: "maintenance_failed", Message: err.Error(), Retryable: true}
	}
	if err := s.waitRepositoryWorkerStopped(ctx, repositoryID); err != nil {
		s.mu.Lock()
		delete(s.maintenance, repositoryID)
		s.mu.Unlock()
		_ = s.reconcile(context.Background())
		return nil, &ProtocolError{Code: "maintenance_stop_incomplete", Message: err.Error(), Retryable: true}
	}
	return lease, nil
}

func (s *Server) renewMaintenance(repositoryID string, params json.RawMessage) (any, *ProtocolError) {
	token, protocolErr := maintenanceToken(params)
	if protocolErr != nil {
		return nil, protocolErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.expireMaintenanceLocked(now)
	lease, held := s.maintenance[repositoryID]
	if !held || lease.Token != token {
		return nil, &ProtocolError{Code: "maintenance_lease_lost", Message: "repository maintenance lease is not active", Retryable: false}
	}
	lease.ExpiresAt = now.Add(maintenanceLeaseTTL)
	s.maintenance[repositoryID] = lease
	return lease, nil
}

func (s *Server) endMaintenance(ctx context.Context, repositoryID string, params json.RawMessage) (any, *ProtocolError) {
	token, protocolErr := maintenanceToken(params)
	if protocolErr != nil {
		return nil, protocolErr
	}
	s.mu.Lock()
	lease, held := s.maintenance[repositoryID]
	if !held || lease.Token != token {
		s.mu.Unlock()
		return nil, &ProtocolError{Code: "maintenance_lease_lost", Message: "repository maintenance lease is not active", Retryable: false}
	}
	delete(s.maintenance, repositoryID)
	s.mu.Unlock()
	if err := s.reconcile(ctx); err != nil {
		return nil, &ProtocolError{Code: "maintenance_restart_failed", Message: err.Error(), Retryable: true}
	}
	return MaintenanceStatus{Released: true}, nil
}

func maintenanceToken(params json.RawMessage) (string, *ProtocolError) {
	var request struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(params, &request); err != nil || request.Token == "" {
		return "", &ProtocolError{Code: "invalid_request", Message: "maintenance token is required"}
	}
	return request.Token, nil
}

func (s *Server) expireMaintenanceLocked(now time.Time) {
	for repositoryID, lease := range s.maintenance {
		if !now.Before(lease.ExpiresAt) {
			delete(s.maintenance, repositoryID)
		}
	}
}

func (s *Server) status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := Status{PID: os.Getpid(), Version: s.Version, BinaryDigest: s.BinaryDigest,
		Ownership: os.Getenv(supervisorOwnershipEnv), Compatibility: s.Compatibility,
		Workers: make([]WorkerStatus, 0, len(s.workers))}
	for _, worker := range s.workers {
		item := WorkerStatus{RepositoryID: worker.id, Restarts: worker.restarts, LastError: worker.lastError, State: "backoff", Version: s.Version}
		if runtimeStatus, err := ReadWorkerRuntimeStatus(s.Roots, worker.id); err == nil &&
			(worker.launchd || worker.cmd != nil) {
			item.PID = runtimeStatus.PID
			item.State = runtimeStatus.State
			item.Restarts = runtimeStatus.Restarts
			item.LastError = runtimeStatus.LastError
		} else if worker.launchd {
			item.State = "starting"
		} else if worker.cmd != nil && worker.cmd.Process != nil {
			item.PID = worker.cmd.Process.Pid
			item.State = "starting"
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
	cleanupNeeded := false
	for _, record := range registry.Repos {
		if record.LifecycleDisabled() ||
			(record.FirstRegisteredTS <= 0 && record.LastSeenTS <= 0) {
			continue
		}
		assessment, _ := central.AssessMissingRepo(ctx, record)
		if assessment.Missing {
			cleanupNeeded = true
			break
		}
	}
	if cleanupNeeded {
		if err := central.WithLock(s.Roots, func(locked *central.Registry) error {
			_, _, reconcileErr := locked.ReconcileMissingRepos(
				ctx, time.Now().UTC())
			return reconcileErr
		}); err != nil {
			return fmt.Errorf("supervisor: reconcile missing worktrees: %w", err)
		}
		registry, err = central.Load(s.Roots)
		if err != nil {
			return fmt.Errorf("supervisor: reload reconciled registry: %w", err)
		}
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
	s.expireMaintenanceLocked(now)
	for repositoryID := range s.maintenance {
		delete(desired, repositoryID)
	}
	for id, worker := range s.workers {
		if _, enabled := desired[id]; !enabled {
			s.stopWorkerLocked(worker)
			if worker.launchd || worker.cmd != nil {
				continue
			}
			_ = RemoveWorkerRuntimeStatus(s.Roots, id)
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
	startLimit := max(0,
		maxConcurrentWorkerStarts-startingWorkers(s.Roots, s.workers, now))
	for _, id := range startableWorkerIDs(s.workers, now, startLimit) {
		s.startWorkerLocked(ctx, s.workers[id])
	}
	return nil
}

func startingWorkers(
	roots paths.Roots,
	workers map[string]*workerProcess,
	now time.Time,
) int {
	count := 0
	for _, worker := range workers {
		if worker == nil || (worker.cmd == nil && !worker.launchd) {
			continue
		}
		status, err := ReadWorkerRuntimeStatus(roots, worker.id)
		if workerHoldsStartupAdmission(worker, status, err, now) {
			count++
		}
	}
	return count
}

func workerHoldsStartupAdmission(
	worker *workerProcess,
	status WorkerRuntimeStatus,
	statusErr error,
	now time.Time,
) bool {
	started := worker.started
	if statusErr == nil {
		if status.State != "starting" {
			return false
		}
		if status.UpdatedTS > 0 {
			started = time.UnixMilli(status.UpdatedTS)
		}
	}
	if started.IsZero() {
		return false
	}
	return now.Before(started.Add(workerStartupAdmissionLease))
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
	args := []string{
		"internal", "worker", "supervise", "--repository-id", worker.id,
		"--state-root", s.Roots.State,
		"--share-root", s.Roots.Share,
		"--config-root", s.Roots.Config,
	}
	if _, err := os.Stat(s.Roots.SetupPublicationHoldPath()); err == nil {
		args = append(args, "--publication-hold", s.Roots.SetupPublicationHoldPath())
	}
	if s.LaunchdWorkers && runtime.GOOS == "darwin" {
		s.startLaunchdWorkerLocked(ctx, worker, args)
		return
	}
	_ = RemoveWorkerRuntimeStatus(s.Roots, worker.id)
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
	content, err := renderWorkerService(s.Roots, s.BinaryPath, worker.id, args)
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

func (s *Server) waitRepositoryWorkerStopped(ctx context.Context, repositoryID string) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		worker := s.workers[repositoryID]
		stopped := worker == nil || (!worker.launchd && worker.cmd == nil)
		s.mu.Unlock()
		if stopped {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("supervisor: repository worker %s did not stop: %w", repositoryID, ctx.Err())
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
