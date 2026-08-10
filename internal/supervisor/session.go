package supervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

const sessionStartTimeout = 15 * time.Second

// EnsureSession starts the macOS supervisor from the current authorized
// terminal or agent process. Unlike launchd, the child remains in the caller's
// macOS privacy responsibility chain and therefore needs no Full Disk Access.
func EnsureSession(ctx context.Context, roots paths.Roots, binary, logPath string) error {
	if runtime.GOOS != "darwin" {
		return nil
	}
	ownerID, err := responsibilitySessionIdentity(ctx)
	if err != nil {
		return err
	}
	return ensureSessionForOwner(ctx, roots, binary, logPath, ownerID)
}

func ensureSessionForOwner(ctx context.Context, roots paths.Roots, binary, logPath, ownerID string) error {
	if binary == "" || logPath == "" {
		return errors.New("supervisor: incomplete session configuration")
	}
	if ownerID == "" {
		return errors.New("supervisor: responsibility session identity is unavailable")
	}
	ready, err := sessionReady(ctx, roots, ownerID)
	if err != nil {
		return err
	}
	if ready {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(roots.SupervisorSocketPath()), 0o700); err != nil {
		return fmt.Errorf("supervisor: create session run directory: %w", err)
	}
	lock, err := AcquireSessionLifecycleLock(ctx, roots)
	if err != nil {
		return err
	}
	defer lock.Release()
	ready, err = sessionReady(ctx, roots, ownerID)
	if err != nil {
		return err
	}
	if ready {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		return fmt.Errorf("supervisor: create log directory: %w", err)
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("supervisor: open session log: %w", err)
	}
	command := exec.Command(binary, "internal", "supervisor", "run")
	command.Env = sessionProcessEnvironment(roots, os.Environ(), ownerID)
	command.Stdout = logFile
	command.Stderr = logFile
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("supervisor: start session: %w", err)
	}
	_ = logFile.Close()
	processDone := make(chan error, 1)
	go func() { processDone <- command.Wait() }()
	readyProven := false
	defer func() {
		if !readyProven {
			stopSessionChild(command, processDone)
		}
	}()

	deadline := time.NewTimer(sessionStartTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready, probeErr := sessionReady(ctx, roots, ownerID)
		if probeErr != nil {
			return probeErr
		}
		if ready {
			readyProven = true
			return nil
		}
		select {
		case childErr := <-processDone:
			readyProven = true
			if childErr == nil {
				return errors.New("supervisor: session exited before readiness")
			}
			return fmt.Errorf("supervisor: session exited before readiness: %w", childErr)
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("supervisor: session did not become ready; inspect %s", logPath)
		case <-ticker.C:
		}
	}
}

func sessionRunning(ctx context.Context, roots paths.Roots) bool {
	ownerID, err := responsibilitySessionIdentity(ctx)
	if err != nil {
		return false
	}
	ready, _ := sessionReady(ctx, roots, ownerID)
	return ready
}

func sessionReady(ctx context.Context, roots paths.Roots, ownerID string) (bool, error) {
	requestCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	response, err := (&Client{
		SocketPath: roots.SupervisorSocketPath(), Timeout: 300 * time.Millisecond,
	}).Do(requestCtx, Request{Version: ProtocolVersion, ID: "session-probe", Method: "status"})
	if err != nil {
		return false, nil
	}
	if response.Error != nil || response.Version != ProtocolVersion {
		return false, errors.New("supervisor: incompatible owner on the canonical session socket")
	}
	body, err := json.Marshal(response.Data)
	if err != nil {
		return false, err
	}
	var status Status
	if err := json.Unmarshal(body, &status); err != nil {
		return false, err
	}
	wantOwnership := "session:" + ownerID
	if status.Ownership != wantOwnership {
		return false, fmt.Errorf("supervisor: canonical socket belongs to another responsibility session; stop its ACD supervisor before retrying")
	}
	return true, nil
}

// SessionLifecycleLock serializes session startup with uninstall shutdown proof.
type SessionLifecycleLock struct{ file *os.File }

func AcquireSessionLifecycleLock(ctx context.Context, roots paths.Roots) (*SessionLifecycleLock, error) {
	lockPath := roots.SupervisorSocketPath() + ".start.lock"
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o700); err != nil {
		return nil, fmt.Errorf("supervisor: create session run directory: %w", err)
	}
	file, err := acquireSessionStartLock(ctx, lockPath)
	if err != nil {
		return nil, err
	}
	return &SessionLifecycleLock{file: file}, nil
}

func (l *SessionLifecycleLock) Release() {
	if l == nil || l.file == nil {
		return
	}
	releaseSessionStartLock(l.file)
	l.file = nil
}

func acquireSessionStartLock(ctx context.Context, path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("supervisor: open session start lock: %w", err)
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = file.Close()
			return nil, fmt.Errorf("supervisor: lock session start: %w", err)
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func stopSessionChild(command *exec.Cmd, done <-chan error) {
	if command == nil || command.Process == nil {
		return
	}
	_ = command.Process.Signal(syscall.SIGTERM)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
		return
	case <-timer.C:
		_ = command.Process.Kill()
		<-done
	}
}

func responsibilitySessionIdentity(ctx context.Context) (string, error) {
	pid := os.Getpid()
	var command string
	for depth := 0; depth < 64; depth++ {
		output, err := exec.CommandContext(ctx, "/bin/ps", "-o", "ppid=,comm=", "-p", strconv.Itoa(pid)).Output()
		if err != nil {
			return "", fmt.Errorf("supervisor: identify responsibility session: %w", err)
		}
		fields := strings.Fields(strings.TrimSpace(string(output)))
		if len(fields) < 2 {
			return "", errors.New("supervisor: identify responsibility session: invalid process ancestry")
		}
		parent, err := strconv.Atoi(fields[0])
		if err != nil || parent < 0 {
			return "", errors.New("supervisor: identify responsibility session: invalid parent process")
		}
		command = strings.Join(fields[1:], " ")
		if parent <= 1 {
			digest := sha256.Sum256([]byte(strconv.Itoa(pid) + "\x00" + command))
			return hex.EncodeToString(digest[:16]), nil
		}
		pid = parent
	}
	return "", errors.New("supervisor: identify responsibility session: process ancestry is too deep")
}

func releaseSessionStartLock(file *os.File) {
	if file == nil {
		return
	}
	_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	_ = file.Close()
}
