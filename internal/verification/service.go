// Package verification runs user-approved repository checks against exact
// candidate commits without exposing the live index or worktree.
package verification

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

const (
	// OutputLimit is the maximum sanitized command output returned to callers.
	OutputLimit = 64 * 1024

	markerVersion       = 1
	workspaceDirName    = "verification-worktrees"
	workspacePrefix     = "candidate-"
	workspaceMarkerName = "marker.json"
	workspaceTreeName   = "tree"
	maxApprovalIDBytes  = 128
	maxCommandBytes     = 16 * 1024
	maxMarkerBytes      = 8 * 1024
	commandWaitDelay    = 2 * time.Second
	incompleteGrace     = 5 * time.Minute
)

type Mode string

const (
	ModeFast Mode = "fast"
	ModeFull Mode = "full"
)

type Status string

const (
	StatusPassed         Status = "passed"
	StatusFailed         Status = "failed"
	StatusTimedOut       Status = "timed_out"
	StatusNeedsAttention Status = "needs_attention"
)

// ApprovedCommand is repository-scoped verification input. Its fields are
// intentionally private: callers must pass through NewApprovedCommand after
// loading a stored runtime revision or receiving an exact interactive
// confirmation.
type ApprovedCommand struct {
	repositoryRoot string
	approvalID     string
	mode           Mode
	command        string
	timeout        time.Duration
	digest         string
}

// NewApprovedCommand binds an exact shell command to one canonical repository
// and approval identity. approvalID should identify the immutable config
// revision during replay, or the explicit preview confirmation during setup.
func NewApprovedCommand(
	repositoryRoot string,
	approvalID string,
	mode Mode,
	command string,
	timeout time.Duration,
) (ApprovedCommand, error) {
	root, err := canonicalPath(repositoryRoot)
	if err != nil {
		return ApprovedCommand{}, fmt.Errorf("verification approval: repository: %w", err)
	}
	if err := boundedLabel("approval id", approvalID, maxApprovalIDBytes); err != nil {
		return ApprovedCommand{}, err
	}
	if mode != ModeFast && mode != ModeFull {
		return ApprovedCommand{}, fmt.Errorf("verification approval: unsupported mode %q", mode)
	}
	if strings.TrimSpace(command) == "" {
		return ApprovedCommand{}, errors.New("verification approval: command is required")
	}
	if len(command) > maxCommandBytes || strings.IndexByte(command, 0) >= 0 {
		return ApprovedCommand{}, fmt.Errorf("verification approval: command exceeds %d safe bytes", maxCommandBytes)
	}
	if timeout <= 0 {
		return ApprovedCommand{}, errors.New("verification approval: timeout must be positive")
	}
	sum := sha256.Sum256([]byte(command))
	return ApprovedCommand{
		repositoryRoot: root,
		approvalID:     approvalID,
		mode:           mode,
		command:        command,
		timeout:        timeout,
		digest:         hex.EncodeToString(sum[:]),
	}, nil
}

// Request identifies the exact candidate commit to verify. CommitOID must be a
// full commit object ID; symbolic revisions are rejected.
type Request struct {
	RepoPath    string
	CandidateID string
	CommitOID   string
	Command     ApprovedCommand
}

// Result is safe to retain in candidate state. Output is sanitized and limited
// to its final 64 KiB. Any non-passing result requires operator attention and
// must not be used as publication approval.
type Result struct {
	Status         Status
	NeedsAttention bool
	Mode           Mode
	ApprovalID     string
	CommandDigest  string
	CommitOID      string
	ExitCode       int
	TimedOut       bool
	Duration       time.Duration
	Output         string
}

// CleanupResult reports only bounded workspace counts, never source paths.
type CleanupResult struct {
	Removed int
	Active  int
	Skipped int
}

// Runner owns the ephemeral verification workspace lifecycle. WorkspaceRoot is
// an optional test/embedding override; production callers should leave it empty
// so workspaces live below the repository's private ACD Git state directory.
type Runner struct {
	WorkspaceRoot string
}

type workspace struct {
	sessionPath string
	treePath    string
	marker      workspaceMarker
}

type workspaceMarker struct {
	Version     int    `json:"version"`
	State       string `json:"state"`
	RepoPath    string `json:"repo_path"`
	GitDir      string `json:"git_dir"`
	CandidateID string `json:"candidate_id"`
	CommitOID   string `json:"commit_oid"`
	PID         int    `json:"pid"`
	CreatedNS   int64  `json:"created_ns"`
}

const (
	workspaceActive          = "active"
	workspaceCleanupRequired = "cleanup_required"
)

// Run materializes the exact detached candidate commit, runs the approved
// command, and removes the registered worktree. Command failures and timeouts
// return a non-error Result with NeedsAttention=true; setup, cancellation, and
// cleanup failures additionally return an error.
func (r Runner) Run(ctx context.Context, request Request) (result Result, retErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	result = Result{
		Status:         StatusNeedsAttention,
		NeedsAttention: true,
		ExitCode:       -1,
		Mode:           request.Command.mode,
		ApprovalID:     request.Command.approvalID,
		CommandDigest:  request.Command.digest,
		CommitOID:      request.CommitOID,
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := boundedLabel("candidate id", request.CandidateID, 128); err != nil {
		return result, err
	}
	worktree, err := gitpkg.ResolveWorktree(ctx, request.RepoPath)
	if err != nil {
		return result, fmt.Errorf("verification: resolve repository: %w", err)
	}
	if request.Command.repositoryRoot == "" ||
		request.Command.repositoryRoot != worktree.Root {
		return result, errors.New("verification: approved command repository does not match candidate repository")
	}
	commitOID, err := resolveExactCommit(ctx, worktree.Root, request.CommitOID)
	if err != nil {
		return result, err
	}
	result.CommitOID = commitOID

	root, err := r.workspaceRoot(worktree)
	if err != nil {
		return result, err
	}
	ws, err := prepareWorkspace(ctx, root, worktree, request.CandidateID, commitOID)
	if err != nil {
		return result, err
	}
	defer func() {
		markerErr := markWorkspaceCleanupRequired(ws)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if cleanupErr := cleanupWorkspace(cleanupCtx, worktree.Root, ws); cleanupErr != nil {
			result.Status = StatusNeedsAttention
			result.NeedsAttention = true
			retErr = errors.Join(retErr, markerErr, cleanupErr)
		}
	}()

	started := time.Now()
	output := &tailBuffer{capacity: OutputLimit * 2}
	runErr, timedOut, cancelled := runApprovedCommand(
		ctx,
		ws.treePath,
		filepath.Join(ws.sessionPath, "environment"),
		request.Command,
		output,
	)
	result.Duration = time.Since(started)
	result.Output = sanitizeOutput(output.Bytes())
	result.TimedOut = timedOut

	if timedOut {
		result.Status = StatusTimedOut
		return result, nil
	}
	if cancelled {
		return result, ctx.Err()
	}
	if runErr == nil {
		result.Status = StatusPassed
		result.NeedsAttention = false
		result.ExitCode = 0
		return result, nil
	}
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		result.Status = StatusFailed
		result.ExitCode = exitErr.ExitCode()
		return result, nil
	}
	var supervisedExit approvedCommandExitError
	if errors.As(runErr, &supervisedExit) {
		result.Status = StatusFailed
		result.ExitCode = supervisedExit.code
		return result, nil
	}
	return result, fmt.Errorf("verification: execute approved command: %w", runErr)
}

// CleanupStale removes marked workspaces whose creating process is no longer
// alive. It ignores live-process workspaces and never removes unmarked,
// malformed, cross-repository, or symlinked paths.
func (r Runner) CleanupStale(ctx context.Context, repoPath string) (CleanupResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	worktree, err := gitpkg.ResolveWorktree(ctx, repoPath)
	if err != nil {
		return CleanupResult{}, fmt.Errorf("verification cleanup: resolve repository: %w", err)
	}
	root, err := r.workspaceRoot(worktree)
	if err != nil {
		return CleanupResult{}, err
	}
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return CleanupResult{}, nil
	}
	if err != nil {
		return CleanupResult{}, fmt.Errorf("verification cleanup: read workspace root: %w", err)
	}

	var result CleanupResult
	var cleanupErrs []error
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), workspacePrefix) {
			result.Skipped++
			continue
		}
		sessionPath := filepath.Join(root, entry.Name())
		info, err := os.Lstat(sessionPath)
		if err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("verification cleanup: inspect marked workspace: %w", err))
			continue
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			result.Skipped++
			continue
		}
		marker, err := readWorkspaceMarker(filepath.Join(sessionPath, workspaceMarkerName))
		if err != nil {
			if time.Since(info.ModTime()) >= incompleteGrace && ownedByCurrentUser(info) {
				ws := workspace{
					sessionPath: sessionPath,
					treePath:    filepath.Join(sessionPath, workspaceTreeName),
				}
				if cleanupErr := cleanupWorkspace(ctx, worktree.Root, ws); cleanupErr != nil {
					cleanupErrs = append(cleanupErrs, errors.Join(err, cleanupErr))
					continue
				}
				result.Removed++
				continue
			}
			result.Skipped++
			cleanupErrs = append(cleanupErrs, err)
			continue
		}
		if marker.RepoPath != worktree.Root || marker.GitDir != worktree.GitDir {
			result.Skipped++
			continue
		}
		if marker.State == workspaceActive && processAlive(marker.PID) {
			result.Active++
			continue
		}
		ws := workspace{
			sessionPath: sessionPath,
			treePath:    filepath.Join(sessionPath, workspaceTreeName),
			marker:      marker,
		}
		if err := cleanupWorkspace(ctx, worktree.Root, ws); err != nil {
			cleanupErrs = append(cleanupErrs, err)
			continue
		}
		result.Removed++
	}
	return result, errors.Join(cleanupErrs...)
}

func (r Runner) workspaceRoot(worktree gitpkg.Worktree) (string, error) {
	root := r.WorkspaceRoot
	if root == "" {
		root = filepath.Join(worktree.GitDir, "acd", workspaceDirName)
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("verification: resolve workspace root: %w", err)
	}
	if err := ensurePrivateDirectory(root); err != nil {
		return "", err
	}
	return root, nil
}

func prepareWorkspace(
	ctx context.Context,
	root string,
	repo gitpkg.Worktree,
	candidateID string,
	commitOID string,
) (workspace, error) {
	id, err := randomID()
	if err != nil {
		return workspace{}, fmt.Errorf("verification: allocate workspace id: %w", err)
	}
	sessionPath := filepath.Join(root, workspacePrefix+id)
	if err := os.Mkdir(sessionPath, 0o700); err != nil {
		return workspace{}, fmt.Errorf("verification: create marked workspace: %w", err)
	}
	ws := workspace{
		sessionPath: sessionPath,
		treePath:    filepath.Join(sessionPath, workspaceTreeName),
	}
	marker := workspaceMarker{
		Version:     markerVersion,
		State:       workspaceActive,
		RepoPath:    repo.Root,
		GitDir:      repo.GitDir,
		CandidateID: candidateID,
		CommitOID:   commitOID,
		PID:         os.Getpid(),
		CreatedNS:   time.Now().UnixNano(),
	}
	ws.marker = marker
	if err := writeWorkspaceMarker(sessionPath, marker); err != nil {
		_ = os.RemoveAll(sessionPath)
		return workspace{}, err
	}
	if err := syncDirectory(root); err != nil {
		_ = os.RemoveAll(sessionPath)
		return workspace{}, fmt.Errorf("verification: sync workspace root: %w", err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{
		Dir:     repo.Root,
		Timeout: gitpkg.DefaultWriteTimeout,
	}, "worktree", "add", "--detach", ws.treePath, commitOID); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return workspace{}, errors.Join(
			fmt.Errorf("verification: add detached worktree: %w", err),
			cleanupWorkspace(cleanupCtx, repo.Root, ws),
		)
	}
	head, err := gitpkg.RevParse(ctx, ws.treePath, "HEAD^{commit}")
	if err != nil || head != commitOID {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupErr := cleanupWorkspace(cleanupCtx, repo.Root, ws)
		if err != nil {
			return workspace{}, errors.Join(
				fmt.Errorf("verification: resolve detached candidate: %w", err),
				cleanupErr,
			)
		}
		return workspace{}, errors.Join(
			errors.New("verification: detached candidate changed during materialization"),
			cleanupErr,
		)
	}
	return ws, nil
}

func cleanupWorkspace(ctx context.Context, repoPath string, ws workspace) error {
	if ctx == nil {
		ctx = context.Background()
	}
	treeInfo, statErr := os.Lstat(ws.treePath)
	switch {
	case statErr == nil && treeInfo.Mode()&os.ModeSymlink != 0:
		return errors.New("verification cleanup: refusing symlinked candidate worktree")
	case statErr != nil && !errors.Is(statErr, os.ErrNotExist):
		return fmt.Errorf("verification cleanup: inspect candidate worktree: %w", statErr)
	}

	// Git can remove a registered worktree even when the command deleted its
	// checkout directory. If this path was never registered (for example, a
	// crash before worktree add), the failed remove is safe to ignore only
	// after a bounded registration check.
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{
		Dir:     repoPath,
		Timeout: gitpkg.DefaultWriteTimeout,
	}, "worktree", "remove", "--force", "--force", ws.treePath); err != nil {
		registered, inspectErr := worktreeRegistered(ctx, repoPath, ws.treePath)
		if inspectErr != nil {
			return errors.Join(
				fmt.Errorf("verification cleanup: remove registered worktree: %w", err),
				inspectErr,
			)
		}
		if registered {
			return fmt.Errorf("verification cleanup: remove registered worktree: %w", err)
		}
	}
	if err := os.RemoveAll(ws.sessionPath); err != nil {
		return fmt.Errorf("verification cleanup: remove marked workspace: %w", err)
	}
	if err := syncDirectory(filepath.Dir(ws.sessionPath)); err != nil {
		return fmt.Errorf("verification cleanup: sync workspace root: %w", err)
	}
	return nil
}

func runApprovedCommand(
	ctx context.Context,
	dir string,
	environmentRoot string,
	approved ApprovedCommand,
	output io.Writer,
) (runErr error, timedOut bool, cancelled bool) {
	runCtx, cancel := context.WithTimeout(ctx, approved.timeout)
	defer cancel()
	if err := runCtx.Err(); err != nil {
		return err, false, true
	}

	env, err := verificationEnv(environmentRoot)
	if err != nil {
		return err, false, false
	}
	statusReader, statusWriter, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("verification: create command status pipe: %w", err), false, false
	}
	defer statusReader.Close()

	// Kill the process group from the shell's EXIT trap before the leader can
	// disappear. This closes the scheduler race where a background descendant
	// could run after the approved shell exited but before Go observed Wait.
	// fd 3 preserves the command's real exit status before SIGKILL reaches the
	// supervisor itself; Go repeats the group kill as a bounded fallback.
	const supervisorScript = `finish() {
	status=$?
	trap - EXIT HUP INT TERM
	printf '%d\n' "$status" >&3
	kill -KILL -- -$$
	exit "$status"
}
trap finish EXIT
eval "$1"`
	cmd := exec.Command("/bin/sh", "-c", supervisorScript, "acd-verification", approved.command)
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdout = output
	cmd.Stderr = output
	cmd.ExtraFiles = []*os.File{statusWriter}
	cmd.WaitDelay = commandWaitDelay
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = statusWriter.Close()
		return err, false, false
	}
	_ = statusWriter.Close()

	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	statusDone := make(chan commandStatus, 1)
	go func() {
		var status int
		_, scanErr := fmt.Fscan(statusReader, &status)
		statusDone <- commandStatus{exitCode: status, err: scanErr}
	}()
	select {
	case status := <-statusDone:
		killProcessGroup(cmd.Process.Pid)
		waitErr := <-waitDone
		if status.err != nil {
			if waitErr == nil {
				return nil, false, false
			}
			return waitErr, false, false
		}
		if status.exitCode == 0 {
			return nil, false, false
		}
		return approvedCommandExitError{code: status.exitCode}, false, false
	case <-runCtx.Done():
		killProcessGroup(cmd.Process.Pid)
		err := <-waitDone
		if errors.Is(ctx.Err(), context.Canceled) ||
			errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return err, false, true
		}
		return err, true, false
	}
}

type commandStatus struct {
	exitCode int
	err      error
}

type approvedCommandExitError struct {
	code int
}

func (e approvedCommandExitError) Error() string {
	return fmt.Sprintf("verification command exited with status %d", e.code)
}

func killProcessGroup(pid int) {
	if pid > 0 {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	}
}

func resolveExactCommit(ctx context.Context, repoPath, input string) (string, error) {
	if strings.TrimSpace(input) == "" || strings.ContainsAny(input, " \t\r\n") {
		return "", errors.New("verification: exact candidate commit oid is required")
	}
	resolved, err := gitpkg.RevParse(ctx, repoPath, input+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("verification: resolve candidate commit: %w", err)
	}
	if resolved != input {
		return "", errors.New("verification: symbolic or abbreviated candidate commit is not allowed")
	}
	return resolved, nil
}

func ensurePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := os.MkdirAll(path, 0o700); err != nil {
			return fmt.Errorf("verification: create workspace root: %w", err)
		}
		info, err = os.Lstat(path)
	case err != nil:
		return fmt.Errorf("verification: inspect workspace root: %w", err)
	}
	if err != nil {
		return fmt.Errorf("verification: inspect workspace root: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("verification: workspace root must be a real directory")
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return fmt.Errorf("verification: protect workspace root: %w", err)
	}
	return nil
}

func writeWorkspaceMarker(sessionPath string, marker workspaceMarker) error {
	path := filepath.Join(sessionPath, workspaceMarkerName)
	id, err := randomID()
	if err != nil {
		return fmt.Errorf("verification: allocate workspace marker: %w", err)
	}
	tempPath := filepath.Join(sessionPath, ".marker-"+id+".tmp")
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("verification: create workspace marker: %w", err)
	}
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	writeErr := encoder.Encode(marker)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("verification: write workspace marker: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("verification: close workspace marker: %w", closeErr)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("verification: replace workspace marker: %w", err)
	}
	removeTemp = false
	if err := syncDirectory(sessionPath); err != nil {
		return fmt.Errorf("verification: sync workspace directory: %w", err)
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	return nil
}

func markWorkspaceCleanupRequired(ws workspace) error {
	marker := ws.marker
	if marker.Version == 0 {
		return nil
	}
	marker.State = workspaceCleanupRequired
	return writeWorkspaceMarker(ws.sessionPath, marker)
}

func readWorkspaceMarker(path string) (workspaceMarker, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return workspaceMarker{}, fmt.Errorf("verification cleanup: inspect workspace marker: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return workspaceMarker{}, errors.New("verification cleanup: workspace marker must be a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return workspaceMarker{}, errors.New("verification cleanup: workspace marker permissions are too broad")
	}
	if info.Size() > maxMarkerBytes {
		return workspaceMarker{}, fmt.Errorf(
			"verification cleanup: workspace marker exceeds %d bytes",
			maxMarkerBytes,
		)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return workspaceMarker{}, fmt.Errorf("verification cleanup: read workspace marker: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var marker workspaceMarker
	if err := decoder.Decode(&marker); err != nil {
		return workspaceMarker{}, fmt.Errorf("verification cleanup: decode workspace marker: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return workspaceMarker{}, errors.New("verification cleanup: workspace marker contains multiple JSON values")
		}
		return workspaceMarker{}, fmt.Errorf("verification cleanup: decode workspace marker suffix: %w", err)
	}
	if marker.Version != markerVersion ||
		(marker.State != workspaceActive && marker.State != workspaceCleanupRequired) ||
		marker.RepoPath == "" ||
		marker.GitDir == "" ||
		marker.CandidateID == "" ||
		marker.CommitOID == "" ||
		marker.CreatedNS <= 0 {
		return workspaceMarker{}, errors.New("verification cleanup: invalid workspace marker")
	}
	return marker, nil
}

func ownedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func verificationEnv(root string) ([]string, error) {
	privatePaths := map[string]string{
		"HOME":             filepath.Join(root, "home"),
		"XDG_CONFIG_HOME":  filepath.Join(root, "config"),
		"XDG_CACHE_HOME":   filepath.Join(root, "cache"),
		"XDG_STATE_HOME":   filepath.Join(root, "state"),
		"XDG_RUNTIME_DIR":  filepath.Join(root, "runtime"),
		"TMPDIR":           filepath.Join(root, "tmp"),
		"GOPATH":           filepath.Join(root, "go"),
		"NPM_CONFIG_CACHE": filepath.Join(root, "npm-cache"),
	}
	for _, path := range privatePaths {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return nil, fmt.Errorf("verification: create private command environment: %w", err)
		}
		if err := os.Chmod(path, 0o700); err != nil {
			return nil, fmt.Errorf("verification: protect private command environment: %w", err)
		}
	}

	allowed := map[string]bool{
		"PATH": true, "LANG": true, "LC_ALL": true, "LC_CTYPE": true,
		"TZ": true, "TERM": true, "NO_COLOR": true, "CI": true,
		"USER": true, "LOGNAME": true,
		"GOROOT": true, "GOMODCACHE": true, "GOCACHE": true,
		"GOTOOLCHAIN": true, "CGO_ENABLED": true, "GOFLAGS": true,
		"CC": true, "CXX": true, "AR": true, "PKG_CONFIG_PATH": true,
		"JAVA_HOME": true, "SDKROOT": true, "DEVELOPER_DIR": true,
		"ANDROID_HOME": true, "ANDROID_SDK_ROOT": true,
		"SSL_CERT_FILE": true, "SSL_CERT_DIR": true,
		"REQUESTS_CA_BUNDLE": true, "NODE_EXTRA_CA_CERTS": true,
	}
	filtered := make([]string, 0, len(allowed)+len(privatePaths)+5)
	present := make(map[string]bool)
	for _, item := range os.Environ() {
		name, _, ok := strings.Cut(item, "=")
		if !ok || !allowed[name] ||
			secretEnvironmentName(name) ||
			gitControlEnvironmentName(name) {
			continue
		}
		filtered = append(filtered, item)
		present[name] = true
	}
	if !present["PATH"] {
		filtered = append(filtered, "PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
	}
	if !present["GOMODCACHE"] {
		if home, err := os.UserHomeDir(); err == nil {
			cache := filepath.Join(home, "go", "pkg", "mod")
			if info, statErr := os.Stat(cache); statErr == nil && info.IsDir() {
				filtered = append(filtered, "GOMODCACHE="+cache)
			}
		}
	}
	if !present["GOCACHE"] {
		if cacheRoot, err := os.UserCacheDir(); err == nil {
			cache := filepath.Join(cacheRoot, "go-build")
			if info, statErr := os.Stat(cache); statErr == nil && info.IsDir() {
				filtered = append(filtered, "GOCACHE="+cache)
			}
		}
	}
	for name, value := range privatePaths {
		filtered = append(filtered, name+"="+value)
	}
	filtered = append(filtered,
		"SHELL=/bin/sh",
		"ACD_VERIFICATION=1",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_OPTIONAL_LOCKS=0",
	)
	return filtered, nil
}

func secretEnvironmentName(name string) bool {
	upper := strings.ToUpper(name)
	if strings.HasPrefix(upper, "ACD_") {
		return true
	}
	for _, token := range []string{
		"API_KEY", "APIKEY", "TOKEN", "SECRET", "PASSWORD", "PASSWD",
		"CREDENTIAL", "PRIVATE_KEY",
	} {
		if strings.Contains(upper, token) {
			return true
		}
	}
	return false
}

func gitControlEnvironmentName(name string) bool {
	return strings.HasPrefix(strings.ToUpper(name), "GIT_")
}

func worktreeRegistered(ctx context.Context, repoPath, target string) (bool, error) {
	output, err := gitpkg.Run(ctx, gitpkg.RunOpts{
		Dir:     repoPath,
		Timeout: gitpkg.DefaultReadTimeout,
	}, "worktree", "list", "--porcelain", "-z")
	if err != nil {
		return false, fmt.Errorf("verification cleanup: inspect registered worktrees: %w", err)
	}
	prefix := []byte("worktree ")
	for _, field := range bytes.Split(output, []byte{0}) {
		if bytes.HasPrefix(field, prefix) &&
			filepath.Clean(string(bytes.TrimPrefix(field, prefix))) == filepath.Clean(target) {
			return true, nil
		}
	}
	return false, nil
}

var (
	ansiEscape = regexp.MustCompile(`\x1b(?:\[[0-?]*[ -/]*[@-~]|[@-_])`)
	jsonSecret = regexp.MustCompile(
		`(?i)("(?:[^"]*(?:api[_-]?key|token|secret|password|credential)[^"]*)"\s*:\s*")[^"]*"`,
	)
	labeledSecret = regexp.MustCompile(
		`(?i)((?:api[_-]?key|token|secret|password|credential)\s*[:=]\s*)[^,\s;]+`,
	)
	authorizationSecret = regexp.MustCompile(
		`(?i)(authorization\s*:\s*(?:bearer|basic)\s+)[^\s]+`,
	)
	urlUserInfo = regexp.MustCompile(`(?i)(https?://)[^/@\s]+@`)
)

func sanitizeOutput(raw []byte) string {
	value := string(bytes.ToValidUTF8(raw, []byte("\uFFFD")))
	value = ansiEscape.ReplaceAllString(value, "")
	value = jsonSecret.ReplaceAllString(value, `${1}[redacted]"`)
	value = labeledSecret.ReplaceAllString(value, `${1}[redacted]`)
	value = authorizationSecret.ReplaceAllString(value, `${1}[redacted]`)
	value = urlUserInfo.ReplaceAllString(value, `${1}[redacted]@`)
	value = strings.Map(func(r rune) rune {
		switch r {
		case '\n', '\t':
			return r
		case '\r':
			return '\n'
		}
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, value)
	if len(value) <= OutputLimit {
		return value
	}
	value = value[len(value)-OutputLimit:]
	for len(value) > 0 && !utf8.RuneStart(value[0]) {
		value = value[1:]
	}
	return value
}

type tailBuffer struct {
	mu       sync.Mutex
	capacity int
	data     []byte
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.capacity <= 0 {
		return len(p), nil
	}
	if len(p) >= b.capacity {
		b.data = append(b.data[:0], p[len(p)-b.capacity:]...)
		return len(p), nil
	}
	overflow := len(b.data) + len(p) - b.capacity
	if overflow > 0 {
		copy(b.data, b.data[overflow:])
		b.data = b.data[:len(b.data)-overflow]
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *tailBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.data...)
}

func canonicalPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("path is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}

func boundedLabel(name, value string, maxBytes int) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("verification: %s is required", name)
	}
	if len(value) > maxBytes || !utf8.ValidString(value) {
		return fmt.Errorf("verification: %s exceeds %d safe bytes", name, maxBytes)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return fmt.Errorf("verification: %s contains control characters", name)
		}
	}
	return nil
}

func randomID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
