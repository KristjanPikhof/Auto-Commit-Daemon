package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
	restorepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/restore"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
	acdtrace "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/trace"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version"
)

func newInternalCmd() *cobra.Command {
	root := &cobra.Command{Use: "internal", Hidden: true}
	supervisorCmd := &cobra.Command{Use: "supervisor", Hidden: true}
	supervisorCmd.AddCommand(&cobra.Command{
		Use: "run", Hidden: true, Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSupervisor(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	})
	var repositoryID string
	var publicationHold string
	worker := &cobra.Command{Use: "worker", Hidden: true}
	workerRun := &cobra.Command{
		Use: "run", Hidden: true, Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRepositoryWorker(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), repositoryID, publicationHold)
		},
	}
	workerRun.Flags().StringVar(&repositoryID, "repository-id", "", "Canonical common-directory identity")
	workerRun.Flags().StringVar(&publicationHold, "publication-hold", "", "Setup-owned publication hold marker")
	_ = workerRun.MarkFlagRequired("repository-id")
	var workerRoots paths.Roots
	workerSupervise := &cobra.Command{
		Use: "supervise", Hidden: true, Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return superviseRepositoryWorker(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), workerRoots, repositoryID, publicationHold)
		},
	}
	workerSupervise.Flags().StringVar(&repositoryID, "repository-id", "", "Canonical common-directory identity")
	workerSupervise.Flags().StringVar(&publicationHold, "publication-hold", "", "Setup-owned publication hold marker")
	workerSupervise.Flags().StringVar(&workerRoots.State, "state-root", "", "Resolved ACD state root")
	workerSupervise.Flags().StringVar(&workerRoots.Share, "share-root", "", "Resolved ACD data root")
	workerSupervise.Flags().StringVar(&workerRoots.Config, "config-root", "", "Resolved ACD configuration root")
	_ = workerSupervise.MarkFlagRequired("repository-id")
	_ = workerSupervise.MarkFlagRequired("state-root")
	_ = workerSupervise.MarkFlagRequired("share-root")
	_ = workerSupervise.MarkFlagRequired("config-root")
	var accessCheckPaths []string
	var accessCheckResult string
	workerAccessCheck := &cobra.Command{
		Use: "access-check", Hidden: true, Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runWorkerAccessCheck(cmd.Context(), accessCheckPaths, accessCheckResult)
		},
	}
	workerAccessCheck.Flags().StringSliceVar(&accessCheckPaths, "path", nil, "Repository paths to verify")
	workerAccessCheck.Flags().StringVar(&accessCheckResult, "result", "", "Absolute result file")
	_ = workerAccessCheck.MarkFlagRequired("path")
	_ = workerAccessCheck.MarkFlagRequired("result")
	worker.AddCommand(workerRun, workerSupervise, workerAccessCheck)
	hint := newInternalHintCmd()
	session := &cobra.Command{Use: "session", Hidden: true}
	session.AddCommand(newInternalSessionCmd("open"), newInternalSessionCmd("close"))
	integration := &cobra.Command{Use: "integration", Hidden: true}
	stdinExtract := newHookStdinExtractCmd()
	stdinExtract.Use = "stdin-extract"
	cursorExtract := newHookCursorExtractCmd()
	cursorExtract.Use = "cursor-extract"
	integration.AddCommand(stdinExtract, cursorExtract)
	root.AddCommand(supervisorCmd, worker, hint, session, integration)
	return root
}

func newInternalHintCmd() *cobra.Command {
	var kind string
	var sessionID, harness string
	cmd := &cobra.Command{Use: "hint", Hidden: true, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		repo, _ := cmd.Flags().GetString("repo")
		return sendInternalHint(cmd.Context(), repo, kind, false, "", sessionID, harness, 0)
	}}
	cmd.Flags().StringVar(&kind, "kind", "wake", "wake, soft_boundary, logical_boundary, or checkpoint")
	cmd.Flags().StringVar(&sessionID, "session-id", "", "Optional integration session identity")
	cmd.Flags().StringVar(&harness, "harness", "", "Optional integration name")
	return cmd
}

func newInternalSessionCmd(action string) *cobra.Command {
	var sessionID, harness string
	var watchPID int
	var flush, force bool
	cmd := &cobra.Command{Use: action, Hidden: true, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		repo, _ := cmd.Flags().GetString("repo")
		kind := "wake"
		if action == "close" && flush {
			kind = "checkpoint"
		}
		if err := sendInternalHint(cmd.Context(), repo, kind, flush, action, sessionID, harness, watchPID); err != nil && !(action == "close" && force) {
			return err
		}
		return nil
	}}
	cmd.Flags().StringVar(&sessionID, "session-id", "", "Optional integration session identity")
	cmd.Flags().StringVar(&harness, "harness", "", "Optional integration name")
	cmd.Flags().IntVar(&watchPID, "watch-pid", 0, "Optional integration process id")
	if action == "close" {
		cmd.Flags().BoolVar(&flush, "flush", false, "Checkpoint and drain publication before closing")
		cmd.Flags().BoolVar(&force, "force", false, "Close even when the final drain fails")
	}
	return cmd
}

func sendInternalHint(ctx context.Context, repo, kind string, drain bool, sessionAction, sessionID, harness string, watchPID int) error {
	record, roots, _, err := lookupRegisteredRepo("internal hint", repo)
	if err != nil {
		return err
	}
	method := "hint"
	if kind == "checkpoint" || kind == "logical_boundary" || drain {
		method = "checkpoint_barrier"
	}
	params, _ := json.Marshal(map[string]any{"kind": kind, "drain_publication": drain,
		"session_action": sessionAction, "session_id": sessionID, "harness": harness,
		"watch_pid": watchPID})
	request := supervisor.Request{Version: supervisor.ProtocolVersion, ID: fmt.Sprintf("hint-%d", time.Now().UnixNano()), Method: method, RepositoryID: record.RepositoryID, WorktreeID: record.WorktreeID, DeadlineMS: time.Now().Add(60 * time.Second).UnixMilli(), Params: params}
	response, err := (&supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 60 * time.Second}).Do(ctx, request)
	if err != nil {
		return err
	}
	if response.Error != nil {
		return errors.New(response.Error.Message)
	}
	return nil
}

func newCompatStartCmd() *cobra.Command {
	var sessionID, harness string
	var watchPID int
	cmd := &cobra.Command{Use: "start", Hidden: true, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if strings.TrimSpace(sessionID) == "" {
			return invalidCommandError("acd start: --session-id is required")
		}
		if harness == "" {
			harness = "manual"
		}
		repo, _ := cmd.Flags().GetString("repo")
		record, roots, worktreeRoot, err := lookupRegisteredRepo("start", repo)
		if err != nil {
			return err
		}
		existed, _, err := state.ReadClientRegistration(cmd.Context(), record.StateDB, sessionID)
		if err != nil {
			return err
		}
		if err := sendInternalHint(cmd.Context(), worktreeRoot, "wake", false, "open", sessionID, harness, watchPID); err != nil {
			return err
		}
		_, count, err := state.ReadClientRegistration(cmd.Context(), record.StateDB, sessionID)
		if err != nil {
			return err
		}
		jsonOut, _ := cmd.Flags().GetBool("json")
		if jsonOut {
			return json.NewEncoder(cmd.OutOrStdout()).Encode(startResult{
				Started: !existed, Duplicate: existed,
				DaemonPID: supervisorWorkerPID(cmd.Context(), roots, record.RepositoryID),
				Repo:      worktreeRoot, RepoHash: record.RepositoryID,
				SessionID: sessionID, Harness: harness, ClientCount: count,
			})
		}
		return nil
	}}
	cmd.Flags().StringVar(&sessionID, "session-id", "", "Compatibility session identity")
	cmd.Flags().StringVar(&harness, "harness", "", "Compatibility integration name")
	cmd.Flags().IntVar(&watchPID, "watch-pid", 0, "Compatibility integration process id")
	return cmd
}

func supervisorWorkerPID(ctx context.Context, roots paths.Roots, repositoryID string) int {
	request := supervisor.Request{Version: supervisor.ProtocolVersion,
		ID: fmt.Sprintf("compat-status-%d", time.Now().UnixNano()), Method: "status",
		DeadlineMS: time.Now().Add(5 * time.Second).UnixMilli()}
	response, err := (&supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: 5 * time.Second}).Do(ctx, request)
	if err != nil || response.Error != nil {
		return 0
	}
	raw, err := json.Marshal(response.Data)
	if err != nil {
		return 0
	}
	var status supervisor.Status
	if json.Unmarshal(raw, &status) != nil {
		return 0
	}
	for _, worker := range status.Workers {
		if worker.RepositoryID == repositoryID {
			return worker.PID
		}
	}
	return 0
}
func newCompatStopCmd() *cobra.Command {
	cmd := newInternalSessionCmd("close")
	cmd.Use = "stop"
	cmd.Hidden = true
	var all bool
	cmd.Flags().BoolVar(&all, "all", false, "Stop every repository worker")
	run := cmd.RunE
	cmd.RunE = func(command *cobra.Command, args []string) error {
		if all {
			return errors.New("acd stop --all is no longer supported; use acd off per repository")
		}
		return run(command, args)
	}
	return cmd
}
func newCompatHintCmd(name, kind string) *cobra.Command {
	cmd := &cobra.Command{Use: name, Hidden: true, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		repo, _ := cmd.Flags().GetString("repo")
		sessionID, _ := cmd.Flags().GetString("session-id")
		return sendInternalHint(cmd.Context(), repo, kind, false, "", sessionID, "compatibility", 0)
	}}
	cmd.Flags().String("session-id", "", "Compatibility session identity")
	if name == "touch" {
		cmd.Flags().Bool("soft-boundary", false, "Record a soft semantic boundary")
	}
	if name == "flush" {
		cmd.Flags().Bool("logical", false, "Record a logical boundary")
	}
	return cmd
}

func newCompatDaemonCmd() *cobra.Command {
	parent := &cobra.Command{Use: "daemon", Hidden: true}
	run := &cobra.Command{Use: "run", Hidden: true, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		repo, _ := cmd.Flags().GetString("repo")
		gitDir, _ := cmd.Flags().GetString("git-dir")
		record, _, worktreeRoot, err := lookupRegisteredRepo("daemon run", repo)
		if err != nil {
			return err
		}
		if gitDir != "" {
			resolved, resolveErr := git.AbsoluteGitDir(cmd.Context(), worktreeRoot)
			if resolveErr != nil || !central.SameRepoPath(resolved, gitDir) {
				return invalidCommandError("acd daemon run: --git-dir does not match the registered worktree")
			}
		}
		return runRepositoryWorker(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), record.RepositoryID, "")
	}}
	run.Flags().String("git-dir", "", "Compatibility Git directory assertion")
	parent.AddCommand(run)
	return parent
}

func runSupervisor(ctx context.Context, _ io.Writer, _ io.Writer) error {
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd supervisor: resolve paths: %w", err)
	}
	binary, err := os.Executable()
	if err != nil {
		return fmt.Errorf("acd supervisor: resolve executable: %w", err)
	}
	cctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	server := &supervisor.Server{
		Roots: roots, BinaryPath: binary, Version: version.String(),
		Handler: cliSupervisorHandler{
			roots: roots, environment: supervisor.WorkerEnvironment(os.Environ()),
		},
	}
	return server.Run(cctx)
}

func runRepositoryWorker(ctx context.Context, _ io.Writer, errOut io.Writer, repositoryID, publicationHold string) error {
	roots, err := paths.Resolve()
	if err != nil {
		return err
	}
	return runRepositoryWorkerAtRoots(ctx, errOut, roots, repositoryID, publicationHold)
}

func runWorkerAccessCheck(ctx context.Context, targets []string, resultPath string) error {
	if !filepath.IsAbs(resultPath) {
		return errors.New("acd worker access-check: --result must be absolute")
	}
	if len(targets) == 0 {
		return errors.New("acd worker access-check: at least one --path is required")
	}
	for _, target := range targets {
		if !filepath.IsAbs(target) {
			return fmt.Errorf("acd worker access-check: path must be absolute: %s", target)
		}
		status := supervisor.ServiceAccessStatus{State: "checking", Target: target}
		if err := supervisor.WriteServiceAccessStatus(resultPath, status); err != nil {
			return err
		}
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := git.ResolveWorktree(probeCtx, target)
		cancel()
		if err != nil {
			status.State = "failed"
			status.Error = err.Error()
			if writeErr := supervisor.WriteServiceAccessStatus(resultPath, status); writeErr != nil {
				return errors.Join(err, writeErr)
			}
			return err
		}
	}
	return supervisor.WriteServiceAccessStatus(resultPath,
		supervisor.ServiceAccessStatus{State: "completed"})
}

func runRepositoryWorkerAtRoots(ctx context.Context, errOut io.Writer, roots paths.Roots, repositoryID, publicationHold string) error {
	canonicalHold := roots.SetupPublicationHoldPath()
	if publicationHold != "" && filepath.Clean(publicationHold) != filepath.Clean(canonicalHold) {
		return errors.New("acd worker: publication hold path is not the canonical setup marker")
	}
	registry, err := central.Load(roots)
	if err != nil {
		return err
	}
	if registry.Version != central.RegistryVersion {
		return fmt.Errorf("acd worker: registry v%d requires `acd setup` before v%d workers can start", registry.Version, central.RegistryVersion)
	}
	records := make([]central.RepoRecord, 0)
	for _, record := range registry.Repos {
		if record.RepositoryID == repositoryID && !record.LifecycleDisabled() {
			records = append(records, record)
		}
	}
	if len(records) == 0 {
		return fmt.Errorf("acd worker: no enabled worktrees for repository %s", repositoryID)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].WorktreeID < records[j].WorktreeID })
	firstWT, err := git.ResolveWorktree(ctx, records[0].Path)
	if err != nil {
		return err
	}
	lock, err := daemon.AcquireDaemonLock(firstWT.GitDir)
	if err != nil {
		return fmt.Errorf("acd worker: acquire repository ownership: %w", err)
	}
	defer lock.Release()

	cctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	wakeSignals := make(chan os.Signal, 1)
	signal.Notify(wakeSignals, syscall.SIGUSR1)
	defer signal.Stop(wakeSignals)
	var wakeMu sync.RWMutex
	wakeTargets := make([]chan struct{}, 0, len(records))
	go func() {
		for {
			select {
			case <-cctx.Done():
				return
			case <-wakeSignals:
				wakeMu.RLock()
				for _, target := range wakeTargets {
					select {
					case target <- struct{}{}:
					default:
					}
				}
				wakeMu.RUnlock()
			}
		}
	}()
	errCh := make(chan error, len(records))
	closers := make([]io.Closer, 0, len(records)*2)
	runtimes := make(map[string]*workerRuntime, len(records))
	for _, record := range records {
		wt, resolveErr := git.ResolveWorktree(cctx, record.Path)
		if resolveErr != nil {
			return resolveErr
		}
		db, openErr := state.OpenRuntime(cctx, record.StateDB)
		if openErr != nil {
			return fmt.Errorf("acd worker: open %s: %w", record.Path, openErr)
		}
		closers = append(closers, db)
		opts, logCloser, buildErr := buildDaemonRunOptions(wt.Root, wt.GitDir, db, errOut)
		if buildErr != nil {
			return buildErr
		}
		if logCloser != nil {
			closers = append(closers, logCloser)
		}
		if buildErr := applyWorkerRestartOptions(roots, record, wt, &opts); buildErr != nil {
			return buildErr
		}
		opts.RepoHash = repositoryID
		opts.SkipDaemonLock = true
		opts.SkipSignals = true
		gate := &sync.RWMutex{}
		opts.OperationGate = gate
		opts.PublicationHeld = func() bool {
			info, statErr := os.Lstat(canonicalHold)
			return statErr == nil && info.Mode().IsRegular()
		}
		wakeCh := make(chan struct{}, 1)
		wakeMu.Lock()
		wakeTargets = append(wakeTargets, wakeCh)
		wakeMu.Unlock()
		opts.WakeCh = wakeCh
		opts.EmptySweepThreshold = math.MaxInt
		runtimes[record.WorktreeID] = &workerRuntime{record: record, worktree: wt, db: db, gate: gate}
		go func(path string, runOpts daemon.Options) {
			if runErr := daemon.Run(cctx, runOpts); runErr != nil && !errors.Is(runErr, context.Canceled) {
				errCh <- fmt.Errorf("%s: %w", path, runErr)
				return
			}
			errCh <- nil
		}(wt.Root, opts)
	}
	workerHandler := &repositoryWorkerHandler{runtimes: runtimes, wake: func() {
		wakeMu.RLock()
		defer wakeMu.RUnlock()
		for _, target := range wakeTargets {
			select {
			case target <- struct{}{}:
			default:
			}
		}
	}}
	workerServerErr := make(chan error, 1)
	go func() {
		workerServerErr <- supervisor.ServeWorker(cctx,
			supervisor.WorkerSocketPath(roots, repositoryID), workerHandler)
	}()
	defer func() {
		for i := len(closers) - 1; i >= 0; i-- {
			_ = closers[i].Close()
		}
	}()
	select {
	case <-cctx.Done():
		return nil
	case runErr := <-errCh:
		cancel()
		if runErr == nil {
			return errors.New("acd worker: a worktree loop stopped unexpectedly")
		}
		return runErr
	case serverErr := <-workerServerErr:
		cancel()
		return fmt.Errorf("acd worker: control server: %w", serverErr)
	}
}

func superviseRepositoryWorker(ctx context.Context, out, errOut io.Writer, roots paths.Roots, repositoryID, publicationHold string) error {
	if err := validateWorkerRoots(roots); err != nil {
		return err
	}
	if err := applyWorkerRootEnvironment(roots); err != nil {
		return err
	}
	if err := inheritSupervisorWorkerEnvironment(ctx, roots, repositoryID); err != nil {
		return err
	}
	cctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	runner := func(ctx context.Context, _ io.Writer, errOut io.Writer, repositoryID, publicationHold string) error {
		return runRepositoryWorkerAtRoots(ctx, errOut, roots, repositoryID, publicationHold)
	}
	return superviseRepositoryWorkerWith(cctx, out, errOut, roots, repositoryID, publicationHold, runner)
}

func inheritSupervisorWorkerEnvironment(ctx context.Context, roots paths.Roots, repositoryID string) error {
	request := supervisor.Request{
		Version: supervisor.ProtocolVersion,
		ID:      fmt.Sprintf("worker-environment-%s-%d", repositoryID, time.Now().UnixNano()),
		Method:  "worker_environment", RepositoryID: repositoryID,
		DeadlineMS: time.Now().Add(5 * time.Second).UnixMilli(),
	}
	response, err := (&supervisor.Client{
		SocketPath: roots.SupervisorSocketPath(), Timeout: 5 * time.Second,
	}).Do(ctx, request)
	if err != nil {
		return fmt.Errorf("acd worker: read supervisor environment: %w", err)
	}
	if response.Error != nil {
		return fmt.Errorf("acd worker: read supervisor environment: %s", response.Error.Message)
	}
	body, err := json.Marshal(response.Data)
	if err != nil {
		return fmt.Errorf("acd worker: encode supervisor environment: %w", err)
	}
	var values map[string]string
	if err := json.Unmarshal(body, &values); err != nil {
		return fmt.Errorf("acd worker: decode supervisor environment: %w", err)
	}
	if !supervisor.ValidWorkerEnvironment(values) {
		return errors.New("acd worker: supervisor returned an invalid environment")
	}
	for name, value := range values {
		if err := os.Setenv(name, value); err != nil {
			return fmt.Errorf("acd worker: apply supervisor environment %s: %w", name, err)
		}
	}
	return nil
}

type repositoryWorkerRunner func(context.Context, io.Writer, io.Writer, string, string) error

func superviseRepositoryWorkerWith(
	ctx context.Context,
	out, errOut io.Writer,
	roots paths.Roots,
	repositoryID, publicationHold string,
	runner repositoryWorkerRunner,
) error {
	restarts := 0
	for {
		if ctx.Err() != nil {
			return nil
		}
		_ = supervisor.WriteWorkerRuntimeStatus(roots, supervisor.WorkerRuntimeStatus{
			RepositoryID: repositoryID, PID: os.Getpid(), State: "starting", Restarts: restarts,
		})
		started := time.Now()
		attemptCtx, attemptCancel := context.WithCancel(ctx)
		runDone := make(chan error, 1)
		go func() {
			runDone <- runner(attemptCtx, out, errOut, repositoryID, publicationHold)
		}()
		readyDone := make(chan error, 1)
		go func() {
			readyDone <- waitRepositoryWorkerProtected(attemptCtx, roots, repositoryID)
		}()
		var runErr error
		ready := false
		runExited := false
		for !runExited {
			select {
			case <-ctx.Done():
				attemptCancel()
				<-runDone
				return nil
			case runErr = <-runDone:
				runExited = true
			case readyErr := <-readyDone:
				if readyErr == nil && !ready {
					ready = true
					_ = supervisor.WriteWorkerRuntimeStatus(roots, supervisor.WorkerRuntimeStatus{
						RepositoryID: repositoryID, PID: os.Getpid(), State: "running", Restarts: restarts,
					})
				}
				readyDone = nil
			}
		}
		attemptCancel()
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(started) >= 5*time.Minute {
			restarts = 0
		}
		restarts++
		message := "worker exited unexpectedly"
		if runErr != nil {
			message = runErr.Error()
		}
		stateName := "backoff"
		if restarts >= 5 {
			stateName = "needs_action"
		}
		_ = supervisor.WriteWorkerRuntimeStatus(roots, supervisor.WorkerRuntimeStatus{
			RepositoryID: repositoryID, PID: os.Getpid(), State: stateName,
			Restarts: restarts, LastError: message,
		})
		timer := time.NewTimer(supervisor.WorkerRestartDelay(restarts))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

func waitRepositoryWorkerProtected(ctx context.Context, roots paths.Roots, repositoryID string) error {
	registry, err := central.Load(roots)
	if err != nil {
		return err
	}
	records := make([]central.RepoRecord, 0)
	for _, record := range registry.Repos {
		if record.RepositoryID == repositoryID && !record.LifecycleDisabled() {
			records = append(records, record)
		}
	}
	if len(records) == 0 {
		return fmt.Errorf("acd worker: no enabled worktrees for repository %s", repositoryID)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].WorktreeID < records[j].WorktreeID })
	socket := supervisor.WorkerSocketPath(roots, repositoryID)
	for {
		allProtected := true
		for _, record := range records {
			request := supervisor.Request{
				Version: supervisor.ProtocolVersion,
				ID:      fmt.Sprintf("worker-ready-%s-%d", record.WorktreeID, time.Now().UnixNano()),
				Method:  "checkpoint_barrier", RepositoryID: repositoryID,
				WorktreeID: record.WorktreeID, DeadlineMS: time.Now().Add(60 * time.Second).UnixMilli(),
			}
			response, requestErr := supervisor.DoWorker(ctx, socket, request, 60*time.Second)
			if requestErr != nil || response.Error != nil {
				allProtected = false
				break
			}
		}
		if allProtected {
			return nil
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func validateWorkerRoots(roots paths.Roots) error {
	for name, root := range map[string]string{"state": roots.State, "share": roots.Share, "config": roots.Config} {
		if !filepath.IsAbs(root) || filepath.Base(filepath.Clean(root)) != "acd" {
			return fmt.Errorf("acd worker: %s root must be an absolute ACD directory", name)
		}
	}
	return nil
}

func applyWorkerRootEnvironment(roots paths.Roots) error {
	for name, value := range map[string]string{
		"XDG_STATE_HOME":  filepath.Dir(roots.State),
		"XDG_DATA_HOME":   filepath.Dir(roots.Share),
		"XDG_CONFIG_HOME": filepath.Dir(roots.Config),
	} {
		if err := os.Setenv(name, value); err != nil {
			return fmt.Errorf("acd worker: set %s: %w", name, err)
		}
	}
	return nil
}

func applyWorkerRestartOptions(roots paths.Roots, record central.RepoRecord, wt git.Worktree, opts *daemon.Options) error {
	document, err := config.NewStore(roots).Load()
	if err != nil {
		return fmt.Errorf("acd worker: load repository configuration: %w", err)
	}
	values, err := config.ResolveRestartEnvironment(document, record.WorktreeID, os.LookupEnv)
	if err != nil {
		return fmt.Errorf("acd worker: resolve repository configuration: %w", err)
	}
	parseInt := func(name string) (int64, error) {
		value, err := strconv.ParseInt(strings.TrimSpace(values[name]), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%s=%q: %w", name, values[name], err)
		}
		return value, nil
	}
	maxBytes, err := parseInt("ACD_MAX_FILE_BYTES")
	if err != nil {
		return err
	}
	eventDays, err := parseInt("ACD_EVENT_RETENTION_DAYS")
	if err != nil {
		return err
	}
	clientSeconds, err := parseInt("ACD_CLIENT_TTL_SECONDS")
	if err != nil {
		return err
	}
	rewindSeconds, err := parseInt("ACD_REWIND_GRACE_SECONDS")
	if err != nil {
		return err
	}
	shadowGenerations, err := parseInt("ACD_SHADOW_RETENTION_GENERATIONS")
	if err != nil {
		return err
	}
	rewindGrace := time.Duration(rewindSeconds) * time.Second
	opts.MaxFileBytes = maxBytes
	opts.EventRetention = time.Duration(eventDays) * 24 * time.Hour
	opts.ClientTTL = time.Duration(clientSeconds) * time.Second
	opts.FsnotifyEnabled = truthyConfig(values["ACD_FSNOTIFY_ENABLED"])
	opts.SensitiveMatcher = state.NewSensitiveMatcherFromValue(values["ACD_SENSITIVE_GLOBS"])
	opts.SafeIgnoreMatcher = state.NewSafeIgnoreMatcherFromValues(values["ACD_SAFE_IGNORE"], values["ACD_SAFE_IGNORE_EXTRA"])
	opts.RewindGrace = &rewindGrace
	opts.ShadowRetentionGenerations = &shadowGenerations
	if truthyConfig(values["ACD_TRACE"]) {
		writer, traceErr := acdtrace.New(acdtrace.Options{Repo: wt.Root, GitDir: wt.GitDir, Dir: strings.TrimSpace(os.Getenv(acdtrace.EnvTraceDir))})
		if traceErr == nil {
			opts.Trace = writer
		} else {
			opts.Trace = acdtrace.Noop{}
		}
	} else {
		opts.Trace = acdtrace.Noop{}
	}
	if truthyConfig(values["ACD_AI_PROMPT_TRACE"]) {
		writer, traceErr := prompttrace.New(prompttrace.Options{Repo: wt.Root, GitDir: wt.GitDir})
		if traceErr == nil {
			opts.PromptTrace = writer
		} else {
			opts.PromptTrace = prompttrace.Noop{}
		}
	} else {
		opts.PromptTrace = prompttrace.Noop{}
	}
	return nil
}

func truthyConfig(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on", "enabled":
		return true
	default:
		return false
	}
}

type workerRuntime struct {
	record   central.RepoRecord
	worktree git.Worktree
	db       *state.DB
	gate     *sync.RWMutex
}

type repositoryWorkerHandler struct {
	runtimes map[string]*workerRuntime
	wake     func()
}

func (h *repositoryWorkerHandler) HandleWorkerRequest(ctx context.Context, request supervisor.Request) (any, *supervisor.ProtocolError) {
	runtime, err := h.runtime(request.WorktreeID)
	if err != nil {
		return nil, protocolFailure("worktree_not_found", err, false)
	}
	if sessionErr := applyWorkerSessionParams(ctx, runtime.db, request.Params); sessionErr != nil {
		return nil, protocolFailure("session_update_failed", sessionErr, true)
	}
	if request.Method == "hint" || request.Method == "checkpoint_barrier" {
		if hintErr := applyWorkerActivityHint(ctx, runtime.db, request.Params); hintErr != nil {
			return nil, protocolFailure("hint_failed", hintErr, true)
		}
	}
	switch request.Method {
	case "status":
		projection, readErr := state.ReadCheckpointProjection(ctx, runtime.db.Path(), 100)
		if readErr != nil {
			return nil, protocolFailure("status_failed", readErr, true)
		}
		return projection, nil
	case "hint":
		if _, beginErr := daemon.BeginProtectionObservation(ctx, runtime.db); beginErr != nil {
			return nil, protocolFailure("observation_failed", beginErr, true)
		}
		h.wake()
		return map[string]bool{"accepted": true}, nil
	case "checkpoint_barrier":
		var params struct {
			DrainPublication bool `json:"drain_publication"`
		}
		_ = json.Unmarshal(request.Params, &params)
		acceptedEpoch, beginErr := daemon.BeginProtectionObservation(ctx, runtime.db)
		if beginErr != nil {
			return nil, protocolFailure("observation_failed", beginErr, true)
		}
		h.wake()
		deadline := time.NewTimer(30 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		var lastCovered, lastComplete, lastCheckpoint string
		var lastReadErr error
		for {
			select {
			case <-ctx.Done():
				return nil, protocolFailure("checkpoint_timeout", ctx.Err(), true)
			case <-deadline.C:
				return nil, protocolFailure("checkpoint_timeout", fmt.Errorf(
					"checkpoint barrier timed out (accepted_epoch=%d covered_epoch=%q complete=%q checkpoint=%q read_error=%v)",
					acceptedEpoch, lastCovered, lastComplete, lastCheckpoint, lastReadErr), true)
			case <-ticker.C:
				var coveredErr, completeErr, checkpointErr error
				lastCovered, _, coveredErr = state.MetaGet(ctx, runtime.db, daemon.MetaKeyProtectionCoveredEpoch)
				lastComplete, _, completeErr = state.MetaGet(ctx, runtime.db, daemon.MetaKeyProtectionComplete)
				lastCheckpoint, _, checkpointErr = state.MetaGet(ctx, runtime.db, daemon.MetaKeyProtectionCheckpointID)
				lastReadErr = errors.Join(coveredErr, completeErr, checkpointErr)
				coveredEpoch, parseErr := strconv.ParseInt(lastCovered, 10, 64)
				if lastReadErr == nil && parseErr == nil && coveredEpoch >= acceptedEpoch && lastComplete == "true" {
					if params.DrainPublication {
						pending, pendingErr := state.CountAllPendingCaptureEvents(ctx, runtime.db)
						if pendingErr != nil {
							return nil, protocolFailure("publication_status_failed", pendingErr, true)
						}
						if pending.Count > 0 {
							continue
						}
						// Publishing settles its durable rows before finishing guarded
						// live-index reconciliation. Fence on the operation gate so a
						// drained barrier cannot return while that final step is still
						// running.
						runtime.gate.Lock()
						runtime.gate.Unlock()
					}
					return map[string]any{"checkpoint_id": lastCheckpoint, "protected": true, "publication_drained": params.DrainPublication}, nil
				}
			}
		}
	case "history":
		projection, readErr := state.ReadCheckpointProjection(ctx, runtime.db.Path(), 100)
		if readErr != nil {
			return nil, protocolFailure("history_failed", readErr, false)
		}
		return projection, nil
	case "restore_plan", "restore_apply":
		var params struct {
			ID         string `json:"id"`
			PlanDigest string `json:"plan_digest,omitempty"`
		}
		if err := json.Unmarshal(request.Params, &params); err != nil || params.ID == "" {
			return nil, &supervisor.ProtocolError{Code: "invalid_request", Message: "restore checkpoint id is required"}
		}
		runtime.gate.Lock()
		defer runtime.gate.Unlock()
		plan, previewErr := restorepkg.Preview(ctx, runtime.worktree.Root, runtime.worktree.GitDir, runtime.db.Path(), params.ID)
		if previewErr != nil {
			return nil, protocolFailure("restore_preview_failed", previewErr, false)
		}
		if request.Method == "restore_plan" {
			return plan, nil
		}
		if params.PlanDigest == "" || params.PlanDigest != plan.PlanDigest {
			return nil, &supervisor.ProtocolError{Code: "plan_changed", Message: "restore plan changed; preview again"}
		}
		result, applyErr := restorepkg.Apply(ctx, runtime.db, plan)
		if applyErr != nil {
			return nil, protocolFailure("restore_apply_failed", applyErr, false)
		}
		return result, nil
	case "repair":
		var params struct {
			Apply       bool   `json:"apply"`
			OperationID string `json:"operation_id,omitempty"`
		}
		if len(request.Params) > 0 && json.Unmarshal(request.Params, &params) != nil {
			return nil, &supervisor.ProtocolError{Code: "invalid_request", Message: "invalid repair request"}
		}
		runtime.gate.Lock()
		defer runtime.gate.Unlock()
		plan, previewErr := restorepkg.PreviewRepair(ctx, runtime.db)
		if previewErr != nil {
			return nil, protocolFailure("repair_preview_failed", previewErr, false)
		}
		if !params.Apply {
			return plan, nil
		}
		if params.OperationID == "" || params.OperationID != plan.OperationID {
			return nil, &supervisor.ProtocolError{Code: "plan_changed", Message: "repair plan changed; preview again"}
		}
		result, repairErr := restorepkg.Repair(ctx, runtime.worktree.Root, runtime.db, plan)
		if repairErr != nil {
			return nil, protocolFailure("repair_failed", repairErr, false)
		}
		return result, nil
	default:
		return nil, &supervisor.ProtocolError{Code: "invalid_request", Message: "unsupported worker request"}
	}
}

func applyWorkerActivityHint(ctx context.Context, db *state.DB, raw json.RawMessage) error {
	var params struct {
		Kind             string `json:"kind"`
		SessionID        string `json:"session_id"`
		DrainPublication bool   `json:"drain_publication"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &params) != nil {
		return nil
	}
	var kind, source string
	switch params.Kind {
	case "soft_boundary":
		kind, source = state.IntentBoundarySoft, "touch_soft_boundary"
	case "logical_boundary":
		kind, source = state.IntentBoundaryHard, "flush_logical"
	default:
		if !params.DrainPublication {
			return nil
		}
		kind, source = state.IntentBoundaryHard, "checkpoint_barrier_drain"
	}
	if _, err := state.AppendIntentActivityBoundary(ctx, db,
		newIntentActivityBoundary(kind, source)); err != nil {
		return err
	}
	if params.Kind == "logical_boundary" || params.DrainPublication {
		note := sql.NullString{String: params.SessionID, Valid: params.SessionID != ""}
		_, err := state.EnqueueFlushRequest(ctx, db, "flush_logical", true, note)
		return err
	}
	return nil
}

func applyWorkerSessionParams(ctx context.Context, db *state.DB, raw json.RawMessage) error {
	var params struct {
		SessionAction string `json:"session_action"`
		SessionID     string `json:"session_id"`
		Harness       string `json:"harness"`
		WatchPID      int64  `json:"watch_pid"`
	}
	if len(raw) == 0 || json.Unmarshal(raw, &params) != nil || params.SessionID == "" {
		return nil
	}
	switch params.SessionAction {
	case "open":
		harness := params.Harness
		if harness == "" {
			harness = "integration"
		}
		client := state.Client{SessionID: params.SessionID, Harness: harness}
		if params.WatchPID > 0 {
			client.WatchPID = sql.NullInt64{Int64: params.WatchPID, Valid: true}
		}
		return state.RegisterClient(ctx, db, client)
	case "close":
		_, err := state.DeregisterClient(ctx, db, params.SessionID)
		return err
	default:
		_, err := state.TouchClient(ctx, db, params.SessionID,
			float64(time.Now().UnixNano())/float64(time.Second))
		return err
	}
}

func (h *repositoryWorkerHandler) runtime(worktreeID string) (*workerRuntime, error) {
	if worktreeID != "" {
		if runtime := h.runtimes[worktreeID]; runtime != nil {
			return runtime, nil
		}
		return nil, fmt.Errorf("worktree %s is not managed by this worker", worktreeID)
	}
	if len(h.runtimes) != 1 {
		return nil, errors.New("worktree identity is required")
	}
	for _, runtime := range h.runtimes {
		return runtime, nil
	}
	return nil, errors.New("worker has no worktrees")
}

type cliSupervisorHandler struct {
	roots       paths.Roots
	environment map[string]string
}

func (h cliSupervisorHandler) HandleSupervisorRequest(ctx context.Context, request supervisor.Request) (any, *supervisor.ProtocolError) {
	switch request.Method {
	case "worker_environment":
		registry, err := central.Load(h.roots)
		if err != nil {
			return nil, protocolFailure("registry_unavailable", err, true)
		}
		for _, record := range registry.Repos {
			if record.RepositoryID == request.RepositoryID && !record.LifecycleDisabled() {
				result := make(map[string]string, len(h.environment))
				for name, value := range h.environment {
					result[name] = value
				}
				return result, nil
			}
		}
		return nil, &supervisor.ProtocolError{
			Code: "repository_not_enabled", Message: "repository is not enabled", Retryable: false,
		}
	case "enable_repository", "disable_repository":
		var result central.RepoLifecycleResult
		err := central.WithLock(h.roots, func(registry *central.Registry) error {
			if registry.Version != central.RegistryVersion {
				return state.ErrSetupRequired
			}
			if request.Method == "enable_repository" {
				result = registry.EnableRepo(central.RepoRemovalTarget{Path: worktreePath(registry, request)}, 0)
			} else {
				result = registry.DisableRepo(central.RepoRemovalTarget{Path: worktreePath(registry, request)}, 0)
			}
			if result.NotFound {
				return fmt.Errorf("repository is not registered")
			}
			return nil
		})
		if err != nil {
			return nil, protocolFailure("repository_update_failed", err, false)
		}
		return result, nil
	case "hint", "checkpoint_barrier":
		fallthrough
	case "history", "restore_plan", "restore_apply", "repair":
		workerTimeout := 30 * time.Second
		if request.DeadlineMS > 0 {
			if remaining := time.Until(time.UnixMilli(request.DeadlineMS)); remaining > workerTimeout {
				workerTimeout = remaining
			}
		}
		response, err := supervisor.DoWorker(ctx, supervisor.WorkerSocketPath(h.roots, request.RepositoryID), request, workerTimeout)
		if err != nil {
			return nil, protocolFailure("worker_unavailable", err, true)
		}
		if response.Error != nil {
			return nil, response.Error
		}
		return response.Data, nil
	default:
		return nil, &supervisor.ProtocolError{Code: "invalid_request", Message: "unsupported request", Retryable: false}
	}
}

func worktreePath(registry *central.Registry, request supervisor.Request) string {
	for _, record := range registry.Repos {
		if record.RepositoryID == request.RepositoryID && (request.WorktreeID == "" || record.WorktreeID == request.WorktreeID) {
			return record.Path
		}
	}
	return ""
}

func protocolFailure(code string, err error, retryable bool) *supervisor.ProtocolError {
	return &supervisor.ProtocolError{Code: code, Message: err.Error(), Retryable: retryable}
}
