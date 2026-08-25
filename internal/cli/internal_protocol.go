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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
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
	supervisorCmd.AddCommand(&cobra.Command{
		Use: "upgrade-compatible", Hidden: true, Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			roots, err := paths.Resolve()
			if err != nil {
				return err
			}
			executable, err := os.Executable()
			if err != nil {
				return err
			}
			_, err = installer.ApplyCompatibleRuntime(cmd.Context(), roots, installer.RuntimeUpgradeOptions{
				SourceExecutable: executable, SourceVersion: version.String(),
				Compatibility: runtimeCompatibility(), Integrations: "auto",
				AllowUnadvertised: true,
			})
			return err
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
	worker.AddCommand(workerRun, workerSupervise)
	hint := withInvocationCapabilities(newInternalHintCmd(), commandCapabilities{Repository: true, JSON: true, Quiet: true})
	session := &cobra.Command{Use: "session", Hidden: true}
	session.AddCommand(
		withInvocationCapabilities(newInternalSessionCmd("open"), commandCapabilities{Repository: true, JSON: true, Quiet: true}),
		withInvocationCapabilities(newInternalSessionCmd("close"), commandCapabilities{Repository: true, JSON: true, Quiet: true}),
	)
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
	if strings.TrimSpace(record.RepositoryID) == "" || strings.TrimSpace(record.WorktreeID) == "" {
		return fmt.Errorf("acd internal hint: repository setup is still in progress; retry after `acd setup` completes")
	}
	triggerUpgrade, err := ensureMutationSupervisorMode(ctx, roots, true)
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
	request := supervisor.Request{Version: supervisor.ProtocolVersion, ID: fmt.Sprintf("hint-%d", time.Now().UnixNano()), Method: method, RepositoryID: record.RepositoryID, WorktreeID: record.WorktreeID, DeadlineMS: time.Now().Add(supervisor.CheckpointBarrierTimeout).UnixMilli(), Params: params}
	response, err := (&supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: supervisor.CheckpointBarrierTimeout}).Do(ctx, request)
	if err != nil {
		return err
	}
	if response.Error != nil {
		return errors.New(response.Error.Message)
	}
	if triggerUpgrade != nil {
		_ = triggerUpgrade()
	}
	return nil
}

func newCompatStartCmd() *cobra.Command {
	var sessionID, harness string
	var watchPID int
	cmd := &cobra.Command{Use: "start", Hidden: true, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if strings.TrimSpace(sessionID) == "" {
			return invalidCommandError("acd start is an integration-session alias; use `acd on` to enable protection")
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
		sessionID, _ := command.Flags().GetString("session-id")
		if strings.TrimSpace(sessionID) == "" {
			return invalidCommandError("acd stop only closes an integration session; use `acd off` to disable protection")
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
	binaryDigest, err := version.FileDigest(binary)
	if err != nil {
		return fmt.Errorf("acd supervisor: digest executable: %w", err)
	}
	cctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	server := &supervisor.Server{
		Roots: roots, BinaryPath: binary, Version: version.String(), BinaryDigest: binaryDigest,
		Compatibility: runtimeCompatibility(),
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
	wakeTargets := make(map[string]chan struct{}, len(records))
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
		restorePending, pendingErr := state.RestoreRepairPending(cctx, db)
		if pendingErr != nil {
			return fmt.Errorf("acd worker: inspect interrupted restore for %s: %w", record.Path, pendingErr)
		}
		if !restorePending {
			if recoverErr := daemon.RecoverSelfPublicationsBeforePlanning(cctx, wt.Root, db); recoverErr != nil {
				return fmt.Errorf("acd worker: recover publication journal for %s: %w", record.Path, recoverErr)
			}
		}
		if _, reconcileErr := state.ReconcileCheckpointIntentMemberships(cctx, db, record.WorktreeID); reconcileErr != nil {
			return fmt.Errorf("acd worker: reconcile checkpoint publication state for %s: %w", record.Path, reconcileErr)
		}
		opts, logCloser, buildErr := buildDaemonRunOptionsWithID(wt.Root, wt.GitDir, db, repositoryID)
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
		restoreHeld := &atomic.Bool{}
		restoreHeld.Store(restorePending)
		opts.OperationGate = gate
		opts.PublicationHeld = func() bool {
			return workerPublicationHeld(canonicalHold, restoreHeld)
		}
		wakeCh := make(chan struct{}, 1)
		wakeMu.Lock()
		wakeTargets[record.WorktreeID] = wakeCh
		wakeMu.Unlock()
		opts.WakeCh = wakeCh
		opts.EmptySweepThreshold = math.MaxInt
		runtimes[record.WorktreeID] = &workerRuntime{
			record: record, worktree: wt, db: db, gate: gate,
			policy: restorepkg.ProtectionPolicy{
				Sensitive: opts.SensitiveMatcher, SafeIgnore: opts.SafeIgnoreMatcher,
				MaxFileBytes: opts.MaxFileBytes,
			},
			restoreHeld: restoreHeld,
		}
		go func(path string, runOpts daemon.Options) {
			if runErr := daemon.Run(cctx, runOpts); runErr != nil && !errors.Is(runErr, context.Canceled) {
				errCh <- fmt.Errorf("%s: %w", path, runErr)
				return
			}
			errCh <- nil
		}(wt.Root, opts)
	}
	workerHandler := &repositoryWorkerHandler{repositoryID: repositoryID, runtimes: runtimes, wake: func(worktreeID string) {
		wakeMu.RLock()
		defer wakeMu.RUnlock()
		target, ok := wakeTargets[worktreeID]
		if !ok {
			return
		}
		select {
		case target <- struct{}{}:
		default:
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
				WorktreeID: record.WorktreeID, DeadlineMS: time.Now().Add(supervisor.CheckpointBarrierTimeout).UnixMilli(),
			}
			response, requestErr := supervisor.DoWorker(ctx, socket, request, supervisor.CheckpointBarrierTimeout)
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
	record      central.RepoRecord
	worktree    git.Worktree
	db          *state.DB
	gate        *sync.RWMutex
	policy      restorepkg.ProtectionPolicy
	restoreHeld *atomic.Bool
}

type repositoryWorkerHandler struct {
	repositoryID string
	runtimes     map[string]*workerRuntime
	wake         func(string)
}

func (h *repositoryWorkerHandler) HandleWorkerRequest(ctx context.Context, request supervisor.Request) (any, *supervisor.ProtocolError) {
	if request.Method == "status" && request.WorktreeID == "" {
		if request.RepositoryID != h.repositoryID {
			return nil, &supervisor.ProtocolError{
				Code: "worker_identity_mismatch", Message: "worker repository identity does not match the request",
			}
		}
		return supervisor.WorkerReadiness{
			RepositoryID: h.repositoryID, PID: os.Getpid(), Ready: true,
		}, nil
	}
	runtime, err := h.runtime(request.WorktreeID)
	if err != nil {
		return nil, protocolFailure("worktree_not_found", err, false)
	}
	publicationDrainStart := request.Method == "publication_drain_start"
	if publicationDrainStart {
		request.Method = "checkpoint_barrier"
		var params map[string]any
		if len(request.Params) > 0 {
			_ = json.Unmarshal(request.Params, &params)
		}
		if params == nil {
			params = make(map[string]any)
		}
		params["drain_publication"] = true
		request.Params, _ = json.Marshal(params)
	}
	if sessionErr := applyWorkerSessionParams(ctx, runtime.db, request.Params); sessionErr != nil {
		return nil, protocolFailure("session_update_failed", sessionErr, true)
	}
	if request.Method == "hint" {
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
		h.wake(request.WorktreeID)
		return map[string]bool{"accepted": true}, nil
	case "publication_drain_status":
		projection, projectionErr := state.ReadPublicationDrainProjection(
			ctx, runtime.db.Path())
		if projectionErr != nil {
			return nil, protocolFailure(
				"publication_status_failed", projectionErr, true)
		}
		return projection, nil
	case "checkpoint_barrier":
		var params struct {
			DrainPublication bool `json:"drain_publication"`
			ConsumeStaged    bool `json:"consume_staged"`
		}
		_ = json.Unmarshal(request.Params, &params)
		var drainAnchor publicationDrainTarget
		var requestedPublicationBranch string
		var minimumPublicationCheckpointSeq int64
		publicationWorktreeID := checkpointpkg.WorktreeID(runtime.worktree.Root)
		runtime.gate.Lock()
		if params.DrainPublication {
			reason, unsafeErr := publicationUnsafeReason(
				ctx, runtime.worktree, params.ConsumeStaged)
			if unsafeErr != nil {
				runtime.gate.Unlock()
				return nil, protocolFailure("publication_status_failed", unsafeErr, true)
			}
			if reason != "" {
				runtime.gate.Unlock()
				return nil, protocolFailure("publication_needs_action", errors.New(reason), false)
			}
			branchRef, branchErr := git.RunBranchRef(ctx, runtime.worktree.Root)
			if branchErr != nil {
				runtime.gate.Unlock()
				return nil, protocolFailure("publication_status_failed", branchErr, true)
			}
			generation, anchorErr := daemon.LoadBranchGeneration(ctx, runtime.db)
			if anchorErr == nil {
				activeDrain, activeErr := daemon.ActivePublicationDrainForPair(
					ctx, runtime.db, branchRef, generation)
				if activeErr != nil {
					anchorErr = activeErr
				} else if activeDrain == nil {
					activeDrain, anchorErr = daemon.RestartablePublicationDrainForPair(
						ctx, runtime.db, branchRef, generation)
				}
				if anchorErr == nil && activeDrain != nil {
					*activeDrain, anchorErr = daemon.ResumePublicationDrainCheckpointing(
						ctx, runtime.worktree.Root, runtime.db, *activeDrain, time.Now())
					if anchorErr != nil {
						runtime.gate.Unlock()
						return nil, protocolFailure(
							"publication_status_failed", anchorErr, true)
					}
					runtime.gate.Unlock()
					h.wake(request.WorktreeID)
					if publicationDrainStart {
						return publicationDrainOperationResult(ctx, runtime.db, *activeDrain)
					}
					return waitForPublicationDrain(
						ctx, runtime.db, *activeDrain)
				}
			}
			if anchorErr == nil {
				requestedPublicationBranch = branchRef
				drainAnchor = publicationDrainTarget{BranchRef: branchRef}
				persistedToken, tokenExists, tokenErr := state.MetaGet(
					ctx, runtime.db, daemon.MetaKeyBranchToken)
				if tokenErr != nil {
					anchorErr = tokenErr
				} else if !tokenExists || publicationTokenBranchRef(persistedToken) == branchRef {
					drainAnchor, anchorErr = snapshotPublicationDrainTarget(
						ctx, runtime.db, branchRef, generation)
				}
			}
			if anchorErr != nil {
				runtime.gate.Unlock()
				return nil, protocolFailure("publication_status_failed", anchorErr, true)
			}
		}
		if hintErr := applyWorkerActivityHint(ctx, runtime.db, request.Params); hintErr != nil {
			runtime.gate.Unlock()
			return nil, protocolFailure("hint_failed", hintErr, true)
		}
		if params.DrainPublication {
			var checkpointSeqErr error
			minimumPublicationCheckpointSeq, checkpointSeqErr = state.LatestCheckpointSeq(
				ctx, runtime.db)
			if checkpointSeqErr != nil {
				runtime.gate.Unlock()
				return nil, protocolFailure("observation_failed", checkpointSeqErr, true)
			}
		}
		acceptedEpoch, beginErr := daemon.BeginProtectionObservation(ctx, runtime.db)
		if beginErr != nil {
			runtime.gate.Unlock()
			return nil, protocolFailure("observation_failed", beginErr, true)
		}
		if params.DrainPublication {
			if requireErr := daemon.RequireProtectionCheckpoint(
				ctx, runtime.db, publicationWorktreeID, acceptedEpoch); requireErr != nil {
				runtime.gate.Unlock()
				return nil, protocolFailure("observation_failed", requireErr, true)
			}
		}
		h.wake(request.WorktreeID)
		runtime.gate.Unlock()
		deadline := time.NewTimer(checkpointBarrierWait(ctx))
		defer deadline.Stop()
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		var lastCovered, lastComplete, lastCheckpoint, lastRejectedCheckpoint string
		var lastReadErr error
		var drainTarget publicationDrainTarget
		var lastUnsafeCheck time.Time
		for {
			select {
			case <-ctx.Done():
				return nil, protocolFailure("checkpoint_timeout", fmt.Errorf(
					"checkpoint barrier interrupted (accepted_epoch=%d covered_epoch=%q complete=%q checkpoint=%q rejected_checkpoint=%q read_error=%v): %w",
					acceptedEpoch, lastCovered, lastComplete, lastCheckpoint,
					lastRejectedCheckpoint, lastReadErr, ctx.Err()), true)
			case <-deadline.C:
				return nil, protocolFailure("checkpoint_timeout", fmt.Errorf(
					"checkpoint barrier timed out (accepted_epoch=%d covered_epoch=%q complete=%q checkpoint=%q rejected_checkpoint=%q read_error=%v)",
					acceptedEpoch, lastCovered, lastComplete, lastCheckpoint,
					lastRejectedCheckpoint, lastReadErr), true)
			case <-ticker.C:
				var coveredErr, completeErr, checkpointErr error
				lastCovered, _, coveredErr = state.MetaGet(ctx, runtime.db, daemon.MetaKeyProtectionCoveredEpoch)
				lastComplete, _, completeErr = state.MetaGet(ctx, runtime.db, daemon.MetaKeyProtectionComplete)
				lastCheckpoint, _, checkpointErr = state.MetaGet(ctx, runtime.db, daemon.MetaKeyProtectionCheckpointID)
				lastReadErr = errors.Join(coveredErr, completeErr, checkpointErr)
				coveredEpoch, parseErr := strconv.ParseInt(lastCovered, 10, 64)
				barrierReady := lastReadErr == nil && parseErr == nil &&
					coveredEpoch >= acceptedEpoch && lastComplete == "true"
				if params.DrainPublication {
					currentBranch, branchErr := git.RunBranchRef(ctx, runtime.worktree.Root)
					if branchErr != nil {
						return nil, protocolFailure("publication_status_failed", branchErr, true)
					}
					expectedBranch := requestedPublicationBranch
					if drainTarget.EventSeqs != nil {
						expectedBranch = drainTarget.BranchRef
					}
					if currentBranch != expectedBranch {
						return nil, protocolFailure("publication_needs_action", fmt.Errorf(
							"publication branch changed while saving work: before=%s current=%s",
							expectedBranch, currentBranch), false)
					}
					if drainTarget.EventSeqs == nil {
						checkpoint, found, selectErr := state.CompletedCheckpointForBarrier(
							ctx, runtime.db, publicationWorktreeID, acceptedEpoch,
							minimumPublicationCheckpointSeq, expectedBranch)
						if selectErr != nil {
							return nil, protocolFailure(
								"publication_status_failed", selectErr, true)
						}
						if found {
							lastCheckpoint = checkpoint.ID
							barrierReady = true
						} else {
							lastRejectedCheckpoint = lastCheckpoint
							barrierReady = false
							h.wake(request.WorktreeID)
						}
					} else if generation, generationErr := daemon.LoadBranchGeneration(
						ctx, runtime.db); generationErr != nil {
						return nil, protocolFailure(
							"publication_status_failed", generationErr, true)
					} else if generation != drainTarget.Generation {
						return nil, protocolFailure("publication_needs_action", fmt.Errorf(
							"publication branch generation changed while saving work: before=%d current=%d",
							drainTarget.Generation, generation), false)
					}
				}
				if barrierReady {
					if params.DrainPublication {
						if drainTarget.EventSeqs == nil {
							lastUnsafeCheck = time.Now()
							runtime.gate.Lock()
							reason, unsafeErr := publicationUnsafeReason(
								ctx, runtime.worktree, params.ConsumeStaged)
							if unsafeErr == nil && reason == "" && drainAnchor.EventSeqs == nil {
								generation, generationErr := daemon.LoadBranchGeneration(
									ctx, runtime.db)
								if generationErr != nil {
									unsafeErr = generationErr
								} else {
									drainAnchor, unsafeErr = snapshotPublicationDrainTarget(
										ctx, runtime.db, requestedPublicationBranch, generation)
								}
							}
							if unsafeErr == nil && reason == "" {
								drainTarget, unsafeErr = freezePublicationDrainTarget(
									ctx, runtime.db, runtime.worktree.Root, lastCheckpoint,
									publicationWorktreeID, acceptedEpoch, drainAnchor)
								if unsafeErr == nil {
									nowTS := float64(time.Now().UnixNano()) / 1e9
									preparedDrain := state.PublicationDrain{
										ID:               "drain-" + lastCheckpoint,
										CheckpointID:     lastCheckpoint,
										WorktreeID:       publicationWorktreeID,
										BranchRef:        drainTarget.BranchRef,
										BranchGeneration: drainTarget.Generation,
										Phase:            state.PublicationDrainCheckpointing,
										TargetEventCount: int64(len(drainTarget.EventSeqs)),
										CreatedTS:        nowTS, UpdatedTS: nowTS,
										LastProgressTS: nowTS,
										StagedConsent:  params.ConsumeStaged,
										EventSeqs:      append([]int64(nil), drainTarget.EventSeqs...),
									}
									if unsafeErr == nil {
										_, unsafeErr = state.PreparePublicationDrain(
											ctx, runtime.db, preparedDrain)
									}
									if unsafeErr == nil {
										persistedDrain, loadErr := state.PublicationDrainByID(
											ctx, runtime.db, preparedDrain.ID)
										if loadErr != nil {
											unsafeErr = loadErr
										} else {
											var activeDrain state.PublicationDrain
											activeDrain, unsafeErr = daemon.ResumePublicationDrainCheckpointing(
												ctx, runtime.worktree.Root, runtime.db,
												persistedDrain, time.Now().UTC())
											if unsafeErr == nil &&
												activeDrain.Phase == state.PublicationDrainNeedsAction {
												runtime.gate.Unlock()
												return nil, protocolFailure(
													"publication_needs_action",
													errors.New(activeDrain.LastError), false)
											}
											if unsafeErr == nil && publicationDrainStart {
												h.wake(request.WorktreeID)
												runtime.gate.Unlock()
												return publicationDrainOperationResult(
													ctx, runtime.db, activeDrain)
											}
										}
									}
								}
								if unsafeErr == nil {
									h.wake(request.WorktreeID)
								}
							}
							runtime.gate.Unlock()
							if unsafeErr != nil {
								return nil, protocolFailure("publication_status_failed", unsafeErr, true)
							}
							if reason != "" {
								return nil, protocolFailure("publication_needs_action", errors.New(reason), false)
							}
						} else if time.Since(lastUnsafeCheck) >= time.Second {
							lastUnsafeCheck = time.Now()
							runtime.gate.Lock()
							reason, unsafeErr := publicationUnsafeReason(
								ctx, runtime.worktree, params.ConsumeStaged)
							runtime.gate.Unlock()
							if unsafeErr != nil {
								return nil, protocolFailure("publication_status_failed", unsafeErr, true)
							}
							if reason != "" {
								return nil, protocolFailure("publication_needs_action", errors.New(reason), false)
							}
						}
						progress, statusErr := publicationDrainStatus(ctx, runtime.db, drainTarget)
						if statusErr != nil {
							return nil, protocolFailure("publication_status_failed", statusErr, true)
						}
						if progress.Recovered > 0 || progress.Terminal > 0 {
							return nil, protocolFailure("publication_needs_action", fmt.Errorf(
								"target events did not publish (recovered=%d terminal=%d)",
								progress.Recovered, progress.Terminal), false)
						}
						if progress.Remaining > 0 {
							continue
						}
						// Publishing settles its durable rows before finishing guarded
						// live-index reconciliation. Fence on the operation gate so a
						// drained barrier cannot return while that final step is still
						// running.
						runtime.gate.Lock()
						runtime.gate.Unlock()
						if durableDrain, ok, loadErr := state.PublicationDrainByCheckpoint(
							ctx, runtime.db, lastCheckpoint); loadErr != nil {
							return nil, protocolFailure(
								"publication_status_failed", loadErr, true)
						} else if ok && durableDrain.Phase != state.PublicationDrainCompleted {
							nowTS := float64(time.Now().UnixNano()) / 1e9
							update := daemon.PublicationDrainUpdateFrom(
								durableDrain, nowTS, nowTS)
							update.Phase = state.PublicationDrainCompleted
							update.PublishedEventCount = durableDrain.TargetEventCount
							update.CommitCount = progress.Commits
							update.CompletedTS = sql.NullFloat64{
								Float64: nowTS, Valid: true,
							}
							if _, advanceErr := state.AdvancePublicationDrain(
								ctx, runtime.db, durableDrain.ID, update); advanceErr != nil {
								return nil, protocolFailure(
									"publication_status_failed", advanceErr, true)
							}
						}
						return publicationDrainResult(lastCheckpoint, drainTarget, progress, true, ""), nil
					}
					return map[string]any{
						"checkpoint_id": lastCheckpoint, "protected": true,
						"publication_drained": false,
					}, nil
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
		plan, previewErr := restorepkg.PreviewWithPolicy(ctx, runtime.worktree.Root,
			runtime.worktree.GitDir, runtime.db.Path(), params.ID, runtime.policy)
		if previewErr != nil {
			return nil, protocolFailure("restore_preview_failed", previewErr, false)
		}
		if request.Method == "restore_plan" {
			return plan, nil
		}
		if params.PlanDigest == "" || params.PlanDigest != plan.PlanDigest {
			return nil, &supervisor.ProtocolError{Code: "plan_changed", Message: "restore plan changed; preview again"}
		}
		setRestorePublicationHold(runtime, true)
		result, applyErr := restorepkg.Apply(ctx, runtime.db, plan)
		holdErr := refreshRestorePublicationHold(ctx, runtime)
		if holdErr != nil {
			return nil, protocolFailure("restore_status_failed", holdErr, true)
		}
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
			if errors.Is(previewErr, sql.ErrNoRows) {
				setRestorePublicationHold(runtime, false)
				return restorepkg.RepairPlan{}, nil
			}
			return nil, protocolFailure("repair_preview_failed", previewErr, false)
		}
		if !params.Apply {
			return plan, nil
		}
		if params.OperationID == "" || params.OperationID != plan.OperationID {
			return nil, &supervisor.ProtocolError{Code: "plan_changed", Message: "repair plan changed; preview again"}
		}
		result, repairErr := restorepkg.RepairWithPolicy(ctx, runtime.worktree.Root,
			runtime.db, plan, runtime.policy)
		holdErr := refreshRestorePublicationHold(ctx, runtime)
		if holdErr != nil {
			return nil, protocolFailure("restore_status_failed", holdErr, true)
		}
		if repairErr != nil {
			return nil, protocolFailure("repair_failed", repairErr, false)
		}
		return result, nil
	default:
		return nil, &supervisor.ProtocolError{Code: "invalid_request", Message: "unsupported worker request"}
	}
}

func refreshRestorePublicationHold(ctx context.Context, runtime *workerRuntime) error {
	pending, err := state.RestoreRepairPending(ctx, runtime.db)
	if err != nil {
		return err
	}
	setRestorePublicationHold(runtime, pending)
	return nil
}

func setRestorePublicationHold(runtime *workerRuntime, held bool) {
	if runtime.restoreHeld != nil {
		runtime.restoreHeld.Store(held)
	}
}

func workerPublicationHeld(setupMarker string, restoreHeld *atomic.Bool) bool {
	if restoreHeld != nil && restoreHeld.Load() {
		return true
	}
	info, err := os.Lstat(setupMarker)
	return err == nil && info.Mode().IsRegular()
}

type publicationDrainTarget struct {
	BranchRef  string
	Generation int64
	EventSeqs  []int64
	MaxSeq     int64
}

type publicationDrainProgress struct {
	Remaining, Published, Recovered, Terminal, Commits int64
}

func publicationTokenBranchRef(token string) string {
	parts := strings.SplitN(strings.TrimSpace(token), " ", 2)
	if len(parts) != 2 {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func snapshotPublicationDrainTarget(
	ctx context.Context,
	db *state.DB,
	branchRef string,
	generation int64,
) (publicationDrainTarget, error) {
	if branchRef == "" || generation <= 0 {
		return publicationDrainTarget{}, errors.New("pre-barrier publication branch generation is unavailable")
	}
	target := publicationDrainTarget{
		BranchRef: branchRef, Generation: generation, EventSeqs: []int64{},
	}
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT seq FROM capture_events
WHERE branch_ref=? AND branch_generation=? AND state='pending'
ORDER BY seq`, branchRef, generation)
	if err != nil {
		return publicationDrainTarget{}, fmt.Errorf("snapshot pre-barrier publication backlog: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return publicationDrainTarget{}, fmt.Errorf("scan pre-barrier publication backlog: %w", err)
		}
		target.EventSeqs = append(target.EventSeqs, seq)
		target.MaxSeq = seq
	}
	if err := rows.Err(); err != nil {
		return publicationDrainTarget{}, fmt.Errorf("iterate pre-barrier publication backlog: %w", err)
	}
	return target, nil
}

func freezePublicationDrainTarget(
	ctx context.Context,
	db *state.DB,
	repoRoot, checkpointID, worktreeID string,
	minimumCoverageEpoch int64,
	anchor publicationDrainTarget,
) (publicationDrainTarget, error) {
	if strings.TrimSpace(checkpointID) == "" {
		return publicationDrainTarget{}, errors.New("publication checkpoint identity is unavailable")
	}
	if anchor.BranchRef == "" || anchor.Generation <= 0 {
		return publicationDrainTarget{}, errors.New("pre-barrier publication branch generation is unavailable")
	}
	branchRef, err := git.RunBranchRef(ctx, repoRoot)
	if err != nil {
		return publicationDrainTarget{}, fmt.Errorf("resolve publication branch: %w", err)
	}
	if branchRef != anchor.BranchRef {
		return publicationDrainTarget{}, fmt.Errorf(
			"publication branch changed after barrier: before=%s current=%s", anchor.BranchRef, branchRef)
	}
	headBefore, err := publicationHead(ctx, repoRoot)
	if err != nil {
		return publicationDrainTarget{}, fmt.Errorf("resolve publication HEAD: %w", err)
	}
	tx, err := db.ReadSQL().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return publicationDrainTarget{}, fmt.Errorf("begin publication target snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var observedHead, observedRef, checkpointWorktreeID, phase string
	var coverageEpoch int64
	if err := tx.QueryRowContext(ctx, `
SELECT observed_head,observed_ref,worktree_id,coverage_epoch,phase
FROM checkpoints WHERE id=?`, checkpointID).Scan(
		&observedHead, &observedRef, &checkpointWorktreeID,
		&coverageEpoch, &phase); err != nil {
		return publicationDrainTarget{}, fmt.Errorf("load publication checkpoint: %w", err)
	}
	if phase != state.CheckpointCompleted || observedRef == "" {
		return publicationDrainTarget{}, errors.New("publication checkpoint is not a completed attached snapshot")
	}
	if checkpointWorktreeID != worktreeID || coverageEpoch < minimumCoverageEpoch {
		return publicationDrainTarget{}, fmt.Errorf(
			"publication checkpoint identity changed: worktree=%s coverage_epoch=%d",
			checkpointWorktreeID, coverageEpoch)
	}
	if observedRef != anchor.BranchRef {
		return publicationDrainTarget{}, fmt.Errorf(
			"publication checkpoint branch changed: before=%s checkpoint=%s", anchor.BranchRef, observedRef)
	}
	currentGeneration, err := daemon.LoadBranchGeneration(ctx, db)
	if err != nil {
		return publicationDrainTarget{}, err
	}
	if currentGeneration != anchor.Generation {
		return publicationDrainTarget{}, fmt.Errorf(
			"publication branch generation changed after barrier: before=%d current=%d",
			anchor.Generation, currentGeneration)
	}

	target := publicationDrainTarget{
		BranchRef: anchor.BranchRef, Generation: anchor.Generation,
		EventSeqs: make([]int64, len(anchor.EventSeqs)), MaxSeq: anchor.MaxSeq,
	}
	copy(target.EventSeqs, anchor.EventSeqs)
	seen := make(map[int64]struct{}, len(target.EventSeqs))
	for _, seq := range target.EventSeqs {
		seen[seq] = struct{}{}
	}
	memberRows, err := tx.QueryContext(ctx, `
SELECT e.seq,e.branch_ref,e.branch_generation
FROM checkpoint_events ce
JOIN capture_events e ON e.seq=ce.event_seq
WHERE ce.checkpoint_id=?
ORDER BY ce.ord`, checkpointID)
	if err != nil {
		return publicationDrainTarget{}, fmt.Errorf("load publication checkpoint membership: %w", err)
	}
	for memberRows.Next() {
		var seq int64
		var memberRef string
		var memberGeneration int64
		if err := memberRows.Scan(&seq, &memberRef, &memberGeneration); err != nil {
			memberRows.Close()
			return publicationDrainTarget{}, err
		}
		if memberRef != anchor.BranchRef || memberGeneration != anchor.Generation {
			memberRows.Close()
			return publicationDrainTarget{}, errors.New("publication checkpoint membership changed branch generation")
		}
		if _, exists := seen[seq]; exists {
			continue
		}
		seen[seq] = struct{}{}
		target.EventSeqs = append(target.EventSeqs, seq)
		target.MaxSeq = max(target.MaxSeq, seq)
	}
	if err := memberRows.Err(); err != nil {
		memberRows.Close()
		return publicationDrainTarget{}, fmt.Errorf("iterate publication checkpoint membership: %w", err)
	}
	if err := memberRows.Close(); err != nil {
		return publicationDrainTarget{}, err
	}
	if err := tx.Commit(); err != nil {
		return publicationDrainTarget{}, fmt.Errorf("commit publication target snapshot: %w", err)
	}
	branchAfter, err := git.RunBranchRef(ctx, repoRoot)
	if err != nil {
		return publicationDrainTarget{}, fmt.Errorf("revalidate publication branch: %w", err)
	}
	if branchAfter != anchor.BranchRef {
		return publicationDrainTarget{}, fmt.Errorf(
			"publication branch changed while freezing target: before=%s current=%s", anchor.BranchRef, branchAfter)
	}
	headAfter, err := publicationHead(ctx, repoRoot)
	if err != nil {
		return publicationDrainTarget{}, fmt.Errorf("revalidate publication HEAD: %w", err)
	}
	if headAfter != headBefore {
		return publicationDrainTarget{}, fmt.Errorf(
			"publication HEAD changed while freezing target: before=%s current=%s",
			headBefore, headAfter)
	}
	if headAfter != observedHead {
		owned, proofErr := publicationHeadAdvanceOwnedByTarget(
			ctx, db, observedHead, headAfter, target)
		if proofErr != nil {
			return publicationDrainTarget{}, fmt.Errorf(
				"prove publication HEAD advance: %w", proofErr)
		}
		if !owned {
			return publicationDrainTarget{}, fmt.Errorf(
				"publication checkpoint HEAD changed without a completed ACD publication chain: checkpoint=%s current=%s",
				observedHead, headAfter)
		}
	}
	return target, nil
}

func publicationHead(ctx context.Context, repoRoot string) (string, error) {
	head, err := git.RevParse(ctx, repoRoot, "HEAD")
	if errors.Is(err, git.ErrRefNotFound) {
		return "", nil
	}
	return head, err
}

func publicationHeadAdvanceOwnedByTarget(
	ctx context.Context,
	db *state.DB,
	sourceHead, targetHead string,
	target publicationDrainTarget,
) (bool, error) {
	unusedEvents := make(map[int64]struct{}, len(target.EventSeqs))
	for _, seq := range target.EventSeqs {
		unusedEvents[seq] = struct{}{}
	}
	seenHeads := map[string]struct{}{sourceHead: {}}
	current := sourceHead
	for step := 0; step < len(target.EventSeqs) && current != targetHead; step++ {
		rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT id,target_commit_oid FROM self_publications
WHERE branch_ref=? AND branch_generation=? AND source_head=?
  AND phase='completed'
ORDER BY created_ts,id LIMIT 2`, target.BranchRef, target.Generation, current)
		if err != nil {
			return false, err
		}
		var transitions [][2]string
		for rows.Next() {
			var id, nextHead string
			if err := rows.Scan(&id, &nextHead); err != nil {
				rows.Close()
				return false, err
			}
			transitions = append(transitions, [2]string{id, nextHead})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return false, err
		}
		if err := rows.Close(); err != nil {
			return false, err
		}
		if len(transitions) != 1 {
			return false, nil
		}

		publicationID, nextHead := transitions[0][0], transitions[0][1]
		memberRows, err := db.ReadSQL().QueryContext(ctx, `
SELECT event_seq FROM self_publication_members
WHERE publication_id=? ORDER BY ord`, publicationID)
		if err != nil {
			return false, err
		}
		memberCount := 0
		for memberRows.Next() {
			var seq int64
			if err := memberRows.Scan(&seq); err != nil {
				memberRows.Close()
				return false, err
			}
			if _, ok := unusedEvents[seq]; !ok {
				memberRows.Close()
				return false, nil
			}
			delete(unusedEvents, seq)
			memberCount++
		}
		if err := memberRows.Err(); err != nil {
			memberRows.Close()
			return false, err
		}
		if err := memberRows.Close(); err != nil {
			return false, err
		}
		if memberCount == 0 {
			return false, nil
		}
		if _, duplicate := seenHeads[nextHead]; duplicate {
			return false, nil
		}
		seenHeads[nextHead] = struct{}{}
		current = nextHead
	}
	return current == targetHead, nil
}

func publicationDrainResult(
	checkpointID string,
	target publicationDrainTarget,
	progress publicationDrainProgress,
	drained bool,
	waitingReason string,
) map[string]any {
	return map[string]any{
		"checkpoint_id": checkpointID, "protected": true,
		"publication_drained": drained, "target_event_seq": target.MaxSeq,
		"target_events": len(target.EventSeqs), "published_events": progress.Published,
		"remaining_events": progress.Remaining, "commits_created": progress.Commits,
		"recovered_events": progress.Recovered, "terminal_events": progress.Terminal,
		"waiting_reason": waitingReason,
	}
}

func publicationDrainOperationResult(
	ctx context.Context,
	db *state.DB,
	drain state.PublicationDrain,
) (any, *supervisor.ProtocolError) {
	target := publicationDrainTarget{
		BranchRef: drain.BranchRef, Generation: drain.BranchGeneration,
		EventSeqs: append([]int64(nil), drain.EventSeqs...),
	}
	for _, seq := range target.EventSeqs {
		target.MaxSeq = max(target.MaxSeq, seq)
	}
	progress, err := publicationDrainStatus(ctx, db, target)
	if err != nil {
		return nil, protocolFailure("publication_status_failed", err, true)
	}
	result := publicationDrainResult(
		drain.CheckpointID, target, progress,
		drain.Phase == state.PublicationDrainCompleted, drain.LastError)
	result["drain_id"] = drain.ID
	result["phase"] = drain.Phase
	result["fallback_mode"] = drain.FallbackMode
	result["last_error"] = drain.LastError
	result["semantic_rebuild_attempts"] = drain.SemanticRebuildAttempts
	result["event_fallback_count"] = drain.EventFallbackCount
	result["last_progress_ts"] = drain.LastProgressTS
	result["staged_consumed"] = drain.StagedConsumed
	return result, nil
}

func waitForPublicationDrain(
	ctx context.Context,
	db *state.DB,
	drain state.PublicationDrain,
) (any, *supervisor.ProtocolError) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, protocolFailure("publication_wait_detached", fmt.Errorf(
				"client wait ended while durable publication drain %s continues: %w",
				drain.ID, ctx.Err()), true)
		case <-ticker.C:
			current, err := state.PublicationDrainByID(ctx, db, drain.ID)
			if err != nil {
				return nil, protocolFailure("publication_status_failed", err, true)
			}
			target := publicationDrainTarget{
				BranchRef: current.BranchRef, Generation: current.BranchGeneration,
				EventSeqs: append([]int64(nil), current.EventSeqs...),
			}
			for _, seq := range target.EventSeqs {
				target.MaxSeq = max(target.MaxSeq, seq)
			}
			progress, err := publicationDrainStatus(ctx, db, target)
			if err != nil {
				return nil, protocolFailure("publication_status_failed", err, true)
			}
			switch current.Phase {
			case state.PublicationDrainCompleted:
				result := publicationDrainResult(
					current.CheckpointID, target, progress, true, "")
				result["drain_id"] = current.ID
				result["phase"] = current.Phase
				result["fallback_mode"] = current.FallbackMode
				return result, nil
			case state.PublicationDrainNeedsAction:
				return nil, protocolFailure("publication_needs_action",
					errors.New(current.LastError), false)
			}
		}
	}
}

func publicationDrainStatus(ctx context.Context, db *state.DB, target publicationDrainTarget) (publicationDrainProgress, error) {
	var progress publicationDrainProgress
	commits := map[string]struct{}{}
	const batchSize = 500
	for start := 0; start < len(target.EventSeqs); start += batchSize {
		end := min(start+batchSize, len(target.EventSeqs))
		args := make([]any, 0, end-start+2)
		placeholders := make([]string, 0, end-start)
		for _, seq := range target.EventSeqs[start:end] {
			placeholders = append(placeholders, "?")
			args = append(args, seq)
		}
		args = append(args, target.BranchRef, target.Generation)
		rows, err := db.SQL().QueryContext(ctx, `
SELECT state,COALESCE(commit_oid,'') FROM capture_events
WHERE seq IN (`+strings.Join(placeholders, ",")+`)
  AND branch_ref=? AND branch_generation=?`, args...)
		if err != nil {
			return progress, err
		}
		seen := 0
		for rows.Next() {
			var eventState, commitOID string
			if err := rows.Scan(&eventState, &commitOID); err != nil {
				rows.Close()
				return progress, err
			}
			seen++
			switch eventState {
			case state.EventStatePending:
				progress.Remaining++
			case state.EventStatePublished:
				progress.Published++
				if commitOID != "" {
					commits[commitOID] = struct{}{}
				}
			case state.EventStateRecovered:
				progress.Recovered++
			default:
				progress.Terminal++
			}
		}
		if err := rows.Close(); err != nil {
			return progress, err
		}
		if seen != end-start {
			return progress, errors.New("publication target ownership changed")
		}
	}
	progress.Commits = int64(len(commits))
	return progress, nil
}

func publicationUnsafeReason(
	ctx context.Context,
	worktree git.Worktree,
	allowStaged bool,
) (string, error) {
	branchRef, err := git.RunBranchRef(ctx, worktree.Root)
	if err != nil {
		return "", err
	}
	if branchRef == "" {
		return "publication is paused on detached HEAD; attach a branch and run `acd commit-all --yes` again", nil
	}
	for _, marker := range []string{"rebase-merge", "rebase-apply", "MERGE_HEAD", "CHERRY_PICK_HEAD", "BISECT_LOG"} {
		if _, err := os.Lstat(filepath.Join(worktree.GitDir, marker)); err == nil {
			return "publication is paused during an active Git operation; finish it and run `acd commit-all --yes` again", nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
	}
	body, err := git.Run(ctx, git.RunOpts{Dir: worktree.Root},
		"status", "--porcelain=v1", "-z", "--untracked-files=no")
	if err != nil {
		return "", err
	}
	for _, item := range strings.Split(string(body), "\x00") {
		if len(item) < 2 {
			continue
		}
		xy := item[:2]
		if strings.Contains(xy, "U") || xy == "AA" || xy == "DD" {
			return "publication is paused by unresolved Git conflicts; resolve them and run `acd commit-all --yes` again", nil
		}
		if !allowStaged && xy[0] != ' ' && xy[0] != '?' {
			return "publication is paused because the Git index contains staged changes; unstage them and run `acd commit-all --yes` again", nil
		}
	}
	return "", nil
}

func checkpointBarrierWait(ctx context.Context) time.Duration {
	if requestDeadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(requestDeadline)
		const diagnosticHeadroom = 100 * time.Millisecond
		if remaining > diagnosticHeadroom {
			return remaining - diagnosticHeadroom
		}
		return max(remaining, time.Millisecond)
	}
	return supervisor.CheckpointBarrierTimeout
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
	case "hint", "checkpoint_barrier", "publication_drain_start",
		"publication_drain_status":
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
