package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"

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
	integration := &cobra.Command{Use: "integration", Hidden: true}
	stdinExtract := newHookStdinExtractCmd()
	stdinExtract.Use = "stdin-extract"
	cursorExtract := newHookCursorExtractCmd()
	cursorExtract.Use = "cursor-extract"
	event := withInvocationCapabilities(newInternalIntegrationEventCmd(), commandCapabilities{Repository: true, Quiet: true})
	integration.AddCommand(stdinExtract, cursorExtract, event)
	root.AddCommand(supervisorCmd, worker, hint, integration)
	return root
}

func newInternalHintCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "hint", Hidden: true, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		repo, _ := cmd.Flags().GetString("repo")
		return sendInternalHint(cmd.Context(), repo)
	}}
	return cmd
}

func sendInternalHint(ctx context.Context, repo string) error {
	decision := evaluateIntegrationRepo(ctx, repo)
	switch decision.State {
	case integrationRepoInactiveNoGit,
		integrationRepoInactiveUnregistered,
		integrationRepoInactiveDisabled,
		integrationRepoInactiveUnactivated:
		return nil
	case integrationRepoIndeterminate:
		return decision.Err
	case integrationRepoActive:
		return sendInternalHintToRepo(ctx, decision.Record, decision.Roots)
	default:
		return fmt.Errorf("acd internal hint: unknown integration repository state %q", decision.State)
	}
}

func sendInternalHintToRepo(ctx context.Context, record central.RepoRecord, roots paths.Roots) error {
	if strings.TrimSpace(record.RepositoryID) == "" || strings.TrimSpace(record.WorktreeID) == "" {
		return fmt.Errorf("acd internal hint: repository setup is still in progress; retry after `acd setup` completes")
	}
	triggerUpgrade, err := ensureMutationSupervisorMode(ctx, roots, true)
	if err != nil {
		return err
	}
	request := supervisor.Request{Version: supervisor.ProtocolVersion, ID: fmt.Sprintf("hint-%d", time.Now().UnixNano()), Method: "hint", RepositoryID: record.RepositoryID, WorktreeID: record.WorktreeID, DeadlineMS: time.Now().Add(supervisor.CheckpointBarrierTimeout).UnixMilli()}
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

func runSupervisor(ctx context.Context, _ io.Writer, _ io.Writer) error {
	roots, err := paths.Resolve()
	if err != nil {
		return fmt.Errorf("acd supervisor: resolve paths\nDetails: %w", err)
	}
	binary, err := os.Executable()
	if err != nil {
		return fmt.Errorf("acd supervisor: resolve executable\nDetails: %w", err)
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
		return fmt.Errorf("acd worker: acquire repository ownership\nDetails: %w", err)
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
	var runJobs sync.WaitGroup
	defer func() {
		cancel()
		runJobs.Wait()
		for _, runtime := range runtimes {
			runtime.finishMu.Lock()
			runtime.finishMu.Unlock()
			runtime.finishJobs.Wait()
		}
		for i := len(closers) - 1; i >= 0; i-- {
			_ = closers[i].Close()
		}
	}()
	for _, record := range records {
		wt, resolveErr := git.ResolveWorktree(cctx, record.Path)
		if resolveErr != nil {
			return resolveErr
		}
		db, openErr := state.OpenRuntime(cctx, record.StateDB)
		if openErr != nil {
			return fmt.Errorf("acd worker: open %s\nDetails: %w", record.Path, openErr)
		}
		closers = append(closers, db)
		restorePending, pendingErr := state.RestoreRepairPending(cctx, db)
		if pendingErr != nil {
			return fmt.Errorf("acd worker: inspect interrupted restore for %s\nDetails: %w", record.Path, pendingErr)
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
		publicationHeld := func() bool {
			return workerPublicationHeld(canonicalHold, restoreHeld)
		}
		wakeCh := make(chan struct{}, 1)
		wakeMu.Lock()
		wakeTargets[record.WorktreeID] = wakeCh
		wakeMu.Unlock()
		opts.WakeCh = wakeCh
		protectionGate := &sync.Mutex{}
		opts.ProtectionGate = protectionGate
		activateWorkerFinish(cctx, errOut, wt.Root, repositoryID, record.WorktreeID, db, publicationHeld())
		opts.FinishRecoveryDone = true
		runtimes[record.WorktreeID] = &workerRuntime{
			record: record, worktree: wt, db: db, gate: gate,
			roots: roots, workerCtx: cctx, protectionGate: protectionGate,
			policy: restorepkg.ProtectionPolicy{
				Sensitive: opts.SensitiveMatcher, SafeIgnore: opts.SafeIgnoreMatcher,
				MaxFileBytes: opts.MaxFileBytes,
			},
			restoreHeld: restoreHeld,
		}
		runJobs.Add(1)
		go func(path string, runOpts daemon.Options) {
			defer runJobs.Done()
			if runErr := daemon.Run(cctx, runOpts); runErr != nil && !errors.Is(runErr, context.Canceled) {
				errCh <- fmt.Errorf("%s\nDetails: %w", path, runErr)
				return
			}
			errCh <- nil
		}(wt.Root, opts)
	}
	wakeWorker := func(worktreeID string) {
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
	}
	activityHints := newWorkerActivityHintCoalescer(
		workerActivityHintQuietPeriod, workerActivityHintMaxTail, wakeWorker)
	workerHandler := &repositoryWorkerHandler{
		repositoryID: repositoryID, runtimes: runtimes, wake: wakeWorker,
		activityHints: activityHints,
	}
	workerServerErr := make(chan error, 1)
	go func() {
		workerServerErr <- supervisor.ServeWorker(cctx,
			supervisor.WorkerSocketPath(roots, repositoryID), workerHandler)
	}()
	defer func() {
		activityHints.Close()
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
		return fmt.Errorf("acd worker: control server\nDetails: %w", serverErr)
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
		return fmt.Errorf("acd worker: read supervisor environment\nDetails: %w", err)
	}
	if response.Error != nil {
		return fmt.Errorf("acd worker: read supervisor environment: %s", response.Error.Message)
	}
	body, err := json.Marshal(response.Data)
	if err != nil {
		return fmt.Errorf("acd worker: encode supervisor environment\nDetails: %w", err)
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
			return fmt.Errorf("acd worker: apply supervisor environment %s\nDetails: %w", name, err)
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
			return fmt.Errorf("acd worker: set %s\nDetails: %w", name, err)
		}
	}
	return nil
}

func applyWorkerRestartOptions(roots paths.Roots, record central.RepoRecord, wt git.Worktree, opts *daemon.Options) error {
	document, err := config.NewStore(roots).Load()
	if err != nil {
		return fmt.Errorf("acd worker: load repository configuration\nDetails: %w", err)
	}
	values, err := config.ResolveRestartEnvironment(document, record.WorktreeID, os.LookupEnv)
	if err != nil {
		return fmt.Errorf("acd worker: resolve repository configuration\nDetails: %w", err)
	}
	parseInt := func(name string) (int64, error) {
		value, err := strconv.ParseInt(strings.TrimSpace(values[name]), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%s=%q\nDetails: %w", name, values[name], err)
		}
		return value, nil
	}
	maxBytes, err := parseInt("ACD_MAX_FILE_BYTES")
	if err != nil {
		return err
	}
	opts.MaxFileBytes = maxBytes
	opts.FsnotifyEnabled = truthyConfig(values["ACD_FSNOTIFY_ENABLED"])
	opts.SensitiveMatcher = state.NewSensitiveMatcherFromValue(values["ACD_SENSITIVE_GLOBS"])
	opts.SafeIgnoreMatcher = state.NewSafeIgnoreMatcherFromValues(values["ACD_SAFE_IGNORE"], values["ACD_SAFE_IGNORE_EXTRA"])
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
	roots            paths.Roots
	workerCtx        context.Context
	protectionGate   *sync.Mutex
	finishMu         sync.Mutex
	finishJobs       sync.WaitGroup
	activeFinishJobs atomic.Int64
	historyJobs      map[string]bool
	record           central.RepoRecord
	worktree         git.Worktree
	db               *state.DB
	gate             *sync.RWMutex
	policy           restorepkg.ProtectionPolicy
	restoreHeld      *atomic.Bool
}

type repositoryWorkerHandler struct {
	repositoryID  string
	runtimes      map[string]*workerRuntime
	wake          func(string)
	activityHints *workerActivityHintCoalescer
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
	if request.Method == "history_start" || request.Method == "history_status" || request.Method == "history_save" {
		return h.handleFinishHistory(ctx, runtime, request)
	}
	if request.Method == "finish_start" || request.Method == "finish_status" {
		return h.handleFinish(ctx, runtime, request)
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
		h.wakeActivityHint(request.WorktreeID)
		return map[string]bool{"accepted": true}, nil
	case "checkpoint_barrier":
		if workerPublicationHeld(runtime.roots.SetupPublicationHoldPath(), nil) {
			// A start may have passed its hold check before setup wrote the marker.
			// Keep new starts out until this final protection barrier completes.
			runtime.finishMu.Lock()
			defer runtime.finishMu.Unlock()
			if runtime.activeFinishJobs.Load() > 0 {
				return nil, protocolFailure("finish_busy", errors.New("checkpoint barrier: an accepted finish or history job is still active; wait for it to complete"), true)
			}
		}
		runtime.gate.Lock()
		acceptedEpoch, beginErr := daemon.BeginProtectionObservation(ctx, runtime.db)
		if beginErr == nil {
			h.wake(request.WorktreeID)
		}
		runtime.gate.Unlock()
		if beginErr != nil {
			return nil, protocolFailure("observation_failed", beginErr, true)
		}
		ctx, cancel := context.WithTimeout(ctx, checkpointBarrierWait(ctx))
		defer cancel()
		readFailure := func(err error) *supervisor.ProtocolError {
			if ctx.Err() != nil {
				return protocolFailure("checkpoint_timeout", fmt.Errorf("checkpoint barrier accepted_epoch=%d: %w", acceptedEpoch, ctx.Err()), true)
			}
			return protocolFailure("checkpoint_status_failed", err, true)
		}
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil, protocolFailure("checkpoint_timeout", fmt.Errorf("checkpoint barrier accepted_epoch=%d: %w", acceptedEpoch, ctx.Err()), true)
			case <-ticker.C:
				covered, _, err := state.MetaGet(ctx, runtime.db, daemon.MetaKeyProtectionCoveredEpoch)
				if err != nil {
					return nil, readFailure(err)
				}
				epoch, _ := strconv.ParseInt(covered, 10, 64)
				complete, _, err := state.MetaGet(ctx, runtime.db, daemon.MetaKeyProtectionComplete)
				if err != nil {
					return nil, readFailure(err)
				}
				if epoch >= acceptedEpoch && complete == "true" {
					id, _, err := state.MetaGet(ctx, runtime.db, daemon.MetaKeyProtectionCheckpointID)
					if err != nil {
						return nil, readFailure(err)
					}
					return map[string]any{"checkpoint_id": id, "protected": true}, nil
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

func (h *repositoryWorkerHandler) wakeActivityHint(worktreeID string) {
	if h.activityHints != nil {
		h.activityHints.Hint(worktreeID)
		return
	}
	h.wake(worktreeID)
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

// Plain checkpoint barriers also serve setup, upgrades and readiness probes.
// Only user/harness requests should renew the compact dashboard activity window.
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
	case "history_start", "history_status", "history_save", "finish_start", "finish_status", "hint", "checkpoint_barrier",
		"history", "restore_plan", "restore_apply", "repair":
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
