package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// newDaemonCmd builds `acd daemon ...` (hidden parent) plus the `run`
// subcommand spawned by `acd start`. Per §7.12 the run subcommand:
//   - Holds <git-dir>/acd/daemon.lock for the entire process lifetime.
//   - Loses the flock => exit 75 (EX_TEMPFAIL).
//   - Translates SIGTERM/SIGINT/SIGUSR1 into the run-loop signals (the
//     internal/daemon package installs its own handlers; we still wire the
//     CLI ctx so a higher-level cancel propagates).
func newDaemonCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "daemon",
		Short:  "Daemon mode (long-running). Not normally invoked manually.",
		Hidden: true,
	}
	run := &cobra.Command{
		Use:   "run",
		Short: "Run the long-lived daemon for a single repo",
		RunE: func(c *cobra.Command, args []string) error {
			repoFlag, _ := c.Flags().GetString("repo")
			gitDirFlag, _ := c.Flags().GetString("git-dir")
			return runDaemon(c.Context(), c.OutOrStdout(), c.ErrOrStderr(), repoFlag, gitDirFlag)
		},
	}
	run.Flags().String("git-dir", "", "Override .git path (rare)")
	cmd.AddCommand(run)
	return cmd
}

func runDaemon(ctx context.Context, out, errOut io.Writer, repoFlag, gitDirFlag string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	repo, err := resolveRepo(repoFlag)
	if err != nil {
		return err
	}
	if !fileExists(repo) {
		return fmt.Errorf("acd daemon run: repo %s does not exist", repo)
	}
	gitDir := gitDirFlag
	if gitDir == "" {
		// Resolve via git so worktrees/submodules work.
		resolved, gerr := git.AbsoluteGitDir(ctx, repo)
		if gerr != nil {
			// Fall back to <repo>/.git when git invocation fails (common
			// in tests with synthetic repos).
			fallback := filepath.Join(repo, ".git")
			if !fileExists(fallback) {
				return fmt.Errorf("acd daemon run: resolve git dir for %s: %w", repo, gerr)
			}
			gitDir = fallback
		} else {
			gitDir = resolved
		}
	}

	// Wire SIGTERM/SIGINT to ctx cancel. The daemon package installs its
	// own handlers but the top-level CLI ctx is the canonical "stop now"
	// trigger for this binary.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		select {
		case <-sigCh:
			cancel()
		case <-cctx.Done():
		}
	}()
	defer signal.Stop(sigCh)

	// Open the per-repo state.db. The run loop expects the caller to own
	// the DB lifetime, so we keep it open for the duration here.
	dbPath := state.DBPathFromGitDir(gitDir)
	db, err := state.Open(cctx, dbPath)
	if err != nil {
		return fmt.Errorf("acd daemon run: open state.db: %w", err)
	}
	defer func() { _ = db.Close() }()

	fmt.Fprintf(out, "acd daemon run: repo=%s git_dir=%s pid=%d\n", repo, gitDir, os.Getpid())

	opts := buildDaemonRunOptions(repo, gitDir, db, errOut)
	if err := daemon.Run(cctx, opts); err != nil {
		if errors.Is(err, daemon.ErrDaemonLockHeld) {
			fmt.Fprintf(errOut, "acd daemon run: another daemon is already running for %s\n", repo)
			os.Exit(daemon.ExitTempFail)
		}
		return fmt.Errorf("acd daemon run: %w", err)
	}
	fmt.Fprintln(out, "acd daemon run: stopped")
	return nil
}

// buildDaemonRunOptions assembles the daemon.Options struct for the
// `acd daemon run` invocation. Centralised so tests can pin the wiring
// invariants without spinning up the full daemon loop.
//
// Behaviour:
//   - FsnotifyEnabled comes from ACD_FSNOTIFY_ENABLED (any value other
//     than "" / "0" / "false" enables it).
//   - CentralStatsDBPath + RepoHash are resolved best-effort. A
//     resolution failure is logged to errOut but does not abort the
//     run loop — the daemon's rollup-push step gates on non-empty
//     values, so missing wiring degrades to "no stats" rather than a
//     fatal error. Both fields MUST be wired here; the previous
//     implementation left them blank, which silently disabled
//     `acd stats` for every install.
func buildDaemonRunOptions(repo, gitDir string, db *state.DB, errOut io.Writer) daemon.Options {
	fsEnabled := false
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("ACD_FSNOTIFY_ENABLED"))); v != "" && v != "0" && v != "false" {
		fsEnabled = true
	}

	var (
		centralStatsPath string
		repoHash         string
	)
	if roots, rErr := paths.Resolve(); rErr != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "acd daemon run: resolve paths for stats: %v (stats disabled)\n", rErr)
		}
	} else {
		centralStatsPath = roots.StatsDBPath()
	}
	if h, hErr := paths.RepoHash(repo); hErr != nil {
		if errOut != nil {
			fmt.Fprintf(errOut, "acd daemon run: compute repo hash for stats: %v (stats disabled)\n", hErr)
		}
	} else {
		repoHash = h
	}

	return daemon.Options{
		RepoPath:           repo,
		GitDir:             gitDir,
		DB:                 db,
		FsnotifyEnabled:    fsEnabled,
		CentralStatsDBPath: centralStatsPath,
		RepoHash:           repoHash,
	}
}
