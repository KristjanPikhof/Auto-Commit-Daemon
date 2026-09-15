package cli

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	acdlogger "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/logger"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func buildDaemonRunOptions(repo, gitDir string, db *state.DB, _ io.Writer) (daemon.Options, io.Closer, error) {
	return buildDaemonRunOptionsWithID(repo, gitDir, db, "")
}

func buildDaemonRunOptionsWithID(repo, gitDir string, db *state.DB, repositoryID string) (daemon.Options, io.Closer, error) {
	fsEnabled := false
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("ACD_FSNOTIFY_ENABLED"))); v != "" && v != "0" && v != "false" {
		fsEnabled = true
	}

	roots, err := paths.Resolve()
	if err != nil {
		return daemon.Options{}, nil, fmt.Errorf("acd daemon run: resolve paths: %w", err)
	}
	repoHash := strings.TrimSpace(repositoryID)
	if repoHash == "" {
		repoHash, err = paths.RepoHash(repo)
		if err != nil {
			return daemon.Options{}, nil, fmt.Errorf("acd daemon run: compute repo hash: %w", err)
		}
	}
	runLogger, logCloser, err := acdlogger.New(acdlogger.Options{Path: roots.RepoLogPath(repoHash)})
	if err != nil {
		return daemon.Options{}, nil, fmt.Errorf("acd daemon run: open daemon log: %w", err)
	}

	return daemon.Options{
		RepoPath:           repo,
		GitDir:             gitDir,
		DB:                 db,
		Logger:             runLogger,
		FsnotifyEnabled:    fsEnabled,
		CentralStatsDBPath: roots.StatsDBPath(),
		RepoHash:           repoHash,
	}, logCloser, nil
}
