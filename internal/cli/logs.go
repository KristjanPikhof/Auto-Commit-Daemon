package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

const (
	defaultLogLines          = 100
	defaultLogFollowInterval = 250 * time.Millisecond
)

var logFollowPollInterval = defaultLogFollowInterval

func newLogsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Print the current repo daemon log tail",
		Long: `Print the current repo daemon log tail as raw JSONL.

The default repo is the current working directory. By default acd logs prints
the last 100 raw log lines and exits. Use --lines to choose the initial tail
length, or --follow to keep streaming appended lines until interrupted. For
bundled diagnostics and sanitized tails, use acd doctor or acd doctor --bundle.`,
		Example: `  acd logs
  acd logs --lines 200
  acd logs --follow
  acd logs --repo /path/to/repo --lines 50 --follow`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			lines, _ := cmd.Flags().GetInt("lines")
			follow, _ := cmd.Flags().GetBool("follow")
			ctx := cmd.Context()
			stop := func() {}
			if follow {
				ctx, stop = signal.NotifyContext(ctx, os.Interrupt)
			}
			defer stop()
			return runLogs(ctx, cmd.OutOrStdout(), repo, lines, follow)
		},
	}
	cmd.Flags().Int("lines", defaultLogLines, "Number of log lines to print before exiting or following")
	cmd.Flags().BoolP("follow", "f", false, "Stream appended log lines until interrupted")
	return cmd
}

func runLogs(ctx context.Context, out io.Writer, repo string, lines int, follow bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if lines < 0 {
		return fmt.Errorf("acd logs: --lines must be non-negative")
	}

	logPath, abs, err := resolveRepoLogPath(repo)
	if err != nil {
		return err
	}
	if _, err := os.Stat(logPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("acd logs: daemon log missing for repo %s at %s (try `acd start --repo %s` or run `acd doctor`)", abs, logPath, abs)
		}
		return fmt.Errorf("acd logs: stat daemon log %s: %w", logPath, err)
	}

	if lines > 0 {
		tail, offset, err := readLastLogLines(logPath, lines)
		if err != nil {
			return fmt.Errorf("acd logs: read daemon log %s: %w", logPath, err)
		}
		for _, line := range tail {
			if _, err := fmt.Fprintln(out, line); err != nil {
				return fmt.Errorf("acd logs: write output: %w", err)
			}
		}
		if follow {
			return followLog(ctx, out, logPath, offset, logFollowPollInterval)
		}
		return nil
	}

	if !follow {
		return nil
	}

	info, err := os.Stat(logPath)
	if err != nil {
		return fmt.Errorf("acd logs: stat daemon log %s: %w", logPath, err)
	}
	return followLog(ctx, out, logPath, info.Size(), logFollowPollInterval)
}

func resolveRepoLogPath(repo string) (logPath, absRepo string, err error) {
	abs, err := resolveRepo(repo)
	if err != nil {
		return "", "", err
	}
	roots, err := paths.Resolve()
	if err != nil {
		return "", "", fmt.Errorf("acd logs: resolve paths: %w", err)
	}
	reg, err := central.Load(roots)
	if err != nil {
		return "", "", fmt.Errorf("acd logs: load registry: %w", err)
	}
	rec, ok := findRepo(reg, abs)
	if !ok {
		return "", "", fmt.Errorf("acd logs: repo %s is not registered (try `acd start --repo %s`)", abs, abs)
	}
	return roots.RepoLogPath(rec.RepoHash), abs, nil
}

func readLastLogLines(path string, n int) ([]string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	lines := make([]string, 0, n)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > n {
			copy(lines, lines[1:])
			lines = lines[:n]
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, err
	}
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, 0, err
	}
	return lines, offset, nil
}

func followLog(ctx context.Context, out io.Writer, path string, offset int64, interval time.Duration) error {
	if interval <= 0 {
		interval = defaultLogFollowInterval
	}
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}

		nextOffset, err := copyLogAppend(out, path, offset)
		switch {
		case err == nil:
			offset = nextOffset
		case errors.Is(err, os.ErrNotExist):
			offset = 0
		default:
			return fmt.Errorf("acd logs: follow daemon log %s: %w", path, err)
		}

		timer.Reset(interval)
	}
}

func copyLogAppend(out io.Writer, path string, offset int64) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return offset, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return offset, err
	}
	if info.Size() < offset {
		offset = 0
	}
	if info.Size() == offset {
		return offset, nil
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return offset, err
	}
	n, err := io.Copy(out, f)
	if err != nil {
		return offset, err
	}
	return offset + n, nil
}
