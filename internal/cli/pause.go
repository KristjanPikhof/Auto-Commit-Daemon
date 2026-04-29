package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"time"

	"github.com/spf13/cobra"

	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
)

// PauseMarker is the durable gitDir/acd/paused file format.
type PauseMarker = pausepkg.Marker

type pauseResult struct {
	OK         bool        `json:"ok"`
	Repo       string      `json:"repo"`
	MarkerPath string      `json:"marker_path"`
	Overwrote  bool        `json:"overwrote,omitempty"`
	Marker     PauseMarker `json:"marker"`
}

type resumeResult struct {
	OK                bool        `json:"ok"`
	Status            string      `json:"status"`
	Repo              string      `json:"repo"`
	MarkerPath        string      `json:"marker_path"`
	Removed           bool        `json:"removed"`
	ExistedForSeconds int64       `json:"existed_for_seconds,omitempty"`
	Marker            PauseMarker `json:"marker,omitempty"`
}

func newPauseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause",
		Short: "Pause daemon capture and replay for a repo",
		RunE: func(c *cobra.Command, args []string) error {
			repoFlag, _ := c.Flags().GetString("repo")
			reason, _ := c.Flags().GetString("reason")
			ttl, _ := c.Flags().GetString("ttl")
			yes, _ := c.Flags().GetBool("yes")
			jsonOut, _ := c.Flags().GetBool("json")
			return runPause(c.Context(), c.OutOrStdout(), repoFlag, reason, ttl, yes, jsonOut)
		},
	}
	cmd.Flags().String("reason", "manual", "Pause reason")
	cmd.Flags().String("ttl", "", "Optional pause TTL, for example 1h")
	cmd.Flags().Bool("yes", false, "Overwrite an existing pause marker")
	return cmd
}

func newResumeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume",
		Short: "Resume daemon capture and replay for a repo",
		RunE: func(c *cobra.Command, args []string) error {
			repoFlag, _ := c.Flags().GetString("repo")
			yes, _ := c.Flags().GetBool("yes")
			jsonOut, _ := c.Flags().GetBool("json")
			return runResume(c.Context(), c.OutOrStdout(), repoFlag, yes, jsonOut)
		},
	}
	cmd.Flags().Bool("yes", false, "Remove the pause marker")
	return cmd
}

func runPause(ctx context.Context, out io.Writer, repoFlag, reason, ttlFlag string, yes, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	repo, err := resolveRepo(repoFlag)
	if err != nil {
		return err
	}
	gitDir, err := resolveGitDir(ctx, repo)
	if err != nil {
		return fmt.Errorf("acd pause: resolve git dir: %w", err)
	}
	if reason == "" {
		reason = "manual"
	}

	now := time.Now().UTC()
	marker := PauseMarker{
		Reason: reason,
		SetAt:  now.Format(time.RFC3339),
		SetBy:  defaultPauseSetBy(),
	}
	if ttlFlag != "" {
		ttl, err := time.ParseDuration(ttlFlag)
		if err != nil {
			return fmt.Errorf("acd pause: parse --ttl: %w", err)
		}
		if ttl <= 0 {
			return fmt.Errorf("acd pause: --ttl must be positive")
		}
		expiresAt := now.Add(ttl).Format(time.RFC3339)
		marker.ExpiresAt = &expiresAt
	}

	markerPath := pauseMarkerPath(gitDir)
	if err := writePauseMarker(markerPath, marker, yes); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("acd pause: pause marker already exists at %s; pass --yes to overwrite", markerPath)
		}
		return fmt.Errorf("acd pause: write marker: %w", err)
	}

	res := pauseResult{
		OK:         true,
		Repo:       repo,
		MarkerPath: markerPath,
		Overwrote:  yes,
		Marker:     marker,
	}
	return renderPause(out, res, jsonOut)
}

func runResume(ctx context.Context, out io.Writer, repoFlag string, yes, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	repo, err := resolveRepo(repoFlag)
	if err != nil {
		return err
	}
	gitDir, err := resolveGitDir(ctx, repo)
	if err != nil {
		return fmt.Errorf("acd resume: resolve git dir: %w", err)
	}

	marker, ok, err := ReadMarker(gitDir)
	if err != nil {
		return fmt.Errorf("acd resume: read marker: %w", err)
	}
	markerPath := pauseMarkerPath(gitDir)
	if !ok {
		return renderResume(out, resumeResult{
			OK:         false,
			Status:     "not-paused",
			Repo:       repo,
			MarkerPath: markerPath,
			Removed:    false,
		}, jsonOut)
	}
	if !yes {
		return fmt.Errorf("acd resume: refusing to remove pause marker without --yes")
	}
	if err := os.Remove(markerPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("acd resume: remove marker: %w", err)
	}

	res := resumeResult{
		OK:                true,
		Status:            "resumed",
		Repo:              repo,
		MarkerPath:        markerPath,
		Removed:           true,
		ExistedForSeconds: markerAgeSeconds(marker, time.Now().UTC()),
		Marker:            marker,
	}
	return renderResume(out, res, jsonOut)
}

// ReadMarker reads gitDir/acd/paused. It returns ok=false when the marker is
// absent or malformed, matching the daemon's best-effort pause-marker handling.
func ReadMarker(gitDir string) (PauseMarker, bool, error) {
	marker, ok, err := pausepkg.Read(gitDir)
	if errors.Is(err, pausepkg.ErrMalformed) {
		log.Printf("acd pause: ignoring malformed pause marker %s: %v", pauseMarkerPath(gitDir), err)
		return PauseMarker{}, false, nil
	}
	if err != nil {
		return PauseMarker{}, false, err
	}
	return marker, ok, nil
}

func pauseMarkerPath(gitDir string) string {
	return pausepkg.Path(gitDir)
}

func writePauseMarker(path string, marker PauseMarker, overwrite bool) error {
	return pausepkg.Write(path, marker, overwrite)
}

func renderPause(out io.Writer, res pauseResult, jsonOut bool) error {
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	fmt.Fprintf(out, "Paused %s\n", res.Repo)
	fmt.Fprintf(out, "Reason: %s\n", res.Marker.Reason)
	fmt.Fprintf(out, "Marker: %s\n", res.MarkerPath)
	if res.Marker.ExpiresAt != nil {
		fmt.Fprintf(out, "Expires at: %s\n", *res.Marker.ExpiresAt)
	}
	return nil
}

func renderResume(out io.Writer, res resumeResult, jsonOut bool) error {
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	if !res.OK {
		fmt.Fprintf(out, "Repo is not paused: %s\n", res.Repo)
		return nil
	}
	fmt.Fprintf(out, "Resumed %s\n", res.Repo)
	fmt.Fprintf(out, "Prior reason: %s\n", res.Marker.Reason)
	if res.ExistedForSeconds > 0 {
		fmt.Fprintf(out, "Paused for: %s\n", formatDurationCompact(time.Duration(res.ExistedForSeconds)*time.Second))
	}
	return nil
}

func defaultPauseSetBy() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	name := os.Getenv("USER")
	if name == "" {
		name = os.Getenv("USERNAME")
	}
	if current, err := user.Current(); err == nil && current.Username != "" {
		name = current.Username
	}
	if name == "" {
		name = "unknown-user"
	}
	return host + ":" + name
}

func markerAgeSeconds(marker PauseMarker, now time.Time) int64 {
	setAt, err := time.Parse(time.RFC3339, marker.SetAt)
	if err != nil {
		return 0
	}
	if now.Before(setAt) {
		return 0
	}
	return int64(now.Sub(setAt).Seconds())
}
