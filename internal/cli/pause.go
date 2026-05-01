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
	Status     string      `json:"status"`
	Repo       string      `json:"repo"`
	MarkerPath string      `json:"marker_path"`
	Overwrote  bool        `json:"overwrote"`
	Marker     PauseMarker `json:"marker"`
}

// resumeResult is the JSON envelope returned by `acd resume`. Status takes
// the values "resumed", "not-paused", "requires-yes",
// "backpressure-cleared", or "no-backpressure" so machine readers can
// switch on a single field across all outcomes.
type resumeResult struct {
	OK                  bool        `json:"ok"`
	Status              string      `json:"status"`
	Repo                string      `json:"repo"`
	MarkerPath          string      `json:"marker_path"`
	Removed             bool        `json:"removed"`
	ExistedForSeconds   int64       `json:"existed_for_seconds,omitempty"`
	Marker              PauseMarker `json:"marker,omitempty"`
	BackpressureCleared bool        `json:"backpressure_cleared,omitempty"`
	BackpressureWasSet  bool        `json:"backpressure_was_set,omitempty"`
	BackpressureSetAt   string      `json:"backpressure_set_at,omitempty"`
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
			acceptOverflow, _ := c.Flags().GetBool("accept-overflow")
			return runResume(c.Context(), c.OutOrStdout(), repoFlag, yes, jsonOut, acceptOverflow)
		},
	}
	cmd.Flags().Bool("yes", false, "Remove the pause marker")
	cmd.Flags().Bool("accept-overflow", false,
		"Clear the durable capture-backpressure pause and accept any silent loss; independent from --yes (manual pause marker)")
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
	overwrote, err := writePauseMarker(markerPath, marker, yes)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("acd pause: pause marker already exists at %s; pass --yes to overwrite", markerPath)
		}
		return fmt.Errorf("acd pause: write marker: %w", err)
	}

	res := pauseResult{
		OK:         true,
		Status:     "paused",
		Repo:       repo,
		MarkerPath: markerPath,
		Overwrote:  overwrote,
		Marker:     marker,
	}
	return renderPause(out, res, jsonOut)
}

func runResume(ctx context.Context, out io.Writer, repoFlag string, yes, jsonOut, acceptOverflow bool) error {
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

	// --accept-overflow path: independent from the manual pause marker. When
	// supplied (alone or with --yes) it always inspects + clears the durable
	// capture-backpressure meta key. We resolve and clear BEFORE handling the
	// manual marker so the operator gets a clean signal even on a repo whose
	// only "pause" is the implicit backpressure gate.
	var bpCleared bool
	var bpWasSet bool
	var bpSetAt string
	if acceptOverflow {
		set, stamp, clearErr := clearBackpressureFromCLI(ctx, repo)
		if clearErr != nil {
			return fmt.Errorf("acd resume: clear backpressure: %w", clearErr)
		}
		bpWasSet = set
		bpSetAt = stamp
		bpCleared = set
	}

	marker, ok, err := ReadMarker(gitDir)
	if err != nil {
		return fmt.Errorf("acd resume: read marker: %w", err)
	}
	markerPath := pauseMarkerPath(gitDir)

	// No manual marker: short-circuit. If --accept-overflow was supplied,
	// surface the backpressure outcome under a dedicated status. Otherwise
	// emit the existing "not-paused" envelope.
	if !ok {
		if acceptOverflow {
			status := "no-backpressure"
			if bpCleared {
				status = "backpressure-cleared"
			}
			return renderResume(out, resumeResult{
				OK:                  bpCleared || !bpWasSet,
				Status:              status,
				Repo:                repo,
				MarkerPath:          markerPath,
				Removed:             false,
				BackpressureCleared: bpCleared,
				BackpressureWasSet:  bpWasSet,
				BackpressureSetAt:   bpSetAt,
			}, jsonOut)
		}
		return renderResume(out, resumeResult{
			OK:         false,
			Status:     "not-paused",
			Repo:       repo,
			MarkerPath: markerPath,
			Removed:    false,
		}, jsonOut)
	}
	if !yes {
		// Marker present but --yes not passed: emit a structured envelope so
		// JSON consumers can branch on Status="requires-yes" without parsing
		// stderr text. Always render BEFORE returning an error so cobra still
		// surfaces the non-zero exit code while stdout carries valid JSON.
		res := resumeResult{
			OK:                  false,
			Status:              "requires-yes",
			Repo:                repo,
			MarkerPath:          markerPath,
			Removed:             false,
			ExistedForSeconds:   markerAgeSeconds(marker, time.Now().UTC()),
			Marker:              marker,
			BackpressureCleared: bpCleared,
			BackpressureWasSet:  bpWasSet,
			BackpressureSetAt:   bpSetAt,
		}
		if renderErr := renderResume(out, res, jsonOut); renderErr != nil {
			return renderErr
		}
		return fmt.Errorf("acd resume: refusing to remove pause marker without --yes")
	}
	if err := os.Remove(markerPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("acd resume: remove marker: %w", err)
	}

	res := resumeResult{
		OK:                  true,
		Status:              "resumed",
		Repo:                repo,
		MarkerPath:          markerPath,
		Removed:             true,
		ExistedForSeconds:   markerAgeSeconds(marker, time.Now().UTC()),
		Marker:              marker,
		BackpressureCleared: bpCleared,
		BackpressureWasSet:  bpWasSet,
		BackpressureSetAt:   bpSetAt,
	}
	return renderResume(out, res, jsonOut)
}

// clearBackpressureFromCLI loads the per-repo state.db, inspects the
// durable capture.backpressure_paused_at meta key, deletes it if present,
// and emits a trace breadcrumb so operators can correlate the explicit
// override with the daemon's later behavior. Returns (was_set, prior_set_at,
// err). The function is independent from the manual pause marker.
func clearBackpressureFromCLI(ctx context.Context, repo string) (bool, string, error) {
	roots, err := paths.Resolve()
	if err != nil {
		return false, "", fmt.Errorf("resolve paths: %w", err)
	}
	reg, err := central.Load(roots)
	if err != nil {
		return false, "", fmt.Errorf("load registry: %w", err)
	}
	rec, ok := findRepo(reg, repo)
	if !ok {
		// No registered repo means no state.db to mutate. Treat as
		// "nothing to clear" rather than an error so the operator can
		// still run `acd resume --accept-overflow` defensively.
		return false, "", nil
	}
	if !fileExists(rec.StateDB) {
		return false, "", nil
	}
	db, err := state.Open(ctx, rec.StateDB)
	if err != nil {
		return false, "", fmt.Errorf("open state.db: %w", err)
	}
	defer func() { _ = db.Close() }()

	prior, present, err := state.MetaGet(ctx, db, daemon.MetaKeyCaptureBackpressurePausedAt)
	if err != nil {
		return false, "", fmt.Errorf("read backpressure meta: %w", err)
	}
	if !present {
		return false, "", nil
	}
	if _, err := state.MetaDelete(ctx, db, daemon.MetaKeyCaptureBackpressurePausedAt); err != nil {
		return true, prior, fmt.Errorf("delete backpressure meta: %w", err)
	}
	// Stamp a breadcrumb so operators can correlate the override.
	_ = state.MetaSet(ctx, db, "capture.backpressure_overridden_at", time.Now().UTC().Format(time.RFC3339))
	return true, prior, nil
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

func writePauseMarker(path string, marker PauseMarker, overwrite bool) (bool, error) {
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
	switch res.Status {
	case "requires-yes":
		// Human-mode: nothing on stdout (the cobra-returned error covers it).
		// JSON-mode is handled above.
		return nil
	case "not-paused":
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

// defaultPauseSetBy composes the host:user identifier stamped into the
// pause marker. user.Current is most reliable across shells (sudo, login
// shells, sandboxed CI runners), so we prefer it over $USER / $USERNAME.
// Both host and user are sanitized so a hostile or accidentally-malformed
// env var cannot inject control characters into the on-disk marker JSON.
func defaultPauseSetBy() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	var name string
	if current, err := user.Current(); err == nil && current.Username != "" {
		name = current.Username
	}
	if name == "" {
		name = os.Getenv("USER")
	}
	if name == "" {
		name = os.Getenv("USERNAME")
	}
	if name == "" {
		name = "unknown-user"
	}
	return sanitizePauseField(host) + ":" + sanitizePauseField(name)
}

// sanitizePauseField replaces ASCII control characters (bytes < 0x20 and
// 0x7F DEL) with '_' so the host:user identifier in the marker JSON never
// contains literal newlines, tabs, or escapes that would break grep-friendly
// audit logs. Non-ASCII bytes are passed through.
func sanitizePauseField(s string) string {
	if s == "" {
		return s
	}
	b := []byte(s)
	for i, c := range b {
		if c < 0x20 || c == 0x7F {
			b[i] = '_'
		}
	}
	return string(b)
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
