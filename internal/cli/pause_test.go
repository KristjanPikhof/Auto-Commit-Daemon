package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

func TestPause_WritesMarkerWithReason(t *testing.T) {
	ctx := context.Background()
	repo := makeStartRepo(t)

	var out bytes.Buffer
	if err := runPause(ctx, &out, repo, "operator maintenance", "", false, true); err != nil {
		t.Fatalf("runPause: %v", err)
	}

	markerPath := pauseMarkerPath(mustResolveGitDir(t, ctx, repo))
	info, err := os.Stat(markerPath)
	if err != nil {
		t.Fatalf("stat marker: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("marker mode=%#o want 0600", got)
	}

	marker := readPauseMarkerFile(t, markerPath)
	if marker.Reason != "operator maintenance" {
		t.Fatalf("reason=%q", marker.Reason)
	}
	if marker.SetBy == "" {
		t.Fatalf("set_by must be populated")
	}
	if _, err := time.Parse(time.RFC3339, marker.SetAt); err != nil {
		t.Fatalf("set_at is not RFC3339: %v", err)
	}
	if marker.ExpiresAt != nil {
		t.Fatalf("expires_at=%v want nil", *marker.ExpiresAt)
	}

	var got pauseResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal output: %v\n%s", err, out.String())
	}
	if !got.OK || got.Marker.Reason != marker.Reason || got.MarkerPath != markerPath {
		t.Fatalf("unexpected pause result: %+v", got)
	}
}

func TestPause_RefusesOverwriteWithoutYes(t *testing.T) {
	ctx := context.Background()
	repo := makeStartRepo(t)
	markerPath := pauseMarkerPath(mustResolveGitDir(t, ctx, repo))

	var out bytes.Buffer
	if err := runPause(ctx, &out, repo, "first", "", false, true); err != nil {
		t.Fatalf("first runPause: %v", err)
	}
	beforeInfo, err := os.Stat(markerPath)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}
	beforeBody, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatalf("read before: %v", err)
	}

	out.Reset()
	if err := runPause(ctx, &out, repo, "second", "", false, true); err == nil {
		t.Fatalf("second runPause succeeded without --yes")
	}

	afterInfo, err := os.Stat(markerPath)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	afterBody, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatalf("read after: %v", err)
	}
	if !afterInfo.ModTime().Equal(beforeInfo.ModTime()) {
		t.Fatalf("mtime changed: before=%s after=%s", beforeInfo.ModTime(), afterInfo.ModTime())
	}
	if !bytes.Equal(afterBody, beforeBody) {
		t.Fatalf("marker body changed:\nbefore=%s\nafter=%s", beforeBody, afterBody)
	}
}

func TestPause_TTLEmitsExpiresAt(t *testing.T) {
	ctx := context.Background()
	repo := makeStartRepo(t)

	var out bytes.Buffer
	if err := runPause(ctx, &out, repo, "short break", "1h", false, true); err != nil {
		t.Fatalf("runPause: %v", err)
	}

	marker := readPauseMarkerFile(t, pauseMarkerPath(mustResolveGitDir(t, ctx, repo)))
	if marker.ExpiresAt == nil {
		t.Fatalf("expires_at is nil")
	}
	setAt, err := time.Parse(time.RFC3339, marker.SetAt)
	if err != nil {
		t.Fatalf("parse set_at: %v", err)
	}
	expiresAt, err := time.Parse(time.RFC3339, *marker.ExpiresAt)
	if err != nil {
		t.Fatalf("parse expires_at: %v", err)
	}
	if got := expiresAt.Sub(setAt); got != time.Hour {
		t.Fatalf("expires_at-set_at=%s want 1h", got)
	}
}

func TestResume_RemovesMarkerAndReportsPriorReason(t *testing.T) {
	ctx := context.Background()
	repo := makeStartRepo(t)
	markerPath := pauseMarkerPath(mustResolveGitDir(t, ctx, repo))

	var out bytes.Buffer
	if err := runPause(ctx, &out, repo, "deploy window", "", false, true); err != nil {
		t.Fatalf("runPause: %v", err)
	}
	out.Reset()
	if err := runResume(ctx, &out, repo, true, true); err != nil {
		t.Fatalf("runResume: %v", err)
	}

	if _, err := os.Stat(markerPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("marker still exists or stat failed: %v", err)
	}
	var got resumeResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal output: %v\n%s", err, out.String())
	}
	if !got.OK || !got.Removed || got.Status != "resumed" || got.Marker.Reason != "deploy window" {
		t.Fatalf("unexpected resume result: %+v", got)
	}
}

func TestResume_RefusesWithoutYes(t *testing.T) {
	ctx := context.Background()
	repo := makeStartRepo(t)
	markerPath := pauseMarkerPath(mustResolveGitDir(t, ctx, repo))

	var out bytes.Buffer
	if err := runPause(ctx, &out, repo, "manual", "", false, true); err != nil {
		t.Fatalf("runPause: %v", err)
	}
	out.Reset()
	if err := runResume(ctx, &out, repo, false, true); err == nil {
		t.Fatalf("runResume succeeded without --yes")
	}
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("marker should remain after refused resume: %v", err)
	}
}

func TestResume_NoMarkerIsNoop(t *testing.T) {
	ctx := context.Background()
	repo := makeStartRepo(t)

	var out bytes.Buffer
	if err := runResume(ctx, &out, repo, false, true); err != nil {
		t.Fatalf("runResume: %v", err)
	}
	var got resumeResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal output: %v\n%s", err, out.String())
	}
	if got.OK || got.Removed || got.Status != "not-paused" {
		t.Fatalf("unexpected no-op resume result: %+v", got)
	}
}

func readPauseMarkerFile(t *testing.T, markerPath string) PauseMarker {
	t.Helper()
	body, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	var marker PauseMarker
	if err := json.Unmarshal(body, &marker); err != nil {
		t.Fatalf("unmarshal marker: %v\n%s", err, body)
	}
	return marker
}

func mustResolveGitDir(t *testing.T, ctx context.Context, repo string) string {
	t.Helper()
	gitDir, err := resolveGitDir(ctx, repo)
	if err != nil {
		t.Fatalf("resolveGitDir: %v", err)
	}
	return gitDir
}
