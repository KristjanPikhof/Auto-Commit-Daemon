package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	osuser "os/user"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
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
	if err := runResume(ctx, &out, repo, true, true, false); err != nil {
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
	if err := runResume(ctx, &out, repo, false, true, false); err == nil {
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
	if err := runResume(ctx, &out, repo, false, true, false); err != nil {
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

func TestResume_RequiresYes_JSONEnvelope(t *testing.T) {
	ctx := context.Background()
	repo := makeStartRepo(t)
	markerPath := pauseMarkerPath(mustResolveGitDir(t, ctx, repo))

	var pauseOut bytes.Buffer
	if err := runPause(ctx, &pauseOut, repo, "deploy", "", false, true); err != nil {
		t.Fatalf("runPause: %v", err)
	}

	var resumeOut bytes.Buffer
	err := runResume(ctx, &resumeOut, repo, false, true, false)
	if err == nil {
		t.Fatalf("runResume succeeded without --yes; want error to surface non-zero exit")
	}
	if !strings.Contains(err.Error(), "without --yes") {
		t.Fatalf("err=%v want refuse-without-yes phrase", err)
	}
	if _, err := os.Stat(markerPath); err != nil {
		t.Fatalf("marker should still exist: %v", err)
	}

	var got resumeResult
	if uerr := json.Unmarshal(resumeOut.Bytes(), &got); uerr != nil {
		t.Fatalf("stdout is not valid JSON: %v\n%s", uerr, resumeOut.String())
	}
	if got.OK || got.Removed {
		t.Fatalf("requires-yes envelope leaks OK/Removed: %+v", got)
	}
	if got.Status != "requires-yes" {
		t.Fatalf("status=%q want requires-yes", got.Status)
	}
	if got.MarkerPath != markerPath {
		t.Fatalf("marker_path=%q want %q", got.MarkerPath, markerPath)
	}
	if got.Marker.Reason != "deploy" {
		t.Fatalf("envelope marker reason=%q want deploy", got.Marker.Reason)
	}
}

func TestResume_NotPaused_JSONEnvelope(t *testing.T) {
	ctx := context.Background()
	repo := makeStartRepo(t)

	var out bytes.Buffer
	if err := runResume(ctx, &out, repo, false, true, false); err != nil {
		t.Fatalf("runResume: %v", err)
	}
	var got resumeResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("stdout is not valid JSON: %v\n%s", err, out.String())
	}
	if got.Status != "not-paused" || got.OK || got.Removed {
		t.Fatalf("unexpected not-paused envelope: %+v", got)
	}
}

func TestPause_OverwroteFlagFact(t *testing.T) {
	ctx := context.Background()
	repo := makeStartRepo(t)

	// --yes against absent marker must report overwrote=false (the flag
	// permits overwrite but no marker existed to replace).
	var out bytes.Buffer
	if err := runPause(ctx, &out, repo, "manual", "", true, true); err != nil {
		t.Fatalf("runPause: %v", err)
	}
	var first pauseResult
	if err := json.Unmarshal(out.Bytes(), &first); err != nil {
		t.Fatalf("unmarshal first: %v\n%s", err, out.String())
	}
	if first.Overwrote {
		t.Fatalf("first pause reports overwrote=true with no prior marker: %+v", first)
	}

	// Second pause with --yes against the now-present marker reports true.
	out.Reset()
	if err := runPause(ctx, &out, repo, "manual", "", true, true); err != nil {
		t.Fatalf("second runPause: %v", err)
	}
	var second pauseResult
	if err := json.Unmarshal(out.Bytes(), &second); err != nil {
		t.Fatalf("unmarshal second: %v\n%s", err, out.String())
	}
	if !second.Overwrote {
		t.Fatalf("second pause reports overwrote=false: %+v", second)
	}
}

// TestResume_AcceptOverflow_ClearsBackpressureMeta: the new
// --accept-overflow flag must be independent from the manual pause marker.
// When invoked alone it clears MetaKeyCaptureBackpressurePausedAt and
// reports status="backpressure-cleared".
func TestResume_AcceptOverflow_ClearsBackpressureMeta(t *testing.T) {
	ctx := context.Background()
	roots := withIsolatedHome(t)
	repo, _, db := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db.Path(), "codex")

	// Stamp the durable backpressure meta key.
	stamp := time.Now().UTC().Format(time.RFC3339)
	if err := state.MetaSet(ctx, db, daemon.MetaKeyCaptureBackpressurePausedAt, stamp); err != nil {
		t.Fatalf("seed backpressure meta: %v", err)
	}

	var out bytes.Buffer
	// --yes=false, --accept-overflow=true: clears the gate independently.
	if err := runResume(ctx, &out, repo, false, true, true); err != nil {
		t.Fatalf("runResume --accept-overflow: %v", err)
	}
	var got resumeResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if got.Status != "backpressure-cleared" {
		t.Fatalf("status=%q want backpressure-cleared; envelope=%+v", got.Status, got)
	}
	if !got.OK || !got.BackpressureCleared || !got.BackpressureWasSet {
		t.Fatalf("flags wrong: %+v", got)
	}
	if got.BackpressureSetAt != stamp {
		t.Fatalf("set_at=%q want %q", got.BackpressureSetAt, stamp)
	}
	if _, ok, err := state.MetaGet(ctx, db, daemon.MetaKeyCaptureBackpressurePausedAt); err != nil {
		t.Fatalf("MetaGet: %v", err)
	} else if ok {
		t.Fatalf("backpressure meta key should be deleted after --accept-overflow")
	}
	// Operator-acknowledgement breadcrumb should be stamped.
	if _, ok, err := state.MetaGet(ctx, db, "capture.backpressure_overridden_at"); err != nil {
		t.Fatalf("MetaGet override: %v", err)
	} else if !ok {
		t.Fatalf("expected capture.backpressure_overridden_at to be stamped")
	}
}

// TestResume_AcceptOverflow_NoBackpressure: when no durable backpressure
// is active, --accept-overflow must be a friendly no-op with
// status="no-backpressure" rather than an error.
func TestResume_AcceptOverflow_NoBackpressure(t *testing.T) {
	ctx := context.Background()
	roots := withIsolatedHome(t)
	repo, _, db := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db.Path(), "codex")

	var out bytes.Buffer
	if err := runResume(ctx, &out, repo, false, true, true); err != nil {
		t.Fatalf("runResume --accept-overflow no-op: %v", err)
	}
	var got resumeResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if got.Status != "no-backpressure" {
		t.Fatalf("status=%q want no-backpressure; envelope=%+v", got.Status, got)
	}
	if got.BackpressureCleared || got.BackpressureWasSet {
		t.Fatalf("no-backpressure must not flag cleared/was_set: %+v", got)
	}
}

func TestPauseStatus_ExpiredManualMarker_Visible(t *testing.T) {
	ctx := context.Background()
	roots := withIsolatedHome(t)
	repo, dbPath, _ := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")

	// Write a manual marker with expires_at in the past.
	expired := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	gitDir := mustResolveGitDir(t, ctx, repo)
	markerPath := pauseMarkerPath(gitDir)
	if _, err := pausepkg.Write(markerPath, pausepkg.Marker{
		Reason:    "stuck-marker",
		SetAt:     time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339),
		SetBy:     "test",
		ExpiresAt: &expired,
	}, true); err != nil {
		t.Fatalf("pausepkg.Write: %v", err)
	}

	var out bytes.Buffer
	if err := runStatus(ctx, &out, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !rep.Paused || rep.Pause == nil {
		t.Fatalf("expected paused=true with pause object: %+v", rep)
	}
	if rep.Pause.Source != "manual_expired" {
		t.Fatalf("pause.source=%q want manual_expired", rep.Pause.Source)
	}
	if rep.Pause.ExpiresAt == "" {
		t.Fatalf("pause.expires_at empty for expired marker: %+v", rep.Pause)
	}
}

func TestDefaultPauseSetBy_PrecedenceUserCurrentFirst(t *testing.T) {
	// user.Current() returns the actual login user under `go test`. We pin
	// behavior by ensuring USER env doesn't overwrite the current.Username.
	cur, err := osuser.Current()
	if err != nil {
		t.Skipf("user.Current unavailable: %v", err)
	}
	if cur.Username == "" {
		t.Skip("user.Current returned empty username; skipping precedence assertion")
	}
	t.Setenv("USER", "envuser-foo")
	t.Setenv("USERNAME", "envuser-bar")

	got := defaultPauseSetBy()
	parts := strings.SplitN(got, ":", 2)
	if len(parts) != 2 {
		t.Fatalf("defaultPauseSetBy=%q does not contain host:user", got)
	}
	wantUser := sanitizePauseField(cur.Username)
	if parts[1] != wantUser {
		t.Fatalf("user component=%q want user.Current=%q (USER/USERNAME must NOT win)", parts[1], wantUser)
	}
}

func TestSanitizePauseField_StripsControlChars(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"plain", "plain"},
		{"line1\nline2", "line1_line2"},
		{"tab\there", "tab_here"},
		{"with\x7fdel", "with_del"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := sanitizePauseField(tc.in); got != tc.want {
			t.Fatalf("sanitizePauseField(%q)=%q want %q", tc.in, got, tc.want)
		}
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
