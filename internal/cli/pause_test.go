package cli

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
)

func TestFinishStatusExpiredLegacyPauseCannotStopProtection(t *testing.T) {
	rec, db, _ := finishStatusFixture(t)
	finishStatusWrite(t, rec.Path, "work.txt", "observed work")
	finishStatusCheckpoint(t, rec, db)
	expired := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	markerPath := pauseMarkerPath(mustResolveGitDir(t, t.Context(), rec.Path))
	if _, err := pausepkg.Write(markerPath, pausepkg.Marker{Reason: "old publication pause", SetAt: time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339), SetBy: "test", ExpiresAt: &expired}, true); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatal(err)
	}
	report := finishStatusRead(t, rec, db)
	if !report.Protected || report.Paused || report.PublicationOutcome.PendingChanges != 1 {
		t.Fatalf("old marker stopped protection: %+v", report)
	}
	finishStatusAssertViews(t, rec, db, report)
	after, err := os.ReadFile(markerPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("read-only status modified legacy marker: %v", err)
	}
}

func mustResolveGitDir(t *testing.T, ctx context.Context, repo string) string {
	t.Helper()
	gitDir, err := resolveGitDir(ctx, repo)
	if err != nil {
		t.Fatalf("resolveGitDir: %v", err)
	}
	return gitDir
}
