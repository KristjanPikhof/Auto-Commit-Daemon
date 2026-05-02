package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestClassifyTokenTransition_LegacyTokenForcesDiverged(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	got, err := ClassifyTokenTransition(ctx, f.dir,
		"rev:"+head,
		branchTokenRev(head, "refs/heads/main"),
	)
	if err != nil {
		t.Fatalf("ClassifyTokenTransition: %v", err)
	}
	if got != TokenTransitionDiverged {
		t.Fatalf("got %v want %v", got, TokenTransitionDiverged)
	}
}

func TestClassifyTokenTransition_LegacyMissingForcesDiverged(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	got, err := ClassifyTokenTransition(ctx, f.dir,
		BranchTokenMissing,
		branchTokenMissing("refs/heads/main"),
	)
	if err != nil {
		t.Fatalf("ClassifyTokenTransition: %v", err)
	}
	if got != TokenTransitionDiverged {
		t.Fatalf("got %v want %v", got, TokenTransitionDiverged)
	}
}

func TestBranchToken_RewindSetsPausedUntil(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()
	base, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	child := commitSingleFile(t, ctx, f.dir, base, "child.txt", "child\n", "child")
	now := time.Date(2026, 4, 29, 12, 0, 30, 0, time.UTC)

	paused, until, err := maybeSetRewindGrace(ctx, f.dir, f.db,
		branchTokenRev(child, "refs/heads/main"),
		branchTokenRev(base, "refs/heads/main"),
		now,
	)
	if err != nil {
		t.Fatalf("maybeSetRewindGrace: %v", err)
	}
	if !paused {
		t.Fatal("expected rewind to set replay pause")
	}
	want := now.Add(defaultRewindGrace).Format(time.RFC3339)
	if until != want {
		t.Fatalf("until=%q want %q", until, want)
	}
	got, ok, err := state.MetaGet(ctx, f.db, MetaKeyReplayPausedUntil)
	if err != nil {
		t.Fatalf("MetaGet: %v", err)
	}
	if !ok || got != want {
		t.Fatalf("replay.paused_until=(%q,%v) want (%q,true)", got, ok, want)
	}
}

func TestBranchToken_DivergedNotRewindDoesNotPause(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()
	base, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	child := commitSingleFile(t, ctx, f.dir, base, "child.txt", "child\n", "child")
	sibling := commitSingleFile(t, ctx, f.dir, "", "sibling.txt", "sibling\n", "sibling root")

	paused, until, err := maybeSetRewindGrace(ctx, f.dir, f.db,
		branchTokenRev(child, "refs/heads/main"),
		branchTokenRev(sibling, "refs/heads/main"),
		time.Now(),
	)
	if err != nil {
		t.Fatalf("maybeSetRewindGrace: %v", err)
	}
	if paused || until != "" {
		t.Fatalf("paused=%v until=%q, want no pause", paused, until)
	}
	if got, ok, err := state.MetaGet(ctx, f.db, MetaKeyReplayPausedUntil); err != nil {
		t.Fatalf("MetaGet: %v", err)
	} else if ok {
		t.Fatalf("unexpected replay.paused_until=%q", got)
	}
}

func TestBranchToken_FastForwardDoesNotBumpOrPause(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()
	base, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	child := commitSingleFile(t, ctx, f.dir, base, "child.txt", "child\n", "child")

	transition, err := ClassifyTokenTransition(ctx, f.dir,
		branchTokenRev(base, "refs/heads/main"),
		branchTokenRev(child, "refs/heads/main"),
	)
	if err != nil {
		t.Fatalf("ClassifyTokenTransition: %v", err)
	}
	if transition != TokenTransitionFastForward {
		t.Fatalf("transition=%v want %v", transition, TokenTransitionFastForward)
	}
	paused, until, err := maybeSetRewindGrace(ctx, f.dir, f.db,
		branchTokenRev(base, "refs/heads/main"),
		branchTokenRev(child, "refs/heads/main"),
		time.Now(),
	)
	if err != nil {
		t.Fatalf("maybeSetRewindGrace: %v", err)
	}
	if paused || until != "" {
		t.Fatalf("paused=%v until=%q, want no pause", paused, until)
	}
}

func TestBranchToken_GraceZeroSkipsPause(t *testing.T) {
	t.Setenv(EnvRewindGraceSeconds, "0")
	f := newDaemonFixture(t)
	ctx := context.Background()
	base, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	child := commitSingleFile(t, ctx, f.dir, base, "child.txt", "child\n", "child")

	paused, until, err := maybeSetRewindGrace(ctx, f.dir, f.db,
		branchTokenRev(child, "refs/heads/main"),
		branchTokenRev(base, "refs/heads/main"),
		time.Now(),
	)
	if err != nil {
		t.Fatalf("maybeSetRewindGrace: %v", err)
	}
	if paused || until != "" {
		t.Fatalf("paused=%v until=%q, want grace disabled", paused, until)
	}
	if got, ok, err := state.MetaGet(ctx, f.db, MetaKeyReplayPausedUntil); err != nil {
		t.Fatalf("MetaGet: %v", err)
	} else if ok {
		t.Fatalf("unexpected replay.paused_until=%q", got)
	}
}

// TestClassifyTokenTransition_AsymmetricRefDiverged checks that transitions
// where exactly one token carries a branch ref are always classified as
// Diverged, regardless of direction. This covers:
//   - prev has no ref, new has ref (legacy token upgrade)
//   - prev has ref, new has no ref (legacy token downgrade / bare rev token)
func TestClassifyTokenTransition_AsymmetricRefDiverged(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}

	t.Run("prev_no_ref_new_has_ref", func(t *testing.T) {
		// Legacy bare rev token (no ref) → named-ref token: must be Diverged.
		got, err := ClassifyTokenTransition(ctx, f.dir,
			"rev:"+head,
			branchTokenRev(head, "refs/heads/main"),
		)
		if err != nil {
			t.Fatalf("ClassifyTokenTransition: %v", err)
		}
		if got != TokenTransitionDiverged {
			t.Fatalf("got %v want %v", got, TokenTransitionDiverged)
		}
	})

	t.Run("prev_has_ref_new_no_ref", func(t *testing.T) {
		// Named-ref token → legacy bare rev token (no ref): must be Diverged.
		got, err := ClassifyTokenTransition(ctx, f.dir,
			branchTokenRev(head, "refs/heads/main"),
			"rev:"+head,
		)
		if err != nil {
			t.Fatalf("ClassifyTokenTransition: %v", err)
		}
		if got != TokenTransitionDiverged {
			t.Fatalf("got %v want %v", got, TokenTransitionDiverged)
		}
	})
}

// TestRewindGrace_ClampedAtStartup proves the startup clamp normalizes a
// far-future replay-pause marker. The marker is persisted as an absolute
// RFC3339 timestamp; if the host clock was running fast when
// maybeSetRewindGrace stamped it (or jumped backward afterwards and got
// corrected at boot) the persisted value can sit hours past the configured
// grace window. The clamp caps anything more than 2*grace into the future
// at exactly grace from now.
func TestRewindGrace_ClampedAtStartup(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	// Persist a paused_until marker ~1 hour in the future — far above the
	// default 60-second grace, so 2*grace is also exceeded.
	farFuture := now.Add(1 * time.Hour).Format(time.RFC3339)
	if err := state.MetaSet(ctx, f.db, MetaKeyReplayPausedUntil, farFuture); err != nil {
		t.Fatalf("MetaSet: %v", err)
	}

	clamped, original, replacement, err := ClampRewindGraceAtStartup(ctx, f.db, now)
	if err != nil {
		t.Fatalf("ClampRewindGraceAtStartup: %v", err)
	}
	if !clamped {
		t.Fatal("expected clamp to fire on far-future marker")
	}
	if original != farFuture {
		t.Fatalf("original=%q want %q", original, farFuture)
	}
	wantReplacement := now.Add(defaultRewindGrace).Format(time.RFC3339)
	if replacement != wantReplacement {
		t.Fatalf("replacement=%q want %q", replacement, wantReplacement)
	}

	got, ok, err := state.MetaGet(ctx, f.db, MetaKeyReplayPausedUntil)
	if err != nil {
		t.Fatalf("MetaGet: %v", err)
	}
	if !ok || got != wantReplacement {
		t.Fatalf("persisted=(%q,%v) want (%q,true)", got, ok, wantReplacement)
	}

	// A second invocation should be a no-op now that the marker is in range.
	clamped2, _, _, err := ClampRewindGraceAtStartup(ctx, f.db, now)
	if err != nil {
		t.Fatalf("ClampRewindGraceAtStartup second pass: %v", err)
	}
	if clamped2 {
		t.Fatal("clamp re-fired on already-clamped marker")
	}
}

// TestRewindGrace_StartupPreservesInRangeMarker confirms the clamp leaves
// well-formed markers untouched.
func TestRewindGrace_StartupPreservesInRangeMarker(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	// 30s into the future — well within 2*grace.
	inRange := now.Add(30 * time.Second).Format(time.RFC3339)
	if err := state.MetaSet(ctx, f.db, MetaKeyReplayPausedUntil, inRange); err != nil {
		t.Fatalf("MetaSet: %v", err)
	}

	clamped, _, _, err := ClampRewindGraceAtStartup(ctx, f.db, now)
	if err != nil {
		t.Fatalf("ClampRewindGraceAtStartup: %v", err)
	}
	if clamped {
		t.Fatal("clamp fired on in-range marker")
	}
	got, _, _ := state.MetaGet(ctx, f.db, MetaKeyReplayPausedUntil)
	if got != inRange {
		t.Fatalf("marker mutated: got %q want %q", got, inRange)
	}
}

func commitSingleFile(t *testing.T, ctx context.Context, repoDir, parent, path, content, message string) string {
	t.Helper()
	blob, err := git.HashObjectStdin(ctx, repoDir, []byte(content))
	if err != nil {
		t.Fatalf("hash object: %v", err)
	}
	tree, err := git.Mktree(ctx, repoDir, []git.MktreeEntry{
		{Mode: git.RegularFileMode, Type: "blob", OID: blob, Path: path},
	})
	if err != nil {
		t.Fatalf("mktree: %v", err)
	}
	var parents []string
	if parent != "" {
		parents = append(parents, parent)
	}
	commit, err := git.CommitTree(ctx, repoDir, tree, message, parents...)
	if err != nil {
		t.Fatalf("commit-tree: %v", err)
	}
	return commit
}
