package git

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRevParseReturnsErrRefNotFoundForMissingRef(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	_, err := RevParse(ctx, dir, "refs/heads/does-not-exist")
	if !errors.Is(err, ErrRefNotFound) {
		t.Fatalf("expected ErrRefNotFound, got %v", err)
	}
}

func TestRevParseResolvesExistingRef(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	blob, err := HashObjectStdin(ctx, dir, []byte("x"))
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	tree, err := Mktree(ctx, dir, []MktreeEntry{
		{Mode: "100644", Type: "blob", OID: blob, Path: "x"},
	})
	if err != nil {
		t.Fatalf("mktree: %v", err)
	}
	commit, err := CommitTree(ctx, dir, tree, "x")
	if err != nil {
		t.Fatalf("commit-tree: %v", err)
	}
	if err := UpdateRef(ctx, dir, "refs/heads/main", commit, ""); err != nil {
		t.Fatalf("update-ref: %v", err)
	}
	got, err := RevParse(ctx, dir, "refs/heads/main")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if got != commit {
		t.Fatalf("rev-parse mismatch: %s vs %s", got, commit)
	}
}

func TestUpdateRefCompareAndSwapRejectsStaleOld(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	blob, _ := HashObjectStdin(ctx, dir, []byte("y"))
	tree, _ := Mktree(ctx, dir, []MktreeEntry{
		{Mode: "100644", Type: "blob", OID: blob, Path: "y"},
	})
	c1, err := CommitTree(ctx, dir, tree, "y1")
	if err != nil {
		t.Fatalf("c1: %v", err)
	}
	if err := UpdateRef(ctx, dir, "refs/heads/main", c1, ""); err != nil {
		t.Fatalf("set c1: %v", err)
	}
	c2, err := CommitTree(ctx, dir, tree, "y2", c1)
	if err != nil {
		t.Fatalf("c2: %v", err)
	}
	// Bogus expected-old triggers a CAS failure.
	bogus := "0000000000000000000000000000000000000000"
	err = UpdateRef(ctx, dir, "refs/heads/main", c2, bogus)
	if err == nil {
		t.Fatalf("expected CAS failure with stale old oid")
	}
	// With the correct expected-old, the swap succeeds.
	if err := UpdateRef(ctx, dir, "refs/heads/main", c2, c1); err != nil {
		t.Fatalf("CAS with correct old: %v", err)
	}
}

func TestEnsureRecoveryRefCreatesAndReusesExactCommit(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	commit := commitFile(t, ctx, dir, "recovery.txt", "saved", "recovery")
	const ref = "refs/acd/recovery/main/g1/1-2-deadbeef"

	created, err := EnsureRecoveryRef(ctx, dir, ref, commit)
	if err != nil {
		t.Fatalf("EnsureRecoveryRef create: %v", err)
	}
	if !created.Created || created.Reused || created.Ref != ref || created.CommitOID != commit {
		t.Fatalf("create result=%+v", created)
	}
	reused, err := EnsureRecoveryRef(ctx, dir, ref, commit)
	if err != nil {
		t.Fatalf("EnsureRecoveryRef reuse: %v", err)
	}
	if reused.Created || !reused.Reused || reused.CommitOID != commit {
		t.Fatalf("reuse result=%+v", reused)
	}
	got, err := RevParse(ctx, dir, ref)
	if err != nil || got != commit {
		t.Fatalf("recovery ref=(%q,%v) want (%q,nil)", got, err, commit)
	}
}

func TestEnsureRecoveryRefRejectsCollisionWithoutOverwrite(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	first := commitFile(t, ctx, dir, "first.txt", "first", "first")
	second := commitFile(t, ctx, dir, "second.txt", "second", "second", first)
	const ref = "refs/acd/recovery/main/g1/3-4-cafebabe"
	if _, err := EnsureRecoveryRef(ctx, dir, ref, first); err != nil {
		t.Fatalf("seed recovery ref: %v", err)
	}
	if _, err := EnsureRecoveryRef(ctx, dir, ref, second); !errors.Is(err, ErrRecoveryRefCollision) {
		t.Fatalf("collision err=%v want ErrRecoveryRefCollision", err)
	}
	got, err := RevParse(ctx, dir, ref)
	if err != nil || got != first {
		t.Fatalf("collision overwrote ref: got=(%q,%v) want (%q,nil)", got, err, first)
	}
}

func TestEnsureRecoveryRefValidatesNamespaceAndCommitType(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	commit := commitFile(t, ctx, dir, "commit.txt", "commit", "commit")
	if _, err := EnsureRecoveryRef(ctx, dir, "refs/heads/not-hidden", commit); err == nil {
		t.Fatal("EnsureRecoveryRef accepted a user branch ref")
	}
	blob, err := HashObjectStdin(ctx, dir, []byte("blob"))
	if err != nil {
		t.Fatalf("hash blob: %v", err)
	}
	if _, err := EnsureRecoveryRef(ctx, dir, "refs/acd/recovery/blob", blob); err == nil {
		t.Fatal("EnsureRecoveryRef accepted a blob target")
	}
}

func TestWithLockedRecoveryRefBlocksConcurrentMutation(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	first := commitFile(t, ctx, dir, "first.txt", "first", "first")
	second := commitFile(t, ctx, dir, "second.txt", "second", "second", first)
	const ref = "refs/acd/recovery/prune-race"
	if _, err := EnsureRecoveryRef(ctx, dir, ref, first); err != nil {
		t.Fatalf("seed recovery ref: %v", err)
	}

	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- WithLockedRecoveryRef(ctx, dir, ref, first, func() error {
			close(locked)
			<-release
			return nil
		})
	}()
	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery ref lock was not acquired")
	}

	mutationCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err := UpdateRef(mutationCtx, dir, ref, second, first)
	cancel()
	if err == nil {
		t.Fatal("concurrent recovery ref mutation succeeded while proof lock was held")
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WithLockedRecoveryRef: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recovery ref lock was not released")
	}

	if err := UpdateRef(ctx, dir, ref, second, first); err != nil {
		t.Fatalf("mutation after lock release: %v", err)
	}
}

func TestWithLockedExpectedRefBlocksConcurrentBranchMutation(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	first := commitFile(t, ctx, dir, "first.txt", "first", "first")
	second := commitFile(t, ctx, dir, "second.txt", "second", "second", first)
	const ref = "refs/heads/main"
	if err := UpdateRef(ctx, dir, ref, first, ""); err != nil {
		t.Fatalf("seed branch ref: %v", err)
	}

	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- WithLockedExpectedRef(ctx, dir, ref, first, func() error {
			close(locked)
			<-release
			return nil
		})
	}()
	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		t.Fatal("branch ref lock was not acquired")
	}

	mutationCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err := UpdateRef(mutationCtx, dir, ref, second, first)
	cancel()
	if err == nil {
		t.Fatal("concurrent branch mutation succeeded while lock was held")
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WithLockedExpectedRef: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("branch ref lock was not released")
	}

	if err := UpdateRef(ctx, dir, ref, second, first); err != nil {
		t.Fatalf("mutation after lock release: %v", err)
	}
}

func TestWithLockedRecoveryAndBranchRefCancelsCallbackWork(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	first := commitFile(t, ctx, dir, "first.txt", "first", "first")
	second := commitFile(t, ctx, dir, "second.txt", "second", "second", first)
	const (
		recoveryRef = "refs/acd/recovery/test-timeout"
		branchRef   = "refs/heads/main"
	)
	if err := UpdateRef(ctx, dir, recoveryRef, first, ""); err != nil {
		t.Fatalf("seed recovery ref: %v", err)
	}
	if err := UpdateRef(ctx, dir, branchRef, first, ""); err != nil {
		t.Fatalf("seed branch ref: %v", err)
	}

	lockCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	var mutationErr error
	err := WithLockedRecoveryRefAndExpectedRef(
		lockCtx, dir, recoveryRef, first, branchRef, first,
		func(callbackCtx context.Context) error {
			<-callbackCtx.Done()
			mutationErr = UpdateRef(
				callbackCtx, dir, branchRef, second, first)
			return callbackCtx.Err()
		})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock timeout err=%v want deadline exceeded", err)
	}
	if mutationErr == nil {
		t.Fatal("callback mutation succeeded after lock context expired")
	}
	got, err := RevParse(ctx, dir, branchRef)
	if err != nil || got != first {
		t.Fatalf("branch after timeout=%q err=%v want %s", got, err, first)
	}
}

func TestWithLockedRecoveryAndBranchRefAcceptsCompletedCallbackAfterCancellation(t *testing.T) {
	dir := initRepo(t)
	ctx, cancel := context.WithCancel(context.Background())
	first := commitFile(t, context.Background(), dir,
		"first.txt", "first", "first")
	const (
		recoveryRef = "refs/acd/recovery/test-post-callback-cancel"
		branchRef   = "refs/heads/main"
	)
	if err := UpdateRef(context.Background(), dir, recoveryRef, first, ""); err != nil {
		t.Fatalf("seed recovery ref: %v", err)
	}
	if err := UpdateRef(context.Background(), dir, branchRef, first, ""); err != nil {
		t.Fatalf("seed branch ref: %v", err)
	}

	callbackCompleted := false
	err := WithLockedRecoveryRefAndExpectedRef(
		ctx, dir, recoveryRef, first, branchRef, first,
		func(callbackCtx context.Context) error {
			callbackCompleted = true
			cancel()
			<-callbackCtx.Done()
			return nil
		})
	if err != nil {
		t.Fatalf("completed callback reported as failed: %v", err)
	}
	if !callbackCompleted {
		t.Fatal("callback did not complete")
	}
	got, err := RevParse(context.Background(), dir, branchRef)
	if err != nil || got != first {
		t.Fatalf("branch after callback=%q err=%v want %s", got, err, first)
	}
}

func TestWithLockedRecoveryAndBranchRefKeepsLocksUntilCallbackReturnsAfterCancellation(t *testing.T) {
	dir := initRepo(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := commitFile(t, context.Background(), dir,
		"first.txt", "first", "first")
	second := commitFile(t, context.Background(), dir,
		"second.txt", "second", "second", first)
	const (
		recoveryRef = "refs/acd/recovery/test-cancelled-commit"
		branchRef   = "refs/heads/main"
	)
	if err := UpdateRef(context.Background(), dir, recoveryRef, first, ""); err != nil {
		t.Fatalf("seed recovery ref: %v", err)
	}
	if err := UpdateRef(context.Background(), dir, branchRef, first, ""); err != nil {
		t.Fatalf("seed branch ref: %v", err)
	}

	commitBoundary := make(chan struct{})
	callbackCancelled := make(chan struct{})
	releaseCommit := make(chan struct{})
	lockDone := make(chan error, 1)
	go func() {
		lockDone <- WithLockedRecoveryRefAndExpectedRef(
			ctx, dir, recoveryRef, first, branchRef, first,
			func(callbackCtx context.Context) error {
				// Model a database commit that has crossed its commit boundary:
				// cancellation is visible, but the call cannot return until the
				// commit finishes.
				close(commitBoundary)
				<-callbackCtx.Done()
				close(callbackCancelled)
				<-releaseCommit
				return nil
			})
	}()
	select {
	case <-commitBoundary:
	case <-time.After(5 * time.Second):
		t.Fatal("callback did not reach its commit boundary")
	}
	cancel()
	select {
	case <-callbackCancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("callback did not observe caller cancellation")
	}

	// Repeated real update-ref attempts provide a deterministic barrier: the
	// caller has been cancelled and the competing writer has tried the exact
	// ref many times, but none may acquire it while the callback is still
	// finishing its commit. The old caller-bound process lifetime lets one of
	// these attempts succeed as soon as cancellation kills update-ref.
	failedAttempt := make(chan struct{}, 1)
	writerAcquired := make(chan struct{})
	writerStopped := make(chan struct{})
	writerCtx, stopWriter := context.WithCancel(context.Background())
	defer stopWriter()
	go func() {
		defer close(writerStopped)
		for {
			attemptCtx, attemptCancel := context.WithTimeout(
				writerCtx, 50*time.Millisecond)
			err := UpdateRef(attemptCtx, dir, branchRef, second, first)
			attemptCancel()
			if err == nil {
				close(writerAcquired)
				return
			}
			select {
			case failedAttempt <- struct{}{}:
			default:
			}
			select {
			case <-writerCtx.Done():
				return
			default:
			}
		}
	}()
	for range 20 {
		select {
		case <-writerAcquired:
			t.Fatal("concurrent writer acquired branch ref before callback returned")
		case <-failedAttempt:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent writer did not finish a lock attempt")
		}
	}
	stopWriter()
	select {
	case <-writerStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent writer did not stop")
	}

	close(releaseCommit)
	select {
	case err := <-lockDone:
		if err != nil {
			t.Fatalf("completed callback reported as failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ref transaction did not finish after callback returned")
	}
	updateCtx, cancelUpdate := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelUpdate()
	if err := UpdateRef(updateCtx, dir, branchRef, second, first); err != nil {
		t.Fatalf("update ref after callback returned: %v", err)
	}
}

func TestWithLockedRecoveryAndBranchRefKeepsLocksUntilCallbackReturnsAfterDeadline(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	first := commitFile(t, ctx, dir, "first.txt", "first", "first")
	second := commitFile(t, ctx, dir, "second.txt", "second", "second", first)
	const (
		recoveryRef = "refs/acd/recovery/test-expired-callback-deadline"
		branchRef   = "refs/heads/main"
	)
	if err := UpdateRef(ctx, dir, recoveryRef, first, ""); err != nil {
		t.Fatalf("seed recovery ref: %v", err)
	}
	if err := UpdateRef(ctx, dir, branchRef, first, ""); err != nil {
		t.Fatalf("seed branch ref: %v", err)
	}

	commitBoundary := make(chan struct{})
	deadlineExpired := make(chan struct{})
	releaseCommit := make(chan struct{})
	lockDone := make(chan error, 1)
	go func() {
		lockDone <- withLockedExpectedRefTimeout(
			ctx, dir, recoveryRef, first, branchRef, first, true,
			50*time.Millisecond, func(callbackCtx context.Context) error {
				close(commitBoundary)
				<-callbackCtx.Done()
				close(deadlineExpired)
				<-releaseCommit
				return nil
			})
	}()
	select {
	case <-commitBoundary:
	case <-time.After(5 * time.Second):
		t.Fatal("callback did not reach its commit boundary")
	}
	select {
	case <-deadlineExpired:
	case <-time.After(5 * time.Second):
		t.Fatal("callback hard deadline did not expire")
	}

	acquiredEarly := false
	for range 10 {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, 50*time.Millisecond)
		err := UpdateRef(attemptCtx, dir, branchRef, second, first)
		attemptCancel()
		if err == nil {
			acquiredEarly = true
			break
		}
	}
	close(releaseCommit)
	select {
	case err := <-lockDone:
		if acquiredEarly {
			t.Fatal("concurrent writer acquired branch ref after callback deadline but before callback return")
		}
		if err != nil {
			t.Fatalf("completed callback reported as failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ref transaction did not finish after callback returned")
	}
	if err := UpdateRef(ctx, dir, branchRef, second, first); err != nil {
		t.Fatalf("concurrent writer after callback: %v", err)
	}
}

func TestWithLockedExpectedRefRejectsMismatchBeforeCallback(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	first := commitFile(t, ctx, dir, "first.txt", "first", "first")
	second := commitFile(t, ctx, dir, "second.txt", "second", "second", first)
	const ref = "refs/heads/main"
	if err := UpdateRef(ctx, dir, ref, first, ""); err != nil {
		t.Fatalf("seed branch ref: %v", err)
	}

	called := false
	err := WithLockedExpectedRef(ctx, dir, ref, second, func() error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("mismatched branch ref unexpectedly verified")
	}
	if called {
		t.Fatal("callback ran without an exact branch-ref match")
	}
}

func TestWithLockedAbsentRefBlocksConcurrentCreation(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	commit := commitFile(t, ctx, dir, "first.txt", "first", "first")
	const ref = "refs/heads/unborn"

	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- WithLockedAbsentRef(ctx, dir, ref, func() error {
			close(locked)
			<-release
			return nil
		})
	}()
	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		t.Fatal("absent ref lock was not acquired")
	}

	mutationCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	err := UpdateRef(mutationCtx, dir, ref, commit, "")
	cancel()
	if err == nil {
		t.Fatal("concurrent ref creation succeeded while absence lock was held")
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WithLockedAbsentRef: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("absent ref lock was not released")
	}

	if err := UpdateRef(ctx, dir, ref, commit, ""); err != nil {
		t.Fatalf("creation after lock release: %v", err)
	}
}

func TestWithLockedAbsentRefSupportsSHA256Repository(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	if _, err := Run(ctx, RunOpts{Dir: dir},
		"init", "--object-format=sha256"); err != nil {
		t.Skipf("git does not support sha256 repositories: %v", err)
	}
	called := false
	if err := WithLockedAbsentRef(
		ctx, dir, "refs/heads/main", func() error {
			called = true
			return nil
		}); err != nil {
		t.Fatalf("WithLockedAbsentRef sha256: %v", err)
	}
	if !called {
		t.Fatal("callback did not run for absent sha256 ref")
	}
}

func TestWithLockedRecoveryRefFailsBeforeCallbackOnMismatch(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	first := commitFile(t, ctx, dir, "first.txt", "first", "first")
	second := commitFile(t, ctx, dir, "second.txt", "second", "second", first)
	const ref = "refs/acd/recovery/prune-mismatch"
	if _, err := EnsureRecoveryRef(ctx, dir, ref, first); err != nil {
		t.Fatalf("seed recovery ref: %v", err)
	}

	called := false
	err := WithLockedRecoveryRef(ctx, dir, ref, second, func() error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("mismatched proof ref unexpectedly verified")
	}
	if called {
		t.Fatal("callback ran without an exact proof-ref match")
	}
}

func TestWithLockedRecoveryRefAbortsOnCallbackError(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	first := commitFile(t, ctx, dir, "first.txt", "first", "first")
	second := commitFile(t, ctx, dir, "second.txt", "second", "second", first)
	const ref = "refs/acd/recovery/prune-abort"
	if _, err := EnsureRecoveryRef(ctx, dir, ref, first); err != nil {
		t.Fatalf("seed recovery ref: %v", err)
	}

	want := errors.New("sqlite prune failed")
	if err := WithLockedRecoveryRef(ctx, dir, ref, first, func() error { return want }); !errors.Is(err, want) {
		t.Fatalf("callback error=%v want %v", err, want)
	}
	if err := UpdateRef(ctx, dir, ref, second, first); err != nil {
		t.Fatalf("mutation after callback abort: %v", err)
	}
}

func TestWithLockedRecoveryRefAndAbsentRefBlocksCreation(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	commit := commitFile(t, ctx, dir, "recovery.txt", "saved", "recovery")
	const recoveryRef = "refs/acd/recovery/dead-branch-race"
	const absentRef = "refs/heads/recreated"
	if _, err := EnsureRecoveryRef(ctx, dir, recoveryRef, commit); err != nil {
		t.Fatalf("seed recovery ref: %v", err)
	}

	creationDone := make(chan error, 1)
	creationStarted := make(chan struct{})
	completedWhileLocked := false
	var creationErr error
	err := WithLockedRecoveryRefAndAbsentRef(ctx, dir, recoveryRef, commit, absentRef, func() error {
		go func() {
			close(creationStarted)
			creationDone <- UpdateRef(ctx, dir, absentRef, commit, "")
		}()
		<-creationStarted
		select {
		case creationErr = <-creationDone:
			completedWhileLocked = true
		case <-time.After(200 * time.Millisecond):
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithLockedRecoveryRefAndAbsentRef: %v", err)
	}
	if completedWhileLocked && creationErr == nil {
		t.Fatal("expected-missing ref was created while transaction lock was held")
	}
	if !completedWhileLocked {
		select {
		case creationErr = <-creationDone:
		case <-time.After(5 * time.Second):
			t.Fatal("expected-missing ref lock was not released")
		}
	}
	if creationErr != nil {
		if err := UpdateRef(ctx, dir, absentRef, commit, ""); err != nil {
			t.Fatalf("create expected-missing ref after lock release: %v", err)
		}
	}
}

func TestShowToplevelAndAbsoluteGitDir(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	top, err := ShowToplevel(ctx, dir)
	if err != nil {
		t.Fatalf("show-toplevel: %v", err)
	}
	if top == "" {
		t.Fatal("expected non-empty toplevel")
	}
	gd, err := AbsoluteGitDir(ctx, dir)
	if err != nil {
		t.Fatalf("absolute-git-dir: %v", err)
	}
	if gd == "" {
		t.Fatal("expected non-empty git dir")
	}
}

func TestIsAncestor_True(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	base := commitFile(t, ctx, dir, "base.txt", "base", "base")
	descendant := commitFile(t, ctx, dir, "descendant.txt", "descendant", "descendant", base)

	ok, err := IsAncestor(ctx, dir, base, descendant)
	if err != nil {
		t.Fatalf("is ancestor: %v", err)
	}
	if !ok {
		t.Fatal("expected base commit to be ancestor of descendant")
	}
}

func TestIsAncestor_False(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	base := commitFile(t, ctx, dir, "base.txt", "base", "base")
	main := commitFile(t, ctx, dir, "main.txt", "main", "main", base)
	divergent := commitFile(t, ctx, dir, "branch.txt", "branch", "branch", base)

	ok, err := IsAncestor(ctx, dir, divergent, main)
	if err != nil {
		t.Fatalf("is ancestor: %v", err)
	}
	if ok {
		t.Fatal("expected divergent commit not to be ancestor of main commit")
	}
}

func TestIsAncestor_BadOID(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	base := commitFile(t, ctx, dir, "base.txt", "base", "base")

	ok, err := IsAncestor(ctx, dir, "not-an-oid", base)
	if err == nil {
		t.Fatal("expected malformed oid to return an error")
	}
	if ok {
		t.Fatal("expected malformed oid not to be reported as ancestor")
	}
	var gerr *Error
	if !errors.As(err, &gerr) {
		t.Fatalf("expected *git.Error, got %T: %v", err, err)
	}
	if gerr.ExitCode != 128 {
		t.Fatalf("expected exit 128 for bad oid, got %d", gerr.ExitCode)
	}
}

func TestIsAncestor_RepoMissing(t *testing.T) {
	requireGit(t)
	ctx := context.Background()
	missingRepo := filepath.Join(t.TempDir(), "missing")

	ok, err := IsAncestor(ctx, missingRepo, "HEAD", "HEAD")
	if err == nil {
		t.Fatal("expected missing repo to return an error")
	}
	if ok {
		t.Fatal("expected missing repo not to be reported as ancestor")
	}
	var gerr *Error
	if !errors.As(err, &gerr) {
		t.Fatalf("expected *git.Error, got %T: %v", err, err)
	}
}

// TestRevParse_DisambiguatesAmbiguousRef proves the exit-1 disambiguation
// contract: when git emits an "ambiguous" warning to stderr at exit 1, we
// must surface it as a real error rather than coercing it to ErrRefNotFound.
//
// Pre-fix `--quiet` swallowed the ambiguity warning and ExitCode==1 mapped
// unconditionally to ErrRefNotFound, hiding misconfiguration. With --quiet
// dropped and the empty-stderr predicate added, the ambiguous case surfaces
// as a *git.Error whose stderr contains the warning.
//
// Setup: create a branch named "foo" AND a tag named "foo" pointing at the
// same commit. `git rev-parse --verify foo` exits 1 with an
// "ambiguous"/"refers to multiple objects" warning to stderr.
func TestRevParse_DisambiguatesAmbiguousRef(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()

	// Build a commit so we have something to reference.
	blob, err := HashObjectStdin(ctx, dir, []byte("hello"))
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	tree, err := Mktree(ctx, dir, []MktreeEntry{
		{Mode: "100644", Type: "blob", OID: blob, Path: "hello"},
	})
	if err != nil {
		t.Fatalf("mktree: %v", err)
	}
	commit, err := CommitTree(ctx, dir, tree, "seed")
	if err != nil {
		t.Fatalf("commit-tree: %v", err)
	}
	if err := UpdateRef(ctx, dir, "refs/heads/main", commit, ""); err != nil {
		t.Fatalf("update-ref main: %v", err)
	}
	// Create a branch and a tag with the same short name.
	if err := UpdateRef(ctx, dir, "refs/heads/foo", commit, ""); err != nil {
		t.Fatalf("update-ref refs/heads/foo: %v", err)
	}
	if err := UpdateRef(ctx, dir, "refs/tags/foo", commit, ""); err != nil {
		t.Fatalf("update-ref refs/tags/foo: %v", err)
	}

	_, err = RevParse(ctx, dir, "foo")
	if err == nil {
		t.Fatal("expected ambiguous-ref error, got nil")
	}
	if errors.Is(err, ErrRefNotFound) {
		t.Fatalf("ambiguous ref incorrectly mapped to ErrRefNotFound: %v", err)
	}
	if !errors.Is(err, ErrRefAmbiguous) {
		t.Fatalf("expected ErrRefAmbiguous, got: %v", err)
	}
	// The error string must surface git's ambiguity warning so callers can
	// diagnose the misconfiguration.
	if !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("expected error to mention 'ambiguous', got: %v", err)
	}
}

// TestRefExists_PresentAndMissing verifies the basic present/absent contract.
func TestRefExists_PresentAndMissing(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()

	// Set up a real commit so refs/heads/main resolves.
	commit := commitFile(t, ctx, dir, "init.txt", "hello", "init")
	if err := UpdateRef(ctx, dir, "refs/heads/main", commit, ""); err != nil {
		t.Fatalf("update-ref main: %v", err)
	}

	// Existing ref → true.
	ok, err := RefExists(ctx, dir, "refs/heads/main")
	if err != nil {
		t.Fatalf("RefExists refs/heads/main: %v", err)
	}
	if !ok {
		t.Fatal("expected refs/heads/main to exist")
	}

	// Missing ref → false, no error.
	ok, err = RefExists(ctx, dir, "refs/heads/does-not-exist")
	if err != nil {
		t.Fatalf("RefExists missing: %v", err)
	}
	if ok {
		t.Fatal("expected refs/heads/does-not-exist to be absent")
	}
}

// TestRefExists_CreateAndDelete creates a second branch, checks it, then
// deletes it and checks again — covering the full present→absent lifecycle.
func TestRefExists_CreateAndDelete(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()

	commit := commitFile(t, ctx, dir, "seed.txt", "seed", "seed")
	if err := UpdateRef(ctx, dir, "refs/heads/main", commit, ""); err != nil {
		t.Fatalf("update-ref main: %v", err)
	}

	// Create feat-x pointing at HEAD.
	if _, err := Run(ctx, RunOpts{Dir: dir}, "update-ref", "refs/heads/feat-x", commit); err != nil {
		t.Fatalf("create refs/heads/feat-x: %v", err)
	}

	ok, err := RefExists(ctx, dir, "refs/heads/feat-x")
	if err != nil {
		t.Fatalf("RefExists after create: %v", err)
	}
	if !ok {
		t.Fatal("expected refs/heads/feat-x to exist after creation")
	}

	// Delete feat-x.
	if _, err := Run(ctx, RunOpts{Dir: dir}, "update-ref", "-d", "refs/heads/feat-x"); err != nil {
		t.Fatalf("delete refs/heads/feat-x: %v", err)
	}

	ok, err = RefExists(ctx, dir, "refs/heads/feat-x")
	if err != nil {
		t.Fatalf("RefExists after delete: %v", err)
	}
	if ok {
		t.Fatal("expected refs/heads/feat-x to be absent after deletion")
	}
}

// TestRefExists_EmptyRef ensures that an empty ref argument returns an error
// immediately without shelling out.
func TestRefExists_EmptyRef(t *testing.T) {
	requireGit(t)
	ctx := context.Background()
	ok, err := RefExists(ctx, t.TempDir(), "")
	if err == nil {
		t.Fatal("expected error for empty ref, got nil")
	}
	if ok {
		t.Fatal("expected ok=false for empty ref")
	}
}

// TestRefExists_CancelledCtx confirms that a cancelled context propagates as
// an error (not a silent false).
func TestRefExists_CancelledCtx(t *testing.T) {
	dir := initRepo(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately before the call

	ok, err := RefExists(ctx, dir, "refs/heads/main")
	if err == nil {
		t.Fatal("expected error for cancelled ctx, got nil")
	}
	if ok {
		t.Fatal("expected ok=false for cancelled ctx")
	}
}

// TestLiveBranchSet_EmptyRepoReturnsEmptySet exercises the no-refs case. A
// freshly-initialized repo with no commits has no refs under refs/heads/;
// for-each-ref exits 0 with empty stdout. The helper must return a
// non-nil empty map (callers do `_, ok := set[ref]` membership probes).
func TestLiveBranchSet_EmptyRepoReturnsEmptySet(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()

	set, err := LiveBranchSet(ctx, dir)
	if err != nil {
		t.Fatalf("LiveBranchSet on empty repo: %v", err)
	}
	if set == nil {
		t.Fatal("expected non-nil empty set, got nil")
	}
	if len(set) != 0 {
		t.Fatalf("empty repo set len=%d want 0; got %v", len(set), set)
	}
}

// TestLiveBranchSet_OneBranch creates a single ref and asserts the set
// contains exactly that ref.
func TestLiveBranchSet_OneBranch(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()

	commit := commitFile(t, ctx, dir, "seed.txt", "seed", "seed")
	if err := UpdateRef(ctx, dir, "refs/heads/main", commit, ""); err != nil {
		t.Fatalf("update-ref main: %v", err)
	}

	set, err := LiveBranchSet(ctx, dir)
	if err != nil {
		t.Fatalf("LiveBranchSet: %v", err)
	}
	if _, ok := set["refs/heads/main"]; !ok {
		t.Fatalf("expected refs/heads/main in set; got %v", set)
	}
	if len(set) != 1 {
		t.Fatalf("set len=%d want 1; got %v", len(set), set)
	}
}

// TestLiveBranchSet_MultipleBranches asserts the set contains every ref
// when several branches coexist. Order-independent: callers do membership
// probes, not iteration in a fixed order.
func TestLiveBranchSet_MultipleBranches(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()

	commit := commitFile(t, ctx, dir, "seed.txt", "seed", "seed")
	for _, ref := range []string{
		"refs/heads/main",
		"refs/heads/feature-a",
		"refs/heads/feature-b",
		"refs/heads/release/v1",
	} {
		if err := UpdateRef(ctx, dir, ref, commit, ""); err != nil {
			t.Fatalf("update-ref %s: %v", ref, err)
		}
	}

	set, err := LiveBranchSet(ctx, dir)
	if err != nil {
		t.Fatalf("LiveBranchSet: %v", err)
	}
	for _, ref := range []string{
		"refs/heads/main",
		"refs/heads/feature-a",
		"refs/heads/feature-b",
		"refs/heads/release/v1",
	} {
		if _, ok := set[ref]; !ok {
			t.Fatalf("expected %q in set; got %v", ref, set)
		}
	}
	if len(set) != 4 {
		t.Fatalf("set len=%d want 4; got %v", len(set), set)
	}
}

// TestLiveBranchSet_BogusRepoReturnsError asserts that a non-git directory
// surfaces a real error rather than a silently-empty map (which would let
// the dead-branch sweep prune every ref on a transient FS error).
func TestLiveBranchSet_BogusRepoReturnsError(t *testing.T) {
	requireGit(t)
	ctx := context.Background()

	bogus := t.TempDir()
	_, err := LiveBranchSet(ctx, bogus)
	if err == nil {
		t.Fatal("expected error for non-git directory, got nil")
	}
}

// TestLiveBranchSet_EmptyRepoDirReturnsError asserts the defensive guard.
func TestLiveBranchSet_EmptyRepoDirReturnsError(t *testing.T) {
	ctx := context.Background()
	_, err := LiveBranchSet(ctx, "")
	if err == nil {
		t.Fatal("expected error for empty repoDir, got nil")
	}
}

func commitFile(t *testing.T, ctx context.Context, dir, path, content, message string, parents ...string) string {
	t.Helper()
	blob, err := HashObjectStdin(ctx, dir, []byte(content))
	if err != nil {
		t.Fatalf("hash %s: %v", path, err)
	}
	tree, err := Mktree(ctx, dir, []MktreeEntry{
		{Mode: RegularFileMode, Type: "blob", OID: blob, Path: path},
	})
	if err != nil {
		t.Fatalf("mktree %s: %v", path, err)
	}
	commit, err := CommitTree(ctx, dir, tree, message, parents...)
	if err != nil {
		t.Fatalf("commit-tree %s: %v", path, err)
	}
	return commit
}
