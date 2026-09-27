//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestCommitAllIntentReplansCachedWaitAfterRestart(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	var plannerCalls atomic.Int32
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		req := decodeIntentChatRequest(t, r)
		if req.ToolChoice.Function.Name == "commit_message" {
			writeIntentMessageRewriteResponse(t, w, req)
			return
		}
		seqs := offeredIntentSeqsLenient(t, req)
		if len(seqs) != 1 {
			t.Logf("cached-wait planner expected one capture, offered seqs=%v", seqs)
			http.Error(w, "expected one offered capture", http.StatusBadRequest)
			return
		}
		if plannerCalls.Add(1) == 1 {
			writeNativeIntentCandidatesResponse(t, w, "call_wait", []map[string]any{{
				"candidate_id": "cached-wait", "selected_seqs": seqs,
				"purpose": "wait for a possible companion", "readiness": "wait",
				"missing_companions": []string{"possible_test.go"},
				"grouping_reason":    "the first non-forced pass may wait",
			}})
			return
		}
		if active := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM publication_drains WHERE phase='semantic' AND target_event_count=1"); active != "1" {
			t.Errorf("second planner call preceded the explicit commit-all target: active drains=%s", active)
			http.Error(w, "explicit publication target required", http.StatusBadRequest)
			return
		}
		writeNativeIntentCandidatesResponse(t, w, "call_forced", []map[string]any{
			nativeReadyIntentCandidate("forced-ready", seqs,
				"Commit forced capture", "", "commit-all forces the complete capture"),
		})
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		// One soft boundary starts the initial plan. Ordinary wakes then wait
		// for another capture; only commit-all bypasses that batch threshold.
		"ACD_INTENT_MIN_PENDING=2",
		"ACD_INTENT_DEFER_LIMIT=2",
		"ACD_INTENT_SETTLE_WINDOW=0",
		"ACD_INTENT_MAX_PENDING_AGE=5m",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-6-luna",
		trustEnv,
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
	fullEnv := envWith(env, extra...)
	firstSession := startSession(t, ctx, env, repo, "cached-wait-a", "shell", extra...)
	// This scenario needs one complete capture, without an intermediate
	// empty file observed between create and write.
	writeFileAtomically(t, repo, filepath.Join(repo, "forced.go"),
		"package forced\n\nfunc Ready() bool { return true }\n")
	wakeSession(t, ctx, fullEnv, repo, "cached-wait-a")
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("cached-wait state: planner_calls=%d captures=%s candidates=%s deferrals=%s",
				plannerCalls.Load(),
				sqliteScalar(t, dbPath, "SELECT group_concat(seq || ':' || state || ':' || operation || ':' || path) FROM capture_events"),
				sqliteScalar(t, dbPath, "SELECT group_concat(id || ':' || status) FROM intent_candidates"),
				sqliteScalar(t, dbPath, "SELECT group_concat(event_seq || ':' || defer_count) FROM planner_state"))
		}
	})
	waitFor(t, "one capture before the initial boundary", 15*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM capture_events WHERE state='pending'") == "1"
	})
	hint := func(kind string) {
		t.Helper()
		res := runAcd(t, ctx, fullEnv, "internal", "hint", "--repo", repo, "--kind", kind)
		if res.ExitCode != 0 {
			t.Fatalf("%s hint exit=%d: %s", kind, res.ExitCode, res.Stderr)
		}
	}
	hint("soft_boundary")
	waitFor(t, "non-forced plan waits", 15*time.Second, func() bool {
		return plannerCalls.Load() == 1 && sqliteScalar(t, dbPath,
			"SELECT COUNT(*) FROM intent_candidates WHERE status='waiting'") == "1" &&
			sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM intent_activity_boundaries WHERE consumed_ts IS NOT NULL") == "1"
	})
	const completedPlans = "SELECT group_concat(fingerprint) FROM intent_plan_runs WHERE completed=1"
	waitFingerprint := sqliteScalar(t, dbPath, completedPlans)
	if waitFingerprint == "" {
		t.Fatal("waiting plan was not cached durably")
	}
	assertWaiting := func() {
		t.Helper()
		if plannerCalls.Load() != 1 || sqliteScalar(t, dbPath, completedPlans) != waitFingerprint ||
			sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM intent_candidates WHERE status='waiting'") != "1" ||
			sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM capture_events WHERE state='pending'") != "1" {
			t.Fatal("ordinary wake or restart changed the cached waiting target")
		}
	}
	wakeSession(t, ctx, fullEnv, repo, "cached-wait-a")
	hint("checkpoint")
	assertWaiting()
	// The compatibility stop command only closes a session. Disable this
	// isolated repository and prove worker ownership ended before restarting.
	off := runAcd(t, ctx, fullEnv, "off", "--force", "--repo", repo, "--json")
	if off.ExitCode != 0 {
		t.Fatalf("off exit=%d: %s", off.ExitCode, off.Stderr)
	}
	var stoppedLock *daemon.DaemonLock
	waitFor(t, "cached-wait worker ownership released", 10*time.Second, func() bool {
		lock, err := daemon.AcquireDaemonLock(filepath.Join(repo, ".git"))
		if errors.Is(err, daemon.ErrDaemonLockHeld) {
			return false
		}
		if err != nil {
			t.Fatal(err)
		}
		stoppedLock = lock
		return true
	})
	t.Cleanup(func() { _ = stoppedLock.Release() })
	assertWaiting()
	if err := stoppedLock.Release(); err != nil {
		t.Fatal(err)
	}
	on := runAcd(t, ctx, fullEnv, "on", "--repo", repo, "--json")
	if on.ExitCode != 0 {
		t.Fatalf("on exit=%d: %s", on.ExitCode, on.Stderr)
	}

	secondSession := startSession(t, ctx, env, repo, "cached-wait-b", "shell", extra...)
	if firstSession.DaemonPID <= 0 || secondSession.DaemonPID <= 0 || firstSession.DaemonPID == secondSession.DaemonPID {
		t.Fatalf("worker did not restart: before=%d after=%d", firstSession.DaemonPID, secondSession.DaemonPID)
	}
	t.Cleanup(func() { shutdownDaemon(t, fullEnv, repo, "cached-wait-b") })
	hint("checkpoint")
	assertWaiting()
	result := runAcd(t, ctx, fullEnv, "commit-all", "--repo", repo, "--yes")
	if result.ExitCode != 0 {
		t.Fatalf("commit-all exit=%d\nstdout=%s\nstderr=%s",
			result.ExitCode, result.Stdout, result.Stderr)
	}
	if got := plannerCalls.Load(); got != 2 {
		t.Fatalf("planner calls=%d want 2; forced request reused cached wait", got)
	}
	if dirty := strings.TrimSpace(runGitOK(t, repo, "status", "--porcelain")); dirty != "" {
		t.Fatalf("commit-all left worktree dirty: %s", dirty)
	}
}

func TestCommitAllIntentForcedRepairPublishesWideCandidateAfterRestart(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	forcedRequest := make(chan struct{})
	releaseForced := make(chan struct{})
	var plannerCalls atomic.Int32
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("wide candidate diagnostics: calls=%d candidates=%s plans=%s drains=%s replay=%s",
				plannerCalls.Load(),
				sqliteScalar(t, dbPath, "SELECT group_concat(id || ':' || status || ':' || missing_companions) FROM intent_candidates"),
				sqliteScalar(t, dbPath, "SELECT group_concat(resolution_mode || ':' || completed) FROM intent_plan_runs"),
				sqliteScalar(t, dbPath, "SELECT group_concat(id || ':' || phase || ':' || reason_code || ':' || last_error) FROM publication_drains"),
				sqliteScalar(t, dbPath, "SELECT group_concat(key || ':' || value) FROM daemon_meta WHERE key LIKE '%attention%' OR key LIKE '%replay%'"))
		}
	})
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeIntentChatRequest(t, r)
		if req.ToolChoice.Function.Name == "commit_message" {
			writeIntentMessageRewriteResponse(t, w, req)
			return
		}
		seqs := offeredIntentSeqsLenient(t, req)
		switch plannerCalls.Add(1) {
		case 1:
			if len(seqs) != 1 {
				http.Error(w, "expected forced singleton", http.StatusBadRequest)
				return
			}
			close(forcedRequest)
			select {
			case <-releaseForced:
			case <-r.Context().Done():
				return
			}
			writeNativeIntentCandidatesResponse(t, w, "forced_wait", []map[string]any{{
				"candidate_id": "wide-shortcuts", "selected_seqs": seqs,
				"purpose": "finish one shortcut change", "readiness": "wait",
				"missing_companions": []string{"model-only companion"},
				"grouping_reason":    "the model incorrectly deferred forced work",
			}})
		default:
			writeNativeIntentCandidatesResponse(t, w, "wide_retry", []map[string]any{
				nativeReadyIntentCandidate("wide-shortcuts", seqs,
					"Finish shortcut change", "", "complete captured shortcut work"),
			})
		}
	}))
	defer server.Close()
	defer func() {
		select {
		case <-releaseForced:
		default:
			close(releaseForced)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	extra := []string{
		"ACD_COMMIT_STRATEGY=intent", "ACD_INTENT_WINDOW=20",
		"ACD_INTENT_MIN_PENDING=14", "ACD_INTENT_DEFER_LIMIT=2",
		"ACD_INTENT_SETTLE_WINDOW=0",
		"ACD_INTENT_MAX_PENDING_AGE=5m", "ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL, "ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-6-luna", trustEnv,
	}
	extra = activateIntentV2RuntimeWithPreset(t, repo, "balanced", extra...)
	fullEnv := envWith(env, extra...)
	firstSession := startSession(t, ctx, env, repo, "wide-forced-a", "shell", extra...)
	if got := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key='intent.v2.preset_id'"); got != "intent.balanced" {
		t.Fatalf("runtime preset=%q want intent.balanced", got)
	}
	for i := 0; i < 13; i++ {
		name := "shortcut-" + strconv.Itoa(i) + ".go"
		writeFileAtomically(t, repo, filepath.Join(repo, name),
			"package shortcuts\n\nfunc Shortcut"+strconv.Itoa(i)+"() {}\n")
	}
	wakeSession(t, ctx, fullEnv, repo, "wide-forced-a")
	waitFor(t, "wide changes checkpointed", 15*time.Second, func() bool {
		return sqliteScalar(t, dbPath, `SELECT COUNT(DISTINCT e.seq)
FROM capture_events e
JOIN checkpoint_events ce ON ce.event_seq=e.seq
JOIN checkpoints c ON c.id=ce.checkpoint_id
WHERE e.state='pending' AND c.phase='completed'`) == "13"
	})

	off := runAcd(t, ctx, fullEnv, "off", "--force", "--repo", repo, "--json")
	if off.ExitCode != 0 {
		t.Fatalf("off exit=%d: %s", off.ExitCode, off.Stderr)
	}
	var stoppedLock *daemon.DaemonLock
	waitFor(t, "wide candidate worker released", 10*time.Second, func() bool {
		lock, err := daemon.AcquireDaemonLock(filepath.Join(repo, ".git"))
		if errors.Is(err, daemon.ErrDaemonLockHeld) {
			return false
		}
		if err != nil {
			t.Fatal(err)
		}
		stoppedLock = lock
		return true
	})
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.SQL().QueryContext(ctx,
		"SELECT seq, branch_ref, branch_generation FROM capture_events WHERE state='pending' ORDER BY seq")
	if err != nil {
		t.Fatal(err)
	}
	var branchRef string
	var generation int64
	var events []state.IntentCandidateEvent
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq, &branchRef, &generation); err != nil {
			t.Fatal(err)
		}
		events = append(events, state.IntentCandidateEvent{EventSeq: seq, EventRole: "code"})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if len(events) != 13 {
		t.Fatalf("captured events=%d want 13", len(events))
	}
	if err := state.SaveIntentCandidate(ctx, db, state.IntentCandidate{
		ID: "wide-shortcuts", BranchRef: branchRef, BranchGeneration: generation,
		Status: state.IntentCandidateWaiting, Readiness: state.IntentReadinessWait,
		Purpose:           "finish one shortcut change",
		MissingCompanions: "balanced fallback exceeds 12 paths", Events: events,
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := state.RecordPlannerDefer(ctx, db, events[0].EventSeq,
			float64(time.Now().Unix()), "wide candidate waiting"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stoppedLock.Release(); err != nil {
		t.Fatal(err)
	}
	on := runAcd(t, ctx, fullEnv, "on", "--repo", repo, "--json")
	if on.ExitCode != 0 {
		status := runAcd(t, ctx, fullEnv, "status", "--repo", repo, "--json")
		t.Fatalf("on exit=%d: %s\nstatus=%s", on.ExitCode, on.Stderr, status.Stdout)
	}
	secondSession := startSession(t, ctx, env, repo, "wide-forced-b", "shell", extra...)
	if firstSession.DaemonPID <= 0 || secondSession.DaemonPID <= 0 || firstSession.DaemonPID == secondSession.DaemonPID {
		t.Fatalf("worker did not restart: before=%d after=%d", firstSession.DaemonPID, secondSession.DaemonPID)
	}
	t.Cleanup(func() { shutdownDaemon(t, fullEnv, repo, "wide-forced-b") })
	t.Logf("seeded candidate restart: planner state=%s",
		sqliteScalar(t, dbPath, "SELECT group_concat(event_seq || ':' || defer_count) FROM planner_state"))
	done := make(chan ExecResult, 1)
	go func() { done <- runAcd(t, ctx, fullEnv, "commit-all", "--repo", repo, "--yes") }()
	select {
	case <-forcedRequest:
		t.Log("forced planner request held")
	case <-time.After(15 * time.Second):
		t.Fatalf("forced planner request did not start: calls=%d drains=%s", plannerCalls.Load(),
			sqliteScalar(t, dbPath, "SELECT group_concat(id || ':' || phase || ':' || last_error) FROM publication_drains"))
	}
	waitFor(t, "frozen wide commit-all target", 20*time.Second, func() bool {
		return sqliteScalar(t, dbPath,
			"SELECT COUNT(*) FROM publication_drains WHERE phase='semantic' AND target_event_count=13") == "1"
	})
	writeFileAtomically(t, repo, filepath.Join(repo, "later.go"),
		"package shortcuts\n\nfunc Later() {}\n")
	wakeSession(t, ctx, fullEnv, repo, "wide-forced-b")
	close(releaseForced)
	result := <-done
	if result.ExitCode != 0 {
		t.Fatalf("commit-all exit=%d\nstdout=%s\nstderr=%s", result.ExitCode, result.Stdout, result.Stderr)
	}
	if calls := plannerCalls.Load(); calls != 1 {
		t.Fatalf("planner calls=%d want one forced repair", calls)
	}
	waitFor(t, "wide forced candidate published after commit-all", 10*time.Second, func() bool {
		return plannerCalls.Load() >= 2 && sqliteScalar(t, dbPath,
			"SELECT COUNT(*) FROM capture_events WHERE state='published'") == "13"
	})
	for i := 0; i < 13; i++ {
		name := "shortcut-" + strconv.Itoa(i) + ".go"
		if _, err := runGit(repo, "cat-file", "-e", "HEAD:"+name); err != nil {
			t.Fatalf("wide candidate path %s missing from branch: %v", name, err)
		}
	}
	if _, err := runGit(repo, "cat-file", "-e", "HEAD:later.go"); err == nil {
		t.Fatal("later capture entered the frozen target")
	}
	waitFor(t, "later edit remains protected", 15*time.Second, func() bool {
		return sqliteScalar(t, dbPath, `SELECT COUNT(*)
FROM capture_events e
JOIN checkpoint_events ce ON ce.event_seq=e.seq
JOIN checkpoints c ON c.id=ce.checkpoint_id
WHERE e.path='later.go' AND e.state='pending' AND c.phase='completed'`) == "1"
	})
	status := runAcd(t, ctx, fullEnv, "status", "--repo", repo, "--json")
	var payload struct {
		Data struct {
			Protected bool `json:"protected"`
			Outcome   struct {
				BranchChanges  int `json:"branch_changes"`
				WaitingChanges int `json:"waiting_changes"`
			} `json:"publication_outcome"`
		} `json:"data"`
	}
	if status.ExitCode != 0 || json.Unmarshal([]byte(status.Stdout), &payload) != nil ||
		!payload.Data.Protected || payload.Data.Outcome.BranchChanges != 13 || payload.Data.Outcome.WaitingChanges != 1 {
		t.Fatalf("status after forced repair: %+v\n%s", status, status.Stdout)
	}
	list := runAcd(t, ctx, fullEnv, "list", "--once", "--all")
	if list.ExitCode != 0 || !strings.Contains(list.Stdout, filepath.Base(repo)) ||
		strings.Contains(list.Stdout, "needs action") {
		t.Fatalf("list after forced repair: %+v", list)
	}
}

// commitAllFixture seeds a tempRepo with a deterministic dirty worktree:
// many uncommitted files spread across multiple directories with sibling
// clusters, so the planner sees coherent windows. Returns the repo dir and
// the lexicographically sorted list of expected paths.
func commitAllFixture(t *testing.T) (string, []string) {
	t.Helper()
	repo := tempRepo(t)
	return repo, writeCommitAllFixture(t, repo)
}

func writeCommitAllFixture(t *testing.T, repo string) []string {
	t.Helper()
	files := []string{
		"cmd/main.go",
		"docs/a.md",
		"docs/b.md",
		"pkg/a/x.go",
		"pkg/a/y.go",
		"pkg/a/z.go",
		"pkg/b/x.go",
		"pkg/b/y.go",
		"pkg/b/z.go",
	}
	// Defensive: lex-sort regardless of source ordering.
	sort.Strings(files)
	for i, rel := range files {
		writeFile(t, filepath.Join(repo, rel), "// "+rel+"\n// content "+strconv.Itoa(i)+"\n")
	}
	return files
}

// commitsTouchingPath returns commit OIDs (oldest-first) that touched path.
// Walks `git log --reverse --name-only HEAD` so we observe the on-disk order
// commit-all produced.
func commitsTouchingPath(t *testing.T, repo, path string) []string {
	t.Helper()
	out := runGitOK(t, repo, "log", "--reverse", "--name-only", "--pretty=format:COMMIT %H", "HEAD")
	var hits []string
	current := ""
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "COMMIT ") {
			current = strings.TrimPrefix(line, "COMMIT ")
			continue
		}
		if line == path {
			hits = append(hits, current)
		}
	}
	return hits
}

// allCommitFiles returns, oldest-first, the set of (commit_oid -> [paths])
// pairs and the flat ordered list of paths as they appear across the history
// past the seed commit.
func allCommitFiles(t *testing.T, repo string) (orderedCommits []string, perCommit map[string][]string) {
	t.Helper()
	out := runGitOK(t, repo, "log", "--reverse", "--name-only", "--pretty=format:COMMIT %H", "HEAD")
	perCommit = map[string][]string{}
	current := ""
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "COMMIT ") {
			current = strings.TrimPrefix(line, "COMMIT ")
			orderedCommits = append(orderedCommits, current)
			continue
		}
		perCommit[current] = append(perCommit[current], line)
	}
	return orderedCommits, perCommit
}

// commitAllEnv returns withIsolatedHome augmented with the requested commit
// strategy / provider knobs. We always pin ACD_AI_PROVIDER explicitly so
// host-level env (or absence thereof) cannot perturb the test outcome.
func commitAllEnv(t *testing.T, strategy, provider string) []string {
	t.Helper()
	base := withIsolatedHome(t)
	extras := []string{
		"ACD_COMMIT_STRATEGY=" + strategy,
		"ACD_AI_PROVIDER=" + provider,
	}
	return envWith(base, extras...)
}

// TestCommitAllEventStrategyOrdersByPath: with strategy=event, every dirty
// file becomes its own commit and commit ordering follows lex(path).
func TestCommitAllEventStrategyOrdersByPath(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary required")
	}
	repo, files := commitAllFixture(t)
	env := commitAllEnv(t, "event", "deterministic")
	ensureCheckpointRuntime(t, env, repo, buildAcdBinary(t))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seedCommitCount := commitCount(t, repo)

	res := runAcd(t, ctx, env, "commit-all", "--repo", repo, "--yes")
	if res.ExitCode != 0 {
		t.Fatalf("commit-all exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}

	// Worktree must be clean.
	if dirty := strings.TrimSpace(runGitOK(t, repo, "status", "--porcelain")); dirty != "" {
		t.Fatalf("worktree still dirty after commit-all:\n%s", dirty)
	}

	finalCount := commitCount(t, repo)
	wantCount := seedCommitCount + len(files)
	if finalCount != wantCount {
		t.Fatalf("event-strategy commit count=%d want=%d (seed=%d files=%d)",
			finalCount, wantCount, seedCommitCount, len(files))
	}

	orderedCommits, perCommit := allCommitFiles(t, repo)
	// Skip the seed commit (first); inspect only the post-seed history.
	if len(orderedCommits) < seedCommitCount {
		t.Fatalf("history shorter than seedCount=%d: got %d commits", seedCommitCount, len(orderedCommits))
	}
	postSeed := orderedCommits[seedCommitCount:]
	if len(postSeed) != len(files) {
		t.Fatalf("post-seed commit count=%d want=%d", len(postSeed), len(files))
	}

	// Each post-seed commit must touch exactly one of our fixture paths,
	// and the order must be lex(path).
	wantOrder := append([]string(nil), files...)
	sort.Strings(wantOrder)
	gotOrder := make([]string, 0, len(postSeed))
	for _, c := range postSeed {
		paths := perCommit[c]
		if len(paths) != 1 {
			t.Fatalf("event-strategy commit %s touched %d files, want 1: %v", c, len(paths), paths)
		}
		gotOrder = append(gotOrder, paths[0])
	}
	for i, want := range wantOrder {
		if gotOrder[i] != want {
			t.Fatalf("event-strategy commit ordering mismatch at idx=%d:\nwant=%v\ngot =%v", i, wantOrder, gotOrder)
		}
	}
	assertRecentDashboardActivity(t, repo)

}

// TestCommitAllIntentStrategyDeterministic: with strategy=intent and the
// deterministic AI provider, every dirty file ends up committed and no file
// is left dirty. The deterministic planner currently emits one-event plans,
// but commit-all must still sweep every offered seq to terminal published.
func TestCommitAllIntentStrategyDeterministic(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary required")
	}
	repo, files := commitAllFixture(t)
	env := commitAllEnv(t, "intent", "deterministic")
	env = activateIntentV2Runtime(t, repo, env...)
	ensureCheckpointRuntime(t, env, repo, buildAcdBinary(t))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seedCommitCount := commitCount(t, repo)

	res := runAcd(t, ctx, env, "commit-all", "--repo", repo, "--yes")
	if res.ExitCode != 0 {
		t.Fatalf("commit-all (intent) exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}

	if dirty := strings.TrimSpace(runGitOK(t, repo, "status", "--porcelain")); dirty != "" {
		t.Fatalf("worktree still dirty after commit-all (intent):\n%s\nstdout=%s\nstderr=%s",
			dirty, res.Stdout, res.Stderr)
	}

	// All fixture files must be present in the post-seed history. Grouped
	// commits are allowed; we only assert that no file was dropped.
	orderedCommits, perCommit := allCommitFiles(t, repo)
	if len(orderedCommits) <= seedCommitCount {
		t.Fatalf("intent strategy produced no new commits beyond seed (seed=%d, total=%d)",
			seedCommitCount, len(orderedCommits))
	}
	committed := map[string]bool{}
	for _, c := range orderedCommits[seedCommitCount:] {
		for _, p := range perCommit[c] {
			committed[p] = true
		}
	}
	for _, want := range files {
		if !committed[want] {
			t.Fatalf("intent strategy did not commit %q; committed=%v", want, sortedKeys(committed))
		}
	}

	// At least as many commits as planner-style passes — bounded above by
	// number of files. We don't constrain the exact count because grouping
	// is provider-defined.
	postSeed := len(orderedCommits) - seedCommitCount
	if postSeed < 1 || postSeed > len(files) {
		t.Fatalf("intent post-seed commit count=%d out of [1, %d]", postSeed, len(files))
	}
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestCommitAllWorksWhileWorkerAlive verifies the public command uses the
// managed worker instead of contending with it through a direct writer path.
func TestCommitAllWorksWhileWorkerAlive(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary required")
	}
	repo, _ := commitAllFixture(t)
	env := commitAllEnv(t, "event", "deterministic")
	ensureCheckpointRuntime(t, env, repo, buildAcdBinary(t))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res := runAcd(t, ctx, env, "commit-all", "--repo", repo, "--yes")
	if res.ExitCode != 0 {
		t.Fatalf("commit-all with live worker exit=%d\nstdout=%s\nstderr=%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}
	if dirty := strings.TrimSpace(runGitOK(t, repo, "status", "--porcelain")); dirty != "" {
		t.Fatalf("worktree still dirty after worker-driven commit-all:\n%s", dirty)
	}
}

// TestCommitAllRefusesOnDetachedHEAD: detach HEAD, then assert commit-all
// refuses and mentions the detached state.
func TestCommitAllRefusesOnDetachedHEAD(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary required")
	}
	repo, _ := commitAllFixture(t)
	env := commitAllEnv(t, "event", "deterministic")
	ensureCheckpointRuntime(t, env, repo, buildAcdBinary(t))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Detach HEAD onto the seed commit's SHA.
	headSHA := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	runGitOK(t, repo, "checkout", "--quiet", "--detach", headSHA)

	res := runAcd(t, ctx, env, "commit-all", "--repo", repo, "--yes")
	if res.ExitCode == 0 {
		t.Fatalf("commit-all should refuse on detached HEAD; stdout=%s\nstderr=%s",
			res.Stdout, res.Stderr)
	}
	combined := strings.ToLower(res.Stdout + res.Stderr)
	if !strings.Contains(combined, "detached") {
		t.Fatalf("expected refusal message to mention detached HEAD; got:\nstdout=%s\nstderr=%s",
			res.Stdout, res.Stderr)
	}
}
