//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestIntentWorktreeReliability exercises the production binary across the
// failure sequence that originally exposed the self-healing gaps. Linked
// worktrees take the canonical writer lock one at a time; main then remains
// the sole attached writer while the Intent v2 and history-reconciliation
// assertions run.
func TestIntentWorktreeReliability(t *testing.T) {
	requireSQLite(t)
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary required")
	}

	repo := tempRepo(t)
	baseEnv := withIsolatedHome(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	peers := make([]string, 3)
	peerEnvs := make([][]string, 3)
	peerRemoved := make([]*bool, 3)
	for i := range peers {
		peers[i] = filepath.Join(t.TempDir(), fmt.Sprintf("peer-%d", i+1))
		peerEnvs[i] = withIsolatedHome(t)
		runGitOK(t, repo, "worktree", "add", "-q", "-b", fmt.Sprintf("intent-peer-%d", i+1), peers[i])
		peer := peers[i]
		removed := new(bool)
		peerRemoved[i] = removed
		t.Cleanup(func() {
			if *removed {
				return
			}
			if err := removeIntentWorktree(repo, peer); err != nil {
				t.Errorf("cleanup linked worktree %s: %v", peer, err)
			}
		})
	}

	// Canonical ownership permits one linked-worktree worker at a time. Drive
	// each peer through the same transition, prove poll wakes do not spam the
	// log, then stop it before handing ownership to the next worktree.
	for i, peer := range peers {
		peerEnv := peerEnvs[i]
		session := fmt.Sprintf("intent-peer-%d", i+1)
		started := startSessionJSON(t, ctx, peerEnv, peer, session, "shell",
			"ACD_FSNOTIFY_ENABLED=0")
		active := true
		t.Cleanup(func() {
			if active {
				if err := stopIntentTestWorker(t, peerEnv, peer, session); err != nil {
					t.Errorf("cleanup %s: %v", session, err)
				}
			}
		})
		peerGitDir := strings.TrimSpace(runGitOK(t, peer, "rev-parse", "--absolute-git-dir"))
		peerDB := filepath.Join(peerGitDir, "acd", "state.db")
		waitFor(t, session+" running", 8*time.Second, func() bool {
			return sqliteScalar(t, peerDB, "SELECT mode FROM daemon_state WHERE id=1") == "running"
		})
		head := strings.TrimSpace(runGitOK(t, peer, "rev-parse", "HEAD"))
		runGitOK(t, peer, "checkout", "--quiet", "--detach", head)
		for range 3 {
			wakeSession(t, ctx, peerEnv, peer, session)
		}
		waitFor(t, session+" detached marker", 8*time.Second, func() bool {
			return sqliteScalar(t, peerDB, "SELECT COUNT(*) FROM daemon_meta WHERE key='detached_head_paused'") == "1"
		})
		logText := readDaemonLogTail(t, peerEnv, started.RepoHash)
		message := "detached HEAD detected; capture and publication paused for this worktree"
		if got := strings.Count(logText, message); got != 1 {
			t.Fatalf("%s detach logs=%d want 1\n%s", session, got, logText)
		}
		canonicalPeer, err := filepath.EvalSymlinks(peer)
		if err != nil {
			t.Fatalf("canonicalize %s: %v", peer, err)
		}
		for _, field := range []string{`"repo_hash":"` + started.RepoHash + `"`, `"worktree":"` + canonicalPeer + `"`, `"git_dir":"`} {
			if !strings.Contains(logText, field) {
				t.Fatalf("%s detach log missing context %s\n%s", session, field, logText)
			}
		}
		runGitOK(t, peer, "checkout", "--quiet", fmt.Sprintf("intent-peer-%d", i+1))
		wakeSession(t, ctx, peerEnv, peer, session)
		waitFor(t, session+" reattached marker cleared", 8*time.Second, func() bool {
			return sqliteScalar(t, peerDB, "SELECT COUNT(*) FROM daemon_meta WHERE key='detached_head_paused'") == "0"
		})
		if err := stopIntentTestWorker(t, peerEnv, peer, session); err != nil {
			t.Fatalf("stop %s: %v", session, err)
		}
		active = false
		runGitOK(t, peer, "checkout", "--quiet", "--detach", head)
	}

	var plannerCalls atomic.Int32
	var rewriteCalls atomic.Int32
	var responseMu sync.Mutex
	var plannerPaths [][]string
	slowStarted := make(chan struct{}, 1)
	slowRelease := make(chan struct{})
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		req := decodeIntentChatRequest(t, r)
		if req.ToolChoice.Function.Name == "commit_message" {
			rewriteCalls.Add(1)
			writeIntentMessageRewriteResponse(t, w, req)
			return
		}
		call := plannerCalls.Add(1)
		captures := offeredIntentCaptures(t, req)
		seqs := make([]int64, 0, len(captures))
		paths := make([]string, 0, len(captures))
		for _, capture := range captures {
			seqs = append(seqs, capture.Seq)
			paths = append(paths, capture.Path)
		}
		responseMu.Lock()
		plannerPaths = append(plannerPaths, append([]string(nil), paths...))
		responseMu.Unlock()

		switch {
		case containsIntentPath(paths, "flow.txt"):
			if call == 1 {
				slowStarted <- struct{}{}
				select {
				case <-slowRelease:
				case <-time.After(2500 * time.Millisecond):
				}
			}
			candidates := make([]map[string]any, 0, len(seqs))
			for i, seq := range seqs {
				candidates = append(candidates, nativeReadyIntentCandidate(
					fmt.Sprintf("flow-%d", i+1), []int64{seq},
					fmt.Sprintf("Apply flow step %d", i+1),
					"Preserve the ordered flow update.",
					"ordered same-path capture"))
			}
			// Same-path order is a hard edge. Omitting depends_on_candidates
			// deliberately produces hard_dependency_undeclared; local repair
			// must add the provable edge without another remote call.
			writeNativeIntentCandidatesResponse(t, w, "call_hard_edge", candidates)
		case containsIntentPath(paths, "outside.txt"):
			writeNativeIntentCandidatesResponse(t, w, "call_outside", []map[string]any{
				nativeReadyIntentCandidate("outside", []int64{999999}, "Outside window", "Invalid selection.", "structural reject"),
			})
		case containsIntentPath(paths, "forced.txt"):
			candidates := make([]map[string]any, 0, len(seqs))
			for _, seq := range seqs {
				candidates = append(candidates, map[string]any{
					"candidate_id": fmt.Sprintf("forced-%d", seq), "selected_seqs": []int64{seq},
					"purpose": "wait for an unavailable companion", "readiness": "wait",
					"missing_companions":    []string{"companion outside the offered window"},
					"depends_on_candidates": []string{}, "subject": "", "body": "",
					"grouping_reason": "forced-aging deferral fixture",
				})
			}
			writeNativeIntentCandidatesResponse(t, w, "call_forced_defer", candidates)
		default:
			http.Error(w, "unexpected offered paths: "+strings.Join(paths, ","), http.StatusBadRequest)
		}
	}))
	defer server.Close()

	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_COMMIT_PRESET=fast",
		"ACD_INTENT_WINDOW=2",
		"ACD_INTENT_MIN_PENDING=2",
		"ACD_INTENT_SETTLE_WINDOW=1s",
		"ACD_INTENT_MAX_PENDING_AGE=2s",
		"ACD_INTENT_DEFER_LIMIT=1",
		"ACD_PATH_QUIESCENCE_SECONDS=0",
		"ACD_FSNOTIFY_ENABLED=0",
		"ACD_REWIND_GRACE_SECONDS=0",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=integration-test-key",
		"ACD_AI_MODEL=gpt-5.4-mini",
		"ACD_AI_TIMEOUT=10s",
		trustEnv,
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
	env := envWith(baseEnv, extra...)
	startSessionJSON(t, ctx, env, repo, "intent-self-healing", "shell")
	mainActive := true
	t.Cleanup(func() {
		if mainActive {
			if err := stopIntentTestWorker(t, env, repo, "intent-self-healing"); err != nil {
				t.Errorf("cleanup intent-self-healing: %v", err)
			}
		}
	})
	waitMode(t, repo, "running", 5*time.Second)
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	baselineHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))

	// Fill the entire planner window with two ordered same-path captures.
	// Continued activity must hold the full window until the quiet period.
	flowPath := filepath.Join(repo, "flow.txt")
	writeFile(t, flowPath, "step one\n")
	wakeSession(t, ctx, env, repo, "intent-self-healing")
	waitFor(t, "first flow capture", 8*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM capture_events WHERE path='flow.txt'") == "1"
	})
	writeFile(t, flowPath, "step two\n")
	wakeSession(t, ctx, env, repo, "intent-self-healing")
	waitFor(t, "full flow window", 8*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM capture_events WHERE path='flow.txt'") == "2"
	})
	time.Sleep(250 * time.Millisecond)
	if got := plannerCalls.Load(); got != 0 {
		t.Fatalf("planner calls while full window still active=%d want 0", got)
	}

	select {
	case <-slowStarted:
	case <-time.After(8 * time.Second):
		t.Fatal("slow planner response did not start after quiet release")
	}
	maxHeartbeatAge := time.Duration(0)
	for i := 0; i < 8; i++ {
		hb := readHeartbeatTs(repo)
		if hb > 0 {
			age := time.Since(time.Unix(int64(hb), int64((hb-float64(int64(hb)))*1e9)))
			if age > maxHeartbeatAge {
				maxHeartbeatAge = age
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	close(slowRelease)
	waitForEventState(t, dbPath, "flow.txt", "published", 20*time.Second)
	if maxHeartbeatAge > 2*time.Second {
		t.Fatalf("heartbeat maximum age during slow provider=%s want <=2s", maxHeartbeatAge)
	}
	assertIntentCLITruthAgreement(t, ctx, env, repo, true, true)

	// Structural outside-window output must skip correction and fall back in
	// the same pass. A logical boundary releases the single pending capture.
	writeFile(t, filepath.Join(repo, "outside.txt"), "outside\n")
	wakeSession(t, ctx, env, repo, "intent-self-healing")
	flushIntentBoundary(t, ctx, env, repo)
	waitForEventState(t, dbPath, "outside.txt", "published", 20*time.Second)

	// First waiting response persists a candidate. Once age forces the same
	// capture ready, the second waiting response is forced_capture_deferred
	// and deterministic fallback must drain it without needs_attention.
	writeFile(t, filepath.Join(repo, "forced.txt"), "forced\n")
	wakeSession(t, ctx, env, repo, "intent-self-healing")
	flushIntentBoundary(t, ctx, env, repo)
	waitFor(t, "forced candidate waiting", 10*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM intent_candidates WHERE status='waiting' AND purpose LIKE '%unavailable companion%'") == "1"
	})
	time.Sleep(2300 * time.Millisecond)
	wakeSession(t, ctx, env, repo, "intent-self-healing")
	waitForEventState(t, dbPath, "forced.txt", "published", 20*time.Second)

	if got := plannerCalls.Load(); got > 6 {
		t.Fatalf("planner calls=%d want <=6 across three bounded phases", got)
	}
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM capture_events WHERE state IN ('pending','blocked_conflict','failed')"); got != "0" {
		t.Fatalf("terminal/pending events before rewrite=%s want 0", got)
	}
	rejectPath := filepath.Join(repo, ".git", "acd", "planner-rejects.jsonl")
	rejectRaw, err := os.ReadFile(rejectPath)
	if err != nil {
		t.Fatalf("read active planner rejects: %v", err)
	}
	for _, code := range []string{"hard_dependency_undeclared", "capture_outside_window", "forced_capture_deferred"} {
		if !strings.Contains(string(rejectRaw), code) {
			t.Fatalf("active planner rejects missing %s\n%s", code, rejectRaw)
		}
	}
	for _, peer := range peers {
		peerGitDir := strings.TrimSpace(runGitOK(t, peer, "rev-parse", "--absolute-git-dir"))
		if _, err := os.Stat(filepath.Join(peerGitDir, "acd", "planner-rejects.jsonl")); !os.IsNotExist(err) {
			t.Fatalf("detached peer unexpectedly has planner rejects: %s err=%v", peer, err)
		}
	}
	assertIntentCLITruthAgreement(t, ctx, env, repo, true, true)

	// Replace ACD's history with three externally authored commits that all
	// point at the identical final tree. The daemon must bump generation and
	// reseed rather than recapture or claim the new checkpoint as its own.
	acdHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	finalTree := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD^{tree}"))
	generationBefore, err := strconv.Atoi(sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key='branch.generation'"))
	if err != nil {
		t.Fatalf("parse generation before rewrite: %v", err)
	}
	pause := runAcd(t, ctx, env, "pause", "--repo", repo, "--reason", "identical-tree integration rewrite", "--yes", "--json")
	if pause.ExitCode != 0 {
		t.Fatalf("pause before rewrite exit=%d stdout=%s stderr=%s", pause.ExitCode, pause.Stdout, pause.Stderr)
	}
	parent := baselineHead
	for i := 1; i <= 3; i++ {
		parent = strings.TrimSpace(runGitOK(t, repo, "commit-tree", finalTree, "-p", parent,
			"-m", fmt.Sprintf("external identical tree %d", i)))
	}
	runGitOK(t, repo, "update-ref", "refs/heads/main", parent, acdHead)
	runGitOK(t, repo, "reset", "--hard", parent)
	resume := runAcd(t, ctx, env, "resume", "--repo", repo, "--yes", "--json")
	if resume.ExitCode != 0 {
		t.Fatalf("resume after rewrite exit=%d stdout=%s stderr=%s", resume.ExitCode, resume.Stdout, resume.Stderr)
	}
	wakeSession(t, ctx, env, repo, "intent-self-healing")
	waitFor(t, "generation bump after identical-tree rewrite", 12*time.Second, func() bool {
		got, convErr := strconv.Atoi(sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key='branch.generation'"))
		return convErr == nil && got > generationBefore
	})
	if got := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD^{tree}")); got != finalTree {
		t.Fatalf("final tree=%s want pre-rewrite ACD tree=%s", got, finalTree)
	}
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM capture_events WHERE state IN ('pending','blocked_conflict','failed')"); got != "0" {
		t.Fatalf("terminal/pending events after rewrite=%s want 0", got)
	}
	assertIntentCLITruthAgreement(t, ctx, env, repo, true, true)

	responseMu.Lock()
	pathsEvidence := append([][]string(nil), plannerPaths...)
	responseMu.Unlock()
	captureLatency := sqliteScalar(t, dbPath, "SELECT printf('%.3f', MAX(published_ts-captured_ts)) FROM capture_events WHERE published_ts IS NOT NULL")
	captureCount := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM capture_events")
	commitGroups := sqliteScalar(t, dbPath, "SELECT COUNT(DISTINCT commit_oid) FROM capture_events WHERE commit_oid IS NOT NULL")
	finalHealth := `{"worktree_clean":true,"all_changes_committed_in_git":true,"checkpoint_published_by_acd":true,"pending":0,"blocked":0,"failed":0}`
	t.Logf("planner_calls=%d rewrite_calls=%d planner_paths=%v capture_latency_max_s=%s captures=%s commit_groups=%s heartbeat_max_age=%s generation=%d->%s acd_head=%s external_head=%s tree=%s rejects=%s reject_lines=%d peers=%d final_health=%s",
		plannerCalls.Load(), rewriteCalls.Load(), pathsEvidence, captureLatency, captureCount, commitGroups,
		maxHeartbeatAge, generationBefore,
		sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key='branch.generation'"), acdHead, parent, finalTree,
		rejectPath, len(strings.Split(strings.TrimSpace(string(rejectRaw)), "\n")), len(peers), finalHealth)

	if err := stopIntentTestWorker(t, env, repo, "intent-self-healing"); err != nil {
		t.Fatalf("stop intent-self-healing: %v", err)
	}
	mainActive = false
	for i, peer := range peers {
		if err := removeIntentWorktree(repo, peer); err != nil {
			t.Fatalf("remove linked worktree %s: %v", peer, err)
		}
		*peerRemoved[i] = true
		if _, err := os.Stat(peer); !os.IsNotExist(err) {
			t.Fatalf("linked worktree still exists after cleanup: %s err=%v", peer, err)
		}
	}
}

func containsIntentPath(paths []string, want string) bool {
	for _, path := range paths {
		if path == want {
			return true
		}
	}
	return false
}

func stopIntentTestWorker(t *testing.T, env []string, repo, session string) error {
	t.Helper()
	gitDir, err := runGit(repo, "rev-parse", "--absolute-git-dir")
	if err != nil {
		return fmt.Errorf("resolve test git dir: %w", err)
	}
	dbPath := filepath.Join(strings.TrimSpace(gitDir), "acd", "state.db")
	pidOut, err := exec.Command("sqlite3", dbPath, "SELECT pid FROM daemon_state WHERE id=1").CombinedOutput()
	if err != nil {
		return fmt.Errorf("read test worker pid: %w: %s", err, strings.TrimSpace(string(pidOut)))
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidOut)))
	if err != nil || pid <= 0 {
		return fmt.Errorf("invalid test worker pid %q", strings.TrimSpace(string(pidOut)))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res := runAcd(t, ctx, env, "stop", "--repo", repo, "--session-id", session, "--force", "--json")
	if res.ExitCode != 0 {
		return fmt.Errorf("normal stop exit=%d stdout=%s stderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	off := runAcd(t, ctx, env, "off", "--repo", repo, "--force", "--json")
	if off.ExitCode != 0 {
		return fmt.Errorf("normal off exit=%d stdout=%s stderr=%s", off.ExitCode, off.Stdout, off.Stderr)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); err != nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("test-owned worker pid %d survived normal session stop", pid)
}

func removeIntentWorktree(repo, peer string) error {
	out, err := runGit(repo, "worktree", "remove", "--force", peer)
	if err != nil {
		return fmt.Errorf("git worktree remove: %w: %s", err, strings.TrimSpace(out))
	}
	if _, err := os.Stat(peer); !os.IsNotExist(err) {
		return fmt.Errorf("worktree path remains after removal: %s: %v", peer, err)
	}
	return nil
}

func flushIntentBoundary(t *testing.T, ctx context.Context, env []string, repo string) {
	t.Helper()
	res := runAcd(t, ctx, env, "flush", "--repo", repo, "--session-id", "intent-self-healing", "--logical", "--json")
	if res.ExitCode != 0 {
		t.Fatalf("intent logical flush exit=%d stdout=%s stderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
}

func assertIntentCLITruthAgreement(
	t *testing.T,
	ctx context.Context,
	env []string,
	repo string,
	wantCommitted bool,
	wantACDPublished bool,
) {
	t.Helper()
	for _, command := range []string{"status", "diagnose", "doctor"} {
		res := runAcd(t, ctx, env, command, "--repo", repo, "--json")
		if res.ExitCode != 0 {
			t.Fatalf("acd %s exit=%d stdout=%s stderr=%s", command, res.ExitCode, res.Stdout, res.Stderr)
		}
		var payload any
		if err := json.Unmarshal([]byte(res.Stdout), &payload); err != nil {
			t.Fatalf("decode %s JSON: %v\n%s", command, err, res.Stdout)
		}
		for key, want := range map[string]bool{
			"worktree_clean": true, "all_changes_committed_in_git": wantCommitted,
			"checkpoint_published_by_acd": wantACDPublished,
		} {
			got, ok := findJSONBool(payload, key)
			if !ok || got != want {
				t.Fatalf("%s %s=%v found=%v want=%v\n%s", command, key, got, ok, want, res.Stdout)
			}
		}
	}
}

func findJSONBool(value any, key string) (bool, bool) {
	switch typed := value.(type) {
	case map[string]any:
		if got, ok := typed[key].(bool); ok {
			return got, true
		}
		for _, child := range typed {
			if got, ok := findJSONBool(child, key); ok {
				return got, true
			}
		}
	case []any:
		for _, child := range typed {
			if got, ok := findJSONBool(child, key); ok {
				return got, true
			}
		}
	}
	return false, false
}
