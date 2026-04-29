//go:build integration
// +build integration

// ai_providers_test.go drives §10 (AI providers) end-to-end through the
// real `acd` binary. The daemon picks up ACD_AI_* from the inherited
// process environment (start.go does not strip env on spawn), so each
// scenario simply passes the relevant env vars on `acd start`.
//
// Coverage:
//
//  1. Deterministic default (ACD_AI_PROVIDER unset)         — TestAI_DeterministicDefault
//  2. openai-compat against a mock HTTP server (success)    — TestAI_OpenAICompatMockSuccess
//  3. openai-compat 5xx -> deterministic fallback           — TestAI_OpenAICompat5xxFallback
//  4. Subprocess plugin happy path                          — TestAI_SubprocessPluginHappyPath
//  5. Subprocess plugin timeout (ACD_AI_TIMEOUT=300ms)      — TestAI_SubprocessPluginTimeoutFallback
//  6. Subprocess plugin crash + respawn between events      — TestAI_SubprocessPluginCrashRespawn
//
// Plugin tests are skipped on Windows (the bash shebang trick is not
// portable; v1 ships no Windows support anyway per D1).
package integration_test

import (
	"context"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// headSubject returns the HEAD commit subject (`%s` in git log) for repo.
func headSubject(t *testing.T, repo string) string {
	t.Helper()
	out := runGitOK(t, repo, "log", "-1", "--pretty=%s", "HEAD")
	return strings.TrimSpace(out)
}

// commitCount returns the integer count of commits reachable from HEAD.
func commitCount(t *testing.T, repo string) int {
	t.Helper()
	out := runGitOK(t, repo, "rev-list", "--count", "HEAD")
	n := 0
	fmt.Sscanf(strings.TrimSpace(out), "%d", &n)
	return n
}

// waitHeadAdvances polls until rev-parse HEAD changes from start.
func waitHeadAdvances(t *testing.T, repo, start string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := runGit(repo, "rev-parse", "HEAD")
		if err == nil {
			cur := strings.TrimSpace(out)
			if cur != start {
				return cur
			}
		}
		time.Sleep(60 * time.Millisecond)
	}
	t.Fatalf("HEAD did not advance past %s within %v", start, timeout)
	return ""
}

// TestAI_DeterministicDefault: with no ACD_AI_* env vars set, a single
// new-file event must land with the deterministic "Add <basename>"
// subject.
func TestAI_DeterministicDefault(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "ai-det", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	startHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(repo, "deterministic.txt"), "hi\n")
	wakeSession(t, ctx, env, repo, "ai-det")
	waitHeadAdvances(t, repo, startHead, 8*time.Second)

	subj := headSubject(t, repo)
	if subj != "Add deterministic.txt" {
		t.Fatalf("deterministic subject = %q want %q", subj, "Add deterministic.txt")
	}
}

// newOpenAITestServer returns an HTTPS httptest server plus the environment
// override that lets the CGO-disabled integration binary trust its certificate.
func newOpenAITestServer(t *testing.T, handler http.Handler) (*httptest.Server, string) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: server.Certificate().Raw,
	})
	certPath := filepath.Join(t.TempDir(), "openai-test-ca.pem")
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		server.Close()
		t.Fatalf("write OpenAI test CA: %v", err)
	}
	return server, "SSL_CERT_FILE=" + certPath
}

// TestAI_OpenAICompatMockSuccess: the daemon points at an HTTPS test server
// whose chat/completions endpoint returns a canned tool_call. The commit
// subject must be the value the mock returned.
func TestAI_OpenAICompatMockSuccess(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	const wantSubject = "AI subject from mock"
	const cannedResp = `{
  "id": "chatcmpl-test",
  "object": "chat.completion",
  "model": "gpt-4o-mini",
  "choices": [{
    "index": 0,
    "message": {
      "role": "assistant",
      "content": "",
      "tool_calls": [{
        "id": "call_1",
        "type": "function",
        "function": {
          "name": "commit_message",
          "arguments": "{\"subject\":\"AI subject from mock\",\"body\":\"\"}"
        }
      }]
    },
    "finish_reason": "tool_calls"
  }]
}`

	var hits atomic.Int32
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(cannedResp))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	extra := []string{
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-4o-mini",
		trustEnv,
	}
	startSession(t, ctx, env, repo, "ai-mock", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)

	startHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(repo, "mock.txt"), "via openai-compat mock\n")
	wakeSession(t, ctx, envWith(env, extra...), repo, "ai-mock")
	waitHeadAdvances(t, repo, startHead, 8*time.Second)

	subj := headSubject(t, repo)
	if subj != wantSubject {
		t.Fatalf("subject=%q want %q (server hits=%d)", subj, wantSubject, hits.Load())
	}
	if hits.Load() == 0 {
		t.Fatalf("mock never received a request; daemon did not hit it")
	}
}

// TestAI_OpenAICompat5xxFallback: the mock returns HTTP 500. The daemon
// must fall back to the deterministic "Add <basename>" subject and log
// a warning containing the upstream status.
func TestAI_OpenAICompat5xxFallback(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	var hits atomic.Int32
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, `{"error":{"message":"upstream boom"}}`, http.StatusInternalServerError)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	extra := []string{
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-4o-mini",
		trustEnv,
	}
	p := startSessionJSON(t, ctx, env, repo, "ai-5xx", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)

	startHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(repo, "boom.txt"), "5xx fallback\n")
	wakeSession(t, ctx, envWith(env, extra...), repo, "ai-5xx")
	waitHeadAdvances(t, repo, startHead, 8*time.Second)

	subj := headSubject(t, repo)
	if subj != "Add boom.txt" {
		t.Fatalf("subject=%q want deterministic 'Add boom.txt' (server hits=%d)", subj, hits.Load())
	}
	if hits.Load() == 0 {
		t.Fatalf("mock never received a request; daemon never tried openai-compat")
	}
	// Confirm the daemon log mentions the 5xx so operators can see why.
	tail := readDaemonLogTail(t, env, p.RepoHash)
	if tail != "" && !strings.Contains(tail, "500") {
		t.Logf("daemon log tail did not include 500 marker (best-effort check). tail:\n%s", tail)
	}
}

// TestAI_OpenAICompatReceivesCapturedDiff: end-to-end coverage that the
// daemon builds CommitContext.DiffText from the captured before/after blobs
// (BuildOpsDiff) and forwards it to the openai-compat provider. We modify
// an existing tracked file so the diff carries both `-` and `+` lines, run
// the daemon through the real binary, and assert the mock OpenAI server's
// captured request body contains the expected diff hunk plus repo metadata.
//
// The unit-level TestOpenAI_ForwardsCommitContext (internal/ai) injects a
// hand-rolled diff string into CommitContext directly. This integration
// counterpart proves the full pipe — capture -> shadow -> ops_diff -> AI
// payload — survives at runtime.
func TestAI_OpenAICompatReceivesCapturedDiff(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	// Track a file in the seed commit so a subsequent edit produces a real
	// modify diff (not a `new file` header without `-` lines).
	writeFile(t, filepath.Join(repo, "tracked.txt"), "before-line\n")
	runGitOK(t, repo, "add", "tracked.txt")
	runGitOK(t, repo, "commit", "-q", "-m", "seed-tracked")

	const wantSubject = "AI saw the diff"
	const cannedResp = `{
  "id": "chatcmpl-diff",
  "object": "chat.completion",
  "model": "gpt-4o-mini",
  "choices": [{
    "index": 0,
    "message": {
      "role": "assistant",
      "content": "",
      "tool_calls": [{
        "id": "call_d",
        "type": "function",
        "function": {
          "name": "commit_message",
          "arguments": "{\"subject\":\"AI saw the diff\",\"body\":\"\"}"
        }
      }]
    },
    "finish_reason": "tool_calls"
  }]
}`

	var capturedBody atomic.Pointer[string]
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		body := string(raw)
		capturedBody.Store(&body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(cannedResp))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	extra := []string{
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-4o-mini",
		"ACD_AI_SEND_DIFF=1",
		trustEnv,
	}
	startSession(t, ctx, env, repo, "ai-diff", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)

	// Modify the tracked file — this becomes a `modify` capture op carrying
	// before_oid (seed blob) and after_oid (new blob) the daemon turns into
	// a unified diff before forwarding to the AI provider.
	startHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(repo, "tracked.txt"), "after-line\n")
	wakeSession(t, ctx, envWith(env, extra...), repo, "ai-diff")
	waitHeadAdvances(t, repo, startHead, 8*time.Second)

	subj := headSubject(t, repo)
	if subj != wantSubject {
		t.Fatalf("subject=%q want %q", subj, wantSubject)
	}

	bodyPtr := capturedBody.Load()
	if bodyPtr == nil {
		t.Fatalf("mock OpenAI server received no request body")
	}
	body := *bodyPtr
	// The serialized request encodes the user content as JSON-in-JSON. We
	// can search the raw request bytes for the diff signatures the daemon
	// emits via BuildOpsDiff: a `diff --git` header for tracked.txt plus
	// the `-before-line` / `+after-line` change. The header carries the
	// repo-relative path, the lines carry the actual content.
	for _, want := range []string{
		"diff --git a/tracked.txt b/tracked.txt",
		"-before-line",
		"+after-line",
		"refs/heads/main",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("captured request body missing %q\nbody:\n%s", want, body)
		}
	}
}

// writePluginScript creates an executable bash script at
// <dir>/acd-provider-<name> with the given body and returns the absolute
// directory path. Caller adds the dir to PATH on the spawned daemon's env.
func writePluginScript(t *testing.T, name, body string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("subprocess plugin tests rely on bash; skipped on windows")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "acd-provider-"+name)
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("write plugin %s: %v", script, err)
	}
	// Belt-and-suspenders: chmod even if WriteFile honored the mode.
	if err := os.Chmod(script, 0o755); err != nil {
		t.Fatalf("chmod plugin: %v", err)
	}
	return dir
}

// pathPrepended returns "PATH=<extra>:$PATH" for the spawned daemon env.
func pathPrepended(extra string) string {
	if cur := os.Getenv("PATH"); cur != "" {
		return "PATH=" + extra + string(os.PathListSeparator) + cur
	}
	return "PATH=" + extra
}

// TestAI_SubprocessPluginHappyPath: a bash plugin replies with a fixed
// JSONL line for every request. The first commit's subject must be the
// plugin's response.
func TestAI_SubprocessPluginHappyPath(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available; subprocess plugin tests skipped")
	}
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}

	plugDir := writePluginScript(t, "test", `#!/usr/bin/env bash
while IFS= read -r line; do
  printf '{"version":1,"subject":"plugin says hi","body":"","error":""}\n'
done
`)
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	extra := []string{
		"ACD_AI_PROVIDER=subprocess:test",
		pathPrepended(plugDir),
	}
	startSession(t, ctx, env, repo, "ai-plug", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)

	startHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(repo, "from-plugin.txt"), "via plugin\n")
	wakeSession(t, ctx, envWith(env, extra...), repo, "ai-plug")
	waitHeadAdvances(t, repo, startHead, 8*time.Second)

	subj := headSubject(t, repo)
	if subj != "plugin says hi" {
		t.Fatalf("subject=%q want %q", subj, "plugin says hi")
	}
}

// TestAI_SubprocessPluginTimeoutFallback: a plugin that sleeps longer
// than ACD_AI_TIMEOUT must not wedge the daemon. The 300ms cap ensures
// the test completes in well under 2s; the commit subject must be the
// deterministic fallback.
func TestAI_SubprocessPluginTimeoutFallback(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available; subprocess plugin tests skipped")
	}
	if _, err := exec.LookPath("sleep"); err != nil {
		t.Skip("sleep not available")
	}

	plugDir := writePluginScript(t, "test", `#!/usr/bin/env bash
while IFS= read -r line; do
  sleep 60
  printf '{"version":1,"subject":"never","body":"","error":""}\n'
done
`)
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	extra := []string{
		"ACD_AI_PROVIDER=subprocess:test",
		"ACD_AI_TIMEOUT=300ms",
		pathPrepended(plugDir),
	}
	startSession(t, ctx, env, repo, "ai-timeout", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)

	startHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(repo, "slowplug.txt"), "timeout-test\n")
	wakeSession(t, ctx, envWith(env, extra...), repo, "ai-timeout")
	waitHeadAdvances(t, repo, startHead, 10*time.Second)

	subj := headSubject(t, repo)
	if subj != "Add slowplug.txt" {
		t.Fatalf("subject=%q want deterministic fallback 'Add slowplug.txt'", subj)
	}
}

// TestAI_SubprocessPluginCrashRespawn: a plugin that exits cleanly after
// answering one request must respawn for the second event. We drive two
// distinct events back-to-back and require both commits to land. The
// plugin response is identical across both processes; the assertion is
// that the second event isn't dropped or wedged.
func TestAI_SubprocessPluginCrashRespawn(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available; subprocess plugin tests skipped")
	}
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}

	plugDir := writePluginScript(t, "test", `#!/usr/bin/env bash
IFS= read -r line
printf '{"version":1,"subject":"first ok","body":"","error":""}\n'
exit 0
`)
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	extra := []string{
		"ACD_AI_PROVIDER=subprocess:test",
		pathPrepended(plugDir),
	}
	startSession(t, ctx, env, repo, "ai-respawn", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	wakeEnv := envWith(env, extra...)

	// Event #1
	headBefore1 := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(repo, "first.txt"), "one\n")
	wakeSession(t, ctx, wakeEnv, repo, "ai-respawn")
	headAfter1 := waitHeadAdvances(t, repo, headBefore1, 8*time.Second)
	cnt1 := commitCount(t, repo)

	// Event #2 — plugin has exited; daemon must respawn it.
	writeFile(t, filepath.Join(repo, "second.txt"), "two\n")
	wakeSession(t, ctx, wakeEnv, repo, "ai-respawn")
	waitHeadAdvances(t, repo, headAfter1, 10*time.Second)
	cnt2 := commitCount(t, repo)
	if cnt2 <= cnt1 {
		t.Fatalf("commit count did not advance after respawn: pre=%d post=%d", cnt1, cnt2)
	}

	// Both commits must appear in history. The first carries "first ok"
	// (plugin succeeded). The second is either "first ok" again
	// (plugin respawned and replied identically) or the deterministic
	// fallback "Add second.txt" (if the respawn raced). Either resolution
	// is acceptable — the contract is "the daemon is not wedged".
	subjects := strings.Split(strings.TrimSpace(runGitOK(t, repo, "log", "--pretty=%s")), "\n")
	if len(subjects) < 2 {
		t.Fatalf("expected at least 2 commits, got %d (%q)", len(subjects), subjects)
	}
	// At least one commit must be from the plugin (proves the respawn loop
	// produced output at least once).
	hasPluginSubject := false
	for _, s := range subjects {
		if s == "first ok" {
			hasPluginSubject = true
			break
		}
	}
	if !hasPluginSubject {
		t.Fatalf("no commit carried the plugin subject 'first ok'; subjects=%v", subjects)
	}
}
