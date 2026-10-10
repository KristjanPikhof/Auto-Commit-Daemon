//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCaptureResilienceLargeAssetsDuringProviderOutage(t *testing.T) {
	t.Parallel()
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })
	var plannerHits, messageHits atomic.Int32
	var sawMetadata atomic.Bool
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeIntentChatRequest(t, r)
		if req.ToolChoice.Function.Name == "commit_message" {
			messageHits.Add(1)
		} else {
			plannerHits.Add(1)
		}
		for _, message := range req.Messages {
			if strings.Contains(message.Content, `"file_metadata"`) && strings.Contains(message.Content, `"kind":"binary"`) {
				sawMetadata.Store(true)
			}
			if strings.Contains(message.Content, `\u0000\u0000`) {
				t.Error("binary contents reached the provider")
			}
		}
		http.Error(w, "provider unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	extra := activateIntentV2Runtime(t, repo,
		"ACD_AI_PROVIDER=openai-compat", "ACD_AI_BASE_URL="+server.URL,
		"ACD_AI_API_KEY=test-key", "ACD_AI_MODEL=gpt-6-luna", trustEnv)
	startSession(t, ctx, env, repo, "large-assets", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	fullEnv := envWith(env, extra...)
	startHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	if result := runAcd(t, ctx, fullEnv, "pause", "--repo", repo, "--yes", "--json"); result.ExitCode != 0 {
		t.Fatalf("pause: %s %s", result.Stdout, result.Stderr)
	}
	names := []string{"eng_autocomplete.bin", "est_autocomplete.bin", "rus_autocomplete.bin", "spa_autocomplete.bin"}
	sizes := []int{14832809, 21685601, 18343302, 15617454}
	if err := os.MkdirAll(filepath.Join(repo, "assets"), 0755); err != nil {
		t.Fatal(err)
	}
	for i, name := range names {
		if err := os.WriteFile(filepath.Join(repo, "assets", name), bytes.Repeat([]byte{byte(i)}, sizes[i]), 0644); err != nil {
			t.Fatal(err)
		}
	}
	writeFile(t, filepath.Join(repo, "autocomplete.go"), "package autocomplete\n\nfunc Complete() string { return \"eng_autocomplete.bin\" }\n")
	writeFile(t, filepath.Join(repo, "autocomplete_test.go"), "package autocomplete\n\n// Complete uses the bundled dictionary.\n")
	writeFile(t, filepath.Join(repo, "autocomplete.md"), "Autocomplete uses the bundled dictionaries.\n")
	writeFile(t, filepath.Join(repo, "settings.py"), "def save_settings(value):\n    return value\n")
	// Keep a deliberate staging choice separate from the captured worktree bytes.
	writeFile(t, filepath.Join(repo, ".gitignore"), "# user staging choice\n")
	runGitOK(t, repo, "add", ".gitignore")
	writeFile(t, filepath.Join(repo, ".gitignore"), "# acd integration seed\n")
	indexBefore := runGitOK(t, repo, "ls-files", "--stage", ".gitignore")
	if result := runAcd(t, ctx, fullEnv, "resume", "--repo", repo, "--yes", "--json"); result.ExitCode != 0 {
		t.Fatalf("resume: %s %s", result.Stdout, result.Stderr)
	}
	if result := runAcd(t, ctx, fullEnv, "flush", "--repo", repo, "--session-id", "large-assets", "--logical", "--json"); result.ExitCode != 0 {
		t.Fatalf("flush: %s %s", result.Stdout, result.Stderr)
	}
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	for _, name := range append(names, "../autocomplete.go", "../autocomplete_test.go", "../autocomplete.md", "../settings.py") {
		path := filepath.ToSlash(filepath.Clean(filepath.Join("assets", name)))
		waitForEventState(t, dbPath, path, "pending", 30*time.Second)
		want := strings.TrimSpace(runGitOK(t, repo, "hash-object", path))
		ref := sqliteScalar(t, dbPath, "SELECT checkpoint_ref FROM checkpoints WHERE phase='completed' AND retained=1 ORDER BY seq DESC LIMIT 1")
		got := strings.TrimSpace(runGitOK(t, repo, "rev-parse", ref+":"+path))
		if got != want {
			t.Fatalf("%s differs from protected checkpoint blob", path)
		}
	}
	indexAfter := runGitOK(t, repo, "ls-files", "--stage", ".gitignore")
	if indexAfter != indexBefore {
		t.Fatalf("user staging changed: before=%s after=%s", indexBefore, indexAfter)
	}
	waitFor(t, "provider outage after large assets are protected", 15*time.Second, func() bool {
		return plannerHits.Load() >= 1 && sawMetadata.Load()
	})
	if plannerHits.Load() < 1 || plannerHits.Load() > 3 || messageHits.Load() != 0 || !sawMetadata.Load() {
		t.Fatalf("provider calls=%d message calls=%d binary metadata=%t", plannerHits.Load(), messageHits.Load(), sawMetadata.Load())
	}
	if got := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); got != startHead {
		t.Fatalf("provider outage published unverified local history: %s want=%s", got, startHead)
	}
	assertOutageStatusAndList(t, ctx, fullEnv, repo, 8)
}
