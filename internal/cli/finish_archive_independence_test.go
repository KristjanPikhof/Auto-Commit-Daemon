package cli

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/migration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
)

func finishArchiveAttention(t *testing.T, runtime *workerRuntime) state.FinishLegacyCutover {
	t.Helper()
	ctx, repo := t.Context(), runtime.worktree.Root
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	missing := strings.Repeat("d", len(head))
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "cat-file", "-e", missing); err == nil {
		t.Fatal("fixture requires a genuinely missing archived object")
	}
	_, err = testutil.SeedLegacyCapture(ctx, runtime.db, state.CaptureEvent{BranchRef: "refs/heads/main", BaseHead: head, Operation: "create", Path: "archive-only.txt", Fidelity: "full"}, []state.CaptureOp{{Op: "create", Path: "archive-only.txt", AfterOID: sql.NullString{String: missing, Valid: true}, AfterMode: sql.NullString{String: "100644", Valid: true}, Fidelity: "full"}})
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := migration.ActivateFinish(ctx, repo, runtime.db)
	if !errors.Is(err, migration.ErrFinishLegacyAttention) || legacy.State != "needs_attention" || legacy.ReasonCode != "legacy_object_missing" || legacy.SnapshotRef == "" {
		t.Fatalf("missing archive was not durably diagnosed: %+v error=%v", legacy, err)
	}
	return legacy
}

func finishArchiveIntentProvider(t *testing.T, runtime *workerRuntime) *atomic.Int32 {
	t.Helper()
	calls := new(atomic.Int32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []struct{ Role, Content string }
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var request ai.FinishPlanRequest
		for _, message := range body.Messages {
			if message.Role == "user" {
				if err := json.Unmarshal([]byte(message.Content), &request); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
			}
		}
		if r.Header.Get("Authorization") != "Bearer archive-independence-key" || len(request.Changes) == 0 {
			t.Error("configured Intent transport or current evidence was bypassed")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var ids []string
		for _, change := range request.Changes {
			if change.Path == "archive-only.txt" {
				t.Error("archived capture entered current planning evidence")
			}
			ids = append(ids, change.ID)
		}
		calls.Add(1)
		plan, _ := json.Marshal(map[string]any{"goals": []finish.Goal{{Purpose: "Complete current behavior", Boundary: "Keep the selected current changes together", Message: "Complete current behavior\n\n- Preserve the current behavior without changing other work.", ChangeIDs: ids}}})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"tool_calls": []any{map[string]any{"type": "function", "function": map[string]string{"name": "finish_plan", "arguments": string(plan)}}}}}}})
	}))
	t.Cleanup(server.Close)
	t.Setenv(ai.EnvAPIKey, "archive-independence-key")
	finishConfigRevision(t, runtime.db, map[string]any{
		config.FieldCommitStrategy: "intent", config.FieldCommitFormat: "imperative",
		config.FieldProvider: "openai-compat", config.FieldBaseURL: server.URL,
		config.FieldModel: "archive-independence-model", config.FieldIntentVerification: "none",
		"confirmations": []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials, ai.ConfirmationInsecureEndpointCredentials},
	}, sql.NullInt64{}, true)
	return calls
}

func assertFinishArchiveUnchanged(t *testing.T, runtime *workerRuntime, before state.FinishLegacyCutover) {
	t.Helper()
	after, err := state.LoadFinishLegacyCutover(t.Context(), runtime.db)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("current work changed archived attention: before=%+v after=%+v error=%v", before, after, err)
	}
	var oid, status string
	if err := runtime.db.ReadSQL().QueryRowContext(t.Context(), `SELECT after_oid,state FROM capture_ops JOIN capture_events ON event_seq=seq WHERE capture_ops.path='archive-only.txt'`).Scan(&oid, &status); err != nil || oid != strings.Repeat("d", 40) || status != state.EventStatePending {
		t.Fatalf("archived capture changed: oid=%s status=%s error=%v", oid, status, err)
	}
}

func TestFinishCLICurrentWorkPublishesDespiteMissingLegacyArchive(t *testing.T) {
	lookup, runtime, handler := finishCLIFixture(t)
	legacy := finishArchiveAttention(t, runtime)
	calls := finishArchiveIntentProvider(t, runtime)
	serveIntentHistoryWorker(t, lookup, handler)
	live := []byte("Current protected work\n")
	if err := os.WriteFile(filepath.Join(runtime.worktree.Root, "current.txt"), live, 0600); err != nil {
		t.Fatal(err)
	}
	result, _, err := finishCLIExecute(t, t.Context(), runtime.worktree.Root, "finish", "Complete current behavior")
	if err != nil || !result.Protected || !result.Published || result.State != state.FinishCompleted || result.CommitsCreated != 1 || calls.Load() != 1 {
		t.Fatalf("current finish: %+v calls=%d error=%v", result, calls.Load(), err)
	}
	if body, err := git.Run(t.Context(), git.RunOpts{Dir: runtime.worktree.Root}, "show", "HEAD:current.txt"); err != nil || !bytes.Equal(body, live) {
		t.Fatalf("published current bytes: %q error=%v", body, err)
	}
	if body, err := os.ReadFile(filepath.Join(runtime.worktree.Root, "current.txt")); err != nil || !bytes.Equal(body, live) {
		t.Fatal("finish changed live current work")
	}
	assertFinishArchiveUnchanged(t, runtime, legacy)
}

func TestFinishHistoryCurrentGitRangeIgnoresMissingLegacyArchive(t *testing.T) {
	lookup, runtime, handler := finishCLIFixture(t)
	legacy := finishArchiveAttention(t, runtime)
	calls := finishArchiveIntentProvider(t, runtime)
	repo := runtime.worktree.Root
	var originals []string
	for _, path := range []string{"first.txt", "second.txt"} {
		writeRewriteTestFile(t, repo, path, "Recorded current behavior\n")
		for _, args := range [][]string{{"add", path}, {"commit", "-qm", "Preserve current " + path}} {
			if _, err := git.Run(t.Context(), git.RunOpts{Dir: repo}, args...); err != nil {
				t.Fatal(err)
			}
		}
		head, err := git.RevParse(t.Context(), repo, "HEAD")
		if err != nil {
			t.Fatal(err)
		}
		originals = append(originals, head)
	}
	selection, err := git.ResolveRewriteSelection(t.Context(), repo, git.RewriteSelectionOptions{FromSHA: originals[0]})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := generateFinishHistoryPlan(t.Context(), &out, repo, selection, rewriteCommitsOptions{planOnly: true, planOut: filepath.Join(t.TempDir(), "current-plan.json")}, true); err != nil {
		t.Fatal(err)
	}
	var plan state.IntentHistoryPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.SourceChain, originals) || plan.Mode != "intent" || plan.PlanningCalls != 1 || calls.Load() != 1 || len(plan.Units) != 2 {
		t.Fatalf("plan did not use only selected Git history: %+v calls=%d", plan, calls.Load())
	}
	lock, err := daemon.AcquireDaemonLock(runtime.worktree.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	serveIntentHistoryWorker(t, lookup, handler)
	index, err := os.ReadFile(filepath.Join(runtime.worktree.GitDir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	for _, dryRun := range []bool{true, false} {
		out.Reset()
		if err := applyIntentHistoryPlan(t.Context(), &out, repo, plan, dryRun); err != nil {
			t.Fatal(err)
		}
		r, ok, err := state.LoadFinishHistoryRequest(t.Context(), runtime.db, plan.ID)
		if err != nil || !ok || r.Status != "completed" || len(r.Checks) == 0 || r.DryRun != dryRun {
			t.Fatalf("history verification: %+v error=%v", r, err)
		}
		if head, err := git.RevParse(t.Context(), repo, "HEAD"); err != nil || (dryRun && head != plan.ExpectedHead) || (!dryRun && head == plan.ExpectedHead) {
			t.Fatalf("history branch outcome dryRun=%t head=%s error=%v", dryRun, head, err)
		}
		if !dryRun {
			var proof state.FinishHistoryProof
			if err := json.Unmarshal(r.Proof, &proof); err != nil {
				t.Fatal(err)
			}
			if head, err := git.RevParse(t.Context(), repo, proof.BackupRef); err != nil || head != plan.ExpectedHead || len(proof.Mappings) != len(originals) {
				t.Fatalf("original current history not preserved: %+v error=%v", proof, err)
			}
		}
	}
	if after, err := os.ReadFile(filepath.Join(runtime.worktree.GitDir, "index")); err != nil || !bytes.Equal(index, after) || calls.Load() != 1 {
		t.Fatalf("apply changed live index or repeated planning: calls=%d error=%v", calls.Load(), err)
	}
	assertFinishArchiveUnchanged(t, runtime, legacy)
}
