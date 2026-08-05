//go:build integration
// +build integration

package integration_test

// intent_atomicity_test.go — verification-lane Wave 3 acceptance tests for
// the b1 outcome (intent grouped publishes are atomic) that landed earlier
// in this branch.
//
// The exhaustive same-path 4-edit visibility proof lives at the daemon
// package level in internal/daemon/replay_test.go (see
// TestReplay_IntentSamePathCapturesRemainPlannerVisible): four sequential
// captures on burst.txt stay planner-visible, the planner may select them
// into one commit, and decision_records carries one row per original seq
// joined by commit_oid.
//
// The integration suite cannot drive four sequential same-path captures
// deterministically: `acd pause` halts capture as well as replay, so a
// write-pause-write sequence produces ONE capture against the worktree state
// at resume rather than four. We therefore drive the same b1
// guarantee end-to-end at the multi-FILE granularity here:
//
//   - Pause the daemon, write four distinct new files in one shot, resume.
//     Capture observes four creates against the baseline shadow; the
//     planner is offered four distinct entries; the mock provider selects
//     all four; the daemon publishes ONE grouped commit covering every
//     capture seq under the same commit_oid; decision_records carries
//     four rows joined by that commit_oid.
//
//   - Pause, write three files, resume. With the planner deferring the
//     middle file (B), publishes A and C in one grouped commit while B
//     stays pending. This proves the daemon does NOT coalesce across
//     planner-deferred captures even when they sit between two selected
//     ones — the at-least-two-commits contract is the planner-decision
//     analogue of the b1 path-boundary contract.

import (
	"context"
	"encoding/json"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type plannerWindowRow struct {
	OfferedSeqs         []int64 `json:"offered_seqs"`
	VisibleOriginalSeqs []int64 `json:"visible_original_seqs"`
	HiddenSeqs          []int64 `json:"hidden_seqs"`
	SelectedGroups      []struct {
		SelectedSeqs   []int64 `json:"selected_seqs"`
		OriginalSeqs   []int64 `json:"original_seqs"`
		Subject        string  `json:"subject"`
		GroupingReason string  `json:"grouping_reason"`
	} `json:"selected_groups"`
	DeferredSeqs []int64 `json:"deferred_seqs"`
}

// TestIntentAtomicity_FourFileBatchLandsAsOneGroupedCommit drives four
// distinct creates through the real daemon under intent strategy. The
// mock planner accepts every offered seq; the daemon must publish ONE
// commit covering all four, with decision_records carrying one row per
// original seq joined by commit_oid (so the CLI's grouped_seqs derivation
// reports len 4). This is the integration-level acceptance of the
// "intent group publishes atomically" contract from b1.
func TestIntentAtomicity_FourFileBatchLandsAsOneGroupedCommit(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	var hits atomic.Int32
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		req := decodeIntentChatRequest(t, r)
		if writeIntentMessageRewriteResponse(t, w, req) {
			return
		}
		hits.Add(1)
		seqs := offeredIntentSeqs(t, req)
		if len(seqs) < 4 {
			http.Error(w, "expected at least four offered captures", http.StatusBadRequest)
			return
		}
		plan := map[string]any{
			"selected_seqs":    seqs,
			"deferred_seqs":    []int64{},
			"subject":          "Atomic four-file group",
			"body":             "Group every offered capture in one commit.",
			"grouping_reason":  "atomicity test: select all four offered seqs",
			"deferred_reasons": []map[string]any{},
		}
		writeIntentPlanResponse(t, w, "call_atomic", plan)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-5.4-mini",
		trustEnv,
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
	startSession(t, ctx, env, repo, "intent-atomic-batch4", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	fullEnv := envWith(env, extra...)

	// Pause-then-write-then-resume so all four creates surface in a single
	// post-resume capture pass. This mirrors how a multi-file edit in a
	// real harness flows: the harness pauses its own activity while the
	// editor writes the burst, then the daemon catches the whole batch.
	paused := runAcd(t, ctx, fullEnv, "pause", "--repo", repo, "--reason", "atomic batch test", "--yes", "--json")
	if paused.ExitCode != 0 {
		t.Fatalf("acd pause exit=%d\nstdout=%s\nstderr=%s", paused.ExitCode, paused.Stdout, paused.Stderr)
	}
	files := []string{
		"internal/atomic/a.go",
		"internal/atomic/b.go",
		"internal/atomic/c.go",
		"internal/atomic/d.go",
	}
	for _, name := range files {
		writeFile(t, filepath.Join(repo, name), "atomic content for "+name+"\n")
	}

	startCount := commitCount(t, repo)
	resumed := runAcd(t, ctx, fullEnv, "resume", "--repo", repo, "--yes", "--json")
	if resumed.ExitCode != 0 {
		t.Fatalf("acd resume exit=%d\nstdout=%s\nstderr=%s", resumed.ExitCode, resumed.Stdout, resumed.Stderr)
	}
	flushed := runAcd(t, ctx, fullEnv, "flush", "--repo", repo, "--session-id", "intent-atomic-batch4", "--logical", "--json")
	if flushed.ExitCode != 0 {
		t.Fatalf("acd flush exit=%d\nstdout=%s\nstderr=%s", flushed.ExitCode, flushed.Stdout, flushed.Stderr)
	}

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	for _, name := range files {
		waitForEventState(t, dbPath, name, "published", 20*time.Second)
	}

	// Exactly one new commit covering all four files.
	if got, want := commitCount(t, repo), startCount+1; got != want {
		t.Fatalf("commit count=%d want %d (four-file batch must land as one commit)", got, want)
	}

	// All four capture rows must share the same commit_oid.
	oid := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path='internal/atomic/a.go' AND state='published' ORDER BY seq DESC LIMIT 1")
	if oid == "" {
		t.Fatalf("expected non-empty commit_oid for internal/atomic/a.go")
	}
	for _, name := range files {
		got := sqliteScalar(t, dbPath,
			"SELECT commit_oid FROM capture_events WHERE path="+sqliteQuote(name)+" AND state='published' ORDER BY seq DESC LIMIT 1")
		if got != oid {
			t.Fatalf("commit_oid for %s = %q want %q (all four captures must share one commit)", name, got, oid)
		}
	}

	// decision_records must carry one committed row per original seq for
	// the same commit_oid (this is what the CLI's events command reads
	// when deriving grouped_seqs).
	committed := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM decision_records WHERE commit_oid="+sqliteLiteral(oid)+" AND kind='committed'")
	if committed != "4" {
		t.Fatalf("committed decision rows for grouped commit=%s = %s want 4 (one per original seq, the grouped_seqs basis)",
			oid, committed)
	}

	if subj := headSubject(t, repo); subj != "Atomic four-file group" {
		t.Fatalf("HEAD subject=%q want %q (planner subject must land for grouped commit)", subj, "Atomic four-file group")
	}
	if hits.Load() != 1 {
		t.Fatalf("planner hits=%d want 1 (single offered window for the four creates)", hits.Load())
	}
	plannerErrors := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM decision_records WHERE kind='intent_planner_error'")
	if plannerErrors != "0" {
		t.Fatalf("intent_planner_error decisions=%s want 0 for clean grouped publish", plannerErrors)
	}
	if got := sqliteScalar(t, dbPath, `
SELECT COUNT(*) FROM intent_candidates
WHERE planner_protocol='v2' AND status='published'`); got != "1" {
		t.Fatalf("native v2 published candidates=%s want 1", got)
	}
}

// TestIntentAtomicity_DeferredMiddleSplitsCommit drives an A, B, C three-file
// batch where the planner defers B and selects A+C. The daemon must publish
// A and C as ONE grouped commit and leave B pending — proving the
// at-least-two-commits negative arm: when the planner draws a boundary in
// the middle of the offered window, the daemon honors it and does NOT fold
// the prefix and suffix together. The deferred capture remains in
// planner_state with defer_count >= 1.
func TestIntentAtomicity_RejectsBookendsAcrossDeferredMiddle(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	var hits atomic.Int32
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		req := decodeIntentChatRequest(t, r)
		if writeIntentMessageRewriteResponse(t, w, req) {
			return
		}
		hits.Add(1)
		seqs := offeredIntentSeqs(t, req)
		if len(seqs) < 3 {
			http.Error(w, "expected three offered captures", http.StatusBadRequest)
			return
		}
		// Defer the middle seq, select the bookends. ValidateIntentPlan
		// requires every offered seq to appear in selected or deferred and
		// requires deferred_reasons to cover every deferred seq.
		selected := []int64{seqs[0], seqs[2]}
		deferred := []int64{seqs[1]}
		plan := map[string]any{
			"selected_seqs":    selected,
			"deferred_seqs":    deferred,
			"subject":          "Bookends only",
			"body":             "Defer middle to prove no prefix/suffix coalesce.",
			"grouping_reason":  "split: select first and third, defer middle",
			"deferred_reasons": buildDeferredReasons(deferred),
		}
		writeIntentPlanResponse(t, w, "call_split", plan)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		// Hold off forced-aging so the deferred middle does NOT immediately
		// publish on the next tick — we want it to remain pending so the
		// "at least two commits" contract is observable as a delta in
		// commit_count after a single replay pass.
		"ACD_INTENT_DEFER_LIMIT=5",
		"ACD_INTENT_MAX_PENDING_AGE=5m",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-5.4-mini",
		trustEnv,
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
	startSession(t, ctx, env, repo, "intent-split-middle", "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	fullEnv := envWith(env, extra...)

	// Pause-then-write-then-resume so all three creates surface in one
	// capture pass and the planner sees three offered seqs together.
	paused := runAcd(t, ctx, fullEnv, "pause", "--repo", repo, "--reason", "split test", "--yes", "--json")
	if paused.ExitCode != 0 {
		t.Fatalf("acd pause exit=%d\nstdout=%s\nstderr=%s", paused.ExitCode, paused.Stdout, paused.Stderr)
	}
	for _, name := range []string{"split-a.txt", "split-b.txt", "split-c.txt"} {
		writeFile(t, filepath.Join(repo, name), name+" content\n")
	}

	startCount := commitCount(t, repo)
	resumed := runAcd(t, ctx, fullEnv, "resume", "--repo", repo, "--yes", "--json")
	if resumed.ExitCode != 0 {
		t.Fatalf("acd resume exit=%d\nstdout=%s\nstderr=%s", resumed.ExitCode, resumed.Stdout, resumed.Stderr)
	}
	flushed := runAcd(t, ctx, fullEnv, "flush", "--repo", repo, "--session-id", "intent-split-middle", "--logical", "--json")
	if flushed.ExitCode != 0 {
		t.Fatalf("acd flush exit=%d\nstdout=%s\nstderr=%s", flushed.ExitCode, flushed.Stdout, flushed.Stderr)
	}

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	waitForEventState(t, dbPath, "split-a.txt", "published", 20*time.Second)

	// The bookends have no dependency evidence once their middle capture is
	// deferred. Intent v2 rejects that disconnected group and Fast publishes
	// only its smallest safe component.
	if got, want := commitCount(t, repo), startCount+1; got != want {
		t.Fatalf("commit count=%d want %d (one smallest safe component)",
			got, want)
	}

	// Neither deferred B nor disconnected C may be folded into A.
	oidA := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path='split-a.txt' AND state='published'")
	oidC := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path='split-c.txt' ORDER BY seq DESC LIMIT 1")
	if oidA == "" || oidC != "" {
		t.Fatalf("bookend commit_oids A=%q C=%q (disconnected C must remain pending)", oidA, oidC)
	}
	stateB := sqliteScalar(t, dbPath,
		"SELECT state FROM capture_events WHERE path='split-b.txt' ORDER BY seq DESC LIMIT 1")
	if stateB != "pending" {
		t.Fatalf("split-b.txt state=%q want pending (planner deferred it; daemon must NOT coalesce across)", stateB)
	}
	waiting := sqliteScalar(t, dbPath, `
SELECT COUNT(*)
FROM intent_candidates candidate
JOIN intent_candidate_events member ON member.candidate_id=candidate.id
JOIN capture_events capture ON capture.seq=member.event_seq
WHERE capture.path IN ('split-b.txt','split-c.txt')
  AND candidate.status='waiting'`)
	if waiting != "2" {
		t.Fatalf("waiting v2 candidates=%s want 2 for deferred B and disconnected C",
			waiting)
	}

	if hits.Load() != 3 {
		t.Fatalf("planner hits=%d want 3 (initial plan plus two configured corrections)", hits.Load())
	}
}

func TestIntentAtomicity_PartitionWindowSplitsIndependentIntents(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required")
	}
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	samePath := filepath.Join(repo, "same.go")
	writeFile(t, samePath, "package main\n\nfunc alpha() string { return \"a0\" }\n\nfunc beta() string { return \"b0\" }\n")
	gitCommitAll(t, repo, "seed same.go", "same.go")

	var hits atomic.Int32
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		req := decodeIntentChatRequest(t, r)
		if writeIntentMessageRewriteResponse(t, w, req) {
			return
		}
		hits.Add(1)
		captures := offeredIntentCaptures(t, req)
		if len(captures) != 5 {
			http.Error(w, "expected five offered captures", http.StatusBadRequest)
			return
		}
		var sameSeqs []int64
		byPath := map[string]int64{}
		for _, capture := range captures {
			if capture.Path == "same.go" {
				sameSeqs = append(sameSeqs, capture.Seq)
				continue
			}
			byPath[capture.Path] = capture.Seq
		}
		if len(sameSeqs) != 2 || byPath["feature/api.go"] == 0 ||
			byPath["feature/ui.go"] == 0 || byPath["unrelated-note.txt"] == 0 {
			http.Error(w, "unexpected offered paths", http.StatusBadRequest)
			return
		}
		commitGroups := []map[string]any{
			{
				"selected_seqs":   sameSeqs,
				"subject":         "Update same-file helpers",
				"body":            "- Preserve the dependent same-file capture chain.",
				"grouping_reason": "same-path hard dependency keeps both captures together",
			},
			{
				"selected_seqs":   []int64{byPath["feature/api.go"], byPath["feature/ui.go"]},
				"subject":         "Add related feature files",
				"body":            "- Group related API and UI notes together.",
				"grouping_reason": "feature API and UI edits share one intent",
			},
			{
				"selected_seqs":   []int64{byPath["unrelated-note.txt"]},
				"subject":         "Add unrelated note",
				"body":            "- Keep the unrelated note separately revertable.",
				"grouping_reason": "unrelated note should not join feature work",
			},
		}
		selected := []int64{
			sameSeqs[0],
			sameSeqs[1],
			byPath["feature/api.go"],
			byPath["feature/ui.go"],
			byPath["unrelated-note.txt"],
		}
		plan := map[string]any{
			"selected_seqs":    selected,
			"deferred_seqs":    []int64{},
			"subject":          "Partition intent window",
			"body":             "- Split one planner window into atomic groups.",
			"grouping_reason":  "partition close-together edits by intent",
			"commit_groups":    commitGroups,
			"deferred_reasons": []map[string]any{},
		}
		writeIntentPlanResponse(t, w, "call_partition_window", plan)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	extra := []string{
		"ACD_COMMIT_STRATEGY=intent",
		"ACD_INTENT_WINDOW=10",
		"ACD_INTENT_MIN_PENDING=5",
		"ACD_INTENT_SETTLE_WINDOW=0",
		"ACD_INTENT_MAX_PENDING_AGE=1h",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL=" + server.URL,
		"ACD_AI_API_KEY=test-key",
		"ACD_AI_MODEL=gpt-5.4-mini",
		trustEnv,
	}
	extra = activateIntentV2Runtime(t, repo, extra...)
	sessionID := "intent-partition-window"
	startSession(t, ctx, env, repo, sessionID, "shell", extra...)
	waitMode(t, repo, "running", 5*time.Second)
	fullEnv := envWith(env, extra...)
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")

	startCount := commitCount(t, repo)
	steps := []struct {
		path string
		body string
		want string
	}{
		{"same.go", "package main\n\nfunc alpha() string { return \"a1\" }\n\nfunc beta() string { return \"b0\" }\n", "1"},
		{"same.go", "package main\n\nfunc alpha() string { return \"a1\" }\n\nfunc beta() string { return \"b1\" }\n", "2"},
		{"feature/api.go", "package feature\n\nfunc API() {}\n", "3"},
		{"feature/ui.go", "package feature\n\nfunc UI() {}\n", "4"},
		{"unrelated-note.txt", "independent note\n", ""},
	}
	for _, step := range steps {
		writeFile(t, filepath.Join(repo, step.path), step.body)
		wakeSession(t, ctx, fullEnv, repo, sessionID)
		if step.want == "" {
			continue
		}
		waitFor(t, "pending captures="+step.want, 10*time.Second, func() bool {
			return sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM capture_events WHERE state='pending'") == step.want
		})
	}

	waitForEventState(t, dbPath, "same.go", "published", 25*time.Second)
	for _, path := range []string{"feature/api.go", "feature/ui.go", "unrelated-note.txt"} {
		waitForEventState(t, dbPath, path, "published", 25*time.Second)
	}
	waitFor(t, "two same.go captures published", 10*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM capture_events WHERE path='same.go' AND state='published'") == "2"
	})

	if hits.Load() != 1 {
		t.Fatalf("planner hits=%d want 1 for one five-capture window", hits.Load())
	}
	if got, want := commitCount(t, repo), startCount+3; got != want {
		t.Fatalf("commit count=%d want %d (one same-file chain, one related group, one unrelated commit)", got, want)
	}

	sameDistinct := sqliteScalar(t, dbPath,
		"SELECT COUNT(DISTINCT commit_oid) FROM capture_events WHERE path='same.go' AND state='published'")
	if sameDistinct != "1" {
		t.Fatalf("same.go distinct commit_oids=%s want 1 (dependent same-file edits stay atomic)", sameDistinct)
	}
	featureAPI := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path='feature/api.go' AND state='published'")
	featureUI := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path='feature/ui.go' AND state='published'")
	unrelated := sqliteScalar(t, dbPath,
		"SELECT commit_oid FROM capture_events WHERE path='unrelated-note.txt' AND state='published'")
	if featureAPI == "" || featureAPI != featureUI {
		t.Fatalf("feature commit_oids api=%q ui=%q want one related commit", featureAPI, featureUI)
	}
	if unrelated == "" || unrelated == featureAPI {
		t.Fatalf("unrelated commit_oid=%q feature_oid=%q want separate commits", unrelated, featureAPI)
	}

	if got := sqliteScalar(t, dbPath, `
SELECT COUNT(*) FROM intent_candidates
WHERE planner_protocol='v2'`); got != "3" {
		t.Fatalf("native v2 durable candidates=%s want 3", got)
	}

	status := runAcd(t, ctx, fullEnv, "status", "--repo", repo, "--json")
	if status.ExitCode != 0 {
		t.Fatalf("acd status exit=%d\nstdout=%s\nstderr=%s", status.ExitCode, status.Stdout, status.Stderr)
	}
	if !strings.Contains(status.Stdout, `"latest_planner_protocol": "v2"`) {
		t.Fatalf("status JSON missing native v2 candidate protocol:\n%s",
			status.Stdout)
	}
	events := runAcd(t, ctx, fullEnv, "events", "--repo", repo, "--json", "--limit", "20")
	if events.ExitCode != 0 {
		t.Fatalf("acd events exit=%d\nstdout=%s\nstderr=%s", events.ExitCode, events.Stdout, events.Stderr)
	}
	if !strings.Contains(events.Stdout, `"candidate_id"`) {
		t.Fatalf("events JSON missing candidate decisions:\n%s", events.Stdout)
	}
}

func loadLastPlannerWindowRow(t *testing.T, dbPath string) plannerWindowRow {
	t.Helper()
	raw := sqliteScalar(t, dbPath, `
SELECT json_object(
  'offered_seqs', json(offered_seqs),
  'visible_original_seqs', json(visible_original_seqs),
  'hidden_seqs', json(hidden_seqs),
  'selected_groups', json(selected_groups),
  'deferred_seqs', json(deferred_seqs)
)
FROM intent_planner_windows
ORDER BY id DESC
LIMIT 1`)
	if raw == "" {
		t.Fatalf("missing intent_planner_windows row")
	}
	var row plannerWindowRow
	if err := json.Unmarshal([]byte(raw), &row); err != nil {
		t.Fatalf("decode planner window row: %v\n%s", err, raw)
	}
	return row
}
