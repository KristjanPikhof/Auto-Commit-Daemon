package cli

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/templates"
)

func TestDoctor_Human_HasSectionHeaders(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 99, Mode: "running", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon: %v", err)
	}
	registerRepo(t, roots, repo, db, "claude-code")
	_ = d.Close()

	var out bytes.Buffer
	if err := runDoctor(ctx, &out, false, "", false); err != nil {
		t.Fatalf("runDoctor: %v", err)
	}
	body := out.String()
	for _, want := range []string{"acd doctor", "Registry", "Sensitive globs", "Safe-ignore patterns", "Repos"} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected %q in human output:\n%s", want, body)
		}
	}
}

func TestDoctor_JSON_Shape(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 42, Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	registerRepo(t, roots, repo, db, "codex")
	_ = d.Close()

	var out bytes.Buffer
	if err := runDoctor(ctx, &out, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var got doctorReport
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if got.ACDVersion == "" {
		t.Fatalf("missing ACDVersion in %+v", got)
	}
	if got.RegistryRepoCount != 1 || len(got.Repos) != 1 {
		t.Fatalf("expected 1 repo, got %+v", got)
	}
	if got.Repos[0].DaemonPID != 42 {
		t.Fatalf("repo pid mismatch: %+v", got.Repos[0])
	}
	if len(got.SensitiveGlobsActive) == 0 {
		t.Fatalf("sensitive globs should be non-empty by default")
	}
	if len(got.SafeIgnoreActive) == 0 {
		t.Fatalf("safe-ignore patterns should be non-empty by default")
	}
}

func TestDoctor_Bundle_LayoutMatchesSpec(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := state.RegisterClient(ctx, d, state.Client{
		SessionID: "sess-1", Harness: "claude-code",
	}); err != nil {
		t.Fatalf("client: %v", err)
	}
	if err := state.MetaSet(ctx, d, "fsnotify.mode", "fsnotify"); err != nil {
		t.Fatalf("meta: %v", err)
	}
	if err := state.MetaSet(ctx, d, "fsnotify.watch_count", "42"); err != nil {
		t.Fatalf("meta: %v", err)
	}
	registerRepo(t, roots, repo, db, "claude-code")

	// Seed a fake daemon.log so log-tail population is exercised.
	logPath := roots.RepoLogPath(filepath.Base(filepath.Dir(db)))
	_ = os.MkdirAll(filepath.Dir(roots.RepoLogPath("placeholder")), 0o700)
	// Compute the actual repo_hash via the helper used by the registry.
	repoHash := ""
	{
		// Discover by reading the registry record we just wrote.
		regBody, _ := os.ReadFile(roots.RegistryPath())
		var rg struct {
			Repos []struct {
				RepoHash string `json:"repo_hash"`
			} `json:"repos"`
		}
		_ = json.Unmarshal(regBody, &rg)
		if len(rg.Repos) > 0 {
			repoHash = rg.Repos[0].RepoHash
		}
	}
	if repoHash != "" {
		logPath = roots.RepoLogPath(repoHash)
		_ = os.MkdirAll(filepath.Dir(logPath), 0o700)
		_ = os.WriteFile(logPath, []byte("hello from "+os.Getenv("HOME")+"\nline two\n"), 0o600)
	}
	_ = d.Close()

	outDir := t.TempDir()
	var out bytes.Buffer
	if err := runDoctor(ctx, &out, true, outDir, true); err != nil {
		t.Fatalf("runDoctor bundle: %v", err)
	}
	var br bundleResult
	if err := json.Unmarshal(out.Bytes(), &br); err != nil {
		t.Fatalf("unmarshal bundle result: %v", err)
	}
	if br.Path == "" || br.SizeBytes == 0 || br.FilesCount == 0 {
		t.Fatalf("expected non-empty bundle, got %+v", br)
	}
	r, err := zip.OpenReader(br.Path)
	if err != nil {
		t.Fatalf("open zip: %v", err)
	}
	defer r.Close()
	have := map[string]bool{}
	for _, f := range r.File {
		have[f.Name] = true
	}
	wantBase := []string{
		"manifest.json",
		"acd-version.txt",
		"git-version.txt",
		"uname.txt",
		"ulimit.txt",
		"registry.json",
	}
	for _, name := range wantBase {
		if !have[name] {
			t.Fatalf("zip missing top-level file %s — got %v", name, sortedKeys(have))
		}
	}
	if repoHash != "" {
		base := "repos/" + repoHash + "/"
		for _, sub := range []string{
			"state-schema.txt",
			"daemon-state.json",
			"daemon-clients.json",
			"daemon-meta.json",
			"sensitive-globs.txt",
			"safe-ignore-patterns.txt",
			"fsnotify-stats.json",
			"daemon-tail.log",
		} {
			if !have[base+sub] {
				t.Fatalf("zip missing %s — got %v", base+sub, sortedKeys(have))
			}
		}
		// Verify daemon-tail.log content was sanitized: $HOME → ~.
		body := readZipFile(t, r, base+"daemon-tail.log")
		if strings.Contains(body, os.Getenv("HOME")) {
			t.Fatalf("daemon-tail.log retains HOME prefix:\n%s", body)
		}
		if !strings.Contains(body, "hello from") {
			t.Fatalf("daemon-tail.log missing seeded content:\n%s", body)
		}
	}

	// Verify manifest.json sanitized RegistryPath.
	manifest := readZipFile(t, r, "manifest.json")
	if strings.Contains(manifest, os.Getenv("HOME")) {
		t.Fatalf("manifest retains HOME prefix:\n%s", manifest)
	}
}

// TestDoctor_JSON_FsnotifyFields verifies that all four fsnotify daemon_meta
// keys (mode, watch_count, dropped_events, fallback_reason) surface correctly
// in `acd doctor --json` output. Runs twice: once with mode=fsnotify (no
// fallback_reason) and once with mode=poll + fallback_reason=watch_budget_exceeded.
func TestDoctor_JSON_FsnotifyFields(t *testing.T) {
	tests := []struct {
		name           string
		mode           string
		watchCount     string
		dropped        string
		fallbackReason string
	}{
		{
			name:       "fsnotify_mode",
			mode:       "fsnotify",
			watchCount: "17",
			dropped:    "0",
		},
		{
			name:           "poll_fallback",
			mode:           "poll",
			watchCount:     "0",
			dropped:        "3",
			fallbackReason: "watch_budget_exceeded",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			roots := withIsolatedHome(t)
			ctx := context.Background()

			repo, db, d := makeRepoStateDB(t)
			if err := state.SaveDaemonState(ctx, d, state.DaemonState{
				PID: 55, Mode: "running", HeartbeatTS: nowFloat(),
			}); err != nil {
				t.Fatalf("save daemon state: %v", err)
			}
			if err := state.MetaSet(ctx, d, "fsnotify.mode", tc.mode); err != nil {
				t.Fatalf("meta set mode: %v", err)
			}
			if err := state.MetaSet(ctx, d, "fsnotify.watch_count", tc.watchCount); err != nil {
				t.Fatalf("meta set watch_count: %v", err)
			}
			if err := state.MetaSet(ctx, d, "fsnotify.dropped_events", tc.dropped); err != nil {
				t.Fatalf("meta set dropped_events: %v", err)
			}
			if tc.fallbackReason != "" {
				if err := state.MetaSet(ctx, d, "fsnotify.fallback_reason", tc.fallbackReason); err != nil {
					t.Fatalf("meta set fallback_reason: %v", err)
				}
			}
			registerRepo(t, roots, repo, db, "claude-code")
			_ = d.Close()

			var out bytes.Buffer
			if err := runDoctor(ctx, &out, false, "", true); err != nil {
				t.Fatalf("runDoctor: %v", err)
			}
			var rep doctorReport
			if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
				t.Fatalf("unmarshal: %v\n%s", err, out.String())
			}
			if len(rep.Repos) != 1 {
				t.Fatalf("expected 1 repo, got %d", len(rep.Repos))
			}
			rr := rep.Repos[0]
			if rr.FsnotifyMode != tc.mode {
				t.Errorf("FsnotifyMode=%q want %q", rr.FsnotifyMode, tc.mode)
			}
			if rr.FsnotifyFallbackReason != tc.fallbackReason {
				t.Errorf("FsnotifyFallbackReason=%q want %q", rr.FsnotifyFallbackReason, tc.fallbackReason)
			}
			// watch_count is only non-zero for fsnotify mode, but the field
			// must be readable (int) in both cases.
			if tc.mode == "fsnotify" && rr.FsnotifyWatches == 0 {
				t.Errorf("FsnotifyWatches=0 want %s (parsed)", tc.watchCount)
			}
			if tc.dropped != "0" && rr.FsnotifyDropped == 0 {
				t.Errorf("FsnotifyDropped=0 want >0 (dropped=%s)", tc.dropped)
			}
		})
	}
}

// TestDoctor_Human_FsnotifySection verifies the human-readable output
// includes a "watcher" line per repo, and includes fallback_reason only when
// mode=poll.
func TestDoctor_Human_FsnotifySection(t *testing.T) {
	tests := []struct {
		name           string
		mode           string
		fallbackReason string
		wantFallback   bool
	}{
		{
			name: "fsnotify_mode_no_fallback",
			mode: "fsnotify",
		},
		{
			name:           "poll_mode_with_fallback",
			mode:           "poll",
			fallbackReason: "watch_budget_exceeded",
			wantFallback:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			roots := withIsolatedHome(t)
			ctx := context.Background()

			repo, db, d := makeRepoStateDB(t)
			if err := state.SaveDaemonState(ctx, d, state.DaemonState{
				PID: 12, Mode: "running", HeartbeatTS: nowFloat(),
			}); err != nil {
				t.Fatalf("save daemon state: %v", err)
			}
			if err := state.MetaSet(ctx, d, "fsnotify.mode", tc.mode); err != nil {
				t.Fatalf("meta mode: %v", err)
			}
			if err := state.MetaSet(ctx, d, "fsnotify.watch_count", "5"); err != nil {
				t.Fatalf("meta watch_count: %v", err)
			}
			if err := state.MetaSet(ctx, d, "fsnotify.dropped_events", "0"); err != nil {
				t.Fatalf("meta dropped_events: %v", err)
			}
			if tc.fallbackReason != "" {
				if err := state.MetaSet(ctx, d, "fsnotify.fallback_reason", tc.fallbackReason); err != nil {
					t.Fatalf("meta fallback_reason: %v", err)
				}
			}
			registerRepo(t, roots, repo, db, "claude-code")
			_ = d.Close()

			var out bytes.Buffer
			if err := runDoctor(ctx, &out, false, "", false); err != nil {
				t.Fatalf("runDoctor: %v", err)
			}
			body := out.String()

			// Human output must contain the watcher line with mode.
			wantMode := "mode=" + tc.mode
			if !strings.Contains(body, wantMode) {
				t.Errorf("human output missing %q:\n%s", wantMode, body)
			}
			if tc.wantFallback {
				wantFB := "fallback=" + tc.fallbackReason
				if !strings.Contains(body, wantFB) {
					t.Errorf("human output missing %q:\n%s", wantFB, body)
				}
			} else {
				if strings.Contains(body, "fallback=") {
					t.Errorf("human output unexpectedly contains fallback= when mode=fsnotify:\n%s", body)
				}
			}
		})
	}
}

func TestDoctor_Bundle_TwoRunsDistinctZips(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, db, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db, "claude-code")
	_ = d.Close()
	outDir := t.TempDir()

	var out1, out2 bytes.Buffer
	if err := runDoctor(ctx, &out1, true, outDir, true); err != nil {
		t.Fatalf("first bundle: %v", err)
	}
	// Sleep ~1.1s so the second-resolution timestamp differs.
	time.Sleep(1100 * time.Millisecond)
	if err := runDoctor(ctx, &out2, true, outDir, true); err != nil {
		t.Fatalf("second bundle: %v", err)
	}
	var b1, b2 bundleResult
	_ = json.Unmarshal(out1.Bytes(), &b1)
	_ = json.Unmarshal(out2.Bytes(), &b2)
	if b1.Path == b2.Path {
		t.Fatalf("expected distinct paths, got both %s", b1.Path)
	}
	for _, p := range []string{b1.Path, b2.Path} {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		if st.Size() == 0 {
			t.Fatalf("zero-byte zip %s", p)
		}
	}
}

// TestDoctor_BlockedConflictSurfaced verifies that doctor exposes pending
// + blocked_conflict counts and the most recent blocked event's path /
// timestamp / error in both JSON and human output. Mirrors what `acd list`
// and `acd status` report so all three commands agree on the same repo.
func TestDoctor_BlockedConflictSurfaced(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 77, Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	// One pending event.
	if _, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "live.go",
		Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "live.go", Fidelity: "exact"}}); err != nil {
		t.Fatalf("append pending: %v", err)
	}
	// One blocked-conflict event.
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "stuck.go",
		Fidelity: "rescan",
	}, []state.CaptureOp{{Op: "modify", Path: "stuck.go", Fidelity: "rescan"}})
	if err != nil {
		t.Fatalf("append blocker: %v", err)
	}
	if err := state.MarkEventBlocked(ctx, d, seq, "before-state mismatch", nowFloat(),
		sql.NullString{String: "refs/heads/main", Valid: true},
		sql.NullInt64{Int64: 1, Valid: true},
		sql.NullString{String: "deadbeef", Valid: true},
	); err != nil {
		t.Fatalf("MarkEventBlocked: %v", err)
	}
	registerRepo(t, roots, repo, db, "claude-code")
	_ = d.Close()

	// JSON shape: counts + last replay conflict info populated.
	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if len(rep.Repos) != 1 {
		t.Fatalf("expected 1 repo, got %d", len(rep.Repos))
	}
	rr := rep.Repos[0]
	if rr.PendingEvents != 1 {
		t.Errorf("PendingEvents=%d want 1", rr.PendingEvents)
	}
	if rr.BlockedConflicts != 1 {
		t.Errorf("BlockedConflicts=%d want 1", rr.BlockedConflicts)
	}
	if rr.LastReplayConflictPath != "stuck.go" {
		t.Errorf("LastReplayConflictPath=%q want stuck.go", rr.LastReplayConflictPath)
	}
	if rr.LastReplayConflictErr == "" {
		t.Errorf("LastReplayConflictErr empty, want non-empty error message")
	}
	if rr.LastReplayConflictTS == 0 {
		t.Errorf("LastReplayConflictTS=0, want non-zero")
	}

	// Human output renders pending and blocked lines, plus last conflict.
	var humanOut bytes.Buffer
	if err := runDoctor(ctx, &humanOut, false, "", false); err != nil {
		t.Fatalf("runDoctor human: %v", err)
	}
	body := humanOut.String()
	for _, want := range []string{"pending    : 1", "blocked    : 1", "stuck.go", "last conflict"} {
		if !strings.Contains(body, want) {
			t.Errorf("doctor human output missing %q in:\n%s", want, body)
		}
	}
}

func TestDoctor_IntentBatchWaitAddsOperationalNotes(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	if err := state.RegisterClient(ctx, d, state.Client{
		SessionID: "intent-session", Harness: "codex", LastSeenTS: nowFloat(),
	}); err != nil {
		t.Fatalf("register client: %v", err)
	}
	if err := state.MetaSet(ctx, d, "commit.strategy", "intent"); err != nil {
		t.Fatalf("set commit.strategy: %v", err)
	}
	appendIntentPendingEvent(t, ctx, d, "sparse.go", nowFloat()-30)
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if len(rep.Repos) != 1 {
		t.Fatalf("repos=%d want 1", len(rep.Repos))
	}
	rr := rep.Repos[0]
	if !rr.IntentStrategy.BatchWaitActive ||
		rr.IntentStrategy.MinPending != 10 ||
		rr.IntentStrategy.MaxPendingAgeSeconds != 300 {
		t.Fatalf("intent strategy = %+v, want defaulted active batch wait", rr.IntentStrategy)
	}
	notes := strings.Join(rr.Notes, "\n")
	for _, want := range []string{
		"intent replay is waiting",
		"acd flush --logical --repo " + repo + " --session-id intent-session",
		"explicit flushes bypass intent batch wait",
		"registered active session id",
		"lower ACD_INTENT_MIN_PENDING",
		"ACD_COMMIT_STRATEGY=event",
	} {
		if !strings.Contains(notes, want) {
			t.Fatalf("doctor notes missing %q: %v", want, rr.Notes)
		}
	}
	if strings.Contains(notes, "run acd flush --logical for the active session") {
		t.Fatalf("doctor notes still include bare logical flush hint: %v", rr.Notes)
	}

	var humanOut bytes.Buffer
	if err := runDoctor(ctx, &humanOut, false, "", false); err != nil {
		t.Fatalf("runDoctor human: %v", err)
	}
	if !strings.Contains(humanOut.String(), "batch wait : pending=1 min_pending=10") {
		t.Fatalf("doctor human missing batch wait line:\n%s", humanOut.String())
	}
}

func TestDoctor_FailedBarrierSurfaced(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 77, Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "bad.go",
		Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "bad.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append failed event: %v", err)
	}
	if err := state.MarkEventPublished(ctx, d, seq, state.EventStateFailed,
		sql.NullString{}, sql.NullString{String: "commit-tree failed", Valid: true},
		sql.NullString{}, nowFloat()); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "later.go",
		Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "later.go", Fidelity: "exact"}}); err != nil {
		t.Fatalf("append pending successor: %v", err)
	}
	registerRepo(t, roots, repo, dbPath, "claude-code")
	_ = d.Close()

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal doctor: %v\n%s", err, jsonOut.String())
	}
	if len(rep.Repos) != 1 {
		t.Fatalf("repos len=%d want 1", len(rep.Repos))
	}
	rr := rep.Repos[0]
	if rr.FailedEvents != 1 || rr.FailedBlockingPending != 1 || rr.LastReplayFailurePath != "bad.go" {
		t.Fatalf("failed doctor fields = %+v, want failed blocker bad.go", rr)
	}

	var humanOut bytes.Buffer
	if err := runDoctor(ctx, &humanOut, false, "", false); err != nil {
		t.Fatalf("runDoctor human: %v", err)
	}
	for _, want := range []string{"failed     : 1", "failed blockers : 1", "acd fix --dry-run", "last failure : bad.go"} {
		if !strings.Contains(humanOut.String(), want) {
			t.Fatalf("doctor human missing %q in:\n%s", want, humanOut.String())
		}
	}
}

func TestDoctorBundleReadsPreDecisionLedgerDBReadOnly(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, db := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")
	preparePreDecisionLedgerDB(t, db, dbPath)
	before := mustSHA256(t, dbPath)
	versionBefore := readUserVersionReadOnly(t, dbPath)

	var out bytes.Buffer
	if err := runDoctor(ctx, &out, true, t.TempDir(), true); err != nil {
		t.Fatalf("runDoctor bundle: %v\n%s", err, out.String())
	}
	var bundle bundleResult
	if err := json.Unmarshal(out.Bytes(), &bundle); err != nil {
		t.Fatalf("unmarshal bundle: %v\n%s", err, out.String())
	}
	if bundle.Path == "" || bundle.FilesCount == 0 {
		t.Fatalf("bundle result not populated: %+v", bundle)
	}
	if after := mustSHA256(t, dbPath); after != before {
		t.Fatalf("state.db checksum changed: before=%s after=%s", before, after)
	}
	if got := readUserVersionReadOnly(t, dbPath); got != versionBefore {
		t.Fatalf("user_version changed: before=%d after=%d", versionBefore, got)
	}
}

func TestDoctor_InstallReportsHarnessMarkersAndCodexHooksJSON(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	home := os.Getenv("HOME")
	if err := os.MkdirAll(filepath.Join(home, ".claude"), 0o700); err != nil {
		t.Fatalf("mkdir claude: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".claude", "settings.json"), []byte(`{"_acd_managed": true}`), 0o600); err != nil {
		t.Fatalf("write claude settings: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatalf("mkdir codex: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "hooks.json"), []byte(`{"_acd_managed": true,"hooks":{}}`), 0o600); err != nil {
		t.Fatalf("write codex hooks.json: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(home, ".cursor"), 0o700); err != nil {
		t.Fatalf("mkdir cursor: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".cursor", "hooks.json"), []byte(`{"_acd_managed": true,"hooks":{}}`), 0o600); err != nil {
		t.Fatalf("write cursor hooks.json: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	claude := findDoctorHarness(t, rep, "claude-code")
	if !claude.Installed || !claude.MarkerFound || !claude.ConfigReadable {
		t.Fatalf("claude-code install report wrong: %+v", claude)
	}
	codex := findDoctorHarness(t, rep, "codex")
	if !codex.Installed || !codex.MarkerFound {
		t.Fatalf("codex hooks.json install report wrong: %+v", codex)
	}
	if !strings.HasSuffix(codex.ConfigPath, "/.codex/hooks.json") {
		t.Fatalf("codex ConfigPath=%q, want ~/.codex/hooks.json", codex.ConfigPath)
	}
	if got := strings.Join(codex.Notes, "\n"); strings.Contains(got, "legacy") {
		t.Fatalf("codex should not show legacy warning when only hooks.json exists: %+v", codex)
	}
	cursor := findDoctorHarness(t, rep, "cursor")
	if !cursor.Installed || !cursor.MarkerFound {
		t.Fatalf("cursor hooks.json install report wrong: %+v", cursor)
	}
	if !strings.HasSuffix(cursor.ConfigPath, "/.cursor/hooks.json") {
		t.Fatalf("cursor ConfigPath=%q, want ~/.cursor/hooks.json", cursor.ConfigPath)
	}

	var humanOut bytes.Buffer
	if err := runDoctor(ctx, &humanOut, false, "", false); err != nil {
		t.Fatalf("runDoctor human: %v", err)
	}
	body := humanOut.String()
	for _, want := range []string{"Install", "claude-code : yes", "codex       : yes", "cursor      : yes"} {
		if !strings.Contains(body, want) {
			t.Fatalf("human doctor missing %q:\n%s", want, body)
		}
	}
}

func TestDoctor_CodexShadowWarningWhenLegacyTOMLAlongsideHooksJSON(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	home := os.Getenv("HOME")
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatalf("mkdir codex: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "hooks.json"), []byte(`{"_acd_managed": true,"hooks":{}}`), 0o600); err != nil {
		t.Fatalf("write codex hooks.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "config.toml"), []byte("# acd-managed: true\n[features]\n"), 0o600); err != nil {
		t.Fatalf("write legacy config.toml: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	codex := findDoctorHarness(t, rep, "codex")
	if !codex.Installed {
		t.Fatalf("codex should be installed: %+v", codex)
	}
	got := strings.Join(codex.Notes, "\n")
	if !strings.Contains(got, "Codex merges all hook sources and will fire each event twice") {
		t.Fatalf("codex duplicate-hook warning missing: %+v", codex)
	}
}

func TestDoctor_CodexRepoLocalInstallDetected(t *testing.T) {
	_ = withIsolatedHome(t)
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	repo := t.TempDir()
	if err := os.Mkdir(filepath.Join(repo, ".git"), 0o700); err != nil {
		t.Fatalf("mkdir .git: %v", err)
	}
	hooksPath := filepath.Join(repo, ".codex", "hooks.json")
	if err := os.MkdirAll(filepath.Dir(hooksPath), 0o700); err != nil {
		t.Fatalf("mkdir .codex: %v", err)
	}
	if err := os.WriteFile(hooksPath, []byte(`{"_acd_managed": true,"hooks":{}}`), 0o600); err != nil {
		t.Fatalf("write repo-local hooks.json: %v", err)
	}
	chdirForTest(t, repo)

	rep, err := collectDoctorReport(context.Background())
	if err != nil {
		t.Fatalf("collectDoctorReport: %v", err)
	}
	codex := findDoctorHarness(t, rep, "codex")
	if !codex.Installed {
		t.Fatalf("codex should be installed via repo-local hooks.json: %+v", codex)
	}
	wantHooksPath := canonicalCLIResolverTestPath(t, hooksPath)
	if codex.MatchedPath != wantHooksPath {
		t.Fatalf("MatchedPath=%q, want repo-local %q", codex.MatchedPath, wantHooksPath)
	}
	if got := strings.Join(codex.Notes, "\n"); !strings.Contains(got, "alternate config path") {
		t.Fatalf("codex alternate-path note missing for repo-local install: %+v", codex)
	}
}

func TestDoctor_CodexInstalledWhenPrimaryHooksJSONUnmanagedButLegacyTOMLManaged(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	home := os.Getenv("HOME")
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatalf("mkdir codex: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "hooks.json"), []byte(`{"hooks":{}}`), 0o600); err != nil {
		t.Fatalf("write unmanaged hooks.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "config.toml"), []byte("# acd-managed: true\n[features]\n"), 0o600); err != nil {
		t.Fatalf("write legacy config.toml: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	codex := findDoctorHarness(t, rep, "codex")
	if !codex.Installed {
		t.Fatalf("codex should be installed from alternate config path: %+v", codex)
	}
	if codex.MarkerFound {
		t.Fatalf("primary unmanaged hooks.json should not set MarkerFound: %+v", codex)
	}
	if got := strings.Join(codex.Notes, "\n"); !strings.Contains(got, "alternate config path") {
		t.Fatalf("codex alternate-path note missing: %+v", codex)
	}
}

func TestDoctor_CodexShadowWarningWithConfigHomeLegacyTOML(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	home := os.Getenv("HOME")
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatalf("mkdir codex: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(home, ".config", "codex"), 0o700); err != nil {
		t.Fatalf("mkdir config codex: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "hooks.json"), []byte(`{"_acd_managed": true,"hooks":{}}`), 0o600); err != nil {
		t.Fatalf("write codex hooks.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".config", "codex", "config.toml"), []byte("# acd-managed: true\n[features]\n"), 0o600); err != nil {
		t.Fatalf("write legacy config.toml: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	codex := findDoctorHarness(t, rep, "codex")
	if got := strings.Join(codex.Notes, "\n"); !strings.Contains(got, "Codex merges all hook sources and will fire each event twice") {
		t.Fatalf("codex duplicate-hook warning missing for ~/.config/codex/config.toml: %+v", codex)
	}
}

func TestDoctor_NoCodexShadowWhenOnlyOneFile(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	home := os.Getenv("HOME")
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatalf("mkdir codex: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "config.toml"), []byte("# acd-managed: true\n"), 0o600); err != nil {
		t.Fatalf("write legacy config.toml: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	codex := findDoctorHarness(t, rep, "codex")
	if got := strings.Join(codex.Notes, "\n"); strings.Contains(got, "Codex merges all hook sources") {
		t.Fatalf("no duplicate-hook warning expected when only legacy config.toml exists: %+v", codex)
	}
}

func TestDoctor_AIProviderOpenAICompatRequiresAPIKey(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "openai-compat")
	t.Setenv(ai.EnvAPIKey, "")

	var out bytes.Buffer
	if err := runDoctor(ctx, &out, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if rep.AI.Provider != "openai-compat" {
		t.Fatalf("provider=%q want openai-compat", rep.AI.Provider)
	}
	if rep.AI.APIKeySet {
		t.Fatalf("APIKeySet=true, want false")
	}
	if !strings.Contains(strings.Join(rep.AI.Notes, "\n"), ai.EnvAPIKey) {
		t.Fatalf("AI notes missing API key warning: %+v", rep.AI)
	}
}

func TestDoctor_AISubprocessProviderChecksPATH(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "subprocess:missing")
	t.Setenv("PATH", t.TempDir())

	var out bytes.Buffer
	if err := runDoctor(ctx, &out, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if rep.AI.ProviderCommand != "acd-provider-missing" {
		t.Fatalf("ProviderCommand=%q want acd-provider-missing", rep.AI.ProviderCommand)
	}
	if rep.AI.ProviderCommandFound {
		t.Fatalf("ProviderCommandFound=true, want false")
	}
	if !strings.Contains(strings.Join(rep.AI.Notes, "\n"), "not found on PATH") {
		t.Fatalf("AI notes missing PATH warning: %+v", rep.AI)
	}
}

func TestDoctor_RepoWarnsOnMultipleDaemonProcesses(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")

	repo, db, d := makeRepoStateDB(t)
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 1001, Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon: %v", err)
	}
	registerRepo(t, roots, repo, db, "claude-code")
	_ = d.Close()

	old := doctorProcessList
	doctorProcessList = func(context.Context) ([]doctorProcess, error) {
		return []doctorProcess{
			{PID: 1001, Command: "acd daemon run --repo " + repo},
			{PID: 1002, Command: "/usr/local/bin/acd daemon run --repo " + repo},
			{PID: 1003, Command: "/usr/local/bin/acd daemon run --repo " + filepath.Dir(repo)},
		}, nil
	}
	t.Cleanup(func() { doctorProcessList = old })

	var out bytes.Buffer
	if err := runDoctor(ctx, &out, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if len(rep.Repos) != 1 {
		t.Fatalf("repos=%d want 1", len(rep.Repos))
	}
	rr := rep.Repos[0]
	if rr.DaemonProcessCount != 2 {
		t.Fatalf("DaemonProcessCount=%d want 2: %+v", rr.DaemonProcessCount, rr)
	}
	if !strings.Contains(strings.Join(rr.Notes, "\n"), "multiple acd daemon processes") {
		t.Fatalf("repo notes missing multiple process warning: %+v", rr.Notes)
	}
}

// TestDoctor_DriftWarningClaudeCodeWakeOnly seeds a Claude Code settings.json
// whose PreToolUse body only calls `acd wake` (missing `acd start`) and
// asserts the doctor report flags the drift with a copy/pasteable
// remediation command.
func TestDoctor_DriftWarningClaudeCodeWakeOnly(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	home := os.Getenv("HOME")
	if err := os.MkdirAll(filepath.Join(home, ".claude"), 0o700); err != nil {
		t.Fatalf("mkdir claude: %v", err)
	}
	// Drifted snippet: PreToolUse only fires `acd wake`, PostToolUse is
	// canonical. Marker is present so the harness counts as installed.
	body := `{
		"_acd_managed": true,
		"hooks": {
			"PreToolUse": [
				{ "matcher": "", "hooks": [
					{ "type": "command", "command": "bash -c 'acd wake --session-id \"$SID\" --repo \"$PWD\"'" }
				]}
			],
			"PostToolUse": [
				{ "matcher": "", "hooks": [
					{ "type": "command", "command": "bash -c 'acd start --harness claude-code --session-id \"$SID\" --repo \"$PWD\"; acd wake --session-id \"$SID\" --repo \"$PWD\"'" }
				]}
			]
		}
	}`
	if err := os.WriteFile(filepath.Join(home, ".claude", "settings.json"), []byte(body), 0o600); err != nil {
		t.Fatalf("write claude settings: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	cc := findDoctorHarness(t, rep, "claude-code")
	if !cc.Installed {
		t.Fatalf("claude-code should be installed: %+v", cc)
	}
	notes := strings.Join(cc.Notes, "\n")
	if !strings.Contains(notes, "installed snippet drift") {
		t.Fatalf("expected drift warning, got notes=%v", cc.Notes)
	}
	if !strings.Contains(notes, "acd setup claude-code --raw") {
		t.Fatalf("drift note missing remediation command, got notes=%v", cc.Notes)
	}
}

// TestDoctor_DriftCleanWhenSnippetMatchesTemplate seeds a fully canonical
// claude-code snippet (both `acd start` and `acd wake` present in every
// active hook) and asserts no drift warning fires.
func TestDoctor_DriftCleanWhenSnippetMatchesTemplate(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	home := os.Getenv("HOME")
	if err := os.MkdirAll(filepath.Join(home, ".claude"), 0o700); err != nil {
		t.Fatalf("mkdir claude: %v", err)
	}
	body := `{
		"_acd_managed": true,
		"hooks": {
			"PreToolUse": [
				{ "matcher": "", "hooks": [
					{ "type": "command", "command": "bash -c 'acd start --harness claude-code; acd wake'" }
				]}
			],
			"PostToolUse": [
				{ "matcher": "", "hooks": [
					{ "type": "command", "command": "bash -c 'acd start --harness claude-code; acd wake'" }
				]}
			]
		}
	}`
	if err := os.WriteFile(filepath.Join(home, ".claude", "settings.json"), []byte(body), 0o600); err != nil {
		t.Fatalf("write claude settings: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	cc := findDoctorHarness(t, rep, "claude-code")
	if got := strings.Join(cc.Notes, "\n"); strings.Contains(got, "installed snippet drift") {
		t.Fatalf("did not expect drift warning, got notes=%v", cc.Notes)
	}
}

// TestDoctor_OpenCodeLegacyOnlyDriftSurfacesMatchedPath seeds a legacy
// OpenCode hooks.yaml at the pre-canonical path (~/.config/opencode/hooks.yaml)
// with a drifted active hook body (missing `acd start`+`acd wake`). The
// canonical path does not exist. Doctor must:
//   - mark the harness installed (via legacy fallback)
//   - populate MatchedPath in the JSON report
//   - emit a drift note that names the legacy file
//   - NEVER recommend overwriting the canonical primary
func TestDoctor_OpenCodeLegacyOnlyDriftSurfacesMatchedPath(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	home := os.Getenv("HOME")
	legacy := filepath.Join(home, ".config", "opencode", "hooks.yaml")
	if err := os.MkdirAll(filepath.Dir(legacy), 0o700); err != nil {
		t.Fatalf("mkdir opencode dir: %v", err)
	}
	// Marker present, but the active hook body is missing acd start+wake.
	body := "# acd-managed: true\nhooks:\n" +
		"  - event: tool.before.write\n" +
		"    command:\n" +
		"      bash: |\n" +
		"        echo no-op\n"
	if err := os.WriteFile(legacy, []byte(body), 0o600); err != nil {
		t.Fatalf("write legacy hooks.yaml: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	oc := findDoctorHarness(t, rep, "opencode")
	if !oc.Installed {
		t.Fatalf("opencode should be installed via legacy path: %+v", oc)
	}
	if oc.MatchedPath == "" {
		t.Fatalf("MatchedPath must be populated when marker lives on legacy path: %+v", oc)
	}
	// runDoctor's JSON output emits absolute paths (sanitization is only
	// applied for bundle manifests). Confirm MatchedPath points at the
	// legacy file we wrote.
	if oc.MatchedPath != legacy {
		t.Fatalf("MatchedPath=%q, want %q", oc.MatchedPath, legacy)
	}
	notes := strings.Join(oc.Notes, "\n")
	if !strings.Contains(notes, "installed snippet drift") {
		t.Fatalf("expected drift warning, got notes=%v", oc.Notes)
	}
	// Drift note must reference the matched (legacy) absolute path.
	if !strings.Contains(notes, legacy) {
		t.Fatalf("drift note must name matched/legacy path %q, got notes=%v", legacy, oc.Notes)
	}
	// Crucial: must never suggest overwriting the canonical primary when the
	// marker is on the legacy file — that would destroy a user's canonical
	// config they have not yet migrated.
	canonical := filepath.Join(home, ".config", "opencode", "hook", "hooks.yaml")
	if strings.Contains(notes, "> "+canonical) {
		t.Fatalf("drift note must NOT recommend overwriting canonical primary, got notes=%v", oc.Notes)
	}
}

// TestDoctor_OpenCodeCanonicalUnmarkedLegacyMarkedNoDestructiveOverwrite seeds
// a canonical OpenCode hooks.yaml WITHOUT the acd marker (a user's
// hand-authored config) alongside a legacy hooks.yaml WITH the marker and
// drifted body. Doctor must steer remediation toward the legacy file and
// never recommend overwriting the canonical primary that the user authored.
func TestDoctor_OpenCodeCanonicalUnmarkedLegacyMarkedNoDestructiveOverwrite(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	home := os.Getenv("HOME")
	canonical := filepath.Join(home, ".config", "opencode", "hook", "hooks.yaml")
	legacy := filepath.Join(home, ".config", "opencode", "hooks.yaml")
	if err := os.MkdirAll(filepath.Dir(canonical), 0o700); err != nil {
		t.Fatalf("mkdir canonical dir: %v", err)
	}
	// Canonical exists WITHOUT acd marker — represents user-authored config
	// that acd must not blow away.
	if err := os.WriteFile(canonical, []byte("hooks:\n  - event: tool.before.write\n    command:\n      bash: |\n        user-hook\n"), 0o600); err != nil {
		t.Fatalf("write canonical: %v", err)
	}
	// Legacy carries the acd marker and a drifted body.
	body := "# acd-managed: true\nhooks:\n" +
		"  - event: tool.before.write\n" +
		"    command:\n" +
		"      bash: |\n" +
		"        echo no-op\n"
	if err := os.WriteFile(legacy, []byte(body), 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	oc := findDoctorHarness(t, rep, "opencode")
	if !oc.Installed {
		t.Fatalf("opencode should be installed via legacy path: %+v", oc)
	}
	if oc.MatchedPath == "" {
		t.Fatalf("MatchedPath must be populated when marker is on legacy not canonical: %+v", oc)
	}
	notes := strings.Join(oc.Notes, "\n")
	if !strings.Contains(notes, "installed snippet drift") {
		t.Fatalf("expected drift warning, got notes=%v", oc.Notes)
	}
	// Must not contain the destructive cp/redirect recipe against the
	// canonical primary — that would clobber the user-authored canonical
	// file. JSON output is unsanitized so check the absolute canonical path.
	if strings.Contains(notes, "> "+canonical) {
		t.Fatalf("drift note must NOT recommend destructive overwrite of canonical, got notes=%v", oc.Notes)
	}
	if strings.Contains(notes, "cp "+canonical) {
		t.Fatalf("drift note must NOT recommend destructive cp on canonical, got notes=%v", oc.Notes)
	}
}

// TestDoctor_FallbackOnEACCESPrimaryConfig writes a primary config that is
// unreadable (mode 0000) and a legacy alternate-path TOML carrying the acd
// marker. Doctor must mark the harness as installed via fallback and append a
// "primary-path read failed" Note, instead of skipping fallback as it did
// before.
func TestDoctor_FallbackOnEACCESPrimaryConfig(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("EACCES test cannot run as root: 0o000 still readable")
	}
	_ = withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	home := os.Getenv("HOME")
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatalf("mkdir codex: %v", err)
	}
	hooksPath := filepath.Join(home, ".codex", "hooks.json")
	if err := os.WriteFile(hooksPath, []byte(`{"_acd_managed": true,"hooks":{}}`), 0o600); err != nil {
		t.Fatalf("write codex hooks.json: %v", err)
	}
	// Make primary config unreadable so the read produces EACCES.
	if err := os.Chmod(hooksPath, 0o000); err != nil {
		t.Fatalf("chmod hooks.json 0000: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(hooksPath, 0o600) })

	// Seed alternate-path legacy TOML so DetectInstalled flags codex.
	if err := os.WriteFile(filepath.Join(home, ".codex", "config.toml"), []byte("# acd-managed: true\n[features]\n"), 0o600); err != nil {
		t.Fatalf("write legacy config.toml: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	codex := findDoctorHarness(t, rep, "codex")
	if !codex.Installed {
		t.Fatalf("codex should be installed via alternate-path fallback after EACCES: %+v", codex)
	}
	notes := strings.Join(codex.Notes, "\n")
	if !strings.Contains(notes, "primary-path read failed") {
		t.Fatalf("expected primary-path read failed note, got %v", codex.Notes)
	}
	if !strings.Contains(notes, "alternate-path detection") {
		t.Fatalf("expected alternate-path detection note, got %v", codex.Notes)
	}
}

// TestDoctor_CodexHookLogTailSurfaced seeds a codex-hook.log under the
// isolated XDG_STATE_HOME and asserts doctor's codex Notes surface a count
// plus the first error line. Fixture matches the wrapper printf shape
// emitted by templates/codex/hooks.json: a bracketed numeric-zone
// timestamp prefix followed by `... failed exit=N cmd=acd-...`.
func TestDoctor_CodexHookLogTailSurfaced(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	home := os.Getenv("HOME")
	// Install codex hook so doctor reports the harness at all.
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatalf("mkdir codex: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "hooks.json"), []byte(`{"_acd_managed": true,"hooks":{}}`), 0o600); err != nil {
		t.Fatalf("write codex hooks.json: %v", err)
	}

	// Seed codex-hook.log at the XDG state path. roots.State already
	// contains the trailing /acd subdir.
	logDir := roots.State
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		t.Fatalf("mkdir codex log dir: %v", err)
	}
	logPath := filepath.Join(logDir, "codex-hook.log")
	now := time.Now().UTC()
	// Wrapper printf shape: `date +%FT%T%z` produces e.g.
	// `2026-05-08T12:34:56+0000` (no colon in the zone). The line
	// emitted by the bash wrapper is bracketed.
	stamp := now.Format("2006-01-02T15:04:05-0700")
	logBody := "[" + stamp + "] session-start failed exit=2 cmd=acd-start\n" +
		"[" + stamp + "] active hook failed exit=1 cmd=acd-start-wake\n" +
		"info: harmless line\n"
	if err := os.WriteFile(logPath, []byte(logBody), 0o600); err != nil {
		t.Fatalf("write codex hook log: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	codex := findDoctorHarness(t, rep, "codex")
	notes := strings.Join(codex.Notes, "\n")
	if !strings.Contains(notes, "codex-hook.log") {
		t.Fatalf("expected codex-hook.log note, got %v", codex.Notes)
	}
	if !strings.Contains(notes, "cmd=acd-") {
		t.Fatalf("expected first-error excerpt to include cmd=acd-, got %v", codex.Notes)
	}
	// Ensure the count is at least 1 (we wrote 2 errors).
	if !strings.Contains(notes, "1 ") && !strings.Contains(notes, "2 ") {
		t.Fatalf("expected error count, got %v", codex.Notes)
	}
}

// TestDoctor_CodexHookLogQuietWhenNoErrors verifies the log-tail check stays
// silent for lines that are not wrapper-printf failures: plain info lines,
// JSONL info lines that happen to mention "failed_blocking_pending=0", and
// "no error encountered" prose. None of these should trigger a Note.
func TestDoctor_CodexHookLogQuietWhenNoErrors(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	home := os.Getenv("HOME")
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatalf("mkdir codex: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "hooks.json"), []byte(`{"_acd_managed": true,"hooks":{}}`), 0o600); err != nil {
		t.Fatalf("write codex hooks.json: %v", err)
	}
	if err := os.MkdirAll(roots.State, 0o700); err != nil {
		t.Fatalf("mkdir state: %v", err)
	}
	logBody := "info: ok\n" +
		"info: ok again\n" +
		`{"ts":"2026-05-08T12:00:00Z","level":"info","msg":"replay","failed_blocking_pending":0}` + "\n" +
		"info: no error encountered while flushing\n"
	if err := os.WriteFile(filepath.Join(roots.State, "codex-hook.log"), []byte(logBody), 0o600); err != nil {
		t.Fatalf("write codex hook log: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	codex := findDoctorHarness(t, rep, "codex")
	if got := strings.Join(codex.Notes, "\n"); strings.Contains(got, "codex-hook.log") {
		t.Fatalf("did not expect codex-hook.log note for clean log, got %v", codex.Notes)
	}
}

func findDoctorHarness(t *testing.T, rep doctorReport, name string) doctorHarnessReport {
	t.Helper()
	for _, h := range rep.Harnesses {
		if h.Name == name {
			return h
		}
	}
	t.Fatalf("harness %q not found in %+v", name, rep.Harnesses)
	return doctorHarnessReport{}
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func readZipFile(t *testing.T, r *zip.ReadCloser, name string) string {
	t.Helper()
	for _, f := range r.File {
		if f.Name == name {
			rc, err := f.Open()
			if err != nil {
				t.Fatalf("open %s: %v", name, err)
			}
			defer rc.Close()
			body, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("read %s: %v", name, err)
			}
			return string(body)
		}
	}
	t.Fatalf("zip member %s not found", name)
	return ""
}

// readSnippet reads a verbatim template snippet from the embedded
// templates FS so YAML-drift tests do not drift from the shipped shape.
func readSnippet(t *testing.T, path string) []byte {
	t.Helper()
	body, err := fs.ReadFile(templates.FS, path)
	if err != nil {
		t.Fatalf("read embedded snippet %s: %v", path, err)
	}
	return body
}

// TestYAMLDrift_FromVerbatimSnippet feeds the verbatim
// opencode/pi snippet bodies into extractYAMLHookBodies and asserts that
// (a) the unmodified body has every active hook carrying both `acd start`
// and `acd wake` (no drift), and (b) when one `acd start` invocation is
// stripped from a tool.before/tool.after item, the drift scanner reports
// at least one stale hook. This locks down the regression where the
// scanner misread nested `actions: - bash:` items as new orphan hookItems
// and silently dropped the parent event association.
func TestYAMLDrift_FromVerbatimSnippet(t *testing.T) {
	cases := []struct {
		harness     string
		snippetPath string
	}{
		{harness: "opencode", snippetPath: "opencode/hooks.snippet.yaml"},
		{harness: "pi", snippetPath: "pi/hooks.snippet.yaml"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.harness, func(t *testing.T) {
			body := readSnippet(t, tc.snippetPath)
			// Clean snippet: every active hook (tool.before.* / tool.after.*)
			// carries `acd start` AND `acd wake`. scanHookBodyDrift returns
			// "" when no drift is detected.
			if note := scanHookBodyDrift(tc.harness, body); note != "" {
				t.Fatalf("verbatim %s snippet should not report drift, got %q", tc.harness, note)
			}
			// Bodies should also be non-empty: at least the two active hooks
			// (tool.before.*, tool.after.*) must parse out.
			bodies := extractYAMLHookBodies(body, []string{"tool.before.", "tool.after."})
			if len(bodies) < 2 {
				t.Fatalf("expected at least 2 active hook bodies for %s, got %d", tc.harness, len(bodies))
			}
			for i, b := range bodies {
				if !strings.Contains(b, "acd start") {
					t.Fatalf("%s active hook[%d] missing 'acd start' in body=%q", tc.harness, i, b)
				}
				if !strings.Contains(b, "acd wake") {
					t.Fatalf("%s active hook[%d] missing 'acd wake' in body=%q", tc.harness, i, b)
				}
			}
			// Now strip the FIRST `acd start \` line under any
			// tool.before/tool.after action to simulate user drift. We
			// only remove a line that begins (after whitespace) with
			// `acd start` — the regression we are guarding against: drift
			// detection silently never fires for real OpenCode/Pi configs.
			drifted := stripFirstAcdStart(t, string(body))
			note := scanHookBodyDrift(tc.harness, []byte(drifted))
			if note == "" {
				t.Fatalf("%s drift snippet should report drift, got empty note", tc.harness)
			}
			if !strings.Contains(note, "installed snippet drift") {
				t.Fatalf("%s drift note missing prefix, got %q", tc.harness, note)
			}
		})
	}
}

// TestJSONDrift_FromVerbatimCursorSnippet feeds the shipped cursor/hooks.json
// flat schema into extractCursorFlatHookBodies and scanHookBodyDrift.
func TestJSONDrift_FromVerbatimCursorSnippet(t *testing.T) {
	body := readSnippet(t, "cursor/hooks.json")
	if note := scanHookBodyDrift("cursor", body); note != "" {
		t.Fatalf("verbatim cursor snippet should not report drift, got %q", note)
	}
	bodies := extractCursorFlatHookBodies(body, []string{"postToolUse", "afterFileEdit"})
	if len(bodies) < 2 {
		t.Fatalf("expected at least 2 active hook bodies for cursor, got %d", len(bodies))
	}
	for i, b := range bodies {
		if !activeHookBodyHasStartWake("cursor", b) {
			t.Fatalf("cursor active hook[%d] missing canonical start+wake behavior in %q", i, b)
		}
	}
	drifted := strings.Replace(string(body),
		`acd wake --session-id`,
		`acd woke --session-id`, 1)
	note := scanHookBodyDrift("cursor", []byte(drifted))
	if note == "" {
		t.Fatalf("cursor drift snippet should report drift, got empty note")
	}
	if !strings.Contains(note, "installed snippet drift") {
		t.Fatalf("cursor drift note missing prefix, got %q", note)
	}
}

func TestJSONDrift_CursorSessionStartWrongSubcommand(t *testing.T) {
	body := readSnippet(t, "cursor/hooks.json")
	drifted := strings.Replace(string(body),
		`acd start --harness cursor`,
		`acd wake --session-id SID`, 1)
	note := scanHookBodyDrift("cursor", []byte(drifted))
	if note == "" {
		t.Fatalf("sessionStart wired to wake should report drift")
	}
}

func TestJSONDrift_CursorMissingRequiredEvents(t *testing.T) {
	note := scanHookBodyDrift("cursor", []byte(`{"_acd_managed":true,"hooks":{}}`))
	if note == "" {
		t.Fatalf("empty managed cursor hooks should report drift")
	}
	if !strings.Contains(note, "5 active hook(s)") {
		t.Fatalf("empty managed cursor hooks should count five missing lifecycle hooks, got %q", note)
	}
}

func TestJSONDrift_CursorRejectsMissingWakeCommand(t *testing.T) {
	body := readSnippet(t, "cursor/hooks.json")
	drifted := strings.Replace(string(body),
		`acd wake --session-id`,
		`acd woke --session-id`, 1)
	note := scanHookBodyDrift("cursor", []byte(drifted))
	if note == "" {
		t.Fatalf("active hook without acd wake should report drift")
	}
}

// TestDoctor_DriftWarningCursorActiveHook seeds a cursor hooks.json whose
// postToolUse body no longer invokes start+wake and asserts doctor surfaces
// drift with the cursor remediation hint.
func TestDoctor_DriftWarningCursorActiveHook(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	home := os.Getenv("HOME")
	if err := os.MkdirAll(filepath.Join(home, ".cursor"), 0o700); err != nil {
		t.Fatalf("mkdir cursor: %v", err)
	}
	body := `{
		"version": 1,
		"_acd_managed": true,
		"hooks": {
			"postToolUse": [
				{ "command": "echo no-op", "timeout": 15 }
			],
			"afterFileEdit": [
				{ "command": "acd start --harness cursor && acd wake", "timeout": 15 }
			]
		}
	}`
	if err := os.WriteFile(filepath.Join(home, ".cursor", "hooks.json"), []byte(body), 0o600); err != nil {
		t.Fatalf("write cursor hooks.json: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	cur := findDoctorHarness(t, rep, "cursor")
	if !cur.Installed {
		t.Fatalf("cursor should be installed: %+v", cur)
	}
	notes := strings.Join(cur.Notes, "\n")
	if !strings.Contains(notes, "installed snippet drift") {
		t.Fatalf("expected drift warning, got notes=%v", cur.Notes)
	}
	if !strings.Contains(notes, "acd setup cursor") {
		t.Fatalf("drift note missing remediation command, got notes=%v", cur.Notes)
	}
}

// TestDriftRemediation_CursorCanonicalPath locks in that cursor drift hints
// reference ~/.cursor/hooks.json for merge and overwrite recipes.
func TestDriftRemediation_CursorCanonicalPath(t *testing.T) {
	cmd, ok := driftRemediationCommands["cursor"]
	if !ok {
		t.Fatal("driftRemediationCommands missing entry for cursor")
	}
	for _, want := range []string{
		"acd setup cursor",
		"merge output into ~/.cursor/hooks.json",
		"cp ~/.cursor/hooks.json ~/.cursor/hooks.json.bak",
		"acd setup cursor --raw > ~/.cursor/hooks.json",
	} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("cursor remediation missing %q\nfull: %s", want, cmd)
		}
	}
	tokens := extractPathTokensAfter(t, cmd, []string{
		"merge output into ",
		"cp ",
		"--raw > ",
	})
	if len(tokens) == 0 {
		t.Fatalf("cursor remediation: extracted 0 path tokens\nfull: %s", cmd)
	}
	for _, tok := range tokens {
		normalized := strings.TrimSuffix(tok, ".bak")
		if !strings.HasSuffix(normalized, "/.cursor/hooks.json") {
			t.Fatalf("cursor remediation path token %q does not end with /.cursor/hooks.json\nfull: %s", tok, cmd)
		}
	}
}

// TestDriftRemediation_OpenCodePiCanonicalPaths locks in that the
// remediation hints surfaced by `acd doctor` for OpenCode and Pi point at
// the canonical default hook paths (`~/.config/opencode/hook/hooks.yaml`
// and `~/.pi/agent/hook/hooks.yaml`). Both the merge-into hint and the
// destructive cp/backup-then-overwrite recipe must mention the canonical
// path, never the legacy pre-canonical layout. Guards against silent
// regressions when the path map is touched.
//
// The legacy-leak check is performed via token-suffix validation, not a
// loose `strings.Contains` substring check: every path token that follows
// `merge output into `, `cp `, or `--raw > ` must end with the canonical
// `/hook/hooks.yaml` (opencode) or `/agent/hook/hooks.yaml` (pi). This
// tighter assertion would catch a regression where the format changes in
// a way the legacy bare `hooks.yaml` leaks in but the legacy substring
// happens to also appear as a prefix of the canonical path (which it
// does: `~/.config/opencode/hooks.yaml` is a suffix of nothing legitimate
// but a future format like `~/.config/opencode/hooks.yaml.tmpl` would
// confuse a naive Contains check).
func TestDriftRemediation_OpenCodePiCanonicalPaths(t *testing.T) {
	cases := []struct {
		harness            string
		mustContain        []string // every substring must appear in the hint
		canonicalPathToken string   // each extracted path token must end with this
	}{
		{
			harness: "opencode",
			mustContain: []string{
				"acd setup opencode",
				"merge output into ~/.config/opencode/hook/hooks.yaml",
				"cp ~/.config/opencode/hook/hooks.yaml ~/.config/opencode/hook/hooks.yaml.bak",
				"acd setup opencode --raw > ~/.config/opencode/hook/hooks.yaml",
			},
			canonicalPathToken: "/hook/hooks.yaml",
		},
		{
			harness: "pi",
			mustContain: []string{
				"acd setup pi",
				"merge output into ~/.pi/agent/hook/hooks.yaml",
				"cp ~/.pi/agent/hook/hooks.yaml ~/.pi/agent/hook/hooks.yaml.bak",
				"acd setup pi --raw > ~/.pi/agent/hook/hooks.yaml",
			},
			canonicalPathToken: "/agent/hook/hooks.yaml",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.harness, func(t *testing.T) {
			cmd, ok := driftRemediationCommands[tc.harness]
			if !ok {
				t.Fatalf("driftRemediationCommands missing entry for %q", tc.harness)
			}
			for _, want := range tc.mustContain {
				if !strings.Contains(cmd, want) {
					t.Fatalf("%s remediation missing %q\nfull: %s", tc.harness, want, cmd)
				}
			}
			// Tighter assertion: extract every path-shaped token that
			// appears after `merge output into `, `cp `, or `--raw > ` and
			// confirm it ends with the canonical suffix. A leaked legacy
			// token would end in `/hooks.yaml` (opencode) or `/hook/hooks.yaml`
			// (pi) which differ from the canonical suffix.
			tokens := extractPathTokensAfter(t, cmd, []string{
				"merge output into ",
				"cp ",
				"--raw > ",
			})
			if len(tokens) == 0 {
				t.Fatalf("%s remediation: extracted 0 path tokens\nfull: %s", tc.harness, cmd)
			}
			for _, tok := range tokens {
				// Strip the `.bak` suffix used by the cp recipe so the
				// canonical-suffix check applies uniformly to backup paths.
				normalized := strings.TrimSuffix(tok, ".bak")
				if !strings.HasSuffix(normalized, tc.canonicalPathToken) {
					t.Fatalf("%s remediation path token %q does not end with canonical suffix %q\nfull: %s",
						tc.harness, tok, tc.canonicalPathToken, cmd)
				}
			}
		})
	}
}

// extractPathTokensAfter scans s for each marker in markers and returns
// every whitespace-bounded token that immediately follows. Used to assert
// that every path mentioned in a remediation hint is the canonical one.
func extractPathTokensAfter(t *testing.T, s string, markers []string) []string {
	t.Helper()
	var out []string
	for _, m := range markers {
		rest := s
		for {
			idx := strings.Index(rest, m)
			if idx < 0 {
				break
			}
			after := rest[idx+len(m):]
			// The token runs until the next whitespace, semicolon, or
			// end-of-string.
			end := len(after)
			for i, r := range after {
				if r == ' ' || r == '\t' || r == ';' || r == '\n' {
					end = i
					break
				}
			}
			if end > 0 {
				out = append(out, after[:end])
			}
			rest = after[end:]
		}
	}
	return out
}

// stripFirstAcdStart removes the first leading-whitespace `acd start \`
// line from body — used by YAMLDrift tests to introduce a single drift.
func stripFirstAcdStart(t *testing.T, body string) string {
	t.Helper()
	lines := strings.Split(body, "\n")
	for i, ln := range lines {
		trimmed := strings.TrimSpace(ln)
		// The OpenCode/Pi snippets open the active-hook chain with
		// `{ acd start \\` (literal backslash) under each
		// tool.before/tool.after item. Drop just the first such line.
		if strings.HasPrefix(trimmed, "{ acd start") {
			lines = append(lines[:i], lines[i+1:]...)
			return strings.Join(lines, "\n")
		}
	}
	t.Fatalf("could not find an `{ acd start` line to strip in body")
	return body
}

// TestParseLogTimestamp_WrapperPrintfShape locks down the parser against
// the production wrapper printf line shape. Run with TZ=Asia/Tokyo to
// catch zone bugs where the parser silently coerced into local time.
func TestParseLogTimestamp_WrapperPrintfShape(t *testing.T) {
	// Pin codexHookLogRecentWindow to 5 minutes (matches default; reset
	// is not strictly needed but documents the intent of the test).
	now := time.Now()
	wrap := func(off time.Duration) string {
		// Mirror `date +%FT%T%z`: e.g. 2026-05-08T12:34:56+0900.
		return "[" + now.Add(off).Format("2006-01-02T15:04:05-0700") + "] active hook failed exit=1 cmd=acd-start-wake"
	}
	cases := []struct {
		name       string
		line       string
		wantOK     bool
		wantRecent bool
	}{
		{
			name:       "bracketed_recent_within_5min",
			line:       wrap(-1 * time.Minute),
			wantOK:     true,
			wantRecent: true,
		},
		{
			name:       "bracketed_outside_5min_window",
			line:       wrap(-30 * time.Minute),
			wantOK:     true,
			wantRecent: false,
		},
		{
			name:       "bare_iso_no_zone",
			line:       now.Format("2006-01-02T15:04:05") + " bash error",
			wantOK:     true,
			wantRecent: true,
		},
		{
			name:       "jsonl_ts_field",
			line:       fmt.Sprintf(`{"ts":%q,"level":"error","msg":"boom"}`, now.Format(time.RFC3339Nano)),
			wantOK:     true,
			wantRecent: true,
		},
		{
			name:       "no_timestamp",
			line:       "info: harmless line",
			wantOK:     false,
			wantRecent: false,
		},
	}
	cutoff := time.Now().Add(-codexHookLogRecentWindow)
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ts, ok := parseLogTimestamp(tc.line)
			if ok != tc.wantOK {
				t.Fatalf("parseLogTimestamp ok mismatch: line=%q got=%v want=%v ts=%v", tc.line, ok, tc.wantOK, ts)
			}
			if !ok {
				return
			}
			recent := ts.After(cutoff)
			if recent != tc.wantRecent {
				t.Fatalf("recent mismatch: line=%q ts=%v cutoff=%v got recent=%v want=%v", tc.line, ts, cutoff, recent, tc.wantRecent)
			}
		})
	}
}

// TestDoctor_UnreadablePrimaryConfigSetsConfigReadError verifies that an
// EACCES on the primary harness config produces a non-empty
// ConfigReadError, ConfigPresent=true, ConfigReadable=false, and
// MarkerFound=false. JSON consumers can use this to disambiguate
// "marker missing" from "fell back to alternate-path detection".
func TestDoctor_UnreadablePrimaryConfigSetsConfigReadError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("EACCES test cannot run as root: 0o000 still readable")
	}
	_ = withIsolatedHome(t)
	ctx := context.Background()
	t.Setenv(ai.EnvProvider, "")
	t.Setenv(ai.EnvAPIKey, "")

	home := os.Getenv("HOME")
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatalf("mkdir codex: %v", err)
	}
	hooksPath := filepath.Join(home, ".codex", "hooks.json")
	if err := os.WriteFile(hooksPath, []byte(`{"_acd_managed": true,"hooks":{}}`), 0o600); err != nil {
		t.Fatalf("write codex hooks.json: %v", err)
	}
	if err := os.Chmod(hooksPath, 0o000); err != nil {
		t.Fatalf("chmod 0000: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(hooksPath, 0o600) })

	var jsonOut bytes.Buffer
	if err := runDoctor(ctx, &jsonOut, false, "", true); err != nil {
		t.Fatalf("runDoctor json: %v", err)
	}
	var rep doctorReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	codex := findDoctorHarness(t, rep, "codex")
	if !codex.ConfigPresent {
		t.Fatalf("expected ConfigPresent=true for unreadable primary, got %+v", codex)
	}
	if codex.ConfigReadable {
		t.Fatalf("expected ConfigReadable=false for unreadable primary, got %+v", codex)
	}
	if codex.MarkerFound {
		t.Fatalf("expected MarkerFound=false for unreadable primary, got %+v", codex)
	}
	if codex.ConfigReadError == "" {
		t.Fatalf("expected non-empty ConfigReadError for unreadable primary, got %+v", codex)
	}
}

// TestLooksLikeHookError_TightMatcher locks down the new wrapper-aware
// rules: only the wrapper printf shape and explicit JSONL error/fatal
// levels are flagged. JSONL info lines (even ones that mention
// "failed_blocking_pending=0") and bland prose stay silent.
func TestLooksLikeHookError_TightMatcher(t *testing.T) {
	cases := []struct {
		name string
		line string
		want bool
	}{
		{
			name: "wrapper_active_hook_failed",
			line: "[2026-05-08T12:34:56+0300] active hook failed exit=1 cmd=acd-start-wake",
			want: true,
		},
		{
			name: "wrapper_session_start_failed",
			line: "[2026-05-08T12:34:56+0300] session-start failed exit=2 cmd=acd-start",
			want: true,
		},
		{
			name: "jsonl_error_level",
			line: `{"ts":"2026-05-08T12:34:56Z","level":"error","msg":"boom"}`,
			want: true,
		},
		{
			name: "jsonl_fatal_level",
			line: `{"ts":"2026-05-08T12:34:56Z","level":"fatal","msg":"halt"}`,
			want: true,
		},
		{
			name: "jsonl_info_failed_blocking_pending_zero",
			line: `{"ts":"2026-05-08T12:34:56Z","level":"info","failed_blocking_pending":0}`,
			want: false,
		},
		{
			name: "info_no_error_encountered",
			line: "info: no error encountered while flushing",
			want: false,
		},
		{
			name: "plain_info_line",
			line: "info: ok",
			want: false,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := looksLikeHookError(tc.line)
			if got != tc.want {
				t.Fatalf("looksLikeHookError(%q) = %v, want %v", tc.line, got, tc.want)
			}
		})
	}
}
