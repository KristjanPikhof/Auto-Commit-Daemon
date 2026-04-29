package cli

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
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
	for _, want := range []string{"acd doctor", "Registry", "Sensitive globs", "Repos"} {
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

func TestDoctor_InstallReportsHarnessMarkersAndCodexLegacy(t *testing.T) {
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
	if err := os.WriteFile(filepath.Join(home, ".codex", "hooks.json"), []byte(`{"hooks":[]}`), 0o600); err != nil {
		t.Fatalf("write legacy codex hooks: %v", err)
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
	if codex.Installed || codex.MarkerFound {
		t.Fatalf("codex should be absent when only hooks.json exists: %+v", codex)
	}
	if !strings.Contains(strings.Join(codex.Notes, "\n"), "legacy ~/.codex/hooks.json exists") {
		t.Fatalf("codex legacy hooks warning missing: %+v", codex)
	}

	var humanOut bytes.Buffer
	if err := runDoctor(ctx, &humanOut, false, "", false); err != nil {
		t.Fatalf("runDoctor human: %v", err)
	}
	body := humanOut.String()
	for _, want := range []string{"Install", "claude-code : yes", "codex       : no", "legacy ~/.codex/hooks.json exists"} {
		if !strings.Contains(body, want) {
			t.Fatalf("human doctor missing %q:\n%s", want, body)
		}
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
