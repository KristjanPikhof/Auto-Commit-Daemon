package cli

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	// Sleep a second so the timestamp differs.
	if err := waitOneSecond(); err != nil {
		t.Fatalf("wait: %v", err)
	}
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
