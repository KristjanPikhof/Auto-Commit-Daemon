package cli

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	pathspkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestChangedCommandCapabilityMatrix(t *testing.T) {
	paths := [][]string{
		{"setup"}, {"status"}, {"on"}, {"off"}, {"list"}, {"commit-all"}, {"history"}, {"restore"},
		{"doctor"}, {"uninstall"}, {"config", "get"}, {"config", "set"},
		{"config", "edit"}, {"config", "reset"}, {"config", "credentials"},
		{"support", "diagnose"}, {"support", "logs"}, {"support", "repair"},
		{"support", "bundle"}, {"repo", "list"}, {"repo", "remove"}, {"repo", "gc"},
	}
	for _, path := range paths {
		root := newRootCmd()
		command, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("find %v: %v", path, err)
		}
		for _, flag := range []string{"repo", "json", "quiet", "log-level"} {
			if command.Flags().Lookup(flag) == nil && command.InheritedFlags().Lookup(flag) == nil {
				t.Errorf("%s lacks persistent --%s capability", command.CommandPath(), flag)
			}
		}
	}
}

func TestChangedCommandCapabilitiesAreDeclarative(t *testing.T) {
	tests := []struct {
		path          []string
		repository    bool
		json          bool
		interactive   bool
		streaming     bool
		jsonStreaming bool
	}{
		{path: []string{"status"}, repository: true, json: true},
		{path: []string{"list"}, json: true, interactive: true, streaming: true},
		{path: []string{"uninstall"}, json: true, interactive: true},
		{path: []string{"config", "edit"}, repository: true, interactive: true},
		{path: []string{"config", "credentials"}, json: true},
		{path: []string{"support", "diagnose"}, repository: true, json: true},
		{path: []string{"support", "logs"}, repository: true, json: true, streaming: true},
		{path: []string{"repo", "list"}, json: true},
	}
	for _, test := range tests {
		root := newRootCmd()
		command, _, err := root.Find(test.path)
		if err != nil {
			t.Fatalf("find %v: %v", test.path, err)
		}
		got := invocationCapabilities(command)
		if got.Repository != test.repository || got.JSON != test.json ||
			got.Interactive != test.interactive || got.Streaming != test.streaming ||
			got.JSONStreaming != test.jsonStreaming {
			t.Errorf("%s capabilities=%+v", command.CommandPath(), got)
		}
	}
}

func TestCompatibilityProtocolCommandsRetainJSONCapability(t *testing.T) {
	for _, path := range [][]string{
		{"start"}, {"stop"}, {"wake"}, {"touch"}, {"flush"}, {"daemon", "run"},
		{"internal", "hint"}, {"internal", "session", "open"}, {"internal", "session", "close"},
	} {
		root := newRootCmd()
		command, _, err := root.Find(path)
		if err != nil {
			t.Fatalf("find %v: %v", path, err)
		}
		capabilities := invocationCapabilities(command)
		if !capabilities.Repository || !capabilities.JSON || !capabilities.Quiet {
			t.Errorf("%s capabilities=%+v, want repository/JSON/quiet", command.CommandPath(), capabilities)
		}
	}
}

func TestGlobalAndInteractiveFlagsAreExplicitlyRejected(t *testing.T) {
	cases := [][]string{
		{"uninstall", "--repo", ".", "--dry-run"},
		{"list", "--repo", ".", "--once"},
		{"repo", "list", "--repo", "."},
		{"repo", "gc", "--repo", "."},
		{"gc", "--repo", "."},
		{"config", "credentials", "--repo", "."},
		{"config", "edit", "--json"},
		{"list", "--interactive", "--json"},
		{"support", "logs", "--follow", "--json"},
		{"status", "--log-level", "nope"},
	}
	for _, args := range cases {
		root := newRootCmd()
		root.SetOut(&bytes.Buffer{})
		root.SetErr(&bytes.Buffer{})
		root.SetArgs(args)
		err := root.ExecuteContext(context.Background())
		if err == nil {
			t.Errorf("args=%v unexpectedly succeeded", args)
			continue
		}
		classified := classifyCobraError(err)
		if ExitCode(classified) != ExitInvalid {
			t.Errorf("args=%v exit=%d err=%v, want %d", args,
				ExitCode(classified), err, ExitInvalid)
		}
	}
}

func TestLexicalJSONAndParseFailuresRemainInvalidInput(t *testing.T) {
	for _, spelling := range []string{
		"--json", "--json=1", "--json=t", "--json=T", "--json=TRUE", "--json=true", "--json=True",
	} {
		if !commandLineRequestsJSON([]string{"not-a-command", spelling}) {
			t.Errorf("lexical JSON intent was not retained for %q", spelling)
		}
	}
	for _, spelling := range []string{
		"--json=0", "--json=f", "--json=F", "--json=FALSE", "--json=false", "--json=False",
	} {
		if commandLineRequestsJSON([]string{"not-a-command", spelling}) {
			t.Errorf("explicit false JSON value %q requested JSON output", spelling)
		}
	}
	if !commandLineRequestsJSON([]string{"--json=false", "--json=1"}) {
		t.Fatal("later true JSON value was ignored")
	}
	if commandLineRequestsJSON([]string{"--", "--json=1"}) {
		t.Fatal("argument after -- requested JSON output")
	}
	for _, message := range []string{
		"unknown shorthand flag: 'x' in -x",
		"flag needs an argument: --repo",
	} {
		classified := classifyCobraError(errors.New(message))
		if ExitCode(classified) != ExitInvalid {
			t.Errorf("message=%q exit=%d, want %d", message, ExitCode(classified), ExitInvalid)
		}
	}
}

func TestInvocationCapabilitiesFailClosedAndCanonicalAliasesDeclareIntent(t *testing.T) {
	root := newRootCmd()
	internal, _, err := root.Find([]string{"internal"})
	if err != nil {
		t.Fatal(err)
	}
	if got := invocationCapabilities(internal); got != (commandCapabilities{}) {
		t.Fatalf("undeclared internal capabilities=%+v, want fail-closed zero value", got)
	}
	if got := invocationCapabilities(root); !got.Repository || !got.JSON || !got.Quiet {
		t.Fatalf("root capabilities=%+v, want explicit repository/JSON/quiet", got)
	}

	tests := []struct {
		alias       string
		canonical   []string
		replacement string
	}{
		{alias: "events", canonical: []string{"history", "activity"}, replacement: "acd history activity"},
		{alias: "prompt", canonical: []string{"support", "prompt"}, replacement: "acd support prompt"},
		{alias: "fix", canonical: []string{"support", "recover"}, replacement: "acd support recover"},
		{alias: "recover", canonical: []string{"support", "recover"}, replacement: "acd support recover"},
		{alias: "stats", canonical: []string{"repo", "stats"}, replacement: "acd repo stats"},
	}
	for _, test := range tests {
		alias, _, err := root.Find([]string{test.alias})
		if err != nil {
			t.Fatalf("find alias %s: %v", test.alias, err)
		}
		canonical, _, err := root.Find(test.canonical)
		if err != nil {
			t.Fatalf("find canonical %v: %v", test.canonical, err)
		}
		if !alias.Hidden {
			t.Errorf("alias %s is visible", test.alias)
		}
		if got := alias.Annotations["acd.compatibility.replacement"]; got != test.replacement {
			t.Errorf("alias %s replacement=%q, want %q", test.alias, got, test.replacement)
		}
		if got, want := invocationCapabilities(alias), invocationCapabilities(canonical); got != want {
			t.Errorf("alias %s capabilities=%+v, canonical=%+v", test.alias, got, want)
		}
	}
}

func TestCanonicalAdvancedJSONUsesSingleEnvelope(t *testing.T) {
	t.Run("credentials", func(t *testing.T) {
		withIsolatedHome(t)
		assertSingleProductEnvelope(t, []string{"config", "credentials", "--json"})
	})

	t.Run("repo list", func(t *testing.T) {
		withIsolatedHome(t)
		assertSingleProductEnvelope(t, []string{"repo", "list", "--json"})
	})

	for _, command := range []string{"support diagnose", "history explain", "support logs"} {
		command := command
		t.Run(command, func(t *testing.T) {
			roots := withIsolatedHome(t)
			repo, stateDB, db := makeRepoStateDB(t)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			registerRepo(t, roots, repo, stateDB, "codex")
			if command == "support logs" {
				registry, err := central.Load(roots)
				if err != nil {
					t.Fatal(err)
				}
				logPath := roots.RepoLogPath(registry.Repos[0].RepoHash)
				if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(logPath, []byte("{\"message\":\"ready\"}\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			args := append(strings.Fields(command), "--repo", repo, "--json")
			assertSingleProductEnvelope(t, args)
		})
	}
}

func TestDoctorBundlesHonorRepositoryScopeAndUseSingleEnvelope(t *testing.T) {
	roots := withIsolatedHome(t)
	repo1, stateDB1, db1 := makeRepoStateDB(t)
	repo2, stateDB2, db2 := makeRepoStateDB(t)
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	registerRepo(t, roots, repo1, stateDB1, "codex")
	registerRepo(t, roots, repo2, stateDB2, "cursor")

	for _, prefix := range [][]string{{"doctor", "--bundle"}, {"support", "bundle"}} {
		name := strings.Join(prefix, " ")
		t.Run(name, func(t *testing.T) {
			args := append(append([]string{}, prefix...),
				"--repo", repo1, "--json", "--output", t.TempDir())
			envelope := executeProductEnvelope(t, args)
			body, err := json.Marshal(envelope.Data)
			if err != nil {
				t.Fatal(err)
			}
			var bundle bundleResult
			if err := json.Unmarshal(body, &bundle); err != nil {
				t.Fatalf("decode bundle result: %v\ndata=%s", err, body)
			}
			registry := readBundleRegistry(t, bundle.Path)
			if len(registry.Repos) != 1 || filepath.Clean(registry.Repos[0].Path) != filepath.Clean(repo1) {
				t.Fatalf("bundle registry=%+v, want only %s", registry.Repos, repo1)
			}
			if strings.Contains(string(body), repo2) {
				t.Fatalf("bundle result disclosed unrelated repo %s: %s", repo2, body)
			}
		})
	}
}

func executeProductEnvelope(t *testing.T, args []string) productEnvelope {
	t.Helper()
	root := newRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs(args)
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("acd %s: %v\n%s", strings.Join(args, " "), err, out.String())
	}
	decoder := json.NewDecoder(&out)
	var envelope productEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		t.Fatalf("decode envelope: %v\n%s", err, out.String())
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		t.Fatalf("expected exactly one envelope, extra=%v err=%v", extra, err)
	}
	return envelope
}

func readBundleRegistry(t *testing.T, path string) central.Registry {
	t.Helper()
	reader, err := zip.OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, file := range reader.File {
		if file.Name != "registry.json" {
			continue
		}
		input, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(input)
		closeErr := input.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			t.Fatal(err)
		}
		var registry central.Registry
		if err := json.Unmarshal(body, &registry); err != nil {
			t.Fatalf("decode registry.json: %v\n%s", err, body)
		}
		return registry
	}
	t.Fatal("bundle lacks registry.json")
	return central.Registry{}
}

func assertSingleProductEnvelope(t *testing.T, args []string) {
	t.Helper()
	envelope := executeProductEnvelope(t, args)
	if !envelope.OK || envelope.Actions == nil || envelope.Data == nil {
		t.Fatalf("incomplete envelope: %+v", envelope)
	}
}

func TestStableJSONEnvelopeHasMandatoryKeys(t *testing.T) {
	var out bytes.Buffer
	if err := renderJSONEnvelope(&out, productEnvelope{OK: true, State: productStateProtected,
		Actions: []productAction{}, Data: map[string]any{}}); err != nil {
		t.Fatal(err)
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(out.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"ok", "state", "changed", "actions", "next_action", "data", "error"} {
		if _, ok := value[key]; !ok {
			t.Errorf("mandatory JSON key %q missing: %s", key, out.String())
		}
	}
}

func TestCheckpointDrainCreatesLogicalPublicationBoundary(t *testing.T) {
	ctx := context.Background()
	_, _, db := makeRepoStateDB(t)
	defer db.Close()
	params, err := json.Marshal(map[string]bool{"drain_publication": true})
	if err != nil {
		t.Fatal(err)
	}
	if err := applyWorkerActivityHint(ctx, db, params); err != nil {
		t.Fatal(err)
	}
	flush, ok, err := state.ClaimNextFlushRequest(ctx, db)
	if err != nil || !ok || flush.Command != "flush_logical" || !flush.NonBlocking {
		t.Fatalf("publication drain flush=%+v ok=%t err=%v", flush, ok, err)
	}
	boundaries, err := state.PendingIntentActivityBoundaries(ctx, db, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(boundaries) != 1 || boundaries[0].Kind != state.IntentBoundaryHard ||
		boundaries[0].Source != "checkpoint_barrier_drain" {
		t.Fatalf("publication drain boundaries=%+v", boundaries)
	}
}

func TestLaunchdWorkerUsesSupervisorXDGRoots(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "wrong-state"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "wrong-data"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "wrong-config"))
	root := t.TempDir()
	want := pathspkg.Roots{
		State:  filepath.Join(root, "state", "acd"),
		Share:  filepath.Join(root, "data", "acd"),
		Config: filepath.Join(root, "config", "acd"),
	}
	if err := applyWorkerRootEnvironment(want); err != nil {
		t.Fatal(err)
	}
	got, err := pathspkg.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("worker roots=%+v want %+v", got, want)
	}
}
