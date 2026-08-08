package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	pathspkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestChangedCommandCapabilityMatrix(t *testing.T) {
	paths := [][]string{
		{"setup"}, {"status"}, {"on"}, {"off"}, {"history"}, {"restore"},
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

func TestGlobalAndInteractiveFlagsAreExplicitlyRejected(t *testing.T) {
	cases := [][]string{
		{"uninstall", "--repo", ".", "--dry-run"},
		{"repo", "list", "--repo", "."},
		{"repo", "gc", "--repo", "."},
		{"config", "credentials", "--repo", "."},
		{"config", "edit", "--json"},
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
