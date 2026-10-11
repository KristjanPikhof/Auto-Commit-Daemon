package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// Cross multiple native transaction groups and retain every restart/retry
// proof. Count operations instead of asserting host-dependent timings.
func TestFinishLegacyInventoryRestartUsesBoundedBatches(t *testing.T) {
	const commits, blobs = 129, 256
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	repo, wt := migrationRepo(t, ctx)
	var input strings.Builder
	for i := 1; i <= blobs; i++ {
		body := fmt.Sprintf("original captured version %d\n", i)
		fmt.Fprintf(&input, "blob\nmark :%d\ndata %d\n%s\n", i, len(body), body)
	}
	for i := 1; i <= commits; i++ {
		body := fmt.Sprintf("Original goal %d\n", i)
		fmt.Fprintf(&input, "commit refs/heads/seed\nmark :%d\nauthor Original <original@example.com> 1700000000 +0000\ncommitter Original <original@example.com> 1700000000 +0000\ndata %d\n%s", 10000+i, len(body), body)
		if i > 1 {
			fmt.Fprintf(&input, "from :%d\n", 10000+i-1)
		}
		input.WriteString("\n")
	}
	marks := filepath.Join(t.TempDir(), "marks")
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo, Stdin: strings.NewReader(input.String())}, "fast-import", "--quiet", "--export-marks="+marks); err != nil {
		t.Fatal(err)
	}
	rawMarks, err := os.ReadFile(marks)
	if err != nil {
		t.Fatal(err)
	}
	identities := make(map[int]string)
	for _, line := range strings.Split(strings.TrimSpace(string(rawMarks)), "\n") {
		fields := strings.Fields(line)
		mark, err := strconv.Atoi(strings.TrimPrefix(fields[0], ":"))
		if err != nil {
			t.Fatal(err)
		}
		identities[mark] = fields[1]
	}
	if _, err = gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "update-ref", "-d", "refs/heads/seed"); err != nil {
		t.Fatal(err)
	}
	if _, err = gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "read-tree", "--empty"); err != nil {
		t.Fatal(err)
	}
	beforeIndex, err := os.ReadFile(filepath.Join(wt.GitDir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	live := []byte("later live work\n")
	if err = os.WriteFile(filepath.Join(repo, "later.txt"), live, 0600); err != nil {
		t.Fatal(err)
	}
	dbPath := state.DBPathFromGitDir(wt.GitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { db.Close() }()
	if _, err = db.SQL().ExecContext(ctx, state.LegacySchemaDDL); err != nil {
		t.Fatal(err)
	}
	tx, err := db.SQL().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= blobs; i++ {
		if _, err = tx.ExecContext(ctx, `INSERT INTO capture_events(seq,branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts) VALUES(?,'refs/heads/main',1,'','create',?,'exact',1)`, i, fmt.Sprintf("old-%d.txt", i)); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO capture_ops(event_seq,ord,op,path,after_oid,after_mode,fidelity) VALUES(?,0,'create',? ,?,'100644','exact')`, i, fmt.Sprintf("old-%d.txt", i), identities[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	plan := state.IntentHistoryPlan{Version: 1, ID: "large", ExpectedHead: identities[10000+commits]}
	for i := 1; i <= commits; i++ {
		plan.SourceChain = append(plan.SourceChain, identities[10000+i])
	}
	rawPlan, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if err = state.MetaSetMany(ctx, db, map[string]string{"intent.history.plan.large": string(rawPlan), "intent.history.plan.broken": "{"}); err != nil {
		t.Fatal(err)
	}
	originalRows := finishLegacyCaptureRows(t, ctx, db)

	// Count real Git subprocesses; the shim only forwards to the original Git.
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "git-calls")
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
	shim := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> " + quote(logPath) + "\nexec " + quote(realGit) + " \"$@\"\n"
	if err = os.WriteFile(filepath.Join(bin, "git"), []byte(shim), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	creationLog := filepath.Join(t.TempDir(), "native-creations")
	hook := "#!/bin/sh\nif [ \"$1\" = committed ]; then\n while read old new ref; do\n  case \"$old:$ref\" in 0000000000000000000000000000000000000000:refs/acd/migration/*) printf '%s\\n' \"$ref\" >> " + quote(creationLog) + ";; esac\n done\nfi\n"
	if err := os.WriteFile(filepath.Join(wt.GitDir, "hooks", "reference-transaction"), []byte(hook), 0755); err != nil {
		t.Fatal(err)
	}
	activate := func(label string, attention bool, wantCreates int) state.FinishLegacyCutover {
		t.Helper()
		if err := os.WriteFile(logPath, nil, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(creationLog, nil, 0600); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		activationCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		result, err := ActivateFinish(activationCtx, repo, db)
		if attention {
			if !errors.Is(err, ErrFinishLegacyAttention) || result.State != "needs_attention" || result.ReasonCode != "legacy_inventory_invalid" || result.SnapshotRef == "" {
				t.Fatalf("%s attention %+v: %v", label, result, err)
			}
		} else if err != nil || result.State != "completed" {
			t.Fatalf("%s completion %+v: %v", label, result, err)
		}
		calls, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		created, err := os.ReadFile(creationLog)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(strings.Fields(string(created))); got != wantCreates {
			t.Fatalf("%s recreated retained pins: got %d native creations, want %d", label, got, wantCreates)
		}
		lines := strings.Split(strings.TrimSpace(string(calls)), "\n")
		transactions := strings.Count(string(calls), "update-ref --no-deref --stdin")
		objectBatches := strings.Count(string(calls), "cat-file --batch-check=")
		groups := (commits + 1 + 63) / 64
		if len(lines) > groups*12+80 || transactions > groups*3+5 || objectBatches > groups*3+12 || strings.Count(string(calls), "ls-tree ") > 1 {
			t.Fatalf("%s repeated per-object work: %d Git calls, %d transactions, %d object batches", label, len(lines), transactions, objectBatches)
		}
		t.Logf("%s: %d commits, %d blobs; elapsed %s; %d Git calls, %d native transactions, %d object batches", label, commits, blobs, time.Since(start), len(lines), transactions, objectBatches)
		return result
	}
	first := activate("initial invalid inventory", true, commits+1)
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = state.OpenRuntime(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	restarted := activate("worker restart with recorded failure", true, 0)
	if restarted.SnapshotRef != first.SnapshotRef || restarted.SnapshotOID != first.SnapshotOID {
		t.Fatal("restart replaced immutable proof")
	}
	pins, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "for-each-ref", "--format=%(refname)", "refs/acd/migration/")
	if err != nil || len(strings.Fields(string(pins))) != commits+1 {
		t.Fatalf("available commits were not all pinned: %v", err)
	}
	afterIndex, err := os.ReadFile(filepath.Join(wt.GitDir, "index"))
	if err != nil || !bytes.Equal(beforeIndex, afterIndex) {
		t.Fatal("activation changed native index")
	}
	body, err := os.ReadFile(filepath.Join(repo, "later.txt"))
	if err != nil || !bytes.Equal(body, live) {
		t.Fatal("activation changed live bytes")
	}
	if got := finishLegacyCaptureRows(t, ctx, db); got != originalRows {
		t.Fatal("activation rewrote original provenance")
	}
	summary, err := daemon.ProtectWorktree(ctx, repo, db, daemon.CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 1}, daemon.CaptureOpts{CheckpointStore: &checkpoint.Store{DB: db}, WorktreeID: checkpoint.WorktreeID(repo)})
	if err != nil || !summary.Protected || summary.Partial {
		t.Fatalf("recorded failure blocked ordinary protection: %+v %v", summary, err)
	}
	activate("failure restart after later protection", true, 2)
	pins, err = gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "for-each-ref", "--format=%(refname)", "refs/acd/migration/")
	if err != nil || len(strings.Fields(string(pins))) != commits+3 {
		t.Fatalf("later checkpoint duplicated legacy commit pins: %v", err)
	}
	if _, err = state.MetaDelete(ctx, db, "intent.history.plan.broken"); err != nil {
		t.Fatal(err)
	}
	activate("explicit repaired evidence retry", false, 0)
	activate("completed worker startup", false, 0)
}
