//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
)

// Old queue and client rows remain inert while current protection makes progress.
func TestStartFromPopulatedStateReachesFirstHeartbeat(t *testing.T) {
	requireSQLite(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	repo := tempRepo(t)
	env := finishLaunchctlEnv(t, withIsolatedHome(t))
	_, _ = prepareCheckpointRegistration(t, env, repo)
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	repo = wt.Root
	dbPath := state.DBPathFromGitDir(wt.GitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = testutil.InitLegacy(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err = db.SQL().ExecContext(ctx, "PRAGMA user_version=32"); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	SeedFlushRequests(t, dbPath, 1000)
	SeedDaemonClients(t, dbPath, 25)
	// Real historical objects make the archived rows valid preservation evidence.
	blob := gitHashObjectStdin(t, repo, "historical protected content\n")
	head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	var sql strings.Builder
	sql.WriteString("BEGIN;")
	for gen := 1; gen <= 3; gen++ {
		for i := 0; i < 800; i++ {
			fmt.Fprintf(&sql, "INSERT INTO shadow_paths(branch_ref,branch_generation,path,operation,mode,oid,base_head,fidelity,updated_ts) VALUES('refs/heads/legacy',%d,'seed/g%d/p%05d.txt','create','100644','%s','%s','exact',1);", gen, gen, i, blob, head)
		}
	}
	sql.WriteString("COMMIT;")
	seed := exec.CommandContext(ctx, "sqlite3", dbPath)
	seed.Stdin = strings.NewReader(sql.String())
	if out, err := seed.CombinedOutput(); err != nil {
		t.Fatalf("seed %v %s", err, out)
	}
	before := sqliteScalar(t, dbPath, "SELECT COUNT(*) || '|' || (SELECT COUNT(*) FROM daemon_clients) || '|' || (SELECT COUNT(*) FROM shadow_paths) FROM flush_requests")
	if before != "1000|25|2400" {
		t.Fatalf("population %s", before)
	}
	writeFile(t, filepath.Join(repo, "startup.txt"), "protect populated work\n")
	start := time.Now()
	ensureCheckpointRuntime(t, env, repo, buildAcdBinary(t))
	waitFor(t, "populated current protection", 10*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM checkpoints WHERE phase='completed' AND coverage_complete=1") != "0"
	})
	if elapsed := time.Since(start); elapsed > 12*time.Second {
		t.Fatalf("populated startup %s", elapsed)
	}
	heartbeat := readHeartbeatTs(repo)
	if heartbeat <= 0 {
		t.Fatal("worker heartbeat missing")
	}
	writeFile(t, filepath.Join(repo, "later.txt"), "new protection remains independent\n")
	hint := runAcd(t, ctx, env, "internal", "hint", "--repo", repo)
	if hint.ExitCode != 0 {
		t.Fatalf("hint %+v", hint)
	}
	waitFor(t, "later checkpoint bytes", 10*time.Second, func() bool {
		ref := sqliteScalar(t, dbPath, "SELECT checkpoint_ref FROM checkpoints WHERE phase='completed' ORDER BY seq DESC LIMIT 1")
		out, err := runGit(repo, "show", ref+":later.txt")
		return err == nil && out == "new protection remains independent\n"
	})
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) || '|' || (SELECT COUNT(*) FROM daemon_clients) || '|' || (SELECT COUNT(*) FROM shadow_paths) FROM flush_requests"); got != before {
		t.Fatalf("archived population mutated %s -> %s", before, got)
	}
	if got := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD")); got != head {
		t.Fatal("startup published old work")
	}
	if readHeartbeatTs(repo) < heartbeat {
		t.Fatal("heartbeat regressed")
	}
}
