//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const finishLegacyWorktreeID = "0123456789abcdef"
const finishLegacyCheckpointID = "cp-1-0123456789abcdef"

// These builders intentionally use the saved legacy DDL and SQL rather than
// current state writers. A newer binary must not upgrade a fixture while making it.
type finishLegacyFixture struct {
	Root, Repo, DBPath, LinkedRepo                       string
	SourceHead, TargetHead, Tree, Blob, CapturedOnlyBlob string
	Env                                                  []string
}

type finishLegacyManifest struct {
	SchemaVersion int
	Refs          string
	Objects       map[string][]byte
	Worktrees     map[string]finishLegacyWorktreeManifest
	Config        map[string][]byte
	Tables        map[string]json.RawMessage
}

type finishLegacyWorktreeManifest struct {
	Head, Branch string
	Files        map[string][]byte
	Index        []byte
}

func newFinishLegacyFixture(t *testing.T, scenario string) *finishLegacyFixture {
	t.Helper()
	f := &finishLegacyFixture{Root: t.TempDir()}
	f.Repo = filepath.Join(f.Root, "repo")
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if name != "HOME" && !strings.HasPrefix(name, "XDG_") && !strings.HasPrefix(name, "GIT_") && !strings.HasPrefix(name, "ACD_") {
			f.Env = append(f.Env, entry)
		}
	}
	f.Env = append(f.Env, "HOME="+filepath.Join(f.Root, "home"),
		"XDG_CONFIG_HOME="+filepath.Join(f.Root, "config"), "XDG_STATE_HOME="+filepath.Join(f.Root, "state"),
		"XDG_DATA_HOME="+filepath.Join(f.Root, "data"), "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=Legacy Author", "GIT_AUTHOR_EMAIL=legacy@example.test",
		"GIT_COMMITTER_NAME=Legacy Committer", "GIT_COMMITTER_EMAIL=committer@example.test",
		"GIT_AUTHOR_DATE=2026-01-01T00:00:00Z", "GIT_COMMITTER_DATE=2026-01-01T00:00:00Z")
	f.git(t, f.Root, "init", "-q", f.Repo)
	f.git(t, f.Repo, "symbolic-ref", "HEAD", "refs/heads/main")
	f.git(t, f.Repo, "config", "commit.gpgsign", "false")
	f.git(t, f.Repo, "config", "core.hooksPath", filepath.Join(f.Root, "no-hooks"))
	writeFile(t, filepath.Join(f.Repo, "kept.txt"), "original\n")
	f.git(t, f.Repo, "add", "--", "kept.txt")
	baseTree := f.gitText(t, f.Repo, "write-tree")
	f.SourceHead = f.gitText(t, f.Repo, "commit-tree", baseTree, "-m", "Seed legacy history")
	f.git(t, f.Repo, "update-ref", "refs/heads/main", f.SourceHead)
	writeFile(t, filepath.Join(f.Repo, "work.txt"), "protected work\n")
	f.Blob = f.gitText(t, f.Repo, "hash-object", "-w", "--", "work.txt")
	f.git(t, f.Repo, "add", "--", "work.txt")
	f.Tree = f.gitText(t, f.Repo, "write-tree")
	f.TargetHead = f.gitText(t, f.Repo, "commit-tree", f.Tree, "-p", f.SourceHead, "-m", "Complete legacy work")
	f.git(t, f.Repo, "reset", "-q", f.SourceHead)
	checkpointRef := "refs/acd/checkpoints/v1/" + finishLegacyWorktreeID + "/" + finishLegacyCheckpointID
	f.git(t, f.Repo, "update-ref", checkpointRef, f.TargetHead)
	// This version exists only in the capture ledger, outside the final checkpoint.
	writeFile(t, filepath.Join(f.Root, "captured-only.txt"), "earlier captured bytes\n")
	f.CapturedOnlyBlob = f.gitText(t, f.Repo, "hash-object", "-w", "--", filepath.Join(f.Root, "captured-only.txt"))

	f.DBPath = filepath.Join(f.Repo, ".git", "acd", "state.db")
	writeFile(t, filepath.Join(f.Root, "config", "acd", "config.json"), `{"commit":{"strategy":"event","format":"imperative"}}`)
	writeFile(t, filepath.Join(f.Repo, ".git", "acd", "config.json"), `{"commit":{"strategy":"intent","format":"conventional"}}`)
	db := f.openSQL(t, false)
	defer db.Close()
	f.execSQL(t, db, finishLegacyDDL(t))
	f.execSQL(t, db, "PRAGMA user_version=30")
	f.execSQL(t, db, `INSERT INTO capture_events(seq,branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state)
VALUES(1,'refs/heads/main',1,?,'create','work.txt','full',1,'pending'),
      (2,'refs/heads/main',1,?,'modify','work.txt','full',2,'pending')`, f.SourceHead, f.SourceHead)
	f.execSQL(t, db, `INSERT INTO capture_ops(event_seq,ord,op,path,before_oid,before_mode,after_oid,after_mode,fidelity)
VALUES(1,0,'create','work.txt',NULL,NULL,?,'100644','full'),
      (2,0,'modify','work.txt',?,'100644',?,'100644','full')`, f.CapturedOnlyBlob, f.CapturedOnlyBlob, f.Blob)
	f.execSQL(t, db, `INSERT INTO operations(id,kind,worktree_id,phase,status,plan_digest,created_ts,updated_ts,completed_ts)
VALUES('checkpoint-op','checkpoint',?,'completed','completed',?,3,4,4)`, finishLegacyWorktreeID, finishLegacyDigest([]byte(f.Tree)))
	f.execSQL(t, db, `INSERT INTO checkpoints(id,seq,operation_id,worktree_id,reason,observation_epoch,coverage_epoch,observed_head,observed_ref,tree_oid,commit_oid,checkpoint_ref,phase,created_ts,completed_ts)
VALUES(?,1,'checkpoint-op',?,'watch',2,2,?,'refs/heads/main',?,?,?,'completed',3,4)`, finishLegacyCheckpointID, finishLegacyWorktreeID, f.SourceHead, f.Tree, f.TargetHead, checkpointRef)
	f.execSQL(t, db, `INSERT INTO checkpoint_events(checkpoint_id,ord,event_seq) VALUES(?,0,1),(?,1,2)`, finishLegacyCheckpointID, finishLegacyCheckpointID)
	f.execSQL(t, db, `INSERT INTO daemon_meta(key,value,updated_ts) VALUES('commit.strategy','intent',4),('commit.format','conventional',4)`)

	switch scenario {
	case "protected-only":
	case "pending-drain", "failed-drain", "staged":
		phase, reason := "semantic", ""
		if scenario == "failed-drain" {
			phase, reason = "needs_action", "legacy provider timeout"
		}
		indexDigest, consent := "", 0
		if scenario == "staged" {
			writeFile(t, filepath.Join(f.Repo, "staged.txt"), "staged-only bytes\n")
			f.git(t, f.Repo, "add", "--", "staged.txt")
			writeFile(t, filepath.Join(f.Repo, "staged.txt"), "later unstaged bytes\n")
			indexDigest, consent = f.indexDigest(t), 1
		}
		f.execSQL(t, db, `INSERT INTO publication_drains(id,checkpoint_id,worktree_id,branch_ref,branch_generation,commit_strategy,commit_format,provider,provider_model,provider_fingerprint,phase,target_event_count,last_error,reason_code,expected_index_digest,staged_consent,created_ts,updated_ts,last_progress_ts)
VALUES('drain',?,?,'refs/heads/main',1,'intent','conventional','legacy-test','recorded-model',?,?,2,?,'',?,?,5,5,5)`, finishLegacyCheckpointID, finishLegacyWorktreeID, finishLegacyDigest([]byte("provider")), phase, reason, indexDigest, consent)
		f.execSQL(t, db, `INSERT INTO publication_drain_events(drain_id,ord,event_seq) VALUES('drain',0,1),('drain',1,2)`)
	case "prepared", "git-applied":
		// Legacy event-only digest: ord, event sequence and nullable candidate flag.
		digest := finishLegacyDigest([]byte("0\x001\x000\x001\x002\x000\x00"))
		f.execSQL(t, db, `INSERT INTO operations(id,kind,worktree_id,phase,status,created_ts,updated_ts) VALUES('publish-op','publication',?,'prepared','prepared',5,5)`, finishLegacyWorktreeID)
		f.execSQL(t, db, `INSERT INTO self_publications(id,operation_id,branch_ref,branch_generation,source_head,target_commit_oid,target_tree_oid,membership_digest,member_count,phase,created_ts,updated_ts,completion_published_ts,completion_candidate_status)
VALUES('publication','publish-op','refs/heads/main',1,?,?,?, ?,2,'prepared',5,5,6,'published')`, f.SourceHead, f.TargetHead, f.Tree, digest)
		f.execSQL(t, db, `INSERT INTO self_publication_members(publication_id,ord,event_seq) VALUES('publication',0,1),('publication',1,2)`)
		if scenario == "git-applied" {
			f.git(t, f.Repo, "update-ref", "refs/heads/main", f.TargetHead, f.SourceHead)
			f.execSQL(t, db, `UPDATE self_publications SET phase='git_applied',git_applied_ts=6,updated_ts=6 WHERE id='publication'`)
			f.execSQL(t, db, `UPDATE operations SET phase='git_applied',status='active',updated_ts=6 WHERE id='publish-op'`)
		}
	case "history-repair", "older-schema":
		f.seedRepair(t, db)
		if scenario == "older-schema" {
			// These are exactly the additive v30 objects, leaving genuine v29 DDL.
			f.execSQL(t, db, `DROP VIEW intent_repair_commit_mappings; DROP TABLE intent_repair_commit_lineage; PRAGMA user_version=29;`)
		}
	case "linked-worktree":
		f.LinkedRepo = filepath.Join(f.Root, "linked")
		f.git(t, f.Repo, "worktree", "add", "-q", "-b", "linked", f.LinkedRepo)
		writeFile(t, filepath.Join(f.LinkedRepo, "linked.txt"), "linked worktree bytes\n")
		f.git(t, f.LinkedRepo, "add", "--", "linked.txt")
	case "partial-checkpoint":
		writeFile(t, filepath.Join(f.Repo, "unreadable.txt"), "not in checkpoint\n")
		f.execSQL(t, db, `UPDATE checkpoints SET coverage_complete=0 WHERE id=?`, finishLegacyCheckpointID)
		f.execSQL(t, db, `INSERT INTO checkpoint_capture_issues(checkpoint_id,path,subtree,reason) VALUES(?,'unreadable.txt',0,'unreadable')`, finishLegacyCheckpointID)
	default:
		t.Fatalf("unknown legacy fixture scenario %q", scenario)
	}
	return f
}

func (f *finishLegacyFixture) seedRepair(t *testing.T, db *sql.DB) {
	t.Helper()
	backup := "refs/acd/intent-repair/legacy-backup"
	f.git(t, f.Repo, "update-ref", backup, f.SourceHead)
	f.execSQL(t, db, `INSERT INTO intent_candidates(id,branch_ref,branch_generation,status,purpose,readiness,created_ts,updated_ts) VALUES('goal','refs/heads/main',1,'ready','complete legacy work','ready',4,4)`)
	f.execSQL(t, db, `INSERT INTO intent_candidate_events(candidate_id,ord,event_seq,event_role,membership_state) VALUES('goal',0,1,'code','active'),('goal',1,2,'code','active')`)
	f.execSQL(t, db, `INSERT INTO intent_repairs(id,branch_ref,branch_generation,status,expected_head,plan_digest,backup_ref,old_head,created_ts,updated_ts) VALUES('repair','refs/heads/main',1,'prepared',?,?,?,?,5,5)`, f.SourceHead, finishLegacyDigest([]byte("repair")), backup, f.SourceHead)
	f.execSQL(t, db, `INSERT INTO intent_repair_commits(repair_id,ord,candidate_id,old_oid) VALUES('repair',0,'goal',?)`, f.SourceHead)
	f.execSQL(t, db, `INSERT INTO intent_repair_members(repair_id,ord,candidate_id,event_seq,prior_state) VALUES('repair',0,'goal',1,'pending'),('repair',1,'goal',2,'pending')`)
	f.execSQL(t, db, `INSERT INTO intent_repair_member_seals(repair_id,membership_mode,member_count) VALUES('repair','frozen',2)`)
}

func (f *finishLegacyFixture) git(t *testing.T, repo string, args ...string) []byte {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir, cmd.Env = repo, f.Env
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("fixture git %v: %v", args, err)
	}
	return out
}

func (f *finishLegacyFixture) gitText(t *testing.T, repo string, args ...string) string {
	t.Helper()
	return strings.TrimSpace(string(f.git(t, repo, args...)))
}

func (f *finishLegacyFixture) openSQL(t *testing.T, readOnly bool) *sql.DB {
	t.Helper()
	mode := "rwc"
	if readOnly {
		mode = "ro"
	}
	db, err := sql.Open("sqlite", "file:"+f.DBPath+"?mode="+mode+"&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	return db
}

func (f *finishLegacyFixture) execSQL(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("seed legacy SQL: %v", err)
	}
}

func (f *finishLegacyFixture) index(t *testing.T, repo string) []byte {
	t.Helper()
	path := f.gitText(t, repo, "rev-parse", "--path-format=absolute", "--git-path", "index")
	return finishLegacyReadFile(t, path)
}

func (f *finishLegacyFixture) indexDigest(t *testing.T) string {
	t.Helper()
	// Match the legacy consent protocol; the manifest separately retains bytes.
	entries := f.git(t, f.Repo, "ls-files", "--stage", "-v", "-z")
	emptyTree := f.gitText(t, f.Repo, "hash-object", "-t", "tree", "--stdin")
	staged := f.git(t, f.Repo, "diff", "--cached", "--raw", "--no-abbrev", "--no-renames", "--no-ext-diff", "--ita-invisible-in-index", "-z", emptyTree)
	return strings.TrimPrefix(finishLegacyDigest(append(entries, staged...)), "sha256:")
}

// manifest reads only. Every ledger table and all object bytes are retained, so
// migration tests can compare the old evidence without relying on current APIs.
func (f *finishLegacyFixture) manifest(t *testing.T) finishLegacyManifest {
	t.Helper()
	m := finishLegacyManifest{Refs: string(f.git(t, f.Repo, "for-each-ref", "--format=%(refname) %(objectname)")), Objects: map[string][]byte{}, Worktrees: map[string]finishLegacyWorktreeManifest{}, Config: map[string][]byte{}, Tables: map[string]json.RawMessage{}}
	for _, object := range strings.Split(f.gitText(t, f.Repo, "cat-file", "--batch-all-objects", "--batch-check=%(objectname) %(objecttype)"), "\n") {
		fields := strings.Fields(object)
		m.Objects[fields[0]] = f.git(t, f.Repo, "cat-file", fields[1], fields[0])
	}
	for name, repo := range map[string]string{"main": f.Repo, "linked": f.LinkedRepo} {
		if repo == "" {
			continue
		}
		w := finishLegacyWorktreeManifest{Head: f.gitText(t, repo, "rev-parse", "HEAD"), Branch: f.gitText(t, repo, "symbolic-ref", "HEAD"), Index: f.index(t, repo), Files: map[string][]byte{}}
		err := filepath.WalkDir(repo, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.Name() == ".git" && entry.IsDir() {
				return filepath.SkipDir
			}
			if !entry.IsDir() {
				rel, err := filepath.Rel(repo, path)
				if err != nil {
					return err
				}
				w.Files[rel] = finishLegacyReadFile(t, path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		m.Worktrees[name] = w
	}
	for _, path := range []string{"repo/.git/config", "repo/.git/acd/config.json", "config/acd/config.json"} {
		m.Config[path] = finishLegacyReadFile(t, filepath.Join(f.Root, path))
	}
	db := f.openSQL(t, true)
	defer db.Close()
	if err := db.QueryRow("PRAGMA user_version").Scan(&m.SchemaVersion); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, table := range tables {
		m.Tables[table] = finishLegacyTableRows(t, db, table)
	}
	return m
}

func finishLegacyTableRows(t *testing.T, db *sql.DB, table string) json.RawMessage {
	t.Helper()
	// Identifiers originate in SQLite, but still quote them for arbitrary tables.
	rows, err := db.Query(`SELECT * FROM "` + strings.ReplaceAll(table, `"`, `""`) + `"`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	values, destinations := make([]any, len(columns)), make([]any, len(columns))
	for i := range values {
		destinations[i] = &values[i]
	}
	var encoded []string
	for rows.Next() {
		if err := rows.Scan(destinations...); err != nil {
			t.Fatal(err)
		}
		row := map[string]any{}
		for i, column := range columns {
			row[column] = values[i]
		}
		data, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, string(data))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(encoded)
	return json.RawMessage("[" + strings.Join(encoded, ",") + "]")
}

func finishLegacyReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func finishLegacyDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func finishLegacyDDL(t *testing.T) string {
	t.Helper()
	if finishLegacyDigest([]byte(finishLegacySchemaV30)) != finishLegacySchemaV30Digest {
		t.Fatal("legacy schema snapshot checksum changed")
	}
	return finishLegacySchemaV30
}

func TestFinishLegacyFixtureRoundTrip(t *testing.T) {
	for _, scenario := range []string{"protected-only", "pending-drain", "failed-drain", "prepared", "git-applied", "history-repair", "staged", "linked-worktree", "partial-checkpoint", "older-schema"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFinishLegacyFixture(t, scenario)
			before := f.manifest(t)
			wantVersion := 30
			if scenario == "older-schema" {
				wantVersion = 29
			}
			if before.SchemaVersion != wantVersion {
				t.Fatalf("version=%d want %d", before.SchemaVersion, wantVersion)
			}
			if !bytes.Equal(before.Objects[f.CapturedOnlyBlob], []byte("earlier captured bytes\n")) {
				t.Fatal("capture-only object lost")
			}
			if !strings.Contains(string(before.Objects[f.SourceHead]), "author Legacy Author <legacy@example.test>") {
				t.Fatal("author provenance lost")
			}
			if !strings.Contains(before.Refs, "refs/acd/checkpoints/v1/"+finishLegacyWorktreeID) {
				t.Fatal("private checkpoint ref missing")
			}
			if !bytes.Equal(before.Worktrees["main"].Files["work.txt"], []byte("protected work\n")) {
				t.Fatal("worktree content changed")
			}
			assertFinishLegacyScenario(t, f, scenario)
			data, err := json.Marshal(before)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.Root, "before.json")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			var saved finishLegacyManifest
			if err := json.Unmarshal(finishLegacyReadFile(t, path), &saved); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, saved) || !reflect.DeepEqual(saved, f.manifest(t)) {
				t.Fatal("read-only reopen or manifest round-trip changed legacy evidence")
			}
		})
	}
}

func assertFinishLegacyScenario(t *testing.T, f *finishLegacyFixture, scenario string) {
	t.Helper()
	db := f.openSQL(t, true)
	defer db.Close()
	var pending int
	if err := db.QueryRow(`SELECT COUNT(*) FROM capture_events WHERE state='pending'`).Scan(&pending); err != nil || pending != 2 {
		t.Fatalf("pending captures=%d err=%v", pending, err)
	}
	query, expected := "", ""
	switch scenario {
	case "pending-drain", "staged":
		query, expected = `SELECT phase FROM publication_drains WHERE id='drain'`, "semantic"
	case "failed-drain":
		query, expected = `SELECT phase FROM publication_drains WHERE id='drain'`, "needs_action"
	case "prepared":
		query, expected = `SELECT phase FROM self_publications WHERE id='publication'`, "prepared"
	case "git-applied":
		query, expected = `SELECT phase FROM self_publications WHERE id='publication'`, "git_applied"
	case "history-repair", "older-schema":
		query, expected = `SELECT membership_mode FROM intent_repair_member_seals WHERE repair_id='repair'`, "frozen"
	case "partial-checkpoint":
		query, expected = `SELECT reason FROM checkpoint_capture_issues`, "unreadable"
	}
	if query != "" {
		var actual string
		if err := db.QueryRow(query).Scan(&actual); err != nil || actual != expected {
			t.Fatalf("%s state=%q want %q err=%v", scenario, actual, expected, err)
		}
	}
	head := f.gitText(t, f.Repo, "rev-parse", "HEAD")
	want := f.SourceHead
	if scenario == "git-applied" {
		want = f.TargetHead
	}
	if head != want {
		t.Fatalf("HEAD=%s want %s", head, want)
	}
	if scenario == "staged" {
		if string(f.git(t, f.Repo, "show", ":staged.txt")) != "staged-only bytes\n" {
			t.Fatal("index version does not differ from live file")
		}
		var digest string
		if err := db.QueryRow(`SELECT expected_index_digest FROM publication_drains`).Scan(&digest); err != nil || digest != f.indexDigest(t) {
			t.Fatalf("approved index digest=%s err=%v", digest, err)
		}
	}
	if scenario == "linked-worktree" && f.gitText(t, f.LinkedRepo, "symbolic-ref", "HEAD") != "refs/heads/linked" {
		t.Fatal("linked worktree is not attached to its own branch")
	}
	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("legacy fixture has foreign key violations")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestFinishLegacyFixtureOlderSchemaUpgrade(t *testing.T) {
	f := newFinishLegacyFixture(t, "older-schema")
	before := f.manifest(t)
	upgraded, err := state.Open(context.Background(), f.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := upgraded.Close(); err != nil {
		t.Fatal(err)
	}
	after := f.manifest(t)
	if after.SchemaVersion != state.SchemaVersion {
		t.Fatalf("upgraded version=%d", after.SchemaVersion)
	}
	if !bytes.Equal(after.Tables["intent_repair_commit_lineage"], before.Tables["intent_repair_commits"]) {
		t.Fatal("older-schema mapping provenance was not copied")
	}
	for table, evidence := range before.Tables {
		if !bytes.Equal(evidence, after.Tables[table]) {
			t.Fatalf("upgrade changed retained table %s", table)
		}
	}
	after.SchemaVersion = before.SchemaVersion
	after.Tables = before.Tables
	if !reflect.DeepEqual(before, after) {
		t.Fatal("schema upgrade changed refs, objects, files, index or configuration")
	}
}

// Exact v30 DDL from internal/state/schema.go at
// 84ed324a6866ec4294319986d302160a774ea9c1, including coverage tables.
// Keep this historical fixture independent of current schema changes.
//
//go:embed testdata/finish-legacy-v30.sql
var finishLegacySchemaV30 string

const finishLegacySchemaV30Digest = "sha256:fc915edaac30a3eac169d5d3fb73cdae5955307de596fec6cbf471901b37997a"
