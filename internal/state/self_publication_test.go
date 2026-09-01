package state

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestSelfPublicationSchemaMigrationAndIdempotentReopen(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	d, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQL().ExecContext(ctx, `
INSERT INTO daemon_meta(key, value, updated_ts)
VALUES ('v17.keep', 'yes', 1)`); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	downgradeSelfPublicationFixtureToV17(t, dbPath)

	migrated, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open migrated v17: %v", err)
	}
	if version, err := migrated.UserVersion(ctx); err != nil ||
		version != SchemaVersion {
		t.Fatalf("migrated user_version=(%d,%v), want (%d,nil)",
			version, err, SchemaVersion)
	}
	var keep string
	if err := migrated.SQL().QueryRowContext(ctx,
		`SELECT value FROM daemon_meta WHERE key='v17.keep'`).Scan(&keep); err != nil || keep != "yes" {
		t.Fatalf("preserved v17 row=%q err=%v", keep, err)
	}
	assertSelfPublicationSchema(t, migrated.SQL())
	migratedSeq := appendSelfPublicationEvent(
		t, migrated, "refs/heads/unborn-migrated", 0, "initial.go")
	migratedUnborn := SelfPublication{
		ID:               "migrated-unborn",
		BranchRef:        "refs/heads/unborn-migrated",
		BranchGeneration: 0,
		SourceHead:       "",
		TargetCommitOID:  "initial-target",
		TargetTreeOID:    "initial-tree",
		Members:          []SelfPublicationMember{{EventSeq: migratedSeq}},
	}
	if created, err := PrepareSelfPublication(
		ctx, migrated, migratedUnborn); err != nil || !created {
		t.Fatalf("prepare migrated unborn publication=(%v,%v)", created, err)
	}
	if got, ok, err := SelfPublicationByID(
		ctx, migrated, migratedUnborn.ID); err != nil || !ok ||
		got.SourceHead != "" {
		t.Fatalf("migrated unborn publication=%+v ok=%v err=%v", got, ok, err)
	}
	if err := migrated.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("idempotent reopen: %v", err)
	}
	defer reopened.Close()
	assertSelfPublicationSchema(t, reopened.SQL())
}

func TestSelfPublicationPrepareAtomicallyLinksOperation(t *testing.T) {
	ctx := context.Background()
	d, err := Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	seq := appendSelfPublicationEvent(t, d, "refs/heads/main", 1, "linked.txt")
	publication := SelfPublication{
		ID: "linked-publication", BranchRef: "refs/heads/main", BranchGeneration: 1,
		SourceHead: "source", TargetCommitOID: "target", TargetTreeOID: "tree",
		Members: []SelfPublicationMember{{EventSeq: seq}},
	}
	if created, err := PrepareSelfPublication(ctx, d, publication); err != nil || !created {
		t.Fatalf("PrepareSelfPublication=(%t,%v)", created, err)
	}
	loaded, ok, err := SelfPublicationByID(ctx, d, publication.ID)
	if err != nil || !ok || !loaded.OperationID.Valid {
		t.Fatalf("publication=%+v ok=%t err=%v", loaded, ok, err)
	}
	var kind, phase, status, digest string
	if err := d.SQL().QueryRowContext(ctx, `
SELECT kind,phase,status,plan_digest FROM operations WHERE id=?`,
		loaded.OperationID.String).Scan(&kind, &phase, &status, &digest); err != nil {
		t.Fatal(err)
	}
	if kind != "self_publication" || phase != OperationPrepared ||
		status != OperationPrepared || digest != loaded.MembershipDigest {
		t.Fatalf("operation=(%q,%q,%q,%q)", kind, phase, status, digest)
	}
}

func TestSelfPublicationByIDBoundedCapsMembershipEvidence(t *testing.T) {
	ctx := context.Background()
	d, _ := openTestDB(t)
	first := appendSelfPublicationEvent(
		t, d, "refs/heads/main", 1, "first.txt")
	second := appendSelfPublicationEvent(
		t, d, "refs/heads/main", 1, "second.txt")
	publication := SelfPublication{
		ID: "bounded-publication", BranchRef: "refs/heads/main",
		BranchGeneration: 1, SourceHead: "source",
		TargetCommitOID: "target", TargetTreeOID: "tree",
		Members: []SelfPublicationMember{
			{EventSeq: first}, {EventSeq: second},
		},
	}
	if created, err := PrepareSelfPublication(
		ctx, d, publication); err != nil || !created {
		t.Fatalf("PrepareSelfPublication=(%t,%v)", created, err)
	}
	got, ok, rows, err := SelfPublicationByIDBounded(
		ctx, d, publication.ID, 2)
	if err != nil || !ok || rows != 2 || len(got.Members) != 2 {
		t.Fatalf("bounded publication=%+v ok=%t rows=%d err=%v",
			got, ok, rows, err)
	}
	if _, _, _, err := SelfPublicationByIDBounded(
		ctx, d, publication.ID, 1); !errors.Is(err, ErrCompletedBranchTransitionProof) {
		t.Fatalf("member overflow err=%v want completed-transition proof", err)
	}
}

func TestSelfPublicationOperationTransitionsRequireExactCorrelation(t *testing.T) {
	newPublication := func(t *testing.T, id string) (*DB, SelfPublication, int64, string) {
		t.Helper()
		db, _ := openTestDB(t)
		seq := appendSelfPublicationEvent(t, db, "refs/heads/main", 1, id+".txt")
		publication := SelfPublication{
			ID: id, BranchRef: "refs/heads/main", BranchGeneration: 1,
			SourceHead: "source", TargetCommitOID: "target", TargetTreeOID: "tree",
			Members: []SelfPublicationMember{{EventSeq: seq}},
		}
		if created, err := PrepareSelfPublication(context.Background(), db, publication); err != nil || !created {
			t.Fatalf("PrepareSelfPublication=(%t,%v)", created, err)
		}
		loaded, ok, err := SelfPublicationByID(context.Background(), db, id)
		if err != nil || !ok || !loaded.OperationID.Valid {
			t.Fatalf("publication=%+v ok=%t err=%v", loaded, ok, err)
		}
		return db, publication, seq, loaded.OperationID.String
	}

	t.Run("git applied", func(t *testing.T) {
		db, publication, _, operationID := newPublication(t, "operation-mark")
		if _, err := db.SQL().Exec(`UPDATE operations SET status='completed' WHERE id=?`, operationID); err != nil {
			t.Fatal(err)
		}
		if applied, err := MarkSelfPublicationGitApplied(context.Background(), db, publication, 2); applied || !errors.Is(err, ErrSelfPublicationOwnershipChanged) {
			t.Fatalf("MarkSelfPublicationGitApplied=(%t,%v)", applied, err)
		}
		loaded, ok, err := SelfPublicationByID(context.Background(), db, publication.ID)
		if err != nil || !ok || loaded.Phase != SelfPublicationPrepared {
			t.Fatalf("publication after rejected transition=%+v ok=%t err=%v", loaded, ok, err)
		}
	})

	t.Run("completed", func(t *testing.T) {
		db, publication, seq, operationID := newPublication(t, "operation-complete")
		if applied, err := MarkSelfPublicationGitApplied(context.Background(), db, publication, 2); err != nil || !applied {
			t.Fatalf("MarkSelfPublicationGitApplied=(%t,%v)", applied, err)
		}
		if _, err := db.SQL().Exec(`UPDATE operations SET status='rolled_back' WHERE id=?`, operationID); err != nil {
			t.Fatal(err)
		}
		if completed, err := CompleteSelfPublication(context.Background(), db, publication,
			SelfPublicationCompletion{PublishedTS: 3}); completed ||
			!errors.Is(err, ErrSelfPublicationOwnershipChanged) {
			t.Fatalf("CompleteSelfPublication=(%t,%v)", completed, err)
		}
		loaded, ok, err := SelfPublicationByID(context.Background(), db, publication.ID)
		if err != nil || !ok || loaded.Phase != SelfPublicationGitApplied {
			t.Fatalf("publication after rejected completion=%+v ok=%t err=%v", loaded, ok, err)
		}
		var eventState string
		if err := db.SQL().QueryRow(`SELECT state FROM capture_events WHERE seq=?`, seq).Scan(&eventState); err != nil {
			t.Fatal(err)
		}
		if eventState != EventStatePending {
			t.Fatalf("event state=%q, want pending rollback", eventState)
		}
	})
}

func TestSelfPublicationV18CompletionMigrationIsIdempotent(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	raw, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`
CREATE TABLE self_publications(
    id TEXT PRIMARY KEY,
    branch_ref TEXT NOT NULL,
    branch_generation INTEGER NOT NULL,
    source_head TEXT NOT NULL,
    target_commit_oid TEXT NOT NULL,
    target_tree_oid TEXT NOT NULL,
    membership_digest TEXT NOT NULL,
    member_count INTEGER NOT NULL,
    phase TEXT NOT NULL,
    created_ts REAL NOT NULL,
    updated_ts REAL NOT NULL,
    git_applied_ts REAL,
    completed_ts REAL,
    abandoned_ts REAL,
    error TEXT NOT NULL DEFAULT ''
);
CREATE TABLE self_publication_members(
    publication_id TEXT NOT NULL,
    ord INTEGER NOT NULL,
    event_seq INTEGER NOT NULL,
    candidate_id TEXT,
    PRIMARY KEY (publication_id, ord)
);
CREATE TABLE daemon_meta(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_ts REAL NOT NULL
);
CREATE TRIGGER self_publications_identity_immutable
BEFORE UPDATE OF branch_ref, branch_generation, source_head,
                 target_commit_oid, target_tree_oid, membership_digest,
                 member_count
ON self_publications
BEGIN
    SELECT RAISE(ABORT, 'self-publication identity is immutable');
END;
INSERT INTO self_publications(
    id, branch_ref, branch_generation, source_head, target_commit_oid,
    target_tree_oid, membership_digest, member_count, phase, created_ts,
    updated_ts, git_applied_ts
) VALUES
    ('v18-prepared', 'refs/heads/main', 1, 'source-1', 'target-1',
     'tree-1', 'sha256:0000000000000000000000000000000000000000000000000000000000000000',
     1, 'prepared', 10, 10, NULL),
    ('v18-applied', 'refs/heads/main', 1, 'source-2', 'target-2',
     'tree-2', 'sha256:1111111111111111111111111111111111111111111111111111111111111111',
     1, 'git_applied', 20, 21, 21);
INSERT INTO self_publication_members(
    publication_id, ord, event_seq
) VALUES ('v18-prepared', 0, 1), ('v18-applied', 0, 2);
PRAGMA user_version=18;`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	projection, err := LoadSelfPublicationStateReadOnly(ctx, dbPath)
	if err != nil || !projection.Available ||
		projection.SchemaVersion != 18 ||
		len(projection.Recoverable) != 2 ||
		projection.Recoverable[0].Completion.CandidateStatus !=
			SelfPublicationCompletionUnknown ||
		projection.Recoverable[1].Completion.CandidateStatus !=
			SelfPublicationCompletionUnknown ||
		projection.UnknownRecoverable == nil ||
		projection.UnknownRecoverable.ID != "v18-prepared" {
		t.Fatalf("v18 read-only projection=%+v err=%v", projection, err)
	}

	migrated, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open v18 fixture: %v", err)
	}
	assertSelfPublicationCompletionColumns(t, migrated.SQL())
	for _, id := range []string{"v18-prepared", "v18-applied"} {
		got, ok, err := SelfPublicationByID(ctx, migrated, id)
		if err != nil || !ok ||
			got.Completion.CandidateStatus !=
				SelfPublicationCompletionUnknown ||
			got.Completion.PublishedTS != 0 ||
			got.Completion.SoftPublicationDeadline.Valid {
			t.Fatalf("migrated %s=%+v ok=%v err=%v", id, got, ok, err)
		}
	}
	if unknown, ok, err := FirstUnknownRecoverableSelfPublication(
		ctx, migrated); err != nil || !ok || unknown.ID != "v18-prepared" {
		t.Fatalf("first migrated unknown=%+v ok=%v err=%v",
			unknown, ok, err)
	}
	if version, err := migrated.UserVersion(ctx); err != nil ||
		version != SchemaVersion {
		t.Fatalf("version=(%d,%v), want %d", version, err, SchemaVersion)
	}
	if err := migrated.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("idempotent reopen: %v", err)
	}
	defer reopened.Close()
	assertSelfPublicationCompletionColumns(t, reopened.SQL())
	got, ok, err := SelfPublicationByID(ctx, reopened, "v18-applied")
	if err != nil || !ok ||
		got.Completion.CandidateStatus != SelfPublicationCompletionUnknown {
		t.Fatalf("reopened v18-applied=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestReadOnlySelfPublicationV17DoesNotMigrate(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	d, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	downgradeSelfPublicationFixtureToV17(t, dbPath)

	projection, err := LoadSelfPublicationStateReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Available || projection.SchemaVersion != 17 {
		t.Fatalf("projection=%+v, want unavailable v17", projection)
	}
	raw, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var version, tables int
	if err := raw.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := raw.QueryRowContext(ctx, `
SELECT COUNT(*) FROM sqlite_master
WHERE type='table' AND name LIKE 'self_publication%'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if version != 17 || tables != 0 {
		t.Fatalf("read-only inspection mutated db: version=%d tables=%d", version, tables)
	}
}

func TestSelfPublicationPhaseCASAndEventCompletion(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	seq := appendSelfPublicationEvent(t, d, "refs/heads/main", 4, "event.go")
	if err := RecordPlannerOffer(ctx, d, seq, 2); err != nil {
		t.Fatal(err)
	}
	publication := SelfPublication{
		ID:               "event-publication",
		BranchRef:        "refs/heads/main",
		BranchGeneration: 4,
		SourceHead:       "source",
		TargetCommitOID:  "target",
		TargetTreeOID:    "tree",
		Members: []SelfPublicationMember{{
			EventSeq: seq,
		}},
	}
	created, err := PrepareSelfPublication(ctx, d, publication)
	if err != nil || !created {
		t.Fatalf("PrepareSelfPublication=(%v,%v)", created, err)
	}
	created, err = PrepareSelfPublication(ctx, d, publication)
	if err != nil || created {
		t.Fatalf("idempotent prepare=(%v,%v)", created, err)
	}
	mismatched := publication
	mismatched.TargetTreeOID = "other-tree"
	if _, err := PrepareSelfPublication(ctx, d, mismatched); !errors.Is(err, ErrSelfPublicationIdentityMismatch) {
		t.Fatalf("mismatched prepare err=%v", err)
	}
	if _, err := CompleteSelfPublication(ctx, d, publication,
		SelfPublicationCompletion{PublishedTS: 3}); !errors.Is(err, ErrSelfPublicationPhaseConflict) {
		t.Fatalf("complete before git apply err=%v", err)
	}
	applied, err := MarkSelfPublicationGitApplied(ctx, d, publication, 4)
	if err != nil || !applied {
		t.Fatalf("MarkSelfPublicationGitApplied=(%v,%v)", applied, err)
	}
	applied, err = MarkSelfPublicationGitApplied(ctx, d, publication, 5)
	if err != nil || applied {
		t.Fatalf("idempotent git apply=(%v,%v)", applied, err)
	}
	if _, err := AbandonSelfPublication(ctx, d, publication, "too late", 6); !errors.Is(err, ErrSelfPublicationPhaseConflict) {
		t.Fatalf("abandon after git apply err=%v", err)
	}
	recoverable, err := RecoverableSelfPublications(ctx, d, 10)
	if err != nil || len(recoverable) != 1 ||
		recoverable[0].Phase != SelfPublicationGitApplied {
		t.Fatalf("recoverable=%+v err=%v", recoverable, err)
	}

	completed, err := CompleteSelfPublication(ctx, d, publication,
		SelfPublicationCompletion{
			PublishedTS: 7,
			Message:     sql.NullString{String: "published", Valid: true},
		})
	if err != nil || !completed {
		t.Fatalf("CompleteSelfPublication=(%v,%v)", completed, err)
	}
	completed, err = CompleteSelfPublication(ctx, d, publication,
		SelfPublicationCompletion{PublishedTS: 8})
	if err != nil || completed {
		t.Fatalf("idempotent completion=(%v,%v)", completed, err)
	}
	assertSelfPublicationCompletedEvent(t, d, seq, "target")
	if _, ok, err := PlannerStateForEvent(ctx, d, seq); err != nil || ok {
		t.Fatalf("planner state after completion ok=%v err=%v", ok, err)
	}
	assertSelfPublicationMeta(t, d, "branch.generation", "4")
	assertSelfPublicationMeta(t, d, "branch.head", "target")
	assertSelfPublicationMeta(t, d, "branch_token",
		"rev:target refs/heads/main")
	publish, ok, err := LoadPublishState(ctx, d)
	if err != nil || !ok || publish.EventSeq.Int64 != seq ||
		publish.SourceHead.String != "source" ||
		publish.TargetCommitOID.String != "target" ||
		publish.Status != "published" {
		t.Fatalf("publish_state=%+v ok=%v err=%v", publish, ok, err)
	}
}

func TestPrepareSelfPublicationRejectsUnknownCompletion(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	seq := appendSelfPublicationEvent(
		t, d, "refs/heads/main", 1, "unknown.go")
	publication := SelfPublication{
		ID: "unknown-completion", BranchRef: "refs/heads/main",
		BranchGeneration: 1, SourceHead: "source",
		TargetCommitOID: "target", TargetTreeOID: "tree",
		Completion: SelfPublicationCompletion{
			CandidateStatus: SelfPublicationCompletionUnknown,
		},
		Members: []SelfPublicationMember{{EventSeq: seq}},
	}
	if _, err := PrepareSelfPublication(
		ctx, d, publication); err == nil ||
		!strings.Contains(err.Error(), "invalid self-publication completion") {
		t.Fatalf("PrepareSelfPublication unknown completion err=%v", err)
	}
}

func TestSelfPublicationUnbornSourceFreshSchemaPrepareAndComplete(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	seq := appendSelfPublicationEvent(
		t, d, "refs/heads/unborn", 0, "initial.go")
	publication := SelfPublication{
		ID:               "unborn-publication",
		BranchRef:        "refs/heads/unborn",
		BranchGeneration: 0,
		SourceHead:       "",
		TargetCommitOID:  "initial-commit",
		TargetTreeOID:    "initial-tree",
		Members:          []SelfPublicationMember{{EventSeq: seq}},
	}
	if created, err := PrepareSelfPublication(
		ctx, d, publication); err != nil || !created {
		t.Fatalf("PrepareSelfPublication unborn=(%v,%v)", created, err)
	}
	if applied, err := MarkSelfPublicationGitApplied(
		ctx, d, publication, 2); err != nil || !applied {
		t.Fatalf("MarkSelfPublicationGitApplied unborn=(%v,%v)", applied, err)
	}
	if completed, err := CompleteSelfPublication(
		ctx, d, publication,
		SelfPublicationCompletion{PublishedTS: 3},
	); err != nil || !completed {
		t.Fatalf("CompleteSelfPublication unborn=(%v,%v)", completed, err)
	}
	got, ok, err := SelfPublicationByID(ctx, d, publication.ID)
	if err != nil || !ok || got.SourceHead != "" ||
		got.Phase != SelfPublicationCompleted {
		t.Fatalf("unborn journal=%+v ok=%v err=%v", got, ok, err)
	}
	publish, ok, err := LoadPublishState(ctx, d)
	if err != nil || !ok || !publish.SourceHead.Valid ||
		publish.SourceHead.String != "" ||
		publish.TargetCommitOID.String != publication.TargetCommitOID {
		t.Fatalf("unborn publish_state=%+v ok=%v err=%v", publish, ok, err)
	}
	assertSelfPublicationCompletedEvent(
		t, d, seq, publication.TargetCommitOID)
}

func TestSelfPublicationCandidateCompletionAndRollback(t *testing.T) {
	t.Run("candidate soft publication", func(t *testing.T) {
		d, _ := openTestDB(t)
		ctx := context.Background()
		seq1 := appendSelfPublicationEvent(t, d, "refs/heads/main", 9, "one.go")
		seq2 := appendSelfPublicationEvent(t, d, "refs/heads/main", 9, "two.go")
		if err := SaveIntentCandidate(ctx, d, IntentCandidate{
			ID:               "candidate",
			BranchRef:        "refs/heads/main",
			BranchGeneration: 9,
			Status:           IntentCandidateReady,
			Readiness:        IntentReadinessReady,
			Events: []IntentCandidateEvent{
				{EventSeq: seq1, EventRole: "code"},
				{EventSeq: seq2, EventRole: "test"},
			},
		}); err != nil {
			t.Fatal(err)
		}
		publication := SelfPublication{
			ID:               "intent-publication",
			BranchRef:        "refs/heads/main",
			BranchGeneration: 9,
			SourceHead:       "source-9",
			TargetCommitOID:  "target-9",
			TargetTreeOID:    "tree-9",
			Completion: SelfPublicationCompletion{
				PublishedTS:             11,
				CandidateStatus:         IntentCandidateSoftPublished,
				SoftPublicationDeadline: sql.NullFloat64{Float64: 20, Valid: true},
			},
			Members: []SelfPublicationMember{
				{EventSeq: seq1, CandidateID: sql.NullString{String: "candidate", Valid: true}},
				{EventSeq: seq2, CandidateID: sql.NullString{String: "candidate", Valid: true}},
			},
		}
		if created, err := PrepareSelfPublication(ctx, d, publication); err != nil || !created {
			t.Fatalf("prepare=(%v,%v)", created, err)
		}
		if applied, err := MarkSelfPublicationGitApplied(ctx, d, publication, 10); err != nil || !applied {
			t.Fatalf("git applied=(%v,%v)", applied, err)
		}
		if completed, err := CompleteSelfPublication(ctx, d, publication,
			SelfPublicationCompletion{
				PublishedTS:             11,
				CandidateStatus:         IntentCandidateSoftPublished,
				SoftPublicationDeadline: sql.NullFloat64{Float64: 20, Valid: true},
			}); err != nil || !completed {
			t.Fatalf("complete=(%v,%v)", completed, err)
		}
		candidate, ok, err := IntentCandidateByID(ctx, d, "candidate")
		if err != nil || !ok || candidate.Status != IntentCandidateSoftPublished ||
			candidate.PublishedCommitOID.String != "target-9" ||
			candidate.SoftPublicationDeadline.Float64 != 20 {
			t.Fatalf("candidate=%+v ok=%v err=%v", candidate, ok, err)
		}
		assertSelfPublicationCompletedEvent(t, d, seq1, "target-9")
		assertSelfPublicationCompletedEvent(t, d, seq2, "target-9")
	})

	t.Run("ownership change rolls back all completion writes", func(t *testing.T) {
		d, _ := openTestDB(t)
		ctx := context.Background()
		seq1 := appendSelfPublicationEvent(t, d, "refs/heads/main", 2, "one.go")
		seq2 := appendSelfPublicationEvent(t, d, "refs/heads/main", 2, "two.go")
		publication := SelfPublication{
			ID:               "rollback-publication",
			BranchRef:        "refs/heads/main",
			BranchGeneration: 2,
			SourceHead:       "source",
			TargetCommitOID:  "target",
			TargetTreeOID:    "tree",
			Members: []SelfPublicationMember{
				{EventSeq: seq1},
				{EventSeq: seq2},
			},
		}
		if _, err := PrepareSelfPublication(ctx, d, publication); err != nil {
			t.Fatal(err)
		}
		if _, err := MarkSelfPublicationGitApplied(ctx, d, publication, 3); err != nil {
			t.Fatal(err)
		}
		if _, err := d.SQL().ExecContext(ctx,
			`UPDATE capture_events SET state='failed' WHERE seq=?`, seq2); err != nil {
			t.Fatal(err)
		}
		if _, err := CompleteSelfPublication(ctx, d, publication,
			SelfPublicationCompletion{PublishedTS: 4}); !errors.Is(err, ErrSelfPublicationOwnershipChanged) {
			t.Fatalf("completion ownership error=%v", err)
		}
		var state1, state2 string
		if err := d.SQL().QueryRowContext(ctx,
			`SELECT state FROM capture_events WHERE seq=?`, seq1).Scan(&state1); err != nil {
			t.Fatal(err)
		}
		if err := d.SQL().QueryRowContext(ctx,
			`SELECT state FROM capture_events WHERE seq=?`, seq2).Scan(&state2); err != nil {
			t.Fatal(err)
		}
		if state1 != EventStatePending || state2 != EventStateFailed {
			t.Fatalf("rollback states=(%s,%s)", state1, state2)
		}
		got, ok, err := SelfPublicationByID(ctx, d, publication.ID)
		if err != nil || !ok || got.Phase != SelfPublicationGitApplied {
			t.Fatalf("journal after rollback=%+v ok=%v err=%v", got, ok, err)
		}
		if _, ok, err := LoadPublishState(ctx, d); err != nil || ok {
			t.Fatalf("publish_state after rollback ok=%v err=%v", ok, err)
		}
		if _, ok, err := MetaGet(ctx, d, "branch.head"); err != nil || ok {
			t.Fatalf("branch.head after rollback ok=%v err=%v", ok, err)
		}
	})
}

func TestSelfPublicationAbandonAndImmutableIdentity(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	seq := appendSelfPublicationEvent(t, d, "refs/heads/main", 1, "abandon.go")
	publication := SelfPublication{
		ID:               "abandoned-publication",
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		SourceHead:       "source",
		TargetCommitOID:  "target",
		TargetTreeOID:    "tree",
		Members:          []SelfPublicationMember{{EventSeq: seq}},
	}
	if _, err := PrepareSelfPublication(ctx, d, publication); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQL().ExecContext(ctx, `
UPDATE self_publications SET source_head='changed' WHERE id=?`,
		publication.ID); err == nil {
		t.Fatal("identity UPDATE succeeded")
	}
	if _, err := d.SQL().ExecContext(ctx, `
UPDATE self_publications SET phase='completed' WHERE id=?`,
		publication.ID); err == nil {
		t.Fatal("prepared -> completed phase skip succeeded")
	}
	if _, err := d.SQL().ExecContext(ctx, `
DELETE FROM self_publication_members WHERE publication_id=?`,
		publication.ID); err == nil {
		t.Fatal("membership DELETE succeeded")
	}
	abandoned, err := AbandonSelfPublication(ctx, d, publication, "CAS not attempted", 5)
	if err != nil || !abandoned {
		t.Fatalf("AbandonSelfPublication=(%v,%v)", abandoned, err)
	}
	abandoned, err = AbandonSelfPublication(ctx, d, publication, "repeat", 6)
	if err != nil || abandoned {
		t.Fatalf("idempotent abandon=(%v,%v)", abandoned, err)
	}
	got, ok, err := SelfPublicationByID(ctx, d, publication.ID)
	if err != nil || !ok || got.Phase != SelfPublicationAbandoned ||
		!got.AbandonedTS.Valid || got.Error != "CAS not attempted" {
		t.Fatalf("abandoned publication=%+v ok=%v err=%v", got, ok, err)
	}
	recoverable, err := RecoverableSelfPublications(ctx, d, 10)
	if err != nil || len(recoverable) != 0 {
		t.Fatalf("recoverable after abandon=%+v err=%v", recoverable, err)
	}
	pruned, err := PruneTerminalSelfPublicationsBefore(ctx, d, 6, 1)
	if err != nil || pruned != 1 {
		t.Fatalf("PruneTerminalSelfPublicationsBefore=(%d,%v)", pruned, err)
	}
	if _, ok, err := SelfPublicationByID(ctx, d, publication.ID); err != nil || ok {
		t.Fatalf("pruned publication ok=%v err=%v", ok, err)
	}
	var members int
	if err := d.SQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM self_publication_members WHERE publication_id=?`,
		publication.ID).Scan(&members); err != nil || members != 0 {
		t.Fatalf("pruned membership count=%d err=%v", members, err)
	}
}

func TestSelfPublicationOwnershipCASRejectsCandidateOmission(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	seq := appendSelfPublicationEvent(t, d, "refs/heads/main", 3, "owned.go")
	if err := SaveIntentCandidate(ctx, d, IntentCandidate{
		ID:               "owner",
		BranchRef:        "refs/heads/main",
		BranchGeneration: 3,
		Status:           IntentCandidateReady,
		Readiness:        IntentReadinessReady,
		Events: []IntentCandidateEvent{{
			EventSeq: seq, EventRole: "code",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	publication := SelfPublication{
		ID:               "omitted-owner",
		BranchRef:        "refs/heads/main",
		BranchGeneration: 3,
		SourceHead:       "source",
		TargetCommitOID:  "target",
		TargetTreeOID:    "tree",
		Members:          []SelfPublicationMember{{EventSeq: seq}},
	}
	if _, err := PrepareSelfPublication(ctx, d, publication); !errors.Is(
		err, ErrSelfPublicationOwnershipChanged) {
		t.Fatalf("event-only prepare with active candidate err=%v", err)
	}
}

func TestSelfPublicationRejectsOverlappingLiveMembership(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	seq := appendSelfPublicationEvent(t, d, "refs/heads/main", 5, "overlap.go")
	first := SelfPublication{
		ID: "first", BranchRef: "refs/heads/main", BranchGeneration: 5,
		SourceHead: "source", TargetCommitOID: "first-target",
		TargetTreeOID: "first-tree",
		Members:       []SelfPublicationMember{{EventSeq: seq}},
	}
	second := SelfPublication{
		ID: "second", BranchRef: "refs/heads/main", BranchGeneration: 5,
		SourceHead: "source", TargetCommitOID: "first-target",
		TargetTreeOID: "first-tree",
		Members:       []SelfPublicationMember{{EventSeq: seq}},
	}
	if _, err := PrepareSelfPublication(ctx, d, first); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareSelfPublication(ctx, d, second); !errors.Is(
		err, ErrSelfPublicationOwnershipChanged) {
		t.Fatalf("overlapping prepare err=%v", err)
	}
	if _, err := AbandonSelfPublication(ctx, d, first, "CAS not attempted", 2); err != nil {
		t.Fatal(err)
	}
	if created, err := PrepareSelfPublication(ctx, d, second); err != nil || !created {
		t.Fatalf("prepare after abandon=(%v,%v)", created, err)
	}
}

func TestSelfPublicationReadOnlyCurrentProjection(t *testing.T) {
	d, dbPath := openTestDB(t)
	ctx := context.Background()
	seq1 := appendSelfPublicationEvent(t, d, "refs/heads/main", 1, "prepared.go")
	seq2 := appendSelfPublicationEvent(t, d, "refs/heads/main", 1, "applied.go")
	prepared := SelfPublication{
		ID: "prepared", BranchRef: "refs/heads/main", BranchGeneration: 1,
		SourceHead: "source", TargetCommitOID: "target-1",
		TargetTreeOID: "tree-1",
		Members:       []SelfPublicationMember{{EventSeq: seq1}},
	}
	applied := SelfPublication{
		ID: "applied", BranchRef: "refs/heads/main", BranchGeneration: 1,
		SourceHead: "target-1", TargetCommitOID: "target-2",
		TargetTreeOID: "tree-2",
		Members:       []SelfPublicationMember{{EventSeq: seq2}},
	}
	if _, err := PrepareSelfPublication(ctx, d, prepared); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareSelfPublication(ctx, d, applied); err != nil {
		t.Fatal(err)
	}
	if _, err := MarkSelfPublicationGitApplied(ctx, d, applied, 3); err != nil {
		t.Fatal(err)
	}
	projection, err := LoadSelfPublicationStateReadOnly(ctx, dbPath)
	if err != nil || !projection.Available ||
		projection.SchemaVersion != SchemaVersion ||
		projection.Prepared != 1 || projection.GitApplied != 1 ||
		len(projection.Recoverable) != 2 ||
		len(projection.Recoverable[0].Members) != 1 {
		t.Fatalf("projection=%+v err=%v", projection, err)
	}
}

func appendSelfPublicationEvent(
	t *testing.T,
	d *DB,
	branch string,
	generation int64,
	path string,
) int64 {
	t.Helper()
	seq, err := AppendCaptureEvent(context.Background(), d, CaptureEvent{
		BranchRef: branch, BranchGeneration: generation, BaseHead: "base",
		Operation: "update", Path: path, Fidelity: "full",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func assertSelfPublicationCompletedEvent(t *testing.T, d *DB, seq int64, oid string) {
	t.Helper()
	var state string
	var commitOID sql.NullString
	if err := d.SQL().QueryRow(`
SELECT state, commit_oid FROM capture_events WHERE seq=?`, seq).Scan(
		&state, &commitOID); err != nil {
		t.Fatal(err)
	}
	if state != EventStatePublished || !commitOID.Valid || commitOID.String != oid {
		t.Fatalf("event %d=(state=%s, commit=%+v)", seq, state, commitOID)
	}
}

func assertSelfPublicationMeta(t *testing.T, d *DB, key, want string) {
	t.Helper()
	got, ok, err := MetaGet(context.Background(), d, key)
	if err != nil || !ok || got != want {
		t.Fatalf("meta %s=(%q,%v,%v), want %q", key, got, ok, err, want)
	}
}

func assertSelfPublicationSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, name := range []string{
		"self_publications", "self_publication_members",
		"idx_self_publications_pair_target",
		"idx_self_publications_pair_phase_created",
		"idx_self_publications_phase_created",
		"idx_self_publication_members_event",
		"idx_self_publication_members_candidate",
		"self_publications_prepare_only",
		"self_publications_identity_immutable",
		"self_publications_phase_monotonic",
		"self_publication_members_no_live_overlap",
		"self_publication_members_immutable_update",
		"self_publication_members_immutable_delete",
	} {
		var count int
		if err := db.QueryRow(`
SELECT COUNT(*) FROM sqlite_master WHERE name=?`, name).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("schema object %s count=%d", name, count)
		}
	}
	assertSelfPublicationCompletionColumns(t, db)
}

func assertSelfPublicationCompletionColumns(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, column := range []string{
		"completion_published_ts",
		"completion_candidate_status",
		"completion_soft_deadline",
	} {
		var count int
		if err := db.QueryRow(`
SELECT COUNT(*) FROM pragma_table_info('self_publications')
WHERE name=?`, column).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("self_publications column %s count=%d", column, count)
		}
	}
}

func downgradeSelfPublicationFixtureToV17(t *testing.T, dbPath string) {
	t.Helper()
	raw, err := sql.Open(driverName, buildDSN(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`
DROP TRIGGER IF EXISTS self_publications_identity_immutable;
DROP TRIGGER IF EXISTS self_publications_prepare_only;
DROP TRIGGER IF EXISTS self_publications_phase_monotonic;
DROP TRIGGER IF EXISTS self_publication_members_no_live_overlap;
DROP TRIGGER IF EXISTS self_publication_members_immutable_update;
DROP TRIGGER IF EXISTS self_publication_members_immutable_delete;
DROP TABLE IF EXISTS self_publication_members;
DROP TABLE IF EXISTS self_publications;
PRAGMA user_version=17;`); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
}
