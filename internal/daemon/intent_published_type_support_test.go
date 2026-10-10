package daemon

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentPublishedTypedProofSupportsPendingMetadataFields(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newCaptureFixture(t)
	before := "package app\ntype IntentMetadata struct {\n Label string\n}\n"
	f.cctx.BaseHead = mustCommitPath(t, f.dir, "metadata.go", before, "Add intent metadata")
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "proof.go", "package app\nfunc NewProof() *IntentMetadata { return &IntentMetadata{ privateProof: 1 } }\nfunc ValidProof(p *IntentMetadata) bool { return p.privateProof == 1 }\n")
	writePublicationFile(t, f, "proof_test.go", "package app\nimport \"testing\"\nfunc TestProof(t *testing.T) { if !ValidProof(NewProof()) { t.Fatal(\"missing proof\") } }\n")
	if captured := capturePublicationFiles(t, f); !captured.Protected {
		t.Fatalf("proof support unprotected: %+v", captured)
	}
	events, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(events) != 2 {
		t.Fatalf("support events=%+v err=%v", events, err)
	}
	published, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyEvent})
	if err != nil || published.Published != 2 {
		t.Fatalf("support was not published: %+v err=%v", published, err)
	}
	f.cctx.BaseHead = published.BaseHead
	baseline := state.IntentCandidate{ID: "published-proof-support", BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		Status: state.IntentCandidatePublished, Readiness: state.IntentReadinessReady, Purpose: "produce and validate private metadata proofs", PublishedCommitOID: sql.NullString{String: published.BaseHead, Valid: true}}
	// Event publication produced one commit per capture. Keep each exact
	// published version as its own approved read-only descriptor.
	for _, event := range events {
		current, err := loadIntentCaptureEvent(ctx, f.db, event.Seq)
		if err != nil {
			t.Fatal(err)
		}
		candidate := baseline
		candidate.ID = event.Path
		candidate.PublishedCommitOID = current.CommitOID
		candidate.Events = []state.IntentCandidateEvent{{EventSeq: event.Seq, EventRole: "code"}}
		if err := state.SaveIntentCandidate(ctx, f.db, candidate); err != nil {
			t.Fatal(err)
		}
	}
	writePublicationFile(t, f, "metadata.go", strings.Replace(before, " Label string\n", " Label string\n privateProof int\n", 1))
	if captured := capturePublicationFiles(t, f); !captured.Protected {
		t.Fatalf("metadata fields unprotected: %+v", captured)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending metadata=%+v err=%v", pending, err)
	}
	ops, err := state.LoadCaptureOps(ctx, f.db, pending[0].Seq)
	if err != nil {
		t.Fatal(err)
	}
	input := IntentCandidateEvaluation{RepoPath: f.dir, BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration, IncludeDiffs: true, LatestCommit: &ai.CommitSummary{OID: f.cctx.BaseHead[:8]}, Now: time.Now().UTC(), Captures: []IntentCandidateCapture{{Event: pending[0], Ops: ops}}}
	existing, err := loadPublishedIntentFormerCompanions(ctx, f.db, &input, nil)
	if err != nil || len(existing) != 1 || existing[0].ID != "proof.go" {
		t.Fatalf("exact typed producer not supplied: %+v err=%v", existing, err)
	}
	context, err := loadCandidateCaptureContext(ctx, f.db, input, existing)
	if err != nil {
		t.Fatal(err)
	}
	context, err = loadFocusedIntentGoalEvidence(ctx, input, existing, context)
	if err != nil {
		t.Fatal(err)
	}
	var declaration, producer bool
	for _, capture := range context {
		if capture.Event.Path == "metadata.go" {
			declaration = strings.Contains(capture.CapturedDiff, " type IntentMetadata struct {")
		}
		if capture.Event.Path == "proof.go" {
			producer = strings.Contains(capture.CapturedDiff, "*IntentMetadata")
		}
	}
	if !declaration || !producer {
		t.Fatalf("typed ownership evidence was lost: declaration=%t producer=%t", declaration, producer)
	}
	planner := &semanticRetryReplayPlanner{intentCandidatePlannerStub: intentCandidatePlannerStub{plan: ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{CandidateID: "metadata-proof-fields", SelectedSeqs: []int64{pending[0].Seq}, Readiness: ai.IntentCandidateReady, Purpose: "retain private proof state in intent metadata", Subject: "Retain private intent metadata proofs", Body: "- Complete storage used by the already-published typed producer", GroupingReason: "the changed metadata declaration supports the recorded typed proof producer already in HEAD"}}}}}
	result, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner, IntentPreset: config.PresetBalanced, IntentIncludeDiffs: true, IntentBypassBatchWait: true, IntentVerificationMode: "structural"})
	if err != nil || result.Published != 1 || result.Failed != 0 {
		t.Fatalf("metadata completion did not publish: %+v err=%v", result, err)
	}
}

func TestIntentGoTypedSupportRejectsLabelsAndShadowedOwners(t *testing.T) {
	t.Parallel()
	source := intentGoRegressionSource{packageName: "app", types: map[string]bool{"Metadata": true}}
	for _, tc := range []struct {
		code string
		want bool
	}{
		{"package app\nfunc Build() *Metadata {return &Metadata{}}\n", true},
		{"package app\nfunc Use(value *Metadata) {}\n", true},
		{"package app\nfunc Build() string {return \"Metadata{}\"}\n", false},
		{"package app\nfunc Build(Metadata string) string {return Metadata}\n", false},
		{"package app\ntype Metadata struct{}\nfunc Build() Metadata {return Metadata{}}\n", false},
		{"package other\nfunc Build() *Metadata {return &Metadata{}}\n", false},
		{"package app\nfunc Build() *foreign.Metadata {return &foreign.Metadata{}}\n", false},
	} {
		if got := intentGoPublishedReferenceContext("proof.go", []byte(tc.code), source) != ""; got != tc.want {
			t.Fatalf("typed proof=%t want=%t source=%s", got, tc.want, tc.code)
		}
	}
}

func TestIntentRecordedGoDeletedDeclarationDoesNotOwnUnchangedNeighbor(t *testing.T) {
	t.Parallel()
	for _, neighbor := range []string{
		"func Kept() int { return 3 }\n",
		"type Kept struct { Value int }\n",
	} {
		contents := "package app\n" + neighbor
		diff := "@@ -1,3 +1,2 @@\n package app\n-func Removed() int { return 2 }\n " + neighbor
		calls := intentGoRecordedCallContext("kept.go", contents, diff, intentSourceReferenceContextCap)
		if len(calls.ownedFunctions) != 0 || len(calls.ownedTypes) != 0 || calls.context != "" {
			t.Fatalf("deleted predecessor invented ownership: %+v", calls)
		}
	}
}
