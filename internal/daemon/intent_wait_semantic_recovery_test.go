package daemon

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentBlockedWaitSemanticReviewReopensProtectedGoalAfterRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newCaptureFixture(t)
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	for path, body := range map[string]string{
		"package.json":                     "{\"name\":\"worker-team\",\"version\":\"1.1.0\"}\n",
		"package-lock.json":                "{\"name\":\"worker-team\",\"version\":\"1.1.0\",\"lockfileVersion\":3}\n",
		"src/runtime/pi-version.ts":        "export const SUPPORTED_PI_VERSION = \"1.1.0\";\n",
		"tests/package-manifest.test.ts":   "import assert from \"node:assert/strict\";\nimport { readFileSync } from \"node:fs\";\nexport function checkManifestVersion() { assert.equal(JSON.parse(readFileSync(new URL(\"../package.json\", import.meta.url), \"utf8\")).version, \"1.1.0\"); }\n",
		"tests/runtime/pi-version.test.ts": "import { SUPPORTED_PI_VERSION } from \"../../src/runtime/pi-version\";\nexport function checkPiVersion() { if (SUPPORTED_PI_VERSION !== \"1.1.0\") throw new Error(\"unsupported Pi version\"); }\n",
	} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(f.dir, path)), 0o755); err != nil {
			t.Fatal(err)
		}
		writePublicationFile(t, f, path, body)
	}
	if protected := capturePublicationFiles(t, f); !protected.Protected {
		t.Fatalf("five goal captures were not protected: %+v", protected)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 5 {
		t.Fatalf("goal membership=%+v err=%v", pending, err)
	}
	input := IntentCandidateEvaluation{RepoPath: f.dir, BranchRef: f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration, Preset: config.PresetBalanced,
		PresetID: "intent.balanced", PresetVersion: config.PresetCatalogVersion,
		CommitFormat: ai.CommitFormatImperative, IncludeDiffs: true, VerificationMode: "structural",
		Now: time.Now().UTC(), Materialize: intentCandidateScratchMaterializer(f.dir, f.gitDir, f.cctx.BaseHead)}
	bySeq := map[int64]IntentCandidateCapture{}
	var seqs []int64
	for _, event := range pending {
		ops, err := state.LoadCaptureOps(ctx, f.db, event.Seq)
		if err != nil {
			t.Fatal(err)
		}
		diff, err := BuildOpsDiffWithCap(ctx, f.dir, ops, ai.IntentStageDiffCap)
		if err != nil {
			t.Fatal(err)
		}
		capture := IntentCandidateCapture{Event: event, Ops: ops, CapturedDiff: diff}
		input.Captures = append(input.Captures, capture)
		bySeq[event.Seq] = capture
		seqs = append(seqs, event.Seq)
	}
	if err := attachIntentFileMetadata(ctx, f.dir, input.Captures); err != nil {
		t.Fatal(err)
	}
	deps, err := BuildIntentCandidateDependencies(input.BranchRef, input.BranchGeneration,
		input.Captures, runtimeIntentDependencyHints(input.Captures), input.Now)
	if err != nil {
		t.Fatal(err)
	}
	assignment := ai.IntentCandidateAssignment{CandidateID: "unclassified-pi-upgrade", SelectedSeqs: seqs,
		Purpose: "retain dependency component until its goal is known", Readiness: ai.IntentCandidateWait,
		MissingCompanions: []string{unclassifiedIntentCompanion}, Subject: "Update files",
		GroupingReason: "bounded fallback requires planner review"}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{assignment}}
	decision, err := evaluateIntentCandidateAssignment(ctx, f.db, input, plan, assignment, deps, nil, bySeq)
	if err != nil || decision.Publishable || decision.Candidate.Status != state.IntentCandidateWaiting ||
		!intentAtomicityNeedsSemanticReview(decision.Atomicity) {
		t.Fatalf("actual WAIT goal did not request semantic review: %+v err=%v", decision, err)
	}
	for _, finding := range []string{"gate=cohesion code=candidate_lacks_semantic_evidence:",
		"gate=completeness code=candidate_waiting:", "gate=materialization code=candidate_not_sealed:"} {
		if !strings.Contains(decision.Candidate.AtomicitySummary, finding) {
			t.Fatalf("fixture did not reproduce original gates: missing %q in %q", finding, decision.Candidate.AtomicitySummary)
		}
	}
	legacy := decision.Candidate
	legacy.Status = state.IntentCandidateBlocked
	if err := state.SaveIntentCandidate(ctx, f.db, legacy); err != nil {
		t.Fatal(err)
	}
	bundle := &RuntimeBundle{PresetID: "intent.balanced", PresetVersion: config.PresetCatalogVersion}
	if err := updateIntentV2EvaluationMeta(ctx, f.db, bundle, ReplaySummary{}, nil); err != nil {
		t.Fatal(err)
	}
	if raw, _, err := state.MetaGet(ctx, f.db, "intent.v2.needs_attention"); err != nil || raw == "" {
		t.Fatalf("legacy block did not reproduce visible required action: %q err=%v", raw, err)
	}
	head, index := mustGitOutput(t, f.dir, "rev-parse", "HEAD"), mustGitOutput(t, f.dir, "ls-files", "--stage")
	path := f.db.Path()
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	f.db, err = state.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.db.Close() })
	for _, tc := range []struct {
		name   string
		mutate func(*state.IntentCandidate)
	}{
		{"dependency", func(c *state.IntentCandidate) {
			c.AtomicitySummary += "\ncandidate=" + c.ID + " gate=dependency code=hard_dependency_undeclared: prerequisite is unknown"
		}},
		{"materialization", func(c *state.IntentCandidate) {
			c.AtomicitySummary += "\ncandidate=" + c.ID + " gate=materialization code=materialization_failed: tree could not be proved"
		}},
		{"verification", func(c *state.IntentCandidate) { c.VerificationStatus = sql.NullString{String: "failed", Valid: true} }},
		{"wrong_gate", func(c *state.IntentCandidate) {
			c.AtomicitySummary = strings.Replace(c.AtomicitySummary, "gate=materialization code=candidate_not_sealed:", "gate=dependency code=candidate_not_sealed:", 1)
		}},
		{"ready", func(c *state.IntentCandidate) { c.Readiness = state.IntentReadinessReady }},
		{"branch", func(c *state.IntentCandidate) { c.BranchGeneration++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unsafe := legacy
			tc.mutate(&unsafe)
			rows := []state.IntentCandidate{unsafe}
			if err := reopenProtectedSemanticIntentCandidates(ctx, f.db, input, rows); err != nil || !reflect.DeepEqual(rows[0], unsafe) {
				t.Fatalf("unsafe WAIT block reopened: %+v err=%v", rows, err)
			}
		})
	}
	if _, err := f.db.SQL().ExecContext(ctx, "UPDATE checkpoints SET coverage_complete=0"); err != nil {
		t.Fatal(err)
	}
	rows := []state.IntentCandidate{legacy}
	if err := reopenProtectedSemanticIntentCandidates(ctx, f.db, input, rows); err != nil || rows[0].Status != state.IntentCandidateBlocked {
		t.Fatalf("unproved WAIT checkpoint reopened: %+v err=%v", rows, err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, "UPDATE checkpoints SET coverage_complete=1"); err != nil {
		t.Fatal(err)
	}
	rows = []state.IntentCandidate{legacy}
	if err := reopenProtectedSemanticIntentCandidates(ctx, f.db, input, rows); err != nil || rows[0].Status != state.IntentCandidateWaiting {
		t.Fatalf("protected WAIT semantic-only block remained permanent: %+v err=%v", rows, err)
	}
	if err := updateIntentV2EvaluationMeta(ctx, f.db, bundle, ReplaySummary{SkippedReason: "intent_v2_waiting_semantic_retry"}, nil); err != nil {
		t.Fatal(err)
	}
	if raw, _, err := state.MetaGet(ctx, f.db, "intent.v2.needs_attention"); err != nil || raw != "" {
		t.Fatalf("safe automatic review still asks for action: %q err=%v", raw, err)
	}
	got, found, err := state.IntentCandidateByID(ctx, f.db, legacy.ID)
	if err != nil || !found || len(got.Events) != 5 || got.Status != state.IntentCandidateWaiting || got.Readiness != state.IntentReadinessWait || got.PublishedCommitOID.Valid {
		t.Fatalf("recovery lost goal ownership or authorized publication: %+v found=%t err=%v", got, found, err)
	}
	after, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || !reflect.DeepEqual(after, pending) || mustGitOutput(t, f.dir, "rev-parse", "HEAD") != head || mustGitOutput(t, f.dir, "ls-files", "--stage") != index {
		t.Fatalf("classification recovery changed protected work: pending=%+v err=%v", after, err)
	}
	for _, gate := range decision.Atomicity.Gates {
		if gate.Status == ai.IntentAtomicityPending && gate.Finding != nil && intentPendingWaitFinding(gate.Gate, gate.Finding.Code) {
			unsafe := decision.Atomicity
			unsafe.Gates = append([]ai.IntentAtomicityGateResult(nil), unsafe.Gates...)
			for i := range unsafe.Gates {
				if unsafe.Gates[i].Gate == gate.Gate {
					unsafe.Gates[i].Status = ai.IntentAtomicityFailed
				}
			}
			if intentAtomicityNeedsSemanticReview(unsafe) {
				t.Fatalf("WAIT pending exception accepted a failed gate: %+v", unsafe)
			}
		}
	}
}
