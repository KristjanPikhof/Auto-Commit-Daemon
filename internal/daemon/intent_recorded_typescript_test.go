package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func BenchmarkIntentTypeScriptPublishedSupport(b *testing.B) {
	const count = 16
	owner := "export function Helper() {\n" + strings.Repeat(" const value = 1;\n", 64) + " return 1;\n}\n"
	paths := make([]string, count)
	tests := make([]string, count)
	for i := range paths {
		paths[i] = fmt.Sprintf("owner%d.ts", i)
		tests[i] = fmt.Sprintf("import { Helper } from \"./owner%d.ts\";\nHelper();\n", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sources := make([]intentTypeScriptRecordedFile, count)
		for i := range sources {
			sources[i] = newIntentTypeScriptRecordedFile(int64(i+1), paths[i], owner, "")
		}
		for i, contents := range tests {
			test := newIntentTypeScriptRecordedFile(int64(count+i+1), "support.test.ts", contents, "")
			pair := append(append([]intentTypeScriptRecordedFile(nil), sources...), test)
			if intentTypeScriptReferenceContexts(pair)[test.seq] == "" {
				b.Fatal("published support lost its recorded import")
			}
		}
	}
}

func TestIntentTypeScriptRecordedReferencesRequireExactImportsAndOwners(t *testing.T) {
	t.Parallel()
	const ownerPath = "tests/runtime/test-helpers.ts"
	const consumerPath = "tests/runtime/worker-manager.test.ts"
	const ownerContents = "export class MockWorkerTransport {\n prompt(input: string) { return input.split(/\\r?\\n/); }\n}\nexport class MockWorkerHandle {}\nexport function waitForMicrotasks() {}\n"
	const consumerContents = "import { MockWorkerHandle, MockWorkerTransport, waitForMicrotasks } from \"./test-helpers\";\nnew MockWorkerTransport();\n"
	owner := newIntentTypeScriptRecordedFile(1, ownerPath, ownerContents, "")
	consumer := newIntentTypeScriptRecordedFile(2, consumerPath, consumerContents, "")
	proof := intentTypeScriptReferenceContexts([]intentTypeScriptRecordedFile{owner, consumer})
	if proof[1] != " export class MockWorkerTransport\n" || proof[2] != " import { MockWorkerHandle, MockWorkerTransport, waitForMicrotasks } from \"./test-helpers\";\n" {
		t.Fatalf("recorded import and owner were not retained: %+v", proof)
	}
	for _, tc := range []struct {
		name, source, target string
		ambiguous            bool
	}{
		{"comment", "// import { MockWorkerTransport } from \"./test-helpers\";\n", ownerContents, false},
		{"block comment", "/*\nimport { MockWorkerTransport } from \"./test-helpers\";\n*/\n", ownerContents, false},
		{"quoted prose", "const label = `\nimport { MockWorkerTransport } from \"./test-helpers\";\n`;\n", ownerContents, false},
		{"dynamic", "const module = await import(\"./test-helpers\");\n", ownerContents, false},
		{"namespace", "import * as helpers from \"./test-helpers\";\n", ownerContents, false},
		{"pathspec syntax", "import { MockWorkerTransport } from \"./test-helpers?.ts\";\n", ownerContents, false},
		{"wrong owner", consumerContents, "export class OtherTransport {}\n", false},
		{"private owner", consumerContents, "class MockWorkerTransport {}\n", false},
		{"nested owner", consumerContents, "namespace Private {\nexport class MockWorkerTransport {}\n}\n", false},
		{"other directory", "import { MockWorkerTransport } from \"../unrelated/test-helpers\";\n", ownerContents, false},
		{"ambiguous resolution", consumerContents, ownerContents, true},
		{"oversized owner", consumerContents, strings.Repeat(" ", intentSourceReferenceScanCap+1) + ownerContents, false},
		{"binary owner", consumerContents, ownerContents + "\x00", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := []intentTypeScriptRecordedFile{newIntentTypeScriptRecordedFile(1, ownerPath, tc.target, ""), newIntentTypeScriptRecordedFile(2, consumerPath, tc.source, "")}
			if tc.ambiguous {
				files = append(files, newIntentTypeScriptRecordedFile(3, "tests/runtime/test-helpers/index.ts", ownerContents, ""))
			}
			if got := intentTypeScriptReferenceContexts(files); len(got) != 0 {
				t.Fatalf("unproved relationship: %+v", got)
			}
		})
	}
}

func TestIntentTypeScriptJavaScriptCounterpartRequiresUniqueRecordedSource(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                            string
		trackedJS, pendingJS, renamedJS bool
		baseHead                        string
	}{
		{name: "recorded TypeScript source"},
		{name: "tracked JavaScript competitor", trackedJS: true},
		{name: "pending JavaScript competitor", pendingJS: true},
		{name: "pending JavaScript rename origin", renamedJS: true},
		{name: "missing immutable base", baseHead: "missing"},
		{name: "invalid immutable base", baseHead: strings.Repeat("x", 40)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCaptureFixture(t)
			ctx := context.Background()
			for p, contents := range map[string]string{
				"owner.ts":  "export class RpcClient {\n prompt(): void {}\n}\n",
				"caller.ts": "import { RpcClient } from \"./owner.js\";\nconst client = new RpcClient();\n",
			} {
				seedTrackedFileCommit(t, ctx, f, p, contents)
			}
			if tc.trackedJS {
				seedTrackedFileCommit(t, ctx, f, "owner.js", "export class RpcClient {}\n")
			}
			if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
				t.Fatal(err)
			}
			captureSamePathEdit(t, ctx, f, "owner.ts", "export class RpcClient {\n prompt() { return \"handled\"; }\n}\n")
			captureSamePathEdit(t, ctx, f, "caller.ts", "import { RpcClient } from \"./owner.js\";\nconst disposition = new RpcClient().prompt();\n")
			if tc.pendingJS {
				captureSamePathEdit(t, ctx, f, "owner.js", "export class RpcClient {}\n")
			}
			pending, err := state.PendingEvents(ctx, f.db, 0)
			if err != nil {
				t.Fatal(err)
			}
			var captures []IntentCandidateCapture
			for _, event := range pending {
				if tc.baseHead != "" {
					event.BaseHead = tc.baseHead
				}
				ops, err := state.LoadCaptureOps(ctx, f.db, event.Seq)
				if err != nil {
					t.Fatal(err)
				}
				captures = append(captures, IntentCandidateCapture{Event: event, Ops: ops})
			}
			if tc.renamedJS {
				captures = append(captures, IntentCandidateCapture{
					Event: state.CaptureEvent{Seq: pending[len(pending)-1].Seq + 1, Path: "owner.backup", Operation: "rename", OldPath: sql.NullString{String: "owner.js", Valid: true}},
					Ops:   []state.CaptureOp{{Op: "rename", Path: "owner.backup", OldPath: sql.NullString{String: "owner.js", Valid: true}}},
				})
			}
			references, err := loadIntentRecordedTypeScriptReferences(ctx, f.dir, captures)
			if err != nil {
				t.Fatal(err)
			}
			want := !tc.trackedJS && !tc.pendingJS && !tc.renamedJS && tc.baseHead == ""
			if (len(references) != 0) != want {
				t.Fatalf("counterpart proof=%v want=%t", references, want)
			}
		})
	}
}

func TestReplayIntentTypeScriptRecordedImportPublishesWaitingMockWithCompanions(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	helperPath, rpcPath := "tests/runtime/test-helpers.ts", "src/runtime/rpc-client.ts"
	managerPath, testPath := "src/runtime/worker-manager.ts", "tests/runtime/worker-manager.test.ts"
	before := map[string]string{
		helperPath:  "export interface MockTransportOptions {\n autoCompletePrompt?: boolean;\n}\nexport class MockWorkerTransport {\n constructor(private options: MockTransportOptions = {}) {}\n prompt(): void {}\n parse(input: string) { return input.split(/\\r?\\n/); }\n}\nexport class MockWorkerHandle {}\nexport function waitForMicrotasks() {}\n",
		rpcPath:     "export class RpcClient {\n prompt(): void {}\n}\n",
		managerPath: "import { RpcClient } from \"./rpc-client.js\";\n" + strings.Repeat("// unchanged manager context\n", 12) + "export class WorkerManager {\n constructor(private client: RpcClient) {}\n prompt() { this.client.prompt(); }\n}\n",
		testPath:    "import { strict as assert } from \"node:assert\";\nimport { MockWorkerHandle, MockWorkerTransport, waitForMicrotasks } from \"./test-helpers\";\nimport { WorkerManager } from \"../../src/runtime/worker-manager\";\n" + strings.Repeat("// unchanged test context\n", 12) + "const transport = new MockWorkerTransport();\nassert.doesNotThrow(() => new WorkerManager(transport).prompt());\n",
	}
	after := map[string]string{
		helperPath:  "export interface MockTransportOptions {\n autoCompletePrompt?: boolean;\n promptDisposition?: \"started\" | \"queued\" | \"handled\";\n}\nexport class MockWorkerTransport {\n constructor(private options: MockTransportOptions = {}) {}\n prompt() { return this.options.promptDisposition ?? \"started\"; }\n parse(input: string) { return input.split(/\\r?\\n/); }\n}\nexport class MockWorkerHandle {}\nexport function waitForMicrotasks() {}\n",
		rpcPath:     "export class RpcClient {\n prompt(): \"started\" | \"queued\" | \"handled\" { return \"handled\"; }\n}\n",
		managerPath: strings.Replace(before[managerPath], "prompt() { this.client.prompt(); }", "prompt() {\n const disposition = this.client.prompt();\n if (disposition === \"handled\") throw new Error(\"handled prompt\");\n}", 1),
		testPath:    strings.Replace(strings.Replace(before[testPath], "new MockWorkerTransport()", "new MockWorkerTransport({ promptDisposition: \"handled\" })", 1), "assert.doesNotThrow(() => new WorkerManager(transport).prompt());", "assert.throws(() => new WorkerManager(transport).prompt(), /handled prompt/);", 1),
	}
	for _, p := range []string{helperPath, rpcPath, managerPath, testPath} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(f.dir, p)), 0755); err != nil {
			t.Fatal(err)
		}
		seedTrackedFileCommit(t, ctx, f, p, before[p])
	}
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	helper := captureSamePathEdit(t, ctx, f, helperPath, after[helperPath])
	if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{
		ID: "prompt-dispositions", BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		Status: state.IntentCandidateWaiting, Readiness: state.IntentReadinessWait,
		Purpose:           "extend the mock transport with prompt disposition controls",
		MissingCompanions: "runtime behavior and regression companions are missing",
		Events:            []state.IntentCandidateEvent{{EventSeq: helper, EventRole: "code"}},
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := state.RecordPlannerDefer(ctx, f.db, helper, intentPlannerHealthTimestamp(time.Now().Add(-time.Hour)), "runtime and test companions are missing"); err != nil {
			t.Fatal(err)
		}
	}
	unrelated := captureSamePathEdit(t, ctx, f, "walkthrough.md", "# Independent recording guide\n")
	rpc := captureSamePathEdit(t, ctx, f, rpcPath, after[rpcPath])
	manager := captureSamePathEdit(t, ctx, f, managerPath, after[managerPath])
	test := captureSamePathEdit(t, ctx, f, testPath, after[testPath])
	pendingBefore, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil {
		t.Fatal(err)
	}
	frozen, _, reason, err := expandIntentGoalWindow(ctx, f.dir, f.db, f.cctx, pendingBefore, pendingBefore[:1], intentReplayConfig{targetEventSeqs: []int64{helper}}, time.Now())
	if err != nil || reason != "" || len(frozen) != 1 || frozen[0].Seq != helper {
		t.Fatalf("later companions crossed frozen target: %+v %s %v", frozen, reason, err)
	}
	fingerprint := "sha256:" + strings.Repeat("a", 64)
	run, err := state.EnsureIntentPlanRun(ctx, f.db, state.IntentPlanRun{
		Fingerprint: fingerprint, BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration, AttemptLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	run.Completed, run.AttemptCount = true, 1
	run.UnresolvedSeqs = []int64{helper}
	run.ProgressState = sql.NullString{String: "waiting_semantic_retry", Valid: true}
	run.ResolutionMode = run.ProgressState
	if err := state.UpdateIntentPlanRun(ctx, f.db, run); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSetJSON(ctx, f.db, intentSemanticRetryKey(fingerprint), IntentSemanticRetrySnapshot{
		Version: 1, BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		EvidenceFingerprint: fingerprint, PlanFingerprint: fingerprint, ReviewCount: 1,
		ScheduledAtTS: intentPlannerHealthTimestamp(time.Now().Add(-10 * time.Minute)), RetryAtTS: intentPlannerHealthTimestamp(time.Now().Add(-time.Minute)),
	}); err != nil {
		t.Fatal(err)
	}
	// Neither live imports nor live source bytes are evidence for the goal.
	if err := os.WriteFile(filepath.Join(f.dir, testPath), []byte("// unrelated live edit\n"), 0644); err != nil {
		t.Fatal(err)
	}
	planner := &intentGoalWindowPlanner{intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{CandidateID: "prompt-dispositions", SelectedSeqs: []int64{helper, rpc, manager, test},
			Purpose: "handle prompt dispositions without waiting for absent settlement", Readiness: ai.IntentCandidateReady,
			Subject: "Handle prompt disposition outcomes", Body: "- Keep runtime handling and its mock regression companion together",
			GroupingReason: "recorded imports link the mock and test to their runtime chain"}},
	}}}
	count := revListCount(t, ctx, f.dir, "HEAD")
	summary, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetBalanced, IntentWindow: 1, IntentBypassBatchWait: true,
		IntentIncludeDiffs: true, IntentVerificationMode: "structural"})
	if err != nil || summary.Published != 4 || planner.calls != 1 || revListCount(t, ctx, f.dir, "HEAD") != count+1 {
		t.Fatalf("waiting mock stayed isolated: summary=%+v calls=%d err=%v", summary, planner.calls, err)
	}
	var offered []int64
	for _, capture := range planner.req.OfferedCaptures {
		offered = append(offered, capture.Seq)
	}
	if !reflect.DeepEqual(offered, []int64{helper, rpc, manager, test}) {
		t.Fatalf("goal closure=%v", offered)
	}
	if err := ValidateIntentGoalPlan(planner.req, planner.plan); err != nil {
		t.Fatalf("shared semantic gate rejected recorded imports: %v", err)
	}
	for p, body := range after {
		got, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "show", "HEAD:"+p)
		if err != nil || string(got) != body {
			t.Fatalf("published %s=%q err=%v", p, got, err)
		}
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Seq != unrelated {
		t.Fatalf("independent work moved: %+v err=%v", pending, err)
	}
	if got, err := os.ReadFile(filepath.Join(f.dir, testPath)); err != nil || string(got) != "// unrelated live edit\n" {
		t.Fatalf("live work changed: %q %v", got, err)
	}
}
