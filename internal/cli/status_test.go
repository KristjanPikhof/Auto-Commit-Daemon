package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestStatus_RegisteredRepoWithClientsAndCommit(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db, "claude-code")

	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 12345, Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}

	// Two clients.
	now := nowFloat()
	if err := state.RegisterClient(ctx, d, state.Client{
		SessionID: "8c7d0000-aaaa-bbbb-cccc-000000000001", Harness: "claude-code",
		LastSeenTS: now,
	}); err != nil {
		t.Fatalf("register A: %v", err)
	}
	if err := state.RegisterClient(ctx, d, state.Client{
		SessionID: "9f3e0000-aaaa-bbbb-cccc-000000000002", Harness: "pi",
		LastSeenTS: now - 14,
	}); err != nil {
		t.Fatalf("register B: %v", err)
	}

	// One commit.
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "auth.py",
		Fidelity: "exact", CapturedTS: now - 47,
	}, nil)
	if err != nil {
		t.Fatalf("append event: %v", err)
	}
	if err := state.MarkEventPublished(ctx, d, seq, "published",
		sql.NullString{String: "a1b2c3deeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Valid: true},
		sql.NullString{}, sql.NullString{String: "Update auth.py", Valid: true},
		now-47); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Branch generation token in meta.
	if err := state.MetaSet(ctx, d, "branch.generation_token", "rev:deadbeef"); err != nil {
		t.Fatalf("meta set: %v", err)
	}

	var out bytes.Buffer
	if err := runStatus(ctx, &out, repo, false); err != nil {
		t.Fatalf("runStatus: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Repo: " + repo,
		"running",
		"pid 12345",
		"Clients (2):",
		"claude-code",
		"pi ",
		"a1b2c3d",
		"Update auth.py",
		"rev:deadbeef",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q in:\n%s", want, got)
		}
	}
}

func TestRegularPublicationProgressBaselineTracksActiveQueueOnly(t *testing.T) {
	_, _, db := makeRepoStateDB(t)
	ctx := context.Background()
	insert := func(branch string, generation int64, captured float64,
		eventState string, published *float64) {
		t.Helper()
		var publishedValue any
		if published != nil {
			publishedValue = *published
		}
		if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,
 captured_ts,state,commit_oid,published_ts
) VALUES(?,?, 'head','modify',?,'exact',?,?,?,?)`,
			branch, generation,
			fmt.Sprintf("%s-%d-%0.f", branch, generation, captured),
			captured, eventState,
			func() any {
				if published != nil {
					return "commit"
				}
				return nil
			}(), publishedValue); err != nil {
			t.Fatal(err)
		}
	}
	mainProgress := float64(200)
	otherProgress := float64(900)
	insert("refs/heads/main", 7, 100, state.EventStatePending, nil)
	insert("refs/heads/main", 7, 50, state.EventStatePublished, &mainProgress)
	insert("refs/heads/other", 7, 50, state.EventStatePublished, &otherProgress)
	// New work must not make an old stuck frontier look fresh.
	insert("refs/heads/main", 7, 500, state.EventStatePending, nil)

	got, err := regularPublicationProgressBaseline(
		ctx, db.SQL(), "refs/heads/main", 7)
	if err != nil {
		t.Fatal(err)
	}
	if got != mainProgress {
		t.Fatalf("last progress=%v, want active-branch publication %v", got,
			mainProgress)
	}
}

func TestStatusAndListKeepPausedDrainTargetFromDurablePair(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, db := makeRepoStateDB(t)
	registerProtectedControlRepo(t, roots, repo)

	seqs := insertCompletedCheckpoint(t, db, "cp-paused-target",
		"0123456789abcdef", []checkpointMemberFixture{{
			State: state.EventStatePending,
		}})
	drain := state.PublicationDrain{
		ID: "drain-paused-target", CheckpointID: "cp-paused-target",
		WorktreeID: "0123456789abcdef", BranchRef: "refs/heads/main",
		BranchGeneration: 7, CommitStrategy: "intent",
		CommitFormat: "imperative", Provider: "openai-compat",
		ProviderFingerprint: "sha256:" + strings.Repeat("a", 64),
		Phase:               state.PublicationDrainSemantic, TargetEventCount: 1,
		CreatedTS: nowFloat(), UpdatedTS: nowFloat(),
		LastProgressTS: nowFloat(), EventSeqs: seqs,
	}
	if created, err := state.PreparePublicationDrain(ctx, db, drain); err != nil || !created {
		t.Fatalf("prepare drain=(%t,%v)", created, err)
	}
	if err := state.MetaSet(ctx, db, "commit.strategy", "intent"); err != nil {
		t.Fatal(err)
	}
	seedCurrentReplayPair(t, db, drain.BranchRef, drain.BranchGeneration)
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatal(err)
	}
	assertDaemonReplayAnchorNull(t, db)
	writePauseMarkerForStateDB(t, dbPath, pausepkg.Marker{
		Reason: "inspect publication", SetAt: time.Now().UTC().Format(time.RFC3339),
		SetBy: "test",
	})

	record := central.RepoRecord{
		Path: repo, StateDB: dbPath, RepoHash: "0123456789abcdef",
		RepositoryID: "repository-id", WorktreeID: "0123456789abcdef",
	}
	report, err := buildStatusReport(ctx, record, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if report.BranchRef != drain.BranchRef ||
		report.BranchGeneration != drain.BranchGeneration ||
		report.PublicationDrain.ID != drain.ID ||
		report.PublicationProgress.Phase != "paused" ||
		report.PublicationProgress.TargetRemaining != 1 ||
		report.PublicationProgress.TargetTotal != 1 {
		t.Fatalf("paused status=%+v progress=%+v branch=%s/%d",
			report.PublicationDrain, report.PublicationProgress,
			report.BranchRef, report.BranchGeneration)
	}

	overview, err := readProductListRepo(ctx, record, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	entry := productListEntry{PublicationProgress: overview.report.PublicationProgress}
	if got := productListTarget(entry); got != "commit-all:1/1" {
		t.Fatalf("paused list target=%q progress=%+v", got,
			overview.report.PublicationProgress)
	}

	var out bytes.Buffer
	if err := runControlStatus(ctx, &out, repo, false); ExitCode(err) != ExitActionRequired {
		t.Fatalf("paused status exit=%d err=%v\n%s", ExitCode(err), err, out.String())
	}
	for _, want := range []string{
		"Active target: earlier commit-all request, 1 of 1 left",
		"Publication phase: paused by the user",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("paused status missing %q:\n%s", want, out.String())
		}
	}

	// A paused worker may not have observed a manual branch switch yet. The
	// persisted pair must match the checked-out branch before it scopes status.
	seedCurrentReplayPair(t, db, "refs/heads/other", 8)
	mismatched, err := buildStatusReport(ctx, record, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if mismatched.BranchRef != "" || mismatched.PublicationDrain.ID != "" ||
		mismatched.PublicationProgress.TargetTotal != 0 {
		t.Fatalf("stale paused pair leaked drain=%+v progress=%+v branch=%q",
			mismatched.PublicationDrain, mismatched.PublicationProgress,
			mismatched.BranchRef)
	}
}

func TestPublicationProgressSeparatesStallFromExpectedWait(t *testing.T) {
	t.Setenv("ACD_AI_TIMEOUT", "1m")
	now := time.Unix(1_000, 0)
	base := statusReport{
		Daemon: "running", PID: os.Getpid(), PendingEvents: 2,
		IntentStrategy: intentStrategyReport{Strategy: "intent", Active: true},
		PublicationDrain: publicationDrainReport{
			ID: "drain", Phase: state.PublicationDrainSemantic,
			TargetEvents: 2, RemainingEvents: 2, LastProgressTS: 100,
		},
	}
	progress, err := buildPublicationProgressReport(
		context.Background(), nil, base, now)
	if err != nil {
		t.Fatal(err)
	}
	if progress.Phase != "stalled" || !progress.WorkerResponsive ||
		progress.LastProgressAgeSeconds != 900 {
		t.Fatalf("stalled progress=%+v", progress)
	}

	base.PublicationDrain = publicationDrainReport{}
	base.IntentStrategy.BatchWaitActive = true
	base.IntentStrategy.BatchWaitReason = "skipped_due_intent_batch_wait"
	base.IntentStrategy.AgeTriggerInSeconds = 30
	// No DB is available for a regular queue baseline in this unit case, but
	// the explicit wait must remain a normal wait rather than become stalled.
	progress, err = buildPublicationProgressReport(
		context.Background(), nil, base, now)
	if err != nil {
		t.Fatal(err)
	}
	if progress.Phase != "intent_wait" || progress.WaitRemainingSeconds != 30 {
		t.Fatalf("wait progress=%+v", progress)
	}
}

func TestPublicationProgressShowsAutomaticVerificationRecovery(t *testing.T) {
	report := statusReport{
		Daemon: "running", PID: os.Getpid(), PendingEvents: 7,
		IntentStrategy: intentStrategyReport{Strategy: "intent", Active: true},
		IntentV2:       intentV2Report{VerificationRecovering: 2},
	}
	progress, err := buildPublicationProgressReport(
		context.Background(), nil, report, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if progress.Phase != "intent_verification_recovery" {
		t.Fatalf("verification recovery progress=%+v", progress)
	}
	if label := publicationProgressPhaseLabel(progress); !strings.Contains(
		label, "automatic checkpoint replan",
	) {
		t.Fatalf("verification recovery label=%q", label)
	}
}

func TestPublicationProgressShowsActiveIntentRecoveryTarget(t *testing.T) {
	ctx := context.Background()
	_, _, db := makeRepoStateDB(t)
	now := time.Now()
	seqs := insertCompletedCheckpoint(t, db, "cp-intent-recovery",
		"0123456789abcdef", []checkpointMemberFixture{
			{State: state.EventStatePending},
			{State: state.EventStatePublished, CommitOID: "published-commit"},
			{State: state.EventStatePending},
		})
	publishedTS := float64(now.Add(-10*time.Second).UnixNano()) / 1e9
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET published_ts=? WHERE seq=?`,
		publishedTS, seqs[1]); err != nil {
		t.Fatal(err)
	}
	markerTS := float64(now.Add(-30*time.Second).UnixNano()) / 1e9
	marker := state.IntentForwardRecovery{
		BranchRef: "refs/heads/main", BranchGeneration: 7,
		CandidateID: "failed-candidate", Stage: "semantic_replan",
		TargetEventSeqs: []int64{seqs[0], seqs[1]},
		LastProgressTS:  markerTS,
	}
	if err := state.MetaSetJSON(ctx, db, "intent.v2.forward_recovery",
		marker); err != nil {
		t.Fatal(err)
	}
	report := statusReport{
		Daemon: "running", PID: os.Getpid(), PendingEvents: 2,
		BranchRef: "refs/heads/main", BranchGeneration: 7,
		CheckpointProtectionAvailable: true, Protected: false, Busy: true,
		IntentStrategy: intentStrategyReport{Strategy: "intent", Active: true},
	}
	progress, err := buildPublicationProgressReport(ctx, db.SQL(), report, now)
	if err != nil {
		t.Fatal(err)
	}
	if progress.Origin != "intent_recovery" ||
		progress.Phase != "intent_replanning" ||
		progress.TargetTotal != 2 || progress.TargetRemaining != 1 ||
		progress.LastProgressTS < publishedTS {
		t.Fatalf("intent recovery progress=%+v", progress)
	}
	entry := productListEntry{PublicationProgress: progress}
	if target, phase := productListTarget(entry), productListPhase(entry); target != "recover:1/2" || phase != "intent-replan" {
		t.Fatalf("intent recovery list target=%q phase=%q", target, phase)
	}
	var human bytes.Buffer
	renderProductPublicationProgress(&human, progress)
	for _, want := range []string{
		"Active target: automatic Intent recovery, 1 of 2 left",
		"Publication phase: recovering by replanning commit groups by Intent",
	} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("intent recovery status missing %q:\n%s", want, human.String())
		}
	}

	report.PublicationProgress = progress
	result := controlResult{OK: true}
	applyControlStatusWithDaemonAlive(&result, report, true)
	if result.Health != controlHealthPublishing ||
		!strings.Contains(result.Summary, "automatically rebuilding semantic") ||
		strings.Contains(result.Summary, "scanning recent changes") {
		t.Fatalf("intent recovery control=%+v", result)
	}

	progress.Phase = "stalled"
	report.PublicationProgress = progress
	result = controlResult{OK: true}
	applyControlStatusWithDaemonAlive(&result, report, true)
	if result.Health != controlHealthDegraded ||
		!strings.Contains(result.Summary, "Automatic Intent recovery is active") ||
		!strings.Contains(result.NextAction, "keep replanning the exact recovery target") {
		t.Fatalf("stalled intent recovery control=%+v", result)
	}
	if label := publicationProgressPhaseLabel(progress); !strings.Contains(
		label, "automatic Intent recovery active",
	) {
		t.Fatalf("stalled intent recovery label=%q", label)
	}

	marker.Stage = "local_unlock"
	if err := state.MetaSetJSON(ctx, db, "intent.v2.forward_recovery",
		marker); err != nil {
		t.Fatal(err)
	}
	legacyLocal, err := buildPublicationProgressReport(ctx, db.SQL(), report, now)
	if err != nil {
		t.Fatal(err)
	}
	if legacyLocal.Phase != "intent_replanning" ||
		legacyLocal.TemporaryLocalFallback {
		t.Fatalf("unanchored local recovery progress=%+v", legacyLocal)
	}

	marker.PlanFingerprint = "sha256:verified-plan"
	marker.PrefixCursor = 1
	marker.PrefixUnresolvedCount = 1
	marker.PrefixBaseHead = "base-head"
	if err := state.MetaSetJSON(ctx, db, "intent.v2.forward_recovery",
		marker); err != nil {
		t.Fatal(err)
	}
	local, err := buildPublicationProgressReport(ctx, db.SQL(), report, now)
	if err != nil {
		t.Fatal(err)
	}
	if local.Phase != "local_fallback" || !local.TemporaryLocalFallback ||
		productListPhase(productListEntry{PublicationProgress: local}) != "intent-widen" {
		t.Fatalf("local Intent recovery progress=%+v", local)
	}
	var localHuman bytes.Buffer
	renderProductPublicationProgress(&localHuman, local)
	if !strings.Contains(localHuman.String(), "widening a verified Intent group") ||
		strings.Contains(localHuman.String(), "temporary local fallback") {
		t.Fatalf("local Intent recovery wording:\n%s", localHuman.String())
	}
	report.PublicationProgress = local
	result = controlResult{OK: true}
	applyControlStatusWithDaemonAlive(&result, report, true)
	if result.Health != controlHealthPublishing ||
		!strings.Contains(result.Summary, "widening a verified Intent group") {
		t.Fatalf("local Intent recovery control=%+v", result)
	}

	marker.PrefixExhausted = true
	marker.NeedsAttention = true
	marker.AttentionReason = "complete semantic prefix failed verification"
	if err := state.MetaSetJSON(ctx, db, "intent.v2.forward_recovery",
		marker); err != nil {
		t.Fatal(err)
	}
	blocked, err := buildPublicationProgressReport(ctx, db.SQL(), report, now)
	if err != nil {
		t.Fatal(err)
	}
	blockedEntry := productListEntry{PublicationProgress: blocked}
	if blocked.Phase != "needs_action" || !blocked.NeedsAttention ||
		blocked.AttentionReason != marker.AttentionReason ||
		productListTarget(blockedEntry) != "recover:1/2" ||
		productListPhase(blockedEntry) != "blocked" ||
		productListStatus(blockedEntry) != "needs action" {
		t.Fatalf("exhausted Intent recovery progress=%+v target=%q phase=%q status=%q",
			blocked, productListTarget(blockedEntry), productListPhase(blockedEntry),
			productListStatus(blockedEntry))
	}
	var blockedHuman bytes.Buffer
	renderProductPublicationProgress(&blockedHuman, blocked)
	for _, want := range []string{
		intentRecoveryVerificationAttentionSummary,
		"Run `acd doctor`",
		"Recovery reason: complete semantic prefix failed verification",
	} {
		if !strings.Contains(blockedHuman.String(), want) {
			t.Fatalf("exhausted recovery status missing %q:\n%s",
				want, blockedHuman.String())
		}
	}
	if strings.Contains(blockedHuman.String(), "commit-all") {
		t.Fatalf("exhausted recovery suggested commit-all:\n%s",
			blockedHuman.String())
	}
	report.PublicationProgress = blocked
	report.Repo = "/repo"
	var verbose bytes.Buffer
	if err := renderStatusHuman(&verbose, report); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(verbose.String(), intentRecoveryVerificationAttentionSummary) ||
		!strings.Contains(verbose.String(), "Run `acd doctor`") ||
		strings.Contains(verbose.String(), "commit-all") {
		t.Fatalf("verbose exhausted recovery status:\n%s", verbose.String())
	}
	result = controlResult{OK: true}
	applyControlStatusWithDaemonAlive(&result, report, true)
	if result.OK || result.Health != controlHealthNeedsAttention ||
		result.Summary != intentRecoveryVerificationAttentionSummary ||
		!strings.Contains(result.NextAction, "acd doctor") ||
		strings.Contains(result.NextAction, "commit-all") {
		t.Fatalf("exhausted Intent recovery control=%+v", result)
	}
	var controlHuman bytes.Buffer
	if err := renderProductEnvelope(&controlHuman,
		envelopeFromControl(result), false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"State: needs_action",
		"Action needed: yes",
		"Status: " + intentRecoveryVerificationAttentionSummary,
		"Next: " + intentRecoveryVerificationAttentionNext,
	} {
		if !strings.Contains(controlHuman.String(), want) {
			t.Fatalf("control status missing %q:\n%s", want,
				controlHuman.String())
		}
	}

	report.BranchGeneration = 8
	stale, err := buildPublicationProgressReport(ctx, db.SQL(), report, now)
	if err != nil {
		t.Fatal(err)
	}
	if stale.Origin == "intent_recovery" || stale.TargetTotal != 0 {
		t.Fatalf("stale recovery marker leaked into progress=%+v", stale)
	}
}

func TestActiveIntentRecoveryShowsOpenProviderWait(t *testing.T) {
	ctx := context.Background()
	_, _, db := makeRepoStateDB(t)
	now := time.Unix(1_000, 0)
	seqs := insertCompletedCheckpoint(t, db, "cp-provider-wait-recovery",
		"0123456789abcdef", []checkpointMemberFixture{{
			State: state.EventStatePending,
		}})
	if err := state.MetaSetJSON(ctx, db, "intent.v2.forward_recovery",
		state.IntentForwardRecovery{
			BranchRef: "refs/heads/main", BranchGeneration: 7,
			CandidateID: "failed-candidate", Stage: "semantic_replan",
			TargetEventSeqs: seqs, LastProgressTS: 900,
		}); err != nil {
		t.Fatal(err)
	}
	report := statusReport{
		Daemon: "running", PID: os.Getpid(), PendingEvents: 1,
		BranchRef: "refs/heads/main", BranchGeneration: 7,
		CheckpointProtectionAvailable: true, Protected: true, Busy: true,
		IntentStrategy: intentStrategyReport{
			Strategy: "intent", Active: true,
			PlannerHealth: &daemon.IntentPlannerHealthSnapshot{
				State: daemon.IntentPlannerCircuitOpen, NextProbeTS: 1_045,
			},
		},
	}
	progress, err := buildPublicationProgressReport(ctx, db.SQL(), report, now)
	if err != nil {
		t.Fatal(err)
	}
	entry := productListEntry{PublicationProgress: progress}
	if progress.Origin != "intent_recovery" || progress.Phase != "provider_wait" ||
		progress.WaitRemainingSeconds != 45 || progress.TargetRemaining != 1 ||
		progress.TargetTotal != 1 || productListTarget(entry) != "recover:1/1" ||
		productListPhase(entry) != "provider-wait:45s" ||
		productListStatus(entry) != "waiting" {
		t.Fatalf("provider-wait recovery progress=%+v target=%q phase=%q status=%q",
			progress, productListTarget(entry), productListPhase(entry),
			productListStatus(entry))
	}
	var human bytes.Buffer
	renderProductPublicationProgress(&human, progress)
	for _, want := range []string{
		"Active target: automatic Intent recovery, 1 of 1 left",
		"Publication phase: waiting for the Intent provider retry (45s remaining)",
	} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("provider-wait recovery status missing %q:\n%s",
				want, human.String())
		}
	}

	report.PublicationProgress = progress
	result := controlResult{OK: true}
	applyControlStatusWithDaemonAlive(&result, report, true)
	if result.Health != controlHealthWaiting ||
		!strings.Contains(result.Summary, "recovery target and your work remain protected") ||
		!strings.Contains(result.NextAction, "retry in 45s") {
		t.Fatalf("provider-wait recovery control=%+v", result)
	}

	// Half-open owns the sole provider probe lease, so the active recovery
	// phase remains visible while that request is in flight.
	report.IntentStrategy.PlannerHealth.State = daemon.IntentPlannerCircuitHalfOpen
	halfOpen, err := buildPublicationProgressReport(ctx, db.SQL(), report, now)
	if err != nil {
		t.Fatal(err)
	}
	if halfOpen.Phase != "intent_replanning" ||
		halfOpen.WaitRemainingSeconds != 0 {
		t.Fatalf("half-open recovery progress=%+v", halfOpen)
	}
}

func TestActiveIntentRecoveryUsesMarkerUpdateForProgress(t *testing.T) {
	t.Setenv("ACD_AI_TIMEOUT", "1m")
	ctx := context.Background()
	_, _, db := makeRepoStateDB(t)
	now := time.Now()
	staleTS := float64(now.Add(-5*time.Minute).UnixNano()) / 1e9
	seqs := insertCompletedCheckpoint(t, db, "cp-marker-progress",
		"0123456789abcdef", []checkpointMemberFixture{{
			State: state.EventStatePending,
		}})
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events SET captured_ts=? WHERE seq=?`, staleTS, seqs[0]); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSetJSON(ctx, db, "intent.v2.forward_recovery",
		state.IntentForwardRecovery{
			BranchRef: "refs/heads/main", BranchGeneration: 7,
			CandidateID: "failed-candidate", Stage: "local_unlock",
			TargetEventSeqs: seqs, LastProgressTS: staleTS,
			PlanFingerprint: "sha256:verified-plan", PrefixCursor: 1,
			PrefixUnresolvedCount: 1, PrefixBaseHead: "base-head",
		}); err != nil {
		t.Fatal(err)
	}
	report := statusReport{
		Daemon: "running", PID: os.Getpid(), PendingEvents: 1,
		BranchRef: "refs/heads/main", BranchGeneration: 7,
		CheckpointProtectionAvailable: true, Protected: true, Busy: true,
		IntentStrategy: intentStrategyReport{Strategy: "intent", Active: true},
	}
	progress, err := buildPublicationProgressReport(ctx, db.SQL(), report, now)
	if err != nil {
		t.Fatal(err)
	}
	if progress.Phase != "local_fallback" ||
		progress.LastProgressTS <= staleTS {
		t.Fatalf("fresh marker progress=%+v", progress)
	}

	// If the marker itself stops changing, its durable update boundary ages
	// normally and the same recovery can still be reported as stalled.
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE daemon_meta SET updated_ts=? WHERE key=?`,
		staleTS, "intent.v2.forward_recovery"); err != nil {
		t.Fatal(err)
	}
	stalled, err := buildPublicationProgressReport(ctx, db.SQL(), report, now)
	if err != nil {
		t.Fatal(err)
	}
	if stalled.Phase != "stalled" || stalled.LastProgressTS != staleTS ||
		stalled.LastProgressAgeSeconds < stalled.StallThresholdSeconds {
		t.Fatalf("aged marker progress=%+v", stalled)
	}
}

func TestStatusProjectsFailedVerificationAsAutomaticRecovery(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "recover.go",
		Fidelity: "exact", CapturedTS: nowFloat(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SaveIntentCandidate(ctx, d, state.IntentCandidate{
		ID: "verification-recovery", BranchRef: "refs/heads/main",
		BranchGeneration: 1, Status: state.IntentCandidateWaiting,
		Readiness: state.IntentReadinessWait,
		VerificationStatus: sql.NullString{
			String: "failed", Valid: true,
		},
		Events: []state.IntentCandidateEvent{{
			EventSeq: seq, EventRole: "implementation",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSetMany(ctx, d, map[string]string{
		"commit.strategy":           "intent",
		"intent.v2.migration_state": "active",
	}); err != nil {
		t.Fatal(err)
	}

	record := central.RepoRecord{
		Path: repo, StateDB: dbPath, RepoHash: central.CanonicalID(repo),
	}
	report, err := buildStatusReport(ctx, record, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if report.IntentV2.VerificationRecovering != 1 ||
		report.IntentV2.VerificationAttention != 0 ||
		report.PublicationProgress.Phase != "intent_verification_recovery" {
		t.Fatalf("verification recovery report=%+v progress=%+v",
			report.IntentV2, report.PublicationProgress)
	}
}

func TestPublicationProgressPrioritizesDeliberateWaits(t *testing.T) {
	t.Setenv("ACD_AI_TIMEOUT", "1m")
	now := time.Unix(1_000, 0)
	base := statusReport{
		Daemon: "running", PID: os.Getpid(), PendingEvents: 2,
		Busy: true, CheckpointProtectionAvailable: true, Protected: true,
		IntentStrategy: intentStrategyReport{Strategy: "intent", Active: true},
		PublicationDrain: publicationDrainReport{
			ID: "drain", Phase: state.PublicationDrainSemantic,
			TargetEvents: 2, RemainingEvents: 2, LastProgressTS: 100,
		},
	}
	tests := []struct {
		name     string
		mutate   func(*statusReport)
		want     string
		wantWait int64
	}{
		{name: "manual pause", mutate: func(report *statusReport) {
			report.Paused = true
			report.Pause = &pauseInfo{Source: "manual"}
		}, want: "paused"},
		{name: "rewind grace", mutate: func(report *statusReport) {
			report.Paused = true
			report.Pause = &pauseInfo{Source: "rewind_grace", RemainingSeconds: 23}
		}, want: "rewind_wait", wantWait: 23},
		{name: "configuration validation", mutate: func(report *statusReport) {
			report.Configuration.Configuration = "validating"
		}, want: "config_wait"},
		{name: "checkpoint protection", mutate: func(report *statusReport) {
			report.Protected = false
		}, want: "checkpointing"},
		{name: "provider circuit", mutate: func(report *statusReport) {
			report.IntentStrategy.PlannerHealth = &daemon.IntentPlannerHealthSnapshot{
				State: daemon.IntentPlannerCircuitOpen,
			}
		}, want: "provider_wait"},
		{name: "ordinary intent wait", mutate: func(report *statusReport) {
			report.PublicationDrain = publicationDrainReport{}
			report.IntentStrategy.BatchWaitActive = true
			report.IntentStrategy.BatchWaitReason = "skipped_due_intent_batch_wait"
			report.IntentStrategy.AgeTriggerInSeconds = 31
		}, want: "intent_wait", wantWait: 31},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report := base
			test.mutate(&report)
			progress, err := buildPublicationProgressReport(
				context.Background(), nil, report, now)
			if err != nil {
				t.Fatal(err)
			}
			if progress.Phase != test.want ||
				progress.WaitRemainingSeconds != test.wantWait {
				t.Fatalf("progress=%+v, want phase=%q wait=%d",
					progress, test.want, test.wantWait)
			}
		})
	}
}

func TestRenderPublicationDrainHumanSuppressesCompletedHistory(t *testing.T) {
	var out bytes.Buffer
	renderPublicationDrainHuman(&out, publicationDrainReport{
		Available: true, ID: "old-drain", Phase: state.PublicationDrainCompleted,
		TargetEvents: 2, PublishedEvents: 2,
	})
	if out.Len() != 0 {
		t.Fatalf("completed drain rendered as current work: %q", out.String())
	}
}

func TestStatus_RepeatedReplayErrorIsRetrying(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")

	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "same.go",
		Fidelity: "exact", CapturedTS: nowFloat(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"candidate-a", "candidate-b"} {
		if err := state.SaveIntentCandidate(ctx, d, state.IntentCandidate{
			ID: id, BranchRef: "refs/heads/main", BranchGeneration: 1,
			Status: state.IntentCandidateWaiting, Purpose: "retain work",
			Readiness: state.IntentReadinessWait,
			Events: []state.IntentCandidateEvent{{
				EventSeq: seq, EventRole: "code",
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	lastError := fmt.Sprintf(
		`fallback capture %d connects persisted candidates "candidate-a" and "candidate-b"`,
		seq)
	if err := state.MetaSetMany(ctx, d, map[string]string{
		"last_replay_error":         lastError,
		"replay.error_repeat_count": "3",
		"replay.error_last_seen_ts": "1786464200",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.AppendIntentPlannerWindow(ctx, d,
		state.IntentPlannerWindow{
			PlannedTS: nowFloat(), BranchRef: "refs/heads/main",
			BranchGeneration: 1, OfferedSeqs: []int64{seq, seq + 1},
			VisibleOriginalSeqs: []int64{seq, seq + 1},
			SelectedGroups: []state.IntentPlannerWindowGroup{{
				SelectedSeqs: []int64{seq, seq + 1},
			}},
			FallbackUsed: true,
			Outcome: sql.NullString{
				String: "provider_error_fallback_selected", Valid: true,
			},
		}); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := runStatus(ctx, &out, repo, true); err != nil {
		t.Fatal(err)
	}
	var report statusReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Replay.State != "degraded" ||
		report.Replay.ErrorRepeatCount != 3 ||
		report.Replay.ErrorLastSeenTS != 1786464200 ||
		report.Replay.BlockedSeq != seq ||
		report.Replay.LastFallbackMode != "provider_error_fallback_selected" ||
		report.Replay.LastFallbackSize != 2 {
		t.Fatalf("replay report=%+v", report.Replay)
	}
	if report.IntentV2.ReplayState != "degraded" {
		t.Fatalf("Intent v2 replay state=%q, want degraded: %+v",
			report.IntentV2.ReplayState, report.IntentV2)
	}
	if len(report.Replay.CandidateIDs) != 2 {
		t.Fatalf("candidate IDs=%v", report.Replay.CandidateIDs)
	}

	out.Reset()
	if err := runStatus(ctx, &out, repo, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Replay: degraded", "repeats=3", "last_seen=1786464200",
		fmt.Sprintf("blocked_seq=%d", seq),
		"candidate-a", "candidate-b",
		"fallback=provider_error_fallback_selected size=2",
		"Last replay error:",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("status output missing %q:\n%s", want, out.String())
		}
	}
}

func TestReplayObservabilityProjectionHidesOrphanedRepeatMetadata(t *testing.T) {
	_, _, d := makeRepoStateDB(t)
	ctx := context.Background()
	if err := state.MetaSetMany(ctx, d, map[string]string{
		"last_replay_error":         "",
		"replay.error_repeat_count": "19",
		"replay.error_last_seen_ts": "123456",
	}); err != nil {
		t.Fatal(err)
	}

	report, err := loadReplayObservabilityReport(ctx, d.SQL())
	if err != nil {
		t.Fatal(err)
	}
	if report.State != "active" || report.LastError != "" ||
		report.ErrorRepeatCount != 0 || report.BlockedSeq != 0 ||
		len(report.CandidateIDs) != 0 {
		t.Fatalf("orphaned repeat metadata leaked into projection: %+v", report)
	}
}

func TestStatusSelfPublicationHumanJSONParity(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	now := time.Now()
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running",
		HeartbeatTS: float64(now.UnixNano()) / 1e9,
	}); err != nil {
		t.Fatal(err)
	}
	source := strings.Repeat("a", 40)
	target := strings.Repeat("b", 40)
	seedCLISelfPublication(t, d, "publication-status", source, target,
		state.SelfPublicationPrepared, now)

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatal(err)
	}
	var report statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if !report.SelfPublication.Available ||
		report.SelfPublication.Phase != "active" ||
		report.SelfPublication.JournalPhase != state.SelfPublicationPrepared ||
		report.SelfPublication.SourceHead != source[:12] ||
		report.SelfPublication.TargetHead != target[:12] {
		t.Fatalf("self-publication report=%+v", report.SelfPublication)
	}
	if strings.Contains(jsonOut.String(), source) ||
		strings.Contains(jsonOut.String(), target) {
		t.Fatalf("full publication OID leaked into JSON: %s", jsonOut.String())
	}

	var human bytes.Buffer
	if err := runStatus(ctx, &human, repo, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Self-publication: phase=active",
		"journal=prepared",
		"source=" + source[:12],
		"target=" + target[:12],
	} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("human status missing %q:\n%s", want, human.String())
		}
	}
	if strings.Contains(human.String(), source) ||
		strings.Contains(human.String(), target) {
		t.Fatalf("full publication OID leaked into human output: %s",
			human.String())
	}
}

func TestDiagnoseWriterSelfPublicationRemediationParity(t *testing.T) {
	ctx := context.Background()
	_, dbPath, d := makeRepoStateDB(t)
	now := time.Now()
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running",
		HeartbeatTS: float64(now.UnixNano()) / 1e9,
	}); err != nil {
		t.Fatal(err)
	}
	seedCLISelfPublication(t, d, "publication-writer",
		strings.Repeat("c", 40), strings.Repeat("d", 40),
		state.SelfPublicationGitApplied, now)

	report, err := loadSelfPublicationReport(
		ctx, d.SQL(), dbPath, now, 2)
	if err != nil {
		t.Fatal(err)
	}
	if report.Phase != "stale" ||
		report.RemediationKind != "stop_old_owner" ||
		!strings.Contains(report.Remediation, "stable repository lock") {
		t.Fatalf("duplicate-writer report=%+v", report)
	}

	var statusOut, diagnoseOut, doctorOut bytes.Buffer
	if err := renderStatusHuman(&statusOut,
		statusReport{SelfPublication: report}); err != nil {
		t.Fatal(err)
	}
	if err := renderDiagnoseHuman(&diagnoseOut, diagnoseReport{
		SelfPublication:         report,
		StateDBChecksumVerified: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := renderDoctorHuman(&doctorOut, doctorReport{
		Repos: []doctorRepoReport{{SelfPublication: report}},
	}); err != nil {
		t.Fatal(err)
	}
	for name, output := range map[string]string{
		"status": statusOut.String(), "diagnose": diagnoseOut.String(),
		"doctor": doctorOut.String(),
	} {
		for _, want := range []string{
			"Self-publication: phase=stale",
			"writers=2",
			"remediation (stop_old_owner)",
		} {
			if !strings.Contains(output, want) {
				t.Fatalf("%s output missing %q:\n%s", name, want, output)
			}
		}
	}
}

func TestSelfPublicationDurableAttentionStatusDiagnoseDoctorParity(
	t *testing.T,
) {
	ctx := context.Background()
	_, dbPath, d := makeRepoStateDB(t)
	now := time.Now()
	seedCLISelfPublication(t, d, "publication-attention",
		strings.Repeat("a", 40), strings.Repeat("b", 40),
		state.SelfPublicationGitApplied, now)
	unsafe := "Automatic recovery is blocked: api_key=sk-private prompt=repository_diff=secret\x1b[31m"
	if err := state.SetSelfPublicationRecoveryAttention(
		ctx, d, "publication-attention", unsafe); err != nil {
		t.Fatal(err)
	}

	report, err := loadSelfPublicationReport(
		ctx, d.SQL(), dbPath, now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.Phase != "needs_attention" ||
		report.RemediationKind != "needs_attention" ||
		report.NeedsAttention == "" ||
		report.Remediation != report.NeedsAttention {
		t.Fatalf("durable-attention report=%+v", report)
	}

	var statusOut, diagnoseOut, doctorOut bytes.Buffer
	if err := renderStatusHuman(&statusOut,
		statusReport{SelfPublication: report}); err != nil {
		t.Fatal(err)
	}
	if err := renderDiagnoseHuman(&diagnoseOut, diagnoseReport{
		SelfPublication: report, StateDBChecksumVerified: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := renderDoctorHuman(&doctorOut, doctorReport{
		Repos: []doctorRepoReport{{SelfPublication: report}},
	}); err != nil {
		t.Fatal(err)
	}
	jsonBody, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	combined := string(jsonBody) + statusOut.String() +
		diagnoseOut.String() + doctorOut.String()
	for _, want := range []string{
		"phase=needs_attention", "remediation (needs_attention)",
	} {
		if !strings.Contains(combined, want) {
			t.Fatalf("shared projection missing %q: %s", want, combined)
		}
	}
	for _, forbidden := range []string{
		"sk-private", "repository_diff=secret", "\x1b",
	} {
		if strings.Contains(combined, forbidden) {
			t.Fatalf("shared projection leaked %q: %s", forbidden, combined)
		}
	}
}

func TestSelfPublicationWriterWinsOverRecoveryAttentionParity(t *testing.T) {
	ctx := context.Background()
	_, dbPath, d := makeRepoStateDB(t)
	now := time.Now()
	seedCLISelfPublication(t, d, "publication-writer-attention",
		strings.Repeat("a", 40), strings.Repeat("b", 40),
		state.SelfPublicationGitApplied, now)
	if err := state.SetSelfPublicationRecoveryAttention(
		ctx, d, "publication-writer-attention",
		"Automatic recovery is blocked"); err != nil {
		t.Fatal(err)
	}

	report, err := loadSelfPublicationReport(
		ctx, d.SQL(), dbPath, now, 2)
	if err != nil {
		t.Fatal(err)
	}
	if report.Phase != "stale" ||
		report.RemediationKind != "stop_old_owner" ||
		report.NeedsAttention == "" {
		t.Fatalf("writer-attention report=%+v", report)
	}

	var statusOut, diagnoseOut, doctorOut bytes.Buffer
	if err := renderStatusHuman(&statusOut,
		statusReport{SelfPublication: report}); err != nil {
		t.Fatal(err)
	}
	if err := renderDiagnoseHuman(&diagnoseOut, diagnoseReport{
		SelfPublication: report, StateDBChecksumVerified: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := renderDoctorHuman(&doctorOut, doctorReport{
		Repos: []doctorRepoReport{{SelfPublication: report}},
	}); err != nil {
		t.Fatal(err)
	}
	jsonBody, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(jsonBody), `"phase":"stale"`) ||
		!strings.Contains(
			string(jsonBody), `"remediation_kind":"stop_old_owner"`) {
		t.Fatalf("JSON masks split-brain remediation: %s", jsonBody)
	}
	for name, output := range map[string]string{
		"status": statusOut.String(), "diagnose": diagnoseOut.String(),
		"doctor": doctorOut.String(),
	} {
		for _, want := range []string{
			"Self-publication: phase=stale",
			"writers=2",
			"remediation (stop_old_owner)",
		} {
			if !strings.Contains(output, want) {
				t.Fatalf("%s missing %q: %s", name, want, output)
			}
		}
	}
}

func TestSelfPublicationUnknownPreMarkerStatusDiagnoseDoctorParity(
	t *testing.T,
) {
	ctx := context.Background()
	_, dbPath, d := makeRepoStateDB(t)
	now := time.Now()
	seedCLIUnknownSelfPublication(t, d, "unknown-pre-marker", now)
	if _, ok, err := state.MetaGet(
		ctx, d, state.SelfPublicationNeedsAttentionMetaKey); err != nil || ok {
		t.Fatalf("unexpected preexisting attention ok=%v err=%v", ok, err)
	}

	report, err := loadSelfPublicationReport(
		ctx, d.SQL(), dbPath, now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.Phase != "needs_attention" ||
		report.JournalPhase != state.SelfPublicationPrepared ||
		report.RemediationKind != "needs_attention" ||
		!strings.Contains(report.NeedsAttention, "unknown completion") {
		t.Fatalf("unknown pre-marker report=%+v", report)
	}

	var statusOut, diagnoseOut, doctorOut bytes.Buffer
	if err := renderStatusHuman(&statusOut,
		statusReport{SelfPublication: report}); err != nil {
		t.Fatal(err)
	}
	if err := renderDiagnoseHuman(&diagnoseOut, diagnoseReport{
		SelfPublication: report, StateDBChecksumVerified: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := renderDoctorHuman(&doctorOut, doctorReport{
		Repos: []doctorRepoReport{{SelfPublication: report}},
	}); err != nil {
		t.Fatal(err)
	}
	for name, output := range map[string]string{
		"status": statusOut.String(), "diagnose": diagnoseOut.String(),
		"doctor": doctorOut.String(),
	} {
		for _, want := range []string{
			"phase=needs_attention", "remediation (needs_attention)",
			"unknown completion",
		} {
			if !strings.Contains(output, want) {
				t.Fatalf("%s missing %q: %s", name, want, output)
			}
		}
	}
}

func TestDoctorPublicationStaleWakeDiagnosis(t *testing.T) {
	ctx := context.Background()
	_, dbPath, d := makeRepoStateDB(t)
	now := time.Now()
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running",
		HeartbeatTS: float64(now.Add(-10*time.Second).UnixNano()) / 1e9,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.EnqueueFlushRequest(ctx, d, "wake", false,
		sql.NullString{}); err != nil {
		t.Fatal(err)
	}
	report, err := loadSelfPublicationReport(
		ctx, d.SQL(), dbPath, now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.Phase != "stale" || !report.HeartbeatStale ||
		report.PendingWakes != 1 ||
		report.RemediationKind != "needs_attention" {
		t.Fatalf("stale-wake report=%+v", report)
	}
	if strings.Contains(strings.ToLower(report.Remediation), "purge") {
		t.Fatalf("destructive remediation surfaced: %q", report.Remediation)
	}
}

func TestStatusSelfPublicationHeartbeatFractionBelowBudget(t *testing.T) {
	ctx := context.Background()
	_, dbPath, d := makeRepoStateDB(t)
	now := time.Unix(1000, 200_000_000)
	heartbeat := now.Add(-2900 * time.Millisecond)
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running",
		HeartbeatTS: float64(heartbeat.UnixNano()) / 1e9,
	}); err != nil {
		t.Fatal(err)
	}
	report, err := loadSelfPublicationReport(
		ctx, d.SQL(), dbPath, now, 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.HeartbeatStale || report.Phase != "active" {
		t.Fatalf("fractional heartbeat falsely stale: %+v", report)
	}
}

func TestStatusSelfPublicationSanitizesCorruptJournalIDs(t *testing.T) {
	ctx := context.Background()
	_, dbPath, d := makeRepoStateDB(t)
	now := time.Now()
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running",
		HeartbeatTS: float64(now.UnixNano()) / 1e9,
	}); err != nil {
		t.Fatal(err)
	}
	source := "\x1b[31mapi_key=sk-private"
	target := "prompt=private-repository-payload"
	seedCLISelfPublication(t, d, "publication-corrupt", source, target,
		state.SelfPublicationPrepared, now)
	report, err := loadSelfPublicationReport(
		ctx, d.SQL(), dbPath, now, 1)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var human bytes.Buffer
	renderSelfPublicationHuman(&human, report, "")
	combined := string(body) + human.String()
	for _, forbidden := range []string{
		"\x1b", "sk-private", "private-repository-payload",
	} {
		if strings.Contains(combined, forbidden) {
			t.Fatalf("corrupt journal value leaked %q: %s", forbidden, combined)
		}
	}
	if !strings.Contains(combined, "[redacted") {
		t.Fatalf("sanitized marker missing: %s", combined)
	}
}

func TestPreV18SelfPublicationReadOnlyChecksum(t *testing.T) {
	ctx := context.Background()
	_, dbPath, d := makeRepoStateDB(t)
	if _, err := d.SQL().ExecContext(ctx, `PRAGMA user_version=17`); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQL().ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := fileSHA256(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := openStateDBReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	report, err := loadSelfPublicationReport(ctx, conn, dbPath, time.Now(), 0)
	if err != nil {
		t.Fatal(err)
	}
	after, err := fileSHA256(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if report.Available || report.SchemaVersion != 17 ||
		report.Phase != "unavailable" {
		t.Fatalf("pre-v18 report=%+v", report)
	}
	if before != after {
		t.Fatalf("read-only projection changed state.db: before=%s after=%s",
			before, after)
	}
}

func seedCLISelfPublication(
	t *testing.T,
	d *state.DB,
	id, source, target, phase string,
	now time.Time,
) {
	t.Helper()
	ts := float64(now.UnixNano()) / 1e9
	digest := "sha256:" + strings.Repeat("0", 64)
	if _, err := d.SQL().Exec(`
INSERT INTO self_publications(
    id, branch_ref, branch_generation, source_head, target_commit_oid,
    target_tree_oid, membership_digest, member_count, phase, created_ts,
    updated_ts, git_applied_ts, completion_published_ts,
    completion_candidate_status
) VALUES (?, 'refs/heads/main', 1, ?, ?, ?, ?, 1, 'prepared', ?, ?,
          NULL, ?, 'published')`,
		id, source, target, strings.Repeat("e", 40), digest, ts, ts, ts,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQL().Exec(`
INSERT INTO self_publication_members(publication_id, ord, event_seq)
VALUES (?, 0, 1)`, id); err != nil {
		t.Fatal(err)
	}
	if phase == state.SelfPublicationGitApplied {
		if _, err := d.SQL().Exec(`
UPDATE self_publications
SET phase='git_applied', updated_ts=?, git_applied_ts=?
WHERE id=?`, ts, ts, id); err != nil {
			t.Fatal(err)
		}
	} else if phase != state.SelfPublicationPrepared {
		t.Fatalf("unsupported fixture phase %q", phase)
	}
}

func seedCLIUnknownSelfPublication(
	t *testing.T,
	d *state.DB,
	id string,
	now time.Time,
) {
	t.Helper()
	ts := float64(now.UnixNano()) / 1e9
	digest := "sha256:" + strings.Repeat("1", 64)
	if _, err := d.SQL().Exec(`
INSERT INTO self_publications(
    id, branch_ref, branch_generation, source_head, target_commit_oid,
    target_tree_oid, membership_digest, member_count, phase, created_ts,
    updated_ts, completion_published_ts, completion_candidate_status
) VALUES (?, 'refs/heads/migrated', 2, 'old-source', 'old-target',
          'old-tree', ?, 1, 'prepared', ?, ?, 0, 'unknown')`,
		id, digest, ts, ts); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQL().Exec(`
INSERT INTO self_publication_members(publication_id, ord, event_seq)
VALUES (?, 0, 1)`, id); err != nil {
		t.Fatal(err)
	}
}

func TestStatusRuntimeConfigHumanJSONAndRedaction(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Global[config.FieldModel] = json.RawMessage(`"saved-model"`)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	baseline := cliRuntimeRevision(t, d, "baseline", 1)
	candidate := cliRuntimeRevision(t, d, "candidate", 1)
	request, ok, err := state.RequestConfigActivation(ctx, d, baseline.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatalf("baseline request: %v %v", ok, err)
	}
	_, _ = state.AcknowledgeConfigActivation(ctx, d, request.ID, baseline.ID)
	_, _ = state.ApplyConfigActivation(ctx, d, request.ID, baseline.ID)
	pending, ok, err := state.RequestConfigActivation(ctx, d, candidate.ID, sql.NullInt64{Int64: baseline.ID, Valid: true})
	if err != nil || !ok {
		t.Fatalf("candidate request: %v %v", ok, err)
	}
	_, _ = state.RejectConfigActivation(ctx, d, pending.ID, candidate.ID, "rejected")
	unsafe := "https://user:password@provider.invalid api_key=sk-visible prompt=private repository_diff=secret provider_response=raw\x1b[31m"
	if _, err := d.SQL().Exec(`UPDATE runtime_config_state SET last_error=? WHERE id=1`, unsafe); err != nil {
		t.Fatal(err)
	}
	experiment, err := state.CreateConfigExperiment(ctx, d, state.ConfigExperimentInput{
		BaselineRevisionID: baseline.ID, CandidateRevisionID: candidate.ID,
		WindowBudget: 10, ExpiresTS: sql.NullFloat64{Float64: float64(time.Now().Add(time.Hour).Unix()), Valid: true},
		FailurePolicy: "continue",
	})
	if err != nil {
		t.Fatal(err)
	}
	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatal(err)
	}
	var report statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	runtime := report.RuntimeConfig
	if runtime.SavedGeneration != 1 || runtime.DesiredRevisionID != candidate.ID ||
		runtime.AppliedRevisionID != baseline.ID || runtime.LastKnownGoodRevisionID != baseline.ID ||
		runtime.Profile != "profile-a" || runtime.ApplyState != "rejected" ||
		runtime.ApplyBoundary != "next_work_boundary" || runtime.Experiment == nil ||
		runtime.Experiment.ID != experiment.ID || runtime.Experiment.WindowBudget != 10 {
		t.Fatalf("runtime JSON projection = %+v", runtime)
	}
	var human bytes.Buffer
	if err := runStatus(ctx, &human, repo, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Runtime settings: rejected", "desired=", "known_good=", "boundary=next_work_boundary", "saved_generation=1", "Experiment #"} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("human runtime output missing %q:\n%s", want, human.String())
		}
	}
	combined := jsonOut.String() + human.String()
	for _, forbidden := range []string{"user:password", "sk-visible", "prompt=private", "repository_diff=secret", "provider_response=raw", "\x1b"} {
		if strings.Contains(combined, forbidden) {
			t.Fatalf("runtime observability leaked %q:\n%s", forbidden, combined)
		}
	}
}

func TestStatusIntentV2ProjectionAndRedaction(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")

	snapshot, err := json.Marshal(map[string]any{
		"preset_id":                        "intent.balanced",
		"preset_version":                   config.PresetCatalogVersion,
		"customized":                       true,
		config.FieldIntentVerification:     "fast",
		config.FieldIntentRepairEnabled:    "true",
		config.FieldIntentRepairHorizon:    "10m",
		config.FieldIntentRepairMaxCommits: "3",
	})
	if err != nil {
		t.Fatal(err)
	}
	revision, err := state.InsertConfigRevision(ctx, d,
		state.ConfigRevisionInput{
			Snapshot: snapshot, Profile: "intent-v2", Scope: "repository",
			SourceGeneration: 1,
		})
	if err != nil {
		t.Fatal(err)
	}
	request, ok, err := state.RequestConfigActivation(ctx, d, revision.ID,
		sql.NullInt64{})
	if err != nil || !ok {
		t.Fatalf("request activation: ok=%v err=%v", ok, err)
	}
	_, _ = state.AcknowledgeConfigActivation(ctx, d, request.ID, revision.ID)
	_, _ = state.ApplyConfigActivation(ctx, d, request.ID, revision.ID)

	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 7,
		BaseHead: "base", Operation: "modify", Path: "service.go",
		Fidelity: "exact", CapturedTS: nowFloat(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.SaveIntentCandidate(ctx, d, state.IntentCandidate{
		ID: "candidate-observable", BranchRef: "refs/heads/main",
		BranchGeneration: 7, Status: state.IntentCandidateBlocked,
		Readiness: state.IntentReadinessWait, Purpose: "one purpose",
		PlannerProtocol:  sql.NullString{String: "v2", Valid: true},
		AtomicityStatus:  sql.NullString{String: "invalid", Valid: true},
		AtomicitySummary: "api_key=sk-hidden disconnected components",
		VerificationStatus: sql.NullString{
			String: "needs_attention", Valid: true,
		},
		Events: []state.IntentCandidateEvent{{
			EventSeq: seq, EventRole: "code",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.AppendIntentActivityBoundary(ctx, d,
		state.IntentActivityBoundary{
			Kind: state.IntentBoundaryHard, Source: "logical_flush",
		}); err != nil {
		t.Fatal(err)
	}
	if err := state.SaveIntentRepair(ctx, d, state.IntentRepair{
		ID: "repair-observable", BranchRef: "refs/heads/main",
		BranchGeneration: 7, Status: state.IntentRepairPrepared,
		ExpectedHead: "old-head",
		PlanDigest:   "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Error:        "prompt=private",
		Commits:      []state.IntentRepairCommit{{OldOID: "old-head"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSetMany(ctx, d, map[string]string{
		"intent.v2.migration_state": "active",
		"intent.v2.needs_attention": "run acd configure",
	}); err != nil {
		t.Fatal(err)
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatal(err)
	}
	var report statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	got := report.IntentV2
	if !got.Available || got.SchemaVersion != state.SchemaVersion ||
		got.ReplayState != "needs_attention" ||
		got.PresetID != "intent.balanced" ||
		got.PresetVersion != config.PresetCatalogVersion ||
		!got.Customized || got.VerificationMode != "fast" ||
		!got.RepairEnabled || got.RepairHorizon != "10m" ||
		got.RepairMaxCommits != 3 || got.OpenCandidates != 1 ||
		got.BlockedCandidates != 1 || got.VerificationAttention != 1 ||
		got.RecoverableRepairs != 1 || got.LastBoundaryEpoch != 1 ||
		got.LatestPlannerProtocol != "v2" ||
		got.LatestAtomicityStatus != "invalid" ||
		got.LatestVerificationStatus != "needs_attention" ||
		got.LatestRepairStatus != state.IntentRepairPrepared {
		t.Fatalf("Intent v2 projection=%+v", got)
	}
	var human bytes.Buffer
	if err := runStatus(ctx, &human, repo, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Intent v2: needs_attention", "intent.balanced@3 customized",
		"verification=fast", "recoverable_repairs=1",
		"Latest candidate: status=blocked protocol=v2",
	} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("human Intent v2 output missing %q:\n%s",
				want, human.String())
		}
	}
	combined := jsonOut.String() + human.String()
	for _, forbidden := range []string{"sk-hidden", "prompt=private"} {
		if strings.Contains(combined, forbidden) {
			t.Fatalf("Intent v2 observability leaked %q:\n%s",
				forbidden, combined)
		}
	}
	if err := state.MetaSetMany(ctx, d, map[string]string{
		"intent.v2.cutover_required": "true",
		"intent.v2.migration_state":  "needs_attention",
		"intent.v2.needs_attention":  "",
	}); err != nil {
		t.Fatal(err)
	}
	jsonOut.Reset()
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(jsonOut.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.IntentV2.ReplayState != "needs_attention" ||
		!strings.Contains(report.IntentV2.NeedsAttention, "cutover") {
		t.Fatalf("required cutover reported healthy: %+v", report.IntentV2)
	}
}

func TestStatusRuntimeConfigPreV14MissingTablesReadOnly(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if _, err := d.SQL().Exec(`
DROP TABLE config_validation_runs;
DROP TABLE config_experiments;
DROP TABLE config_activation_requests;
DROP TABLE runtime_config_state;
DROP TRIGGER config_revisions_no_update;
DROP TRIGGER config_revisions_no_delete;
DROP TABLE config_revisions;
PRAGMA user_version=13;
PRAGMA wal_checkpoint(TRUNCATE);`); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := fileSHA256(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runStatus(ctx, &out, repo, true); err != nil {
		t.Fatal(err)
	}
	var report statusReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.RuntimeConfig.ApplyState != "unset" || report.RuntimeConfig.DesiredRevisionID != 0 {
		t.Fatalf("old schema runtime projection = %+v", report.RuntimeConfig)
	}
	if report.IntentV2.Available || report.IntentV2.SchemaVersion != 13 {
		t.Fatalf("old schema Intent v2 projection = %+v", report.IntentV2)
	}
	after, _ := fileSHA256(dbPath)
	if before != after {
		t.Fatalf("status mutated pre-v14 DB: %s -> %s", before, after)
	}
	var diagnoseOut bytes.Buffer
	if err := runDiagnose(ctx, &diagnoseOut, repo, true); err != nil {
		t.Fatal(err)
	}
	var diagnose diagnoseReport
	if err := json.Unmarshal(diagnoseOut.Bytes(), &diagnose); err != nil {
		t.Fatal(err)
	}
	if diagnose.RuntimeConfig.ApplyState != "unset" || diagnose.RuntimeConfig.DesiredRevisionID != 0 {
		t.Fatalf("old schema diagnose projection = %+v", diagnose.RuntimeConfig)
	}
	if diagnose.IntentV2.Available || diagnose.IntentV2.SchemaVersion != 13 {
		t.Fatalf("old schema diagnose Intent v2 projection = %+v",
			diagnose.IntentV2)
	}
	afterDiagnose, _ := fileSHA256(dbPath)
	if before != afterDiagnose {
		t.Fatalf("diagnose mutated pre-v14 DB: %s -> %s", before, afterDiagnose)
	}
	conn, err := openStateDBReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var version, tables int
	_ = conn.QueryRow(`PRAGMA user_version`).Scan(&version)
	_ = conn.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE 'config_%'`).Scan(&tables)
	if version != 13 || tables != 0 {
		t.Fatalf("old schema changed: version=%d config_tables=%d", version, tables)
	}
}

func cliRuntimeRevision(t *testing.T, d *state.DB, model string, generation int64) state.ConfigRevision {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"ai.model": model, "confirmations": []string{}})
	revision, err := state.InsertConfigRevision(context.Background(), d, state.ConfigRevisionInput{
		Snapshot: body, Profile: "profile-a", Scope: "repository", SourceGeneration: generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func TestStatus_StaleHeartbeatOverlay(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db, "claude-code")

	stale := float64(time.Now().Add(-2 * time.Hour).Unix())
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 1, Mode: "running", HeartbeatTS: stale,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	var out bytes.Buffer
	if err := runStatus(ctx, &out, repo, false); err != nil {
		t.Fatalf("runStatus: %v", err)
	}
	if !strings.Contains(out.String(), "stale") {
		t.Fatalf("expected stale daemon line, got:\n%s", out.String())
	}
}

func TestStatus_UnregisteredRepoErrors(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()

	stranger := initCLIResolverRepo(t)
	var out bytes.Buffer
	err := runStatus(ctx, &out, stranger, false)
	if err == nil {
		t.Fatal("expected error for unregistered repo")
	}
	if !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("error should mention 'not registered': %v", err)
	}
}

// TestStatus_BlockedConflictCount verifies `acd status` reports a non-zero
// blocked_conflicts count when the replay loop has terminally settled an
// event in state.EventStateBlockedConflict, and renders a "Blocked
// conflicts:" line in human output. Keeps the CLI surface honest about
// stuck rows that will not retry on their own.
func TestStatus_BlockedConflictCount(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db, "claude-code")

	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 99, Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}

	// Append a blocker event and settle it directly via MarkEventBlocked.
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "ghost.txt",
		Fidelity: "rescan",
	}, []state.CaptureOp{{
		Op: "modify", Path: "ghost.txt", Fidelity: "rescan",
	}})
	if err != nil {
		t.Fatalf("append event: %v", err)
	}
	if err := state.MarkEventBlocked(ctx, d, seq, "before-state mismatch", nowFloat(),
		sql.NullString{String: "refs/heads/main", Valid: true},
		sql.NullInt64{Int64: 1, Valid: true},
		sql.NullString{String: "deadbeef", Valid: true},
	); err != nil {
		t.Fatalf("MarkEventBlocked: %v", err)
	}

	// Human output mentions the blocker.
	var humanOut bytes.Buffer
	if err := runStatus(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	if !strings.Contains(humanOut.String(), "Blocked conflicts: 1") {
		t.Fatalf("missing 'Blocked conflicts: 1' in:\n%s", humanOut.String())
	}

	// JSON shape exposes the field as an integer count.
	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if rep.BlockedConflicts != 1 {
		t.Fatalf("BlockedConflicts = %d, want 1", rep.BlockedConflicts)
	}
	// Pending must be 0 — blocked rows leave the FIFO.
	if rep.PendingEvents != 0 {
		t.Fatalf("PendingEvents = %d, want 0 (blocker is terminal)", rep.PendingEvents)
	}
}

func TestStatus_BlockedBarrierGuidance(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
		BranchRef:        sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "barrier.go",
		Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "barrier.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append blocked event: %v", err)
	}
	if err := state.MarkEventBlocked(ctx, d, seq, "before-state mismatch", nowFloat(),
		sql.NullString{String: "refs/heads/main", Valid: true},
		sql.NullInt64{Int64: 1, Valid: true},
		sql.NullString{String: "deadbeef", Valid: true},
	); err != nil {
		t.Fatalf("mark blocked: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "later.go",
		Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "later.go", Fidelity: "exact"}}); err != nil {
		t.Fatalf("append pending successor: %v", err)
	}

	var humanOut bytes.Buffer
	if err := runStatus(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	human := humanOut.String()
	for _, want := range []string{"Blocked conflicts: 1", "acd fix --dry-run", "Blocked barriers with pending replay: 1", "acd fix --force --dry-run"} {
		if !strings.Contains(human, want) {
			t.Fatalf("status human missing %q in:\n%s", want, human)
		}
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if rep.ActiveBarriers != 1 || rep.BlockedConflicts != 1 || rep.PendingEvents != 1 {
		t.Fatalf("status counts = blocked %d active %d pending %d, want 1/1/1", rep.BlockedConflicts, rep.ActiveBarriers, rep.PendingEvents)
	}
}

func TestStatus_FailedBarrierGuidance(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "bad.go",
		Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "bad.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append failed event: %v", err)
	}
	if err := state.MarkEventPublished(ctx, d, seq, state.EventStateFailed,
		sql.NullString{}, sql.NullString{String: "commit-tree failed", Valid: true},
		sql.NullString{}, nowFloat()); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "later.go",
		Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "later.go", Fidelity: "exact"}}); err != nil {
		t.Fatalf("append pending successor: %v", err)
	}

	var humanOut bytes.Buffer
	if err := runStatus(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	human := humanOut.String()
	for _, want := range []string{"Failed terminal events: 1", "Failed barriers blocking pending replay: 1", "acd fix --dry-run"} {
		if !strings.Contains(human, want) {
			t.Fatalf("status human missing %q in:\n%s", want, human)
		}
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal status: %v\n%s", err, jsonOut.String())
	}
	if rep.FailedEvents != 1 || rep.FailedBlockingPending != 1 {
		t.Fatalf("failed fields = events %d blocking %d, want 1/1", rep.FailedEvents, rep.FailedBlockingPending)
	}
}

func TestStatus_BodyRendersPauseSection(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	expiresAt := time.Now().UTC().Add(time.Minute).Format(time.RFC3339)
	writePauseMarkerForStateDB(t, dbPath, pausepkg.Marker{
		Reason:    "deploy window",
		SetAt:     time.Now().UTC().Format(time.RFC3339),
		SetBy:     "test",
		ExpiresAt: &expiresAt,
	})

	var humanOut bytes.Buffer
	if err := runStatus(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	human := humanOut.String()
	for _, want := range []string{"Pause:", "Source: manual", "Reason: deploy window", "Expires at:"} {
		if !strings.Contains(human, want) {
			t.Fatalf("status output missing %q in:\n%s", want, human)
		}
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if !rep.Paused || rep.Pause == nil || rep.Pause.Source != "manual" || rep.Pause.Reason != "deploy window" {
		t.Fatalf("unexpected pause JSON: %+v", rep.Pause)
	}
}

func TestStatus_DecisionSummary(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	firstID, err := state.AppendDecision(ctx, d, state.DecisionRecord{
		DecisionTS:  10,
		Kind:        state.DecisionKindProtected,
		Path:        sqlNullStr("secrets.env"),
		Reason:      sqlNullStr("sensitive"),
		ActionTaken: sqlNullStr("no_delete_generated"),
	})
	if err != nil {
		t.Fatalf("AppendDecision protected: %v", err)
	}
	secondID, err := state.AppendDecision(ctx, d, state.DecisionRecord{
		DecisionTS:  11,
		Kind:        state.DecisionKindHandledExternal,
		Path:        sqlNullStr("src/app.go"),
		Reason:      sqlNullStr("already_published_by_external_committer"),
		ActionTaken: sqlNullStr("marked_published"),
	})
	if err != nil {
		t.Fatalf("AppendDecision handled: %v", err)
	}

	var humanOut bytes.Buffer
	if err := runStatus(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	human := humanOut.String()
	for _, want := range []string{
		"Decisions: protected=1 handled_external=1",
		"Recent decisions:",
		"#" + strconv.FormatInt(secondID, 10) + " handled_external src/app.go (marked_published)",
		"#" + strconv.FormatInt(firstID, 10) + " protected secrets.env (no_delete_generated)",
		"acd explain --path FILE",
		"acd events --watch",
	} {
		if !strings.Contains(human, want) {
			t.Fatalf("status output missing %q in:\n%s", want, human)
		}
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if rep.DecisionCursor != secondID {
		t.Fatalf("DecisionCursor = %d, want %d", rep.DecisionCursor, secondID)
	}
	if rep.DecisionCounts[state.DecisionKindProtected] != 1 || rep.DecisionCounts[state.DecisionKindHandledExternal] != 1 {
		t.Fatalf("DecisionCounts = %#v, want protected=1 handled_external=1", rep.DecisionCounts)
	}
	if len(rep.RecentDecisions) != 2 || rep.RecentDecisions[0].ID != secondID || rep.RecentDecisions[1].ID != firstID {
		t.Fatalf("RecentDecisions = %#v, want newest first", rep.RecentDecisions)
	}
}

func TestStatus_IntentStrategyUsesDaemonMetadata(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	for k, v := range map[string]string{
		"commit.strategy":       "intent",
		"commit.format":         "conventional",
		"intent.window":         "7",
		"intent.settle_window":  "15s",
		"intent.recent_commits": "3",
		"intent.defer_limit":    "1",
	} {
		if err := state.MetaSet(ctx, d, k, v); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}

	t.Setenv("ACD_COMMIT_STRATEGY", "event")
	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if !rep.IntentStrategy.Active || rep.IntentStrategy.Strategy != "intent" ||
		rep.IntentStrategy.CommitFormat != "conventional" ||
		rep.IntentStrategy.Window != 7 || rep.IntentStrategy.RecentCommits != 3 ||
		rep.IntentStrategy.SettleWindowSeconds != 15 ||
		rep.IntentStrategy.DeferLimit != 1 {
		t.Fatalf("intent strategy = %+v, want daemon metadata", rep.IntentStrategy)
	}
}

func TestStatus_IntentStrategyReportsPlannerHealth(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.MetaSet(ctx, d, "commit.strategy", "intent"); err != nil {
		t.Fatalf("set commit strategy: %v", err)
	}
	nextProbe := time.Date(2026, 7, 13, 3, 21, 0, 0, time.UTC)
	health := daemon.IntentPlannerHealthSnapshot{
		State:               daemon.IntentPlannerCircuitOpen,
		ProviderFingerprint: testPlannerHealthFingerprint(),
		ConsecutiveFailures: 3,
		BackoffLevel:        1,
		NextProbeTS:         float64(nextProbe.Unix()),
		LastFailureClass:    daemon.IntentPlannerFailureValidation,
		LastError:           "planner returned an invalid group",
		BypassCount:         7,
	}
	if err := state.MetaSetJSON(ctx, d, daemon.MetaKeyIntentPlannerHealth, struct {
		Version int `json:"version"`
		daemon.IntentPlannerHealthSnapshot
	}{Version: 1, IntentPlannerHealthSnapshot: health}); err != nil {
		t.Fatalf("set planner health: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if rep.IntentStrategy.PlannerHealth == nil ||
		rep.IntentStrategy.PlannerHealth.State != daemon.IntentPlannerCircuitOpen ||
		rep.IntentStrategy.PlannerHealth.ConsecutiveFailures != 3 ||
		rep.IntentStrategy.PlannerHealth.BypassCount != 7 ||
		rep.IntentStrategy.PlannerHealth.LastFailureClass != daemon.IntentPlannerFailureValidation {
		t.Fatalf("planner health=%+v", rep.IntentStrategy.PlannerHealth)
	}
	if !bytes.Contains(jsonOut.Bytes(), []byte(`"planner_health"`)) ||
		!bytes.Contains(jsonOut.Bytes(), []byte(`"next_probe_ts"`)) {
		t.Fatalf("status JSON missing planner health fields: %s", jsonOut.String())
	}

	var human bytes.Buffer
	if err := runStatus(ctx, &human, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	for _, want := range []string{
		"Intent planner health: open failures=3 bypasses=7",
		"next_probe=2026-07-13T03:21:00Z",
		"last_failure_class=validation",
		"Last circuit failure: planner returned an invalid group",
	} {
		if !strings.Contains(human.String(), want) {
			t.Fatalf("status human missing %q:\n%s", want, human.String())
		}
	}
}

func TestStatus_IntentPlannerHealthWarningIsReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name    string
		raw     string
		warning string
	}{
		{name: "empty", raw: "", warning: plannerHealthInvalidWarning},
		{name: "invalid", raw: `{"version":1,"last_error":"sk-secret`, warning: plannerHealthInvalidWarning},
		{name: "unsupported", raw: `{"version":99,"state":"open","last_error":"sk-secret"}`, warning: plannerHealthVersionWarning},
	} {
		t.Run(tc.name, func(t *testing.T) {
			roots := withIsolatedHome(t)
			ctx := context.Background()
			repo, dbPath, d := makeRepoStateDB(t)
			registerRepo(t, roots, repo, dbPath, "codex")
			if err := state.MetaSet(ctx, d, daemon.MetaKeyIntentPlannerHealth, tc.raw); err != nil {
				t.Fatalf("set planner health: %v", err)
			}
			if err := d.Close(); err != nil {
				t.Fatalf("close db: %v", err)
			}
			before, err := fileSHA256(dbPath)
			if err != nil {
				t.Fatalf("checksum before: %v", err)
			}

			var jsonOut bytes.Buffer
			if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
				t.Fatalf("runStatus json: %v", err)
			}
			var rep statusReport
			if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
				t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
			}
			if rep.IntentStrategy.PlannerHealth != nil || rep.IntentStrategy.PlannerHealthWarning != tc.warning {
				t.Fatalf("intent strategy=%+v", rep.IntentStrategy)
			}
			if strings.Contains(jsonOut.String(), "sk-secret") {
				t.Fatalf("status leaked malformed metadata: %s", jsonOut.String())
			}

			var human bytes.Buffer
			if err := runStatus(ctx, &human, repo, false); err != nil {
				t.Fatalf("runStatus human: %v", err)
			}
			if !strings.Contains(human.String(), "Intent planner health warning: "+tc.warning) {
				t.Fatalf("status human missing safe warning:\n%s", human.String())
			}
			after, err := fileSHA256(dbPath)
			if err != nil {
				t.Fatalf("checksum after: %v", err)
			}
			if before != after {
				t.Fatalf("status mutated state.db: before=%q after=%q", before, after)
			}
		})
	}
}

func testPlannerHealthFingerprint() string {
	return "sha256:" + strings.Repeat("0", 64)
}

func TestStatus_IntentStrategyReportsBatchWaitState(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	for k, v := range map[string]string{
		"commit.strategy":        "intent",
		"intent.window":          "7",
		"intent.min_pending":     "3",
		"intent.settle_window":   "0s",
		"intent.max_pending_age": "2m",
		"intent.recent_commits":  "3",
		"intent.defer_limit":     "1",
	} {
		if err := state.MetaSet(ctx, d, k, v); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}
	seq := appendIntentPendingEvent(t, ctx, d, "wait-a.go", nowFloat()-30)
	appendIntentPendingEvent(t, ctx, d, "wait-b.go", nowFloat()-20)

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if !rep.IntentStrategy.BatchWaitActive ||
		rep.IntentStrategy.BatchWaitReason != "skipped_due_intent_batch_wait" ||
		rep.IntentStrategy.VisiblePendingEvents != 2 ||
		rep.IntentStrategy.MinPending != 3 ||
		rep.IntentStrategy.SettleWindowSeconds != 0 ||
		rep.IntentStrategy.MaxPendingAgeSeconds != 120 ||
		rep.IntentStrategy.OldestPendingEventSeq != seq ||
		rep.IntentStrategy.OldestPendingPath != "wait-a.go" {
		t.Fatalf("intent strategy = %+v, want active batch wait", rep.IntentStrategy)
	}
	if rep.IntentStrategy.OldestPendingAgeSeconds <= 0 || rep.IntentStrategy.AgeTriggerInSeconds <= 0 {
		t.Fatalf("intent ages = %+v, want positive oldest age and trigger countdown", rep.IntentStrategy)
	}

	var humanOut bytes.Buffer
	if err := runStatus(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	if !strings.Contains(humanOut.String(), "Intent batch wait: pending=2 min_pending=3") {
		t.Fatalf("status human missing batch wait line:\n%s", humanOut.String())
	}
}

func TestStatus_IntentStrategyReportsSettleWaitState(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	for k, v := range map[string]string{
		"commit.strategy":        "intent",
		"intent.window":          "2",
		"intent.min_pending":     "2",
		"intent.settle_window":   "1m",
		"intent.max_pending_age": "2m",
		"intent.recent_commits":  "3",
		"intent.defer_limit":     "1",
	} {
		if err := state.MetaSet(ctx, d, k, v); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}
	appendIntentPendingEvent(t, ctx, d, "settle-a.go", nowFloat()-10)
	newest := appendIntentPendingEvent(t, ctx, d, "settle-b.go", nowFloat()-5)

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if !rep.IntentStrategy.BatchWaitActive ||
		rep.IntentStrategy.BatchWaitReason != "skipped_due_intent_settle_window" ||
		rep.IntentStrategy.VisiblePendingEvents != 2 ||
		rep.IntentStrategy.MinPending != 2 ||
		rep.IntentStrategy.Window != 2 ||
		rep.IntentStrategy.SettleWindowSeconds != 60 ||
		rep.IntentStrategy.NewestPendingEventSeq != newest {
		t.Fatalf("intent strategy = %+v, want active settle wait", rep.IntentStrategy)
	}
	if rep.IntentStrategy.NewestPendingAgeSeconds <= 0 || rep.IntentStrategy.SettleTriggerInSeconds <= 0 {
		t.Fatalf("intent settle ages = %+v, want positive newest age and trigger countdown", rep.IntentStrategy)
	}

	var humanOut bytes.Buffer
	if err := runStatus(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runStatus human: %v", err)
	}
	if !strings.Contains(humanOut.String(), "Intent settle wait: pending=2") {
		t.Fatalf("status human missing settle wait line:\n%s", humanOut.String())
	}
}

func TestStatus_IntentStrategyUsesDurablePlannerErrorLedger(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
		BranchRef:        sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	if err := state.MetaSet(ctx, d, "commit.strategy", "intent"); err != nil {
		t.Fatalf("set commit.strategy: %v", err)
	}
	eventSeq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "src/app.go",
		Fidelity: "exact", CapturedTS: 19,
	}, nil)
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	if _, err := state.AppendDecision(ctx, d, state.DecisionRecord{
		DecisionTS:  20,
		Kind:        state.DecisionKindIntentPlannerError,
		Path:        sqlNullStr("src/app.go"),
		Reason:      sqlNullStr(`planner returned unsafe seq {"token":"legacy-secret"}`),
		EventSeq:    sql.NullInt64{Int64: eventSeq, Valid: true},
		ActionTaken: sqlNullStr("planner validation failed"),
		UserMessage: sqlNullStr("fallback used"),
	}); err != nil {
		t.Fatalf("AppendDecision planner error: %v", err)
	}
	if _, err := d.SQL().ExecContext(ctx, `DROP TABLE planner_state`); err != nil {
		t.Fatalf("drop planner_state: %v", err)
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if rep.IntentStrategy.LastPlannerErrorEventSeq != eventSeq ||
		rep.IntentStrategy.LastPlannerErrorPath != "src/app.go" ||
		strings.Contains(rep.IntentStrategy.LastPlannerError, "legacy-secret") ||
		!strings.Contains(rep.IntentStrategy.LastPlannerError, "[REDACTED]") {
		t.Fatalf("last planner error = %+v, want durable decision_records error", rep.IntentStrategy)
	}
}

func TestStatus_IntentStrategyPlannerSummaryIsBarrierAware(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	for k, v := range map[string]string{
		"commit.strategy":    "intent",
		"intent.defer_limit": "2",
	} {
		if err := state.MetaSet(ctx, d, k, v); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}

	barrierSeq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "blocked.go", Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "blocked.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append barrier event: %v", err)
	}
	if err := state.MarkEventPublished(ctx, d, barrierSeq, state.EventStateFailed,
		sql.NullString{}, sqlNullStr("commit failed"), sql.NullString{}, nowFloat()); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	hiddenSeq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "hidden.go", Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "hidden.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append hidden event: %v", err)
	}
	visibleSeq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 2,
		BaseHead: "feedface", Operation: "modify", Path: "visible.go", Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "visible.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append visible event: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := state.RecordPlannerDefer(ctx, d, hiddenSeq, 100+float64(i), "hidden behind barrier"); err != nil {
			t.Fatalf("defer hidden %d: %v", i, err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := state.RecordPlannerDefer(ctx, d, visibleSeq, 50+float64(i), "visible defer"); err != nil {
			t.Fatalf("defer visible %d: %v", i, err)
		}
	}

	var jsonOut bytes.Buffer
	if err := runStatus(ctx, &jsonOut, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(jsonOut.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, jsonOut.String())
	}
	if rep.IntentStrategy.DeferredEvents != 1 ||
		rep.IntentStrategy.MaxDeferCount != 2 ||
		rep.IntentStrategy.ForcedAgingReady != 1 ||
		rep.IntentStrategy.LastDeferredEventSeq != visibleSeq ||
		rep.IntentStrategy.LastDeferredPath != "visible.go" {
		t.Fatalf("intent strategy = %+v, want only visible pending planner row", rep.IntentStrategy)
	}
}

func TestStatus_SkipsDecisionSummaryForPreV5DB(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	if _, err := d.SQL().ExecContext(ctx, `DROP TABLE decision_records`); err != nil {
		t.Fatalf("drop decision_records: %v", err)
	}

	var out bytes.Buffer
	if err := runStatus(ctx, &out, repo, false); err != nil {
		t.Fatalf("runStatus should tolerate missing decision_records: %v\n%s", err, out.String())
	}
	if strings.Contains(out.String(), "Decisions:") {
		t.Fatalf("pre-v5 status rendered decisions unexpectedly:\n%s", out.String())
	}
}

func TestStatusWatchRejectsNonPositiveInterval(t *testing.T) {
	var out bytes.Buffer
	if err := runStatusWatch(context.Background(), &out, ".", 0); err == nil {
		t.Fatal("runStatusWatch with zero interval succeeded")
	}
}

// TestList_Status_Doctor_AgreeOnCounts asserts that when the same repo is
// inspected by acd list, acd status, and acd doctor they all report the
// same pending + blocked_conflict counts. This is the contract the cli-lane
// task is meant to enforce: list must not say "PENDING 0" while status sees
// pending events, and doctor must agree with both.
func TestList_Status_Doctor_AgreeOnCounts(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "claude-code")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}

	// 3 pending + 2 blocked.
	for _, p := range []string{"a.go", "b.go", "c.go"} {
		if _, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: "deadbeef", Operation: "modify", Path: p, Fidelity: "exact",
		}, []state.CaptureOp{{Op: "modify", Path: p, Fidelity: "exact"}}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	for _, p := range []string{"x.go", "y.go"} {
		seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: "deadbeef", Operation: "modify", Path: p, Fidelity: "rescan",
		}, []state.CaptureOp{{Op: "modify", Path: p, Fidelity: "rescan"}})
		if err != nil {
			t.Fatalf("append blocker: %v", err)
		}
		if err := state.MarkEventBlocked(ctx, d, seq, "before-state mismatch", nowFloat(),
			sql.NullString{String: "refs/heads/main", Valid: true},
			sql.NullInt64{Int64: 1, Valid: true},
			sql.NullString{String: "deadbeef", Valid: true},
		); err != nil {
			t.Fatalf("block: %v", err)
		}
	}

	// list (json)
	var lOut, lErr bytes.Buffer
	if err := runList(ctx, &lOut, &lErr, true, false); err != nil {
		t.Fatalf("runList: %v", err)
	}
	var listGot struct {
		Repos []listEntry `json:"repos"`
	}
	if err := json.Unmarshal(lOut.Bytes(), &listGot); err != nil {
		t.Fatalf("list unmarshal: %v\n%s", err, lOut.String())
	}
	if len(listGot.Repos) != 1 {
		t.Fatalf("list: want 1 repo, got %d", len(listGot.Repos))
	}

	// status (json)
	var sOut bytes.Buffer
	if err := runStatus(ctx, &sOut, repo, true); err != nil {
		t.Fatalf("runStatus: %v", err)
	}
	var statusGot statusReport
	if err := json.Unmarshal(sOut.Bytes(), &statusGot); err != nil {
		t.Fatalf("status unmarshal: %v\n%s", err, sOut.String())
	}

	// doctor (json)
	var docOut bytes.Buffer
	if err := runDoctor(ctx, &docOut, false, "", true); err != nil {
		t.Fatalf("runDoctor: %v", err)
	}
	var docGot doctorReport
	if err := json.Unmarshal(docOut.Bytes(), &docGot); err != nil {
		t.Fatalf("doctor unmarshal: %v\n%s", err, docOut.String())
	}
	if len(docGot.Repos) != 1 {
		t.Fatalf("doctor: want 1 repo, got %d", len(docGot.Repos))
	}

	// Pending + blocked must all match (3, 2).
	if listGot.Repos[0].PendingEvents != 3 {
		t.Errorf("list pending=%d want 3", listGot.Repos[0].PendingEvents)
	}
	if statusGot.PendingEvents != 3 {
		t.Errorf("status pending=%d want 3", statusGot.PendingEvents)
	}
	if docGot.Repos[0].PendingEvents != 3 {
		t.Errorf("doctor pending=%d want 3", docGot.Repos[0].PendingEvents)
	}
	if listGot.Repos[0].BlockedConflicts != 2 {
		t.Errorf("list blocked=%d want 2", listGot.Repos[0].BlockedConflicts)
	}
	if statusGot.BlockedConflicts != 2 {
		t.Errorf("status blocked=%d want 2", statusGot.BlockedConflicts)
	}
	if docGot.Repos[0].BlockedConflicts != 2 {
		t.Errorf("doctor blocked=%d want 2", docGot.Repos[0].BlockedConflicts)
	}
}

func TestStatus_JSONShape(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db, "claude-code")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	var out bytes.Buffer
	if err := runStatus(ctx, &out, repo, true); err != nil {
		t.Fatalf("runStatus json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if rep.Repo != repo {
		t.Fatalf("repo = %q, want %q", rep.Repo, repo)
	}
	if rep.PID != 7 {
		t.Fatalf("pid = %d, want 7", rep.PID)
	}
	if rep.Daemon != "running" {
		t.Fatalf("daemon = %q, want running", rep.Daemon)
	}
}

func appendIntentPendingEvent(t *testing.T, ctx context.Context, d *state.DB, path string, capturedTS float64) int64 {
	t.Helper()
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: path,
		Fidelity: "exact", CapturedTS: capturedTS,
	}, []state.CaptureOp{{Op: "modify", Path: path, Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	return seq
}
