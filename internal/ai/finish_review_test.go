package ai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

func finishTestReview(request FinishPlanRequest) []byte {
	observations := []map[string]string{}
	for _, part := range request.Review.Parts {
		observations = append(observations, map[string]string{"id": part.ID, "summary": "Preserve explicit authored settings and their consumers."})
	}
	body, _ := json.Marshal(map[string]any{"observations": observations})
	return body
}

func TestFinishLargeEvidenceReviewedBeforeGlobalPlan(t *testing.T) {
	evidence := finishTestEvidence()
	options := finishTestOptions(evidence)
	text := "+ password=abcdefgh12345678\n" + strings.Repeat("+ preserve authored 界 values <>&\\\"\n", 16000) + "+ final evidence\n"
	options.Diffs[0].Text = text
	reviewed := ""
	reservations := 0
	options.BeforeEvidenceCall = func(context.Context) error { reservations++; return nil }
	provider := &finishFakeTransport{exchange: func(_ context.Context, r FinishPlanRequest) ([]byte, error) {
		if r.Review != nil {
			for _, part := range r.Review.Parts {
				if strings.Contains(part.Text, "abcdefgh12345678") {
					t.Fatal("unredacted review")
				}
				reviewed += part.Text
			}
			return finishTestReview(r), nil
		}
		if reviewed != finishRedactText(text) {
			t.Fatal("review lost, duplicated, or reordered bytes")
		}
		if len(r.Changes) != 1 || len(r.Reviews) < 2 || len(r.Diffs) != 1 || r.Diffs[0].Text != "" {
			t.Fatal("global plan lost original identity or sent the whole diff again")
		}
		return finishTestOutput(finishTestGoal(evidence)), nil
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || result.Calls != 1 || result.Disclosure.EvidenceCalls != reservations || reservations < 2 || !result.Disclosure.Redacted {
		t.Fatalf("large plan %+v %v", result, err)
	}
	for _, r := range provider.requests {
		body, _ := provider.EncodeFinishRequest(r)
		if len(body) > FinishContextByteCap {
			t.Fatal("oversized exchange")
		}
	}
	if len(result.Disclosure.EvidenceReviews) != len(provider.requests[len(provider.requests)-1].Reviews) {
		t.Fatal("saved review provenance missing")
	}
}

func TestFinishEvidenceReviewFailsWithoutPlanning(t *testing.T) {
	for _, scenario := range []string{"rejected", "missing fragment", "duplicate fragment", "verification claim", "deadline", "budget"} {
		t.Run(scenario, func(t *testing.T) {
			evidence := finishTestEvidence()
			options := finishTestOptions(evidence)
			options.Diffs[0].Text = strings.Repeat("+ authored settings\n", 27000)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if scenario == "budget" {
				options.BeforeEvidenceCall = func(context.Context) error { return errors.New("durable budget exhausted") }
			}
			provider := &finishFakeTransport{exchange: func(_ context.Context, r FinishPlanRequest) ([]byte, error) {
				if r.Review == nil {
					t.Fatal("failed review reached planning")
				}
				switch scenario {
				case "rejected":
					return nil, errors.New("provider refused review")
				case "missing fragment":
					return []byte(`{"observations":[]}`), nil
				case "duplicate fragment", "verification claim":
					var reply map[string][]map[string]string
					_ = json.Unmarshal(finishTestReview(r), &reply)
					if scenario == "duplicate fragment" {
						reply["observations"][1]["id"] = reply["observations"][0]["id"]
					} else {
						reply["observations"][0]["summary"] = "All tests passed"
					}
					raw, _ := json.Marshal(reply)
					return raw, nil
				case "deadline":
					cancel()
					return finishTestReview(r), nil
				}
				return nil, nil
			}}
			result, err := PlanFinish(ctx, provider, evidence, options)
			limit := 1
			if scenario == "missing fragment" || scenario == "duplicate fragment" || scenario == "verification claim" {
				limit = 2
			}
			if err == nil || result.Calls != 0 || len(result.Plan.Steps) != 0 || len(provider.requests) > limit {
				t.Fatalf("failed review exceeded its correction allowance or reached planning: %+v %v", result, err)
			}
			if scenario == "duplicate fragment" && !strings.Contains(err.Error(), "appears more than once") {
				t.Fatalf("duplicate membership was not diagnosed: %v", err)
			}
			if scenario == "verification claim" && !strings.Contains(err.Error(), "not claim tests or verification ran") {
				t.Fatalf("unsupported verification claim was not diagnosed: %v", err)
			}
		})
	}
}

func TestFinishDiffPartsPreserveLongUnicodeLines(t *testing.T) {
	text := strings.Repeat("界", 30000) + "\n" + strings.Repeat("x", 100000)
	parts := finishDiffParts(text, 16384)
	if len(parts) < 2 || strings.Join(parts, "") != text {
		t.Fatal("fragmentation clipped or reordered text")
	}
}

func TestFinishReviewLimitRejectedBeforeProviderCalls(t *testing.T) {
	evidence := finishTestEvidence()
	options := finishTestOptions(evidence)
	options.Diffs[0].Text = strings.Repeat("x", (33<<16)+1)
	provider := &finishFakeTransport{padding: 440 << 10, exchange: func(context.Context, FinishPlanRequest) ([]byte, error) {
		t.Fatal("exhausted review budget reached provider")
		return nil, nil
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err == nil || result.Calls != 0 || result.Disclosure.EvidenceCalls != 0 || len(provider.requests) != 0 {
		t.Fatalf("unbounded reviews %+v %v", result, err)
	}
}

func TestFinishLargeHistoryKeepsOriginalMembership(t *testing.T) {
	evidence := finishTestEvidence()
	options := finishTestOptions(evidence)
	options.Diffs[0].Text = strings.Repeat("+ authored settings\n", 30000)
	commits := []HistoryMessageCommit{{ID: "original", Author: "author-a", TreeOID: strings.Repeat("b", 40), ChangeIDs: []string{"settings.go"}}}
	provider := &finishFakeTransport{exchange: func(_ context.Context, r FinishPlanRequest) ([]byte, error) {
		if r.Review != nil {
			return finishTestReview(r), nil
		}
		if len(r.HistoryCommits) != 1 || len(r.Reviews) == 0 {
			t.Fatal("history grouping context was lost")
		}
		return finishTestOutput(finish.Goal{Purpose: "Preserve authored settings", Boundary: "Keep original commit membership", Message: "Preserve authored settings\n\n- Keep explicit authored values ahead of defaults.", ChangeIDs: []string{"original"}}), nil
	}}
	goals, result, err := PlanHistoryMessages(t.Context(), provider, evidence, commits, options)
	if err != nil || len(goals) != 1 || goals[0].ChangeIDs[0] != "original" || result.Disclosure.EvidenceCalls < 2 || result.Calls != 1 {
		t.Fatalf("history %+v %+v %v", goals, result, err)
	}
}

type finishBlockingReviewTransport struct {
	started, stopped chan struct{}
	failure          error
}

func (*finishBlockingReviewTransport) Name() string { return "blocking-review" }
func (*finishBlockingReviewTransport) EncodeFinishRequest(FinishPlanRequest) ([]byte, error) {
	return nil, nil
}
func (p *finishBlockingReviewTransport) ExchangeFinish(ctx context.Context, body []byte) ([]byte, error) {
	if string(body) == "slow" {
		close(p.started)
		<-ctx.Done()
		close(p.stopped)
		return nil, ctx.Err()
	}
	<-p.started
	return nil, p.failure
}
func TestFinishEvidenceBatchCancelsAndJoinsPeers(t *testing.T) {
	failure := errors.New("provider rejected evidence")
	provider := &finishBlockingReviewTransport{started: make(chan struct{}), stopped: make(chan struct{}), failure: failure}
	_, err := ExchangeFinishReviews(t.Context(), provider, [][]byte{[]byte("slow"), []byte("reject")})
	if !errors.Is(err, failure) {
		t.Fatalf("original rejection lost: %v", err)
	}
	select {
	case <-provider.stopped:
	default:
		t.Fatal("returned before joining cancelled exchange")
	}
}

func TestFinishReviewChunksDoNotDefineCommitBoundaries(t *testing.T) {
	evidence := finish.Evidence{Base: finish.Tree{}, Target: finish.Tree{"library.go": finishTestEntry("a"), "consumer.go": finishTestEntry("b"), "privacy.go": finishTestEntry("c")}}
	evidence.Changes = finish.NetChanges(evidence.Base, evidence.Target, "author-a")
	// Recorded prerequisites precede their consumers in valid evidence.
	evidence.Changes[0], evidence.Changes[1] = evidence.Changes[1], evidence.Changes[0]
	for i := range evidence.Changes {
		if evidence.Changes[i].ID == "consumer.go" {
			evidence.Changes[i].DependsOn = []string{"library.go"}
		}
	}
	options := finishTestOptions(evidence)
	for i := range options.Diffs {
		options.Diffs[i].Text = strings.Repeat("+ recorded behavior\n", 16000)
	}
	provider := &finishFakeTransport{exchange: func(_ context.Context, r FinishPlanRequest) ([]byte, error) {
		if r.Review != nil {
			return finishTestReview(r), nil
		}
		if len(r.Changes) != 3 || len(r.Reviews) < 9 {
			t.Fatal("global planner did not see all evidence together")
		}
		return finishTestOutput(
			finish.Goal{Purpose: "Provide reusable parsing", Boundary: "Keep the parser and its working consumer together", Message: "Provide reusable parsing\n\n- Share recorded parsing behavior with its consumer.", ChangeIDs: []string{"library.go", "consumer.go"}},
			finish.Goal{Purpose: "Preserve private defaults", Boundary: "Review the independent privacy policy separately", Message: "Preserve private defaults\n\n- Keep private defaults independent from parser configuration.", ChangeIDs: []string{"privacy.go"}},
		), nil
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || len(result.Plan.Steps) != 2 || result.Calls != 1 || result.Disclosure.EvidenceCalls < 2 {
		t.Fatalf("global boundaries %+v %v", result, err)
	}
}

func TestFinishReviewCorrectsOnlyInvalidBatch(t *testing.T) {
	evidence := finishTestEvidence()
	options := finishTestOptions(evidence)
	options.Diffs[0].Text = strings.Repeat("+ exact recorded value\n", 50000)
	batches := map[string]int{}
	initial := 0
	badID := ""
	corrections := 0
	reservations := 0
	options.BeforeEvidenceCall = func(context.Context) error { reservations++; return nil }
	provider := &finishFakeTransport{exchange: func(_ context.Context, r FinishPlanRequest) ([]byte, error) {
		if r.Review == nil {
			return finishTestOutput(finishTestGoal(evidence)), nil
		}
		key := r.Review.Parts[0].ID
		batches[key]++
		if r.Correction != nil {
			corrections++
			if key != badID || r.Correction.PreviousOutput != "" || !strings.Contains(r.Correction.ValidationError, "maximum is 256") {
				t.Fatal("correction lacked bounded, specific feedback")
			}
			return finishTestReview(r), nil
		}
		initial++
		if initial == 2 {
			badID = key
			var reply map[string][]map[string]string
			_ = json.Unmarshal(finishTestReview(r), &reply)
			reply["observations"][0]["summary"] = strings.Repeat("overlong factual summary ", 20)
			raw, _ := json.Marshal(reply)
			return raw, nil
		}
		return finishTestReview(r), nil
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || result.Calls != 1 || corrections != 1 || result.Disclosure.EvidenceCalls != reservations || initial < 3 {
		t.Fatalf("bounded correction %+v %v", result, err)
	}
	for id, n := range batches {
		want := 1
		if id == badID {
			want = 2
		}
		if n != want {
			t.Fatal("accepted review was repeated")
		}
	}
	for _, r := range provider.requests {
		body, _ := provider.EncodeFinishRequest(r)
		if len(body) > FinishContextByteCap {
			t.Fatal("correction exceeded context bound")
		}
	}
}

func TestFinishReviewCorrectionCannotBypassBudget(t *testing.T) {
	evidence := finishTestEvidence()
	options := finishTestOptions(evidence)
	options.Diffs[0].Text = strings.Repeat("+ exact recorded value\n", 30000)
	reservations := 0
	options.BeforeEvidenceCall = func(context.Context) error {
		reservations++
		if reservations == 2 {
			return errors.New("durable review budget exhausted")
		}
		return nil
	}
	provider := &finishFakeTransport{exchange: func(context.Context, FinishPlanRequest) ([]byte, error) { return []byte(`{"observations":[]}`), nil }}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err == nil || result.Calls != 0 || result.Disclosure.EvidenceCalls != 1 || len(provider.requests) != 1 {
		t.Fatalf("correction bypassed budget %+v %v", result, err)
	}
}

func TestFinishManySmallChangesBoundReviewOutput(t *testing.T) {
	evidence := finish.Evidence{Base: finish.Tree{}, Target: finish.Tree{}}
	for i := 0; i < 300; i++ {
		evidence.Target[fmt.Sprintf("settings-%03d.go", i)] = finishTestEntry("b")
	}
	evidence.Changes = finish.NetChanges(evidence.Base, evidence.Target, "author-a")
	options := finishTestOptions(evidence)
	for i := range options.Diffs {
		options.Diffs[i].Text = strings.Repeat("+ compatible shared default\n", 80)
	}
	provider := &finishFakeTransport{exchange: func(_ context.Context, r FinishPlanRequest) ([]byte, error) {
		if r.Review != nil {
			if len(r.Review.Parts) > 32 {
				t.Fatal("review asked for an unbounded observation list")
			}
			return finishTestReview(r), nil
		}
		goal := finishTestGoal(evidence)
		goal.Boundary = "Keep the compatible shared-default migration together"
		return finishTestOutput(goal), nil
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err != nil || result.Calls != 1 || len(result.Disclosure.EvidenceReviews) != 300 || result.Disclosure.EvidenceCalls < 10 {
		t.Fatalf("many small changes %+v %v", result, err)
	}
}

type finishBarrierReviewTransport struct {
	started chan struct{}
	release chan struct{}
}

func (*finishBarrierReviewTransport) Name() string { return "barrier-review" }
func (*finishBarrierReviewTransport) EncodeFinishRequest(FinishPlanRequest) ([]byte, error) {
	return nil, nil
}
func (p *finishBarrierReviewTransport) ExchangeFinish(ctx context.Context, body []byte) ([]byte, error) {
	p.started <- struct{}{}
	select {
	case <-p.release:
		return body, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func TestFinishEvidenceBatchBoundsParallelWorkAndKeepsOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	provider := &finishBarrierReviewTransport{started: make(chan struct{}, 4), release: make(chan struct{})}
	bodies := [][]byte{[]byte("first"), []byte("second"), []byte("third"), []byte("fourth")}
	type outcome struct {
		replies [][]byte
		err     error
	}
	done := make(chan outcome, 1)
	go func() { r, e := ExchangeFinishReviews(ctx, provider, bodies); done <- outcome{r, e} }()
	for range bodies {
		select {
		case <-provider.started:
		case <-ctx.Done():
			t.Fatal("reviews did not start together")
		}
	}
	close(provider.release)
	result := <-done
	if result.err != nil || len(result.replies) != 4 {
		t.Fatalf("bounded batch: %v", result.err)
	}
	for i, reply := range result.replies {
		if string(reply) != string(bodies[i]) {
			t.Fatal("parallel replies changed order")
		}
	}
	if _, err := ExchangeFinishReviews(t.Context(), nil, make([][]byte, 5)); err == nil {
		t.Fatal("unbounded parallel batch accepted")
	}
}

func TestFinishReviewRetryReusesOnlyMatchingValidatedEvidence(t *testing.T) {
	for _, scenario := range []string{"same", "identity", "diff", "transport", "digest", "summary"} {
		t.Run(scenario, func(t *testing.T) {
			evidence := finishTestEvidence()
			options := finishTestOptions(evidence)
			options.ReviewIdentity = "exact frozen target and provider revision"
			options.MaxInputBytes = 128 << 10
			options.Diffs[0].Text = strings.Repeat("+ preserve complete authored library values\n", 1700)
			var saved finish.ReviewProgress
			options.SaveReviewProgress = func(_ context.Context, progress finish.ReviewProgress) error {
				raw, _ := json.Marshal(progress)
				return json.Unmarshal(raw, &saved)
			}
			calls := 0
			first := &finishFakeTransport{padding: 80 << 10, exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
				calls++
				if calls == 2 {
					return nil, context.DeadlineExceeded
				}
				return finishTestReview(request), nil
			}}
			result, err := PlanFinish(t.Context(), first, evidence, options)
			if err == nil || result.Calls != 0 || len(saved.Observations) == 0 || len(saved.Observations) >= saved.TotalFragments {
				t.Fatalf("accepted progress lost before timeout: %+v %v", saved, err)
			}
			options.ReviewProgress = saved
			cached := map[string]bool{}
			for _, observation := range saved.Observations {
				cached[observation.ID] = true
			}
			switch scenario {
			case "identity":
				options.ReviewIdentity = "different provider approval or frozen target"
			case "diff":
				options.Diffs[0].Text += "+ another exact byte\n"
			case "digest":
				options.ReviewProgress.Observations[0].Digest = "unproven"
			case "summary":
				options.ReviewProgress.Observations[0].Summary = "All tests passed"
			}
			retry := &finishFakeTransport{padding: 80 << 10, exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
				if request.Review != nil {
					for _, part := range request.Review.Parts {
						if scenario == "same" && cached[part.ID] {
							t.Fatal("accepted evidence was sent again")
						}
					}
					return finishTestReview(request), nil
				}
				if len(request.Reviews) != saved.TotalFragments {
					t.Fatal("global plan lacks complete review coverage")
				}
				return finishTestOutput(finishTestGoal(evidence)), nil
			}}
			if scenario == "transport" {
				retry.padding++
			}
			options.Deadline = time.Now().Add(15 * time.Minute)
			completed, err := PlanFinish(t.Context(), retry, evidence, options)
			if err != nil || completed.Calls != 1 {
				t.Fatalf("retry: %+v %v", completed, err)
			}
			if (completed.Disclosure.ReusedFragments > 0) != (scenario == "same") {
				t.Fatalf("unproven cache reused for %s: %+v", scenario, completed.Disclosure)
			}
		})
	}
}

func TestFinishReviewPersistenceFailureStopsPlanning(t *testing.T) {
	evidence := finishTestEvidence()
	options := finishTestOptions(evidence)
	options.Diffs[0].Text = strings.Repeat("+ authored settings\n", 35000)
	options.SaveReviewProgress = func(context.Context, finish.ReviewProgress) error { return errors.New("disk unavailable") }
	provider := &finishFakeTransport{exchange: func(context.Context, FinishPlanRequest) ([]byte, error) {
		t.Fatal("persistence failure reached provider")
		return nil, nil
	}}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if err == nil || result.Calls != 0 || result.Disclosure.EvidenceCalls != 0 {
		t.Fatalf("lost durability: %+v %v", result, err)
	}
}

// Complete peers before rejecting one exchange, without depending on scheduling
// delays. Retry can then prove that accepted siblings are not transmitted again.
type finishPeerFailureTransport struct {
	t                           *testing.T
	rejectedID, invalidID       string
	rejection                   error
	correctionFailure           error
	errorBody, waitForDeadline  bool
	peers                       int32
	completed                   atomic.Int32
	reviews, corrections, plans atomic.Int32
	ready                       chan struct{}
	forbidden                   map[string]bool
	goal                        finish.Goal
}

func (*finishPeerFailureTransport) Name() string { return "peer-failure-test" }
func (*finishPeerFailureTransport) EncodeFinishRequest(request FinishPlanRequest) ([]byte, error) {
	body, err := json.Marshal(request)
	return append(body, []byte(strings.Repeat(" ", 80<<10))...), err
}
func (p *finishPeerFailureTransport) ExchangeFinish(ctx context.Context, body []byte) ([]byte, error) {
	var request FinishPlanRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, err
	}
	if request.Review == nil {
		p.plans.Add(1)
		return finishTestOutput(p.goal), nil
	}
	p.reviews.Add(1)
	if request.Correction != nil {
		p.corrections.Add(1)
		if p.correctionFailure != nil {
			return nil, p.correctionFailure
		}
	}
	for _, part := range request.Review.Parts {
		if p.forbidden[part.ID] {
			p.t.Errorf("retry transmitted accepted peer fragment %s", part.ID)
		}
	}
	if request.Review.Parts[0].ID == p.rejectedID {
		select {
		case <-p.ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if p.waitForDeadline {
			<-ctx.Done()
		}
		var body []byte
		if p.errorBody {
			body = finishTestReview(request)
		}
		return body, p.rejection
	}
	reply := finishTestReview(request)
	if request.Review.Parts[0].ID == p.invalidID {
		reply = []byte(`{"observations":[]}`)
	}
	if p.peers > 0 && p.completed.Add(1) == p.peers {
		close(p.ready)
	}
	return reply, nil
}
func (p *finishPeerFailureTransport) ExchangeFinishReviews(ctx context.Context, bodies [][]byte) ([][]byte, error) {
	return ExchangeFinishReviews(ctx, p, bodies)
}
func finishPeerFailureFixture(t *testing.T) (finish.Evidence, FinishOptions, []FinishPlanRequest) {
	t.Helper()
	evidence := finishTestEvidence()
	options := finishTestOptions(evidence)
	options.MaxInputBytes = 128 << 10
	options.Diffs[0].Text = strings.Repeat("+ preserve complete authored library values\n", 2500)
	request, _, err := prepareFinishRequest(evidence, options)
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := finishReviewChunks(&finishPeerFailureTransport{}, request, (128<<10)-(4<<10))
	if err != nil || len(chunks) != 4 {
		t.Fatalf("four-peer fixture: %d chunks, %v", len(chunks), err)
	}
	return evidence, options, chunks
}
func TestFinishReviewRetainsSuccessfulPeersAfterBatchFailure(t *testing.T) {
	for _, scenario := range []string{"rejected peer", "errored body", "invalid peer"} {
		t.Run(scenario, func(t *testing.T) {
			evidence, options, chunks := finishPeerFailureFixture(t)
			rejection := errors.New("one peer rejected evidence")
			provider := &finishPeerFailureTransport{t: t, rejectedID: chunks[3].Review.Parts[0].ID, rejection: rejection, errorBody: scenario == "errored body", peers: 3, ready: make(chan struct{}), goal: finishTestGoal(evidence)}
			if scenario == "invalid peer" {
				provider.invalidID = chunks[0].Review.Parts[0].ID
			}
			var saved finish.ReviewProgress
			options.SaveReviewProgress = func(_ context.Context, progress finish.ReviewProgress) error {
				raw, _ := json.Marshal(progress)
				return json.Unmarshal(raw, &saved)
			}
			result, err := PlanFinish(t.Context(), provider, evidence, options)
			if !errors.Is(err, rejection) || result.Calls != 0 || provider.plans.Load() != 0 || provider.reviews.Load() != 4 || provider.corrections.Load() != 0 || result.Disclosure.EvidenceCalls != 4 {
				t.Fatalf("batch failure changed or retried: %+v %v", result, err)
			}
			expected := 0
			for _, chunk := range chunks[:3] {
				if chunk.Review.Parts[0].ID != provider.invalidID {
					expected += len(chunk.Review.Parts)
				}
			}
			if provider.completed.Load() != 3 || len(saved.Observations) != expected {
				t.Fatalf("successful peers lost: completed=%d, saved=%d, want=%d", provider.completed.Load(), len(saved.Observations), expected)
			}
			accepted := saved
			forbidden := map[string]bool{}
			for _, observation := range accepted.Observations {
				forbidden[observation.ID] = true
			}
			for _, part := range chunks[3].Review.Parts {
				if forbidden[part.ID] {
					t.Fatal("an errored reply body was accepted")
				}
			}
			options.ReviewProgress = accepted
			retry := &finishPeerFailureTransport{t: t, forbidden: forbidden, goal: finishTestGoal(evidence)}
			completed, err := PlanFinish(t.Context(), retry, evidence, options)
			wantReviews := int32(1)
			if scenario == "invalid peer" {
				wantReviews = 2
			}
			if err != nil || completed.Calls != 1 || completed.Disclosure.ReusedFragments != expected || retry.reviews.Load() != wantReviews || retry.plans.Load() != 1 || retry.corrections.Load() != 0 || len(completed.Disclosure.EvidenceReviews) != saved.TotalFragments {
				t.Fatalf("retry did not resume missing reviews: %+v %v", completed, err)
			}
		})
	}
}
func TestFinishReviewDoesNotRetainPeersAfterDeadline(t *testing.T) {
	evidence, options, chunks := finishPeerFailureFixture(t)
	options.Deadline = time.Now().Add(5 * time.Second)
	rejection := errors.New("peer failed after the attempt deadline")
	provider := &finishPeerFailureTransport{t: t, rejectedID: chunks[3].Review.Parts[0].ID, rejection: rejection, waitForDeadline: true, peers: 3, ready: make(chan struct{}), goal: finishTestGoal(evidence)}
	saved := 0
	options.SaveReviewProgress = func(ctx context.Context, progress finish.ReviewProgress) error {
		if ctx.Err() != nil {
			t.Error("attempted a progress write after the deadline")
		}
		saved = len(progress.Observations)
		return nil
	}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if !errors.Is(err, rejection) || result.Calls != 0 || provider.completed.Load() != 3 || saved != 0 || len(result.Disclosure.EvidenceReviews) != 0 || provider.corrections.Load() != 0 {
		t.Fatalf("late peer response was retained: %+v %v saved=%d", result, err, saved)
	}
}
func TestFinishReviewRetainsPeersWhenCorrectionFails(t *testing.T) {
	evidence, options, chunks := finishPeerFailureFixture(t)
	failure := errors.New("review correction rejected")
	provider := &finishPeerFailureTransport{t: t, invalidID: chunks[0].Review.Parts[0].ID, correctionFailure: failure, goal: finishTestGoal(evidence)}
	saved := 0
	options.SaveReviewProgress = func(_ context.Context, progress finish.ReviewProgress) error {
		saved = len(progress.Observations)
		return nil
	}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	expected := 0
	for _, chunk := range chunks[1:] {
		expected += len(chunk.Review.Parts)
	}
	if !errors.Is(err, failure) || result.Calls != 0 || provider.reviews.Load() != 5 || provider.corrections.Load() != 1 || saved != expected {
		t.Fatalf("correction failure stranded accepted peers: %+v %v saved=%d want=%d", result, err, saved, expected)
	}
}

func TestFinishReviewPeerFailurePreservesProgressWriteError(t *testing.T) {
	evidence, options, chunks := finishPeerFailureFixture(t)
	rejection := errors.New("one peer rejected evidence")
	diskFailure := errors.New("progress storage unavailable")
	provider := &finishPeerFailureTransport{t: t, rejectedID: chunks[3].Review.Parts[0].ID, rejection: rejection, peers: 3, ready: make(chan struct{}), goal: finishTestGoal(evidence)}
	options.SaveReviewProgress = func(_ context.Context, progress finish.ReviewProgress) error {
		if len(progress.Observations) > 0 {
			return diskFailure
		}
		return nil
	}
	result, err := PlanFinish(t.Context(), provider, evidence, options)
	if !errors.Is(err, rejection) || !errors.Is(err, diskFailure) || result.Calls != 0 || provider.corrections.Load() != 0 {
		t.Fatalf("original failure or storage failure lost: %+v %v", result, err)
	}
}
