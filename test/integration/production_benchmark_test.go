//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Opt-in measurements use the same isolated runtime and cleanup as production
// scenarios. Run scripts/dev/benchmark.sh to retain the JSON measurements.
func TestProductionMeasurements(t *testing.T) {
	if os.Getenv("ACD_BENCHMARK") != "1" {
		t.Skip("opt-in production measurements")
	}
	requireSQLite(t)
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	called := make(chan struct{}, 1)
	server, trust := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeIntentChatRequest(t, r)
		if req.ToolChoice.Function.Name == "commit_message" {
			writeIntentMessageRewriteResponse(t, w, req)
			return
		}
		select {
		case called <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		candidates := []map[string]any{}
		for _, capture := range offeredIntentCaptures(t, req) {
			candidates = append(candidates, nativeReadyIntentCandidate(
				fmt.Sprintf("measurement-%d", capture.Seq), []int64{capture.Seq},
				"Add measured change", "Keep the measured changes independent.", "independent benchmark change"))
		}
		writeNativeIntentCandidatesResponse(t, w, "measurement", candidates)
	}))
	// Registered after the server so a failed assertion releases blocked calls
	// before server cleanup attempts to join them.
	t.Cleanup(unblock)
	extra := activateIntentV2Runtime(t, repo,
		"ACD_COMMIT_STRATEGY=intent", "ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_BASE_URL="+server.URL, "ACD_AI_MODEL=benchmark",
		"ACD_AI_API_KEY=synthetic-benchmark-key", "ACD_INTENT_MIN_PENDING=1",
		"ACD_INTENT_MAX_PENDING_AGE=1s", trust)
	env = envWith(env, extra...)
	t.Cleanup(func() { unblock(); stopSessionForce(t, env, repo) })
	startSessionJSON(t, ctx, env, repo, "production-measurements", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	metrics := map[string]any{"binary": "release-style", "runtime_scope": "isolated worker"}
	defer func() {
		data, err := json.Marshal(metrics)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("ACD_MEASUREMENT %s", data)
	}()
	pid := readDaemonStatePID(repo)
	idleStart := time.Now()
	cpuBefore, _, beforeOK := productionProcessResources(pid)
	// A bounded observation window measures idle CPU consumption rather than
	// ps %cpu, whose lifetime average includes startup work.
	select {
	case <-time.After(time.Second):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cpuAfter, rss, afterOK := productionProcessResources(pid)
	metrics["idle_resources_available"] = beforeOK && afterOK
	if beforeOK && afterOK {
		metrics["idle_cpu_seconds"] = cpuAfter - cpuBefore
		metrics["idle_sample_seconds"] = time.Since(idleStart).Seconds()
		metrics["idle_rss_kib"] = rss
	}
	checkpoint := func(name string) time.Duration {
		before, err := strconv.ParseInt(readDaemonStateScalar(repo,
			"SELECT COALESCE(MAX(coverage_epoch),0) FROM checkpoints WHERE phase='completed'"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		body := "benchmark checkpoint\n"
		writeFile(t, filepath.Join(repo, name), body)
		waitFor(t, "protected bytes for "+name, 15*time.Second, func() bool {
			// Protection may precede classification. Prove the exact bytes are
			// reachable through a newly completed, retained checkpoint ref.
			row := strings.Fields(readDaemonStateScalar(repo, `SELECT tree_oid || ' ' || checkpoint_ref || ' ' || coverage_epoch || ' ' || observation_epoch
FROM checkpoints WHERE phase='completed' AND retained=1 ORDER BY seq DESC LIMIT 1`))
			if len(row) != 4 {
				return false
			}
			coverage, err := strconv.ParseInt(row[2], 10, 64)
			if err != nil || coverage <= before || row[2] != row[3] {
				return false
			}
			tree, err := runGit(repo, "rev-parse", row[1]+"^{tree}")
			if err != nil || strings.TrimSpace(tree) != row[0] {
				return false
			}
			content, err := runGit(repo, "show", row[1]+":"+name)
			return err == nil && content == body
		})
		return time.Since(start)
	}
	metrics["checkpoint_seconds"] = checkpoint("first.txt").Seconds()
	select {
	case <-called:
	case <-ctx.Done():
		t.Fatal("provider was never called")
	}
	metrics["provider_wait_checkpoint_seconds"] = checkpoint("during-provider.txt").Seconds()
	unblock()
	waitFor(t, "separate publication after provider returns", 20*time.Second, func() bool {
		return readDaemonStateScalar(repo, `SELECT COUNT(DISTINCT e.commit_oid)
FROM capture_events e JOIN capture_ops o ON o.event_seq=e.seq
WHERE e.state='published' AND o.path IN ('first.txt','during-provider.txt')`) == "2"
	})
	for _, name := range []string{"first.txt", "during-provider.txt"} {
		if got := runGitOK(t, repo, "show", "HEAD:"+name); got != "benchmark checkpoint\n" {
			t.Fatalf("published %s bytes=%q", name, got)
		}
	}
}

// ps reports cumulative CPU as [[days-]hours:]minutes:seconds and RSS in KiB.
func productionProcessResources(pid int) (float64, int, bool) {
	out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "time=", "-o", "rss=").Output()
	fields := strings.Fields(string(out))
	if err != nil || len(fields) != 2 {
		return 0, 0, false
	}
	clock := fields[0]
	seconds := 0.0
	if days, rest, ok := strings.Cut(clock, "-"); ok {
		value, err := strconv.ParseFloat(days, 64)
		if err != nil {
			return 0, 0, false
		}
		seconds = value * 24 * 60 * 60
		clock = rest
	}
	timeValue := 0.0
	for _, part := range strings.Split(clock, ":") {
		value, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return 0, 0, false
		}
		timeValue = timeValue*60 + value
	}
	rss, err := strconv.Atoi(fields[1])
	return seconds + timeValue, rss, err == nil
}
