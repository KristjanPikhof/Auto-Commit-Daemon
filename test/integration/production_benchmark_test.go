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
		select {
		case called <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		http.Error(w, "temporary benchmark outage", http.StatusServiceUnavailable)
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
	metrics := map[string]any{"binary": "release-style", "scope": "isolated worker"}
	pid := readDaemonStatePID(repo)
	if out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "%cpu=", "-o", "rss=").Output(); err == nil {
		fields := strings.Fields(string(out))
		if len(fields) == 2 {
			metrics["idle_sample_lifetime_cpu_percent"], _ = strconv.ParseFloat(fields[0], 64)
			metrics["idle_rss_kib"], _ = strconv.Atoi(fields[1])
		}
	}
	checkpoint := func(name string) time.Duration {
		start := time.Now()
		writeFile(t, filepath.Join(repo, name), "benchmark checkpoint\n")
		waitFor(t, "completed checkpoint for "+name, 15*time.Second, func() bool {
			query := fmt.Sprintf(`SELECT COUNT(*) FROM checkpoint_events ce
JOIN checkpoints c ON c.id=ce.checkpoint_id
JOIN capture_ops o ON o.event_seq=ce.event_seq
WHERE c.phase='completed' AND o.path=%s`, sqliteLiteral(name))
			count := readDaemonStateScalar(repo, query)
			return count != "0" && count != ""
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
	data, err := json.Marshal(metrics)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ACD_MEASUREMENT %s", data)
}
