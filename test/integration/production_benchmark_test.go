//go:build integration

package integration_test

import (
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Opt-in release-binary measurements share the test-owned runtime and watchdog.
func TestProductionMeasurements(t *testing.T) {
	if os.Getenv("ACD_BENCHMARK") != "1" {
		t.Skip("opt-in production measurements")
	}
	called := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	var calls atomic.Int32
	f := finishIntentFixture(t, func(w http.ResponseWriter, r *http.Request) {
		req := decodeFinishChatRequest(t, r)
		calls.Add(1)
		called <- struct{}{}
		select {
		case <-release:
			writeFinishChatResponse(t, w, req)
		case <-r.Context().Done():
		}
	}, "30s")
	metrics := map[string]any{"binary": "release-style", "runtime_scope": "isolated worker"}
	defer func() {
		b, err := json.Marshal(metrics)
		if err != nil {
			t.Error(err)
			return
		}
		t.Logf("ACD_MEASUREMENT %s", b)
	}()
	pid := readDaemonStatePID(f.Repo)
	before, _, beforeOK := productionProcessResources(pid)
	start := time.Now()
	time.Sleep(time.Second)
	after, rss, afterOK := productionProcessResources(pid)
	metrics["idle_resources_available"] = beforeOK && afterOK
	if beforeOK && afterOK {
		metrics["idle_cpu_seconds"] = after - before
		metrics["idle_sample_seconds"] = time.Since(start).Seconds()
		metrics["idle_rss_kib"] = rss
	}
	checkpoint := func(name string) time.Duration {
		start := time.Now()
		writeFile(t, filepath.Join(f.Repo, name), "benchmark checkpoint\n")
		f.Barrier(t, t.Context())
		finishWaitProtectedFile(t, f, name, "benchmark checkpoint\n", 10*time.Second)
		return time.Since(start)
	}
	metrics["checkpoint_seconds"] = checkpoint("first.txt").Seconds()
	request := finishStartRPC(t, f, map[string]any{"purpose": "Complete measured goal", "message": "Complete measured goal"})
	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("provider did not start")
	}
	metrics["provider_wait_checkpoint_seconds"] = checkpoint("during-provider.txt").Seconds()
	release <- struct{}{}
	result := finishWaitRequest(t, f, request.ID)
	if result.State != "completed" {
		t.Fatalf("finish state %+v", result)
	}
	if runGitOK(t, f.Repo, "show", "HEAD:first.txt") != "benchmark checkpoint\n" {
		t.Fatal("selected bytes changed")
	}
	if _, err := runGit(f.Repo, "show", "HEAD:during-provider.txt"); err == nil {
		t.Fatal("later work entered frozen request")
	}
	metrics["provider_calls"] = calls.Load()
	metrics["commits_created"] = commitCount(t, f.Repo) - 1
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
