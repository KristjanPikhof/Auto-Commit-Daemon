//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRewriteCommitsSelectionCLI(t *testing.T) {
	repo := tempRepo(t)
	for _, spec := range []struct{ path, body, msg string }{
		{"one.txt", "one\n", "one"},
		{"two.txt", "two\n", "two"},
		{"three.txt", "three\n", "three"},
	} {
		if err := os.WriteFile(filepath.Join(repo, spec.path), []byte(spec.body), 0o644); err != nil {
			t.Fatalf("write %s: %v", spec.path, err)
		}
		runGitOK(t, repo, "add", spec.path)
		runGitOK(t, repo, "commit", "-q", "-m", spec.msg)
	}

	res := runAcd(t, context.Background(), withIsolatedHome(t), "--repo", repo, "--json", "rewrite-commits", "--range", "2-3")
	if res.ExitCode != 0 {
		t.Fatalf("acd rewrite-commits failed exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	var got struct {
		OK       bool `json:"ok"`
		Selected []struct {
			Subject string `json:"subject"`
		} `json:"selected"`
		RecreateUnchanged []struct {
			Subject string `json:"subject"`
		} `json:"recreate_unchanged"`
		SelectedPositions string `json:"selected_positions"`
	}
	if err := json.Unmarshal([]byte(res.Stdout), &got); err != nil {
		t.Fatalf("decode json: %v\n%s", err, res.Stdout)
	}
	if !got.OK || got.SelectedPositions != "2-3" {
		t.Fatalf("unexpected report: %+v", got)
	}
	if len(got.Selected) != 2 || got.Selected[0].Subject != "one" || got.Selected[1].Subject != "two" {
		t.Fatalf("selected subjects mismatch: %+v", got.Selected)
	}
	if len(got.RecreateUnchanged) != 1 || got.RecreateUnchanged[0].Subject != "three" {
		t.Fatalf("recreated subjects mismatch: %+v", got.RecreateUnchanged)
	}
}

func TestRewriteCommitsSelectionCLIRefusesDirtyWorktree(t *testing.T) {
	repo := tempRepo(t)
	if err := os.WriteFile(filepath.Join(repo, "dirty.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty: %v", err)
	}
	res := runAcd(t, context.Background(), withIsolatedHome(t), "--repo", repo, "rewrite-commits", "--last", "1")
	if res.ExitCode == 0 || !strings.Contains(res.Stderr, "dirty index or worktree") {
		t.Fatalf("expected dirty refusal, exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
}
