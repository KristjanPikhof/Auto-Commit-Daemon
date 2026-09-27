package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

func TestProductFixJSONPreservesRefusedPlan(t *testing.T) {
	repo, stateDB, _ := makeRegisteredGitRepoStateDB(t)
	if _, err := git.Run(context.Background(), git.RunOpts{Dir: repo}, "checkout", "--detach"); err != nil {
		t.Fatal(err)
	}
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = runProductFix(context.Background(), &out, repo, false, true, false, false, true)
	if err == nil || !ErrorRendered(err) {
		t.Fatalf("fix error=%v rendered=%v", err, ErrorRendered(err))
	}
	var result struct {
		OK    bool          `json:"ok"`
		State productState  `json:"state"`
		Data  fixPlan       `json:"data"`
		Error *productError `json:"error"`
	}
	decoder := json.NewDecoder(&out)
	if err := decoder.Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.OK || result.State != productStateNeedsAction || result.Error == nil || len(result.Data.Unsafe) == 0 {
		t.Fatalf("refusal lost actionable plan: %+v", result)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		t.Fatalf("extra JSON output: %v", err)
	}
	after, err := fileSHA256(stateDB)
	if err != nil || before != after {
		t.Fatalf("refused fix changed state: before=%s after=%s err=%v", before, after, err)
	}
}
