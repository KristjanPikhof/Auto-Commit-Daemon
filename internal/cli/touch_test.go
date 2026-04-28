package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestTouch_RefreshesLastSeenOnly(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	if err := state.RegisterClient(ctx, db, state.Client{
		SessionID: "s1", Harness: "claude-code",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	_ = db.Close()

	// Track signal calls — touch must NOT signal.
	count, _, restore := installFakeSignal(t)
	defer restore()

	var out bytes.Buffer
	if err := runTouch(ctx, &out, repoDir, "s1", true); err != nil {
		t.Fatalf("runTouch: %v", err)
	}
	if count.Load() != 0 {
		t.Fatalf("touch must not signal, got %d signal calls", count.Load())
	}

	// Verify there's no flush_request queued.
	d2, err := state.Open(ctx, state.DBPathFromGitDir(repoDir+"/.git"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	_, ok, err := state.ClaimNextFlushRequest(ctx, d2)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if ok {
		t.Fatalf("touch must not enqueue flush_request")
	}

	var got touchResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.OK {
		t.Fatalf("expected ok=true, got %+v", got)
	}
}

func TestTouch_LazyRegistersUnknownSession(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()
	repoDir, _, db := makeRepoStateDB(t)
	_ = db.Close()
	_, _, restore := installFakeSignal(t)
	defer restore()

	var out bytes.Buffer
	if err := runTouch(ctx, &out, repoDir, "fresh", true); err != nil {
		t.Fatalf("runTouch: %v", err)
	}
	d2, err := state.Open(ctx, state.DBPathFromGitDir(repoDir+"/.git"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer d2.Close()
	clients, err := state.ListClients(ctx, d2)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(clients) != 1 || clients[0].SessionID != "fresh" {
		t.Fatalf("expected lazy-register, got %+v", clients)
	}
}
