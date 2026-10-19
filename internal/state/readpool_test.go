package state

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Finish status remains readable while an unrelated transaction holds the writer.
func TestLoadFinishRequestDoesNotBlockBehindWriter(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()
	request := startFinishFixture(t, d, finishFixture(t, d, 1))

	// Hold the writer connection inside a long transaction. SQLite WAL
	// permits readers to keep working; the writer pool MaxOpenConns=1 means
	// any QueryContext routed through d.conn would block here.
	tx, err := d.SQL().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO daemon_meta(key, value, updated_ts) VALUES (?, ?, ?)`,
		"hold", "1", nowSeconds()); err != nil {
		t.Fatalf("hold write: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		loaded, err := LoadFinishRequest(readCtx, d, request.ID)
		if err == nil && (loaded.ID != request.ID || loaded.State != FinishRunning) {
			err = fmt.Errorf("finish request unavailable under writer hold: %+v", loaded)
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("LoadFinishRequest under writer-hold: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("LoadFinishRequest blocked behind held writer tx — read pool not in use")
	}
}
