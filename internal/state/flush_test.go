package state

import (
	"context"
	"database/sql"
	"sync"
	"testing"
)

func TestDrainFlushRequestsBoundedFIFO(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	commands := []string{"wake", "flush_logical", "wake", "wake", "flush_logical"}
	ids := make([]int64, 0, len(commands))
	for _, command := range commands {
		id, err := EnqueueFlushRequest(
			ctx, d, command, false, sql.NullString{})
		if err != nil {
			t.Fatalf("enqueue %s: %v", command, err)
		}
		ids = append(ids, id)
	}

	first, err := DrainFlushRequests(ctx, d, 3,
		sql.NullString{String: "flushed", Valid: true})
	if err != nil {
		t.Fatalf("first drain: %v", err)
	}
	if len(first) != 3 {
		t.Fatalf("first drain len=%d want 3", len(first))
	}
	for i, request := range first {
		if request.ID != ids[i] || request.Command != commands[i] {
			t.Fatalf("first[%d]=%+v want id=%d command=%s",
				i, request, ids[i], commands[i])
		}
		if request.Status != "completed" ||
			!request.AcknowledgedTS.Valid ||
			!request.CompletedTS.Valid ||
			request.AcknowledgedTS.Float64 != request.CompletedTS.Float64 ||
			!request.Note.Valid ||
			request.Note.String != "flushed" {
			t.Fatalf("first[%d] incomplete transition: %+v", i, request)
		}
	}
	assertFlushStatusCount(t, d, "completed", 3)
	assertFlushStatusCount(t, d, "pending", 2)

	second, err := DrainFlushRequests(ctx, d, 10, sql.NullString{})
	if err != nil {
		t.Fatalf("second drain: %v", err)
	}
	if len(second) != 2 ||
		second[0].ID != ids[3] ||
		second[1].ID != ids[4] {
		t.Fatalf("second drain=%+v want final ids %v", second, ids[3:])
	}
	assertFlushStatusCount(t, d, "completed", 5)
	assertFlushStatusCount(t, d, "pending", 0)

	empty, err := DrainFlushRequests(ctx, d, 10, sql.NullString{})
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty drain len=%d err=%v", len(empty), err)
	}
}

func TestDrainFlushRequestsConcurrentOwnership(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx := context.Background()

	const total = 200
	for i := 0; i < total; i++ {
		if _, err := EnqueueFlushRequest(
			ctx, d, "wake", false, sql.NullString{}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	start := make(chan struct{})
	results := make(chan []FlushRequest, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			drained, err := DrainFlushRequests(
				ctx, d, 150, sql.NullString{})
			results <- drained
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent drain: %v", err)
		}
	}
	seen := make(map[int64]struct{}, total)
	for batch := range results {
		for _, request := range batch {
			if _, duplicate := seen[request.ID]; duplicate {
				t.Fatalf("request %d returned by multiple drainers", request.ID)
			}
			seen[request.ID] = struct{}{}
		}
	}
	if len(seen) != total {
		t.Fatalf("unique drained=%d want %d", len(seen), total)
	}
	assertFlushStatusCount(t, d, "completed", total)
	assertFlushStatusCount(t, d, "pending", 0)
}

func TestDrainFlushRequestsCancellationPreservesPending(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := EnqueueFlushRequest(
		context.Background(), d, "wake", false, sql.NullString{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	cancel()

	if _, err := DrainFlushRequests(ctx, d, 1, sql.NullString{}); err == nil {
		t.Fatal("canceled drain returned nil error")
	}
	assertFlushStatusCount(t, d, "pending", 1)
	assertFlushStatusCount(t, d, "completed", 0)
}

func TestDrainFlushRequestsRejectsInvalidLimit(t *testing.T) {
	t.Parallel()
	d, _ := openTestDB(t)
	if _, err := DrainFlushRequests(
		context.Background(), d, 0, sql.NullString{}); err == nil {
		t.Fatal("zero limit returned nil error")
	}
}

func assertFlushStatusCount(t *testing.T, d *DB, status string, want int) {
	t.Helper()
	var got int
	if err := d.SQL().QueryRow(
		`SELECT COUNT(*) FROM flush_requests WHERE status = ?`,
		status).Scan(&got); err != nil {
		t.Fatalf("count flush status %s: %v", status, err)
	}
	if got != want {
		t.Fatalf("flush status %s count=%d want %d", status, got, want)
	}
}
