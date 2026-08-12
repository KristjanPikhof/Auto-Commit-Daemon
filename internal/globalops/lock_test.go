package globalops

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestConcurrentLifecycleLockWaitsAndHonorsCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operations.db")
	first, err := AcquireUserLock(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := AcquireUserLock(ctx, path); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second lock error=%v, want deadline", err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	second, err := AcquireUserLock(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
}
