package settings

import (
	"context"
	"errors"
	"testing"
)

func TestFinishConfigExperimentsUnavailableWithoutWrites(t *testing.T) {
	svc, _ := testService(t, nil, nil, nil)
	var before, after int
	if err := svc.db.SQL().QueryRow("SELECT total_changes()").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.StartExperiment(context.Background(), ExperimentRequest{}); !errors.Is(err, errExperimentsRemoved) {
		t.Fatalf("start error=%v", err)
	}
	if _, err := svc.CancelExperiment(context.Background(), 1); !errors.Is(err, errExperimentsRemoved) {
		t.Fatalf("cancel error=%v", err)
	}
	if _, err := svc.RevertExperiment(context.Background(), 1); !errors.Is(err, errExperimentsRemoved) {
		t.Fatalf("revert error=%v", err)
	}
	if err := svc.db.SQL().QueryRow("SELECT total_changes()").Scan(&after); err != nil || before != after {
		t.Fatalf("retired experiment wrote state: %d -> %d %v", before, after, err)
	}
}
