package state

import (
	"errors"
	"fmt"
	"testing"
)

func TestFinishTargetVersionsPreserveLegacyRequestIdentity(t *testing.T) {
	db, _ := openTestDB(t)
	legacy := finishFixture(t, db, 1).Target
	if _, err := finishTargetJSON(&legacy); err != nil || legacy.Version != 1 {
		t.Fatalf("old frozen target was upgraded: %+v %v", legacy, err)
	}
	legacy.Mode = "intent"
	legacy.Checkpoints = []FinishCheckpoint{{ID: "earlier", Ref: "refs/acd/finish/" + legacy.WorktreeID + "/request/versions/0", CommitOID: "commit", TreeOID: "tree"}}
	if _, err := finishTargetJSON(&legacy); !errors.Is(err, ErrFinishIdentity) {
		t.Fatalf("version-one target accepted new evidence: %v", err)
	}
	legacy.Version = FinishVersion
	if _, err := finishTargetJSON(&legacy); err != nil {
		t.Fatalf("new target rejected recorded evidence: %v", err)
	}
	legacy.Version++
	if _, err := finishTargetJSON(&legacy); !errors.Is(err, ErrFinishIdentity) {
		t.Fatalf("unknown future version accepted: %v", err)
	}
}

func TestFinishCheckpointsStayInsideCurrentProtectionWindow(t *testing.T) {
	for _, boundary := range []string{"head", "branch", "restore", "migration", "legacy captures"} {
		t.Run(boundary, func(t *testing.T) {
			db, _ := openTestDB(t)
			put := func(n int, marker bool) Checkpoint {
				cp := Checkpoint{ID: fmt.Sprintf("cp-%d-0123456789abcdef", n), OperationID: fmt.Sprintf("versions-%d", n), WorktreeID: "0123456789abcdef", Reason: CheckpointReasonWatch, ObservedHead: "head", ObservedRef: "refs/heads/main", TreeOID: fmt.Sprintf("tree-%d", n), CommitOID: fmt.Sprintf("commit-%d", n), CreatedTS: float64(n)}
				cp.Ref = "refs/acd/checkpoints/v1/" + cp.WorktreeID + "/" + cp.ID
				if marker {
					switch boundary {
					case "head":
						cp.ObservedHead = "different-head"
					case "branch":
						cp.ObservedRef = "refs/heads/other"
					case "restore":
						cp.Reason = CheckpointReasonRestore
					case "migration":
						cp.Reason = CheckpointReasonMigration
					}
				}
				if _, err := PrepareCheckpoint(t.Context(), db, cp, checkpointTestDigest); err != nil {
					t.Fatal(err)
				}
				if err := CompleteCheckpoint(t.Context(), db, cp.ID, cp.Ref, cp.CommitOID, float64(n)); err != nil {
					t.Fatal(err)
				}
				if marker && boundary == "legacy captures" {
					if _, err := db.SQL().Exec("INSERT INTO checkpoint_events(checkpoint_id,ord,event_seq) VALUES(?,0,42)", cp.ID); err != nil {
						t.Fatal(err)
					}
				}
				loaded, _, err := checkpointByIDQuery(t.Context(), db.ReadSQL(), cp.ID, false)
				if err != nil {
					t.Fatal(err)
				}
				return loaded
			}
			put(1, false)
			put(2, true)
			current := put(3, false)
			target := put(4, false)
			put(5, false) // A later save is outside this frozen request.
			got, err := FinishCheckpoints(t.Context(), db, target)
			if err != nil || len(got) != 1 || got[0].ID != current.ID {
				t.Fatalf("versions crossed the %s boundary: %+v %v", boundary, got, err)
			}
		})
	}
}
