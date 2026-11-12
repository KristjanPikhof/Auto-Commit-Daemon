package state

import (
	"reflect"
	"testing"
)

func TestFinishRetryPreservesReviewPolicyVersion(t *testing.T) {
	for _, version := range []int{4, 5} {
		t.Run(map[int]string{4: "existing", 5: "measured review"}[version], func(t *testing.T) {
			db, _ := openTestDB(t)
			request := finishFixture(t, db, version)
			request.Target.Version = version
			request.Target.Mode = "intent"
			saved := startFinishFixture(t, db, request)
			failFinishFixture(t, db, saved)
			retried, err := RetryFinishRequest(t.Context(), db, saved.ID, nowSeconds()+300, false)
			if err != nil || !reflect.DeepEqual(retried.Target, saved.Target) || retried.Target.Version != version {
				t.Fatalf("retry changed frozen review policy: %+v %v", retried, err)
			}
		})
	}
}
