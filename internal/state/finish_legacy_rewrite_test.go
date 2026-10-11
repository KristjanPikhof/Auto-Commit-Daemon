package state

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestFinishLegacyRewriteGroupInventoryPreservesMemberObjects(t *testing.T) {
	first, second := strings.Repeat("a", 40), strings.Repeat("b", 40)
	valid := fmt.Sprintf(`[ {"old_oid":%q,"original_message":"Original subject\n\nbody"}, {"old_oid":%q,"original_message":"Second"} ]`, first, second)
	for _, tc := range []struct {
		name, raw string
		invalid   bool
		want      []FinishLegacyObject
	}{
		{"canonical_objects", valid, false, []FinishLegacyObject{{first, "commit"}, {second, "commit"}}},
		{"mixed_member", fmt.Sprintf(`[{"old_oid":%q,"original_message":"Valid"},{"old_oid":"invalid"}]`, first), true, []FinishLegacyObject{{first, "commit"}}},
		{"invalid_message_valid_identity", fmt.Sprintf(`[{"old_oid":%q,"original_message":123}]`, first), true, []FinishLegacyObject{{first, "commit"}}},
		{"bare_string_not_legacy_schema", fmt.Sprintf(`[%q]`, first), true, nil},
		{"malformed_json", `[{`, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			db, _ := openLegacyTestDB(t)
			if _, err := db.SQL().ExecContext(ctx, `INSERT INTO rewrite_plans(id,created_ts,updated_ts,revision,branch_ref,expected_head,validation_status) VALUES('old',1,1,1,'refs/heads/main','','valid'); INSERT INTO rewrite_plan_groups(plan_id,ord,members_json,proposed_message,grouping_reason) VALUES('old',0,?,'Proposed subject','Saved boundary')`, tc.raw); err != nil {
				t.Fatal(err)
			}
			manifest, err := InventoryFinishLegacy(ctx, db)
			if tc.invalid != errors.Is(err, ErrFinishIdentity) || !reflect.DeepEqual(manifest.Objects, tc.want) {
				t.Fatalf("inventory=%+v error=%v", manifest.Objects, err)
			}
			var original string
			if err := db.ReadSQL().QueryRowContext(ctx, "SELECT members_json FROM rewrite_plan_groups").Scan(&original); err != nil || original != tc.raw {
				t.Fatalf("legacy metadata changed: %q error=%v", original, err)
			}
		})
	}
}
